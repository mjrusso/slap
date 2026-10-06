defmodule Slap.Streams.DurabilityTest do
  use Slap.Streams.Test.ClusterCase, async: false

  alias Slap.SlateDB
  alias Slap.Streams
  alias Slap.Streams.Test.FakeClock
  alias Slap.Streams.Wait

  @moduletag :capture_log

  describe "pipelining" do
    test "1,000 appends from one producer, 5 in flight, are acknowledged in order" do
      {:ok, :created, _} = Streams.create("/pipe", content_type: "text/plain")
      pid = server("/pipe")
      window = 5

      # Requests from one process arrive in the order sent, like a client
      # pipelining over one connection.
      send_append = fn seq ->
        req = %{body: "m#{seq}", producer: {"w", 0, seq}}
        {seq, :gen_server.send_request(pid, {:append, req})}
      end

      {first, rest} = Enum.split(0..999, window)
      pending = :queue.from_list(Enum.map(first, send_append))

      acks =
        Enum.reduce(rest ++ List.duplicate(nil, window), {pending, []}, fn next,
                                                                           {pending, acks} ->
          {{:value, {seq, request}}, pending} = :queue.out(pending)
          {:reply, reply} = :gen_server.receive_response(request, 5_000)
          pending = if next, do: :queue.in(send_append.(next), pending), else: pending
          {pending, [{seq, reply} | acks]}
        end)
        |> elem(1)
        |> Enum.reverse()

      assert Enum.count(acks) == 1_000

      for {seq, reply} <- acks do
        assert {:ok, %{result: :appended, producer: {0, ^seq}}} = reply
      end

      offsets = for {_seq, {:ok, %{next_offset: o}}} <- acks, do: o
      assert offsets == Enum.sort(offsets) and offsets == Enum.uniq(offsets)

      {:ok, read} = Streams.read("/pipe", 0, max_bytes: 100_000_000)
      assert bodies(read) == Enum.map(0..999, &"m#{&1}")
    end
  end

  describe "with a long flush interval" do
    # Nothing becomes durable until the test flushes the WAL.
    @describetag settings: %{flush_interval: "1h"}

    test "an expiry read waits for removal to become durable" do
      FakeClock.install(1_700_000_000_000)
      on_exit(&FakeClock.uninstall/0)

      {:ok, :created, _} =
        acked(
          fn -> Streams.create("/expiry", expires_at_ms: FakeClock.now_ms() + 500) end,
          "/expiry"
        )

      FakeClock.advance(500)
      pid = server("/expiry")
      :erlang.trace(pid, true, [:receive])
      read = Task.async(fn -> Streams.read("/expiry", 0) end)
      assert_receive {:trace, ^pid, :receive, {_, _, :read_info}}, 1_000
      :erlang.trace(pid, false, [:receive])

      assert Task.yield(read, 50) == nil
      assert {:ok, ["/expiry"]} = Streams.list("/expiry")

      :ok = SlateDB.flush(ctx_for("/expiry").db)
      assert {:error, :not_found} = Task.await(read)
      assert {:ok, []} = Streams.list("/expiry")
    end

    test "readers see an append only once it is acknowledged" do
      {:ok, :created, _} = acked(fn -> Streams.create("/gate", body: "a") end, "/gate")
      {:ok, {:waiting, pending}} = Streams.wait("/gate", 5)
      ref = Wait.ref(pending)

      append = Task.async(fn -> Streams.append("/gate", "b") end)
      Process.sleep(100)

      # Written to SlateDB's memtable, but not durable: not acknowledged,
      # and not visible to readers, head or waiters.
      assert Task.yield(append, 0) == nil
      assert {:ok, %{messages: [{0, "a"}], next_offset: 5}} = Streams.read("/gate", 0)
      assert {:ok, %{next_offset: 5}} = Streams.head("/gate")
      refute_received {:slap_streams_wake, ^ref, _}

      # A reply that depends on the pending write waits for it too.
      duplicate_check = Task.async(fn -> Streams.create("/gate", body: "a") end)
      Process.sleep(50)
      assert Task.yield(duplicate_check, 0) == nil

      :ok = SlateDB.flush(ctx_for("/gate").db)
      assert {:ok, %{next_offset: 10}} = Task.await(append)
      assert {:ok, :exists, %{next_offset: 10}} = Task.await(duplicate_check)
      assert_receive {:slap_streams_wake, ^ref, :data}
      assert {:ok, %{messages: [{0, "a"}, {5, "b"}]}} = Streams.read("/gate", 0)
    end

    test "a reloaded stream does not serve an append that is not durable yet" do
      {:ok, :created, _} = acked(fn -> Streams.create("/reload", body: "a") end, "/reload")
      Process.flag(:trap_exit, true)
      append = Task.async(fn -> Streams.append("/reload", "unacked") end)
      Process.sleep(100)
      assert Task.yield(append, 0) == nil

      # The stream server dies with its write in SlateDB's memtable, not
      # durable, and not acknowledged.
      Process.exit(server("/reload"), :kill)

      # The next request reloads the stream. It must not serve "unacked"
      # while it could still be lost: it waits until it is durable.
      read = Task.async(fn -> Streams.read("/reload", 0) end)
      Process.sleep(200)
      assert Task.yield(read, 0) == nil

      :ok = SlateDB.flush(ctx_for("/reload").db)
      assert {:ok, %{messages: [{0, "a"}, {5, "unacked"}]}} = Task.await(read)
    end

    test "stopping the shard fails in-flight appends instead of acknowledging them" do
      {:ok, :created, _} = acked(fn -> Streams.create("/stop") end, "/stop")
      append = Task.async(fn -> Streams.append("/stop", "never acked") end)
      Process.sleep(100)
      assert Task.yield(append, 0) == nil

      stop_supervised!(Streams.Cluster)
      assert Task.await(append) == {:error, :unavailable}
    end
  end

  # Runs `fun` in a task and flushes the WAL until it replies.
  defp acked(fun, path) do
    task = Task.async(fun)
    db = ctx_for(path).db

    Stream.repeatedly(fn ->
      :ok = SlateDB.flush(db)
      Task.yield(task, 20)
    end)
    |> Enum.find_value(fn
      {:ok, result} -> result
      nil -> nil
    end)
  end
end
