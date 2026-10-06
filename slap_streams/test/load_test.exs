defmodule Slap.Streams.LoadTest do
  use Slap.Streams.Test.ClusterCase, async: false

  @moduletag :capture_log
  # Nothing becomes durable until a test flushes the WAL.
  @moduletag settings: %{flush_interval: "1h"}

  alias Slap.SlateDB
  alias Slap.Streams
  alias Slap.Streams.ShardLoad

  @tag child_options: [max_inflight_bytes_per_stream: 100]
  test "an append over the stream's in-flight cap is refused, one on a quiet stream is not" do
    {:ok, :created, _} = acked(fn -> Streams.create("/bp") end, "/bp")
    ctx = ctx_for("/bp")

    big = String.duplicate("x", 1000)
    first = Task.async(fn -> Streams.append("/bp", big) end)
    wait_until(fn -> ShardLoad.snapshot(ctx).inflight_requests == 1 end)
    assert ShardLoad.snapshot(ctx).inflight_bytes > 1000

    assert Streams.append("/bp", "y") == {:error, :overloaded}

    :ok = SlateDB.flush(ctx.db)
    assert {:ok, %{result: :appended}} = Task.await(first)
    wait_until(fn -> ShardLoad.snapshot(ctx).inflight_bytes == 0 end)
    assert {:ok, _} = acked(fn -> Streams.append("/bp", "y") end, "/bp")
  end

  @tag child_options: [max_inflight_bytes_per_shard: 500]
  test "the shard's cap applies across streams" do
    [a, b | _] = same_shard_paths()
    {:ok, :created, _} = acked(fn -> Streams.create(a) end, a)
    {:ok, :created, _} = acked(fn -> Streams.create(b) end, b)
    ctx = ctx_for(a)

    t1 = Task.async(fn -> Streams.append(a, String.duplicate("x", 1000)) end)
    wait_until(fn -> ShardLoad.snapshot(ctx).inflight_requests == 1 end)
    # b is quiet, so its first append is still taken...
    t2 = Task.async(fn -> Streams.append(b, "y") end)
    wait_until(fn -> ShardLoad.snapshot(ctx).inflight_requests == 2 end)
    # ...but not a second one while the shard is over its cap.
    assert Streams.append(b, "z") == {:error, :overloaded}

    :ok = SlateDB.flush(ctx.db)
    assert {:ok, _} = Task.await(t1)
    assert {:ok, _} = Task.await(t2)
  end

  test "a killed stream server's in-flight bytes and waiters are released" do
    {:ok, :created, _} = acked(fn -> Streams.create("/k", body: "a") end, "/k")
    ctx = ctx_for("/k")
    {:ok, {:waiting, _pending}} = Streams.wait("/k", 5)
    append = Task.async(fn -> Streams.append("/k", "b") end)
    wait_until(fn -> ShardLoad.snapshot(ctx).inflight_requests == 1 end)
    assert %{waiters: 1, stream_servers: 1} = ShardLoad.snapshot(ctx)

    Process.exit(server("/k"), :kill)
    assert {:error, _} = Task.await(append)

    wait_until(fn ->
      match?(%{inflight_bytes: 0, inflight_requests: 0, waiters: 0}, ShardLoad.snapshot(ctx))
    end)
  end

  test "a fork's copy is in flight until durable, with the write that finishes it" do
    {:ok, :created, _} =
      acked(fn -> Streams.create("/src", body: String.duplicate("x", 1000)) end, "/src")

    src_db = ctx_for("/src").db
    # On the other shard: the source's writes are flushed, the fork's are not.
    fork = Enum.find(for(i <- 0..200, do: "/f#{i}"), &(ctx_for(&1).n != ctx_for("/src").n))
    ctx = ctx_for(fork)
    task = Task.async(fn -> Streams.create(fork, forked_from: "/src") end)

    # The copy's start and its one page, and the finish.
    wait_until(fn ->
      :ok = SlateDB.flush(src_db)
      ShardLoad.snapshot(ctx).inflight_requests == 3
    end)

    assert ShardLoad.snapshot(ctx).inflight_bytes > 1000
    assert Task.yield(task, 0) == nil

    :ok = SlateDB.flush(ctx.db)
    assert {:ok, :created, %{next_offset: 1004}} = Task.await(task)
    wait_until(fn -> ShardLoad.snapshot(ctx).inflight_bytes == 0 end)
  end

  test "telemetry: append latency and load" do
    parent = self()
    id = "load-test-#{System.unique_integer()}"

    :telemetry.attach_many(
      id,
      [[:slap, :streams, :append, :acknowledged], [:slap, :streams, :append, :rejected]],
      fn event, measurements, metadata, _ -> send(parent, {event, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    {:ok, :created, _} = acked(fn -> Streams.create("/t") end, "/t")
    {:ok, _} = acked(fn -> Streams.append("/t", "hello") end, "/t")

    assert_receive {[:slap, :streams, :append, :acknowledged], %{duration: d, bytes: 5},
                    %{result: :appended, shard: _}}

    assert d > 0
  end

  defp same_shard_paths do
    shard = Streams.Cluster.shard_for("/s0")
    for i <- 0..200, p = "/s#{i}", Streams.Cluster.shard_for(p) == shard, do: p
  end

  defp wait_until(fun, tries \\ 200) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("timed out")
      true -> Process.sleep(10) && wait_until(fun, tries - 1)
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
