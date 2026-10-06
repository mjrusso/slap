defmodule Slap.KV.Test.OtherCluster do
  use Slap.KV.Cluster, otp_app: :slap_kv
end

defmodule Slap.KVTest do
  use Slap.KV.Test.ClusterCase, async: false

  alias Slap.KV.Test.OtherCluster

  test "a durable write emits an acknowledgement event" do
    id = {__MODULE__, make_ref()}
    event = [:slap, :kv, :write, :acknowledged]
    parent = self()

    :ok =
      :telemetry.attach(
        id,
        event,
        &__MODULE__.send_telemetry/4,
        parent
      )

    on_exit(fn -> :telemetry.detach(id) end)

    assert {:ok, _} = KV.put("telemetry", "key", "value")
    assert_receive {^event, %{duration: duration}, %{op: :put, shard: _}}, 1_000
    assert duration >= 0
  end

  def send_telemetry(event, measurements, metadata, parent),
    do: send(parent, {event, measurements, metadata})

  test "invalid shard options fail before the cluster starts" do
    assert_raise ArgumentError, ~r/:partition_writers/, fn ->
      OtherCluster.start_link(
        store: :memory,
        shards: 1,
        child_options: [partition_writers: 0]
      )
    end
  end

  test "a second cluster keeps its rows separate" do
    other = OtherCluster
    start_supervised!({other, store: :memory, shards: 1, child_options: [partition_writers: 64]})

    {:ok, {:local, ctx}} = other.lookup(0)
    assert {:ok, 64} = Registry.meta(ctx.registry, {KV.PartitionWriter, 0})

    assert {:ok, _} = KV.put("p", "k", "other", cluster: other)
    assert {:ok, nil} = KV.get("p", "k")
    assert {:ok, %{value: "other"}} = KV.get("p", "k", cluster: other)
    assert {:ok, _} = KV.put("p", "k", "default")
    assert {:ok, %{value: "default"}} = KV.get("p", "k")
    assert :ok = KV.delete("p", "k", cluster: other)
    assert {:ok, nil} = KV.get("p", "k", cluster: other)
  end

  describe "get, put and delete" do
    test "a row is written, read back with its version, and deleted" do
      assert {:ok, nil} = KV.get("p", "k")
      assert {:ok, v1} = KV.put("p", "k", "one")
      assert {:ok, %{value: "one", version: ^v1}} = KV.get("p", "k")

      assert {:ok, v2} = KV.put("p", "k", "two")
      assert v2 > v1
      assert {:ok, %{value: "two", version: ^v2}} = KV.get("p", "k")

      assert :ok = KV.delete("p", "k")
      assert {:ok, nil} = KV.get("p", "k")
      assert :ok = KV.delete("p", "k")
    end

    test "keys and values are arbitrary binaries" do
      key = <<0, 255, ?/, 1>>
      value = :crypto.strong_rand_bytes(100_000)
      {:ok, _} = KV.put(<<0, 1>>, key, value)
      assert {:ok, %{value: ^value}} = KV.get(<<0, 1>>, key)
      {:ok, _} = KV.put("p", "empty value", "")
      assert {:ok, %{value: ""}} = KV.get("p", "empty value")
    end

    test "invalid arguments are bad requests" do
      assert {:error, {:bad_request, :invalid_partition}} = KV.get("", "k")
      assert {:error, {:bad_request, :invalid_key}} = KV.put("p", "", "v")
      assert {:error, {:bad_request, :invalid_value}} = KV.put("p", "k", :v)
      assert {:error, {:bad_request, :key_too_long}} = KV.get("p", String.duplicate("k", 65_534))
      assert {:error, {:bad_request, :invalid_version}} = KV.put("p", "k", "v", if_version: -1)
      assert {:error, {:bad_request, :invalid_version}} = KV.put("p", "k", "v", if_version: nil)
      assert {:error, {:bad_request, :invalid_version}} = KV.delete("p", "k", if_version: :absent)
      assert {:error, {:bad_request, :invalid_deadline}} = KV.put("p", "k", "v", deadline: 1.0)

      assert_raise ArgumentError, ~r/:consistency/, fn ->
        KV.get("p", "k", consistency: :any)
      end

      assert_raise ArgumentError, ~r/:timeout/, fn -> KV.get("p", "k", timeout: -1) end
      assert_raise ArgumentError, ~r/:cluster/, fn -> KV.get("p", "k", cluster: 1) end

      assert_raise ArgumentError, ~r/:with_versions/, fn ->
        KV.scan("p", with_versions: :yes)
      end

      assert_raise ArgumentError, fn -> KV.put("p", "k", "v", if_vesion: :absent) end

      assert_raise ArgumentError, fn -> KV.delete("p", "k", if_vesion: 1) end
      assert_raise ArgumentError, fn -> KV.get("p", "k", consistensy: :linearizable) end
      assert_raise ArgumentError, fn -> KV.scan("p", limt: 1) end

      assert {:ok, nil} = KV.get("p", "k")
    end
  end

  describe "deadlines" do
    test "a write is applied only before its deadline" do
      later = System.os_time(:millisecond) + 60_000
      assert {:ok, v} = KV.put("p", "k", "v", deadline: later)

      past = System.os_time(:millisecond) - 1
      assert {:error, :deadline_exceeded} = KV.put("p", "k", "late", deadline: past)
      assert {:error, :deadline_exceeded} = KV.delete("p", "k", deadline: past)
      assert {:ok, %{value: "v", version: ^v}} = KV.get("p", "k")
    end

    test "a write is not applied at its deadline" do
      # Each is handled in the millisecond it names, or later.
      for i <- 1..20 do
        deadline = System.os_time(:millisecond)
        assert {:error, :deadline_exceeded} = KV.put("p", "k#{i}", "v", deadline: deadline)
      end

      assert {:ok, %{rows: []}} = KV.scan("p")
    end

    test "the deadline holds for a write that waits for its partition writer" do
      writer = partition_writer("p")
      :ok = :sys.suspend(writer)
      deadline = System.os_time(:millisecond) + 20
      late = Task.async(fn -> KV.put("p", "k", "late", deadline: deadline) end)

      # Sent before its deadline, and handled after it.
      1 = :erlang.trace(writer, true, [:receive])
      pid = late.pid
      assert_receive {:trace, ^writer, :receive, {:"$gen_call", {^pid, _}, {:write, _, _, _}}}
      1 = :erlang.trace(writer, false, [:receive])
      Process.sleep(max(deadline - System.os_time(:millisecond) + 1, 0))

      :ok = :sys.resume(writer)
      assert {:error, :deadline_exceeded} = Task.await(late)
      assert {:ok, nil} = KV.get("p", "k", consistency: :linearizable)
    end
  end

  describe "linearizable reads" do
    test "see a write that was sent before them, and has no reply yet" do
      writer = partition_writer("p")
      :ok = :sys.suspend(writer)
      1 = :erlang.trace(writer, true, [:receive])
      put = Task.async(fn -> KV.put("p", "k", "pending") end)
      pid = put.pid
      assert_receive {:trace, ^writer, :receive, {:"$gen_call", {^pid, _}, {:write, _, _, _}}}
      1 = :erlang.trace(writer, false, [:receive])

      # A normal read does not wait for it; a linearizable one is behind it.
      assert {:ok, nil} = KV.get("p", "k")
      read = Task.async(fn -> KV.get("p", "k", consistency: :linearizable) end)

      :ok = :sys.resume(writer)
      assert {:ok, v} = Task.await(put)
      assert {:ok, %{value: "pending", version: ^v}} = Task.await(read)
    end
  end

  describe "partition writers" do
    test "a stalled partition writer does not hold up another's partitions on its shard" do
      # Two partitions on one shard, with different partition writers.
      [a, b] =
        Stream.map(1..1_000, &"w#{&1}")
        |> Enum.filter(&(KV.Cluster.shard_for(&1) == 0))
        |> Enum.uniq_by(&index/1)
        |> Enum.take(2)

      :ok = :sys.suspend(partition_writer(a))
      stalled = Task.async(fn -> KV.put(a, "k", "v") end)

      assert {:ok, _} = KV.put(b, "k", "v")
      assert Task.yield(stalled, 100) == nil

      :ok = :sys.resume(partition_writer(a))
      assert {:ok, _} = Task.await(stalled)
    end
  end

  describe "scan" do
    setup do
      for k <- ~w(a b c d e) do
        {:ok, _} = KV.put("s", "item/#{k}", "v#{k}")
      end

      {:ok, _} = KV.put("s", "other", "o")
      {:ok, _} = KV.put("s2", "item/z", "not in s")
      :ok
    end

    test "returns a partition's rows in key order, and only its own" do
      assert {:ok, %{rows: rows, cursor: nil}} = KV.scan("s")

      assert rows == [
               {"item/a", "va"},
               {"item/b", "vb"},
               {"item/c", "vc"},
               {"item/d", "vd"},
               {"item/e", "ve"},
               {"other", "o"}
             ]
    end

    test "prefix and range bounds" do
      assert {:ok, %{rows: rows}} = KV.scan("s", prefix: "item/")
      assert Enum.map(rows, &elem(&1, 0)) == ~w(item/a item/b item/c item/d item/e)

      assert {:ok, %{rows: rows}} = KV.scan("s", gte: "item/b", lt: "item/d")
      assert Enum.map(rows, &elem(&1, 0)) == ~w(item/b item/c)
    end

    test "scan can include versions" do
      assert {:ok, %{rows: rows}} = KV.scan("s", prefix: "item/", with_versions: true)

      for {key, value, version} <- rows do
        assert {:ok, %{value: ^value, version: ^version}} = KV.get("s", key)
      end
    end

    test "pages continue from the cursor" do
      assert {:ok, %{rows: page1, cursor: cursor}} = KV.scan("s", prefix: "item/", limit: 2)
      assert Enum.map(page1, &elem(&1, 0)) == ~w(item/a item/b)

      assert {:ok, %{rows: page2, cursor: cursor}} =
               KV.scan("s", prefix: "item/", limit: 2, cursor: cursor)

      assert Enum.map(page2, &elem(&1, 0)) == ~w(item/c item/d)

      assert {:ok, %{rows: [{"item/e", "ve"}], cursor: nil}} =
               KV.scan("s", prefix: "item/", limit: 2, cursor: cursor)
    end

    test "a page that ends exactly at the last row has no cursor" do
      assert {:ok, %{rows: [_, _, _, _, _], cursor: nil}} =
               KV.scan("s", prefix: "item/", limit: 5)
    end

    test "invalid options are bad requests" do
      assert {:error, {:bad_request, :invalid_limit}} = KV.scan("s", limit: 0)
      assert {:error, {:bad_request, :invalid_limit}} = KV.scan("s", limit: 1_001)
      assert {:error, {:bad_request, :invalid_range}} = KV.scan("s", prefix: 1)
      assert {:error, {:bad_request, :invalid_partition}} = KV.scan("")
    end
  end
end
