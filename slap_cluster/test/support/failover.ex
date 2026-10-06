defmodule Slap.Cluster.Test.Failover do
  @moduledoc false
  # The multi-node scenarios shared by the strategies' tests. Each node is its
  # own OS process (:peer) running the counter app (test/support/counter.ex)
  # under a strategy; load comes from workers incrementing keys through
  # random nodes, and every acknowledged increment must survive: no
  # acknowledged value may be handed out twice for the same key.
  #
  # `start` is a function of no arguments that starts one node and returns
  # its name; `ttl` bounds how long a failover may take.

  import ExUnit.Assertions

  alias Slap.Cluster.Test.{Counter, Peers, TestCluster}

  @shards 8

  def shards, do: @shards

  @doc "Starts a node running the cluster with `strategy` on `store`."
  def start_node(store, strategy, vm_args \\ []) do
    {peer, node} = Peers.start("n", vm_args)
    sink = spawn(fn -> Stream.repeatedly(fn -> receive do: (_ -> :ok) end) |> Stream.run() end)

    :ok =
      Peers.start_cluster(node,
        store: store,
        shards: @shards,
        settings: %{manifest_poll_interval: "100ms", flush_interval: "10ms"},
        shard_children: {Counter, :child_specs, [sink]},
        strategy: strategy
      )

    ExUnit.Callbacks.on_exit(fn ->
      try do
        :peer.stop(peer)
      catch
        # Killed by the test.
        :exit, _ -> :ok
      end
    end)

    node
  end

  # -- Scenarios ------------------------------------------------------------

  def placement(start) do
    a = start.()
    assert {:ok, _} = await_spread([a], @shards)

    b = start.()
    c = start.()
    assert {:ok, owned} = await_spread([a, b, c], 3)
    assert Enum.all?(Map.values(owned), &(&1 != []))

    # Any node reaches every shard's owner.
    for key <- Enum.map(1..40, &"k#{&1}"), node <- [a, b, c] do
      assert {:ok, _} = :erpc.call(node, Counter, :increment, [TestCluster, key])
    end

    assert :erpc.call(a, Counter, :get, [TestCluster, "k1"]) == 3

    # A clean stop hands its shards over.
    :ok = Peers.stop_cluster(c)
    assert {:ok, _} = await_spread([a, b], 4)
    assert :erpc.call(b, Counter, :get, [TestCluster, "k1"]) == 3
  end

  def kill(start, ttl) do
    nodes = [a, b, c] = for _ <- 1..3, do: start.()
    {:ok, _} = await_spread(nodes, 3)

    load = start_load(nodes)
    Process.sleep(1_000)
    Peers.kill(c)
    assert {:ok, _} = await_spread([a, b], 4, ttl * 5)
    Process.sleep(1_000)
    check_load(load, [a, b])
  end

  # Pauses a node until the others have taken its shards, then resumes it:
  # it must give up (or be fenced out of) its shards, and the three share
  # them again.
  def pause(start, ttl) do
    nodes = [a, b, c] = for _ <- 1..3, do: start.()
    {:ok, _} = await_spread(nodes, 3)

    load = start_load(nodes)
    Process.sleep(1_000)
    os_pid = Peers.pause(c)
    assert {:ok, _} = await_spread([a, b], 4, ttl * 5)
    Peers.resume(c, os_pid)
    assert {:ok, _} = await_spread(nodes, 3, ttl * 5)
    Process.sleep(1_000)
    check_load(load, nodes)
  end

  # -- Helpers --------------------------------------------------------------

  # Waits until the live nodes own every shard between them, each at most
  # `max`, with every shard on exactly one node.
  def await_spread(nodes, max, timeout \\ 20_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      owned = Map.new(nodes, &{&1, Peers.local_shards(&1)})
      all = owned |> Map.values() |> Enum.reject(&(&1 == :down)) |> List.flatten()

      cond do
        Enum.sort(all) == Enum.to_list(0..(@shards - 1)) and
            Enum.all?(Map.values(owned), &(&1 != :down and length(&1) <= max)) ->
          {:ok, owned}

        System.monotonic_time(:millisecond) > deadline ->
          {:timeout, owned}

        true ->
          Process.sleep(200)
          nil
      end
    end)
    |> Enum.find(& &1)
  end

  # Workers increment random keys through random live nodes, recording
  # every acknowledged value.
  def start_load(nodes) do
    stop = :atomics.new(1, [])
    tasks = for w <- 1..8, do: Task.async(fn -> load_loop(nodes, stop, w, []) end)
    {stop, tasks}
  end

  defp load_loop(nodes, stop, w, acks) do
    if :atomics.get(stop, 1) == 1 do
      acks
    else
      key = "key#{:rand.uniform(20)}"
      node = Enum.random(nodes)

      acks =
        try do
          case :erpc.call(node, Counter, :increment, [TestCluster, key], 5_000) do
            {:ok, value} -> [{key, value} | acks]
            _ -> acks
          end
        catch
          _, _ -> acks
        end

      load_loop(nodes, stop, w, acks)
    end
  end

  def check_load({stop, tasks}, live) do
    :atomics.put(stop, 1, 1)
    acks = tasks |> Enum.map(&Task.await(&1, 60_000)) |> List.flatten()
    assert Enum.count_until(acks, 101) > 100

    for {key, values} <- Enum.group_by(acks, &elem(&1, 0), &elem(&1, 1)) do
      # A lost acknowledged increment would be handed out again.
      assert length(values) == length(Enum.uniq(values)), "#{key}: a value was acknowledged twice"
      stored = :erpc.call(hd(live), Counter, :get, [TestCluster, key])

      assert stored >= Enum.max(values),
             "#{key}: stored #{stored} < acknowledged #{Enum.max(values)}"
    end
  end

  def wait_until(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        fun.() ->
          :ok

        System.monotonic_time(:millisecond) > deadline ->
          flunk("timed out")

        true ->
          Process.sleep(50)
          nil
      end
    end)
    |> Enum.find(& &1)
  end

  def tmp_dir do
    dir = Path.join(System.tmp_dir!(), "slatedb-failover-#{System.unique_integer([:positive])}")
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  def s3_store(prefix) do
    {:url,
     "s3://#{System.get_env("SLAP_TEST_S3_BUCKET", "slatedb-test")}/#{prefix}-#{System.unique_integer([:positive])}",
     [
       aws_endpoint: System.fetch_env!("SLAP_TEST_S3_ENDPOINT"),
       aws_allow_http: "true",
       aws_region: System.get_env("SLAP_TEST_S3_REGION", "us-east-1"),
       aws_access_key_id: System.get_env("SLAP_TEST_S3_KEY", "rustfsadmin"),
       aws_secret_access_key: System.get_env("SLAP_TEST_S3_SECRET", "rustfsadmin")
     ]}
  end
end
