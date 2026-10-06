defmodule Slap.Cluster.Test.Cluster do
  use Slap.Cluster, otp_app: :slap_cluster
end

defmodule Slap.Cluster.Test.ValidatingStrategy do
  @moduledoc false

  def validate_options(opts) do
    Keyword.validate!(opts, [:accepted])
    :ok
  end
end

defmodule Slap.ClusterTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  # telemetry is an optional dependency: called through an attribute, so the
  # test compiles without it (its tests are excluded then).
  @telemetry :telemetry

  alias Slap.Cluster.{Config, Hash, Host}
  alias Slap.Cluster.Test.{Cluster, Counter, GatedChild}
  alias Slap.SlateDB

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "slatedb-cluster-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  defp start_cluster(opts) do
    opts =
      Keyword.merge(
        [
          shard_children: {Counter, :child_specs, [self()]},
          settings: %{manifest_poll_interval: "100ms", flush_interval: "10ms"},
          strategy: {Slap.Cluster.Strategy.Local, backoff_base: 50, backoff_max: 1_000}
        ],
        opts
      )

    start_supervised!({Cluster, opts})
  end

  describe "configuration" do
    test "invalid call settings raise before routing" do
      assert_raise ArgumentError, ~r/key must be a binary/, fn -> Cluster.shard_for(1) end

      assert_raise ArgumentError, ~r/:timeout/, fn ->
        Cluster.call(0, {Counter, :get_local, ["x"]}, timeout: -1)
      end

      assert_raise ArgumentError, ~r/:retries/, fn ->
        Cluster.call(0, {Counter, :get_local, ["x"]}, retries: -1)
      end

      assert_raise ArgumentError, ~r/mfa must be/, fn -> Cluster.call(0, :bad) end
    end

    test "store and shards are required, and options are checked" do
      assert_raise ArgumentError, ~r/:store/, fn -> Cluster.start_link(shards: 4) end
      assert_raise ArgumentError, ~r/:shards/, fn -> Cluster.start_link(store: :memory) end

      assert_raise ArgumentError, ~r/positive integer/, fn ->
        Cluster.start_link(store: :memory, shards: 0)
      end

      assert_raise ArgumentError, ~r/unknown options/, fn ->
        Cluster.start_link(store: :memory, shards: 4, shardz: 4)
      end

      assert_raise ArgumentError, ~r/:shards must be/, fn ->
        Cluster.start_link(store: :memory, shards: false)
      end

      assert_raise ArgumentError, ~r/:close_timeout must be/, fn ->
        Cluster.start_link(store: :memory, shards: 4, close_timeout: nil)
      end

      assert_raise ArgumentError, ~r/:cache/, fn ->
        Cluster.start_link(store: :memory, shards: 4, cache: [bytes: 1])
      end

      assert_raise ArgumentError, ~r/:settings invalid settings/, fn ->
        Cluster.start_link(store: :memory, shards: 1, settings: %{flush_interval: "bogus"})
      end

      assert_raise ArgumentError, ~r/lease_tll/, fn ->
        Cluster.start_link(
          store: :memory,
          shards: 4,
          strategy: {Slap.Cluster.Strategy.ObjectLease, lease_tll: 15_000}
        )
      end

      assert_raise ArgumentError, ~r/unknown keys \[:typo\]/, fn ->
        Config.load(Cluster, nil,
          store: :memory,
          shards: 4,
          strategy: {Slap.Cluster.Test.ValidatingStrategy, typo: true}
        )
      end

      assert_raise ArgumentError, ~r/strategy.*Slap.Cluster.Strategy/, fn ->
        Config.load(Cluster, nil, store: :memory, shards: 4, strategy: Slap.Cluster.Strategy)
      end
    end

    test "shard paths are fixed-width" do
      config = %Slap.Cluster.Config{cluster: Cluster, store: :memory, shards: 64}
      assert Config.shard_path(config, 7) == "shard-007"

      config = %{config | shards: 5000, path: "streams"}
      assert Config.shard_path(config, 7) == "streams/shard-0007"
    end
  end

  describe "startup probe" do
    test "a store that ignores conditional writes stops the cluster from starting" do
      Process.flag(:trap_exit, true)

      assert {:error, {:shutdown, {:failed_to_start_child, Slap.Cluster.Probe, reason}}} =
               Cluster.start_link(store: :memory_ignoring_preconditions, shards: 2)

      assert {:probe_failed, steps} = reason
      assert {:failed, _} = steps[:create_again]
    end

    test "can be turned off" do
      start_cluster(store: :memory_ignoring_preconditions, shards: 1, probe: false)
      assert Cluster.local_shards() == [0]
    end
  end

  describe "with the Local strategy" do
    setup do
      %{dir: tmp_dir()}
    end

    test "opens every shard before start_link returns", %{dir: dir} do
      start_cluster(store: {:local, dir}, shards: 8, cache: [capacity_bytes: 16_000_000])

      assert Cluster.local_shards() == Enum.to_list(0..7)
      assert Enum.all?(Cluster.assignments(), &match?({_, {:local, _}}, &1))

      for n <- 0..7 do
        assert_received {:counter_started, ^n, _}
        assert {:ok, {:local, %Slap.Cluster.Shard{n: ^n}}} = Cluster.lookup(n)
        assert File.dir?(Path.join(dir, "shard-00#{n}"))
      end

      assert_raise ArgumentError, fn -> Cluster.lookup(8) end
    end

    test "acknowledged increments are durable and survive a restart", %{dir: dir} do
      start_cluster(store: {:local, dir}, shards: 8)
      keys = for i <- 1..40, do: "key-#{i}"

      results =
        1..400
        |> Task.async_stream(
          fn i ->
            key = Enum.at(keys, rem(i, 40))
            {key, Counter.increment(Cluster, key)}
          end,
          max_concurrency: 64
        )
        |> Enum.map(fn {:ok, result} -> result end)

      for {key, values} <- Enum.group_by(results, &elem(&1, 0), &elem(&1, 1)) do
        assert Enum.sort(values) == Enum.map(1..10, &{:ok, &1}), key
      end

      # Every acknowledgement came after its seq was durable, in seq order
      # per shard.
      acks = collect_acks()
      assert Enum.count(acks) == 400

      for {n, seqs} <- Enum.group_by(acks, &elem(&1, 0), &elem(&1, 1)) do
        assert seqs == Enum.sort(seqs), "shard #{n}"
      end

      stop_supervised!(Cluster)
      start_cluster(store: {:local, dir}, shards: 8)

      for key <- keys, do: assert(Counter.get(Cluster, key) == 10)
    end

    test "notifications for one process arrive in seq order", %{dir: dir} do
      start_cluster(store: {:local, dir}, shards: 1, shard_children: nil)
      {:ok, {:local, ctx}} = Cluster.lookup(0)

      # Asked in seq order, as a single writer does, while some are durable
      # and some are not.
      seqs =
        for i <- 1..200 do
          {:ok, seq} = SlateDB.put(ctx.db, "k#{i}", "v")
          Slap.Cluster.notify_when_durable(ctx, seq, seq)
          if rem(i, 50) == 0, do: Process.sleep(20)
          seq
        end

      assert received(length(seqs)) == seqs
      assert Slap.Cluster.durable_seq(ctx) >= List.last(seqs)

      Slap.Cluster.notify_when_durable(ctx, hd(seqs), :old)
      assert_receive {:slap_cluster_durable, :old}, 1_000
    end

    test "waiters that become durable together are released lowest seq first", %{dir: dir} do
      # A long flush interval, so nothing becomes durable until the flush.
      start_cluster(
        store: {:local, dir},
        shards: 1,
        shard_children: nil,
        settings: %{flush_interval: "1h"}
      )

      {:ok, {:local, ctx}} = Cluster.lookup(0)
      # The WAL flush timer ticks once right after open; let that pass.
      {:ok, _} = SlateDB.put(ctx.db, "warm-up", "v")
      :ok = SlateDB.flush(ctx.db)
      Process.sleep(100)

      seqs =
        for i <- 1..100 do
          {:ok, seq} = SlateDB.put(ctx.db, "k#{i}", "v")
          seq
        end

      for seq <- Enum.shuffle(seqs), do: Slap.Cluster.notify_when_durable(ctx, seq, seq)
      refute_receive {:slap_cluster_durable, _}, 100
      :ok = SlateDB.flush(ctx.db)
      assert received(length(seqs)) == seqs
    end

    test "children stop before the database closes", %{dir: dir} do
      start_cluster(store: {:local, dir}, shards: 4)
      stop_supervised!(Cluster)

      for n <- 0..3 do
        assert_received {:counter_stopped, ^n, :stopping, true}
      end
    end

    test "a fenced shard stops its children, then reopens after a backoff", %{dir: dir} do
      start_cluster(store: {:local, dir}, shards: 4)
      assert_receive {:counter_started, 2, first}

      # Another writer opens shard 2's database.
      {:ok, intruder} =
        SlateDB.open("shard-002",
          store: {:local, dir},
          settings: %{manifest_poll_interval: "100ms"}
        )

      {:ok, %{ref: sub_ref}} = SlateDB.subscribe(intruder, :intruder)

      ref = Process.monitor(first)
      assert_receive {:counter_stopped, 2, :fenced, _}, 2_000
      assert_receive {:DOWN, ^ref, _, _, _}

      # Local opens it again, which fences the intruder in turn.
      assert_receive {:counter_started, 2, second}, 2_000
      assert second != first
      assert_receive {:slap_slatedb_closed, ^sub_ref, :intruder, :fenced}, 2_000
      SlateDB.close(intruder)

      assert {:ok, 1} = Counter.increment(Cluster, key_on_shard(2, 4))
      refute_received {:counter_stopped, _, _, _}
    end

    test "if the database process crashes, the shard stops and the strategy reopens it",
         %{dir: dir} do
      start_cluster(store: {:local, dir}, shards: 2)
      assert_received {:counter_started, 1, _}
      key = key_on_shard(1, 2)
      assert {:ok, 1} = Counter.increment(Cluster, key)
      {:ok, {:local, ctx}} = Cluster.lookup(1)

      Process.exit(ctx.shard_db, :kill)
      assert_receive {:counter_stopped, 1, _, _}, 2_000
      assert_receive {:counter_started, 1, _}, 2_000

      {:ok, {:local, new_ctx}} = Cluster.lookup(1)
      assert new_ctx.shard_db != ctx.shard_db
      assert {:ok, 2} = Counter.increment(Cluster, key)
    end

    test "a shard can be stopped and started by hand", %{dir: dir} do
      start_cluster(store: {:local, dir}, shards: 2)

      assert :ok = Host.stop_shard(Cluster, 0)
      assert_receive {:counter_stopped, 0, :stopping, true}
      assert Cluster.local_shards() == [1]
      assert Cluster.lookup(0) == {:error, :unassigned}
      assert Cluster.call(0, {Counter, :get_local, ["x"]}) == {:error, :unassigned}

      assert {:ok, {:error, :not_owner}} =
               Cluster.call(1, {Counter, :return_not_owner, [self()]})

      assert_receive :application_called

      started = System.monotonic_time(:millisecond)

      assert Cluster.call(0, {Counter, :get_local, ["x"]}, timeout: 50, retry_delay: 1_000) ==
               {:error, {:erpc, :timeout}}

      assert System.monotonic_time(:millisecond) - started < 250

      assert {:ok, _} = Host.start_shard(Cluster, 0)
      assert {:error, :already_started} = Host.start_shard(Cluster, 0)
      assert Cluster.local_shards() == [0, 1]
    end

    test "a shard stopped while it opens is unreachable at once, and its opener is answered once it is closed" do
      start_cluster(
        store: :memory,
        shards: 2,
        shard_children: {GatedChild, :child_specs, [self()]},
        strategy: {Slap.Cluster.Strategy.Local, shards: []}
      )

      opener = Task.async(fn -> Host.start_shard(Cluster, 0) end)
      # The database is open and its context registered; the child is
      # starting.
      assert_receive {:child_starting, 0, _, child}
      assert {:ok, {:local, _}} = Cluster.lookup(0)

      host = Process.whereis(Slap.Cluster.host(Cluster))
      :erlang.trace(host, true, [:receive])
      stopper = Task.async(fn -> Host.stop_shard(Cluster, 0) end)
      assert_receive {:trace, ^host, :receive, {:"$gen_call", _, {:stop, 0}}}
      :erlang.trace(host, false, [:receive])
      # Once the host has taken the stop (it answers in order).
      :sys.get_state(host)

      assert Cluster.lookup(0) == {:error, :unassigned}
      send(child, :go)

      assert Task.await(opener) == {:error, :stopped}
      # The opener was answered after the shard closed.
      assert_received {:child_stopped, 0, ^child}
      assert Task.await(stopper) == :ok
      assert Cluster.local_shards() == []

      restart = Task.async(fn -> Host.start_shard(Cluster, 0) end)
      assert_receive {:child_starting, 0, _, child}
      send(child, :go)
      assert {:ok, _} = Task.await(restart)
      assert {:ok, {:local, _}} = Cluster.lookup(0)
    end

    test "if a shard's open fails outside the shard, the host stops and the caller is told" do
      # A shard that takes half a second to open; the strategy opens none.
      slow = fn _ctx -> [{Agent, fn -> Process.sleep(500) end}] end

      start_cluster(
        store: :memory,
        shards: 2,
        shard_children: slow,
        strategy: {Slap.Cluster.Strategy.Local, shards: []}
      )

      host = Process.whereis(Slap.Cluster.host(Cluster))
      host_ref = Process.monitor(host)
      :erlang.trace(host, true, [:procs])
      {caller, caller_ref} = spawn_monitor(fn -> Host.start_shard(Cluster, 0) end)

      # The task that opens the shard dies.
      assert_receive {:trace, ^host, :spawn, task, _}
      :erlang.trace(host, false, [:procs])
      Process.exit(task, :kill)

      reason = {:shard_task_failed, {:start, 0}, :killed}
      assert_receive {:DOWN, ^host_ref, :process, ^host, ^reason}
      assert_receive {:DOWN, ^caller_ref, :process, ^caller, {^reason, _call}}
      # The cluster has started over; stop it while the log is captured.
      stop_supervised!(Cluster)
    end
  end

  describe "telemetry" do
    setup do
      test_pid = self()
      id = {__MODULE__, make_ref()}

      events = [
        [:slap, :cluster, :shard, :start],
        [:slap, :cluster, :shard, :fenced],
        [:slap, :cluster, :durability, :lag],
        [:slap, :cluster, :probe]
      ]

      :ok =
        @telemetry.attach_many(
          id,
          events,
          fn event, measurements, metadata, _ ->
            send(test_pid, {:event, event, measurements, metadata})
          end,
          nil
        )

      on_exit(fn -> @telemetry.detach(id) end)
      %{dir: tmp_dir()}
    end

    test "reports the probe, shard starts, durability lag and fencing", %{dir: dir} do
      start_cluster(store: {:local, dir}, shards: 2, lag_interval: 50)

      assert_received {:event, [:slap, :cluster, :probe], _, %{ok: true}}
      assert_received {:event, [:slap, :cluster, :shard, :start], _, %{shard: 0}}

      assert_receive {:event, [:slap, :cluster, :durability, :lag], %{lag: lag}, %{shard: 1}},
                     1_000

      assert is_integer(lag) and lag >= 0

      {:ok, intruder} =
        SlateDB.open("shard-001",
          store: {:local, dir},
          settings: %{manifest_poll_interval: "100ms"}
        )

      assert_receive {:event, [:slap, :cluster, :shard, :fenced], _, %{shard: 1}}, 2_000
      SlateDB.close(intruder)
    end
  end

  # Runs against a real S3-compatible server, as in slap_slatedb's tests.
  describe "on S3" do
    @describetag :s3

    test "runs 64 shards" do
      endpoint = System.fetch_env!("SLAP_TEST_S3_ENDPOINT")
      bucket = System.get_env("SLAP_TEST_S3_BUCKET", "slatedb-test")

      store =
        {:url, "s3://#{bucket}/slatedb-cluster/#{System.unique_integer([:positive])}",
         aws_endpoint: endpoint,
         aws_allow_http: "true",
         aws_region: "us-east-1",
         aws_access_key_id: System.get_env("SLAP_TEST_S3_KEY", "rustfsadmin"),
         aws_secret_access_key: System.get_env("SLAP_TEST_S3_SECRET", "rustfsadmin")}

      start_cluster(store: store, shards: 64, cache: [capacity_bytes: 64_000_000])
      assert Cluster.local_shards() == Enum.to_list(0..63)

      results =
        1..640
        |> Task.async_stream(&Counter.increment(Cluster, "key-#{rem(&1, 128)}"),
          max_concurrency: 128,
          timeout: 30_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:ok, _}, &1))
      assert Enum.count(results, &(&1 == {:ok, 5})) == 128
    end
  end

  defp received(count) do
    for _ <- 1..count do
      assert_receive {:slap_cluster_durable, seq}, 2_000
      seq
    end
  end

  defp collect_acks(acc \\ []) do
    receive do
      {:acked, n, seq} -> collect_acks([{n, seq} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp key_on_shard(n, shards) do
    Enum.find_value(1..10_000, fn i ->
      key = "key-#{i}"
      Hash.shard_for(key, shards) == n && key
    end)
  end
end
