defmodule Slap.Cluster.ReadinessTest do
  # These tests share the named cluster and distributed Erlang node.
  use ExUnit.Case, async: false

  alias Slap.Cluster.Host
  alias Slap.Cluster.Strategy.{Distributed, Local, Static}
  alias Slap.Cluster.Test.{Failover, GatedChild, Peers, TestCluster}
  alias Slap.SlateDB

  @moduletag :capture_log

  test "readiness waits for every shard's children and becomes false on departure" do
    start_supervised!(
      {TestCluster,
       store: :memory,
       shards: 2,
       shard_children: {GatedChild, :child_specs, [self()]},
       strategy: {Local, shards: []}}
    )

    assert {:ok, %{ready?: false}} = TestCluster.status()
    first = Task.async(fn -> Host.start_shard(TestCluster, 0) end)
    assert_receive {:child_starting, 0, _, child}, 5_000
    assert {:ok, {:local, _ctx}} = TestCluster.lookup(0)
    assert {:ok, %{ready?: false, shards: %{0 => %{status: :starting}}}} = TestCluster.status()
    assert TestCluster.await_ready(25) == {:error, :timeout}
    send(child, :go)
    assert {:ok, _} = Task.await(first)
    assert {:ok, %{ready?: false}} = TestCluster.status()

    second = Task.async(fn -> Host.start_shard(TestCluster, 1) end)
    assert_receive {:child_starting, 1, _, child}, 5_000
    waiter = Task.async(fn -> TestCluster.await_ready(:infinity) end)
    send(child, :ignore)
    assert {:ok, _} = Task.await(second)
    assert Task.await(waiter) == :ok
    assert {:ok, %{ready?: true}} = TestCluster.status()

    assert :ok = Host.stop_shard(TestCluster, 0)
    assert {:ok, %{ready?: false, shards: %{0 => %{status: :unassigned}}}} = TestCluster.status()
    assert TestCluster.await_ready(25) == {:error, :timeout}
  end

  test "waiting skips child inspection while a required shard is unassigned" do
    start_supervised!({TestCluster, store: :memory, shards: 2, strategy: {Local, shards: []}})
    {:ok, shard} = Host.start_shard(TestCluster, 0)
    :erlang.trace(shard, true, [:receive, {:tracer, self()}])

    try do
      assert TestCluster.await_ready(50) == {:error, :timeout}
      delivered = :erlang.trace_delivered(shard)
      assert_receive {:trace_delivered, ^shard, ^delivered}
      refute_received {:trace, ^shard, :receive, {:"$gen_call", _, :which_children}}

      assert {:ok, %{shards: %{0 => %{status: :running}, 1 => %{status: :unassigned}}}} =
               TestCluster.status()

      assert_receive {:trace, ^shard, :receive, {:"$gen_call", _, :which_children}}
    after
      :erlang.trace(shard, false, [:receive])
    end
  end

  test "a crashed shard invalidates a previously ready cluster" do
    start_supervised!({TestCluster, store: :memory, shards: 1, strategy: {Local, shards: []}})
    {:ok, shard} = Host.start_shard(TestCluster, 0)
    assert :ok = TestCluster.await_ready(5_000)
    ref = Process.monitor(shard)
    Process.exit(shard, :kill)
    assert_receive {:DOWN, ^ref, :process, ^shard, :killed}
    assert {:ok, %{ready?: false}} = TestCluster.status()
  end

  test "child and child-supervisor restarts must finish before readiness recovers" do
    start_supervised!(
      {TestCluster,
       store: :memory,
       shards: 1,
       shard_children: {GatedChild, :child_specs, [self()]},
       strategy: {Local, shards: []}}
    )

    opener = Task.async(fn -> Host.start_shard(TestCluster, 0) end)
    assert_receive {:child_starting, 0, _, child}, 5_000
    send(child, :go)
    assert {:ok, supervisor} = Task.await(opener)
    assert :ok = TestCluster.await_ready(5_000)

    [children] =
      for {_, pid, :supervisor, _} <- Supervisor.which_children(supervisor), do: pid

    for target <- [child, children] do
      Process.exit(target, :kill)
      assert_receive {:child_starting, 0, _, replacement}, 5_000

      try do
        assert {:ok, %{ready?: false, shards: %{0 => %{status: :starting}}}} =
                 TestCluster.status()

        assert TestCluster.await_ready(25) == {:error, :timeout}
      after
        send(replacement, :go)
      end

      assert :ok = TestCluster.await_ready(5_000)
      assert {:ok, %{ready?: true}} = TestCluster.status()
    end
  end

  test "completed temporary children and deleted specifications do not block readiness" do
    parent = self()

    start_supervised!(
      {TestCluster,
       store: :memory,
       shards: 1,
       shard_children: fn _ctx ->
         [
           Supervisor.child_spec(
             {Task,
              fn ->
                send(parent, {:task_started, self()})
                receive do: (:finish -> :ok)
              end},
             restart: :temporary
           ),
           {Agent, fn -> :ok end}
         ]
       end}
    )

    assert_receive {:task_started, child}
    assert :ok = TestCluster.await_ready(5_000)
    ref = Process.monitor(child)
    send(child, :finish)
    assert_receive {:DOWN, ^ref, :process, ^child, :normal}
    assert :ok = TestCluster.await_ready(1_000)

    [{_, shard, :supervisor, _}] =
      DynamicSupervisor.which_children(Slap.Cluster.shard_supervisor(TestCluster, 0))

    [children] = for {_, pid, :supervisor, _} <- Supervisor.which_children(shard), do: pid
    assert :ok = Supervisor.terminate_child(children, Agent)
    assert :ok = Supervisor.delete_child(children, Agent)
    assert {:ok, %{ready?: true}} = TestCluster.status()
  end

  test "readiness includes children added after startup" do
    start_supervised!({TestCluster, store: :memory, shards: 1, shard_children: fn _ctx -> [] end})

    assert :ok = TestCluster.await_ready(5_000)

    [{_, shard, :supervisor, _}] =
      DynamicSupervisor.which_children(Slap.Cluster.shard_supervisor(TestCluster, 0))

    [children] = for {_, pid, :supervisor, _} <- Supervisor.which_children(shard), do: pid
    parent = self()

    opener =
      Task.async(fn -> Supervisor.start_child(children, {GatedChild, {0, nil, parent}}) end)

    assert_receive {:child_starting, 0, _, child}, 5_000

    try do
      assert {:ok, %{ready?: false, shards: %{0 => %{status: :starting}}}} = TestCluster.status()
      assert TestCluster.await_ready(25) == {:error, :timeout}
      assert TestCluster.local_shards() == [0]
    after
      send(child, :go)
    end

    assert {:ok, ^child} = Task.await(opener)
    assert :ok = TestCluster.await_ready(5_000)
  end

  test "a failed child start is reported while Local retries and after recovery" do
    start_supervised!(
      {TestCluster,
       store: :memory,
       shards: 1,
       probe: :background,
       shard_children: {GatedChild, :child_specs, [self()]},
       strategy: {Local, backoff_base: 1, backoff_max: 10}}
    )

    assert_receive {:child_starting, 0, _, child}, 5_000
    send(child, {:fail, :startup_failed})
    assert_receive {:child_starting, 0, _, replacement}, 5_000

    assert {:ok, %{ready?: false, shards: %{0 => %{last_error: :startup_failed}}}} =
             TestCluster.status()

    send(replacement, :go)
    assert :ok = TestCluster.await_ready(5_000)

    assert {:ok, %{ready?: true, shards: %{0 => %{last_error: :startup_failed}}}} =
             TestCluster.status()
  end

  test "a startup failure is retained when a stop is already pending" do
    start_supervised!(
      {TestCluster,
       store: :memory,
       shards: 1,
       shard_children: {GatedChild, :child_specs, [self()]},
       strategy: {Local, shards: []}}
    )

    opener = Task.async(fn -> Host.start_shard(TestCluster, 0) end)
    assert_receive {:child_starting, 0, _, child}, 5_000
    stop = :gen_server.send_request(Slap.Cluster.host(TestCluster), {:stop, 0})
    assert TestCluster.local_shards() == []
    assert {:ok, %{shards: %{0 => %{status: :stopping}}}} = TestCluster.status()

    send(child, {:fail, :startup_failed})
    assert Task.await(opener) == {:error, :startup_failed}
    assert :gen_server.receive_response(stop, 5_000) == {:reply, :ok}

    assert {:ok, %{ready?: false, shards: %{0 => %{last_error: :startup_failed}}}} =
             TestCluster.status()

    replacement = Task.async(fn -> Host.start_shard(TestCluster, 0) end)
    assert_receive {:child_starting, 0, _, child}, 5_000
    send(child, :go)
    assert {:ok, _} = Task.await(replacement)

    assert {:ok, %{ready?: true, shards: %{0 => %{last_error: :startup_failed}}}} =
             TestCluster.status()
  end

  test "fencing invalidates readiness" do
    dir = Failover.tmp_dir()

    start_supervised!(
      {TestCluster,
       store: {:local, dir},
       shards: 1,
       settings: %{manifest_poll_interval: "100ms"},
       strategy: {Local, backoff_base: 60_000}}
    )

    assert :ok = TestCluster.await_ready(5_000)
    parent = self()
    handler = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler,
        [:slap, :cluster, :shard, :fenced],
        fn _, _, _, _ ->
          send(parent, :fenced)
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    {:ok, intruder} = SlateDB.open("shard-000", store: {:local, dir})

    try do
      assert_receive :fenced, 5_000
      assert {:ok, %{ready?: false}} = TestCluster.status()
    after
      SlateDB.close(intruder)
    end
  end

  test "background probe failures are reported and retained after recovery" do
    dir = Failover.tmp_dir()
    File.mkdir_p!(dir)
    blocker = Path.join(dir, "_cluster")
    File.write!(blocker, "")
    parent = self()
    handler = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler,
        [:slap, :cluster, :probe],
        fn _, _, meta, _ ->
          send(parent, {:probe, meta.ok})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    start_supervised!({TestCluster, store: {:local, dir}, shards: 1, probe: :background})
    assert_receive {:probe, false}, 5_000

    assert {:ok, %{ready?: false, probe: %{status: :failed, last_error: failure}}} =
             TestCluster.status()

    assert failure != nil
    File.rm!(blocker)
    assert :ok = TestCluster.await_ready(5_000)
    assert {:ok, %{probe: %{status: :passed, last_error: ^failure}}} = TestCluster.status()
  end

  @tag :multinode
  test "all owners receive a status query before any suspended owner replies" do
    Peers.distribute!()

    remotes =
      for name <- ["readiness-a", "readiness-b"] do
        {peer, remote} = Peers.start(name)
        on_exit(fn -> :peer.stop(peer) end)
        remote
      end

    opts = [store: :memory, shards: 3, strategy: {Static, nodes: [node() | remotes]}]
    start_supervised!({TestCluster, opts})
    for remote <- remotes, do: :ok = Peers.start_cluster(remote, opts)
    host = Slap.Cluster.host(TestCluster)

    observers =
      for remote <- remotes do
        pid = :erpc.call(remote, Process, :whereis, [host], 5_000)
        :ok = :erpc.call(remote, :sys, :suspend, [pid], 5_000)
        tracer = Peers.trace_receive(remote, pid, self())
        {remote, pid, tracer}
      end

    query = Task.async(fn -> TestCluster.status(5_000) end)

    try do
      for {_remote, pid, _tracer} <- observers do
        assert_receive {:trace, ^pid, :receive, {:"$gen_call", _, :status}}, 1_000
      end
    after
      for {remote, pid, tracer} <- observers do
        :erpc.call(remote, :erlang, :trace, [pid, false, [:receive]], 5_000)
        :erpc.call(remote, Process, :exit, [tracer, :kill], 5_000)
        :erpc.call(remote, :sys, :resume, [pid], 5_000)
      end
    end

    assert {:ok, %{ready?: true}} = Task.await(query)
  end

  @tag :multinode
  test "finite and infinite waits rediscover placement while the previous owner is suspended" do
    Peers.distribute!()
    {peer, remote} = Peers.start("readiness-recovery")
    on_exit(fn -> :peer.stop(peer) end)
    opts = [store: :memory, shards: 2, strategy: {Distributed, interval: 100, settle: 0}]
    start_supervised!({TestCluster, opts})
    :ok = Peers.start_cluster(remote, opts)

    {shard, ^remote} =
      Enum.find(Distributed.placement(2, [node(), remote]), fn {_, owner} -> owner == remote end)

    Failover.wait_until(fn -> TestCluster.lookup(shard) == {:ok, {:remote, remote}} end, 5_000)
    assert :ok = TestCluster.await_ready(5_000)
    host = :erpc.call(remote, Process, :whereis, [Slap.Cluster.host(TestCluster)], 5_000)
    :ok = :erpc.call(remote, :sys, :suspend, [host], 5_000)

    waiters =
      for timeout <- [10_000, :infinity] do
        Peers.trace_receive(remote, host, self())
        waiter = Task.async(fn -> TestCluster.await_ready(timeout) end)
        assert_receive {:trace, ^host, :receive, {:"$gen_call", _, :status}}, 1_000
        waiter
      end

    try do
      strategy =
        :erpc.call(remote, Process, :whereis, [Module.concat(TestCluster, Strategy)], 5_000)

      scope = Module.concat(TestCluster, StrategyGroup)
      :ok = :erpc.call(remote, :pg, :leave, [scope, :members, strategy], 5_000)

      Failover.wait_until(fn -> match?({:ok, {:local, _}}, TestCluster.lookup(shard)) end, 5_000)
      assert {:ok, %{ready?: true}} = TestCluster.status()

      results = Task.yield_many(waiters, 2_000)
      assert Enum.map(results, &elem(&1, 1)) == [{:ok, :ok}, {:ok, :ok}]
    after
      for waiter <- waiters, do: Task.shutdown(waiter, :brutal_kill)
      :erpc.call(remote, :sys, :resume, [host], 5_000)
    end
  end

  @tag :multinode
  test "remote placement requires completed startup and remote checks share the wait deadline" do
    Peers.distribute!()
    {peer, remote} = Peers.start("readiness")
    on_exit(fn -> :peer.stop(peer) end)
    opts = [store: :memory, shards: 2, strategy: {Static, nodes: [node(), remote]}]
    start_supervised!({TestCluster, opts})
    assert {:ok, %{ready?: false, shards: %{1 => %{status: :unavailable}}}} = TestCluster.status()

    remote_opts =
      opts
      |> Keyword.put(:probe, :background)
      |> Keyword.put(:shard_children, {GatedChild, :child_specs, [self()]})

    :ok = Peers.start_cluster(remote, remote_opts)
    assert_receive {:child_starting, 1, _, child}, 5_000
    assert {:ok, %{ready?: false, shards: %{1 => %{status: :starting}}}} = TestCluster.status()
    send(child, :go)
    assert :ok = TestCluster.await_ready(5_000)

    host = Slap.Cluster.host(TestCluster)
    :ok = :erpc.call(remote, :sys, :suspend, [host], 5_000)

    try do
      started = System.monotonic_time(:millisecond)
      assert TestCluster.await_ready(100) == {:error, :timeout}
      assert System.monotonic_time(:millisecond) - started < 1_000
    after
      :erpc.call(remote, :sys, :resume, [host], 5_000)
    end

    :ok = Peers.stop_cluster(remote)
    assert {:ok, %{ready?: false}} = TestCluster.status()
  end
end
