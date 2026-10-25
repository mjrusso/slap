defmodule Slap.Cluster.DatabaseLifecycleTest.Cluster do
  use Slap.Cluster, otp_app: :slap_cluster
end

defmodule Slap.Cluster.DatabaseLifecycleTest do
  use ExUnit.Case, async: false

  alias Slap.Cluster.{Database, Host}
  alias Slap.Cluster.DatabaseLifecycleTest.Cluster
  alias Slap.SlateDB

  @moduletag :capture_log

  setup do
    start_supervised!(
      {Cluster,
       store: :memory,
       shards: 1,
       close_timeout: 50,
       strategy: {Slap.Cluster.Strategy.Local, shards: []}}
    )

    :ok = :sys.suspend(Module.concat(Cluster, Strategy))
    {:ok, _} = Host.start_shard(Cluster, 0)
    {:ok, {:local, ctx}} = Cluster.lookup(0)
    {:ok, %{ref: subscription}} = SlateDB.subscribe(ctx.db, :retired)
    %{ctx: ctx, subscription: subscription}
  end

  test "a killed shard owner closes its handle without a replacement", %{
    ctx: ctx,
    subscription: subscription
  } do
    ref = Process.monitor(ctx.shard_db)
    Process.exit(ctx.shard_db, :kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}
    assert_receive {:slap_slatedb_closed, ^subscription, :retired, :clean}, 5_000
    assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.get(ctx.db, "k")
  end

  test "a shard whose database process stopped reports its own stop", %{ctx: ctx} do
    test = self()
    handler = "database-lifecycle-#{inspect(test)}"

    :telemetry.attach(
      handler,
      [:slap, :cluster, :shard, :stop],
      fn _, _, meta, _ -> send(test, {:shard_stop, meta.reason}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    # The shard sees the database close or its process exit, whichever is first.
    :ok = GenServer.stop(ctx.db_owner)
    assert_receive {:shard_stop, reason}, 5_000
    assert reason in [{:db_closed, :clean}, {:database_owner_down, :normal}]
  end

  test "closing a database process that shuts down during the call succeeds", %{ctx: ctx} do
    pid = ctx.db_owner
    :ok = :sys.suspend(pid)
    1 = :erlang.trace(pid, true, [:receive])
    closing = Task.async(fn -> Database.close(pid) end)
    # The queued close waits; the suspended process still handles the stop.
    assert_receive {:trace, ^pid, :receive, {:"$gen_call", _, :close}}
    :ok = GenServer.stop(pid, :shutdown)
    assert :ok = Task.await(closing)
    assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.get(ctx.db, "k")
  end

  test "orderly shutdown releases the database lifetime process", %{ctx: ctx} do
    ref = Process.monitor(ctx.db_owner)
    assert :ok = Host.stop_shard(Cluster, 0)
    assert_receive {:DOWN, ^ref, :process, _, :normal}, 5_000
    assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.get(ctx.db, "k")
  end

  test "replacement opening waits for the old handle to close", %{ctx: ctx} do
    :ok = :sys.suspend(ctx.db_owner)
    retired = Process.monitor(ctx.db_owner)
    owner = Process.monitor(ctx.shard_db)
    Process.exit(ctx.shard_db, :kill)
    assert_receive {:DOWN, ^owner, :process, _, :killed}
    Host.stop_shard(Cluster, 0)

    host = Process.whereis(Slap.Cluster.host(Cluster))
    :erlang.trace(host, true, [:receive])
    task = Task.async(fn -> Host.start_shard(Cluster, 0) end)
    task_ref = task.ref

    try do
      assert_receive {:trace, ^host, :receive, {:"$gen_call", _, {:start, 0, []}}}
      :sys.get_state(host)
      refute_receive {^task_ref, {:ok, _}}, 100
      assert Process.alive?(ctx.db_owner)
    after
      :erlang.trace(host, false, [:receive])
      :ok = :sys.resume(ctx.db_owner)
    end

    assert_receive {:DOWN, ^retired, :process, _, :normal}, 5_000
    assert {:ok, _} = Task.await(task)
    assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.get(ctx.db, "k")
    {:ok, {:local, replacement}} = Cluster.lookup(0)
    refute replacement.db_owner == ctx.db_owner
    assert {:ok, _} = SlateDB.put(replacement.db, "k", "recovered")
  end

  test "closing continues after the cluster exhausts its shutdown timeout", %{
    ctx: ctx,
    subscription: subscription
  } do
    ref = Process.monitor(ctx.shard_db)
    :erlang.suspend_process(ctx.shard_db)
    stop_supervised!(Cluster)
    assert_receive {:DOWN, ^ref, :process, _, :killed}
    assert_receive {:slap_slatedb_closed, ^subscription, :retired, :clean}, 5_000
    assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.get(ctx.db, "k")
  end
end
