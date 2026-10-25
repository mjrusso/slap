defmodule Slap.KV.AdmissionRecoveryTest.Cluster do
  use Slap.KV.Cluster, otp_app: :slap_kv
end

defmodule Slap.KV.AdmissionRecoveryTest do
  use Slap.KV.Test.ClusterCase, async: false

  import Slap.KV.Test.WriterControl, only: [flush: 1, pending: 2, pending_writer: 2]

  alias Slap.Cluster.Host
  alias Slap.KV.Admission
  alias Slap.SlateDB

  @moduletag :capture_log
  @moduletag shards: 1
  @moduletag settings: %{flush_interval: nil}
  @moduletag child_options: [max_inflight_requests_per_writer: 2]

  test "zero-byte reservations of dead incarnations share the stable writer-index cap" do
    ctx = ctx_for("p")
    {:ok, seq} = SlateDB.put(ctx.db, KV.Keys.confirm_key(), "")

    for _ <- 1..2 do
      test = self()

      {pid, ref} =
        spawn_monitor(fn ->
          {:ok, token} = Admission.reserve(ctx, 0, "p", :get, 0)
          :ok = Admission.submitting(ctx, token)
          :ok = Admission.submitted(ctx, token, seq)
          send(test, :charged)
        end)

      assert_receive :charged
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    assert %{bytes: 0, requests: 2} = Admission.snapshot(ctx)
    assert {:error, :overloaded} = Admission.reserve(ctx, 0, "p", :get, 0)
    flush("p")
    assert %{requests: 0} = Admission.snapshot(ctx)
    assert {:ok, token} = Admission.reserve(ctx, 0, "p", :get, 0)
    assert :ok = Admission.release(ctx, [token])
  end

  test "duplicate and late releases cannot reduce a later reservation" do
    ctx = ctx_for("p")
    {:ok, old} = Admission.reserve(ctx, 0, "p", :put, 10)
    :ok = Admission.release(ctx, [old, old])
    {:ok, current} = Admission.reserve(ctx, 0, "p", :put, 20)
    :ok = Admission.release(ctx, [old])
    send(admission(ctx), {:slap_cluster_durable, {:admission, old}})
    assert %{bytes: 20, requests: 1} = Admission.snapshot(ctx)
    :ok = Admission.release(ctx, [old, current, old])
  end

  test "uncertain submission retires the old handle before a replacement can grant work" do
    ctx = ctx_for("p")
    owner_ref = Process.monitor(ctx.shard_db)
    test = self()

    {writer, ref} =
      spawn_monitor(fn ->
        {:ok, token} = Admission.reserve(ctx, 0, "p", :put, 10)
        :ok = Admission.submitting(ctx, token)
        send(test, :submitting)
        receive do: (:stop -> :ok)
      end)

    assert_receive :submitting
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^writer, :killed}
    assert_receive {:DOWN, ^owner_ref, :process, _, _}, 5_000
    assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.get(ctx.db, "old")
    Host.stop_shard(ctx.cluster, ctx.n)
    assert {:ok, _} = Host.start_shard(ctx.cluster, ctx.n)
    replacement = ctx_for("p")
    refute replacement.shard_db == ctx.shard_db
    assert {:error, :unavailable} = Admission.reserve(ctx, 0, "p", :put, 10)
    assert {:ok, token} = Admission.reserve(replacement, 0, "p", :put, 10)
    assert :ok = Admission.release(replacement, [token])
  end

  test "accounting failure stops writers while the database drains, across shard restarts" do
    strategy = Module.concat(KV.Cluster, Strategy)
    strategy = Process.whereis(strategy)
    :ok = :sys.suspend(strategy)
    on_exit(fn -> if Process.alive?(strategy), do: :sys.resume(strategy) end)

    for _ <- 1..3 do
      ctx = ctx_for("p")
      task = pending("p", fn -> KV.put("p", "k", "pending") end)
      writer = partition_writer("p")
      writer_ref = Process.monitor(writer)
      owner_ref = Process.monitor(ctx.shard_db)
      retired = Process.monitor(ctx.db_owner)
      :ok = :sys.suspend(ctx.db_owner)

      try do
        Process.exit(admission(ctx), :kill)
        assert_receive {:DOWN, ^writer_ref, :process, ^writer, _}, 1_000
        assert [] = Registry.lookup(ctx.registry, {ctx.n, :kv_admission})
        assert {:ok, _} = SlateDB.get(ctx.db, "still open")
        assert {:error, :unavailable} = Task.await(task)
      after
        :ok = :sys.resume(ctx.db_owner)
      end

      assert_receive {:DOWN, ^owner_ref, :process, _, _}, 5_000
      assert_receive {:DOWN, ^retired, :process, _, :normal}, 5_000
      assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.get(ctx.db, "old")
      Host.stop_shard(ctx.cluster, ctx.n)
      assert {:ok, _} = Host.start_shard(ctx.cluster, ctx.n)
      assert %{bytes: 0, requests: 0} = Admission.snapshot(ctx_for("p"))
      recovery = pending("p", fn -> KV.put("p", "k", "recovered") end)
      flush("p")
      assert {:ok, _} = Task.await(recovery)
    end
  end

  test "replacement initialization checks unregistered submissions before reading its sequence" do
    ctx = ctx_for("p")
    index = index("p")
    {:ok, token} = Admission.reserve(ctx, index, "p", :put, 1)
    :ok = Admission.submitting(ctx, token)
    owner_ref = Process.monitor(ctx.shard_db)
    task = Task.async(fn -> KV.PartitionWriter.init({ctx, index}) end)
    assert {:stop, :unavailable} = Task.await(task)
    assert_receive {:DOWN, ^owner_ref, :process, _, _}, 5_000
  end

  test "cluster cleanup closes a killed owner before replacement accounting starts" do
    ctx = ctx_for("p")
    task = pending("p", fn -> KV.put("p", "k", "unknown") end)
    retired = Process.monitor(ctx.db_owner)
    ref = Process.monitor(ctx.shard_db)
    Process.exit(ctx.shard_db, :kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}
    assert {:error, :unavailable} = Task.await(task)
    assert_receive {:DOWN, ^retired, :process, _, :normal}, 5_000
    assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.get(ctx.db, "old")
    Host.stop_shard(ctx.cluster, ctx.n)
    assert {:ok, _} = Host.start_shard(ctx.cluster, ctx.n)
    assert %{bytes: 0, requests: 0} = Admission.snapshot(ctx_for("p"))
    recovery = pending("p", fn -> KV.put("p", "k", "recovered") end)
    flush("p")
    assert {:ok, _} = Task.await(recovery)
  end

  test "restarting the cluster cannot forget a handle left open by its shutdown timeout", %{
    dir: dir
  } do
    cluster = __MODULE__.Cluster

    opts = [
      store: {:local, Path.join(dir, "forced-close")},
      shards: 1,
      close_timeout: 50,
      settings: %{flush_interval: nil}
    ]

    start_supervised!({cluster, opts})
    {:ok, {:local, ctx}} = cluster.lookup(0)
    {:ok, index} = KV.PartitionWriter.index(ctx, "p")
    [{writer, _}] = Registry.lookup(ctx.registry, {ctx.n, {:partition_writer, index}})
    task = pending_writer(writer, fn -> KV.put("p", "k", "unknown", cluster: cluster) end)
    ref = Process.monitor(ctx.shard_db)
    :erlang.suspend_process(ctx.shard_db)
    stop_supervised!(cluster)
    assert_receive {:DOWN, ^ref, :process, _, :killed}, 5_000
    assert {:error, :unavailable} = Task.await(task)
    retired = Process.monitor(ctx.db_owner)
    assert_receive {:DOWN, ^retired, :process, _, _}, 5_000
    assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.get(ctx.db, "old")
    start_supervised!({cluster, opts})
    {:ok, {:local, replacement}} = cluster.lookup(0)
    assert %{requests: 0} = Admission.snapshot(replacement)
  end

  defp admission(ctx) do
    [{pid, _}] = Registry.lookup(ctx.registry, {ctx.n, :kv_admission})
    pid
  end
end
