defmodule Slap.KV.DurabilityTest do
  use Slap.KV.Test.ClusterCase, async: false

  alias Slap.Cluster.Config
  alias Slap.KV.Keys
  alias Slap.SlateDB

  @moduletag :capture_log

  describe "with a long flush interval" do
    # Nothing becomes durable until the test flushes the WAL.
    @describetag settings: %{flush_interval: "1h"}

    setup do
      warm_up()
    end

    test "a write is acknowledged, and read, only once durable" do
      put = pending_write("p", fn -> KV.put("p", "k", "v") end)

      # In SlateDB's memtable, but not durable: not acknowledged, not read.
      assert Task.yield(put, 0) == nil
      assert {:ok, nil} = KV.get("p", "k")
      assert {:ok, %{rows: []}} = KV.scan("p")

      :ok = SlateDB.flush(ctx_for("p").db)
      assert {:ok, version} = Task.await(put)
      assert {:ok, %{value: "v", version: ^version}} = KV.get("p", "k")
    end

    test "a conflict with a write that is not durable waits for it" do
      put = pending_write("p", fn -> KV.put("p", "k", "first") end)
      assert Task.yield(put, 0) == nil

      # The row exists in the partition writer's latest state, so this
      # conflicts; but the reply waits, since the row could still be lost.
      create = pending_write("p", fn -> KV.put("p", "k", "second", if_version: :absent) end)
      assert Task.yield(create, 0) == nil

      :ok = SlateDB.flush(ctx_for("p").db)
      assert {:ok, version} = Task.await(put)
      assert {:error, {:conflict, ^version}} = Task.await(create)
    end

    test "a linearizable read of a write that is not durable waits for it" do
      put = pending_write("p", fn -> KV.put("p", "k", "v") end)
      read = Task.async(fn -> KV.get("p", "k", consistency: :linearizable) end)
      assert Task.yield(read, 0) == nil

      assert {:ok, %{value: "v", version: version}} = flush_until_reply(read, "p")
      assert {:ok, ^version} = Task.await(put)
    end

    test "a new partition writer does not check against writes that are not durable yet" do
      put = pending_write("p", fn -> KV.put("p", "k", "unacked") end)
      assert Task.yield(put, 0) == nil

      # The partition writer dies with its write in the memtable. Its caller
      # learns nothing about the outcome.
      {supervisor, _id} = partition_writer_child("p")
      1 = :erlang.trace(supervisor, true, [:procs])
      Process.exit(partition_writer("p"), :kill)
      assert {:error, :unavailable} = Task.await(put)
      assert_receive {:trace, ^supervisor, :spawn, writer, _}
      1 = :erlang.trace(supervisor, false, [:procs])

      # Trace the new partition writer's messages and condition checks.
      # Trace messages from one process arrive in order.
      1 = :erlang.trace(writer, true, [:receive, :call])
      :erlang.trace_pattern({SlateDB, :get_key_value, :_}, true, [])
      on_exit(fn -> :erlang.trace_pattern({SlateDB, :get_key_value, :_}, false, []) end)

      create = Task.async(fn -> KV.put("p", "k", "retry", if_version: :absent) end)

      # The unacknowledged write was applied after all. (The conflict is
      # confirmed by a write of its own, which needs a flush too.)
      assert {:error, {:conflict, version}} = flush_until_reply(create, "p")
      assert {:ok, %{value: "unacked", version: ^version}} = KV.get("p", "k")

      # The new partition writer learned that the write was durable before it
      # checked the condition against it.
      first =
        receive do
          {:trace, ^writer, :receive, {:slap_cluster_durable, :loaded}} -> :durable
          {:trace, ^writer, :call, {SlateDB, :get_key_value, _}} -> :check
        after
          1_000 -> :neither
        end

      assert first == :durable
      assert_receive {:trace, ^writer, :call, {SlateDB, :get_key_value, _}}
    end

    test "stopping the shard fails in-flight writes instead of acknowledging them" do
      put = pending_write("p", fn -> KV.put("p", "k", "never acked") end)
      assert Task.yield(put, 0) == nil

      stop_supervised!(KV.Cluster)
      assert Task.await(put) == {:error, :unavailable}
    end
  end

  test "a partition writer whose node lost the shard does not report a stale conflict" do
    {:ok, _} = KV.put("p", "k", "old")

    # Another node takes the shard over (a placement without leases) and
    # deletes the row. This node does not know yet.
    config = Config.get(KV.Cluster)
    path = Config.shard_path(config, ctx_for("p").n)
    {:ok, other} = SlateDB.open(path, store: config.store, settings: config.settings)
    on_exit(fn -> SlateDB.close(other) end)
    {:ok, _} = SlateDB.delete(other, Keys.encode("p", "k"), await_durable: true)

    # Its state says the row exists. That conflict is not reported, since
    # the row is gone: the write that would confirm it is fenced.
    assert KV.put("p", "k", "new", if_version: :absent) == {:error, :unavailable}
  end

  test "a linearizable read on a node that lost the shard fails" do
    {:ok, _} = KV.put("p", "k", "old")

    config = Config.get(KV.Cluster)
    path = Config.shard_path(config, ctx_for("p").n)
    {:ok, other} = SlateDB.open(path, store: config.store, settings: config.settings)
    on_exit(fn -> SlateDB.close(other) end)
    {:ok, _} = SlateDB.delete(other, Keys.encode("p", "k"), await_durable: true)

    assert KV.get("p", "k", consistency: :linearizable) == {:error, :unavailable}
  end

  test "writes through a fenced database fail" do
    {:ok, _} = KV.put("p", "k", "v")

    # Another node opens the shard's database, as one that took the
    # shard over would.
    config = Config.get(KV.Cluster)
    path = Config.shard_path(config, ctx_for("p").n)
    {:ok, other} = SlateDB.open(path, store: config.store, settings: config.settings)
    on_exit(fn -> SlateDB.close(other) end)

    assert {:error, :unavailable} = KV.put("p", "k", "stale")
    assert {:ok, "v"} = SlateDB.get(other, Keys.encode("p", "k"))
  end

  describe "with fast compaction" do
    @describetag settings: %{
                   compactor_options: %{
                     poll_interval: "100ms",
                     commit_compacted_interval: "100ms",
                     scheduler_options: %{min_compaction_sources: "2"},
                     worker: %{compactions_poll_interval: "100ms"}
                   }
                 }

    # A row's version is the sequence number SlateDB stored with it. If
    # compaction rewrote it, two different values could share a version and
    # a conditional write could overwrite the wrong one.
    test "a row keeps its version through compaction" do
      db = ctx_for("p").db
      {:ok, _} = KV.put("p", "k", "old")
      :ok = SlateDB.flush(db, type: :memtable)
      {:ok, version} = KV.put("p", "k", "new")
      :ok = SlateDB.flush(db, type: :memtable)

      # Compaction cannot report that it is done: poll for it.
      assert eventually(fn ->
               %{l0_sst_count: l0, sorted_run_count: runs} = SlateDB.stats(db)
               l0 == 0 and runs >= 1
             end)

      assert {:ok, %{value: "new", version: ^version}} = KV.get("p", "k")
      assert {:ok, _} = KV.put("p", "k", "newer", if_version: version)
    end
  end

  # Runs `fun` in a task and returns once `partition`'s partition writer has
  # handled its request, so that any write it makes is in SlateDB.
  defp pending_write(partition, fun) do
    writer = partition_writer(partition)
    1 = :erlang.trace(writer, true, [:receive])
    task = Task.async(fun)
    pid = task.pid
    assert_receive {:trace, ^writer, :receive, {:"$gen_call", {^pid, _}, {:write, _, _, _}}}
    1 = :erlang.trace(writer, false, [:receive])
    # Handled after the request, which is ahead of it in the mailbox.
    _ = :sys.get_state(writer)
    task
  end

  # Flushes the partition's WAL until `task` replies.
  defp flush_until_reply(task, partition) do
    db = ctx_for(partition).db

    Stream.repeatedly(fn ->
      :ok = SlateDB.flush(db)
      Task.yield(task, 20)
    end)
    |> Enum.find_value(fn
      {:ok, result} -> result
      nil -> nil
    end)
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() ->
        true

      attempts > 0 ->
        Process.sleep(50)
        eventually(fun, attempts - 1)

      true ->
        false
    end
  end
end
