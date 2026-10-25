defmodule Slap.KV.SubmissionTest do
  use Slap.KV.Test.ClusterCase, async: false

  import Slap.KV.Test.WriterControl

  alias Slap.Cluster.Config
  alias Slap.KV.Admission
  alias Slap.SlateDB

  @moduletag :capture_log

  for {name, ops} <- [
        mutation: [{:put, "a", "v"}],
        conflict: [{:check, "a", 0}],
        check_only: [{:check, "a", :absent}]
      ] do
    @tag ops: ops
    test "#{name} rechecks its deadline after conditions and admission", %{ops: ops} do
      ctx = ctx_for("p")
      before = SlateDB.last_write_seq(ctx.db)
      id = pause_admission(ctx, :submitting)
      deadline = System.os_time(:millisecond) + 200
      task = Task.async(fn -> KV.batch("p", ops, deadline: deadline) end)
      assert_receive {:paused, ^id, authority, _from}, 5_000
      Process.send_after(self(), {:deadline, id}, max(deadline - System.os_time(:millisecond), 0))
      assert_receive {:deadline, ^id}, 5_000
      send(authority, {:resume, id})
      assert {:error, :deadline_exceeded} = Task.await(task)
      assert SlateDB.last_write_seq(ctx.db) == before
      assert %{requests: 0, bytes: 0} = Admission.snapshot(ctx)
    end
  end

  for {name, request} <- [
        conflict: [{:check, "a", :absent}],
        check_only: [{:check, "missing", :absent}],
        mutation: [{:put, "a", "new"}],
        linearizable_get: :get
      ] do
    @tag request: request
    test "#{name} cannot return its captured result after another owner takes over", %{
      request: request
    } do
      {:ok, _} = KV.put("p", "a", "old")
      ctx = ctx_for("p")
      id = pause_admission(ctx, :submitting)

      task =
        Task.async(fn ->
          case request do
            :get -> KV.get("p", "a", consistency: :linearizable)
            ops -> KV.batch("p", ops)
          end
        end)

      assert_receive {:paused, ^id, authority, {writer, _}}, 5_000
      :erlang.suspend_process(ctx.shard_db)

      try do
        config = Config.get(KV.Cluster)

        {:ok, other} =
          SlateDB.open(Config.shard_path(config, ctx.n),
            store: config.store,
            settings: config.settings
          )

        try do
          {:ok, _} = SlateDB.delete(other, KV.Keys.encode("p", "a"), await_durable: true)
          send(authority, {:resume, id})

          try do
            :sys.get_state(writer)
          catch
            :exit, _ -> :ok
          end
        after
          SlateDB.close(other)
        end
      after
        if Process.alive?(ctx.shard_db), do: :erlang.resume_process(ctx.shard_db)
      end

      assert {:error, :unavailable} = Task.await(task)
    end
  end

  @tag settings: %{flush_interval: nil}
  test "writer death before submission releases a reservation without writing" do
    ctx = ctx_for("p")
    id = pause_admission(ctx, :submitting)
    task = Task.async(fn -> KV.put("p", "k", "v") end)
    assert_receive {:paused, ^id, authority, {writer, _}}, 5_000
    ref = Process.monitor(writer)
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^writer, :killed}
    send(authority, {:resume, id})
    assert {:error, :unavailable} = Task.await(task)
    assert %{bytes: 0, requests: 0} = Admission.snapshot(ctx)
    assert {:ok, nil} = KV.get("p", "k")
  end

  @tag settings: %{flush_interval: nil}
  test "writer death after native submission keeps its charge until the sequence is durable" do
    ctx = ctx_for("p")
    id = pause_admission(ctx, :submitted)
    task = Task.async(fn -> KV.put("p", "k", "v") end)
    assert_receive {:paused, ^id, authority, {writer, _}}, 5_000
    assert SlateDB.last_write_seq(ctx.db) > 0
    :erlang.trace(authority, true, [:receive])
    ref = Process.monitor(writer)
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^writer, :killed}
    assert {:error, :unavailable} = Task.await(task)
    send(authority, {:resume, id})
    assert_receive {:trace, ^authority, :receive, {:DOWN, _, :process, ^writer, :killed}}, 5_000
    :erlang.trace(authority, false, [:receive])
    :sys.get_state(authority)
    assert %{bytes: 5, requests: 1} = Admission.snapshot(ctx)
    flush("p")
    assert %{bytes: 0, requests: 0} = Admission.snapshot(ctx)
    recovery = pending("p", fn -> KV.put("p", "k", "recovered") end)
    flush("p")
    assert {:ok, _} = Task.await(recovery)
  end

  @tag settings: %{flush_interval: nil}
  test "accounting loss after native submission cannot reopen admission on the same handle" do
    ctx = ctx_for("p")
    id = pause_admission(ctx, :submitted)
    task = Task.async(fn -> KV.put("p", "k", "v") end)
    assert_receive {:paused, ^id, authority, _from}, 5_000
    assert SlateDB.last_write_seq(ctx.db) > 0
    owner_ref = Process.monitor(ctx.shard_db)
    Process.exit(authority, :kill)
    assert {:error, :unavailable} = Task.await(task)
    assert_receive {:DOWN, ^owner_ref, :process, _, _}, 5_000
    assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.get(ctx.db, "closed")
  end
end
