defmodule Slap.KV.AdmissionTest do
  use Slap.KV.Test.ClusterCase, async: false
  use ExUnitProperties

  import Slap.KV.Test.WriterControl

  alias Slap.KV.Admission
  alias Slap.SlateDB

  @moduletag :capture_log
  @moduletag settings: %{flush_interval: nil}
  @moduletag shards: 1
  @moduletag child_options: [
               partition_writers: 4,
               max_inflight_bytes_per_writer: 10,
               max_inflight_bytes_per_shard: 15,
               max_inflight_requests_per_writer: 2
             ]

  test "exact encoded byte bounds, permanent rejection and recovery" do
    assert {:error, {:bad_request, :request_too_large}} = KV.put("p", "k", "1234567")
    put = pending("p", fn -> KV.put("p", "k", "123456") end)
    assert %{bytes: 10, requests: 1} = Admission.snapshot(ctx_for("p"))
    assert {:error, :overloaded} = KV.delete("p", "k")
    assert {:error, {:bad_request, :request_too_large}} = KV.put("p", "k", "1234567")
    assert {:ok, nil} = KV.get("p", "k")
    assert {:ok, %{rows: []}} = KV.scan("p")
    assert Task.yield(put, 0) == nil
    :ok = SlateDB.flush(ctx_for("p").db)
    assert {:ok, _} = Task.await(put)
    assert %{bytes: 0, requests: 0} = Admission.snapshot(ctx_for("p"))
    delete = pending("p", fn -> KV.delete("p", "k") end)
    :ok = SlateDB.flush(ctx_for("p").db)
    assert :ok = Task.await(delete)
  end

  test "confirmation requests keep slots even when they charge no bytes" do
    a = pending("p", fn -> KV.get("p", "k", consistency: :linearizable) end)
    b = pending("p", fn -> KV.put("p", "k", "x", if_version: 0) end)
    assert %{bytes: 5, requests: 2} = Admission.snapshot(ctx_for("p"))
    assert {:error, :overloaded} = KV.get("p", "missing", consistency: :linearizable)
    assert {:error, :overloaded} = KV.put("p", "k", "")
    :ok = SlateDB.flush(ctx_for("p").db)
    assert {:ok, nil} = Task.await(a)
    assert {:error, {:conflict, nil}} = Task.await(b)
    assert %{bytes: 0, requests: 0} = Admission.snapshot(ctx_for("p"))
  end

  test "different writers contend for the shard's byte budget" do
    [a, b] = partitions()
    first = pending(a, fn -> KV.put(a, "k", "12345") end)
    second = pending(b, fn -> KV.put(b, "k", "") end)
    assert %{bytes: 15, requests: 2} = Admission.snapshot(ctx_for(a))
    assert {:error, :overloaded} = KV.delete(b, "k")
    :ok = SlateDB.flush(ctx_for(a).db)
    assert {:ok, _} = Task.await(first)
    assert {:ok, _} = Task.await(second)
  end

  test "caller timeout leaves work charged until its durable reply retires" do
    task = pending("p", fn -> KV.put("p", "k", "123456", timeout: 1) end)
    assert {:error, :timeout} = Task.await(task)
    assert %{bytes: 10, requests: 1} = Admission.snapshot(ctx_for("p"))
    assert {:error, :overloaded} = KV.delete("p", "k")
    flush("p")
    assert %{bytes: 0, requests: 0} = Admission.snapshot(ctx_for("p"))
  end

  test "a dead writer's known submission remains charged until durable" do
    task = pending("p", fn -> KV.put("p", "k", "123456") end)
    writer = partition_writer("p")
    ref = Process.monitor(writer)
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^writer, :killed}
    assert {:error, :unavailable} = Task.await(task)
    assert %{bytes: 10, requests: 1} = Admission.snapshot(ctx_for("p"))
    :ok = SlateDB.flush(ctx_for("p").db)
    # The durable notification and DOWN can reach accounting in either order.
    Slap.Cluster.notify_when_durable(ctx_for("p"), SlateDB.last_write_seq(ctx_for("p").db), :test)
    assert_receive {:slap_cluster_durable, :test}
    assert {:ok, %{value: "123456"}} = KV.get("p", "k")
  end

  @tag timeout: :infinity
  property "admission recovers across generated flushes and a writer crash", context do
    runs = String.to_integer(System.get_env("SLAP_KV_PROP_RUNS", "100"))
    commands = list_of(member_of([:admit, :flush]), max_length: 8)

    check all(before <- commands, following <- commands, max_runs: runs) do
      try do
        history =
          [:admit, :admit, :admit] ++
            before ++ [:admit, :crash, :admit] ++ following ++ [:flush, :admit, :flush]

        assert [] = Enum.reduce(history, [], &admission_step/2)
      after
        flush("p")
        stop_supervised!(KV.Cluster)
        File.rm_rf!(context.dir)
        start_supervised!({KV.Cluster, context.cluster_opts})
      end
    end
  end

  defp admission_step(:admit, [_, _] = tasks) do
    assert {:error, :overloaded} = KV.get("p", "property", consistency: :linearizable)
    assert %{bytes: 0, requests: 2} = Admission.snapshot(ctx_for("p"))
    tasks
  end

  defp admission_step(:admit, tasks) do
    task = pending("p", fn -> KV.get("p", "property", consistency: :linearizable) end)
    assert Task.yield(task, 0) == nil
    count = length(tasks) + 1
    assert %{bytes: 0, requests: ^count} = Admission.snapshot(ctx_for("p"))
    [task | tasks]
  end

  defp admission_step(:flush, tasks) do
    flush("p")
    for task <- tasks, do: assert({:ok, nil} = Task.await(task))
    assert %{bytes: 0, requests: 0} = Admission.snapshot(ctx_for("p"))
    []
  end

  defp admission_step(:crash, tasks) do
    writer = partition_writer("p")
    {supervisor, _} = partition_writer_child("p")
    :erlang.trace(supervisor, true, [:procs])
    ref = Process.monitor(writer)
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^writer, :killed}, 5_000
    assert_receive {:trace, ^supervisor, :spawn, replacement, _}, 5_000
    :erlang.trace(supervisor, false, [:procs])
    for task <- tasks, do: assert({:error, :unavailable} = Task.await(task))
    count = length(tasks)
    assert %{bytes: 0, requests: ^count} = Admission.snapshot(ctx_for("p"))
    flush("p")
    :sys.get_state(replacement)
    assert %{bytes: 0, requests: 0} = Admission.snapshot(ctx_for("p"))
    []
  end

  test "durable replies retire together after a suspended writer resumes" do
    put = pending("p", fn -> KV.put("p", "k", "x") end)
    get = pending("p", fn -> KV.get("p", "k", consistency: :linearizable) end)
    writer = partition_writer("p")
    ctx = ctx_for("p")
    :sys.get_state(ctx.shard_db)
    :ok = :sys.suspend(writer)
    :ok = SlateDB.flush(ctx.db)
    tag = make_ref()
    Slap.Cluster.notify_when_durable(ctx, SlateDB.last_write_seq(ctx.db), tag)
    assert_receive {:slap_cluster_durable, ^tag}
    assert %{bytes: 5, requests: 2} = Admission.snapshot(ctx)

    :erlang.trace_pattern({Admission, :release, 2}, true, [])
    on_exit(fn -> :erlang.trace_pattern({Admission, :release, 2}, false, []) end)
    :erlang.trace(writer, true, [:call])
    :ok = :sys.resume(writer)
    assert {:ok, version} = Task.await(put)
    assert {:ok, %{value: "x", version: ^version}} = Task.await(get)
    ref = :erlang.trace_delivered(writer)
    assert_receive {:trace_delivered, ^writer, ^ref}
    :erlang.trace(writer, false, [:call])
    assert_receive {:trace, ^writer, :call, {Admission, :release, [^ctx, tokens]}}
    assert [_, _] = tokens
    refute_received {:trace, ^writer, :call, {Admission, :release, _}}
    assert %{bytes: 0, requests: 0} = Admission.snapshot(ctx)
  end

  test "overload and passed deadlines do not read rows or submit confirmations" do
    task = pending("p", fn -> KV.put("p", "k", "123456") end)
    writer = partition_writer("p")
    :erlang.trace_pattern({SlateDB, :get_key_value, :_}, true, [])
    :erlang.trace_pattern({SlateDB, :write, :_}, true, [])
    :erlang.trace(writer, true, [:call])

    try do
      assert {:error, :overloaded} = KV.put("p", "a", "", if_version: :absent)
      assert {:error, :overloaded} = KV.batch("p", [{:check, "a", :absent}])
      assert {:error, :deadline_exceeded} = KV.put("p", "a", "", if_version: :absent, deadline: 0)
      ref = :erlang.trace_delivered(writer)
      assert_receive {:trace_delivered, ^writer, ^ref}
      refute_received {:trace, ^writer, :call, _}
    after
      :erlang.trace(writer, false, [:call])
      :erlang.trace_pattern({SlateDB, :get_key_value, :_}, false, [])
      :erlang.trace_pattern({SlateDB, :write, :_}, false, [])
    end

    flush("p")
    assert {:ok, _} = Task.await(task)
  end

  @tag child_options: [max_inflight_bytes_per_writer: 20, max_inflight_bytes_per_shard: 15]
  test "the shard's permanent size bound also applies to an idle writer" do
    assert {:error, {:bad_request, :request_too_large}} = KV.put("p", "k", "123456789012")
    task = pending("p", fn -> KV.put("p", "k", "12345678901") end)
    assert %{bytes: 15, requests: 1} = Admission.snapshot(ctx_for("p"))
    assert {:error, {:bad_request, :request_too_large}} = KV.put("p", "k", "123456789012")
    assert {:error, :overloaded} = KV.delete("p", "k")
    flush("p")
    assert {:ok, _} = Task.await(task)
  end

  test "simultaneous writers cannot over-reserve shared shard bytes" do
    ps = 10..99 |> Enum.map(&Integer.to_string/1) |> Enum.uniq_by(&index/1) |> Enum.take(3)
    writers = Enum.map(ps, &partition_writer/1)
    Enum.each(writers, &:erlang.trace(&1, true, [:receive]))
    tasks = Enum.map(ps, fn p -> Task.async(fn -> KV.put(p, "k", "12345") end) end)

    for {writer, task} <- Enum.zip(writers, tasks) do
      pid = task.pid
      assert_receive {:trace, ^writer, :receive, {:"$gen_call", {^pid, _}, _}}, 5_000
      :erlang.trace(writer, false, [:receive])
      :sys.get_state(writer)
    end

    assert %{bytes: 10, requests: 1} = Admission.snapshot(ctx_for(hd(ps)))
    flush(hd(ps))
    results = Task.await_many(tasks)
    assert Enum.count(results, &match?({:ok, _}, &1)) == 1
    assert Enum.count(results, &(&1 == {:error, :overloaded})) == 2
  end

  test "caller death does not cancel submitted work" do
    task = pending("p", fn -> KV.put("p", "k", "123456") end)
    ref = Process.monitor(task.pid)
    Task.shutdown(task, :brutal_kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}
    assert %{bytes: 10, requests: 1} = Admission.snapshot(ctx_for("p"))
    assert {:error, :overloaded} = KV.delete("p", "k")
    flush("p")
    recovery = pending("p", fn -> KV.put("p", "k", "ok") end)
    flush("p")
    assert {:ok, _} = Task.await(recovery)
  end

  test "all admission and batch options require positive integers" do
    for key <- KV.Cluster.child_option_keys(), value <- [0, -1, nil, :infinity, "1"] do
      assert_raise ArgumentError, fn -> Admission.options!([{key, value}]) end
    end

    assert_raise ArgumentError, fn -> Admission.options!(max_inflight_bytes_per_writter: 1) end
  end

  defp partitions do
    10..99
    |> Enum.map(&Integer.to_string/1)
    |> Enum.uniq_by(&index/1)
    |> Enum.take(2)
  end
end
