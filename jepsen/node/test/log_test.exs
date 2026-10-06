defmodule JepsenNode.LogTest do
  use ExUnit.Case, async: false

  alias JepsenNode.{Log, LogRouter}

  test "a missing follower is reported for a touched key" do
    for child <- Log.child_specs(), do: start_supervised!(child)

    assert %{followers: 0, missing: [%{key: "unfollowed"}]} = Log.check(["unfollowed"])
  end

  test "a failed follower read is reported" do
    dir = Path.join(System.tmp_dir!(), "jepsen-log-test-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    start_supervised!(
      {Slap.Streams.Cluster,
       store: {:local, dir}, path: "streams", shards: 1, settings: %{flush_interval: "2ms"}}
    )

    for child <- Log.child_specs(), do: start_supervised!(child)
    assert :ok = Slap.SnapshotLog.delete(Log.base("missing"))
    assert {:ok, _} = DynamicSupervisor.start_child(Log.Followers, {Log.Follower, "missing"})
    Log.put_follower("missing", [1])

    assert %{followers: 1, read_errors: [%{key: "missing"}]} = Log.check(["missing"])
  end

  test "followers for retired keys can be restored after node recovery" do
    dir = Path.join(System.tmp_dir!(), "jepsen-log-test-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    start_supervised!(
      {Slap.Streams.Cluster,
       store: {:local, dir}, path: "streams", shards: 1, settings: %{flush_interval: "2ms"}}
    )

    for child <- Log.child_specs(), do: start_supervised!(child)
    assert {:ok, _} = Slap.SnapshotLog.append(Log.base("retired"), "1")
    assert %{missing: [%{key: "retired"}]} = Log.check(["retired"])

    assert :ok = Log.restore_followers(["retired"])
    assert %{followers: 1, missing: [], lagging: [], mismatches: []} = Log.check(["retired"])
  end

  test "a follower ahead of the first log read is checked against the current log" do
    dir = Path.join(System.tmp_dir!(), "jepsen-log-test-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    start_supervised!(
      {Slap.Streams.Cluster,
       store: {:local, dir}, path: "streams", shards: 1, settings: %{flush_interval: "2ms"}}
    )

    for child <- Log.child_specs(), do: start_supervised!(child)
    assert {:ok, _} = Slap.SnapshotLog.append(Log.base("growing"), "1")
    assert {:ok, _} = Registry.register(Log.Registry, "growing", nil)
    Log.put_follower("growing", [])

    :erlang.trace_pattern({Log, :await_follower, 3}, true, [:local])
    :erlang.trace(:new, true, [:call, {:tracer, self()}])

    on_exit(fn ->
      :erlang.trace(:new, false, [:call])
      :erlang.trace_pattern({Log, :await_follower, 3}, false, [:local])
    end)

    task =
      Task.async(fn ->
        receive do
          :check -> Log.check(["growing"])
        end
      end)

    send(task.pid, :check)
    assert_receive {:trace, _, :call, {Log, :await_follower, ["growing", [1], _]}}, 5_000

    assert {:ok, _} = Slap.SnapshotLog.append(Log.base("growing"), "2")
    Log.put_follower("growing", [1, 2])

    assert %{mismatches: [], lagging: [], read_errors: []} = Task.await(task, 5_000)

    Log.put_follower("growing", [1, 2, 3])
    assert %{mismatches: [%{follower: [1, 2, 3], log: [1, 2]}]} = Log.check(["growing"])
  end

  test "a lagging follower does not block checks of other keys" do
    dir = Path.join(System.tmp_dir!(), "jepsen-log-test-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    start_supervised!(
      {Slap.Streams.Cluster,
       store: {:local, dir}, path: "streams", shards: 1, settings: %{flush_interval: "2ms"}}
    )

    for child <- Log.child_specs(), do: start_supervised!(child)

    for key <- ["slow", "fast"] do
      assert {:ok, _} = Slap.SnapshotLog.append(Log.base(key), "1")
      assert {:ok, _} = Registry.register(Log.Registry, key, nil)
    end

    Log.put_follower("slow", [])
    Log.put_follower("fast", [1])

    :erlang.trace_pattern({Log, :await_follower, 3}, true, [:local])
    :erlang.trace(:new, true, [:call, {:tracer, self()}])

    on_exit(fn ->
      :erlang.trace(:new, false, [:call])
      :erlang.trace_pattern({Log, :await_follower, 3}, false, [:local])
    end)

    task = Task.async(fn -> Log.check(["slow", "fast"]) end)
    assert_receive {:trace, _, :call, {Log, :await_follower, ["fast", [1], _]}}, 2_000

    Log.put_follower("slow", [1])
    assert %{followers: 2, lagging: [], mismatches: [], read_errors: []} = Task.await(task, 5_000)
  end

  test "the check reports a configured node that is disconnected" do
    for child <- Log.child_specs(), do: start_supervised!(child)
    LogRouter.setup([node(), :missing@localhost])

    conn =
      :post
      |> Plug.Test.conn("/check", JSON.encode!([]))
      |> LogRouter.call(LogRouter.init([]))

    assert conn.status == 200
    assert "missing@localhost" in JSON.decode!(conn.resp_body)["unreachable"]
  end
end
