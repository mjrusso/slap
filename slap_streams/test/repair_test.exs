defmodule Slap.Streams.RepairTest do
  # The cluster is registered under one node-wide name.
  use Slap.Streams.Test.ClusterCase, async: false

  import Slap.Streams.Test.CreationFixtures, only: [meta: 1, registrations: 1]

  alias Slap.Streams.Jobs.Repair
  alias Slap.Streams.Store.{Identity, Keys, Read}
  alias Slap.Streams.StreamServer

  @moduletag :capture_log
  @moduletag child_options: [repair_interval: :timer.hours(1)]

  @tag shards: 1
  test "repair leaves active references and copies with inherited trims available" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    for body <- ["b", "c"], do: {:ok, _} = Streams.append("/source", body)
    :ok = Streams.trim("/source", 5)
    {:ok, :created, _} = Streams.create("/reference", forked_from: "/source")
    {:ok, :created, _} = Streams.copy("/source", "/copy")
    ctx = ctx_for("/source")
    Repair.sweep(ctx)
    targets = for path <- ["/reference", "/copy"], do: {identity(path), server(path)}
    for {_id, pid} <- targets, do: :erlang.trace(pid, true, [:receive, {:tracer, self()}])

    try do
      for _ <- 1..3 do
        Repair.sweep(ctx)

        for {id, pid} <- targets do
          assert server(id.path) == pid
          assert {:ok, _} = Streams.append(id.path, "own")
          assert {:ok, %{messages: [{5, "b"}]}} = Streams.read(id.path, :start, max_bytes: 1)
        end
      end

      barrier = :erlang.trace_delivered(:all)
      assert_receive {:trace_delivered, :all, ^barrier}, 5_000
      refute_received {:trace, _, :receive, {:"$gen_call", _, {:repair_check, _}}}

      for {id, _pid} <- targets do
        assert {:ok, <<5::64>>} =
                 Slap.SlateDB.get(ctx.db, Keys.reclaim(id.sid), durability: :remote)

        assert {:ok, %{reclaimed_before: 5}} = Read.get_meta(ctx.db, id.path, durability: :remote)
      end
    after
      for {_id, pid} <- targets, do: :erlang.trace(pid, false, [:receive])
    end
  end

  @tag shards: 1
  test "notifications release unrelated sources while an earlier check is blocked" do
    {:ok, :created, _} = Streams.create("/slow-source", body: "a")
    {:ok, :created, _} = Streams.create("/slow-target", forked_from: "/slow-source")
    ctx = ctx_for("/slow-source")
    Repair.sweep(ctx)
    job = repair_job(ctx)
    :ok = :sys.suspend(job)
    :ok = Streams.delete("/slow-target")
    slow = server("/slow-target")
    slow_id = identity("/slow-source")
    [%{target: slow_target}] = registrations("/slow-source")
    :ok = :sys.suspend(slow)
    :erlang.trace_pattern({Repair, :target_status, 2}, true, [:local])
    :erlang.trace(:new, true, [:call, {:tracer, self()}])
    :ok = :sys.resume(job)

    try do
      assert_receive {:trace, _, :call, {Repair, :target_status, [_, ^slow_target]}}, 5_000
      {:ok, :created, _} = Streams.create("/source", body: "b")
      {:ok, :created, _} = Streams.create("/target", forked_from: "/source")
      source = identity("/source")
      target = identity("/target")
      await_notification(job, target)
      pid = server("/source")
      :erlang.trace(pid, true, [:receive, {:tracer, self()}])
      :ok = Streams.delete("/source")
      :ok = Streams.delete("/target")

      assert_receive {:trace, ^pid, :receive, {:"$gen_call", _, {:unregister, ^source, ^target}}},
                     1_000

      assert :ok =
               Streams.internal_identity(
                 source,
                 {:compact_registration, source, target},
                 5_000,
                 Streams.Cluster
               )

      assert Streams.head("/source") == {:error, :not_found}
      assert {:ok, :created, _} = Streams.create("/source", body: "replacement")
      assert identity("/source") != source
      assert identity("/slow-source") == slow_id
    after
      :erlang.trace(:new, false, [:call])
      :erlang.trace_pattern({Repair, :target_status, 2}, false, [:local])
      :sys.resume(slow)
      Repair.sweep(ctx)
    end
  end

  @tag shards: 1
  test "registration checks bound concurrency and recover overflowed notifications" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    ctx = ctx_for("/source")
    job = repair_job(ctx)
    Repair.sweep(ctx)
    :ok = :sys.suspend(job)

    targets =
      for i <- 1..82 do
        path = "/target-#{i}"
        {:ok, :created, _} = Streams.create(path, forked_from: "/source")
        :ok = Streams.delete(path)
        pid = server(path)
        :ok = :sys.suspend(pid)
        {path, pid}
      end

    :erlang.trace_pattern({Repair, :target_status, 2}, true, [:local])
    :erlang.trace(:new, true, [:call, {:tracer, self()}])

    try do
      :ok = :sys.resume(job)

      paths =
        for _ <- 1..8 do
          assert_receive {:trace, _, :call, {Repair, :target_status, [_, target]}}, 5_000
          target.path
        end

      :sys.get_state(job)
      barrier = :erlang.trace_delivered(:all)
      assert_receive {:trace_delivered, :all, ^barrier}, 5_000
      refute_received {:trace, _, :call, {Repair, :target_status, _}}
      {_, pid} = Enum.find(targets, fn {path, _} -> path in paths end)
      :ok = :sys.resume(pid)
      assert_receive {:trace, _, :call, {Repair, :target_status, _}}, 5_000
    after
      :erlang.trace(:new, false, [:call])
      :erlang.trace_pattern({Repair, :target_status, 2}, false, [:local])
      for {_, pid} <- targets, do: :sys.resume(pid)
      Repair.sweep(ctx)
    end

    assert registrations("/source") == []
  end

  test "stopping repair stops its blocked workers and preserves protection" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    {:ok, :created, _} = Streams.create("/target", forked_from: "/source")
    ctx = ctx_for("/source")
    job = repair_job(ctx)
    Repair.sweep(ctx)
    :ok = :sys.suspend(job)
    :ok = Streams.delete("/target")
    target = server("/target")
    :ok = :sys.suspend(target)
    :erlang.trace_pattern({Repair, :target_status, 2}, true, [:local])
    :erlang.trace(:new, true, [:call, {:tracer, self()}])

    try do
      :ok = :sys.resume(job)
      assert_receive {:trace, worker, :call, {Repair, :target_status, _}}, 5_000
      ref = Process.monitor(worker)
      Process.exit(job, :kill)
      assert_receive {:DOWN, ^ref, :process, ^worker, :killed}, 5_000
      assert [_] = registrations("/source")
    after
      :erlang.trace(:new, false, [:call])
      :erlang.trace_pattern({Repair, :target_status, 2}, false, [:local])
      :sys.resume(target)
    end
  end

  test "maintenance requests do not refresh idle activity" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    {:ok, :created, _} = Streams.create("/target", forked_from: "/source")
    ctx = ctx_for("/source")
    Repair.sweep(ctx)
    source = identity("/source")
    target = identity("/target")

    for {id, request} <- [
          {source, {:repair_check, source.sid}},
          {target, {:terminal, target}},
          {source, {:unregister, source, %{target | sid: target.sid + 1}}},
          {source, {:compact_registration, source, target}},
          {source, {:expiry_check, source.sid, 0}}
        ] do
      pid = server(id.path)
      :sys.replace_state(pid, &%{&1 | last_active: 0})
      StreamServer.call(ctx_for(id.path), id.path, request, 5_000)
      assert :sys.get_state(pid).last_active == 0
    end

    assert {:ok, _} = Streams.head("/source")
    assert :sys.get_state(server("/source")).last_active > 0
  end

  test "notifications retain cross-shard identities and ignore a replaced target" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    ctx = ctx_for("/source")
    path = Enum.find(for(i <- 1..100, do: "/target-#{i}"), &(ctx_for(&1).n != ctx.n))
    job = repair_job(ctx)
    :ok = :sys.suspend(job)

    try do
      {:ok, :created, _} = Streams.create(path, forked_from: "/source")
      [cap] = registrations("/source")
      :ok = Streams.delete(path)
      :sys.get_state(server(path))
      :ok = :sys.resume(job)
      Repair.sweep(ctx)
      assert registrations("/source") == []
      {:ok, :created, _} = Streams.create(path, forked_from: "/source")
      replacement = identity(path)
      assert replacement != cap.target
      Repair.notify(Streams.Cluster, cap.source, cap.target)
      Repair.sweep(ctx)
      assert [%{target: ^replacement}] = registrations("/source")
    after
      :sys.resume(job)
    end
  end

  test "repair lets idle reference graphs shut down" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    {:ok, :created, _} = Streams.create("/target", forked_from: "/source")
    Repair.sweep(ctx_for("/source"))

    servers =
      for path <- ["/source", "/target"] do
        pid = server(path)
        :sys.replace_state(pid, &%{&1 | last_active: 0})
        {pid, Process.monitor(pid)}
      end

    Repair.sweep(ctx_for("/source"))

    for {pid, ref} <- servers do
      send(pid, :idle_check)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
    end
  end

  test "repair does not restart healthy reference graphs or deleted intermediates" do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    {:ok, :created, _} = Streams.create("/middle", forked_from: "/source")
    {:ok, :created, _} = Streams.create("/target", forked_from: "/middle")
    :ok = Streams.delete("/middle")
    for path <- ["/source", "/middle"], do: Repair.sweep(ctx_for(path))

    for path <- ["/source", "/middle", "/target"] do
      :ok = GenServer.stop(server(path), :normal, 5_000)
    end

    for path <- ["/source", "/middle"], do: Repair.sweep(ctx_for(path))
    for path <- ["/source", "/middle", "/target"], do: assert(server(path) == nil)
    assert {:ok, %{messages: [{0, "a"}]}} = Streams.read("/target", 0)
  end

  @tag shards: 1
  test "publication and finalization repair only their source registration" do
    {:ok, :created, _} = Streams.create("/other-source", body: "other")
    {:ok, :created, _} = Streams.create("/other-target", forked_from: "/other-source")
    {:ok, :created, _} = Streams.create("/source", body: "a")
    ctx = ctx_for("/source")
    Repair.sweep(ctx)
    source = identity("/source")
    pid = server("/source")
    :erlang.trace(pid, true, [:receive, {:tracer, self()}])
    :erlang.trace_pattern({Repair, :target_status, 2}, true, [:local])
    :erlang.trace(:new, true, [:call, {:tracer, self()}])

    try do
      {:ok, :created, _} = Streams.copy("/source", "/copy")
      await_release(pid, source)
      {:ok, :created, _} = Streams.create("/target", forked_from: "/source")
      await_notification(repair_job(ctx), identity("/target"))
      :ok = Streams.delete("/target")
      await_release(pid, source)
      assert registrations("/source") == []
      assert [%{target: %{path: "/other-target"}}] = registrations("/other-source")
      barrier = :erlang.trace_delivered(:all)
      assert_receive {:trace_delivered, :all, ^barrier}, 5_000
      refute_received {:trace, _, :call, {Repair, :target_status, [_, %{path: "/other-target"}]}}
    after
      :erlang.trace(pid, false, [:receive])
      :erlang.trace(:new, false, [:call])
      :erlang.trace_pattern({Repair, :target_status, 2}, false, [:local])
    end
  end

  @tag shards: 1
  test "queued sweep callers share a fresh pass without waiting for later requests or ticks" do
    during_terminal_checks(fn job, _source, target ->
      first = :gen_server.send_request(job, :sweep)
      assert_receive {:"$gen_call", first_check, {:terminal, ^target}}, 5_000
      for _ <- 1..100, do: send(job, :tick)
      waiting = for _ <- 1..20, do: :gen_server.send_request(job, :sweep)
      abandoned = :gen_server.send_request(job, :sweep)
      :sys.get_state(job)
      assert :timeout = :gen_server.receive_response(abandoned, 0)
      assert :timeout = :gen_server.wait_response(first, 0)
      GenServer.reply(first_check, {:error, :unavailable})
      assert {:reply, 1} = :gen_server.receive_response(first, 1_000)
      assert_receive {:"$gen_call", second_check, {:terminal, ^target}}, 5_000
      for caller <- waiting, do: assert(:timeout = :gen_server.wait_response(caller, 0))
      later = :gen_server.send_request(job, :sweep)
      for _ <- 1..100, do: send(job, :tick)
      :sys.get_state(job)
      GenServer.reply(second_check, {:error, :unavailable})
      for caller <- waiting, do: assert({:reply, 1} = :gen_server.receive_response(caller, 1_000))
      assert_receive {:"$gen_call", later_check, {:terminal, ^target}}, 5_000
      assert :timeout = :gen_server.wait_response(later, 0)
      GenServer.reply(later_check, {:error, :unavailable})
      assert {:reply, 1} = :gen_server.receive_response(later, 1_000)
      assert [_] = registrations("/source")
    end)
  end

  @tag shards: 1
  test "suspending model repair waits for existing workers before deferring later cleanup" do
    during_terminal_checks(fn job, source, target ->
      Repair.notify(Streams.Cluster, source, target)
      assert_receive {:"$gen_call", notification, {:terminal, ^target}}, 5_000
      :erlang.trace(job, true, [:receive, {:tracer, self()}])
      pause = Task.async(fn -> suspend_repair() end)
      pausing = pause.pid

      try do
        assert_receive {:trace, ^job, :receive, {:system, {^pausing, _}, :suspend}}, 5_000
        assert Task.yield(pause, 100) == nil
        GenServer.reply(notification, {:ok, :terminal})
        assert Task.await(pause, 5_000) == :ok
        assert registrations("/source") == []

        {:ok, :created, _} = Streams.create("/later", forked_from: "/source")
        :ok = Streams.delete("/later")
        :ok = Streams.delete("/source")
        assert Streams.head("/source") == {:error, :gone}
        assert [_] = registrations("/source")
        resume_repair()
        repair_until(fn -> retention_counts() == %{} end)
        assert Streams.head("/source") == {:error, :not_found}
      after
        :erlang.trace(job, false, [:receive])
        GenServer.reply(notification, {:ok, :terminal})
        resume_repair()
      end
    end)
  end

  @tag shards: 1
  test "sweeps finish independently of notification checks and later callers get fresh checks" do
    during_terminal_checks(fn job, source, target ->
      first = :gen_server.send_request(job, :sweep)
      assert_receive {:"$gen_call", first_check, {:terminal, ^target}}, 5_000
      for _ <- 1..100, do: Repair.notify(Streams.Cluster, source, target)
      assert_receive {:"$gen_call", notification_check, {:terminal, ^target}}, 5_000
      :sys.get_state(job)
      GenServer.reply(first_check, {:error, :unavailable})
      assert {:reply, 1} = :gen_server.receive_response(first, 1_000)

      second = :gen_server.send_request(job, :sweep)
      assert_receive {:"$gen_call", fresh_check, {:terminal, ^target}}, 5_000
      assert :timeout = :gen_server.wait_response(second, 0)
      GenServer.reply(fresh_check, {:error, :unavailable})
      assert {:reply, 1} = :gen_server.receive_response(second, 1_000)
      GenServer.reply(notification_check, {:error, :unavailable})
      assert [_] = registrations("/source")
    end)
  end

  defp during_terminal_checks(fun) do
    {:ok, :created, _} = Streams.create("/source", body: "a")
    {:ok, :created, _} = Streams.create("/target", forked_from: "/source")
    ctx = ctx_for("/source")
    job = repair_job(ctx)
    Repair.sweep(ctx)
    :ok = :sys.suspend(job)
    target = identity("/target")
    source = identity("/source")
    :ok = Streams.delete("/target")
    :ok = GenServer.stop(server("/target"), :normal, 5_000)
    key = {ctx.n, {:stream, "/target"}}
    {:ok, _} = Registry.register(ctx.registry, key, nil)
    :ok = :sys.resume(job)

    try do
      assert_receive {:"$gen_call", {worker, _} = notification, {:terminal, ^target}}, 5_000
      ref = Process.monitor(worker)
      GenServer.reply(notification, {:error, :unavailable})
      assert_receive {:DOWN, ^ref, :process, ^worker, :normal}, 5_000
      :sys.get_state(job)
      fun.(job, source, target)
    after
      Registry.unregister(ctx.registry, key)
    end
  end

  defp await_notification(job, target) do
    assert_receive {:trace, worker, :call, {Repair, :target_status, [_, ^target]}}, 5_000
    ref = Process.monitor(worker)
    assert_receive {:DOWN, ^ref, :process, ^worker, _reason}, 5_000
    :sys.get_state(job)
  end

  defp await_release(pid, source) do
    assert_receive {:trace, ^pid, :receive, {:"$gen_call", _, {:unregister, ^source, target}}},
                   5_000

    assert :ok =
             Streams.internal_identity(
               source,
               {:compact_registration, source, target},
               5_000,
               Streams.Cluster
             )
  end

  defp repair_job(ctx), do: GenServer.whereis(Slap.Cluster.via(Streams.Cluster, ctx.n, :repair))

  defp identity(path), do: Identity.new(ctx_for(path), path, meta(path).sid)
end
