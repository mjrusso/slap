defmodule Slap.Streams.CreationDurabilityTest do
  # The named cluster and its durability controller are shared by these tests.
  use Slap.Streams.Test.ClusterCase, async: false

  alias Slap.SlateDB
  alias Slap.Streams.Creation.Target
  alias Slap.Streams.Jobs.{Deleter, Repair}
  alias Slap.Streams.Store.{Keys, Read}
  alias Slap.Streams.StreamServer

  @moduletag :capture_log
  @moduletag settings: %{flush_interval: "1h"}

  test "publication commits inherited trim, reclamation bound and active metadata together" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)
    for body <- ["b", "c"], do: {:ok, _} = durable(fn -> Streams.append("/source", body) end)
    :ok = durable(fn -> Streams.trim("/source", 5) end)

    for kind <- [:reference, :copy] do
      path = "/#{kind}"
      c = reserve(path, %{}, kind)
      {:ok, cap} = durable(fn -> call(c.source, {:register, Target.selection(c), nil}) end)

      activation =
        if kind == :copy do
          {:ok, {:pending, _}} = durable(fn -> call(c.target, {:capture, c.target, cap}) end)

          {:ok, {:pending, 15}} =
            durable(fn ->
              call(c.target, {:copy_page, c.target, 5, [{5, "b"}, {10, "c"}], 15})
            end)

          {:activate, c.target}
        else
          {:activate, c.target, cap}
        end

      pid = server(path)
      ctx = ctx_for(path)
      request = :gen_server.send_request(pid, activation)
      state = :sys.get_state(pid, 5_000)
      assert state.stream.meta.reclaimed_before == 5
      assert state.durable.status == :pending
      assert Streams.head(path) == {:error, :unavailable}
      assert {:ok, <<5::64>>} = SlateDB.get(ctx.db, Keys.reclaim(c.target.sid))
      assert {:ok, nil} = SlateDB.get(ctx.db, Keys.reclaim(c.target.sid), durability: :remote)
      assert {:ok, %{reclaimed_before: 0}} = Read.get_meta(ctx.db, path, durability: :remote)
      assert :timeout = :gen_server.wait_response(request, 0)
      :ok = SlateDB.flush(ctx.db)

      assert {:reply, {:ok, {:completed, :created, _}}} =
               :gen_server.receive_response(request, 5_000)

      assert {:ok, <<5::64>>} =
               SlateDB.get(ctx.db, Keys.reclaim(c.target.sid), durability: :remote)

      assert {:ok, %{reclaimed_before: 5}} = Read.get_meta(ctx.db, path, durability: :remote)
      Deleter.drain(ctx)
      assert {:ok, %{messages: [{5, "b"}, {10, "c"}]}} = Streams.read(path, :start)
    end
  end

  test "registration rows and the retention summary change durably together" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)
    {:ok, _} = durable(fn -> Streams.append("/source", "b") end)
    first = reserve("/first")
    {:ok, _} = durable(fn -> call(first.source, {:register, Target.selection(first), nil}) end)
    :ok = durable(fn -> Streams.trim("/source", 5) end)
    second = reserve("/second")

    {:ok, cap} =
      durable(fn -> call(second.source, {:register, Target.selection(second), nil}) end)

    assert cap.start == 5
    :ok = durable(fn -> Streams.trim("/source", 10) end)
    pid = server("/source")
    ctx = ctx_for("/source")
    request = :gen_server.send_request(pid, {:unregister, first.source, first.target})
    state = :sys.get_state(pid)
    assert state.stream.meta.pin_start == 5
    assert state.stream.meta.registration_count == 1
    assert state.durable.meta.pin_start == 0
    assert state.durable.meta.registration_count == 2
    {:ok, meta} = Read.get_meta(ctx.db, "/source", durability: :remote)
    assert meta.pin_start == 0
    assert meta.reclaimed_before == 0
    assert registration(meta, first.target) != nil
    :ok = SlateDB.flush(ctx.db)
    assert {:reply, :ok} = :gen_server.receive_response(request, 5_000)
    {:ok, meta} = Read.get_meta(ctx.db, "/source", durability: :remote)
    assert meta.pin_start == 5
    assert meta.reclaimed_before == 5
    assert registration(meta, first.target) == nil
    assert registration(meta, second.target) != nil
    assert {:ok, <<5::64>>} = SlateDB.get(ctx.db, Keys.reclaim(meta.sid), durability: :remote)
  end

  test "registration cannot pin rows authorized by a trim still awaiting durability" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)
    c = reserve("/target")
    pid = server("/source")
    trim = :gen_server.send_request(pid, {:trim, 5})
    register = :gen_server.send_request(pid, {:register, Target.selection(c), nil})
    state = :sys.get_state(pid)
    assert state.stream.trim == 5
    assert state.durable.trim == 0
    assert state.stream.meta.registration_count == 0
    assert :timeout = :gen_server.wait_response(register, 0)
    :ok = SlateDB.flush(ctx_for("/source").db)
    assert {:reply, :ok} = :gen_server.receive_response(trim, 5_000)
    assert {:reply, {:error, :unavailable}} = :gen_server.receive_response(register, 5_000)
    cap = durable(fn -> call(c.source, {:register, Target.selection(c), nil}) end) |> elem(1)
    assert cap.start == 5
    assert cap.offset == 5
  end

  test "deletion awaiting durability cannot be overtaken by registration" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)
    c = reserve("/target")
    pid = server("/source")
    delete = :gen_server.send_request(pid, :delete)
    register = :gen_server.send_request(pid, {:register, Target.selection(c), nil})
    state = :sys.get_state(pid)
    assert state.stream.status == :absent
    assert state.durable.status == :active
    :ok = SlateDB.flush(ctx_for("/source").db)
    assert {:reply, :ok} = :gen_server.receive_response(delete, 5_000)
    assert {:reply, {:error, :source_not_found}} = :gen_server.receive_response(register, 5_000)
  end

  test "protection and activation each wait for durability, and the capture excludes later appends" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)
    c = reserve("/target")
    source = server("/source")
    append = :gen_server.send_request(source, {:append, %{body: "unacknowledged"}})
    reg = :gen_server.send_request(source, {:register, Target.selection(c), nil})
    state = :sys.get_state(source)
    assert state.stream.meta.registration_count == 1
    assert state.durable.meta.registration_count == 0
    assert Streams.head("/target") == {:error, :unavailable}
    assert :timeout = :gen_server.wait_response(reg, 0)
    :ok = SlateDB.flush(ctx_for("/source").db)
    assert {:reply, {:ok, _}} = :gen_server.receive_response(append, 5_000)
    assert {:reply, {:ok, cap}} = :gen_server.receive_response(reg, 5_000)
    assert cap.offset == 5
    target = server("/target")
    activation = :gen_server.send_request(target, {:activate, c.target, cap})
    state = :sys.get_state(target)
    assert state.stream.status == :active
    assert state.durable.status == :pending
    assert Streams.head("/target") == {:error, :unavailable}
    :ok = SlateDB.flush(ctx_for("/target").db)

    assert {:reply, {:ok, {:completed, :created, _}}} =
             :gen_server.receive_response(activation, 5_000)

    assert {:ok, %{messages: [{0, "a"}], next_offset: 5}} = Streams.read("/target", 0)
  end

  test "terminal evidence waits for cancellation durability before allowing unregister" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)
    c = reserve("/target")
    {:ok, _cap} = durable(fn -> call(c.source, {:register, Target.selection(c), nil}) end)
    pid = server("/target")
    delete = :gen_server.send_request(pid, :delete)
    terminal = :gen_server.send_request(pid, {:terminal, c.target})
    :sys.get_state(pid)
    assert :timeout = :gen_server.wait_response(delete, 0)
    assert :timeout = :gen_server.wait_response(terminal, 0)
    assert {:ok, :protected} = Repair.target_status(ctx_for("/target"), c.target)
    {:ok, meta} = Read.get_meta(ctx_for("/source").db, "/source", durability: :remote)
    assert registration(meta, c.target) != nil
    :ok = SlateDB.flush(ctx_for("/target").db)
    assert {:reply, :ok} = :gen_server.receive_response(delete, 5_000)
    assert {:reply, {:ok, :terminal}} = :gen_server.receive_response(terminal, 5_000)

    :ok = durable(fn -> call(c.source, {:unregister, c.source, c.target}) end)
    {:ok, meta} = Read.get_meta(ctx_for("/source").db, "/source", durability: :remote)
    assert meta.registration_count == 0
  end

  test "source registrations recover releases after a target dies before notifying repair" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)

    for action <- [:activate, :delete] do
      c = reserve("/target-#{action}", %{}, :copy)
      {:ok, cap} = durable(fn -> call(c.source, {:register, Target.selection(c), nil}) end)
      {:ok, {:pending, _}} = durable(fn -> call(c.target, {:capture, c.target, cap}) end)

      {:ok, {:pending, 5}} =
        durable(fn -> call(c.target, {:copy_page, c.target, 0, [{0, "a"}], 5}) end)

      target = server(c.target.path)

      operation =
        if action == :activate,
          do: {:activate, c.target},
          else: :delete

      request = :gen_server.send_request(target, operation)
      :sys.get_state(target)
      assert :timeout = :gen_server.wait_response(request, 0)
      monitor = Process.monitor(target)
      Process.exit(target, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^target, :killed}
      assert {:error, _} = :gen_server.receive_response(request, 5_000)
      :ok = SlateDB.flush(ctx_for(c.target.path).db)
      source = server("/source")
      monitor = Process.monitor(source)
      Process.exit(source, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^source, :killed}
      durable(fn -> Repair.sweep(ctx_for("/source")) end)
      {:ok, meta} = Read.get_meta(ctx_for("/source").db, "/source", durability: :remote)
      assert meta.registration_count == 0

      case action do
        :activate ->
          assert {:ok, %{messages: [{0, "a"}]}} = Streams.read(c.target.path, 0)

        :delete ->
          assert Streams.head(c.target.path) == {:error, :not_found}
      end
    end
  end

  test "reconciliation preserves durable pending copy progress and registration" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)
    c = reserve("/target", %{}, :copy)
    {:ok, cap} = durable(fn -> call(c.source, {:register, Target.selection(c), nil}) end)
    {:ok, {:pending, _}} = durable(fn -> call(c.target, {:capture, c.target, cap}) end)

    {:ok, {:pending, 5}} =
      durable(fn -> call(c.target, {:copy_page, c.target, 0, [{0, "a"}], 5}) end)

    pid = server("/target")
    before = :sys.get_state(pid)
    error = {:error, :source_not_found}
    request = :gen_server.send_request(pid, {:reconcile_creation, c.target, error})
    assert {:reply, ^error} = :gen_server.receive_response(request, 5_000)
    after_reconcile = :sys.get_state(pid)
    assert after_reconcile.last_write_seq == before.last_write_seq
    assert after_reconcile.stream == before.stream
    assert Streams.head("/target") == {:error, :unavailable}
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}
    durable(fn -> Repair.sweep(ctx_for("/source")) end)
    assert {:ok, :protected} = Repair.target_status(ctx_for("/target"), c.target)
    assert {:ok, meta} = Read.get_meta(ctx_for("/source").db, "/source", durability: :remote)
    assert meta.registration_count == 1
    assert registration(meta, c.target) == cap

    assert {:ok, {:pending, %{progress: %{cursor: 5}}}} =
             call(c.target, {:capture, c.target, cap})

    assert {:ok, {:completed, :created, _}} =
             durable(fn -> call(c.target, {:activate, c.target}) end)

    assert {:ok, %{messages: [{0, "a"}]}} = Streams.read("/target", 0)
  end

  test "repair cannot discard a captured prefix before publication is durable" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "hello") end)
    c = reserve("/target", %{fork_offset: 0, fork_sub_offset: 3})

    {:ok, cap} =
      durable(fn ->
        call(c.source, {:register, Target.selection(c), {c.source.sid, 0, 9, 0, "hel"}})
      end)

    target = server("/target")
    activation = :gen_server.send_request(target, {:activate, c.target, cap})
    terminal = :gen_server.send_request(target, {:terminal, c.target})
    assert %{stream: %{status: :active}, durable: %{status: :pending}} = :sys.get_state(target)
    assert :timeout = :gen_server.wait_response(terminal, 0)
    assert {:ok, :protected} = Repair.target_status(ctx_for("/target"), c.target)
    {:ok, source} = Read.get_meta(ctx_for("/source").db, "/source", durability: :remote)
    assert registration(source, c.target).prefix == "hel"
    :ok = SlateDB.flush(ctx_for("/target").db)

    assert {:reply, {:ok, {:completed, :created, _}}} =
             :gen_server.receive_response(activation, 5_000)

    assert {:reply, {:ok, :referenced}} = :gen_server.receive_response(terminal, 5_000)
    assert {:ok, :referenced} = Repair.target_status(ctx_for("/target"), c.target)
    :ok = durable(fn -> call(c.source, {:compact_registration, c.source, c.target}) end)
    {:ok, source} = Read.get_meta(ctx_for("/source").db, "/source", durability: :remote)
    assert registration(source, c.target).prefix == nil
    assert {:ok, %{messages: [{0, "hel"}]}} = Streams.read("/target", 0)
  end

  test "a running deleter consumes monotonic authorization while a new pin holds later trims" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)
    {:ok, _} = durable(fn -> Streams.append("/source", "b") end)
    :ok = durable(fn -> Streams.trim("/source", 5) end)
    c = reserve("/target")
    Deleter.drain(ctx_for("/source"))
    {:ok, cap} = durable(fn -> call(c.source, {:register, Target.selection(c), nil}) end)
    assert cap.start == 5

    {:ok, {:completed, :created, _}} =
      durable(fn -> call(c.target, {:activate, c.target, cap}) end)

    :ok = durable(fn -> Streams.trim("/source", 10) end)
    Deleter.drain(ctx_for("/source"))
    assert {:ok, %{messages: [{5, "b"}]}} = Streams.read("/target", :start)

    assert {:ok, <<5::64>>} =
             SlateDB.get(ctx_for("/source").db, Keys.reclaim(c.source.sid), durability: :remote)
  end

  test "an interrupted activation cannot become readable before its batch is durable" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "x") end)
    c = reserve("/target", %{body: "own"})
    {:ok, cap} = durable(fn -> call(c.source, {:register, Target.selection(c), nil}) end)
    pid = server("/target")
    activation = :gen_server.send_request(pid, {:activate, c.target, cap})
    assert %{stream: %{status: :active}, durable: %{status: :pending}} = :sys.get_state(pid)
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}
    assert {:error, _} = :gen_server.receive_response(activation, 5_000)
    :erlang.trace_pattern({StreamServer, :await_loaded_durable, 1}, true, [:local])
    :erlang.trace(:new, true, [:call, {:tracer, self()}])

    on_exit(fn ->
      :erlang.trace(:new, false, [:call])

      :erlang.trace_pattern({StreamServer, :await_loaded_durable, 1}, false, [:local])
    end)

    read = Task.async(fn -> Streams.read("/target", 0) end)

    assert_receive {:trace, _, :call, {StreamServer, :await_loaded_durable, _}},
                   5_000

    assert Task.yield(read, 50) == nil
    :ok = SlateDB.flush(ctx_for("/target").db)
    assert {:ok, %{messages: [{0, "x"}, {5, "own"}], next_offset: 12}} = Task.await(read)
  end

  test "matching capture retries wait for durability without rewriting or resetting progress" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)
    c = reserve("/target", %{}, :copy)
    {:ok, cap} = durable(fn -> call(c.source, {:register, Target.selection(c), nil}) end)
    pid = server("/target")
    request = {:capture, c.target, cap}
    first = :gen_server.send_request(pid, request)
    written = :sys.get_state(pid).last_write_seq
    duplicate = :gen_server.send_request(pid, request)
    assert :sys.get_state(pid).last_write_seq == written
    assert :timeout = :gen_server.wait_response(first, 0)
    assert :timeout = :gen_server.wait_response(duplicate, 0)
    :ok = SlateDB.flush(ctx_for("/target").db)
    assert {:reply, {:ok, {:pending, captured}}} = :gen_server.receive_response(first, 5_000)
    assert {:reply, {:ok, {:pending, ^captured}}} = :gen_server.receive_response(duplicate, 5_000)

    assert {:ok, {:pending, 5}} =
             durable(fn -> call(c.target, {:copy_page, c.target, 0, [{0, "a"}], 5}) end)

    written = :sys.get_state(pid).last_write_seq
    assert {:ok, {:pending, %{progress: %{cursor: 5}}}} = call(c.target, request)
    assert :sys.get_state(pid).last_write_seq == written

    activation = :gen_server.send_request(pid, {:activate, c.target})
    published = :sys.get_state(pid).last_write_seq
    late_capture = :gen_server.send_request(pid, request)

    reconciliation =
      :gen_server.send_request(pid, {:reconcile_creation, c.target, {:error, :source_not_found}})

    late_page = :gen_server.send_request(pid, {:copy_page, c.target, 0, [{0, "a"}], 5})
    assert :sys.get_state(pid).last_write_seq == published
    assert :timeout = :gen_server.wait_response(late_capture, 0)
    assert :timeout = :gen_server.wait_response(reconciliation, 0)
    assert :timeout = :gen_server.wait_response(late_page, 0)
    :ok = SlateDB.flush(ctx_for("/target").db)

    assert {:reply, {:ok, {:completed, :created, info}}} =
             :gen_server.receive_response(activation, 5_000)

    assert {:reply, {:ok, {:completed, :exists, ^info}}} =
             :gen_server.receive_response(late_capture, 5_000)

    assert {:reply, {:ok, {:completed, :exists, ^info}}} =
             :gen_server.receive_response(reconciliation, 5_000)

    assert {:reply, {:ok, {:completed, :exists, ^info}}} =
             :gen_server.receive_response(late_page, 5_000)
  end

  test "duplicate copy pages wait for durable progress and cannot rewind it" do
    {:ok, :created, _} = durable(fn -> Streams.create("/source", body: "a") end)
    {:ok, _} = durable(fn -> Streams.append("/source", "b") end)
    c = reserve("/target", %{}, :copy)
    {:ok, cap} = durable(fn -> call(c.source, {:register, Target.selection(c), nil}) end)
    {:ok, {:pending, _}} = durable(fn -> call(c.target, {:capture, c.target, cap}) end)
    pid = server("/target")
    first = :gen_server.send_request(pid, {:copy_page, c.target, 0, [{0, "a"}], 5})
    second = :gen_server.send_request(pid, {:copy_page, c.target, 5, [{5, "b"}], 10})
    duplicate = :gen_server.send_request(pid, {:copy_page, c.target, 0, [{0, "a"}], 5})
    :sys.get_state(pid, 5_000)
    assert :timeout = :gen_server.wait_response(first, 0)
    assert :timeout = :gen_server.wait_response(duplicate, 0)
    :ok = SlateDB.flush(ctx_for("/target").db)
    assert {:reply, {:ok, {:pending, 5}}} = :gen_server.receive_response(first, 5_000)
    assert {:reply, {:ok, {:pending, 10}}} = :gen_server.receive_response(second, 5_000)
    assert {:reply, {:ok, {:pending, 10}}} = :gen_server.receive_response(duplicate, 5_000)

    assert {:ok, {:completed, :created, %{next_offset: 10}}} =
             durable(fn -> call(c.target, {:activate, c.target}) end)

    assert {:ok, %{messages: [{0, "a"}, {5, "b"}]}} = Streams.read("/target", 0)
  end

  defp reserve(path, req \\ %{}, kind \\ :reference) do
    {:ok, source} = Streams.internal("/source", :source_identity)
    req = Target.request(Map.put(req, :forked_from, "/source"))

    {:ok, {:pending, c}} =
      durable(fn -> Streams.internal(path, {:reserve, kind, req, source}) end)

    c
  end

  defp registration(meta, target) do
    {:ok, registration} =
      Read.get_registration(ctx_for("/source").db, meta.sid, target, durability: :remote)

    registration
  end

  defp call(id, req), do: Streams.internal_identity(id, req, 10_000, Streams.Cluster)

  defp durable(fun) do
    task = Task.async(fun)

    Stream.repeatedly(fn ->
      for ctx <- contexts(), do: :ok = SlateDB.flush(ctx.db)

      Task.yield(task, 1)
    end)
    |> Enum.find_value(fn
      {:ok, result} -> result
      nil -> nil
    end)
  end
end
