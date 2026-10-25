defmodule Slap.Streams.ControlTest do
  use Slap.Streams.Test.ClusterCase, async: false

  alias Slap.Cluster.Config
  alias Slap.SlateDB
  alias Slap.Streams.Store.Identity
  alias Slap.Streams.Test.{CreationFixtures, FakeClock}

  test "control publication compares its durable version and excludes a concurrent seal" do
    path = "/control-test/.control"

    assert {:ok, :created, %{next_offset: offset}} =
             Streams.control_create(path, "{}", [], true, [])

    {:ok, view} = Streams.metadata(path)
    identity = Identity.new(ctx_for(path), path, view.sid)
    assert {Streams.Cluster, path} in local_work()

    assert {:ok, %{next_offset: next}} =
             Streams.control_append(path, identity, offset, "{}", false, true, [])

    assert next > offset
    refute {Streams.Cluster, path} in local_work()

    assert {:error, :conflict} =
             Streams.control_append(path, identity, offset, "{}", true, true, [])

    :ok = Streams.seal("/control-test")
    assert {:error, :sealed} = Streams.control_append(path, identity, next, "{}", false, true, [])
    assert {:ok, _} = Streams.control_append(path, identity, next, "{}", false, false, [])
  end

  test "a fenced control version rejects delayed component reservations and activation" do
    source = "/guard-source"
    control = "/guard-target/.control"
    target = "/guard-target/.updates"
    {:ok, _, _} = Streams.create(source, body: "source")
    {:ok, _, %{next_offset: offset}} = Streams.control_create(control, "{}", [], true, [])
    {:ok, view} = Streams.metadata(control)
    identity = Identity.new(ctx_for(control), control, view.sid)
    guard = %{identity: identity, offset: offset}
    {:ok, _} = Streams.control_append(control, identity, offset, "{}", true, false, [])
    assert {:error, :conflict} = Streams.create_guarded(target, guard, forked_from: source)
    assert {:error, :not_found} = Streams.head(target)
    assert {:error, :conflict} = Streams.create_guarded(target, guard, body: "late")
    assert {:error, :not_found} = Streams.head(target)
  end

  test "activation checks the control version after source registration is durable" do
    source = "/delayed-activation-source"
    control = "/delayed-activation/.control"
    target = "/delayed-activation/.updates"
    {:ok, _, _} = Streams.create(source, body: "source")
    {:ok, _, %{next_offset: offset}} = Streams.control_create(control, "{}", [], true, [])
    {:ok, view} = Streams.metadata(control)
    identity = Identity.new(ctx_for(control), control, view.sid)
    guard = %{identity: identity, offset: offset}
    creation = CreationFixtures.reserve(target, %{forked_from: source, guard: guard})
    capture = CreationFixtures.register(creation)
    {:ok, _} = Streams.control_append(control, identity, offset, "{}", true, false, [])

    assert {:error, :conflict} =
             CreationFixtures.call(creation.target, {:activate, creation.target, capture})

    assert {:error, :unavailable} = Streams.head(target)
    assert :ok = Streams.delete_identity(creation.target, [])
  end

  @tag settings: %{manifest_poll_interval: "1h"}
  test "fenced metadata and index reads reject an empty view from a former owner" do
    path = "/fenced-index/.index"
    ctx = ctx_for(path)
    config = Config.get(Streams.Cluster)
    assert {:error, :not_found} = Streams.metadata(path)
    # Keep placement active so the fencing write must reject the stale view.
    :ok = :sys.suspend(ctx.shard_db, 5_000)
    on_exit(fn -> if Process.alive?(ctx.shard_db), do: :sys.resume(ctx.shard_db, 5_000) end)

    {:ok, newer} =
      SlateDB.open(Config.shard_path(config, ctx.n),
        store: config.store,
        settings: %{manifest_poll_interval: "1h"}
      )

    on_exit(fn -> SlateDB.close(newer) end)
    assert {:ok, _} = SlateDB.put(newer, "new-owner", "committed", await_durable: true)
    assert {:ok, nil} = SlateDB.get(ctx.db, "new-owner", durability: :remote)
    assert {:error, metadata_error} = Streams.metadata_fenced(path, timeout: 100)
    assert metadata_error in [:unavailable, :timeout]
    assert {:error, read_error} = Streams.read_fenced(path, :start, timeout: 100)
    assert read_error in [:unavailable, :timeout]
  end

  test "legacy component presence and control reservation are checked in one write" do
    base = "/legacy-control"
    {:ok, _, _} = Streams.create(base <> "/.updates")

    assert {:error, :conflict} =
             Streams.control_create(base <> "/.control", "{}", [base <> "/.updates"], true, [])

    assert {:error, :not_found} = Streams.head(base <> "/.control")
  end

  test "an expired stream ignores request guards while recording expiry" do
    FakeClock.install(1_700_000_000_000)
    on_exit(&FakeClock.uninstall/0)
    path = "/expired-control/.control"
    {:ok, _, _} = Streams.create(path, body: "{}", expires_at_ms: FakeClock.now_ms() + 100)
    {:ok, view} = Streams.metadata(path)
    identity = Identity.new(ctx_for(path), path, view.sid)
    pid = server(path)
    FakeClock.advance(100)

    assert {:error, :not_found} = Streams.control_append(path, identity, 0, "{}", nil, true, [])
    assert Process.alive?(pid)
    assert {:error, :not_found} = Streams.head(path)
    refute {Streams.Cluster, path} in local_work()
  end

  test "a rejected guarded append neither guards a TTL touch nor changes its work index" do
    FakeClock.install(1_700_000_000_000)
    on_exit(&FakeClock.uninstall/0)
    path = "/ttl-control/.control"
    {:ok, _, _} = Streams.create(path, body: "{}", ttl_s: 10, content_type: "application/json")
    {:ok, view} = Streams.metadata(path)
    identity = Identity.new(ctx_for(path), path, view.sid)
    pid = server(path)
    FakeClock.advance(2_000)

    assert {:error, :conflict} = Streams.control_append(path, identity, 0, "{}", true, true, [])
    assert Process.alive?(pid)
    refute {Streams.Cluster, path} in local_work()
    FakeClock.advance(9_000)
    assert {:ok, _} = Streams.head(path)
  end

  test "work is consumed incrementally and a stalled shard does not block another" do
    first = "/work-a/.control"
    shard = ctx_for(first).n
    second = Enum.find(Stream.map(1..100, &"/work-b-#{&1}/.control"), &(ctx_for(&1).n != shard))
    later = Enum.find(Stream.map(1..100, &"/work-c-#{&1}/.control"), &(ctx_for(&1).n == shard))
    for path <- [first, second, later], do: Streams.control_create(path, "{}", [], true, [])
    caller = self()

    task =
      Task.async(fn ->
        Streams.each_local_work(fn {_, path} ->
          send(caller, {:entry, self(), path})
          if path == first, do: receive(do: (:continue -> :ok))
        end)
      end)

    assert_receive {:entry, worker, ^first}, 5_000
    assert_receive {:entry, _, ^second}, 5_000
    refute_receive {:entry, _, ^later}, 0
    send(worker, :continue)
    assert_receive {:entry, _, ^later}, 5_000
    assert :ok = Task.await(task)
  end

  defp local_work do
    caller = self()
    Streams.each_local_work(&send(caller, {:work, &1}))
    collect_work([])
  end

  defp collect_work(acc) do
    receive do
      {:work, entry} -> collect_work([entry | acc])
    after
      0 -> acc
    end
  end
end
