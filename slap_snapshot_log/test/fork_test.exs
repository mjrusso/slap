defmodule Slap.SnapshotLog.ForkTest do
  use Slap.SnapshotLog.Test.ClusterCase

  alias Slap.SnapshotLog.{Control, Fork, Initialization, Recovery, Store}
  alias Slap.Streams
  alias Slap.Streams.Cluster
  alias Slap.Streams.Creation.Target

  test "unsnapshotted and empty forks preserve their capture across edits and deletion" do
    source = base()
    target = other_shard(source)
    assert {:ok, 5} = SnapshotLog.append(source, "a")
    assert {:ok, %{offset: 5, snapshot_offset: nil} = capture} = SnapshotLog.fork(source, target)
    assert load(target) == {nil, ["a"], 5}
    assert {:ok, 10} = SnapshotLog.append(target, "b")
    assert {:ok, 10} = SnapshotLog.append(source, "c")
    assert :ok = SnapshotLog.delete(source)
    assert {:ok, ^capture} = SnapshotLog.fork(source, target)
    assert load(target) == {nil, ["a", "b"], 10}

    empty = base()
    {:reset, _} = SnapshotLog.next(empty, nil)
    assert {:ok, %{offset: 0, snapshot_offset: nil}} = SnapshotLog.fork(empty, base())
  end

  test "a snapshot fork uses a closed reference and a fresh target index" do
    source = base()
    target = other_shard(source)
    {:ok, at} = SnapshotLog.append(source, "a")
    assert :ok = SnapshotLog.snapshot(source, at, "[a]")
    {:ok, tail} = SnapshotLog.append(source, "b")
    assert {:ok, %{offset: ^tail, snapshot_offset: ^at}} = SnapshotLog.fork(source, target)
    assert load(target) == {"[a]", ["b"], tail}
    assert {:ok, %{closed: true}} = Streams.head(SnapshotLog.path(target, {:snapshot, at}))
    assert {:error, {:closed, _}} = Streams.append(SnapshotLog.path(target, {:snapshot, at}), "x")
    assert {:ok, [%{offset: ^at}]} = SnapshotLog.snapshots(target)
    assert :ok = SnapshotLog.delete(source)
    assert load(target) == {"[a]", ["b"], tail}
  end

  test "the target index names only the current snapshot and uses target metadata" do
    source = base()
    target = other_shard(source)
    {:ok, first} = SnapshotLog.append(source, "a")
    history = [history: [{1, 10_000}]]
    :ok = SnapshotLog.snapshot_internal(source, first, "a", [now: 100] ++ history)
    {:ok, current} = SnapshotLog.append(source, "b")
    :ok = SnapshotLog.snapshot_internal(source, current, "ab", [now: 200] ++ history)
    assert {:ok, [%{offset: ^first}, %{offset: ^current}]} = SnapshotLog.snapshots(source)
    assert {:ok, %{snapshot_offset: ^current}} = SnapshotLog.fork(source, target)
    {:ok, %{messages: [{_, body}]}} = Streams.read(SnapshotLog.path(target, :index), :start)

    assert %{"retained" => [], "createdAt" => created, "snapshotOffset" => encoded} =
             JSON.decode!(body)

    assert created > 200
    assert encoded == Streams.Offset.encode(current)
    assert {:error, :not_found} = Streams.head(SnapshotLog.path(target, {:snapshot, first}))
    assert load(target) == {"ab", [], current}
  end

  for step <- [
        :reservation,
        :selection,
        :updates,
        :snapshot,
        :capture,
        :index,
        :before_publish,
        :publish
      ] do
    test "interruption at #{step} resumes from durable state without initializing a pending target" do
      source = base()
      target = other_shard(source)
      {:ok, at} = SnapshotLog.append(source, "a")
      :ok = SnapshotLog.snapshot(source, at, "snapshot")
      {:ok, tail} = SnapshotLog.append(source, "b")
      step = unquote(step)

      assert catch_throw(
               SnapshotLog.fork_internal(source, target,
                 after_step: fn reached -> if reached == step, do: throw(:interrupted) end
               )
             ) == :interrupted

      if unquote(step != :publish) do
        assert {:error, :busy} = SnapshotLog.next(target, nil)
        assert {:error, :busy} = SnapshotLog.next(target, 0, wait: 0)
        assert {:error, :busy} = SnapshotLog.append(target, "x")
        assert {:error, :busy} = SnapshotLog.snapshot(target, 0, "x")
        assert {:error, :busy} = SnapshotLog.tail(target)
        assert {:error, :busy} = SnapshotLog.snapshots(target)
      end

      assert :ok = Recovery.sweep()
      assert {:ok, %{offset: ^tail, snapshot_offset: ^at}} = SnapshotLog.fork(source, target)
      assert load(target) == {"snapshot", ["b"], tail}
    end
  end

  test "deletion fences a delayed creation and prevents retry revival" do
    source = base()
    target = other_shard(source)
    {:ok, _} = SnapshotLog.append(source, "a")
    parent = self()

    task =
      Task.async(fn ->
        SnapshotLog.fork_internal(source, target,
          after_step: fn
            :reservation ->
              send(parent, {:reserved, self()})

              receive do
                :continue -> :ok
              end

            _ ->
              :ok
          end
        )
      end)

    assert_receive {:reserved, pid}, 5_000
    assert :ok = SnapshotLog.delete(target)
    send(pid, :continue)
    assert {:error, :deleted} = Task.await(task)
    assert {:error, :deleted} = SnapshotLog.fork(source, target)
    assert {:error, :deleted} = SnapshotLog.next(target, nil)
  end

  test "matching creates converge and conflicting sources do not claim the reservation" do
    source = base()
    target = other_shard(source)
    {:ok, tail} = SnapshotLog.append(source, "a")
    tasks = for _ <- 1..5, do: Task.async(fn -> SnapshotLog.fork(source, target) end)
    for task <- tasks, do: assert({:ok, %{offset: ^tail}} = Task.await(task))
    assert {:error, :exists} = SnapshotLog.fork(base(), target)
    deletes = for _ <- 1..5, do: Task.async(fn -> SnapshotLog.delete(target) end)
    for task <- deletes, do: assert(:ok = Task.await(task))
    assert {:error, :deleted} = SnapshotLog.next(target, nil)
  end

  test "source compaction after protection does not advance the fixed capture" do
    source = base()
    target = other_shard(source)
    {:ok, at} = SnapshotLog.append(source, "a")
    :ok = SnapshotLog.snapshot(source, at, "a")
    {:ok, tail} = SnapshotLog.append(source, "b")

    assert {:ok, %{offset: ^tail, snapshot_offset: ^at}} =
             SnapshotLog.fork_internal(source, target,
               after_step: fn
                 :capture ->
                   {:ok, newer} = SnapshotLog.append(source, "c")
                   :ok = SnapshotLog.snapshot(source, newer, "abc")
                   :ok = SnapshotLog.delete(source)

                 _ ->
                   :ok
               end
             )

    assert load(target) == {"a", ["b"], tail}
  end

  test "public errors do not initialize missing sources or replace empty targets" do
    source = base()
    assert {:error, {:bad_request, :same_source_and_target}} = SnapshotLog.fork(source, source)
    assert {:error, :not_found} = SnapshotLog.fork(source, base())
    assert {:error, :not_found} = Streams.head(SnapshotLog.path(source, :updates))
    {:reset, _} = SnapshotLog.next(source, nil)
    target = base()
    {:reset, _} = SnapshotLog.next(target, nil)
    assert {:error, :exists} = SnapshotLog.fork(source, target)
    sealed = base()
    :ok = Streams.seal(sealed)
    assert {:error, :sealed} = SnapshotLog.fork(source, sealed)
    assert_raise ArgumentError, fn -> SnapshotLog.fork(source, base(), at: 0) end
    assert_raise ArgumentError, fn -> SnapshotLog.fork(source, base(), retries: -1) end
  end

  test "candidate replacement releases its old reference before selecting a later snapshot" do
    source = base()
    target = other_shard(source)
    {:ok, at} = SnapshotLog.append(source, "a")
    :ok = SnapshotLog.snapshot(source, at, "a")
    {:ok, tail} = SnapshotLog.append(source, "b")
    parent = self()

    task =
      Task.async(fn ->
        SnapshotLog.fork_internal(source, target,
          after_step: fn
            :updates ->
              send(parent, {:updates_protected, self()})

              receive do
                :continue -> :ok
              end

            _ ->
              :ok
          end
        )
      end)

    assert_receive {:updates_protected, pid}, 5_000
    :ok = SnapshotLog.snapshot(source, tail, "ab")
    send(pid, :continue)
    assert_receive {:updates_protected, ^pid}, 5_000
    send(pid, :continue)
    assert {:ok, %{offset: ^tail, snapshot_offset: ^tail}} = Task.await(task)
    assert load(target) == {"ab", [], tail}
  end

  test "compaction and deletion through a three-level chain retain the last branch" do
    a = base()
    b = other_shard(a)
    c = other_shard(b)
    {:ok, at} = SnapshotLog.append(a, "a")
    :ok = SnapshotLog.snapshot(a, at, "a")
    {:ok, tail} = SnapshotLog.append(a, "b")
    {:ok, _} = SnapshotLog.fork(a, b)
    {:ok, _} = SnapshotLog.fork(b, c)
    :ok = SnapshotLog.snapshot(b, tail, "ab")
    :ok = SnapshotLog.delete(a)
    :ok = SnapshotLog.delete(b)
    assert load(c) == {"a", ["b"], tail}
    {:ok, next} = SnapshotLog.append(c, "c")
    assert :ok = SnapshotLog.snapshot(c, next, "abc")
    assert load(c) == {"abc", [], next}
    assert :ok = SnapshotLog.delete(c)
    assert {:error, :deleted} = SnapshotLog.fork(b, c)
  end

  test "a seal between component creation and publication prevents readiness" do
    source = base()
    target = other_shard(source)
    {:ok, _} = SnapshotLog.append(source, "a")

    assert {:error, :sealed} =
             SnapshotLog.fork_internal(source, target,
               after_step: fn
                 :before_publish -> :ok = Streams.seal(target)
                 _ -> :ok
               end
             )

    assert {:error, :deleted} = SnapshotLog.next(target, nil)
    assert {:error, :sealed} = SnapshotLog.fork(source, target)
  end

  test "recovery discovers an abandoned capture after the runtime restarts", %{cluster_opts: opts} do
    source = base()
    target = other_shard(source)
    {:ok, at} = SnapshotLog.append(source, "a")
    :ok = SnapshotLog.snapshot(source, at, "a")
    {:ok, tail} = SnapshotLog.append(source, "b")

    assert catch_throw(
             SnapshotLog.fork_internal(source, target,
               after_step: fn
                 :capture -> throw(:stopped)
                 _ -> :ok
               end
             )
           ) == :stopped

    :ok = Application.stop(:slap_snapshot_log)
    stop_supervised!(Streams.Cluster)
    start_supervised!({Streams.Cluster, opts})
    :ok = Application.start(:slap_snapshot_log)
    assert :ok = Recovery.sweep()
    assert load(target) == {"a", ["b"], tail}
  end

  test "a durable cancellation is discovered and cleaned without another target request", %{
    cluster_opts: opts
  } do
    :ok = Application.stop(:slap_snapshot_log)
    on_exit(fn -> Application.ensure_all_started(:slap_snapshot_log) end)
    source = base()
    target = other_shard(source)
    {:ok, _} = SnapshotLog.append(source, "a")

    assert catch_throw(
             SnapshotLog.fork_internal(source, target,
               after_step: fn
                 :updates -> throw(:stopped)
                 _ -> :ok
               end
             )
           ) == :stopped

    store = Store.new(target, [])
    deadline = Control.deadline(5_000)
    {:ok, control} = Control.read(store, deadline)
    assert {:ok, _} = Fork.cancel(store, control, :busy, deadline)
    stop_supervised!(Streams.Cluster)
    start_supervised!({Streams.Cluster, opts})
    :ok = Application.start(:slap_snapshot_log)
    assert :ok = Recovery.sweep()
    assert {:error, :not_found} = Streams.head(SnapshotLog.path(target, :updates))
    assert {:error, :busy} = SnapshotLog.fork(source, target)
    assert {:error, :deleted} = SnapshotLog.append(target, "x")
  end

  test "depth rejection on the snapshot cleans an already-protected updates component" do
    source = base()
    target = other_shard(source)
    {:ok, at} = SnapshotLog.append(source, "a")
    :ok = SnapshotLog.snapshot(source, at, "a")
    original = SnapshotLog.path(source, {:snapshot, at})

    seed = base() <> "/.snapshot"
    {:ok, _, _} = Streams.create(seed, body: "a", closed: true)

    deepest =
      Enum.reduce(1..7, seed, fn n, previous ->
        path = base() <> "/.snapshot-#{n}"
        {:ok, _, _} = Streams.create(path, forked_from: previous, closed: true)
        path
      end)

    :ok = Streams.delete(original)
    {:ok, _, _} = Streams.create(original, forked_from: deepest, closed: true)
    assert {:error, {:bad_request, :max_fork_depth_exceeded}} = SnapshotLog.fork(source, target)
    assert {:error, :deleted} = SnapshotLog.next(target, nil)
    assert :ok = Recovery.sweep()
    assert {:error, :not_found} = Streams.head(SnapshotLog.path(target, :updates))
  end

  @tag child_options: [creation_grace: 0]
  test "recovery resolves a component cancelled by stream maintenance" do
    source = base()
    target = other_shard(source)
    {:ok, _} = SnapshotLog.append(source, "a")

    assert catch_throw(
             SnapshotLog.fork_internal(source, target,
               after_step: fn
                 :selection -> throw(:paused)
                 _ -> :ok
               end
             )
           ) == :paused

    store = Store.new(target, [])
    deadline = Control.deadline(5_000)
    {:ok, control} = Control.read(store, deadline)
    source_path = SnapshotLog.path(source, :updates)
    {:ok, identity} = Streams.internal(source_path, :source_identity)

    request =
      Target.request(%{
        forked_from: source_path,
        fork_offset: control.record["requested_cutoff"],
        guard: Control.guard(control)
      })

    {:ok, {:pending, creation}} =
      Streams.internal(
        SnapshotLog.path(target, :updates),
        {:reserve, :reference, request, identity}
      )

    {:ok, capture} =
      Streams.internal(
        source_path,
        {:register, Target.selection(creation), nil}
      )

    assert :ok = Streams.internal(creation.target.path, {:repair_check, creation.target.sid})
    {:ok, _} = SnapshotLog.append(source, "later")
    assert :ok = Recovery.sweep()
    assert {:ok, %{offset: 5}} = SnapshotLog.fork(source, target)
    assert load(target) == {nil, ["a"], 5}

    assert {:error, :conflict} =
             Streams.internal(
               creation.target.path,
               {:activate, creation.target, capture}
             )

    assert load(target) == {nil, ["a"], 5}
  end

  test "update depth rejection never creates a readable log" do
    source = base()
    {:ok, _} = SnapshotLog.append(source, "a")

    source =
      Enum.reduce(1..8, source, fn _, previous ->
        next = other_shard(previous)
        assert {:ok, _} = SnapshotLog.fork(previous, next)
        next
      end)

    target = other_shard(source)
    assert {:error, {:bad_request, :max_fork_depth_exceeded}} = SnapshotLog.fork(source, target)
    assert {:error, :deleted} = SnapshotLog.next(target, nil)
    assert :ok = Recovery.sweep()
    assert {:error, :not_found} = Streams.head(SnapshotLog.path(target, :updates))
  end

  test "source deletion between component references cannot publish a partial log" do
    source = base()
    target = other_shard(source)
    {:ok, at} = SnapshotLog.append(source, "a")
    :ok = SnapshotLog.snapshot(source, at, "a")
    {:ok, _} = SnapshotLog.append(source, "b")

    assert {:error, :not_found} =
             SnapshotLog.fork_internal(source, target,
               after_step: fn
                 :updates -> :ok = SnapshotLog.delete(source)
                 _ -> :ok
               end
             )

    assert {:error, :deleted} = SnapshotLog.next(target, nil)
    assert :ok = Recovery.sweep()
    assert {:error, :not_found} = Streams.head(SnapshotLog.path(target, :updates))
  end

  test "exhausted candidate retries retain their terminal failure" do
    source = base()
    target = other_shard(source)
    {:ok, at} = SnapshotLog.append(source, "a")

    assert {:error, :retries_exhausted} =
             SnapshotLog.fork_internal(source, target,
               retries: 0,
               after_step: fn
                 :selection -> :ok = SnapshotLog.snapshot(source, at, "a")
                 _ -> :ok
               end
             )

    assert :ok = Recovery.sweep()
    assert {:error, :retries_exhausted} = SnapshotLog.fork(source, target)
    assert {:error, :deleted} = SnapshotLog.next(target, nil)
  end

  test "existing empty logs remain readable after their placement group was sealed" do
    source = base()
    {:ok, _, _} = Streams.create(SnapshotLog.path(source, :updates))

    {:ok, _, _} =
      Streams.create(SnapshotLog.path(source, :index), content_type: "application/json")

    :ok = Streams.seal(source)
    assert {:reset, %{offset: 0, snapshot: nil}} = SnapshotLog.next(source, nil)
    assert {:ok, _} = SnapshotLog.append(source, "a")
    assert {:ok, _} = SnapshotLog.fork(source, other_shard(source))
  end

  test "a publication between selection and protection retries a fenced candidate" do
    source = base()
    target = other_shard(source)
    {:ok, at} = SnapshotLog.append(source, "a")

    assert {:ok, %{offset: ^at, snapshot_offset: ^at}} =
             SnapshotLog.fork_internal(source, target,
               after_step: fn
                 :selection -> :ok = SnapshotLog.snapshot(source, at, "a")
                 _ -> :ok
               end
             )

    assert load(target) == {"a", [], at}
    {:ok, [%{created_at: created}]} = SnapshotLog.snapshots(target)
    assert created > 0
  end

  test "publication past the selected cutoff retries before fixing capture" do
    source = base()
    target = other_shard(source)
    {:ok, at} = SnapshotLog.append(source, "a")
    parent = self()

    assert {:ok, %{offset: tail, snapshot_offset: tail}} =
             SnapshotLog.fork_internal(source, target,
               after_step: fn
                 :selection ->
                   {:ok, current} = SnapshotLog.tail(source)

                   if current == at do
                     {:ok, newer} = SnapshotLog.append(source, "b")
                     :ok = SnapshotLog.snapshot(source, newer, "ab")
                     send(parent, {:new_tail, newer})
                   end

                 _ ->
                   :ok
               end
             )

    assert_receive {:new_tail, ^tail}, 5_000
    assert load(target) == {"ab", [], tail}
  end

  test "an invocation that expires after selection keeps its original cutoff for retry" do
    source = base()
    target = other_shard(source)
    {:ok, at} = SnapshotLog.append(source, "a")
    parent = self()

    task =
      Task.async(fn ->
        SnapshotLog.fork_internal(source, target,
          timeout: 1_000,
          after_step: fn
            :selection ->
              send(parent, {:selected, self()})

              receive do
                :continue -> :ok
              end

            _ ->
              :ok
          end
        )
      end)

    assert_receive {:selected, pid}, 5_000
    Process.send_after(pid, :continue, 1_000)
    assert {:error, :timeout} = Task.await(task)
    {:ok, _} = SnapshotLog.append(source, "later")
    assert {:ok, %{offset: ^at}} = SnapshotLog.fork(source, target)
    assert load(target) == {nil, ["a"], at}
  end

  test "recovery finishes an ordinary initialization through the control lifecycle" do
    store = Store.new(base(), [])
    deadline = Control.deadline(5_000)

    {:ok, :created, %{state: :initializing} = control} =
      Control.claim(store, %{"kind" => "initialization", "status" => "pending"}, [], deadline)

    {:ok, _, _} = Streams.create_guarded(Store.path(store, :updates), Control.guard(control), [])

    assert :ok = Recovery.sweep()
    assert {:ok, %{state: :ordinary}} = Control.read(store, deadline)
    assert {:reset, %{snapshot: nil, offset: 0}} = SnapshotLog.next(store.base, nil)
    assert :ok = SnapshotLog.delete(store.base)
    assert {:ok, %{state: :ordinary_deleted}} = Control.read(store, deadline)
    assert {:error, :deleted} = SnapshotLog.next(store.base, nil)
  end

  test "a fork from a source that is still initializing stays pending for a retry" do
    source = Store.new(base(), [])
    target = other_shard(source.base)
    deadline = Control.deadline(5_000)

    {:ok, :created, _} =
      Control.claim(source, %{"kind" => "initialization", "status" => "pending"}, [], deadline)

    assert {:error, :busy} = SnapshotLog.fork(source.base, target)
    assert :ok = Recovery.sweep()
    assert {:ok, %{offset: 0}} = SnapshotLog.fork(source.base, target)
  end

  test "an append finishes an initialization whose claimant stopped" do
    store = Store.new(base(), [])
    deadline = Control.deadline(5_000)

    {:ok, :created, %{state: :initializing}} =
      Control.claim(store, %{"kind" => "initialization", "status" => "pending"}, [], deadline)

    assert {:error, :busy} = SnapshotLog.tail(store.base)
    assert {:ok, _} = SnapshotLog.append(store.base, "a")
    assert {:ok, %{state: :ordinary}} = Control.read(store, deadline)
  end

  test "recovery removes owned components after ordinary initialization is cancelled" do
    target = Store.new(base(), [])
    deadline = Control.deadline(5_000)

    {:ok, :created, control} =
      Control.claim(target, %{"kind" => "initialization", "status" => "pending"}, [], deadline)

    assert {:error, :busy} = SnapshotLog.tail(target.base)
    source = base()
    {:ok, _} = SnapshotLog.append(source, "a")
    assert {:error, :exists} = SnapshotLog.fork(source, target.base)
    {:ok, _, _} = Streams.create_guarded(Store.path(target, :updates), Control.guard(control), [])
    {:ok, _} = Initialization.cancel_pending(target, control, deadline)
    assert :ok = Recovery.sweep()
    assert {:error, :not_found} = Streams.head(Store.path(target, :updates))
    assert {:error, :deleted} = SnapshotLog.next(target.base, nil)
    assert {:error, :sealed} = Streams.create(target.base <> "/.late")
  end

  test "deleting a legacy log during initialization removes its unowned components" do
    legacy = Store.new(base(), [])
    updates = Store.path(legacy, :updates)
    {:ok, _, _} = Streams.create(updates, body: "legacy")
    {:ok, _, _} = Streams.create(Store.path(legacy, :index), content_type: "application/json")
    deadline = Control.deadline(5_000)

    {:ok, :created, _} =
      Control.claim(legacy, %{"kind" => "initialization", "status" => "pending"}, [], deadline)

    assert :ok = SnapshotLog.delete(legacy.base)
    assert {:error, :not_found} = Streams.head(updates)
    assert {:error, :deleted} = SnapshotLog.next(legacy.base, nil)
  end

  test "initialization rollback preserves pre-existing streams with incompatible content types" do
    legacy = Store.new(base(), [])
    updates = Store.path(legacy, :updates)
    {:ok, _, _} = Streams.create(updates, body: "foreign", content_type: "text/plain")
    assert {:error, :deleted} = SnapshotLog.next(legacy.base, nil)
    assert :ok = Recovery.sweep()
    assert {:ok, %{messages: [{_, "foreign"}]}} = Streams.read(updates, :start)
    assert {:ok, _, _} = Streams.create(legacy.base <> "/.foreign-child")
    assert :ok = SnapshotLog.delete(legacy.base)
    assert {:error, :not_found} = Streams.head(updates)
  end

  test "cleanup completion is idempotent when a repair worker wins the final transition" do
    source = base()
    target = other_shard(source)
    {:ok, _} = SnapshotLog.append(source, "a")

    assert catch_throw(
             SnapshotLog.fork_internal(source, target,
               after_step: fn
                 :updates -> throw(:paused)
                 _ -> :ok
               end
             )
           ) == :paused

    store = Store.new(target, [])
    deadline = Control.deadline(5_000)
    {:ok, control} = Control.read(store, deadline)
    {:ok, cancelled} = Fork.cancel(store, control, :busy, deadline)
    assert :ok = Fork.cleanup(store, cancelled, deadline)
    assert :ok = Fork.cleanup(store, cancelled, deadline)
    assert {:error, :busy} = SnapshotLog.fork(source, target)
  end

  defp other_shard(source) do
    source_shard = Cluster.shard_for(source)
    Enum.find(Stream.repeatedly(&base/0), &(Cluster.shard_for(&1) != source_shard))
  end
end
