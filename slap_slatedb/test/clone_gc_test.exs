defmodule Slap.SlateDB.CloneGCTest do
  use ExUnit.Case, async: true

  alias Slap.SlateDB
  alias Slap.SlateDB.{Admin, ObjectStore, Reader}

  @moduletag :capture_log
  @moduletag timeout: 180_000

  @settings %{
    garbage_collector_options: nil,
    flush_interval: nil,
    manifest_poll_interval: "20ms",
    max_wal_flushes_before_l0_flush: 1_000_000,
    compactor_options: %{
      poll_interval: "20ms",
      commit_compacted_interval: "20ms",
      checkpoint_lifetime: "1s",
      enable_trivial_move: false,
      max_concurrent_compactions: 1,
      scheduler_options: %{
        min_compaction_sources: "2",
        include_size_threshold: "1000000"
      },
      worker: %{compactions_poll_interval: "20ms", max_concurrent_compactions: 1}
    }
  }

  # Zero ages are confined to closed, quiescent databases. Fence objects must
  # survive; both tested backends support conditional boundary updates.
  @directory_gc %{interval: nil, min_age: "0s", dry_run: false}
  @gc %{
    manifest_options: @directory_gc,
    wal_options: @directory_gc,
    wal_fence_options: nil,
    compacted_options: @directory_gc,
    compactions_options: @directory_gc,
    detach_options: %{interval: nil},
    boundary_files_enabled: true,
    object_store_max_retries: 0
  }

  for backend <- [:local, :s3] do
    describe "#{backend}" do
      @describetag store_backend: backend
      if backend == :s3, do: @describetag(:s3)

      setup :store

      test "a retained checkpoint protects an obsolete SST until deletion", f do
        db = open(f, "a")
        old_map = generation(db, %{}, 1)
        [protected] = ssts(f, "a")
        checkpoint = checkpoint(db)
        {current, inputs} = advance(f, "a", db, old_map, 2)
        close(db)
        f = Map.put(f, :expected, %{"a" => current, checkpoint => old_map})

        collect(f, ["a"], %{"a" => inputs})
        assert protected in ssts(f, "a")
        verify(f, "a", current)
        verify(f, "a", old_map, checkpoint)

        assert :ok = Admin.delete_checkpoint(admin(f, "a"), checkpoint)
        collect(f, ["a"], %{"a" => [protected]})
        verify(f, "a", current)
        assert_missing_checkpoint(f, "a", checkpoint)
      end

      test "an explicit clone survives deleting its selection checkpoint", f do
        f = f |> clone_source() |> explicit_clone()
        assert :ok = Admin.delete_checkpoint(admin(f, "a"), f.selection)
        assert_clone_retained(f)
      end

      test "an explicit clone survives expiry and expired IDs reject new operations", f do
        f = f |> clone_source() |> explicit_clone()
        assert :ok = Admin.refresh_checkpoint(admin(f, "a"), f.selection, 0)
        assert_clone_retained(f)
      end

      test "an explicit clone copies durable WAL data before source collection", f do
        f = clone_source(f)
        {selected, wal} = wal_generation(f, f.writer, f.selected)
        f = explicit_clone(%{f | selected: selected})

        for key <- wal do
          assert {:ok, {body, _version}} = ObjectStore.get(f.objects, key)

          assert {:ok, {^body, _version}} =
                   ObjectStore.get(f.objects, "b/wal/" <> Path.basename(key))
        end

        assert :ok = Admin.delete_checkpoint(admin(f, "a"), f.selection)
        assert_clone_retained(f)
      end

      test "an implicit clone survives temporary selection checkpoint removal", f do
        db = open(f, "a")
        selected = generation(db, %{}, 1)
        assert checkpoints(f, "a") == []
        assert :ok = Admin.clone(admin(f, "a"), "b")
        {[retention], [temporary]} = Enum.split_with(checkpoints(f, "a"), &is_nil(&1.expires_at))
        assert temporary.manifest_id == retention.manifest_id
        assert temporary.id != retention.id
        assert :ok = Admin.refresh_checkpoint(admin(f, "a"), temporary.id, 0)

        child = open(f, "b")
        {child_map, child_inputs} = advance(f, "b", child, selected, 10)
        {current, inputs} = advance(f, "a", db, selected, 2)
        close(child)
        close(db)
        f = Map.put(f, :expected, %{"a" => current, "b" => child_map})
        collect(f, ["b", "a"], %{"a" => inputs, "b" => child_inputs})
        refute checkpoint_present?(f, "a", temporary.id)
        verify(f, "a", current)
        verify(f, "b", child_map)
      end

      test "four generations of clones retain dependencies through intermediate ancestors", f do
        a = open(f, "a")
        initial = generation(a, %{}, 1)
        [inherited] = ssts(f, "a")

        {databases, inputs} =
          Enum.reduce(
            [{"a", "b", 10}, {"b", "c", 20}, {"c", "d", 30}],
            {%{"a" => {a, initial}}, %{}},
            fn {parent, child, gen}, {databases, inputs} ->
              {source, selected} = Map.fetch!(databases, parent)
              selection = checkpoint(source)
              assert :ok = Admin.clone(admin(f, parent), child, checkpoint: selection)
              assert :ok = Admin.delete_checkpoint(admin(f, parent), selection)
              db = open(f, child)
              child_map = generation(db, selected, gen, :wal)

              {advanced, inputs} =
                Enum.reduce(databases, {%{}, inputs}, fn {path, {writer, expected}},
                                                         {updated, inputs} ->
                  {next, candidates} = advance(f, path, writer, expected, gen + 1)
                  inputs = Map.update(inputs, path, candidates, &(candidates ++ &1))
                  {Map.put(updated, path, {writer, next}), inputs}
                end)

              {Map.put(advanced, child, {db, child_map}), inputs}
            end
          )

        assert ssts(f, "d") == []
        Enum.each(databases, fn {_path, {db, _expected}} -> close(db) end)

        f =
          Map.put(
            f,
            :expected,
            Map.new(databases, fn {path, {_db, expected}} -> {path, expected} end)
          )

        collect(f, ["d", "c", "b", "a"], inputs)
        assert inherited in ssts(f, "a")
        assert Enum.any?(checkpoints(f, "a"), &is_nil(&1.expires_at))
        Enum.each(databases, fn {path, {_db, expected}} -> verify(f, path, expected) end)
      end

      test "a clone checkpoint delays detach until its inherited SSTs are unpinned", f do
        source = open(f, "a")
        selected = generation(source, %{}, 1)
        [inherited] = ssts(f, "a")
        selection = checkpoint(source)
        assert :ok = Admin.clone(admin(f, "a"), "b", checkpoint: selection)
        [retention] = Enum.reject(checkpoints(f, "a"), &(&1.id == selection))
        assert retention.expires_at == nil
        assert :ok = Admin.delete_checkpoint(admin(f, "a"), selection)

        child = open(f, "b")
        held = checkpoint(child)
        {child_map, child_inputs} = advance(f, "b", child, selected, 10)
        {source_map, source_inputs} = advance(f, "a", source, selected, 2)
        close(child)
        close(source)
        f = Map.put(f, :expected, %{"a" => source_map, "b" => child_map, held => selected})

        collect(f, ["b", "a"], %{"a" => source_inputs, "b" => child_inputs})
        assert checkpoint_present?(f, "a", retention.id)
        assert inherited in ssts(f, "a")
        verify(f, "b", selected, held)
        verify(f, "b", child_map)
        verify(f, "a", source_map)

        assert :ok = Admin.delete_checkpoint(admin(f, "b"), held)
        collect(f, ["b", "a"], %{"a" => [inherited]})
        refute checkpoint_present?(f, "a", retention.id)
        verify(f, "a", source_map)
        verify(f, "b", child_map)
      end

      test "a selected checkpoint excludes source writes before and after cloning", f do
        f = clone_source(f)
        selection = checkpoint(f.writer)
        before_clone = generation(f.writer, f.selected, 100, :wal)
        assert :ok = Admin.clone(admin(f, "a"), "b", checkpoint: selection)
        written = generation(f.writer, before_clone, 101, :wal)

        assert :ok = Admin.delete_checkpoint(admin(f, "a"), selection)
        assert_captured_clone(f, written)
      end

      # --include overrides :s3 exclusions, so stress tags must name the backend.
      @tag stress: backend
      test "cloning a selected checkpoint while the source writer advances", f do
        f = clone_source(f)
        selection = checkpoint(f.writer)
        source = admin(f, "a")
        supervisor = start_supervised!(Task.Supervisor)
        owner = self()

        writer =
          Task.Supervisor.async_nolink(supervisor, fn ->
            first = generation(f.writer, f.selected, 100, :wal)
            send(owner, :writer_started)
            write_until_stopped(f.writer, first, 101, [])
          end)

        assert_receive :writer_started, 5_000
        started = System.monotonic_time()

        finished =
          try do
            assert :ok = Admin.clone(source, "b", checkpoint: selection)
            System.monotonic_time()
          after
            send(writer.pid, :stop)
            send(owner, {:writer_stopped, Task.await(writer, 30_000)})
          end

        assert_receive {:writer_stopped, {written, completed}}

        assert Enum.any?(completed, &(&1 > started and &1 < finished)),
               "no source write completed during clone: #{inspect(%{started: started, finished: finished, completed: completed})}"

        assert :ok = Admin.delete_checkpoint(admin(f, "a"), selection)
        assert_captured_clone(f, written)
      end
    end
  end

  defp store(%{store_backend: :local}) do
    dir = Path.join(System.tmp_dir!(), "slap-clone-gc-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    fixture({:local, dir})
  end

  defp store(%{store_backend: :s3}) do
    prefix = "clone-gc-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}"
    bucket = System.get_env("SLAP_TEST_S3_BUCKET", "slatedb-test")

    store =
      {:url, "s3://#{bucket}/#{prefix}",
       [
         aws_endpoint: System.fetch_env!("SLAP_TEST_S3_ENDPOINT"),
         aws_allow_http: "true",
         aws_region: System.get_env("SLAP_TEST_S3_REGION", "us-east-1"),
         aws_access_key_id: System.get_env("SLAP_TEST_S3_KEY", "rustfsadmin"),
         aws_secret_access_key: System.get_env("SLAP_TEST_S3_SECRET", "rustfsadmin")
       ]}

    f = fixture(store)

    on_exit(fn ->
      Enum.each(inventory(f), fn key -> :ok = ObjectStore.delete(f.objects, key) end)
    end)

    f
  end

  defp fixture(store) do
    {:ok, objects} = ObjectStore.open("", store: store)
    %{store: store, objects: objects}
  end

  defp open(f, path) do
    assert {:ok, db} = SlateDB.open(path, store: f.store, settings: @settings)
    on_exit(fn -> SlateDB.close(db) end)
    db
  end

  defp close(db), do: assert(:ok = SlateDB.close(db))

  defp admin(f, path) do
    assert {:ok, admin} = Admin.open(path, store: f.store)
    admin
  end

  defp checkpoint(db, lifetime \\ nil) do
    assert {:ok, %{id: id}} =
             SlateDB.create_checkpoint(db, scope: :durable, lifetime: lifetime)

    id
  end

  defp checkpoints(f, path) do
    assert {:ok, checkpoints} = Admin.list_checkpoints(admin(f, path))
    checkpoints
  end

  defp checkpoint_present?(f, path, id), do: Enum.any?(checkpoints(f, path), &(&1.id == id))

  defp generation(db, expected, gen, flush \\ :memtable) do
    puts = %{
      "shared" => "value/#{gen}",
      "new/#{gen}" => "new/#{gen}",
      "drop/#{gen}" => "old/#{gen}"
    }

    deleted = Enum.filter(Map.keys(expected), &String.starts_with?(&1, "drop/"))

    ops =
      Enum.map(deleted, &{:delete, &1}) ++
        Enum.map(puts, fn {key, value} -> {:put, key, value} end)

    assert {:ok, _seq} = SlateDB.write(db, ops)
    assert :ok = SlateDB.flush(db, type: flush)
    expected |> Map.drop(deleted) |> Map.merge(puts)
  end

  defp advance(f, path, db, expected, gen) do
    l0_count = SlateDB.stats(db).l0_sst_count
    assert l0_count in [0, 1]

    prepared =
      case l0_count do
        0 -> expected
        1 -> generation(db, expected, gen)
      end

    await_compaction(f, path, db, prepared)

    # SlateDB schedules L0 and sorted runs separately. With at least two
    # inputs required, the first new L0 cannot compact before the second flush.
    before = ssts(f, path)
    next = generation(db, prepared, gen + 1)
    [obsolete] = ssts(f, path) -- before
    current = generation(db, next, gen + 2)
    await_compaction(f, path, db, current)
    {current, [obsolete]}
  end

  defp await_compaction(f, path, db, expected) do
    f = Map.merge(f, %{writer: db, expected: expected})

    wait(f, [path], "L0 inputs rewritten into sorted runs", fn ->
      stats = SlateDB.stats(db)

      stats.l0_sst_count == 0 and stats.sorted_run_count == 1 and
        metric(db, "slatedb.compactor.running_compactions") == 0
    end)
  end

  defp metric(db, name) do
    for %{name: ^name, value: value} <- SlateDB.metrics(db), reduce: 0 do
      total -> total + value
    end
  end

  defp inventory(f) do
    assert {:ok, keys} = ObjectStore.list(f.objects, "")
    Enum.sort(keys)
  end

  defp ssts(f, path) do
    Enum.filter(
      inventory(f),
      &(String.starts_with?(&1, path <> "/compacted/") and String.ends_with?(&1, ".sst"))
    )
  end

  defp collect(f, paths, witnesses) do
    assert map_size(witnesses) > 0
    before = inventory(f)

    for {path, files} <- witnesses do
      assert path in paths
      assert files != []
      assert Enum.all?(files, &(&1 in before and String.starts_with?(&1, path <> "/compacted/")))
    end

    f = Map.merge(f, %{before_gc: before, gc_witnesses: witnesses})

    wait(f, paths, "collection of obsolete SSTs in each database: #{inspect(witnesses)}", fn ->
      Enum.each(paths, fn path -> assert :ok = Admin.run_gc(admin(f, path), settings: @gc) end)
      remaining = inventory(f)
      Enum.all?(witnesses, fn {_path, files} -> Enum.all?(files, &(&1 not in remaining)) end)
    end)
  end

  defp verify(f, path, expected, checkpoint \\ nil) do
    assert {:ok, reader} =
             Reader.open(path,
               store: f.store,
               mode: :latest,
               cache: :disabled,
               checkpoint: checkpoint
             )

    try do
      assert reader |> Reader.scan() |> Enum.to_list() |> Enum.sort() == Enum.sort(expected),
             "cold contents of #{path}, checkpoint #{inspect(checkpoint)}"
    after
      assert :ok = Reader.close(reader)
      :erlang.garbage_collect()
    end
  end

  defp assert_missing_checkpoint(f, path, id) do
    result = Reader.open(path, store: f.store, checkpoint: id, cache: :disabled)

    case result do
      {:ok, reader} -> assert :ok = Reader.close(reader)
      {:error, _error} -> :ok
    end

    assert {:error, %SlateDB.Error{kind: :data, message: message}} = result

    assert message =~ id
  end

  defp clone_source(f) do
    db = open(f, "a")
    selected = generation(db, %{}, 1)
    inherited = ssts(f, "a")
    Map.merge(f, %{writer: db, selected: selected, inherited: inherited})
  end

  defp explicit_clone(f) do
    selection = checkpoint(f.writer, 60_000)
    assert ssts(f, "a") == f.inherited
    assert :ok = Admin.clone(admin(f, "a"), "b", checkpoint: selection)

    roles = checkpoints(f, "a")
    [retention] = Enum.reject(roles, &(&1.id == selection))
    assert retention.expires_at == nil

    assert retention.manifest_id == Enum.find(roles, &(&1.id == selection)).manifest_id

    Map.merge(f, %{selection: selection, retention: retention})
  end

  defp assert_clone_retained(f) do
    {current, inputs} = advance(f, "a", f.writer, f.selected, 3)
    close(f.writer)

    f =
      f
      |> Map.delete(:writer)
      |> Map.put(:expected, %{"a" => current, "b" => f.selected, f.selection => f.selected})

    collect(f, ["b", "a"], %{"a" => inputs})
    refute checkpoint_present?(f, "a", f.selection)
    assert checkpoint_present?(f, "a", f.retention.id)
    assert Enum.all?(f.inherited, &(&1 in ssts(f, "a")))
    verify(f, "a", current)
    verify(f, "b", f.selected)
    assert_missing_checkpoint(f, "a", f.selection)

    assert {:error, %SlateDB.Error{kind: :data, message: message}} =
             Admin.clone(admin(f, "a"), "rejected", checkpoint: f.selection)

    assert message =~ f.selection
    verify(f, "a", f.selected, f.retention.id)
  end

  defp wal_generation(f, db, selected) do
    before = ssts(f, "a")
    before_wal = inventory(f)
    next = generation(db, selected, 2, :wal)
    assert ssts(f, "a") == before
    assert SlateDB.stats(db).l0_sst_count == 1
    assert SlateDB.durable_seq(db) == SlateDB.last_write_seq(db)

    wal =
      Enum.filter(inventory(f) -- before_wal, fn
        "a/wal/" <> _suffix = key ->
          assert {:ok, {body, _version}} = ObjectStore.get(f.objects, key)
          byte_size(body) > 0

        _key ->
          false
      end)

    assert wal != []
    {next, wal}
  end

  defp assert_captured_clone(f, written) do
    {expected, inputs} = advance(f, "a", f.writer, written, 200)
    close(f.writer)
    f = f |> Map.delete(:writer) |> Map.put(:expected, %{"a" => expected, "b" => f.selected})
    collect(f, ["b", "a"], %{"a" => inputs})
    verify(f, "a", expected)
    verify(f, "b", f.selected)
  end

  defp write_until_stopped(db, expected, gen, completed) do
    receive do
      :stop -> {expected, completed}
    after
      0 ->
        value = "value/#{gen}"
        assert {:ok, _seq} = SlateDB.put(db, "concurrent", value)
        assert :ok = SlateDB.flush(db, type: :wal)
        completed_at = System.monotonic_time()
        next = Map.put(expected, "concurrent", value)
        write_until_stopped(db, next, gen + 1, [completed_at | completed])
    end
  end

  defp diagnostics(f, paths) do
    objects = inventory(f)
    before = Map.get(f, :before_gc, objects)

    %{
      database_paths: paths,
      store: store_name(f.store),
      settings: @settings,
      gc: @gc,
      expected: Map.get(f, :expected),
      compaction: compaction_state(f),
      checkpoints: Map.new(paths, &{&1, checkpoints(f, &1)}),
      objects: objects,
      remaining_gc_witnesses:
        Map.new(Map.get(f, :gc_witnesses, %{}), fn {path, files} ->
          {path, Enum.filter(files, &(&1 in objects))}
        end),
      removed: before -- objects,
      added: objects -- before
    }
  end

  defp store_name({:local, dir}), do: dir
  defp store_name({:url, url, _options}), do: url

  defp compaction_state(%{writer: db}) do
    %{
      stats: SlateDB.stats(db),
      metrics: Enum.filter(SlateDB.metrics(db), &String.contains?(&1.name, "compactor"))
    }
  end

  defp compaction_state(_f), do: :writers_closed

  defp wait(f, paths, reason, predicate) do
    poll(f, paths, reason, predicate, System.monotonic_time(:millisecond) + 30_000)
  end

  defp poll(f, paths, reason, predicate, deadline) do
    cond do
      predicate.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("#{reason}\n#{inspect(diagnostics(f, paths), pretty: true, limit: :infinity)}")

      true ->
        Process.sleep(25)
        poll(f, paths, reason, predicate, deadline)
    end
  end
end
