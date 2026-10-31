defmodule Slap.InspectViewsTest do
  # These views share the built-in named Streams, KV and Files instances.
  use ExUnit.Case, async: false

  alias Slap.{Files, KV, SlateDB, SnapshotLog, Streams}
  alias Slap.Files.Config
  alias Slap.Inspect.Query
  alias Slap.Streams.Store.{Codec, Keys, Read}
  alias Slap.Yjs.Frame

  setup context do
    start_supervised!(
      {Streams.Cluster, store: :memory, shards: 2, child_options: context[:child_options] || []}
    )

    start_supervised!({KV.Cluster, store: :memory, shards: 2})
    assert :ok = Streams.Cluster.await_ready(5000)
    assert :ok = KV.Cluster.await_ready(5000)
    :ok
  end

  test "KV inspection reads opaque bytes without calling partition writers" do
    assert {:ok, version} = KV.put("users", <<255>>, <<0, 255>>)
    {:ok, {:local, ctx}} = KV.Cluster.lookup(KV.Cluster.shard_for("users"))
    {:ok, index} = KV.PartitionWriter.index(ctx, "users")
    [{writer, _}] = Registry.lookup(ctx.registry, {ctx.n, {:partition_writer, index}})
    :ok = :sys.suspend(writer, 5000)
    on_exit(fn -> if Process.alive?(writer), do: :sys.resume(writer, 5000) end)
    assert {:ok, page} = query(KV.Cluster, :kv, partition: "users")

    assert [%{seq: seq, value: <<0, 255>>, decoded: {:ok, %{partition: "users", key: <<255>>}}}] =
             page.rows

    assert version == seq
  end

  test "raw reads preserve storage bytes in KV and Streams clusters" do
    assert {:ok, _} = KV.put("users", "id", "value")
    assert {:ok, _, _} = Streams.create("/raw-neutral", body: "message")

    for cluster <- [KV.Cluster, Streams.Cluster] do
      connection = %{entry: node(), cluster: cluster}

      shard =
        if cluster == KV.Cluster,
          do: KV.Cluster.shard_for("users"),
          else: Streams.Cluster.shard_for("/raw-neutral")

      request = %{view: :raw, shard: shard, budget: 5000, opts: []}
      assert {:ok, page} = Query.execute(connection, request)
      assert page.rows != []
      assert Enum.all?(page.rows, &match?(%{decoded: {:ok, %{kind: :raw}}}, &1))
      row = hd(page.rows)

      assert {:ok, %{row: %{key: key, value: value, decoded: {:ok, %{kind: :raw}}}}} =
               Query.execute(connection, %{request | view: :get, opts: [key: row.key]})

      assert key == row.key
      assert value == row.value
    end
  end

  test "an unassigned shard includes the cluster's owner status" do
    assert {:error, %{reason: :unassigned, shard: 9, status: {:ok, {:ok, status}}}} =
             query(Streams.Cluster, :streams, [], 9)

    assert status.ready?
    assert %{0 => %{status: :running}, 1 => %{status: :running}} = status.shards
  end

  test "files expose inline and object records, and range previews use the configured store" do
    start_supervised!({Files, store: :memory, inline_max_bytes: 4, inline_limit: 8})
    assert {:ok, _} = Files.put({"p", "inline"}, "abc")
    assert {:ok, _} = Files.put({"p", "object"}, "0123456789")
    partition = Config.partition(Config.get(), :record, "p")
    assert {:ok, page} = query(KV.Cluster, :files, partition: partition)

    assert [
             %{decoded: {:ok, %{value: %{body: [:inline, %{bytes: 3}]}}}},
             %{decoded: {:ok, %{value: %{body: [:object, key]}}}}
           ] = page.rows

    assert {:ok, %{object: %{value: "234", value_size: 10, offset: 2}}} =
             query(KV.Cluster, :object, [object_key: key, value_range: {2, 3}], page.shard)
  end

  test "current log control, snapshot index and interrupted snapshots stay distinguishable" do
    base = "/inspect/log"
    assert {:ok, offset} = SnapshotLog.append(base, "entry")
    assert :ok = SnapshotLog.snapshot(base, offset, "snapshot")
    assert {:ok, page} = query(Streams.Cluster, :log, base: base)

    assert %{recognition: %{kind: :snapshot_log, state: :ordinary}, current: %{offset: ^offset}} =
             page.log

    assert {:ok, _, _} =
             Streams.create(SnapshotLog.path(base, {:snapshot, offset + 5}),
               body: "unpublished",
               closed: true
             )

    assert {:ok, refreshed} = query(Streams.Cluster, :log, base: base)
    assert refreshed.log.current.offset == offset
    assert [%{selection: :current}, %{selection: :not_selected}] = refreshed.log.stored_snapshots
  end

  test "log component reads keep the catalog's byte limits and stay within their bounds" do
    base = "/inspect/limited-log"
    assert {:ok, offset} = SnapshotLog.append(base, "entry")
    assert :ok = SnapshotLog.snapshot(base, offset, "snapshot")
    assert {:ok, page} = query(Streams.Cluster, :log, base: base, max_value_bytes: 8)

    for component <- [:control, :index] do
      assert {:ok, %{rows: [_ | _] = rows} = component_page} = Map.fetch!(page.log, component)
      assert Enum.all?(rows, &(byte_size(&1.value) <= 8))
      assert :erlang.external_size(component_page) <= component_page.hard_max_bytes
    end
  end

  test "logical forks honor target trims while retaining a trimmed source's captured rows" do
    assert {:ok, _, _} = Streams.create("/inspect/source", body: "old")
    assert {:ok, %{next_offset: boundary}} = Streams.append("/inspect/source", "kept")
    assert {:ok, _, _} = Streams.create("/inspect/fork", forked_from: "/inspect/source")

    assert {:ok, %{row: %{decoded: {:ok, %{value: %{sid: sid}}}}}} =
             query(Streams.Cluster, :stream, path: "/inspect/source")

    assert {:ok, retention} =
             query(Streams.Cluster, :retention, path: "/inspect/source", sid: sid)

    assert {:ok,
            %{row: %{decoded: {:ok, %{value: %{registration_count: 1, reclaimed_before: 0}}}}}} =
             retention.metadata

    assert :ok = Streams.trim("/inspect/source", boundary)
    assert {:ok, fork} = query(Streams.Cluster, :logical, path: "/inspect/fork")
    assert [%{value: "old"}, %{value: "kept"}] = fork.messages
    assert [%{capture: %{source: %{sid: ^sid}}}] = fork.lineage
    assert :ok = Streams.trim("/inspect/fork", boundary)
    assert {:ok, trimmed} = query(Streams.Cluster, :logical, path: "/inspect/fork")
    assert trimmed.lineage == []
    assert trimmed.messages == []
  end

  test "Yjs structural views include the selected snapshot and update frame sizes" do
    base = "/inspect/yjs"
    assert {:ok, offset} = SnapshotLog.append(base, Frame.frames(["one", <<0, 255>>]))
    assert :ok = SnapshotLog.snapshot(base, offset, "opaque snapshot")
    assert {:ok, _} = SnapshotLog.append(base, Frame.frames(["next"]))
    assert {:ok, page} = query(Streams.Cluster, :yjs, base: base)
    assert page.log.current.offset == offset
    assert [%{interpretation: {:ok, %{frame_count: 1, frame_sizes: [4]}}}] = page.rows
  end

  test "logical pagination visits inherited rows before the fork's own rows" do
    source = "/inspect/paged-source"
    target = "/inspect/paged-fork"
    assert {:ok, _, _} = Streams.create(source)
    values = Enum.map(1..55, &Integer.to_string/1)
    for value <- values, do: assert({:ok, _} = Streams.append(source, value))
    assert {:ok, _, _} = Streams.create(target, forked_from: source)
    assert {:ok, _} = Streams.append(target, "own")
    opts = [path: target, limit: 50]
    assert {:ok, first} = query(Streams.Cluster, :logical, opts)
    assert Enum.map(first.messages, & &1.value) == Enum.take(values, 50)
    assert is_binary(first.cursor)

    assert {:ok, second} =
             query(Streams.Cluster, :logical, Keyword.put(opts, :cursor, first.cursor))

    assert Enum.map(second.messages, & &1.value) == Enum.drop(values, 50)
    assert is_binary(second.cursor)

    assert {:ok, third} =
             query(Streams.Cluster, :logical, Keyword.put(opts, :cursor, second.cursor))

    assert [%{value: "own", complete: true}] = third.messages
    assert third.cursor == nil
  end

  test "logical pagination preserves oversized key diagnostics and continues through forks" do
    source = "/inspect/oversized-source"
    target = "/inspect/oversized-fork"
    assert {:ok, _, _} = Streams.create(source, body: "first")
    assert {:ok, _} = Streams.append(source, "second")
    assert {:ok, _, _} = Streams.create(target, forked_from: source, body: "own")

    for maximum <- [1, 18] do
      assert {:ok, first} =
               query(Streams.Cluster, :logical, path: target, max_key_bytes: maximum)

      assert [%{oversized_key: true, diagnostic: :oversized_keys_with_prefix_skipped}] =
               first.rows

      assert [%{status: :invalid}] = first.fragments

      expected = if maximum == 1, do: ["own"], else: ["second", "own"]

      cursor =
        Enum.reduce(expected, first.cursor, fn value, cursor ->
          assert is_binary(cursor)
          assert {:ok, page} = query(Streams.Cluster, :logical, path: target, cursor: cursor)
          assert [%{value: ^value, complete: true}] = page.messages
          page.cursor
        end)

      assert cursor == nil
    end
  end

  test "logical traversal accepts all eight source links allowed by default" do
    assert_fork_chain(8)
  end

  @tag child_options: [max_fork_depth: 16]
  test "logical traversal follows the server's configured reference depth" do
    assert_fork_chain(16)
  end

  test "logical traversal respects reference bounds and cycles" do
    root = "/inspect/depth-bound/root"
    middle = "/inspect/depth-bound/middle"
    target = "/inspect/depth-bound/target"
    assert {:ok, _, _} = Streams.create(root, body: "inherited")
    assert {:ok, _, _} = Streams.create(middle, forked_from: root)
    assert {:ok, _, _} = Streams.create(target, forked_from: middle)
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(Streams.Cluster.shard_for(target))

    for {pid, _} <- Registry.lookup(ctx.registry, {ctx.n, {:stream, target}}),
        do: GenServer.stop(pid, :normal, 5000)

    {:ok, meta} = Read.get_meta(ctx.db, target)
    limited = put_in(meta.creation.capture.depth, 1)

    assert {:ok, _} =
             SlateDB.put(ctx.db, Keys.meta(target), Codec.encode(limited), await_durable: true)

    assert {:error, :lineage_depth_limit} = query(Streams.Cluster, :logical, path: target)

    source = %{meta.creation.capture.source | path: target, shard: ctx.n, sid: meta.sid}
    cyclic = put_in(meta.creation.capture.source, source)

    assert {:ok, _} =
             SlateDB.put(ctx.db, Keys.meta(target), Codec.encode(cyclic), await_durable: true)

    assert {:error, :lineage_cycle} = query(Streams.Cluster, :logical, path: target)
  end

  test "forked Yjs logs expose inherited frame structure and paginate to own updates" do
    source = "/inspect/yjs-source"
    target = "/inspect/yjs-fork"
    assert {:ok, _} = SnapshotLog.append(source, Frame.frames(["inherited"]))
    assert {:ok, _} = SnapshotLog.fork(source, target)
    assert {:ok, _} = SnapshotLog.append(target, Frame.frames(["own"]))
    opts = [base: target, limit: 1]
    assert {:ok, first} = query(Streams.Cluster, :yjs, opts)
    assert [%{interpretation: {:ok, %{frame_sizes: [9]}}}] = first.rows
    assert [%{complete: true}] = first.messages
    assert is_binary(first.cursor)
    assert {:ok, second} = query(Streams.Cluster, :yjs, Keyword.put(opts, :cursor, first.cursor))
    assert [%{interpretation: {:ok, %{frame_sizes: [3]}}}] = second.rows
  end

  test "complete messages at page boundaries retain their interpretations" do
    base = "/inspect/boundaries"
    assert {:ok, _} = SnapshotLog.append(base, Frame.frames(["first"]))
    assert {:ok, _} = SnapshotLog.append(base, Frame.frames(["second"]))
    assert {:ok, page} = query(Streams.Cluster, :yjs, base: base, limit: 1)
    assert [%{complete: true, diagnostic: nil}] = page.messages
    assert [%{interpretation: {:ok, %{frame_sizes: [5]}}}] = page.rows
  end

  test "nonboundary captures preserve physical messages and logical continuation offsets" do
    source = "/inspect/nonboundary-source"
    target = "/inspect/nonboundary-fork"
    assert {:ok, _, _} = Streams.create(source, body: "hello")
    assert {:ok, _} = Streams.append(source, "later")
    assert {:ok, _, _} = Streams.create(target, forked_from: source, fork_offset: 3, body: "own")
    assert {:ok, %{messages: [{0, "hello"}, {3, "own"}]}} = Streams.read(target, :start)

    for opts <- [[path: target], [path: target, limit: 1]] do
      assert {:ok, first} = query(Streams.Cluster, :logical, opts)
      assert [%{value: "hello", complete: true, next_offset: 3, diagnostic: nil}] = first.messages

      assert {:ok, second} =
               query(Streams.Cluster, :logical, Keyword.put(opts, :cursor, first.cursor))

      assert [%{value: "own", complete: true, next_offset: 10}] = second.messages
    end

    updates = "/inspect/nonboundary-yjs"
    assert {:ok, _, _} = Streams.create(updates, body: Frame.frames(["hello"]))

    assert {:ok, _, _} =
             Streams.create(updates <> "-fork", forked_from: updates, fork_offset: 3)

    assert {:ok, page} = query(Streams.Cluster, :logical, path: updates <> "-fork")

    assert {:ok, structural} =
             query(Streams.Cluster, :yjs, [sid: page.sid, until: 3], page.shard)

    assert [%{interpretation: {:ok, %{frame_sizes: [5]}}}] = structural.rows
    assert [%{complete: true, next_offset: 3}] = structural.messages
  end

  test "inline bodies do not consume the decoded metadata allowance" do
    start_supervised!({Files, store: :memory})
    body = String.duplicate("x", 5000)

    assert {:ok, _} =
             Files.put({"p", "large-inline"}, body,
               content_type: "text/plain",
               metadata: %{"label" => "notes"}
             )

    partition = Config.partition(Config.get(), :record, "p")
    assert {:ok, page} = query(KV.Cluster, :files, partition: partition)

    assert [
             %{
               decoded:
                 {:ok,
                  %{
                    value: %{
                      size: 5000,
                      sha256: hash,
                      content_type: "text/plain",
                      storage: :inline,
                      metadata: %{"label" => "notes"}
                    }
                  }}
             }
           ] = page.rows

    assert hash == :crypto.hash(:sha256, body)
    assert :erlang.external_size(hd(page.rows).decoded) < 4096
  end

  test "nested logical cursors advance through each source and target without repeating rows" do
    assert {:ok, _, _} = Streams.create("/inspect/ancestor", body: "a")
    assert {:ok, _, _} = Streams.create("/inspect/middle", forked_from: "/inspect/ancestor")
    assert {:ok, _} = Streams.append("/inspect/middle", "b")
    assert {:ok, _, _} = Streams.create("/inspect/descendant", forked_from: "/inspect/middle")
    assert {:ok, _} = Streams.append("/inspect/descendant", "c")

    {pages, cursor} =
      Enum.map_reduce(1..3, nil, fn _, cursor ->
        opts = [path: "/inspect/descendant", limit: 1]
        opts = if cursor, do: Keyword.put(opts, :cursor, cursor), else: opts
        assert {:ok, page} = query(Streams.Cluster, :logical, opts)
        {page, page.cursor}
      end)

    assert Enum.flat_map(pages, & &1.messages) |> Enum.map(& &1.value) == ["a", "b", "c"]
    assert Enum.map(pages, &length(&1.lineage)) == [2, 1, 0]
    assert cursor == nil
  end

  test "pages cut within a multipart Yjs message remain incomplete" do
    base = "/inspect/multipart"
    assert {:ok, _} = SnapshotLog.append(base, Frame.frames([String.duplicate("x", 256 * 1024)]))

    for opts <- [[base: base], [base: base, until: 3]] do
      assert {:ok, page} = query(Streams.Cluster, :yjs, Keyword.put(opts, :limit, 1))
      assert [%{complete: false}] = page.messages
      assert [%{interpretation: {:error, :incomplete_message_parts}}] = page.rows
      assert {:ok, complete} = query(Streams.Cluster, :yjs, opts)
      assert [%{complete: true}] = complete.messages
      assert [%{interpretation: {:ok, %{frame_count: 1}}}, _] = complete.rows
    end
  end

  test "snapshot index entries at page boundaries remain decoded" do
    base = "/inspect/index-boundaries"
    assert {:ok, first} = SnapshotLog.append(base, "a")
    assert :ok = SnapshotLog.snapshot(base, first, "a")
    index = SnapshotLog.path(base, :index)
    assert {:ok, %{messages: [{_, body}]}} = Streams.read(index, :start)
    assert {:ok, _} = Streams.append(index, body)
    assert {:ok, page} = query(Streams.Cluster, :log, base: base, component: :index, limit: 1)
    assert [%{interpretation: {:ok, %{offset: ^first}}}] = page.rows
    assert [%{complete: true}] = page.messages
    assert is_binary(page.cursor)
  end

  test "legacy recognition distinguishes complete and partial indexes" do
    for count <- [100, 101] do
      base = "/inspect/legacy-#{count}"
      assert {:ok, _, _} = Streams.create(SnapshotLog.path(base, :updates))
      assert {:ok, _, _} = Streams.create(SnapshotLog.path(base, :index))
      body = ~s({"snapshotOffset":"0000000000000000_0000000000000012","createdAt":5})

      for _ <- 1..count,
          do: assert({:ok, _} = Streams.append(SnapshotLog.path(base, :index), body))

      assert {:ok, %{log: log}} = query(Streams.Cluster, :log, base: base)
      assert log.components.control == nil

      if count == 101 do
        assert log.recognition == %{kind: :snapshot_log, legacy: true, index: :partial}
        assert log.current == %{diagnostic: :index_incomplete}
      else
        assert log.recognition == %{kind: :snapshot_log, legacy: true}
        assert log.current.offset == 12
      end
    end
  end

  defp assert_fork_chain(depth) do
    root = "/inspect/depth-#{depth}/0"
    assert {:ok, _, _} = Streams.create(root, body: "inherited")

    target =
      Enum.reduce(1..depth, root, fn n, source ->
        path = "/inspect/depth-#{depth}/#{n}"
        assert {:ok, _, _} = Streams.create(path, forked_from: source)
        path
      end)

    assert {:ok, page} = query(Streams.Cluster, :logical, path: target)
    assert [%{value: "inherited", complete: true}] = page.messages
    assert Enum.map(page.lineage, & &1.capture.depth) == Enum.to_list(depth..1)
  end

  defp query(cluster, view, opts, shard \\ nil) do
    Query.execute(%{entry: node(), cluster: cluster}, %{
      view: view,
      opts: opts,
      shard: shard,
      budget: 5000
    })
  end
end
