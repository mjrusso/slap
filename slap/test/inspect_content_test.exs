defmodule Slap.InspectContentTest do
  # The inspection calls share the built-in Streams cluster and node admission.
  use ExUnit.Case, async: false

  alias Slap.Inspect.{Capture, Query}
  alias Slap.{SlateDB, SnapshotLog, Streams}
  alias Slap.Streams.Store.Keys
  alias Slap.Yjs.{Docs, DocServer, Frame, Store}

  defmodule Server do
    @moduledoc false
    use Slap.Yjs.DocServer, restart: :temporary
  end

  setup do
    start_supervised!({Streams.Cluster, store: :memory, shards: 2})
    assert :ok = Streams.Cluster.await_ready(5000)
    :ok
  end

  test "current reconstruction combines a selected snapshot and later updates" do
    base = "/content/current"
    doc = Yex.Doc.new()
    first = append(doc, base, "first")
    assert :ok = SnapshotLog.snapshot(base, first, Yex.encode_state_as_update!(doc))
    tail = append(doc, base, " second")
    assert {:ok, result} = content(base)
    assert text(result) == "first second"
    assert result.coverage == %{from: first, until: tail, snapshot_offset: first, exact: false}
    assert result.pending == %{updates: 0, deletes: 0}

    assert {:error, {:history_unavailable, ^first}} = content(base, at_offset: tail)
    assert {:ok, %{encoded_bytes: size}} = content(base, preflight: true)
    assert size > 0
  end

  test "reconstruction agrees with a document server on reordered stored updates" do
    start_supervised!(Docs)
    doc_id = {"inspect", "reordered"}
    a = Yex.Doc.new()
    :ok = Yex.Text.insert(Yex.Doc.get_text(a, "text"), 0, "x")
    x = Yex.encode_state_as_update!(a)
    {:ok, clock} = Yex.encode_state_vector(a)
    :ok = Yex.Text.insert(Yex.Doc.get_text(a, "other"), 0, "o")
    o = Yex.encode_state_as_update!(a, clock)
    b = Yex.Doc.new()
    :ok = Yex.apply_update(b, x)
    {:ok, clock} = Yex.encode_state_vector(b)
    :ok = Yex.Text.insert(Yex.Doc.get_text(b, "text"), 1, "p")
    p = Yex.encode_state_as_update!(b, clock)
    assert {:ok, _} = Store.append(doc_id, Frame.frames([o, p, x]))
    assert {:ok, result} = content(Store.base(doc_id))
    server = start_supervised!({Server, doc_id: doc_id, compact_on_stop: false})
    assert :ok = DocServer.subscribe(server, self(), 5000)
    assert {:ok, loaded} = DocServer.doc(server, 5000)
    assert text(result) == Yex.Text.to_string(Yex.Doc.get_text(loaded, "text"))
    assert text(result) == "xp"
    assert {:ok, pending} = Yex.Doc.get_pending_update(loaded)
    assert result.pending.updates == byte_size(pending || <<>>)
  end

  test "history uses complete entries and excludes later state" do
    base = "/content/history"
    doc = Yex.Doc.new()
    first = append(doc, base, "a")
    tail = append(doc, base, "b")
    assert {:ok, history} = content(base, at_offset: first)
    assert text(history) == "a"
    assert history.coverage.exact
    assert is_nil(history.coverage.snapshot_offset)
    assert {:ok, current} = content(base, at_offset: tail)
    assert text(current) == "ab"
    assert {:error, :not_an_entry_boundary} = content(base, at_offset: 3)
  end

  test "exact history remains readable when the current snapshot is missing" do
    base = "/content/history-missing-snapshot"
    doc = Yex.Doc.new()
    first = append(doc, base, "a")
    snapshot = Yex.encode_state_as_update!(doc)
    tail = append(doc, base, "b")

    assert catch_throw(
             SnapshotLog.snapshot_internal(base, first, snapshot,
               after_step: fn
                 :index -> throw(:published)
                 _step -> :ok
               end
             )
           ) == :published

    assert :ok = Streams.delete(SnapshotLog.path(base, {:snapshot, first}))
    assert {:ok, history} = content(base, at_offset: first)
    assert text(history) == "a"
    assert history.coverage == %{from: 0, until: first, snapshot_offset: nil, exact: true}
    assert {:ok, later} = content(base, at_offset: tail)
    assert text(later) == "ab"
    assert {:ok, %{encoded_bytes: ^tail}} = content(base, at_offset: tail, preflight: true)
    assert :ok = Streams.trim(SnapshotLog.path(base, :updates), first)
    assert {:error, {:history_unavailable, ^first}} = content(base, at_offset: tail)
  end

  test "cross-shard forks retain complete history after their source is trimmed" do
    source = "/content/source"
    source_shard = Streams.Cluster.shard_for(Streams.placement_key(source))

    target =
      Enum.find_value(1..100, fn n ->
        path = "/content/fork-#{n}"
        if Streams.Cluster.shard_for(Streams.placement_key(path)) != source_shard, do: path
      end)

    doc = Yex.Doc.new()
    captured = append(doc, source, "inherited")
    assert {:ok, _} = SnapshotLog.fork(source, target)
    assert :ok = SnapshotLog.snapshot(source, captured, Yex.encode_state_as_update!(doc))
    tail = append(doc, target, " own")
    assert {:ok, result} = content(target, at_offset: tail)
    assert text(result) == "inherited own"
    assert {:ok, current} = content(target)
    assert text(current) == "inherited own"
    assert :ok = Streams.trim(SnapshotLog.path(target, :updates), captured)
    assert {:error, {:history_unavailable, ^captured}} = content(target, at_offset: tail)
  end

  test "collection reassembles a message spanning multiple bounded pages" do
    base = "/content/parts"
    doc = Yex.Doc.new()
    value = String.duplicate("x", 2 * 1024 * 1024)
    tail = append(doc, base, value)

    assert {:error, {:too_large, ^tail, _}} = content(base, max_input_bytes: tail - 1)

    assert {:ok, %{encoded_bytes: ^tail}} =
             content(base, preflight: true, max_input_bytes: tail)

    assert {:ok, result} =
             content(base, max_input_bytes: tail, max_output_bytes: 3 * 1024 * 1024)

    assert text(result) == value
    assert {:error, {:truncated, _, 100}} = content(base, max_output_bytes: 100)
    assert {:error, {:too_large, _, 100}} = content(base, max_input_bytes: 100)
  end

  test "snapshot and update limits consistently count wire bytes" do
    base = "/content/wire-limit"
    doc = Yex.Doc.new()
    first = append(doc, base, "first")
    snapshot = Yex.encode_state_as_update!(doc)
    assert :ok = SnapshotLog.snapshot(base, first, snapshot)
    tail = append(doc, base, " second")
    wire_bytes = byte_size(snapshot) + 4 + tail - first

    assert {:ok, %{encoded_bytes: ^wire_bytes}} =
             content(base, preflight: true, max_input_bytes: wire_bytes)

    assert {:ok, result} = content(base, max_input_bytes: wire_bytes)
    assert text(result) == "first second"

    for extra <- [[], [preflight: true]] do
      assert {:error, {:too_large, ^wire_bytes, limit}} =
               content(base, Keyword.put(extra, :max_input_bytes, wire_bytes - 1))

      assert limit == wire_bytes - 1
    end
  end

  test "repeated capture changes stop after three attempts" do
    base = "/content/changing"
    doc = Yex.Doc.new()
    tail = append(doc, base, "stored")
    assert :ok = SnapshotLog.snapshot(base, tail, Yex.encode_state_as_update!(doc))
    path = SnapshotLog.path(base, {:snapshot, tail})
    shard = Streams.Cluster.shard_for(Streams.placement_key(path))
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(shard)

    assert {:ok, _} =
             SlateDB.delete(ctx.db, Keys.meta(path), await_durable: true)

    Code.ensure_loaded!(Capture)
    :erlang.trace_pattern({Capture, :prepare, 3}, true, [:call_count])
    on_exit(fn -> :erlang.trace_pattern({Capture, :prepare, 3}, false, [:call_count]) end)

    assert {:error, :incomplete} = content(base, [], 1000)
    assert {:call_count, 3} = :erlang.trace_info({Capture, :prepare, 3}, :call_count)
  end

  test "reconstruction does not start stream servers" do
    base = "/content/inactive"
    doc = Yex.Doc.new()
    tail = append(doc, base, "stored")
    assert :ok = SnapshotLog.snapshot(base, tail, Yex.encode_state_as_update!(doc))
    shard = Streams.Cluster.shard_for(Streams.placement_key(base))
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(shard)

    paths = [
      base <> "/.control",
      SnapshotLog.path(base, :index),
      SnapshotLog.path(base, :updates),
      SnapshotLog.path(base, {:snapshot, tail})
    ]

    for path <- paths,
        {pid, _} <- Registry.lookup(ctx.registry, {ctx.n, {:stream, path}}),
        do: GenServer.stop(pid, :normal, 5000)

    assert {:ok, result} = content(base)
    assert text(result) == "stored"
    for path <- paths, do: assert(Registry.lookup(ctx.registry, {ctx.n, {:stream, path}}) == [])
  end

  defp append(doc, base, text) do
    {:ok, clock} = Yex.encode_state_vector(doc)
    root = Yex.Doc.get_text(doc, "text")
    :ok = Yex.Text.insert(root, Yex.Text.length(root), text)
    update = Yex.encode_state_as_update!(doc, clock)
    assert {:ok, tail} = SnapshotLog.append(base, Frame.frame(update))
    tail
  end

  defp content(base, opts \\ [], budget \\ 10_000) do
    Query.execute(%{entry: node(), cluster: Streams.Cluster}, %{
      view: :yjs_content,
      shard: nil,
      budget: budget,
      opts: Keyword.merge([base: base, roots: [{"text", :text}]], opts)
    })
  end

  defp text(%{content: {:json, json}}) do
    JSON.decode!(json)["roots"] |> Enum.find(&(&1["name"] == "text")) |> Map.fetch!("value")
  end
end
