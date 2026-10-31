defmodule Slap.InspectPublicationTest do
  # These tests use the named Streams cluster and its node-wide inspection slot.
  use ExUnit.Case, async: false

  alias Slap.Inspect.Query
  alias Slap.{SlateDB, SnapshotLog, Streams}
  alias Slap.Test.StorageProxy
  alias Slap.Yjs.Frame

  @moduletag :s3
  @moduletag timeout: 120_000

  setup context do
    proxy = StorageProxy.start("inspect-publication")
    store = proxy.store

    start_supervised!(
      {Streams.Cluster,
       store: store, shards: 1, settings: %{flush_interval: "2ms", manifest_poll_interval: "1ms"}}
    )

    assert :ok = Streams.Cluster.await_ready(10_000)
    doc = Yex.Doc.new()
    base = "/publication"
    first = append(doc, base, "first")

    unless context[:untrimmed] do
      assert :ok = SnapshotLog.snapshot(base, first, Yex.encode_state_as_update!(doc))
    end

    middle = " second" <> String.duplicate("x", 512 * 1024)
    tail = append(doc, base, middle)
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(0)
    assert :ok = SlateDB.flush(ctx.db, type: :memtable)
    stop_supervised!(Streams.Cluster)

    start_supervised!(
      {Streams.Cluster,
       store: store, shards: 1, settings: %{flush_interval: "2ms", manifest_poll_interval: "1ms"}}
    )

    assert :ok = Streams.Cluster.await_ready(10_000)
    %{doc: doc, base: base, proxy: proxy, text: "first" <> middle, tail: tail}
  end

  test "publication during a native read restarts from the new snapshot", context do
    reader = begin_read(context)
    blocked = blocked_read(false)
    StorageProxy.release(context.proxy, blocked)
    tail = append(context.doc, context.base, " third")

    assert :ok =
             SnapshotLog.snapshot(context.base, tail, Yex.encode_state_as_update!(context.doc))

    send(blocked, :release)

    assert {:ok, %{content: {:json, json}, coverage: %{snapshot_offset: ^tail}}} =
             Task.await(reader, 30_000)

    assert [%{"value" => value}] = JSON.decode!(json)["roots"]
    assert value == context.text <> " third"
  end

  test "a restart repeats the size preflight", context do
    limit = 1024 * 1024
    reader = begin_read(context, max_input_bytes: limit)
    blocked = blocked_read(false)
    StorageProxy.release(context.proxy, blocked)
    tail = append(context.doc, context.base, String.duplicate("x", 2 * 1024 * 1024))

    assert :ok =
             SnapshotLog.snapshot(context.base, tail, Yex.encode_state_as_update!(context.doc))

    send(blocked, :release)
    assert {:error, {:too_large, _, ^limit}} = Task.await(reader, 30_000)
  end

  test "appends during collection preserve the captured tail without a restart", context do
    reader = begin_read(context)
    blocked = blocked_read(false)
    StorageProxy.release(context.proxy, blocked)
    append(context.doc, context.base, " later")
    send(blocked, :release)

    assert {:ok, %{content: {:json, json}, coverage: %{until: tail}}} =
             Task.await(reader, 30_000)

    assert tail == context.tail
    assert [%{"value" => value}] = JSON.decode!(json)["roots"]
    assert value == context.text
  end

  @tag :untrimmed
  test "history collection ignores a publication whose selected snapshot disappears", context do
    reader = begin_read(context, at_offset: context.tail)
    blocked = blocked_read(false)
    StorageProxy.release(context.proxy, blocked)

    assert catch_throw(
             SnapshotLog.snapshot_internal(
               context.base,
               context.tail,
               Yex.encode_state_as_update!(context.doc),
               after_step: fn
                 :index -> throw(:published)
                 _step -> :ok
               end
             )
           ) == :published

    assert :ok = Streams.delete(SnapshotLog.path(context.base, {:snapshot, context.tail}))
    send(blocked, :release)

    assert {:ok, %{content: {:json, json}, coverage: coverage}} = Task.await(reader, 30_000)
    assert coverage == %{from: 0, until: context.tail, snapshot_offset: nil, exact: true}
    assert [%{"value" => value}] = JSON.decode!(json)["roots"]
    assert value == context.text
  end

  test "an expired collection returns no partial content", context do
    reader = begin_read(context, [], 5000)
    blocked = blocked_read(false)
    StorageProxy.release(context.proxy, blocked)
    Process.send_after(self(), :past_budget, 5100)
    assert_receive :past_budget, 10_000
    send(blocked, :release)
    assert {:error, :deadline} = Task.await(reader, 5000)
  end

  defp begin_read(context, opts \\ [], budget \\ 20_000) do
    observer = self()
    Code.ensure_loaded!(Slap.Inspect.Collection)
    StorageProxy.hold(context.proxy, :sst)

    on_exit(fn ->
      :erlang.trace_pattern({Slap.Inspect.Collection, :read, 7}, false, [:local])
    end)

    reader =
      Task.async(fn ->
        receive do: (:begin -> :ok)

        Query.execute(%{entry: node(), cluster: Streams.Cluster}, %{
          view: :yjs_content,
          shard: 0,
          budget: budget,
          opts: Keyword.merge([base: context.base, roots: [{"text", :text}]], opts),
          observer: observer
        })
      end)

    assert :erlang.trace_pattern({Slap.Inspect.Collection, :read, 7}, true, [:local]) == 1
    :erlang.trace(reader.pid, true, [:call])
    send(reader.pid, :begin)
    reader
  end

  defp blocked_read(updates?) do
    receive do
      {:trace, _, :call, {Slap.Inspect.Collection, :read, [_conn, _request, stream | _]}} ->
        blocked_read(String.ends_with?(stream.path, "/.updates"))

      {:inspection_owner, _, _} ->
        blocked_read(updates?)

      {:storage_blocked, pid, _headers} when updates? ->
        pid

      {:storage_blocked, pid, _headers} ->
        send(pid, :release)
        blocked_read(updates?)
    after
      10_000 -> flunk("collection did not reach a blocked update read")
    end
  end

  defp append(doc, base, text) do
    {:ok, clock} = Yex.encode_state_vector(doc)
    root = Yex.Doc.get_text(doc, "text")
    :ok = Yex.Text.insert(root, Yex.Text.length(root), text)

    assert {:ok, tail} =
             SnapshotLog.append(base, Frame.frame(Yex.encode_state_as_update!(doc, clock)))

    tail
  end
end
