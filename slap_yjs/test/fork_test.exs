defmodule Slap.Yjs.ForkTest do
  use Slap.Yjs.Test.ClusterCase, async: false

  alias Slap.SnapshotLog
  alias Slap.Yjs.{Docs, DocServer, Frame, Store}
  alias Slap.Yjs.Test.Server

  test "a fork preserves snapshot deletions and additional buffered state" do
    source = doc_id()
    target = doc_id()
    doc = Yex.Doc.new()
    text = Yex.Doc.get_text(doc, "text")
    Yex.Text.insert(text, 0, "abc")
    {:ok, at} = Store.append(source, Frame.frame(Yex.encode_state_as_update!(doc)))
    Yex.Text.delete(text, 1, 1)
    Yex.Text.insert(text, 2, "x")
    snapshot = Yex.encode_state_as_update!(doc)
    :ok = Store.snapshot(source, at, snapshot)
    assert {:ok, %{offset: ^at, snapshot_offset: ^at}} = Store.fork(source, target)
    assert :ok = Store.delete_doc(source)
    assert {:ok, pid} = Docs.join(Server, target, compact_on_stop: false)
    fork = GenServer.call(pid, :doc)
    assert content(fork) == "acx"

    Yex.Text.insert(Yex.Doc.get_text(fork, "text"), 3, "z")
    assert :ok = DocServer.sync(pid, 5_000)
    assert {:ok, loaded} = Store.load(target)
    replay = Yex.Doc.new()

    for update <- List.wrap(loaded.snapshot) ++ loaded.updates,
        do: :ok = Yex.apply_update(replay, update)

    assert content(replay) == "acxz"
    :ok = Yex.apply_update(replay, Yex.encode_state_as_update!(doc))
    assert content(replay) == "acxz"
    assert GenServer.call(pid, :awareness_ids) == []
  end

  test "forking does not flush a live server; callers can synchronize explicitly" do
    source = doc_id()
    first = doc_id()
    second = doc_id()
    assert {:ok, pid} = Docs.join(Server, source, flush_after: 60_000, compact_on_stop: false)
    doc = GenServer.call(pid, :doc)
    Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, "persisted")
    assert :ok = DocServer.sync(pid, 5_000)
    {:ok, before} = Store.tail(source)
    Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 9, " buffered")
    assert content(doc) == "persisted buffered"
    assert {:ok, %{offset: ^before}} = Store.fork(source, first)
    assert {:ok, ^before} = Store.tail(source)
    assert {:ok, first_loaded} = Store.load(first)
    assert replay(first_loaded) == "persisted"

    assert :ok = DocServer.sync(pid, 5_000)
    {:ok, after_sync} = Store.tail(source)
    assert after_sync > before
    assert {:ok, %{offset: ^after_sync}} = Store.fork(source, second)
    assert {:ok, second_loaded} = Store.load(second)
    assert replay(second_loaded) == "persisted buffered"
    Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, "source ")
    assert :ok = DocServer.sync(pid, 5_000)
    assert {:ok, second_loaded} = Store.load(second)
    assert replay(second_loaded) == "persisted buffered"
    {:ok, branch} = Docs.join(Server, second, compact_on_stop: false)
    branch_doc = GenServer.call(branch, :doc)
    Yex.Text.insert(Yex.Doc.get_text(branch_doc, "text"), 0, "branch ")
    assert :ok = DocServer.sync(branch, 5_000)
    assert content(doc) == "source persisted buffered"
    assert {:ok, source_loaded} = Store.load(source)
    assert replay(source_loaded) == "source persisted buffered"
    assert {:ok, branch_loaded} = Store.load(second)
    assert replay(branch_loaded) == "branch persisted buffered"
  end

  test "concurrent insertions and deletions survive capture without sharing awareness" do
    source = doc_id()
    target = doc_id()
    a = Yex.Doc.new()
    Yex.Text.insert(Yex.Doc.get_text(a, "text"), 0, "ab")
    b = Yex.Doc.new()
    :ok = Yex.apply_update(b, Yex.encode_state_as_update!(a))
    Yex.Text.insert(Yex.Doc.get_text(a, "text"), 1, "x")
    Yex.Text.delete(Yex.Doc.get_text(b, "text"), 0, 1)
    Yex.Text.insert(Yex.Doc.get_text(b, "text"), 1, "y")

    for doc <- [a, b],
        do: {:ok, _} = Store.append(source, Frame.frame(Yex.encode_state_as_update!(doc)))

    :ok = Yex.apply_update(a, Yex.encode_state_as_update!(b))
    {:ok, at} = Store.tail(source)
    :ok = Store.snapshot(source, at, Yex.encode_state_as_update!(a))
    {:ok, source_pid} = Docs.join(Server, source, compact_on_stop: false)
    {:ok, awareness} = Yex.Awareness.new(Yex.Doc.new())
    Yex.Awareness.set_local_state(awareness, %{"user" => "source"})
    {:ok, update} = Yex.Awareness.encode_update(awareness)
    [id] = Yex.Awareness.get_client_ids(awareness)
    :ok = DocServer.claim(source_pid, id, :source)
    :ok = Server.process_message_v1(source_pid, <<1>> <> Frame.frame(update), self())
    assert GenServer.call(source_pid, :awareness_ids) != []

    assert {:ok, %{offset: ^at}} = Store.fork(source, target)
    assert {:ok, target_pid} = Docs.join(Server, target, compact_on_stop: false)
    target_doc = GenServer.call(target_pid, :doc)
    assert content(target_doc) == content(a)
    assert GenServer.call(target_pid, :awareness_ids) == []
    assert Docs.whereis(Server, source) == source_pid
    assert :ok = Store.delete_doc(source)
    Yex.Text.insert(Yex.Doc.get_text(target_doc, "text"), 0, "target")
    assert :ok = DocServer.sync(target_pid, 5_000)
    assert {:ok, loaded} = Store.load(target)
    assert replay(loaded) == "target" <> content(a)
  end

  test "pending documents cannot be loaded or joined" do
    source = doc_id()
    target = doc_id()
    {:ok, _} = Store.load(source)

    assert catch_throw(
             SnapshotLog.fork_internal(Store.base(source), Store.base(target),
               after_step: fn
                 :reservation -> throw(:paused)
                 _ -> :ok
               end
             )
           ) == :paused

    assert {:error, :busy} = Store.load(target)
    assert {:error, :busy} = Docs.join(Server, target)
    assert {:error, :busy} = Store.append(target, Frame.frame("x"))
    assert {:ok, _} = Store.fork(source, target)
    assert {:ok, _pid} = Docs.join(Server, target)
  end

  test "fork validates identifiers and options at the persistence API" do
    assert {:error, {:bad_request, :invalid_document}} = Store.fork({"", "bad"}, doc_id())

    for opts <- [[flush: true], [at: 0], [retries: -1]] do
      assert_raise ArgumentError, fn -> Store.fork(doc_id(), doc_id(), opts) end
    end

    source = doc_id()
    assert {:error, :not_found} = Store.fork(source, doc_id(), prefix: "/other")
  end

  defp replay(loaded) do
    doc = Yex.Doc.new()

    for update <- List.wrap(loaded.snapshot) ++ loaded.updates,
        do: :ok = Yex.apply_update(doc, update)

    content(doc)
  end

  defp content(doc), do: doc |> Yex.Doc.get_text("text") |> Yex.Text.to_string()
end
