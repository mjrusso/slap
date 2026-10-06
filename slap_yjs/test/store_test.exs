defmodule Slap.Yjs.StoreTest do
  use Slap.Yjs.Test.ClusterCase, async: false

  alias Slap.Streams
  alias Slap.Yjs.{Docs, DocServer, Frame, Store}
  alias Slap.Yjs.Test.Server

  defmodule OtherCluster do
    use Slap.Streams.Cluster, otp_app: :slap_yjs
  end

  defmodule OtherDocs do
    @moduledoc false
  end

  test "a document server uses its Docs instance's store" do
    start_supervised!({OtherCluster, store: :memory, shards: 1})

    start_supervised!(%{
      id: OtherDocs,
      start: {Docs, :start_link, [[name: OtherDocs, cluster: OtherCluster, prefix: "/other"]]}
    })

    doc = doc_id()
    assert {:ok, pid} = Docs.join(Server, doc, docs: OtherDocs)
    assert is_pid(pid)
    assert {:ok, %{updates: []}} = Store.load(doc, cluster: OtherCluster, prefix: "/other")
    assert {:error, :not_found} = Streams.head(Store.path(doc, :updates))
    assert :ok = Docs.delete(doc, docs: OtherDocs)
    assert {:error, :deleted} = Store.load(doc, cluster: OtherCluster, prefix: "/other")
  end

  test "a new document loads empty, and appends load back as updates" do
    doc = doc_id()
    assert {:ok, %{snapshot: nil, updates: [], offset: 0}} = Store.load(doc)

    {:ok, _} = Store.append(doc, Frame.frames(["u1", "u2"]))
    {:ok, tail} = Store.append(doc, Frame.frame("u3"))

    assert {:ok, %{snapshot: nil, updates: ["u1", "u2", "u3"], offset: ^tail}} = Store.load(doc)
  end

  test "a compaction's snapshot is loaded, with the updates after it" do
    doc = doc_id()
    {:ok, at} = Store.append(doc, Frame.frames(["u1", "u2"]))
    {:ok, tail} = Store.append(doc, Frame.frame("u3"))
    :ok = Store.snapshot(doc, at, "state-to-u2", timeout: 5_000)

    assert {:ok, %{snapshot: "state-to-u2", snapshot_offset: ^at, updates: ["u3"], offset: ^tail}} =
             Store.load(doc, timeout: 5_000)

    assert {:ok, [%{offset: ^at}]} = Store.snapshots(doc, timeout: 5_000)
    assert {:ok, "state-to-u2"} = Store.read_snapshot(doc, at, timeout: 5_000)
    assert {:ok, ^tail} = Store.tail(doc, timeout: 5_000)

    :ok = Store.delete_doc(doc, timeout: 5_000)
    assert {:error, :deleted} = Store.load(doc)
  end

  test "a document's streams are below its base path, on one shard" do
    doc = doc_id()
    assert Store.base(doc) =~ ~r"^/v1/stream/yjs/test/docs/doc-\d+$"

    for stream <- [:updates, :index, {:snapshot, 0}] do
      assert Streams.placement_key(Store.path(doc, stream)) == Store.base(doc)
    end
  end

  test "document ids are path segments" do
    for bad <- [{"", "d"}, {"s", ".x"}, {"s", "a/b"}, {"s", nil}] do
      assert_raise ArgumentError, fn -> Store.base(bad) end
      assert {:error, {:bad_request, :invalid_document}} = Store.load(bad)
      assert {:error, {:bad_request, :invalid_document}} = Store.append(bad, Frame.frame("u"))
    end
  end

  test "an empty update batch is a bad request" do
    assert {:error, {:bad_request, :invalid_frames}} = Store.append(doc_id(), "")
    assert {:error, {:bad_request, :invalid_frames}} = Store.append(doc_id(), :frames)
  end

  test "invalid control options raise at the public call" do
    doc = doc_id()
    assert_raise ArgumentError, fn -> Store.append(:bad_doc, "", timout: 1) end
    assert_raise ArgumentError, fn -> Store.snapshot(:bad_doc, -1, "", historry: []) end
    assert_raise ArgumentError, fn -> Store.snapshot(doc, 0, "", now: 0) end

    assert_raise ArgumentError, ~r/:timeout/, fn ->
      Store.snapshot(:bad_doc, 0, "", timeout: -1)
    end

    assert_raise ArgumentError, fn ->
      Store.snapshot(doc, 0, "", after_step: fn _ -> :ok end)
    end

    assert_raise ArgumentError, ~r/:prefix/, fn -> Store.load(doc, prefix: 12) end
    assert_raise ArgumentError, ~r/:cluster/, fn -> Store.load(doc, cluster: 12) end
    assert_raise ArgumentError, ~r/:timeout/, fn -> Store.tail(:bad_doc, timeout: -1) end
    assert_raise ArgumentError, ~r/:timeout/, fn -> Docs.join(Server, doc, timeout: -1) end

    assert_raise ArgumentError, ~r/:history/, fn ->
      Docs.join(Server, doc, compaction: [history: [{0, 1_000}]])
    end

    assert_raise ArgumentError, fn -> Docs.join(Server, doc, compaction: [now: 0]) end

    assert_raise ArgumentError, ~r/timeout/, fn ->
      Docs.join(Server, doc, compaction: [timeout: -1])
    end

    assert_raise ArgumentError, fn ->
      Docs.join(Server, doc, compaction: [after_step: fn _ -> :ok end])
    end

    assert_raise ArgumentError, ~r/timeout/, fn ->
      DocServer.sync(self(), -1)
    end
  end
end
