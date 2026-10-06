defmodule Slap.Yjs.DeletionTest do
  # Two nodes, each running Slap.Streams.Cluster (Distributed strategy, one
  # shared local directory) and Slap.Yjs.Docs, with a server of one document
  # on each. The document is deleted from one node while the server on the
  # other has an update buffered, and has not read the deletion.
  #
  # Synchronous: it makes this node distributed.
  use ExUnit.Case, async: false

  alias Slap.Yjs
  alias Slap.Yjs.Test.{Peers, Server}

  @moduletag :cluster
  @moduletag timeout: 120_000

  setup do
    Peers.distribute!()
    dir = Path.join(System.tmp_dir!(), "yjs-deletion-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    nodes = for _ <- 1..2, do: Peers.start(dir)
    on_exit(fn -> Enum.each(nodes, &Peers.kill_if_up/1) end)
    %{nodes: nodes}
  end

  test "a deleted document's servers are stopped, and it stays deleted", %{nodes: [one, two]} do
    doc_id = {"test", "doc-#{System.unique_integer([:positive])}"}
    a = Peers.join(one, doc_id)
    b = Peers.join(two, doc_id, flush_after: 60_000)
    :ok = Peers.insert(b, "buffered")
    # b cannot learn of the deletion by itself: the delete must stop it.
    {:ok, _holder} = Peers.suspend_follower(b)

    :ok = :erpc.call(one, Yjs.Docs, :delete, [doc_id])

    for pid <- [a, b] do
      refute :erpc.call(node(pid), Process, :alive?, [pid])
    end

    for node <- [one, two] do
      assert {:error, :deleted} = :erpc.call(node, Yjs.Store, :load, [doc_id])
      assert {:error, :deleted} = :erpc.call(node, Yjs.Docs, :join, [Server, doc_id])
    end
  end
end
