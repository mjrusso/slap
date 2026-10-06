defmodule Slap.Yjs.FollowerTest do
  use Slap.Yjs.Test.ClusterCase, async: false

  alias Slap.Yjs.{Follower, Frame, Store}

  test "a follower whose offset was trimmed reloads the snapshot and follows on from it" do
    doc_id = doc_id()
    {:ok, _} = Store.append(doc_id, Frame.frames(["u1", "u2"]))
    {:ok, at} = Store.append(doc_id, Frame.frame("u3"))
    :ok = Store.snapshot(doc_id, at, "state-to-u3")

    follower = Follower.start_link(doc_id, 0)

    assert_receive {Follower, ^follower, {:reload, %{snapshot: "state-to-u3", offset: ^at}}},
                   5_000

    Follower.ack(follower)
    {:ok, _} = Store.append(doc_id, Frame.frame("u4"))
    assert_receive {Follower, ^follower, {:updates, ["u4"], _next, _bytes}}, 5_000
  end

  test "a follower sends its next page once the previous one is acknowledged" do
    doc_id = doc_id()
    {:ok, at} = Store.append(doc_id, Frame.frame("u1"))
    follower = Follower.start_link(doc_id, 0)
    assert_receive {Follower, ^follower, {:updates, ["u1"], ^at, _bytes}}, 5_000

    {:ok, _} = Store.append(doc_id, Frame.frame("u2"))
    refute_receive {Follower, ^follower, _}, 200

    Follower.ack(follower)
    assert_receive {Follower, ^follower, {:updates, ["u2"], _next, _bytes}}, 5_000
  end
end
