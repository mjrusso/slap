defmodule Slap.Yjs.ForkRecoveryTest do
  # These tests share distributed Erlang and named peer clusters.
  use ExUnit.Case, async: false

  alias Slap.SnapshotLog.Recovery
  alias Slap.{Streams, Yjs}
  alias Slap.Yjs.{Frame, Store}
  alias Slap.Yjs.Test.{ForkWorker, Peers}

  @moduletag :cluster
  @moduletag timeout: 120_000

  setup do
    Peers.distribute!()

    dir =
      Path.join(System.tmp_dir!(), "document-fork-recovery-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(dir) end)
    nodes = for _ <- 1..3, do: Peers.start(dir)
    on_exit(fn -> Enum.each(nodes, &Peers.kill_if_up/1) end)
    for node <- nodes, do: :ok = :erpc.call(node, Streams.Cluster, :await_ready, [30_000], 35_000)
    %{nodes: nodes}
  end

  for step <- [:updates, :capture, :index] do
    test "owner replacement at #{step} recovers the saved representation", %{nodes: nodes} do
      [one | _] = nodes
      {source, target, state, at, cutoff} = fixture(one)
      creator = Node.spawn(one, ForkWorker, :pause, [source, target, unquote(step), self()])
      assert_receive {:fork_step, ^creator, unquote(step)}, 10_000
      victim = agreed_owner(nodes, target)
      monitor = Process.monitor(creator)
      if node(creator) != victim, do: :erpc.call(node(creator), Process, :exit, [creator, :kill])
      Node.monitor(victim, true)
      :ok = Peers.kill(victim)
      assert_receive {:nodedown, ^victim}, 10_000
      assert_receive {:DOWN, ^monitor, :process, ^creator, _}, 10_000
      survivors = nodes -- [victim]

      for node <- survivors,
          do: :ok = :erpc.call(node, Streams.Cluster, :await_ready, [30_000], 35_000)

      assert agreed_owner(survivors, target) != victim
      [reader | _] = survivors
      vector = Yex.encode_state_vector!(state)
      Yex.Text.insert(Yex.Doc.get_text(state, "text"), 3, "e")

      {:ok, later} =
        :erpc.call(reader, Store, :append, [
          source,
          Frame.frame(Yex.encode_state_as_update!(state, vector))
        ])

      if unquote(step) != :updates do
        :ok =
          :erpc.call(reader, Store, :snapshot, [source, later, Yex.encode_state_as_update!(state)])

        :ok = :erpc.call(reader, Store, :delete_doc, [source])
      end

      for node <- survivors, do: :ok = :erpc.call(node, Recovery, :sweep, [], 35_000)

      assert {:ok, %{offset: ^cutoff, snapshot_offset: ^at}} =
               :erpc.call(reader, Store, :fork, [source, target])

      assert Peers.stored_text(reader, target) == "acd"
      :ok = :erpc.call(reader, Store, :delete_doc, [source])
      server = Peers.join(reader, target, compact_on_stop: false)
      assert :ok = :erpc.call(reader, Yjs.DocServer, :sync, [server, 5_000])
      assert Peers.stored_text(reader, target) == "acd"
    end
  end

  test "a new owner discovers a durable cancellation and releases both component pins", %{
    nodes: nodes
  } do
    [one | _] = nodes
    {source, target, _state, at, _cutoff} = fixture(one)
    creator = Node.spawn(one, ForkWorker, :pause, [source, target, :capture, self()])
    assert_receive {:fork_step, ^creator, :capture}, 10_000
    :ok = :erpc.call(one, ForkWorker, :cancel, [target])
    victim = agreed_owner(nodes, target)
    if node(creator) != victim, do: :erpc.call(node(creator), Process, :exit, [creator, :kill])
    Node.monitor(victim, true)
    :ok = Peers.kill(victim)
    assert_receive {:nodedown, ^victim}, 10_000
    survivors = nodes -- [victim]

    for node <- survivors,
        do: :ok = :erpc.call(node, Streams.Cluster, :await_ready, [30_000], 35_000)

    for node <- survivors, do: :ok = :erpc.call(node, Recovery, :sweep, [], 35_000)
    for node <- survivors, do: :ok = :erpc.call(node, ForkWorker, :drain, [], 35_000)
    [reader | _] = survivors
    assert {:error, :deleted} = :erpc.call(reader, Store, :load, [target])
    assert {:error, :busy} = :erpc.call(reader, Store, :fork, [source, target])

    for path <- [Store.path(source, :updates), Store.path(source, {:snapshot, at})] do
      assert {:ok, %{meta: %{registration_count: 0}}} =
               :erpc.call(reader, Streams, :metadata_fenced, [path])
    end
  end

  defp fixture(node) do
    source = id()
    source_shard = :erpc.call(node, Streams.Cluster, :shard_for, [Store.base(source)])

    target =
      Enum.find(Stream.repeatedly(&id/0), fn target ->
        :erpc.call(node, Streams.Cluster, :shard_for, [Store.base(target)]) != source_shard
      end)

    state = Yex.Doc.new()
    Yex.Text.insert(Yex.Doc.get_text(state, "text"), 0, "abc")

    {:ok, at} =
      :erpc.call(node, Store, :append, [source, Frame.frame(Yex.encode_state_as_update!(state))])

    Yex.Text.delete(Yex.Doc.get_text(state, "text"), 1, 1)
    :ok = :erpc.call(node, Store, :snapshot, [source, at, Yex.encode_state_as_update!(state)])
    vector = Yex.encode_state_vector!(state)
    Yex.Text.insert(Yex.Doc.get_text(state, "text"), 2, "d")

    {:ok, cutoff} =
      :erpc.call(node, Store, :append, [
        source,
        Frame.frame(Yex.encode_state_as_update!(state, vector))
      ])

    {source, target, state, at, cutoff}
  end

  defp id, do: {"fork-recovery", "doc-#{System.unique_integer([:positive])}"}

  defp agreed_owner(nodes, doc) do
    task = Task.async(fn -> owner(nodes, doc) end)
    assert {:ok, owner} = Task.yield(task, 30_000) || Task.shutdown(task, :brutal_kill)
    owner
  end

  # Placement changes are asynchronous; creation is stopped at an acknowledged hook.
  defp owner(nodes, doc) do
    case nodes |> Enum.map(&Peers.owner(&1, doc)) |> Enum.uniq() do
      [owner] when is_atom(owner) -> owner
      _ -> owner(nodes, doc)
    end
  end
end
