defmodule Slap.Yjs.PresenceTest do
  use Slap.Yjs.Test.ClusterCase, async: false

  alias Slap.Cluster.Host
  alias Slap.Streams.Cluster, as: StreamsCluster
  alias Slap.Yjs
  alias Slap.Yjs.{Awareness, Presence}
  alias Slap.Yjs.Test.Server

  @state %{"user" => %{"name" => "Assistant"}}

  defp next_entries(doc_id) do
    assert_receive {:slap_yjs_awareness, ^doc_id, update}, 2_000
    {:ok, entries} = Awareness.decode(update)
    entries
  end

  test "publishes and updates its state" do
    doc_id = doc_id()
    {:ok, _server} = Yjs.Docs.join(Server, doc_id)
    presence = start_supervised!({Presence, module: Server, doc_id: doc_id, state: @state})

    assert [{id, _clock, @state}] = next_entries(doc_id)

    busy = Map.merge(@state, %{"busy" => true, "progress" => 0.5, "tasks" => [1, nil, %{}]})
    :ok = Presence.update(presence, busy)
    assert [{^id, _clock, ^busy}] = next_entries(doc_id)
  end

  test "rejects invalid states and renewal intervals" do
    opts = [module: Server, doc_id: doc_id()]

    assert_raise ArgumentError, ~r/:state must be a map/, fn ->
      Presence.start_link(Keyword.put(opts, :state, nil))
    end

    assert_raise ArgumentError, ~r/:renew_every must be a positive integer/, fn ->
      Presence.start_link(Keyword.merge(opts, state: @state, renew_every: 0))
    end

    presence = start_supervised!({Presence, Keyword.put(opts, :state, @state)})

    assert_raise ArgumentError, ~r/state must be a map/, fn ->
      Presence.update(presence, nil)
    end

    assert Process.alive?(presence)
  end

  test "rejects incompatible states in the caller and preserves presence" do
    doc_id = doc_id()
    {:ok, _server} = Yjs.Docs.join(Server, doc_id)
    opts = [module: Server, doc_id: doc_id]
    presence = start_supervised!({Presence, Keyword.put(opts, :state, @state)})
    [{id, clock, @state}] = next_entries(doc_id)

    for state <- [
          %{busy: true},
          %{"user" => [%{name: "Assistant"}]},
          %{"busy" => :yes},
          %{"value" => self()},
          %{"value" => [true | false]},
          %{"value" => <<255>>},
          %{<<255>> => true},
          %{"value" => 9_223_372_036_854_775_808}
        ] do
      assert_raise ArgumentError, ~r/state must contain/, fn ->
        Presence.update(presence, state)
      end
    end

    assert_raise ArgumentError, ~r/state must contain/, fn ->
      Presence.start_link(Keyword.put(opts, :state, %{busy: true}))
    end

    assert :ok = Presence.update(presence, @state)
    assert [{^id, renewed, @state}] = next_entries(doc_id)
    assert renewed == clock + 1
  end

  test "renews its state with a later clock" do
    doc_id = doc_id()
    {:ok, _server} = Yjs.Docs.join(Server, doc_id)
    start_supervised!({Presence, module: Server, doc_id: doc_id, state: @state, renew_every: 50})

    [{id, clock, @state}] = next_entries(doc_id)
    assert [{^id, renewed, @state}] = next_entries(doc_id)
    assert renewed > clock
  end

  test "stopping removes its state" do
    doc_id = doc_id()
    {:ok, _server} = Yjs.Docs.join(Server, doc_id)
    start_supervised!({Presence, module: Server, doc_id: doc_id, state: @state})
    [{id, _clock, @state}] = next_entries(doc_id)

    stop_supervised!(Presence)
    assert [{^id, _clock, nil}] = next_entries(doc_id)
  end

  test "document deletion leaves the presence supervisor running" do
    doc_id = doc_id()

    supervisor =
      start_supervised!(%{
        id: :presence_supervisor,
        start: {Supervisor, :start_link, [[], [strategy: :one_for_one]]},
        restart: :temporary,
        type: :supervisor
      })

    {:ok, presence} =
      Supervisor.start_child(
        supervisor,
        {Presence, module: Server, doc_id: doc_id, state: @state}
      )

    ref = Process.monitor(presence)

    assert :ok = Yjs.Docs.delete(doc_id)
    assert_receive {:DOWN, ^ref, :process, ^presence, {:shutdown, :deleted}}, 2_000
    assert Supervisor.count_children(supervisor).active == 0
  end

  test "rejoining restores presence on a server that removed it" do
    doc_id = doc_id()
    observer = start_supervised!({Server, doc_id: doc_id})
    :ok = Yjs.DocServer.subscribe(observer, self(), 5_000)

    presence =
      start_supervised!(
        {Presence, module: Server, doc_id: doc_id, state: @state, renew_every: 60_000}
      )

    [{id, _clock, @state}] = next_entries(doc_id)
    server = Yjs.Docs.whereis(Server, doc_id)

    :sys.suspend(presence)

    try do
      Process.exit(server, :kill)
      assert [{^id, _clock, nil}] = next_entries(doc_id)
    after
      :sys.resume(presence)
    end

    assert [{^id, _clock, @state}] = next_entries(doc_id)
  end

  test "retries an unavailable shard while keeping its client id and latest state" do
    doc_id = doc_id()
    observer = start_supervised!({Server, doc_id: doc_id})
    :ok = Yjs.DocServer.subscribe(observer, self(), 5_000)

    presence =
      start_supervised!(
        {Presence,
         module: Server, doc_id: doc_id, state: @state, renew_every: 1_000, timeout: 200}
      )

    [{id, _clock, @state}] = next_entries(doc_id)
    server = Yjs.Docs.whereis(Server, doc_id)
    shard = StreamsCluster.shard_for(Yjs.Store.base(doc_id))
    :sys.suspend(presence)

    try do
      :ok = Host.stop_shard(StreamsCluster, shard)
      Process.exit(server, :kill)
      assert [{^id, _clock, nil}] = next_entries(doc_id)
    after
      :sys.resume(presence)
    end

    busy = Map.put(@state, "busy", true)
    assert :ok = Presence.update(presence, busy)

    assert {:ok, _} = Host.start_shard(StreamsCluster, shard)
    assert [{^id, _clock, ^busy}] = next_entries(doc_id)
    assert Process.alive?(presence)
  end
end
