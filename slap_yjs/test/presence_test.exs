defmodule Slap.Yjs.PresenceTest do
  use Slap.Yjs.Test.ClusterCase, async: false

  alias Slap.Cluster.Host
  alias Slap.Streams.Cluster, as: StreamsCluster
  alias Slap.Yjs
  alias Slap.Yjs.{Awareness, Presence}
  alias Slap.Yjs.Test.Server

  @state %{"user" => %{"name" => "Assistant"}}

  defmodule StopAfterPublishServer do
    @moduledoc false
    use Slap.Yjs.DocServer, restart: :temporary

    @impl Yex.DocServer
    def handle_awareness_update(awareness, change, origin, state) do
      {:noreply, state} = super(awareness, change, origin, state)

      if state.assigns[:stop_after_publish],
        do: {:stop, :normal, state},
        else: {:noreply, state}
    end
  end

  defp next_entries(doc_id) do
    assert_receive {:slap_yjs_awareness, ^doc_id, update}, 2_000
    {:ok, entries} = Awareness.decode(update)
    entries
  end

  defp presence_supervisor do
    start_supervised!(%{
      id: :presence_supervisor,
      start: {Supervisor, :start_link, [[], [strategy: :one_for_one]]},
      restart: :temporary,
      type: :supervisor
    })
  end

  defp owner do
    start_supervised!({Task, fn -> receive do: (:finish -> :ok) end})
  end

  defp disconnect(presence, doc_id, id) do
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

    shard
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

    assert_raise ArgumentError, ~r/:owner must be a pid/, fn ->
      Presence.start_link(Keyword.merge(opts, state: @state, owner: :caller))
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

  test "rejects an incompatible state returned by the initial state function" do
    supervisor = presence_supervisor()

    assert {:error, {{%ArgumentError{}, _stacktrace}, _child}} =
             Supervisor.start_child(
               supervisor,
               {Presence, module: Server, doc_id: doc_id(), state: fn _id -> %{busy: true} end}
             )
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

  test "normal owner exit removes presence without a supervisor restart" do
    doc_id = doc_id()
    {:ok, _server} = Yjs.Docs.join(Server, doc_id)
    owner = owner()
    supervisor = presence_supervisor()

    {:ok, presence} =
      Supervisor.start_child(
        supervisor,
        {Presence, module: Server, doc_id: doc_id, state: @state, owner: owner}
      )

    [{id, _clock, @state}] = next_entries(doc_id)
    ref = Process.monitor(presence)
    send(owner, :finish)

    assert_receive {:DOWN, ^ref, :process, ^presence, {:shutdown, :owner_down}}, 2_000
    assert [{^id, _clock, nil}] = next_entries(doc_id)
    assert Supervisor.count_children(supervisor).active == 0
  end

  test "server exit before the publication reply preserves the presence process and id" do
    doc_id = doc_id()

    {:ok, server} =
      Yjs.Docs.join(StopAfterPublishServer, doc_id, assigns: %{stop_after_publish: true})

    :ok = Yjs.Docs.leave(server)
    {:ok, _observer} = Yjs.Docs.join(Server, doc_id)

    presence =
      start_supervised!({Presence, module: StopAfterPublishServer, doc_id: doc_id, state: @state})

    [{id, _clock, @state}] = next_entries(doc_id)
    assert [{^id, _clock, nil}] = next_entries(doc_id)
    assert [{^id, _clock, @state}] = next_entries(doc_id)
    assert Process.alive?(presence)
  end

  test "document deletion leaves the presence supervisor running" do
    doc_id = doc_id()

    supervisor = presence_supervisor()

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

    test = self()

    state = fn id ->
      send(test, {:initialized, self(), id})
      Map.put(@state, "colorIndex", rem(id, 8))
    end

    presence =
      start_supervised!(
        {Presence, module: Server, doc_id: doc_id, state: state, renew_every: 1_000, timeout: 200}
      )

    [{id, _clock, initial}] = next_entries(doc_id)
    assert initial == Map.put(@state, "colorIndex", rem(id, 8))
    assert_received {:initialized, ^presence, ^id}
    shard = disconnect(presence, doc_id, id)

    busy = Map.put(initial, "busy", true)
    assert :ok = Presence.update(presence, busy)

    assert {:ok, _} = Host.start_shard(StreamsCluster, shard)
    assert [{^id, _clock, ^busy}] = next_entries(doc_id)
    assert Process.alive?(presence)
    refute_received {:initialized, _presence, _id}
  end

  test "owner exit while retrying a join stops presence without a supervisor restart" do
    doc_id = doc_id()
    observer = start_supervised!({Server, doc_id: doc_id})
    :ok = Yjs.DocServer.subscribe(observer, self(), 5_000)
    owner = owner()
    supervisor = presence_supervisor()

    {:ok, presence} =
      Supervisor.start_child(
        supervisor,
        {Presence, module: Server, doc_id: doc_id, state: @state, owner: owner, timeout: 200}
      )

    [{id, _clock, @state}] = next_entries(doc_id)
    disconnect(presence, doc_id, id)

    assert :ok = Presence.update(presence, @state)
    ref = Process.monitor(presence)
    Process.exit(owner, :kill)

    assert_receive {:DOWN, ^ref, :process, ^presence, {:shutdown, :owner_down}}, 2_000
    assert Supervisor.count_children(supervisor).active == 0
  end
end
