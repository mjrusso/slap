defmodule Slap.Yjs.OwnershipTest do
  # ClusterCase starts the named Streams cluster and Docs supervisor.
  use Slap.Yjs.Test.ClusterCase, async: false

  alias Slap.Yjs.{Awareness, Docs, DocServer, Frame, Presence}

  defmodule Server do
    @moduledoc false
    use Slap.Yjs.DocServer, restart: :temporary

    @impl Yex.DocServer
    def handle_info({:DOWN, _ref, :process, pid, _reason} = message, state) do
      result = super(message, state)
      send(state.assigns.observer, {:departed, pid})
      result
    end

    def handle_info(message, state), do: super(message, state)
  end

  defmodule Subscriber do
    @moduledoc false
    use GenServer

    def start_link(server), do: GenServer.start_link(__MODULE__, server)

    @impl true
    def init(server) do
      :ok = DocServer.subscribe(server, self(), 5_000)
      {:ok, server}
    end

    @impl true
    def handle_call({:claim, id, principal}, _from, server),
      do: {:reply, DocServer.claim(server, id, principal), server}

    def handle_call({:publish, entries}, _from, server) do
      message = <<1>> <> Frame.frame(Awareness.encode(entries))
      {:reply, Server.process_message_v1(server, message, self()), server}
    end

    def handle_call(:leave, _from, server),
      do: {:reply, DocServer.unsubscribe(server, self(), 5_000), server}

    def handle_call(:join, _from, server),
      do: {:reply, DocServer.subscribe(server, self(), 5_000), server}

    @impl true
    def handle_info(_message, server), do: {:noreply, server}
  end

  setup do
    doc_id = doc_id()
    {:ok, server} = Docs.join(Server, doc_id, assigns: %{observer: self()})
    %{server: server, doc_id: doc_id}
  end

  defp subscriber(server, id),
    do: start_supervised!(Supervisor.child_spec({Subscriber, server}, id: id))

  defp claim(pid, id, principal), do: GenServer.call(pid, {:claim, id, principal}, 5_000)
  defp publish(pid, entries), do: GenServer.call(pid, {:publish, entries}, 5_000)

  defp states(server) do
    {:ok, [message]} = Server.process_message_v1(server, <<3>>, self())
    {:ok, {:awareness, update}} = Yex.Sync.message_decode(message)
    {:ok, entries} = Awareness.decode(update)
    Map.new(entries, fn {id, _clock, state} -> {id, state} end)
  end

  defp next_entries(doc_id) do
    assert_receive {:slap_yjs_awareness, ^doc_id, update}, 2_000
    {:ok, entries} = Awareness.decode(update)
    entries
  end

  test "takeover rejects the previous owner's states, removals, and claims", context do
    %{server: server, doc_id: doc_id} = context
    a = subscriber(server, :a)
    b = subscriber(server, :b)
    c = subscriber(server, :c)

    assert :ok = claim(a, 7, :ann)
    assert :ok = publish(a, [{7, 1, %{"name" => "ann"}}])
    assert [{7, 1, _}] = next_entries(doc_id)
    assert {:error, :claimed} = claim(c, 7, :bob)
    assert :ok = claim(b, 7, :ann)
    assert :ok = claim(b, 7, :ann)
    assert :ok = publish(b, [{7, 1, %{"name" => "ann"}}])

    assert {:error, :not_owner} = publish(a, [{7, 100, %{"name" => "stale"}}])
    assert {:error, :not_owner} = publish(a, [{7, 100, nil}])

    for tag <- [<<129, 0>>, <<129, 128, 128, 128, 128, 128, 128, 128, 0>>] do
      noncanonical = tag <> Frame.frame(Awareness.encode([{7, 100, nil}]))
      assert {:error, :not_owner} = Server.process_message_v1(server, noncanonical, self())
    end

    assert {:error, :not_owner} = claim(a, 7, :ann)
    assert states(server) == %{7 => %{"name" => "ann"}}
    refute_received {:slap_yjs_awareness, ^doc_id, _}

    assert :ok = claim(c, 7, :ann)
    assert {:error, :not_owner} = claim(b, 7, :ann)
    assert :ok = publish(c, [{7, 2, %{"name" => "replacement"}}])
    assert [{7, 2, %{"name" => "replacement"}}] = next_entries(doc_id)
    assert :ok = GenServer.call(c, :leave, 5_000)
    assert [{7, _clock, nil}] = next_entries(doc_id)
    assert {:error, :not_owner} = claim(a, 7, :ann)
    assert {:error, :not_owner} = claim(b, 7, :ann)
  end

  test "equal-clock takeover survives the old owner's exit before a newer state", context do
    %{server: server, doc_id: doc_id} = context
    a = subscriber(server, :a)
    b = subscriber(server, :b)
    assert :ok = claim(a, 7, :ann)
    assert :ok = publish(a, [{7, 1, %{"name" => "ann"}}])
    next_entries(doc_id)

    assert :ok = claim(b, 7, :ann)
    assert :ok = publish(b, [{7, 1, %{"name" => "ann"}}])
    stop_supervised!(:a)
    assert_receive {:departed, ^a}, 2_000
    assert states(server) == %{7 => %{"name" => "ann"}}
    refute_received {:slap_yjs_awareness, ^doc_id, _}

    assert :ok = GenServer.call(b, :leave, 5_000)
    assert [{7, _clock, nil}] = next_entries(doc_id)
    assert states(server) == %{}
  end

  test "unsubscribe releases only current ownership and resets revocation", context do
    %{server: server, doc_id: doc_id} = context
    a = subscriber(server, :a)
    b = subscriber(server, :b)
    assert :ok = claim(a, 7, :ann)
    assert :ok = publish(a, [{7, 1, %{"name" => "ann"}}])
    next_entries(doc_id)
    assert :ok = claim(b, 7, :ann)
    assert :ok = GenServer.call(a, :leave, 5_000)
    assert states(server) == %{7 => %{"name" => "ann"}}
    refute_received {:slap_yjs_awareness, ^doc_id, _}
    assert {:error, :not_subscribed} = claim(a, 7, :ann)
    assert {:error, :not_owner} = publish(a, [{7, 100, nil}])
    assert :ok = GenServer.call(a, :join, 5_000)
    assert :ok = claim(a, 7, :ann)
    assert {:error, :not_owner} = publish(b, [{7, 100, nil}])
  end

  test "claim survives a removal; cleanup and rejoin retain awareness clocks", context do
    %{server: server, doc_id: doc_id} = context
    a = subscriber(server, :a)
    b = subscriber(server, :b)
    assert :ok = claim(a, 7, :ann)
    assert :ok = publish(a, [{7, 5, %{"name" => "ann"}}])
    next_entries(doc_id)
    assert :ok = publish(a, [{7, 6, nil}])
    next_entries(doc_id)
    assert {:error, :claimed} = claim(b, 7, :bob)
    assert :ok = publish(a, [{7, 7, %{"name" => "ann"}}])
    next_entries(doc_id)
    assert :ok = GenServer.call(a, :leave, 5_000)
    [{7, removed_clock, nil}] = next_entries(doc_id)

    assert :ok = GenServer.call(a, :join, 5_000)
    assert :ok = claim(a, 7, :ann)
    assert :ok = publish(a, [{7, removed_clock, %{"name" => "ann"}}])
    assert states(server) == %{}
    refute_received {:slap_yjs_awareness, ^doc_id, _}
    assert :ok = publish(a, [{7, removed_clock + 1, %{"name" => "ann"}}])
    assert [{7, _clock, %{"name" => "ann"}}] = next_entries(doc_id)
  end

  test "unclaimed entries are filtered, including removals in a mixed update", context do
    %{server: server, doc_id: doc_id} = context
    a = subscriber(server, :a)
    b = subscriber(server, :b)
    assert {:error, :not_owner} = publish(a, [{7, 100, %{"name" => "unclaimed"}}])
    assert :ok = claim(a, 7, :ann)
    assert :ok = claim(b, 8, :bob)
    assert :ok = publish(b, [{8, 1, %{"name" => "bob"}}])
    next_entries(doc_id)
    assert :ok = publish(a, [{7, 1, %{"name" => "ann"}}, {8, 100, nil}])
    assert [{7, 1, %{"name" => "ann"}}] = next_entries(doc_id)
    assert states(server) == %{7 => %{"name" => "ann"}, 8 => %{"name" => "bob"}}
  end

  test "clients cannot claim, overwrite, or remove server presence", context do
    %{server: server, doc_id: doc_id} = context
    start_supervised!({Presence, module: Server, doc_id: doc_id, state: %{"name" => "agent"}})
    [{id, _clock, state}] = next_entries(doc_id)
    a = subscriber(server, :a)
    assert {:error, :claimed} = claim(a, id, :ann)
    assert {:error, :not_owner} = publish(a, [{id, 100, %{"name" => "forged"}}])
    assert {:error, :not_owner} = publish(a, [{id, 100, nil}])
    assert states(server) == %{id => state}
    refute_received {:slap_yjs_awareness, ^doc_id, _}
  end

  test "a local subscriber cannot submit with another subscriber's origin", %{server: server} do
    a = subscriber(server, :a)
    assert :ok = claim(a, 7, :ann)
    message = <<1>> <> Frame.frame(Awareness.encode([{7, 1, %{"name" => "forged"}}]))
    assert {:error, :invalid_origin} = Server.process_message_v1(server, message, a)
    assert {:error, :invalid_origin} = Server.process_message_v1(server, message)
    assert {:error, :invalid} = Server.process_message_v1(server, <<1, 128>>, self())
    assert states(server) == %{}
  end

  test "server restart clears claims and requires subscribers to join again", context do
    %{server: server, doc_id: doc_id} = context
    assert :ok = DocServer.claim(server, 7, :ann)
    ref = Process.monitor(server)
    Process.exit(server, :kill)
    assert_receive {:DOWN, ^ref, :process, ^server, :killed}, 2_000

    {:ok, replacement} = Docs.join(Server, doc_id, assigns: %{observer: self()})
    assert :ok = DocServer.claim(replacement, 7, :bob)
    assert states(replacement) == %{}
  end
end
