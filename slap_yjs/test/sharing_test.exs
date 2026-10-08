defmodule Slap.Yjs.SharingTest do
  # Several servers of one document on this node stand in for the servers
  # of several nodes: they share the document only through its stream and
  # the :pg group, as they would across nodes.
  use Slap.Yjs.Test.ClusterCase, async: false

  alias Slap.Yjs
  alias Slap.Yjs.Docs.Config
  alias Slap.Yjs.Frame
  alias Slap.Yjs.Test.Server

  defp start(doc_id, id, opts \\ []) do
    spec = Supervisor.child_spec({Server, [doc_id: doc_id] ++ opts}, id: id)
    start_supervised!(spec)
  end

  defp doc(pid), do: GenServer.call(pid, :doc)
  defp text(pid), do: pid |> doc() |> Yex.Doc.get_text("text") |> Yex.Text.to_string()

  defp insert(pid, at, string),
    do: pid |> doc() |> Yex.Doc.get_text("text") |> Yex.Text.insert(at, string)

  # A client's document update, as a sync message.
  defp update_message(update), do: <<0, 2>> <> Frame.frame(update)

  defp awareness_message(update), do: <<1>> <> Frame.frame(update)

  # A client's awareness update setting `state`; returns it and the client id.
  defp awareness_update(state) do
    {:ok, awareness} = Yex.Awareness.new(Yex.Doc.new())
    Yex.Awareness.set_local_state(awareness, state)
    [id] = Yex.Awareness.get_client_ids(awareness)
    {:ok, update} = Yex.Awareness.encode_update(awareness)
    {update, id}
  end

  defp awareness_ids(update) do
    {:ok, awareness} = Yex.Awareness.new(Yex.Doc.new())
    :ok = Yex.Awareness.apply_update(awareness, update)
    Yex.Awareness.get_client_ids(awareness)
  end

  test "join rejects misspelled document server options" do
    assert_raise ArgumentError, fn ->
      Yjs.Docs.join(Server, doc_id(), idle_timout: 50)
    end
  end

  test "Docs removes its static config on stop" do
    stop_supervised!(Yjs.Docs)
    assert_raise ArgumentError, fn -> Config.get(Yjs.Docs) end
  end

  test "servers of one document converge through its stream" do
    doc_id = doc_id()
    a = start(doc_id, :a)
    b = start(doc_id, :b)

    insert(a, 0, "from a")
    :ok = Yjs.DocServer.sync(a, 5_000)
    :ok = Yjs.DocServer.sync(b, 5_000)
    assert text(b) == "from a"

    for i <- 1..20, do: insert(if(rem(i, 2) == 0, do: a, else: b), 0, "#{i} ")
    for pid <- [a, b, a], do: :ok = Yjs.DocServer.sync(pid, 5_000)
    assert text(a) == text(b)

    c = start(doc_id, :c)
    assert text(c) == text(a)
  end

  test "subscribers get every update but their own, from any server" do
    doc_id = doc_id()
    {:ok, a} = Yjs.Docs.join(Server, doc_id)
    b = start(doc_id, :b)
    test = self()

    other =
      spawn_link(fn ->
        {:ok, ^a} = Yjs.Docs.join(Server, doc_id)
        send(test, :joined)
        forward(test)
      end)

    assert_receive :joined

    client = Yex.Doc.new()
    Yex.Text.insert(Yex.Doc.get_text(client, "text"), 0, "hello")
    update = Yex.encode_state_as_update!(client)
    :ok = Server.process_message_v1(a, update_message(update), self())

    assert_receive {:forwarded, {:slap_yjs_update, ^doc_id, _}}
    refute_received {:slap_yjs_update, _, _}

    insert(b, 0, "remote ")
    assert_receive {:slap_yjs_update, ^doc_id, _}, 5_000
    assert_receive {:forwarded, {:slap_yjs_update, ^doc_id, _}}, 5_000
    Process.unlink(other)
  end

  defp forward(test) do
    receive do
      message -> send(test, {:forwarded, message})
    end

    forward(test)
  end

  test "server code edits the document through doc/2; the edit is relayed and stored" do
    doc_id = doc_id()
    {:ok, a} = Yjs.Docs.join(Server, doc_id)

    {:ok, doc} = Yjs.DocServer.doc(a, 5_000)
    Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, "hello")
    assert_receive {:slap_yjs_update, ^doc_id, _}

    :ok = Yjs.DocServer.sync(a, 5_000)
    assert text(start(doc_id, :b)) == "hello"
  end

  test "encode_message/1 makes the y-protocols message for what a subscriber receives" do
    doc_id = doc_id()
    {update, _id} = awareness_update(%{"name" => "a"})

    assert Yjs.DocServer.encode_message({:slap_yjs_update, doc_id, "update"}) ==
             update_message("update")

    assert Yjs.DocServer.encode_message({:slap_yjs_awareness, doc_id, update}) ==
             awareness_message(update)
  end

  test "join sets the server's initial assigns when it starts it" do
    doc_id = doc_id()
    {:ok, pid} = Yjs.Docs.join(Server, doc_id, assigns: %{limit: 8})
    assert GenServer.call(pid, {:assign, :limit}) == 8

    {:ok, ^pid} = Yjs.Docs.join(Server, doc_id, assigns: %{limit: 9})
    assert GenServer.call(pid, {:assign, :limit}) == 8
  end

  test "a server stops after its last subscriber leaves, and compacts" do
    doc_id = doc_id()
    {:ok, pid} = Yjs.Docs.join(Server, doc_id, idle_timeout: 50)
    ref = Process.monitor(pid)
    insert(pid, 0, "kept")
    assert Yjs.Docs.whereis(Server, doc_id) == pid

    :ok = Yjs.Docs.leave(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
    assert {:ok, [_]} = Yjs.Store.snapshots(doc_id)

    {:ok, pid} = Yjs.Docs.join(Server, doc_id)
    assert text(pid) == "kept"
  end

  test "awareness crosses servers, and goes with its client or server" do
    doc_id = doc_id()
    a = start(doc_id, :a)
    b = start(doc_id, :b)
    :ok = Yjs.DocServer.subscribe(b, self(), 5_000)

    test = self()
    {update, id} = awareness_update(%{"user" => "ann"})

    client =
      spawn(fn ->
        :ok = Yjs.DocServer.subscribe(a, self(), 5_000)
        :ok = Server.process_message_v1(a, awareness_message(update), self())
        send(test, :sent)
        Process.sleep(:infinity)
      end)

    assert_receive :sent
    assert_receive {:slap_yjs_awareness, ^doc_id, received}, 5_000
    assert awareness_ids(received) == [id]
    assert GenServer.call(b, :awareness_ids) == [id]

    # A server that starts later gets the current states.
    c = start(doc_id, :c)
    assert eventually(fn -> GenServer.call(c, :awareness_ids) == [id] end)

    Process.exit(client, :kill)
    assert_receive {:slap_yjs_awareness, ^doc_id, _removal}, 5_000
    assert GenServer.call(b, :awareness_ids) == []
    assert eventually(fn -> GenServer.call(c, :awareness_ids) == [] end)

    {update, id} = awareness_update(%{"user" => "bob"})
    :ok = Server.process_message_v1(a, awareness_message(update), nil)
    assert_receive {:slap_yjs_awareness, ^doc_id, _}, 5_000
    assert GenServer.call(b, :awareness_ids) == [id]
    stop_supervised!(:a)
    assert_receive {:slap_yjs_awareness, ^doc_id, _removal}, 5_000
    assert GenServer.call(b, :awareness_ids) == []
  end

  test "a joining server attributes each state to the server whose client set it" do
    doc_id = doc_id()
    a = start(doc_id, :a)
    b = start(doc_id, :b)
    :ok = Yjs.DocServer.subscribe(b, self(), 5_000)

    {update, id_a} = awareness_update(%{"user" => "ann"})
    :ok = Server.process_message_v1(a, awareness_message(update), nil)
    assert_receive {:slap_yjs_awareness, ^doc_id, _}, 5_000
    {update, id_b} = awareness_update(%{"user" => "bob"})
    :ok = Server.process_message_v1(b, awareness_message(update), nil)
    assert eventually(fn -> GenServer.call(a, :awareness_ids) == Enum.sort([id_a, id_b]) end)

    # c hears from a first, and from b only after: a must not pass b's
    # state off as its own.
    :sys.suspend(b)
    c = start(doc_id, :c)
    assert eventually(fn -> id_a in GenServer.call(c, :awareness_ids) end)
    :sys.resume(b)
    assert eventually(fn -> GenServer.call(c, :awareness_ids) == Enum.sort([id_a, id_b]) end)

    # Once the :pg scope has handled a's exit, it has told c that a left.
    stop_supervised!(:a)
    :sys.get_state(Yjs.DocServer.pg_scope())
    assert GenServer.call(c, :awareness_ids) == [id_b]
  end

  test "a client that moves to another server stays when its old server leaves" do
    doc_id = doc_id()
    a = start(doc_id, :a)
    b = start(doc_id, :b)
    c = start(doc_id, :c)
    :ok = Yjs.DocServer.subscribe(c, self(), 5_000)

    {:ok, awareness} = Yex.Awareness.new(Yex.Doc.new())
    Yex.Awareness.set_local_state(awareness, %{"user" => "ann"})
    [id] = Yex.Awareness.get_client_ids(awareness)
    {:ok, update} = Yex.Awareness.encode_update(awareness)
    :ok = Server.process_message_v1(a, awareness_message(update), self())
    assert eventually(fn -> GenServer.call(b, :awareness_ids) == [id] end)
    assert_receive {:slap_yjs_awareness, ^doc_id, _}, 5_000

    # The client reconnects to b, with its next clock.
    Yex.Awareness.set_local_state(awareness, %{"user" => "ann"})
    {:ok, update} = Yex.Awareness.encode_update(awareness)
    :ok = Server.process_message_v1(b, awareness_message(update), self())
    assert_receive {:slap_yjs_awareness, ^doc_id, _}, 5_000

    # Once the :pg scope has handled a's exit, it has told b and c.
    stop_supervised!(:a)
    :sys.get_state(Yjs.DocServer.pg_scope())
    assert GenServer.call(b, :awareness_ids) == [id]
    assert GenServer.call(c, :awareness_ids) == [id]
  end

  # For states reached through messages between two other processes, which
  # this test cannot wait on directly.
  defp eventually(fun, attempts \\ 50) do
    cond do
      fun.() ->
        true

      attempts == 0 ->
        false

      true ->
        receive after: (20 -> :ok)
        eventually(fun, attempts - 1)
    end
  end
end
