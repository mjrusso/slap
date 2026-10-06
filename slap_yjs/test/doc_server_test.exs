defmodule Slap.Yjs.DocServerTest do
  use Slap.Yjs.Test.ClusterCase, async: false

  import ExUnit.CaptureLog

  alias Slap.Cluster.Host
  alias Slap.Streams
  alias Slap.Yjs
  alias Slap.Yjs.{Frame, Store}
  alias Slap.Yjs.Test.Peers

  defmodule Server do
    @moduledoc false
    use Slap.Yjs.DocServer, restart: :temporary

    alias Slap.Yjs.DocServer
    alias Yex.DocServer.State

    @impl Yex.DocServer
    def init(arg, state) do
      {after_step, arg} = Keyword.pop(arg, :test_after_step)

      case super(arg, state) do
        {:ok, state} when is_function(after_step, 1) ->
          persistence = state.assigns[DocServer]

          opts =
            Map.update!(persistence.opts, :compaction, &Keyword.put(&1, :after_step, after_step))

          {:ok,
           State.assign(
             state,
             DocServer,
             %{persistence | opts: opts}
           )}

        result ->
          result
      end
    end

    @impl Yex.DocServer
    def handle_call(:doc, _from, state), do: {:reply, state.doc, state}
  end

  defmodule BlockingServer do
    @moduledoc false
    use Slap.Yjs.DocServer, restart: :temporary

    @impl Yex.DocServer
    def handle_info({Yjs.DocServer, :load, arg} = message, state) do
      if match?({"test", "slow-" <> _}, arg[:doc_id]) do
        send(Process.whereis(Slap.Yjs.DocServerTest.JoinGate), {:loading, self()})

        receive do
          :continue_load -> :ok
        end
      end

      super(message, state)
    end

    def handle_info(message, state), do: super(message, state)
  end

  defmodule StartGateServer do
    @moduledoc false
    use Slap.Yjs.DocServer, restart: :temporary

    @impl Yex.DocServer
    def init(arg, state) do
      send(Process.whereis(Slap.Yjs.DocServerTest.StartGate), {:initializing, self()})

      receive do
        :continue_init -> :ok
      after
        5_000 -> raise "init gate was not released"
      end

      super(arg, state)
    end

    @impl Yex.DocServer
    def handle_info({Yjs.DocServer, :load, _arg} = message, state) do
      send(Process.whereis(Slap.Yjs.DocServerTest.StartGate), {:loading, self()})

      receive do
        :continue_load -> :ok
      after
        5_000 -> raise "load gate was not released"
      end

      super(message, state)
    end

    def handle_info(message, state), do: super(message, state)
  end

  defmodule SlowStopServer do
    @moduledoc false
    use Slap.Yjs.DocServer, restart: :temporary

    @impl Yex.DocServer
    def terminate(reason, state) do
      send(Process.whereis(Slap.Yjs.DocServerTest.StopGate), {:terminating, self()})

      receive do
        :continue_stop -> :ok
      after
        5_000 -> :ok
      end

      super(reason, state)
    end
  end

  # Starts the server; returns it and its document. Yex runs the document's
  # functions in the server (`Yex.Doc`'s worker_pid), so the test can edit it.
  defp start(doc_id, opts \\ []) do
    pid = start_supervised!({Server, [doc_id: doc_id] ++ opts})
    {pid, GenServer.call(pid, :doc)}
  end

  defp text(doc), do: doc |> Yex.Doc.get_text("text") |> Yex.Text.to_string()

  # Runs `fun` in the server, as one message: every update it makes is
  # handled after it.
  defp run(pid, fun), do: GenServer.call(pid, {Yex.Doc, :run, fun})

  defp attach_appends do
    id = {__MODULE__, make_ref()}
    event = [:slap, :yjs, :doc_server, :append]
    :ok = :telemetry.attach(id, event, &__MODULE__.send_append/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
  end

  def send_append(_event, measurements, meta, test), do: send(test, {:append, measurements, meta})

  test "a directly supervised doc server gets its stop budget" do
    spec = Server.child_spec(doc_id: doc_id(), terminate_timeout: 100)
    assert spec.shutdown == 30_100
  end

  test "a document survives a restart" do
    doc_id = doc_id()
    {pid, doc} = start(doc_id, compact_on_stop: false)
    text = Yex.Doc.get_text(doc, "text")
    for word <- ~w(persisted in durable streams), do: Yex.Text.insert(text, 0, word <> " ")
    state = run(pid, fn -> Yex.encode_state_as_update!(doc) end)

    stop_supervised!(Server)
    {pid, doc} = start(doc_id, compact_on_stop: false)

    assert text(doc) == "streams durable in persisted "
    assert run(pid, fn -> Yex.encode_state_as_update!(doc) end) == state

    stop_supervised!(Server)
    assert {:ok, %{updates: [_, _, _, _]}} = Store.load(doc_id)
  end

  test "a client's update, sent as a sync message, is stored" do
    doc_id = doc_id()
    {pid, _doc} = start(doc_id)

    client = Yex.Doc.new()
    Yex.Text.insert(Yex.Doc.get_text(client, "text"), 0, "from a client")
    update = Yex.encode_state_as_update!(client)
    :ok = Server.process_message_v1(pid, <<0, 2>> <> Frame.frame(update), :client)

    stop_supervised!(Server)
    {_pid, doc} = start(doc_id)
    assert text(doc) == "from a client"
  end

  test "updates are appended in batches: by size, what arrives during an append, then on stop" do
    attach_appends()
    doc_id = doc_id()
    {pid, doc} = start(doc_id, flush_bytes: 16 * 1024, flush_after: :timer.minutes(1))

    run(pid, fn ->
      text = Yex.Doc.get_text(doc, "text")
      for _ <- 1..10_000, do: Yex.Text.insert(text, 0, "x")
    end)

    stop_supervised!(Server)

    # The first append starts at 16 KiB; what arrives while it runs goes in
    # the next.
    appends = collect_appends()
    assert Enum.sum_by(appends, & &1.updates) == 10_000
    assert Enum.count_until(appends, 20) < 20
    assert hd(appends).bytes >= 16 * 1024

    {_pid, doc} = start(doc_id)
    assert text(doc) == String.duplicate("x", 10_000)
  end

  test "buffered updates are appended after flush_after" do
    attach_appends()
    {_pid, doc} = start(doc_id(), flush_after: 20)
    Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, "x")
    assert_receive {:append, %{updates: 1}, _}, 1_000
  end

  test "a client update that waited for a stored update is stored too" do
    doc_id = doc_id()
    {pid, _doc} = start(doc_id, flush_after: 1, compact_on_stop: false)

    # Another server's client inserts "x"; a client of this server has seen
    # it (through a server that has read it) and inserts "c" after it.
    other = Yex.Doc.new()
    Yex.Text.insert(Yex.Doc.get_text(other, "text"), 0, "x")
    x = Yex.encode_state_as_update!(other)
    client = Yex.Doc.new()
    :ok = Yex.apply_update(client, x)
    {:ok, sv} = Yex.encode_state_vector(client)
    Yex.Text.insert(Yex.Doc.get_text(client, "text"), 1, "c")
    c = Yex.encode_state_as_update!(client, sv)

    # "c" reaches this server before "x" is stored: it waits (pending), and
    # "x" arrives through the server's follower.
    :ok = Server.process_message_v1(pid, <<0, 2>> <> Frame.frame(c), self())
    {:ok, _} = Store.append(doc_id, Frame.frame(x))
    :ok = Yjs.DocServer.sync(pid, 5_000)

    {:ok, %{updates: updates}} = Store.load(doc_id)
    stored = Yex.Doc.new()
    for u <- updates, do: :ok = Yex.apply_update(stored, u)
    assert text(stored) == "xc"
  end

  test "a client deletion that waited for a stored update is stored too" do
    doc_id = doc_id()
    {pid, _doc} = start(doc_id, flush_after: 1, compact_on_stop: false)

    other = Yex.Doc.new()
    Yex.Text.insert(Yex.Doc.get_text(other, "text"), 0, "xy")
    xy = Yex.encode_state_as_update!(other)
    client = Yex.Doc.new()
    :ok = Yex.apply_update(client, xy)
    {:ok, sv} = Yex.encode_state_vector(client)
    Yex.Text.delete(Yex.Doc.get_text(client, "text"), 0, 1)
    delete = Yex.encode_state_as_update!(client, sv)

    :ok = Server.process_message_v1(pid, <<0, 2>> <> Frame.frame(delete), self())
    {:ok, _} = Store.append(doc_id, Frame.frame(xy))
    :ok = Yjs.DocServer.sync(pid, 5_000)

    {:ok, %{updates: updates}} = Store.load(doc_id)
    stored = Yex.Doc.new()
    for u <- updates, do: :ok = Yex.apply_update(stored, u)
    assert text(stored) == "y"
  end

  # A client inserts "x", then "o" into another text; another client has
  # seen "x" and inserts "p" after it. Delivered "o", "p", "x": yrs
  # integrates "o" before "x" (it does not depend on it), and then does not
  # retry "p" once "x" arrives.
  defp out_of_order do
    a = Yex.Doc.new()
    Yex.Text.insert(Yex.Doc.get_text(a, "text"), 0, "x")
    x = Yex.encode_state_as_update!(a)
    {:ok, sv} = Yex.encode_state_vector(a)
    Yex.Text.insert(Yex.Doc.get_text(a, "other"), 0, "o")
    o = Yex.encode_state_as_update!(a, sv)
    b = Yex.Doc.new()
    :ok = Yex.apply_update(b, x)
    {:ok, sv} = Yex.encode_state_vector(b)
    Yex.Text.insert(Yex.Doc.get_text(b, "text"), 1, "p")
    [o, Yex.encode_state_as_update!(b, sv), x]
  end

  defp stored_text(doc_id) do
    {:ok, loaded} = Store.load(doc_id)
    stored = Yex.Doc.new()
    for u <- List.wrap(loaded.snapshot) ++ loaded.updates, do: :ok = Yex.apply_update(stored, u)
    text(stored)
  end

  test "a client update that waited on an update delivered out of order is applied" do
    doc_id = doc_id()
    {pid, doc} = start(doc_id, flush_after: 1, compact_on_stop: false)

    for u <- out_of_order(),
        do: :ok = Server.process_message_v1(pid, <<0, 2>> <> Frame.frame(u), self())

    :ok = Yjs.DocServer.sync(pid, 5_000)
    assert text(doc) == "xp"
    assert stored_text(doc_id) == "xp"
  end

  test "a stored update that waited on an update delivered out of order is applied" do
    doc_id = doc_id()
    for u <- out_of_order(), do: {:ok, _} = Store.append(doc_id, Frame.frame(u))

    {_pid, doc} = start(doc_id, compact_on_stop: false)
    assert text(doc) == "xp"
  end

  test "a snapshot keeps the updates still waiting" do
    attach_compactions()
    doc_id = doc_id()
    [_o, p, x] = out_of_order()
    {:ok, _} = Store.append(doc_id, Frame.frame(p))
    {_pid, doc} = start(doc_id, compact_bytes: 256, flush_after: 1, compact_on_stop: false)
    Yex.Text.insert(Yex.Doc.get_text(doc, "padding"), 0, String.duplicate("a", 300))
    assert_receive {:compacted, _, %{doc_id: ^doc_id}}, 5_000
    stop_supervised!(Server)

    # "p" is only in the snapshot now.
    {:ok, _} = Store.append(doc_id, Frame.frame(x))
    {_pid, doc} = start(doc_id, compact_on_stop: false)
    assert text(doc) == "xp"
  end

  test "appends are retried while the document's shard is down" do
    attach_appends()
    doc_id = doc_id()
    {pid, doc} = start(doc_id, flush_after: 1, compact_on_stop: false)
    text = Yex.Doc.get_text(doc, "text")
    Yex.Text.insert(text, 0, "a")
    :ok = Yjs.DocServer.sync(pid, 5_000)
    assert_received {:append, _, _}

    # Stop the shard, as when it moves to another node.
    key = Streams.placement_key(Store.path(doc_id, :updates))
    shard = Streams.Cluster.shard_for(key)
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(shard)
    ref = Process.monitor(ctx.shard_db)
    :ok = Host.stop_shard(Streams.Cluster, shard)
    assert_receive {:DOWN, ^ref, :process, _, _}, 5_000

    capture_log(fn ->
      # The append fails once routing gives up on the shard, and is
      # appended again after a backoff.
      Yex.Text.insert(text, 1, "b")
      assert_receive {:append, %{updates: 1}, _}, 5_000
      assert_receive {:append, %{updates: 1}, _}, 10_000
      {:ok, _} = Host.start_shard(Streams.Cluster, shard)
      assert :ok = Yjs.DocServer.sync(pid, 10_000)
    end)

    assert Process.alive?(pid)
    stop_supervised!(Server)
    {_pid, doc} = start(doc_id)
    assert text(doc) == "ab"
  end

  test "deleting the document stops its server, which stores nothing more" do
    doc_id = doc_id()
    {pid, doc} = start(doc_id, flush_after: 60_000)
    ref = Process.monitor(pid)
    run(pid, fn -> Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, "buffered") end)

    :ok = Store.delete_doc(doc_id)
    assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :deleted}}, 5_000
    assert {:error, :deleted} = Store.load(doc_id)
    assert {:error, :deleted} = Yjs.Docs.join(Server, doc_id)
  end

  test "a server that stops when idle stops its follower" do
    {:ok, pid} = Yjs.Docs.join(Server, doc_id(), idle_timeout: 50)
    follower = :sys.get_state(pid).assigns[Yjs.DocServer].follower
    refs = for p <- [pid, follower], do: Process.monitor(p)

    :ok = Yjs.Docs.leave(pid)
    for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _pid, _reason}, 5_000)
  end

  test "join rejects an invalid document before starting a server" do
    assert {:error, {:bad_request, :invalid_document}} =
             Yjs.Docs.join(Server, {"bad/path", "doc"})
  end

  test "loading one document does not block another join" do
    Process.register(self(), Slap.Yjs.DocServerTest.JoinGate)
    slow = {"test", "slow-#{System.unique_integer([:positive])}"}
    fast = doc_id()

    loading = Task.async(fn -> Yjs.Docs.join(BlockingServer, slow) end)
    assert_receive {:loading, server}, 5_000
    assert {:ok, _fast_server} = Yjs.Docs.join(Server, fast)
    send(server, :continue_load)
    assert {:ok, ^server} = Task.await(loading, 5_000)
  end

  test "a timed-out join does not subscribe after loading finishes" do
    Process.register(self(), Slap.Yjs.DocServerTest.JoinGate)
    slow = {"test", "slow-#{System.unique_integer([:positive])}"}

    joining =
      Task.async(fn -> Yjs.Docs.join(BlockingServer, slow, timeout: 100, idle_timeout: 50) end)

    assert_receive {:loading, server}, 5_000
    assert {:error, :timeout} = Task.await(joining, 1_000)
    ref = Process.monitor(server)
    send(server, :continue_load)
    assert_receive {:DOWN, ^ref, :process, ^server, :normal}, 5_000
  end

  test "a server is not discoverable or ready before its document loads" do
    Process.register(self(), Slap.Yjs.DocServerTest.StartGate)
    doc_id = doc_id()
    joining = Task.async(fn -> Yjs.Docs.join(StartGateServer, doc_id) end)

    assert_receive {:initializing, server}, 5_000

    try do
      assert Yjs.Docs.whereis(StartGateServer, doc_id) == nil

      ref = Process.monitor(server, alias: :reply_demonitor)
      send(server, {Yjs.DocServer, :request, :ready, self(), ref})
      send(server, :continue_init)

      assert_receive {:loading, ^server}, 5_000
      refute_received {^ref, :ok}
      assert Yjs.Docs.whereis(StartGateServer, doc_id) == nil

      send(server, :continue_load)
      assert_receive {^ref, :ok}, 5_000
      assert {:ok, ^server} = Task.await(joining, 5_000)
      assert Yjs.Docs.whereis(StartGateServer, doc_id) == server
    after
      send(server, :continue_init)
      send(server, :continue_load)
    end
  end

  test "document servers stop concurrently" do
    Process.register(self(), Slap.Yjs.DocServerTest.StopGate)
    docs = Slap.Yjs.DocServerTest.StopDocs
    start_supervised!({Yjs.Docs, name: docs})
    first = doc_id()
    second = doc_id()

    opts = [docs: docs, compact_on_stop: false, terminate_timeout: 5_000]
    assert {:ok, _} = Yjs.Docs.join(SlowStopServer, first, opts)
    assert {:ok, _} = Yjs.Docs.join(SlowStopServer, second, opts)

    stopping = Task.async(fn -> Supervisor.stop(docs, :normal, 5_000) end)
    assert_receive {:terminating, one}, 1_000

    two =
      receive do
        {:terminating, pid} -> pid
      after
        1_000 -> nil
      end

    send(one, :continue_stop)

    if two do
      send(two, :continue_stop)
    else
      assert_receive {:terminating, later}, 2_000
      send(later, :continue_stop)
    end

    assert :ok = Task.await(stopping, 5_000)
    assert is_pid(two)
  end

  test "using an instance that was not started names the missing instance" do
    assert_raise ArgumentError, ~r/Slap.Yjs.Docs instance .*MissingDocs.* is not started/, fn ->
      Yjs.Docs.delete(doc_id(), docs: Slap.Yjs.Test.MissingDocs)
    end
  end

  test "a join after a delete fails, though the server has not read the deletion" do
    doc_id = doc_id()
    {:ok, pid} = Yjs.Docs.join(Server, doc_id)
    {:ok, _holder} = Peers.suspend_follower(pid)

    :ok = Store.delete_doc(doc_id)
    assert {:error, :deleted} = Yjs.Docs.join(Server, doc_id)
  end

  describe "sync/2" do
    # A server whose follower is suspended stores an update and never reads
    # it back: no sync/2 can finish.
    defp stalled(doc_id) do
      {:ok, pid} = Yjs.Docs.join(Server, doc_id)
      doc = GenServer.call(pid, :doc)
      {:ok, _holder} = Peers.suspend_follower(pid)
      run(pid, fn -> Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, "stored") end)
      pid
    end

    # A caller the server forgot only frees memory: nothing else shows it.
    defp waiting_syncs(pid), do: length(:sys.get_state(pid).assigns[Yjs.DocServer].sync.waiting)

    # The tails the waiting calls wait for (nil: not probed yet).
    defp waiting_tails(pid),
      do: Enum.map(:sys.get_state(pid).assigns[Yjs.DocServer].sync.waiting, &elem(&1, 1))

    test "waits for a client update that reading the store integrates" do
      doc_id = doc_id()
      {:ok, pid} = Yjs.Docs.join(Server, doc_id, flush_after: 60_000, compact_on_stop: false)
      {:ok, holder} = Peers.suspend_follower(pid)

      # yrs keeps p pending after o, p, x; the server integrates it once it
      # reads its append of x back.
      for u <- out_of_order(),
          do: :ok = Server.process_message_v1(pid, <<0, 2>> <> Frame.frame(u), self())

      first = Task.async(fn -> Yjs.DocServer.sync(pid, 5_000) end)
      # x is stored, and the call waits for the server to read it back.
      assert eventually(fn -> match?([tail] when is_integer(tail), waiting_tails(pid)) end)

      # The follower's page, then another call, queued while the server is
      # suspended: the call is handled before what the page integrates.
      :ok = :sys.suspend(pid)
      :erlang.trace(pid, true, [:receive])
      :ok = Peers.resume_follower(holder)
      assert_receive {:trace, ^pid, :receive, {Yjs.Follower, _, {:updates, _, _, _}}}, 5_000
      second = Task.async(fn -> Yjs.DocServer.sync(pid, 5_000) end)
      assert_receive {:trace, ^pid, :receive, {Yjs.DocServer, :request, :sync, _, _}}, 5_000
      :erlang.trace(pid, false, [:receive])
      :ok = :sys.resume(pid)

      assert :ok = Task.await(first)
      assert stored_text(doc_id) == "xp"
      assert :ok = Task.await(second)
    end

    test "a call that times out is forgotten" do
      pid = stalled(doc_id())
      for _ <- 1..5, do: assert({:error, :timeout} = Yjs.DocServer.sync(pid, 50))
      assert waiting_syncs(pid) == 0
    end

    test "a caller that exits is forgotten" do
      pid = stalled(doc_id())
      test = self()

      callers =
        for _ <- 1..5 do
          spawn(fn ->
            send(test, :calling)
            Yjs.DocServer.sync(pid, :infinity)
          end)
        end

      for _ <- callers, do: assert_receive(:calling)
      assert eventually(fn -> waiting_syncs(pid) == 5 end)

      for caller <- callers, do: Process.exit(caller, :kill)
      assert eventually(fn -> waiting_syncs(pid) == 0 end)
    end

    test "a reply after the call timed out does not reach the caller" do
      {pid, _doc} = start(doc_id())
      :ok = :sys.suspend(pid)
      assert {:error, :timeout} = Yjs.DocServer.sync(pid, 50)
      :ok = :sys.resume(pid)

      # The server has answered or forgotten the first call by the time it
      # answers this one.
      :ok = Yjs.DocServer.sync(pid, 5_000)
      {:messages, messages} = Process.info(self(), :messages)
      refute Enum.any?(messages, &match?({ref, _reply} when is_reference(ref), &1))
    end

    # For the server's monitors of callers, set in the server as their
    # requests arrive.
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

  describe "compaction" do
    defp attach_compactions do
      id = {__MODULE__, make_ref()}
      event = [:slap, :yjs, :doc_server, :compact]
      :ok = :telemetry.attach(id, event, &__MODULE__.send_compaction/4, self())
      on_exit(fn -> :telemetry.detach(id) end)
    end

    def send_compaction(_event, measurements, meta, test),
      do: send(test, {:compacted, measurements, meta})

    defp stored_update_bytes(doc_id) do
      {:ok, %{messages: messages}} =
        Streams.read(Store.path(doc_id, :updates), :start, max_bytes: 64 * 1024 * 1024)

      Enum.sum_by(messages, &byte_size(elem(&1, 1)))
    end

    defp snapshot_bytes(doc_id) do
      case Store.snapshots(doc_id) do
        {:ok, []} ->
          0

        {:ok, [%{offset: offset}]} ->
          {:ok, snapshot} = Store.read_snapshot(doc_id, offset)
          byte_size(snapshot)
      end
    end

    test "a failed compaction is tried again once more is read, not at once" do
      doc_id = doc_id()
      test = self()

      fail = fn
        :check ->
          send(test, :attempt)
          raise "compaction fails"

        _ ->
          :ok
      end

      opts = [compact_bytes: 256, flush_after: 1, compact_on_stop: false]
      {pid, doc} = start(doc_id, [test_after_step: fail] ++ opts)
      text = Yex.Doc.get_text(doc, "text")

      capture_log(fn ->
        Yex.Text.insert(text, 0, String.duplicate("a", 300))
        assert_receive :attempt, 5_000
        :ok = Yjs.DocServer.sync(pid, 5_000)
        refute_received :attempt

        Yex.Text.insert(text, 0, "b")
        assert_receive :attempt, 5_000
      end)
    end

    test "a compaction that ends after a reload does not undo the reload's counts" do
      doc_id = doc_id()
      test = self()

      pause = fn
        :check ->
          send(test, {:paused, self()})

          receive do
            :go -> :ok
          end

        _ ->
          :ok
      end

      opts = [compact_bytes: 256, flush_after: 1, compact_on_stop: false]
      {pid, doc} = start(doc_id, [test_after_step: pause] ++ opts)
      text = Yex.Doc.get_text(doc, "text")
      run(pid, fn -> Yex.Text.insert(text, 0, String.duplicate("a", 300)) end)
      assert_receive {:paused, task}, 5_000

      # Another server stores an update and a newer snapshot, which trims
      # what this one has not read: its follower reloads.
      {:ok, holder} = Peers.suspend_follower(pid)
      other = Yex.Doc.new()
      Yex.Text.insert(Yex.Doc.get_text(other, "other"), 0, "remote")
      remote = Yex.encode_state_as_update!(other)
      {:ok, tail} = Store.append(doc_id, Frame.frame(remote))
      state = run(pid, fn -> Yex.encode_state_as_update!(doc) end)
      {:ok, snapshot} = Yex.merge_updates([state, remote])
      :ok = Store.snapshot(doc_id, tail, snapshot)

      :erlang.trace(pid, true, [:receive])
      :ok = Peers.resume_follower(holder)
      assert_receive {:trace, ^pid, :receive, {Yjs.Follower, _, {:reload, _}}}, 5_000
      :erlang.trace(pid, false, [:receive])
      # Handled after the reload.
      :sys.get_state(pid)

      # The paused compaction is superseded, after the reload.
      send(task, :go)
      :ok = Yjs.DocServer.sync(pid, 5_000)

      run(pid, fn -> Yex.Text.insert(text, 0, String.duplicate("b", 300)) end)
      assert_receive {:paused, task}, 5_000
      send(task, :go)
    end

    test "starts once enough is stored, and runs on stop" do
      attach_compactions()
      doc_id = doc_id()
      {_pid, doc} = start(doc_id, compact_bytes: 256, flush_after: 1)
      text = Yex.Doc.get_text(doc, "text")
      Yex.Text.insert(text, 0, String.duplicate("a", 300))
      assert_receive {:compacted, _, %{doc_id: ^doc_id}}, 5_000

      Yex.Text.insert(text, 0, "b")
      stop_supervised!(Server)
      assert_receive {:compacted, _, %{doc_id: ^doc_id}}, 5_000
      # The snapshot is at the offset the server had read up to; its own last
      # update may still follow it, and is applied again on load (harmless).
      assert {:ok, %{snapshot: <<_, _::binary>>}} = Store.load(doc_id)

      {_pid, doc} = start(doc_id)
      assert text(doc) == "b" <> String.duplicate("a", 300)
    end

    test "keeps the updates made while it runs" do
      attach_compactions()
      test = self()
      doc_id = doc_id()

      # Pauses after indexing the new snapshot, before trimming.
      pause = fn
        :index ->
          send(test, {:paused, self()})
          assert_receive :go, 5_000

        _ ->
          :ok
      end

      opts = [compact_bytes: 256, flush_after: 1, compact_on_stop: false]
      {_pid, doc} = start(doc_id, [test_after_step: pause] ++ opts)
      text = Yex.Doc.get_text(doc, "text")
      Yex.Text.insert(text, 0, String.duplicate("a", 300))
      assert_receive {:paused, task}, 5_000

      # The server serves while the compaction runs; wait until the new
      # update is stored.
      {:ok, %{offset: tail}} = Store.load(doc_id)
      Yex.Text.insert(text, 300, "during")

      case Streams.wait(Store.path(doc_id, :updates), tail) do
        {:ok, :data} -> :ok
        {:ok, {:waiting, %{ref: ref}}} -> assert_receive {:slap_streams_wake, ^ref, :data}, 5_000
      end

      send(task, :go)
      assert_receive {:compacted, _, _}, 5_000
      stop_supervised!(Server)

      {_pid, doc} = start(doc_id, compact_on_stop: false)
      assert text(doc) == String.duplicate("a", 300) <> "during"
    end

    for step <- [:check, :snapshot, :index, :delete, :trim] do
      test "a server killed after the #{step} step reloads its document" do
        step = unquote(step)
        doc_id = doc_id()

        # The task's caller is the server; the task is linked to it.
        kill = fn
          ^step -> Process.exit(hd(Process.get(:"$callers")), :kill)
          _ -> :ok
        end

        opts = [compact_bytes: 256, flush_after: 1, test_after_step: kill]
        {pid, doc} = start(doc_id, opts)
        ref = Process.monitor(pid)
        Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, String.duplicate("a", 300))
        assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 5_000

        {_pid, doc} = start(doc_id)
        assert text(doc) == String.duplicate("a", 300)
      end
    end

    test "stored updates stay bounded under continuous edits" do
      attach_compactions()
      doc_id = doc_id()
      {pid, doc} = start(doc_id, compact_bytes: 2048, flush_after: 1)
      text = Yex.Doc.get_text(doc, "text")

      for _round <- 1..20 do
        for _ <- 1..50, do: Yex.Text.insert(text, 0, String.duplicate("x", 20))
        :ok = Yjs.DocServer.sync(pid, 5_000)
        # A compaction starts at 2 KiB, or half the snapshot, and has finished.
        assert stored_update_bytes(doc_id) < max(2048, div(snapshot_bytes(doc_id), 2))
      end

      assert_received {:compacted, _, _}
      stop_supervised!(Server)
      assert stored_update_bytes(doc_id) == 0
      assert {:ok, [_]} = Store.snapshots(doc_id)

      {_pid, doc} = start(doc_id)
      assert text(doc) == String.duplicate("x", 20 * 50 * 20)
    end
  end

  @tag :s3
  test "a document survives a restart on RustFS" do
    doc_id = doc_id()
    {_pid, doc} = start(doc_id)
    Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, "on object storage")
    stop_supervised!(Server)

    {_pid, doc} = start(doc_id)
    assert text(doc) == "on object storage"
  end

  defp collect_appends(acc \\ []) do
    receive do
      {:append, measurements, _} -> collect_appends([measurements | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
