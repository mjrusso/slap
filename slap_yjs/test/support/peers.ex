defmodule Slap.Yjs.Test.Peers do
  @moduledoc false
  # Other BEAM nodes (OS processes) for the multi-node tests, started with
  # :peer on this machine and connected with distributed Erlang. Each runs
  # Slap.Streams.Cluster (the Distributed strategy, on a shared local directory) and
  # Slap.Yjs.Docs. Everything they run is in test/support, which they load from
  # this node's code paths.

  alias Slap.Cluster.Peers, as: ClusterPeers
  alias Slap.Cluster.Strategy.Distributed
  alias Slap.SlateDB
  alias Slap.Streams
  alias Slap.Yjs

  @doc "Makes this node distributed, once."
  def distribute! do
    unless Node.alive?() do
      {_, 0} = System.cmd("epmd", ["-daemon"])
      {:ok, _} = Node.start(:"primary-#{System.unique_integer([:positive])}@127.0.0.1")
    end

    # As the peers': nodes with different tick times disconnect spuriously.
    :net_kernel.set_net_ticktime(4)

    :ok
  end

  @doc """
  Starts a node running Slap.Streams.Cluster on `dir` and Slap.Yjs.Docs. Returns it. Its
  `net_ticktime` is 4 s, so a node paused longer is taken for gone and its
  shards move.
  """
  def start(dir) do
    paths =
      [~c"-kernel", ~c"net_ticktime", ~c"4"] ++
        Enum.flat_map(:code.get_path(), &[~c"-pa", &1])

    {:ok, _peer, node} =
      :peer.start(%{
        name: :"yjs-#{System.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", Atom.to_charlist(Node.get_cookie()) | paths],
        connection: :standard_io
      })

    :ok = :erpc.call(node, __MODULE__, :start_services, [dir, Node.list() -- [node]])
    node
  end

  @doc false
  def start_services(dir, nodes) do
    Logger.configure(level: :error)
    {:ok, _} = Application.ensure_all_started(:slap_yjs)
    # Reconnects after a pause or a partition, as libcluster would. Each
    # node keeps its connections to the nodes started before it.
    {:ok, peers} = ClusterPeers.start_link(nodes)
    Process.unlink(peers)
    for node <- nodes, do: Node.connect(node)
    SlateDB.set_log_level(:none)

    opts = [
      store: {:local, dir},
      shards: 4,
      strategy: {Distributed, interval: 200, settle: 300},
      settings: %{flush_interval: "2ms"}
    ]

    {:ok, cluster} = Streams.Cluster.start_link(opts)
    {:ok, docs} = Yjs.Docs.start_link()
    Process.unlink(cluster)
    Process.unlink(docs)
    :ok
  end

  @doc "SIGKILLs the node's OS process."
  def kill(node), do: signal(os_pid(node), "KILL")

  @doc "The node's OS process id, for `pause/1` and `resume/1`."
  def os_pid(node), do: :erpc.call(node, System, :pid, [])

  @doc "SIGSTOPs, then SIGCONTs, an OS process."
  def pause(os_pid), do: signal(os_pid, "STOP")
  def resume(os_pid), do: signal(os_pid, "CONT")

  defp signal(os_pid, sig) do
    {_, 0} = System.cmd("kill", ["-#{sig}", os_pid])
    :ok
  end

  @doc "How many snapshots the document has (`Slap.Yjs.Store.snapshots/1`), read on `node`."
  def snapshots(node, doc_id) do
    {:ok, history} = :erpc.call(node, Yjs.Store, :snapshots, [doc_id])
    length(history)
  end

  @doc "The awareness client ids known to the document's server on `node`."
  def awareness_ids(node, doc_id) do
    :erpc.call(node, fn ->
      case Yjs.Docs.whereis(Yjs.Test.Server, doc_id) do
        nil -> []
        pid -> GenServer.call(pid, :awareness_ids)
      end
    end)
  end

  @doc "The node that owns the document's shard, as `node` sees it."
  def owner(node, doc_id) do
    :erpc.call(node, fn ->
      key = Streams.placement_key(Yjs.Store.path(doc_id, :updates))

      case Streams.Cluster.lookup(Streams.Cluster.shard_for(key)) do
        {:ok, {:local, _}} -> node
        {:ok, {:remote, owner}} -> owner
        {:error, _} = error -> error
      end
    end)
  end

  @doc "Kills the node if it is still up."
  def kill_if_up(node) do
    kill(node)
  catch
    _, _ -> :ok
  end

  @doc """
  Joins the document's server on `node`, starting it with `opts`, from a
  process there that stays subscribed while it runs. Returns the server.
  """
  def join(node, doc_id, opts \\ []) do
    ref = make_ref()
    Node.spawn(node, __MODULE__, :hold, [self(), ref, doc_id, opts])

    receive do
      {^ref, server} -> server
    after
      30_000 -> exit(:join_timeout)
    end
  end

  @doc false
  def hold(test, ref, doc_id, opts) do
    {:ok, server} = Yjs.Docs.join(Yjs.Test.Server, doc_id, opts)
    monitor = Process.monitor(server)
    send(test, {ref, server})

    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    end
  end

  @doc "Inserts `text` at the start of the server's document, on its node."
  def insert(server, text),
    do: :erpc.call(node(server), __MODULE__, :insert_local, [server, text])

  @doc false
  def insert_local(server, text) do
    doc = GenServer.call(server, :doc)
    Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, text)
  end

  @doc """
  Suspends the follower of a server started by Slap.Yjs.Docs, on its node,
  until the server stops or `resume_follower/1`, so that the server does not
  read what is stored meanwhile. Returns `{:ok, holder}`, for
  `resume_follower/1`.
  """
  def suspend_follower(server) do
    ref = make_ref()
    holder = Node.spawn(node(server), __MODULE__, :suspend_local, [self(), ref, server])

    receive do
      {^ref, :suspended} -> {:ok, holder}
    after
      5_000 -> exit(:suspend_timeout)
    end
  end

  @doc "Resumes a follower `suspend_follower/1` suspended."
  def resume_follower(holder) do
    monitor = Process.monitor(holder)
    send(holder, :resume)

    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    end
  end

  # A suspension lasts while the process that made it lives.
  @doc false
  def suspend_local(test, ref, server) do
    monitor = Process.monitor(server)
    follower = :sys.get_state(server).assigns[Yjs.DocServer].follower
    true = :erlang.suspend_process(follower)
    send(test, {ref, :suspended})

    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
      :resume -> :ok
    end
  end

  @doc "Kills the document's server on `node`, if it runs."
  def kill_server(node, doc_id) do
    :erpc.call(node, fn ->
      case Yjs.Docs.whereis(Yjs.Test.Server, doc_id) do
        nil -> :none
        pid -> Process.exit(pid, :kill)
      end
    end)
  end

  @doc "Waits until every server of the document has stored and read everything."
  def sync_all(nodes, doc_id), do: Enum.map(nodes, &:erpc.call(&1, __MODULE__, :sync, [doc_id]))

  @doc false
  def sync(doc_id) do
    case Yjs.Docs.whereis(Yjs.Test.Server, doc_id) do
      nil -> :ok
      pid -> Yjs.DocServer.sync(pid, 2_000)
    end
  end

  @doc "The document's text as stored, loaded on `node`."
  def stored_text(node, doc_id) do
    :erpc.call(node, fn ->
      {:ok, loaded} = Yjs.Store.load(doc_id)
      doc = Yex.Doc.new()
      for u <- List.wrap(loaded.snapshot) ++ loaded.updates, do: :ok = Yex.apply_update(doc, u)
      # As a server does: yrs does not retry what a gap it skipped held.
      {:ok, pending} = Yex.Doc.prune_pending(doc)
      if pending, do: :ok = Yex.apply_update(doc, pending)
      doc |> Yex.Doc.get_text("text") |> Yex.Text.to_string()
    end)
  end
end
