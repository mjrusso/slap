defmodule Slap.Yjs.Docs do
  @moduledoc """
  The document servers of this node, one per `{module, doc_id}`, started on
  the first `join/3` and stopped after `:idle_timeout` without subscribers.
  Each node that has clients for a document runs its own server; the
  servers share the document through its `.updates` stream and presence
  through a `:pg` group (see `Slap.Yjs.DocServer`).

  Invalid control options and unknown option names raise `ArgumentError`.

  Start it after the Streams cluster, so document servers can store their
  buffers during shutdown. `:name` and `:prefix` select a document-server
  instance on the built-in Streams cluster:

      children = [Slap.Streams.Cluster, Slap.Yjs.Docs]
      children = [Slap.Streams.Cluster,
                  {Slap.Yjs.Docs, name: MyDocs, prefix: "/my/streams"}]
      Slap.Yjs.Docs.join(MyDocServer, {"service", "document"}, docs: MyDocs)

  For an application-owned Streams cluster defined with
  `use Slap.Streams.Cluster`, pass `cluster: MyApp.StreamsCluster` when
  starting Docs. Run one Streams cluster per VM.
  """

  use Supervisor

  alias Slap.Yjs
  alias Slap.Yjs.Docs.Config

  @join_attempts 3
  @start_options [:name, :prefix, :cluster]

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    validate_options!(opts, @start_options)

    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Starts a document server supervisor. `:name` selects the instance name;
  `:cluster` selects its Streams cluster (see `Slap.Streams.Cluster`);
  `:prefix` prefixes stored paths.
  """
  def start_link(opts \\ []) do
    validate_options!(opts, @start_options)
    validate_control_opts!(opts)
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl Supervisor
  def init(opts) do
    validate_options!(opts, @start_options)
    name = Keyword.get(opts, :name, __MODULE__)
    store = Keyword.take(opts, [:prefix, :cluster])

    children = [
      %{id: :pg, start: {:pg, :start_link, [pg(name)]}},
      {Registry, keys: :unique, name: registry(name)},
      {DynamicSupervisor, name: supervisor(name), strategy: :one_for_one},
      %{id: :config, start: {Config, :start_link, [[name: name, store: store]]}}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc """
  Subscribes the caller to the document's server on this node, starting it
  with `opts` (`Slap.Yjs.DocServer` options; `:idle_timeout` defaults to 30 s)
  if it is not running. Server options, and `:assigns` (a map of the
  server's initial assigns), apply only when the server starts. The caller
  then gets the messages described in `Slap.Yjs.DocServer`, and should
  monitor the returned pid: if the server stops, join again and
  resynchronise. `{:error, :deleted}` if the document was deleted
  (`delete/2`). `:docs` selects a named Docs instance; `:timeout` bounds
  loading and subscribing (default 5 s).
  """
  @spec join(module(), Yjs.Store.doc(), keyword()) :: {:ok, pid()} | {:error, term()}
  def join(module, doc_id, opts \\ []) do
    validate_options!(opts, [:docs, :timeout, :assigns] ++ Yjs.DocServer.option_keys())
    validate_control_opts!(opts)
    Yjs.DocServer.validate_options!(opts)
    unless is_atom(module) and module != nil, do: raise(ArgumentError, "module must be a module")
    {name, opts} = Keyword.pop(opts, :docs, __MODULE__)

    with :ok <- Yjs.Store.check_doc(doc_id),
         do: join(name, module, doc_id, opts, @join_attempts)
  end

  defp join(name, _module, doc_id, _opts, 0) do
    case Yjs.Store.tail(doc_id, Config.get(name)) do
      {:error, :deleted} -> {:error, :deleted}
      _ -> {:error, :unavailable}
    end
  end

  defp join(name, module, doc_id, opts, attempts) do
    with {:ok, pid} <- whereis_or_start(name, module, doc_id, opts) do
      case Yjs.DocServer.subscribe(pid, self(), Keyword.get(opts, :timeout, 5_000)) do
        :ok ->
          checked(pid, doc_id, name)

        {:error, {:down, {:shutdown, :deleted}}} ->
          {:error, :deleted}

        {:error, {:down, {:slap_yjs_load_failed, reason}}} ->
          {:error, {:slap_yjs_load_failed, reason}}

        # Stopping (idle) as we joined: start another.
        {:error, {:down, _}} ->
          join(name, module, doc_id, opts, attempts - 1)

        {:error, _} = error ->
          error
      end
    end
  end

  # A server learns of a deletion from its follower, which may lag behind:
  # the store says. A delete closes the document's stream before anything
  # else, so a join that starts after it returns finds it closed.
  defp checked(pid, doc_id, name) do
    case Yjs.Store.tail(doc_id, Config.get(name)) do
      {:ok, _tail} ->
        {:ok, pid}

      {:error, _} = error ->
        _ = leave(pid)
        error
    end
  end

  @doc """
  Deletes the document for good (`Slap.Yjs.Store.delete_doc/1`), then stops
  its servers on every node with `{:shutdown, :deleted}`, waiting up to
  `:timeout` ms (default 5 s) for them. `:docs` selects a named instance.
  Joins return `{:error, :deleted}` from then on, so a deleted document's
  id cannot be reused. A
  server that does not stop in time (on a node that cannot be reached)
  stops once its follower finds the document deleted, and cannot store
  anything meanwhile.
  """
  @spec delete(Yjs.Store.doc(), keyword()) :: :ok | {:error, term()}
  def delete(doc_id, opts \\ []) do
    validate_options!(opts, [:timeout, :docs])
    validate_control_opts!(opts, false)
    name = Keyword.get(opts, :docs, __MODULE__)
    timeout = Keyword.get(opts, :timeout, 5_000)

    with :ok <- Yjs.Store.delete_doc(doc_id, Config.get(name)),
         do: Yjs.DocServer.stop_deleted(doc_id, timeout, pg(name))
  end

  @doc "Unsubscribes the caller. `:timeout` defaults to 5,000 ms."
  @spec leave(pid(), keyword()) :: :ok | {:error, term()}
  def leave(pid, opts \\ []) do
    validate_options!(opts, [:timeout])
    validate_control_opts!(opts)
    unless is_pid(pid), do: raise(ArgumentError, "pid must be a pid")
    Yjs.DocServer.unsubscribe(pid, self(), Keyword.get(opts, :timeout, 5_000))
  end

  @doc "The document's server on this node, if it has finished loading."
  @spec whereis(module(), Yjs.Store.doc(), keyword()) :: pid() | nil
  def whereis(module, doc_id, opts \\ []) do
    validate_options!(opts, [:docs])
    validate_control_opts!(opts)
    unless is_atom(module) and module != nil, do: raise(ArgumentError, "module must be a module")
    name = Keyword.get(opts, :docs, __MODULE__)

    case Registry.lookup(registry(name), {module, doc_id}) do
      [{pid, :ready}] -> pid
      _ -> nil
    end
  end

  defp whereis_or_start(docs, module, doc_id, opts) do
    name = {:via, Registry, {registry(docs), {module, doc_id}}}
    store = Config.get(docs)

    arg =
      [idle_timeout: 30_000]
      |> Keyword.merge(Keyword.delete(opts, :timeout))
      |> Keyword.merge(
        doc_id: doc_id,
        store: store,
        pg: pg(docs),
        __slap_yjs_defer_load__: true,
        __slap_yjs_registry__: registry(docs)
      )

    spec = %{
      id: module,
      start: {module, :start_link, [arg, [name: name]]},
      restart: :temporary,
      shutdown: Yjs.DocServer.shutdown_timeout(arg)
    }

    case DynamicSupervisor.start_child(supervisor(docs), spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, {:shutdown, :deleted}} -> {:error, :deleted}
      {:error, _} = error -> error
    end
  end

  defp registry(name), do: Module.concat(name, Registry)
  defp supervisor(name), do: Module.concat(name, Supervisor)
  defp pg(__MODULE__), do: Yjs.DocServer.pg_scope()
  defp pg(name), do: Module.concat(name, PG)

  defp validate_options!(opts, allowed) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")
    Keyword.validate!(opts, allowed)
  end

  defp validate_control_opts!(opts, allow_infinity \\ true) do
    for key <- [:name, :docs, :cluster],
        do: validate_option!(opts, key, &module?/1, "a module")

    validate_option!(opts, :prefix, &valid_prefix?/1, "a path beginning with /")

    expected =
      if allow_infinity, do: "a non-negative integer or :infinity", else: "a non-negative integer"

    validate_option!(opts, :timeout, &valid_timeout?(&1, allow_infinity), expected)
  end

  defp validate_option!(opts, key, valid?, expected) do
    if Keyword.has_key?(opts, key) and not valid?.(opts[key]),
      do: raise(ArgumentError, "#{inspect(key)} must be #{expected}")
  end

  defp module?(value), do: is_atom(value) and value != nil
  defp valid_prefix?(value), do: is_binary(value) and String.starts_with?(value, "/")

  defp valid_timeout?(value, allow_infinity),
    do: (allow_infinity and value == :infinity) or (is_integer(value) and value >= 0)
end
