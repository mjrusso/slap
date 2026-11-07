defmodule Slap do
  @moduledoc """
  Runs the Slap services under one supervisor: the Streams cluster, the
  KV cluster, and the services built on them.

  Add `{Slap, opts}` to a supervision tree, or call `start_link/1`:

      children = [
        {Slap,
         store: {:url, "s3://bucket/prefix"},
         streams: [shards: 8],
         kv: [shards: 8],
         files: [],
         yjs: []}
      ]

  Each service is started when its key is present, with a keyword list of
  its options (`[]` for the defaults). At least one cluster is required.

    * `:store` - required, as for `Slap.SlateDB.open/2`: for example
      `:memory`, `{:local, dir}` or `{:url, "s3://bucket/prefix"}`. File
      objects default to `files/` in it; inline bodies are stored in KV.
    * `:streams` - starts `Slap.Streams.Cluster`: `[shards: n, path: path,
      settings: settings, child_options: options]`. A key left out comes
      from the cluster's application config (`config :slap_streams,
      Slap.Streams.Cluster`), and otherwise `:shards` defaults to 8 and
      `:path` to `"streams"`. The shard count is fixed for the life of the
      data. See `Slap.Streams.Cluster` for `child_options:`. Snapshot logs
      (`Slap.SnapshotLog`) need only this cluster.
    * `:kv` - starts `Slap.KV.Cluster`, with the same keys (`config :slap_kv,
      Slap.KV.Cluster`; by default 8 shards under `"kv"`). See
      `Slap.KV.Cluster` for `child_options:`.
    * `:files` - starts `Slap.Files` after the KV cluster; requires `:kv`.
      Takes `Slap.Files`' options except `:cluster`. `:store` defaults to
      the store above.
    * `:yjs` - starts `Slap.Yjs.Docs` after the Streams cluster; requires
      `:streams`. Takes its options except `:cluster`.
    * `:strategy` - `module` or `{module, opts}`, a `Slap.Cluster.Strategy`
      for both clusters, to run as one node of a cluster. The nodes must be
      connected with distributed Erlang; see `:peers`. Without it, or a
      strategy in a cluster's application config, every shard is on this
      node (`Slap.Cluster.Strategy.Local`).
    * `:peers` - a fixed list of node names to stay connected to, through
      `Slap.Cluster.Peers`. Omit it on one node, or if something else
      connects the nodes, such as libcluster.

  `:settings` merge with each cluster's defaults, which set
  `flush_interval` to 10 ms.

  Run one `Slap` per VM: it starts the built-in clusters, and a VM runs at
  most one Streams cluster and one KV cluster. Its supervisor is registered
  as `Slap`, so a second start returns `{:error, {:already_started, pid}}`.
  An application that defines its own cluster modules
  (`use Slap.Streams.Cluster`) supervises them and the services on them
  directly.

  Each cluster and its dependent services run in a separate `:rest_for_one`
  branch: a cluster restart also restarts its services. Repeated service
  failures can restart the cluster; repeated branch failures stop `Slap`.
  The supervisors use OTP's default restart limits.

  `Slap` starts no HTTP listener. `Slap.Router` serves the Streams and KV
  HTTP APIs as one Plug.

  Invalid option keys or service combinations raise `ArgumentError` when
  building the child spec or calling `start_link/1`. Cluster values,
  including `:settings`, are checked at startup. A top-level `nil` is
  treated as missing.
  """

  alias Slap.{Files, KV, Streams, Yjs}

  @cluster_keys [:shards, :path, :settings, :child_options]
  @services [:streams, :kv, :files, :yjs]

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {Supervisor, :start_link, [children(opts), supervisor_opts()]},
      type: :supervisor
    }
  end

  @doc "Starts a supervisor that runs the services listed in `opts`."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts), do: Supervisor.start_link(children(opts), supervisor_opts())

  defp supervisor_opts, do: [strategy: :one_for_one, name: __MODULE__]

  defp children(opts) do
    opts = validate!(opts)
    store = Keyword.fetch!(opts, :store)
    peers = if opts[:peers], do: [{Slap.Cluster.Peers, opts[:peers]}], else: []

    streams =
      branch(
        opts,
        store,
        :streams,
        {Streams.Cluster, :slap_streams},
        service(Yjs.Docs, opts[:yjs], [])
      )

    kv =
      branch(opts, store, :kv, {KV.Cluster, :slap_kv}, service(Files, opts[:files], store: store))

    peers ++ streams ++ kv
  end

  defp validate!(opts) do
    opts =
      opts
      |> Keyword.validate!([:store, :strategy, :peers | @services])
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    if !opts[:store], do: raise(ArgumentError, ":store is required")

    for {key, value} <- Keyword.take(opts, @services),
        not Keyword.keyword?(value),
        do: raise(ArgumentError, "#{inspect(key)} must be a keyword list, got: #{inspect(value)}")

    if !opts[:streams] and !opts[:kv],
      do: raise(ArgumentError, "configure :streams, :kv, or both")

    if opts[:files] && !opts[:kv], do: raise(ArgumentError, ":files requires :kv")
    if opts[:yjs] && !opts[:streams], do: raise(ArgumentError, ":yjs requires :streams")
    opts
  end

  defp branch(opts, store, service, cluster, dependents) do
    case opts[service] do
      nil ->
        []

      _ ->
        {module, _otp_app} = cluster
        children = [{module, cluster_opts(opts, store, service, cluster)} | dependents]

        [
          %{
            id: {__MODULE__, service},
            start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]},
            type: :supervisor
          }
        ]
    end
  end

  defp cluster_opts(opts, store, service, {module, otp_app}) do
    service_opts = opts |> Keyword.fetch!(service) |> Keyword.validate!(@cluster_keys)

    for {:child_options, child_options} <- service_opts,
        do: Keyword.validate!(child_options, module.child_option_keys())

    env = Application.get_env(otp_app, module, [])

    defaults =
      for {key, value} <- [path: Atom.to_string(service), shards: 8],
          not Keyword.has_key?(env, key),
          do: {key, value}

    [store: store] ++
      Keyword.merge(defaults, service_opts) ++ Keyword.take(opts, [:strategy])
  end

  defp service(_module, nil, _defaults), do: []

  defp service(module, opts, defaults) do
    if Keyword.has_key?(opts, :cluster),
      do:
        raise(ArgumentError, "#{inspect(module)} runs on Slap's cluster; :cluster is not allowed")

    [module.child_spec(Keyword.merge(defaults, opts))]
  end
end
