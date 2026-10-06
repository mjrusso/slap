defmodule Slap.Server do
  @moduledoc """
  A standalone HTTP server for Durable Streams, KV, or both. Pass `:streams`
  and/or `:kv` to select the services. At least one is required.

  Add `{Slap.Server, opts}` to an application's supervision tree, or run the
  `mix slap.server` task under Mix. The `slap` application starts no listener
  on boot. The supported setup has one Streams cluster and one KV cluster per VM.

    * `:store` - required, as for `Slap.SlateDB.open/2`: `:memory`,
      `{:local, dir}` or `{:url, "s3://bucket/prefix", options}`. For S3,
      the endpoint and credentials can also come from `AWS_*` variables.
      The streams' shards are under `streams/` in it, KV's under `kv/`.
    * `:streams` - `[shards: n, settings: settings,
      child_options: service_options, http: router_options]`. The shard
      count defaults to 8 and is fixed for the life of the data. See
      `Slap.Streams.Cluster` for service options.
    * `:kv` - the same keys (default 8 shards). See `Slap.KV.Cluster` for
      its `child_options:`.
      Each service's settings default to `%{flush_interval: "10ms"}`.
    * `:port` (default 4437), `:ip` (default `:loopback`) - the listener.
    * `:strategy` - run as one node of a cluster: `{module, opts}`, a
      `Slap.Cluster.Strategy`, for both clusters. The nodes must be
      connected with distributed Erlang; requests for a shard on another
      node are served there (`:erpc`). Without it every shard is on this
      node (`Local`).
    * `:peers` - node names to stay connected to (`Slap.Cluster.Peers`).

  The listener does not authenticate (see the routers).
  """

  alias Slap.{KV, Streams}

  @doc "Starts a supervisor for the configured clusters and listener."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(config), do: Supervisor.start_link(child_specs(config), strategy: :one_for_one)

  @doc "A single supervisor child for the configured server."
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(config) do
    children = child_specs(config)

    %{
      id: __MODULE__,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
      type: :supervisor,
      shutdown: :infinity
    }
  end

  @doc false
  @spec child_specs(keyword()) :: [Supervisor.child_spec()]
  def child_specs(config) do
    config = Keyword.validate!(config, [:store, :streams, :kv, :port, :ip, :strategy, :peers])

    if !config[:streams] and !config[:kv],
      do: raise(ArgumentError, "configure :streams, :kv, or both")

    peers = if config[:peers], do: [{Slap.Cluster.Peers, config[:peers]}], else: []

    streams =
      if config[:streams], do: [{Streams.Cluster, service_opts(config, :streams)}], else: []

    kv = if config[:kv], do: [{KV.Cluster, service_opts(config, :kv)}], else: []
    Enum.concat([peers, streams, kv, [listener(config)]])
  end

  defp service_opts(config, service) do
    allowed = [:shards, :settings, :child_options, :http]
    opts = config |> Keyword.fetch!(service) |> Keyword.validate!(allowed)

    child_allowed =
      if service == :streams,
        do: Streams.Cluster.child_option_keys(),
        else: [:partition_writers]

    child_options = opts |> Keyword.get(:child_options, []) |> Keyword.validate!(child_allowed)

    [
      store: Keyword.fetch!(config, :store),
      path: Atom.to_string(service),
      shards: Keyword.get(opts, :shards, 8),
      settings: Keyword.get(opts, :settings, %{flush_interval: "10ms"}),
      child_options: child_options
    ] ++ optional(:strategy, config[:strategy])
  end

  defp listener(config) do
    router = [
      streams: config[:streams] && Keyword.get(config[:streams], :http, []),
      kv: config[:kv] && Keyword.get(config[:kv], :http, [])
    ]

    opts = [
      plug: {Slap.Router, router},
      port: Keyword.get(config, :port, 4437),
      ip: Keyword.get(config, :ip, :loopback)
    ]

    Supervisor.child_spec({Bandit, opts}, id: {__MODULE__, :listener})
  end

  defp optional(_key, nil), do: []
  defp optional(key, value), do: [{key, value}]
end
