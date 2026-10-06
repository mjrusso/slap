defmodule Slap.Cluster.Supervisor do
  @moduledoc false
  # The top of a cluster's tree on one node:
  #
  #     <Cluster>.Supervisor (one_for_all)
  #     ├─ Registry            shard contexts and per-shard names
  #     ├─ Probe               checks conditional writes during startup
  #     ├─ ShardSupervisors    DynamicSupervisor of shard supervisors
  #     ├─ Host                starts, stops and watches shards
  #     └─ Strategy            decides which shards run here
  #
  # Shutdown runs bottom-up: the strategy stops placing shards, the host
  # marks them as stopping, then every shard stops (application children,
  # then the database close). If any of these crashes, everything restarts,
  # since the host's view of the running shards would be lost.

  use Supervisor

  alias Slap.Cluster.{Config, Host, Probe}
  alias Slap.SlateDB.Cache

  def start_link(cluster, otp_app, opts, defaults \\ []) do
    # Loaded here so that a bad option raises in the caller.
    config = Config.load(cluster, otp_app, opts, defaults)
    Supervisor.start_link(__MODULE__, config, name: Module.concat(cluster, Supervisor))
  end

  @impl true
  def init(%Config{cluster: cluster} = config) do
    db_cache =
      case config.cache do
        nil -> nil
        :disabled -> :disabled
        cache_opts -> Cache.new(Keyword.fetch!(cache_opts, :capacity_bytes))
      end

    Config.put(%{config | db_cache: db_cache})
    {strategy, strategy_opts} = config.strategy

    children = [
      {Registry, keys: :unique, name: Slap.Cluster.registry(cluster)},
      %{id: Probe, start: {Probe, :run, [config]}},
      # A dynamic supervisor starts and stops its children one at a time, and
      # a shard's database opens (and closes) in its child: partitions by
      # shard let different shards open and close at once.
      {PartitionSupervisor,
       child_spec: {DynamicSupervisor, strategy: :one_for_one},
       name: Slap.Cluster.shard_supervisors(cluster),
       partitions: min(config.shards, 64)},
      %{id: Host, start: {Host, :start_link, [cluster]}},
      strategy.child_spec(Keyword.put(strategy_opts, :cluster, cluster))
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
