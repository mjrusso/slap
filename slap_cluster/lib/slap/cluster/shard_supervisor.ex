defmodule Slap.Cluster.ShardSupervisor do
  @moduledoc false
  # One per shard placed on this node:
  #
  #     ShardSupervisor (rest_for_one)
  #     ├─ ShardDb        opens the database; closes it last
  #     └─ Children       the application's shard children
  #
  # Children stop first (reverse start order), so they can finish or fail
  # in-flight work while the database is still open. If ShardDb exits, the
  # whole shard stops (it is a significant child) and the host reports it as
  # crashed: the strategy decides whether and where the database reopens,
  # since reopening it here could fence a node the strategy has since given
  # the shard to.

  use Supervisor

  alias Slap.Cluster.{Config, ShardDb}

  def child_spec({_cluster, n, _generation, _status} = arg) do
    %{
      id: {__MODULE__, n},
      start: {__MODULE__, :start_link, [arg]},
      type: :supervisor,
      # The host decides whether a shard comes back, not the supervisor.
      restart: :temporary
    }
  end

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg)

  @impl true
  def init({cluster, n, _generation, _status} = arg) do
    config = Config.get(cluster)
    shard_db = Supervisor.child_spec({ShardDb, arg}, restart: :temporary, significant: true)
    children = [shard_db] ++ children(config, cluster, n)
    Supervisor.init(children, strategy: :rest_for_one, auto_shutdown: :any_significant)
  end

  defp children(%Config{shard_children: nil}, _cluster, _n), do: []

  defp children(_config, cluster, n) do
    [
      %{
        id: __MODULE__.Children,
        start: {__MODULE__, :start_children, [cluster, n]},
        type: :supervisor
      }
    ]
  end

  @doc false
  # Starts the application's children, with the context ShardDb registered
  # just before.
  def start_children(cluster, n) do
    [{_pid, ctx}] = Registry.lookup(Slap.Cluster.registry(cluster), {:shard, n})

    specs =
      case Config.get(cluster).shard_children do
        {m, f, a} -> apply(m, f, [ctx | a])
        fun when is_function(fun, 1) -> fun.(ctx)
      end

    Supervisor.start_link(specs, strategy: :one_for_one)
  end
end
