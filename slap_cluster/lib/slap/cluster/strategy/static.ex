defmodule Slap.Cluster.Strategy.Static do
  @moduledoc """
  A fixed assignment of shards to nodes, from configuration: no database,
  no leases, no failover. A node opens its own shards (and reopens them
  after a backoff if they fail, like `Slap.Cluster.Strategy.Local`), and
  routes to the others by the assignment. If a node is down, its shards are
  unavailable until it is back.

  It suits a Kubernetes StatefulSet (stable names, the platform restarts a
  failed pod) or any small fixed cluster. Changing the assignment moves
  shards; do it with the nodes stopped, or accept that two nodes may open a
  shard for a moment (SlateDB fences one of them).

  ## Options (one of)

    * `:nodes` - an ordered list of node names; shard `n` belongs to
      `Enum.at(nodes, rem(n, length(nodes)))`. For a StatefulSet:
      `nodes: for i <- 0..2, do: :"app@app-\#{i}.app-headless.ns.svc"`.
    * `:assignments` - `%{node => [shard | Range.t()]}`, which must cover
      every shard exactly once.

  Plus `Local`'s `:backoff_base`, `:backoff_max` and `:max_concurrency`.
  Routing goes over distributed Erlang, so the nodes must be connected.
  """

  @behaviour Slap.Cluster.Strategy

  alias Slap.Cluster.Config
  alias Slap.Cluster.Strategy, as: StrategyOptions
  alias Slap.Cluster.Strategy.Local

  @impl true
  def validate_options(opts) do
    StrategyOptions.validate_options!(
      opts,
      [:nodes, :assignments, :backoff_base, :backoff_max, :max_concurrency],
      [:backoff_base, :backoff_max, :max_concurrency]
    )
  end

  @impl true
  def child_spec(opts) do
    cluster = Keyword.fetch!(opts, :cluster)
    owners = owners(cluster, opts)
    :persistent_term.put({__MODULE__, cluster}, owners)
    mine = for {n, owner} <- owners, owner == node(), do: n
    Local.child_spec(Keyword.put(opts, :shards, Enum.sort(mine)))
  end

  @impl true
  def lookup(cluster, n) do
    case :persistent_term.get({__MODULE__, cluster}, %{}) do
      %{^n => owner} when owner != node() -> {:ok, {:remote, owner}}
      %{^n => _me} -> Local.lookup(cluster, n)
      _ -> {:error, :unassigned}
    end
  end

  @impl true
  defdelegate handle_shard_down(cluster, n, reason), to: Local

  defp owners(cluster, opts) do
    shards = Config.get(cluster).shards

    case {opts[:nodes], opts[:assignments]} do
      {[_ | _] = nodes, nil} ->
        Map.new(0..(shards - 1), &{&1, Enum.at(nodes, rem(&1, length(nodes)))})

      {nil, %{} = assignments} ->
        pairs =
          for {node, list} <- assignments,
              item <- list,
              n <- expand(item),
              do: {n, node}

        owners = Map.new(pairs)

        unless length(pairs) == shards and
                 Enum.sort(Map.keys(owners)) == Enum.to_list(0..(shards - 1)),
               do:
                 raise(
                   ArgumentError,
                   "Static :assignments must cover shards 0..#{shards - 1} exactly once"
                 )

        owners

      _ ->
        raise ArgumentError, "#{inspect(__MODULE__)} needs :nodes or :assignments"
    end
  end

  defp expand(%Range{} = range), do: Enum.to_list(range)
  defp expand(n) when is_integer(n), do: [n]
end
