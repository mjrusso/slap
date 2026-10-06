defmodule JepsenNode.Readiness do
  @moduledoc false

  @spec setup([node()]) :: :ok
  def setup(nodes), do: :persistent_term.put(__MODULE__, Enum.sort(nodes))

  @spec ready?() :: boolean()
  def ready? do
    nodes = :persistent_term.get(__MODULE__)

    if Enum.sort([node() | Node.list()]) == nodes do
      case :rpc.multicall(nodes, __MODULE__, :assignments, [], 5_000) do
        {views, []} when length(views) == length(nodes) ->
          Enum.all?(views, &valid?(&1, nodes)) and length(Enum.uniq(views)) == 1

        _ ->
          false
      end
    else
      false
    end
  end

  @spec assignments() :: map()
  def assignments do
    %{
      streams: locations(Slap.Streams.Cluster.assignments()),
      kv: locations(Slap.KV.Cluster.assignments())
    }
  end

  defp locations(assignments) do
    Map.new(assignments, fn
      {shard, {:local, _}} -> {shard, node()}
      {shard, {:remote, owner}} -> {shard, owner}
      {shard, :unassigned} -> {shard, :unassigned}
    end)
  end

  defp valid?(%{streams: streams, kv: kv}, nodes) do
    map_size(streams) > 0 and map_size(kv) > 0 and
      Enum.all?(Map.values(streams) ++ Map.values(kv), &(&1 in nodes))
  end

  defp valid?(_, _nodes), do: false
end
