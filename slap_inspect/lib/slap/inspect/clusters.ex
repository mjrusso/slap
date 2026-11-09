defmodule Slap.Inspect.Clusters do
  @moduledoc false

  alias Slap.Inspect.{Operation, Remote}

  def discover(entry), do: Remote.call(entry, Slap.Cluster, :clusters, [])

  def load(entry, nil), do: discover(entry)

  def load(entry, cluster) do
    with {:ok, clusters} <- discover(entry), do: named(clusters, cluster)
  end

  defp named(clusters, cluster) do
    case Enum.find(clusters, &(&1.cluster == cluster)) do
      nil -> {:error, {:cluster_not_running, inspect(cluster)}}
      found -> {:ok, [found]}
    end
  end

  @doc "Checks whether a cluster or menu entry provides the service needed by `view`."
  def serves?(%{services: services}, view) do
    case Operation.service(view) do
      :any -> true
      service -> service in services
    end
  end

  def views(cluster), do: Enum.filter(Operation.views(), &serves?(cluster, &1))

  @spec menu([map()]) :: [
          %{
            cluster: module(),
            shards: pos_integer() | nil,
            services: [atom()],
            items: [%{view: atom(), key: String.t() | nil}]
          }
        ]
  def menu(clusters) do
    {groups, _count} =
      Enum.map_reduce(clusters, 0, fn cluster, count ->
        views = views(cluster)

        items =
          for {view, number} <- Enum.with_index(views, count + 1),
              do: %{view: view, key: if(number <= 9, do: Integer.to_string(number))}

        group = %{
          cluster: cluster.cluster,
          shards: cluster[:shards],
          services: cluster.services,
          items: items
        }

        {group, count + length(views)}
      end)

    groups
  end

  def find(menu, key) do
    Enum.find_value(menu, fn group ->
      Enum.find_value(group.items, &(&1.key == key and {group.cluster, &1.view}))
    end)
  end

  @doc "The one cluster that can serve `view`, for output that names no cluster."
  def only(clusters, view) do
    case candidates(clusters, view) do
      {:ok, [cluster]} -> {:ok, cluster}
      {:ok, several} -> {:error, {:ambiguous_cluster, view, Enum.map(several, &inspect/1)}}
      error -> error
    end
  end

  @doc "The first cluster that can serve `view`, for the terminal, which shows the others."
  def first(clusters, view) do
    with {:ok, [cluster | _]} <- candidates(clusters, view), do: {:ok, cluster}
  end

  defp candidates([], _view), do: {:error, :no_clusters}

  defp candidates(clusters, view) do
    case for(cluster <- clusters, serves?(cluster, view), do: cluster.cluster) do
      [] -> {:error, {:no_cluster_for_view, view}}
      found -> {:ok, found}
    end
  end
end
