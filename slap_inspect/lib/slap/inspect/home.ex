defmodule Slap.Inspect.Home do
  @moduledoc false

  alias Slap.Inspect.{Clusters, Request, ShardRows}

  # Raw counts overlap the service views and include internal bookkeeping rows.
  @uncounted [:raw]

  @doc """
  Returns one row per cluster view with row counts per shard, plus the
  clusters found on the entry node. Clusters and views are read in order
  until `deadline`, so later ones can be left uncounted.
  """
  def page(connection, deadline) do
    with {:ok, clusters} <- Clusters.discover(connection.entry) do
      rows =
        for cluster <- clusters,
            connection = %{connection | cluster: cluster.cluster},
            shards = ShardRows.assigned(connection, deadline),
            view <- Clusters.views(cluster),
            do: row(cluster, view, counts(connection, view, shards, deadline))

      {:ok, %{rows: rows, cursor: nil, clusters: clusters}}
    end
  end

  defp counts(_connection, view, _shards, _deadline) when view in @uncounted, do: nil
  defp counts(_connection, _view, {:error, _reason}, _deadline), do: nil

  defp counts(connection, view, {:ok, shards}, deadline) do
    {:ok, request} = Request.new(%{view: view, shard: nil, budget: nil, opts: []})
    ShardRows.count(connection, request, shards, deadline)
  end

  defp row(cluster, view, counts) do
    %{
      key: "#{inspect(cluster.cluster)} #{view}",
      value: <<>>,
      decoded:
        {:ok,
         %{
           kind: :cluster_view,
           cluster: cluster.cluster,
           view: view,
           shards: cluster[:shards],
           counts: counts
         }}
    }
  end
end
