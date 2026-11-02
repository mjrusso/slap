defmodule Slap.Cluster.DiscoveryTest.Served do
  use Slap.Cluster, otp_app: :slap_cluster
end

defmodule Slap.Cluster.DiscoveryTest.Plain do
  use Slap.Cluster, otp_app: :slap_cluster
end

defmodule Slap.Cluster.DiscoveryTest.Children do
  @behaviour Slap.Cluster.ShardChildren

  @impl true
  def child_specs(_ctx), do: []

  @impl true
  def services, do: [:example]
end

defmodule Slap.Cluster.DiscoveryTest do
  # Starts named clusters; clusters/0 lists every cluster on the node.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Slap.Cluster.DiscoveryTest.{Children, Plain, Served}
  alias Slap.Cluster.Strategy.Static

  defp discovered(cluster), do: Enum.find(Slap.Cluster.clusters(), &(&1.cluster == cluster))

  test "lists application-defined clusters with their shards and services" do
    start_supervised!(
      {Served, store: :memory, shards: 3, shard_children: {Children, :child_specs, []}}
    )

    start_supervised!({Plain, store: :memory, shards: 2})

    assert %{shards: 3, local_shards: [0, 1, 2], services: [:example]} = discovered(Served)
    assert %{shards: 2, local_shards: [0, 1], services: []} = discovered(Plain)

    listed = for %{cluster: cluster} <- Slap.Cluster.clusters(), do: cluster
    assert listed == Enum.sort(listed)
  end

  test "lists a cluster that owns no shards on this node" do
    start_supervised!(
      {Served,
       store: :memory,
       shards: 2,
       shard_children: {Children, :child_specs, []},
       strategy: {Static, assignments: %{:"elsewhere@127.0.0.1" => [0..1]}}}
    )

    assert %{shards: 2, local_shards: [], services: [:example]} = discovered(Served)
  end

  test "a stopped cluster is not listed, and is listed again after a restart" do
    start_supervised!({Plain, store: :memory, shards: 1})
    assert discovered(Plain)

    :ok = stop_supervised(Plain)
    refute discovered(Plain)

    start_supervised!({Plain, store: :memory, shards: 1})
    assert %{shards: 1} = discovered(Plain)
  end
end
