defmodule Slap.Cluster.ShardChildren do
  @moduledoc """
  Starts application processes for a shard after its database opens.

  A temporary, significant child stops the shard when it exits. The placement
  strategy decides whether and where to restart the shard.

  A module used as `shard_children: {module, :child_specs, args}` can implement
  `validate_child_options!/1`. The cluster calls it before starting, so a
  configuration error does not leave shards retrying their startup. It can
  also implement `services/0`, which `Slap.Cluster.clusters/0` reports.
  """

  @callback child_specs(Slap.Cluster.Shard.t()) :: [Supervisor.child_spec() | {module(), term()}]

  @doc "Raises `ArgumentError` for invalid application settings."
  @callback validate_child_options!(keyword()) :: :ok

  @doc "Names for what the shards serve, such as `[:streams]`."
  @callback services() :: [atom()]

  @optional_callbacks validate_child_options!: 1, services: 0
end
