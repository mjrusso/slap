defmodule Slap.Cluster.ShardChildren do
  @moduledoc """
  Starts application processes for a shard after its database opens.

  A module used as `shard_children: {module, :child_specs, args}` can implement
  `validate_child_options!/1`. The cluster calls it before starting, so a
  configuration error does not leave shards retrying their startup.
  """

  @callback child_specs(Slap.Cluster.Shard.t()) :: [Supervisor.child_spec() | {module(), term()}]

  @doc "Raises `ArgumentError` for invalid application settings."
  @callback validate_child_options!(keyword()) :: :ok

  @optional_callbacks validate_child_options!: 1
end
