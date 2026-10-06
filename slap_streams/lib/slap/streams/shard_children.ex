defmodule Slap.Streams.ShardChildren do
  @moduledoc false

  @behaviour Slap.Cluster.ShardChildren

  alias Slap.Streams

  @nonnegative_options [:max_fork_copy_bytes, :fork_copy_grace]

  @doc false
  @impl true
  def child_specs(ctx) do
    validate_child_options!(ctx.child_options)

    # Children stop in reverse order: jobs and stream servers stop while
    # the shard database is still open, so they can fail pending requests.
    [
      {Streams.ShardLoad, ctx},
      {Streams.ShardState, ctx},
      %{
        id: :streams,
        start:
          {DynamicSupervisor, :start_link,
           [[strategy: :one_for_one, name: streams_supervisor(ctx)]]},
        type: :supervisor
      },
      {Streams.Jobs.Expiry, ctx},
      {Streams.Jobs.Deleter, ctx},
      {Streams.Jobs.Repair, ctx}
    ]
  end

  @doc false
  @impl true
  def validate_child_options!(opts) do
    Keyword.validate!(opts, Streams.Cluster.child_option_keys())
    Enum.each(opts, fn {key, value} -> validate_value!(key, value) end)

    :ok
  end

  defp validate_value!(key, value)
       when key in @nonnegative_options and is_integer(value) and value >= 0,
       do: :ok

  defp validate_value!(key, value) when key in @nonnegative_options,
    do: raise(ArgumentError, ":#{key} must be a non-negative integer, got: #{inspect(value)}")

  defp validate_value!(_key, value) when is_integer(value) and value > 0, do: :ok

  defp validate_value!(key, value),
    do: raise(ArgumentError, ":#{key} must be a positive integer, got: #{inspect(value)}")

  @doc false
  def streams_supervisor(ctx), do: Slap.Cluster.via(ctx.cluster, ctx.n, :streams)
end
