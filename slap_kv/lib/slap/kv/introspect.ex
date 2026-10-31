defmodule Slap.KV.Introspect do
  @moduledoc false

  alias Slap.Cluster.Inspection
  alias Slap.Cluster.Inspection.Read
  alias Slap.KV.Keys

  def scan(cluster, shard, version, budget, opts) do
    Inspection.request(
      cluster,
      shard,
      version,
      budget,
      fn ctx, deadline -> page(ctx, deadline, opts, &decode/2) end
    )
  end

  @doc """
  A page of rows, decoded with `decode`. With `:partition`, only the rows of
  that partition whose keys start with `:key_prefix`.
  """
  def page(ctx, deadline, opts, decode) do
    with {:ok, opts} <- partition_prefix(opts), do: Read.page(ctx, deadline, opts, decode)
  end

  def decode(key, _value), do: Keys.parse(key)

  defp partition_prefix(opts) do
    case Keyword.get(opts, :partition) do
      nil ->
        {:ok, opts}

      partition ->
        with :ok <- Keys.validate_partition(partition) do
          prefix = Keys.encode(partition, Keyword.get(opts, :key_prefix, ""))
          {:ok, Keyword.put(opts, :prefix, prefix)}
        end
    end
  end
end
