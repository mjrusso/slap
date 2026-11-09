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

  @doc "Lists partitions and the shard's ownership-confirmation row without reading values."
  def list_partitions(cluster, shard, version, budget, opts) do
    Inspection.request(cluster, shard, version, budget, fn ctx, deadline ->
      partitions(ctx, deadline, opts, &listed/1)
    end)
  end

  defp listed(:confirm), do: {:ok, %{kind: :confirm}}
  defp listed(partition), do: {:ok, %{kind: :partition, partition: partition}}

  @doc """
  A page of rows, decoded with `decode`. With `:partition`, only the rows of
  that partition whose keys start with `:key_prefix`.
  """
  def page(ctx, deadline, opts, decode) do
    with {:ok, opts} <- partition_prefix(opts), do: Read.page(ctx, deadline, opts, decode)
  end

  def decode(key, _value), do: Keys.parse(key)

  @doc """
  Lists one row per accepted partition without reading values. Each row's
  key is the partition's key prefix. `parse` receives a partition name or
  `:confirm` for the ownership-confirmation row; it returns `{:ok, fields}`
  to include the row, or any other result to skip it.
  """
  def partitions(ctx, deadline, opts, parse),
    do: Read.groups(ctx, deadline, opts, &group(&1, &2, parse))

  defp group(key, key_size, parse) do
    case Keys.partition(key, key_size) do
      {:ok, {:partition, prefix, partition}} -> listed({:prefix, prefix}, parse.(partition))
      {:ok, :confirm} -> listed(:key, parse.(:confirm))
      {:error, _reason} -> :unknown
    end
  end

  defp listed(scope, {:ok, _fields} = decoded), do: {:list, scope, decoded}
  defp listed(scope, _error), do: {:skip, scope}

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
