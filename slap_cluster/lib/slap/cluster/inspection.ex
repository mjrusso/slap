defmodule Slap.Cluster.Inspection do
  @moduledoc false

  alias Slap.Cluster.Inspection.Read

  # The node's one inspection slot is this registered name: registering the
  # calling process takes it, and the runtime releases it if that process dies.
  @slot __MODULE__
  @version 1

  def check_version(@version), do: :ok
  def check_version(_version), do: {:error, {:unsupported_api, @version}}

  def scan(cluster, shard, version, budget, opts) do
    request(cluster, shard, version, budget, fn ctx, deadline ->
      Read.page(ctx, deadline, opts, &raw/2)
    end)
  end

  def get(cluster, shard, version, budget, opts) do
    request(cluster, shard, version, budget, fn ctx, deadline ->
      Read.get(ctx, deadline, Keyword.fetch!(opts, :key), opts, &raw/2)
    end)
  end

  defp raw(_key, _value), do: {:ok, %{kind: :raw}}

  def request(cluster, shard, version, budget, fun) do
    exclusive(version, budget, fn deadline ->
      case lookup(cluster, shard) do
        {:ok, {:local, ctx}} -> fun.(ctx, deadline)
        _ -> {:error, :not_owner}
      end
    end)
  end

  def exclusive(@version, budget, fun)
      when is_integer(budget) and budget > 0 and budget <= 30_000 do
    case acquire() do
      :ok ->
        try do
          fun.(Read.deadline(budget))
        rescue
          error in Slap.SlateDB.Error -> Read.storage_error(error)
        after
          Process.unregister(@slot)
        end

      error ->
        error
    end
  end

  def exclusive(@version, _budget, _fun), do: {:error, :invalid_budget}
  def exclusive(version, _budget, _fun), do: check_version(version)

  defp lookup(cluster, shard) do
    Slap.Cluster.lookup(cluster, shard)
  rescue
    ArgumentError -> {:error, :not_owner}
  end

  defp acquire do
    case Process.info(self(), :registered_name) do
      {:registered_name, []} -> register()
      {:registered_name, @slot} -> {:error, :busy}
      {:registered_name, _name} -> {:error, :registered_caller}
    end
  end

  defp register do
    Process.register(self(), @slot)
    :ok
  rescue
    ArgumentError -> {:error, :busy}
  end
end
