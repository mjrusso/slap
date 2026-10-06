defmodule Slap.Streams.Group do
  @moduledoc false

  alias Slap.SlateDB
  alias Slap.SlateDB.Transaction
  alias Slap.Streams.Store.Keys

  # Commit conflicts come from a seal, so a retry finds it.
  @attempts 3

  @doc "Seals `group` on the shard of `ctx`, durably."
  @spec seal(map(), binary()) :: :ok | {:error, :unavailable}
  def seal(ctx, group) do
    case SlateDB.put(ctx.db, Keys.seal(group), "", await_durable: true) do
      {:ok, _seq} -> :ok
      {:error, _} -> {:error, :unavailable}
    end
  end

  @doc """
  Writes `ops`, which create a stream in `group`, unless the group is
  sealed. Returns `{:ok, seq}` like `Slap.SlateDB.write/3`.
  """
  @spec create_write(SlateDB.t(), binary(), [SlateDB.write_op()]) ::
          {:ok, non_neg_integer()} | {:error, :sealed | SlateDB.Error.t()}
  def create_write(db, group, ops), do: create_write(db, group, ops, @attempts)

  defp create_write(db, group, ops, attempts) do
    with {:ok, tx} <- SlateDB.begin(db, isolation: :serializable) do
      case Transaction.get(tx, Keys.seal(group)) do
        {:ok, nil} -> commit(db, group, ops, tx, attempts)
        {:ok, _sealed} -> rollback(tx, {:error, :sealed})
        {:error, _} = error -> rollback(tx, error)
      end
    end
  end

  defp commit(db, group, ops, tx, attempts) do
    case Enum.reduce_while(ops, :ok, &add(tx, &1, &2)) do
      :ok -> commit_or_retry(db, group, ops, tx, attempts)
      {:error, _} = error -> rollback(tx, error)
    end
  end

  defp commit_or_retry(db, group, ops, tx, attempts) do
    case Transaction.commit(tx) do
      {:error, %SlateDB.Error{kind: :conflict}} when attempts > 1 ->
        create_write(db, group, ops, attempts - 1)

      result ->
        result
    end
  end

  defp add(tx, op, :ok) do
    case op(tx, op) do
      :ok -> {:cont, :ok}
      error -> {:halt, error}
    end
  end

  defp op(tx, {:put, key, value}), do: Transaction.put(tx, key, value)
  defp op(tx, {:put, key, value, ttl}), do: Transaction.put(tx, key, value, ttl: ttl)
  defp op(tx, {:merge, key, value}), do: Transaction.merge(tx, key, value)
  defp op(tx, {:merge, key, value, ttl}), do: Transaction.merge(tx, key, value, ttl: ttl)
  defp op(tx, {:delete, key}), do: Transaction.delete(tx, key)

  defp rollback(tx, result) do
    :ok = Transaction.rollback(tx)
    result
  end
end
