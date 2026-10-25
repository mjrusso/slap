defmodule Slap.Streams.Group do
  @moduledoc false

  alias Slap.SlateDB
  alias Slap.SlateDB.Transaction
  alias Slap.Streams.Store.{Codec, Keys, Meta, Tail}

  # A commit conflict is an aborted transaction; delivered writes are never retried.
  @attempts 3

  @doc "Seals `group` on the shard of `ctx`, durably."
  @spec seal(map(), binary()) :: :ok | {:error, :unavailable}
  def seal(ctx, group) do
    case SlateDB.put(ctx.db, Keys.seal(group), "", await_durable: true) do
      {:ok, _seq} -> :ok
      {:error, _} -> {:error, :unavailable}
    end
  end

  @doc false
  def guarded_write(db, group, ops, guard, unsealed) do
    request = %{group: group, ops: ops, guard: guard, unsealed: unsealed}
    guarded_write(db, request, @attempts)
  end

  defp guarded_write(db, request, attempts) do
    result =
      with {:ok, tx} <- SlateDB.begin(db, isolation: :serializable),
           do: guarded_commit(tx, request)

    case result do
      {:error, %SlateDB.Error{kind: :conflict}} when attempts > 1 ->
        guarded_write(db, request, attempts - 1)

      result ->
        result
    end
  end

  defp guarded_commit(tx, request) do
    with :ok <- check_seal(tx, request.group, request.unsealed),
         :ok <- check_guard(tx, request.guard),
         :ok <- Enum.reduce_while(request.ops, :ok, &add(tx, &1, &2)) do
      Transaction.commit(tx)
    else
      error -> rollback(tx, error)
    end
  end

  defp check_seal(_tx, _group, false), do: :ok

  defp check_seal(tx, group, true) do
    case Transaction.get(tx, Keys.seal(group)) do
      {:ok, nil} -> :ok
      {:ok, _sealed} -> {:error, :sealed}
      error -> error
    end
  end

  defp check_guard(_tx, nil), do: :ok

  defp check_guard(tx, %{absent: paths}) do
    Enum.reduce_while(paths, :ok, fn path, :ok ->
      case Transaction.get(tx, Keys.meta(path)) do
        {:ok, nil} -> {:cont, :ok}
        {:ok, _existing} -> {:halt, {:error, :conflict}}
        error -> {:halt, error}
      end
    end)
  end

  defp check_guard(tx, %{identity: identity, offset: offset}) do
    with {:ok, encoded} when encoded != nil <- Transaction.get(tx, Keys.meta(identity.path)),
         %Meta{sid: sid, soft_deleted: false} when sid == identity.sid <- Codec.decode(encoded),
         {:ok, encoded} when encoded != nil <- Transaction.get(tx, Keys.tail(sid)),
         %Tail{next_offset: ^offset} <- Codec.decode(encoded) do
      :ok
    else
      {:error, _} = error -> error
      _replaced_or_advanced -> {:error, :conflict}
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
