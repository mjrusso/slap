defmodule Slap.Streams.ShardState do
  @moduledoc false

  use Slap.Streams.ShardProcess, name: :shard_state

  alias Slap.SlateDB
  alias Slap.Streams.Store.{Batch, Read}

  @block 1_000

  @doc "A new stream id for the shard."
  @spec allocate_sid(Slap.Cluster.Shard.t()) :: {:ok, pos_integer()} | {:error, term()}
  def allocate_sid(ctx), do: GenServer.call(name(ctx), :allocate_sid)

  @impl true
  def init(ctx) do
    {:ok, next} = Read.next_sid(ctx.db)
    # Nothing is reserved yet in this process: `limit == next`.
    {:ok, %{ctx: ctx, next: next, limit: next}}
  end

  @impl true
  def handle_call(:allocate_sid, _from, %{next: next, limit: limit} = state) when next < limit,
    do: {:reply, {:ok, next}, %{state | next: next + 1}}

  def handle_call(:allocate_sid, _from, state) do
    limit = state.next + @block

    # Flushed at once rather than waiting for the next WAL flush, so a
    # create is not held up by a long flush_interval.
    with {:ok, _} <- SlateDB.write(state.ctx.db, Batch.reserve_sids(limit)),
         :ok <- SlateDB.flush(state.ctx.db) do
      {:reply, {:ok, state.next}, %{state | next: state.next + 1, limit: limit}}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end
end
