defmodule JepsenNode.Log.Follower do
  @moduledoc """
  Follows one key's log on this node: applies each entry it reads to its
  list, and every 5 entries publishes the list as a snapshot at the offset
  it has read to. When another node's snapshot trims past it, it starts over
  from that snapshot. Its list is kept in an ETS table for
  `JepsenNode.Log.check/1`.
  """

  use GenServer, restart: :temporary

  alias JepsenNode.{Log, Stats}
  alias Slap.SnapshotLog

  @every 5

  def start_link(key),
    do: GenServer.start_link(__MODULE__, key, name: {:via, Registry, {Log.Registry, key}})

  @impl true
  def init(key) do
    send(self(), :follow)
    {:ok, %{key: key, base: Log.base(key), offset: nil, list: [], unsnapshotted: 0}}
  end

  # One read at a time, waiting at most a second, so system messages get in.
  @impl true
  def handle_info(:follow, state) do
    send(self(), :follow)
    {:noreply, step(state, SnapshotLog.next(state.base, state.offset, wait: 1_000))}
  end

  defp step(state, {:reset, %{snapshot: snapshot, offset: offset}}) do
    # A reset after the first read: another node's snapshot trimmed past us.
    if state.offset, do: Stats.add(:log_resets)
    list = Log.decode(snapshot)
    Log.put_follower(state.key, list)
    %{state | offset: offset, list: list, unsnapshotted: 0}
  end

  defp step(state, {:ok, %{entries: entries, offset: offset}}) do
    list = state.list ++ Enum.map(entries, &String.to_integer/1)
    Log.put_follower(state.key, list)

    state = %{
      state
      | offset: offset,
        list: list,
        unsnapshotted: state.unsnapshotted + length(entries)
    }

    if state.unsnapshotted >= @every, do: snapshot(state), else: state
  end

  # Transient errors: next/3 retried already; try again.
  defp step(state, {:error, _}) do
    Process.sleep(100)
    state
  end

  defp snapshot(state) do
    case SnapshotLog.snapshot(state.base, state.offset, JSON.encode!(state.list)) do
      :ok ->
        Stats.add(:log_snapshots)
        Log.record_snapshot(state.key)

      {:error, :superseded} ->
        Stats.add(:log_superseded)

      {:error, _} ->
        :ok
    end

    %{state | unsnapshotted: 0}
  end
end
