defmodule Slap.Streams.Jobs.Repair do
  @concurrency 16

  @moduledoc false

  use Slap.Streams.ShardProcess, name: :repair
  require Logger

  alias Slap.SlateDB
  alias Slap.Streams.{Fork, StreamServer}
  alias Slap.Streams.Store.Keys

  @default_interval 60_000
  @batch 1_000
  @timeout 30_000

  @doc "Sweeps now and returns the number of entries checked (for tests)."
  def sweep(ctx), do: GenServer.call(name(ctx), :sweep, :infinity)

  @doc """
  Checks `forks`, the forks of the soft-deleted stream at `path`, soon,
  unless they are being checked already.
  """
  @spec check_forks(Slap.Cluster.Shard.t(), binary(), [binary()]) :: :ok
  def check_forks(ctx, path, forks), do: GenServer.cast(name(ctx), {:check_forks, path, forks})

  @impl true
  def init(ctx) do
    interval = Keyword.get(ctx.child_options, :repair_interval, @default_interval)
    Process.send_after(self(), :tick, interval)
    # `queue` holds the requested fork checks not started yet, as
    # `{path, fork}`; `running`, the started ones by task ref; and `busy`,
    # how many of each path's are queued or running.
    {:ok, %{ctx: ctx, interval: interval, queue: :queue.new(), running: %{}, busy: %{}}}
  end

  @impl true
  def handle_call(:sweep, _from, state), do: {:reply, run(state.ctx, 0, 0), state}

  @impl true
  def handle_cast({:check_forks, path, _forks}, %{busy: busy} = state)
      when is_map_key(busy, path),
      do: {:noreply, state}

  def handle_cast({:check_forks, _path, []}, state), do: {:noreply, state}

  def handle_cast({:check_forks, path, forks}, state) do
    queue = Enum.reduce(forks, state.queue, &:queue.in({path, &1}, &2))
    busy = Map.put(state.busy, path, length(forks))
    {:noreply, start_checks(%{state | queue: queue, busy: busy})}
  end

  @impl true
  def handle_info(:tick, state) do
    run(state.ctx, 0, 0)
    Process.send_after(self(), :tick, state.interval)
    {:noreply, state}
  end

  # A fork check finished.
  def handle_info({ref, _result}, %{running: running} = state) when is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    {path, running} = Map.pop!(running, ref)

    busy =
      case Map.fetch!(state.busy, path) do
        1 -> Map.delete(state.busy, path)
        n -> Map.put(state.busy, path, n - 1)
      end

    {:noreply, start_checks(%{state | running: running, busy: busy})}
  end

  defp start_checks(%{running: running} = state) when map_size(running) >= @concurrency,
    do: state

  defp start_checks(state) do
    case :queue.out(state.queue) do
      {{:value, {path, fork}}, queue} ->
        %Task{ref: ref} = Task.async(Fork, :check, [state.ctx, path, fork])
        start_checks(%{state | queue: queue, running: Map.put(state.running, ref, path)})

      {:empty, _} ->
        state
    end
  end

  # A copying fork cannot be finished here: its initial body existed only
  # in the create request. After the grace period it is dropped and
  # unregistered from its source. A soft-deleted stream stays indexed until
  # its last fork is gone, so the sweep pages through entries by sid.
  defp run(ctx, from_sid, checked) do
    entries =
      ctx.db
      |> SlateDB.scan(gte: Keys.repair(from_sid), lt: Keys.type_prefix(0x09))
      |> Enum.take(@batch)

    entries
    |> concurrently(fn {key, path} ->
      case StreamServer.call(ctx, path, {:repair_check, Keys.decode_sid(key)}, @timeout) do
        :ok ->
          []

        {:ok, {:forks, forks}} ->
          Enum.map(forks, &{path, &1})

        other ->
          Logger.warning("repair check of #{inspect(path)} failed: #{inspect(other)}")
          []
      end
    end)
    |> Enum.concat()
    |> concurrently(fn {path, fork} -> Fork.check(ctx, path, fork) end)

    checked = checked + length(entries)

    case List.last(entries) do
      {key, _path} when length(entries) == @batch -> run(ctx, Keys.decode_sid(key) + 1, checked)
      _ -> checked
    end
  end

  # The checks are independent (of different streams, or forks), and each
  # is bounded by its calls' timeouts.
  defp concurrently(entries, fun) do
    entries
    |> Task.async_stream(fun, max_concurrency: @concurrency, ordered: false, timeout: :infinity)
    |> Enum.map(fn {:ok, result} -> result end)
  end
end
