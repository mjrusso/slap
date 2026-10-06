defmodule Slap.Streams.Jobs.Expiry do
  @concurrency 16

  @moduledoc false

  use Slap.Streams.ShardProcess, name: :expiry
  require Logger

  alias Slap.SlateDB
  alias Slap.Streams
  alias Slap.Streams.{Clock, StreamServer}
  alias Slap.Streams.Store.Keys

  @default_interval 10_000
  @batch 1_000
  @timeout 30_000

  @doc "Sweeps now and returns the number of entries checked (for tests)."
  def sweep(ctx), do: GenServer.call(name(ctx), :sweep, :infinity)

  @impl true
  def init(ctx) do
    interval = Keyword.get(ctx.child_options, :expiry_interval, @default_interval)
    Process.send_after(self(), :tick, interval)
    {:ok, %{ctx: ctx, interval: interval}}
  end

  @impl true
  def handle_call(:sweep, _from, state), do: {:reply, run(state.ctx, 0), state}

  @impl true
  def handle_info(:tick, state) do
    run(state.ctx, 0)
    Process.send_after(self(), :tick, state.interval)
    {:noreply, state}
  end

  # Checks the due entries, a batch at a time. A checked entry is deleted or
  # moved past now, so the next batch starts where this one ended; an entry
  # whose check failed is left for the next tick.
  defp run(ctx, checked) do
    now = Clock.now_ms()

    due =
      ctx.db
      |> SlateDB.scan(gte: Keys.type_prefix(0x05), lt: Keys.expiry(now + 1, 0))
      |> Enum.take(@batch)

    # The entries are of different streams, so their checks are
    # independent; each is bounded by its call's timeout.
    results =
      due
      |> Task.async_stream(&check(ctx, &1),
        max_concurrency: @concurrency,
        ordered: false,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, result} -> result end)

    checked = checked + length(due)
    Streams.ShardLoad.set_backlog(ctx, :expiry, length(due))

    if length(due) == @batch and :error not in results, do: run(ctx, checked), else: checked
  end

  # The stream server checks the current deadline and sid so a stale index
  # entry cannot delete a stream that was touched, deleted, or recreated.
  defp check(ctx, {key, path}) do
    {deadline, sid} = Keys.decode_expiry(key)

    case StreamServer.call(ctx, path, {:expiry_check, sid, deadline}, @timeout) do
      :ok ->
        :ok

      other ->
        Logger.warning("expiry check of #{inspect(path)} failed: #{inspect(other)}")
        :error
    end
  end
end
