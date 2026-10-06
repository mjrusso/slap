defmodule Slap.Streams.ShardLoad do
  @moduledoc false

  use Slap.Streams.ShardProcess, name: :load

  alias Slap.SlateDB
  alias Slap.Streams

  @default_interval 5_000
  @default_per_stream 64 * 1024 * 1024
  @default_per_shard 256 * 1024 * 1024

  # :counters indexes.
  @bytes 1
  @requests 2
  @waiters 3
  @delete_backlog 4
  @expiry_due 5

  defp key(ctx), do: {__MODULE__, ctx.cluster, ctx.n}

  @doc false
  # For stream servers: `{counters, table}`, to pass back to the functions below.
  def handle(ctx) do
    case :persistent_term.get(key(ctx), nil) do
      nil -> GenServer.call(name(ctx), :handle)
      handle -> handle
    end
  end

  @doc false
  # Registers the calling stream server, so its contribution is removed if it dies.
  def register({_counters, _table} = handle, ctx) do
    GenServer.cast(name(ctx), {:monitor, self()})
    handle
  end

  @doc false
  # Adds to the calling stream server's in-flight bytes, requests and waiters.
  def add({counters, table}, bytes, requests, waiters) do
    :counters.add(counters, @bytes, bytes)
    :counters.add(counters, @requests, requests)
    :counters.add(counters, @waiters, waiters)

    :ets.update_counter(
      table,
      self(),
      [{2, bytes}, {3, requests}, {4, waiters}],
      {self(), 0, 0, 0}
    )

    :ok
  end

  @doc false
  # Removes the calling stream server's whole contribution (on terminate).
  def clear({counters, table}) do
    case :ets.take(table, self()) do
      [{_pid, bytes, requests, waiters}] -> subtract(counters, bytes, requests, waiters)
      [] -> :ok
    end
  end

  @doc false
  # Whether a stream with `stream_bytes` in flight may take `bytes` more.
  def admit?({counters, _table}, stream_bytes, bytes, opts) do
    stream_bytes == 0 or
      (stream_bytes + bytes <=
         Keyword.get(opts, :max_inflight_bytes_per_stream, @default_per_stream) and
         :counters.get(counters, @bytes) + bytes <=
           Keyword.get(opts, :max_inflight_bytes_per_shard, @default_per_shard))
  end

  @doc false
  def set_backlog(ctx, job, n) when job in [:delete, :expiry] do
    {counters, _} = handle(ctx)
    :counters.put(counters, if(job == :delete, do: @delete_backlog, else: @expiry_due), n)
  end

  @doc "The shard's current load, as sent in the `[:slap, :streams, :shard, :load]` event."
  @spec snapshot(Slap.Cluster.Shard.t()) :: map()
  def snapshot(ctx) do
    {counters, _} = handle(ctx)

    %{
      stream_servers: stream_servers(ctx),
      inflight_bytes: :counters.get(counters, @bytes),
      inflight_requests: :counters.get(counters, @requests),
      waiters: :counters.get(counters, @waiters),
      delete_backlog: :counters.get(counters, @delete_backlog),
      expiry_due: :counters.get(counters, @expiry_due)
    }
  end

  defp stream_servers(ctx) do
    DynamicSupervisor.count_children(Streams.ShardChildren.streams_supervisor(ctx)).active
  catch
    :exit, _ -> 0
  end

  @impl true
  def init(ctx) do
    counters = :counters.new(5, [:write_concurrency])
    table = :ets.new(__MODULE__, [:public, :set, write_concurrency: true])
    :persistent_term.put(key(ctx), {counters, table})
    interval = Keyword.get(ctx.child_options, :load_interval, @default_interval)
    Process.send_after(self(), :emit, interval)
    {:ok, %{ctx: ctx, counters: counters, table: table, interval: interval}}
  end

  @impl true
  def handle_call(:handle, _from, state), do: {:reply, {state.counters, state.table}, state}

  @impl true
  def handle_cast({:monitor, pid}, state) do
    Process.monitor(pid)
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    case :ets.take(state.table, pid) do
      [{_pid, bytes, requests, waiters}] -> subtract(state.counters, bytes, requests, waiters)
      [] -> :ok
    end

    {:noreply, state}
  end

  def handle_info(:emit, state) do
    measurements = Map.merge(snapshot(state.ctx), db_stats(state.ctx))
    Streams.Telemetry.execute([:shard, :load], measurements, %{shard: state.ctx.n})
    Process.send_after(self(), :emit, state.interval)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    :persistent_term.erase(key(state.ctx))
  end

  # The LSM tree's shape and the block cache.
  defp db_stats(ctx) do
    ctx.db
    |> SlateDB.stats()
    |> Map.take([:l0_sst_count, :sorted_run_count, :cache_hits, :cache_misses])
  rescue
    _ -> %{}
  end

  defp subtract(counters, bytes, requests, waiters) do
    :counters.sub(counters, @bytes, bytes)
    :counters.sub(counters, @requests, requests)
    :counters.sub(counters, @waiters, waiters)
  end
end
