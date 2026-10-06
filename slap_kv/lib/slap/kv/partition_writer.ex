defmodule Slap.KV.PartitionWriter do
  @moduledoc false

  @behaviour Slap.Cluster.ShardChildren

  use GenServer
  require Logger

  alias Slap.Cluster
  alias Slap.KV
  alias Slap.KV.Keys
  alias Slap.SlateDB

  @load_durable_timeout 10_000

  @default_count 16

  @type condition :: :any | :absent | {:version, non_neg_integer()}
  @type request ::
          {:write, tuple(), condition(), integer() | nil}
          | {:get, binary()}

  @doc false
  @impl true
  def child_specs(ctx) do
    validate_child_options!(ctx.child_options)
    count = Keyword.get(ctx.child_options, :partition_writers, @default_count)

    # Route with the count this shard started with: a partition must not
    # move between writers while the shard is open.
    Registry.put_meta(ctx.registry, count_key(ctx), count)
    for i <- 0..(count - 1), do: {__MODULE__, {ctx, i}}
  end

  @doc false
  @impl true
  def validate_child_options!(opts) do
    Keyword.validate!(opts, [:partition_writers])
    count = Keyword.get(opts, :partition_writers, @default_count)

    unless is_integer(count) and count > 0,
      do: raise(ArgumentError, ":partition_writers must be a positive integer")

    :ok
  end

  def child_spec({_ctx, i} = arg),
    do: %{id: {__MODULE__, i}, start: {__MODULE__, :start_link, [arg]}}

  def start_link({ctx, i}), do: GenServer.start_link(__MODULE__, {ctx, i}, name: name(ctx, i))

  defp name(ctx, i), do: Cluster.via(ctx.cluster, ctx.n, {:partition_writer, i})

  defp count_key(ctx), do: {__MODULE__, ctx.n}

  @doc false
  @spec index(Cluster.Shard.t(), binary()) :: {:ok, non_neg_integer()} | :error
  def index(ctx, partition) do
    with {:ok, count} <- Registry.meta(ctx.registry, count_key(ctx)),
         do: {:ok, :erlang.phash2(partition, count)}
  end

  @doc false
  @spec call(Cluster.Shard.t(), binary(), request(), timeout()) :: term()
  def call(ctx, partition, request, timeout), do: call(ctx, partition, request, timeout, 5)

  # The registry may not name a partition writer for a moment: one that
  # just exited before its replacement registers, or all of them before the
  # shard's first start. The request was not delivered, so it is safe to
  # send again. The index is computed again, since replacements may have
  # been started with a different count.
  defp call(ctx, partition, request, timeout, retries) do
    case send_request(ctx, partition, request, timeout) do
      :noproc when retries > 0 ->
        Process.sleep(1)
        call(ctx, partition, request, timeout, retries - 1)

      :noproc ->
        {:error, :unavailable}

      reply ->
        reply
    end
  end

  defp send_request(ctx, partition, request, timeout) do
    case index(ctx, partition) do
      {:ok, i} -> GenServer.call(name(ctx, i), request, timeout)
      :error -> :noproc
    end
  catch
    :exit, {:noproc, _} ->
      :noproc

    :exit, {:timeout, _} ->
      {:error, :timeout}

    # Not running (the shard is stopping, or it is restarting after
    # a write error), or it stopped during the call.
    :exit, _ ->
      {:error, :unavailable}
  end

  @impl true
  def init({ctx, i}) do
    # To fail in-flight requests when the shard stops.
    Process.flag(:trap_exit, true)

    case await_durable(ctx) do
      :ok -> {:ok, %{ctx: ctx, index: i, inflight: :queue.new()}}
      {:error, reason} -> {:stop, reason}
    end
  end

  # If a write never becomes durable (the shard was fenced or its store
  # failed), the partition writer does not start, rather than check conditions
  # against state that may be lost.
  defp await_durable(ctx) do
    seq = SlateDB.last_write_seq(ctx.db)

    if Cluster.durable_seq(ctx) >= seq do
      :ok
    else
      Cluster.notify_when_durable(ctx, seq, :loaded)

      receive do
        {:slap_cluster_durable, :loaded} -> :ok
      after
        @load_durable_timeout -> {:error, :not_durable}
      end
    end
  end

  @impl true
  def handle_call({:write, op, condition, deadline}, from, state) do
    started = System.monotonic_time()

    with :ok <- before_deadline(deadline),
         :ok <- check(state.ctx.db, elem(op, 1), condition) do
      commit(state, op, from, started)
    else
      {:conflict, version} ->
        confirm(state, {:error, {:conflict, version}}, from)

      {:error, :deadline_exceeded} = error ->
        {:reply, error, state}

      {:error, error} ->
        read_failed(error, state)
    end
  end

  def handle_call({:get, key}, from, state) do
    case SlateDB.get_key_value(state.ctx.db, key) do
      {:ok, nil} -> confirm(state, {:ok, nil}, from)
      {:ok, row} -> confirm(state, {:ok, %{value: row.value, version: row.seq}}, from)
      {:error, error} -> read_failed(error, state)
    end
  end

  # Checked when the request is handled, not when it was sent: a request
  # can wait in the mailbox, or on its way here, after its caller has given
  # up on it. The deadline itself is too late: a caller that waits until it
  # (plus the clock difference) knows that no write can be applied any more.
  defp before_deadline(nil), do: :ok

  defp before_deadline(deadline) do
    if System.os_time(:millisecond) < deadline, do: :ok, else: {:error, :deadline_exceeded}
  end

  # A failed read changes nothing; the database is closing or failing, and
  # the next write will find out.
  defp read_failed(error, state) do
    Logger.debug(
      "kv shard #{state.ctx.n} partition writer #{state.index}: read failed: #{inspect(error)}"
    )

    {:reply, {:error, :unavailable}, state}
  end

  defp check(_db, _key, :any), do: :ok

  defp check(db, key, condition) do
    case SlateDB.get_key_value(db, key) do
      {:ok, row} -> compare(row, condition)
      {:error, _} = error -> error
    end
  end

  defp compare(nil, :absent), do: :ok
  defp compare(%{seq: version}, {:version, version}), do: :ok
  defp compare(nil, {:version, _}), do: {:conflict, nil}
  defp compare(%{seq: current}, _condition), do: {:conflict, current}

  defp commit(state, op, from, started) do
    case SlateDB.write(state.ctx.db, [op]) do
      {:ok, seq} ->
        Cluster.notify_when_durable(state.ctx, seq, :durable)
        reply = if elem(op, 0) == :put, do: {:ok, seq}, else: :ok
        entry = %{seq: seq, from: from, reply: reply, op: elem(op, 0), started: started}
        {:noreply, %{state | inflight: :queue.in(entry, state.inflight)}}

      {:error, error} ->
        write_failed(error, from, state)
    end
  end

  # A conflict or linearizable read may have observed an in-flight write,
  # or stale state after another node took ownership. A durable write to
  # the confirmation key orders the reply and fails if this handle was fenced.
  defp confirm(state, reply, from) do
    case SlateDB.write(state.ctx.db, [{:put, Keys.confirm_key(), ""}]) do
      {:ok, seq} ->
        Cluster.notify_when_durable(state.ctx, seq, :durable)
        entry = %{seq: seq, from: from, reply: reply, op: nil}
        {:noreply, %{state | inflight: :queue.in(entry, state.inflight)}}

      {:error, error} ->
        write_failed(error, from, state)
    end
  end

  # Writes in flight may or may not become durable: fail them all and stop,
  # so a new partition writer starts from what is durable.
  defp write_failed(error, from, state) do
    Logger.error(
      "kv shard #{state.ctx.n} partition writer #{state.index}: write failed: #{inspect(error)}"
    )

    KV.Telemetry.execute([:partition_writer, :write_failed], %{}, %{shard: state.ctx.n})
    GenServer.reply(from, {:error, :unavailable})
    {:stop, {:shutdown, :write_failed}, fail_all(state)}
  end

  @impl true
  def handle_info({:slap_cluster_durable, :durable}, state) do
    {:noreply, drain(state, Cluster.durable_seq(state.ctx))}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state), do: fail_all(state)

  defp fail_all(state) do
    for %{from: from} <- :queue.to_list(state.inflight),
        do: GenServer.reply(from, {:error, :unavailable})

    %{state | inflight: :queue.new()}
  end

  # Later replies wait behind earlier writes, including replies that did
  # not themselves change the requested row.
  defp drain(state, durable) do
    case :queue.peek(state.inflight) do
      {:value, %{seq: seq} = entry} when seq <= durable ->
        GenServer.reply(entry.from, entry.reply)
        if entry.op, do: written(state, entry)
        drain(%{state | inflight: :queue.drop(state.inflight)}, durable)

      _ ->
        state
    end
  end

  defp written(state, entry) do
    KV.Telemetry.execute(
      [:write, :acknowledged],
      %{duration: System.monotonic_time() - entry.started},
      %{shard: state.ctx.n, op: entry.op}
    )
  end
end
