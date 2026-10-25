defmodule Slap.KV.Admission do
  @moduledoc false

  use GenServer

  alias Slap.Cluster
  alias Slap.KV.Telemetry

  @defaults [
    partition_writers: 16,
    max_inflight_bytes_per_writer: 64 * 1024 * 1024,
    max_inflight_bytes_per_shard: 256 * 1024 * 1024,
    max_inflight_requests_per_writer: 1024,
    max_batch_ops: 1_000,
    max_batch_bytes: 8 * 1024 * 1024
  ]

  def option_keys, do: Keyword.keys(@defaults)

  def options!(opts) do
    opts = Keyword.validate!(opts, @defaults)

    for {key, value} <- opts do
      unless is_integer(value) and value > 0,
        do: raise(ArgumentError, "#{inspect(key)} must be a positive integer")
    end

    Map.new(opts)
  end

  def child_spec(ctx) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [ctx]},
      restart: :temporary,
      significant: true
    }
  end

  def start_link(ctx), do: GenServer.start_link(__MODULE__, ctx, name: name(ctx))

  def attach(ctx, index), do: call(ctx, {:attach, index})

  def reserve(ctx, index, partition, op, bytes),
    do: call(ctx, {:reserve, index, partition, op, bytes})

  def submitting(ctx, token), do: call(ctx, {:submitting, token})
  def submitted(ctx, token, seq), do: call(ctx, {:submitted, token, seq})
  def release(ctx, tokens), do: call(ctx, {:release, tokens})
  def snapshot(ctx), do: call(ctx, :snapshot)

  defp name(ctx), do: Cluster.via(ctx.cluster, ctx.n, :kv_admission)

  # A timeout could hide a granted reservation. The public caller's timeout
  # does not cancel the writer's work.
  defp call(ctx, request), do: GenServer.call(name(ctx), {ctx.shard_db, request}, :infinity)

  @impl true
  def init(ctx) do
    state = %{
      ctx: ctx,
      limits: options!(ctx.child_options),
      bytes: 0,
      writers: %{},
      reservations: %{},
      monitors: %{}
    }

    load(state)
    {:ok, state}
  end

  @impl true
  def handle_call({owner, _request}, _from, state) when owner != state.ctx.shard_db,
    do: {:reply, {:error, :unavailable}, state}

  def handle_call({_owner, :snapshot}, _from, state) do
    snapshot = Map.take(state, [:bytes, :writers])
    {:reply, Map.put(snapshot, :requests, map_size(state.reservations)), state}
  end

  def handle_call({_owner, {:attach, index}}, {pid, _}, state) do
    uncertain? =
      Enum.any?(state.reservations, fn {_token, entry} ->
        entry.index == index and entry.pid != pid and entry.submission == :submitting
      end)

    if uncertain? do
      {:stop, {:shutdown, :uncertain_submission}, {:error, :unavailable}, state}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({_owner, {:reserve, index, partition, op, bytes}}, {pid, _}, state) do
    usage = Map.get(state.writers, index, %{bytes: 0, requests: 0})

    case capacity(state, usage, bytes) do
      :ok ->
        token = make_ref()
        entry = %{pid: pid, index: index, bytes: bytes, submission: :reserved}
        state = monitor_writer(state, pid)
        usage = %{bytes: usage.bytes + bytes, requests: usage.requests + 1}

        state = %{
          state
          | bytes: state.bytes + bytes,
            writers: Map.put(state.writers, index, usage),
            reservations: Map.put(state.reservations, token, entry)
        }

        load(state)
        {:reply, {:ok, token}, state}

      {:overloaded, limit} ->
        Telemetry.execute([:request, :rejected], %{bytes: bytes, requests: 1}, %{
          partition: partition,
          shard: state.ctx.n,
          writer: index,
          op: op,
          reason: :overloaded,
          limit: limit
        })

        {:reply, {:error, :overloaded}, state}

      :unavailable ->
        {:reply, {:error, :unavailable}, state}

      :too_large ->
        {:reply, {:error, {:bad_request, :request_too_large}}, state}
    end
  end

  def handle_call({_owner, {:submitting, token}}, {pid, _}, state) do
    case state.reservations do
      %{^token => %{pid: ^pid, submission: :reserved} = entry} ->
        if Process.alive?(pid),
          do:
            {:reply, :ok, put_in(state.reservations[token], %{entry | submission: :submitting})},
          else: {:reply, {:error, :unavailable}, release_token(state, token)}

      _ ->
        {:reply, {:error, :unavailable}, state}
    end
  end

  def handle_call({_owner, {:submitted, token, seq}}, {pid, _}, state) do
    case state.reservations do
      %{^token => %{pid: ^pid, submission: :submitting} = entry} ->
        {:reply, :ok, put_in(state.reservations[token], %{entry | submission: {:sequence, seq}})}

      _ ->
        {:reply, {:error, :unavailable}, state}
    end
  end

  def handle_call({_owner, {:release, tokens}}, {pid, _}, state) do
    state =
      Enum.reduce(tokens, state, fn token, state ->
        case state.reservations do
          %{^token => %{pid: ^pid}} -> release_token(state, token)
          _ -> state
        end
      end)

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.pop(state.monitors, pid) do
      {^ref, monitors} ->
        state = %{state | monitors: monitors}
        entries = Enum.filter(state.reservations, fn {_token, entry} -> entry.pid == pid end)

        if Enum.any?(entries, &match?({_, %{submission: :submitting}}, &1)) do
          {:stop, {:shutdown, :uncertain_submission}, state}
        else
          {:noreply, Enum.reduce(entries, state, &writer_down/2)}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:slap_cluster_durable, {:admission, token}}, state) do
    case state.reservations do
      %{^token => %{submission: {:sequence, seq}}} ->
        if Cluster.durable_seq(state.ctx) >= seq,
          do: {:noreply, release_token(state, token)},
          else: {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  defp capacity(state, usage, bytes) do
    limits = state.limits

    cond do
      bytes > limits.max_inflight_bytes_per_writer or bytes > limits.max_inflight_bytes_per_shard ->
        :too_large

      usage.bytes + bytes > limits.max_inflight_bytes_per_writer ->
        {:overloaded, :writer_bytes}

      state.bytes + bytes > limits.max_inflight_bytes_per_shard ->
        {:overloaded, :shard_bytes}

      usage.requests >= limits.max_inflight_requests_per_writer ->
        {:overloaded, :writer_requests}

      Cluster.shard_status(state.ctx) != :running ->
        :unavailable

      true ->
        :ok
    end
  end

  defp monitor_writer(state, pid) do
    if Map.has_key?(state.monitors, pid),
      do: state,
      else: %{state | monitors: Map.put(state.monitors, pid, Process.monitor(pid))}
  end

  defp writer_down({token, %{submission: :reserved}}, state), do: release_token(state, token)

  defp writer_down({token, %{submission: {:sequence, seq}}}, state) do
    if Cluster.durable_seq(state.ctx) >= seq do
      release_token(state, token)
    else
      Cluster.notify_when_durable(state.ctx, seq, {:admission, token})
      state
    end
  end

  defp release_token(state, token) do
    {entry, reservations} = Map.pop!(state.reservations, token)
    usage = Map.fetch!(state.writers, entry.index)
    usage = %{bytes: usage.bytes - entry.bytes, requests: usage.requests - 1}

    writers =
      if usage.requests == 0,
        do: Map.delete(state.writers, entry.index),
        else: Map.put(state.writers, entry.index, usage)

    state = %{
      state
      | reservations: reservations,
        writers: writers,
        bytes: state.bytes - entry.bytes
    }

    load(state)
    state
  end

  defp load(state) do
    Telemetry.execute(
      [:shard, :load],
      %{bytes: state.bytes, requests: map_size(state.reservations)},
      %{shard: state.ctx.n}
    )
  end
end
