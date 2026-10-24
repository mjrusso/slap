defmodule Slap.Streams.Jobs.Repair do
  @moduledoc false

  use Slap.Streams.ShardProcess, name: :repair
  require Logger

  alias Slap.{Cluster, SlateDB}
  alias Slap.Streams
  alias Slap.Streams.{Clock, Retention, StreamServer}
  alias Slap.Streams.Store.{Keys, Meta, Read}

  @concurrency 8
  @queue_limit 64
  @default_interval 60_000
  @timeout 30_000

  @doc "Waits for a scan started after this request and its checks; returns the source count."
  def sweep(ctx, timeout \\ :infinity), do: GenServer.call(name(ctx), :sweep, timeout)

  @doc false
  def notify(cluster, source, target) do
    case Cluster.lookup(cluster, source.shard) do
      {:ok, {:local, _ctx}} -> kick(cluster, source, target)
      {:ok, {:remote, node}} -> :erpc.cast(node, __MODULE__, :kick, [cluster, source, target])
      {:error, :unassigned} -> :ok
    end
  end

  @doc false
  def kick(cluster, source, target),
    do:
      GenServer.cast(
        Cluster.via(cluster, source.shard, :repair),
        {:check, {:registration, source, target}}
      )

  @doc false
  def target_status(ctx, target) do
    with {:ok, meta} <- Read.get_meta(ctx.db, target.path, durability: :remote) do
      case Retention.protection(meta, target) do
        :unprotected -> StreamServer.call(ctx, target.path, {:terminal, target}, @timeout)
        status -> {:ok, status}
      end
    end
  end

  @impl true
  def init(ctx) do
    interval = Keyword.get(ctx.child_options, :repair_interval, @default_interval)
    Process.send_after(self(), :tick, interval)

    {:ok,
     %{
       ctx: ctx,
       interval: interval,
       sweep: nil,
       waiting: [],
       queue: :queue.new(),
       checks: %{}
     }}
  end

  @impl true
  def handle_call(:sweep, from, %{sweep: nil} = state),
    do: {:noreply, start_sweep(state, [from])}

  def handle_call(:sweep, from, state),
    do: {:noreply, %{state | waiting: [from | state.waiting]}}

  @impl true
  def handle_cast({:check, work}, state) do
    # The durable index recovers dropped notifications.
    if Map.has_key?(state.checks, work) or :queue.len(state.queue) >= @queue_limit do
      {:noreply, state}
    else
      state = %{
        state
        | queue: :queue.in(work, state.queue),
          checks: Map.put(state.checks, work, nil)
      }

      {:noreply, dispatch(state)}
    end
  end

  @impl true
  def handle_info(:tick, state) do
    Process.send_after(self(), :tick, state.interval)
    state = if state.sweep == nil, do: start_sweep(state, []), else: state
    {:noreply, state}
  end

  def handle_info({ref, count}, %{sweep: {ref, callers}} = state) do
    Process.demonitor(ref, [:flush])
    Enum.each(callers, &GenServer.reply(&1, count))
    {:noreply, next_sweep(%{state | sweep: nil})}
  end

  def handle_info({ref, _result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    checks = Map.reject(state.checks, fn {_work, running} -> running == ref end)
    {:noreply, dispatch(%{state | checks: checks})}
  end

  def handle_info({:DOWN, _ref, :process, _pid, :normal}, state), do: {:noreply, state}

  defp start_sweep(state, callers) do
    ctx = state.ctx
    task = Task.async(fn -> scan(ctx) end)
    %{state | sweep: {task.ref, callers}, waiting: []}
  end

  defp next_sweep(%{waiting: []} = state), do: state
  defp next_sweep(state), do: start_sweep(state, state.waiting)

  defp dispatch(state) do
    running = Enum.count(state.checks, fn {_work, ref} -> ref != nil end)
    dispatch(state, @concurrency - running)
  end

  defp dispatch(state, 0), do: state

  defp dispatch(state, slots) do
    case :queue.out(state.queue) do
      {{:value, work}, queue} ->
        ctx = state.ctx
        task = Task.async(fn -> check(ctx, work) end)
        state = %{state | queue: queue, checks: Map.put(state.checks, work, task.ref)}
        dispatch(state, slots - 1)

      {:empty, _} ->
        state
    end
  end

  defp scan(ctx) do
    # Separate worker budgets keep stalled notification checks from blocking a pass.
    ctx.db
    |> SlateDB.scan(prefix: Keys.type_prefix(0x08), durability: :remote)
    |> Stream.flat_map(fn {key, path} ->
      Stream.concat([:source_entry], source_work(ctx, path, Keys.decode_sid(key)))
    end)
    |> Task.async_stream(
      fn
        :source_entry ->
          1

        work ->
          check(ctx, work)
          0
      end,
      max_concurrency: @concurrency,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.reduce(0, fn {:ok, count}, total -> total + count end)
  rescue
    error in SlateDB.Error ->
      Logger.warning("repair: #{Exception.message(error)}")
      0
  end

  defp source_work(ctx, path, sid) do
    case Read.get_meta(ctx.db, path, durability: :remote) do
      {:ok, %Meta{sid: ^sid} = meta} ->
        if (meta.registration_count == 0 and not Meta.pending?(meta)) or
             Retention.repair_due?(meta, ctx.child_options, Clock.now_ms()) do
          [{:source, path, sid}]
        else
          registration_work(ctx, sid)
        end

      {:ok, _absent_or_replaced} ->
        [{:source, path, sid}]

      {:error, _} ->
        []
    end
  end

  defp registration_work(ctx, sid) do
    ctx.db
    |> Read.registrations(sid, durability: :remote)
    |> Stream.map(fn cap -> {:registration, cap.source, cap.target} end)
  end

  defp check(ctx, {:source, path, sid}),
    do: StreamServer.call(ctx, path, {:repair_check, sid}, @timeout)

  defp check(ctx, {:registration, source, target}) do
    with true <- source.shard == ctx.n,
         {:ok, %Meta{sid: sid}} when sid == source.sid <-
           Read.get_meta(ctx.db, source.path, durability: :remote),
         {:ok, registration} when registration != nil <-
           Read.get_registration(ctx.db, sid, target, durability: :remote) do
      check_registration(ctx, registration)
    end
  end

  defp check_registration(ctx, cap) do
    case Cluster.call(ctx.cluster, cap.target.shard, {__MODULE__, :target_status, [cap.target]},
           timeout: @timeout
         ) do
      {:ok, {:ok, :terminal}} ->
        Streams.internal_identity(
          cap.source,
          {:unregister, cap.source, cap.target},
          @timeout,
          ctx.cluster
        )

      {:ok, {:ok, :referenced}} when cap.prefix != nil ->
        Streams.internal_identity(
          cap.source,
          {:compact_registration, cap.source, cap.target},
          @timeout,
          ctx.cluster
        )

      _protected_or_unknown ->
        :ok
    end
  end
end
