defmodule Slap.KV.PartitionWriter do
  @moduledoc false

  @behaviour Slap.Cluster.ShardChildren

  use GenServer
  require Logger

  alias Slap.Cluster
  alias Slap.KV
  alias Slap.KV.{Admission, Keys, Request}
  alias Slap.SlateDB

  @load_durable_timeout 10_000

  @type condition :: :any | Request.condition()
  @type request ::
          {:write, binary(), tuple(), condition(), integer() | nil}
          | {:get, binary(), binary()}
          | {:batch, binary(), term(), integer() | nil}

  @doc false
  @impl true
  def services, do: [:kv]

  @doc false
  @impl true
  def child_specs(ctx) do
    count = Admission.options!(ctx.child_options).partition_writers

    # Route with the count this shard started with: a partition must not
    # move between writers while the shard is open.
    Registry.put_meta(ctx.registry, count_key(ctx), count)
    [{Admission, ctx} | for(i <- 0..(count - 1), do: {__MODULE__, {ctx, i}})]
  end

  @doc false
  @impl true
  def validate_child_options!(opts) do
    Admission.options!(opts)

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

  # Only :noproc proves non-delivery. Never resend a timed-out request.
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

    :exit, _ ->
      {:error, :unavailable}
  end

  @impl true
  def init({ctx, i}) do
    # Fail in-flight calls during orderly shard shutdown.
    Process.flag(:trap_exit, true)

    with :ok <- Admission.attach(ctx, i),
         :ok <- await_durable(ctx) do
      {:ok,
       %{
         ctx: ctx,
         index: i,
         inflight: :queue.new(),
         limits: Admission.options!(ctx.child_options)
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  # A replacement must not check conditions against nondurable predecessor writes.
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
  def handle_call({:write, partition, op, condition, deadline}, from, state) do
    write(state, Request.single(partition, op, condition, deadline), from)
  end

  def handle_call({:batch, partition, ops, deadline}, from, state) do
    case Request.batch(partition, ops, deadline, state.limits) do
      {:ok, plan} -> write(state, plan, from)
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:get, partition, key}, from, state) do
    case reserve(state, Request.get(partition), from) do
      {:ok, entry} ->
        case SlateDB.get_key_value(state.ctx.db, Keys.encode(partition, key)) do
          {:ok, nil} ->
            confirm(state, {:ok, nil}, entry)

          {:ok, row} ->
            confirm(state, {:ok, %{value: row.value, version: row.seq}}, entry)

          {:error, error} ->
            Admission.release(state.ctx, [entry.token])
            read_failed(error, state)
        end

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  defp reserve(state, plan, from) do
    started = System.monotonic_time()

    with :ok <- before_deadline(plan.deadline),
         {:ok, token} <-
           Admission.reserve(state.ctx, state.index, plan.partition, plan.reply_kind, plan.bytes) do
      {:ok, %{request: plan, from: from, started: started, token: token}}
    end
  end

  defp write(state, plan, from) do
    case reserve(state, plan, from) do
      {:ok, entry} -> evaluate(state, entry)
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp evaluate(state, %{request: plan} = entry) do
    case check_all(state.ctx.db, plan.conditions) do
      :ok ->
        commit(state, entry)

      {:conflict, key, version} ->
        reply = conflict_reply(plan.reply_kind, key, version)
        confirm(state, {:error, reply}, entry)

      {:error, error} ->
        Admission.release(state.ctx, [entry.token])
        read_failed(error, state)
    end
  end

  defp conflict_reply(:batch, key, version), do: {:conflict, key, version}
  defp conflict_reply(_kind, _key, version), do: {:conflict, version}

  defp commit(state, %{request: %{mutations: []}} = entry),
    do: confirm(state, {:ok, nil}, entry)

  defp commit(state, %{request: plan} = entry),
    do: submit(state, plan.mutations, {:mutation, plan.reply_kind}, entry)

  defp check_all(db, conditions) do
    Enum.reduce_while(conditions, :ok, fn {encoded, key, condition}, :ok ->
      case check(db, encoded, condition) do
        :ok -> {:cont, :ok}
        {:conflict, version} -> {:halt, {:conflict, key, version}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp before_deadline(nil), do: :ok

  defp before_deadline(deadline) do
    if System.os_time(:millisecond) < deadline, do: :ok, else: {:error, :deadline_exceeded}
  end

  defp read_failed(error, state) do
    Logger.debug(
      "kv shard #{state.ctx.n} partition writer #{state.index}: read failed: #{inspect(error)}"
    )

    {:reply, {:error, :unavailable}, state}
  end

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

  defp submit(state, ops, result, entry) do
    :ok = Admission.submitting(state.ctx, entry.token)

    # Admission and condition reads can outlive the request deadline.
    case before_deadline(entry.request.deadline) do
      :ok ->
        case SlateDB.write(state.ctx.db, ops) do
          {:ok, seq} ->
            :ok = Admission.submitted(state.ctx, entry.token, seq)
            Cluster.notify_when_durable(state.ctx, seq, :durable)
            {reply, op} = submission_reply(result, seq)

            entry = %{
              seq: seq,
              from: entry.from,
              reply: reply,
              op: op,
              started: entry.started,
              token: entry.token
            }

            {:noreply, %{state | inflight: :queue.in(entry, state.inflight)}}

          {:error, error} ->
            write_failed(error, entry.from, state)
        end

      {:error, _} = error ->
        Admission.release(state.ctx, [entry.token])
        {:reply, error, state}
    end
  end

  defp submission_reply({:mutation, kind}, seq) when kind in [:put, :batch],
    do: {{:ok, seq}, kind}

  defp submission_reply({:mutation, :delete}, _seq), do: {:ok, :delete}
  defp submission_reply({:confirmation, reply}, _seq), do: {reply, nil}

  # Confirmation orders observations after pending writes and proves that
  # this database handle still owns the shard.
  defp confirm(state, reply, entry) do
    submit(state, [{:put, Keys.confirm_key(), ""}], {:confirmation, reply}, entry)
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
    {entries, inflight} = durable_prefix(state.inflight, durable, [])

    if entries != [], do: :ok = Admission.release(state.ctx, Enum.map(entries, & &1.token))

    for entry <- entries do
      GenServer.reply(entry.from, entry.reply)
      if entry.op, do: written(state, entry)
    end

    %{state | inflight: inflight}
  end

  defp durable_prefix(inflight, durable, entries) do
    case :queue.out(inflight) do
      {{:value, %{seq: seq} = entry}, rest} when seq <= durable ->
        durable_prefix(rest, durable, [entry | entries])

      _ ->
        {Enum.reverse(entries), inflight}
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
