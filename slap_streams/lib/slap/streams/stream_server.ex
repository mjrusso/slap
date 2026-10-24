defmodule Slap.Streams.StreamServer do
  @moduledoc false

  use GenServer, restart: :temporary
  require Logger

  # How long a load waits for the writes it read to become durable.
  @load_durable_timeout 10_000

  alias Slap.Cluster
  alias Slap.SlateDB
  alias Slap.Streams

  alias Slap.Streams.{
    Clock,
    Group,
    Protocol,
    Retention,
    ShardChildren,
    ShardLoad,
    ShardState,
    Stream
  }

  alias Slap.Streams.Creation.Target
  alias Slap.Streams.Jobs.Repair
  alias Slap.Streams.Store.{Identity, Meta, Read, Tail}

  @default_idle_timeout :timer.minutes(5)

  @enforce_keys [:ctx, :stream, :durable, :load, :idle_timeout, :last_active]
  defstruct @enforce_keys ++
              [
                inflight: :queue.new(),
                inflight_bytes: 0,
                request: nil,
                last_write_seq: 0,
                expiry_seq: nil,
                waiters: %{}
              ]

  # Writes advance `stream` at once so requests can pipeline. Reads and
  # waiters use `durable`, which advances only after the write is durable.
  # `inflight` holds, in request order, a reply for each write not yet
  # durable (with the durable view as of that write) and each reply queued
  # behind one. `request` is the request being handled, for telemetry.

  # -- Client side (runs in the caller, with the shard's context) -----------

  @doc false
  # Sends `request` to the server for `path`, starting it if needed. A server
  # that stops between lookup and call (idle, deleted, crashed) is started
  # again; the registry may still list a dead server for a moment.
  def call(ctx, path, request, timeout) do
    call(ctx, path, request, timeout, 5)
  end

  defp call(ctx, path, request, timeout, retries) do
    with {:ok, pid} <- ensure_started(ctx, path) do
      try do
        GenServer.call(pid, request, timeout)
      catch
        :exit, {reason, _} when reason in [:noproc, :normal] and retries > 0 ->
          if reason == :noproc, do: Process.sleep(1)
          call(ctx, path, request, timeout, retries - 1)

        :exit, {:timeout, _} ->
          {:error, :timeout}

        :exit, _ ->
          {:error, :unavailable}
      end
    end
  end

  defp ensure_started(ctx, path) do
    case Registry.lookup(ctx.registry, {ctx.n, {:stream, path}}) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(
               ShardChildren.streams_supervisor(ctx),
               {__MODULE__, {ctx, path}}
             ) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, _} -> {:error, :unavailable}
        end
    end
  catch
    # The shard's supervisor is gone: the shard is stopping.
    :exit, _ -> {:error, :unavailable}
  end

  # -- Server ---------------------------------------------------------------

  def start_link({ctx, path}) do
    GenServer.start_link(__MODULE__, {ctx, path},
      name: Cluster.via(ctx.cluster, ctx.n, {:stream, path})
    )
  end

  @impl true
  def init({ctx, path}) do
    # To fail in-flight requests when the shard stops.
    Process.flag(:trap_exit, true)

    with {:ok, stream} <- load(ctx, path),
         :ok <- await_loaded_durable(ctx) do
      idle = Keyword.get(ctx.child_options, :idle_timeout, @default_idle_timeout)
      Process.send_after(self(), :idle_check, idle)

      {:ok,
       %__MODULE__{
         ctx: ctx,
         stream: stream,
         durable: Stream.view(stream),
         load: ShardLoad.register(ShardLoad.handle(ctx), ctx),
         idle_timeout: idle,
         last_active: Clock.now_ms()
       }}
    else
      {:error, reason} -> {:stop, {:load_failed, reason}}
    end
  end

  # Loading includes writes left by the previous server. They must become
  # durable before this state can serve reads.
  defp await_loaded_durable(ctx) do
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

  defp load(ctx, path) do
    case Read.get_meta(ctx.db, path) do
      {:ok, nil} ->
        {:ok, Stream.absent(path)}

      {:ok, %Meta{sid: sid} = meta} ->
        with {:ok, %Tail{} = tail} <- Read.get_tail(ctx.db, sid),
             {:ok, trim} <- Read.get_trim(ctx.db, sid) do
          {:ok,
           %Stream{
             path: path,
             status:
               cond do
                 meta.soft_deleted -> :gone
                 Meta.pending?(meta) -> :pending
                 true -> :active
               end,
             meta: meta,
             sid: sid,
             tail: tail.next_offset,
             producers: Read.list_producers(ctx.db, sid),
             last_access: tail.last_access_ms || Clock.now_ms(),
             expiry_key: tail.expiry_key_ms,
             trim: trim
           }}
        end

      {:error, _} = error ->
        error
    end
  end

  @impl true
  def handle_call(request, from, state) do
    now = Clock.now_ms()
    policy = lifecycle(if is_tuple(request), do: elem(request, 0), else: request)
    state = if policy.activity, do: %{state | last_active: now}, else: state
    state = %{state | request: request_info(request)}

    with {:ok, state} <- expire(state, policy.expire, now),
         {:ok, state} <- touch(state, policy.touch, now) do
      case handle(request, from, state) do
        {:reply, reply, state} -> reply_after_expiry(reply, from, state)
        other -> other
      end
    else
      {:error, error, state} -> write_failed(error, from, state)
    end
  end

  defp lifecycle(operation)
       when operation in [
              :read_info,
              :wait,
              :cancel_wait,
              :create,
              :append,
              :delete,
              :trim,
              :reservation,
              :reserve
            ],
       do: %{activity: true, expire: true, touch: true}

  defp lifecycle(operation)
       when operation in [
              :head,
              :source_identity,
              :register,
              :capture,
              :activate,
              :copy_page,
              :reconcile_creation
            ],
       do: %{activity: true, expire: true, touch: false}

  defp lifecycle(:expiry_check), do: %{activity: false, expire: true, touch: false}

  defp lifecycle(operation)
       when operation in [:repair_check, :terminal, :unregister, :compact_registration],
       do: %{activity: false, expire: false, touch: false}

  defp expire(state, false, _now), do: {:ok, state}

  # Expiry changes the read view before its write is durable. Every reply
  # using that view must wait for expiry_seq.
  defp expire(%{stream: %Stream{status: :active} = stream} = state, _request, now) do
    if Stream.expired?(stream, now) do
      with {:ok, state} <- commit(state, Retention.remove(stream, nil), nil),
           do:
             {:ok,
              %{state | durable: Stream.view(state.stream), expiry_seq: state.last_write_seq}}
    else
      {:ok, state}
    end
  end

  defp expire(state, _request, _now), do: {:ok, state}

  defp touch(state, false, _now), do: {:ok, state}

  defp touch(state, _request, now) do
    case Protocol.touch(state.stream, now) do
      {:ok, stream} -> {:ok, %{state | stream: stream}}
      write -> commit(state, write, nil)
    end
  end

  # -- Requests -------------------------------------------------------------

  # Reads, head and waits use the durable view and are answered at once.
  defp handle(:read_info, _from, state),
    do: {:reply, {:ok, state.durable}, state}

  defp handle(:head, _from, state), do: {:reply, Protocol.head(state.durable), state}

  defp handle({:wait, offset, pid}, _from, state) do
    case Protocol.wait(state.durable, offset) do
      {:reply, reply} ->
        {:reply, reply, state}

      {:wait, offset} ->
        ref = Process.monitor(pid)
        waiter = %{pid: pid, offset: offset, sid: state.durable.sid}

        Streams.Telemetry.execute([:wait, :registered], %{}, %{
          shard: state.ctx.n,
          path: state.stream.path,
          offset: offset
        })

        {:reply, {:ok, {:waiting, ref, self()}},
         set_waiters(state, Map.put(state.waiters, ref, waiter))}
    end
  end

  defp handle({:cancel_wait, ref}, _from, state) do
    Process.demonitor(ref, [:flush])
    {:reply, :ok, set_waiters(state, Map.delete(state.waiters, ref))}
  end

  # Writes (and replies that depend on them) go through the live stream.
  defp handle({:create, req}, from, state) do
    case Protocol.create_action(state.stream, req) do
      :create ->
        create(req, from, state)

      {:reply, reply} ->
        reply_in_order(reply, from, state)
    end
  end

  defp handle({:append, req}, from, state) do
    bytes = byte_size(req[:body] || "")

    if ShardLoad.admit?(state.load, state.inflight_bytes, bytes, state.ctx.child_options) do
      decide(Protocol.append(state.stream, req), from, state)
    else
      # Backpressure. The reply depends on nothing in flight, so it goes now.
      Streams.Telemetry.execute([:append, :rejected], %{bytes: bytes}, %{
        shard: state.ctx.n,
        reason: :overloaded
      })

      {:reply, {:error, :overloaded}, state}
    end
  end

  defp handle(:delete, from, state), do: decide(Protocol.delete(state.stream), from, state)

  defp handle({:trim, offset}, from, state),
    do: decide(Protocol.trim(state.stream, offset), from, state)

  defp handle(:source_identity, _from, %{durable: %{status: :active} = view} = state),
    do: {:reply, {:ok, Identity.new(state.ctx, state.stream.path, view.sid)}, state}

  defp handle(:source_identity, _from, %{durable: %{status: status}} = state) do
    error =
      case status do
        :absent -> :source_not_found
        :gone -> :source_gone
        _ -> :unavailable
      end

    {:reply, {:error, error}, state}
  end

  defp handle({:reservation, kind, req}, from, state),
    do: decide(Target.reservation(state.stream, kind, req), from, state)

  defp handle({:reserve, kind, req, source}, from, state) do
    case Target.reservation(state.stream, kind, req) do
      {:reply, :needs_source} ->
        case allocate_sid(state.ctx) do
          {:ok, sid} ->
            decide(
              Target.reserve(state.ctx, state.stream, kind, req, source, sid, Clock.now_ms()),
              from,
              state
            )

          {:error, reason} ->
            reply_in_order({:error, reason}, from, state)
        end

      decision ->
        decide(decision, from, state)
    end
  end

  defp handle({:register, c, resolved}, from, state),
    do:
      decide(Retention.register(state.stream, state.durable, state.ctx, c, resolved), from, state)

  defp handle({:capture, target, cap}, from, state),
    do: decide(Target.capture(state.stream, target, cap, Clock.now_ms()), from, state)

  defp handle({:copy_page, target, cursor, page, next}, from, state),
    do:
      decide(
        Target.page(state.stream, target, cursor, page, next, Clock.now_ms()),
        from,
        state
      )

  defp handle({:activate, target}, from, state),
    do: decide(Target.activate(state.stream, target, nil, Clock.now_ms()), from, state)

  defp handle({:activate, target, cap}, from, state),
    do: decide(Target.activate(state.stream, target, cap, Clock.now_ms()), from, state)

  defp handle({:reconcile_creation, target, failure}, from, state),
    do: decide(Target.reconcile(state.stream, target, failure), from, state)

  defp handle({:unregister, source, target}, from, state),
    do: decide(Retention.unregister(state.ctx, state.stream, source, target), from, state)

  defp handle({:compact_registration, source, target}, from, state),
    do:
      decide(Retention.compact_registration(state.ctx, state.stream, source, target), from, state)

  defp handle({:terminal, target}, from, state),
    do: decide(Retention.terminal(state.stream, target), from, state)

  defp handle({:expiry_check, sid, at}, from, state),
    do: decide(Protocol.expiry_check(state.stream, sid, at), from, state)

  defp handle({:repair_check, sid}, from, state),
    do: decide(Retention.repair_check(state.ctx, state.stream, sid, Clock.now_ms()), from, state)

  defp create(req, from, state) do
    with {:ok, messages} <- Protocol.create_messages(req),
         {:ok, sid} <- allocate_sid(state.ctx) do
      decide(Protocol.create(state.stream, req, messages, sid, Clock.now_ms()), from, state)
    else
      {:error, reason} -> reply_in_order({:error, reason}, from, state)
    end
  end

  defp allocate_sid(ctx) do
    case ShardState.allocate_sid(ctx) do
      {:ok, sid} -> {:ok, sid}
      {:error, _} -> {:error, :unavailable}
    end
  end

  # -- Writes and replies ---------------------------------------------------

  defp decide({:reply, reply}, from, state), do: reply_in_order(reply, from, state)

  defp decide({:write, _, _, _, _} = write, from, state) do
    case commit(state, write, from) do
      {:ok, state} -> {:noreply, state}
      {:sealed, state} -> sealed(from, state)
      {:error, error, state} -> write_failed(error, from, state)
    end
  end

  defp sealed(from, %{stream: %Stream{status: :pending} = s} = state),
    do: decide(Retention.remove(s, {:error, :sealed}), from, state)

  defp sealed(from, state), do: reply_in_order({:error, :sealed}, from, state)

  # Even an error or duplicate reply may depend on preceding live writes;
  # it cannot overtake them before they are durable.
  defp reply_in_order(reply, from, state) do
    if :queue.is_empty(state.inflight) do
      {:reply, reply, state}
    else
      entry = %{seq: state.last_write_seq, from: from, reply: reply, view: nil}
      {:noreply, %{state | inflight: :queue.in(entry, state.inflight)}}
    end
  end

  defp reply_after_expiry(reply, _from, %{expiry_seq: nil} = state),
    do: {:reply, reply, state}

  defp reply_after_expiry(reply, from, state), do: reply_in_order(reply, from, state)

  # Replies and effects must wait for durability: effects can initiate
  # retention cleanup.
  defp commit(state, {:write, ops, reply, %Stream{} = stream, effects}, from) do
    case write(state, ops, stream) do
      {:ok, seq} ->
        Cluster.notify_when_durable(state.ctx, seq, :durable)
        bytes = ops_bytes(ops)
        ShardLoad.add(state.load, bytes, 1, 0)

        entry = %{
          seq: seq,
          from: from,
          reply: reply,
          view: Stream.view(stream),
          bytes: bytes,
          request: if(from, do: state.request),
          effects: effects
        }

        {:ok,
         %{
           state
           | stream: stream,
             inflight: :queue.in(entry, state.inflight),
             inflight_bytes: state.inflight_bytes + bytes,
             last_write_seq: seq
         }}

      {:error, :sealed} ->
        {:sealed, state}

      {:error, error} ->
        {:error, error, state}
    end
  end

  defp write(%{stream: %Stream{status: :absent}} = state, ops, %Stream{status: status} = stream)
       when status in [:pending, :active],
       do: Group.create_write(state.ctx.db, Streams.placement_key(stream.path), ops)

  # A seal can arrive after reservation, so activation must check it again.
  defp write(
         %{stream: %Stream{status: :pending}} = state,
         ops,
         %Stream{status: :active} = stream
       ),
       do: Group.create_write(state.ctx.db, Streams.placement_key(stream.path), ops)

  defp write(state, ops, _stream), do: SlateDB.write(state.ctx.db, ops)

  defp run_effect(:kick_deleter, %{ctx: ctx}),
    do: GenServer.cast(Cluster.via(ctx.cluster, ctx.n, :deleter), :kick)

  defp run_effect({:repair_source, source, target}, %{ctx: ctx}),
    do: Repair.notify(ctx.cluster, source, target)

  # The live stream may no longer match storage. Fail everything in flight
  # and stop; the next request reloads from storage.
  defp write_failed(error, from, state) do
    Logger.error("stream #{inspect(state.stream.path)}: write failed: #{inspect(error)}")
    Streams.Telemetry.execute([:stream_server, :write_failed], %{}, %{shard: state.ctx.n})
    if from, do: GenServer.reply(from, {:error, :unavailable})
    {:stop, {:shutdown, :write_failed}, fail_all(state)}
  end

  @impl true
  def handle_info({:slap_cluster_durable, :durable}, state) do
    {:noreply, drain(state, Cluster.durable_seq(state.ctx))}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, set_waiters(state, Map.delete(state.waiters, ref))}
  end

  def handle_info(:idle_check, state) do
    idle? =
      :queue.is_empty(state.inflight) and map_size(state.waiters) == 0 and
        Clock.now_ms() - state.last_active >= state.idle_timeout

    if idle? do
      {:stop, :normal, state}
    else
      Process.send_after(self(), :idle_check, state.idle_timeout)
      {:noreply, state}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    save_access(state)
    fail_all(state)
  end

  # Save access before shutdown to preserve sliding-TTL resets across reloads.
  # The shard database stays open through termination and flushes on close.
  # A failed save can lose at most a tenth of the TTL.
  defp save_access(state) do
    with [_ | _] = ops <- Protocol.save_access(state.stream),
         {:error, error} <- SlateDB.write(state.ctx.db, ops) do
      Logger.warning("stream #{inspect(state.stream.path)}: #{inspect(error)}")
    end
  end

  defp fail_all(state) do
    for %{from: from} <- :queue.to_list(state.inflight),
        from != nil,
        do: GenServer.reply(from, {:error, :unavailable})

    for {ref, waiter} <- state.waiters,
        do: send(waiter.pid, {:slap_streams_wake, ref, :unavailable})

    ShardLoad.clear(state.load)
    %{state | inflight: :queue.new(), inflight_bytes: 0, waiters: %{}}
  end

  defp drain(state, durable) do
    case :queue.peek(state.inflight) do
      {:value, %{seq: seq} = entry} when seq <= durable ->
        if entry.from, do: GenServer.reply(entry.from, entry.reply)
        state = %{state | inflight: :queue.drop(state.inflight)}
        state = if entry.view, do: written(state, entry), else: state
        drain(state, durable)

      _ ->
        wake(state)
    end
  end

  defp wake(%{durable: view} = state) do
    {woken, waiting} =
      state.waiters
      |> Enum.map(fn {ref, w} -> {ref, w, Protocol.wake_reason(view, w.sid, w.offset)} end)
      |> Enum.split_with(fn {_ref, _w, reason} -> reason != nil end)

    for {ref, w, reason} <- woken do
      Process.demonitor(ref, [:flush])
      send(w.pid, {:slap_streams_wake, ref, reason})
    end

    set_waiters(state, Map.new(waiting, fn {ref, w, nil} -> {ref, w} end))
  end

  defp written(state, entry) do
    ShardLoad.add(state.load, -entry.bytes, -1, 0)

    case entry do
      %{request: {:append, started, bytes}, reply: {:ok, %{result: result}}} ->
        Streams.Telemetry.execute(
          [:append, :acknowledged],
          %{duration: System.monotonic_time() - started, bytes: bytes},
          %{shard: state.ctx.n, result: result}
        )

      _ ->
        :ok
    end

    Enum.each(entry.effects, &run_effect(&1, state))

    expiry_seq =
      case state.expiry_seq do
        seq when is_integer(seq) and entry.seq >= seq -> nil
        seq -> seq
      end

    %{
      state
      | durable: entry.view,
        expiry_seq: expiry_seq,
        inflight_bytes: state.inflight_bytes - entry.bytes
    }
  end

  defp set_waiters(state, waiters) do
    ShardLoad.add(state.load, 0, 0, map_size(waiters) - map_size(state.waiters))
    %{state | waiters: waiters}
  end

  defp request_info({:append, req}),
    do: {:append, System.monotonic_time(), byte_size(req[:body] || "")}

  defp request_info(_request), do: nil

  defp ops_bytes(ops) do
    Enum.reduce(ops, 0, fn
      {:put, key, value}, acc -> acc + byte_size(key) + byte_size(value)
      {_op, key}, acc -> acc + byte_size(key)
      _, acc -> acc
    end)
  end
end
