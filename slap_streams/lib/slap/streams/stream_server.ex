defmodule Slap.Streams.StreamServer do
  @moduledoc false

  use GenServer, restart: :temporary
  require Logger

  # How long a load waits for the writes it read to become durable.
  @load_durable_timeout 10_000

  alias Slap.Cluster
  alias Slap.SlateDB
  alias Slap.Streams
  alias Slap.Streams.{Clock, Fork, Group, Protocol, ShardChildren, ShardLoad, ShardState, Stream}
  alias Slap.Streams.Jobs.Repair
  alias Slap.Streams.Store.{Batch, Meta, Read, Tail}

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
      check_forks(ctx, stream)

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

  # The load read the latest writes, durable or not: a write of the previous
  # server of this stream may not be durable yet (it crashed, or stopped
  # after a write failed). Its state becomes the durable view, so wait until
  # every write made so far is durable. If one never becomes durable (the
  # shard was fenced or its store failed), the load fails, and so does the
  # request (503), rather than serve data that may be lost.
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

      {:ok, %Meta{soft_deleted: true} = meta} ->
        {:ok, Stream.gone(path, meta)}

      {:ok, %Meta{sid: sid} = meta} ->
        with {:ok, %Tail{} = tail} <- Read.get_tail(ctx.db, sid),
             {:ok, trim} <- Read.get_trim(ctx.db, sid) do
          {:ok,
           %Stream{
             path: path,
             status: if(meta.copying, do: :copying, else: :active),
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
    state = %{state | last_active: now, request: request_info(request)}

    # A failed write here is a write failure like any other (rule 7).
    with {:ok, state} <- expire(state, request, now),
         {:ok, state} <- touch(state, request, now) do
      case handle(request, from, state) do
        {:reply, reply, state} -> reply_after_expiry(reply, from, state)
        other -> other
      end
    else
      {:error, error, state} -> write_failed(error, from, state)
    end
  end

  # An expired stream is removed when next accessed (and by
  # Slap.Streams.Jobs.Expiry). The view drops it at once, but no reply based
  # on that view leaves until the removal is durable; list reads durable
  # metadata and may still see the stream before then.
  # Asking a fork what it was forked from (a check by its source) is not an
  # access: it must not remove the stream, which would unregister it.
  defp expire(state, :fork_of, _now), do: {:ok, state}

  defp expire(%{stream: %Stream{status: :active} = stream} = state, _request, now) do
    if Stream.expired?(stream, now) do
      with {:ok, state} <- commit(state, Protocol.remove(stream, nil), nil),
           do:
             {:ok,
              %{state | durable: Stream.view(state.stream), expiry_seq: state.last_write_seq}}
    else
      {:ok, state}
    end
  end

  defp expire(state, _request, _now), do: {:ok, state}

  # Reads, waits and writes reset a sliding TTL; HEAD and internal requests
  # do not (PROTOCOL.md §5.1).
  defp touch(state, request, _now)
       when request in [:head, :peek_info, :fork_of] or
              (is_tuple(request) and
                 elem(request, 0) in [
                   :expiry_check,
                   :repair_check,
                   :fork_source,
                   :unregister_fork
                 ]),
       do: {:ok, state}

  defp touch(state, _request, now) do
    case Protocol.touch(state.stream, now) do
      {:ok, stream} -> {:ok, %{state | stream: stream}}
      write -> commit(state, write, nil)
    end
  end

  # -- Requests -------------------------------------------------------------

  # Reads, head and waits use the durable view and are answered at once.
  defp handle(info, _from, state) when info in [:read_info, :peek_info],
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

      :fork ->
        fork_create(req, from, state, nil)

      :resume_fork ->
        fork_create(req, from, state, state.stream.sid)

      {:reply, reply} ->
        # A soft-deleted stream stays while it has forks: check that they
        # are all still there.
        check_forks(state.ctx, state.stream)
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

  defp handle({:fork_source, fork, req}, from, state),
    do: decide(Fork.register(state.stream, state.durable, state.ctx, fork, req), from, state)

  defp handle({:unregister_fork, fork}, from, state),
    do: decide(Fork.unregister(state.stream, fork), from, state)

  # On a fork: the stream it was forked from, while it exists.
  defp handle(:fork_of, _from, %{stream: %Stream{status: :absent}} = state),
    do: {:reply, {:ok, nil}, state}

  defp handle(:fork_of, _from, %{stream: %Stream{meta: meta}} = state),
    do: {:reply, {:ok, meta.fork_of && meta.fork_of.path}, state}

  defp handle({:expiry_check, sid, at}, from, state),
    do: decide(Protocol.expiry_check(state.stream, sid, at), from, state)

  defp handle({:repair_check, sid}, from, state),
    do: decide(Fork.repair_check(state.ctx, state.stream, sid, Clock.now_ms()), from, state)

  # A soft-deleted stream stays while it has forks. A fork that was deleted
  # but could not unregister itself (its source was unreachable) would keep
  # it forever: have the repair job check them.
  defp check_forks(ctx, %Stream{status: :gone, meta: %Meta{forks: [_ | _] = forks}} = stream),
    do: Repair.check_forks(ctx, stream.path, forks)

  defp check_forks(_ctx, _stream), do: :ok

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

  # -- Forks ----------------------------------------------------------------

  # `sid` is set when resuming an interrupted copy.
  defp fork_create(req, from, state, sid) do
    case Fork.source(state.ctx, state.stream.path, req, sid != nil) do
      {:ok, src} -> start_copy(req, from, state, sid, src)
      {:error, {:abandon, reason}} -> abandon_fork(reason, from, state)
      {:error, reason} -> reply_in_order({:error, reason}, from, state)
    end
  end

  # A new fork that fails from here on is unregistered again.
  defp start_copy(req, from, state, sid, src) do
    with {:ok, body} <- Protocol.split(src.content_type, req[:body] || "", true),
         {:ok, sid} <- if(sid, do: {:ok, sid}, else: allocate_sid(state.ctx)) do
      fork_copy(req, from, state, sid, src, body)
    else
      {:error, reason} ->
        if sid == nil, do: Fork.unregister_from(state.ctx, req[:forked_from], state.stream.path)
        reply_in_order({:error, reason}, from, state)
    end
  end

  defp fork_copy(req, from, state, sid, src, body) do
    now = Clock.now_ms()
    {copying, start} = Fork.start(state.stream.path, req, src, sid, now)

    with {:ok, state} <- copy_write(state, start, copying),
         {:ok, state} <- copy(state, req[:forked_from], Fork.copy_from(src), src.offset) do
      decide(Fork.finish(copying, req, src, body, now), from, state)
    else
      {:error, error, state} ->
        write_failed(error, from, state)

      {:sealed, state} ->
        Fork.unregister_from(state.ctx, req[:forked_from], state.stream.path)
        reply_in_order({:error, :sealed}, from, state)

      {:unreadable, :gone, state} ->
        abandon_fork(:source_gone, from, state)

      # The source could not be read: left as :copying; a retried create
      # resumes.
      {:unreadable, _reason, state} ->
        reply_in_order({:error, :unavailable}, from, state)
    end
  end

  # Copies the source's messages that start in `from..until - 1`, at the
  # same offsets.
  defp copy(state, source, from, until) do
    case Fork.read_page(state.ctx, source, from, until) do
      {:ok, page, next} ->
        with {:ok, state} <- copy_page(state, page), do: copy(state, source, next, until)

      :done ->
        {:ok, state}

      {:error, reason} ->
        {:unreadable, reason, state}
    end
  end

  defp copy_page(state, []), do: {:ok, state}

  defp copy_page(%{stream: copying} = state, page),
    do: copy_write(state, Batch.messages(copying.sid, page), copying)

  # A write of a fork's copy, before the write that finishes it. It goes in
  # flight like any other (load, ordering of replies, durable view) but
  # answers no request: the fork is acknowledged by `Fork.finish/5`'s write,
  # which becomes durable after it.
  defp copy_write(state, ops, %Stream{status: :copying} = copying),
    do: commit(state, {:write, ops, nil, copying, []}, nil)

  # The source was deleted before the copy finished (its rows go with it):
  # the fork cannot be completed. Remove the partial copy and answer as for
  # a new fork of a deleted source, so a retry does not wait forever.
  defp abandon_fork(reason, from, %{stream: stream} = state) do
    Logger.warning(
      "fork #{inspect(stream.path)}: source #{inspect(stream.meta.fork_of.path)} was deleted"
    )

    decide(Fork.drop(stream, {:error, reason}), from, state)
  end

  # -- Writes and replies ---------------------------------------------------

  defp decide({:reply, reply}, from, state), do: reply_in_order(reply, from, state)

  defp decide({:write, _, _, _, _} = write, from, state) do
    case commit(state, write, from) do
      {:ok, state} -> {:noreply, state}
      {:sealed, state} -> reply_in_order({:error, :sealed}, from, state)
      {:error, error, state} -> write_failed(error, from, state)
    end
  end

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

  # Writes `ops` without waiting, and queues `reply` to `from` (if any) for
  # when they are durable. The live stream becomes `stream`, and the
  # durable view becomes it once the write is durable.
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
          request: if(from, do: state.request)
        }

        Enum.each(effects, &run_effect(&1, state))

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

  # A write that creates the stream checks that its group is not sealed.
  defp write(%{stream: %Stream{status: :absent}} = state, ops, %Stream{status: status} = stream)
       when status != :absent,
       do: Group.create_write(state.ctx.db, Streams.placement_key(stream.path), ops)

  defp write(state, ops, _stream), do: SlateDB.write(state.ctx.db, ops)

  defp run_effect(:kick_deleter, %{ctx: ctx}),
    do: GenServer.cast(Cluster.via(ctx.cluster, ctx.n, :deleter), :kick)

  defp run_effect({:unregister_from, source}, state),
    do: Fork.unregister_from(state.ctx, source, state.stream.path)

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

  # A sliding TTL's saved deadline moves only every tenth of the TTL: save
  # the latest access when the server stops (idle, or its shard is moving;
  # the database is still open, and closing it flushes), so a reloaded
  # stream does not expire early. Best effort: a failure costs at most a
  # tenth of the TTL.
  defp save_access(state) do
    with [_ | _] = ops <- Protocol.save_access(state.stream),
         {:error, error} <- SlateDB.write(state.ctx.db, ops) do
      Logger.warning("stream #{inspect(state.stream.path)}: #{inspect(error)}")
    end
  end

  # Replies to every in-flight request and tells every waiter that the
  # stream is unavailable (the shard is stopping, or a write failed).
  defp fail_all(state) do
    for %{from: from} <- :queue.to_list(state.inflight),
        from != nil,
        do: GenServer.reply(from, {:error, :unavailable})

    for {ref, waiter} <- state.waiters,
        do: send(waiter.pid, {:slap_streams_wake, ref, :unavailable})

    ShardLoad.clear(state.load)
    %{state | inflight: :queue.new(), inflight_bytes: 0, waiters: %{}}
  end

  # Replies to every entry whose write is durable, in order, and advances
  # the durable view with them.
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

  # A write is durable: the durable view advances to it, its bytes are no
  # longer in flight, and an append's latency is reported.
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

  # What a request is, for telemetry.
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
