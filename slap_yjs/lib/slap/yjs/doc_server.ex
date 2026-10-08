defmodule Slap.Yjs.DocServer do
  @moduledoc """
  A `Yex.DocServer` whose document is stored with `Slap.Yjs.Store`, and shared
  with the servers of the same document on other nodes through it.

      defmodule MyApp.DocServer do
        use Slap.Yjs.DocServer
      end

      {:ok, pid} = Slap.Yjs.Docs.join(MyApp.DocServer, {"my-service", "doc-1"})

  Several servers may run for one document, one per node with clients
  (`Slap.Yjs.Docs` starts them). None owns the document: each appends its own
  clients' updates to the document's `.updates` stream and follows that
  stream, applying everything in it, so each converges on
  the stream's contents. Yjs updates commute and applying one twice changes
  nothing, so the order of the appends and the server's own updates coming
  back do not matter.

  ## Storing

  Before serving requests the server loads the document (the current
  snapshot, then the updates after it) and follows the stream from there.
  Every update from a client or a local edit is framed into a buffer. The
  buffer is appended after `:flush_after` milliseconds or once it holds
  `:flush_bytes`, whichever comes first. One append runs at a time: what is
  buffered meanwhile goes in the next one, once it is acknowledged. An
  append that fails for a transient reason (`:unavailable`, `:timeout`,
  `:overloaded`, as when the document's shard moves) is retried; any other
  failure stops the server with `{:slap_yjs_append_failed, reason}`.

  The server relays a client's update to its other subscribers before
  storing it. If the server stops first, the update is lost unless a client
  that has it resynchronises. Servers on other nodes receive only stored
  updates.

  A server started through `Slap.Yjs.Docs` begins loading after `init/2`
  returns. Overrides of `init/2` cannot assume stored updates are present.
  `Slap.Yjs.Docs.join/3` and `subscribe/3` wait for loading to finish.

  ## Compaction

  Once the updates read since the last snapshot reach
  `max(:compact_bytes, last snapshot size / 2)`, the server compacts the
  document (`Slap.SnapshotLog.snapshot/4`) in a task, one compaction at a
  time, and keeps serving meanwhile. The snapshot is the document's state
  at the offset it has read up to: every update before that offset has been
  applied. It may hold more (updates pending, and local ones not appended
  yet), which `Slap.SnapshotLog` allows because applying a Yjs update twice
  changes nothing. If another server indexed a newer snapshot meanwhile,
  the compaction is superseded, which is not an error. A compaction that
  fails is tried again once the server reads more.

  ## Clients

  A client process subscribes with `Slap.Yjs.Docs.join/3` (or `subscribe/3`),
  sends its messages with `process_message_v1(server, message, self())`,
  and receives:

    * `{:slap_yjs_update, doc_id, update}` - a document update (v1) from another
      client, on any node;
    * `{:slap_yjs_awareness, doc_id, update}` - an awareness (presence) update.

  `encode_message/1` encodes these messages for the client.

  The server monitors its subscribers. When one goes down, the awareness
  states it set are removed. After `:idle_timeout` with no subscribers the
  server stops.

  ## Awareness

  Awareness is not stored. The servers of a document form a `:pg` group
  (scope `Slap.Yjs.PG`, started by `Slap.Yjs.Docs`) and send each other their
  clients' awareness updates. Each monitors the group (`:pg.monitor/2`,
  which needs OTP 26.2 or later): when a server joins, each of the others
  sends it the states its own clients set (not those it learned from other
  servers); when one leaves (it stops, or its node goes down), the others
  remove the states that came through it.

  ## Deletion

  When the document is deleted (`Slap.Yjs.Docs.delete/2`), each of its
  servers stops with `{:shutdown, :deleted}`, told by the delete or once
  its follower or an append finds out, and stores nothing more: the
  deletion is permanent, and what it had buffered is dropped. Starting a
  deleted document directly returns `{:error, {:shutdown, :deleted}}`;
  `Slap.Yjs.Docs.join/3` returns `{:error, :deleted}`.

  ## Stopping

  Unless an append failed or the document was deleted, the server appends
  what it has buffered on stop, waits up to `:terminate_timeout` for its
  appends and a running compaction, then compacts if it has read anything
  since the last snapshot (`:compact_on_stop`). It traps exits so that a
  supervisor's shutdown reaches `terminate/2`. By default, its
  `child_spec/1` and `Slap.Yjs.Docs` allow `:terminate_timeout` plus 30 seconds
  per server.
  `Docs` stops its document servers concurrently, so its shutdown takes at
  most the largest child shutdown timeout, plus supervisor overhead. With
  defaults, that is about 35 seconds. A server still stopping at its limit
  is killed; a partial snapshot publication is safe to retry.

  ## Options

  Besides `Yex.DocServer`'s options, such as `:assigns`:

    * `:doc_id` (required) - the `Slap.Yjs.Store` document, `{service, name}`.
    * `:flush_after` - milliseconds to buffer an update (default 10).
    * `:flush_bytes` - buffered bytes that start an append (default 256 KiB).
    * `:compact_bytes` - the least bytes read that start a compaction
      (default 1 MiB).
    * `:compact_on_stop` - compact on stop (default true).
    * `:compaction` - options for `Slap.SnapshotLog.snapshot/4`, including
      `:history`, `:cluster`, and a per-call `:timeout` (default `[]`).
    * `:idle_timeout` - milliseconds without subscribers before stopping
      (default `:infinity`; `Slap.Yjs.Docs` starts servers with 30 s).
    * `:terminate_timeout` - milliseconds to wait for appends and a running
      compaction on stop (default 5000).
    * `:store` - `Slap.Yjs.Store` options, including `:prefix` and `:cluster`.

  A module that overrides `init/2`, `handle_update_v1/4`,
  `handle_awareness_update/4`, `handle_info/2` or `terminate/2` must call
  `super` (for `handle_info/2`, with the messages it does not handle
  itself).

  ## Telemetry

    * `[:slap, :yjs, :doc_server, :append]` when an append starts, with
      measurements `%{bytes: b, updates: n}`.
    * `[:slap, :yjs, :doc_server, :compact]` when a compaction finishes, with
      measurements `%{bytes: snapshot_size}` and `:offset` in the metadata.

  The metadata of both includes `:doc_id`.
  """

  require Logger

  import Slap.Yjs.Follower, only: [transient?: 1]

  alias Slap.Yjs
  alias Slap.Yjs.DocServer.{Appends, Compaction, Sync}
  alias Yex.DocServer.State

  # The origin of the updates read from the store: applied, broadcast, not
  # appended again.
  @stored :slap_yjs
  # The origin of pending updates applied again: appended.
  @pending :yjs_pending
  @key __MODULE__
  @ready_waiters {__MODULE__, :ready_waiters}
  @pg Yjs.PG

  @defaults [
    flush_after: 10,
    flush_bytes: 256 * 1024,
    compact_bytes: 1024 * 1024,
    compact_on_stop: true,
    compaction: [],
    idle_timeout: :infinity,
    terminate_timeout: 5_000
  ]

  @stop_compaction_budget 30_000

  @doc false
  @spec shutdown_timeout(keyword()) :: non_neg_integer()
  def shutdown_timeout(opts) do
    Keyword.get(opts, :terminate_timeout, Keyword.fetch!(@defaults, :terminate_timeout)) +
      @stop_compaction_budget
  end

  @doc false
  def option_keys, do: Keyword.keys(@defaults)

  @doc false
  def validate_options!(opts) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")

    for key <- [:flush_after, :terminate_timeout],
        do: validate_option!(opts, key, &nonneg_integer?/1, "a non-negative integer")

    for key <- [:flush_bytes, :compact_bytes],
        do: validate_option!(opts, key, &positive_integer?/1, "a positive integer")

    validate_option!(
      opts,
      :idle_timeout,
      &valid_timeout?/1,
      "a non-negative integer or :infinity"
    )

    validate_option!(opts, :compact_on_stop, &is_boolean/1, "boolean")
    validate_option!(opts, :assigns, &is_map/1, "a map")
    if Keyword.has_key?(opts, :compaction), do: validate_compaction!(opts[:compaction])
  end

  defp validate_compaction!(compaction) do
    unless Keyword.keyword?(compaction),
      do: raise(ArgumentError, ":compaction must be a keyword list")

    Keyword.validate!(compaction, [:history, :cluster, :timeout])

    validate_option!(
      compaction,
      :history,
      &valid_history?/1,
      "positive {every_ms, keep_ms} rules"
    )

    validate_option!(compaction, :cluster, &module?/1, "a module")

    validate_option!(
      compaction,
      :timeout,
      &valid_timeout?/1,
      "a non-negative integer or :infinity"
    )
  end

  defp validate_option!(opts, key, valid?, expected) do
    if Keyword.has_key?(opts, key) and not valid?.(opts[key]),
      do: raise(ArgumentError, "#{inspect(key)} must be #{expected}")
  end

  defp valid_history?(rules) when is_list(rules) do
    Enum.all?(rules, fn
      {every_ms, keep_ms} -> positive_integer?(every_ms) and positive_integer?(keep_ms)
      _ -> false
    end)
  end

  defp valid_history?(_rules), do: false
  defp nonneg_integer?(value), do: is_integer(value) and value >= 0
  defp positive_integer?(value), do: is_integer(value) and value > 0
  defp valid_timeout?(value), do: value == :infinity or nonneg_integer?(value)
  defp module?(value), do: is_atom(value) and value != nil

  # The server's own state, in its `Yex.DocServer.State` assigns. Each
  # workflow has its own: appends, compaction (and how far the server has
  # read), and sync/2 callers. `tasks` holds the running tasks (see
  # start_task/3): pid => :append | :compaction | :probe.
  @enforce_keys [:doc_id, :store, :pg, :opts, :follower, :appends, :compaction, :group, :owners]
  defstruct [
    :doc_id,
    :store,
    :pg,
    # :compact_on_stop, :compaction (Slap.SnapshotLog.snapshot/4 options),
    # :idle_timeout, :terminate_timeout.
    :opts,
    :follower,
    :appends,
    :compaction,
    # The monitor of the document's :pg group.
    :group,
    # Who set each awareness state (Slap.Yjs.Owners).
    :owners,
    sync: %Sync{},
    tasks: %{},
    # pid => monitor ref
    subscribers: %{},
    idle_timer: nil
  ]

  defmacro __using__(opts) do
    quote do
      use Yex.DocServer, unquote(opts)

      def child_spec(arg) do
        shutdown =
          Keyword.get(unquote(opts), :shutdown, Yjs.DocServer.shutdown_timeout(arg))

        Map.put(super(arg), :shutdown, shutdown)
      end

      @impl Yex.DocServer
      def init(arg, state), do: Yjs.DocServer.init(arg, state)

      @impl Yex.DocServer
      def handle_update_v1(doc, update, origin, state),
        do: Yjs.DocServer.handle_update_v1(doc, update, origin, state)

      @impl Yex.DocServer
      def handle_awareness_update(awareness, change, origin, state),
        do: Yjs.DocServer.handle_awareness_update(awareness, change, origin, state)

      @impl Yex.DocServer
      def handle_info(message, state), do: Yjs.DocServer.handle_info(message, state)

      @impl Yex.DocServer
      def terminate(reason, state), do: Yjs.DocServer.terminate(reason, state)

      defoverridable init: 2,
                     handle_update_v1: 4,
                     handle_awareness_update: 4,
                     handle_info: 2,
                     terminate: 2
    end
  end

  @doc false
  def pg_scope, do: @pg

  # -- Client API -------------------------------------------------------------

  @doc "Subscribes `pid` to updates after loading finishes. `timeout` bounds both waits."
  @spec subscribe(GenServer.server(), pid(), timeout()) :: :ok | {:error, term()}
  def subscribe(server, pid, timeout) do
    deadline = timeout_deadline(timeout)

    with :ok <- call(server, :ready, timeout),
         do: call(server, {:subscribe, pid}, remaining_timeout(deadline))
  end

  @doc "Unsubscribes `pid`, and removes the awareness states it set."
  @spec unsubscribe(GenServer.server(), pid(), timeout()) :: :ok | {:error, term()}
  def unsubscribe(server, pid, timeout), do: call(server, {:unsubscribe, pid}, timeout)

  @doc """
  Returns the server's `Yex.Doc`. y_ex functions called on it run in the
  server. Edits are relayed and stored like a client's updates. The handle
  is valid only while the server runs.
  """
  @spec doc(GenServer.server(), timeout()) :: {:ok, Yex.Doc.t()} | {:error, term()}
  def doc(server, timeout), do: call(server, :doc, timeout)

  @doc """
  Encodes a `{:slap_yjs_update, _, _}` or `{:slap_yjs_awareness, _, _}`
  message as the y-protocols v1 message to send to a client.
  """
  @spec encode_message({:slap_yjs_update | :slap_yjs_awareness, Yjs.Store.doc(), binary()}) ::
          binary()
  def encode_message({:slap_yjs_update, _doc_id, update}),
    do: Yex.Sync.message_encode!({:sync, {:sync_update, update}})

  def encode_message({:slap_yjs_awareness, _doc_id, update}),
    do: Yex.Sync.message_encode!({:awareness, update})

  @doc """
  Waits until the server has appended everything it had buffered, has read
  the document's stream up to its tail as of the call (so it has applied
  what other servers had stored), and has no compaction running.
  """
  @spec sync(GenServer.server(), timeout()) :: :ok | {:error, term()}
  def sync(server, timeout), do: call(server, :sync, timeout)

  # The server's handle_call/3 belongs to the module that uses this one, so
  # these requests are messages, answered with {ref, reply}.
  # The reply goes to an alias of the monitor, which a timeout deactivates,
  # so a late reply is dropped; the server is told to forget the request.
  defp call(server, message, timeout) do
    unless timeout == :infinity or (is_integer(timeout) and timeout >= 0),
      do: raise(ArgumentError, "timeout must be a non-negative integer or :infinity")

    case GenServer.whereis(server) do
      nil ->
        {:error, {:down, :noproc}}

      pid ->
        ref = Process.monitor(pid, alias: :reply_demonitor)
        send(pid, {@key, :request, message, self(), ref})

        receive do
          {^ref, reply} ->
            reply

          {:DOWN, ^ref, :process, _pid, reason} ->
            {:error, {:down, reason}}
        after
          timeout ->
            Process.demonitor(ref, [:flush])
            send(pid, {@key, :cancel, ref})
            {:error, :timeout}
        end
    end
  end

  defp timeout_deadline(:infinity), do: :infinity

  defp timeout_deadline(timeout) when is_integer(timeout) and timeout >= 0,
    do: System.monotonic_time(:millisecond) + timeout

  defp timeout_deadline(_timeout),
    do: raise(ArgumentError, "timeout must be a non-negative integer or :infinity")

  defp remaining_timeout(:infinity), do: :infinity
  defp remaining_timeout(deadline), do: remaining(deadline)

  @doc false
  # Stops the document's servers on every node, once it is deleted, and
  # waits up to `timeout` for them (Slap.Yjs.Docs.delete/2).
  @spec stop_deleted(Yjs.Store.doc(), timeout(), atom()) :: :ok
  def stop_deleted(doc_id, timeout, pg \\ @pg) do
    deadline = System.monotonic_time(:millisecond) + timeout

    monitors =
      for pid <- :pg.get_members(pg, {@key, doc_id}) do
        ref = Process.monitor(pid)
        send(pid, {@key, :deleted})
        ref
      end

    for ref <- monitors do
      receive do
        {:DOWN, ^ref, :process, _pid, _reason} -> :ok
      after
        remaining(deadline) -> Process.demonitor(ref, [:flush])
      end
    end

    :ok
  end

  # -- Callbacks --------------------------------------------------------------

  @doc "Starts loading the document. Called by the default `init/2`."
  @spec init(keyword(), State.t()) :: {:ok, State.t()} | {:stop, term()}
  def init(arg, state) do
    validate_options!(arg)

    if Keyword.get(arg, :__slap_yjs_defer_load__, false) do
      send(self(), {@key, :load, arg})
      {:ok, state}
    else
      load(arg, state)
    end
  end

  defp load(arg, %State{doc: doc} = state) do
    doc_id = Keyword.fetch!(arg, :doc_id)
    store = Keyword.get(arg, :store, [])
    pg = Keyword.get(arg, :pg, @pg)
    opts = Keyword.merge(@defaults, Keyword.take(arg, Keyword.keys(@defaults)))

    case Yjs.Store.load(doc_id, store) do
      {:ok, loaded} ->
        apply_stored(doc, List.wrap(loaded.snapshot) ++ loaded.updates)
        Process.flag(:trap_exit, true)

        compaction =
          Compaction.new(
            opts[:compact_bytes],
            loaded.offset,
            frames_bytes(loaded.updates),
            byte_size(loaded.snapshot || "")
          )

        persistence = %__MODULE__{
          doc_id: doc_id,
          store: store,
          pg: pg,
          opts:
            Map.new(
              Keyword.take(opts, [
                :compact_on_stop,
                :compaction,
                :idle_timeout,
                :terminate_timeout
              ])
            ),
          follower: Yjs.Follower.start_link(doc_id, loaded.offset, self(), store),
          appends: Appends.new(opts),
          compaction: compaction,
          group: join_group(doc_id, pg),
          owners: %{}
        }

        state = state |> State.assign(@key, persistence) |> idle()
        mark_ready(arg, state.module, doc_id)
        {:ok, reply_ready_waiters(state)}

      {:error, :deleted} ->
        {:stop, {:shutdown, :deleted}}

      {:error, reason} ->
        {:stop, {:slap_yjs_load_failed, reason}}
    end
  end

  defp mark_ready(arg, module, doc_id) do
    case Keyword.fetch(arg, :__slap_yjs_registry__) do
      {:ok, registry} ->
        {:ready, _previous} =
          Registry.update_value(registry, {module, doc_id}, fn _ -> :ready end)

        :ok

      :error ->
        :ok
    end
  end

  defp reply_ready_waiters(%State{assigns: assigns} = state) do
    for from <- Map.get(assigns, @ready_waiters, []), do: reply(from, :ok)
    %{state | assigns: Map.delete(assigns, @ready_waiters)}
  end

  # Without Slap.Yjs.Docs (and its :pg scope), awareness stays on this server.
  defp join_group(doc_id, pg) do
    case Process.whereis(pg) do
      nil ->
        nil

      _scope ->
        group = {@key, doc_id}
        {ref, _members} = :pg.monitor(pg, group)
        :ok = :pg.join(pg, group, self())
        ref
    end
  end

  # yrs keeps an update whose predecessors are missing pending, and retries
  # it only when a client's clock advances, not when an update fills a gap
  # it skipped: pending updates are applied again after stored ones (the
  # server reads its clients' updates back once appended).

  # One transaction with the stored origin, so handle_update_v1/4 does not
  # append what is already stored. What was pending before is taken out
  # first, and applied again with its own origin, whose update is appended:
  # it may be a client's (what of it was stored already is stored twice,
  # which changes nothing).
  defp apply_from_store(doc, updates) do
    {:ok, pending} = Yex.Doc.prune_pending(doc)
    apply_stored(doc, updates)
    apply_pending(doc, pending)
  end

  defp apply_stored(_doc, []), do: :ok

  defp apply_stored(doc, updates) do
    Yex.Doc.transaction(doc, @stored, fn ->
      for update <- updates, do: :ok = Yex.apply_update(doc, update)
      {:ok, pending} = Yex.Doc.prune_pending(doc)
      if pending, do: :ok = Yex.apply_update(doc, pending)
    end)
  end

  # Applying stored updates may integrate pending ones. yrs sends their
  # updates to this process as it applies them; they are handled here,
  # before any message already queued, so that nothing (a sync/2 reply)
  # finds the server with nothing buffered while one is unhandled. The
  # metadata is Yex.DocServer's, as its worker handles a client's updates
  # the same way.
  defp applied(%State{module: module, doc: doc} = state) do
    receive do
      {:update_v1, update, origin, Yex.DocServer.Worker} ->
        case module.handle_update_v1(doc, update, origin, state) do
          {:noreply, state} -> applied(state)
          other -> other
        end
    after
      0 -> {:noreply, state |> maybe_compact() |> synced()}
    end
  end

  defp apply_pending(_doc, nil), do: :ok

  defp apply_pending(doc, pending),
    do: Yex.Doc.transaction(doc, @pending, fn -> :ok = Yex.apply_update(doc, pending) end)

  # The document's state, with what is pending: a snapshot replaces the
  # updates before it, pending ones included.
  defp snapshot(doc) do
    {:ok, pending} = Yex.Doc.get_pending_update(doc)
    {:ok, deletes} = Yex.Doc.get_pending_ds(doc)
    state = Yex.encode_state_as_update!(doc)

    case Enum.reject([pending, deletes && <<0>> <> deletes], &is_nil/1) do
      [] -> state
      pending -> Yex.merge_updates([state | pending]) |> elem(1)
    end
  end

  defp frames_bytes(updates), do: Enum.sum_by(updates, &byte_size(Yjs.Frame.frame(&1)))

  @doc """
  Broadcasts an update to the subscribers, and buffers it for appending
  unless it was read from the store. Called by the default
  `handle_update_v1/4`.
  """
  @spec handle_update_v1(Yex.Doc.t(), binary(), term(), State.t()) :: {:noreply, State.t()}
  def handle_update_v1(_doc, update, @stored, state) do
    broadcast(state, {:slap_yjs_update, persistence(state).doc_id, update}, nil)
    {:noreply, state}
  end

  def handle_update_v1(_doc, update, origin, state) do
    broadcast(state, {:slap_yjs_update, persistence(state).doc_id, update}, origin)
    {fill, appends} = Appends.add(persistence(state).appends, Yjs.Frame.frame(update))

    state =
      case fill do
        :full -> update_persistence(state, &start_append(%{&1 | appends: appends}))
        :buffered -> update_persistence(state, &%{&1 | appends: Appends.schedule(appends)})
      end

    {:noreply, state}
  end

  @doc """
  Broadcasts an awareness change to the subscribers and, unless it came
  from another server, to the document's other servers. Called by the
  default `handle_awareness_update/4`.
  """
  @spec handle_awareness_update(Yex.Awareness.t(), map(), term(), State.t()) ::
          {:noreply, State.t()}
  def handle_awareness_update(awareness, change, origin, state) do
    %{added: added, updated: updated, removed: removed} = change

    state =
      update_persistence(state, fn p ->
        %{p | owners: Yjs.Owners.changed(p.owners, owner(origin), added ++ updated, removed)}
      end)

    case added ++ updated ++ removed do
      [] ->
        {:noreply, state}

      clients ->
        {:ok, update} = Yex.Awareness.encode_update(awareness, clients)
        broadcast(state, {:slap_yjs_awareness, persistence(state).doc_id, update}, owner(origin))
        unless match?({@stored, _}, origin), do: send_to_group(state, update)
        {:noreply, state}
    end
  end

  defp owner({@stored, pid}), do: {:server, pid}
  defp owner(pid) when is_pid(pid), do: pid
  defp owner(_origin), do: nil

  defp send_to_group(state, update) do
    case persistence(state) do
      %{group: nil} ->
        :ok

      %{doc_id: doc_id, pg: pg} ->
        for pid <- :pg.get_members(pg, {@key, doc_id}),
            pid != self(),
            do: send(pid, {@key, :awareness, self(), update})

        :ok
    end
  end

  # Subscribers other than the update's origin.
  defp broadcast(state, message, origin) do
    for {pid, _ref} <- persistence(state).subscribers, pid != origin, do: send(pid, message)
    :ok
  end

  @doc """
  Handles the server's own messages: requests, timers, what the follower
  reads, awareness from other servers, and exits, among them its tasks'.
  Called by the default `handle_info/2`; other messages are ignored.
  """
  @spec handle_info(term(), State.t()) :: {:noreply, State.t()} | {:stop, term(), State.t()}
  def handle_info({@key, :load, arg}, state) do
    case load(arg, state) do
      {:ok, state} -> {:noreply, state}
      {:stop, reason} -> {:stop, reason, state}
    end
  end

  def handle_info({@key, :request, message, pid, ref}, state),
    do: handle_request(message, {pid, ref}, state)

  def handle_info({@key, :cancel, ref}, %State{assigns: assigns} = state)
      when not is_map_key(assigns, @key) do
    waiters = Enum.reject(Map.get(assigns, @ready_waiters, []), &(elem(&1, 1) == ref))
    {:noreply, State.assign(state, @ready_waiters, waiters)}
  end

  def handle_info({@key, :cancel, ref}, state) do
    {removed, sync} = Sync.remove(persistence(state).sync, ref)
    for caller <- removed, do: Process.demonitor(caller.monitor, [:flush])
    {:noreply, update_persistence(state, &%{&1 | sync: sync})}
  end

  def handle_info({@key, :flush}, state) do
    state =
      update_persistence(state, &start_append(%{&1 | appends: Appends.timer_fired(&1.appends)}))

    {:noreply, state}
  end

  def handle_info({@key, :deleted}, state), do: {:stop, {:shutdown, :deleted}, state}

  def handle_info({@key, :idle}, state) do
    case persistence(state) do
      %{subscribers: subscribers} when map_size(subscribers) == 0 -> {:stop, :normal, state}
      _ -> {:noreply, state}
    end
  end

  def handle_info({Yjs.Follower, pid, {:updates, updates, next_offset, bytes}}, state) do
    apply_from_store(state.doc, updates)
    Yjs.Follower.ack(pid)

    state =
      update_persistence(state, fn p ->
        %{p | compaction: Compaction.read(p.compaction, next_offset, bytes)}
      end)

    applied(state)
  end

  def handle_info({Yjs.Follower, pid, {:reload, %{snapshot: snapshot, offset: offset}}}, state) do
    apply_from_store(state.doc, List.wrap(snapshot))
    Yjs.Follower.ack(pid)

    state =
      update_persistence(state, fn p ->
        %{p | compaction: Compaction.reloaded(p.compaction, offset, byte_size(snapshot || ""))}
      end)

    applied(state)
  end

  def handle_info({@key, :awareness, from, update}, %State{awareness: awareness} = state) do
    :ok = Yex.Awareness.apply_update(awareness, update, {@stored, from})
    {:noreply, state}
  end

  # The document's :pg group: send servers that join the current awareness
  # states, and remove those that came through servers that left.
  def handle_info({ref, event, {@key, _}, pids}, state) when event in [:join, :leave] do
    case persistence(state) do
      %{group: ^ref} -> {:noreply, group_changed(state, event, pids -- [self()])}
      _ -> {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case persistence(state).subscribers do
      %{^pid => ^ref} ->
        {:noreply, remove_subscriber(state, pid)}

      _ ->
        {_removed, sync} = Sync.remove(persistence(state).sync, ref)
        {:noreply, update_persistence(state, &%{&1 | sync: sync})}
    end
  end

  def handle_info({:EXIT, pid, reason}, state) do
    case Map.pop(persistence(state).tasks, pid) do
      {nil, _tasks} when reason == :normal ->
        {:noreply, state}

      {nil, _tasks} ->
        {:stop, reason, state}

      {kind, tasks} ->
        state
        |> update_persistence(&%{&1 | tasks: tasks})
        |> task_done(kind, task_result(reason))
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  # -- Tasks ------------------------------------------------------------------

  # A task is a linked process that exits with its result. The server traps
  # exits, so each task reports exactly once, with {:EXIT, pid, reason},
  # whether it returned or crashed; and it dies with the server, unless the
  # server stops normally. Killing an append does not cancel it in the
  # store: it may still commit. As with Task, `$callers` names the server.
  defp start_task(p, kind, fun) do
    callers = [self() | Process.get(:"$callers", [])]

    pid =
      spawn_link(fn ->
        Process.put(:"$callers", callers)
        exit({@key, :result, fun.()})
      end)

    %{p | tasks: Map.put(p.tasks, pid, kind)}
  end

  defp task_result({@key, :result, result}), do: result
  defp task_result(reason), do: {:error, reason}

  defp task_done(state, kind, {:error, :deleted}) when kind in [:append, :compaction],
    do: {:stop, {:shutdown, :deleted}, state}

  defp task_done(state, :append, {:ok, _offset}) do
    state = update_persistence(state, &start_append(%{&1 | appends: Appends.acked(&1.appends)}))
    {:noreply, synced(state)}
  end

  defp task_done(state, :append, {:error, reason}) when transient?(reason),
    do: {:noreply, update_persistence(state, &%{&1 | appends: Appends.retry(&1.appends)})}

  defp task_done(state, :append, {:error, reason}),
    do: {:stop, {:slap_yjs_append_failed, reason}, state}

  # After a failure, the next read starts the next compaction, not this.
  defp task_done(state, :compaction, result) do
    state = update_persistence(state, &finish_compaction(&1, result))

    case result do
      {:error, reason} when reason != :superseded -> {:noreply, synced(state)}
      _taken -> {:noreply, state |> maybe_compact() |> synced()}
    end
  end

  defp task_done(state, :probe, result), do: {:noreply, probed(state, result)}

  # -- Requests ---------------------------------------------------------------

  defp handle_request(:ready, from, %State{assigns: %{@key => _}} = state) do
    reply(from, :ok)
    {:noreply, state}
  end

  defp handle_request(:ready, from, %State{assigns: assigns} = state) do
    waiters = [from | Map.get(assigns, @ready_waiters, [])]
    {:noreply, State.assign(state, @ready_waiters, waiters)}
  end

  defp handle_request({:subscribe, pid}, from, state) do
    state =
      update_persistence(state, fn p ->
        subscribers = Map.put_new_lazy(p.subscribers, pid, fn -> Process.monitor(pid) end)
        if p.idle_timer, do: Process.cancel_timer(p.idle_timer)
        %{p | subscribers: subscribers, idle_timer: nil}
      end)

    reply(from, :ok)
    {:noreply, state}
  end

  defp handle_request({:unsubscribe, pid}, from, state) do
    state =
      case persistence(state).subscribers do
        %{^pid => ref} ->
          Process.demonitor(ref, [:flush])
          remove_subscriber(state, pid)

        _ ->
          state
      end

    reply(from, :ok)
    {:noreply, state}
  end

  defp handle_request(:sync, {pid, _ref} = from, state) do
    caller = %{from: from, monitor: Process.monitor(pid)}
    state = update_persistence(state, &start_append(%{&1 | sync: Sync.add(&1.sync, caller)}))
    {:noreply, synced(state)}
  end

  defp handle_request(:doc, from, %State{doc: doc} = state) do
    reply(from, {:ok, doc})
    {:noreply, state}
  end

  # To the alias of the caller's monitor (call/3).
  defp reply({_pid, ref}, message), do: send(ref, {ref, message})

  defp reply_sync(caller, message) do
    Process.demonitor(caller.monitor, [:flush])
    reply(caller.from, message)
  end

  defp remove_subscriber(state, pid) do
    state
    |> update_persistence(&%{&1 | subscribers: Map.delete(&1.subscribers, pid)})
    |> forget_owner(pid)
    |> idle()
  end

  defp group_changed(state, _event, []), do: state

  defp group_changed(%State{awareness: awareness} = state, :join, pids) do
    # The joiner attributes what it receives to this server, and removes it
    # when this server leaves: only the states not learned from another.
    ids = Yex.Awareness.get_client_ids(awareness)

    with [_ | _] = clients <- Yjs.Owners.local(persistence(state).owners, ids),
         {:ok, update} <- Yex.Awareness.encode_update(awareness, clients) do
      for pid <- pids, do: send(pid, {@key, :awareness, self(), update})
    end

    state
  end

  defp group_changed(state, :leave, pids),
    do: Enum.reduce(pids, state, &forget_owner(&2, {:server, &1}))

  # Removes the awareness states set through `owner`.
  defp forget_owner(%State{awareness: awareness} = state, owner) do
    case Yjs.Owners.forget(persistence(state).owners, owner) do
      {[], _owners} ->
        state

      {ids, owners} ->
        Yex.Awareness.remove_states(awareness, ids)
        update_persistence(state, &%{&1 | owners: owners})
    end
  end

  defp idle(state) do
    case persistence(state) do
      %{subscribers: subscribers, opts: %{idle_timeout: timeout}, idle_timer: nil}
      when map_size(subscribers) == 0 and timeout != :infinity ->
        timer = Process.send_after(self(), {@key, :idle}, timeout)
        update_persistence(state, &%{&1 | idle_timer: timer})

      _ ->
        state
    end
  end

  # -- sync/2 -----------------------------------------------------------------

  # Answers the sync/2 callers whose tail has been read, once nothing is
  # buffered or running; probes the tail for the others.
  defp synced(state) do
    case persistence(state) do
      %{compaction: %{running: nil}} = p ->
        if Appends.idle?(p.appends), do: settle(state, p), else: state

      _ ->
        state
    end
  end

  defp settle(state, p) do
    {done, probe, sync} = Sync.settled(p.sync, p.compaction.read_offset)

    for caller <- done, do: reply_sync(caller, :ok)
    update_persistence(state, &start_probe(%{&1 | sync: sync}, probe))
  end

  defp start_probe(p, :none), do: p

  defp start_probe(%{doc_id: doc_id, store: store} = p, :probe),
    do: start_task(p, :probe, fn -> Yjs.Store.tail(doc_id, store) end)

  defp probed(state, result) do
    {failed, sync} = Sync.probed(persistence(state).sync, result)
    for caller <- failed, do: reply_sync(caller, result)
    state = update_persistence(state, &%{&1 | sync: sync})

    case result do
      {:ok, _tail} -> synced(state)
      {:error, _} -> state
    end
  end

  # -- Stopping ---------------------------------------------------------------

  @doc """
  Stops the follower; then, unless an append failed or the document was
  deleted, appends what is buffered, waits for the appends and a running
  compaction, and compacts if anything was read since the last snapshot.
  Called by the default `terminate/2`.
  """
  @spec terminate(term(), State.t()) :: :ok
  def terminate(_reason, %State{assigns: assigns}) when not is_map_key(assigns, @key), do: :ok

  def terminate(reason, state) do
    # The link stops the follower when the server exits with any reason
    # but :normal (an idle stop).
    Process.exit(persistence(state).follower, :shutdown)
    store_on_stop(reason, state)
  end

  defp store_on_stop({:slap_yjs_append_failed, _}, _state), do: :ok
  defp store_on_stop({:shutdown, :deleted}, _state), do: :ok

  defp store_on_stop(_reason, state) do
    p = persistence(state)
    deadline = System.monotonic_time(:millisecond) + p.opts.terminate_timeout

    {append_status, p} =
      with {:ok, p} <- await_append(p, deadline) do
        p |> start_append() |> await_append(deadline)
      end

    {compaction_status, p} = await_compaction(p, deadline)

    if append_status == :ok and compaction_status == :ok and p.opts.compact_on_stop and
         Compaction.behind?(p.compaction, p.appends.acked) do
      compact_now(state.doc, p)
    end

    :ok
  end

  # What a failed append held is lost here, but not to its clients: they
  # still have it, and send it again when they resynchronise.
  defp await_append(p, deadline) do
    case await_task(p, :append, deadline) do
      :none ->
        {:ok, p}

      {{:ok, _offset}, p} ->
        {:ok, %{p | appends: Appends.acked(p.appends)}}

      {:timeout, p} ->
        Logger.error("Yjs append did not finish before stop")
        {:error, p}

      {{:error, reason}, p} ->
        Logger.error("Yjs append failed on stop: #{inspect(reason)}")
        {:error, p}
    end
  end

  defp await_compaction(p, deadline) do
    case await_task(p, :compaction, deadline) do
      :none ->
        {:ok, p}

      {:timeout, p} ->
        {:error, p}

      {result, p} ->
        {:ok, finish_compaction(p, result)}
    end
  end

  defp await_task(p, kind, deadline) do
    case Enum.find(p.tasks, &match?({_pid, ^kind}, &1)) do
      nil ->
        :none

      {pid, _kind} ->
        p = %{p | tasks: Map.delete(p.tasks, pid)}

        receive do
          {:EXIT, ^pid, reason} -> {task_result(reason), p}
        after
          remaining(deadline) -> {:timeout, p}
        end
    end
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp compact_now(doc, p) do
    snapshot = snapshot(doc)
    offset = p.compaction.read_offset

    case Yjs.Store.snapshot_internal(
           p.doc_id,
           offset,
           snapshot,
           Keyword.merge(p.opts.compaction, p.store)
         ) do
      :ok -> compaction_event(p.doc_id, offset, byte_size(snapshot))
      {:error, :superseded} -> :ok
      {:error, reason} -> Logger.error("Yjs compaction on stop failed: #{inspect(reason)}")
    end
  end

  # -- Appends ----------------------------------------------------------------

  # Appends the buffer, unless it is empty or an append is running: the
  # next one, once that is acknowledged, takes everything buffered meanwhile.
  defp start_append(p) do
    case Appends.take(p.appends) do
      :none ->
        p

      {:append, body, count, appends} ->
        doc_id = p.doc_id

        :telemetry.execute(
          [:slap, :yjs, :doc_server, :append],
          %{bytes: byte_size(body), updates: count},
          %{doc_id: doc_id}
        )

        store = p.store

        start_task(%{p | appends: appends}, :append, fn ->
          Yjs.Store.append(doc_id, body, store)
        end)
    end
  end

  # -- Compaction -------------------------------------------------------------

  defp maybe_compact(%State{doc: doc} = state) do
    update_persistence(state, fn p ->
      if Compaction.due?(p.compaction), do: start_compaction(p, snapshot(doc)), else: p
    end)
  end

  defp start_compaction(p, snapshot) do
    %{doc_id: doc_id, store: store, opts: %{compaction: opts}, compaction: %{read_offset: offset}} =
      p

    compaction = Compaction.started(p.compaction, byte_size(snapshot), p.appends.acked)

    start_task(%{p | compaction: compaction}, :compaction, fn ->
      Yjs.Store.snapshot_internal(doc_id, offset, snapshot, Keyword.merge(opts, store))
    end)
  end

  defp finish_compaction(%{compaction: %{running: r}} = p, result) do
    case result do
      :ok -> compaction_event(p.doc_id, r.offset, r.bytes)
      {:error, :superseded} -> :ok
      error -> Logger.warning("Yjs compaction failed: #{inspect(error)}")
    end

    %{p | compaction: Compaction.finished(p.compaction, result)}
  end

  defp compaction_event(doc_id, offset, bytes) do
    :telemetry.execute(
      [:slap, :yjs, :doc_server, :compact],
      %{bytes: bytes},
      %{doc_id: doc_id, offset: offset}
    )
  end

  defp persistence(%State{assigns: assigns}), do: Map.fetch!(assigns, @key)

  defp update_persistence(state, fun), do: State.assign(state, @key, fun.(persistence(state)))
end
