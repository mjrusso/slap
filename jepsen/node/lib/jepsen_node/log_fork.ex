defmodule JepsenNode.LogFork do
  @moduledoc false

  alias JepsenNode.{DocServer, Log}
  alias Slap.{SnapshotLog, Streams, Yjs}
  alias Slap.SnapshotLog.{Control, Recovery, Store}

  @timeout 20_000

  def call(conn, mode) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)

    {status, reply} =
      response(
        JepsenNode.ForkRouter.within(System.system_time(:millisecond) + 25_000, fn ->
          operation(mode, JSON.decode!(body))
        end)
      )

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, JSON.encode!(reply))
  end

  defp response({:ok, reply}), do: {200, reply}

  defp response({:error, {:capture_failed, reason, concurrent}}) do
    {status, error} = response({:error, reason})
    {status, put_in(error, [:error, :concurrent], concurrent)}
  end

  defp response({:error, {:audit_failed, diagnostic}}),
    do: {500, %{error: %{code: "audit_failed", message: diagnostic}}}

  defp response({:error, reason}) do
    status = if reason in [:timeout, :unavailable, :overloaded], do: 503, else: 409
    code = if is_atom(reason), do: Atom.to_string(reason), else: "request_failed"
    {status, %{error: %{code: code}}}
  end

  def operation(mode, %{"action" => "plan", "id" => id}) do
    keys =
      Enum.reduce(0..2, [], fn n, keys ->
        key =
          Enum.find_value(0..1000, fn suffix ->
            key = "branch-#{id}-#{n}-#{suffix}"
            if keys == [] or shard(mode, key) != shard(mode, hd(keys)), do: key
          end)

        [key | keys]
      end)
      |> Enum.reverse()

    {:ok, %{keys: keys, shards: Enum.map(keys, &shard(mode, &1))}}
  end

  def operation(mode, %{"action" => "initialize", "key" => key}) do
    case SnapshotLog.next(base(mode, key), nil, timeout: @timeout) do
      {:reset, _} -> {:ok, %{offset: 0}}
      error -> error
    end
  end

  def operation(:log, %{"action" => "append", "key" => key, "value" => value, "id" => id}) do
    appended(
      SnapshotLog.append(base(:log, key), JSON.encode!(value),
        producer: {id, 0, 0},
        timeout: @timeout
      )
    )
  end

  def operation(:yjs, %{"action" => action, "key" => key, "value" => value, "id" => id} = request)
      when action in ["append", "remove"] do
    state = client_document(request)
    vector = Yex.encode_state_vector!(state)
    edit(Yex.Doc.get_array(state, "set"), action, value)
    update = Yex.encode_state_as_update!(state, vector)

    case appended(
           SnapshotLog.append(base(:yjs, key), Yjs.Frame.frame(update),
             producer: {id, 0, 0},
             timeout: @timeout
           )
         ) do
      {:ok, reply} ->
        {:ok, Map.put(reply, :state, Base.encode64(Yex.encode_state_as_update!(state)))}

      error ->
        error
    end
  end

  def operation(mode, %{"action" => "compact", "key" => key} = request) do
    with {:ok, representation} <- publication(mode, key, request),
         :ok <-
           SnapshotLog.snapshot(base(mode, key), representation.offset, representation.snapshot,
             timeout: @timeout
           ) do
      for node <- [node() | Node.list()],
          do: :erpc.cast(node, __MODULE__, :start_publisher, [mode, key])

      reply = %{offset: representation.offset}

      reply =
        if mode == :yjs,
          do: Map.put(reply, :state, Base.encode64(representation.snapshot)),
          else: reply

      {:ok, reply}
    else
      {:error, :offset_beyond_tail} -> {:error, :unavailable}
      error -> error
    end
  end

  def operation(mode, %{"action" => "fork", "source" => source, "key" => target} = request) do
    if request["concurrent"] do
      deadline = Control.deadline(@timeout)
      task = Task.async(fn -> operation(mode, request["concurrent"]) end)
      result = fork(mode, source, target)

      case Task.yield(task, Control.remaining(deadline)) || Task.shutdown(task, :brutal_kill) do
        {:ok, {:ok, concurrent}} -> concurrent_result(result, concurrent)
        _unknown_append -> {:error, :unavailable}
      end
    else
      fork(mode, source, target)
    end
  end

  def operation(mode, %{"action" => "delete", "key" => key}) do
    case SnapshotLog.delete(base(mode, key), timeout: @timeout) do
      :ok -> {:ok, %{deleted: true}}
      error -> error
    end
  end

  def operation(mode, %{"action" => "read", "key" => key}) do
    with {:ok, representation} <- representation(mode, key),
         do: {:ok, Map.take(representation, [:values, :offset])}
  end

  def operation(:yjs, %{"action" => "join", "key" => key}) do
    with {:ok, server} <-
           Yjs.Docs.join(DocServer, doc(key), compact_on_stop: false, timeout: @timeout),
         :ok <- Yjs.DocServer.sync(server, @timeout) do
      state = Yex.Doc.new()
      :ok = Yex.apply_update(state, GenServer.call(server, :state, @timeout))
      {:ok, %{values: values(state)}}
    end
  end

  def operation(:log, %{"action" => "recover", "key" => key}) do
    with :ok <- Log.restore_followers([key]) do
      case Log.check([key]) do
        %{followers: 1, missing: [], lagging: [], mismatches: [], read_errors: []} ->
          operation(:log, %{"action" => "read", "key" => key})

        %{mismatches: [_ | _] = mismatches} ->
          {:error, {:audit_failed, inspect(mismatches)}}

        _unavailable_or_lagging ->
          {:error, :unavailable}
      end
    end
  end

  def operation(:yjs, %{"action" => "recover", "key" => key}) do
    with {:ok, reply} <- operation(:yjs, %{"action" => "join", "key" => key}),
         {:ok, offset} <- Yjs.Store.tail(doc(key), timeout: @timeout),
         do: {:ok, Map.put(reply, :offset, offset)}
  end

  def operation(mode, %{"action" => "audit", "keys" => keys} = request) do
    deadline = Control.deadline(min(request["budget_ms"] || 25_000, 25_000))

    with :ok <- Store.each_ok(request["rejected"] || [], &check_rejected(mode, &1, deadline)),
         :ok <- delete_logs(mode, keys, deadline),
         :ok <- Recovery.sweep(Control.remaining(deadline)),
         {:ok, controls} <- terminal_controls(mode, keys, deadline) do
      JepsenNode.ForkRouter.audit_excluding(controls, Control.remaining(deadline))
    end
  end

  defp terminal_controls(mode, keys, deadline) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, identities} ->
      case Control.read(Store.new(base(mode, key), []), deadline) do
        {:ok, %{record: %{"status" => "cancelled"} = record, identity: identity}} ->
          if record["cleanup"] == true,
            do: {:halt, {:error, :unavailable}},
            else: {:cont, {:ok, [identity | identities]}}

        {:ok, _pending_or_absent} ->
          {:halt, {:error, :unavailable}}

        error ->
          {:halt, error}
      end
    end)
  end

  defp fork(:yjs, source, target), do: Yjs.Store.fork(doc(source), doc(target), timeout: @timeout)

  defp fork(:log, source, target),
    do: SnapshotLog.fork(base(:log, source), base(:log, target), timeout: @timeout)

  defp concurrent_result({:ok, capture}, concurrent),
    do: {:ok, Map.put(capture, :concurrent, concurrent)}

  defp concurrent_result({:error, reason}, concurrent),
    do: {:error, {:capture_failed, reason, concurrent}}

  def start_publisher(mode, key),
    do: Task.Supervisor.start_child(Log.Checks, fn -> publish(mode, key) end)

  defp publish(mode, key) do
    if mode == :log, do: Log.read(key)

    for _ <- 1..3 do
      with {:ok, representation} <- representation(mode, key),
           do:
             SnapshotLog.snapshot(base(mode, key), representation.offset, representation.snapshot,
               timeout: @timeout
             )
    end

    :ok
  end

  defp publication(:log, key, _request), do: representation(:log, key)

  defp publication(:yjs, _key, request) do
    state = client_document(request)
    array = Yex.Doc.get_array(state, "set")
    extra = request["extra"]

    if extra != nil and not Enum.any?(Yex.Array.to_list(array), &(&1 == extra)),
      do: Yex.Array.insert(array, Yex.Array.length(array), extra)

    {:ok, %{offset: request["at"], snapshot: Yex.encode_state_as_update!(state)}}
  end

  defp client_document(request) do
    state = Yex.Doc.with_options(%Yex.Doc.Options{client_id: request["client"] || 1})
    if request["state"], do: :ok = Yex.apply_update(state, Base.decode64!(request["state"]))
    state
  end

  defp check_rejected(mode, %{"key" => key}, deadline) do
    case Control.read(Store.new(base(mode, key), []), deadline) do
      {:ok, %{record: %{"status" => "cancelled"}}} -> :ok
      {:ok, _published_or_pending} -> {:error, :unexpected_publication}
      error -> error
    end
  end

  defp delete_logs(mode, keys, deadline) do
    keys
    |> Task.async_stream(&delete_log(mode, &1, deadline), max_concurrency: 8, timeout: :infinity)
    |> Enum.reduce_while(:ok, fn
      {:ok, :ok}, :ok -> {:cont, :ok}
      {:ok, error}, :ok -> {:halt, error}
    end)
  end

  defp delete_log(mode, key, deadline) do
    path = base(mode, key)
    store = Store.new(path, [])

    with {:ok, control} <- Control.read(store, deadline),
         :ok <- delete_once(store, control, deadline),
         {:ok, terminal} <- Control.read(store, deadline),
         do: retire_ordinary_tombstones(store, terminal, deadline)
  end

  # Ordinary logs retain closed updates and a deletion index for legacy readers.
  # Fork components must be reclaimed by production cleanup before the audit.
  defp retire_ordinary_tombstones(store, %{record: %{"kind" => "ordinary"}}, deadline) do
    with :ok <- remove_stream(Store.path(store, :updates), deadline),
         do: remove_stream(Store.path(store, :index), deadline)
  end

  defp retire_ordinary_tombstones(_store, _control, _deadline), do: :ok

  defp delete_once(store, %{record: %{"status" => "cancelled", "deleted" => true}}, deadline),
    do: Streams.seal(store.base, Control.opts(store, deadline))

  defp delete_once(store, _control, deadline),
    do: SnapshotLog.delete(store.base, Control.opts(store, deadline))

  defp remove_stream(path, deadline) do
    case Streams.delete(path, timeout: Control.remaining(deadline)) do
      result when result in [:ok, {:error, :not_found}, {:error, :gone}] -> :ok
      error -> error
    end
  end

  defp representation(:log, key) do
    with {:reset, %{snapshot: snapshot, offset: at}} <-
           SnapshotLog.next(base(:log, key), nil, timeout: @timeout),
         do: read_entries(base(:log, key), at, if(snapshot, do: JSON.decode!(snapshot), else: []))
  end

  defp representation(:yjs, key) do
    with {:ok, loaded} <- Yjs.Store.load(doc(key), timeout: @timeout) do
      state = document(loaded)

      {:ok,
       %{
         values: values(state),
         offset: loaded.offset,
         snapshot: Yex.encode_state_as_update!(state)
       }}
    end
  end

  defp read_entries(base, at, values) do
    case SnapshotLog.next(base, at, wait: 0, max_bytes: 4096, timeout: @timeout) do
      {:ok, page} ->
        values = values ++ Enum.map(page.entries, &JSON.decode!/1)

        if page.up_to_date,
          do: {:ok, %{values: values, offset: page.offset, snapshot: JSON.encode!(values)}},
          else: read_entries(base, page.offset, values)

      {:reset, %{snapshot: snapshot, offset: offset}} ->
        read_entries(base, offset, if(snapshot, do: JSON.decode!(snapshot), else: []))

      error ->
        error
    end
  end

  defp values(state) do
    state |> Yex.Doc.get_array("set") |> Yex.Array.to_list() |> Enum.map(&integer_value/1)
  end

  defp integer_value(value) when is_float(value) and trunc(value) == value, do: trunc(value)
  defp integer_value(value), do: value

  defp document(loaded) do
    state = Yex.Doc.new()

    for update <- List.wrap(loaded.snapshot) ++ loaded.updates,
        do: :ok = Yex.apply_update(state, update)

    state
  end

  defp edit(array, "append", value), do: Yex.Array.insert(array, Yex.Array.length(array), value)

  defp edit(array, "remove", value) do
    case Enum.find_index(Yex.Array.to_list(array), &(&1 == value)) do
      nil -> :ok
      index -> Yex.Array.delete(array, index)
    end
  end

  defp appended({:ok, offset}), do: {:ok, %{offset: offset}}
  defp appended({:duplicate, offset}), do: {:ok, %{offset: offset}}
  defp appended(error), do: error
  defp doc(key), do: {"jepsen-branch", key}
  defp base(:log, key), do: Log.base(key)
  defp base(:yjs, key), do: Yjs.Store.base(doc(key))
  defp shard(mode, key), do: Streams.Cluster.shard_for(base(mode, key))
end
