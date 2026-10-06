defmodule JepsenNode.FilesRouter do
  @moduledoc """
  The files workload over HTTP, on `Slap.Files`. Key `k` is file `"k<k>"`
  in partition `"jepsen-<k mod 16>"`, so keys spread over shards.

    * `GET /files/:key`: 200 with the body and the version as the `ETag`,
      404, or 503.
    * `PUT /files/:key` with the body, and `If-Match: "version"` or
      `If-None-Match: *`: 204 with the `ETag`; 412 when the condition
      fails; 409 when the upload expired (nothing was written); 503 when
      the outcome is unknown. `X-Jepsen-Write-Id` distinguishes separate
      writes of the same body in the file metadata.
    * `DELETE /files/:key`, with `If-Match` or not: 204, 412 or 503.
    * `POST /audit`: stops every node from taking new writes (they get
      503), waits until no node is running one, then `upload_timeout_ms`,
      so that no store write an intent covers can be applied any more (see
      `Slap.Files.Sweeper`), checks every intent partition with a linearizable
      read, and sweeps and reconciles on every node. Then
      it reports, as JSON, the files whose object is missing (`dangling`),
      the intents left (`intents`), and, of the objects that are neither a
      file's body nor named by an intent, the registered ones (`orphans`)
      and the unregistered ones (`unreconciled`), and the registrations
      that are neither (`leaked_registrations`), with `quiescent: true`.
      If a step fails, or the audit would take longer than 100 s, it
      reports only `quiescent: false` and the `reason`: its reads could
      see a write half-way.
  """

  use Plug.Router

  alias Slap.Files
  alias Slap.Files.{Config, Intent, Object, Record, Scan, Sweeper}
  alias Slap.KV
  alias Slap.SlateDB.ObjectStore
  alias JepsenNode.Stats

  @partitions 16

  # The audit's time, within its client's (120 s).
  @audit_ms 100_000

  # A gate per node: how many write handlers are running, and whether new
  # ones are refused. A handler counts itself, then reads the gate; the
  # audit closes the gate, then reads the count. Atomics are sequentially
  # consistent, so a handler that the audit does not count sees the gate
  # closed.
  @running 1
  @closed 2

  @doc "Sets up the gate; `nodes` are every node's name."
  @spec setup([node()]) :: :ok
  def setup(nodes), do: :persistent_term.put(__MODULE__, {nodes, :atomics.new(2, [])})

  @doc "How many write handlers this node is running."
  @spec running() :: integer()
  def running, do: :atomics.get(gate(), @running)

  @doc "Refuses new writes on this node (`true`), or takes them again."
  @spec close_writes(boolean()) :: :ok
  def close_writes(closed), do: :atomics.put(gate(), @closed, if(closed, do: 1, else: 0))

  defp gate, do: elem(:persistent_term.get(__MODULE__), 1)
  defp nodes, do: elem(:persistent_term.get(__MODULE__), 0)

  # Runs a write handler, counted from its start, since it runs on after
  # its client has given up on it.
  defp admitted(conn, handle) do
    :atomics.add(gate(), @running, 1)

    try do
      if :atomics.get(gate(), @closed) == 1,
        do: send_resp(conn, 503, "writes are closed"),
        else: handle.(conn)
    after
      :atomics.sub(gate(), @running, 1)
    end
  end

  plug :match
  plug :dispatch

  # The body and version come from one read of the file, so a cas compares
  # the value and sends the version of the same write.
  get "/files/:key" do
    case Files.stream(ref(key)) do
      {:ok, nil} ->
        send_resp(conn, 404, "")

      {:ok, {file, chunks}} ->
        conn |> etag(file.version) |> send_resp(200, Enum.join(chunks))

      {:error, reason} ->
        send_resp(conn, 503, inspect(reason))
    end
  end

  put "/files/:key" do
    admitted(conn, fn conn ->
      {:ok, body, conn} = read_body(conn, length: 1_000_000)

      metadata =
        case get_req_header(conn, "x-jepsen-write-id") do
          [id] -> %{"jepsen-write-id" => id}
          _ -> %{}
        end

      case Files.put(ref(key), body, [metadata: metadata] ++ condition(conn)) do
        {:ok, file} ->
          Stats.add(
            if(file.storage == :inline, do: :files_inline_writes, else: :files_object_writes)
          )

          conn |> etag(file.version) |> send_resp(204, "")

        {:error, reason} ->
          error(conn, reason)
      end
    end)
  end

  delete "/files/:key" do
    admitted(conn, fn conn ->
      case Files.delete(ref(key), condition(conn)) do
        :ok -> send_resp(conn, 204, "")
        {:error, reason} -> error(conn, reason)
      end
    end)
  end

  post "/audit" do
    deadline = System.monotonic_time(:millisecond) + @audit_ms
    report = audit(deadline)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, JSON.encode!(report))
  end

  @doc false
  def audit(deadline) do
    try do
      with :ok <- on_every_node(:close_writes, [true], deadline),
           :ok <- quiesce(deadline),
           :ok <- settle(deadline),
           :ok <- barrier_intents(deadline),
           :ok <- on_every_node(Sweeper, :sweep, [], deadline),
           :ok <- on_every_node(Sweeper, :reconcile, [], deadline),
           {:ok, report} <- observe_until(deadline),
           {:ok, stats} <- Stats.collect(nodes(), remaining(deadline)) do
        report
        |> Map.put(:stats, stats)
        |> Map.put(:quiescent, true)
      else
        {:error, reason} -> %{quiescent: false, reason: inspect(reason)}
      end
    after
      :rpc.multicall(nodes(), __MODULE__, :close_writes, [false], 5_000)
    end
  end

  match _ do
    send_resp(conn, 404, "")
  end

  # The store, once settled.
  defp observe do
    config = Config.get()
    {:ok, objects} = ObjectStore.list(config.objects, "objects/", Config.object_opts(config))
    referenced = referenced_objects()

    intents =
      Enum.flat_map(Intent.buckets(), fn b ->
        elem(Intent.reduce(b, [], &[&1 | &2], config), 1)
      end)

    registered = registered_objects()
    accounted = MapSet.new(referenced ++ Enum.flat_map(intents, & &1.keys))
    unaccounted = Enum.reject(objects, &(&1 in accounted))

    # An unregistered object was written by a store request that completed
    # after the object was deleted, perhaps after the last reconciliation:
    # the next one deletes it. A registered one is a leak.
    {orphans, unreconciled} = Enum.split_with(unaccounted, &(&1 in registered))

    %{
      objects: length(objects),
      orphans: orphans,
      unreconciled: unreconciled,
      dangling: referenced -- objects,
      leaked_registrations: Enum.reject(registered, &(&1 in accounted)),
      intents: length(intents)
    }
  end

  # Calls `module.fun(args)` on every node, which must return :ok on each
  # by `deadline`.
  defp on_every_node(module \\ __MODULE__, fun, args, deadline) do
    case :rpc.multicall(nodes(), module, fun, args, remaining(deadline)) do
      {results, []} ->
        if Enum.all?(results, &(&1 == :ok)), do: :ok, else: {:error, {fun, results}}

      {_results, unreachable} ->
        {:error, {fun, :unreachable, unreachable}}
    end
  end

  # Waits until no node is running a write handler.
  defp quiesce(deadline) do
    case :rpc.multicall(nodes(), __MODULE__, :running, [], min(5_000, remaining(deadline))) do
      {counts, []} when is_list(counts) ->
        cond do
          Enum.all?(counts, &(&1 == 0)) ->
            :ok

          remaining(deadline) > 100 ->
            Process.sleep(100)
            quiesce(deadline)

          true ->
            {:error, {:writes_running, counts}}
        end

      {_counts, unreachable} ->
        {:error, {:running, :unreachable, unreachable}}
    end
  end

  # Waits until no write that an intent covers can be applied any more:
  # every intent a write opened is due, plus the clocks' difference.
  defp settle(deadline) do
    wait = Config.get().upload_timeout_ms + Config.get().max_clock_skew_ms

    if remaining(deadline) > wait,
      do: Process.sleep(wait),
      else: {:error, :out_of_time}
  end

  defp barrier_intents(deadline) do
    config = Config.get()

    Enum.reduce_while(Intent.buckets(), :ok, fn bucket, :ok ->
      timeout = remaining(deadline)

      result =
        if timeout > 0,
          do:
            KV.get(
              Intent.partition(bucket, config),
              "__audit_barrier__",
              [consistency: :linearizable, timeout: timeout] ++ Config.route_opts(config)
            ),
          else: {:error, :out_of_time}

      case result do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:intent_barrier, bucket, reason}}}
      end
    end)
  end

  defp observe_until(deadline) do
    task =
      Task.async(fn ->
        try do
          {:ok, observe()}
        rescue
          error -> {:error, Exception.message(error)}
        catch
          kind, reason -> {:error, {kind, reason}}
        end
      end)

    case Task.yield(task, remaining(deadline)) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, :out_of_time}
      {:exit, reason} -> {:error, {:observation_failed, reason}}
    end
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp ref(key), do: {"jepsen-#{rem(String.to_integer(key), @partitions)}", "k" <> key}

  defp condition(conn) do
    case {get_req_header(conn, "if-match"), get_req_header(conn, "if-none-match")} do
      {[etag], _} -> [if_version: etag |> String.trim("\"") |> String.to_integer()]
      {_, ["*"]} -> [if_version: :absent]
      _ -> []
    end
  end

  defp etag(conn, version), do: put_resp_header(conn, "etag", ~s("#{version}"))

  defp error(conn, {:conflict, nil}), do: send_resp(conn, 412, "")
  defp error(conn, {:conflict, version}), do: conn |> etag(version) |> send_resp(412, "")
  defp error(conn, :expired), do: send_resp(conn, 409, "expired")
  defp error(conn, reason), do: send_resp(conn, 503, inspect(reason))

  # The object keys of every file in the workload's partitions.
  defp referenced_objects do
    for p <- 0..(@partitions - 1),
        {_id, _version, record} <- records("jepsen-#{p}", nil),
        key = Record.object_key(record),
        key != nil,
        do: key
  end

  # The object keys that are registered (see Slap.Files.Object).
  defp registered_objects do
    config = Config.get()

    Intent.buckets()
    |> Enum.flat_map(fn b ->
      {:ok, ids} =
        Scan.reduce(
          Object.partition(b, config),
          [],
          fn {id, _}, ids -> [id | ids] end,
          Config.route_opts(config)
        )

      Enum.map(ids, &(Config.object_prefix(config, b) <> &1))
    end)
    |> MapSet.new()
  end

  defp records(partition, cursor) do
    config = Config.get()
    opts = [limit: 1_000] ++ if(cursor, do: [cursor: cursor], else: [])

    {:ok, %{rows: rows, cursor: next}} =
      Record.list(partition, opts ++ Config.route_opts(config), config)

    if next, do: rows ++ records(partition, next), else: rows
  end
end
