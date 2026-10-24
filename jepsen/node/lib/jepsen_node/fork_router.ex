defmodule JepsenNode.ForkRouter do
  @moduledoc "Fork and copy operations and cleanup audits for Jepsen."

  use Plug.Router

  alias Slap.{SlateDB, Streams}
  alias Slap.Streams.Group
  alias Slap.Streams.Jobs.{Deleter, Repair}
  alias Slap.Streams.Store.{Meta, Read}

  plug :match
  plug :dispatch

  post "/operation" do
    {:ok, body, conn} = read_body(conn)
    request = JSON.decode!(body)

    case operation(request) do
      {:ok, result} ->
        json(conn, 200, result)

      {:error, reason} when reason in [:timeout, :unavailable] ->
        json(conn, 503, %{error: %{code: error_code(reason)}})

      {:error, {:audit_failed, diagnostic}} ->
        json(conn, 500, %{error: %{code: "audit_failed", message: diagnostic}})

      {:error, reason} ->
        json(conn, 409, %{error: %{code: error_code(reason)}})
    end
  end

  match _ do
    send_resp(conn, 404, "")
  end

  def operation(%{"action" => "plan", "id" => id}) do
    paths =
      Enum.reduce(0..5, [], fn i, paths ->
        previous = List.first(paths)

        path =
          Enum.find_value(0..1000, fn n ->
            candidate = "/fork/#{id}/#{i}-#{n}"
            if previous == nil or shard(candidate) != shard(previous), do: candidate
          end)

        [path | paths]
      end)
      |> Enum.reverse()

    {:ok,
     %{paths: paths, shards: Enum.map(paths, &shard/1), now_ms: System.system_time(:millisecond)}}
  end

  def operation(%{"action" => "create", "path" => path} = req) do
    options = [content_type: "application/json", body: JSON.encode!([req["value"]])]
    options = if req["ttl_s"], do: Keyword.put(options, :ttl_s, req["ttl_s"]), else: options

    options =
      if req["expires_at_ms"],
        do: Keyword.put(options, :expires_at_ms, req["expires_at_ms"]),
        else: options

    created(path, Streams.create(path, options))
  end

  def operation(%{"action" => "fork", "source" => source, "path" => path} = req) do
    created(
      path,
      Streams.create(path,
        forked_from: source,
        fork_offset: req["at"],
        content_type: "application/json",
        expires_at_ms: req["expires_at_ms"]
      )
    )
  end

  def operation(%{"action" => "copy", "source" => source, "path" => path} = req),
    do: created(path, Streams.copy(source, path, at: req["at"] || :latest))

  def operation(%{"action" => "append", "path" => path, "value" => value, "id" => id}) do
    case Streams.append(path, JSON.encode!([value]), producer: {id, 0, 0}) do
      {:ok, result} -> {:ok, Map.drop(result, [:producer])}
      error -> error
    end
  end

  def operation(%{"action" => "trim", "path" => path, "at" => at}),
    do: result(Streams.trim(path, at))

  def operation(%{"action" => "delete", "path" => path}) do
    case Streams.delete(path) do
      error when error in [{:error, :gone}, {:error, :not_found}] -> {:ok, %{deleted: true}}
      other -> result(other)
    end
  end

  def operation(%{"action" => "read", "path" => path} = req) do
    case Streams.read(path, req["at"] || :start, max_bytes: req["max_bytes"] || 32) do
      {:ok, page} ->
        {:ok,
         %{
           messages: Enum.map(page.messages, fn {at, bytes} -> [at, JSON.decode!(bytes)] end),
           next_offset: page.next_offset,
           up_to_date: page.up_to_date,
           closed: page.closed
         }}

      error ->
        error
    end
  end

  def operation(%{"action" => "await-expiry", "path" => path}) do
    case Streams.head(path) do
      error when error in [{:error, :gone}, {:error, :not_found}] ->
        {:ok, %{expired: true, observed_at_ms: System.system_time(:millisecond)}}

      _ ->
        {:error, :unavailable}
    end
  end

  def operation(%{"action" => "audit", "paths" => paths} = req) do
    with :ok <- cleanup(paths, nil, budget(req)), do: {:ok, %{rows: 0}}
  end

  def operation(%{"action" => "await-reclaimed", "sources" => [_ | _] = sources} = req) do
    with :ok <- cleanup([], sources, budget(req)), do: {:ok, %{reclaimed: sources}}
  end

  defp budget(req), do: min(Map.get(req, "budget_ms", 25_000), 25_000)

  defp cleanup(paths, sources, budget) do
    # Jepsen nodes share the host clock, so the same deadline applies across RPCs.
    deadline = System.system_time(:millisecond) + budget

    within(deadline, fn ->
      with {:ok, _} <- parallel(paths, &delete_path(&1, deadline)),
           do: settle(sources, deadline)
    end)
  end

  defp delete_path(path, deadline) do
    case Streams.delete(path, timeout: remaining(deadline)) do
      result when result in [:ok, {:error, :gone}, {:error, :not_found}] -> {:ok, 0}
      error -> error
    end
  end

  defp settle(sources, deadline) do
    with true <- remaining(deadline) > 0,
         [_ | _] = work <- Enum.to_list(shards(sources)),
         {:ok, rows} <- parallel(work, &drain_shard(&1, deadline)) do
      if rows == 0, do: :ok, else: settle(sources, deadline)
    else
      false -> {:error, :timeout}
      [] -> {:error, :unavailable}
      error -> error
    end
  end

  defp shards(nil), do: Enum.map(Streams.Cluster.assignments(), fn {n, _} -> {n, nil} end)
  defp shards(sources), do: Enum.group_by(sources, & &1["shard"])

  defp drain_shard({shard, sources}, deadline) do
    case Slap.Cluster.call(Streams.Cluster, shard, {__MODULE__, :drain, [sources, deadline]},
           timeout: remaining(deadline)
         ) do
      {:ok, result} -> result
      {:error, {:erpc, :timeout}} -> {:error, :timeout}
      _ -> {:error, :unavailable}
    end
  end

  defp parallel(items, fun) do
    items
    |> Task.async_stream(&guarded(fn -> fun.(&1) end),
      max_concurrency: 8,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.reduce({:ok, 0}, fn
      {:ok, {:error, {:audit_failed, _}} = error}, _acc -> error
      {:ok, {:ok, count}}, {:ok, total} -> {:ok, total + count}
      {:ok, {:error, _} = error}, {:ok, _total} -> error
      _result, {:error, _} = error -> error
    end)
  end

  def drain(ctx, sources, deadline) do
    within(deadline, fn ->
      with :ok <- SlateDB.flush(ctx.db),
           count when is_integer(count) <- Repair.sweep(ctx, remaining(deadline)),
           :ok <- Deleter.drain(ctx, remaining(deadline)),
           :ok <- SlateDB.flush(ctx.db) do
        observe_rows(ctx, sources)
      end
    end)
  end

  defp observe_rows(ctx, sources) do
    case count_rows(ctx, sources) do
      0 ->
        # An empty flush cannot fence a former owner. The workload reserves
        # this group for fencing; both audits exclude its seal.
        with :ok <- Group.seal(ctx, <<0, "jepsen-cleanup-audit">>),
             do: {:ok, count_rows(ctx, sources)}

      rows ->
        {:ok, rows}
    end
  end

  defp count_rows(ctx, sources) do
    sids = if sources, do: MapSet.new(sources, & &1["sid"])

    ctx.db
    |> SlateDB.scan(durability: :remote)
    |> Enum.count(fn row ->
      case Read.row_owner(row) do
        {:stream, sid} -> sids == nil or MapSet.member?(sids, sid)
        :shared -> false
      end
    end)
  end

  defp within(deadline, fun) do
    task = Task.async(fn -> guarded(fun) end)

    case Task.yield(task, remaining(deadline)) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, :timeout}
    end
  end

  defp guarded(fun) do
    case fun.() do
      {:error, %SlateDB.Error{} = error} -> storage_error(error, [])
      result -> result
    end
  rescue
    error in SlateDB.Error -> storage_error(error, __STACKTRACE__)
    error -> audit_error(:error, error, __STACKTRACE__)
  catch
    :exit, {:timeout, {GenServer, :call, _}} ->
      {:error, :timeout}

    :exit, {reason, {GenServer, :call, _}}
    when reason in [:noproc, :normal, :shutdown, :killed] ->
      {:error, :unavailable}

    :exit, {{:shutdown, _}, {GenServer, :call, _}} ->
      {:error, :unavailable}

    kind, reason ->
      audit_error(kind, reason, __STACKTRACE__)
  end

  defp storage_error(%SlateDB.Error{kind: :timeout}, _stack), do: {:error, :timeout}

  defp storage_error(%SlateDB.Error{kind: kind}, _stack) when kind in [:closed, :unavailable],
    do: {:error, :unavailable}

  defp storage_error(error, stack), do: audit_error(:error, error, stack)

  defp audit_error(kind, reason, stack),
    do: {:error, {:audit_failed, Exception.format(kind, reason, stack)}}

  defp remaining(deadline), do: max(deadline - System.system_time(:millisecond), 0)

  def capture_info(ctx, path) do
    with {:ok, meta} <- Read.get_meta(ctx.db, path, durability: :remote) do
      {:ok, tail} = Read.get_tail(ctx.db, meta.sid)
      provenance = Meta.reference(meta) || meta.creation

      %{
        deadline_ms: Slap.Streams.Stream.deadline(meta, tail.last_access_ms),
        sid: meta.sid,
        shard: ctx.n,
        depth: Meta.depth(meta),
        inherited_from: if(provenance, do: provenance.source.path),
        inherited_sid: if(provenance, do: provenance.source.sid),
        captured_offset: if(provenance, do: provenance.offset)
      }
    end
  end

  defp created(path, {:ok, outcome, info}) do
    case Slap.Cluster.call(Streams.Cluster, shard(path), {__MODULE__, :capture_info, [path]}) do
      {:ok, capture} -> {:ok, Map.merge(info, Map.put(capture, :outcome, outcome))}
      _ -> {:error, :unavailable}
    end
  end

  defp created(_path, error), do: error
  defp result(:ok), do: {:ok, %{ok: true}}
  defp result(error), do: error
  defp shard(path), do: Streams.Cluster.shard_for(Streams.placement_key(path))

  defp error_code({:bad_request, code}) when is_atom(code), do: Atom.to_string(code)
  defp error_code(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp error_code(reason) when is_tuple(reason), do: error_code(elem(reason, 0))
  defp error_code(_reason), do: "internal_error"

  defp json(conn, status, value) do
    conn |> put_resp_content_type("application/json") |> send_resp(status, JSON.encode!(value))
  end
end
