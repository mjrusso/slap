defmodule Slap.Inspect.Query do
  @moduledoc false

  alias Slap.Inspect.{Messages, Records, Remote, Request, ShardRows}

  def execute(connection, %{view: :shards, budget: budget} = request) do
    %{entry: entry, cluster: cluster} = connection
    deadline = System.monotonic_time(:millisecond) + budget
    # Leave time for the remote status call to return its own timeout result.
    timeout = Enum.max([budget - 500, div(budget, 2), 1])

    status =
      Task.async(fn -> Remote.call(entry, Slap.Cluster, :status, [cluster, timeout], budget) end)

    shards = ShardRows.assigned(connection, deadline)

    # Remote.call returns within the budget; the margin covers the task.
    statuses =
      case Task.yield(status, budget + 1000) || Task.shutdown(status, :brutal_kill) do
        {:ok, {:ok, {:ok, %{shards: shards}}}} -> shards
        _unavailable -> :unknown
      end

    with {:ok, shards} <- shards do
      shards = for shard <- shards, do: Map.put(shard, :status, status(statuses, shard.shard))

      {:ok, ShardRows.count(connection, request[:listing], shards, deadline)}
    end
  end

  # The CLI and the view validate requests; other callers pass plain maps.
  def execute(connection, %Request{} = request) do
    deadline = System.monotonic_time(:millisecond) + request.budget

    with {:ok, shard} <- Records.shard(connection, request) do
      request = %{request | shard: shard}

      result = connection |> Records.page(request, deadline) |> ended(request)
      observation(connection, shard, others(connection, request, messages(result), deadline))
    end
  end

  def execute(connection, request) do
    with {:ok, request} <- Request.new(request), do: execute(connection, request)
  end

  # An empty page with a continuation cursor may still have matches on later
  # pages of the same shard.
  defp others(connection, %{count_others: true} = request, {:ok, %{rows: []} = page}, deadline) do
    with false <- is_binary(page[:cursor]),
         {:ok, shards} <- ShardRows.assigned(connection, deadline) do
      shards
      |> Enum.reject(&(&1.shard == request.shard))
      |> Enum.reduce_while([], &other(connection, request, &1, &2, deadline))
      |> case do
        {:moved, moved} -> {:ok, Map.put(moved, :moved_from, request.shard)}
        counts -> {:ok, Map.put(page, :others, Enum.reverse(counts))}
      end
    else
      _unavailable -> {:ok, page}
    end
  end

  defp others(_connection, _request, result, _deadline), do: result

  defp other(_connection, _request, %{owner: nil} = shard, counts, _deadline),
    do: {:cont, [Map.put(shard, :rows, nil) | counts]}

  defp other(connection, request, shard, counts, deadline) do
    case ShardRows.first_page(connection, %{request | shard: shard.shard}, deadline) do
      {:ok, %{rows: [_ | _]} = moved} -> {:halt, {:moved, moved}}
      result -> {:cont, [Map.put(shard, :rows, ShardRows.rows(result)) | counts]}
    end
  end

  # A server that ignores `:before` reads from the query's start.
  defp ended({:ok, page}, request) do
    if Keyword.has_key?(request.opts, :before) and not Map.has_key?(page, :before),
      do: {:error, :before_unsupported},
      else: {:ok, page}
  end

  defp ended(error, _request), do: error

  defp status(:unknown, _shard), do: :unknown
  defp status(statuses, shard), do: get_in(statuses, [shard, :status])

  defp messages({:ok, %{tail: _} = page}) do
    with {:ok, messages} <- Messages.assemble(page),
         do: {:ok, Map.put(page, :messages, messages)}
  end

  defp messages(result), do: result

  defp observation(connection, shard, {:error, reason})
       when reason in [:unassigned, :not_owner] do
    status = Remote.call(connection.entry, Slap.Cluster, :status, [connection.cluster, 5000])
    {:error, %{reason: reason, shard: shard, status: status}}
  end

  defp observation(_connection, _shard, result), do: result
end
