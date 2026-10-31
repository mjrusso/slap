defmodule Slap.Inspect.Query do
  @moduledoc false

  alias Slap.Inspect.{Messages, Records, Remote, Request}

  def execute(connection, %{view: :shards, budget: budget}) do
    with {:ok, assignments} <-
           Remote.call(connection.entry, Slap.Cluster, :assignments, [connection.cluster], budget),
         do: {:ok, assignments |> Map.keys() |> Enum.sort()}
  end

  # The CLI and the view validate requests; other callers pass plain maps.
  def execute(connection, %Request{} = request) do
    deadline = System.monotonic_time(:millisecond) + request.budget

    with {:ok, shard} <- Records.shard(connection, request) do
      request = %{request | shard: shard}

      result = Records.page(connection, request, deadline)
      observation(connection, shard, messages(result))
    end
  end

  def execute(connection, request) do
    with {:ok, request} <- Request.new(request), do: execute(connection, request)
  end

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
