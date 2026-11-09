defmodule Slap.Inspect.ShardRows do
  @moduledoc false

  alias Slap.Inspect.{Records, Remote, Request}

  @limit 50

  @doc "Returns shard assignments with `owner: nil` for unassigned shards."
  def assigned(connection, deadline) do
    with {:ok, timeout} <- Remote.remaining(deadline),
         {:ok, assignments} <-
           Remote.call(
             connection.entry,
             Slap.Cluster,
             :assignments,
             [connection.cluster],
             timeout
           ),
         do: {:ok, shards(assignments, connection.entry)}
  end

  defp shards(assignments, entry) do
    for {shard, assignment} <- Enum.sort(assignments),
        do: %{shard: shard, owner: owner(assignment, entry)}
  end

  defp owner({:local, _pid}, entry), do: entry
  defp owner({:remote, node}, _entry), do: node
  defp owner(_unassigned, _entry), do: nil

  @doc """
  Adds each shard's listing count as `rows`. `{:exactly, n}` is a complete
  count; `{:at_least, n}` is a lower bound from the first page. `:unchecked`
  means the deadline expired, `:unreadable` a failed read, and nil an
  unassigned shard. Reads are sequential because each node serves one
  inspection request at a time.
  """
  def count(_connection, nil, shards, _deadline), do: shards

  def count(connection, request, shards, deadline) do
    opts =
      request.opts |> Keyword.drop([:cursor, :cursor_exclusive]) |> Keyword.put(:limit, @limit)

    for shard <- shards do
      request = %{request | shard: shard.shard, opts: opts}
      Map.put(shard, :rows, shard.owner && rows(first_page(connection, request, deadline)))
    end
  end

  def rows?(count), do: match?({_kind, rows} when rows > 0, count)

  @doc """
  Skips empty pages with continuation cursors. Returns the first nonempty
  or terminal page, or a read error.
  """
  def first_page(connection, request, deadline) do
    case Records.page(connection, request, deadline) do
      {:ok, %{rows: [], cursor: cursor} = page} when is_binary(cursor) ->
        first_page(connection, Request.next(request, page), deadline)

      result ->
        result
    end
  end

  @doc "Converts a page result to a row count or read status."
  def rows({:ok, %{rows: rows, cursor: cursor}}) when is_binary(cursor),
    do: {:at_least, length(rows)}

  def rows({:ok, %{rows: rows}}), do: {:exactly, length(rows)}
  def rows({:error, :deadline}), do: :unchecked
  def rows({:error, _reason}), do: :unreadable
end
