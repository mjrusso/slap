defmodule Slap.Streams.Reader do
  @moduledoc false

  require Logger

  alias Slap.SlateDB
  alias Slap.Streams.Store.{Keys, Read}
  alias Slap.Streams.{Stream, StreamServer}

  @doc """
  Reads `path` from `offset` (or `:start`, `:now`). `request` is
  `:read_info`, or `:peek_info` for an internal read that must not reset a
  sliding TTL (copying a fork's source).
  """
  def read(ctx, path, offset, max_bytes, timeout, request \\ :read_info) do
    with {:ok, view} <- StreamServer.call(ctx, path, request, timeout) do
      read_view(ctx, view, offset, max_bytes)
    end
  end

  defp read_view(ctx, %Stream.View{status: :active} = view, offset, max_bytes) do
    %Stream.View{next_offset: tail} = view

    offset =
      case offset do
        :now -> tail
        # The earliest data still there (PROTOCOL.md §5.6: -1 is the start).
        :start -> view.trim
        offset -> offset
      end

    cond do
      offset > tail ->
        {:error, :offset_beyond_tail}

      # Before the trim point the data is gone (PROTOCOL.md §5.6).
      offset < view.trim ->
        {:error, :trimmed}

      true ->
        read_msgs(ctx, view, offset, tail, max_bytes)
    end
  end

  defp read_view(_ctx, %Stream.View{status: status}, _offset, _max), do: Stream.error(status)

  defp read_msgs(ctx, view, offset, tail, max_bytes) do
    %Stream.View{sid: sid, meta: meta} = view

    with {:ok, {messages, next}} <- safe_read(ctx, sid, offset, tail, max_bytes),
         :ok <- check_rows_kept(ctx, sid, offset) do
      up_to_date = next == tail

      {:ok,
       %{
         sid: sid,
         # The offset read from (`:start` and `:now` resolved).
         from: offset,
         messages: messages,
         next_offset: next,
         up_to_date: up_to_date,
         # Closed is reported only with the final data (PROTOCOL.md §5.6).
         closed: meta.closed and up_to_date,
         content_type: meta.content_type
       }}
    end
  end

  @doc """
  The paths of the streams in placement group `key` that start with
  `prefix` (`Slap.Streams.list/2`), from durable metadata only, as a reply
  is.
  """
  def list(ctx, prefix, key) do
    paths =
      for {path, meta} <- Read.list_meta(ctx.db, prefix, durability: :remote),
          not meta.soft_deleted,
          Slap.Streams.placement_key(path) == key,
          do: path

    {:ok, paths}
  rescue
    error in SlateDB.Error ->
      Logger.debug("list of #{prefix} failed: #{Exception.message(error)}")
      {:error, :unavailable}
  end

  # The view this read used may predate a trim or delete that became durable
  # since, and whose rows the deleter may have removed during the scan:
  # answer as the next read will (410) rather than with messages missing.
  # The markers are read durable only (one that is not durable yet may be
  # lost, and the deleter waits for it); the tail row is removed last, once
  # a deleted stream's rows are gone.
  defp check_rows_kept(ctx, sid, offset) do
    remote = [durability: :remote]

    with {:ok, marker} <- SlateDB.get(ctx.db, Keys.delete_pending(sid), remote),
         {:ok, trim} <- SlateDB.get(ctx.db, Keys.trim(sid), remote),
         {:ok, tail} <- SlateDB.get(ctx.db, Keys.tail(sid)) do
      cond do
        marker != nil or tail == nil -> {:error, :gone}
        match?(<<t::64>> when offset < t, trim) -> {:error, :trimmed}
        true -> :ok
      end
    else
      {:error, _} -> {:error, :unavailable}
    end
  end

  # The scan raises if the database fails or closes under it (the shard is
  # stopping: it moved, or was fenced): the read is retryable elsewhere.
  defp safe_read(ctx, sid, offset, tail, max_bytes) do
    {:ok, Read.read_msgs(ctx.db, sid, offset, tail, max_bytes)}
  rescue
    error in SlateDB.Error ->
      Logger.debug("read of stream #{sid} failed: #{Exception.message(error)}")
      {:error, :unavailable}
  end
end
