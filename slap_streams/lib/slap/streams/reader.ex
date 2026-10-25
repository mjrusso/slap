defmodule Slap.Streams.Reader do
  @moduledoc false

  require Logger

  alias Slap.SlateDB
  alias Slap.Streams.{Retention, Stream, StreamServer}
  alias Slap.Streams.Store.{Keys, Meta, PublishedReference, Read}

  def read(ctx, path, offset, max_bytes, timeout) do
    deadline = deadline(timeout)

    with {:ok, view} <- StreamServer.call(ctx, path, :read_info, remaining(deadline)) do
      read_view(ctx, path, view, offset, max_bytes, deadline)
    end
  end

  def read_fenced(ctx, path, offset, max_bytes, timeout) do
    deadline = deadline(timeout)

    with {:ok, view} <- StreamServer.call(ctx, path, :fenced_view, remaining(deadline)),
         do: read_view(ctx, path, view, offset, max_bytes, deadline)
  end

  defp read_view(ctx, path, %Stream.View{status: :active} = view, offset, max_bytes, deadline) do
    %Stream.View{next_offset: tail} = view

    offset =
      case offset do
        :now -> tail
        :start -> view.trim
        offset -> offset
      end

    cond do
      offset > tail ->
        {:error, :offset_beyond_tail}

      offset < view.trim ->
        {:error, :trimmed}

      true ->
        read_msgs(ctx, path, view, offset, tail, max_bytes, deadline)
    end
  end

  defp read_view(_ctx, _path, %Stream.View{status: status}, _offset, _max, _deadline),
    do: Stream.error(status)

  defp read_msgs(ctx, path, view, offset, tail, max_bytes, deadline) do
    %Stream.View{sid: sid, meta: meta} = view
    page = range(ctx, view, offset, tail, max_bytes, deadline)

    with :ok <- check_rows_kept(ctx, path, sid, offset),
         {:ok, {messages, next}} <- page,
         :ok <- within_budget(deadline) do
      up_to_date = next == tail

      {:ok,
       %{
         sid: sid,
         from: offset,
         messages: messages,
         next_offset: next,
         up_to_date: up_to_date,
         closed: meta.closed and up_to_date,
         content_type: meta.content_type
       }}
    end
  end

  @doc false
  def read_identity(ctx, identity, view, offset, max_bytes, timeout) do
    if identity.sid == view.sid,
      do: read_view(ctx, identity.path, view, offset, max_bytes, deadline(timeout)),
      else: {:error, :unavailable}
  end

  @doc false
  def retained(ctx, cap, offset, until, max_bytes, timeout) do
    deadline = deadline(timeout)

    with true <- offset >= cap.start and until <= cap.offset,
         {:ok, meta} <- retained_meta(ctx, cap),
         {:ok, page} <-
           range(ctx, %{sid: meta.sid, meta: meta}, offset, until, max_bytes, deadline),
         {:ok, _meta} <- retained_meta(ctx, cap),
         :ok <- within_budget(deadline) do
      {:ok, page}
    else
      false -> {:error, :unavailable}
      {:error, _} = error -> error
    end
  end

  defp range(_ctx, _view, from, until, _max, _deadline) when from >= until,
    do: {:ok, {[], until}}

  defp range(
         ctx,
         %{meta: %{creation: %PublishedReference{capture: cap}}} = view,
         from,
         until,
         max_bytes,
         deadline
       )
       when from < cap.offset do
    boundary = min(until, cap.offset)

    case Slap.Streams.retained(cap, from, boundary, max_bytes, remaining(deadline), ctx.cluster) do
      {:ok, {messages, next}} ->
        next = min(next, boundary)
        bytes = Enum.reduce(messages, 0, fn {_, b}, n -> n + byte_size(b) end)
        continue_page(ctx, view, {messages, next, bytes}, boundary, until, max_bytes)

      {:error, :timeout} ->
        {:error, :unavailable}

      error ->
        error
    end
  end

  defp range(ctx, view, from, until, max_bytes, _deadline),
    do: safe_read(ctx, view.sid, from, until, max_bytes)

  defp continue_page(ctx, view, {messages, next, bytes}, boundary, until, max_bytes)
       when next == boundary and boundary < until and bytes < max_bytes do
    with {:ok, {own, own_next}} <- safe_read(ctx, view.sid, boundary, until, max_bytes - bytes),
         do: {:ok, {messages ++ own, own_next}}
  end

  defp continue_page(_ctx, _view, {messages, next, _bytes}, _boundary, _until, _max_bytes),
    do: {:ok, {messages, next}}

  defp retained_meta(ctx, %{source: %{shard: shard, sid: sid}} = cap) when shard == ctx.n do
    with {:ok, %Meta{sid: ^sid} = meta} <-
           Read.get_meta(ctx.db, cap.source.path, durability: :remote),
         true <- Retention.registered?(ctx.db, meta, cap) do
      {:ok, meta}
    else
      _ -> {:error, :unavailable}
    end
  end

  defp retained_meta(_ctx, _cap), do: {:error, :unavailable}

  defp within_budget(deadline) do
    if remaining(deadline) == 0, do: {:error, :timeout}, else: :ok
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout
  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  @doc """
  Lists paths in placement group `key` that start with `prefix`, using durable
  metadata. Excludes deleted streams and pending creations.
  """
  def list(ctx, prefix, key) do
    paths =
      for {path, meta} <- Read.list_meta(ctx.db, prefix, durability: :remote),
          not meta.soft_deleted and not Meta.pending?(meta),
          Slap.Streams.placement_key(path) == key,
          do: path

    {:ok, paths}
  rescue
    error in SlateDB.Error ->
      Logger.debug("list of #{prefix} failed: #{Exception.message(error)}")
      {:error, :unavailable}
  end

  # Read metadata last: final deletion may remove the trim row after cleanup
  # has already removed messages from this scan.
  defp check_rows_kept(ctx, path, sid, offset) do
    with {:ok, trim} <- SlateDB.get(ctx.db, Keys.trim(sid), durability: :remote),
         {:ok, %Meta{sid: ^sid, soft_deleted: false}} <-
           Read.get_meta(ctx.db, path, durability: :remote) do
      if match?(<<t::64>> when offset < t, trim), do: {:error, :trimmed}, else: :ok
    else
      {:ok, %Meta{sid: ^sid, soft_deleted: true}} -> {:error, :gone}
      {:ok, _meta} -> {:error, :not_found}
      {:error, _reason} -> {:error, :unavailable}
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
