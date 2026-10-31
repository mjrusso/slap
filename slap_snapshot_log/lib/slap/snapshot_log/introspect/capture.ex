defmodule Slap.SnapshotLog.Introspect.Capture do
  @moduledoc false

  alias Slap.Cluster.Inspection.Read
  alias Slap.SnapshotLog.{Control, Introspect, Store}
  alias Slap.Streams.Introspect, as: Streams
  alias Slap.Streams.Introspect.Messages

  @input_bytes 1024 * 1024
  @entries 200_000

  def read(ctx, deadline, opts) do
    base = Keyword.fetch!(opts, :base)
    paths = Introspect.component_paths(base)

    with {:ok, control} <- control(ctx, deadline, paths.control),
         {:ok, updates} <- stream(ctx, deadline, paths.updates),
         false <- updates.closed,
         {:ok, capture} <- capture(ctx, deadline, base, updates, opts[:at_offset]),
         {:ok, checked_updates} <- stream(ctx, deadline, paths.updates),
         false <- checked_updates.closed,
         :ok <- stable(updates.sid, checked_updates.sid),
         {:ok, checked_control} <- control(ctx, deadline, paths.control),
         :ok <- stable(control, checked_control),
         :ok <- Read.check_deadline(deadline) do
      {:ok,
       Map.merge(capture, %{
         owner: updates.owner,
         control: control,
         updates: updates,
         tail: updates.tail,
         identity: {control_identity(control), updates.sid, capture.identity}
       })}
    else
      true -> {:error, :deleted}
      error -> error
    end
  end

  defp capture(ctx, deadline, base, updates, nil) do
    index_path = Store.path(base, :index)

    with {:ok, index} <- stream(ctx, deadline, index_path),
         {:ok, messages} <- component(ctx, deadline, index),
         {:ok, current} <- current(Enum.map(messages, &index_entry/1)),
         {:ok, snapshot} <- snapshot(ctx, deadline, base, current),
         {:ok, checked_index} <- stream(ctx, deadline, index_path),
         :ok <- stable(index, checked_index) do
      from = (current && current.offset) || 0

      {:ok,
       %{
         index: index,
         current: current,
         snapshot: snapshot,
         from: from,
         until: updates.tail,
         encoded_bytes: max(updates.tail - from, 0) + if(snapshot, do: snapshot.tail, else: 0),
         history: false,
         identity: {index.sid, snapshot_identity(snapshot)}
       }}
    end
  end

  # Deletion closes updates before publishing an index tombstone.
  defp capture(_ctx, _deadline, _base, updates, offset) do
    with {:ok, until} <- boundary(offset, updates.tail) do
      {:ok,
       %{
         index: nil,
         current: nil,
         snapshot: nil,
         from: 0,
         until: until,
         encoded_bytes: until,
         history: true,
         identity: :history
       }}
    end
  end

  def index_selection(%{cursor: nil, interpretations_limited: false} = page, opts) do
    with %{trim: trim, tail: tail} when is_integer(tail) <- page.offsets,
         true <-
           is_nil(opts[:cursor]) and Keyword.get(opts, :from, 0) <= trim and
             (is_nil(opts[:until]) or (is_integer(opts[:until]) and opts[:until] >= tail)),
         :ok <- Messages.readable(page.rows),
         {:ok, entries} <- interpreted(page.rows, trim) do
      current(entries)
    else
      {:error, reason} when reason in [:incomplete_message_parts, :unreadable_index] ->
        {:error, :unreadable_index}

      _ ->
        {:error, :index_incomplete}
    end
  end

  def index_selection(_page, _opts), do: {:error, :index_incomplete}

  # Entries that Store skips as malformed are skipped here too; an entry this
  # page could not interpret is not known to be malformed.
  defp interpreted(rows, trim) do
    entries =
      for %{decoded: {:ok, %{offset: offset}}, interpretation: entry} <- rows,
          offset >= trim,
          do: {offset, parsed(entry)}

    if Enum.any?(entries, &match?({_offset, :unreadable}, &1)),
      do: {:error, :unreadable_index},
      else: {:ok, entries}
  end

  defp parsed({:ok, %{deleted: true}}), do: :deleted
  defp parsed({:ok, entry}), do: {:ok, entry}
  defp parsed({:error, :malformed_index}), do: :error
  defp parsed(_entry), do: :unreadable

  defp index_entry({offset, json}), do: {offset, Store.parse_index_entry(json)}

  defp current(entries) do
    case Store.index_state(entries) do
      {current, nil} -> {:ok, current}
      {_current, _deleted} -> {:error, :deleted}
    end
  end

  defp stable(value, value), do: :ok
  defp stable(_before, _after), do: {:error, :capture_changed}

  defp control_identity(nil), do: nil
  defp control_identity(control), do: {control.sid, control.tail}
  defp snapshot_identity(nil), do: nil
  defp snapshot_identity(snapshot), do: {snapshot.sid, snapshot.offset}

  defp stream(ctx, deadline, path) do
    with {:ok, state} <- observed(Streams.state(ctx, deadline, path)),
         false <- state.meta.soft_deleted,
         tail when is_integer(tail) <- state.tail || {:error, :tail_unavailable} do
      {:ok,
       %{
         path: path,
         sid: state.meta.sid,
         tail: tail,
         trim: state.trim,
         closed: state.meta.closed,
         owner: node(),
         shard: ctx.n
       }}
    else
      true -> {:error, :deleted}
      error -> error
    end
  end

  defp observed({:ok, nil}), do: {:error, :stream_not_observed}
  defp observed(state), do: state

  defp control(ctx, deadline, path) do
    case stream(ctx, deadline, path) do
      {:ok, stream} ->
        with {:ok, messages} <- component(ctx, deadline, stream),
             do: control_state(List.last(messages), stream)

      {:error, :stream_not_observed} ->
        {:ok, nil}

      error ->
        error
    end
  end

  defp control_state({_offset, json}, stream) do
    with {:ok, control} <- Control.parse(json),
         :ok <- Control.access(control) do
      {:ok, stream}
    else
      :error -> {:error, :log_not_ready}
      # The inspection client reports `:busy` as a busy inspection slot.
      {:error, :busy} -> {:error, :log_busy}
      error -> error
    end
  end

  defp control_state(nil, _stream), do: {:error, :log_not_ready}

  # Control and index streams are small; this bounds the whole read before it starts.
  defp component(ctx, deadline, stream) do
    bytes = stream.tail - stream.trim

    with :ok <- at_most(bytes, @input_bytes),
         {:ok, messages} <-
           Streams.read_messages(ctx, deadline, stream.sid, stream.trim, stream.tail),
         :ok <- at_most(length(messages), @entries),
         do: {:ok, messages}
  end

  defp at_most(n, limit) when n > limit, do: {:error, {:too_large, n, limit}}
  defp at_most(_n, _limit), do: :ok

  defp snapshot(_ctx, _deadline, _base, nil), do: {:ok, nil}

  defp snapshot(ctx, deadline, base, current) do
    case stream(ctx, deadline, Store.path(base, {:snapshot, current.offset})) do
      {:error, reason} when reason in [:stream_not_observed, :deleted] ->
        {:error, :capture_changed}

      {:ok, stream} ->
        {:ok, Map.put(stream, :offset, current.offset)}

      error ->
        error
    end
  end

  defp boundary(offset, tail) when is_integer(offset) and offset >= 0 and offset <= tail,
    do: {:ok, offset}

  defp boundary(_offset, _tail), do: {:error, :not_an_entry_boundary}
end
