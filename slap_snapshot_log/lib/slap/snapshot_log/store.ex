defmodule Slap.SnapshotLog.Store do
  @moduledoc false

  alias Slap.Streams
  alias Slap.Streams.Offset

  @octets "application/octet-stream"
  @json "application/json"
  @page_bytes 1024 * 1024
  @missing [:not_found, :gone]

  @enforce_keys [:base]
  defstruct [:base, :cluster, :timeout]

  @type t :: %__MODULE__{base: String.t(), cluster: module() | nil, timeout: timeout() | nil}

  def new(base, opts) do
    %__MODULE__{
      base: base,
      cluster: Keyword.get(opts, :cluster),
      timeout: Keyword.get(opts, :timeout)
    }
  end

  @type snapshot :: %{offset: non_neg_integer(), created_at: integer()}

  @type entry :: %{
          offset: non_neg_integer(),
          created_at: integer(),
          retained: [snapshot()],
          index_offset: non_neg_integer()
        }

  @type state :: %{
          current: entry() | nil,
          index_tail: non_neg_integer(),
          deleted: non_neg_integer() | nil
        }

  def path(base, :updates), do: base_path(base) <> "/.updates"
  def path(base, :index), do: base_path(base) <> "/.index"

  def path(base, {:snapshot, offset}),
    do: base_path(base) <> "/.snapshots/#{Offset.encode(offset)}_snapshot"

  defp snapshots_prefix(base), do: base_path(base) <> "/.snapshots/"

  defp base_path(%__MODULE__{base: base}), do: base
  defp base_path(base), do: base

  def stream_opts(%__MODULE__{cluster: cluster, timeout: timeout}) do
    Enum.reject([cluster: cluster, timeout: timeout], fn {_key, value} -> is_nil(value) end)
  end

  def stream_opts(_base), do: []

  # Creates `.updates` and `.index`, if they do not exist. A deleted log's
  # `.updates` is closed, which a create of an open one conflicts with.
  @spec ensure(String.t() | t(), keyword()) :: :ok | {:error, term()}
  def ensure(base, opts \\ []) do
    deadline = Keyword.get(opts, :deadline)
    opts = Keyword.delete(opts, :deadline)
    opts = Keyword.merge(stream_opts(base), opts)

    with :ok <- create(path(base, :updates), @octets, opts, deadline),
         do: create(path(base, :index), @json, opts, deadline)
  end

  defp create(path, content_type, opts, deadline) do
    with {:ok, opts} <- call_opts(opts, deadline) do
      case Streams.create(path, [content_type: content_type] ++ opts) do
        {:ok, _, _} -> :ok
        {:error, reason} when reason in [:conflict, :sealed] -> {:error, :deleted}
        {:error, _} = error -> error
      end
    end
  end

  # :ok unless the log is deleted, or being deleted.
  @spec live(String.t() | t(), integer() | :infinity | nil) :: :ok | {:error, term()}
  def live(base, deadline \\ nil) do
    with {:ok, opts} <- call_opts(stream_opts(base), deadline) do
      case Streams.head(path(base, :updates), opts) do
        {:ok, %{closed: true}} -> {:error, :deleted}
        {:ok, _info} -> :ok
        {:error, :not_found} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  @spec state(String.t() | t(), integer() | :infinity | nil) ::
          {:ok, state()} | {:error, term()}
  def state(base, deadline \\ nil) do
    case read_all(path(base, :index), stream_opts(base), deadline) do
      {:ok, messages, tail} ->
        {current, deleted} = Enum.reduce(messages, {nil, nil}, &entry/2)

        {:ok, %{current: current, index_tail: tail, deleted: deleted}}

      {:error, :not_found} ->
        {:ok, %{current: nil, index_tail: 0, deleted: nil}}

      {:error, _} = error ->
        error
    end
  end

  # The current snapshot's offset and bytes, or {nil, nil}. A publication
  # may delete the snapshot after this reads the index: then the index is
  # read again.
  @spec current(String.t() | t(), integer() | :infinity, pos_integer()) ::
          {:ok, {non_neg_integer() | nil, binary() | nil}} | {:error, term()}
  def current(base, deadline, attempts \\ 3) do
    case state(base, deadline) do
      {:ok, %{deleted: at}} when at != nil ->
        {:error, :deleted}

      {:ok, %{current: nil}} ->
        {:ok, {nil, nil}}

      {:ok, %{current: %{offset: offset}}} ->
        case read_snapshot(base, offset, deadline) do
          {:ok, bytes} ->
            {:ok, {offset, bytes}}

          {:error, reason} when reason in @missing and attempts > 1 ->
            current(base, deadline, attempts - 1)

          {:error, _} = error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  @spec tail(String.t() | t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def tail(base) do
    case Streams.head(path(base, :updates), stream_opts(base)) do
      {:ok, %{closed: true}} -> {:error, :deleted}
      {:ok, %{next_offset: offset}} -> {:ok, offset}
      {:error, _} = error -> error
    end
  end

  # Writes a snapshot of the entries before `offset`. It is not current
  # until index/3. A snapshot already at `offset` (from an attempt that
  # stopped before index/3) is kept: it too covers the entries before
  # `offset`. Offsets are never reused, as a deleted log's base is not, and
  # once it is deleted, its sealed group admits no new snapshot.
  @spec snapshot(String.t() | t(), non_neg_integer(), binary()) :: :ok | {:error, term()}
  def snapshot(base, offset, bytes) when is_binary(bytes) do
    case Streams.create(
           path(base, {:snapshot, offset}),
           [content_type: @octets, body: bytes, closed: true] ++ stream_opts(base)
         ) do
      {:ok, _, _} -> :ok
      {:error, :sealed} -> {:error, :deleted}
      {:error, _} = error -> error
    end
  end

  @spec stored(String.t() | t()) :: {:ok, [non_neg_integer()]} | {:error, term()}
  def stored(base) do
    prefix = snapshots_prefix(base)

    with {:ok, paths} <- Streams.list(prefix, stream_opts(base)) do
      {:ok, Enum.flat_map(paths, &snapshot_offset(&1, prefix))}
    end
  end

  defp snapshot_offset(path, prefix) do
    name = String.replace_prefix(path, prefix, "")

    case name |> String.replace_suffix("_snapshot", "") |> Offset.parse() do
      {:ok, offset} when is_integer(offset) -> [offset]
      _ -> []
    end
  end

  @spec index(
          String.t() | t(),
          %{offset: non_neg_integer(), created_at: integer(), retained: [snapshot()]},
          non_neg_integer()
        ) :: :ok | {:error, term()}
  def index(base, entry, index_tail) do
    entry
    |> encode_snapshot()
    |> Map.put("retained", Enum.map(entry.retained, &encode_snapshot/1))
    |> append_if(base, index_tail)
  end

  @spec index_deleted(String.t() | t(), non_neg_integer()) :: :ok | {:error, term()}
  def index_deleted(base, index_tail) do
    append_if(
      %{"deleted" => true, "createdAt" => System.system_time(:millisecond)},
      base,
      index_tail
    )
  end

  # The append carries the tail as its `Stream-Seq` (index_seq/1), which
  # Durable Streams requires to increase. Every append moves the tail past
  # the `Stream-Seq` before it, so of the writers that read the same index,
  # only the first appends; the others get {:error, :conflict}.
  defp append_if(entry, base, index_tail) do
    case Streams.append(
           path(base, :index),
           JSON.encode!(entry),
           [content_type: @json, stream_seq: index_seq(index_tail)] ++ stream_opts(base)
         ) do
      {:ok, _} -> :ok
      {:error, :stream_seq_conflict} -> {:error, :conflict}
      {:error, _} = error -> error
    end
  end

  # Indexes written before this scheme carry `Offset.encode(snapshot
  # offset)`, which starts with a digit and can exceed any tail. The "t"
  # sorts after every digit, so the first append under this scheme succeeds
  # whatever the earlier `Stream-Seq`.
  defp index_seq(index_tail),
    do: "t" <> String.pad_leading(Integer.to_string(index_tail), 20, "0")

  @spec read_snapshot(String.t() | t(), non_neg_integer(), integer() | :infinity | nil) ::
          {:ok, binary()} | {:error, term()}
  def read_snapshot(base, offset, deadline \\ nil) do
    with {:ok, messages, _} <-
           read_all(path(base, {:snapshot, offset}), stream_opts(base), deadline),
         do: {:ok, messages |> Enum.map(&elem(&1, 1)) |> IO.iodata_to_binary()}
  end

  @spec history(String.t() | t()) :: {:ok, [snapshot()]} | {:error, term()}
  def history(base) do
    with :ok <- live(base), do: history(base, state(base))
  end

  defp history(base, state) do
    case state do
      {:ok, %{deleted: at}} when at != nil ->
        {:error, :deleted}

      {:ok, %{current: nil}} ->
        {:ok, []}

      {:ok, %{current: current}} ->
        with {:ok, stored} <- stored(base) do
          retained = Enum.filter(current.retained, &(&1.offset in stored))
          {:ok, Enum.sort_by([snapshot(current) | retained], & &1.offset)}
        end

      {:error, _} = error ->
        error
    end
  end

  @spec named(entry()) :: [non_neg_integer()]
  def named(entry), do: [entry.offset | Enum.map(entry.retained, & &1.offset)]

  @spec delete_snapshot(String.t() | t(), non_neg_integer()) :: :ok | {:error, term()}
  def delete_snapshot(base, offset) do
    case Streams.delete(path(base, {:snapshot, offset}), stream_opts(base)) do
      :ok -> :ok
      {:error, reason} when reason in @missing -> :ok
      {:error, _} = error -> error
    end
  end

  @spec trim(String.t() | t(), :updates | :index, non_neg_integer()) ::
          :ok | {:error, term()}
  def trim(base, stream, offset), do: Streams.trim(path(base, stream), offset, stream_opts(base))

  # Deletes the log for good, in steps that a retry repeats:
  #
  #   1. closes `.updates`: from here the log reads as deleted (live/1),
  #      appends fail, waiting readers wake, and ensure/1 cannot create it
  #      again;
  #   2. indexes the deletion entry, if the index is unchanged since it was
  #      read (append_if/3), so that every publication that read the index
  #      before fails to index its snapshot, and finds the log deleted;
  #   3. seals the placement group, so that no snapshot can be written from
  #      here on, and every one written before is listed;
  #   4. trims `.updates` to its tail, deletes every stored snapshot, and
  #      trims `.index` before the deletion entry.
  #
  # Options: `:after_step`, a function called with :close, :index, :seal and
  # :clean after each step completes (for tests).
  @spec delete(String.t() | t(), keyword()) :: :ok | {:error, term()}
  def delete(base, opts \\ []) do
    after_step = Keyword.get(opts, :after_step, fn _step -> :ok end)

    with :ok <- step(close(base), :close, after_step),
         {:ok, deleted_at} <- index_deletion(base),
         after_step.(:index),
         :ok <- step(Streams.seal(base_path(base), stream_opts(base)), :seal, after_step),
         do: step(clean(base, deleted_at), :clean, after_step)
  end

  defp clean(base, deleted_at) do
    with {:ok, %{next_offset: tail}} <- Streams.head(path(base, :updates), stream_opts(base)),
         :ok <- trim(base, :updates, tail),
         {:ok, stored} <- stored(base),
         :ok <- each_ok(stored, &delete_snapshot(base, &1)),
         do: trim(base, :index, deleted_at)
  end

  defp step(:ok, name, after_step) do
    after_step.(name)
    :ok
  end

  defp step({:error, _} = error, _name, _after_step), do: error

  defp index_deletion(base) do
    with :ok <- create(path(base, :index), @json, stream_opts(base), nil),
         {:ok, state} <- state(base),
         do: index_deletion(base, state)
  end

  defp index_deletion(_base, %{deleted: at}) when at != nil, do: {:ok, at}

  defp index_deletion(base, state) do
    case index_deleted(base, state.index_tail) do
      :ok -> deletion_offset(base)
      {:error, :conflict} -> index_deletion_again(base, state.index_tail)
      {:error, _} = error -> error
    end
  end

  defp index_deletion_again(base, tried) do
    case state(base) do
      {:ok, %{index_tail: ^tried}} -> {:error, :index_seq_conflict}
      {:ok, state} -> index_deletion(base, state)
      {:error, _} = error -> error
    end
  end

  defp deletion_offset(base) do
    with {:ok, %{deleted: at}} <- state(base), do: {:ok, at}
  end

  # A log that was never created gets a closed `.updates` too.
  defp close(base) do
    case Streams.close(path(base, :updates), stream_opts(base)) do
      {:ok, _} ->
        :ok

      {:error, :not_found} ->
        case Streams.create(
               path(base, :updates),
               [content_type: @octets, closed: true] ++ stream_opts(base)
             ) do
          {:ok, _, _} -> :ok
          # Created open since: close it.
          {:error, :conflict} -> close(base)
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
  end

  # An unreadable entry is skipped: the previous one stays current.
  defp entry({index_offset, json}, {current, deleted}) do
    case JSON.decode(json) do
      {:ok, %{"deleted" => true}} -> {current, deleted || index_offset}
      {:ok, decoded} -> snapshot_entry(decoded, index_offset, {current, deleted})
      {:error, _} -> {current, deleted}
    end
  end

  defp snapshot_entry(decoded, index_offset, {current, deleted}) do
    case decode_snapshot(decoded) do
      [snapshot] ->
        retained = Enum.flat_map(List.wrap(decoded["retained"]), &decode_snapshot/1)
        {Map.merge(snapshot, %{retained: retained, index_offset: index_offset}), deleted}

      [] ->
        {current, deleted}
    end
  end

  defp snapshot(entry), do: Map.take(entry, [:offset, :created_at])

  defp encode_snapshot(%{offset: offset, created_at: created_at}),
    do: %{"snapshotOffset" => Offset.encode(offset), "createdAt" => created_at}

  defp decode_snapshot(%{"snapshotOffset" => encoded} = decoded) when is_binary(encoded) do
    case Offset.parse(encoded) do
      {:ok, offset} when is_integer(offset) ->
        [%{offset: offset, created_at: created_at(decoded)}]

      _ ->
        []
    end
  end

  defp decode_snapshot(_decoded), do: []

  defp created_at(%{"createdAt" => ms}) when is_integer(ms), do: ms
  defp created_at(_entry), do: 0

  defp read_all(path, opts, deadline), do: read_all(path, :start, [], opts, deadline)

  defp read_all(path, from, acc, opts, deadline) do
    with {:ok, opts} <- call_opts(opts, deadline) do
      case Streams.read(path, from, [max_bytes: @page_bytes] ++ opts) do
        {:ok, %{up_to_date: true} = r} -> {:ok, Enum.reverse(acc, r.messages), r.next_offset}
        {:ok, r} -> read_all(path, r.next_offset, Enum.reverse(r.messages, acc), opts, deadline)
        {:error, _} = error -> error
      end
    end
  end

  defp call_opts(opts, nil), do: {:ok, opts}
  defp call_opts(opts, :infinity), do: {:ok, Keyword.put(opts, :timeout, :infinity)}

  defp call_opts(opts, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)
    if remaining > 0, do: {:ok, Keyword.put(opts, :timeout, remaining)}, else: {:error, :timeout}
  end

  @spec each_ok([term()], (term() -> :ok | {:error, term()})) :: :ok | {:error, term()}
  def each_ok(items, fun) do
    Enum.reduce_while(items, :ok, fn item, :ok ->
      case fun.(item) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end
