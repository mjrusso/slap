defmodule Slap.Yjs.Store do
  @moduledoc """
  Yjs documents as `Slap.SnapshotLog`s, in the reference `y-durable-streams`
  server's layout, so its HTTP clients can later read the same documents:

      <prefix>/yjs/<service>/docs/<doc>/.updates                     application/octet-stream
      <prefix>/yjs/<service>/docs/<doc>/.index                       application/json
      <prefix>/yjs/<service>/docs/<doc>/.snapshots/<offset>_snapshot application/octet-stream

  Each entry in `.updates` is a batch of lib0-framed updates
  (`Slap.Yjs.Frame`), and a snapshot is an update holding the document's
  state. `<prefix>` defaults to `/v1/stream` and can be set per call.
  Pass `prefix:` and `cluster:` to each operation to select another store.
  For `load/2`, `:timeout` bounds each `Slap.SnapshotLog.next/3` call,
  including its waits and retries. For other operations it bounds each
  underlying Streams call. Define a cluster module with
  `use Slap.Streams.Cluster, otp_app: :my_app` and run one Streams cluster
  per VM.

  Store operations return `{:error, {:bad_request, reason}}` for invalid
  document ids, frames and offsets. Invalid `:prefix`, `:cluster` or
  `:timeout` settings and unknown options raise `ArgumentError`. The
  `base/2` and `path/3` path builders raise for invalid document ids.
  """

  alias Slap.SnapshotLog
  alias Slap.Yjs

  @typedoc "A document: `{service, name}`, each a non-empty path segment."
  @type doc :: {String.t(), String.t()}

  @type loaded :: %{
          snapshot: binary() | nil,
          snapshot_offset: non_neg_integer() | nil,
          updates: [binary()],
          offset: non_neg_integer()
        }
  @type error :: SnapshotLog.error() | {:bad_frames, term()}

  @doc "The document's base path: its `Slap.SnapshotLog`."
  @spec base(doc(), keyword()) :: String.t()
  def base(doc, opts \\ []) do
    validate_options!(opts, [:prefix, :cluster, :timeout])
    validate_options!(opts)

    unless valid_doc?(doc), do: raise(ArgumentError, "invalid Yjs document: #{inspect(doc)}")
    {service, name} = doc

    prefix = Keyword.get(opts, :prefix, "/v1/stream")
    "#{prefix}/yjs/#{service}/docs/#{name}"
  end

  defp valid_segment?(segment),
    do:
      is_binary(segment) and segment != "" and not String.starts_with?(segment, ".") and
        not String.contains?(segment, "/")

  defp valid_doc?({service, name}),
    do: valid_segment?(service) and valid_segment?(name)

  defp valid_doc?(_doc), do: false

  @doc false
  @spec check_doc(doc()) :: :ok | {:error, {:bad_request, :invalid_document}}
  def check_doc(doc) do
    if valid_doc?(doc), do: :ok, else: {:error, {:bad_request, :invalid_document}}
  end

  defp check_doc(doc, opts) do
    validate_options!(opts, [:prefix, :cluster, :timeout])
    validate_options!(opts)
    check_doc(doc)
  end

  defp validate_options!(opts, allowed) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")
    Keyword.validate!(opts, allowed)
  end

  defp validate_options!(opts) do
    if Keyword.has_key?(opts, :prefix) and
         not (is_binary(opts[:prefix]) and String.starts_with?(opts[:prefix], "/")),
       do: raise(ArgumentError, ":prefix must be a path beginning with /")

    if Keyword.has_key?(opts, :cluster) and
         not (is_atom(opts[:cluster]) and opts[:cluster] != nil),
       do: raise(ArgumentError, ":cluster must be a module")

    validate_timeout!(opts)
  end

  defp validate_timeout!(opts) do
    if Keyword.has_key?(opts, :timeout) and
         not (opts[:timeout] == :infinity or
                (is_integer(opts[:timeout]) and opts[:timeout] >= 0)),
       do: raise(ArgumentError, ":timeout must be a non-negative integer or :infinity")
  end

  @doc "The path of the document's `.updates`, `.index` or a snapshot stream."
  @spec path(doc(), :updates | :index | {:snapshot, non_neg_integer()}, keyword()) :: String.t()
  def path(doc, stream, opts \\ []), do: SnapshotLog.path(base(doc, opts), stream)

  defp cluster_opts(opts), do: Keyword.take(opts, [:cluster, :timeout])

  @doc """
  Opens the document (creating it if needed) and reads it: the current
  snapshot, and every update after it, up to the durable tail (`:offset`,
  where the next append goes).
  """
  @spec load(doc(), keyword()) :: {:ok, loaded()} | {:error, error()}
  def load(doc, opts \\ []) do
    with :ok <- check_doc(doc, opts), do: load_valid(doc, opts)
  end

  defp load_valid(doc, opts) do
    base = base(doc, opts)

    with {:reset, %{snapshot: snapshot, offset: at}} <-
           SnapshotLog.next(base, nil, cluster_opts(opts)) do
      case read_to_tail(base, at, [], cluster_opts(opts)) do
        {:ok, entries, offset} -> loaded(snapshot, at, entries, offset)
        # A snapshot published meanwhile trimmed what was left: start again.
        :reset -> load_valid(doc, opts)
        {:error, _} = error -> error
      end
    end
  end

  defp loaded(snapshot, at, entries, offset) do
    with {:ok, updates} <- parse_all(entries) do
      {:ok,
       %{
         snapshot: snapshot,
         snapshot_offset: if(snapshot, do: at),
         updates: updates,
         offset: offset
       }}
    end
  end

  # `acc` holds the entries read so far, last first.
  defp read_to_tail(base, offset, acc, opts) do
    case SnapshotLog.next(base, offset, [wait: 0] ++ opts) do
      {:ok, %{entries: entries, offset: next, up_to_date: true}} ->
        {:ok, Enum.reverse(acc, entries), next}

      {:ok, %{entries: entries, offset: next}} ->
        read_to_tail(base, next, Enum.reverse(entries, acc), opts)

      {:reset, _} ->
        :reset

      {:error, _} = error ->
        error
    end
  end

  @doc "Appends a batch of frames (`Slap.Yjs.Frame`); returns the offset after it."
  @spec append(doc(), binary(), keyword()) :: {:ok, non_neg_integer()} | {:error, error()}
  def append(doc, frames, opts \\ []) do
    validate_options!(opts, [:prefix, :cluster, :timeout])
    validate_options!(opts)

    with :ok <- check_doc(doc),
         :ok <- check_frames(frames) do
      SnapshotLog.append(
        base(doc, Keyword.take(opts, [:prefix, :cluster])),
        frames,
        Keyword.take(opts, [:timeout, :cluster])
      )
    end
  end

  defp check_frames(frames) when is_binary(frames) and frames != "", do: :ok
  defp check_frames(_frames), do: {:error, {:bad_request, :invalid_frames}}

  @doc """
  Publishes the document's state as a snapshot at `offset`
  (`Slap.SnapshotLog.snapshot/4`). The state may hold more than the updates
  before `offset` (pending and local updates): applying an update twice
  changes nothing. `:timeout` bounds each underlying Streams call.
  """
  @spec snapshot(doc(), non_neg_integer(), binary(), keyword()) :: :ok | {:error, error()}
  def snapshot(doc, offset, state, opts \\ []) do
    validate_options!(opts, [:prefix, :cluster, :history, :timeout])
    snapshot_internal(doc, offset, state, opts)
  end

  @doc false
  @spec snapshot_internal(doc(), non_neg_integer(), binary(), keyword()) ::
          :ok | {:error, error()}
  def snapshot_internal(doc, offset, state, opts) do
    validate_options!(opts, [:prefix, :cluster, :history, :now, :after_step, :timeout])
    validate_options!(opts)

    with :ok <- check_doc(doc) do
      SnapshotLog.snapshot_internal(
        base(doc, Keyword.take(opts, [:prefix, :cluster])),
        offset,
        state,
        Keyword.delete(opts, :prefix)
      )
    end
  end

  @doc "The document's stored snapshots, oldest first (`Slap.SnapshotLog.snapshots/1`)."
  @spec snapshots(doc(), keyword()) ::
          {:ok, [SnapshotLog.snapshot_info()]} | {:error, error()}
  def snapshots(doc, opts \\ []) do
    with :ok <- check_doc(doc, opts),
         do: SnapshotLog.snapshots(base(doc, opts), cluster_opts(opts))
  end

  @doc "The snapshot at `offset`."
  @spec read_snapshot(doc(), non_neg_integer(), keyword()) :: {:ok, binary()} | {:error, error()}
  def read_snapshot(doc, offset, opts \\ []) do
    with :ok <- check_doc(doc, opts),
         do: SnapshotLog.read_snapshot(base(doc, opts), offset, cluster_opts(opts))
  end

  @doc "The durable tail of the document's updates."
  @spec tail(doc(), keyword()) :: {:ok, non_neg_integer()} | {:error, error()}
  def tail(doc, opts \\ []) do
    with :ok <- check_doc(doc, opts),
         do: SnapshotLog.tail(base(doc, opts), cluster_opts(opts))
  end

  @doc """
  Deletes the document for good: later calls on it return `{:error,
  :deleted}`, and its id cannot be used again (`Slap.SnapshotLog.delete/1`).
  """
  @spec delete_doc(doc(), keyword()) :: :ok | {:error, error()}
  def delete_doc(doc, opts \\ []) do
    with :ok <- check_doc(doc, opts),
         do: SnapshotLog.delete(base(doc, opts), cluster_opts(opts))
  end

  defp parse_all(entries), do: parse_all(entries, [])

  defp parse_all([], acc), do: {:ok, acc |> Enum.reverse() |> Enum.concat()}

  defp parse_all([entry | rest], acc) do
    case Yjs.Frame.parse(entry) do
      {:ok, updates} -> parse_all(rest, [updates | acc])
      {:error, reason} -> {:error, {:bad_frames, reason}}
    end
  end
end
