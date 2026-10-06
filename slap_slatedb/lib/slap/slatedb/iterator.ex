defmodule Slap.SlateDB.Iterator do
  @moduledoc """
  A low-level iterator over a key range.

  Most code should use the streams from `Slap.SlateDB.scan/2`,
  `Slap.SlateDB.Snapshot.scan/2` and `Slap.SlateDB.Transaction.scan/2` instead. Use
  this module when you need `seek/2`.

  An iterator is not safe to share between processes that read from it at the
  same time. Calls are serialized, so the rows each process sees will
  interleave.
  """

  alias Slap.SlateDB
  alias Slap.SlateDB.{Native, Options}

  @enforce_keys [:resource, :with_versions]
  defstruct [:resource, :with_versions]

  @opaque t :: %__MODULE__{resource: reference(), with_versions: boolean()}

  @default_batch_size 256

  @doc false
  # Opens an iterator on a handle that can read (see `Slap.SlateDB.Read`).
  def open(%{resource: resource}, opts) do
    if Keyword.has_key?(opts, :batch_size),
      do: raise(ArgumentError, ":batch_size belongs to scan/2; pass the size to next_batch/3")

    with_versions = Keyword.get(opts, :with_versions, false)
    if not is_boolean(with_versions), do: raise(ArgumentError, ":with_versions must be boolean")
    {range, prefix, scan_opts} = Options.scan(Keyword.delete(opts, :with_versions))
    call = &Native.read_scan(resource, range, prefix, scan_opts, &1)

    with {:ok, resource} <- Native.call(call, Native.timeout(opts)) do
      {:ok, %__MODULE__{resource: resource, with_versions: with_versions}}
    end
  end

  @doc """
  Returns up to `max` rows as `{key, value}` tuples. An iterator opened with
  `with_versions: true` returns `{key, value, version}` tuples. The version
  is nil for a transaction's uncommitted writes. Returns `{:ok, []}` when
  the iterator is done. `:timeout` applies to this batch fetch.
  """
  @spec next_batch(t(), pos_integer(), keyword()) ::
          {:ok, [{binary(), binary()} | {binary(), binary(), non_neg_integer() | nil}]}
          | {:error, SlateDB.Error.t()}
  def next_batch(
        %__MODULE__{resource: iter, with_versions: with_versions},
        max \\ @default_batch_size,
        opts \\ []
      )
      when is_integer(max) and max > 0 do
    Keyword.validate!(opts, [:timeout])

    Native.call(
      &Native.iterator_next_batch(iter, max, with_versions, &1),
      Native.timeout(opts)
    )
  end

  @doc """
  Moves the iterator forward to the first key that is at least `key`.

  The key must be inside the iterator's range and not before its current
  position.
  """
  @spec seek(t(), binary(), keyword()) :: :ok | {:error, SlateDB.Error.t()}
  def seek(%__MODULE__{resource: iter}, key, opts \\ []) when is_binary(key) do
    Keyword.validate!(opts, [:timeout])
    Native.call(&Native.iterator_seek(iter, key, &1), Native.timeout(opts))
  end

  @doc false
  # Builds a lazy stream. Each time the stream is run it opens a new
  # iterator, so the stream can be run more than once.
  def stream(handle, opts) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    if not (is_integer(batch_size) and batch_size > 0),
      do: raise(ArgumentError, ":batch_size must be a positive integer")

    call_opts = Keyword.take(opts, [:timeout])
    open_opts = Keyword.delete(opts, :batch_size)

    Stream.resource(
      fn ->
        case open(handle, open_opts) do
          {:ok, iter} -> iter
          {:error, error} -> raise error
        end
      end,
      fn iter ->
        case next_batch(iter, batch_size, call_opts) do
          {:ok, []} -> {:halt, iter}
          {:ok, rows} -> {rows, iter}
          {:error, error} -> raise error
        end
      end,
      # The native iterator is freed when the resource is garbage collected.
      fn _iter -> :ok end
    )
  end
end
