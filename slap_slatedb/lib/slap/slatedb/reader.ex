defmodule Slap.SlateDB.Reader do
  @moduledoc """
  Read-only access to a database, alongside its writer.

  A reader does not fence the writer, so any number of readers can follow one
  database, in this VM or elsewhere. It picks up the writer's changes by
  polling the manifest and replaying the WAL.

      {:ok, reader} = Slap.SlateDB.Reader.open("my-db", store: store)
      {:ok, value} = Slap.SlateDB.Reader.get(reader, "key")
      reader |> Slap.SlateDB.Reader.scan(prefix: "user:") |> Enum.take(10)
      :ok = Slap.SlateDB.Reader.close(reader)

  A reader sees writes once they are durable (in the WAL in object storage),
  within `manifest_poll_interval`. It does not see writes that are still only
  in the writer's memory.
  """

  alias Slap.SlateDB
  alias Slap.SlateDB.{Iterator, Native, Options, Read}

  @enforce_keys [:resource]
  defstruct [:resource]

  @opaque t :: %__MODULE__{resource: reference()}

  @doc """
  Opens a reader for the database at `path`.

  ## Options

    * `:store` - required, as for `Slap.SlateDB.open/2`.
    * `:mode` - how the reader tracks the database:
      * `:managed` (default) - the reader keeps its own checkpoint, refreshed
        as it follows the writer, so garbage collection cannot delete files
        it is reading.
      * `:latest` - no checkpoint and no writes to the store. Reads can fail
        if garbage collection deletes a file the reader still uses.
    * `:checkpoint` - a checkpoint id (from `Slap.SlateDB.create_checkpoint/2`).
      The reader stays at that checkpoint instead of following the writer.
    * `:settings` - a map of reader options, merged over SlateDB's
      defaults: `manifest_poll_interval` (default 10 s),
      `checkpoint_lifetime` (10 min), `max_memtable_bytes`,
      `skip_wal_replay`, `object_store_cache_options` and so on. Give the two
      durations in milliseconds or as a string such as `"100ms"`, `"10s"`,
      `"5m"` or `"1h"`.
    * `:cache` - as for `Slap.SlateDB.open/2`.
    * `:merge_operator` - the database's merge operator, if it has one.
    * `:timeout` - see `Slap.SlateDB`.
  """
  @spec open(String.t(), keyword()) :: {:ok, t()} | {:error, SlateDB.Error.t()}
  def open(path, opts) when is_binary(path) and is_list(opts) do
    Keyword.validate!(opts, [
      :store,
      :mode,
      :checkpoint,
      :settings,
      :cache,
      :merge_operator,
      :timeout
    ])

    store = Options.store(Keyword.fetch!(opts, :store))
    options_json = Options.reader_settings(Keyword.get(opts, :settings))
    mode = Options.one_of(Keyword.get(opts, :mode, :managed), :mode, [:managed, :latest])
    checkpoint = Keyword.get(opts, :checkpoint)
    cache = Options.cache(Keyword.get(opts, :cache))
    merge_operator = Options.merge_operator(Keyword.get(opts, :merge_operator))

    open =
      &Native.reader_open(path, store, options_json, mode, checkpoint, cache, merge_operator, &1)

    with {:ok, resource} <- Native.call(open, Native.timeout(opts)) do
      {:ok, %__MODULE__{resource: resource}}
    end
  end

  @doc """
  Closes the reader. It waits for calls in flight; later calls get a
  `:closed` error. Closing twice is allowed. A `:managed` reader's
  checkpoint is left to expire.
  """
  @spec close(t(), keyword()) :: :ok | {:error, SlateDB.Error.t()}
  def close(%__MODULE__{resource: reader}, opts \\ []) do
    Keyword.validate!(opts, [:timeout])
    Native.call(&Native.reader_close(reader, &1), Native.timeout(opts))
  end

  @doc "Reads `key`. Takes the options of `Slap.SlateDB.get/3`."
  @spec get(t(), binary(), keyword()) :: {:ok, binary() | nil} | {:error, SlateDB.Error.t()}
  # Typed entry points to the shared read path (`Slap.SlateDB.Read` and
  # `Slap.SlateDB.Iterator`). Each handle module has the same ones, so the
  # duplication checker is told to skip them.
  # ex_dna:disable-for-next-line
  def get(%__MODULE__{} = reader, key, opts \\ []), do: Read.get(reader, key, opts)

  @doc "Reads `key` with its metadata, like `Slap.SlateDB.get_key_value/3`."
  @spec get_key_value(t(), binary(), keyword()) ::
          {:ok, SlateDB.key_value() | nil} | {:error, SlateDB.Error.t()}
  # ex_dna:disable-for-next-line
  def get_key_value(%__MODULE__{} = reader, key, opts \\ []),
    do: Read.get_key_value(reader, key, opts)

  @doc "Scans rows. Takes the options of `Slap.SlateDB.scan/2`."
  @spec scan(t(), keyword()) :: Enumerable.t()
  # ex_dna:disable-for-next-line
  def scan(%__MODULE__{} = reader, opts \\ []), do: Iterator.stream(reader, opts)

  @doc "Opens a `Slap.SlateDB.Iterator`. Takes the options of `Slap.SlateDB.iterator/2`."
  @spec iterator(t(), keyword()) :: {:ok, Iterator.t()} | {:error, SlateDB.Error.t()}
  # ex_dna:disable-for-next-line
  def iterator(%__MODULE__{} = reader, opts \\ []), do: Iterator.open(reader, opts)

  @doc "Returns the highest durable sequence number the reader has seen."
  @spec durable_seq(t()) :: non_neg_integer()
  def durable_seq(%__MODULE__{resource: reader}), do: Native.reader_durable_seq(reader)
end
