defmodule Slap.SlateDB do
  @moduledoc """
  Elixir bindings for [SlateDB](https://slatedb.io), an embedded key-value
  store that keeps its data in object storage.

      {:ok, db} = Slap.SlateDB.open("my-db", store: {:local, "/tmp/slatedb"})
      {:ok, _seq} = Slap.SlateDB.put(db, "hello", "world")
      {:ok, "world"} = Slap.SlateDB.get(db, "hello")
      [{"hello", "world"}] = db |> Slap.SlateDB.scan(prefix: "hel") |> Enum.to_list()
      :ok = Slap.SlateDB.close(db)

  Keys and values are binaries. Keys cannot be empty.
  Elixir-side argument and option checks raise. The NIF reports invalid
  operations or store settings as `{:error, %Slap.SlateDB.Error{kind: :invalid}}`;
  other database and object-store failures also return an error tuple.

  ## How calls run

  SlateDB does its I/O on a Tokio runtime inside the NIF library. A call does
  not block a BEAM scheduler: the NIF starts the work and returns, and the
  calling process waits in `receive` for the result. So many processes can
  use one database at the same time, and a slow object store only makes the
  waiting processes slow.

  Set `config :slap_slatedb, runtime_threads: n` in `config/runtime.exs` or
  earlier, or set `SLAP_SLATEDB_RUNTIME_THREADS=n` before starting the VM.
  Application config takes precedence when it is not `nil`. `System.put_env/2` in
  `config/runtime.exs` works too; your application's `start/2` runs too late.
  Invalid values fail application startup. The default is one per CPU.

  ## Durability

  By default a write returns when it is in memory and visible to reads. SlateDB
  writes it to object storage within the `flush_interval` setting (100 ms by
  default). Every write returns `{:ok, seq}`, its sequence number. A write is
  durable once `durable_seq/1` is at least `seq`. To find out:

    * pass `await_durable: true` to the write, which waits before returning,
    * call `flush/1`, or
    * `subscribe/3` to get a message each time the durable sequence number
      goes up. This lets one process write without waiting and confirm many
      writes at once.

  ## Timeouts

  Calls wait for their reply with no timeout by default. Pass
  `timeout: ms` to a call to get
  `{:error, %Slap.SlateDB.Error{kind: :timeout}}` after `ms` milliseconds.
  A timeout does not cancel the operation: a write that timed out may still be
  applied. A reply that comes later is dropped. A call with a timeout runs
  through a short-lived process, so it costs a little more than one without.

  ## Logs and metrics

  SlateDB's log records go to `Logger` with a `:slatedb_target` metadata key.
  Only records at `config :slap_slatedb, log_level: level` or above are sent;
  the default is `:warning`. Change it at runtime with `set_log_level/1`.
  `stats/1` and `metrics/1` report on a database, and `Slap.SlateDB.Telemetry`
  emits its durability progress, stats and close as `:telemetry` events.

  ## Write ordering

  Each call waits for its reply, and SlateDB assigns the sequence number
  before the reply is sent. So the writes of one process are applied in the
  order the process makes them, and each gets a higher `seq` than the one
  before. Writes from different processes at the same time have no defined
  order between them. Use `write/3` or a transaction when several writes must
  be applied together.

  This holds only for calls that got a reply. A write that timed out (see
  "Timeouts") is still running and may be applied after the process's next
  write. Code that depends on write order should not pass `:timeout` to
  writes, and should treat any write error as the end of the handle: stop,
  reopen and reload state from storage.
  """

  alias Slap.SlateDB
  alias Slap.SlateDB.{Iterator, Native, Options, Read, Snapshot, Subscription, Transaction}

  @enforce_keys [:resource]
  defstruct [:resource, :merge_operator]

  # Batches larger than this are converted on a dirty CPU scheduler.
  @dirty_batch_ops 1_000

  @opaque t :: %__MODULE__{resource: reference(), merge_operator: atom() | nil}
  @typedoc "A key, value and metadata returned by `get_key_value/3`."
  @type key_value :: %{
          key: binary(),
          value: binary(),
          seq: non_neg_integer() | nil,
          create_ts: non_neg_integer(),
          expire_ts: non_neg_integer() | nil
        }
  @type store ::
          :memory
          | :memory_ignoring_preconditions
          | {:local, Path.t()}
          | {:url, String.t()}
          | {:url, String.t(), [{atom() | String.t(), String.t()}]}
  @type write_op ::
          {:put, binary(), binary()}
          | {:put, binary(), binary(), non_neg_integer()}
          | {:merge, binary(), binary()}
          | {:merge, binary(), binary(), non_neg_integer()}
          | {:delete, binary()}

  @doc """
  Opens the database at `path` in the given object store, or creates it.

  Only one writer can have a database open at a time. When a second writer
  opens it, the first one is fenced: its next write fails with a
  `:closed` error with reason `:fenced`.

  ## Options

    * `:store` - where the data lives. Required. One of:
      * `:memory` - an in-memory store, lost when the VM exits. Good for tests.
      * `{:local, dir}` - a directory on the local file system.
      * `{:url, url}` or `{:url, url, options}` - an object storage URL:
        `"s3://bucket/prefix"` (Amazon S3 and compatible stores),
        `"az://container/prefix"` (Azure Blob Storage; also `abfs://` and
        `azure://`) or `"gs://bucket/prefix"` (Google Cloud Storage).
        Configuration comes from environment variables (`AWS_*`, `AZURE_*`
        or `GOOGLE_*`, such as `AWS_REGION`, `AZURE_STORAGE_ACCOUNT_NAME`
        or `GOOGLE_SERVICE_ACCOUNT`), then from `options`, which win. For
        example `[aws_region: "us-east-1", aws_endpoint: "http://localhost:9000"]`.
        An unknown option key is an error.

      SlateDB uses conditional puts to fence other writers. S3 stores always
      use ETag conditional puts, and a `conditional_put` setting other than
      `"etag"`, in the environment or in `options`, is an error. Azure and
      GCS support conditional puts without configuration.
    * `:settings` - a map of SlateDB settings, merged over the defaults. The
      keys match SlateDB's JSON settings format, for example
      `%{flush_interval: "50ms", default_ttl_millis: 60_000}`. To keep a
      local disk cache of object storage files, set
      `%{object_store_cache_options: %{root_folder: "/var/cache/slatedb"}}`.
    * `:cache` - the in-memory block and metadata cache. By default each
      database has its own. Pass a `Slap.SlateDB.Cache` to share one between
      databases, or `:disabled` to turn it off.
    * `:merge_operator` - a built-in merge operator for `merge/4`. See
      `Slap.SlateDB.MergeOperator`. Every process that opens the database,
      including readers, must use the same one.
    * `:compaction_filter` - a `Slap.SlateDB.CompactionFilter` that deletes keys
      under a set of prefixes as they are compacted. Needs the compactor,
      which is on by default.
    * `:timeout` - see "Timeouts" above.
  """
  @spec open(String.t(), keyword()) :: {:ok, t()} | {:error, SlateDB.Error.t()}
  def open(path, opts) when is_binary(path) and is_list(opts) do
    opts =
      Keyword.validate!(opts, [
        :store,
        :settings,
        :cache,
        :merge_operator,
        :compaction_filter,
        :timeout
      ])

    store = Options.store(Keyword.fetch!(opts, :store))
    settings_json = Options.settings(Keyword.get(opts, :settings))
    cache = Options.cache(Keyword.get(opts, :cache))
    merge_operator = Options.merge_operator(Keyword.get(opts, :merge_operator))
    filter = Options.compaction_filter(Keyword.get(opts, :compaction_filter))
    open = &Native.db_open(path, store, settings_json, cache, merge_operator, filter, &1)

    with {:ok, resource} <- Native.call(open, Native.timeout(opts)) do
      {:ok, %__MODULE__{resource: resource, merge_operator: merge_operator}}
    end
  end

  @doc """
  Checks a store specification with the same Elixir parser used by `open/2`,
  without opening the store. Returns `:ok` or raises on a malformed
  specification, as `open/2` does. Store connectivity is checked when opened.
  """
  @spec validate_store!(store()) :: :ok
  def validate_store!(store) do
    Options.store(store)
    :ok
  end

  @doc """
  Checks a `:settings` map with the same parser used by `open/2`, without
  opening a database. Returns `:ok` or an `:invalid` error.
  """
  @spec validate_settings(map()) :: :ok | {:error, SlateDB.Error.t()}
  def validate_settings(settings) when is_map(settings) do
    settings |> Options.settings() |> Native.db_validate_settings() |> Native.normalize()
  end

  def validate_settings(settings),
    do: raise(ArgumentError, "settings must be a map, got: #{inspect(settings)}")

  @doc """
  Flushes buffered writes and closes the database.

  Close first waits for calls already in flight on this database to finish.
  Calls made after close starts fail with
  `{:error, %Slap.SlateDB.Error{kind: :closed, reason: :clean}}`, including calls
  on snapshots, transactions and iterators of this database. Closing again
  returns `:ok`.

  If you never call `close/1`, the database is closed in the background once
  it and everything opened from it (snapshots, transactions, iterators) have
  been garbage collected, and a warning is logged. Do not rely on that for
  shutdown.

  After a fence (see `open/2`), close returns `:ok` if the handle already
  knew it was fenced, for example from a failed write or a
  `{:slap_slatedb_closed, ref, tag, :fenced}` message. If close is what discovers the
  fence, it returns `{:error, %Slap.SlateDB.Error{kind: :closed, reason: :fenced}}`:
  writes that were not yet durable are lost. Either way the handle is closed.
  """
  @spec close(t(), keyword()) :: :ok | {:error, SlateDB.Error.t()}
  def close(%__MODULE__{resource: db}, opts \\ []) do
    Keyword.validate!(opts, [:timeout])
    Native.call(&Native.db_close(db, &1), Native.timeout(opts))
  end

  @doc """
  Writes buffered writes to object storage.

  ## Options

    * `:type` - `:wal` (default) writes the WAL, which makes all writes so
      far durable. `:memtable` also writes the memtable out as an L0 SST.
    * `:timeout` - see "Timeouts" above.
  """
  @spec flush(t(), keyword()) :: :ok | {:error, SlateDB.Error.t()}
  def flush(%__MODULE__{resource: db}, opts \\ []) do
    opts = Keyword.validate!(opts, [:type, :timeout])
    type = Options.one_of(Keyword.get(opts, :type, :wal), :type, [:wal, :memtable])
    Native.call(&Native.db_flush(db, type, &1), Native.timeout(opts))
  end

  @doc """
  Reads the value for `key`. Returns `{:ok, nil}` when the key is not found.

  ## Options

    * `:durability` - `:memory` (default) reads the latest write, even if it
      is not durable yet. `:remote` reads only data that is durable in object
      storage.
    * `:dirty` - when `true`, also reads writes that are not yet committed
      to the WAL. Defaults to `false`.
    * `:cache_blocks` - whether blocks read from object storage go into the
      block cache. Defaults to `true`.
    * `:timeout` - see "Timeouts" above.
  """
  @spec get(t(), binary(), keyword()) :: {:ok, binary() | nil} | {:error, SlateDB.Error.t()}
  # Typed entry points to the shared read path (`Slap.SlateDB.Read` and
  # `Slap.SlateDB.Iterator`). Each handle module has the same ones, so the
  # duplication checker is told to skip them.
  # ex_dna:disable-for-next-line
  def get(%__MODULE__{} = db, key, opts \\ []), do: Read.get(db, key, opts)

  @doc """
  Reads the current row for `key` with its metadata.

  Returns `{:ok, nil}` when the key is not found, or `{:ok, row}` where `row`
  is a map with these keys:

    * `:key` and `:value`
    * `:seq` - the sequence number of the write
    * `:create_ts` - when the row was written, in Unix milliseconds
    * `:expire_ts` - when the row expires, in Unix milliseconds, or `nil`

  Takes the options of `get/3`.
  """
  @spec get_key_value(t(), binary(), keyword()) ::
          {:ok, key_value() | nil} | {:error, SlateDB.Error.t()}
  # ex_dna:disable-for-next-line
  def get_key_value(%__MODULE__{} = db, key, opts \\ []), do: Read.get_key_value(db, key, opts)

  @doc """
  Writes `value` for `key`. Returns `{:ok, seq}`, the write's sequence number.

  ## Options

    * `:ttl` - time to live in milliseconds. By default, the `default_ttl_millis`
      setting applies (no expiry unless you set it). SlateDB removes expired
      rows during compaction, so a read can still return a row for a while
      after it expires. Check `:expire_ts` from `get_key_value/2` if you need
      an exact cutoff.
    * `:await_durable` - when `true`, returns only after the write is durable
      in object storage. Defaults to `false`.
    * `:timeout` - see "Timeouts" above.

  Values of 64 KiB or more are passed to SlateDB without copying.
  """
  @spec put(t(), binary(), binary(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, SlateDB.Error.t()}
  def put(%__MODULE__{resource: db}, key, value, opts \\ [])
      when is_binary(key) and is_binary(value) do
    opts = Keyword.validate!(opts, [:ttl, :await_durable, :timeout])
    ttl = Keyword.get(opts, :ttl)
    await_durable = Keyword.get(opts, :await_durable, false)

    Native.call(
      &Native.db_put(db, key, value, ttl, await_durable, &1),
      Native.timeout(opts)
    )
  end

  @doc """
  Deletes `key`. Returns `{:ok, seq}`. Takes the `:await_durable` option of
  `put/4`.
  """
  @spec delete(t(), binary(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, SlateDB.Error.t()}
  def delete(%__MODULE__{resource: db}, key, opts \\ []) when is_binary(key) do
    opts = Keyword.validate!(opts, [:await_durable, :timeout])
    await_durable = Keyword.get(opts, :await_durable, false)
    Native.call(&Native.db_delete(db, key, await_durable, &1), Native.timeout(opts))
  end

  @doc """
  Writes a merge operand for `key`. Returns `{:ok, seq}`.

  The database must be opened with a `:merge_operator`, which combines the
  operand with the key's current value when the key is read or compacted.
  See `Slap.SlateDB.MergeOperator` for the operators and their operand formats.
  An operand in the wrong format is rejected with an `:invalid` error.

  Takes the `:ttl`, `:await_durable` and `:timeout` options of `put/4`.
  """
  @spec merge(t(), binary(), binary(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, SlateDB.Error.t()}
  def merge(%__MODULE__{resource: db}, key, operand, opts \\ [])
      when is_binary(key) and is_binary(operand) do
    opts = Keyword.validate!(opts, [:ttl, :await_durable, :timeout])
    ttl = Keyword.get(opts, :ttl)
    await_durable = Keyword.get(opts, :await_durable, false)

    Native.call(
      &Native.db_merge(db, key, operand, ttl, await_durable, &1),
      Native.timeout(opts)
    )
  end

  @doc """
  Adds `by` to the counter at `key`, without reading it first.

  The database must be opened with `merge_operator: :u64_add` or `:i64_add`.
  Read the counter with `get/3` and `Slap.SlateDB.MergeOperator.decode_u64/1` or
  `decode_i64/1`. Takes the options of `merge/4`. With no `by`, the increment
  is 1; options can be passed as the third argument.
  """
  @spec increment(t(), binary()) :: {:ok, non_neg_integer()} | {:error, SlateDB.Error.t()}
  @spec increment(t(), binary(), integer() | keyword()) ::
          {:ok, non_neg_integer()} | {:error, SlateDB.Error.t()}
  @spec increment(t(), binary(), integer(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, SlateDB.Error.t()}
  def increment(db, key), do: increment(db, key, 1, [])
  def increment(db, key, opts) when is_list(opts), do: increment(db, key, 1, opts)
  def increment(db, key, by), do: increment(db, key, by, [])

  def increment(%__MODULE__{merge_operator: :u64_add} = db, key, by, opts)
      when is_integer(by) and by >= 0,
      do: merge(db, key, SlateDB.MergeOperator.encode_u64(by), opts)

  def increment(%__MODULE__{merge_operator: :i64_add} = db, key, by, opts) when is_integer(by),
    do: merge(db, key, SlateDB.MergeOperator.encode_i64(by), opts)

  def increment(%__MODULE__{merge_operator: op}, _key, by, _opts) do
    raise ArgumentError,
          "increment/4 needs merge_operator: :u64_add (with by >= 0) or :i64_add; " <>
            "this database has #{inspect(op)} and by is #{inspect(by)}"
  end

  @doc """
  Creates a checkpoint: a named, durable view of the database that garbage
  collection will not remove. Returns `{:ok, %{id: id, manifest_id: id}}`.

  Open a `Slap.SlateDB.Reader` at the checkpoint, or clone the database from it
  with `Slap.SlateDB.Admin.clone/3`. Manage checkpoints with `Slap.SlateDB.Admin`.

  ## Options

    * `:scope` - `:durable` (default) covers what is durable now. `:all`
      first flushes writes still in memory, so it covers every write so far.
    * `:lifetime` - milliseconds until the checkpoint expires. By default it
      never expires.
    * `:name` - a name, for `Slap.SlateDB.Admin.list_checkpoints/2`.
    * `:source` - the id of an existing checkpoint to copy.
    * `:timeout` - see "Timeouts" above.
  """
  @spec create_checkpoint(t(), keyword()) ::
          {:ok, %{id: String.t(), manifest_id: non_neg_integer()}}
          | {:error, SlateDB.Error.t()}
  def create_checkpoint(%__MODULE__{resource: db}, opts \\ []) do
    opts = Keyword.validate!(opts, [:scope, :lifetime, :name, :source, :timeout])
    scope = Options.one_of(Keyword.get(opts, :scope, :durable), :scope, [:durable, :all])
    lifetime = Keyword.get(opts, :lifetime)
    source = Keyword.get(opts, :source)
    name = Keyword.get(opts, :name)
    create = &Native.db_create_checkpoint(db, scope, lifetime, source, name, &1)

    with {:ok, {id, manifest_id}} <- Native.call(create, Native.timeout(opts)) do
      {:ok, %{id: id, manifest_id: manifest_id}}
    end
  end

  @doc """
  Applies a list of writes atomically: either all of them happen or none do.
  Returns `{:ok, seq}`. All writes in the batch share that sequence number.

      Slap.SlateDB.write(db, [
        {:put, "a", "1"},
        {:put, "b", "2", 60_000},
        {:delete, "c"}
      ])

  A `{:merge, key, operand}` needs a `:merge_operator` (see `merge/4`). A
  four-element `:put` or `:merge` sets a TTL in milliseconds. Takes the
  `:await_durable` and `:timeout` options of `put/4`.

  A batch of more than #{@dirty_batch_ops} operations is converted on a dirty
  CPU scheduler, so it does not hold up a normal scheduler.
  """
  @spec write(t(), [write_op()], keyword()) ::
          {:ok, non_neg_integer()} | {:error, SlateDB.Error.t()}
  def write(%__MODULE__{resource: db}, ops, opts \\ []) when is_list(ops) do
    opts = Keyword.validate!(opts, [:await_durable, :timeout])
    await_durable = Keyword.get(opts, :await_durable, false)

    write =
      if length(ops) > @dirty_batch_ops,
        do: &Native.db_write_dirty/4,
        else: &Native.db_write/4

    Native.call(&write.(db, ops, await_durable, &1), Native.timeout(opts))
  end

  @doc """
  Returns a lazy stream of `{key, value}` tuples in key order. With
  `with_versions: true`, each row is `{key, value, version}`.

  The scan starts when the stream is run. Each run sees the data as it is at
  that time. Enumeration raises `Slap.SlateDB.Error` if a read fails.

  ## Options

    * `:gte` / `:gt` - lower bound, inclusive or exclusive.
    * `:lte` / `:lt` - upper bound, inclusive or exclusive.
    * `:prefix` - only keys that start with this binary. Can be combined with
      the bounds above.
    * `:batch_size` - rows fetched per native call. Defaults to 256.
    * `:with_versions` - include each row's sequence number as its version.
    * `:order` - `:asc` (default) or `:desc`.
    * `:durability`, `:dirty` - as for `get/3`.
    * `:cache_blocks` - whether blocks read by the scan go into the block
      cache. Defaults to `false`, so a large scan does not push out hot blocks.
    * `:read_ahead_bytes` - how many bytes to fetch ahead from object storage.
    * `:max_fetch_tasks` - how many fetches to run at the same time.
    * `:timeout` - applies to opening the scan and to each batch fetch.

  ## Examples

      Slap.SlateDB.scan(db, gte: "user:", lt: "user;") |> Enum.take(10)
      Slap.SlateDB.scan(db, prefix: "user:") |> Stream.map(&elem(&1, 0)) |> Enum.to_list()
  """
  @spec scan(t(), keyword()) :: Enumerable.t()
  # ex_dna:disable-for-next-line
  def scan(%__MODULE__{} = db, opts \\ []), do: Iterator.stream(db, opts)

  @doc """
  Opens a `Slap.SlateDB.Iterator`. Takes `scan/2`'s bounds, prefix and read
  options. `:with_versions` fixes the row shape when the iterator opens;
  `Slap.SlateDB.Iterator.next_batch/3` sets the fetch size.
  """
  @spec iterator(t(), keyword()) :: {:ok, Iterator.t()} | {:error, SlateDB.Error.t()}
  # ex_dna:disable-for-next-line
  def iterator(%__MODULE__{} = db, opts \\ []), do: Iterator.open(db, opts)

  @doc """
  Checks that a store honours conditional writes, which SlateDB's writer
  fencing depends on. Run it before opening databases on a store you have
  not checked, such as a new S3-compatible server.

  It works on a new object at `path` (under the store's URL prefix, if any)
  in five steps:

    * `:create` - a create-if-absent PUT succeeds.
    * `:create_again` - a second create-if-absent PUT is refused.
    * `:stale_if_match` - a PUT with `If-Match` and a stale ETag is refused.
    * `:current_if_match` - a PUT with `If-Match` and the current ETag
      succeeds.
    * `:delete` - the object is deleted.

  Each step is `:ok`, `{:failed, message}`, `:skipped` (after an earlier
  failure) or, for the two `If-Match` steps, `:unsupported`: SlateDB only
  needs create-if-absent, and some stores (the local file system) do not
  implement `If-Match`. A store that accepts a stale ETag fails.

  Returns `{:ok, steps}` when nothing failed, `{:error, {:probe_failed, steps}}` when a step
  failed, or `{:error, %Slap.SlateDB.Error{}}` if the store cannot be reached.
  `{:probe_failed, steps}` means the probe ran and found a failed condition;
  `%Slap.SlateDB.Error{}` means a store operation failed.

  The `:memory_ignoring_preconditions` store, an in-memory store that treats
  every conditional PUT as an overwrite, exists only to test code that runs
  this probe. Do not open databases on it.

  Takes the `:timeout` option.
  """
  @spec probe_store(store(), String.t(), keyword()) ::
          {:ok, [{atom(), term()}]}
          | {:error, {:probe_failed, [{atom(), term()}]} | SlateDB.Error.t()}
  def probe_store(store, path, opts \\ []) when is_binary(path) do
    Keyword.validate!(opts, [:timeout])
    store = Options.store(store)

    with {:ok, steps} <-
           Native.call(&Native.store_probe(store, path, &1), Native.timeout(opts)) do
      if Enum.any?(steps, &match?({_, {:failed, _}}, &1)),
        do: {:error, {:probe_failed, steps}},
        else: {:ok, steps}
    end
  end

  @doc """
  Returns the highest sequence number that is durable in object storage.

  A write is durable when its `seq` is at most this value.
  """
  @spec durable_seq(t()) :: non_neg_integer()
  def durable_seq(%__MODULE__{resource: db}), do: Native.db_durable_seq(db)

  @doc """
  Returns the highest sequence number written through this handle (0 before
  any write). Every write so far is durable once `durable_seq/1` reaches it.
  """
  @spec last_write_seq(t()) :: non_neg_integer()
  def last_write_seq(%__MODULE__{resource: db}) do
    {_durable, last_write, _l0, _runs} = Native.db_stats(db)
    last_write
  end

  @doc """
  Returns `last_write_seq/1 - durable_seq/1`, or 0: how many sequence numbers
  written through this handle are not durable yet. Unlike `stats/1`, it does
  not collect metrics, so it is cheap enough to sample often.
  """
  @spec durability_lag(t()) :: non_neg_integer()
  def durability_lag(%__MODULE__{resource: db}) do
    {durable, last_write, _l0, _runs} = Native.db_stats(db)
    max(last_write - durable, 0)
  end

  @doc """
  Subscribes `pid` to durability and close events.

  `pid` gets `{:slap_slatedb_durable, ref, tag, durable_seq}` straight away
  and then each time the durable sequence number goes up. Updates are
  coalesced: a busy process gets the latest value, not every step.

  When the database closes, `pid` gets `{:slap_slatedb_closed, ref, tag,
  reason}` once, where `reason` is `:clean`, `:fenced`, `:panic` or
  `:unknown`, and the subscription ends. A fenced writer finds out that it is
  fenced when it next writes or flushes, or when it next reads the manifest
  (every `manifest_poll_interval`).

  `ref` is the returned subscription's `:ref`, unique to this subscription;
  `tag` is the caller's, and several subscriptions may share it.

  The subscription also ends when `pid` exits (it is monitored) or on
  `unsubscribe/2`. It does not end when the returned value is garbage
  collected.
  """
  @spec subscribe(t(), term(), pid()) :: {:ok, Subscription.t()}
  def subscribe(%__MODULE__{resource: db}, tag, pid \\ self()) when is_pid(pid) do
    ref = make_ref()
    {:ok, %Subscription{ref: ref, resource: Native.db_subscribe(db, pid, ref, tag)}}
  end

  @doc """
  Ends a subscription from `subscribe/3`. Ending it twice is allowed.

  No message is sent once this returns, but messages sent before it may be
  waiting in the subscriber's mailbox. With `flush: true`, called from the
  subscriber, they are removed.
  """
  @spec unsubscribe(Subscription.t(), keyword()) :: :ok
  def unsubscribe(%Subscription{ref: ref, resource: resource}, opts \\ []) do
    Keyword.validate!(opts, [:flush])
    flush = Keyword.get(opts, :flush, false)

    unless is_boolean(flush),
      do: raise(ArgumentError, ":flush must be boolean")

    :ok = Native.subscription_cancel(resource)
    if flush, do: flush_subscription(ref)
    :ok
  end

  defp flush_subscription(ref) do
    receive do
      {:slap_slatedb_durable, ^ref, _tag, _seq} -> flush_subscription(ref)
      {:slap_slatedb_closed, ^ref, _tag, _reason} -> flush_subscription(ref)
    after
      0 -> :ok
    end
  end

  @doc """
  Returns a summary of the database's state:

    * `:durable_seq` - see `durable_seq/1`.
    * `:last_write_seq` - the highest `seq` written through this handle.
    * `:durability_lag` - see `durability_lag/1`.
    * `:l0_sst_count` and `:sorted_run_count` - the shape of the LSM tree. A
      growing L0 count means compaction is falling behind.
    * `:cache_hits` and `:cache_misses` - block cache lookups, over all entry
      kinds, since the database opened.

  It works after `close/1` too.
  """
  @spec stats(t()) :: %{atom() => non_neg_integer()}
  def stats(%__MODULE__{resource: db}) do
    {durable, last_write, l0, runs} = Native.db_stats(db)
    {hits, misses} = Native.db_cache_stats(db)

    %{
      durable_seq: durable,
      last_write_seq: last_write,
      durability_lag: max(last_write - durable, 0),
      l0_sst_count: l0,
      sorted_run_count: runs,
      cache_hits: hits,
      cache_misses: misses
    }
  end

  @doc """
  Returns every metric SlateDB records for the database, as a list of
  `%{name: name, labels: %{label => value}, value: value}`.

  `value` is an integer for counters and gauges. For a histogram it is a map
  with `:count`, `:sum`, `:min`, `:max`, `:boundaries` and `:bucket_counts`.
  Names start with `slatedb.`, for example `"slatedb.db.write_ops"`.
  """
  @spec metrics(t()) :: [%{name: String.t(), labels: map(), value: term()}]
  def metrics(%__MODULE__{resource: db}) do
    for {name, labels, value} <- Native.db_metrics(db) do
      value =
        case value do
          {:histogram, count, sum, min, max, boundaries, bucket_counts} ->
            %{
              count: count,
              sum: sum,
              min: min,
              max: max,
              boundaries: boundaries,
              bucket_counts: bucket_counts
            }

          value ->
            value
        end

      %{name: name, labels: Map.new(labels), value: value}
    end
  end

  @doc """
  Sets the lowest level of SlateDB log records sent to `Logger`: `:debug`,
  `:info`, `:warning`, `:error`, or `:none` to send none.
  """
  @spec set_log_level(:debug | :info | :warning | :error | :none) :: :ok
  def set_log_level(level), do: Native.log_set_level(Options.log_level(level))

  @doc "Takes a consistent, read-only snapshot. See `Slap.SlateDB.Snapshot`."
  @spec snapshot(t(), keyword()) :: {:ok, Snapshot.t()} | {:error, SlateDB.Error.t()}
  def snapshot(%__MODULE__{resource: db}, opts \\ []) do
    Keyword.validate!(opts, [:timeout])

    with {:ok, resource} <- Native.call(&Native.db_snapshot(db, &1), Native.timeout(opts)) do
      {:ok, %Snapshot{resource: resource}}
    end
  end

  @doc """
  Starts a transaction. See `Slap.SlateDB.Transaction`.

  `:isolation` is `:snapshot` (the default), which detects write-write
  conflicts, or `:serializable`, which also detects read-write conflicts.
  """
  @spec begin(t(), keyword()) ::
          {:ok, Transaction.t()} | {:error, SlateDB.Error.t()}
  def begin(%__MODULE__{resource: db}, opts \\ []) do
    Keyword.validate!(opts, [:isolation, :timeout])
    isolation = Keyword.get(opts, :isolation, :snapshot)

    unless isolation in [:snapshot, :serializable],
      do: raise(ArgumentError, "invalid isolation: #{inspect(isolation)}")

    with {:ok, resource} <-
           Native.call(&Native.db_begin(db, isolation, &1), Native.timeout(opts)) do
      {:ok, %Transaction{resource: resource}}
    end
  end

  @doc """
  Runs `fun` in a transaction and commits it.

  `fun` returns `{:ok, value}` to commit or `{:error, reason}` to roll back.
  Returns `{:ok, value}` only after the commit is durable. A rollback returns
  `{:error, reason}`. If `fun` raises, throws or exits, the transaction is
  rolled back and the failure is propagated.

  ## Options

    * `:isolation` - `:snapshot` (default) or `:serializable`.
    * `:retries` - how many times to run `fun` again after a commit conflict.
      Defaults to `0`. `fun` must be safe to run more than once.
    * `:timeout` - applies to starting and to committing the transaction.

  Use `begin/2` and `Slap.SlateDB.Transaction.commit/2` to commit without
  waiting for durability and receive the commit's sequence number.

  ## Example

      Slap.SlateDB.transaction(db, fn tx ->
        {:ok, balance} = Slap.SlateDB.Transaction.get(tx, "balance")
        new_balance = String.to_integer(balance || "0") + 10
        :ok = Slap.SlateDB.Transaction.put(tx, "balance", Integer.to_string(new_balance))
        {:ok, new_balance}
      end, retries: 3)
  """
  @spec transaction(t(), (Transaction.t() -> {:ok, any()} | {:error, any()}), keyword()) ::
          {:ok, any()} | {:error, any()}
  def transaction(%__MODULE__{} = db, fun, opts \\ []) when is_function(fun, 1) do
    opts = Keyword.validate!(opts, [:isolation, :retries, :timeout])
    isolation = Keyword.get(opts, :isolation, :snapshot)
    retries = Keyword.get(opts, :retries, 0)
    commit_opts = [await_durable: true] ++ Keyword.take(opts, [:timeout])
    run_transaction(db, fun, isolation, retries, commit_opts)
  end

  defp run_transaction(db, fun, isolation, retries, commit_opts) do
    with {:ok, tx} <- begin(db, [isolation: isolation] ++ Keyword.take(commit_opts, [:timeout])) do
      result =
        try do
          fun.(tx)
        catch
          kind, reason ->
            Transaction.rollback(tx)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      case result do
        {:error, value} ->
          :ok = Transaction.rollback(tx)
          {:error, value}

        {:ok, value} ->
          commit_or_retry(tx, value, {db, fun, isolation, retries, commit_opts})

        other ->
          :ok = Transaction.rollback(tx)

          raise ArgumentError,
                "transaction callback must return {:ok, value} or {:error, reason}, got: #{inspect(other)}"
      end
    end
  end

  # A conflict retries the whole transaction, `retries` more times.
  defp commit_or_retry(tx, value, {db, fun, isolation, retries, commit_opts}) do
    case Transaction.commit(tx, commit_opts) do
      {:ok, _seq} ->
        {:ok, value}

      {:error, %SlateDB.Error{kind: :conflict}} when retries > 0 ->
        run_transaction(db, fun, isolation, retries - 1, commit_opts)

      {:error, _} = error ->
        error
    end
  end
end
