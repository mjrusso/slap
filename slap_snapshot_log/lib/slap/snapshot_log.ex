defmodule Slap.SnapshotLog do
  @moduledoc """
  An append-only log of opaque entries, with snapshots that replace its
  prefix, stored in Durable Streams (`Slap.Streams`) below a base path. A
  consumer rebuilds its state from the current snapshot and the entries
  after it, follows the log to stay current, and publishes snapshots of
  what it has applied, which trims the entries they cover.

      {:reset, %{snapshot: nil, offset: 0}} = SnapshotLog.next(base, nil)
      {:ok, _} = SnapshotLog.append(base, "entry")
      {:ok, %{entries: ["entry"], offset: offset}} = SnapshotLog.next(base, 0)
      :ok = SnapshotLog.snapshot(base, offset, state_bytes)

  A consumer's loop starts with `next(base, nil)` and handles `{:reset, …}`
  the same way on start and later: another process's snapshot may trim
  the entries the consumer has not read yet.

  ## Snapshots

  A snapshot at `offset` must contain the effect of every entry before
  `offset`; anything more it contains must be harmless to apply again when
  the entries from `offset` on are replayed. A consumer whose entries cannot
  be applied twice snapshots only the entries it has read from the log; one
  whose entries can (Yjs updates, for example) may snapshot its live state.
  The log cannot check this: snapshots and entries are opaque bytes.

  Snapshots are immutable. Of several processes publishing at once, only
  the snapshot at the highest offset becomes current (`{:error,
  :superseded}` for the others), and each step of a publication can be
  interrupted: the log stays readable, and a later publication, or a
  retry at the same offset, cleans up.

  ## Deletion

  `delete/1` deletes the log for good: its entries and snapshots go, and
  every later call on its base returns `{:error, :deleted}`, including a
  publication or an append that started before the delete. A base cannot
  be used again; its `.index`, an empty, closed `.updates` and a seal on
  its placement group (`Slap.Streams.seal/2`) stay, to fence out those
  calls. The log reads as deleted from the delete's first step, so an
  interrupted delete acknowledges nothing more; a retry finishes it.

  ## Storage

  The log's streams are `<base>/.updates` (the entries, one message each),
  `<base>/.index` (JSON entries `{"snapshotOffset", "createdAt",
  "retained"}`, the last current) and `<base>/.snapshots/<offset>_snapshot`:
  the reference `y-durable-streams` server's layout. They share `base` as
  their placement key, so they are on one shard. `base` must be a stream
  path with no segment that starts with `.`.
  Pass `cluster:` to use an application-owned cluster defined with
  `use Slap.Streams.Cluster, otp_app: :my_app`; run one Streams cluster per VM.

  ## Errors

  Invalid base paths, entries, snapshots and offsets return
  `{:error, {:bad_request, reason}}`. Invalid control options and unknown
  option names raise `ArgumentError`. The `path/2` path helper raises for an
  invalid base path.

  Calls return `Slap.Streams` errors, and `{:error, :deleted}` on a
  deleted log (`next/3` also when the log is deleted while it waits). On
  an append, `:unavailable` and
  `:timeout` mean the outcome is unknown: the entry may still be stored.
  Retry an ambiguous append with the same `:producer` sequence to avoid a
  second entry. `next/3` retries transient read errors within its `:timeout`
  budget.

  `:timeout` on `append/3`, `snapshot/4`, `snapshots/2`,
  `read_snapshot/3`, `tail/2`, and `delete/2` bounds each underlying
  Streams call (default 30 s). For `next/3` it bounds the whole retry
  loop (default 60 s).
  """

  alias Slap.SnapshotLog.{Compaction, Store}
  alias Slap.Streams

  @octets "application/octet-stream"
  @transient [:unavailable, :timeout, :overloaded]
  @backoff_min 50
  @backoff_max 2_000
  @default_next_timeout 60_000

  @type base :: String.t()
  @type offset :: non_neg_integer()
  @type page :: %{entries: [binary()], offset: offset(), up_to_date: boolean()}
  @type reset :: %{snapshot: binary() | nil, offset: offset()}
  @type snapshot_info :: %{offset: offset(), created_at: integer()}
  @type error ::
          Streams.error()
          | :deleted
          | :index_seq_conflict
          | :offset_beyond_tail
          | :superseded

  @doc """
  Reads the log. With `offset` nil, or an offset its entries were trimmed
  past, returns `{:reset, %{snapshot: bytes | nil, offset: offset}}`: the
  current snapshot (nil if none), and the offset to read the entries after
  it from. Otherwise returns `{:ok, %{entries: entries, offset: next,
  up_to_date: boolean}}`: a page of entries from `offset`, the offset after
  them, and whether they reach the durable tail.

  At the tail with nothing to read, waits up to `:wait` ms (default 30,000;
  0 returns at once) for an entry. Options also: `:max_bytes` (about how
  many bytes a page holds, default 1 MiB; at least one entry), `:timeout`
  (total budget including waits and retries, default 60,000 ms), and
  `:cluster` (the Streams cluster). `next(base, nil)` creates the log if it
  does not exist; a timed-out call may still have created it.
  """
  @spec next(base(), offset() | nil, keyword()) ::
          {:ok, page()}
          | {:reset, reset()}
          | {:error, error()}
  def next(base, offset, opts \\ []) do
    validate_options!(opts, [:wait, :max_bytes, :timeout, :cluster])
    validate_control_opts!(opts)

    with :ok <- validate(base),
         :ok <- validate_next_offset(offset) do
      deadline = deadline(Keyword.get(opts, :timeout, @default_next_timeout))
      store = Store.new(base, opts)

      retrying(deadline, fn -> read(store, offset, opts, deadline) end)
    end
  end

  defp validate_next_offset(nil), do: :ok
  defp validate_next_offset(offset) when is_integer(offset) and offset >= 0, do: :ok
  defp validate_next_offset(_offset), do: {:error, {:bad_request, :invalid_offset}}

  defp read(base, nil, _opts, deadline) do
    with :ok <- Store.ensure(base, deadline: deadline),
         {:ok, {offset, snapshot}} <- Store.current(base, deadline) do
      {:reset, %{snapshot: snapshot, offset: offset || 0}}
    end
  end

  defp read(base, offset, opts, deadline) do
    path = Store.path(base, :updates)
    remaining = remaining(deadline)

    read_opts =
      Keyword.merge(Store.stream_opts(base),
        max_bytes: Keyword.get(opts, :max_bytes, 1024 * 1024),
        wait: min(Keyword.get(opts, :wait, 30_000), remaining),
        timeout: remaining
      )

    case Streams.read(path, offset, read_opts) do
      {:error, :trimmed} -> read(base, nil, opts, deadline)
      result -> page(base, result, deadline)
    end
  end

  # A read reports `closed` only with the page that reaches the tail; a
  # deleted log's `.updates` is closed from the delete's first step, so a
  # page short of the tail checks for it.
  defp page(_base, {:ok, %{closed: true}}, _deadline), do: {:error, :deleted}

  defp page(base, {:ok, %{up_to_date: false}} = result, deadline) do
    with :ok <- Store.live(base, deadline), do: page(result)
  end

  defp page(_base, result, _deadline), do: page(result)

  defp page({:ok, read}) do
    {:ok,
     %{
       entries: Enum.map(read.messages, &elem(&1, 1)),
       offset: read.next_offset,
       up_to_date: read.up_to_date
     }}
  end

  defp page({:error, _} = error), do: error

  defp retrying(deadline, fun, backoff \\ @backoff_min) do
    case fun.() do
      {:error, reason} when reason in @transient ->
        retry_after(deadline, fun, backoff)

      result ->
        result
    end
  end

  defp retry_after(:infinity, fun, backoff) do
    Process.sleep(backoff)
    retrying(:infinity, fun, min(backoff * 2, @backoff_max))
  end

  defp retry_after(deadline, fun, backoff) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    if remaining == 0 do
      {:error, :timeout}
    else
      Process.sleep(min(backoff, remaining))
      retrying(deadline, fun, min(backoff * 2, @backoff_max))
    end
  end

  defp deadline(:infinity), do: :infinity

  defp deadline(ms) when is_integer(ms) and ms >= 0,
    do: System.monotonic_time(:millisecond) + ms

  defp deadline(other), do: raise(ArgumentError, "invalid :timeout: #{inspect(other)}")

  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  @doc """
  Appends an entry (a non-empty binary), creating the log if needed.
  Returns `{:ok, offset}`, the offset after the entry, once it is durable.
  On `{:error, :unavailable}` or `{:error, :timeout}` the entry may still
  be stored; it is not retried.

  `:producer` accepts `{id, epoch, seq}` as in `Slap.Streams.append/3`.
  Retrying with the same producer sequence does not append again and returns
  `{:duplicate, tail}`. `tail` is the current tail, which may be past the
  original entry. `:timeout` bounds each Streams call; `:cluster` selects
  the Streams cluster.
  """
  @spec append(base(), binary(), keyword()) ::
          {:ok, offset()} | {:duplicate, offset()} | {:error, error()}
  def append(base, entry, opts \\ []) do
    validate_options!(opts, [:timeout, :cluster, :producer])
    validate_control_opts!(opts)

    with :ok <- validate(base),
         :ok <- validate_entry(entry) do
      append_valid(Store.new(base, opts), entry, Keyword.take(opts, [:timeout, :producer]))
    end
  end

  defp validate_entry(entry) when is_binary(entry) and entry != "", do: :ok
  defp validate_entry(_entry), do: {:error, {:bad_request, :invalid_entry}}

  defp append_valid(base, entry, opts) do
    case do_append(base, entry, opts) do
      # Not sent to a stream at all: safe to create the log and send.
      {:error, :not_found} ->
        with :ok <- Store.ensure(base, Keyword.take(opts, [:timeout])),
             do: do_append(base, entry, opts)

      result ->
        result
    end
  end

  defp do_append(base, entry, opts) do
    stream_opts = [content_type: @octets] ++ Keyword.merge(Store.stream_opts(base), opts)

    case Streams.append(Store.path(base, :updates), entry, stream_opts) do
      {:ok, %{result: :duplicate, next_offset: tail}} -> {:duplicate, tail}
      {:ok, %{next_offset: offset}} -> {:ok, offset}
      {:error, {:closed, _tail}} -> {:error, :deleted}
      {:error, _} = error -> error
    end
  end

  @doc """
  Publishes a snapshot that replaces the entries before `offset` (see
  "Snapshots"), makes it current, then trims those entries. Returns `:ok`,
  also when it is current already (finishing the cleanup an interrupted
  publication left), `{:error, :superseded}` when a snapshot at a higher
  offset became current first, or `{:error, :offset_beyond_tail}` when
  `offset` is after the tail.

  Options: `:history`, a list of `{every_ms, keep_ms}` rules that keep the
  latest snapshot of each `every_ms` period for `keep_ms` (default `[]`:
  only the current one), `:cluster`, and `:timeout`.
  """
  @spec snapshot(base(), offset(), binary(), keyword()) :: :ok | {:error, error()}
  def snapshot(base, offset, bytes, opts \\ []) do
    validate_options!(opts, [:history, :cluster, :timeout])
    snapshot_internal(base, offset, bytes, opts)
  end

  @doc false
  @spec snapshot_internal(base(), offset(), binary(), keyword()) :: :ok | {:error, error()}
  def snapshot_internal(base, offset, bytes, opts) do
    validate_options!(opts, [:history, :now, :after_step, :cluster, :timeout])
    validate_control_opts!(opts)
    validate_history!(Keyword.get(opts, :history, []))

    with :ok <- validate(base),
         :ok <- validate_snapshot(offset, bytes) do
      Compaction.run(Store.new(base, opts), offset, bytes, opts)
    end
  end

  defp validate_snapshot(offset, bytes)
       when is_integer(offset) and offset >= 0 and
              is_binary(bytes),
       do: :ok

  defp validate_snapshot(_offset, _bytes), do: {:error, {:bad_request, :invalid_snapshot}}

  defp validate_history!(rules) when is_list(rules) do
    if Enum.all?(rules, fn
         {every_ms, keep_ms} ->
           is_integer(every_ms) and every_ms > 0 and is_integer(keep_ms) and keep_ms > 0

         _ ->
           false
       end),
       do: :ok,
       else: raise(ArgumentError, ":history must contain positive {every_ms, keep_ms} rules")
  end

  defp validate_history!(_rules),
    do: raise(ArgumentError, ":history must contain positive {every_ms, keep_ms} rules")

  @doc """
  The stored snapshots, oldest first: the current one, and the older ones
  the history rules keep. Each is `%{offset: offset, created_at: ms}`.
  """
  @spec snapshots(base(), keyword()) ::
          {:ok, [snapshot_info()]} | {:error, error()}
  def snapshots(base, opts \\ []) do
    validate_options!(opts, [:cluster, :timeout])
    validate_control_opts!(opts)
    with :ok <- validate(base), do: Store.history(Store.new(base, opts))
  end

  @doc "The snapshot at `offset`."
  @spec read_snapshot(base(), offset(), keyword()) :: {:ok, binary()} | {:error, error()}
  def read_snapshot(base, offset, opts \\ []) do
    validate_options!(opts, [:cluster, :timeout])
    validate_control_opts!(opts)

    with :ok <- validate(base),
         :ok <- validate_snapshot_offset(offset),
         store = Store.new(base, opts),
         :ok <- Store.live(store),
         do: Store.read_snapshot(store, offset)
  end

  defp validate_snapshot_offset(offset) when is_integer(offset) and offset >= 0, do: :ok
  defp validate_snapshot_offset(_offset), do: {:error, {:bad_request, :invalid_offset}}

  @doc "The durable tail: the offset after the last entry."
  @spec tail(base(), keyword()) :: {:ok, offset()} | {:error, error()}
  def tail(base, opts \\ []) do
    validate_options!(opts, [:cluster, :timeout])
    validate_control_opts!(opts)
    with :ok <- validate(base), do: Store.tail(Store.new(base, opts))
  end

  @doc """
  Deletes the log for good (see "Deletion"). Deleting a deleted log, or one
  never created, is fine; a retry finishes an interrupted delete.
  """
  @spec delete(base(), keyword()) :: :ok | {:error, error()}
  def delete(base, opts \\ []) do
    validate_options!(opts, [:cluster, :timeout])
    validate_control_opts!(opts)
    with :ok <- validate(base), do: Store.delete(Store.new(base, opts))
  end

  defp validate_control_opts!(opts) do
    validate_option!(opts, :wait, &nonneg_integer?/1, "a non-negative integer")
    validate_option!(opts, :max_bytes, &positive_integer?/1, "a positive integer")
    validate_option!(opts, :timeout, &valid_timeout?/1, "a non-negative integer or :infinity")
    validate_option!(opts, :cluster, &module?/1, "a module")
    validate_option!(opts, :now, &is_integer/1, "an integer")
    validate_option!(opts, :after_step, &is_function(&1, 1), "a one-arity function")
  end

  defp validate_options!(opts, allowed) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")
    Keyword.validate!(opts, allowed)
  end

  defp validate_option!(opts, key, valid?, expected) do
    if Keyword.has_key?(opts, key) and not valid?.(opts[key]),
      do: raise(ArgumentError, "#{inspect(key)} must be #{expected}")
  end

  defp nonneg_integer?(value), do: is_integer(value) and value >= 0
  defp positive_integer?(value), do: is_integer(value) and value > 0
  defp valid_timeout?(value), do: value == :infinity or nonneg_integer?(value)
  defp module?(value), do: is_atom(value) and value != nil

  @doc "The path of the log's `:updates` or `:index` stream, or of a `{:snapshot, offset}`."
  @spec path(base(), :updates | :index | {:snapshot, offset()}) :: String.t()
  def path(base, stream) do
    validate!(base)
    Store.path(base, stream)
  end

  defp validate!(base) do
    case validate(base) do
      :ok -> :ok
      {:error, _} -> raise ArgumentError, "invalid snapshot log base path: #{inspect(base)}"
    end
  end

  defp validate(base) do
    if is_binary(base) and String.starts_with?(base, "/") and not String.ends_with?(base, "/") and
         not String.contains?(base, "/.") do
      :ok
    else
      {:error, {:bad_request, :invalid_base}}
    end
  end
end
