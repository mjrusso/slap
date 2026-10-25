defmodule Slap.KV do
  @moduledoc """
  A durable key-value store on `Slap.KV.Cluster`. Rows are addressed by a
  partition and a key, both non-empty binaries: the partition places the
  row on a shard (`Slap.KV.Cluster.shard_for(partition)`), and within a
  partition rows are in key order, so a partition can be scanned. There
  are no scans across partitions.

  Each row has a version, an integer that changes with every write to it.
  `put/4` and `delete/3` accept `if_version: version` to detect a concurrent
  change. `put/4` also accepts `if_version: :absent` to create a missing row.

  A write returns once it is durable, and reads return only durable rows:
  a read never sees a write that could still be lost.
  Pass `cluster:` to use a module defined with `use Slap.KV.Cluster,
  otp_app: :my_app`. Run one KV cluster per VM.

  After `:timeout` or `:unavailable`, a write may still be applied. Give the
  write a `:deadline`: once the deadline plus the largest difference between
  the nodes' clocks has passed, the write can no longer be applied, and a
  read with `consistency: :linearizable` returns a state it can no longer
  change. The row alone shows whether the write was applied only if no
  other writer could have changed it. To tell in every case, use `batch/3`
  to write a receipt with the change: a row under a key unique to the
  request, in the same partition, that nothing else deletes. After the
  deadline, the write was applied if and only if a linearizable read finds
  the receipt.

  ## Errors

  Invalid control options (`:consistency`, `:with_versions`, `:timeout`,
  `:cluster`) and unknown option names raise `ArgumentError`.

    * `{:error, :unavailable}` - the shard is stopping, moving or has no
      owner, or a write failed. A write may still be applied.
    * `{:error, :timeout}` - no reply within `:timeout` (default 30 s). A
      write may still be applied.
    * `{:error, :deadline_exceeded}` - a write's `:deadline` passed before
      it was applied. It was not, and will not be, applied.
    * `{:error, :overloaded}` - the shard has too much work waiting for
      durability (see `Slap.KV.Cluster`). The request was not applied.
    * `{:error, {:bad_request, :request_too_large}}` - the request is larger
      than the shard's limits on pending bytes. It was not applied.
    * `{:error, {:bad_request, :batch_too_large}}` - a batch has more
      operations or bytes than `:max_batch_ops` or `:max_batch_bytes`. It
      was not applied.
    * `{:error, {:bad_request, reason}}` - invalid row data, batch operations
      or scan bounds.
  """

  alias Slap.KV
  alias Slap.KV.{Keys, PartitionWriter, Read}

  @default_timeout 30_000
  @default_limit 100
  @max_limit 1_000

  @type partition :: binary()
  @type key :: binary()
  @type version :: non_neg_integer()
  @typedoc "The value and version returned by `get/3`."
  @type entry :: %{value: binary(), version: version()}
  @typedoc "A row returned by `scan/2`, with its key and optional version."
  @type scan_entry :: {key(), binary()} | {key(), binary(), version()}
  @type page :: %{
          rows: [scan_entry()],
          cursor: binary() | nil
        }
  @type error ::
          :deadline_exceeded
          | :timeout
          | :unavailable
          | :overloaded
          | {:bad_request, term()}
          | {:conflict, version() | nil}
          | {:conflict, key(), version() | nil}

  @doc """
  Reads a row. Returns `{:ok, %{value: value, version: version}}`, or
  `{:ok, nil}` when there is none.

  With `consistency: :linearizable`, the read goes through the row's
  partition writer, behind the writes it has received, and returns once
  every write it has applied is durable and a write of its own confirms
  that its node still owns the shard. Such a read sees every write that
  was applied before it, acknowledged or not; it costs a write. The
  default, `:durable`, reads what is durable without waiting, and may miss
  a write that is applied but not yet acknowledged.
  """
  @spec get(partition(), key(), keyword()) ::
          {:ok, entry() | nil} | {:error, error()}
  def get(partition, key, opts \\ []) do
    validate_options!(opts, [:consistency, :timeout, :cluster])
    validate_control_opts!(opts)

    with :ok <- Keys.validate(partition, key) do
      case Keyword.get(opts, :consistency, :durable) do
        :durable ->
          route(partition, opts, {Read, :get, [Keys.encode(partition, key)]})

        :linearizable ->
          route(
            partition,
            opts,
            {PartitionWriter, :call, [partition, {:get, partition, key}, timeout(opts)]}
          )
      end
    end
  end

  @doc """
  Writes a row. Returns `{:ok, version}` once it is durable.

  With `if_version: version` the row must be at `version`, and with
  `if_version: :absent` there must be no row; otherwise the result is
  `{:error, {:conflict, current}}`, where `current` is the row's version,
  or `nil` if there is none.

  With `deadline: ms` (a system time, `System.os_time(:millisecond)`) the
  write is applied only if the row's partition writer gets to it before
  then, by its node's clock; otherwise the result is `{:error,
  :deadline_exceeded}`. So once a clock reads `ms` plus the largest
  difference between nodes' clocks, the write can no longer be applied.
  """
  @spec put(partition(), key(), binary(), keyword()) :: {:ok, version()} | {:error, error()}
  def put(partition, key, value, opts \\ []) do
    validate_options!(opts, [:if_version, :deadline, :timeout, :cluster])
    validate_control_opts!(opts)

    with :ok <- Keys.validate(partition, key),
         :ok <- validate_value(value),
         {:ok, condition} <- condition(opts, [:absent]),
         {:ok, deadline} <- deadline(opts) do
      write(partition, {:put, key, value}, condition, deadline, opts)
    end
  end

  @doc """
  Deletes a row. Returns `:ok` once the deletion is durable, whether or not
  the row existed. With `if_version: version` the row must be at `version`,
  and with `deadline:` it must be submitted before then (as for `put/4`).
  """
  @spec delete(partition(), key(), keyword()) :: :ok | {:error, error()}
  def delete(partition, key, opts \\ []) do
    validate_options!(opts, [:if_version, :deadline, :timeout, :cluster])
    validate_control_opts!(opts)

    with :ok <- Keys.validate(partition, key),
         {:ok, condition} <- condition(opts, []),
         {:ok, deadline} <- deadline(opts) do
      write(partition, {:delete, key}, condition, deadline, opts)
    end
  end

  @typedoc "A batch condition: require absence or a particular row version."
  @type batch_condition :: :absent | version()
  @type batch_op ::
          {:put, key(), binary()}
          | {:put, key(), binary(), batch_condition()}
          | {:delete, key()}
          | {:delete, key(), version()}
          | {:check, key(), batch_condition()}

  @doc """
  Applies a nonempty list of operations atomically within one partition.

  Operations are `{:put, key, value}`, `{:delete, key}`, or either with a
  final version condition. Puts also accept `:absent`. A
  `{:check, key, version_or_absent}` checks without changing the row.
  Keys must be distinct, including checks. Conditions are evaluated in
  input order against the state before the batch. The first failure returns
  `{:error, {:conflict, key, current_version_or_nil}}` without applying mutations.

  Mutation success returns `{:ok, version}`; every put shares that version.
  A check-only batch returns `{:ok, nil}` after a durable write confirms
  shard ownership.
  Options are `:deadline`, `:timeout`, and `:cluster`, as for `put/4`.
  After `:timeout` or `:unavailable`, the batch may still be applied; check
  its outcome as for a single write (see the module documentation) before
  retrying.

  The owning shard limits a batch's operations (default 1,000) and its
  encoded key and value bytes (default 8 MiB), and the work waiting for
  durability (see `Slap.KV.Cluster`). After `:overloaded` or a size error,
  the batch was not applied.
  """
  @spec batch(partition(), [batch_op()], keyword()) :: {:ok, version() | nil} | {:error, error()}
  def batch(partition, ops, opts \\ []) do
    validate_options!(opts, [:deadline, :timeout, :cluster])
    validate_control_opts!(opts)

    with :ok <- Keys.validate_partition(partition),
         {:ok, deadline} <- deadline(opts) do
      request = {:batch, partition, ops, deadline}
      route(partition, opts, {PartitionWriter, :call, [partition, request, timeout(opts)]})
    end
  end

  @doc """
  Lists a partition's rows in key order. Returns `{:ok, %{rows: [{key,
  value}], cursor: cursor}}`, where `cursor` is `nil` after the last page.
  With `with_versions: true`, rows are `{key, value, version}`.

  Options:

    * `:prefix` - only keys that start with it.
    * `:gte`, `:lt` - only keys in this range.
    * `:limit` - rows per page, 1 to 1,000 (default 100).
    * `:cursor` - the previous page's cursor, to continue after it. An input
      of `nil` starts at the beginning of the selected range.
    * `:with_versions` - include each row's version (default `false`).

  Each page reads one consistent view of the durable rows. Rows written between
  pages can appear in later pages if their keys come later. Independent gets
  and pages do not share a snapshot. A binary cursor marks a key position and
  remains valid after that row is deleted. Only `:cursor` accepts `nil`; supplied
  `:prefix`, `:gte`, and `:lt` values must be binaries.
  """
  @spec scan(partition(), keyword()) ::
          {:ok, page()} | {:error, error()}
  def scan(partition, opts \\ []) do
    validate_options!(opts, [
      :prefix,
      :gte,
      :lt,
      :limit,
      :cursor,
      :timeout,
      :cluster,
      :with_versions
    ])

    validate_control_opts!(opts)

    limit = Keyword.get(opts, :limit, @default_limit)
    with_versions = Keyword.get(opts, :with_versions, false)

    with :ok <- Keys.validate_partition(partition),
         :ok <- validate_limit(limit),
         {:ok, range} <- range(partition, opts) do
      route(partition, opts, {Read, :scan, [partition, range, limit, with_versions]})
    else
      :empty -> {:ok, %{rows: [], cursor: nil}}
      error -> error
    end
  end

  defp write(partition, op, condition, deadline, opts) do
    request = {:write, partition, op, condition, deadline}
    route(partition, opts, {PartitionWriter, :call, [partition, request, timeout(opts)]})
  end

  defp timeout(opts), do: Keyword.get(opts, :timeout, @default_timeout)

  defp validate_options!(opts, allowed) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")
    Keyword.validate!(opts, allowed)
  end

  defp validate_control_opts!(opts) do
    validate_option!(opts, :with_versions, &is_boolean/1, "boolean")
    validate_option!(opts, :consistency, &valid_consistency?/1, ":durable or :linearizable")
    validate_option!(opts, :timeout, &valid_timeout?/1, "a non-negative integer or :infinity")
    validate_option!(opts, :cluster, &module?/1, "a module")
  end

  defp validate_option!(opts, key, valid?, expected) do
    if Keyword.has_key?(opts, key) and not valid?.(opts[key]),
      do: raise(ArgumentError, "#{inspect(key)} must be #{expected}")
  end

  defp valid_consistency?(value), do: value in [:durable, :linearizable]
  defp valid_timeout?(value), do: value == :infinity or (is_integer(value) and value >= 0)
  defp module?(value), do: is_atom(value) and value != nil

  defp condition(opts, allowed) do
    case Keyword.fetch(opts, :if_version) do
      :error -> {:ok, :any}
      {:ok, version} when is_integer(version) and version >= 0 -> {:ok, {:version, version}}
      {:ok, other} -> if other in allowed, do: {:ok, other}, else: bad_request(:invalid_version)
    end
  end

  defp deadline(opts) do
    case Keyword.get(opts, :deadline) do
      deadline when is_integer(deadline) or is_nil(deadline) -> {:ok, deadline}
      _ -> bad_request(:invalid_deadline)
    end
  end

  defp validate_value(value) when is_binary(value), do: :ok
  defp validate_value(_value), do: bad_request(:invalid_value)

  defp validate_limit(limit) when is_integer(limit) and limit in 1..@max_limit, do: :ok
  defp validate_limit(_limit), do: bad_request(:invalid_limit)

  defp range(partition, opts) do
    bounds = Keyword.take(opts, [:prefix, :gte, :lt, :cursor])

    if Enum.all?(bounds, fn
         {:cursor, nil} -> true
         {_, value} -> is_binary(value)
       end) do
      Keys.range(
        partition,
        Keyword.get(bounds, :prefix, ""),
        bounds[:gte],
        bounds[:cursor],
        bounds[:lt]
      )
    else
      bad_request(:invalid_range)
    end
  end

  defp bad_request(reason), do: {:error, {:bad_request, reason}}

  defp route(partition, opts, mfa) do
    timeout = timeout(opts)
    # Bounds a remote call whose owner stops answering.
    remote_timeout = if timeout == :infinity, do: :infinity, else: timeout + 5_000
    cluster = Keyword.get(opts, :cluster, KV.Cluster)
    shard = Slap.Cluster.shard_for(cluster, partition)

    case Slap.Cluster.call(cluster, shard, mfa, timeout: remote_timeout) do
      {:error, reason} when reason in [:unassigned, :not_owner] -> {:error, :unavailable}
      # The owner could not be reached, or did not answer: a write may or
      # may not have been applied.
      {:error, {:erpc, _}} -> {:error, :unavailable}
      {:ok, result} -> result
    end
  end
end
