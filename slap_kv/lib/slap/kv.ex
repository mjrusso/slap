defmodule Slap.KV do
  @moduledoc """
  A durable key-value store on `Slap.KV.Cluster`. Rows are addressed by a
  partition and a key, both non-empty binaries: the partition places the
  row on a shard (`Slap.KV.Cluster.shard_for(partition)`), and within a
  partition rows are in key order, so a partition can be scanned. There
  are no scans across partitions.

  Each row has a version, an integer that changes with every write to it.
  `put/4` and `delete/3` take `if_version:` to write only if the row is at
  that version (`:absent` for a row that must not exist), so a
  read-modify-write can detect a concurrent change.

  A write returns once it is durable, and reads return only durable rows:
  a read never sees a write that could still be lost.
  Pass `cluster:` to use a module defined with `use Slap.KV.Cluster,
  otp_app: :my_app`. Run one KV cluster per VM.

  A write may be applied after its caller has stopped waiting for it (see
  `:timeout` below). A write with a `:deadline` is not applied after it,
  and a read with `consistency: :linearizable` sees every write applied
  before it: together, they let a caller find out when a write it gave up
  on can no longer happen.

  ## Errors shared by all calls

  Invalid row data and scan bounds return `{:error, {:bad_request, reason}}`.
  Invalid control options (`:consistency`, `:with_versions`, `:timeout`,
  `:cluster`) and unknown option names raise `ArgumentError`.

    * `{:error, :unavailable}` - the shard is stopping, moving or has no
      owner, or a write failed. A write that fails this way may still have
      been applied: read the row to find out rather than retry blindly.
    * `{:error, :timeout}` - no reply within `:timeout` (default 30 s). A
      write may still be applied.
    * `{:error, :deadline_exceeded}` - a write's `:deadline` passed before
      it was applied. It was not, and will not be, applied.
    * `{:error, {:bad_request, reason}}` - invalid row data or scan bounds.
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
          | {:bad_request, term()}
          | {:conflict, version() | nil}

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
      key = Keys.encode(partition, key)

      case Keyword.get(opts, :consistency, :durable) do
        :durable ->
          route(partition, opts, {Read, :get, [key]})

        :linearizable ->
          route(
            partition,
            opts,
            {PartitionWriter, :call, [partition, {:get, key}, timeout(opts)]}
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
      write(partition, {:put, Keys.encode(partition, key), value}, condition, deadline, opts)
    end
  end

  @doc """
  Deletes a row. Returns `:ok` once the deletion is durable, whether or not
  the row existed. With `if_version: version` the row must be at `version`,
  and with `deadline:` it must be applied by then (as for `put/4`).
  """
  @spec delete(partition(), key(), keyword()) :: :ok | {:error, error()}
  def delete(partition, key, opts \\ []) do
    validate_options!(opts, [:if_version, :deadline, :timeout, :cluster])
    validate_control_opts!(opts)

    with :ok <- Keys.validate(partition, key),
         {:ok, condition} <- condition(opts, []),
         {:ok, deadline} <- deadline(opts) do
      write(partition, {:delete, Keys.encode(partition, key)}, condition, deadline, opts)
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
    * `:cursor` - the `cursor` of the previous page, to continue after it.
    * `:with_versions` - include each row's version (default `false`).

  Each page is a separate read of the durable rows: rows written between
  pages appear in later pages if their keys come later.
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
    request = {:write, op, condition, deadline}
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

    if Enum.all?(bounds, fn {_, v} -> is_binary(v) end) do
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
