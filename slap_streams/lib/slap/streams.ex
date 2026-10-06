defmodule Slap.Streams do
  @moduledoc """
  Durable Streams operations, in process (the HTTP layer and Yjs build on
  these). Each stream lives on the shard `Slap.Streams.Cluster.shard_for(path)` and is
  served by one process there. Writes are acknowledged only once
  durable.

  Offsets are integers here (`Slap.Streams.Offset` converts to and from the wire
  format), and results are protocol outcomes, not HTTP responses.
  Pass `cluster:` to use a module defined with `use Slap.Streams.Cluster,
  otp_app: :my_app`. Run one Streams cluster per VM.

  `PROTOCOL.md` sections below refer to the
  [Durable Streams specification](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md).

  ## Errors shared by all calls

  Invalid stream names, offsets, bodies and request fields return
  `{:error, {:bad_request, reason}}`. Invalid control options (`:timeout`,
  `:cluster`, `:placement_key`, read limits and waits) and unknown option
  names raise `ArgumentError`. The `placement_key/1` path helper also raises
  for a non-binary argument.

    * `{:error, :not_found}` - no such stream (404).
    * `{:error, :gone}` - soft-deleted (410).
    * `{:error, :unavailable}` - the shard is stopping or moved, or a write
      failed; retry (503).
    * `{:error, :timeout}` - no reply within `:timeout` (default 30 s). The
      request may still complete.
    * `{:error, {:bad_request, reason}}` - 400.

  `reason` is one of:

    * `:invalid_path`, `:invalid_offset`, `:invalid_body`,
      `:invalid_content_type`, `:invalid_ttl`, `:invalid_expires_at`,
      `:invalid_closed`, `:invalid_close`, `:invalid_stream_seq`,
      `:invalid_producer`;
    * `:invalid_fork_source`, `:invalid_fork_offset`,
      `:invalid_fork_sub_offset`, `:fork_of_itself`,
      `:fork_offset_beyond_source`, `:fork_offset_trimmed`;
    * `:empty_body`, `:empty_array`, `:invalid_json`,
      `:ttl_and_expires_at`, `:new_epoch_must_start_at_zero`,
      `:trim_offset_beyond_tail`.
  """

  alias Slap.Streams
  alias Slap.Streams.{Reader, StreamServer, Wait}

  @default_timeout 30_000
  @default_max_bytes 1024 * 1024
  @final_read_reserve_ms 50

  @type path :: binary()
  @type bad_request_reason ::
          :empty_array
          | :empty_body
          | :fork_of_itself
          | :fork_offset_beyond_source
          | :fork_offset_trimmed
          | :invalid_body
          | :invalid_close
          | :invalid_closed
          | :invalid_content_type
          | :invalid_expires_at
          | :invalid_fork_offset
          | :invalid_fork_source
          | :invalid_fork_sub_offset
          | :invalid_json
          | :invalid_offset
          | :invalid_path
          | :invalid_producer
          | :invalid_stream_seq
          | :invalid_ttl
          | :new_epoch_must_start_at_zero
          | :trim_offset_beyond_tail
          | :ttl_and_expires_at

  @type error ::
          :bad_offset
          | :conflict
          | :content_type_mismatch
          | :deleted
          | :gone
          | :not_found
          | :overloaded
          | :offset_beyond_tail
          | :payload_too_large
          | :sealed
          | :source_gone
          | :source_not_found
          | :stream_seq_conflict
          | :timeout
          | :trimmed
          | :unavailable
          | {:bad_request, bad_request_reason()}
          | {:closed, non_neg_integer()}
          | {:producer_seq_gap, non_neg_integer(), non_neg_integer()}
          | {:stale_epoch, non_neg_integer()}
  @type info :: %{
          next_offset: non_neg_integer(),
          closed: boolean(),
          content_type: binary(),
          ttl_s: non_neg_integer() | nil,
          expires_at_ms: integer() | nil
        }
  @type append_result :: %{
          result: :appended | :duplicate | :closed,
          next_offset: non_neg_integer(),
          closed: boolean(),
          producer: {non_neg_integer(), non_neg_integer()} | nil
        }
  @type read_result :: %{
          messages: [{non_neg_integer(), binary()}],
          next_offset: non_neg_integer(),
          up_to_date: boolean(),
          closed: boolean(),
          content_type: binary()
        }

  @read_options [:wait, :max_bytes, :timeout, :placement_key, :cluster]

  @doc """
  Creates a stream (PROTOCOL.md §5.1). Options: `:content_type`, `:ttl_s`,
  `:expires_at_ms`, `:closed`, `:body` (initial content).

  A fork (§4.2) also takes `:forked_from` (the source's path), and optionally
  `:fork_offset` (default: the source's tail) and `:fork_sub_offset`. The
  source's data before the fork offset is copied into the fork.

  Returns `{:ok, :created, info}` (201), `{:ok, :exists, info}` when a stream
  with the same configuration exists (200), or `{:error, :conflict}` when one
  with a different configuration does, or the path is soft-deleted (409).

  Fork errors: `:source_not_found` (404), `:source_gone` (409),
  `:content_type_mismatch` (409), `:payload_too_large` (the copy would
  exceed `:max_fork_copy_bytes`, 413). A fork whose copy was interrupted is
  `:unavailable` (503) until the create is retried, which finishes it.
  """
  @spec create(path(), keyword()) :: {:ok, :created | :exists, info()} | {:error, error()}
  def create(path, opts \\ []) do
    validate_options!(opts, [
      :content_type,
      :ttl_s,
      :expires_at_ms,
      :closed,
      :body,
      :forked_from,
      :fork_offset,
      :fork_sub_offset,
      :timeout,
      :placement_key,
      :cluster
    ])

    with :ok <- validate_create(opts) do
      request(
        path,
        {:create, Map.new(Keyword.drop(opts, [:timeout, :placement_key, :cluster]))},
        opts
      )
    end
  end

  @doc """
  Appends `body` (PROTOCOL.md §5.2). Options:

    * `:content_type` - checked against the stream's, if given.
    * `:close` - close the stream with this append (an empty body closes
      without appending, §5.3).
    * `:stream_seq` - the `Stream-Seq` header.
    * `:producer` - `{producer_id, epoch, seq}` (§5.2.1).

  Returns `{:ok, %{result: r, next_offset: o, closed: c, producer: p}}` where
  `r` is `:appended` (200 with a producer, else 204), `:duplicate` (204) or
  `:closed` (a close-only request, 204), and `p` is `{epoch, seq}` to echo.

  Errors besides the shared ones: `{:closed, next_offset}` (409 with
  `Stream-Closed`), `:content_type_mismatch` (409), `:stream_seq_conflict`
  (409), `{:stale_epoch, epoch}` (403), `{:producer_seq_gap, expected,
  received}` (409).
  """
  @spec append(path(), binary(), keyword()) :: {:ok, append_result()} | {:error, error()}
  def append(path, body, opts \\ []) do
    validate_options!(opts, [
      :content_type,
      :close,
      :stream_seq,
      :producer,
      :timeout,
      :placement_key,
      :cluster
    ])

    with :ok <- validate_append(body, opts) do
      req =
        opts
        |> Keyword.take([:content_type, :close, :stream_seq, :producer])
        |> Map.new()
        |> Map.put(:body, body)

      request(path, {:append, req}, opts)
    end
  end

  @doc "Closes the stream without appending: `append(path, \"\", close: true)`."
  @spec close(path(), keyword()) :: {:ok, append_result()} | {:error, error()}
  def close(path, opts \\ []), do: append(path, "", Keyword.put(opts, :close, true))

  @doc "Deletes the stream (PROTOCOL.md §5.4). Returns `:ok` once durable."
  @spec delete(path(), keyword()) :: :ok | {:error, error()}
  def delete(path, opts \\ []) do
    validate_options!(opts, [:timeout, :placement_key, :cluster])
    request(path, :delete, opts)
  end

  @doc """
  Trims the stream before `offset`: reads from earlier offsets get
  `{:error, :trimmed}` (410), and a background job deletes the data. An
  offset past the tail is a bad request.
  """
  @spec trim(path(), non_neg_integer(), keyword()) :: :ok | {:error, error()}
  def trim(path, offset, opts \\ []) do
    validate_options!(opts, [:timeout, :placement_key, :cluster])
    with :ok <- validate_offset(offset), do: request(path, {:trim, offset}, opts)
  end

  @doc false
  # A request between stream servers (forks), routed like any other.
  def internal(path, request, opts \\ []), do: request(path, request, opts)

  @doc """
  The paths, in order, of the streams in `prefix`'s placement group
  (`placement_key/1`) whose paths start with `prefix`: every stream whose
  creation is durable and that is not deleted. A stream past its expiry is
  listed until it is removed. Not part of the Durable Streams protocol.
  """
  @spec list(path(), keyword()) :: {:ok, [path()]} | {:error, error()}
  def list(prefix, opts \\ []) do
    validate_options!(opts, [:timeout, :cluster])

    if is_binary(prefix) do
      timeout = Keyword.get(opts, :timeout, @default_timeout)
      route(prefix, opts, timeout, {Reader, :list, [prefix, placement_key(prefix)]})
    else
      {:error, {:bad_request, :invalid_path}}
    end
  end

  @doc """
  Seals the placement group `group` for good: from when this returns, a
  create of a stream in the group (`placement_key/1` of its path is
  `group`), or a fork into it, fails with `{:error, :sealed}` (409), and
  every stream created in it before is durable, so `list/2` returns it.
  The group's existing streams are not changed. Not part of the Durable
  Streams protocol.
  """
  @spec seal(binary(), keyword()) :: :ok | {:error, error()}
  def seal(group, opts \\ []) do
    validate_options!(opts, [:timeout, :cluster])
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    route(group, opts, timeout, {Streams.Group, :seal, [group]})
  end

  @doc "The stream's durable metadata and tail (PROTOCOL.md §5.5)."
  @spec head(path(), keyword()) :: {:ok, info()} | {:error, error()}
  def head(path, opts \\ []) do
    validate_options!(opts, [:timeout, :placement_key, :cluster])
    request(path, :head, opts)
  end

  @doc """
  Reads from `offset` (an integer, `:start` for the first retained message,
  or `:now`) up to the durable tail (PROTOCOL.md §5.6), at most about
  `:max_bytes` (default 1 MiB; at least one message). The data is read in
  the calling process.

  Returns `{:ok, %{messages: [{offset, bytes}], next_offset, up_to_date,
  closed, content_type}}`. For a JSON stream each message is one JSON value;
  the HTTP layer joins them into a response array. `closed` is true only when the
  read reached the end of a closed stream. An offset past the durable tail is
  `{:error, :offset_beyond_tail}`.

  With `wait: ms`, a read that finds no messages at the tail of an open
  stream waits up to `ms` for data (long-poll), then reads again from the
  tail; that read's result is returned, empty if nothing came. It reads
  again after a timeout too, so that an empty result's `next_offset` and
  `closed` come from one view of the stream. A stream deleted during the
  wait is `{:error, :deleted}`; one whose server or node went down without
  saying so is `{:error, :unavailable}`.

  An explicit `:timeout` bounds the read, wait, and final read together. A
  long-poll reserves time for the final read. If that read takes longer than
  the remaining budget, the call returns `{:error, :timeout}` even when data
  arrived; the caller can read again. Without `:timeout`, each request uses
  its 30-second default and `:wait` bounds the wait.
  """
  @spec read(path(), non_neg_integer() | :start | :now, keyword()) ::
          {:ok, read_result()} | {:error, error()}
  def read(path, offset, opts \\ []), do: read_request(path, offset, opts, false)

  @doc false
  @spec read_internal(path(), non_neg_integer() | :start | :now, keyword()) ::
          {:ok, map()} | {:error, error()}
  def read_internal(path, offset, opts \\ []), do: read_request(path, offset, opts, true)

  defp read_request(path, offset, opts, internal?) do
    validate_options!(opts, @read_options ++ if(internal?, do: [:peek], else: []))
    validate_read_opts!(opts)
    deadline = read_deadline(opts)

    with :ok <- validate_offset(offset, [:start, :now]) do
      result =
        case {read_now(path, offset, opts, deadline), Keyword.get(opts, :wait, 0)} do
          {{:ok, %{messages: [], closed: false} = r}, wait} when wait > 0 ->
            read_after_wait(path, r, wait, opts, deadline)

          {result, _wait} ->
            result
        end

      if internal?, do: result, else: public_read(result)
    end
  end

  defp public_read({:ok, result}), do: {:ok, Map.drop(result, [:sid, :from])}
  defp public_read(other), do: other

  defp read_now(path, offset, opts, deadline) do
    max_bytes = Keyword.get(opts, :max_bytes, @default_max_bytes)
    timeout = read_remaining(deadline, Keyword.get(opts, :timeout, @default_timeout))

    request = if opts[:peek], do: :peek_info, else: :read_info

    if timeout == 0,
      do: {:error, :timeout},
      else:
        route(path, opts, timeout, {Reader, :read, [path, offset, max_bytes, timeout, request]})
  end

  defp read_after_wait(path, initial, wait, opts, deadline) do
    wait =
      case deadline do
        :infinity ->
          wait

        _ ->
          min(
            wait,
            max(deadline - System.monotonic_time(:millisecond) - @final_read_reserve_ms, 0)
          )
      end

    if wait == 0,
      do: {:ok, initial},
      else: wait_and_read(path, initial, wait, opts, deadline)
  end

  defp wait_and_read(path, initial, wait, opts, deadline) do
    route_opts =
      opts
      |> Keyword.take([:placement_key, :cluster])
      |> Keyword.put(
        :timeout,
        read_remaining(deadline, Keyword.get(opts, :timeout, @default_timeout))
      )

    woken = wake(path, initial.next_offset, wait, route_opts, deadline)
    read_after_wake(path, initial, opts, deadline, woken)
  end

  defp wake(path, offset, wait_ms, route_opts, deadline) do
    case wait(path, offset, self(), route_opts) do
      {:ok, {:waiting, pending}} -> await_wake(path, pending, wait_ms, deadline)
      {:ok, reason} -> reason
      {:error, _} = error -> error
    end
  end

  defp read_after_wake(path, initial, opts, deadline, woken) do
    case woken do
      :timeout ->
        if deadline != :infinity and read_remaining(deadline, 1) == 0,
          do: {:ok, initial},
          else: read_now(path, initial.next_offset, opts, deadline)

      reason when reason in [:data, :closed] ->
        read_now(path, initial.next_offset, opts, deadline)

      reason when reason in [:deleted, :unavailable] ->
        {:error, reason}

      {:error, _} = error ->
        error
    end
  end

  defp read_deadline(opts) do
    case Keyword.fetch(opts, :timeout) do
      {:ok, ms} when is_integer(ms) and ms >= 0 -> System.monotonic_time(:millisecond) + ms
      {:ok, :infinity} -> :infinity
      :error -> :infinity
      {:ok, other} -> raise ArgumentError, "invalid :timeout: #{inspect(other)}"
    end
  end

  defp read_remaining(:infinity, default), do: default

  defp read_remaining(deadline, default),
    do: min(default, max(deadline - System.monotonic_time(:millisecond), 0))

  @doc """
  Waits for data after `offset` (an integer or `:now`), for long-poll and
  SSE. Returns `{:ok, :data}` or `{:ok, :closed}` at once if there is
  something to report, else `{:ok, {:waiting, wait}}`: `pid` then gets
  `{:slap_streams_wake, ref, reason}` once, where `ref` is
  `Slap.Streams.Wait.ref(wait)` and `reason` is `:data`, `:closed`,
  `:deleted` or `:unavailable`. Cancel with `cancel_wait/2`.

  The third argument can be an options list; the waiting pid then defaults
  to the caller.

  When `pid` is the caller, the stream's server (perhaps on another node)
  is monitored too, so that `await_wake/3` reports `:unavailable` if it or
  its node goes down without saying so.
  """
  @spec wait(path(), non_neg_integer() | :now) ::
          {:ok, :data | :closed | {:waiting, Wait.t()}} | {:error, error()}
  @spec wait(path(), non_neg_integer() | :now, pid() | keyword()) ::
          {:ok, :data | :closed | {:waiting, Wait.t()}} | {:error, error()}
  @spec wait(path(), non_neg_integer() | :now, pid(), keyword()) ::
          {:ok, :data | :closed | {:waiting, Wait.t()}} | {:error, error()}
  def wait(path, offset), do: wait(path, offset, self(), [])
  def wait(path, offset, opts) when is_list(opts), do: wait(path, offset, self(), opts)
  def wait(path, offset, pid) when is_pid(pid), do: wait(path, offset, pid, [])

  def wait(_path, _offset, _pid),
    do: raise(ArgumentError, "third argument must be a pid or options list")

  def wait(path, offset, pid, opts) when is_pid(pid) do
    validate_options!(opts, [:timeout, :placement_key, :cluster])
    with :ok <- validate_offset(offset, [:now]), do: wait_valid(path, offset, pid, opts)
  end

  def wait(_path, _offset, _pid, _opts), do: raise(ArgumentError, "third argument must be a pid")

  defp wait_valid(path, offset, pid, opts) do
    case request(path, {:wait, offset, pid}, opts) do
      {:ok, {:waiting, ref, server}} ->
        monitor =
          if pid == self(),
            do: :erlang.monitor(:process, server, tag: {:slap_streams_owner_down, ref}),
            else: nil

        {:ok, {:waiting, Wait.new(ref, monitor, opts)}}

      other ->
        other
    end
  end

  @doc """
  Waits up to `timeout` ms for the wake of a wait from `wait/4`: `:data`,
  `:closed`, `:deleted`, `:unavailable` (the stream's server or its node
  went down) or `:timeout` (the wait is then cancelled).
  """
  @spec await_wake(path(), Wait.t(), timeout()) ::
          :data | :closed | :deleted | :unavailable | :timeout
  def await_wake(path, pending, timeout) do
    case Wait.cast(pending) do
      {:ok, wait} ->
        unless valid_timeout?(timeout),
          do: raise(ArgumentError, "timeout must be a non-negative integer or :infinity")

        await_wake(path, wait, timeout, :infinity)

      :error ->
        raise ArgumentError, "pending must be a Slap.Streams.Wait"
    end
  end

  defp await_wake(path, pending, timeout, deadline) do
    {ref, monitor, _opts} = Wait.details(pending)

    receive do
      {:slap_streams_wake, ^ref, reason} ->
        forget(monitor)
        reason

      {{:slap_streams_owner_down, ^ref}, ^monitor, :process, _pid, _reason} ->
        :unavailable
    after
      timeout ->
        cancel_wait(path, pending, cancel_opts(deadline))

        # A wake sent just before the cancel.
        receive do
          {:slap_streams_wake, ^ref, reason} -> reason
        after
          0 -> :timeout
        end
    end
  end

  defp cancel_opts(:infinity), do: []

  defp cancel_opts(deadline),
    do: [timeout: max(deadline - System.monotonic_time(:millisecond), 1)]

  @doc "Cancels a wait from `wait/4`."
  @spec cancel_wait(path(), Wait.t(), keyword()) :: :ok | {:error, error()}
  def cancel_wait(path, pending, opts \\ [])

  def cancel_wait(path, pending, opts) do
    case Wait.cast(pending) do
      {:ok, wait} ->
        validate_options!(opts, [:timeout, :placement_key, :cluster])
        validate_route_opts!(opts)
        {ref, monitor, route_opts} = Wait.details(wait)
        forget(monitor)
        request(path, {:cancel_wait, ref}, Keyword.merge(route_opts, opts))

      :error ->
        raise ArgumentError, "pending must be a Slap.Streams.Wait"
    end
  end

  defp forget(nil), do: :ok
  defp forget(monitor), do: Process.demonitor(monitor, [:flush])

  @doc """
  The key that places `path` on a shard: the path up to its first segment
  that starts with `.`, or the whole path. Streams that differ only from
  such a segment are one group on one shard, whether they are reached here
  or over HTTP: a Yjs document's `.updates`, `.index` and
  `.snapshots/<offset>_snapshot` (`slap_yjs`), for example. The
  `:placement_key` option of the calls here overrides it.
  """
  @spec placement_key(path()) :: String.t()
  def placement_key(path) do
    unless is_binary(path), do: raise(ArgumentError, "path must be a binary")

    case :binary.match(path, "/.") do
      {at, _} -> binary_part(path, 0, at)
      :nomatch -> path
    end
  end

  defp request(path, request, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    route(path, opts, timeout, {StreamServer, :call, [path, request, timeout]})
  end

  # Runs `mfa` with the context of the shard that owns `path`, on the node
  # that owns it (Slap.Cluster.call/4).
  defp route(path, opts, timeout, mfa) do
    validate_route_opts!(opts)

    if is_binary(path) do
      route_valid(path, opts, timeout, mfa)
    else
      {:error, {:bad_request, :invalid_path}}
    end
  end

  defp validate_options!(opts, allowed) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")
    Keyword.validate!(opts, allowed)
  end

  defp route_valid(path, opts, timeout, mfa) do
    key = Keyword.get_lazy(opts, :placement_key, fn -> placement_key(path) end)
    cluster = Keyword.get(opts, :cluster, Streams.Cluster)
    shard = Slap.Cluster.shard_for(cluster, key)

    remote_timeout =
      if timeout == :infinity or Keyword.has_key?(opts, :timeout),
        do: timeout,
        else: timeout + 5_000

    case Slap.Cluster.call(cluster, shard, mfa, timeout: remote_timeout) do
      {:error, reason} when reason in [:unassigned, :not_owner] -> {:error, :unavailable}
      # The owner could not be reached, or did not answer: the request may
      # or may not have been applied, like any request that fails with 503.
      {:error, {:erpc, _}} -> {:error, :unavailable}
      {:ok, result} -> result
    end
  end

  defp validate_route_opts!(opts) do
    if Keyword.has_key?(opts, :timeout) and not valid_timeout?(opts[:timeout]),
      do: raise(ArgumentError, ":timeout must be a non-negative integer or :infinity")

    if Keyword.has_key?(opts, :cluster) and not valid_module?(opts[:cluster]),
      do: raise(ArgumentError, ":cluster must be a module")

    if Keyword.has_key?(opts, :placement_key) and not is_binary(opts[:placement_key]),
      do: raise(ArgumentError, ":placement_key must be a binary")
  end

  defp validate_read_opts!(opts) do
    for key <- [:wait, :max_bytes], Keyword.has_key?(opts, key) do
      value = opts[key]
      min = if key == :wait, do: 0, else: 1

      unless is_integer(value) and value >= min,
        do: raise(ArgumentError, "#{inspect(key)} must be an integer at least #{min}")
    end

    if Keyword.has_key?(opts, :peek) and not is_boolean(opts[:peek]),
      do: raise(ArgumentError, ":peek must be boolean")
  end

  defp validate_create(opts) do
    validate_fields(opts, [
      {:content_type, &optional_binary?/1, :invalid_content_type},
      {:ttl_s, &optional_nonneg_integer?/1, :invalid_ttl},
      {:expires_at_ms, &optional_integer?/1, :invalid_expires_at},
      {:closed, &optional_boolean?/1, :invalid_closed},
      {:body, &optional_binary?/1, :invalid_body},
      {:forked_from, &optional_binary?/1, :invalid_fork_source},
      {:fork_offset, &optional_nonneg_integer?/1, :invalid_fork_offset},
      {:fork_sub_offset, &optional_nonneg_integer?/1, :invalid_fork_sub_offset}
    ])
  end

  defp validate_append(body, opts) do
    if is_binary(body) do
      validate_fields(opts, [
        {:content_type, &optional_binary?/1, :invalid_content_type},
        {:close, &optional_boolean?/1, :invalid_close},
        {:stream_seq, &optional_binary?/1, :invalid_stream_seq},
        {:producer, &valid_producer?/1, :invalid_producer}
      ])
    else
      {:error, {:bad_request, :invalid_body}}
    end
  end

  defp validate_fields(opts, fields) do
    Enum.find_value(fields, :ok, fn {field, valid?, reason} ->
      if valid?.(opts[field]), do: false, else: {:error, {:bad_request, reason}}
    end)
  end

  defp valid_producer?(nil), do: true

  defp valid_producer?({id, epoch, seq}),
    do: is_binary(id) and is_integer(epoch) and epoch >= 0 and is_integer(seq) and seq >= 0

  defp valid_producer?(_), do: false

  defp optional_binary?(value), do: is_nil(value) or is_binary(value)
  defp optional_boolean?(value), do: is_nil(value) or is_boolean(value)
  defp optional_integer?(value), do: is_nil(value) or is_integer(value)
  defp optional_nonneg_integer?(nil), do: true
  defp optional_nonneg_integer?(value), do: is_integer(value) and value >= 0
  defp valid_timeout?(:infinity), do: true
  defp valid_timeout?(value), do: is_integer(value) and value >= 0
  defp valid_module?(value), do: is_atom(value) and value != nil

  defp validate_offset(value, extra \\ []) do
    if (is_integer(value) and value >= 0) or value in extra,
      do: :ok,
      else: {:error, {:bad_request, :invalid_offset}}
  end
end
