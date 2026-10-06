defmodule Slap.Streams.HTTP.Router do
  @moduledoc """
  The [Durable Streams protocol](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md)
  (§5) as a Plug, over
  `Slap.Streams`. The stream's name is the full request path, under `:prefix`
  (default `/v1/stream`). When mounted with `Plug.forward/4`, the mount path
  precedes `:prefix` and is part of the stream name.

  It does not authenticate: every request is allowed. Put authentication in
  front of it, in the Plug pipeline that mounts it or in a proxy. An
  authorization check by path must also cover the `Stream-Forked-From`
  header of a `PUT`: creating a fork copies its source, so it reads that
  path. With authentication, set `:private_cache`.

  Options:

    * `:prefix` - only paths under it are streams (default `"/v1/stream"`).
    * `:long_poll_timeout` - ms a long-poll waits for data (default 30,000).
    * `:sse_timeout` - ms before an SSE response ends, so clients reconnect
      and CDNs can collapse them (default 60,000).
    * `:max_body` - the largest request body, in bytes (default 64 MiB). A
      larger `Content-Length` gets 413 before the body is read.
    * `:max_buffered` - bytes of request bodies this VM holds at once
      (default 512 MiB). All Streams routers in the VM share the counter;
      configure the same limit on each. A request over it gets 503 with
      `Retry-After` before its body is read.
    * `:trust_forwarded` - build the `Location` of a created stream from
      `X-Forwarded-Proto` and `X-Forwarded-Host` (set it only behind a
      proxy that sets them). Default `false`.
    * `:max_read` - about how many bytes one read returns (default 1 MiB).
    * `:max_path` - the longest stream path, in bytes (default 512; 414
      above it).
    * `:private_cache` - send `Cache-Control: private` rather than `public`
      on cacheable historical reads (§8), so that a shared cache (a CDN)
      does not serve one user's reads to another. Default `false`.
    * `:cors` - `"*"` (default), a specific origin such as
      `"https://app.example"`, or `false` to omit CORS response headers.
    * `:cluster` - Streams cluster serving these requests (default
      `Slap.Streams.Cluster`).

  Each response sends a `[:slap, :streams, :http, :request]` telemetry
  event.
  """

  @behaviour Plug

  import Plug.Conn

  alias Slap.Streams
  alias Slap.Streams.{ContentType, Json}
  alias Slap.Streams.HTTP.{Cursor, Errors, SSE}
  alias Slap.Streams.Offset

  @max_int 9_007_199_254_740_991

  @defaults %{
    prefix: "/v1/stream",
    long_poll_timeout: 30_000,
    sse_timeout: 60_000,
    max_body: 64 * 1024 * 1024,
    max_read: 1024 * 1024,
    max_path: 512,
    max_buffered: 512 * 1024 * 1024,
    trust_forwarded: false,
    private_cache: false,
    cors: "*",
    cluster: Slap.Streams.Cluster
  }

  @impl true
  def init(opts) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")
    Keyword.validate!(opts, Map.keys(@defaults))
    cors = Keyword.get(opts, :cors, "*")

    unless cors == false or (is_binary(cors) and cors != ""),
      do: raise(ArgumentError, ":cors must be false or a non-empty origin string")

    Map.merge(@defaults, Map.new(opts))
  end

  @impl true
  def call(conn, opts) do
    opts = %{opts | prefix: mount_prefix(conn) <> opts.prefix}
    conn = conn |> common_headers(opts) |> instrument()
    path = conn.request_path

    cond do
      not String.starts_with?(path, opts.prefix <> "/") ->
        send_resp(conn, 404, "not found")

      byte_size(path) > opts.max_path ->
        send_resp(conn, 414, "stream path too long")

      conn.method == "OPTIONS" ->
        send_resp(conn, 204, "")

      true ->
        try do
          conn
          |> assign(:slap_streams_private_cache, opts.private_cache)
          |> dispatch(conn.method, path, opts)
        after
          release_body()
        end
    end
  end

  defp mount_prefix(conn), do: Enum.map_join(conn.script_name, "", &("/" <> &1))

  defp instrument(conn) do
    start = System.monotonic_time()

    register_before_send(conn, fn conn ->
      Streams.Telemetry.execute(
        [:http, :request],
        %{duration: System.monotonic_time() - start},
        %{method: method_label(conn.method), status: conn.status}
      )

      conn
    end)
  end

  # Any client can send any method: only the protocol's become labels.
  defp method_label(method)
       when method in ["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS"],
       do: method

  defp method_label(_method), do: "OTHER"

  defp dispatch(conn, "PUT", path, opts), do: create(conn, path, opts)
  defp dispatch(conn, "POST", path, opts), do: append(conn, path, opts)
  defp dispatch(conn, "GET", path, opts), do: read(conn, path, opts)
  defp dispatch(conn, "HEAD", path, opts), do: head(conn, path, opts)
  defp dispatch(conn, "DELETE", path, opts), do: delete(conn, path, opts)
  defp dispatch(conn, _method, _path, _opts), do: send_resp(conn, 405, "")

  defp common_headers(conn, opts) do
    # Plug sets `cache-control: max-age=0, private, must-revalidate` by
    # default; caching is decided per response here (§10).
    conn
    |> delete_resp_header("cache-control")
    |> merge_resp_headers(
      cors_headers(opts.cors) ++
        [
          {"x-content-type-options", "nosniff"},
          {"cross-origin-resource-policy", "cross-origin"}
        ]
    )
  end

  defp cors_headers(false), do: []

  defp cors_headers(origin) do
    [
      {"access-control-allow-origin", origin},
      {"access-control-allow-methods", "GET, POST, PUT, DELETE, HEAD, OPTIONS"},
      {"access-control-allow-headers",
       "Content-Type, Stream-Seq, Stream-TTL, Stream-Expires-At, Stream-Closed, " <>
         "If-None-Match, Producer-Id, Producer-Epoch, Producer-Seq, Stream-Forked-From, " <>
         "Stream-Fork-Offset, Stream-Fork-Sub-Offset, Authorization"},
      {"access-control-expose-headers",
       "Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, Stream-Closed, ETag, " <>
         "Location, Producer-Epoch, Producer-Seq, Producer-Expected-Seq, " <>
         "Producer-Received-Seq"}
    ]
  end

  # -- PUT: create (§5.1) ---------------------------------------------------

  defp create(conn, path, opts) do
    ttl = header(conn, "stream-ttl")
    expires = header(conn, "stream-expires-at")

    with {:ok, fork} <- fork_options(conn, opts),
         :ok <- if(ttl && expires, do: {:error, {:bad_request, :ttl_and_expires_at}}, else: :ok),
         {:ok, ttl_s} <- parse_ttl(ttl),
         {:ok, expires_at_ms} <- parse_expires_at(expires),
         {:ok, body, conn} <- read_all(conn, opts) do
      result =
        Streams.create(
          path,
          [
            cluster: opts.cluster,
            content_type: header(conn, "content-type"),
            ttl_s: ttl_s,
            expires_at_ms: expires_at_ms,
            closed: closed?(conn),
            body: body
          ] ++ fork
        )

      case result do
        {:ok, kind, info} -> respond_created(conn, kind, info, opts)
        {:error, reason} -> Errors.send_error(conn, reason)
      end
    else
      {:error, reason, conn} -> Errors.send_error(conn, reason)
      {:error, reason} -> Errors.send_error(conn, reason)
    end
  end

  # 201 for a new stream, 200 for an existing one with the same config.
  defp respond_created(conn, kind, info, opts) do
    conn =
      conn
      |> put_resp_header("content-type", info.content_type)
      |> put_resp_header("stream-next-offset", Offset.encode(info.next_offset))
      |> maybe_header("stream-closed", info.closed && "true")

    if kind == :created,
      do: conn |> put_resp_header("location", location(conn, opts)) |> send_resp(201, ""),
      else: send_resp(conn, 200, "")
  end

  # §4.2. As in the official server, a Stream-Fork-Sub-Offset header counts
  # even when empty, and needs Stream-Forked-From.
  defp fork_options(conn, opts) do
    source = header(conn, "stream-forked-from")
    sub = get_req_header(conn, "stream-fork-sub-offset")

    with :ok <- check_fork_source(source, opts),
         {:ok, offset} <- parse_fork_offset(header(conn, "stream-fork-offset")),
         {:ok, sub} <- parse_sub_offset(sub, source) do
      opts = [forked_from: source, fork_offset: offset, fork_sub_offset: sub]
      {:ok, if(source, do: opts, else: [])}
    end
  end

  # Only streams can be forked: a path under the prefix, like a request's.
  defp check_fork_source(nil, _opts), do: :ok

  defp check_fork_source(source, opts) do
    if String.starts_with?(source, opts.prefix <> "/") and byte_size(source) <= opts.max_path,
      do: :ok,
      else: {:error, :source_not_found}
  end

  defp parse_fork_offset(nil), do: {:ok, nil}

  defp parse_fork_offset(value) do
    case Offset.parse(value) do
      {:ok, offset} when is_integer(offset) -> {:ok, offset}
      {:ok, :start} -> {:ok, 0}
      _ -> {:error, {:bad_request, "invalid Stream-Fork-Offset format"}}
    end
  end

  defp parse_sub_offset([], _source), do: {:ok, nil}

  defp parse_sub_offset(_value, nil),
    do: {:error, {:bad_request, "Stream-Fork-Sub-Offset requires Stream-Forked-From"}}

  defp parse_sub_offset([value | _], _source) do
    if value =~ ~r/\A(0|[1-9][0-9]*)\z/ and byte_size(value) <= 19,
      do: {:ok, String.to_integer(value)},
      else:
        {:error,
         {:bad_request,
          "invalid Stream-Fork-Sub-Offset format: must be a non-negative integer without leading zeros"}}
  end

  defp parse_ttl(nil), do: {:ok, nil}

  defp parse_ttl(value) do
    if value =~ ~r/\A(0|[1-9][0-9]*)\z/,
      do: {:ok, String.to_integer(value)},
      else:
        {:error,
         {:bad_request,
          "invalid TTL format: must be a non-negative integer without leading zeros"}}
  end

  defp parse_expires_at(nil), do: {:ok, nil}

  defp parse_expires_at(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _offset} -> {:ok, DateTime.to_unix(dt, :millisecond)}
      _ -> {:error, {:bad_request, "invalid Stream-Expires-At format"}}
    end
  end

  # X-Forwarded-* is set by a proxy in front, but any client can send it:
  # trusted only when configured.
  defp location(conn, opts) do
    forwarded = fn name -> if opts.trust_forwarded, do: header(conn, name) end
    scheme = forwarded.("x-forwarded-proto") || Atom.to_string(conn.scheme)
    host = forwarded.("x-forwarded-host") || header(conn, "host") || conn.host
    "#{scheme}://#{host}#{conn.request_path}"
  end

  # -- HEAD (§5.5) ------------------------------------------------------------

  defp head(conn, path, opts) do
    case Streams.head(path, cluster: opts.cluster) do
      {:ok, info} ->
        conn
        |> put_resp_header("content-type", info.content_type)
        |> put_resp_header("stream-next-offset", Offset.encode(info.next_offset))
        |> put_resp_header("cache-control", "no-store")
        |> maybe_header("stream-ttl", info.ttl_s && Integer.to_string(info.ttl_s))
        |> maybe_header("stream-expires-at", info.expires_at_ms && rfc3339(info.expires_at_ms))
        |> maybe_header("stream-closed", info.closed && "true")
        |> send_resp(200, "")

      {:error, reason} ->
        Errors.send_error(conn, reason)
    end
  end

  defp rfc3339(ms) do
    ms |> DateTime.from_unix!(:millisecond) |> DateTime.truncate(:second) |> DateTime.to_iso8601()
  end

  # -- DELETE (§5.4) ----------------------------------------------------------

  defp delete(conn, path, opts) do
    case Streams.delete(path, cluster: opts.cluster) do
      :ok -> send_resp(conn, 204, "")
      {:error, reason} -> Errors.send_error(conn, reason)
    end
  end

  # -- POST: append and close (§5.2, §5.2.1, §5.3) ----------------------------

  defp append(conn, path, opts) do
    close = closed?(conn)
    content_type = header(conn, "content-type")

    with {:ok, body, conn} <- read_all(conn, opts),
         {:ok, producer} <- producer(conn),
         :ok <- check_body(body, close, content_type) do
      result =
        if body == "" do
          Streams.close(path, producer: producer, cluster: opts.cluster)
        else
          Streams.append(path, body,
            content_type: content_type,
            close: close,
            stream_seq: header(conn, "stream-seq"),
            producer: producer,
            cluster: opts.cluster
          )
        end

      case result do
        {:ok, r} -> respond_appended(conn, r, producer)
        {:error, reason} -> Errors.send_error(conn, reason)
      end
    else
      {:error, reason, conn} -> Errors.send_error(conn, reason)
      # A malformed request to a missing stream is a 404, as in the official
      # server, which looks the stream up first.
      {:error, reason} -> Errors.send_error(conn, missing_first(path, reason, opts))
    end
  end

  # 200 for a new append with a producer (§5.2.1), else 204.
  defp respond_appended(conn, r, producer) do
    conn
    |> put_resp_header("stream-next-offset", Offset.encode(r.next_offset))
    |> maybe_header("stream-closed", r.closed && "true")
    |> producer_headers(r.producer)
    |> send_resp(append_status(r.result, producer), "")
  end

  defp append_status(result, _producer) when result in [:duplicate, :closed], do: 204
  defp append_status(_result, nil), do: 204
  defp append_status(_result, _producer), do: 200

  defp check_body("", true, _content_type), do: :ok
  defp check_body("", false, _content_type), do: {:error, {:bad_request, :empty_body}}

  defp check_body(_body, _close, nil),
    do: {:error, {:bad_request, "Content-Type header is required"}}

  defp check_body(_body, _close, _content_type), do: :ok

  defp missing_first(path, reason, opts) do
    case Streams.head(path, cluster: opts.cluster) do
      {:error, missing} when missing in [:not_found, :gone] -> missing
      _ -> reason
    end
  end

  # All three producer headers or none (§5.2.1).
  defp producer(conn) do
    case {header(conn, "producer-id"), header(conn, "producer-epoch"),
          header(conn, "producer-seq")} do
      {nil, nil, nil} ->
        {:ok, nil}

      {id, epoch, seq} when id != nil and epoch != nil and seq != nil ->
        with {:ok, epoch} <- producer_int(epoch, "Producer-Epoch"),
             {:ok, seq} <- producer_int(seq, "Producer-Seq") do
          {:ok, {id, epoch, seq}}
        end

      _ ->
        {:error,
         {:bad_request,
          "all producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be " <>
            "provided together"}}
    end
  end

  defp producer_int(value, name) do
    with true <- value =~ ~r/\A[0-9]+\z/,
         n when n <= @max_int <- String.to_integer(value) do
      {:ok, n}
    else
      _ -> {:error, {:bad_request, "invalid #{name}: must be an integer from 0 to 2^53-1"}}
    end
  end

  defp producer_headers(conn, nil), do: conn

  defp producer_headers(conn, {epoch, seq}) do
    conn
    |> put_resp_header("producer-epoch", Integer.to_string(epoch))
    |> put_resp_header("producer-seq", Integer.to_string(seq))
  end

  # -- GET: catch-up, long-poll and SSE (§5.6-5.8, §8) ------------------------

  defp read(conn, path, opts) do
    params = conn.query_string |> URI.query_decoder() |> Enum.to_list()
    offsets = for {"offset", v} <- params, do: v
    live = List.keyfind(params, "live", 0) |> then(&(&1 && elem(&1, 1)))
    cursor = List.keyfind(params, "cursor", 0) |> then(&(&1 && elem(&1, 1)))

    with {:ok, offset} <- parse_offset_param(offsets),
         :ok <- live_needs_offset(live, offsets) do
      case live do
        "sse" -> SSE.serve(conn, path, offset, cursor, opts)
        "long-poll" -> long_poll(conn, path, offset, cursor, opts)
        _ -> catch_up(conn, path, offset, opts)
      end
    else
      {:error, reason} -> Errors.send_error(conn, reason)
    end
  end

  defp parse_offset_param([]), do: {:ok, :start}

  defp parse_offset_param([_, _ | _]),
    do: {:error, {:bad_request, "multiple offset parameters not allowed"}}

  defp parse_offset_param([""]), do: {:error, {:bad_request, "offset parameter cannot be empty"}}

  defp parse_offset_param([value]) do
    case Offset.parse(value) do
      {:ok, offset} -> {:ok, offset}
      {:error, _} -> {:error, {:bad_request, "invalid offset"}}
    end
  end

  defp live_needs_offset(live, []) when live in ["long-poll", "sse"],
    do: {:error, {:bad_request, "offset required for #{live} mode"}}

  defp live_needs_offset(_live, _offsets), do: :ok

  defp catch_up(conn, path, offset, opts) do
    case Streams.read_internal(path, offset, max_bytes: opts.max_read, cluster: opts.cluster) do
      {:ok, r} when offset == :now ->
        # The tail, with no data and nothing to cache (§8).
        conn
        |> read_headers(r)
        |> put_resp_header("cache-control", "no-store")
        |> send_resp(200, if(ContentType.json?(r.content_type), do: "[]", else: ""))

      {:ok, r} ->
        respond_data(conn, r, r.from, nil)

      {:error, reason} ->
        Errors.send_error(conn, reason)
    end
  end

  defp long_poll(conn, path, offset, cursor, opts) do
    case Streams.read_internal(path, offset,
           max_bytes: opts.max_read,
           wait: opts.long_poll_timeout,
           cluster: opts.cluster
         ) do
      {:ok, %{messages: []} = r} -> no_content(conn, r, cursor)
      {:ok, r} -> respond_data(conn, r, r.from, cursor)
      {:error, reason} -> Errors.send_error(conn, reason)
    end
  end

  # 204: no data within the timeout, or the end of a closed stream (§5.7).
  defp no_content(conn, r, cursor) do
    conn
    |> put_resp_header("content-type", r.content_type)
    |> put_resp_header("stream-next-offset", Offset.encode(r.next_offset))
    |> put_resp_header("stream-up-to-date", "true")
    |> put_resp_header("stream-cursor", Cursor.next(cursor))
    |> maybe_header("stream-closed", r.closed && "true")
    |> send_resp(204, "")
  end

  # Historical reads are cacheable (§8). With authentication in front, only
  # by the client: a shared cache (a CDN) would serve one user's read to
  # others.
  defp cache_control(%{assigns: %{slap_streams_private_cache: true}}),
    do: "private, max-age=60, stale-while-revalidate=300"

  defp cache_control(_conn), do: "public, max-age=60, stale-while-revalidate=300"

  defp respond_data(conn, r, from, cursor) do
    etag = ~s("#{r.sid}:#{from}:#{r.next_offset}#{if r.closed, do: ":c"}")

    conn =
      conn
      |> read_headers(r)
      |> put_resp_header("etag", etag)
      |> maybe_header("stream-cursor", cursor_for(conn, cursor))
      |> maybe_header(
        "cache-control",
        (not r.up_to_date and r.messages != []) && cache_control(conn)
      )

    if header(conn, "if-none-match") == etag do
      send_resp(conn, 304, "")
    else
      send_resp(conn, 200, body(r))
    end
  end

  # Long-poll responses carry a cursor; catch-up reads do not.
  defp cursor_for(conn, cursor) do
    if conn.query_string =~ "live=long-poll", do: Cursor.next(cursor)
  end

  defp read_headers(conn, r) do
    conn
    |> put_resp_header("content-type", r.content_type)
    |> put_resp_header("stream-next-offset", Offset.encode(r.next_offset))
    |> maybe_header("stream-up-to-date", r.up_to_date && "true")
    |> maybe_header("stream-closed", r.closed && "true")
  end

  @doc false
  # The response body for a read: a JSON array in JSON mode, else the bytes.
  def body(%{messages: messages, content_type: content_type}) do
    bodies = Enum.map(messages, &elem(&1, 1))
    if ContentType.json?(content_type), do: Json.join(bodies), else: bodies
  end

  # -- Helpers ----------------------------------------------------------------

  defp header(conn, name) do
    case get_req_header(conn, name) do
      [value | _] when value != "" -> value
      _ -> nil
    end
  end

  # `Stream-Closed: true`, case-insensitively; anything else is absent (§4.1).
  defp closed?(conn), do: String.downcase(header(conn, "stream-closed") || "") == "true"

  defp maybe_header(conn, _name, value) when value in [nil, false], do: conn
  defp maybe_header(conn, name, value), do: put_resp_header(conn, name, value)

  # Reads the whole body, at most `max_body` bytes. A declared length over
  # it is refused before reading, and every byte read counts against the
  # node's `max_buffered` until the request ends (released in call/2).
  defp read_all(conn, opts) do
    case declared_length(conn) do
      {:ok, nil} -> read_all(conn, opts, [], 0)
      {:ok, n} -> with {:ok, held} <- admit(n, 0, opts, conn), do: read_all(conn, opts, [], held)
      :error -> {:error, {:bad_request, "invalid Content-Length"}, conn}
    end
  end

  defp declared_length(conn) do
    case get_req_header(conn, "content-length") do
      [] ->
        {:ok, nil}

      [length | _] ->
        case Integer.parse(length) do
          {n, ""} when n >= 0 -> {:ok, n}
          _ -> :error
        end
    end
  end

  # `held`: bytes already counted for this request.
  defp read_all(conn, opts, acc, held) do
    case read_body(conn, length: 1_000_000) do
      {:ok, chunk, conn} ->
        body = IO.iodata_to_binary([acc, chunk])
        with {:ok, _held} <- admit(byte_size(body), held, opts, conn), do: {:ok, body, conn}

      {:more, chunk, conn} ->
        acc = [acc, chunk]

        with {:ok, held} <- admit(IO.iodata_length(acc), held, opts, conn),
             do: read_all(conn, opts, acc, held)

      {:error, _} ->
        {:error, {:bad_request, "failed to read body"}, conn}
    end
  end

  # A body of `size` bytes so far: at most `max_body`, and counted against
  # the node's budget (beyond the `held` bytes already counted).
  defp admit(size, _held, %{max_body: max}, conn) when size > max,
    do: {:error, :payload_too_large, conn}

  defp admit(size, held, _opts, _conn) when size <= held, do: {:ok, held}

  defp admit(size, held, opts, conn) do
    if hold_body(opts, size - held), do: {:ok, size}, else: {:error, :overloaded, conn}
  end

  @doc false
  # Creates the node's counter of request body bytes held (at startup).
  def setup do
    unless :persistent_term.get(__MODULE__, nil),
      do: :persistent_term.put(__MODULE__, :atomics.new(1, signed: true))

    :ok
  end

  defp buffered, do: :persistent_term.get(__MODULE__)

  # The node's budget for request bodies: this request's share is kept in
  # the process dictionary (one request per process), and released when
  # the request ends.
  defp hold_body(opts, bytes) do
    if :atomics.add_get(buffered(), 1, bytes) > opts.max_buffered do
      :atomics.sub(buffered(), 1, bytes)
      false
    else
      Process.put(:slap_streams_body_held, (Process.get(:slap_streams_body_held) || 0) + bytes)
      true
    end
  end

  defp release_body do
    case Process.delete(:slap_streams_body_held) do
      nil -> :ok
      bytes -> :atomics.sub(buffered(), 1, bytes)
    end
  end
end
