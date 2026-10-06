defmodule Slap.KV.HTTP.Router do
  @moduledoc """
  `Slap.KV` over HTTP, as a Plug. Under `:prefix` (default `/v1/kv`, relative
  to a `Plug.forward/4` mount):

    * `GET /v1/kv/{partition}/{key}` - the value (200, with the version as
      the `ETag`), or 404. `HEAD` is the same without the body.
    * `PUT /v1/kv/{partition}/{key}` - writes the request body. 204 with the
      new version as the `ETag`.
    * `DELETE /v1/kv/{partition}/{key}` - 204.
    * `GET /v1/kv/{partition}?prefix=&gte=&lt=&limit=&cursor=` - a page of
      the partition's rows in key order (see `Slap.KV.scan/2`), as JSON:
      `{"rows": [{"key": k, "value": v}], "cursor": c}`, with keys, values
      and the cursor in unpadded base64url, and `cursor` `null` after the
      last page.

  The partition and key are path segments and the scan's bounds query
  parameters, all percent-encoded (a key may contain `/` as `%2F`, and a
  `+` in a query parameter must be sent as `%2B`).

  `PUT` and `DELETE` take `If-Match: "version"`, and `PUT` takes
  `If-None-Match: *` (the row must not exist); a failed condition is 412,
  with the current version as the `ETag` when there is a row. 503 (with
  `Retry-After`) means the shard is unavailable or the request timed out:
  a write may still have been applied.

  It does not authenticate: put that in front of it.

  Options:

    * `:prefix` - default `"/v1/kv"`.
    * `:max_value` - the largest value, in bytes (default 8 MiB; 413 above
      it).
    * `:cluster` - KV cluster serving these requests (default
      `Slap.KV.Cluster`).

  Each response sends a `[:slap, :kv, :http, :request]` telemetry event.
  """

  @behaviour Plug

  import Plug.Conn

  alias Slap.KV

  @defaults %{prefix: "/v1/kv", max_value: 8 * 1024 * 1024, cluster: Slap.KV.Cluster}

  @impl true
  def init(opts) do
    unless Keyword.keyword?(opts), do: raise(ArgumentError, "options must be a keyword list")
    Keyword.validate!(opts, Map.keys(@defaults))
    Map.merge(@defaults, Map.new(opts))
  end

  @impl true
  def call(conn, opts) do
    prefix = Enum.map_join(conn.script_name, "", &("/" <> &1)) <> opts.prefix
    conn = instrument(conn)

    case segments(conn.request_path, prefix) do
      {:ok, segments} -> dispatch(conn, conn.method, segments, opts)
      :not_found -> text(conn, 404, "not found")
    end
  end

  defp instrument(conn) do
    start = System.monotonic_time()

    register_before_send(conn, fn conn ->
      KV.Telemetry.execute(
        [:http, :request],
        %{duration: System.monotonic_time() - start},
        %{method: method_label(conn.method), status: conn.status}
      )

      conn
    end)
  end

  defp method_label(method) when method in ["GET", "HEAD", "PUT", "DELETE"], do: method
  defp method_label(_method), do: "OTHER"

  # The decoded path segments after the prefix: [partition] or
  # [partition, key]. A malformed escape is kept as it is.
  defp segments(path, prefix) do
    with ["", rest] <- String.split(path, prefix <> "/", parts: 2),
         parts when length(parts) in 1..2 <- String.split(rest, "/") do
      {:ok, Enum.map(parts, &URI.decode/1)}
    else
      _ -> :not_found
    end
  end

  defp dispatch(conn, "GET", [partition], opts), do: scan(conn, partition, opts)
  defp dispatch(conn, "GET", [partition, key], opts), do: get(conn, partition, key, true, opts)
  defp dispatch(conn, "HEAD", [partition, key], opts), do: get(conn, partition, key, false, opts)
  defp dispatch(conn, "PUT", [partition, key], opts), do: put(conn, partition, key, opts)
  defp dispatch(conn, "DELETE", [partition, key], opts), do: delete(conn, partition, key, opts)
  defp dispatch(conn, _method, _segments, _opts), do: text(conn, 405, "method not allowed")

  defp get(conn, partition, key, body?, opts) do
    case KV.get(partition, key, cluster: opts.cluster) do
      {:ok, nil} ->
        text(conn, 404, "not found")

      {:ok, %{value: value, version: version}} ->
        conn
        |> put_etag(version)
        |> put_resp_content_type("application/octet-stream", nil)
        |> send_resp(200, if(body?, do: value, else: ""))

      {:error, reason} ->
        error(conn, reason)
    end
  end

  defp put(conn, partition, key, opts) do
    with {:ok, condition} <- condition(conn, true),
         {:ok, value, conn} <- read_value(conn, opts.max_value) do
      case KV.put(partition, key, value, [{:cluster, opts.cluster} | condition]) do
        {:ok, version} -> conn |> put_etag(version) |> send_resp(204, "")
        {:error, reason} -> error(conn, reason)
      end
    else
      {:error, reason} -> error(conn, reason)
      {:too_large, conn} -> text(conn, 413, "value too large")
    end
  end

  defp delete(conn, partition, key, opts) do
    with {:ok, condition} <- condition(conn, false),
         :ok <- KV.delete(partition, key, [{:cluster, opts.cluster} | condition]) do
      send_resp(conn, 204, "")
    else
      {:error, reason} -> error(conn, reason)
    end
  end

  defp scan(conn, partition, opts) do
    conn = fetch_query_params(conn)

    with {:ok, query_opts} <- scan_opts(conn.query_params),
         {:ok, %{rows: rows, cursor: cursor}} <-
           KV.scan(partition, [{:cluster, opts.cluster} | query_opts]) do
      body = %{
        rows: for({k, v} <- rows, do: %{key: encode64(k), value: encode64(v)}),
        cursor: cursor && encode64(cursor)
      }

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, JSON.encode!(body))
    else
      {:error, reason} -> error(conn, reason)
    end
  end

  defp scan_opts(params) do
    Enum.reduce_while(params, {:ok, []}, fn
      {name, value}, {:ok, acc} when name in ["prefix", "gte", "lt"] ->
        {:cont, {:ok, [{String.to_existing_atom(name), value} | acc]}}

      {"limit", value}, {:ok, acc} ->
        case is_binary(value) && Integer.parse(value) do
          {limit, ""} -> {:cont, {:ok, [{:limit, limit} | acc]}}
          _ -> {:halt, {:error, {:bad_request, :invalid_limit}}}
        end

      {"cursor", value}, {:ok, acc} ->
        case is_binary(value) && Base.url_decode64(value, padding: false) do
          {:ok, cursor} -> {:cont, {:ok, [{:cursor, cursor} | acc]}}
          _ -> {:halt, {:error, {:bad_request, :invalid_cursor}}}
        end

      {_name, _value}, acc ->
        {:cont, acc}
    end)
  end

  # `If-Match: "version"` for a put or delete; `If-None-Match: *` for a put.
  defp condition(conn, allow_absent?) do
    case {get_req_header(conn, "if-match"), get_req_header(conn, "if-none-match")} do
      {[], []} -> {:ok, []}
      {[], ["*"]} when allow_absent? -> {:ok, [if_version: :absent]}
      {[etag], []} -> parse_etag(etag)
      _ -> {:error, {:bad_request, :invalid_condition}}
    end
  end

  defp parse_etag(etag) do
    with <<?", rest::binary>> <- etag,
         {version, "\""} <- Integer.parse(rest),
         true <- version >= 0 do
      {:ok, [if_version: version]}
    else
      _ -> {:error, {:bad_request, :invalid_condition}}
    end
  end

  defp put_etag(conn, version), do: put_resp_header(conn, "etag", ~s("#{version}"))

  defp read_value(conn, max) do
    case get_req_header(conn, "content-length") do
      [length] ->
        case Integer.parse(length) do
          {n, ""} when n > max -> {:too_large, conn}
          _ -> read_value(conn, max, [], 0)
        end

      _ ->
        read_value(conn, max, [], 0)
    end
  end

  defp read_value(conn, max, acc, size) do
    case read_body(conn) do
      {:ok, chunk, conn} when size + byte_size(chunk) <= max ->
        {:ok, IO.iodata_to_binary([acc, chunk]), conn}

      {:more, chunk, conn} when size + byte_size(chunk) <= max ->
        read_value(conn, max, [acc, chunk], size + byte_size(chunk))

      {:error, reason} ->
        {:error, reason}

      {_, _chunk, conn} ->
        {:too_large, conn}
    end
  end

  defp error(conn, {:conflict, current}) do
    conn = if current, do: put_etag(conn, current), else: conn
    text(conn, 412, "precondition failed")
  end

  defp error(conn, {:bad_request, reason}), do: text(conn, 400, "bad request: #{reason}")

  defp error(conn, reason) when reason in [:unavailable, :timeout] do
    conn
    |> put_resp_header("retry-after", "1")
    |> text(503, "service unavailable, retry")
  end

  defp error(conn, _reason), do: text(conn, 503, "service unavailable, retry")

  defp text(conn, status, body) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(status, body)
  end

  defp encode64(binary), do: Base.url_encode64(binary, padding: false)
end
