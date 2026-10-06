defmodule Slap.Streams.HTTP.SSE do
  @moduledoc false

  # Each data batch is followed by a control event with the next offset.
  # A caught-up read sends control alone; a closed stream ends with control.
  # Streams other than text or JSON use base64, signaled by
  # stream-sse-data-encoding.

  import Plug.Conn

  alias Slap.Streams
  alias Slap.Streams.ContentType
  alias Slap.Streams.HTTP.{Cursor, Errors, Router}
  alias Slap.Streams.Offset

  @doc false
  def serve(conn, path, offset, cursor, opts) do
    # Resolve `now` and check the stream exists before starting the response.
    case Streams.read_internal(path, offset, max_bytes: opts.max_read, cluster: opts.cluster) do
      {:ok, first} ->
        base64 = base64?(first.content_type)

        conn =
          conn
          |> put_resp_header("content-type", "text/event-stream")
          |> put_resp_header("cache-control", "no-cache")
          |> put_resp_header("x-accel-buffering", "no")
          |> then(
            &if base64, do: put_resp_header(&1, "stream-sse-data-encoding", "base64"), else: &1
          )
          |> send_chunked(200)

        state = %{
          path: path,
          cursor: cursor,
          base64: base64,
          max_read: opts.max_read,
          cluster: opts.cluster,
          deadline: System.monotonic_time(:millisecond) + opts.sse_timeout,
          sent_control: false
        }

        start = if offset == :now, do: first.next_offset, else: first.from
        loop(conn, start, {:ok, first}, state)

      {:error, reason} ->
        Errors.send_error(conn, reason)
    end
  end

  defp loop(conn, offset, read, state) do
    case read ||
           Streams.read_internal(state.path, offset,
             max_bytes: state.max_read,
             cluster: state.cluster
           ) do
      {:ok, %{messages: [_ | _]} = r} ->
        send_batch(conn, r, state)

      # Caught up: a first control event, or the end of a closed stream.
      {:ok, r} when not state.sent_control or r.closed ->
        send_caught_up(conn, r, state)

      {:ok, r} ->
        wait(conn, r.next_offset, state)

      # Deleted, expired or unavailable: end the response.
      {:error, _} ->
        conn
    end
  end

  # One chunk for both events, so a client sees the control event with its
  # data. Then the end (closed), a wait (caught up), or the next read.
  defp send_batch(conn, r, state) do
    events = [
      event("data", data_lines(r, state)),
      control(r.next_offset, r.closed, r.up_to_date, state)
    ]

    case chunk(conn, events) do
      {:ok, conn} -> after_batch(conn, r, %{state | sent_control: true})
      _ -> conn
    end
  end

  defp after_batch(conn, %{closed: true}, _state), do: conn
  defp after_batch(conn, %{up_to_date: true} = r, state), do: wait(conn, r.next_offset, state)
  defp after_batch(conn, r, state), do: loop(conn, r.next_offset, nil, state)

  defp send_caught_up(conn, r, state) do
    case chunk(conn, control(r.next_offset, r.closed, true, state)) do
      {:ok, conn} when r.closed -> conn
      {:ok, conn} -> wait(conn, r.next_offset, %{state | sent_control: true})
      _ -> conn
    end
  end

  # Waits for data until the response's deadline; it ends then, or when
  # the stream is deleted or unavailable.
  defp wait(conn, offset, state) do
    remaining = state.deadline - System.monotonic_time(:millisecond)

    case remaining > 0 &&
           Streams.read_internal(state.path, offset,
             max_bytes: state.max_read,
             wait: remaining,
             cluster: state.cluster
           ) do
      {:ok, %{messages: [], closed: false}} -> conn
      {:ok, _} = read -> loop(conn, offset, read, state)
      _ -> conn
    end
  end

  defp control(next_offset, closed, up_to_date, state) do
    control =
      if closed do
        # The end of a closed stream is also up to date.
        %{
          "streamNextOffset" => Offset.encode(next_offset),
          "streamClosed" => true,
          "upToDate" => true
        }
      else
        %{
          "streamNextOffset" => Offset.encode(next_offset),
          "streamCursor" => Cursor.next(state.cursor)
        }
        |> then(&if up_to_date, do: Map.put(&1, "upToDate", true), else: &1)
      end

    event("control", [JSON.encode!(control)])
  end

  # The body split into `data:` lines on any line terminator, so data cannot
  # inject events; or base64 in one line. No space after `data:`, since
  # clients strip exactly one.
  defp data_lines(r, %{base64: true}),
    do: [r |> Router.body() |> IO.iodata_to_binary() |> Base.encode64()]

  defp data_lines(r, _state),
    do: r |> Router.body() |> IO.iodata_to_binary() |> String.split(["\r\n", "\r", "\n"])

  defp event(name, lines),
    do: ["event: ", name, "\n", Enum.map(lines, &["data:", &1, "\n"]), "\n"]

  defp base64?(content_type) do
    media = ContentType.media_type(content_type)
    not (String.starts_with?(media, "text/") or media == "application/json")
  end
end
