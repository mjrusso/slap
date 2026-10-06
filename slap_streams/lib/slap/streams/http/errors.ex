defmodule Slap.Streams.HTTP.Errors do
  @moduledoc false

  import Plug.Conn
  require Logger
  alias Slap.Streams.Offset

  @doc "Sends the response for an error from `Slap.Streams`."
  @spec send_error(Plug.Conn.t(), term()) :: Plug.Conn.t()
  # :deleted: deleted during a long-poll wait.
  def send_error(conn, reason) when reason in [:not_found, :deleted],
    do: text(conn, 404, "stream not found")

  def send_error(conn, :gone), do: text(conn, 410, "stream has been deleted")

  def send_error(conn, :conflict),
    do: text(conn, 409, "stream exists with different configuration")

  def send_error(conn, {:closed, next_offset}) do
    conn
    |> put_resp_header("stream-closed", "true")
    |> put_resp_header("stream-next-offset", Offset.encode(next_offset))
    |> text(409, "stream is closed")
  end

  def send_error(conn, :content_type_mismatch), do: text(conn, 409, "content type mismatch")
  def send_error(conn, :sealed), do: text(conn, 409, "stream group is sealed")
  def send_error(conn, :source_not_found), do: text(conn, 404, "source stream not found")

  def send_error(conn, :source_gone),
    do: text(conn, 409, "source stream was deleted but still has active forks")

  def send_error(conn, :trimmed), do: text(conn, 410, "data before this offset has been deleted")
  def send_error(conn, :stream_seq_conflict), do: text(conn, 409, "sequence number conflict")

  def send_error(conn, {:stale_epoch, epoch}) do
    conn
    |> put_resp_header("producer-epoch", Integer.to_string(epoch))
    |> text(403, "producer epoch is stale")
  end

  def send_error(conn, {:producer_seq_gap, expected, received}) do
    conn
    |> put_resp_header("producer-expected-seq", Integer.to_string(expected))
    |> put_resp_header("producer-received-seq", Integer.to_string(received))
    |> text(409, "producer sequence gap detected")
  end

  def send_error(conn, :offset_beyond_tail), do: text(conn, 400, "offset is beyond the tail")
  def send_error(conn, {:bad_request, reason}), do: text(conn, 400, bad_request(reason))
  def send_error(conn, :payload_too_large), do: text(conn, 413, "request body too large")

  def send_error(conn, :overloaded) do
    conn
    |> put_resp_header("retry-after", "1")
    |> text(503, "too many writes in flight, retry")
  end

  def send_error(conn, reason) when reason in [:unavailable, :timeout] do
    conn
    |> put_resp_header("retry-after", "1")
    |> text(503, "service unavailable, retry")
  end

  # Anything else is a bug: log it, and answer 500 rather than crash the
  # connection.
  def send_error(conn, reason) do
    Logger.error("unexpected error for #{conn.method} #{conn.request_path}: #{inspect(reason)}")
    text(conn, 500, "internal error")
  end

  defp bad_request(:empty_body), do: "empty body not allowed"
  defp bad_request(:invalid_json), do: "invalid JSON"
  defp bad_request(:empty_array), do: "empty JSON array not allowed"

  defp bad_request(:ttl_and_expires_at),
    do: "cannot specify both Stream-TTL and Stream-Expires-At"

  defp bad_request(:new_epoch_must_start_at_zero), do: "new epoch must start at sequence 0"
  defp bad_request(:fork_offset_beyond_source), do: "fork offset beyond source stream length"
  defp bad_request(:fork_offset_trimmed), do: "fork offset is before the source's trim point"
  defp bad_request(:invalid_fork_sub_offset), do: "fork sub-offset overshoots or is invalid"
  defp bad_request(:trim_offset_beyond_tail), do: "trim offset beyond the tail"
  defp bad_request(reason) when is_binary(reason), do: reason
  defp bad_request(reason), do: "bad request: #{inspect(reason)}"

  defp text(conn, status, message) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(status, message)
  end
end
