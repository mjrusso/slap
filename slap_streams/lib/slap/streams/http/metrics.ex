defmodule Slap.Streams.HTTP.Metrics do
  @moduledoc """
  Serves `Slap.Streams.Metrics` at `GET /metrics`, for Prometheus: a Plug to mount
  where Prometheus can reach it and the public cannot. It does not
  authenticate.
  """

  @behaviour Plug
  import Plug.Conn

  alias Slap.Streams

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{method: "GET", request_path: "/metrics"} = conn, _opts) do
    conn
    |> put_resp_content_type("text/plain; version=0.0.4")
    |> send_resp(200, Streams.Metrics.render())
  end

  def call(conn, _opts), do: send_resp(conn, 404, "not found")
end
