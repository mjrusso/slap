defmodule Slap.Router do
  @moduledoc """
  The server's HTTP entry point for the configured services.

  Options: `:streams` and `:kv` contain each service router's options,
  or nil when that service is disabled.

  `GET /health` answers 200 once the listener is up. It does not check the
  services' shards.
  """

  @behaviour Plug

  alias Slap.KV
  alias Slap.Streams

  @impl true
  def init(opts) do
    %{
      streams: if(opts[:streams] != nil, do: Streams.HTTP.Router.init(opts[:streams])),
      kv: if(opts[:kv] != nil, do: KV.HTTP.Router.init(opts[:kv]))
    }
  end

  @impl true
  def call(%Plug.Conn{method: "GET", request_path: "/health"} = conn, _routes) do
    Plug.Conn.send_resp(conn, 200, "ok")
  end

  def call(conn, %{streams: streams, kv: kv}) do
    cond do
      kv != nil and String.starts_with?(conn.request_path, kv.prefix <> "/") ->
        KV.HTTP.Router.call(conn, kv)

      streams != nil and String.starts_with?(conn.request_path, streams.prefix <> "/") ->
        Streams.HTTP.Router.call(conn, streams)

      true ->
        Plug.Conn.send_resp(conn, 404, "not found")
    end
  end
end
