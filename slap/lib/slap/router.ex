defmodule Slap.Router do
  @moduledoc """
  A Plug that serves the Streams and KV HTTP APIs, through
  `Slap.Streams.HTTP.Router` and `Slap.KV.HTTP.Router`:

      {Bandit, plug: {Slap.Router, streams: [], kv: []}, port: 4437, ip: :loopback}

  `:streams` and `:kv` are those routers' options. A service whose key is
  missing or nil is not served. With the default prefixes, Streams are
  under `/v1/stream/` and KV under `/v1/kv/`; other paths answer 404.

  `GET /health` answers 200. It does not check the services' shards.

  It does not authenticate requests; put your own authentication in front.
  Without `ip:`, Bandit listens on every interface.
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
