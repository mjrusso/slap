defmodule JepsenNode.YjsRouter do
  @moduledoc """
  The Yjs workload over HTTP:

    * `POST /yjs/:doc` with an element as the body: 200 when stored, 503
      when not sent, 504 when sent but not confirmed stored.
    * `GET /yjs/:doc`: the stored elements as a JSON array, or 503.
    * `GET /stats`: `JepsenNode.Stats.total/0`, as JSON.
    * `GET /ready`: 200 when every node agrees on every stream and KV shard
      owner; 503 while placement is incomplete.
  """

  use Plug.Router

  plug :match
  plug :dispatch

  post "/yjs/:doc" do
    {:ok, element, conn} = Plug.Conn.read_body(conn)

    case JepsenNode.Yjs.add(doc, element) do
      :ok -> send_resp(conn, 200, "")
      {:error, :unavailable} -> send_resp(conn, 503, "")
      {:error, :indeterminate} -> send_resp(conn, 504, "")
    end
  end

  get "/yjs/:doc" do
    case JepsenNode.Yjs.read(doc) do
      {:ok, elements} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, JSON.encode!(elements))

      {:error, reason} ->
        send_resp(conn, 503, inspect(reason))
    end
  end

  get "/stats" do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, JSON.encode!(JepsenNode.Stats.total()))
  end

  get "/ready" do
    if JepsenNode.Readiness.ready?(),
      do: send_resp(conn, 200, ""),
      else: send_resp(conn, 503, "cluster not ready")
  end

  match _ do
    send_resp(conn, 404, "")
  end
end
