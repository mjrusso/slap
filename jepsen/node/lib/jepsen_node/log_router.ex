defmodule JepsenNode.LogRouter do
  @moduledoc """
  The snapshot log workload over HTTP:

    * `POST /logs/:key` with an integer: 200 once durable, 503 when the
      outcome is unknown.
    * `GET /logs/:key`: the list as JSON, or 503.
    * `POST /restore` with touched keys as JSON: start followers for those
      keys on every node after fault recovery.
    * `POST /check` with touched keys as JSON: each configured node checks
      its followers for those keys (`JepsenNode.Log.check/1`) and reports
      missing or lagging followers, mismatches, failed reads, unreachable
      nodes, and the counts of `JepsenNode.Stats`.
  """

  use Plug.Router

  alias JepsenNode.{Log, Stats}

  def setup(nodes), do: :persistent_term.put(__MODULE__, nodes)

  plug :match
  plug :dispatch

  post "/logs/:key" do
    {:ok, body, conn} = read_body(conn)

    case Log.append(key, String.to_integer(body)) do
      :ok -> send_resp(conn, 200, "")
      {:error, reason} -> send_resp(conn, 503, inspect(reason))
    end
  end

  get "/logs/:key" do
    case Log.read(key) do
      {:ok, list} -> json(conn, list)
      {:error, reason} -> send_resp(conn, 503, inspect(reason))
    end
  end

  post "/restore" do
    {:ok, body, conn} = read_body(conn)
    keys = JSON.decode!(body)

    {results, unreachable} =
      :rpc.multicall(:persistent_term.get(__MODULE__), Log, :restore_followers, [keys], 120_000)

    if Enum.all?(results, &(&1 == :ok)) and unreachable == [] do
      send_resp(conn, 200, "")
    else
      send_resp(conn, 503, inspect(%{results: results, unreachable: unreachable}))
    end
  end

  post "/check" do
    {:ok, body, conn} = read_body(conn)
    keys = JSON.decode!(body)

    {results, unreachable} =
      :rpc.multicall(:persistent_term.get(__MODULE__), Log, :check, [keys], 660_000)

    json(conn, %{
      nodes: length(results),
      keys: length(keys),
      followers: Enum.sum_by(results, & &1.followers),
      missing: Enum.flat_map(results, & &1.missing),
      lagging: Enum.flat_map(results, & &1.lagging),
      mismatches: Enum.flat_map(results, & &1.mismatches),
      read_errors: Enum.flat_map(results, & &1.read_errors),
      unreachable: Enum.map(unreachable, &to_string/1),
      publication_nodes:
        results
        |> Enum.flat_map(fn result -> Enum.map(result.publications, &{&1, result.node}) end)
        |> Enum.group_by(fn {key, _node} -> key end, fn {_key, node} -> node end)
        |> Map.new(fn {key, nodes} -> {key, Enum.uniq(nodes)} end),
      stats: Stats.sum(Enum.map(results, & &1.stats))
    })
  end

  match _ do
    send_resp(conn, 404, "")
  end

  defp json(conn, value) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, JSON.encode!(value))
  end
end
