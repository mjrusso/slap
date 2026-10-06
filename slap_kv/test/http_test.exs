defmodule Slap.KV.HTTPTest.Cluster do
  use Slap.KV.Cluster, otp_app: :slap_kv
end

defmodule Slap.KV.HTTPTest do
  use Slap.KV.Test.ClusterCase, async: false

  import Plug.Conn
  import Plug.Test

  alias Slap.KV.HTTP.Router

  @opts Router.init(max_value: 16)

  defp request(method, path, body \\ "", headers \\ []) do
    conn(method, path, body)
    |> then(fn conn ->
      Enum.reduce(headers, conn, fn {k, v}, c -> put_req_header(c, k, v) end)
    end)
    |> Router.call(@opts)
  end

  defp etag(conn), do: conn |> get_resp_header("etag") |> List.first()

  test "put, get, head and delete a row" do
    conn = request(:put, "/v1/kv/p/k", "value")
    assert conn.status == 204
    etag = etag(conn)
    assert etag =~ ~r/^"\d+"$/

    conn = request(:get, "/v1/kv/p/k")
    assert conn.status == 200
    assert conn.resp_body == "value"
    assert etag(conn) == etag
    assert get_resp_header(conn, "content-type") == ["application/octet-stream"]

    conn = request(:head, "/v1/kv/p/k")
    assert {conn.status, conn.resp_body, etag(conn)} == {200, "", etag}

    assert request(:delete, "/v1/kv/p/k").status == 204
    assert request(:get, "/v1/kv/p/k").status == 404
  end

  test "router serves its configured cluster" do
    cluster = __MODULE__.Cluster
    start_supervised!({cluster, store: :memory, shards: 1})
    opts = Router.init(cluster: cluster)
    path = "/v1/kv/other/key"

    assert Router.call(conn(:put, path, "value"), opts).status == 204
    assert {:ok, nil} = KV.get("other", "key")
    assert {:ok, %{value: "value"}} = KV.get("other", "key", cluster: cluster)
    assert Router.call(conn(:delete, path), opts).status == 204
  end

  test "router works under Plug.forward" do
    path = "/mounted/v1/kv/forwarded/key"

    conn =
      Plug.forward(
        conn(:put, path, "value"),
        ["v1", "kv", "forwarded", "key"],
        Router,
        Router.init([])
      )

    assert conn.status == 204
    assert {:ok, %{value: "value"}} = KV.get("forwarded", "key")
  end

  test "segments are percent-decoded, so keys may contain a slash" do
    assert request(:put, "/v1/kv/my%20p/a%2Fb", "x").status == 204
    assert {:ok, %{value: "x"}} = KV.get("my p", "a/b")
  end

  test "If-None-Match: * creates only a row that does not exist" do
    assert request(:put, "/v1/kv/p/k", "a", [{"if-none-match", "*"}]).status == 204
    conn = request(:put, "/v1/kv/p/k", "b", [{"if-none-match", "*"}])
    assert conn.status == 412
    assert {:ok, %{value: "a", version: version}} = KV.get("p", "k")
    assert etag(conn) == ~s("#{version}")
  end

  test "If-Match writes and deletes only over that version" do
    etag = etag(request(:put, "/v1/kv/p/k", "a"))
    new = etag(request(:put, "/v1/kv/p/k", "b", [{"if-match", etag}]))
    assert new != etag

    conn = request(:put, "/v1/kv/p/k", "c", [{"if-match", etag}])
    assert {conn.status, etag(conn)} == {412, new}
    assert request(:delete, "/v1/kv/p/k", "", [{"if-match", etag}]).status == 412
    assert request(:delete, "/v1/kv/p/k", "", [{"if-match", new}]).status == 204

    conn = request(:put, "/v1/kv/p/k", "d", [{"if-match", new}])
    assert {conn.status, etag(conn)} == {412, nil}
  end

  test "invalid conditions and oversized values are rejected" do
    assert request(:put, "/v1/kv/p/k", "v", [{"if-match", "12"}]).status == 400
    assert request(:put, "/v1/kv/p/k", "v", [{"if-none-match", "\"1\""}]).status == 400
    assert request(:delete, "/v1/kv/p/k", "", [{"if-none-match", "*"}]).status == 400
    assert request(:put, "/v1/kv/p/k", String.duplicate("x", 17)).status == 413
    assert request(:put, "/v1/kv/p/k", String.duplicate("x", 16)).status == 204
  end

  test "scans page through a partition as JSON" do
    for k <- ~w(a b c), do: {:ok, _} = KV.put("s", "item/#{k}", "v#{k}")
    {:ok, _} = KV.put("s", "other", "o")

    conn = request(:get, "/v1/kv/s?prefix=item%2F&limit=2")
    assert conn.status == 200
    assert %{"rows" => rows, "cursor" => cursor} = JSON.decode!(conn.resp_body)
    assert decode(rows) == [{"item/a", "va"}, {"item/b", "vb"}]

    conn = request(:get, "/v1/kv/s?prefix=item%2F&limit=2&cursor=#{cursor}")
    assert %{"rows" => rows, "cursor" => nil} = JSON.decode!(conn.resp_body)
    assert decode(rows) == [{"item/c", "vc"}]

    assert request(:get, "/v1/kv/s?limit=x").status == 400
    assert request(:get, "/v1/kv/s?cursor=!").status == 400
    assert request(:get, "/v1/kv/s?limit[x]=1").status == 400
    assert request(:get, "/v1/kv/s?cursor[]=a").status == 400
    assert request(:get, "/v1/kv/s?prefix[x]=a").status == 400
  end

  test "paths outside the prefix are 404, and other methods 405" do
    assert request(:get, "/v1/stream/x").status == 404
    assert request(:get, "/v1/kv/p/k/extra").status == 404
    assert request(:post, "/v1/kv/p/k", "v").status == 405
    assert request(:put, "/v1/kv/p", "v").status == 405
  end

  test "an unavailable partition writer is 503 with Retry-After" do
    {supervisor, id} = partition_writer_child("p")
    :ok = Supervisor.terminate_child(supervisor, id)

    conn = request(:put, "/v1/kv/p/k", "v")
    assert conn.status == 503
    assert get_resp_header(conn, "retry-after") == ["1"]
  end

  defp decode(rows) do
    for %{"key" => k, "value" => v} <- rows,
        do: {Base.url_decode64!(k, padding: false), Base.url_decode64!(v, padding: false)}
  end
end
