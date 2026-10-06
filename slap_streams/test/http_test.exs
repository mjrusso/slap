defmodule Slap.Streams.HTTPTest.Cluster do
  use Slap.Streams.Cluster, otp_app: :slap_streams
end

defmodule Slap.Streams.HTTPTest do
  # The HTTP layer through Plug.Test. The official Durable Streams
  # conformance suite (slap/durable_streams/) covers the protocol in depth;
  # these check the mapping.
  use Slap.Streams.Test.ClusterCase, async: false
  import Plug.Test
  import Plug.Conn

  alias Slap.Streams.HTTP.{Errors, Router}
  alias Slap.Streams.Offset

  @moduletag :capture_log

  @opts Router.init(long_poll_timeout: 200, sse_timeout: 300)

  defp request(method, path, body \\ "", headers \\ []) do
    conn = conn(method, path, body)
    conn = Enum.reduce(headers, conn, fn {k, v}, c -> put_req_header(c, k, v) end)
    Router.call(conn, @opts)
  end

  defp h(conn, name), do: conn |> get_resp_header(name) |> List.first()

  test "OPTIONS only answers stream paths" do
    assert request(:options, "/v1/stream/a").status == 204
    assert request(:options, "/outside/a").status == 404
    assert request(:options, "/v1/streaming/a").status == 404
  end

  test "CORS headers use the configured policy" do
    assert h(request(:options, "/v1/stream/a"), "access-control-allow-origin") == "*"

    opts = Router.init(cors: "https://app.example")
    conn = Router.call(conn(:options, "/v1/stream/a"), opts)
    assert h(conn, "access-control-allow-origin") == "https://app.example"

    opts = Router.init(cors: false)
    conn = Router.call(conn(:options, "/v1/stream/a"), opts)
    assert get_resp_header(conn, "access-control-allow-origin") == []
    assert get_resp_header(conn, "access-control-allow-methods") == []
    assert h(conn, "x-content-type-options") == "nosniff"

    assert_raise ArgumentError, ~r/:cors/, fn -> Router.init(cors: nil) end
  end

  test "create, append, read, head and delete" do
    conn = request(:put, "/v1/stream/a", "hi", [{"content-type", "text/plain"}])
    assert conn.status == 201
    assert h(conn, "location") == "http://www.example.com/v1/stream/a"
    assert h(conn, "stream-next-offset") == "0000000000000000_0000000000000006"
    assert h(conn, "x-content-type-options") == "nosniff"

    assert request(:put, "/v1/stream/a", "", [{"content-type", "text/plain"}]).status == 200
    assert request(:put, "/v1/stream/a", "", [{"content-type", "a/b"}]).status == 409

    conn = request(:post, "/v1/stream/a", " there", [{"content-type", "text/plain"}])
    assert conn.status == 204
    assert h(conn, "stream-next-offset") == "0000000000000000_0000000000000016"

    conn = request(:get, "/v1/stream/a?offset=-1")
    assert conn.status == 200
    assert conn.resp_body == "hi there"
    assert h(conn, "stream-up-to-date") == "true"
    assert h(conn, "content-type") == "text/plain"
    etag = h(conn, "etag")

    conn = request(:get, "/v1/stream/a?offset=-1", "", [{"if-none-match", etag}])
    assert conn.status == 304

    conn = request(:head, "/v1/stream/a")
    assert conn.status == 200
    assert h(conn, "cache-control") == "no-store"

    assert request(:delete, "/v1/stream/a").status == 204
    assert request(:get, "/v1/stream/a").status == 404
  end

  test "router serves its configured cluster" do
    cluster = __MODULE__.Cluster
    start_supervised!({cluster, store: :memory, shards: 1})
    opts = Router.init(cluster: cluster)
    path = "/v1/stream/other"

    assert Router.call(conn(:put, path, "") |> put_req_header("content-type", "text/plain"), opts).status ==
             201

    assert {:error, :not_found} = Streams.head(path)
    assert {:ok, _} = Streams.head(path, cluster: cluster)
    assert Router.call(conn(:delete, path), opts).status == 204
  end

  test "router works under Plug.forward with the full request path as the stream name" do
    path = "/mounted/v1/stream/forwarded"

    conn =
      conn(:put, path, "")
      |> put_req_header("content-type", "text/plain")
      |> Plug.forward(["v1", "stream", "forwarded"], Router, Router.init([]))

    assert conn.status == 201
    assert {:ok, _} = Streams.head(path)
    assert {:error, :not_found} = Streams.head("/forwarded")
  end

  test "an unexpected error is a logged 500 with a body" do
    conn = Errors.send_error(conn(:get, "/v1/stream/x"), {:something, :new})
    assert conn.status == 500
    assert conn.resp_body == "internal error"
  end

  test "offset -1 reads from the earliest data left after a trim; an explicit 0 gets 410" do
    request(:put, "/v1/stream/tr", "", [{"content-type", "text/plain"}])

    for m <- ["one", "two", "three"],
        do: request(:post, "/v1/stream/tr", m, [{"content-type", "text/plain"}])

    :ok = Streams.trim("/v1/stream/tr", 7)

    conn = request(:get, "/v1/stream/tr?offset=-1")
    assert conn.status == 200
    assert conn.resp_body == "twothree"
    assert request(:get, "/v1/stream/tr?offset=#{Offset.encode(0)}").status == 410
  end

  test "X-Forwarded-* only counts when trusted" do
    headers = [{"x-forwarded-host", "evil.example"}, {"x-forwarded-proto", "https"}]
    conn = request(:put, "/v1/stream/xf", "", headers)
    refute h(conn, "location") =~ "evil.example"

    opts = Router.init(trust_forwarded: true)

    conn =
      Enum.reduce(headers, conn(:put, "/v1/stream/xf2", ""), fn {k, v}, c ->
        put_req_header(c, k, v)
      end)

    assert h(Router.call(conn, opts), "location") == "https://evil.example/v1/stream/xf2"
  end

  test "request bodies: early 413 and the node's buffer budget" do
    request(:put, "/v1/stream/b", "", [{"content-type", "text/plain"}])

    # A declared length over max_body is refused before reading.
    opts = Router.init(max_body: 10)

    conn =
      conn(:post, "/v1/stream/b", "x")
      |> put_req_header("content-type", "text/plain")
      |> put_req_header("content-length", "11")

    assert Router.call(conn, opts).status == 413

    # Over the node's budget for bodies being read: 503, retryable.
    opts = Router.init(max_buffered: 10)

    conn =
      conn(:post, "/v1/stream/b", "0123456789ab")
      |> put_req_header("content-type", "text/plain")
      |> put_req_header("content-length", "12")

    conn = Router.call(conn, opts)
    assert conn.status == 503 and h(conn, "retry-after") == "1"

    for _ <- 1..3 do
      conn = conn(:post, "/v1/stream/b", "0123") |> put_req_header("content-type", "text/plain")
      assert Router.call(conn, opts).status in [200, 204]
    end
  end

  test "request validation" do
    request(:put, "/v1/stream/v", "", [{"content-type", "text/plain"}])

    assert request(:put, "/v1/stream/x", "", [{"stream-ttl", "03"}]).status == 400

    assert request(:put, "/v1/stream/x", "", [
             {"stream-ttl", "3"},
             {"stream-expires-at", "2030-01-01T00:00:00Z"}
           ]).status == 400

    assert request(:post, "/v1/stream/v", "").status == 400
    assert request(:post, "/v1/stream/v", "x").status == 400
    assert request(:post, "/v1/stream/missing", "").status == 404

    assert request(:post, "/v1/stream/v", "x", [
             {"content-type", "text/plain"},
             {"producer-id", "p"}
           ]).status == 400

    assert request(:get, "/v1/stream/v?offset=bad").status == 400
    assert request(:get, "/v1/stream/v?offset=-1&offset=-1").status == 400
    assert request(:get, "/v1/stream/v?live=long-poll").status == 400
    assert request(:patch, "/v1/stream/v").status == 405
    assert request(:get, "/elsewhere").status == 404
  end

  test "producers: 200 for new data, 204 for duplicates, 403 for stale epochs" do
    request(:put, "/v1/stream/p", "", [{"content-type", "text/plain"}])

    post = fn epoch, seq ->
      request(:post, "/v1/stream/p", "x", [
        {"content-type", "text/plain"},
        {"producer-id", "w"},
        {"producer-epoch", "#{epoch}"},
        {"producer-seq", "#{seq}"}
      ])
    end

    conn = post.(0, 0)
    assert conn.status == 200
    assert {h(conn, "producer-epoch"), h(conn, "producer-seq")} == {"0", "0"}
    assert post.(0, 0).status == 204
    conn = post.(0, 5)
    assert conn.status == 409
    assert h(conn, "producer-expected-seq") == "1"
    assert post.(1, 0).status == 200
    conn = post.(0, 1)
    assert conn.status == 403
    assert h(conn, "producer-epoch") == "1"
  end

  test "closing" do
    request(:put, "/v1/stream/c", "", [{"content-type", "text/plain"}])
    conn = request(:post, "/v1/stream/c", "", [{"stream-closed", "TRUE"}])
    assert conn.status == 204
    assert h(conn, "stream-closed") == "true"

    conn = request(:post, "/v1/stream/c", "x", [{"content-type", "text/plain"}])
    assert conn.status == 409
    assert h(conn, "stream-closed") == "true"

    conn = request(:get, "/v1/stream/c?offset=-1")
    assert conn.status == 200 and h(conn, "stream-closed") == "true"
  end

  test "JSON mode returns arrays" do
    json = [{"content-type", "application/json"}]
    request(:put, "/v1/stream/j", ~s([{"a":1}, 2]), json)
    assert request(:get, "/v1/stream/j?offset=-1").resp_body == ~s([{"a":1},2])
    assert request(:get, "/v1/stream/j?offset=now").resp_body == "[]"
    assert request(:post, "/v1/stream/j", "[]", json).status == 400
  end

  test "long-poll: data, a timeout, and closed" do
    request(:put, "/v1/stream/lp", "", [{"content-type", "text/plain"}])
    task = Task.async(fn -> request(:get, "/v1/stream/lp?offset=now&live=long-poll") end)
    Process.sleep(50)
    request(:post, "/v1/stream/lp", "new", [{"content-type", "text/plain"}])
    conn = Task.await(task)
    assert conn.status == 200 and conn.resp_body == "new"
    assert h(conn, "stream-cursor") != nil

    conn = request(:get, "/v1/stream/lp?offset=now&live=long-poll")
    assert conn.status == 204
    assert h(conn, "stream-up-to-date") == "true"

    request(:post, "/v1/stream/lp", "", [{"stream-closed", "true"}])
    conn = request(:get, "/v1/stream/lp?offset=now&live=long-poll")
    assert conn.status == 204 and h(conn, "stream-closed") == "true"
  end

  test "SSE: data and control events, base64 for binary streams" do
    request(:put, "/v1/stream/sse", "a\nb", [{"content-type", "text/plain"}])
    request(:post, "/v1/stream/sse", "", [{"stream-closed", "true"}])
    conn = request(:get, "/v1/stream/sse?offset=-1&live=sse")
    assert conn.status == 200
    assert h(conn, "content-type") == "text/event-stream"
    assert conn.resp_body =~ "event: data\ndata:a\ndata:b\n\n"
    assert conn.resp_body =~ ~s("streamClosed":true)

    [closing] =
      Regex.run(~r/event: control\ndata:(\{[^\n]*"streamClosed"[^\n]*\})/, conn.resp_body,
        capture: :all_but_first
      )

    assert JSON.decode!(closing)["upToDate"] == true

    request(:put, "/v1/stream/bin", <<0, 1, 2>>, [{"content-type", "application/octet-stream"}])
    conn = request(:get, "/v1/stream/bin?offset=-1&live=sse")
    assert h(conn, "stream-sse-data-encoding") == "base64"
    assert conn.resp_body =~ "data:" <> Base.encode64(<<0, 1, 2>>)
  end

  test "forks: headers, errors and soft deletes" do
    ct = [{"content-type", "text/plain"}]
    assert request(:put, "/v1/stream/src", "abc", ct).status == 201
    fork = fn path, headers -> request(:put, path, "", ct ++ headers) end
    from = {"stream-forked-from", "/v1/stream/src"}

    conn =
      fork.("/v1/stream/f", [from, {"stream-fork-offset", "0000000000000000_0000000000000000"}])

    assert conn.status == 201
    assert h(conn, "stream-next-offset") == "0000000000000000_0000000000000000"

    conn =
      fork.("/v1/stream/g", [from, {"stream-fork-offset", "-1"}, {"stream-fork-sub-offset", "2"}])

    assert conn.status == 201
    assert request(:get, "/v1/stream/g?offset=-1").resp_body == "ab"

    assert fork.("/v1/stream/x", [{"stream-fork-sub-offset", "0"}]).status == 400
    assert fork.("/v1/stream/x", [from, {"stream-fork-sub-offset", "01"}]).status == 400
    assert fork.("/v1/stream/x", [from, {"stream-fork-sub-offset", ""}]).status == 400
    assert fork.("/v1/stream/x", [from, {"stream-fork-offset", "now"}]).status == 400
    assert fork.("/v1/stream/x", [{"stream-forked-from", "/v1/stream/none"}]).status == 404
    assert request(:put, "/v1/stream/x", "", [from, {"content-type", "a/b"}]).status == 409

    assert request(:delete, "/v1/stream/src").status == 204
    assert request(:get, "/v1/stream/src").status == 410
    assert request(:head, "/v1/stream/src").status == 410
    assert request(:put, "/v1/stream/src", "", ct).status == 409
    assert fork.("/v1/stream/x", [from]).status == 409
  end

  test "reads before a trim point get 410" do
    assert request(:put, "/v1/stream/t", "abc", [{"content-type", "text/plain"}]).status == 201
    :ok = Streams.trim("/v1/stream/t", 7)
    assert request(:get, "/v1/stream/t?offset=0000000000000000_0000000000000000").status == 410
    assert request(:get, "/v1/stream/t?offset=0000000000000000_0000000000000007").status == 200
  end
end
