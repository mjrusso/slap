defmodule Slap.Dev.ServerTest do
  # Not async: the server's clusters are named processes.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Slap.Bench.RawHTTP

  test "serves the Durable Streams protocol over HTTP" do
    port = start_server(store: :memory, streams: [shards: 2])
    {:ok, conn} = RawHTTP.connect(port)

    assert {:ok, 201, _, _} =
             RawHTTP.request(conn, "PUT", "/v1/stream/hello", [{"content-type", "text/plain"}])

    assert {:ok, status, _, _} =
             RawHTTP.request(
               conn,
               "POST",
               "/v1/stream/hello",
               [{"content-type", "text/plain"}],
               "hi"
             )

    assert status in 200..299
    assert {:ok, 200, headers, "hi"} = RawHTTP.request(conn, "GET", "/v1/stream/hello?offset=-1")
    assert RawHTTP.header(headers, "stream-up-to-date") == "true"
    RawHTTP.close(conn)
  end

  test "passes :http options to the router" do
    port = start_server(store: :memory, streams: [shards: 1, http: [long_poll_timeout: 50]])
    {:ok, conn} = RawHTTP.connect(port)

    {:ok, 201, headers, _} =
      RawHTTP.request(conn, "PUT", "/v1/stream/idle", [{"content-type", "text/plain"}])

    offset = RawHTTP.header(headers, "stream-next-offset")

    assert {:ok, 204, _, _} =
             RawHTTP.request(conn, "GET", "/v1/stream/idle?offset=#{offset}&live=long-poll")

    RawHTTP.close(conn)
  end

  test "passes Streams child options to the cluster" do
    assert fork_status([]) == 201
    assert fork_status(max_fork_depth: 0) == 400
  end

  test "serves KV under /v1/kv next to the streams, when configured" do
    port = start_server(store: :memory, streams: [shards: 1], kv: [shards: 2])
    {:ok, conn} = RawHTTP.connect(port)

    assert {:ok, 204, headers, _} =
             RawHTTP.request(conn, "PUT", "/v1/kv/p/k", [{"if-none-match", "*"}], "v1")

    etag = RawHTTP.header(headers, "etag")
    assert {:ok, 200, headers, "v1"} = RawHTTP.request(conn, "GET", "/v1/kv/p/k")
    assert RawHTTP.header(headers, "etag") == etag

    assert {:ok, 412, _, _} =
             RawHTTP.request(conn, "PUT", "/v1/kv/p/k", [{"if-none-match", "*"}], "v2")

    assert {:ok, 201, _, _} =
             RawHTTP.request(conn, "PUT", "/v1/stream/s", [{"content-type", "text/plain"}])

    RawHTTP.close(conn)
  end

  test "serves atomic KV batches with owner-enforced limits" do
    port =
      start_server(
        store: :memory,
        kv: [
          shards: 1,
          child_options: [
            max_batch_ops: 2,
            max_batch_bytes: 32,
            max_inflight_bytes_per_writer: 64,
            max_inflight_bytes_per_shard: 128,
            max_inflight_requests_per_writer: 2
          ]
        ]
      )

    {:ok, conn} = RawHTTP.connect(port)
    body = ~s({"ops":[{"op":"put","key":"YQ","value":"MQ"},{"op":"put","key":"Yg","value":"Mg"}]})

    assert {:ok, 200, headers, response} =
             RawHTTP.request(
               conn,
               "POST",
               "/v1/kv/p",
               [{"content-type", "application/json"}],
               body
             )

    assert %{"version" => version} = JSON.decode!(response)
    assert RawHTTP.header(headers, "etag") == ~s("#{version}")
    assert {:ok, 200, _, "1"} = RawHTTP.request(conn, "GET", "/v1/kv/p/a")

    oversized =
      ~s({"ops":[{"op":"delete","key":"YQ"},{"op":"delete","key":"Yg"},{"op":"delete","key":"Yw"}]})

    assert {:ok, 413, _, _} =
             RawHTTP.request(
               conn,
               "POST",
               "/v1/kv/p",
               [{"content-type", "application/json"}],
               oversized
             )

    RawHTTP.close(conn)
  end

  test "without :kv, /v1/kv is not found" do
    port = start_server(store: :memory, streams: [shards: 1])
    {:ok, conn} = RawHTTP.connect(port)
    assert {:ok, 404, _, _} = RawHTTP.request(conn, "GET", "/v1/kv/p/k")
    RawHTTP.close(conn)
  end

  test "serves KV alone without a Streams route" do
    port = start_server(store: :memory, kv: [shards: 1])
    {:ok, conn} = RawHTTP.connect(port)

    assert {:ok, 204, _, _} =
             RawHTTP.request(conn, "PUT", "/v1/kv/p/k", [{"if-none-match", "*"}], "value")

    assert {:ok, 200, _, "value"} = RawHTTP.request(conn, "GET", "/v1/kv/p/k")
    assert {:ok, 404, _, _} = RawHTTP.request(conn, "GET", "/v1/stream/s")
    assert {:ok, 404, _, _} = RawHTTP.request(conn, "OPTIONS", "/v1/stream/s")
    RawHTTP.close(conn)
  end

  test "answers /health" do
    port = start_server(store: :memory, kv: [shards: 1])
    {:ok, conn} = RawHTTP.connect(port)
    assert {:ok, 200, _, "ok"} = RawHTTP.request(conn, "GET", "/health")
    RawHTTP.close(conn)
  end

  test "listens on loopback by default" do
    server = start_supervised!({Slap.Dev.Server, store: :memory, kv: [shards: 1], port: 0})
    assert {{127, 0, 0, 1}, _port} = listener_address(server)
  end

  test "rejects unknown options when building the child spec" do
    assert_raise ArgumentError, ~r/prot/, fn ->
      Slap.Dev.Server.child_spec(store: :memory, kv: [], prot: 4437)
    end
  end

  test "requires at least one service" do
    assert_raise Mix.Error, ~r/select --streams, --kv, or both/, fn ->
      Mix.Tasks.Slap.Server.run([])
    end
  end

  test "CLI rejects placement flags that would be ignored and port zero" do
    assert_raise Mix.Error, ~r/--lease-ttl requires/, fn ->
      Mix.Tasks.Slap.Server.run(["--kv", "--lease-ttl", "5000"])
    end

    assert_raise Mix.Error, ~r/--static-nodes requires/, fn ->
      Mix.Tasks.Slap.Server.run(["--kv", "--static-nodes", "node@host"])
    end

    assert_raise Mix.Error, ~r/--port/, fn ->
      Mix.Tasks.Slap.Server.run(["--kv", "--port", "0"])
    end

    assert_raise Mix.Error, ~r/--peers requires/, fn ->
      Mix.Tasks.Slap.Server.run(["--kv", "--store", "memory", "--peers", "n@host"])
    end

    assert_raise Mix.Error, ~r/object-lease requires --store s3:URL/, fn ->
      Mix.Tasks.Slap.Server.run([
        "--kv",
        "--store",
        "local:/tmp/slap",
        "--placement",
        "object-lease"
      ])
    end
  end

  test "mix task rejects positional arguments" do
    assert_raise Mix.Error, ~r/unexpected arguments: stray/, fn ->
      Mix.Tasks.Slap.Server.run(["stray", "--kv"])
    end
  end

  test "mix task reports invalid store and IP options" do
    assert_raise Mix.Error, ~r/--store is required/, fn ->
      Mix.Tasks.Slap.Server.run(["--kv"])
    end

    assert_raise Mix.Error, ~r/--store/, fn ->
      Mix.Tasks.Slap.Server.run(["--kv", "--store", "invalid"])
    end

    assert_raise Mix.Error, ~r/--ip/, fn ->
      Mix.Tasks.Slap.Server.run(["--kv", "--store", "memory", "--ip", "invalid"])
    end
  end

  test "mix task reports a server startup failure" do
    assert_raise Mix.Error, ~r/server failed to start: :settings invalid settings/, fn ->
      Mix.Tasks.Slap.Server.run([
        "--streams",
        "--store",
        "memory",
        "--streams-flush-interval",
        "bogus"
      ])
    end
  end

  test "mix task stops the server if it cannot write the pid file" do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)

    pid_file =
      Path.join([System.tmp_dir!(), "slap-missing-#{System.unique_integer([:positive])}", "pid"])

    assert_raise File.Error, fn ->
      Mix.Tasks.Slap.Server.run(~w(--kv --store memory --port #{port} --pid-file #{pid_file}))
    end

    assert Process.whereis(Slap.KV.Cluster.Supervisor) == nil
    assert {:error, :econnrefused} = :gen_tcp.connect(~c"127.0.0.1", port, [])
  end

  test "a task server stops with the slap application" do
    {:ok, server} =
      DynamicSupervisor.start_child(
        Slap.Dev.ServerSupervisor,
        {Slap.Dev.Server, [store: :memory, kv: [], port: 0]}
      )

    on_exit(fn ->
      Application.stop(:slap)
      Application.ensure_all_started(:slap)
    end)

    monitor = Process.monitor(server)

    assert :ok = Application.stop(:slap)
    assert_receive {:DOWN, ^monitor, :process, ^server, :shutdown}
  end

  defp fork_status(child_options) do
    port = start_server(store: :memory, streams: [shards: 1, child_options: child_options])
    {:ok, conn} = RawHTTP.connect(port)
    text = [{"content-type", "text/plain"}]

    {:ok, 201, _, _} = RawHTTP.request(conn, "PUT", "/v1/stream/source", text)
    {:ok, status, _, _} = RawHTTP.request(conn, "POST", "/v1/stream/source", text, "hi")
    assert status in 200..299

    fork = [{"stream-forked-from", "/v1/stream/source"} | text]
    {:ok, status, _, _} = RawHTTP.request(conn, "PUT", "/v1/stream/fork", fork)
    RawHTTP.close(conn)
    :ok = stop_supervised(Slap.Dev.Server)
    status
  end

  defp start_server(config) do
    server = start_supervised!({Slap.Dev.Server, [port: 0] ++ config})
    {_ip, port} = listener_address(server)
    port
  end

  defp listener_address(server) do
    {_id, listener, _type, _modules} =
      Enum.find(Supervisor.which_children(server), fn {id, _, _, _} ->
        id == {Slap.Dev.Server, :listener}
      end)

    {:ok, address} = ThousandIsland.listener_info(listener)
    address
  end
end
