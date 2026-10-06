defmodule Slap.ServerTest do
  # Not async: the server's cluster and listener are named processes.
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

  test "requires at least one service" do
    assert_raise ArgumentError, ~r/configure :streams, :kv, or both/, fn ->
      Slap.Server.child_specs(store: :memory)
    end

    assert_raise Mix.Error, ~r/select --streams, --kv, or both/, fn ->
      Mix.Tasks.Slap.Server.run([])
    end
  end

  test "the listener child id is namespaced" do
    specs = Slap.Server.child_specs(store: :memory, kv: [])
    assert List.last(specs).id == {Slap.Server, :listener}
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

  test "rejects unknown server and service options" do
    assert_raise ArgumentError, ~r/shardz/, fn ->
      Slap.Server.child_specs(store: :memory, streams: [shardz: 2])
    end

    assert_raise ArgumentError, ~r/strams/, fn ->
      Slap.Server.child_specs(store: :memory, streams: [], strams: [shards: 2])
    end

    assert_raise ArgumentError, ~r/shardz/, fn ->
      Slap.Server.child_specs(store: :memory, kv: [shardz: 2])
    end

    assert_raise ArgumentError, ~r/partition_writters/, fn ->
      Slap.Server.child_specs(store: :memory, kv: [child_options: [partition_writters: 2]])
    end
  end

  test "server child spec rejects unknown options in the caller" do
    assert_raise ArgumentError, ~r/shardz/, fn ->
      Supervisor.child_spec({Slap.Server, [store: :memory, streams: [shardz: 2]]}, [])
    end
  end

  test "passes service child options to each cluster" do
    specs =
      Slap.Server.child_specs(
        store: :memory,
        streams: [child_options: [idle_timeout: 123]],
        kv: [child_options: [partition_writers: 3]]
      )

    assert {Slap.Streams.Cluster, streams_opts} = Enum.at(specs, 0)
    assert {Slap.KV.Cluster, kv_opts} = Enum.at(specs, 1)
    assert streams_opts[:child_options] == [idle_timeout: 123]
    assert kv_opts[:child_options] == [partition_writers: 3]
  end

  test "the standalone listener binds to loopback by default" do
    specs = Slap.Server.child_specs(store: :memory, kv: [])
    assert %{start: {Bandit, :start_link, [opts]}} = List.last(specs)
    assert opts[:ip] == :loopback
  end

  test "a task server stops with the slap application" do
    {:ok, server} =
      DynamicSupervisor.start_child(
        Slap.ServerSupervisor,
        {Slap.Server, [store: :memory, kv: [], port: 0]}
      )

    on_exit(fn ->
      if Process.whereis(Slap.ServerSupervisor) do
        DynamicSupervisor.terminate_child(Slap.ServerSupervisor, server)
      end

      Application.ensure_all_started(:slap)
    end)

    monitor = Process.monitor(server)

    assert :ok = Application.stop(:slap)
    assert_receive {:DOWN, ^monitor, :process, ^server, :shutdown}
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

  defp start_server(config) do
    server = start_supervised!({Slap.Server, [port: 0] ++ config})

    {_id, listener, _type, _modules} =
      Enum.find(Supervisor.which_children(server), fn {id, _, _, _} ->
        id == {Slap.Server, :listener}
      end)

    {:ok, {_ip, port}} = ThousandIsland.listener_info(listener)
    port
  end
end
