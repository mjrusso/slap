defmodule JepsenNode.FilesRouterTest do
  use ExUnit.Case, async: false

  alias JepsenNode.{FilesRouter, Readiness, Stats}
  alias Slap.Files
  alias Slap.Files.{Config, Intent, Record}

  setup context do
    dir = Path.join(System.tmp_dir!(), "jepsen-node-test-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    start_supervised!(
      {Slap.KV.Cluster,
       store: {:local, dir}, path: "kv", shards: 2, settings: %{flush_interval: "2ms"}}
    )

    start_supervised!(
      {Files,
       store: context[:files_store] || {:local, dir},
       inline_max_bytes: 16,
       upload_timeout_ms: context[:upload_timeout_ms] || 1,
       max_clock_skew_ms: 0,
       sweep_interval_ms: :timer.hours(1),
       reconcile_interval_ms: :timer.hours(1)}
    )

    FilesRouter.setup([node()])
    {:ok, dir: dir}
  end

  test "inline files do not count as missing objects" do
    assert {:ok, _} = Files.put({"jepsen-0", "k0"}, "inline")

    config = Config.get()

    assert {:ok, %{rows: [{"k0", _version, %{body: {:inline, "inline"}}}]}} =
             Record.list("jepsen-0", Config.route_opts(config), config)

    conn =
      :post
      |> Plug.Test.conn("/audit")
      |> FilesRouter.call(FilesRouter.init([]))

    assert conn.status == 200
    assert %{"quiescent" => true, "dangling" => []} = JSON.decode!(conn.resp_body)
  end

  test "readiness requires all stream and KV shards", %{dir: dir} do
    Readiness.setup([node()])
    refute Readiness.ready?()

    start_supervised!(
      {Slap.Streams.Cluster,
       store: {:local, dir}, path: "streams", shards: 2, settings: %{flush_interval: "2ms"}}
    )

    assert Readiness.ready?()
  end

  @tag upload_timeout_ms: 10_000
  test "successful puts count their actual storage mode" do
    before = Stats.local()

    for {key, body} <- [{"0", "a"}, {"1", String.duplicate("b", 100)}] do
      conn =
        :put
        |> Plug.Test.conn("/files/#{key}", body)
        |> FilesRouter.call(FilesRouter.init([]))

      assert conn.status == 204
    end

    after_counts = Stats.local()
    assert after_counts.files_inline_writes == before.files_inline_writes + 1
    assert after_counts.files_object_writes == before.files_object_writes + 1
  end

  test "distinct create-only requests with the same body do not both succeed" do
    put = fn id ->
      :put
      |> Plug.Test.conn("/files/0", "a")
      |> Plug.Conn.put_req_header("if-none-match", "*")
      |> Plug.Conn.put_req_header("x-jepsen-write-id", id)
      |> FilesRouter.call(FilesRouter.init([]))
    end

    assert put.("first").status == 204
    assert put.("second").status == 412
  end

  test "an unavailable intent partition makes the audit unknown" do
    partition = Intent.partition(0, Config.get())
    {:ok, {:local, ctx}} = Slap.KV.Cluster.lookup(Slap.KV.Cluster.shard_for(partition))
    {:ok, index} = Slap.KV.PartitionWriter.index(ctx, partition)
    [{writer, _}] = Registry.lookup(ctx.registry, {ctx.n, {:partition_writer, index}})

    :ok = :sys.suspend(writer)

    try do
      deadline = System.monotonic_time(:millisecond) + 250
      assert %{quiescent: false, reason: reason} = FilesRouter.audit(deadline)
      assert reason =~ "intent_barrier"
    after
      :ok = :sys.resume(writer)
    end

    conn =
      :put
      |> Plug.Test.conn("/files/0", "a")
      |> FilesRouter.call(FilesRouter.init([]))

    assert conn.status == 204
  end

  @tag files_store:
         {:url, "s3://unreachable",
          aws_endpoint: "http://127.0.0.1:1",
          aws_allow_http: true,
          aws_region: "us-east-1",
          aws_access_key_id: "x",
          aws_secret_access_key: "x"}
  test "observation respects its deadline" do
    FilesRouter.setup([])
    deadline = System.monotonic_time(:millisecond) + 1_500
    assert %{quiescent: false, reason: ":out_of_time"} = FilesRouter.audit(deadline)
  end

end
