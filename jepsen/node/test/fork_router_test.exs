defmodule JepsenNode.ForkRouterTest do
  # The Streams cluster is registered under one node-wide name.
  use ExUnit.Case, async: false

  alias JepsenNode.ForkRouter

  setup context do
    store =
      if context[:local_store] do
        dir =
          Path.join(System.tmp_dir!(), "slap-fork-router-#{System.unique_integer([:positive])}")

        on_exit(fn -> File.rm_rf!(dir) end)
        {:local, dir}
      else
        :memory
      end

    start_supervised!(
      {Slap.Streams.Cluster, store: store, shards: 4, settings: %{manifest_poll_interval: "1h"}}
    )

    :ok
  end

  test "cross-shard references and manual copies keep their captured prefix" do
    {:ok, plan} = ForkRouter.operation(%{"action" => "plan", "id" => "test"})
    [source, fork, copy | _] = plan.paths
    assert Enum.all?(Enum.chunk_every(plan.shards, 2, 1, :discard), fn [a, b] -> a != b end)
    {:ok, %{sid: source_sid, shard: source_shard}} = op("create", source, %{"value" => 1})
    {:ok, %{next_offset: 10}} = op("append", source, %{"value" => 2, "id" => "append-2"})

    {:ok, %{captured_offset: 10, inherited_sid: ^source_sid, sid: fork_sid, shard: fork_shard}} =
      op("fork", fork, %{"source" => source, "at" => 10})

    {:ok, %{depth: 0, captured_offset: 10, inherited_sid: ^fork_sid}} =
      op("copy", copy, %{"source" => fork})

    {:ok, _} = op("append", source, %{"value" => 3, "id" => "append-3"})
    {:ok, _} = op("delete", source)
    {:ok, %{messages: [[0, 1], [5, 2]]}} = op("read", fork)
    {:ok, _} = op("delete", fork)

    assert {:ok, %{outcome: :exists, captured_offset: 10, inherited_sid: ^fork_sid}} =
             op("copy", copy, %{"source" => fork})

    sources = [
      %{"path" => source, "sid" => source_sid, "shard" => source_shard},
      %{"path" => fork, "sid" => fork_sid, "shard" => fork_shard}
    ]

    assert {:ok, %{reclaimed: ^sources}} =
             ForkRouter.operation(%{"action" => "await-reclaimed", "sources" => sources})

    {:ok, %{messages: [[0, 1], [5, 2]]}} = op("read", copy)
    assert {:ok, %{rows: 0}} = ForkRouter.operation(%{"action" => "audit", "paths" => plan.paths})
  end

  test "append replies and page cursors encode through the HTTP adapter" do
    request = fn req ->
      conn = Plug.Test.conn(:post, "/operation", JSON.encode!(req))
      ForkRouter.call(conn, ForkRouter.init([]))
    end

    assert %{status: 200} = request.(%{"action" => "create", "path" => "/a", "value" => "a"})

    assert %{status: 200, resp_body: body} =
             request.(%{"action" => "append", "path" => "/a", "value" => "b", "id" => "b"})

    assert JSON.decode!(body)["next_offset"] == 14

    assert %{status: 200, resp_body: page} =
             request.(%{"action" => "read", "path" => "/a", "max_bytes" => 1})

    assert %{"next_offset" => 7, "up_to_date" => false} = JSON.decode!(page)
  end

  test "operation failures expose stable JSON codes" do
    {:ok, _} = op("create", "/source", %{"value" => 1})

    for {request, status, code} <- [
          {%{"action" => "read", "path" => "/missing"}, 409, "not_found"},
          {%{"action" => "fork", "source" => "/missing", "path" => "/fork"}, 409,
           "source_not_found"},
          {%{"action" => "fork", "source" => "/source", "path" => "/beyond", "at" => 99}, 409,
           "fork_offset_beyond_source"},
          {%{"action" => "await-expiry", "path" => "/source"}, 503, "unavailable"}
        ] do
      conn = Plug.Test.conn(:post, "/operation", JSON.encode!(request))
      response = ForkRouter.call(conn, ForkRouter.init([]))
      assert response.status == status
      assert JSON.decode!(response.resp_body) == %{"error" => %{"code" => code}}
    end
  end

  test "cleanup shares one budget across stalled shard drains" do
    jobs =
      for shard <- Slap.Streams.Cluster.local_shards() do
        job = GenServer.whereis(Slap.Cluster.via(Slap.Streams.Cluster, shard, :repair))
        :ok = :sys.suspend(job)
        job
      end

    task =
      Task.async(fn ->
        receive do
          :go -> ForkRouter.operation(%{"action" => "audit", "paths" => [], "budget_ms" => 500})
        end
      end)

    Code.ensure_loaded!(ForkRouter)
    :erlang.trace_pattern({ForkRouter, :drain, 3}, true, [:local])
    :erlang.trace(task.pid, true, [:call, :set_on_spawn])
    send(task.pid, :go)

    try do
      workers =
        for _ <- jobs do
          assert_receive {:trace, pid, :call, {ForkRouter, :drain, _}}, 1_000
          Process.monitor(pid)
        end

      assert {:ok, {:error, :timeout}} = Task.yield(task, 1_500)
      for monitor <- workers, do: assert_receive({:DOWN, ^monitor, _, _, _}, 1_000)
    after
      :erlang.trace_pattern({ForkRouter, :drain, 3}, false, [:local])
      for job <- jobs, do: :sys.resume(job)
      Task.shutdown(task, :brutal_kill)
    end
  end

  test "retained reference reads cannot stand in for independent copy reads" do
    {:ok, %{sid: sid, shard: shard}} = op("create", "/source", %{"value" => 1})
    {:ok, _} = op("fork", "/reference", %{"source" => "/source"})
    {:ok, _} = op("delete", "/source")
    assert {:ok, %{messages: [[0, 1]]}} = op("read", "/reference")
    sources = [%{"path" => "/source", "sid" => sid, "shard" => shard}]

    assert {:error, :timeout} =
             ForkRouter.operation(%{
               "action" => "await-reclaimed",
               "sources" => sources,
               "budget_ms" => 25
             })

    assert {:ok, %{messages: [[0, 1]]}} = op("read", "/reference")
  end

  test "cleanup preserves unexpected exceptions and their stack traces" do
    {:ok, {:local, ctx}} = Slap.Streams.Cluster.lookup(0)
    deadline = System.system_time(:millisecond) + 5_000

    assert {:error, {:audit_failed, diagnostic}} =
             ForkRouter.drain(%{ctx | db: nil}, nil, deadline)

    assert diagnostic =~ "FunctionClauseError"
    assert diagnostic =~ "Slap.SlateDB.flush"
    assert diagnostic =~ "fork_router.ex:"
  end

  test "cleanup reports closed storage and stopped jobs as unavailable" do
    {:ok, {:local, ctx}} = Slap.Streams.Cluster.lookup(0)
    {:ok, db} = Slap.SlateDB.open("closed-audit", store: :memory)
    :ok = Slap.SlateDB.close(db)
    deadline = System.system_time(:millisecond) + 5_000

    assert {:error, :unavailable} = ForkRouter.drain(%{ctx | db: db}, nil, deadline)
    assert {:error, :unavailable} = ForkRouter.drain(%{ctx | n: 1000}, nil, deadline)
  end

  for scope <- [:ancestors, :whole_store] do
    @scope scope

    test "#{scope} audit rejects unknown and malformed storage keys with diagnostics" do
      {:ok, {:local, ctx}} = Slap.Streams.Cluster.lookup(0)
      sources = [%{"path" => "/source", "sid" => 1, "shard" => 0}]

      request =
        case @scope do
          :ancestors -> %{"action" => "await-reclaimed", "sources" => sources}
          :whole_store -> %{"action" => "audit", "paths" => []}
        end

      for key <- [<<0x0F, 1::64>>, <<0x00, "unknown">>, Slap.Streams.Store.Keys.tail(1) <> <<0>>] do
        assert {:ok, _} = Slap.SlateDB.put(ctx.db, key, "unknown", await_durable: true)
        conn = Plug.Test.conn(:post, "/operation", JSON.encode!(Map.put(request, "budget_ms", 100)))
        response = ForkRouter.call(conn, ForkRouter.init([]))
        assert response.status == 500
        assert %{"error" => %{"code" => "audit_failed", "message" => diagnostic}} =
                 JSON.decode!(response.resp_body)
        assert diagnostic =~ "unrecognized stream key"
        assert diagnostic =~ "store/keys.ex:"
        assert {:ok, _} = Slap.SlateDB.delete(ctx.db, key, await_durable: true)
      end
    end

    @tag :local_store
    test "#{scope} audit rejects an empty view from a former owner" do
      shard = 0
      {:ok, {:local, ctx}} = Slap.Streams.Cluster.lookup(shard)
      config = Slap.Cluster.Config.get(Slap.Streams.Cluster)

      {:ok, newer} =
        Slap.SlateDB.open(Slap.Cluster.Config.shard_path(config, shard),
          store: config.store,
          settings: %{manifest_poll_interval: "1h"}
        )

      on_exit(fn -> Slap.SlateDB.close(newer) end)
      key = Slap.Streams.Store.Keys.msg(1, 0, 0)
      assert {:ok, _} = Slap.SlateDB.put(newer, key, "retained", await_durable: true)
      assert {:ok, nil} = Slap.SlateDB.get(ctx.db, key, durability: :remote)

      sources =
        case @scope do
          :ancestors -> [%{"path" => "/source", "sid" => 1, "shard" => shard}]
          :whole_store -> nil
        end

      deadline = System.system_time(:millisecond) + 5_000
      assert {:error, :unavailable} = ForkRouter.drain(ctx, sources, deadline)
      assert {:ok, "retained"} = Slap.SlateDB.get(newer, key, durability: :remote)
    end
  end

  defp op(action, path, rest \\ %{}),
    do: ForkRouter.operation(Map.merge(rest, %{"action" => action, "path" => path}))
end
