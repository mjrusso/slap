defmodule Slap.InspectStorageTest do
  # These tests share distributed Erlang, the Streams cluster name and node admission.
  use ExUnit.Case, async: false

  import Slap.Test.InspectPeers

  alias Slap.Cluster.Inspection
  alias Slap.Inspect.{Query, Remote, Requests}
  alias Slap.{SlateDB, Streams}
  alias Slap.Streams.Introspect
  alias Slap.Test.StorageProxy

  @moduletag :s3
  @moduletag :capture_log
  @moduletag timeout: 120_000

  setup do
    distribute()
    proxy = StorageProxy.start("inspect-storage")

    options = [
      store: proxy.store,
      shards: 1,
      cache: :disabled,
      settings: %{flush_interval: "2ms", manifest_poll_interval: "1ms"}
    ]

    start_supervised!({Streams.Cluster, options})
    assert :ok = Streams.Cluster.await_ready(10_000)
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(0)
    value = :crypto.strong_rand_bytes(512 * 1024)
    keys = for suffix <- ~w(a b c), do: "inspect/" <> String.duplicate("x", 4087) <> suffix

    assert {:ok, _} =
             SlateDB.write(ctx.db, Enum.map(keys, &{:put, &1, value}), await_durable: true)

    assert :ok = SlateDB.flush(ctx.db, type: :memtable)
    stop_supervised!(Streams.Cluster)
    start_supervised!({Streams.Cluster, options})
    assert :ok = Streams.Cluster.await_ready(10_000)
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(0)
    %{proxy: proxy, options: options, ctx: ctx, value: value, keys: keys}
  end

  test "inspection gets and scans exclude a write whose WAL upload is blocked", context do
    StorageProxy.hold(context.proxy, :wal)

    try do
      assert {:ok, seq} = SlateDB.put(context.ctx.db, "pending", "not durable")
      assert_receive {:storage_blocked, _, "PUT " <> _}, 5000
      assert SlateDB.durable_seq(context.ctx.db) < seq
      assert {:ok, "not durable"} = SlateDB.get(context.ctx.db, "pending")

      assert {:ok, %{row: nil, durable_seq: observed}} =
               Inspection.get(Streams.Cluster, 0, 1, 5000, key: "pending")

      assert observed < seq

      assert {:ok, %{rows: [], durable_seq: observed}} =
               Introspect.scan(Streams.Cluster, 0, 1, 5000, prefix: "pending")

      assert observed < seq
      StorageProxy.release(context.proxy)
      assert :ok = SlateDB.flush(context.ctx.db)

      assert {:ok, %{row: %{value: "not durable"}}} =
               Inspection.get(Streams.Cluster, 0, 1, 5000, key: "pending")

      assert {:ok, %{rows: [%{value: "not durable", seq: ^seq}]}} =
               Introspect.scan(Streams.Cluster, 0, 1, 5000, prefix: "pending")
    after
      StorageProxy.release(context.proxy)
    end
  end

  test "a real S3 stall reports its owner and sends only the latest waiting request", context do
    connection = %{entry: node(), cluster: Streams.Cluster}

    worker =
      start_supervised!({Requests, client: self(), read: &Query.execute(connection, &1)})

    request = %{view: :raw, shard: 0, budget: 300, opts: [prefix: "inspect/"]}
    owner = node()
    StorageProxy.hold(context.proxy, :sst)

    try do
      Requests.submit(worker, 1, request)
      assert_receive {:storage_blocked, _, "GET " <> _}, 5000
      reads = StorageProxy.reads(context.proxy)
      latest = %{request | view: :get, opts: [key: List.last(context.keys)]}
      for id <- 2..20, do: Requests.submit(worker, id, latest)
      assert_receive {:inspection_stalled, 1, ^owner, 0}, 5000
      assert StorageProxy.reads(context.proxy) == reads
      refute_received {:inspection_result, _, _}

      StorageProxy.release(context.proxy)
      assert_receive {:inspection_result, 20, {:ok, %{row: %{value: value}}}}, 10_000
      assert value == context.value
      refute_received {:inspection_result, _, _}
      assert StorageProxy.reads(context.proxy) > reads
    after
      StorageProxy.release(context.proxy)
    end
  end

  test "admission survives inspector disconnects, a second inspector and a restart", context do
    stop_supervised!(Streams.Cluster)
    {server, owner} = peer("storage-owner")
    start_remote_cluster(server, context.options)
    {client, inspector} = peer("storage-client", [~c"-hidden"])
    StorageProxy.hold(context.proxy, :sst)

    try do
      reader = Task.async(fn -> remote_read(client, owner, :scan, 300, prefix: "inspect/") end)
      assert_receive {:storage_blocked, _, "GET " <> _}, 5000
      holder = :peer.call(server, Process, :whereis, [Inspection], 5000)
      assert is_pid(holder)
      holder_ref = Process.monitor(holder)
      reads = StorageProxy.reads(context.proxy)
      Process.send_after(self(), :past_stall_margin, 2400)

      assert true = :peer.call(client, Node, :disconnect, [owner], 5000)
      assert {:error, :disconnected} = Task.await(reader, 5000)
      refute owner in :peer.call(client, Node, :list, [:connected], 5000)
      assert_receive :past_stall_margin, 5000
      assert true = :peer.call(client, :net_kernel, :hidden_connect_node, [owner], 5000)
      assert {:error, :busy} = remote_read(client, owner, :scan, 5000, prefix: "inspect/")

      {second, _} = peer("storage-second", [~c"-hidden"])
      assert {:error, :busy} = remote_read(second, owner, :scan, 5000, prefix: "inspect/")

      :peer.stop(client)
      {restarted, ^inspector} = peer(inspector, [~c"-hidden"])
      assert {:error, :busy} = remote_read(restarted, owner, :scan, 5000, prefix: "inspect/")
      assert :peer.call(server, Process, :whereis, [Inspection], 5000) == holder
      assert StorageProxy.reads(context.proxy) == reads

      StorageProxy.release(context.proxy)
      assert_receive {:DOWN, ^holder_ref, :process, ^holder, _}, 10_000

      assert {:ok, %{row: %{value: value}}} =
               remote_read(restarted, owner, :get, 5000, key: hd(context.keys))

      assert value == context.value
    after
      StorageProxy.release(context.proxy)
    end
  end

  test "a replacement shard opens within ten seconds after a maximum scan drains", context do
    StorageProxy.hold(context.proxy, :sst)

    try do
      reader =
        Task.async(fn -> Introspect.scan(Streams.Cluster, 0, 1, 30_000, maximum_scan()) end)

      assert_receive {:storage_blocked, _, "GET " <> _}, 5000
      retired = Process.monitor(context.ctx.db_owner)
      shard = Process.monitor(context.ctx.shard_db)
      Process.exit(context.ctx.shard_db, :kill)
      assert_receive {:DOWN, ^shard, :process, _, :killed}, 5000
      refute_receive {:DOWN, ^retired, :process, _, _}, 100
      assert {:error, :timeout} = Streams.Cluster.await_ready(100)

      started = System.monotonic_time(:millisecond)
      StorageProxy.release(context.proxy)
      assert_receive {:DOWN, ^retired, :process, _, _}, 10_000
      assert :ok = Streams.Cluster.await_ready(10_000)
      assert System.monotonic_time(:millisecond) - started < 10_000
      assert {:error, {:storage, :closed, _}} = Task.await(reader, 5000)

      {:ok, {:local, replacement}} = Streams.Cluster.lookup(0)
      refute replacement.db_owner == context.ctx.db_owner
      assert {:ok, page} = Introspect.scan(Streams.Cluster, 0, 1, 10_000, maximum_scan())
      assert_maximum_page(context, page)
    after
      StorageProxy.release(context.proxy)
    end
  end

  test "losing the owner node ends an outstanding read and a new owner serves the maximum page",
       context do
    stop_supervised!(Streams.Cluster)
    {server, owner} = peer("storage-lost-owner")
    start_remote_cluster(server, context.options)
    {client, _inspector} = peer("storage-lost-client", [~c"-hidden"])
    StorageProxy.hold(context.proxy, :sst)

    try do
      reader = Task.async(fn -> remote_read(client, owner, :scan, 30_000, maximum_scan()) end)
      assert_receive {:storage_blocked, _, "GET " <> _}, 5000
      os_pid = :peer.call(server, System, :pid, [], 5000)
      started = System.monotonic_time(:millisecond)
      assert {_, 0} = System.cmd("kill", ["-KILL", os_pid])
      assert {:error, :disconnected} = Task.await(reader, 5000)
      StorageProxy.release(context.proxy)
      start_supervised!({Streams.Cluster, context.options})
      assert :ok = Streams.Cluster.await_ready(10_000)
      assert System.monotonic_time(:millisecond) - started < 10_000
      assert {:ok, page} = remote_read(client, node(), :scan, 10_000, maximum_scan())
      assert page.owner == node()
      assert_maximum_page(context, page)
    after
      StorageProxy.release(context.proxy)
    end
  end

  defp start_remote_cluster(server, options) do
    assert {:ok, _} =
             :peer.call(server, Application, :ensure_all_started, [:slap_streams], 30_000)

    assert {:ok, _} =
             eval(
               server,
               "{:ok, pid} = Slap.Streams.Cluster.start_link(options); Process.unlink(pid); :ok",
               options: options
             )

    assert :ok = :peer.call(server, Streams.Cluster, :await_ready, [10_000], 15_000)
  end

  # Remote.read takes a deadline in the client's monotonic clock.
  defp remote_read(client, owner, function, budget, opts) do
    deadline = :peer.call(client, System, :monotonic_time, [:millisecond], 5000) + budget

    :peer.call(
      client,
      Remote,
      :read,
      [
        %{entry: owner, cluster: Streams.Cluster},
        %{shard: 0, opts: opts},
        {Inspection, function},
        deadline
      ],
      30_000
    )
  end

  defp maximum_scan,
    do: [
      prefix: "inspect/",
      max_key_bytes: 4096,
      max_value_bytes: 524_288,
      target_bytes: 1_048_576,
      limit: 200
    ]

  defp assert_maximum_page(context, page) do
    assert [_, _] = page.rows
    assert Enum.map(page.rows, & &1.key) == Enum.take(context.keys, 2)
    assert Enum.all?(page.rows, &(&1.value == context.value and &1.key_size == 4096))
    assert page.stopped == :limit
    assert :erlang.external_size(page) <= page.hard_max_bytes
  end
end
