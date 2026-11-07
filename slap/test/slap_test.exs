defmodule SlapTest do
  # Not async: Slap starts the built-in clusters, Files and Yjs, which are
  # named processes.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Slap.{Files, KV, Streams, Yjs}

  defmodule DocServer do
    use Slap.Yjs.DocServer
  end

  test "starts every service on one store" do
    start_supervised!(
      {Slap, store: :memory, streams: [shards: 1], kv: [shards: 1], files: [], yjs: []}
    )

    assert {:ok, :created, _} = Streams.create("/events", content_type: "text/plain")
    assert {:ok, %{result: :appended}} = Streams.append("/events", "created")
    assert {:ok, _version} = KV.put("users", "42", "Ada", if_version: :absent)
    assert {:ok, %{value: "Ada"}} = KV.get("users", "42")
    assert {:ok, _file} = Files.put({"docs", "a.txt"}, "hello", if_version: :absent)
    assert {:ok, "hello"} = Files.read({"docs", "a.txt"})
    assert {:ok, server} = Yjs.Docs.join(DocServer, {"docs", "doc-1"})
    assert {:ok, _doc} = Yjs.DocServer.doc(server, 5_000)
  end

  test "a second start returns the running supervisor" do
    slap = start_supervised!({Slap, store: :memory, kv: [shards: 1]})

    assert {:error, {:already_started, ^slap}} = Slap.start_link(store: :memory, kv: [shards: 1])
    assert {:ok, _version} = KV.put("p", "k", "v")
  end

  test "starts only the selected cluster, treating nil options as missing" do
    start_supervised!({Slap, store: :memory, kv: [shards: 1], streams: nil, strategy: nil})

    assert {:ok, _version} = KV.put("p", "k", "v")
    assert Process.whereis(Streams.Cluster.Supervisor) == nil
    assert Process.whereis(Files) == nil
  end

  test "a nil option leaves the cluster's application configuration in effect" do
    Process.flag(:trap_exit, true)
    config = Application.get_env(:slap_kv, KV.Cluster)
    on_exit(fn -> restore_env(:slap_kv, KV.Cluster, config) end)
    Application.put_env(:slap_kv, KV.Cluster, strategy: NotAStrategy)

    assert {:error, reason} = Slap.start_link(store: :memory, kv: [shards: 1], strategy: nil)
    assert inspect(reason) =~ "NotAStrategy must implement Slap.Cluster.Strategy"
  end

  @tag :tmp_dir
  test "takes the options it is not given from the cluster's application config", %{
    tmp_dir: dir
  } do
    config = Application.get_env(:slap_kv, KV.Cluster)
    on_exit(fn -> restore_env(:slap_kv, KV.Cluster, config) end)
    Application.put_env(:slap_kv, KV.Cluster, shards: 3, path: "legacy")

    start_supervised!({Slap, store: {:local, dir}, kv: []})

    assert :ok = KV.Cluster.await_ready(10_000)
    assert map_size(KV.Cluster.assignments()) == 3
    assert File.exists?(Path.join(dir, "legacy/_cluster/layout"))
  end

  @tag :tmp_dir
  test "opens each cluster under its :path", %{tmp_dir: dir} do
    start_supervised!({Slap, store: {:local, dir}, kv: [shards: 1, path: "kv-data"]})

    assert :ok = KV.Cluster.await_ready(10_000)
    assert File.exists?(Path.join(dir, "kv-data/_cluster/layout"))
  end

  test "a KV cluster failure restarts KV and Files, and leaves Streams and Yjs running" do
    slap = start_all()
    streams = streams_pids()

    restart_cluster(slap, :kv, KV.Cluster, Files)

    assert streams_pids() == streams
    assert {:ok, _version} = KV.put("users", "42", "Ada", if_version: :absent)
    assert {:ok, _file} = Files.put({"docs", "a.txt"}, "hello", if_version: :absent)
    assert {:ok, "hello"} = Files.read({"docs", "a.txt"})
  end

  test "a Streams cluster failure restarts Streams and Yjs, and leaves KV and Files running" do
    slap = start_all()
    kv = kv_pids()

    restart_cluster(slap, :streams, Streams.Cluster, Yjs.Docs)

    assert kv_pids() == kv
    assert {:ok, :created, _} = Streams.create("/events", content_type: "text/plain")
    assert {:ok, server} = Yjs.Docs.join(DocServer, {"docs", "doc-1"})
    assert {:ok, _doc} = Yjs.DocServer.doc(server, 5_000)
  end

  test "requires a store and at least one cluster" do
    assert_raise ArgumentError, ~r/:store is required/, fn ->
      Slap.child_spec(kv: [])
    end

    assert_raise ArgumentError, ~r/configure :streams, :kv, or both/, fn ->
      Slap.child_spec(store: :memory)
    end
  end

  test "requires the cluster each service runs on" do
    assert_raise ArgumentError, ~r/:files requires :kv/, fn ->
      Slap.child_spec(store: :memory, streams: [], files: [])
    end

    assert_raise ArgumentError, ~r/:yjs requires :streams/, fn ->
      Slap.child_spec(store: :memory, kv: [], yjs: [])
    end

    assert_raise ArgumentError, ~r/:cluster is not allowed/, fn ->
      Slap.child_spec(store: :memory, kv: [], files: [cluster: KV.Cluster])
    end

    assert_raise ArgumentError, ~r/:cluster is not allowed/, fn ->
      Slap.child_spec(store: :memory, streams: [], yjs: [cluster: Streams.Cluster])
    end
  end

  test "requires keyword lists of service options" do
    assert_raise ArgumentError, ~r/:files must be a keyword list, got: true/, fn ->
      Slap.child_spec(store: :memory, kv: [], files: true)
    end

    assert_raise ArgumentError, ~r/:streams must be a keyword list/, fn ->
      Slap.child_spec(store: :memory, streams: true)
    end
  end

  test "rejects unknown options when building the child spec" do
    assert_raise ArgumentError, ~r/strams/, fn ->
      Supervisor.child_spec({Slap, store: :memory, streams: [], strams: []}, [])
    end

    assert_raise ArgumentError, ~r/shardz/, fn ->
      Slap.child_spec(store: :memory, streams: [shardz: 2])
    end

    assert_raise ArgumentError, ~r/partition_writters/, fn ->
      Slap.child_spec(store: :memory, kv: [child_options: [partition_writters: 2]])
    end

    assert_raise ArgumentError, ~r/prefx/, fn ->
      Slap.child_spec(store: :memory, streams: [], yjs: [prefx: "/x"])
    end
  end

  test "reports invalid cluster settings from start_link" do
    Process.flag(:trap_exit, true)

    assert {:error, reason} = Slap.start_link(store: :memory, streams: [settings: nil])
    assert inspect(reason) =~ ":settings must be a map"
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)

  defp start_all do
    start_supervised!(
      {Slap, store: :memory, streams: [shards: 1], kv: [shards: 1], files: [], yjs: []}
    )
  end

  defp streams_pids,
    do: {Process.whereis(Streams.Cluster.Supervisor), Process.whereis(Yjs.Docs)}

  defp kv_pids, do: {Process.whereis(KV.Cluster.Supervisor), Process.whereis(Files)}

  defp restart_cluster(slap, branch, cluster, service) do
    old = Process.whereis(service)
    ref = Process.monitor(old)

    # :sys.terminate stops the cluster's children before it exits: a killed
    # supervisor leaves them running under their registered names, and the
    # restart fails until they exit.
    :ok = :sys.terminate(Module.concat(cluster, Supervisor), :crash, 10_000)
    assert_receive {:DOWN, ^ref, :process, ^old, _reason}, 5_000

    {_id, pid, _type, _modules} =
      slap |> Supervisor.which_children() |> List.keyfind({Slap, branch}, 0)

    # The branch handles this call after it has restarted its children.
    _children = Supervisor.which_children(pid)
    assert Process.whereis(service) not in [nil, old]
  end
end
