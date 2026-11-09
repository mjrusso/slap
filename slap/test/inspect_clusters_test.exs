defmodule Slap.InspectClustersTest do
  # Starts the named Streams and KV clusters.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Breeze.Test
  alias Slap.Inspect.{Clusters, Operation, Request}
  alias Slap.{KV, Streams}

  setup do
    start_supervised!({Streams.Cluster, store: :memory, shards: 2})
    start_supervised!({KV.Cluster, store: :memory, shards: 2})
    assert :ok = Streams.Cluster.await_ready(5000)
    assert :ok = KV.Cluster.await_ready(5000)
    :ok
  end

  defp discovered do
    assert {:ok, clusters} = Clusters.discover(node())
    Enum.filter(clusters, &(&1.cluster in [Streams.Cluster, KV.Cluster]))
  end

  test "discovery offers each Streams and KV cluster's views" do
    views = Map.new(discovered(), &{&1.cluster, Clusters.views(&1)})

    assert views == %{
             Streams.Cluster => [:raw, :streams, :log, :yjs],
             KV.Cluster => [:raw, :kv, :files]
           }
  end

  test "every service the inspector reads is one that Streams or KV clusters report" do
    reported = discovered() |> Enum.flat_map(& &1.services) |> MapSet.new()

    read =
      for view <- Operation.views(),
          service = Operation.service(view),
          service != :any,
          into: MapSet.new(),
          do: service

    assert MapSet.subset?(read, reported)
  end

  test "--cluster reads the named cluster's views, and a cluster not running is an error" do
    assert {:ok, [%{cluster: KV.Cluster} = kv]} = Clusters.load(node(), KV.Cluster)
    assert Clusters.views(kv) == [:raw, :kv, :files]

    assert {:error, {:cluster_not_running, "Slap.Missing.Cluster"}} =
             Clusters.load(node(), Slap.Missing.Cluster)
  end

  test "one terminal session browses Streams and KV data" do
    assert {:ok, _, _} = Streams.create("/inspect-clusters", body: "message")
    assert {:ok, _} = KV.put("users", "ada", "lovelace")
    shard = Streams.Cluster.shard_for("/inspect-clusters")

    {:ok, request} =
      Request.new(%{view: :streams, shard: shard, budget: 5000, opts: []})

    session =
      Test.start!(Slap.Inspect.View,
        size: {160, 40},
        start_opts: [
          connection: %{entry: node(), cluster: Streams.Cluster},
          clusters: discovered(),
          request: request,
          refresh: 0
        ]
      )

    on_exit(fn -> Test.stop(session) end)
    :erlang.trace(session.pid, true, [:receive])

    # Menu: 1 raw, 2 kv, 3 files (KV); 4 raw, 5 streams, 6 log, 7 yjs (Streams).
    screen = await_read(session)
    assert screen =~ "Slap.Streams.Cluster · streams"
    assert screen =~ "/inspect-clusters"

    Test.input(session, "2")
    await_read(session)
    Test.input(session, "/")
    Test.input(session, "\x15")
    Test.render!(session)
    Enum.each(String.graphemes("kv --partition users"), &Test.input(session, &1))
    Test.input(session, "Enter")
    screen = await_read(session)
    assert screen =~ "Slap.KV.Cluster · kv"
    assert screen =~ "1 users / ada"

    Test.input(session, "5")
    screen = await_read(session)
    assert screen =~ "Slap.Streams.Cluster · streams"
    assert screen =~ "/inspect-clusters"
  end

  # Waits for the view's current read and returns the screen that shows it.
  # The metadata call follows any result already in the view's mailbox.
  defp await_read(session) do
    pid = session.pid
    %{assigns: %{id: id, operation: operation}} = Test.metadata(session)

    if operation != :idle,
      do: assert_receive({:trace, ^pid, :receive, {:inspection_result, ^id, {:ok, _page}}}, 5000)

    Test.render!(session)
  end
end
