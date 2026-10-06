defmodule Slap.Cluster.StaticTest do
  # The Static strategy: a fixed assignment, no failover.
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias Slap.Cluster.Hash
  alias Slap.Cluster.Strategy.Static
  alias Slap.Cluster.Test.{Counter, Failover, Peers, TestCluster}

  test "assignments must cover every shard exactly once" do
    Process.flag(:trap_exit, true)

    assert {:error, reason} =
             TestCluster.start_link(
               store: :memory,
               shards: 4,
               strategy: {Static, assignments: %{node() => [0..2]}}
             )

    assert inspect(reason) =~ "exactly once"

    assert {:error, reason} =
             TestCluster.start_link(
               store: :memory,
               shards: 4,
               strategy: {Static, assignments: %{node() => [0..3, 0]}}
             )

    assert inspect(reason) =~ "exactly once"
  end

  describe "two nodes" do
    @describetag :multinode

    setup do
      Peers.distribute!()
      :ok
    end

    test "each opens its shards, calls are routed, and a node's shards wait for it" do
      dir = Failover.tmp_dir()
      {pa, a} = Peers.start("a")
      {pb, b} = Peers.start("b")

      on_exit(fn ->
        for p <- [pa, pb] do
          try do
            :peer.stop(p)
          catch
            :exit, _ -> :ok
          end
        end
      end)

      start = fn node ->
        Peers.start_cluster(node,
          store: {:local, dir},
          shards: 4,
          shard_children: {Counter, :child_specs, [self()]},
          strategy: {Static, nodes: [a, b], backoff_base: 100}
        )
      end

      :ok = start.(a)
      :ok = start.(b)
      assert Peers.local_shards(a) == [0, 2]
      assert Peers.local_shards(b) == [1, 3]

      for key <- Enum.map(1..20, &"k#{&1}") do
        assert {:ok, 1} = :erpc.call(a, Counter, :increment, [TestCluster, key])
        assert {:ok, 2} = :erpc.call(b, Counter, :increment, [TestCluster, key])
      end

      :ok = Peers.stop_cluster(b)

      key =
        Enum.find(Enum.map(1..20, &"k#{&1}"), &(Hash.shard_for(&1, 4) in [1, 3]))

      assert {:error, _} = :erpc.call(a, Counter, :increment, [TestCluster, key])
      :ok = start.(b)
      assert {:ok, 3} = :erpc.call(a, Counter, :increment, [TestCluster, key])
    end
  end
end
