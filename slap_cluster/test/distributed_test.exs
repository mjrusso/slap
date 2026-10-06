defmodule Slap.Cluster.DistributedTest do
  # The nodes run with a 4 s net_ticktime, so a paused node is noticed
  # within about 5 s.
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag :multinode
  @moduletag timeout: 180_000

  alias Slap.Cluster.Strategy.Distributed
  alias Slap.Cluster.Test.{Counter, Failover, GatedChild, Peers, TestCluster}

  # How long a failover may take: net_ticktime, then the settle time.
  @detect 6_000

  setup_all do
    Peers.distribute!()
    :ok
  end

  setup do
    %{dir: Failover.tmp_dir()}
  end

  defp node!(ctx) do
    Failover.start_node({:local, ctx.dir}, {Distributed, interval: 500, settle: 1_000}, [
      ~c"-kernel",
      ~c"net_ticktime",
      ~c"4"
    ])
  end

  @tag multinode: false
  test "placement/2 is balanced, and moves few shards when a node joins" do
    nodes = [:a@h, :b@h, :c@h]
    p3 = Distributed.placement(64, nodes)
    assert p3 == Distributed.placement(64, Enum.reverse(nodes))
    assert Enum.sort(Map.keys(p3)) == Enum.to_list(0..63)
    assert p3 |> Map.values() |> Enum.frequencies() |> Map.values() |> Enum.max() <= 22

    p4 = Distributed.placement(64, [:d@h | nodes])
    assert p4 |> Map.values() |> Enum.frequencies() |> Map.values() |> Enum.max() <= 16
    moved = Enum.count(0..63, &(p3[&1] != p4[&1]))
    # The new node's 16, and a few more to even out the loads.
    assert moved >= 16 and moved <= 32
    assert Distributed.placement(8, []) == %{}
  end

  @tag multinode: false
  test "a slow open does not hold up the placement rounds" do
    # Each shard takes two seconds to open.
    slow = fn _ctx -> [{Agent, fn -> Process.sleep(2_000) end}] end

    start_supervised!(
      {TestCluster,
       store: :memory,
       shards: 4,
       shard_children: slow,
       strategy: {Distributed, interval: 100, settle: 0}}
    )

    # The first round, which starts the opens, is already in the strategy's
    # mailbox: the strategy answers while they run.
    assert %{} = :sys.get_state(TestCluster.Strategy, 1_000)
    assert TestCluster.local_shards() == []
    Failover.wait_until(fn -> TestCluster.local_shards() == [0, 1, 2, 3] end, 10_000)
  end

  # A peer whose :pg scope the cluster's group syncs with, so a process
  # there can join as a member.
  defp start_member_peer(name \\ :"n-#{System.unique_integer([:positive])}") do
    {peer_pid, peer} = Peers.start_as(name)
    scope = Module.concat(TestCluster, StrategyGroup)
    {:ok, _} = :erpc.call(peer, :pg, :start, [scope])

    on_exit(fn ->
      # The member must be gone before the next test's group starts.
      Node.monitor(peer, true)
      :peer.stop(peer_pid)

      receive do
        {:nodedown, ^peer} -> :ok
      after
        5_000 -> flunk("#{peer} did not stop")
      end
    end)

    {peer, scope}
  end

  # Returns the shards placed here once the member has joined.
  defp join_member({peer, scope}, shards \\ 4) do
    member = Node.spawn(peer, :timer, :sleep, [:infinity])
    :ok = :erpc.call(peer, :pg, :join, [scope, :members, member])
    here(shards, peer)
  end

  defp here(shards, peer),
    do:
      for(
        {n, owner} <- Distributed.placement(shards, Enum.sort([node(), peer])),
        owner == node(),
        do: n
      )

  defp start_slow_cluster(open_ms, opts) do
    slow = fn _ctx -> [{Agent, fn -> Process.sleep(open_ms) end}] end

    start_supervised!(
      {TestCluster,
       store: :memory,
       shards: 4,
       shard_children: slow,
       strategy: {Distributed, [interval: 60_000, settle: 0] ++ opts}}
    )

    :sys.get_state(TestCluster.Strategy, 1_000)
  end

  # There are no rounds below but the first and those of member changes.
  test "a shard that moves away while it opens is unreachable here at once, then closed" do
    member_peer = start_member_peer()
    start_slow_cluster(2_000, [])

    # The databases are open and their contexts registered; the children
    # are still starting.
    Failover.wait_until(
      fn -> Enum.all?(0..3, &match?({:ok, {:local, _}}, TestCluster.lookup(&1))) end,
      1_500
    )

    here = join_member(member_peer)
    assert here != [0, 1, 2, 3]
    moved = Enum.to_list(0..3) -- here

    # The moved shards stop being reachable here while they still open.
    Failover.wait_until(
      fn -> Enum.all?(moved, &(not match?({:ok, {:local, _}}, TestCluster.lookup(&1)))) end,
      1_500
    )

    assert TestCluster.local_shards() == []
    Failover.wait_until(fn -> TestCluster.local_shards() == here end, 10_000)
  end

  test "a shard that moves away while its open waits for a slot is not opened" do
    # A peer that takes a shard which, in open order, comes before one that
    # stays here: an open of it would come before this node's are done.
    uniq = System.unique_integer([:positive])

    name =
      Enum.find_value(1..1_000, fn i ->
        name = :"m#{i}-#{uniq}"
        here = here(8, :"#{name}@127.0.0.1")
        if Enum.any?(Enum.to_list(1..7) -- here, &(&1 < Enum.max(here))), do: name
      end)

    {peer, _scope} = member_peer = start_member_peer(name)

    start_supervised!(
      {TestCluster,
       store: :memory,
       shards: 8,
       shard_children: {GatedChild, :child_specs, [self()]},
       strategy: {Distributed, interval: 60_000, settle: 0, max_concurrency: 1}}
    )

    # Shard 0 opens; the others wait. Then the member joins, and this node
    # sees it.
    assert_receive {:child_starting, 0, _, first}
    here = join_member(member_peer, 8)
    moved = Enum.to_list(0..7) -- here

    # (Shard 0, opening, is not remote: it is registered here, stopping.)
    Failover.wait_until(
      fn -> Enum.all?(moved -- [0], &(TestCluster.lookup(&1) == {:ok, {:remote, peer}})) end,
      5_000
    )

    # From then on, only this node's shards open.
    send(first, :go)

    for _ <- here -- [0] do
      assert_receive {:child_starting, n, _, child}, 5_000
      assert n in here
      send(child, :go)
    end

    Failover.wait_until(fn -> TestCluster.local_shards() == here end, 5_000)
  end

  test "placement, routing and a clean stop", ctx do
    Failover.placement(fn -> node!(ctx) end)
  end

  test "a remote application's not_owner result is not retried", ctx do
    a = node!(ctx)
    b = node!(ctx)
    assert {:ok, owned} = Failover.await_spread([a, b], 4)
    shard = hd(owned[b])

    assert {:ok, {:error, :not_owner}} =
             :erpc.call(a, TestCluster, :call, [shard, {Counter, :return_not_owner, [self()]}])

    assert_received :application_called
    refute_received :application_called
  end

  test "kill -9 of a node under load", ctx do
    Failover.kill(fn -> node!(ctx) end, @detect)
  end

  test "a paused node under load: the others take its shards, and it gives them up", ctx do
    Failover.pause(fn -> node!(ctx) end, @detect)
  end
end
