defmodule Slap.Cluster.ObjectLeaseTest do
  # The ObjectLease strategy: leases as objects in the cluster's own bucket,
  # no database. Multi-node scenarios on RustFS (the nodes share the store),
  # and one node on the in-memory store.
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 180_000

  alias Slap.Cluster.Strategy.ObjectLease
  alias Slap.Cluster.Test.{Counter, Failover, Peers, TestCluster}
  alias Slap.SlateDB.ObjectStore

  @ttl 3_000

  describe "one node" do
    test "claims every shard from an empty store" do
      start_supervised!(
        {TestCluster,
         store: :memory,
         shards: 4,
         shard_children: {Counter, :child_specs, [self()]},
         strategy: {ObjectLease, lease_ttl: 1_500, max_clock_skew_ms: 500}}
      )

      Failover.wait_until(fn -> TestCluster.local_shards() == [0, 1, 2, 3] end, 5_000)
      assert {:ok, 1} = Counter.increment(TestCluster, "k")
    end

    @tag :tmp_dir
    test "refuses a store without conditional updates", %{tmp_dir: dir} do
      Process.flag(:trap_exit, true)

      assert {:error, {:shutdown, {:failed_to_start_child, :strategy, {:unsupported, reason}}}} =
               TestCluster.start_link(
                 store: {:local, dir},
                 shards: 2,
                 shard_children: {Counter, :child_specs, [self()]},
                 strategy: {ObjectLease, lease_ttl: 1_500}
               )

      assert reason =~ "has no conditional updates"
    end
  end

  describe "the lease store, in memory" do
    setup do
      ctx = %{
        cluster: nil,
        cluster_name: "mem",
        me: "n1@host",
        ttl: 60_000,
        shards: 4,
        timeout: 5_000
      }

      {:ok, s} = ObjectLease.setup(ObjectLease.init([store: :memory, path: "placement"], ctx))
      %{s: s}
    end

    # A lease as another writer (or an earlier write of this node) left it.
    defp write_lease(s, n, owner, generation) do
      expires_at = System.os_time(:millisecond) + 60_000

      body =
        JSON.encode!(%{"owner" => owner, "expires_at" => expires_at, "generation" => generation})

      {:ok, _} = ObjectStore.put(s.os, "shards/00000#{n}", body)
    end

    test "a renewal adopts a lease this node wrote but did not see acknowledged", %{s: s} do
      {{:ok, _}, s} = ObjectLease.owners(s)
      {{:ok, claimed}, s} = ObjectLease.claim(s, 3)
      [{a, ga}, {b, _}, {c, _}] = Enum.sort(claimed)

      # `a`: this node's write went through, but its reply was lost. `b`:
      # another node claimed it. `c`: untouched.
      write_lease(s, a, "n1@host", ga)
      write_lease(s, b, "n2@host", 7)

      assert {{:ok, renewed}, s} = ObjectLease.renew(s, [a, b, c])
      assert Enum.sort(renewed) == [a, c]
      assert {{:ok, renewed}, _s} = ObjectLease.renew(s, [a, c])
      assert Enum.sort(renewed) == [a, c]
    end

    test "a lease the loop stops renewing (its shard stopped) can be claimed again", %{s: s} do
      {{:ok, _}, s} = ObjectLease.owners(s)
      {{:ok, claimed}, s} = ObjectLease.claim(s, 4)
      [fenced | kept] = claimed |> Enum.map(&elem(&1, 0)) |> Enum.sort()

      # The shard of one lease self-fenced: the loop renews only the others.
      {{:ok, renewed}, s} = ObjectLease.renew(s, kept)
      assert Enum.sort(renewed) == kept
      {{:ok, _}, s} = ObjectLease.owners(s)
      assert {{:ok, [{^fenced, 2}]}, _s} = ObjectLease.claim(s, 1)
    end

    test "a lease that names this node but is not held is claimed at once", %{s: s} do
      # A claim of this node went through, but its reply was lost.
      write_lease(s, 1, "n1@host", 3)
      write_lease(s, 2, "n2@host", 3)

      {{:ok, owners}, s} = ObjectLease.owners(s)
      assert owners == %{1 => "n1@host", 2 => "n2@host"}
      assert {{:ok, claimed}, _s} = ObjectLease.claim(s, 4)
      assert Enum.sort(claimed) == [{0, 1}, {1, 4}, {3, 1}]
    end

    test "a lease that is not a lease is no one's, and can be claimed", %{s: s} do
      {:ok, _} = ObjectStore.put(s.os, "shards/000002", ~s({"owner": 5, "expires_at": "x"}))
      write_lease(s, 3, "n2@host", "two")

      assert {{:ok, owners}, s} = ObjectLease.owners(s)
      assert owners == %{}
      assert {{:ok, claimed}, _s} = ObjectLease.claim(s, 4)
      assert Enum.sort(claimed) == [{0, 1}, {1, 1}, {2, 1}, {3, 1}]
    end
  end

  describe "the lease store, on RustFS" do
    @describetag :s3

    test "a restarted node frees the leases its previous run left" do
      opts = [store: Failover.s3_store("object-lease-restart"), path: "placement"]

      ctx = %{
        cluster: nil,
        cluster_name: "restart",
        me: "n1@host",
        ttl: 60_000,
        shards: 4,
        timeout: 5_000
      }

      {:ok, s} = ObjectLease.setup(ObjectLease.init(opts, ctx))
      assert {{:ok, [_, _, _, _]}, _} = ObjectLease.claim(elem(ObjectLease.owners(s), 1), 4)

      # The node dies without leaving; it comes back under the same name.
      s = ObjectLease.init(opts, ctx)
      {:ok, s} = ObjectLease.setup(s)
      assert {{:ok, owners}, s} = ObjectLease.owners(s)
      assert owners == %{}
      assert {{:ok, [_, _, _, _]}, _} = ObjectLease.claim(s, 4)
    end
  end

  describe "several nodes, on RustFS" do
    @describetag :s3
    @describetag :multinode

    setup do
      Peers.distribute!()
      :ok
    end

    setup do
      %{store: Failover.s3_store("object-lease")}
    end

    defp node!(ctx),
      do: Failover.start_node(ctx.store, {ObjectLease, lease_ttl: @ttl})

    test "placement, routing and a clean stop", ctx do
      Failover.placement(fn -> node!(ctx) end)
    end

    test "kill -9 of a node under load", ctx do
      Failover.kill(fn -> node!(ctx) end, @ttl)
    end

    test "a paused node (SIGSTOP past its leases) under load", ctx do
      Failover.pause(fn -> node!(ctx) end, @ttl)
    end
  end
end
