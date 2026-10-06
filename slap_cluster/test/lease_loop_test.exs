defmodule Slap.Cluster.Test.LoopCluster do
  use Slap.Cluster, otp_app: :slap_cluster
end

defmodule Slap.Cluster.Test.FakeLease do
  @moduledoc false
  # A lease strategy on an in-memory lease store (an Agent), whose calls can
  # be made to fail: for testing Slap.Cluster.Strategy.LeaseLoop itself.

  use Slap.Cluster.Strategy.LeaseLoop

  alias Slap.Cluster.Strategy.LeaseLoop

  def start_link do
    Agent.start_link(
      fn -> %{leases: %{}, fail: 0, fail_shards: [], fail_owners: false, calls: []} end,
      name: __MODULE__
    )
  end

  @doc "Fails the next `n` heartbeats and renewals, or all of them (`:always`)."
  def fail(n), do: Agent.update(__MODULE__, &%{&1 | fail: n})

  @doc "Fails the renewals of `shards` (the others are renewed)."
  def fail_shards(shards), do: Agent.update(__MODULE__, &%{&1 | fail_shards: shards})

  @doc "Fails every read of the owners, or none."
  def fail_owners(fail?), do: Agent.update(__MODULE__, &%{&1 | fail_owners: fail?})

  @doc "Writes a lease of `owner`, as another node would."
  def put_lease(n, owner, ttl),
    do: Agent.update(__MODULE__, &put_in(&1.leases[n], {owner, now() + ttl, 1}))

  @doc "The calls so far, `[{op, ms}]`, oldest first."
  def calls, do: Agent.get(__MODULE__, &Enum.reverse(&1.calls))

  @impl LeaseLoop.Store
  def init(_opts, ctx), do: ctx

  @impl LeaseLoop.Store
  def setup(s), do: {:ok, s}

  @impl LeaseLoop.Store
  def heartbeat(s), do: {check(:heartbeat), s}

  @impl LeaseLoop.Store
  def renew(s, shards) do
    case check(:renew) do
      :ok ->
        failing = Agent.get(__MODULE__, &Enum.filter(shards, fn n -> n in &1.fail_shards end))
        renewed = Agent.get_and_update(__MODULE__, &renew_mine(&1, s, shards -- failing))

        if failing == [],
          do: {{:ok, renewed}, s},
          else: {{:error, :injected, renewed, failing}, s}

      {:error, reason} ->
        {{:error, reason, [], shards}, s}
    end
  end

  @impl LeaseLoop.Store
  def live_nodes(s), do: {{:ok, 1}, s}

  @impl LeaseLoop.Store
  def owners(s) do
    t = now()

    Agent.get(__MODULE__, fn
      %{fail_owners: true} ->
        {{:error, :injected}, s}

      state ->
        owners =
          for {n, {owner, at, _}} <- state.leases,
              owner != nil and at > t,
              into: %{},
              do: {n, owner}

        {{:ok, owners}, s}
    end)
  end

  @impl LeaseLoop.Store
  def claim(s, count) do
    t = now()

    claimed =
      Agent.get_and_update(__MODULE__, &claim_free(&1, s, count, t))

    {{:ok, claimed}, s}
  end

  defp renew_mine(state, s, shards) do
    mine = for n <- shards, match?({owner, _, _} when owner == s.me, state.leases[n]), do: n
    leases = Enum.reduce(mine, state.leases, &renew_one(&1, &2, now() + s.ttl))
    {mine, %{state | leases: leases}}
  end

  defp renew_one(n, leases, expires_at) do
    {owner, _, g} = leases[n]
    Map.put(leases, n, {owner, expires_at, g})
  end

  defp claim_free(state, s, count, t) do
    free = for n <- 0..(s.shards - 1), free?(state.leases[n], t), do: n
    claimed = for n <- Enum.take(free, count), do: {n, next_generation(state.leases[n])}
    leases = Enum.into(claimed, state.leases, fn {n, g} -> {n, {s.me, t + s.ttl, g}} end)
    {claimed, %{state | leases: leases}}
  end

  defp free?(nil, _t), do: true
  defp free?({nil, _, _}, _t), do: true
  defp free?({_owner, expires_at, _}, t), do: expires_at < t

  defp next_generation(nil), do: 1
  defp next_generation({_, _, g}), do: g + 1

  @impl LeaseLoop.Store
  def release(s, n) do
    Agent.update(__MODULE__, fn state ->
      case state.leases[n] do
        {owner, _, g} when owner == s.me -> put_in(state.leases[n], {nil, 0, g})
        _ -> state
      end
    end)

    s
  end

  @impl LeaseLoop.Store
  def leave(s) do
    Agent.update(__MODULE__, fn state ->
      leases =
        Map.new(state.leases, fn
          {n, {owner, _, g}} when owner == s.me -> {n, {nil, 0, g}}
          other -> other
        end)

      %{state | leases: leases}
    end)

    s
  end

  defp check(op) do
    Agent.get_and_update(__MODULE__, fn state ->
      state = %{state | calls: [{op, now()} | state.calls]}

      case state.fail do
        :always -> {{:error, :injected}, state}
        0 -> {:ok, state}
        n -> {{:error, :injected}, %{state | fail: n - 1}}
      end
    end)
  end

  defp now, do: System.monotonic_time(:millisecond)
end

defmodule Slap.Cluster.Test.SlowChild do
  @moduledoc false
  # A shard child that takes `delay` ms to start, as a slow shard open.
  def child_specs(_ctx, delay) do
    [%{id: __MODULE__, start: {__MODULE__, :start_link, [delay]}}]
  end

  def start_link(delay) do
    Process.sleep(delay)
    Agent.start_link(fn -> nil end)
  end
end

defmodule Slap.Cluster.LeaseLoopTest do
  # The lease loop on an in-memory lease store that fails on demand: a failed
  # renewal, a store that stops answering, and slow shard opens.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Slap.Cluster.Strategy.LeaseLoop
  alias Slap.Cluster.Test.{FakeLease, GatedChild, LoopCluster, SlowChild}

  # A 3 s TTL: renewals every second, and the watchdog 0.5 s before expiry.
  @ttl 3_000

  setup do
    start_supervised!(%{id: FakeLease, start: {FakeLease, :start_link, []}})
    test = self()

    :telemetry.attach_many(
      "lease-loop-test",
      [[:slap, :cluster, :lease, :self_fence], [:slap, :cluster, :lease, :renew_failed]],
      fn
        [_, _, _, :self_fence], _, meta, _ -> send(test, {:self_fence, meta.shards})
        [_, _, _, :renew_failed], _, _, _ -> send(test, :store_failed)
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach("lease-loop-test") end)
    :ok
  end

  defp start_cluster(opts \\ []) do
    start_supervised!(
      {LoopCluster,
       Keyword.merge(
         [
           store: :memory,
           shards: 4,
           settings: %{flush_interval: "10ms"},
           strategy: {FakeLease, lease_ttl: @ttl}
         ],
         opts
       )}
    )
  end

  defp wait_until(fun, timeout \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        fun.() -> :ok
        System.monotonic_time(:millisecond) > deadline -> flunk("timed out")
        true -> Process.sleep(50) && nil
      end
    end)
    |> Enum.find(& &1)
  end

  test "a refresh cannot outlive the caller's timeout" do
    start_cluster()
    strategy = Process.whereis(Module.concat(LoopCluster, Strategy))
    :ok = :sys.suspend(strategy)

    try do
      started = System.monotonic_time(:millisecond)
      assert :ok = LeaseLoop.refresh(LoopCluster, 50)
      assert System.monotonic_time(:millisecond) - started < 250
    after
      :sys.resume(strategy)
    end
  end

  test "one failed renewal does not stop the shards, and is retried sooner" do
    start_cluster()
    wait_until(fn -> LoopCluster.local_shards() == [0, 1, 2, 3] end)

    FakeLease.fail(1)
    failed_at = System.monotonic_time(:millisecond)

    # Two TTLs of sampling: the shards stay open throughout.
    for _ <- 1..60 do
      assert LoopCluster.local_shards() == [0, 1, 2, 3]
      Process.sleep(100)
    end

    refute_received {:self_fence, _}

    # The heartbeat after the failed one came within about half an interval.
    [{:heartbeat, t1}, {:heartbeat, t2} | _] =
      for {:heartbeat, t} = call <- FakeLease.calls(), t >= failed_at, do: call

    assert t2 - t1 < 800
  end

  test "a store that stops answering: every shard stops before its lease expires" do
    start_cluster()
    wait_until(fn -> LoopCluster.local_shards() == [0, 1, 2, 3] end)

    FakeLease.fail(:always)
    failed_at = System.monotonic_time(:millisecond)
    wait_until(fn -> LoopCluster.local_shards() == [] end)
    stopped_at = System.monotonic_time(:millisecond)

    assert_received {:self_fence, [0, 1, 2, 3]}
    # The last renewal was at most an interval before the failure; the
    # shards stop half a second before that renewal's leases expire.
    assert stopped_at - failed_at < @ttl

    # When the store answers again, the node takes its shards back.
    FakeLease.fail(0)
    wait_until(fn -> LoopCluster.local_shards() == [0, 1, 2, 3] end)
  end

  test "renewals keep their pace while shards open slowly" do
    # Each shard takes two TTLs to open.
    start_cluster(shard_children: {SlowChild, :child_specs, [2 * @ttl]})
    started = System.monotonic_time(:millisecond)

    wait_until(fn -> LoopCluster.local_shards() == [0, 1, 2, 3] end, 20_000)

    renewals =
      for {:renew, t} <- FakeLease.calls(), t < started + 2 * @ttl, do: t

    # About one a second while the shards were opening.
    assert Enum.count_until(renewals, 5) == 5
    refute_received {:self_fence, _}

    # And the leases were held throughout: nothing was lost.
    Process.sleep(1_500)
    assert LoopCluster.local_shards() == [0, 1, 2, 3]
  end

  test "a failed renewal of one lease keeps the others' shards open" do
    start_cluster()
    wait_until(fn -> LoopCluster.local_shards() == [0, 1, 2, 3] end)

    FakeLease.fail_shards([3])
    assert_receive {:self_fence, [3]}, @ttl

    # Shard 3 is claimed again once its lease expires, and stops again, a
    # deadline later than the others' first one: they were renewed. (Self-
    # fences are reported in order.)
    assert_receive {:self_fence, [3]}, 3 * @ttl
    assert Enum.all?(0..2, &(&1 in LoopCluster.local_shards()))
  end

  test "a shard whose deadline passes while its open waits for a slot is not opened" do
    start_cluster(
      shard_children: {GatedChild, :child_specs, [self()]},
      strategy: {FakeLease, lease_ttl: @ttl, max_concurrency: 1}
    )

    # Shard 0 opens (generation 1); 1, 2 and 3 wait. Then the store fails,
    # and every deadline passes.
    assert_receive {:child_starting, 0, 1, child}
    FakeLease.fail(:always)
    assert_receive {:self_fence, [0, 1, 2, 3]}, @ttl
    send(child, :go)
    assert_receive {:child_stopped, 0, ^child}, 2_000

    # When the store answers again, every shard is claimed again (generation
    # 2) and opens. None opens with its old lease (generation 1) first.
    FakeLease.fail(0)

    for _ <- 0..3 do
      assert_receive {:child_starting, n, generation, child}, 2 * @ttl
      assert generation == 2, "shard #{n} opened with generation #{generation}"
      send(child, :go)
    end

    wait_until(fn -> LoopCluster.local_shards() == [0, 1, 2, 3] end)
  end

  test "a lease lost in a round where another renewal fails is closed at once" do
    start_cluster()
    wait_until(fn -> LoopCluster.local_shards() == [0, 1, 2, 3] end)

    # Shard 3's renewals fail, and another node takes shard 2.
    FakeLease.fail_shards([3])
    FakeLease.put_lease(2, "other@host", 60_000)

    wait_until(fn -> 2 not in LoopCluster.local_shards() end)
    # It was closed as lost, not stopped by its deadline.
    {:messages, messages} = Process.info(self(), :messages)
    refute Enum.any?(for {:self_fence, shards} <- messages, do: 2 in shards)
  end

  test "if the runner exits, the loop stops, and the cluster starts over" do
    start_cluster()
    wait_until(fn -> LoopCluster.local_shards() == [0, 1, 2, 3] end)

    strategy = Process.whereis(LoopCluster.Strategy)
    ref = Process.monitor(strategy)
    Process.exit(:sys.get_state(strategy).runner, :boom)

    assert_receive {:DOWN, ^ref, :process, ^strategy, {:runner_exit, :boom}}

    wait_until(fn ->
      try do
        LoopCluster.local_shards() == [0, 1, 2, 3]
      catch
        :exit, _ -> false
      end
    end)
  end

  test "routing keeps the last owners read while reads fail, and skips unknown owners" do
    # :"other@host" is an atom (it is in this test), so a known node, even
    # though it is not connected; no atom names the owner of shard 2.
    other = :other@host
    FakeLease.put_lease(2, "never" <> "-seen@host", 60_000)
    FakeLease.put_lease(3, "other@host", 60_000)
    start_cluster()
    wait_until(fn -> LoopCluster.local_shards() == [0, 1] end)
    assert FakeLease.lookup(LoopCluster, 3) == {:ok, {:remote, other}}
    assert FakeLease.lookup(LoopCluster, 2) == {:error, :unassigned}
    assert_raise ArgumentError, fn -> String.to_existing_atom("never-seen@host") end

    FakeLease.fail_owners(true)
    FakeLease.refresh(LoopCluster, 5_000)
    assert FakeLease.lookup(LoopCluster, 3) == {:ok, {:remote, other}}

    # Rounds that fail to read the owners.
    for _ <- 1..3 do
      assert_receive :store_failed, @ttl
      assert FakeLease.lookup(LoopCluster, 3) == {:ok, {:remote, other}}
    end
  end
end
