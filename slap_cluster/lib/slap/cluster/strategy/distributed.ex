defmodule Slap.Cluster.Strategy.Distributed do
  @moduledoc """
  Places shards on the connected nodes of a distributed Erlang cluster with
  no database and no leases: every node computes the same placement from
  the list of nodes running the cluster (a `:pg` group), by rendezvous
  hashing with bounded loads: shard `n` prefers the nodes in decreasing
  order of `xxh64("<n>/<node>")`, and goes to the first of them that has
  fewer than `ceil(shards / nodes)` shards so far. Every node gets its fair
  share, and a node joining or leaving moves few shards besides its own.

  Every `:interval` (default 1 s), and whenever the member list changes,
  each node stops the shards it no longer owns (at once), and opens the
  shards it owns once that has been so for `:settle` ms (default 2 s), which
  gives the previous owner time to close them. `lookup/2` reads the
  placement this node computed last. The strategy asks the host to open
  and close shards without waiting for replies, so a slow
  open or close does not hold up the rounds, and up to `:max_concurrency`
  shards open at once. All these requests come from the strategy's
  process, so the host gets them in order: a close requested while the
  shard opens is handled after the open (the host closes it once it has
  opened).

  **Failover** follows distributed Erlang's view of the nodes: a node that
  dies or stops is gone at once, and its shards open elsewhere after
  `:settle`. A node that stops answering (a pause, a partition) is only
  noticed after `net_ticktime` (60 s by default; set it lower, for example
  `-kernel net_ticktime 10`).

  **Partitions.** There is no quorum: in a partition each side computes a
  placement from the nodes it sees, and both may open a shard. SlateDB's
  fencing keeps at most one effective writer (the last to open fences the
  other, whose writes fail and are not acknowledged), so the cost is
  availability, not data. When the partition heals, `:pg` merges the member
  lists and every node computes the same placement again. For stronger
  guarantees, use `ObjectLease`.

  Placement uses `:pg` membership because it resyncs after reconnection.
  `:global` names can retain split views after a node pauses and resumes.

  ## Options

    * `:interval` - ms between placement rounds (default 1,000).
    * `:settle` - ms a node waits, after it becomes a shard's owner, before
      opening it (default 2,000).
    * `:max_concurrency` - shards opened at once (default 16).

  Telemetry: `[:slap, :cluster, :distributed, :open | :close]` with
  `%{cluster, shard}`, and `[:distributed, :members]` with
  `%{count: n}` when the member list changes.
  """

  @behaviour Slap.Cluster.Strategy

  use GenServer
  require Logger

  alias Slap.Cluster.{Config, Hash, Host, Telemetry}
  alias Slap.Cluster.Strategy, as: StrategyOptions

  @impl Slap.Cluster.Strategy
  def validate_options(opts) do
    StrategyOptions.validate_options!(
      opts,
      [:interval, :settle, :max_concurrency],
      [:interval, :max_concurrency],
      [:settle]
    )
  end

  @impl Slap.Cluster.Strategy
  def child_spec(opts),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, shutdown: 60_000}

  @impl Slap.Cluster.Strategy
  def lookup(cluster, n) do
    case :persistent_term.get({__MODULE__, cluster}, %{}) do
      %{^n => owner} when owner != node() -> {:ok, {:remote, owner}}
      _ -> {:error, :unassigned}
    end
  end

  @impl Slap.Cluster.Strategy
  def handle_shard_down(cluster, n, reason), do: GenServer.cast(name(cluster), {:down, n, reason})

  @doc """
  The placement of `shards` shards on `nodes`: `%{shard => node}`, the same
  on every node given the same nodes.
  """
  @spec placement(pos_integer(), [node()]) :: %{non_neg_integer() => node()}
  def placement(_shards, []), do: %{}

  def placement(shards, nodes) do
    cap = div(shards + length(nodes) - 1, length(nodes))

    {placement, _load} =
      Enum.reduce(0..(shards - 1), {%{}, %{}}, fn n, {placement, load} ->
        owner =
          nodes
          |> Enum.sort_by(&{Hash.xxh64("#{n}/#{&1}"), &1}, :desc)
          |> Enum.find(&(Map.get(load, &1, 0) < cap))

        {Map.put(placement, n, owner), Map.update(load, owner, 1, &(&1 + 1))}
      end)

    placement
  end

  def start_link(opts) do
    cluster = Keyword.fetch!(opts, :cluster)
    GenServer.start_link(__MODULE__, opts, name: name(cluster))
  end

  defp name(cluster), do: Module.concat(cluster, Strategy)
  defp scope(cluster), do: Module.concat(cluster, StrategyGroup)

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    cluster = Keyword.fetch!(opts, :cluster)

    case :pg.start_link(scope(cluster)) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    :ok = :pg.join(scope(cluster), :members, self())
    {_ref, _members} = :pg.monitor(scope(cluster), :members)

    state = %{
      cluster: cluster,
      shards: Config.get(cluster).shards,
      interval: Keyword.get(opts, :interval, 1_000),
      settle: Keyword.get(opts, :settle, 2_000),
      max_concurrency: Keyword.get(opts, :max_concurrency, 16),
      nodes: [],
      # shard => ms since which this node has owned it (not yet open).
      pending: %{},
      # Shards open here.
      open: MapSet.new(),
      # shard => {:opening | :closing, tag}, while a request to the host
      # opens or closes it.
      ops: %{},
      # The requests to the host, labelled {:opening | :closing, shard, tag}.
      requests: :gen_server.reqids_new(),
      # shard => ms until which not to reopen it (it was fenced).
      backoff: %{}
    }

    send(self(), :place)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:place, state) do
    Process.send_after(self(), :place, state.interval)
    {:noreply, place(state)}
  end

  # The member list changed.
  def handle_info({_ref, join_or_leave, :members, _pids}, state)
      when join_or_leave in [:join, :leave],
      do: {:noreply, place(state)}

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(msg, state) do
    case :gen_server.check_response(msg, state.requests, true) do
      {{:reply, result}, label, requests} ->
        {:noreply, state |> Map.put(:requests, requests) |> finished(label, result) |> place()}

      # The host is gone: the cluster's supervisor starts over.
      {{:error, {reason, _host}}, _label, _requests} ->
        {:stop, {:host_down, reason}, state}

      _not_a_response ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_cast({:down, n, reason}, state) when reason in [:fenced, :crashed] do
    Logger.warning("#{inspect(state.cluster)}: shard #{n} is down (#{reason})")
    # Fenced: another node opened it, so the member lists disagree for now.
    # Wait before opening it again rather than take it straight back.
    backoff = Map.put(state.backoff, n, now() + 5 * state.settle)
    {:noreply, %{state | open: MapSet.delete(state.open, n), backoff: backoff}}
  end

  def handle_cast({:down, _n, :stopped}, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    # Leave first, so the others take over as these shards close.
    :pg.leave(scope(state.cluster), :members, self())
    :persistent_term.erase({__MODULE__, state.cluster})

    # Sent from this process, so each comes after any open of the shard.
    state.open
    |> Enum.concat(Map.keys(state.ops))
    |> Enum.reduce(:gen_server.reqids_new(), &Host.send_stop(state.cluster, &1, &1, &2))
    |> await_all()
  end

  defp await_all(requests) do
    case :gen_server.receive_response(requests, :infinity, true) do
      :no_request -> :ok
      {_response, _n, requests} -> await_all(requests)
    end
  end

  defp place(state) do
    nodes =
      scope(state.cluster)
      |> :pg.get_members(:members)
      |> Enum.map(&node/1)
      |> Enum.uniq()
      |> Enum.sort()

    state =
      if nodes != state.nodes do
        :persistent_term.put({__MODULE__, state.cluster}, placement(state.shards, nodes))

        Telemetry.execute([:distributed, :members], %{count: length(nodes)}, %{
          cluster: state.cluster
        })

        %{state | nodes: nodes}
      else
        state
      end

    t = now()
    placement = :persistent_term.get({__MODULE__, state.cluster}, %{})
    mine = MapSet.new(for {n, owner} <- placement, owner == node(), do: n)

    # Close what is no longer ours, at once, including shards still
    # opening: the host marks those as stopping (so calls no longer reach
    # them here) and closes them once they have opened.
    settled = state.open |> MapSet.difference(mine) |> Enum.reject(&Map.has_key?(state.ops, &1))
    opening = for {n, {:opening, _}} <- state.ops, not MapSet.member?(mine, n), do: n
    state = close(settled ++ opening, state)

    # Ours, not open and no task on it: open once it has been ours for
    # `settle`.
    pending =
      for n <- mine,
          not MapSet.member?(state.open, n),
          not Map.has_key?(state.ops, n),
          into: %{},
          do: {n, Map.get(state.pending, n, t)}

    opening = Enum.count(state.ops, &match?({_, {:opening, _}}, &1))

    due =
      for {n, since} <- Enum.sort(pending),
          t - since >= state.settle,
          Map.get(state.backoff, n, t) <= t,
          do: n

    state = %{
      state
      | pending: pending,
        backoff: Map.reject(state.backoff, fn {_, u} -> u <= t end)
    }

    open(state, Enum.take(due, max(state.max_concurrency - opening, 0)))
  end

  defp open(state, shards), do: Enum.reduce(shards, state, &request(&2, &1, :opening))

  defp close(shards, state) do
    for n <- shards,
        do: Telemetry.execute([:distributed, :close], %{}, %{cluster: state.cluster, shard: n})

    state = %{state | open: MapSet.difference(state.open, MapSet.new(shards))}
    Enum.reduce(shards, state, &request(&2, &1, :closing))
  end

  # A request to the host; its reply comes as a message (see handle_info/2).
  # A newer request on the same shard replaces it in `ops`, which makes its
  # reply stale.
  defp request(state, n, kind) do
    tag = make_ref()
    label = {kind, n, tag}

    requests =
      case kind do
        :opening -> Host.send_start(state.cluster, n, [], label, state.requests)
        :closing -> Host.send_stop(state.cluster, n, label, state.requests)
      end

    %{state | ops: Map.put(state.ops, n, {kind, tag}), requests: requests}
  end

  defp finished(state, {kind, n, tag}, result) do
    case state.ops do
      %{^n => {^kind, ^tag}} ->
        state = %{state | ops: Map.delete(state.ops, n)}
        if kind == :opening, do: open_finished(state, n, result), else: state

      _stale ->
        state
    end
  end

  defp open_finished(state, n, {:ok, _pid}) do
    Telemetry.execute([:distributed, :open], %{}, %{cluster: state.cluster, shard: n})
    opened(state, n)
  end

  defp open_finished(state, n, {:error, :already_started}), do: opened(state, n)

  defp open_finished(state, n, {:error, reason}) do
    Logger.error("#{inspect(state.cluster)}: shard #{n} failed to open: #{inspect(reason)}")
    %{state | backoff: Map.put(state.backoff, n, now() + state.settle)}
  end

  defp opened(state, n),
    do: %{state | open: MapSet.put(state.open, n), pending: Map.delete(state.pending, n)}

  defp now, do: System.monotonic_time(:millisecond)
end
