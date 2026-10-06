defmodule Slap.Cluster.Strategy.LeaseLoop do
  @moduledoc """
  The placement loop of `Slap.Cluster.Strategy.ObjectLease` (leases in
  the object store), behind a storage behaviour for other lease stores:
  one lease per shard, a heartbeat per node, fair shares, and self-fencing.
  Where the leases live is a `Slap.Cluster.Strategy.LeaseLoop.Store`.

  Every `:interval` (a third of `:lease_ttl` by default), each node:

    1. heartbeats;
    2. renews its leases, and stops any shard whose lease it no longer holds;
    3. computes its fair share, `ceil(shards / live nodes)`, where a node is
       live if it heartbeated within the TTL (three heartbeats), so a dead
       node stops counting when its leases expire;
    4. releases shards above its share: it stops the shard (which closes and
       flushes it) and only then frees the lease, so the next owner opens a
       closed database;
    5. reads every lease (for `lookup/2`), and claims free or expired ones up
       to its share, opening those shards with the lease's generation.

  Shards open and close in a process of their own, the runner, so
  renewals keep their pace however long a rebalance takes. A shard keeps
  its lease (and it is renewed) while it opens or closes. After a failed
  renewal the loop tries again after half the interval.

  **Self-fencing.** The runner (not the loop, which may be stuck in a slow
  call to the lease store) keeps a deadline per shard: its last successful
  renewal or claim, plus the TTL, minus `:margin`. A shard whose deadline
  passes is stopped before its lease can expire, and one still waiting to
  open (`:max_concurrency` are opened at once) is not opened. Every open
  and close goes to the host from the runner, so the host gets each
  shard's in order: a stop always comes after the open it follows. If the
  runner exits, the loop stops too, and the cluster's supervisor stops
  every shard. If a node is too slow even for that (a long pause), the new
  owner's open fences it in SlateDB, so there is still at most one
  effective writer.

  A failed read of the owners skips the claims of that round, and
  `lookup/2` keeps the owners of the last successful read. An owner is
  routed to only if its name is already an atom on this node (it is
  connected, was connected, or is named in configuration such as a peer
  discovery topology): lease data never creates atoms. A node that has
  never heard of an owner treats its shards as unassigned until the nodes
  connect, so the nodes need peer discovery (for example libcluster).

  Options common to the lease strategies: `:lease_ttl` (ms, default
  15,000), `:interval` (default a third of the TTL), `:margin` (default a sixth of
  the TTL), `:name` (this node's name, default `node()`; `call/4` routes to
  it), `:cluster_name` (default the cluster module's name), and
  `:max_concurrency` (shards opened at once, default 16).

  Telemetry: `[:slap, :cluster, :lease, :claim | :release | :lost]` with
  `%{cluster, shard}`, `[:lease, :self_fence]` with `%{cluster, shards}`, and
  `[:lease, :renew_failed]`.
  """

  use GenServer
  require Logger

  @doc """
  Makes the calling module a lease strategy whose leases it keeps itself: it
  implements `Slap.Cluster.Strategy` with this loop, and must implement
  `Slap.Cluster.Strategy.LeaseLoop.Store`. The strategy's options are
  given to the store's `init/2` as well as to the loop.
  """
  defmacro __using__(_opts) do
    loop = __MODULE__

    quote do
      @behaviour Slap.Cluster.Strategy
      @behaviour unquote(loop).Store

      @impl Slap.Cluster.Strategy
      def child_spec(opts),
        do: unquote(loop).child_spec([lease_store: {__MODULE__, opts}] ++ opts)

      @impl Slap.Cluster.Strategy
      defdelegate lookup(cluster, n), to: unquote(loop)

      @impl Slap.Cluster.Strategy
      defdelegate handle_shard_down(cluster, n, reason), to: unquote(loop)

      @impl Slap.Cluster.Strategy
      defdelegate refresh(cluster, timeout), to: unquote(loop)
    end
  end

  alias Slap.Cluster.{Config, Telemetry}
  alias Slap.Cluster.Strategy.LeaseLoop.Runner

  defmodule Store do
    @moduledoc """
    Where a lease strategy keeps its leases and heartbeats. Each callback gets
    the store's state and returns it, possibly changed. A lease is
    `{owner, generation}` while unexpired; `owners/1` returns only those.
    Every call must return within the `:timeout` given to `init/2`.
    """

    @type state :: term()
    @type shard :: non_neg_integer()
    @type ctx :: %{
            cluster: module(),
            cluster_name: String.t(),
            me: String.t(),
            ttl: pos_integer(),
            shards: pos_integer(),
            timeout: pos_integer()
          }

    @callback init(opts :: keyword(), ctx) :: state()
    @doc """
    Creates what is missing. A transient error is retried. Return
    `{:error, {:unsupported, reason}}` when the store cannot meet the lease
    protocol's requirements.
    """
    @callback setup(state()) :: {:ok | {:error, term()}, state()}
    @callback heartbeat(state()) :: {:ok | {:error, term()}, state()}
    @doc """
    Renews the leases of `shards` held by this node; returns those renewed.
    The others are lost. If some calls failed, it returns `{:error, reason,
    renewed, failed}`: `failed` are the shards whose lease may or may not
    still be held (the next renewal tries them again), and the others not
    renewed are lost.
    """
    @callback renew(state(), [shard]) ::
                {{:ok, [shard]} | {:error, term(), [shard], [shard]}, state()}
    @callback live_nodes(state()) :: {{:ok, pos_integer()} | {:error, term()}, state()}
    @doc "The owner of every unexpired lease."
    @callback owners(state()) :: {{:ok, %{shard => String.t()}} | {:error, term()}, state()}
    @doc "Claims up to `count` free or expired leases; returns `[{shard, generation}]`."
    @callback claim(state(), count :: pos_integer()) ::
                {{:ok, [{shard, non_neg_integer()}]} | {:error, term()}, state()}
    @doc "Frees this node's lease of `shard`, if it still holds it."
    @callback release(state(), shard) :: state()
    @doc "Frees every lease of this node and removes its heartbeat (clean stop)."
    @callback leave(state()) :: state()
  end

  # -- For the strategies ---------------------------------------------------

  @doc false
  def child_spec(opts),
    do: %{id: :strategy, start: {__MODULE__, :start_link, [opts]}, shutdown: 60_000}

  @doc false
  def start_link(opts) do
    cluster = Keyword.fetch!(opts, :cluster)
    GenServer.start_link(__MODULE__, opts, name: name(cluster))
  end

  @doc false
  def lookup(cluster, n) do
    case :ets.lookup(table(cluster), n) do
      [{^n, owner}] when owner != node() -> {:ok, {:remote, owner}}
      _ -> {:error, :unassigned}
    end
  rescue
    ArgumentError -> {:error, :unassigned}
  end

  @doc false
  def handle_shard_down(cluster, n, reason), do: GenServer.cast(name(cluster), {:down, n, reason})

  @doc false
  def refresh(cluster, timeout) do
    GenServer.call(name(cluster), :refresh, timeout)
  catch
    :exit, _ -> :ok
  end

  defp name(cluster), do: Module.concat(cluster, Strategy)
  defp table(cluster), do: Module.concat(cluster, LeaseTable)

  # -- Server ---------------------------------------------------------------

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    cluster = Keyword.fetch!(opts, :cluster)
    {store, store_opts} = Keyword.fetch!(opts, :lease_store)
    ttl = Keyword.get(opts, :lease_ttl, 15_000)
    interval = Keyword.get(opts, :interval, div(ttl, 3))

    ctx = %{
      cluster: cluster,
      cluster_name: Keyword.get(opts, :cluster_name, inspect(cluster)),
      me: opts |> Keyword.get(:name, node()) |> to_string(),
      ttl: ttl,
      shards: Config.get(cluster).shards,
      timeout: max(div(interval, 2), 100)
    }

    :ets.new(table(cluster), [:named_table, :protected, :set, read_concurrency: true])
    max_concurrency = Keyword.get(opts, :max_concurrency, 16)
    {:ok, runner} = Runner.start_link(cluster, max_concurrency)

    state = %{
      cluster: cluster,
      store: store,
      store_state: store.init(store_opts, ctx),
      ttl: ttl,
      interval: interval,
      margin: Keyword.get(opts, :margin, div(ttl, 6)),
      shards: ctx.shards,
      me: ctx.me,
      # shard => generation, for the leases held here (the shard is open,
      # opening, or closing).
      mine: %{},
      # shard => :opening | :closing, while the runner opens or closes it.
      ops: %{},
      runner: runner,
      ready: false
    }

    case attempt_setup(state) do
      {:ok, state} ->
        {:ok, state, {:continue, :tick}}

      {{:unsupported, reason}, _state} ->
        {:stop, {:unsupported, reason}}

      {:retry, state} ->
        Process.send_after(self(), :tick, max(div(state.interval, 2), 1))
        {:ok, state}
    end
  end

  @impl true
  def handle_continue(:tick, state), do: {:noreply, tick(state)}

  @impl true
  def handle_info(:tick, state), do: {:noreply, tick(state)}

  # An open finished. The lease may have been lost meanwhile: then close it.
  def handle_info({:opened, n, g, result}, state) do
    state = %{state | ops: Map.delete(state.ops, n)}
    held? = Map.get(state.mine, n) == g

    case result do
      {:ok, _pid} when held? ->
        Telemetry.execute([:lease, :claim], %{}, %{cluster: state.cluster, shard: n})
        {:noreply, state}

      {:ok, _pid} ->
        {:noreply, close(state, n, false)}

      # Its lease was dropped while it waited to open.
      {:error, :cancelled} ->
        {:noreply, state}

      {:error, reason} ->
        Logger.error("#{inspect(state.cluster)}: shard #{n} failed to open: #{inspect(reason)}")
        {:noreply, if(held?, do: drop(state, n, true), else: state)}
    end
  end

  # A close finished: free the lease if it is still held here.
  def handle_info({:closed, n, release?}, state) do
    state = %{state | ops: Map.delete(state.ops, n)}

    if release? and Map.has_key?(state.mine, n) do
      Telemetry.execute([:lease, :release], %{}, %{cluster: state.cluster, shard: n})
      {:noreply, drop(state, n, true)}
    else
      {:noreply, state}
    end
  end

  # The runner stopped these shards: their leases were about to expire,
  # and expire on their own (the store is likely unreachable).
  def handle_info({:self_fenced, shards}, state) do
    state = %{state | ops: Map.drop(state.ops, shards)}
    {:noreply, Enum.reduce(shards, state, &drop(&2, &1, false))}
  end

  # Without the runner, nothing stops the shards whose leases expire.
  def handle_info({:EXIT, pid, reason}, %{runner: pid} = state),
    do: {:stop, {:runner_exit, reason}, state}

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def handle_call(:refresh, _from, state), do: {:reply, :ok, refresh_table(state)}

  @impl true
  def handle_cast({:down, n, reason}, state) when reason in [:fenced, :crashed] do
    Logger.warning("#{inspect(state.cluster)}: shard #{n} is down (#{reason})")

    # A crashed shard's lease is freed so it can be claimed again (perhaps
    # here). A fenced one belongs to whoever opened it.
    Runner.down(state.runner, n)
    {:noreply, drop(state, n, reason == :crashed)}
  end

  def handle_cast({:down, _n, :stopped}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # A clean handoff: close every shard (at once), then free the leases, so
    # the next owners open closed databases right away. Without the runner,
    # the shards stop with the cluster's supervisor.
    try do
      Runner.stop_all(state.runner)
    catch
      :exit, _ -> :ok
    end

    state.store.leave(state.store_state)
  end

  # -- The loop -------------------------------------------------------------

  defp setup(state) do
    case attempt_setup(state) do
      {:ok, state} ->
        state

      {{:unsupported, reason}, _state} ->
        # The first attempt may have failed transiently. Restart through init
        # so the supervisor reports this capability failure as a start error.
        exit({:unsupported, reason})

      {:retry, state} ->
        state
    end
  end

  defp attempt_setup(state) do
    case store(state, :setup, []) do
      {:ok, state} ->
        {:ok, %{state | ready: true}}

      {{:error, {:unsupported, reason}}, state} ->
        {{:unsupported, reason}, state}

      {{:error, error}, state} ->
        Logger.error("#{inspect(state.cluster)}: lease store setup failed: #{inspect(error)}")
        {:retry, state}
    end
  end

  defp tick(state) do
    state = if state.ready, do: state, else: setup(state)
    started = System.monotonic_time(:millisecond)

    {ok?, state} =
      if state.ready do
        with {:ok, state} <- store(state, :heartbeat, []),
             {:ok, state} <- renew(state, started),
             {{:ok, live}, state} <- store(state, :live_nodes, []),
             target = div(state.shards + live - 1, live),
             state = release_excess(state, target),
             # Claims are made from this read: none without it.
             {{:ok, owners}, state} <- store(state, :owners, []) do
          {claimed, state} = claim(state, target)
          {true, write_table(state, Map.merge(owners, claimed))}
        else
          {{:error, error}, state} -> {false, store_failed(state, error)}
        end
      else
        {false, state}
      end

    # After a failure, try again sooner: the runner's deadline is closer.
    delay = if ok?, do: state.interval, else: div(state.interval, 2)
    elapsed = System.monotonic_time(:millisecond) - started
    Process.send_after(self(), :tick, max(delay - elapsed, 0))
    state
  end

  defp store_failed(state, error) do
    Logger.warning("#{inspect(state.cluster)}: lease store call failed: #{inspect(error)}")
    Telemetry.execute([:lease, :renew_failed], %{}, %{cluster: state.cluster})
    state
  end

  defp store(state, fun, args) do
    {result, store_state} = apply(state.store, fun, [state.store_state | args])
    {result, %{state | store_state: store_state}}
  end

  # Renews every lease held here. A shard whose lease is gone (it expired and
  # another node claimed it) is closed at once, even when other renewals
  # failed. After a partial failure, the leases renewed keep their shards
  # open longer.
  defp renew(state, started) do
    mine = Map.keys(state.mine)
    deadline = started + state.ttl - state.margin

    case store(state, :renew, [mine]) do
      {{:ok, renewed}, state} ->
        Runner.renewed(state.runner, deadline, renewed)
        lost = mine -- renewed
        {:ok, Enum.reduce(lost, state, &lost_lease/2)}

      {{:error, error, renewed, failed}, state} ->
        Runner.renewed(state.runner, deadline, renewed)
        lost = mine -- (renewed ++ failed)
        {{:error, error}, Enum.reduce(lost, state, &lost_lease/2)}
    end
  end

  defp lost_lease(n, state) do
    Logger.warning("#{inspect(state.cluster)}: lost the lease of shard #{n}")
    Telemetry.execute([:lease, :lost], %{}, %{cluster: state.cluster, shard: n})
    state |> drop(n, false) |> close(n, false)
  end

  # Closes shard `n` (unless it is still opening; an open that finishes
  # checks the lease). Then frees its lease if `release?`.
  defp close(state, n, release?) do
    case Map.fetch(state.ops, n) do
      {:ok, _} ->
        state

      :error ->
        Runner.close(state.runner, n, release?)
        %{state | ops: Map.put(state.ops, n, :closing)}
    end
  end

  # Forgets the lease of `n` (freeing it in the store if `release?`).
  defp drop(state, n, release?) do
    if Map.has_key?(state.mine, n) do
      Runner.drop(state.runner, [n])
      state = if release?, do: release_lease(state, n), else: state
      %{state | mine: Map.delete(state.mine, n)}
    else
      state
    end
  end

  # Closes the highest-numbered shards above the target (in tasks); their
  # leases are freed once they are closed.
  defp release_excess(state, target) do
    settled = for {n, _} <- state.mine, not Map.has_key?(state.ops, n), do: n
    excess = active(state) - target

    if excess > 0 do
      settled
      |> Enum.sort(:desc)
      |> Enum.take(excess)
      |> Enum.reduce(state, &close(&2, &1, true))
    else
      state
    end
  end

  # Leases held for shards that are not closing.
  defp active(state),
    do: Enum.count(state.mine, fn {n, _} -> state.ops[n] != :closing end)

  # Claims free or expired leases up to the target, and has the runner open
  # them. Each claimed shard has its own deadline from the claim.
  defp claim(state, target) do
    count = target - active(state)
    started = System.monotonic_time(:millisecond)

    {claimed, state} =
      if count > 0 do
        case store(state, :claim, [count]) do
          {{:ok, claimed}, state} -> {claimed, state}
          {{:error, _}, state} -> {[], state}
        end
      else
        {[], state}
      end

    claimed = Enum.reject(claimed, fn {n, _g} -> Map.has_key?(state.ops, n) end)

    if claimed == [] do
      {%{}, state}
    else
      shards = Enum.map(claimed, &elem(&1, 0))
      Runner.open(state.runner, claimed, started + state.ttl - state.margin)

      state = %{
        state
        | mine: Enum.into(claimed, state.mine),
          ops: Enum.into(shards, state.ops, &{&1, :opening})
      }

      {Map.new(shards, &{&1, state.me}), state}
    end
  end

  defp release_lease(state, n),
    do: %{state | store_state: state.store.release(state.store_state, n)}

  # Reads the owners for lookup/2 now. After a failed read, lookup/2 keeps
  # the owners it has.
  defp refresh_table(state) do
    case store(state, :owners, []) do
      {{:ok, owners}, state} -> write_table(state, owners)
      {{:error, _}, state} -> state
    end
  end

  defp write_table(state, owners) do
    owners =
      for {n, owner} <- owners, {:ok, node} <- [owner_node(owner)], into: %{}, do: {n, node}

    :ets.insert(table(state.cluster), Map.to_list(owners))

    for n <- 0..(state.shards - 1),
        not Map.has_key?(owners, n),
        do: :ets.delete(table(state.cluster), n)

    state
  end

  # Owners come from the lease store, so they are not made into atoms: the
  # atom table is never collected.
  defp owner_node(owner) do
    if String.contains?(owner, "@"), do: {:ok, String.to_existing_atom(owner)}, else: :error
  rescue
    ArgumentError -> :error
  end
end
