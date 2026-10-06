defmodule Slap.Cluster.Strategy.Local do
  @moduledoc """
  Places every shard on this node. For single-node deployments, development
  and CI.

  There is no failover: if the node goes down, its shards are unavailable
  until it restarts. Run one instance (for example a Kubernetes Deployment
  with `replicas: 1` and `strategy: Recreate`).

  A shard that fails to open, crashes or is fenced is opened again after an
  exponential backoff. Fencing in this mode means a second instance opened
  the shard, which is logged at error level; the two instances then keep
  taking the shard from each other, slowly, until one stops. SlateDB's
  fencing keeps the data safe meanwhile.

  Options:

    * `:backoff_base` - the first retry delay in ms (default 1,000).
    * `:backoff_max` - the longest delay in ms (default 60,000). A shard
      that stays up this long starts again from `:backoff_base`.
    * `:max_concurrency` - shards opened at once (default 16).
    * `:shards` - the shards to open here (default all of them).
      `Slap.Cluster.Strategy.Static` uses it.
  """

  @behaviour Slap.Cluster.Strategy

  use GenServer
  require Logger

  alias Slap.Cluster.{Config, Host}
  alias Slap.Cluster.Strategy, as: StrategyOptions

  @impl Slap.Cluster.Strategy
  def validate_options(opts) do
    StrategyOptions.validate_options!(
      opts,
      [:backoff_base, :backoff_max, :max_concurrency, :shards],
      [:backoff_base, :backoff_max, :max_concurrency]
    )
  end

  @impl Slap.Cluster.Strategy
  def lookup(cluster, n) do
    case Registry.lookup(Slap.Cluster.registry(cluster), {:shard, n}) do
      [{pid, _ctx}] -> {:ok, {:local, pid}}
      [] -> {:error, :unassigned}
    end
  end

  @impl Slap.Cluster.Strategy
  def handle_shard_down(cluster, n, reason) do
    GenServer.cast(name(cluster), {:down, n, reason})
  end

  def start_link(opts) do
    cluster = Keyword.fetch!(opts, :cluster)
    GenServer.start_link(__MODULE__, opts, name: name(cluster))
  end

  defp name(cluster), do: Module.concat(cluster, Strategy)

  @impl GenServer
  def init(opts) do
    cluster = Keyword.fetch!(opts, :cluster)

    state = %{
      cluster: cluster,
      base: Keyword.get(opts, :backoff_base, 1_000),
      max: Keyword.get(opts, :backoff_max, 60_000),
      max_concurrency: Keyword.get(opts, :max_concurrency, 16),
      # n => {attempts, started_at}
      shards: %{}
    }

    # Opening in init means the cluster's start_link returns once every shard
    # has opened (or failed to, and will be retried).
    shards =
      Keyword.get_lazy(opts, :shards, fn -> Enum.to_list(0..(Config.get(cluster).shards - 1)) end)

    {:ok, start(state, shards)}
  end

  @impl GenServer
  def handle_info({:retry, n}, state), do: {:noreply, start(state, [n])}

  @impl GenServer
  def handle_cast({:down, _n, :stopped}, state), do: {:noreply, state}

  def handle_cast({:down, n, reason}, state) do
    level = if reason == :fenced, do: :error, else: :warning

    Logger.log(
      level,
      "#{inspect(state.cluster)}: shard #{n} is down (#{reason})" <>
        if(reason == :fenced,
          do: "; another writer opened it. Is a second instance running?",
          else: ""
        )
    )

    {:noreply, retry_later(state, n)}
  end

  defp start(state, shards) do
    now = System.monotonic_time(:millisecond)

    shards
    |> Task.async_stream(&{&1, Host.start_shard(state.cluster, &1)},
      max_concurrency: state.max_concurrency,
      timeout: :infinity
    )
    |> Enum.reduce(state, fn
      {:ok, {n, {:ok, _pid}}}, state ->
        {attempts, _} = Map.get(state.shards, n, {0, now})
        put_in(state.shards[n], {attempts, now})

      {:ok, {_n, {:error, :already_started}}}, state ->
        state

      {:ok, {n, {:error, reason}}}, state ->
        Logger.error("#{inspect(state.cluster)}: shard #{n} failed to open: #{inspect(reason)}")
        retry_later(state, n)
    end)
  end

  defp retry_later(state, n) do
    now = System.monotonic_time(:millisecond)
    {attempts, started_at} = Map.get(state.shards, n, {0, now})
    # A shard that stayed up for a while starts over from the base delay.
    attempts = if now - started_at >= state.max, do: 0, else: attempts
    delay = min(state.base * Integer.pow(2, attempts), state.max)
    jitter = :rand.uniform(max(div(delay, 4), 1))
    Process.send_after(self(), {:retry, n}, delay + jitter)
    put_in(state.shards[n], {attempts + 1, now})
  end
end
