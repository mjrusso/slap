defmodule Slap.Cluster.Host do
  @moduledoc """
  Starts and stops the shards placed on this node, for the strategy.

  Opens and closes run in tasks, so several run at once. If one of these
  tasks fails, the host stops: the shard may or may not be running, and
  the cluster's supervisor then stops every shard and starts over.

  Each shard runs under its own supervisor: the process that owns the
  database, then the application's shard children. The host watches them
  and reports every shard that stops to the strategy's
  `c:Slap.Cluster.Strategy.handle_shard_down/3`: `:fenced`, `:crashed` or
  `:stopped`. It never restarts a shard by itself; that is the strategy's
  decision.
  """

  use GenServer
  require Logger

  alias Slap.Cluster.{Config, Shard, ShardSupervisor}

  @doc """
  Opens shard `n` on this node and starts its children. Options:
  `:generation`, the strategy's lease generation (default `nil`).

  Returns `{:ok, pid}`, `{:error, :already_started}`, `{:error, :stopping}`
  (it is still closing), `{:error, :fenced}` (another node opened it while
  it was opening here), `{:error, :stopped}` (`stop_shard/2` was called
  while it was opening; it is closed, and was never `:running` since) or `{:error, reason}` (for example
  when the database cannot be opened). Opens run concurrently.
  """
  @spec start_shard(module(), non_neg_integer(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_shard(cluster, n, opts \\ []) do
    GenServer.call(Slap.Cluster.host(cluster), {:start, n, opts}, :infinity)
  end

  @doc """
  Stops shard `n` on this node: its children first, then the database is
  closed, which flushes it. A shard that is still opening counts as
  stopping at once (`Slap.Cluster.lookup/2` no longer finds it here), and
  is closed once it has opened. Returns `:ok` or `{:error, :not_running}`. Stops run
  concurrently.
  """
  @spec stop_shard(module(), non_neg_integer()) :: :ok | {:error, :not_running}
  def stop_shard(cluster, n) do
    GenServer.call(Slap.Cluster.host(cluster), {:stop, n}, :infinity)
  end

  @doc """
  `start_shard/3` and `stop_shard/2` without waiting: the request is sent
  from the calling process and added to `requests` (a `:gen_server`
  request id collection) under `label`; its reply is a message, for
  `:gen_server.check_response/3`. The host handles the requests of one
  process in the order they were sent, so a stop sent after a start is
  handled after it.
  """
  @spec send_start(
          module(),
          non_neg_integer(),
          keyword(),
          term(),
          :gen_server.request_id_collection()
        ) ::
          :gen_server.request_id_collection()
  def send_start(cluster, n, opts, label, requests),
    do: :gen_server.send_request(Slap.Cluster.host(cluster), {:start, n, opts}, label, requests)

  @doc "See `send_start/5`."
  @spec send_stop(module(), non_neg_integer(), term(), :gen_server.request_id_collection()) ::
          :gen_server.request_id_collection()
  def send_stop(cluster, n, label, requests),
    do: :gen_server.send_request(Slap.Cluster.host(cluster), {:stop, n}, label, requests)

  @doc "The shards running on this node."
  @spec local_shards(module()) :: [non_neg_integer()]
  def local_shards(cluster), do: GenServer.call(Slap.Cluster.host(cluster), :local_shards)

  @doc false
  def start_link(cluster),
    do: GenServer.start_link(__MODULE__, cluster, name: Slap.Cluster.host(cluster))

  @impl true
  def init(cluster) do
    # To mark shards as stopping when the cluster shuts down.
    Process.flag(:trap_exit, true)
    {strategy, _opts} = Config.get(cluster).strategy

    {:ok,
     %{
       cluster: cluster,
       strategy: strategy,
       shards: %{},
       starting: %{},
       # n => [{from, reply}], answered when the shard is closed.
       stopping: %{},
       # task ref => {:start | :stop, n}
       tasks: %{}
     }}
  end

  @impl true
  def handle_call({:start, n, opts}, from, state) do
    config = Config.get(state.cluster)

    cond do
      not (is_integer(n) and n >= 0 and n < config.shards) ->
        {:reply, {:error, :invalid_shard}, state}

      Map.has_key?(state.shards, n) or Map.has_key?(state.starting, n) ->
        {:reply, {:error, :already_started}, state}

      Map.has_key?(state.stopping, n) ->
        {:reply, {:error, :stopping}, state}

      true ->
        # Opening can take a while on object storage, so it runs outside
        # the host, and several shards open at once.
        status = Shard.new_status()
        generation = Keyword.get(opts, :generation)
        spec = {ShardSupervisor, {state.cluster, n, generation, status}}
        sup = Slap.Cluster.shard_supervisor(state.cluster, n)
        task = Task.async(fn -> DynamicSupervisor.start_child(sup, spec) end)
        starting = Map.put(state.starting, n, {from, status})

        {:noreply,
         %{state | starting: starting, tasks: Map.put(state.tasks, task.ref, {:start, n})}}
    end
  end

  # The shard counts as stopping until its database is closed: it cannot
  # be started again before then. A shard still opening is marked stopping
  # at once, and closed when its open finishes (see task_done/3).
  def handle_call({:stop, n}, from, state) do
    cond do
      Map.has_key?(state.shards, n) ->
        {shard, shards} = Map.pop!(state.shards, n)
        Process.demonitor(shard.ref, [:flush])
        state = stop_task(%{state | shards: shards}, n, shard.status, shard.pid)
        {:noreply, %{state | stopping: Map.put(state.stopping, n, [{from, :ok}])}}

      Map.has_key?(state.starting, n) ->
        {_opener, status} = Map.fetch!(state.starting, n)
        Shard.mark_stopping(status)
        {:noreply, stop_later(state, n, from, :ok)}

      Map.has_key?(state.stopping, n) ->
        {:noreply, stop_later(state, n, from, :ok)}

      true ->
        {:reply, {:error, :not_running}, state}
    end
  end

  def handle_call(:local_shards, _from, state) do
    {:reply, state.shards |> Map.keys() |> Enum.sort(), state}
  end

  @impl true
  def handle_info({ref, result}, state) when is_map_key(state.tasks, ref) do
    Process.demonitor(ref, [:flush])
    {op, state} = pop_task(state, ref)
    task_done(op, result, state)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state)
      when is_map_key(state.tasks, ref),
      do: {:stop, {:shard_task_failed, Map.fetch!(state.tasks, ref), reason}, state}

  def handle_info({:shard_fenced, n, _shard_db}, state) do
    case Map.fetch(state.shards, n) do
      {:ok, shard} ->
        state = terminate_shard(state, n, shard)
        report(state, n, :fenced)
        {:noreply, state}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.shards, fn {_n, shard} -> shard.ref == ref end) do
      {n, shard} ->
        down = if Shard.status(shard.status) == :fenced, do: :fenced, else: :crashed

        if down == :crashed do
          Logger.error("#{inspect(state.cluster)}: shard #{n} crashed: #{inspect(reason)}")
        end

        state = %{state | shards: Map.delete(state.shards, n)}
        report(state, n, down)
        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  defp pop_task(state, ref) do
    {op, tasks} = Map.pop!(state.tasks, ref)
    {op, %{state | tasks: tasks}}
  end

  defp task_done({:stop, n}, _result, state) do
    {froms, stopping} = Map.pop(state.stopping, n, [])
    for {from, reply} <- froms, do: GenServer.reply(from, reply)
    state = %{state | stopping: stopping}
    report(state, n, :stopped)
    {:noreply, state}
  end

  defp task_done({:start, n}, result, state) do
    {{from, status}, starting} = Map.pop!(state.starting, n)
    state = %{state | starting: starting}
    stop? = Map.has_key?(state.stopping, n)

    case result do
      # The opener is answered once the shard is closed, so that it does
      # not act (free a lease) while the shard still runs.
      {:ok, pid} when stop? ->
        state = stop_later(state, n, from, {:error, :stopped})
        {:noreply, stop_task(state, n, status, pid)}

      {:error, reason} when stop? ->
        GenServer.reply(from, {:error, unwrap(reason)})
        task_done({:stop, n}, :ok, state)

      {:ok, pid} ->
        if Shard.status(status) == :fenced do
          # Fenced while it was opening (the fence message found no running
          # shard): another node opened it. Stop it rather than register it.
          DynamicSupervisor.terminate_child(
            Slap.Cluster.shard_supervisor(state.cluster, n),
            pid
          )

          GenServer.reply(from, {:error, :fenced})
          {:noreply, state}
        else
          shard = %{pid: pid, ref: Process.monitor(pid), status: status}
          GenServer.reply(from, {:ok, pid})
          {:noreply, %{state | shards: Map.put(state.shards, n, shard)}}
        end

      {:error, reason} ->
        GenServer.reply(from, {:error, unwrap(reason)})
        {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    # The shard supervisors stop after the host (they start before it), so
    # the application's children can tell a shutdown from a fence.
    for {_n, shard} <- state.shards, do: Shard.mark_stopping(shard.status)
    for {_n, {_from, status}} <- state.starting, do: Shard.mark_stopping(status)
  end

  defp stop_later(state, n, from, reply),
    do: %{state | stopping: Map.update(state.stopping, n, [{from, reply}], &[{from, reply} | &1])}

  # Closing can take a while (it flushes), so it runs outside the host and
  # several shards close at once.
  defp stop_task(state, n, status, pid) do
    Shard.mark_stopping(status)
    sup = Slap.Cluster.shard_supervisor(state.cluster, n)
    task = Task.async(fn -> DynamicSupervisor.terminate_child(sup, pid) end)
    %{state | tasks: Map.put(state.tasks, task.ref, {:stop, n})}
  end

  defp terminate_shard(state, n, shard) do
    Process.demonitor(shard.ref, [:flush])

    DynamicSupervisor.terminate_child(
      Slap.Cluster.shard_supervisor(state.cluster, n),
      shard.pid
    )

    %{state | shards: Map.delete(state.shards, n)}
  end

  defp report(state, n, reason), do: state.strategy.handle_shard_down(state.cluster, n, reason)

  # DynamicSupervisor wraps a child's init error.
  defp unwrap({:shutdown, {:failed_to_start_child, _id, reason}}), do: unwrap(reason)
  defp unwrap(reason), do: reason
end
