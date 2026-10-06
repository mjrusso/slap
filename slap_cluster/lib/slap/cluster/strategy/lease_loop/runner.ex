defmodule Slap.Cluster.Strategy.LeaseLoop.Runner do
  @moduledoc false
  # Opens and closes the lease loop's shards on this node, and stops each
  # shard whose deadline passes (self-fencing). It is a process of its own,
  # since the loop may be stuck in a call to the lease store, which is when
  # self-fencing is needed; it never calls the store.
  #
  # Every request to the host comes from this process, so the host gets the
  # requests for a shard in the order they were made: a stop always comes
  # after the open it follows, and the host closes the shard once it has
  # opened. Up to `max_concurrency` opens are in flight; the others wait
  # here, and one whose deadline passes or whose lease the loop drops while
  # it waits is not opened.
  #
  # To the loop: `{:opened, n, generation, result}` for each open (result
  # `{:error, :cancelled}` for one that was not run), `{:closed, n,
  # release?}` for each close, and `{:self_fenced, [n]}` once a shard
  # stopped for its deadline is closed (or its waiting open dropped).

  use GenServer
  require Logger

  alias Slap.Cluster.{Host, Telemetry}

  def start_link(cluster, max_concurrency),
    do: GenServer.start_link(__MODULE__, {cluster, max_concurrency, self()})

  @doc "Opens `claimed` (`[{n, generation}]`), whose deadline is `deadline`."
  def open(runner, claimed, deadline), do: GenServer.cast(runner, {:open, claimed, deadline})

  @doc "Closes shard `n`, which is open (or failed to open)."
  def close(runner, n, release?), do: GenServer.cast(runner, {:close, n, release?})

  @doc "Moves the deadline of `shards` (renewed) to `deadline`."
  def renewed(runner, deadline, shards), do: GenServer.cast(runner, {:renewed, deadline, shards})

  @doc "Forgets the deadlines of `shards`, whose leases are no longer held."
  def drop(runner, shards), do: GenServer.cast(runner, {:drop, shards})

  @doc "Forgets shard `n`, which stopped by itself (fenced or crashed)."
  def down(runner, n), do: GenServer.cast(runner, {:down, n})

  @doc "Closes every shard, and returns once they are closed."
  def stop_all(runner), do: GenServer.call(runner, :stop_all, :infinity)

  @impl true
  def init({cluster, max, loop}) do
    {:ok,
     %{
       cluster: cluster,
       loop: loop,
       max: max,
       # shard => ms (monotonic) by which it must be stopped.
       deadlines: %{},
       # Opens waiting for a slot, [{n, generation}], oldest first.
       waiting: [],
       # shard => {:opening, tag, generation} | {:closing, tag, why}, while a
       # request to the host is in flight. A newer request on the shard
       # replaces it, which makes the older reply stale.
       ops: %{},
       # The shards open here (their open succeeded), not closing.
       open: MapSet.new(),
       requests: :gen_server.reqids_new(),
       # The caller of stop_all/1, answered once every shard is closed.
       stopping: nil
     }}
  end

  @impl true
  def handle_cast({:open, claimed, deadline}, state) do
    state = %{state | waiting: state.waiting ++ claimed}
    state |> put_deadlines(deadline, Enum.map(claimed, &elem(&1, 0))) |> continue()
  end

  def handle_cast({:close, n, release?}, state) do
    if MapSet.member?(state.open, n) do
      state |> stop(n, {:release, release?}) |> continue()
    else
      send(state.loop, {:closed, n, release?})
      continue(state)
    end
  end

  def handle_cast({:renewed, deadline, shards}, state),
    do: state |> put_deadlines(deadline, shards) |> continue()

  def handle_cast({:drop, shards}, state),
    do: continue(%{state | deadlines: Map.drop(state.deadlines, shards)})

  def handle_cast({:down, n}, state),
    do:
      continue(%{
        state
        | open: MapSet.delete(state.open, n),
          deadlines: Map.delete(state.deadlines, n)
      })

  @impl true
  def handle_call(:stop_all, from, state) do
    shards = Enum.uniq(Enum.to_list(state.open) ++ Map.keys(state.ops))
    state = %{state | waiting: [], deadlines: %{}, stopping: from}
    shards |> Enum.reduce(state, &stop(&2, &1, :all)) |> continue()
  end

  @impl true
  # The next deadline (see continue/1).
  def handle_info(:timeout, state), do: continue(state)

  def handle_info(msg, state) do
    case :gen_server.check_response(msg, state.requests, true) do
      {{:reply, result}, label, requests} ->
        %{state | requests: requests} |> finished(label, result) |> continue()

      # The host is gone: the cluster's supervisor starts over.
      {{:error, {reason, _host}}, _label, _requests} ->
        {:stop, {:host_down, reason}, state}

      _not_a_response ->
        continue(state)
    end
  end

  defp put_deadlines(state, deadline, shards),
    do: %{state | deadlines: Enum.into(shards, state.deadlines, &{&1, deadline})}

  defp finished(state, {kind, n, tag}, result) do
    case state.ops do
      %{^n => {^kind, ^tag, info}} ->
        done(%{state | ops: Map.delete(state.ops, n)}, n, kind, info, result)

      _stale ->
        state
    end
  end

  defp done(state, n, :opening, generation, result) do
    state =
      case result do
        {:ok, _pid} -> %{state | open: MapSet.put(state.open, n)}
        {:error, :already_started} -> %{state | open: MapSet.put(state.open, n)}
        {:error, _} -> state
      end

    send(state.loop, {:opened, n, generation, result})
    state
  end

  defp done(state, n, :closing, {:release, release?}, _result) do
    send(state.loop, {:closed, n, release?})
    state
  end

  defp done(state, n, :closing, :fence, _result) do
    send(state.loop, {:self_fenced, [n]})
    state
  end

  defp done(state, _n, :closing, :all, _result), do: state

  # After every message: self-fence, start waiting opens, answer stop_all/1,
  # and wake up at the next deadline.
  defp continue(state) do
    state = state |> fence() |> start_waiting() |> answer_stop_all()

    timeout =
      case Map.values(state.deadlines) do
        [] -> :infinity
        values -> max(Enum.min(values) - now(), 0)
      end

    {:noreply, state, timeout}
  end

  # Stops the shards whose deadline has passed, all at once: their leases
  # are about to expire (the store is likely unreachable).
  defp fence(state) do
    t = now()
    {expired, deadlines} = Map.split_with(state.deadlines, fn {_n, d} -> d <= t end)
    state = %{state | deadlines: deadlines}

    case expired |> Map.keys() |> Enum.filter(&running?(state, &1)) |> Enum.sort() do
      [] ->
        state

      shards ->
        Logger.error(
          "#{inspect(state.cluster)}: cannot renew leases; stopping shards #{inspect(shards)}"
        )

        Telemetry.execute([:lease, :self_fence], %{}, %{cluster: state.cluster, shards: shards})
        Enum.reduce(shards, state, &fence_one(&2, &1))
    end
  end

  defp running?(state, n) do
    MapSet.member?(state.open, n) or match?(%{^n => {:opening, _, _}}, state.ops) or
      List.keymember?(state.waiting, n, 0)
  end

  defp fence_one(state, n) do
    if List.keymember?(state.waiting, n, 0) do
      send(state.loop, {:self_fenced, [n]})
      %{state | waiting: List.keydelete(state.waiting, n, 0)}
    else
      stop(state, n, :fence)
    end
  end

  # Opens waiting shards while there are free slots. A shard whose lease
  # the loop dropped meanwhile (no deadline) is not opened.
  defp start_waiting(%{stopping: nil, waiting: [{n, generation} | rest]} = state) do
    cond do
      opening(state) >= state.max ->
        state

      Map.has_key?(state.deadlines, n) ->
        state = %{state | waiting: rest}

        state
        |> request(
          n,
          :opening,
          generation,
          &Host.send_start(state.cluster, n, [generation: generation], &1, &2)
        )
        |> start_waiting()

      true ->
        send(state.loop, {:opened, n, generation, {:error, :cancelled}})
        start_waiting(%{state | waiting: rest})
    end
  end

  defp start_waiting(state), do: state

  defp opening(state), do: Enum.count(state.ops, &match?({_, {:opening, _, _}}, &1))

  defp stop(state, n, why) do
    state = %{state | open: MapSet.delete(state.open, n)}
    request(state, n, :closing, why, &Host.send_stop(state.cluster, n, &1, &2))
  end

  defp request(state, n, kind, info, send_request) do
    tag = make_ref()
    requests = send_request.({kind, n, tag}, state.requests)
    %{state | ops: Map.put(state.ops, n, {kind, tag, info}), requests: requests}
  end

  defp answer_stop_all(%{stopping: nil} = state), do: state

  defp answer_stop_all(state) do
    if Enum.any?(state.ops, &match?({_, {:closing, _, :all}}, &1)) do
      state
    else
      GenServer.reply(state.stopping, :ok)
      %{state | stopping: nil}
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
