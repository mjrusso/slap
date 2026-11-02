defmodule Slap.Inspect.Requests do
  @moduledoc false
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def submit(pid, id, connection, request),
    do: GenServer.cast(pid, {:submit, id, connection, request})

  @impl true
  def init(opts) do
    client = Keyword.fetch!(opts, :client)

    {:ok,
     %{
       client: client,
       client_ref: Process.monitor(client),
       read: Keyword.fetch!(opts, :read),
       active: nil,
       waiting: nil
     }}
  end

  @impl true
  def handle_cast({:submit, id, connection, request}, state) do
    case state.active do
      nil -> {:noreply, launch(state, {id, connection, request})}
      _active -> {:noreply, %{state | waiting: {id, connection, request}}}
    end
  end

  defp launch(state, {id, connection, request}) do
    parent = self()
    read = state.read
    request = Map.put(request, :observer, parent)
    {_pid, ref} = spawn_monitor(fn -> send(parent, {:result, id, read.(connection, request)}) end)
    timer = Process.send_after(self(), {:stalled, id}, request.budget + 2000)

    %{
      state
      | active: %{id: id, ref: ref, timer: timer, owner: nil, shard: Map.get(request, :shard)},
        waiting: nil
    }
  end

  @impl true
  def handle_info({:stalled, id}, %{active: %{id: id}} = state) do
    case state.active.owner do
      nil -> send(state.client, {:inspection_stalled, id})
      owner -> send(state.client, {:inspection_stalled, id, owner, state.active.shard})
    end

    {:noreply, state}
  end

  def handle_info({:inspection_owner, owner, shard}, %{active: active} = state)
      when not is_nil(active) do
    send(state.client, {:inspection_owner, active.id, owner, shard})
    {:noreply, %{state | active: %{active | owner: owner, shard: shard}}}
  end

  def handle_info({:result, id, result}, %{active: %{id: id} = active} = state) do
    Process.cancel_timer(active.timer)
    Process.demonitor(active.ref, [:flush])
    if is_nil(state.waiting), do: send(state.client, {:inspection_result, id, result})
    {:noreply, next(%{state | active: nil})}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{client_ref: ref} = state),
    do: {:stop, :normal, state}

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{active: %{ref: ref} = active} = state) do
    handle_info({:result, active.id, {:error, :request_failed}}, state)
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp next(%{waiting: nil} = state), do: state
  defp next(%{waiting: waiting} = state), do: launch(state, waiting)
end
