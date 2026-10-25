defmodule Slap.SnapshotLog.Recovery do
  @moduledoc false
  use GenServer

  alias Slap.SnapshotLog.{Control, Lifecycle, Store}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def sweep(timeout \\ 30_000), do: GenServer.call(__MODULE__, :sweep, timeout)

  @impl true
  def init(_opts) do
    Process.send_after(self(), :tick, 60_000)
    {:ok, %{task: nil, waiters: [], waiting: []}}
  end

  @impl true
  def handle_call(:sweep, from, %{task: nil} = state),
    do: {:noreply, start(%{state | waiters: [from]})}

  def handle_call(:sweep, from, state),
    do: {:noreply, %{state | waiting: [from | state.waiting]}}

  @impl true
  def handle_info(:tick, state) do
    Process.send_after(self(), :tick, 60_000)
    {:noreply, start(state)}
  end

  def handle_info({ref, _result}, %{task: ref} = state) do
    Process.demonitor(ref, [:flush])
    Enum.each(state.waiters, &GenServer.reply(&1, :ok))
    next = %{state | task: nil, waiters: state.waiting, waiting: []}
    {:noreply, if(next.waiters == [], do: next, else: start(next))}
  end

  def handle_info({:DOWN, _ref, :process, _pid, :normal}, state), do: {:noreply, state}

  defp start(%{task: nil} = state) do
    task =
      Task.async(fn ->
        Slap.Streams.each_local_work(&recover/1)
      end)

    %{state | task: task.ref}
  end

  defp start(state), do: state

  defp recover({cluster, path}) do
    if String.ends_with?(path, "/.control") do
      store = Store.new(String.replace_suffix(path, "/.control", ""), cluster: cluster)
      deadline = Control.deadline(30_000)

      Lifecycle.recover(store, deadline)
    end
  end
end
