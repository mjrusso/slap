defmodule Slap.Cluster.Database do
  @moduledoc false
  use GenServer
  require Logger

  alias Slap.SlateDB

  # This process outlives the cluster tree. A killed ShardDb cannot interrupt
  # its native close, and a replacement must wait for that close to finish.
  def open(cluster, n, path, opts) do
    key = {cluster, n}

    case Registry.lookup(Slap.Cluster.Databases, key) do
      [{pid, owner}] ->
        if Process.alive?(owner) do
          {:error, :already_open}
        else
          ref = Process.monitor(pid)
          receive do: ({:DOWN, ^ref, :process, ^pid, _} -> open(cluster, n, path, opts))
        end

      [] ->
        spec = {__MODULE__, {key, self()}}

        with {:ok, pid} <- DynamicSupervisor.start_child(Slap.Cluster.DatabaseSupervisor, spec),
             {:ok, db} <- GenServer.call(pid, {:open, path, opts}, :infinity) do
          {:ok, pid, db}
        else
          {:error, {:already_started, _pid}} -> open(cluster, n, path, opts)
          {:error, _} = error -> error
        end
    end
  end

  # A normal or shutdown exit closed the handle in terminate/2 (this process
  # traps exits and its supervisor waits); any other exit may have interrupted
  # the close.
  def close(pid) do
    GenServer.call(pid, :close, :infinity)
  catch
    :exit, {reason, {GenServer, :call, _}} when reason in [:noproc, :normal, :shutdown] ->
      :ok
  end

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]},
      restart: :temporary,
      shutdown: :infinity
    }
  end

  def start_link({key, owner} = arg),
    do:
      GenServer.start_link(__MODULE__, arg,
        name: {:via, Registry, {Slap.Cluster.Databases, key, owner}}
      )

  @impl true
  def init({_key, owner}) do
    Process.flag(:trap_exit, true)
    ref = Process.monitor(owner)
    {:ok, %{owner_ref: ref, db: nil}}
  end

  @impl true
  def handle_call({:open, path, opts}, _from, state) do
    case SlateDB.open(path, opts) do
      {:ok, db} -> {:reply, {:ok, db}, %{state | db: db}}
      {:error, _} = error -> {:stop, :normal, error, state}
    end
  end

  def handle_call(:close, _from, state) do
    result = close_db(state.db)
    {:stop, :normal, result, %{state | db: nil}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner_ref: ref} = state),
    do: {:stop, :normal, state}

  @impl true
  def terminate(_reason, state), do: close_db(state.db)

  defp close_db(nil), do: :ok

  defp close_db(db) do
    case SlateDB.close(db) do
      :ok ->
        :ok

      {:error, %SlateDB.Error{kind: :closed, reason: :fenced}} = error ->
        error

      {:error, error} = result ->
        Logger.error("closing shard database failed: #{Exception.message(error)}")
        result
    end
  end
end
