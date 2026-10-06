defmodule Slap.Cluster.Test.Counter do
  @moduledoc false
  # The stub application from the spec: one counter server per shard. Each
  # increment writes the next value without waiting and is acknowledged once
  # it is durable, so an acknowledged value is never lost.

  use GenServer

  alias Slap.Cluster
  alias Slap.SlateDB

  def child_specs(ctx, test_pid), do: [{__MODULE__, {ctx, test_pid}}]

  def child_spec({ctx, test_pid}) do
    %{
      id: __MODULE__,
      start: {GenServer, :start_link, [__MODULE__, {ctx, test_pid}, [name: name(ctx)]]}
    }
  end

  defp name(ctx), do: Cluster.via(ctx.cluster, ctx.n, :counter)

  @doc "Increments `key` and returns the new value once it is durable."
  def increment(cluster, key) do
    case Cluster.call(
           cluster,
           Cluster.shard_for(cluster, key),
           {__MODULE__, :increment_local, [key]}
         ) do
      {:ok, result} -> result
      {:error, _} = error -> error
    end
  end

  @doc false
  def increment_local(ctx, key), do: GenServer.call(name(ctx), {:increment, key})

  @doc "The stored value of `key`, read from the database."
  def get(cluster, key) do
    case Cluster.call(cluster, Cluster.shard_for(cluster, key), {__MODULE__, :get_local, [key]}) do
      {:ok, result} -> result
      {:error, _} = error -> error
    end
  end

  @doc false
  def get_local(ctx, key) do
    case SlateDB.get(ctx.db, "counter:" <> key) do
      {:ok, nil} -> 0
      {:ok, value} -> String.to_integer(value)
    end
  end

  @doc false
  def return_not_owner(_ctx, caller) do
    send(caller, :application_called)
    {:error, :not_owner}
  end

  @impl true
  def init({ctx, test_pid}) do
    Process.flag(:trap_exit, true)
    send(test_pid, {:counter_started, ctx.n, self()})
    {:ok, %{ctx: ctx, test_pid: test_pid, values: %{}, pending: %{}}}
  end

  @impl true
  def handle_call({:increment, key}, from, state) do
    value = Map.get_lazy(state.values, key, fn -> get_local(state.ctx, key) end) + 1
    {:ok, seq} = SlateDB.put(state.ctx.db, "counter:" <> key, Integer.to_string(value))
    ref = make_ref()
    Cluster.notify_when_durable(state.ctx, seq, {:ack, ref})

    state = %{
      state
      | values: Map.put(state.values, key, value),
        pending: Map.put(state.pending, ref, {from, value, seq})
    }

    {:noreply, state}
  end

  @impl true
  def handle_info({:slap_cluster_durable, {:ack, ref}}, state) do
    {{from, value, seq}, pending} = Map.pop(state.pending, ref)
    send(state.test_pid, {:acked, state.ctx.n, seq})
    GenServer.reply(from, {:ok, value})
    {:noreply, %{state | pending: pending}}
  end

  @impl true
  def terminate(_reason, state) do
    status = Cluster.shard_status(state.ctx)

    # The database is still open here: children stop before it closes.
    db_open? = match?({:ok, _}, SlateDB.get(state.ctx.db, "probe-key"))

    for {_ref, {from, _value, _seq}} <- state.pending do
      GenServer.reply(from, {:error, status})
    end

    send(state.test_pid, {:counter_stopped, state.ctx.n, status, db_open?})
  end
end
