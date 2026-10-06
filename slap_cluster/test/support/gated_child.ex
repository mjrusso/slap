defmodule Slap.Cluster.Test.GatedChild do
  @moduledoc false
  # A shard child that tells `test` it is starting, starts when sent `:go`,
  # and takes a while to stop: the test decides when each open finishes.
  use GenServer

  def child_specs(ctx, test), do: [{__MODULE__, {ctx.n, ctx.generation, test}}]

  def start_link(arg), do: GenServer.start_link(__MODULE__, arg)

  @impl true
  def init({n, generation, test}) do
    Process.flag(:trap_exit, true)
    send(test, {:child_starting, n, generation, self()})

    receive do
      :go -> {:ok, {n, test}}
    end
  end

  @impl true
  def terminate(_reason, {n, test}) do
    Process.sleep(200)
    send(test, {:child_stopped, n, self()})
  end
end
