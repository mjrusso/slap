defmodule Slap.Streams.Test.ReadView do
  @moduledoc false
  use GenServer

  def start_link({ctx, path, view}) do
    GenServer.start_link(__MODULE__, view,
      name: {:via, Registry, {ctx.registry, {ctx.n, {:stream, path}}}}
    )
  end

  @impl true
  def init(view), do: {:ok, view}

  @impl true
  def handle_call(:read_info, _from, view), do: {:reply, {:ok, view}, view}
end
