defmodule Slap.Cluster.Peers do
  @moduledoc """
  Keeps this node connected to a fixed list of other nodes, retrying every
  2 s. Routing between nodes needs distributed Erlang: the node must have a
  name and share the cookie.

  Run one connector per VM. Its connections are available to every cluster
  on that VM.
  """

  use GenServer
  require Logger

  @interval 2_000

  def start_link(peers), do: GenServer.start_link(__MODULE__, peers, name: __MODULE__)

  @impl true
  def init(peers) do
    unless Node.alive?(),
      do:
        Logger.warning(
          "Slap.Cluster.Peers: this node has no name, so it cannot connect to #{inspect(peers)}"
        )

    {:ok, connect(Enum.reject(peers, &(&1 == node())))}
  end

  @impl true
  def handle_info(:connect, peers), do: {:noreply, connect(peers)}

  defp connect(peers) do
    if Node.alive?(), do: for(p <- peers, p not in Node.list(), do: Node.connect(p))
    Process.send_after(self(), :connect, @interval)
    peers
  end
end
