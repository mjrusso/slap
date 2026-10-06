defmodule Slap.Yjs.Docs.Config do
  @moduledoc false
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: name(opts[:name]))
  end

  def get(docs) do
    case :persistent_term.get(name(docs), nil) do
      nil -> raise ArgumentError, "Slap.Yjs.Docs instance #{inspect(docs)} is not started"
      store -> store
    end
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    :persistent_term.put(name(opts[:name]), opts[:store])
    {:ok, opts}
  end

  @impl true
  def terminate(_reason, opts) do
    :persistent_term.erase(name(opts[:name]))
    :ok
  end

  defp name(docs), do: Module.concat(docs, Config)
end
