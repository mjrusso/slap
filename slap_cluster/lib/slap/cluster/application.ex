defmodule Slap.Cluster.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Duplicate keys: a restarted cluster can register before the registry
      # removes its previous supervisor's entry.
      {Registry, keys: :duplicate, name: Slap.Cluster.Instances},
      {Registry, keys: :unique, name: Slap.Cluster.Databases},
      {DynamicSupervisor, strategy: :one_for_one, name: Slap.Cluster.DatabaseSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_all)
  end
end
