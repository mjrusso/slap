defmodule Slap.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    DynamicSupervisor.start_link(strategy: :one_for_one, name: Slap.ServerSupervisor)
  end
end
