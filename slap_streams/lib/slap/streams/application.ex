defmodule Slap.Streams.Application do
  @moduledoc false

  use Application

  alias Slap.Streams
  alias Slap.Streams.HTTP.Router

  @impl true
  def start(_type, _args) do
    Router.setup()
    Supervisor.start_link([Streams.Metrics], strategy: :one_for_one, name: Streams.Supervisor)
  end
end
