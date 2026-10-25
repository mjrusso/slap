defmodule Slap.SnapshotLog.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link([Slap.SnapshotLog.Recovery],
      strategy: :one_for_one,
      name: Slap.SnapshotLog.Supervisor
    )
  end
end
