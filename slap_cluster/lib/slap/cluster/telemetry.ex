defmodule Slap.Cluster.Telemetry do
  @moduledoc false
  # Emits `[:slap, :cluster | event]`.

  def execute(event, measurements, metadata),
    do: :telemetry.execute([:slap, :cluster | event], measurements, metadata)
end
