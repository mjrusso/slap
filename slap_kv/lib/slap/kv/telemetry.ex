defmodule Slap.KV.Telemetry do
  @moduledoc """
  Telemetry events, all under `[:slap, :kv, ...]`:

    * `[:write, :acknowledged]` - a write was acknowledged (durable). Measurements:
      `:duration` (native time units, from the partition writer receiving
      it to the reply). Metadata: `:shard`, `:op` (`:put` or `:delete`).
    * `[:partition_writer, :write_failed]` - a write failed; the partition
      writer stopped and failed the requests in flight. Metadata:
      `:shard`.
    * `[:http, :request]` - an HTTP response was sent. Measurements:
      `:duration`. Metadata: `:method`, `:status`.

  The acknowledgement event is a point event without a matching `:start`.
  """

  @doc false
  def execute(event, measurements, metadata),
    do: :telemetry.execute([:slap, :kv | event], measurements, metadata)
end
