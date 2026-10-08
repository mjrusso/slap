defmodule Slap.Streams.Telemetry do
  @moduledoc """
  Telemetry events, all under `[:slap, :streams, ...]`. `Slap.Streams.Metrics` turns
  them, and `slap_cluster`'s, into Prometheus metrics.

    * `[:append, :acknowledged]` - an append was acknowledged (durable).
      Measurements: `:duration` (native time units, from the stream server
      accepting it to the reply), `:bytes`. Metadata: `:shard`, `:result`
      (`:appended` or `:closed`).
    * `[:append, :rejected]` - an append was refused for backpressure.
      Measurements: `:bytes`. Metadata: `:shard`, `:reason` (`:overloaded`).
    * `[:stream_server, :write_failed]` - a write failed; the server stopped
      and failed its in-flight requests. Metadata: `:shard`.
    * `[:wait, :registered]` - a read with `:wait` (long poll or SSE) found
      no data and is waiting. Any append after this event wakes it.
      Metadata: `:shard`, `:path`, `:offset` (the offset it waits for).
    * `[:shard, :load]` - every `:load_interval`, per shard: the
      measurements of the shard's load. Metadata: `:shard`.
    * `[:http, :request]` - an HTTP response was sent. Measurements:
      `:duration`. Metadata: `:method`, `:status`.

  The acknowledgement and wait events have no matching `:start` or `:stop`
  events.
  """

  @doc false
  def execute(event, measurements, metadata),
    do: :telemetry.execute([:slap, :streams | event], measurements, metadata)
end
