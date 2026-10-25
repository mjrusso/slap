defmodule Slap.KV.Telemetry do
  @moduledoc """
  Telemetry events, all under `[:slap, :kv, ...]`:

    * `[:write, :acknowledged]` - a write was acknowledged (durable). Measurements:
      `:duration` (native time units, from when the partition writer received
      the request to the reply).
      Metadata: `:shard`, `:op` (`:put`, `:delete` or `:batch`).
    * `[:partition_writer, :write_failed]` - a write failed; the partition
      writer stopped and failed the requests in flight. Metadata:
      `:shard`.
    * `[:request, :rejected]` - a request was rejected with `:overloaded`
      (see the README's "Limits on pending work"). Measurements: `:bytes`
      (encoded keys and put values), `:requests` (1). Metadata:
      `:partition`, `:shard`, `:writer` (the partition writer's index, from 0
      to `partition_writers - 1`), `:op`
      (`:put`, `:delete`, `:batch`, `:get`), `:reason` (`:overloaded`),
      `:limit` (`:writer_bytes`, `:shard_bytes`, `:writer_requests`, checked
      in that order). `:request_too_large` and `:batch_too_large` do not emit
      this event.
    * `[:shard, :load]` - the shard's pending work changed. Measurements:
      `:bytes` and `:requests` counted against the shard's limits, including
      conflicts and checks awaiting confirmation and the writes of crashed
      partition writers that are not yet durable. Metadata: `:shard`. They do
      not measure total memory or queued messages.
    * `[:http, :request]` - an HTTP response was sent. Measurements:
      `:duration`. Metadata: `:method`, `:status`.

  Batch acknowledgements emit once per mutation batch. Check-only success,
  conflicts and linearizable gets do not emit mutation acknowledgements.
  The acknowledgement event is a point event without a matching `:start`.
  """

  @doc false
  def execute(event, measurements, metadata),
    do: :telemetry.execute([:slap, :kv | event], measurements, metadata)
end
