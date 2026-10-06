defmodule Slap.Streams.Metrics do
  @moduledoc """
  Prometheus metrics from `Slap.Streams.Telemetry`'s events and `slap_cluster`'s,
  kept in ETS and rendered in the text exposition format by `render/0`
  (served by `Slap.Streams.HTTP.Metrics`).

  One metrics process runs per VM. Its labels do not distinguish separate
  Streams clusters on that VM. It ignores cluster events from other services,
  such as `Slap.KV.Cluster`.

  | Metric | Type | Labels |
  |---|---|---|
  | `slap_streams_append_duration_seconds` | histogram | accept to durable reply |
  | `slap_streams_appends_total` | counter | `result` |
  | `slap_streams_append_bytes_total` | counter | |
  | `slap_streams_appends_rejected_total` | counter | `reason` (backpressure) |
  | `slap_streams_write_failures_total` | counter | |
  | `slap_streams_stream_servers` | gauge | `shard` |
  | `slap_streams_inflight_bytes`, `slap_streams_inflight_requests` | gauge | `shard` |
  | `slap_streams_waiters` | gauge | `shard` (long-poll and SSE) |
  | `slap_streams_delete_backlog`, `slap_streams_expiry_due` | gauge | `shard` (job backlogs) |
  | `slap_streams_l0_ssts`, `slap_streams_sorted_runs` | gauge | `shard` (compaction backlog) |
  | `slap_streams_block_cache_hits_total`, `slap_streams_block_cache_misses_total` | counter | `shard` |
  | `slap_streams_durability_lag` | gauge | `shard` (seqs written, not durable) |
  | `slap_streams_shard_events_total` | counter | `event` (`start`, `stop`, `fenced`) |
  | `slap_streams_store_probe_ok` | gauge | 1 if the startup probe passed |
  | `slap_streams_http_requests_total` | counter | `method`, `status` |
  | `slap_streams_http_request_duration_seconds` | histogram | `method` |

  This is a small exporter of its own rather than `telemetry_metrics` and
  its Prometheus reporter: the metrics are fixed, and it keeps the
  dependencies to `telemetry`.
  """

  use GenServer

  alias Slap.Streams.Cluster

  @table __MODULE__
  @buckets [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0]

  @help %{
    "slap_streams_append_duration_seconds" =>
      {:histogram, "Append latency, accept to durable reply."},
    "slap_streams_appends_total" => {:counter, "Appends acknowledged."},
    "slap_streams_append_bytes_total" => {:counter, "Bytes appended."},
    "slap_streams_appends_rejected_total" => {:counter, "Appends refused for backpressure."},
    "slap_streams_write_failures_total" =>
      {:counter, "Failed writes (the stream server restarts)."},
    "slap_streams_stream_servers" => {:gauge, "Active stream servers."},
    "slap_streams_inflight_bytes" => {:gauge, "Bytes written but not yet durable."},
    "slap_streams_inflight_requests" => {:gauge, "Writes not yet durable."},
    "slap_streams_waiters" => {:gauge, "Long-poll and SSE waiters."},
    "slap_streams_delete_backlog" => {:gauge, "Deleted streams whose rows are not yet deleted."},
    "slap_streams_expiry_due" => {:gauge, "Expiry index entries due at the last sweep."},
    "slap_streams_l0_ssts" => {:gauge, "L0 SSTs (compaction backlog)."},
    "slap_streams_sorted_runs" => {:gauge, "Sorted runs."},
    "slap_streams_block_cache_hits_total" => {:counter, "Block cache hits."},
    "slap_streams_block_cache_misses_total" => {:counter, "Block cache misses."},
    "slap_streams_durability_lag" => {:gauge, "Sequence numbers written but not durable."},
    "slap_streams_shard_events_total" => {:counter, "Shard starts, stops and fences."},
    "slap_streams_store_probe_ok" => {:gauge, "1 if the store's startup probe passed."},
    "slap_streams_http_requests_total" => {:counter, "HTTP responses."},
    "slap_streams_http_request_duration_seconds" => {:histogram, "HTTP request duration."}
  }

  @events [
    [:slap, :streams, :append, :acknowledged],
    [:slap, :streams, :append, :rejected],
    [:slap, :streams, :stream_server, :write_failed],
    [:slap, :streams, :shard, :load],
    [:slap, :streams, :http, :request],
    [:slap, :cluster, :durability, :lag],
    [:slap, :cluster, :shard, :start],
    [:slap, :cluster, :shard, :stop],
    [:slap, :cluster, :shard, :fenced],
    [:slap, :cluster, :probe]
  ]

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "The metrics in the Prometheus text format."
  @spec render() :: iodata()
  def render do
    rows = :ets.tab2list(@table)

    rows
    |> Enum.group_by(fn row -> row |> elem(0) |> elem(0) end)
    |> Enum.sort()
    |> Enum.map(fn {name, series} ->
      {type, help} = Map.fetch!(@help, name)

      [
        "# HELP #{name} #{help}\n# TYPE #{name} #{type}\n",
        series |> Enum.sort() |> Enum.map(&line(type, &1))
      ]
    end)
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
    :telemetry.attach_many({__MODULE__, self()}, @events, &__MODULE__.handle_event/4, nil)
    {:ok, nil}
  end

  @impl true
  def terminate(_reason, _state), do: :telemetry.detach({__MODULE__, self()})

  @doc false
  def handle_event([:slap, :streams, :append, :acknowledged], m, meta, _) do
    observe("slap_streams_append_duration_seconds", [], seconds(m.duration))
    inc("slap_streams_appends_total", result: meta.result)
    inc("slap_streams_append_bytes_total", [], m.bytes)
  end

  def handle_event([:slap, :streams, :append, :rejected], _m, meta, _),
    do: inc("slap_streams_appends_rejected_total", reason: meta.reason)

  def handle_event([:slap, :streams, :stream_server, :write_failed], _m, _meta, _),
    do: inc("slap_streams_write_failures_total", [])

  def handle_event([:slap, :streams, :shard, :load], m, %{shard: shard}, _) do
    for {key, name} <- [
          stream_servers: "slap_streams_stream_servers",
          inflight_bytes: "slap_streams_inflight_bytes",
          inflight_requests: "slap_streams_inflight_requests",
          waiters: "slap_streams_waiters",
          delete_backlog: "slap_streams_delete_backlog",
          expiry_due: "slap_streams_expiry_due",
          l0_sst_count: "slap_streams_l0_ssts",
          sorted_run_count: "slap_streams_sorted_runs",
          cache_hits: "slap_streams_block_cache_hits_total",
          cache_misses: "slap_streams_block_cache_misses_total"
        ],
        Map.has_key?(m, key),
        do: set(name, [shard: shard], Map.fetch!(m, key))

    :ok
  end

  def handle_event([:slap, :streams, :http, :request], m, meta, _) do
    inc("slap_streams_http_requests_total", method: meta.method, status: meta.status)

    observe(
      "slap_streams_http_request_duration_seconds",
      [method: meta.method],
      seconds(m.duration)
    )
  end

  def handle_event([:slap, :cluster | _] = event, measurements, %{cluster: cluster} = meta, _) do
    if Cluster.streams_cluster?(cluster),
      do: handle_cluster_event(event, measurements, meta)
  end

  defp handle_cluster_event([:slap, :cluster, :durability, :lag], %{lag: lag}, meta),
    do: set("slap_streams_durability_lag", [shard: meta.shard], lag)

  defp handle_cluster_event([:slap, :cluster, :shard, event], _m, _meta),
    do: inc("slap_streams_shard_events_total", event: event)

  defp handle_cluster_event([:slap, :cluster, :probe], _m, meta),
    do: set("slap_streams_store_probe_ok", [], if(meta.ok, do: 1, else: 0))

  defp seconds(native), do: System.convert_time_unit(native, :native, :microsecond) / 1.0e6

  defp inc(name, labels, by \\ 1) do
    :ets.update_counter(@table, {name, labels}, {2, by}, {{name, labels}, 0})
  rescue
    ArgumentError -> :ok
  end

  defp set(name, labels, value) do
    :ets.insert(@table, {{name, labels}, value})
  rescue
    ArgumentError -> :ok
  end

  # A histogram row is {key, count, sum_us, bucket counts...}.
  defp observe(name, labels, value) do
    bucket = Enum.find_index(@buckets, &(value <= &1)) || length(@buckets)
    default = List.to_tuple([{name, labels}, 0, 0 | List.duplicate(0, length(@buckets) + 1)])
    ops = [{2, 1}, {3, round(value * 1.0e6)}, {4 + bucket, 1}]
    :ets.update_counter(@table, {name, labels}, ops, default)
  rescue
    ArgumentError -> :ok
  end

  defp line(:histogram, row) do
    [{name, labels}, count, sum_us | buckets] = Tuple.to_list(row)

    # Cumulative counts per bound; the +Inf bucket holds every observation.
    {lines, _} =
      Enum.zip(@buckets, buckets)
      |> Enum.map_reduce(0, fn {le, n}, acc ->
        acc = acc + n
        {bucket_line(name, labels, to_string(le), acc), acc}
      end)

    [
      lines,
      bucket_line(name, labels, "+Inf", count),
      "#{name}_sum#{labels(labels)} #{sum_us / 1.0e6}\n",
      "#{name}_count#{labels(labels)} #{count}\n"
    ]
  end

  defp line(_type, {{name, labels}, value}), do: "#{name}#{labels(labels)} #{value}\n"

  defp bucket_line(name, labels, le, count),
    do: "#{name}_bucket#{labels([{:le, le} | labels])} #{count}\n"

  defp labels([]), do: ""

  defp labels(labels) do
    inner = Enum.map_join(labels, ",", fn {k, v} -> ~s(#{k}="#{escape(to_string(v))}") end)
    "{#{inner}}"
  end

  defp escape(v), do: v |> String.replace("\\", "\\\\") |> String.replace("\"", "\\\"")
end
