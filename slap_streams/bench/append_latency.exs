# Append latency, from Slap.Streams.append/3 returning, which is after the
# append is durable. On the in-memory store, so this is the stream server,
# the cluster's durability fan-out and SlateDB's WAL flush, not storage.
#
#     mix run bench/append_latency.exs
#
# SLAP_BENCH_FLUSH_INTERVAL (default "1ms") sets SlateDB's flush_interval, the
# floor of the latency.
#

Slap.SlateDB.set_log_level(:error)
flush = System.get_env("SLAP_BENCH_FLUSH_INTERVAL", "1ms")

{:ok, _} =
  Slap.Streams.Cluster.start_link(store: :memory, shards: 4, settings: %{flush_interval: flush})

percentile = fn values, p ->
  sorted = Enum.sort(values)
  Enum.at(sorted, max(0, ceil(p / 100 * length(sorted)) - 1))
end

timed = fn fun ->
  start = System.monotonic_time()
  fun.()
  System.convert_time_unit(System.monotonic_time() - start, :native, :microsecond)
end

report = fn label, us ->
  IO.puts(
    "#{label}: p50 #{percentile.(us, 50)} us, p99 #{percentile.(us, 99)} us, " <>
      "max #{Enum.max(us)} us (#{length(us)} appends)"
  )
end

{:ok, :created, _} = Slap.Streams.create("/one", content_type: "text/plain")
for _ <- 1..200, do: {:ok, _} = Slap.Streams.append("/one", "warm-up")

us =
  for _ <- 1..2_000, do: timed.(fn -> {:ok, _} = Slap.Streams.append("/one", "hello") end)

report.("1 writer, 1 stream (flush_interval #{flush})", us)

paths = for i <- 1..64, do: "/many/#{i}"

for p <- paths,
    do: {:ok, :created, _} = Slap.Streams.create(p, content_type: "text/plain")

us =
  paths
  |> Task.async_stream(
    fn p ->
      for _ <- 1..200, do: timed.(fn -> {:ok, _} = Slap.Streams.append(p, "hello") end)
    end,
    max_concurrency: 64,
    timeout: :infinity
  )
  |> Enum.flat_map(fn {:ok, us} -> us end)

report.("64 writers, 64 streams, 4 shards", us)
