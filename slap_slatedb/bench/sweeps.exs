# How settings and scale change durable writes:
#
# - Flush interval: durable put latency, durable throughput from 64 writers,
#   and object store PUTs per second, at each `flush_interval`.
# - Shards: many databases in one VM, each with its own writers doing durable
#   puts, as when one node owns several shards. Reports total durable writes
#   per second, put latency across all shards, and the PUT rate the store
#   has to sustain.
#
#     mix run bench/sweeps.exs
#     SLAP_BENCH_STORE=s3 SLAP_BENCH_S3_ENDPOINT=http://127.0.0.1:9000 \
#       mix run bench/sweeps.exs
#
# Besides the variables in bench/support/bench.exs:
#
# - SLAP_BENCH_FLUSH_INTERVALS: milliseconds, comma-separated (default
#   "5,10,20,100").
# - SLAP_BENCH_SHARDS: database counts (default "8,16,64").
# - SLAP_BENCH_SHARD_FLUSH_INTERVAL: `flush_interval` for the shard runs,
#   in milliseconds (default 10).
# - SLAP_BENCH_SHARD_WRITERS: writers per shard (default 4).
#
# Results go to bench/results/sweeps-<label>-{smaller,bigger}.json and .md.

Code.require_file("support/bench.exs", __DIR__)

Slap.SlateDB.set_log_level(:error)

ints = fn var, default ->
  var
  |> System.get_env(default)
  |> String.split(",", trim: true)
  |> Enum.map(&(&1 |> String.trim() |> String.to_integer()))
end

store = Bench.store()
label = Bench.label()
run_id = System.os_time(:millisecond)
value = :crypto.strong_rand_bytes(100)
results = :ets.new(:results, [:ordered_set, :public])

record = fn name, unit, better, value ->
  :ets.insert(
    results,
    {System.unique_integer([:monotonic]),
     %{name: "#{label}: #{name}", unit: unit, better: better, value: value}}
  )
end

timed = fn fun ->
  start = System.monotonic_time()
  result = fun.()
  {System.monotonic_time() - start, result}
end

puts = fn db ->
  for %{name: "slatedb.object_store.request_count", labels: %{"op" => "put"}, value: n} <-
        Slap.SlateDB.metrics(db),
      reduce: 0,
      do: (acc -> acc + n)
end

# Durable puts from `writers` processes on each database until `deadline`.
# Returns the latency of every put, in milliseconds.
durable_load = fn dbs, writers, deadline ->
  for {db, d} <- Enum.with_index(dbs), w <- 1..writers do
    Task.async(fn ->
      Stream.iterate(1, &(&1 + 1))
      |> Enum.reduce_while([], fn i, acc ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:halt, acc}
        else
          {t, {:ok, _}} =
            timed.(fn ->
              Slap.SlateDB.put(db, "load:#{d}:#{w}:#{i}", value, await_durable: true)
            end)

          {:cont, [Bench.ms(t) | acc]}
        end
      end)
    end)
  end
  |> Task.await_many(:infinity)
  |> List.flatten()
end

open = fn path, flush_ms ->
  {:ok, db} = Slap.SlateDB.open(path, store: store, settings: %{flush_interval: "#{flush_ms}ms"})
  # The first write after opening is flushed at once, so warm up first.
  {:ok, _} = Slap.SlateDB.put(db, "warmup", value, await_durable: true)
  db
end

# 1. Flush interval sweep.
for flush_ms <- ints.("SLAP_BENCH_FLUSH_INTERVALS", "5,10,20,100") do
  db = open.("sweep-#{run_id}-flush-#{flush_ms}", flush_ms)

  latencies =
    for i <- 1..Bench.scale(100) do
      {t, {:ok, _}} =
        timed.(fn -> Slap.SlateDB.put(db, "seq:#{i}", value, await_durable: true) end)

      Bench.ms(t)
    end

  record.(
    "flush #{flush_ms} ms: durable put p50",
    "ms",
    :smaller,
    Bench.percentile(latencies, 50)
  )

  record.(
    "flush #{flush_ms} ms: durable put p99",
    "ms",
    :smaller,
    Bench.percentile(latencies, 99)
  )

  seconds = Bench.scale(3)
  puts_before = puts.(db)
  started = System.monotonic_time(:millisecond)
  latencies = durable_load.([db], 64, started + seconds * 1000)
  elapsed = (System.monotonic_time(:millisecond) - started) / 1000

  record.(
    "flush #{flush_ms} ms: durable puts x64",
    "ops/s",
    :bigger,
    round(length(latencies) / elapsed)
  )

  record.(
    "flush #{flush_ms} ms: PUTs/s under load",
    "requests/s",
    :smaller,
    Float.round((puts.(db) - puts_before) / elapsed, 1)
  )

  :ok = Slap.SlateDB.close(db)
end

# 2. Many databases at once.
shard_flush_ms = ints.("SLAP_BENCH_SHARD_FLUSH_INTERVAL", "10") |> hd()
[writers] = ints.("SLAP_BENCH_SHARD_WRITERS", "4")

for shards <- ints.("SLAP_BENCH_SHARDS", "8,16,64") do
  {t, dbs} =
    timed.(fn ->
      1..shards
      |> Task.async_stream(&open.("sweep-#{run_id}-shards-#{shards}-#{&1}", shard_flush_ms),
        max_concurrency: 16,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, db} -> db end)
    end)

  name = "#{shards} shards"
  record.("#{name}: open all", "ms", :smaller, Bench.ms(t))

  seconds = Bench.scale(5)
  puts_before = dbs |> Enum.map(puts) |> Enum.sum()
  started = System.monotonic_time(:millisecond)
  latencies = durable_load.(dbs, writers, started + seconds * 1000)
  elapsed = (System.monotonic_time(:millisecond) - started) / 1000
  total_puts = (dbs |> Enum.map(puts) |> Enum.sum()) - puts_before

  record.("#{name}: durable puts", "ops/s", :bigger, round(length(latencies) / elapsed))
  record.("#{name}: durable put p50", "ms", :smaller, Bench.percentile(latencies, 50))
  record.("#{name}: durable put p99", "ms", :smaller, Bench.percentile(latencies, 99))
  record.("#{name}: PUTs/s", "requests/s", :smaller, Float.round(total_puts / elapsed, 1))

  dbs
  |> Task.async_stream(&Slap.SlateDB.close/1, max_concurrency: 16, timeout: :infinity)
  |> Stream.run()
end

Bench.write_results(
  "sweeps-#{label}",
  "Sweeps: #{label} (shards: flush_interval #{shard_flush_ms} ms, #{writers} writers each)",
  :ets.tab2list(results) |> Enum.map(&elem(&1, 1))
)
