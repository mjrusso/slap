# End-to-end numbers that depend on the store: durable write latency,
# durability lag, write throughput and the object store requests it takes,
# cold and warm reads, scans, and how far a reader trails the writer.
#
#     mix run bench/scenarios.exs
#     SLAP_BENCH_STORE=s3 SLAP_BENCH_S3_ENDPOINT=http://127.0.0.1:9000 \
#       mix run bench/scenarios.exs
#
# See bench/support/bench.exs for the environment variables. Results go to
# bench/results/scenarios-<label>-{smaller,bigger}.json and .md.
#
# The durable numbers mostly measure SlateDB's WAL flush interval (100 ms
# by default) plus one object store PUT, so they are useful for comparing
# stores and settings, not the binding.

Code.require_file("support/bench.exs", __DIR__)

Slap.SlateDB.set_log_level(:error)

store = Bench.store()
label = Bench.label()
path = "scenarios-#{System.os_time(:millisecond)}"
# Readers find new WAL files by polling. 100 ms keeps the reader lag
# measurement short; it is the setting that decides that number.
settings = %{manifest_poll_interval: "100ms"}
value = :crypto.strong_rand_bytes(100)
key = fn i -> "key:" <> String.pad_leading(Integer.to_string(i), 8, "0") end
# The in-memory store is new each time it is opened, so reopening and
# readers only work on the other stores.
persistent? = store != :memory
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

{:ok, db} = Slap.SlateDB.open(path, store: store, settings: settings)

# The first write after opening is flushed at once, so warm up first.
{:ok, _} = Slap.SlateDB.put(db, "warmup", value, await_durable: true)

# 1. Durable put latency, one writer.
n = Bench.scale(100)

latencies =
  for i <- 1..n do
    {t, {:ok, _}} =
      timed.(fn -> Slap.SlateDB.put(db, "durable:#{i}", value, await_durable: true) end)

    Bench.ms(t)
  end

record.("durable put p50", "ms", :smaller, Bench.percentile(latencies, 50))
record.("durable put p99", "ms", :smaller, Bench.percentile(latencies, 99))

# 2. Durable puts from 64 writers. They share WAL flushes, so throughput
# grows with the number of writers.
per_writer = Bench.scale(20)

{t, _} =
  timed.(fn ->
    1..64
    |> Task.async_stream(
      fn w ->
        for i <- 1..per_writer,
            do: {:ok, _} = Slap.SlateDB.put(db, "durable64:#{w}:#{i}", value, await_durable: true)
      end,
      max_concurrency: 64,
      timeout: :infinity
    )
    |> Stream.run()
  end)

record.("durable puts x64", "ops/s", :bigger, round(64 * per_writer / (Bench.us(t) / 1_000_000)))

# 3. Durability lag: writes that do not wait, at a steady rate, and how long
# until each is durable, from the subscription's notifications.
collector =
  spawn_link(fn ->
    {:ok, %{ref: ref} = sub} = Slap.SlateDB.subscribe(db, :lag)

    loop = fn loop, acc ->
      receive do
        {:slap_slatedb_durable, ^ref, :lag, seq} ->
          loop.(loop, [{System.monotonic_time(), seq} | acc])

        {:done, from} ->
          send(from, {:durable, Enum.reverse(acc)})
      end
    end

    loop.(loop, [])
    Slap.SlateDB.unsubscribe(sub)
  end)

writes =
  for i <- 1..Bench.scale(1_000) do
    {:ok, seq} = Slap.SlateDB.put(db, "lag:#{i}", value)
    written = System.monotonic_time()
    Process.sleep(2)
    {written, seq}
  end

:ok = Slap.SlateDB.flush(db)
Process.sleep(50)
send(collector, {:done, self()})
durable = receive do: ({:durable, d} -> d)

lags =
  for {written, seq} <- writes,
      {at, _} = Enum.find(durable, fn {_, d} -> d >= seq end) do
    Bench.ms(at - written)
  end

record.("durability lag p50", "ms", :smaller, Bench.percentile(lags, 50))
record.("durability lag p99", "ms", :smaller, Bench.percentile(lags, 99))

# 4. Write throughput, 16 writers that do not wait, then one flush so all
# of it is durable. Also counts the object store requests it took.
rows = Bench.scale(20_000)
requests_before = Bench.object_store_requests(db)

{t, _} =
  timed.(fn ->
    1..16
    |> Task.async_stream(
      fn w ->
        for i <- w..rows//16, do: {:ok, _} = Slap.SlateDB.put(db, key.(i), value)
      end,
      max_concurrency: 16,
      timeout: :infinity
    )
    |> Stream.run()

    :ok = Slap.SlateDB.flush(db)
  end)

requests = Bench.object_store_requests(db) - requests_before
record.("writes x16", "ops/s", :bigger, round(rows / (Bench.us(t) / 1_000_000)))

record.(
  "object store requests per 1,000 writes",
  "requests",
  :smaller,
  Float.round(requests * 1000 / rows, 1)
)

# 5. Scan every row written in step 4.
{t, count} =
  timed.(fn -> db |> Slap.SlateDB.scan(prefix: "key:") |> Enum.count() end)

^rows = count
record.("scan", "rows/s", :bigger, round(count / (Bench.us(t) / 1_000_000)))

reads = Bench.scale(500)
sample = for _ <- 1..reads, do: key.(:rand.uniform(rows))

read_all = fn db ->
  for k <- sample do
    {t, {:ok, _}} = timed.(fn -> Slap.SlateDB.get(db, k) end)
    Bench.us(t)
  end
end

if persistent? do
  # 6. Reads from SSTs in object storage, with and without the block cache.
  :ok = Slap.SlateDB.flush(db, type: :memtable)
  :ok = Slap.SlateDB.close(db)

  {:ok, cold} = Slap.SlateDB.open(path, store: store, settings: settings, cache: :disabled)
  times = read_all.(cold)
  record.("get, no cache, p50", "us", :smaller, Bench.percentile(times, 50))
  record.("get, no cache, p99", "us", :smaller, Bench.percentile(times, 99))
  :ok = Slap.SlateDB.close(cold)

  {:ok, db} = Slap.SlateDB.open(path, store: store, settings: settings)
  _ = read_all.(db)
  times = read_all.(db)
  record.("get, warm cache, p50", "us", :smaller, Bench.percentile(times, 50))
  record.("get, warm cache, p99", "us", :smaller, Bench.percentile(times, 99))

  # 7. Reader lag: from a durable write returning to a reader seeing it.
  {:ok, reader} = Slap.SlateDB.Reader.open(path, store: store, settings: settings)

  lags =
    for i <- 1..Bench.scale(20) do
      k = "reader:#{i}"
      {:ok, _} = Slap.SlateDB.put(db, k, value, await_durable: true)
      start = System.monotonic_time()

      wait = fn wait ->
        case Slap.SlateDB.Reader.get(reader, k) do
          {:ok, nil} ->
            Process.sleep(2)
            wait.(wait)

          {:ok, _} ->
            Bench.ms(System.monotonic_time() - start)
        end
      end

      wait.(wait)
    end

  record.("reader lag p50", "ms", :smaller, Bench.percentile(lags, 50))
  record.("reader lag p99", "ms", :smaller, Bench.percentile(lags, 99))
  :ok = Slap.SlateDB.Reader.close(reader)
  :ok = Slap.SlateDB.close(db)
else
  # 6. Reads from the memtable. There is nothing to reopen.
  times = read_all.(db)
  record.("get, memtable, p50", "us", :smaller, Bench.percentile(times, 50))
  :ok = Slap.SlateDB.close(db)
end

Bench.write_results(
  "scenarios-#{label}",
  "Scenarios: #{label}",
  :ets.tab2list(results) |> Enum.map(&elem(&1, 1))
)
