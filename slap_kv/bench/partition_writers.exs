# How many partition writers does a shard need? Runs load against a single
# shard and reports throughput, latency, and how busy the shard's partition
# writers are: the longest mailbox, sampled every 10 ms. One that keeps up
# has an empty mailbox most of the time; one that does not has a queue that
# grows.
#
#     mix run bench/partition_writers.exs [--store memory|local:DIR|s3:URL] [--seconds 5]
#         [--flush-interval 10ms] [--clients 1,16,128] [--partition-writers 16]
#
# --partition-writers sets the cluster's partition_writers child option.
#
# Workloads:
#
#   * put: unconditional puts to random partitions (no read in the writer).
#   * rmw: read-modify-write of each client's own row, in its own partition
#     (get, then put with if_version): the writer's check reads the memtable.
#   * cold: puts with if_version: :absent to rows (one per partition) written
#     and flushed out of the memtable beforehand, with the block cache
#     disabled, so every check reads an SST from the store.

alias Slap.KV
alias Slap.SlateDB

{opts, _} =
  OptionParser.parse!(System.argv(),
    strict: [
      store: :string,
      seconds: :integer,
      flush_interval: :string,
      clients: :string,
      partition_writers: :integer
    ]
  )

store =
  case Keyword.get(opts, :store, "local:" <> Path.join(System.tmp_dir!(), "slap-kv-bench")) do
    "memory" -> :memory
    "local:" <> dir -> File.rm_rf!(dir) && {:local, dir}
    "s3:" <> url -> {:url, url}
  end

seconds = Keyword.get(opts, :seconds, 5)
flush = Keyword.get(opts, :flush_interval, "10ms")

clients =
  opts |> Keyword.get(:clients, "1,16,128") |> String.split(",") |> Enum.map(&String.to_integer/1)

SlateDB.set_log_level(:warning)
writers = Keyword.get(opts, :partition_writers, 16)

{:ok, _} =
  KV.Cluster.start_link(
    store: store,
    path: "bench-#{System.os_time(:millisecond)}",
    shards: 1,
    child_options: [partition_writers: writers],
    settings: %{flush_interval: flush},
    cache: :disabled
  )

{:ok, {:local, ctx}} = KV.Cluster.lookup(0)

writer_pids =
  for i <- 0..(writers - 1) do
    [{pid, _}] = Registry.lookup(ctx.registry, {0, {:partition_writer, i}})
    pid
  end

# Runs `op.(client, i)` in `n` clients for `seconds`, and returns
# {count, latencies in µs, mailbox samples}.
run = fn n, op ->
  deadline = System.monotonic_time(:millisecond) + seconds * 1_000

  sampler =
    Task.async(fn ->
      Stream.repeatedly(fn ->
        Process.sleep(10)

        writer_pids
        |> Enum.map(fn pid -> elem(Process.info(pid, :message_queue_len), 1) end)
        |> Enum.max()
      end)
      |> Enum.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)
    end)

  latencies =
    1..n
    |> Enum.map(fn c ->
      Task.async(fn ->
        Stream.iterate(0, &(&1 + 1))
        |> Stream.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)
        |> Enum.map(fn i ->
          {us, result} = :timer.tc(fn -> op.(c, i) end)
          true = match?({:ok, _}, result) or match?({:error, {:conflict, _}}, result)
          us
        end)
      end)
    end)
    |> Task.await_many(:infinity)
    |> List.flatten()

  {length(latencies), latencies, Task.await(sampler, :infinity)}
end

percentile = fn sorted, p ->
  Enum.at(sorted, min(length(sorted) - 1, trunc(length(sorted) * p)))
end

report = fn name, n, {count, latencies, samples} ->
  sorted = Enum.sort(latencies)
  busy = Enum.count(samples, &(&1 > 0)) / max(length(samples), 1)

  IO.puts(
    String.pad_trailing("#{name}, #{n} clients", 20) <>
      "#{round(count / seconds)} writes/s  " <>
      "p50 #{Float.round(percentile.(sorted, 0.5) / 1000, 1)} ms  " <>
      "p99 #{Float.round(percentile.(sorted, 0.99) / 1000, 1)} ms  " <>
      "mailbox non-empty #{round(busy * 100)}%, max #{Enum.max(samples, fn -> 0 end)}"
  )
end

value = :crypto.strong_rand_bytes(256)

IO.puts(
  "store #{inspect(store)}, flush_interval #{flush}, one shard, #{writers} partition writers, " <>
    "#{seconds} s each"
)

for n <- clients do
  report.("put", n, run.(n, fn c, i -> KV.put("p#{rem(c * 7919 + i, 1000)}", "k#{i}", value) end))
end

for n <- clients do
  for c <- 1..n, do: {:ok, _} = KV.put("rmw#{c}", "row", "0")

  report.(
    "rmw",
    n,
    run.(n, fn c, _i ->
      {:ok, %{version: v}} = KV.get("rmw#{c}", "row")
      KV.put("rmw#{c}", "row", value, if_version: v)
    end)
  )
end

# Rows to check against, out of the memtable, one per partition.
rows = 20_000
cold = fn i -> "cold#{i}" end

for chunk <- Enum.chunk_every(1..rows, 500) do
  chunk |> Enum.map(&Task.async(fn -> KV.put(cold.(&1), "row", value) end)) |> Task.await_many()
end

:ok = SlateDB.flush(ctx.db, type: :memtable)

for n <- clients do
  report.(
    "cold",
    n,
    run.(n, fn _c, _i -> KV.put(cold.(:rand.uniform(rows)), "row", value, if_version: :absent) end)
  )
end
