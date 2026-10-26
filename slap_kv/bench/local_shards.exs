# Run from slap_kv/: mix run bench/local_shards.exs [label]
alias Slap.KV
alias Slap.KV.Cluster
:ok = Slap.SlateDB.set_log_level(:error)
phase = List.first(System.argv()) || "current"

for shards <- [1, 4], trial <- 1..3 do
  root =
    Path.join(
      System.tmp_dir!(),
      "slap-local-shards-#{phase}-#{shards}-#{trial}-#{System.unique_integer([:positive])}"
    )

  {:ok, _} = Cluster.start_link(store: {:local, root}, path: "kv", shards: shards)
  :ok = Cluster.await_ready(30_000)

  partitions =
    for shard <- 0..(shards - 1) do
      Enum.find_value(1..100_000, fn n ->
        key = "partition-#{n}"
        if Cluster.shard_for(key) == shard, do: key
      end)
    end

  body = :binary.copy("x", 1024)

  run = fn count ->
    0..15
    |> Task.async_stream(
      fn client ->
        partition = Enum.at(partitions, rem(client, shards))

        for n <- 1..count do
          start = System.monotonic_time(:microsecond)
          {:ok, _} = KV.put(partition, "#{client}/#{n}", body)
          System.monotonic_time(:microsecond) - start
        end
      end,
      max_concurrency: 16,
      timeout: 120_000
    )
    |> Enum.flat_map(fn {:ok, times} -> times end)
  end

  run.(10)
  start = System.monotonic_time(:microsecond)
  times = run.(100) |> Enum.sort()
  elapsed = System.monotonic_time(:microsecond) - start

  result = %{
    phase: phase,
    shards: shards,
    trial: trial,
    operations: length(times),
    elapsed_us: elapsed,
    ops_per_second: length(times) * 1_000_000 / elapsed,
    p50_us: Enum.at(times, div(length(times), 2)),
    p95_us: Enum.at(times, div(length(times) * 95, 100)),
    p99_us: Enum.at(times, div(length(times) * 99, 100))
  }

  IO.puts("RESULT " <> Jason.encode!(result))
  :ok = Cluster.stop()
  File.rm_rf!(root)
end
