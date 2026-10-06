# Per-call latency of the binding, with Benchee, on the in-memory store.
#
#     MIX_ENV=bench mix run bench/micro.exs
#
# This measures the NIF call, the hop to the Tokio runtime and the reply
# message, plus SlateDB's in-memory paths. Object storage is in
# bench/scenarios.exs.
#
# Writes an HTML report to bench/results/micro/index.html and results for
# github-action-benchmark to bench/results/micro-smaller.json (medians, in
# microseconds). Needs the :bench dependencies (Benchee).

Code.require_file("support/bench.exs", __DIR__)

Slap.SlateDB.set_log_level(:error)

out = Bench.out_dir()
value = :crypto.strong_rand_bytes(100)
rows = 10_000
key = fn i -> "key:" <> String.pad_leading(Integer.to_string(i), 8, "0") end

{:ok, db} = Slap.SlateDB.open("micro", store: :memory)
{:ok, _} = Slap.SlateDB.write(db, for(i <- 1..rows, do: {:put, key.(i), value}))
batch = for i <- 1..100, do: {:put, "batch:#{i}", value}

common = [
  warmup: 1,
  time: Bench.scale(5),
  memory_time: 0,
  max_sample_size: 100_000,
  print: [fast_warning: false]
]

small =
  Benchee.run(
    %{
      "durable_seq (no runtime hop)" => fn -> Slap.SlateDB.durable_seq(db) end,
      "put 100 B" => fn -> {:ok, _} = Slap.SlateDB.put(db, key.(:rand.uniform(rows)), value) end,
      "get hit" => fn -> {:ok, _} = Slap.SlateDB.get(db, key.(:rand.uniform(rows))) end,
      "get miss" => fn -> {:ok, nil} = Slap.SlateDB.get(db, "missing") end,
      "write, 100 puts" => fn -> {:ok, _} = Slap.SlateDB.write(db, batch) end,
      "scan 1,000 rows" => fn ->
        1_000 = db |> Slap.SlateDB.scan(gte: key.(1), lt: key.(1_001)) |> Enum.count()
      end
    },
    [
      title: "slap_slatedb: in-memory store, 100 B values",
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: Path.join(out, "micro/index.html"), auto_open: false}
      ]
    ] ++ common
  )

:ok = Slap.SlateDB.close(db)

# Values of 64 KiB or more cross the NIF boundary without copying. Each size
# gets a fresh database, closed afterwards, and a smaller call cap: `put`
# returns before SlateDB encodes the value into its WAL, so it is fast and
# the memtable grows by the full value each time.
large =
  Benchee.run(
    %{
      "put" => fn {db, value, _} ->
        {:ok, _} = Slap.SlateDB.put(db, "large:#{System.unique_integer([:positive])}", value)
      end,
      "get" => fn {db, _, key} -> {:ok, _} = Slap.SlateDB.get(db, key) end
    },
    [
      title: "slap_slatedb: large values",
      inputs: %{"4 KiB" => 4 * 1024, "256 KiB" => 256 * 1024, "1 MiB" => 1024 * 1024},
      before_scenario: fn size ->
        {:ok, db} =
          Slap.SlateDB.open("large-#{size}",
            store: :memory,
            settings: %{max_unflushed_bytes: 4_000_000_000}
          )

        value = :crypto.strong_rand_bytes(size)
        {:ok, _} = Slap.SlateDB.put(db, "large:get", value)
        {db, value, "large:get"}
      end,
      after_scenario: fn {db, _, _} -> :ok = Slap.SlateDB.close(db) end,
      formatters: [
        Benchee.Formatters.Console,
        {Benchee.Formatters.HTML, file: Path.join(out, "micro/large.html"), auto_open: false}
      ]
    ] ++ Keyword.merge(common, warmup: 0.2, max_sample_size: 1_000)
  )

# Benchee's run times are in nanoseconds. Plain map access, so this does not
# depend on Benchee's struct modules.
results =
  for suite <- [small, large], scenario <- suite.scenarios do
    stats = scenario.run_time_data.statistics

    name =
      case scenario.input_name do
        input when input in [:__no_input, "__no_input", nil] -> scenario.job_name
        input -> "#{scenario.job_name} #{input}"
      end

    p99 = stats.percentiles[99]

    %{
      name: "memory: #{name}",
      unit: "us",
      better: :smaller,
      value: Float.round(stats.median / 1000, 2),
      extra:
        "p99 #{if p99, do: Float.round(p99 / 1000, 2), else: "n/a"} us, " <>
          "#{stats.sample_size} samples"
    }
  end

Bench.write_results("micro", "Micro benchmarks (median)", results)
