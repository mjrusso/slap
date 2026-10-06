// The official benchmarks, against the server at DURABLE_STREAMS_URL.
// Results go to the console and to benchmark-results.json.
import { runBenchmarks } from "@durable-streams/benchmarks"

runBenchmarks({
  baseUrl: process.env.DURABLE_STREAMS_URL ?? `http://127.0.0.1:4437`,
  environment: process.env.BENCH_ENVIRONMENT,
})
