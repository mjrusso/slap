# Append load over HTTP against a running server: reports throughput and the
# latency of durable acknowledgements.
#
#     mix slap.server --streams --store s3:s3://bucket/load &
#     mix run bench/streams_http_load.exs [--port 4437] [--connections 128]
#         [--streams 1000] [--seconds 20] [--bytes 256] [--rate 0]
#
# Each connection appends `--bytes` bodies to random streams among
# `--streams`, one request at a time (keep-alive). With `--rate` (appends a
# second, over all connections) the load is paced; otherwise each
# connection sends as fast as its acknowledgements come back.

Code.require_file("support/raw_http.exs", __DIR__)
alias Slap.Bench.RawHTTP

{opts, _} =
  OptionParser.parse!(System.argv(),
    strict: [
      port: :integer,
      connections: :integer,
      streams: :integer,
      seconds: :integer,
      bytes: :integer,
      rate: :integer
    ]
  )

port = Keyword.get(opts, :port, 4437)
connections = Keyword.get(opts, :connections, 128)
streams = Keyword.get(opts, :streams, 1000)
seconds = Keyword.get(opts, :seconds, 20)
body = :binary.copy("x", Keyword.get(opts, :bytes, 256))
rate = Keyword.get(opts, :rate, 0)
prefix = "/v1/stream/load-#{System.os_time(:millisecond)}"

{:ok, s} = RawHTTP.connect(port)

for i <- 1..streams do
  {:ok, 201, _, _} = RawHTTP.request(s, "PUT", "#{prefix}/#{i}", [{"content-type", "text/plain"}])
end

RawHTTP.close(s)

deadline = System.monotonic_time(:millisecond) + seconds * 1000
interval = if rate > 0, do: connections * 1000 / rate, else: 0

worker = fn ->
  {:ok, s} = RawHTTP.connect(port)

  Stream.repeatedly(fn -> nil end)
  |> Enum.reduce_while({[], 0, System.monotonic_time(:microsecond)}, fn _, {lat, errors, next} ->
    now = System.monotonic_time(:microsecond)

    cond do
      div(now, 1000) >= deadline ->
        {:halt, {lat, errors}}

      interval > 0 and now < next ->
        Process.sleep(max(div(next - now, 1000), 0))
        {:cont, {lat, errors, next}}

      true ->
        path = "#{prefix}/#{:rand.uniform(streams)}"
        t0 = System.monotonic_time(:microsecond)

        case RawHTTP.request(s, "POST", path, [{"content-type", "text/plain"}], body) do
          {:ok, 204, _, _} ->
            t = System.monotonic_time(:microsecond) - t0
            {:cont, {[t | lat], errors, next + round(interval * 1000)}}

          _ ->
            {:cont, {lat, errors + 1, next + round(interval * 1000)}}
        end
    end
  end)
end

started = System.monotonic_time(:millisecond)

results =
  1..connections
  |> Enum.map(fn _ -> Task.async(worker) end)
  |> Enum.map(&Task.await(&1, :infinity))

elapsed = (System.monotonic_time(:millisecond) - started) / 1000

latencies = results |> Enum.flat_map(&elem(&1, 0)) |> Enum.sort() |> List.to_tuple()
errors = results |> Enum.map(&elem(&1, 1)) |> Enum.sum()
n = tuple_size(latencies)
pct = fn p -> elem(latencies, min(n - 1, max(0, ceil(p / 100 * n) - 1))) / 1000 end

IO.puts("""
#{connections} connections, #{streams} streams, #{byte_size(body)}-byte appends, #{seconds} s#{if rate > 0, do: ", paced at #{rate}/s", else: ""}
  #{n} appends acknowledged, #{round(n / elapsed)}/s, #{errors} errors
  latency p50 #{Float.round(pct.(50), 1)} ms, p99 #{Float.round(pct.(99), 1)} ms, p99.9 #{Float.round(pct.(99.9), 1)} ms, max #{Float.round(pct.(100), 1)} ms\
""")
