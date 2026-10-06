# Where should the inline threshold be? For each body size, puts and reads
# files with inline bodies (in their Slap.KV record) and with object bodies,
# from concurrent clients, and reports latency and throughput; then lists a
# partition of inline files of that size, which reads their bodies too.
#
#     mix run bench/inline.exs [--store memory|local:DIR|s3:URL] [--seconds 5]
#         [--clients 16] [--sizes 1024,4096,16384,65536,262144]

alias Slap.Files
alias Slap.SlateDB

{opts, _} =
  OptionParser.parse!(System.argv(),
    strict: [store: :string, seconds: :integer, clients: :integer, sizes: :string]
  )

store =
  case Keyword.get(opts, :store, "local:" <> Path.join(System.tmp_dir!(), "slap-files-bench")) do
    "memory" -> :memory
    "local:" <> dir -> File.rm_rf!(dir) && {:local, dir}
    "s3:" <> url -> {:url, url}
  end

seconds = Keyword.get(opts, :seconds, 5)
clients = Keyword.get(opts, :clients, 16)

sizes =
  opts
  |> Keyword.get(:sizes, "1024,4096,16384,65536,262144")
  |> String.split(",")
  |> Enum.map(&String.to_integer/1)

SlateDB.set_log_level(:warning)
run = "bench-#{System.os_time(:millisecond)}"

{:ok, _} =
  Slap.KV.Cluster.start_link(
    store: store,
    path: run <> "/kv",
    shards: 4,
    settings: %{flush_interval: "10ms"}
  )

{:ok, _} = Files.start_link(store: store, path: run <> "/files", inline_limit: Enum.max(sizes))

# Runs `op.(client, i)` in `clients` processes for `seconds`; returns sorted
# latencies in µs.
load = fn op ->
  deadline = System.monotonic_time(:millisecond) + seconds * 1_000

  1..clients
  |> Enum.map(fn c ->
    Task.async(fn ->
      Stream.iterate(0, &(&1 + 1))
      |> Stream.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)
      |> Enum.map(fn i ->
        {us, {:ok, _}} = :timer.tc(fn -> op.(c, i) end)
        us
      end)
    end)
  end)
  |> Task.await_many(:infinity)
  |> List.flatten()
  |> Enum.sort()
end

pct = fn sorted, p ->
  Enum.at(sorted, min(length(sorted) - 1, trunc(length(sorted) * p))) / 1000
end

row = fn label, latencies ->
  IO.puts(
    String.pad_trailing(label, 24) <>
      String.pad_leading("#{round(length(latencies) / seconds)}/s", 8) <>
      "  p50 #{Float.round(pct.(latencies, 0.5), 1)} ms  p99 #{Float.round(pct.(latencies, 0.99), 1)} ms"
  )
end

IO.puts("store #{inspect(store)}, flush_interval 10ms, #{clients} clients, #{seconds} s each\n")

for size <- sizes do
  body = :crypto.strong_rand_bytes(size)

  for storage <- [:inline, :object] do
    part = "#{storage}-#{size}"
    puts = load.(fn c, i -> Files.put({"#{part}-#{c}", "f#{i}"}, body, storage: storage) end)
    row.("put #{size} B #{storage}", puts)
    reads = load.(fn c, i -> Files.read({"#{part}-#{c}", "f#{rem(i, 50)}"}) end)
    row.("read #{size} B #{storage}", reads)
  end

  # A page of 100 inline files, whose bodies the listing reads too.
  page = "list-#{size}"
  for i <- 1..100, do: {:ok, _} = Files.put({page, "f#{i}"}, body, storage: :inline)
  lists = load.(fn _c, _i -> Files.list(page, limit: 100) end)
  row.("list 100 × #{size} B", lists)
  IO.puts("")
end
