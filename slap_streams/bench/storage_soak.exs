# Storage after deletes: writes streams, deletes them, lets Slap.Streams.Jobs.Deleter
# and SlateDB's compactor and garbage collector run, and reports the size of
# the store's files as it goes.
#
#     mix run bench/storage_soak.exs
#
# Deleted rows are only dropped when compaction merges their tombstones
# with them, so the compactor is set to merge runs of very different sizes
# (a size threshold of 1000), and the garbage collector to run every second. SlateDB 0.16 keeps the
# files a compaction replaces for 15 minutes (each compaction writes a
# checkpoint with that lifetime), so each round then waits SOAK_SETTLE_S
# (default 1080) seconds. Meanwhile it appends to one other stream and
# flushes an L0 each second, as ongoing traffic would: the garbage collector
# also keeps files newer than the newest compacted L0.
#
# SOAK_MIB (default 64) sets how much to write, SOAK_ROUNDS (default 2) how
# many times to write and delete it. By default the store is a temporary
# directory; SOAK_URL (such as s3://bucket/soak, configured by AWS_*
# variables) uses object storage, and then SOAK_SIZE_CMD, a shell command,
# must print the size of the store's files in bytes. On a local directory
# old manifests cannot be collected, so neither can the files they name:
# use object storage to see the space come back.

Slap.SlateDB.set_log_level(String.to_atom(System.get_env("SOAK_LOG", "error")))
Logger.configure(level: String.to_atom(System.get_env("SOAK_LOG", "warning")))

mib = String.to_integer(System.get_env("SOAK_MIB", "64"))
rounds = String.to_integer(System.get_env("SOAK_ROUNDS", "2"))
dir = Path.join(System.tmp_dir!(), "slap-streams-soak-#{System.unique_integer([:positive])}")
url = System.get_env("SOAK_URL")
store = if url, do: {:url, url}, else: {:local, dir}

gc = %{interval: "1s", min_age: "1s"}

settings = %{
  flush_interval: "5ms",
  l0_sst_size_bytes: 4 * 1024 * 1024,
  compactor_options: %{
    poll_interval: "500ms",
    scheduler_options: %{
      min_compaction_sources: "2",
      max_compaction_sources: "16",
      include_size_threshold: "1000.0"
    }
  },
  # The local file system cannot collect old manifests.
  garbage_collector_options:
    if(url, do: %{manifest_options: gc}, else: %{})
    |> Map.merge(%{wal_options: gc, compacted_options: gc})
}

{:ok, _} = Slap.Streams.Cluster.start_link(store: store, shards: 1, settings: settings)
{:ok, {:local, ctx}} = Slap.Streams.Cluster.lookup(0)

size =
  if url do
    fn ->
      {out, 0} = System.cmd("sh", ["-c", System.fetch_env!("SOAK_SIZE_CMD")])
      out |> String.trim() |> String.to_integer()
    end
  else
    fn ->
      Path.wildcard(Path.join(dir, "**/*"))
      |> Enum.filter(&File.regular?/1)
      |> Enum.map(&File.stat!(&1).size)
      |> Enum.sum()
    end
  end

fmt = fn bytes -> :io_lib.format("~.1f MiB", [bytes / 1_048_576]) |> to_string() end
chunk = :crypto.strong_rand_bytes(64 * 1024)
streams = 64
per_stream = div(mib * 16, streams)

{:ok, :created, _} = Slap.Streams.create("/soak/live")
settle_s = String.to_integer(System.get_env("SOAK_SETTLE_S", "1080"))

settle = fn label ->
  # A small write and an L0 flush each second stand in for other traffic:
  # the garbage collector keeps files newer than the newest compacted L0.
  for t <- 1..settle_s do
    Process.sleep(1000)
    {:ok, _} = Slap.Streams.append("/soak/live", "tick")
    :ok = Slap.SlateDB.flush(ctx.db, type: :memtable)
    if rem(t, 60) == 0, do: IO.puts("  #{label} +#{t}s: #{fmt.(size.())}")
  end

  size.()
end

for round <- 1..rounds do
  prefix = "/soak/#{round}"

  for s <- 1..streams do
    path = "#{prefix}/#{s}"
    {:ok, :created, _} = Slap.Streams.create(path)
    for _ <- 1..per_stream, do: {:ok, _} = Slap.Streams.append(path, chunk)
  end

  :ok = Slap.SlateDB.flush(ctx.db, type: :memtable)
  IO.puts("round #{round}: wrote #{mib} MiB in #{streams} streams, store #{fmt.(size.())}")

  for s <- 1..streams, do: :ok = Slap.Streams.delete("#{prefix}/#{s}")
  Slap.Streams.Jobs.Deleter.drain(ctx)
  :ok = Slap.SlateDB.flush(ctx.db, type: :memtable)
  left = ctx.db |> Slap.SlateDB.scan(gte: <<1>>) |> Enum.count()
  IO.puts("round #{round}: deleted, #{left} rows left, store #{fmt.(size.())}")
  IO.puts("round #{round}: after compaction and GC, store #{fmt.(settle.("round #{round}"))}")

  if System.get_env("SOAK_DEBUG") do
    IO.inspect(Slap.SlateDB.stats(ctx.db), label: "stats")
    {:ok, admin} = Slap.SlateDB.Admin.open("shard-000", store: store)
    IO.inspect(Slap.SlateDB.Admin.list_checkpoints(admin), label: "checkpoints")

    for %{name: "slatedb.gc" <> _ = n, labels: l, value: v} <- Slap.SlateDB.metrics(ctx.db),
        do: IO.puts("#{n} #{inspect(l)}: #{inspect(v)}")
  end
end

unless System.get_env("SOAK_KEEP"), do: File.rm_rf!(dir), else: IO.puts("kept #{dir}")
