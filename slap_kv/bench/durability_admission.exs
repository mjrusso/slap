defmodule Slap.KV.Bench.Admission do
  alias Slap.KV
  alias Slap.KV.Admission
  alias Slap.SlateDB

  def run(opts) do
    store =
      case Keyword.get(opts, :store, "memory") do
        "memory" -> :memory
        "local:" <> dir -> {:local, dir}
        url -> {:url, url}
      end

    {:ok, cluster} =
      KV.Cluster.start_link(
        store: store,
        path: "admission-#{System.os_time(:millisecond)}",
        shards: 1,
        child_options: [
          partition_writers: 4,
          max_inflight_bytes_per_writer: Keyword.get(opts, :writer_bytes, 131_072),
          max_inflight_bytes_per_shard: Keyword.get(opts, :shard_bytes, 524_288),
          max_inflight_requests_per_writer: Keyword.get(opts, :writer_requests, 4)
        ],
        settings: %{flush_interval: Keyword.get(opts, :flush_interval, "250ms")}
      )

    {:ok, {:local, ctx}} = KV.Cluster.lookup(0)
    {:ok, _} = KV.put("bank", "seed", "seed")
    started = System.monotonic_time(:millisecond)
    sampler = Task.async(fn -> sample_until_stopped(ctx, started, []) end)
    requests = Keyword.get(opts, :requests, 24)

    clients =
      for client <- 1..Keyword.get(opts, :clients, 64) do
        Task.async(fn -> for _ <- 1..requests, do: operation(client) end)
      end

    results = clients |> Task.await_many(:infinity) |> List.flatten()
    send(sampler.pid, :stop)
    samples = Task.await(sampler)

    report = %{
      elapsed_ms: System.monotonic_time(:millisecond) - started,
      requests: length(results),
      outcomes: Enum.frequencies(results),
      samples: samples,
      recovery: inspect(KV.put("recovery", "k", "ok"))
    }

    IO.puts(JSON.encode!(report))
    Supervisor.stop(cluster)
  end

  defp operation(client) do
    partition = "p#{rem(client, 16)}"

    result =
      case rem(client, 4) do
        0 -> KV.put(partition, "k", :binary.copy("v", 65_536))
        1 -> KV.put(partition, "tiny", "t")
        2 -> KV.put("bank", "seed", "x", if_version: :absent)
        3 -> KV.get(partition, "k", consistency: :linearizable)
      end

    if result == {:error, :overloaded}, do: Process.sleep(10)

    case result do
      {:ok, _} -> "ok"
      {:error, {:conflict, _}} -> "conflict"
      other -> inspect(other)
    end
  end

  defp sample_until_stopped(ctx, started, samples) do
    receive do
      :stop -> Enum.reverse(samples)
    after
      50 ->
        sample = %{
          at_ms: System.monotonic_time(:millisecond) - started,
          reserved: Admission.snapshot(ctx),
          beam_bytes: :erlang.memory(:total),
          native_buffers:
            SlateDB.metrics(ctx.db)
            |> Enum.filter(
              &(&1.name in [
                  "slatedb.db.total_mem_size_bytes",
                  "slatedb.wal.wal_buffer_estimated_bytes"
                ])
            ),
          mailboxes: mailboxes(ctx),
          durability: SlateDB.stats(ctx.db)
        }

        sample_until_stopped(ctx, started, [sample | samples])
    end
  end

  defp mailboxes(ctx) do
    for key <- [:kv_admission | Enum.map(0..3, &{:partition_writer, &1})] do
      [{pid, _}] = Registry.lookup(ctx.registry, {ctx.n, key})
      {inspect(key), elem(Process.info(pid, :message_queue_len), 1)}
    end
    |> Map.new()
  end
end

{opts, []} =
  OptionParser.parse!(System.argv(),
    strict: [
      store: :string,
      clients: :integer,
      requests: :integer,
      writer_bytes: :integer,
      shard_bytes: :integer,
      writer_requests: :integer,
      flush_interval: :string
    ]
  )

Slap.KV.Bench.Admission.run(opts)
