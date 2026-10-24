defmodule ForkAndCopyMeasurements do
  alias Slap.Streams
  alias Slap.Streams.Jobs.{Deleter, Repair}
  alias Slap.Streams.Store.Read

  @sizes [1024, 64 * 1024 * 1024, 1024 * 1024 * 1024]
  @samples 10

  def run do
    for size <- @sizes do
      with_cluster(fn ->
        populate(size)

        times =
          for _ <- 1..@samples do
            {us, {:ok, :created, _}} =
              :timer.tc(fn -> Streams.create("/target", forked_from: "/source") end)

            remove_target()
            us
          end

        io = measure_io(fn -> Streams.create("/target", forked_from: "/source") end)
        report("reference", size, times, io)
      end)

      times =
        for _ <- 1..@samples do
          with_cluster(fn ->
            populate(size)

            {us, {:ok, :created, _}} =
              :timer.tc(fn -> Streams.copy("/source", "/target", timeout: 300_000) end)

            us
          end)
        end

      io =
        with_cluster(fn ->
          populate(size)
          measure_io(fn -> Streams.copy("/source", "/target", timeout: 300_000) end)
        end)

      report("copy", size, times, io)
    end

    read_depths()
    retained_storage()
  end

  defp populate(size) do
    {:ok, :created, _} = Streams.create("/source")
    chunk = :binary.copy("a", min(size, 1024 * 1024))
    for _ <- 1..div(size, byte_size(chunk)), do: {:ok, _} = Streams.append("/source", chunk)
  end

  defp with_cluster(fun) do
    {:ok, pid} =
      Streams.Cluster.start_link(
        store: :memory,
        shards: 2,
        child_options: [repair_interval: 3_600_000, deleter_interval: 3_600_000]
      )

    try do
      fun.()
    after
      Supervisor.stop(pid, :normal, 300_000)
      :erlang.garbage_collect()
    end
  end

  defp remove_target do
    :ok = Streams.delete("/target")
    for _ <- 1..3, ctx <- shards(), do: Repair.sweep(ctx)
    for ctx <- shards(), do: Deleter.drain(ctx)
  end

  defp report(mode, size, times, io) do
    sorted = Enum.sort(times)

    IO.puts(
      JSON.encode!(
        Map.merge(io, %{
          mode: mode,
          size: size,
          samples_us: times,
          p50_us: Enum.at(sorted, 4),
          p95_us: Enum.at(sorted, 9)
        })
      )
    )
  end

  defp measure_io(fun) do
    tracer =
      spawn(fn -> trace_loop(%{message_reads: 0, message_bytes_read: 0, bytes_written: 0}) end)

    :erlang.trace_pattern({Read, :read_msgs, 5}, [{:_, [], [{:return_trace}]}], [:local])
    :erlang.trace_pattern({Slap.SlateDB, :write, 3}, true, [:local])
    :erlang.trace_pattern({Slap.SlateDB.Transaction, :put, 4}, true, [:local])
    :erlang.trace(:all, true, [:call, {:tracer, tracer}])
    {:ok, :created, _} = fun.()
    ref = :erlang.trace_delivered(:all)
    receive do: ({:trace_delivered, :all, ^ref} -> :ok)
    :erlang.trace(:all, false, [:call])
    :erlang.trace_pattern({Read, :read_msgs, 5}, false, [:local])
    :erlang.trace_pattern({Slap.SlateDB, :write, 3}, false, [:local])
    :erlang.trace_pattern({Slap.SlateDB.Transaction, :put, 4}, false, [:local])
    send(tracer, {:result, self()})
    receive do: ({:io_result, stats} -> stats)
  end

  defp trace_loop(stats) do
    receive do
      {:trace, _, :call, {Read, :read_msgs, _}} ->
        trace_loop(%{stats | message_reads: stats.message_reads + 1})

      {:trace, _, :return_from, {Read, :read_msgs, 5}, {messages, _}} ->
        bytes = Enum.sum(Enum.map(messages, fn {_, b} -> byte_size(b) end))
        trace_loop(%{stats | message_bytes_read: stats.message_bytes_read + bytes})

      {:trace, _, :call, {Slap.SlateDB, :write, [_, ops, _]}} ->
        bytes =
          Enum.sum(
            Enum.map(ops, fn
              {:put, k, v} -> byte_size(k) + byte_size(v)
              {_, k} -> byte_size(k)
              _ -> 0
            end)
          )

        trace_loop(%{stats | bytes_written: stats.bytes_written + bytes})

      {:trace, _, :call, {Slap.SlateDB.Transaction, :put, [_, k, v, _]}} ->
        trace_loop(%{stats | bytes_written: stats.bytes_written + byte_size(k) + byte_size(v)})

      {:result, caller} ->
        send(caller, {:io_result, stats})

      _ ->
        trace_loop(stats)
    end
  end

  defp read_depths do
    with_cluster(fn ->
      populate(64 * 1024 * 1024)

      for depth <- 1..8 do
        source = if depth == 1, do: "/source", else: "/depth#{depth - 1}"
        {:ok, :created, _} = Streams.create("/depth#{depth}", forked_from: source)
      end

      for depth <- [0, 1, 2, 4, 8] do
        path = if depth == 0, do: "/source", else: "/depth#{depth}"

        times =
          for _ <- 1..100 do
            {us, {:ok, _}} = :timer.tc(fn -> Streams.read(path, 0, max_bytes: 64 * 1024) end)
            us
          end

        sorted = Enum.sort(times)

        IO.puts(
          JSON.encode!(%{
            mode: "read",
            depth: depth,
            p50_us: Enum.at(sorted, 49),
            p95_us: Enum.at(sorted, 94)
          })
        )
      end
    end)
  end

  defp retained_storage do
    with_cluster(fn ->
      populate(64 * 1024 * 1024)
      {:ok, :created, %{next_offset: tail}} = Streams.create("/target", forked_from: "/source")
      :ok = Streams.trim("/source", tail)
      :ok = Streams.trim("/target", tail)
      for ctx <- shards(), do: Deleter.drain(ctx)

      bytes =
        Enum.sum(
          for ctx <- shards(),
              {_, value} <- Slap.SlateDB.scan(ctx.db, prefix: <<4>>),
              do: byte_size(value)
        )

      IO.puts(
        JSON.encode!(%{
          mode: "retained-storage",
          source_bytes: 64 * 1024 * 1024,
          source_trim: tail,
          fork_trim: tail,
          message_bytes_retained: bytes
        })
      )
    end)
  end

  defp shards do
    for n <- Streams.Cluster.local_shards() do
      {:ok, {:local, ctx}} = Streams.Cluster.lookup(n)
      ctx
    end
  end
end

Logger.configure(level: :error)
ForkAndCopyMeasurements.run()
