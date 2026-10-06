defmodule Slap.Streams.Bench.ReadScaling do
  alias Slap.Streams

  def run(["--" | argv]), do: run(argv)

  def run(argv) do
    {opts, []} =
      OptionParser.parse!(argv,
        strict: [
          streams: :integer,
          messages: :integer,
          samples: :integer,
          message_bytes: :integer,
          page_bytes: :integer,
          store: :string,
          cache: :string,
          flush_interval: :string
        ]
      )

    streams = positive!(opts, :streams, 1_000)
    messages = positive!(opts, :messages, 1_024)
    samples = positive!(opts, :samples, 200)
    message_bytes = positive!(opts, :message_bytes, 1_024)
    page_bytes = positive!(opts, :page_bytes, 4_096)
    flush_interval = Keyword.get(opts, :flush_interval, "1ms")
    store = store(Keyword.get(opts, :store, "local"))
    cache = cache(Keyword.get(opts, :cache, "default"))

    Slap.SlateDB.set_log_level(:error)

    {:ok, _} =
      Streams.Cluster.start_link(
        store: store,
        shards: 1,
        cache: cache,
        settings: %{flush_interval: flush_interval}
      )

    try do
      IO.puts(
        "store=#{inspect(store)} cache=#{if(cache, do: "disabled", else: "default")} " <>
          "streams=#{streams} messages=#{messages} " <>
          "message_bytes=#{message_bytes} page_bytes=#{page_bytes} samples=#{samples} " <>
          "flush_interval=#{flush_interval}"
      )

      count_sweep(streams, samples, message_bytes, page_bytes)
      length_sweep(messages, samples, message_bytes, page_bytes)
    after
      :ok = Streams.Cluster.stop()

      case store do
        {:local, dir} -> File.rm_rf!(dir)
        _ -> :ok
      end
    end
  end

  defp positive!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      value -> raise ArgumentError, "#{key} must be positive, got: #{inspect(value)}"
    end
  end

  defp store("memory"), do: :memory

  defp store("local") do
    dir = Path.join(System.tmp_dir!(), "slap-streams-read-#{System.unique_integer([:positive])}")
    {:local, dir}
  end

  defp store("s3:" <> url), do: {:url, url}

  defp store(value),
    do: raise(ArgumentError, "store must be local, memory, or s3:<url>, got: #{value}")

  defp cache("default"), do: nil
  defp cache("disabled"), do: :disabled

  defp cache(value),
    do: raise(ArgumentError, "cache must be default or disabled, got: #{value}")

  defp stages(limit) do
    [1, max(2, div(limit, 10)), limit]
    |> Enum.filter(&(&1 <= limit))
    |> Enum.uniq()
  end

  defp count_sweep(max_streams, samples, message_bytes, page_bytes) do
    IO.puts("\nStream count: each stream has one #{message_bytes}-byte message")
    body = :binary.copy("x", message_bytes)

    read_one = fn path ->
      {:ok, %{messages: [{0, ^body}]}} = Streams.read(path, 0, max_bytes: page_bytes)
    end

    {:ok, {:local, ctx}} = Streams.Cluster.lookup(0)
    prefix = "/bench/.items/"

    Enum.reduce(stages(max_streams), 0, fn count, previous ->
      for i <- (previous + 1)..count do
        path = path(prefix, i)
        {:ok, :created, _} = Streams.create(path)
        {:ok, _} = Streams.append(path, body)
      end

      :ok = Slap.SlateDB.flush(ctx.db, type: :memtable)
      paths = for i <- 1..count, do: path(prefix, i)
      Enum.each(paths, &stop_server(ctx, &1))
      first_read_paths = for _ <- 1..samples, do: Enum.random(paths)

      measure(
        "first server read, #{count} streams",
        first_read_paths,
        read_one,
        &stop_server(ctx, &1)
      )

      first_read_paths |> Enum.uniq() |> Enum.each(&stop_server(ctx, &1))
      read_paths = for _ <- 1..samples, do: Enum.random(paths)

      read_paths
      |> Enum.uniq()
      |> Enum.each(read_one)

      measure("warm random read, #{count} streams", read_paths, read_one)

      measure(
        "warm narrow list, #{count} streams",
        List.duplicate(hd(paths), samples),
        fn path ->
          {:ok, [^path]} = Streams.list(path)
        end
      )

      {:ok, listed} = Streams.list(prefix)
      ^count = length(listed)

      measure("warm broad list, #{count} streams", List.duplicate(prefix, samples), fn key ->
        {:ok, _} = Streams.list(key)
      end)

      count
    end)
  end

  defp length_sweep(max_messages, samples, message_bytes, page_bytes) do
    IO.puts("\nStream length: fixed #{message_bytes}-byte messages, #{page_bytes}-byte reads")
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(0)
    path = "/bench/long"
    body = :binary.copy("x", message_bytes)
    stride = message_bytes + 4
    page_messages = max(1, div(page_bytes, message_bytes))
    {:ok, :created, _} = Streams.create(path)

    Enum.reduce(stages(max_messages), 0, fn count, previous ->
      for i <- (previous + 1)..count do
        {:ok, %{next_offset: next}} = Streams.append(path, body)
        ^next = i * stride
      end

      :ok = Slap.SlateDB.flush(ctx.db, type: :memtable)
      IO.puts("  #{count} messages, #{count * message_bytes} body bytes")

      for {position, index} <- [
            start: 0,
            middle: div(count, 2),
            near_tail: max(0, count - page_messages)
          ] do
        offset = index * stride

        measure("warm #{position} read", List.duplicate(offset, samples), fn from ->
          {:ok, %{messages: [{^from, _} | _]}} =
            Streams.read(path, from, max_bytes: page_bytes)
        end)
      end

      count
    end)
  end

  defp path(prefix, i), do: prefix <> String.pad_leading(Integer.to_string(i), 8, "0")

  defp stop_server(ctx, path) do
    key = {ctx.n, {:stream, path}}

    case Registry.lookup(ctx.registry, key) do
      [{pid, _}] -> :ok = GenServer.stop(pid, :normal, 5_000)
      [] -> :ok
    end

    wait_unregistered(ctx.registry, key, 100)
  end

  defp wait_unregistered(registry, key, attempts) do
    case Registry.lookup(registry, key) do
      [] ->
        :ok

      _ when attempts > 0 ->
        Process.sleep(1)
        wait_unregistered(registry, key, attempts - 1)

      _ ->
        raise "stream server remained registered: #{inspect(key)}"
    end
  end

  defp measure(label, inputs, fun, prepare \\ fn _ -> :ok end) do
    inputs
    |> Enum.take(10)
    |> Enum.each(fn input ->
      prepare.(input)
      fun.(input)
    end)

    times =
      for input <- inputs do
        prepare.(input)
        start = System.monotonic_time(:microsecond)
        fun.(input)
        System.monotonic_time(:microsecond) - start
      end

    sorted = Enum.sort(times)
    n = length(sorted)
    p50 = Enum.at(sorted, ceil(n * 0.5) - 1)
    p99 = Enum.at(sorted, ceil(n * 0.99) - 1)
    IO.puts("  #{label}: p50 #{p50} us, p99 #{p99} us, max #{List.last(sorted)} us (n=#{n})")
  end
end

Slap.Streams.Bench.ReadScaling.run(System.argv())
