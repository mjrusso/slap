defmodule Slap.Streams.ModelTest do
  # Random command sequences against Slap.Streams and Slap.Streams.Test.Model: every
  # result, read and head must match. Includes forks, trims, TTLs and
  # Expires-At on a fake clock, the expiry and repair sweeps, and killing
  # stream servers, so state must reload from storage. After each sequence every stream is
  # deleted, and at the end the deleter must have left no rows behind.
  #
  # Each command is chosen from the model's state and a seed; StreamData
  # generates the seeds, so a failure shrinks to the shortest sequence of
  # commands that fails. SLAP_STREAMS_PROP_RUNS (default 100) sets the number of
  # sequences; `mix test --seed N` replays a run.
  use Slap.Streams.Test.ClusterCase, async: false
  use ExUnitProperties

  @moduletag :capture_log
  @moduletag timeout: :infinity

  alias Slap.SlateDB
  alias Slap.Streams
  alias Slap.Streams.Jobs.{Deleter, Expiry, Repair}
  alias Slap.Streams.Test.{FakeClock, Model}

  @commands 40
  @start_ms 1_700_000_000_000
  @max_fork_copy_bytes 100
  @moduletag child_options: [
               expiry_interval: :timer.hours(1),
               max_fork_copy_bytes: @max_fork_copy_bytes
             ]

  setup_all do
    FakeClock.install(@start_ms)
    on_exit(fn -> FakeClock.uninstall() end)
  end

  property "an idle stream stays at its tail after a bounded wait" do
    check all(timeout <- integer(60..100), max_runs: 5) do
      path = "/idle-#{System.unique_integer([:positive])}"
      assert {:ok, :created, _} = Streams.create(path)

      assert {:ok, %{messages: [], next_offset: 0, up_to_date: true}} =
               Streams.read(path, 0, wait: timeout, timeout: timeout)

      assert :ok = Streams.delete(path)
    end
  end

  property "Streams behaves like the model" do
    runs = String.to_integer(System.get_env("SLAP_STREAMS_PROP_RUNS", "100"))
    seeds = list_of(integer(0..0xFFFF_FFFF), min_length: 1, max_length: @commands)

    check all(seeds <- seeds, max_runs: runs, initial_size: @commands) do
      prefix = "/run#{System.unique_integer([:positive])}"
      paths = for i <- 1..4, do: "#{prefix}/s#{i}"
      start = Model.new(FakeClock.now_ms(), @max_fork_copy_bytes)

      {model, _} =
        seeds
        |> Enum.with_index(1)
        |> Enum.reduce({start, []}, fn {seed, i}, {model, history} ->
          :rand.seed(:exsss, {seed, 0, 17})
          command = command(model, paths)
          history = [command | history]
          {expected, model} = Model.run(model, command)
          actual = execute(command)

          assert actual == expected, """
          command #{i}: #{inspect(command)}
          expected: #{inspect(expected)}
          got:      #{inspect(actual)}
          history (latest first): #{inspect(Enum.take(history, String.to_integer(System.get_env("SLAP_STREAMS_PROP_HISTORY", "10"))))}
          """

          if System.get_env("SLAP_STREAMS_PROP_STATS"), do: tally(command, actual)
          {model, history}
        end)

      delete_all(model)
    end

    # Every stream is deleted: once the deleters have run, nothing is left
    # but the stream id counter and the seals, which are kept for good.
    for ctx <- shards() do
      Deleter.drain(ctx)

      left =
        for {<<type, _::binary>> = k, _} <- SlateDB.scan(ctx.db, gte: <<1>>), type != 0x09, do: k

      assert left == [],
             "shard #{ctx.n}: #{length(left)} rows left, such as #{inspect(Enum.take(left, 3))}"
    end

    if System.get_env("SLAP_STREAMS_PROP_STATS") do
      Process.get()
      |> Enum.filter(&match?({{:tally, _}, _}, &1))
      |> Enum.sort()
      |> Enum.each(fn {{:tally, k}, n} -> IO.puts("#{inspect(k)}: #{n}") end)
    end
  end

  # With SLAP_STREAMS_PROP_STATS set, counts outcomes per command type, to check that
  # the generators reach the interesting cases.
  defp tally(command, result) do
    key = {:tally, {command_type(command), outcome(result)}}
    Process.put(key, (Process.get(key) || 0) + 1)
  end

  defp outcome({:ok, %{result: r}}), do: r
  defp outcome({:ok, kind, _}), do: kind
  defp outcome({:error, {kind, _}}), do: kind
  defp outcome({:error, {kind, _, _}}), do: kind
  defp outcome({:error, kind}), do: kind
  defp outcome(_), do: :ok

  defp command_type({:create, _, opts}), do: if(opts[:forked_from], do: :fork, else: :create)
  defp command_type(command), do: elem(command, 0)

  # Deletes every stream. A source deleted before its forks is soft-deleted,
  # and goes with its last fork.
  defp delete_all(model) do
    for path <- Model.paths(model) do
      assert Streams.delete(path) in [:ok, {:error, :gone}, {:error, :not_found}]
    end

    for path <- Model.paths(model) do
      assert Streams.head(path) == {:error, :not_found}, "#{path} left behind"
    end
  end

  defp shards do
    for n <- Streams.Cluster.local_shards() do
      {:ok, {:local, ctx}} = Streams.Cluster.lookup(n)
      ctx
    end
  end

  defp execute({:create, path, opts}), do: Streams.create(path, opts)
  defp execute({:trim, path, offset}), do: Streams.trim(path, offset)
  defp execute({:invalid, request, _reason}), do: execute(request)

  defp execute({:advance, ms}) do
    FakeClock.advance(ms)
    :ok
  end

  defp execute({:sweep}) do
    for ctx <- shards(), do: Expiry.sweep(ctx)
    :ok
  end

  defp execute({:repair}) do
    for ctx <- shards(), do: Repair.sweep(ctx)
    :ok
  end

  defp execute({:append, path, body, opts}), do: Streams.append(path, body, opts)
  defp execute({:delete, path}), do: Streams.delete(path)
  defp execute({:head, path}), do: Streams.head(path)
  defp execute({:list, prefix}), do: Streams.list(prefix)
  defp execute({:seal, group}), do: Streams.seal(group)

  defp execute({:read, path, offset, max}) do
    Streams.read(path, offset, max_bytes: max, timeout: 30_000)
  end

  defp execute({:kill, path}) do
    if pid = server(path) do
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}
    end

    :ok
  end

  @types ["text/plain", "application/json", nil, "Text/Plain; charset=utf-8"]

  # Out of 100: how often each kind of command is picked.
  @kinds [
    create: 10,
    fork: 6,
    trim: 4,
    advance: 3,
    sweep: 2,
    repair: 2,
    append: 35,
    close: 2,
    delete: 5,
    head: 7,
    kill: 6,
    list: 3,
    seal: 1,
    read: 12,
    invalid: 2
  ]

  # Picks a command with arguments that often hit the interesting cases:
  # duplicates, gaps, stale epochs, content type and Stream-Seq conflicts.
  defp command(model, paths) do
    path = choose(paths)
    stream = Model.get(model, path)

    # Mostly create a missing stream, so the other commands have one to act on.
    kind =
      if stream == nil and chance(0.7),
        do: choose([:create, :fork, :fork]),
        else: weighted(int(99), @kinds)

    generate(kind, %{model: model, paths: paths, path: path, stream: stream})
  end

  defp weighted(roll, [{kind, weight} | _]) when roll < weight, do: kind
  defp weighted(roll, [{_kind, weight} | rest]), do: weighted(roll - weight, rest)

  # TTLs of 1-3 s with clock steps of at least 400 ms: every step moves a
  # sliding TTL's persisted deadline (at most once per tenth of the TTL),
  # so killing a server never loses a TTL reset.
  defp generate(:create, %{path: path, model: model}) do
    {:create, path,
     present(
       content_type: choose(@types),
       closed: chance(0.08),
       ttl_s: if(chance(0.15), do: choose([1, 3, 3600])),
       expires_at_ms: if(chance(0.1), do: model.now + choose([500, 3000, 100_000_000])),
       body: if(chance(0.4), do: body(choose(@types)))
     )}
  end

  # Mostly from an active source to a free path.
  defp generate(:fork, %{model: model} = ctx) do
    {source, path} = fork_paths(ctx)
    src = Model.get(model, source)
    ct = if src, do: src.meta.content_type, else: choose(@types)

    {:create, path,
     present(
       forked_from: source,
       content_type: if(chance(0.8), do: ct, else: choose(@types)),
       fork_offset: if(chance(0.5), do: fork_offset(model, source)),
       fork_sub_offset: if(chance(0.25), do: choose([0, 1, 2, 3])),
       ttl_s: if(chance(0.1), do: choose([1, 3600])),
       expires_at_ms: if(chance(0.05), do: model.now + choose([500, 100_000_000])),
       closed: chance(0.05),
       body: if(chance(0.3), do: body(ct))
     )}
  end

  defp generate(:trim, %{model: model, path: path}), do: {:trim, path, fork_offset(model, path)}

  defp generate(:invalid, %{path: path}) do
    choose([
      {:invalid, {:create, path, [ttl_s: "bad"]}, :invalid_ttl},
      {:invalid, {:append, path, :body, []}, :invalid_body},
      {:invalid, {:trim, path, -1}, :invalid_offset},
      {:invalid, {:read, path, -1, 1}, :invalid_offset}
    ])
  end

  defp generate(:advance, _ctx), do: {:advance, choose([400, 1000, 2500])}
  defp generate(:sweep, _ctx), do: {:sweep}
  defp generate(:repair, _ctx), do: {:repair}

  defp generate(:append, %{path: path, stream: stream}) do
    ct = if stream, do: stream.meta.content_type, else: choose(@types)

    opts =
      present(
        content_type: if(chance(0.15), do: choose(@types)),
        stream_seq: if(chance(0.25), do: choose(["a", "b", "c", "d"])),
        close: chance(0.04),
        producer: if(chance(0.4), do: producer(stream))
      )

    {:append, path, if(chance(0.08), do: "", else: body(ct)), opts}
  end

  defp generate(:close, %{path: path, stream: stream}),
    do: {:append, path, "", present(close: true, producer: if(chance(0.5), do: producer(stream)))}

  defp generate(:delete, %{path: path}), do: {:delete, path}
  defp generate(:head, %{path: path}), do: {:head, path}
  defp generate(:kill, %{path: path}), do: {:kill, path}

  # A path's own group: creating it there fails for the rest of the run.
  defp generate(:seal, %{path: path}), do: {:seal, path}

  # The path itself, or a shorter prefix in another placement group.
  defp generate(:list, %{path: path}),
    do: {:list, if(chance(0.7), do: path, else: binary_part(path, 0, byte_size(path) - 1))}

  defp generate(:read, %{model: model, path: path}) do
    offset = if chance(0.1), do: :now, else: choose(Model.boundaries(model, path))
    offset = if chance(0.05) and offset != :now, do: offset + 1000, else: offset
    {:read, path, offset, choose([1, 10, 100, 1_000_000])}
  end

  defp fork_paths(%{model: model, paths: paths, path: path}) do
    active = Enum.filter(paths, &Model.get(model, &1))
    free = Enum.reject(paths, &(&1 in Model.paths(model)))
    source = if active != [] and chance(0.8), do: choose(active), else: choose(paths)
    {source, if(free != [] and chance(0.8), do: choose(free), else: path)}
  end

  # The options that are set.
  defp present(opts), do: Enum.reject(opts, fn {_, v} -> v in [nil, false] end)

  # A message boundary, sometimes past the tail.
  defp fork_offset(model, path) do
    offset = choose(Model.boundaries(model, path))
    if chance(0.05), do: offset + 1000, else: offset
  end

  defp producer(stream) do
    id = choose(["pa", "pb"])

    {epoch, last} =
      case stream && Map.get(stream.producers, id) do
        {e, l} -> {e, l}
        _ -> {0, -1}
      end

    epoch = choose([epoch, epoch, epoch, max(epoch - 1, 0), epoch + 1])
    seq = max(0, choose([last + 1, last + 1, last, last - 1, last + 2, 0]))
    {id, epoch, seq}
  end

  defp body(content_type) do
    if Streams.ContentType.json?(content_type) do
      choose([
        ~s({"n": #{int(99)}}),
        "[#{int(9)}, #{int(9)}]",
        ~s(["a,b", {"c": [1, 2]}]),
        "[]",
        "{bad",
        "  7  "
      ])
    else
      choose(["x", "hello", String.duplicate("z", int(40)), bytes(20)])
    end
  end

  # Choices within a command, from the :rand state seeded for it.
  defp int(max), do: :rand.uniform(max + 1) - 1
  defp choose(list), do: Enum.at(list, :rand.uniform(length(list)) - 1)
  defp chance(p), do: :rand.uniform() < p
  defp bytes(max), do: for(_ <- 1..int(max)//1, into: <<>>, do: <<:rand.uniform(256) - 1>>)
end
