defmodule Slap.Streams.ModelTest do
  # SLAP_STREAMS_PROP_RUNS (default 100) sets the sequence count;
  # `mix test --seed N` replays a run.
  use Slap.Streams.Test.ClusterCase, async: false
  use ExUnitProperties

  import Slap.Streams.Test.CreationFixtures, only: [reserve: 3, register: 1, call: 2, meta: 1]

  @moduletag :capture_log
  @moduletag timeout: :infinity

  alias Slap.SlateDB
  alias Slap.Streams
  alias Slap.Streams.Jobs.{Deleter, Expiry}
  alias Slap.Streams.{Reader, StreamServer}
  alias Slap.Streams.Store.{CopyCreation, CopyProgress, Identity, Meta}
  alias Slap.Streams.Test.{FakeClock, Model, ReadView}

  @commands 40
  @start_ms 1_700_000_000_000
  @max_fork_depth 3
  @moduletag child_options: [
               expiry_interval: :timer.hours(1),
               max_fork_depth: @max_fork_depth
             ]

  setup_all do
    FakeClock.install(@start_ms)
    on_exit(fn -> FakeClock.uninstall() end)
  end

  setup do
    # Only generated repair commands release the model's deferred holds.
    suspend_repair()
    :ok
  end

  property "a control transition fences stale appends and component creates after TTL touches" do
    check all(transitions <- list_of(boolean(), min_length: 2, max_length: 10), max_runs: 20) do
      base = "/guard-model-#{System.unique_integer([:positive])}"
      path = base <> "/.control"

      {:ok, _, %{next_offset: offset}} =
        Streams.create(path, body: "{}", content_type: "application/json", ttl_s: 10)

      {:ok, view} = Streams.metadata(path)
      identity = Identity.new(ctx_for(path), path, view.sid)

      Enum.reduce(Enum.with_index(transitions), offset, fn {advance?, n}, offset ->
        guard = %{identity: identity, offset: offset}

        next =
          if advance? do
            {:ok, %{next_offset: next}} =
              Streams.control_append(path, identity, offset, "{}", true, false, [])

            FakeClock.advance(2_000)

            assert {:error, :conflict} =
                     Streams.control_append(path, identity, offset, "{}", true, false, [])

            next
          else
            offset
          end

        result = Streams.create_guarded(base <> "/.child-#{n}", guard, body: "entry")

        if advance? do
          assert result == {:error, :conflict}
        else
          assert {:ok, :created, _} = result
          assert :ok = Streams.delete(base <> "/.child-#{n}")
        end

        next
      end)

      assert :ok = Streams.delete(path)
    end
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

  test "a read view predating deletion agrees with the model before and after cleanup" do
    start_supervised!({Registry, keys: :unique, name: __MODULE__.ReadViews})

    for fork? <- [false, true] do
      path = "/deleted-read-#{System.unique_integer([:positive])}"
      create = {:create, path, body: "old"}
      {expected, model} = Model.run(Model.new(FakeClock.now_ms(), @max_fork_depth), create)
      assert execute(create) == expected
      ctx = ctx_for(path)
      {:ok, view} = StreamServer.call(ctx, path, :read_info, 5_000)
      held = %{ctx | registry: __MODULE__.ReadViews}
      start_supervised!(Supervisor.child_spec({ReadView, {held, path, view}}, id: path))

      model =
        if fork? do
          command = {:create, path <> "-fork", forked_from: path}
          {expected, model} = Model.run(model, command)
          assert execute(command, model) == expected
          model
        else
          model
        end

      {expected, model} = Model.run(model, {:delete, path})
      deleter = GenServer.whereis(Slap.Cluster.via(Streams.Cluster, ctx.n, :deleter))
      :ok = :sys.suspend(deleter)

      try do
        assert execute({:delete, path}) == expected
        {expected, _} = Model.run(model, {:read, path, 0, 1_024})
        assert Reader.read(held, path, 0, 1_024, 5_000) == expected
      after
        :sys.resume(deleter)
      end

      :ok = Deleter.drain(ctx)
      {expected, _} = Model.run(model, {:read, path, 0, 1_024})
      assert Reader.read(held, path, 0, 1_024, 5_000) == expected
      delete_all(model)
      stop_supervised!(path)
    end
  end

  test "inherited sub-offsets and copy retries preserve captured pagination" do
    commands = [
      {:create, "/a", [content_type: "application/json", body: "[1]", closed: true]},
      {:create, "/b", [forked_from: "/a", fork_offset: 3, body: "[2]"]},
      {:create, "/one", [forked_from: "/b", fork_offset: 0, fork_sub_offset: 1]},
      {:create, "/two", [forked_from: "/b", fork_offset: 0, fork_sub_offset: 2]},
      {:create, "/one",
       [forked_from: "/b", content_type: "application/json", fork_offset: 0, fork_sub_offset: 1]},
      {:create, "/one",
       [forked_from: "/b", content_type: "application/json", fork_offset: 5, fork_sub_offset: 1]},
      {:copy, "/b", "/copy", []},
      {:append, "/b", "[3]", []},
      {:repair},
      {:copy, "/b", "/copy", []},
      {:copy, "/b", "/copy", [at: 0]},
      {:read, "/one", 0, 1},
      {:read, "/two", 0, 1},
      {:read, "/two", 3, 1},
      {:read, "/copy", 0, 1},
      {:read, "/copy", 3, 1},
      {:delete, "/one"},
      {:delete, "/two"},
      {:delete, "/b"},
      {:delete, "/a"},
      {:repair},
      {:copy, "/b", "/copy", []},
      {:read, "/copy", 0, 1}
    ]

    check_commands(commands)
  end

  test "repair cancels a copy that stops making progress, not one that is still copying" do
    check_commands([
      {:create, "/source", [body: "a"]},
      {:append, "/source", "b", []},
      {:append, "/source", "c", []},
      {:interrupt_copy, "/source", "/copying"},
      {:interrupt_copy, "/source", "/stalled"},
      {:advance, :timer.minutes(40)},
      {:copy_page, "/copying"},
      {:advance, :timer.minutes(40)},
      {:repair},
      {:head, "/copying"},
      {:head, "/stalled"},
      {:append, "/source", "d", []},
      {:copy, "/source", "/copying", []},
      {:read, "/copying", 0, 100},
      {:repair}
    ])
  end

  test "seals reject new derived streams before capture and preserve published retries" do
    check_commands([
      {:create, "/source", [body: "a"]},
      {:create, "/g/.reference", [forked_from: "/source"]},
      {:copy, "/source", "/g/.copy", []},
      {:seal, "/g"},
      {:create, "/g/.new-reference", [forked_from: "/source", fork_offset: 99]},
      {:copy, "/source", "/g/.new-copy", [at: 99]},
      {:head, "/g/.new-reference"},
      {:head, "/g/.new-copy"},
      {:create, "/g/.reference", [forked_from: "/source"]},
      {:copy, "/source", "/g/.copy", []},
      {:repair},
      {:read, "/g/.reference", 0, 100},
      {:read, "/g/.copy", 0, 100}
    ])
  end

  test "published fork retries keep their selected source route" do
    commands = [
      {:create, "/source", [body: "original"]},
      {:create, "/fork", [forked_from: "/source"]},
      {:create, "/fork", [forked_from: "/source", source_placement_key: "another-route"]},
      {:kill, "/fork"},
      {:create, "/fork", [forked_from: "/source", source_placement_key: "/source"]},
      {:read, "/fork", 0, 100}
    ]

    check_commands(commands)
  end

  test "inherited trims survive repair while reference and copy targets remain active" do
    check_commands([
      {:create, "/source", [body: "a"]},
      {:append, "/source", "b", []},
      {:append, "/source", "c", []},
      {:trim, "/source", 5},
      {:create, "/reference", [forked_from: "/source"]},
      {:copy, "/source", "/copy", []},
      {:repair},
      {:append, "/reference", "own", []},
      {:append, "/copy", "own", []},
      {:repair},
      {:read, "/reference", 5, 100},
      {:read, "/copy", 5, 100},
      {:delete, "/source"},
      {:repair},
      {:read, "/reference", 5, 100},
      {:read, "/copy", 5, 100}
    ])
  end

  test "cleanup remains pending across reads, retries and target path reuse" do
    step = fn command, model ->
      {expected, model} = Model.run(model, command)
      assert execute(command, model) == expected
      model
    end

    commands = [
      {:create, "/a", [body: "original"]},
      {:create, "/b", [forked_from: "/a"]},
      {:copy, "/a", "/copy", []},
      {:delete, "/b"},
      {:create, "/b", [forked_from: "/a", body: "replacement"]},
      {:delete, "/a"},
      {:head, "/a"},
      {:repair},
      {:head, "/a"},
      {:read, "/b", 0, 100},
      {:delete, "/b"}
    ]

    model = Enum.reduce(commands, Model.new(FakeClock.now_ms(), @max_fork_depth), step)
    assert {{:error, :gone}, _} = Model.run(model, {:head, "/a"})
    assert execute({:head, "/a"}) == {:error, :gone}

    model = step.({:repair}, model)
    assert execute({:head, "/a"}) == {:error, :not_found}

    model =
      Enum.reduce(
        [
          {:create, "/a", [body: "new source"]},
          {:copy, "/a", "/copy", []},
          {:read, "/copy", 0, 100}
        ],
        model,
        step
      )

    delete_all(model)
  end

  test "failed observations reserve their request and invalid target bodies release their holds" do
    request = [forked_from: "/pending-source", fork_offset: 0, fork_sub_offset: 3]

    commands = [
      {:create, "/pending-source", []},
      {:create, "/future-fork", [forked_from: "/pending-source", fork_offset: 9]},
      {:head, "/future-fork"},
      {:create, "/future-fork", [forked_from: "/pending-source", fork_offset: 0]},
      {:copy, "/pending-source", "/future-copy", [at: 9]},
      {:head, "/future-copy"},
      {:create, "/pending-target", request},
      {:head, "/pending-target"},
      {:append, "/pending-target", "x", []},
      {:copy, "/pending-source", "/pending-target", []},
      {:create, "/pending-target", Keyword.put(request, :body, "different")},
      {:append, "/pending-source", "hello", []},
      {:copy, "/pending-source", "/future-copy", [at: 9]},
      {:create, "/future-fork", [forked_from: "/pending-source", fork_offset: 9]},
      {:read, "/future-copy", 0, 100},
      {:read, "/future-fork", 0, 100},
      {:create, "/pending-target", request},
      {:read, "/pending-target", 0, 100},
      {:create, "/pending-abandon", Keyword.put(request, :fork_sub_offset, 99)},
      {:delete, "/pending-abandon"},
      {:create, "/reused-source", [body: "a"]},
      {:copy, "/reused-source", "/stale-copy", [at: 10]},
      {:create, "/stale-fork", [forked_from: "/reused-source", fork_offset: 10]},
      {:delete, "/reused-source"},
      {:copy, "/reused-source", "/stale-copy", [at: 10]},
      {:create, "/reused-source", [body: "replacement"]},
      {:copy, "/reused-source", "/stale-copy", [at: 10]},
      {:create, "/stale-fork", [forked_from: "/reused-source", fork_offset: 10]},
      {:head, "/stale-copy"},
      {:head, "/stale-fork"},
      {:create, "/json-source", [content_type: "application/json"]},
      {:create, "/invalid-body", [forked_from: "/json-source", body: "{bad"]},
      {:delete, "/json-source"},
      {:head, "/json-source"},
      {:repair},
      {:head, "/json-source"}
    ]

    check_commands(commands)
  end

  property "Streams behaves like the model" do
    runs = String.to_integer(System.get_env("SLAP_STREAMS_PROP_RUNS", "100"))
    seeds = list_of(integer(0..0xFFFF_FFFF), min_length: 1, max_length: @commands)

    check all(seeds <- seeds, max_runs: runs, initial_size: @commands) do
      prefix = "/run#{System.unique_integer([:positive])}"
      paths = for i <- 1..4, do: "#{prefix}/s#{i}"
      start = Model.new(FakeClock.now_ms(), @max_fork_depth)

      {model, _} =
        seeds
        |> Enum.with_index(1)
        |> Enum.reduce({start, []}, fn {seed, i}, {model, history} ->
          :rand.seed(:exsss, {seed, 0, 17})
          command = command(model, paths)
          history = [command | history]
          {expected, model} = Model.run(model, command)
          actual = execute(command, model)

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
    for ctx <- contexts() do
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

    repair_all(fn -> retention_counts() == %{} end)

    for path <- Model.paths(model) do
      assert Streams.head(path) == {:error, :not_found}, "#{path} left behind"
    end
  end

  defp check_commands(commands) do
    model =
      Enum.reduce(commands, Model.new(FakeClock.now_ms(), @max_fork_depth), fn command, model ->
        {expected, model} = Model.run(model, command)
        assert execute(command, model) == expected
        model
      end)

    delete_all(model)
  end

  defp execute(command, model \\ nil)

  defp execute({:repair}, model) do
    expected =
      Map.new(model.streams, fn {path, stream} -> {path, length(Map.get(stream, :forks, []))} end)

    repair_all(fn -> retention_counts() == expected end)
  end

  defp execute(command, _model), do: execute_request(command)

  defp repair_all(condition) do
    resume_repair()

    try do
      repair_until(condition)
    after
      suspend_repair()
    end

    assert condition.()
    :ok
  end

  defp execute_request({:copy, source, target, opts}), do: Streams.copy(source, target, opts)

  defp execute_request({:interrupt_copy, source, target}) do
    c = reserve(target, %{forked_from: source}, :copy)
    cap = register(c)
    {:ok, {:pending, _}} = call(c.target, {:capture, c.target, cap})
    :ok
  end

  defp execute_request({:copy_page, target}) do
    %Meta{
      creation: %CopyCreation{
        target: identity,
        progress: %CopyProgress{capture: cap, cursor: cursor}
      }
    } = meta(target)

    {:ok, {page, next}} = Streams.retained(cap, cursor, cap.offset, 1, 5_000, Streams.Cluster)
    {:ok, {:pending, ^next}} = call(identity, {:copy_page, identity, cursor, page, next})
    :ok
  end

  defp execute_request({:create, path, opts}), do: Streams.create(path, opts)
  defp execute_request({:trim, path, offset}), do: Streams.trim(path, offset)
  defp execute_request({:invalid, request, _reason}), do: execute_request(request)

  defp execute_request({:advance, ms}) do
    FakeClock.advance(ms)
    :ok
  end

  defp execute_request({:sweep}) do
    for ctx <- contexts(), do: Expiry.sweep(ctx)
    :ok
  end

  defp execute_request({:append, path, body, opts}), do: Streams.append(path, body, opts)

  defp execute_request({:delete, path}), do: Streams.delete(path)

  defp execute_request({:head, path}), do: Streams.head(path)
  defp execute_request({:list, prefix}), do: Streams.list(prefix)
  defp execute_request({:seal, group}), do: Streams.seal(group)

  defp execute_request({:read, path, offset, max}) do
    Streams.read(path, offset, max_bytes: max, timeout: 30_000)
  end

  defp execute_request({:kill, path}) do
    if pid = server(path) do
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}
    end

    :ok
  end

  @types ["text/plain", "application/json", nil, "Text/Plain; charset=utf-8"]

  @kinds [
    create: 10,
    copy: 5,
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

  defp generate(:copy, %{model: model, path: path, paths: paths}) do
    source = Enum.random(paths)
    {:copy, source, path, at: if(chance(0.5), do: :latest, else: fork_offset(model, source))}
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

  defp generate(:seal, %{path: path}), do: {:seal, path}

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

  defp present(opts), do: Enum.reject(opts, fn {_, v} -> v in [nil, false] end)

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
