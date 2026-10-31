{:ok, _} = Application.ensure_all_started(:mix)

if is_nil(Mix.Project.get()) do
  Mix.install([:slap])
end

defmodule Slap.Examples.DemoDoc do
  use Slap.Yjs.DocServer
end

defmodule Slap.Examples.KitchenSink do
  @moduledoc false

  alias Slap.{Cluster, Files, KV, SlateDB, SnapshotLog, Streams, Yjs}
  alias Slap.Examples.DemoDoc
  alias Slap.Files.Config

  @events "/demo/events"
  @fork "/demo/events-fork"
  @log "/demo/log"
  @document {"demo", "notes"}

  def main(args) do
    case options(args) do
      {:ok, opts} ->
        run(opts)

      :help ->
        usage(:stdio)

      :error ->
        usage(:stderr)
        exit({:shutdown, 1})
    end
  end

  defp options(["--help"]), do: :help

  defp options(args) do
    with {opts, [], []} <- OptionParser.parse(args, strict: [interval: :integer, ticks: :integer]),
         interval = Keyword.get(opts, :interval, 1000),
         ticks = Keyword.get(opts, :ticks, :infinity),
         true <- interval > 0 and (ticks == :infinity or ticks > 0) do
      {:ok, %{interval: interval, ticks: ticks}}
    else
      _ -> :error
    end
  end

  defp usage(device) do
    IO.puts(device, """
    mix run examples/kitchen_sink.exs [--interval MS] [--ticks N]
    elixir kitchen_sink.exs [--interval MS] [--ticks N]
    """)
  end

  defp run(opts) do
    cookie_file = distribution()
    Logger.configure(level: :warning)
    SlateDB.set_log_level(:warning)

    children = [
      {Streams.Cluster, store: :memory, path: "demo-streams", shards: 2},
      {KV.Cluster, store: :memory, path: "demo-kv", shards: 2},
      {Files, store: :memory, path: "demo-files", retention_ms: 5000},
      Yjs.Docs
    ]

    {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)
    {:ok, db} = SlateDB.open("demo-native", store: :memory)

    try do
      state = seed(db)
      instructions(cookie_file, opts)
      Process.send_after(self(), :tick, opts.interval)
      loop(state, opts)
    after
      :ok = Supervisor.stop(supervisor, :normal, 30_000)
      :ok = SlateDB.close(db)
    end
  end

  defp distribution do
    unless Node.alive?() do
      {_, 0} = System.cmd("epmd", ["-daemon"])
      {:ok, _} = Node.start(:"slap_demo_#{System.pid()}@127.0.0.1", :longnames)
      cookie = :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower) |> String.to_atom()
      true = Node.set_cookie(cookie)
    end

    directory =
      Path.join(
        System.tmp_dir!(),
        "slap-demo-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    File.mkdir!(directory)
    File.chmod!(directory, 0o700)
    path = Path.join(directory, "cookie")
    File.write!(path, Atom.to_string(Node.get_cookie()))
    File.chmod!(path, 0o600)

    System.at_exit(fn _status ->
      File.rm(path)
      File.rmdir(directory)
    end)

    path
  end

  defp seed(db) do
    body = JSON.encode!(%{event: "started", tick: 0})
    {:ok, :created, _} = Streams.create(@events, content_type: "application/json", body: body)
    {:ok, :created, _} = Streams.create(@fork, forked_from: @events)
    {:ok, server} = Yjs.Docs.join(DemoDoc, @document, compact_bytes: 256)
    {:ok, doc} = Yjs.DocServer.doc(server, 5000)

    state = %{
      db: db,
      server: server,
      doc: doc,
      tick: 0,
      kv_version: :absent,
      inline_version: :absent,
      object_version: :absent
    }

    state = write(state)
    {:error, {:conflict, current}} = KV.put("demo", "counter", "conflicting", if_version: :absent)
    true = current == state.kv_version
    state
  end

  defp write(state) do
    tick = state.tick
    event = %{event: "tick", tick: tick, at: DateTime.to_iso8601(DateTime.utc_now())}
    body = JSON.encode!(event)
    {:ok, _} = SlateDB.put(state.db, "counter", Integer.to_string(tick), await_durable: true)
    {:ok, counter} = SlateDB.get(state.db, "counter", durability: :remote)
    {:ok, _} = Streams.append(@events, body)
    {:ok, _} = Streams.append(@fork, JSON.encode!(Map.put(event, :branch, "fork")))
    {:ok, offset} = SnapshotLog.append(@log, body)

    if rem(tick, 5) == 0 do
      snapshot = JSON.encode!(%{count: tick + 1, last_event: event})
      :ok = SnapshotLog.snapshot(@log, offset, snapshot)
    end

    :ok =
      Yex.Doc.transaction(state.doc, fn ->
        text = Yex.Doc.get_text(state.doc, "text")
        :ok = Yex.Text.insert(text, Yex.Text.length(text), "Tick #{tick}\n")
        status = Yex.Doc.get_map(state.doc, "status")
        :ok = Yex.Map.set(status, "tick", tick)
        :ok = Yex.Map.set(status, "updated_at", event.at)
      end)

    :ok = Yjs.DocServer.sync(state.server, 5000)
    {:ok, kv_version} = KV.put("demo", "counter", body, if_version: state.kv_version)

    {:ok, inline} =
      Files.put({"demo", "status.json"}, body,
        if_version: state.inline_version,
        content_type: "application/json",
        storage: :inline
      )

    {:ok, object} =
      Files.put({"demo", "report.txt"}, body <> "\n" <> String.duplicate("demo report\n", 2048),
        if_version: state.object_version,
        content_type: "text/plain",
        metadata: %{"tick" => Integer.to_string(tick)},
        storage: :object
      )

    IO.puts(
      "Tick #{tick}: streams, fork, log, Yjs, KV and both files updated; SlateDB counter=#{counter}"
    )

    %{
      state
      | kv_version: kv_version,
        inline_version: inline.version,
        object_version: object.version
    }
  end

  defp loop(state, opts) do
    receive do
      :tick ->
        state = write(%{state | tick: state.tick + 1})

        if state.tick == opts.ticks do
          IO.puts("Completed #{state.tick} timed updates.")
        else
          Process.send_after(self(), :tick, opts.interval)
          loop(state, opts)
        end

      {:slap_yjs_update, _document, _update} ->
        loop(state, opts)
    end
  end

  defp instructions(cookie_file, opts) do
    naming = if :net_kernel.longnames(), do: " --longnames", else: ""

    connection =
      "slap-inspect#{naming} --node #{Node.self()} --cookie-file #{quote_arg(cookie_file)}"

    streams = connection <> " --cluster Slap.Streams.Cluster"
    kv = connection <> " --cluster Slap.KV.Cluster"
    shard = Cluster.shard_for(Streams.Cluster, Streams.placement_key(@events))
    partition = Config.partition(Config.get(), :record, "demo")

    IO.puts("""

    Kitchen sink demo: all application data is in memory and is lost on exit.
    Updating every #{opts.interval} ms. Stop with Ctrl+C, then a.
    Streams shards: #{inspect(Cluster.local_shards(Streams.Cluster))}
    KV shards: #{inspect(Cluster.local_shards(KV.Cluster))}

    Inspect from another terminal (install with: mix escript.install hex slap_inspect):

      Streams: #{streams} streams --shard #{shard}
      Fork:    #{streams} logical --path #{@fork}
      Log:     #{streams} log --base #{@log}
      Yjs:     #{streams} yjs --base #{Yjs.Store.base(@document)}
      KV:      #{kv} kv --partition demo
      Files:   #{kv} files --partition #{partition}
      Raw:     #{streams} scan --shard #{shard}

    Add --json to any command to print one page and exit.
    In the Fork view, press n to move from inherited data to the fork's own updates.
    The standalone SlateDB handle is separate from the inspected cluster handles.
    """)

    unless Code.ensure_loaded?(Slap.Streams.Introspect) do
      IO.puts(
        "These Slap packages do not support slap-inspect yet; use a checkout or release with inspection support."
      )
    end
  end

  defp quote_arg(value), do: "'" <> String.replace(value, "'", "'\\''") <> "'"
end

Slap.Examples.KitchenSink.main(System.argv())
