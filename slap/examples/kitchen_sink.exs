{:ok, _} = Application.ensure_all_started(:mix)

if is_nil(Mix.Project.get()) do
  Mix.install([{:slap, "~> 0.7.1"}])
end

defmodule Slap.Examples.DemoDoc do
  use Slap.Yjs.DocServer
end

defmodule Slap.Examples.CounterCluster do
  use Slap.Cluster, otp_app: :slap
end

# The shard child is the one writer of its shard's database. Callers reach it
# through Slap.Cluster.call/4, which runs put/2 on the node that owns the shard.
defmodule Slap.Examples.Counter do
  @behaviour Slap.Cluster.ShardChildren

  use GenServer

  alias Slap.{Cluster, SlateDB}

  @impl Slap.Cluster.ShardChildren
  def child_specs(ctx), do: [{__MODULE__, ctx}]

  def start_link(ctx), do: GenServer.start_link(__MODULE__, ctx.db, name: name(ctx))

  def put(ctx, value), do: GenServer.call(name(ctx), {:put, value}, 30_000)

  defp name(ctx), do: Cluster.via(ctx.cluster, ctx.n, __MODULE__)

  @impl GenServer
  def init(db), do: {:ok, db}

  @impl GenServer
  def handle_call({:put, value}, _from, db) do
    {:ok, _} = SlateDB.put(db, "counter", value, await_durable: true)
    {:reply, :ok, db}
  end
end

defmodule Slap.Examples.KitchenSink do
  @moduledoc false

  alias Slap.{Cluster, Files, KV, SlateDB, SnapshotLog, Streams, Yjs}
  alias Slap.Examples.{Counter, CounterCluster, DemoDoc}
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
    with {opts, [], []} <-
           OptionParser.parse(args,
             strict: [interval: :integer, ticks: :integer, exit: :boolean]
           ),
         interval = Keyword.get(opts, :interval, 1000),
         ticks = Keyword.get(opts, :ticks, :infinity),
         exit? = Keyword.get(opts, :exit, false),
         true <- interval > 0 and (ticks == :infinity or ticks > 0),
         true <- not exit? or ticks != :infinity do
      {:ok, %{interval: interval, ticks: ticks, exit: exit?}}
    else
      _ -> :error
    end
  end

  defp usage(device) do
    IO.puts(device, """
    mix run examples/kitchen_sink.exs [--interval MS] [--ticks N [--exit]]
    elixir kitchen_sink.exs [--interval MS] [--ticks N [--exit]]

    The demo exits when you press Enter. --ticks N stops the updates after
    N ticks and keeps the data until you press Enter. With --exit, the demo
    exits after the last tick instead.
    """)
  end

  defp run(opts) do
    distribution()
    Logger.configure(level: :warning)
    SlateDB.set_log_level(:warning)

    children = [
      {Slap,
       store: :memory, streams: [shards: 2], kv: [shards: 2], files: [retention_ms: 5000], yjs: []},
      {CounterCluster,
       store: :memory,
       path: "demo-counter",
       shards: 1,
       shard_children: {Counter, :child_specs, []}}
    ]

    {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

    try do
      instructions(opts)
      exit_on_enter()
      state = seed()
      Process.send_after(self(), :tick, opts.interval)
      loop(state, opts)
    after
      :ok = Supervisor.stop(supervisor, :normal, 30_000)
    end
  end

  # The printed commands name this node and rely on Erlang's default cookie.
  defp distribution do
    case {Node.alive?(), :init.get_argument(:setcookie)} do
      {false, :error} ->
        {_, 0} = System.cmd("epmd", ["-daemon"])
        {:ok, _} = Node.start(:"slap_demo_#{System.pid()}@127.0.0.1", :longnames)

      _ ->
        IO.puts(
          :stderr,
          "Run the demo without --name, --sname or --cookie; it starts its own node."
        )

        exit({:shutdown, 1})
    end
  end

  defp seed do
    body = JSON.encode!(%{event: "started", tick: 0})
    {:ok, :created, _} = Streams.create(@events, content_type: "application/json", body: body)
    {:ok, :created, _} = Streams.create(@fork, forked_from: @events)
    {:ok, server} = Yjs.Docs.join(DemoDoc, @document, compact_bytes: 256)
    {:ok, doc} = Yjs.DocServer.doc(server, 5000)

    state = %{
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
    {:ok, :ok} = CounterCluster.call(0, {Counter, :put, [Integer.to_string(tick)]})
    {:ok, _} = Streams.append(@events, body)
    {:ok, _} = Streams.append(@fork, JSON.encode!(Map.put(event, :branch, "fork")))
    {:ok, offset} = SnapshotLog.append(@log, body)

    snapshot? = rem(tick, 5) == 0

    if snapshot? do
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

    IO.puts(if snapshot?, do: "Tick #{tick} (log snapshot)", else: "Tick #{tick}")

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
          finish(state, opts)
        else
          Process.send_after(self(), :tick, opts.interval)
          loop(state, opts)
        end

      :exit ->
        :ok

      {:slap_yjs_update, _document, _update} ->
        loop(state, opts)
    end
  end

  defp finish(state, %{exit: true}), do: IO.puts("Completed #{state.tick} ticks.")

  defp finish(state, _opts) do
    IO.puts("Completed #{state.tick} ticks. Press Enter to exit.")
    await_exit()
  end

  defp await_exit do
    receive do
      :exit -> :ok
      {:slap_yjs_update, _document, _update} -> await_exit()
    end
  end

  # Without a terminal, stdin reaches end of file at once; the demo then
  # keeps running until the VM is stopped.
  defp exit_on_enter do
    demo = self()

    spawn_link(fn ->
      case IO.gets("") do
        line when is_binary(line) -> send(demo, :exit)
        _eof_or_error -> :ok
      end
    end)
  end

  defp instructions(opts) do
    connection = "slap-inspect --node #{Node.self()}"

    shard = Cluster.shard_for(Streams.Cluster, Streams.placement_key(@events))
    partition = Config.partition(Config.get(), :record, "demo")

    IO.puts("""
    Slap kitchen sink demo on node #{Node.self()}

    All data is in memory and is lost when the demo exits.
    Press Enter to exit.

    Every #{opts.interval} ms, the demo:

      - appends to the stream #{@events} and its fork #{@fork}
      - appends to the snapshot log #{@log}, with a snapshot every 5 ticks
      - edits the Yjs document #{elem(@document, 0)}/#{elem(@document, 1)}
      - updates the KV row demo/counter
      - updates two files: demo/status.json (inline) and demo/report.txt (object)
      - writes the tick number to the one shard of #{inspect(CounterCluster)}

    Inspect from another terminal:

      #{connection}

    Or open one view:

      Streams: #{connection} streams --shard #{shard}
      Fork:    #{connection} logical --path #{@fork}
      Log:     #{connection} log --base #{@log}
      Yjs:     #{connection} yjs --base #{Yjs.Store.base(@document)}
      KV:      #{connection} kv --partition demo
      Files:   #{connection} files --partition #{partition}
      Counter: #{connection} --cluster #{inspect(CounterCluster)} scan --shard 0
      Raw:     #{connection} --cluster Slap.Streams.Cluster scan --shard #{shard}

    Add --json to a command to print one page and exit.

    In the Fork view, the first pages show messages inherited from
    #{@events}. Press n to reach the fork's own updates.

    Install the inspector with: mix escript.install hex slap_inspect
    """)
  end
end

Slap.Examples.KitchenSink.main(System.argv())
