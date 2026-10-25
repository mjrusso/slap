defmodule DocumentForkMeasurements.Server do
  use Slap.Yjs.DocServer, restart: :temporary
  @impl Yex.DocServer
  def handle_call(:shape, _from, state),
    do:
      {:reply,
       {Yex.Text.length(Yex.Doc.get_text(state.doc, "text")),
        Yex.Array.to_list(Yex.Doc.get_array(state.doc, "set"))}, state}

  def handle_call(:snapshot, _from, state),
    do: {:reply, Yex.encode_state_as_update!(state.doc), state}
end

defmodule DocumentForkMeasurements do
  alias Slap.{SlateDB, Streams, Yjs}
  alias Slap.Streams.Jobs.{Deleter, Repair}
  alias Slap.Streams.Store.{Keys, Read}

  @sizes [1024, 64 * 1024 * 1024, 1024 * 1024 * 1024]
  @forks 5
  @updates 4

  def run do
    for size <- @sizes, do: measure(size)
  end

  defp measure(size) do
    dir = Path.join(System.tmp_dir!(), "document-fork-bench-#{size}")
    File.rm_rf!(dir)

    {:ok, cluster} =
      Streams.Cluster.start_link(
        store: {:local, dir},
        shards: 2,
        settings: %{flush_interval: "10ms"},
        child_options: [repair_interval: 3_600_000, deleter_interval: 3_600_000]
      )

    {:ok, docs} = Yjs.Docs.start_link()

    try do
      actual = populate(size)
      for pid <- Process.list(), do: :erlang.garbage_collect(pid)
      snapshot_path = Yjs.Store.path({"bench", "source"}, {:snapshot, 0})
      {:ok, view} = Streams.metadata(snapshot_path)
      source_sid = view.sid
      source_shard = Streams.Cluster.shard_for(Yjs.Store.base({"bench", "source"}))

      for n <- 1..@forks do
        target = {"bench", "target-#{n}"}

        stats =
          observe(
            fn -> Yjs.Store.fork({"bench", "source"}, target, timeout: 300_000) end,
            source_sid,
            source_shard
          )

        output(
          Map.merge(stats, %{
            operation: "fork",
            snapshot_bytes: actual,
            depth: 1,
            update_count: @updates,
            fork_count: @forks,
            sample: n
          })
        )
      end

      target = {"bench", "target-1"}

      stats =
        observe(
          fn ->
            Yjs.Docs.join(DocumentForkMeasurements.Server, target,
              timeout: 300_000,
              compact_on_stop: false
            )
          end,
          source_sid,
          source_shard
        )

      output(
        Map.merge(stats, %{
          operation: "first-join",
          snapshot_bytes: actual,
          depth: 1,
          update_count: @updates,
          fork_count: @forks
        })
      )

      pid = Yjs.Docs.whereis(DocumentForkMeasurements.Server, target)
      {payload_size, values} = GenServer.call(pid, :shape, 300_000)
      true = values == [1, 2, 3, 4]
      true = payload_size > actual - 100
      {:ok, at} = Yjs.Store.tail(target)
      snapshot = GenServer.call(pid, :snapshot, 300_000)
      :ok = Yjs.Store.snapshot(target, at, snapshot, timeout: 300_000)
      {:ok, own_view} = Streams.metadata(Yjs.Store.path(target, {:snapshot, at}))
      {:ok, updates_view} = Streams.metadata(Yjs.Store.path({"bench", "source"}, :updates))
      target_shard = Streams.Cluster.shard_for(Yjs.Store.base(target))
      sids = [{source_shard, source_sid}, {target_shard, own_view.sid}]
      source_sid = {source_shard, source_sid}
      updates_sid = {source_shard, updates_view.sid}
      settle(source_sid, @forks - 1, updates_sid, @forks)
      retained("after-compaction", actual, sids, updates_sid)
      :ok = Yjs.Store.delete_doc({"bench", "source"}, timeout: 300_000)
      settle(source_sid, @forks - 1, updates_sid, @forks)
      retained("after-source-deletion", actual, sids, updates_sid)

      for n <- 2..@forks,
          do: :ok = Yjs.Store.delete_doc({"bench", "target-#{n}"}, timeout: 300_000)

      settle(source_sid, 0, updates_sid, 1)
      retained("after-other-branches-deleted", actual, sids, updates_sid)
      :ok = Yjs.Docs.delete(target, timeout: 300_000)
      settle(source_sid, 0, updates_sid, 0)
      retained("after-final-branch-deletion", actual, sids, updates_sid)
    after
      Supervisor.stop(docs, :normal, 300_000)
      Supervisor.stop(cluster, :normal, 300_000)
      File.rm_rf!(dir)
    end
  end

  defp settle(snapshot, snapshots, updates, update_count) do
    deadline = System.monotonic_time(:millisecond) + 300_000
    settle(snapshot, snapshots, updates, update_count, deadline)
  end

  defp settle(snapshot, snapshots, updates, update_count, deadline) do
    if System.monotonic_time(:millisecond) >= deadline, do: raise("retention cleanup deadline")
    for ctx <- shards(), do: Repair.sweep(ctx)
    for ctx <- shards(), do: Deleter.drain(ctx)

    if registrations(snapshot) != snapshots or registrations(updates) != update_count,
      do: settle(snapshot, snapshots, updates, update_count, deadline)
  end

  defp registrations({shard, sid}) do
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(shard)
    Enum.count(Read.registrations(ctx.db, sid, durability: :remote))
  end

  defp retained(stage, size, snapshots, updates) do
    output(%{
      operation: "retained-storage",
      stage: stage,
      snapshot_bytes: size,
      snapshot_message_bytes: Enum.sum_by(snapshots, &message_bytes/1),
      source_update_bytes: message_bytes(updates),
      source_update_registrations: registrations(updates)
    })
  end

  defp message_bytes({shard, sid}) do
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(shard)

    ctx.db
    |> SlateDB.scan(prefix: Keys.msg(sid, 0, 0) |> binary_part(0, 9), durability: :remote)
    |> Enum.sum_by(fn {_, bytes} -> byte_size(bytes) end)
  end

  defp populate(size) do
    source = {"bench", "source"}
    {:ok, _} = Yjs.Store.load(source)
    {doc, snapshot} = snapshot_document(size, max(size - 30, 1))
    :ok = Yjs.Store.snapshot(source, 0, snapshot, timeout: 300_000)

    for n <- 1..@updates do
      vector = Yex.encode_state_vector!(doc)
      Yex.Array.insert(Yex.Doc.get_array(doc, "set"), n - 1, n)

      {:ok, _} =
        Yjs.Store.append(source, Yjs.Frame.frame(Yex.encode_state_as_update!(doc, vector)),
          timeout: 300_000
        )
    end

    byte_size(snapshot)
  end

  defp snapshot_document(size, payload_size) do
    doc = Yex.Doc.with_options(%Yex.Doc.Options{client_id: 1})
    Yex.Text.insert(Yex.Doc.get_text(doc, "text"), 0, :binary.copy("a", payload_size))
    snapshot = Yex.encode_state_as_update!(doc)

    if byte_size(snapshot) == size,
      do: {doc, snapshot},
      else: snapshot_document(size, payload_size + size - byte_size(snapshot))
  end

  defp observe(fun, source_sid, source_shard) do
    parent = self()
    baseline = %{baseline_beam_bytes: :erlang.memory(:total), baseline_rss_bytes: rss()}

    tracer =
      spawn(fn ->
        trace_loop(parent, source_sid, source_shard, %{
          snapshot_bytes_read: 0,
          bytes_written: 0,
          calls: %{}
        })
      end)

    :erlang.trace_pattern({Read, :read_msgs, 5}, [{:_, [], [{:return_trace}]}], [:local])
    :erlang.trace_pattern({SlateDB, :write, 3}, true, [:local])
    :erlang.trace_pattern({Slap.SlateDB.Transaction, :put, 4}, true, [:local])
    :erlang.trace(:all, true, [:call, {:tracer, tracer}])

    monitor =
      spawn(fn -> memory(parent, baseline.baseline_beam_bytes, baseline.baseline_rss_bytes) end)

    {us, {:ok, _}} = :timer.tc(fun)
    ref = :erlang.trace_delivered(:all)
    receive do: ({:trace_delivered, :all, ^ref} -> :ok)
    :erlang.trace(:all, false, [:call])
    :erlang.trace_pattern({Read, :read_msgs, 5}, false, [:local])
    :erlang.trace_pattern({SlateDB, :write, 3}, false, [:local])
    :erlang.trace_pattern({Slap.SlateDB.Transaction, :put, 4}, false, [:local])
    send(tracer, :result)
    send(monitor, :result)
    io = receive do: ({:io, io} -> io)
    peak = receive do: ({:peak, peak} -> peak)
    Map.merge(Map.merge(Map.delete(io, :calls), baseline), Map.put(peak, :latency_us, us))
  end

  defp trace_loop(parent, source_sid, source_shard, stats) do
    receive do
      {:trace, pid, :call, {Read, :read_msgs, [db, sid, _, _, _]}} ->
        source? =
          sid == source_sid and Enum.any?(shards(), &(&1.n == source_shard and &1.db == db))

        trace_loop(parent, source_sid, source_shard, put_in(stats, [:calls, pid], source?))

      {:trace, pid, :return_from, {Read, :read_msgs, 5}, {messages, _}} ->
        bytes =
          if stats.calls[pid],
            do: Enum.sum_by(messages, fn {_, bytes} -> byte_size(bytes) end),
            else: 0

        trace_loop(parent, source_sid, source_shard, %{
          stats
          | snapshot_bytes_read: stats.snapshot_bytes_read + bytes
        })

      {:trace, _, :call, {SlateDB, :write, [_, ops, _]}} ->
        trace_loop(parent, source_sid, source_shard, %{
          stats
          | bytes_written: stats.bytes_written + op_bytes(ops)
        })

      {:trace, _, :call, {Slap.SlateDB.Transaction, :put, [_, key, value, _]}} ->
        trace_loop(parent, source_sid, source_shard, %{
          stats
          | bytes_written: stats.bytes_written + byte_size(key) + byte_size(value)
        })

      :result ->
        send(parent, {:io, stats})

      _ ->
        trace_loop(parent, source_sid, source_shard, stats)
    end
  end

  defp op_bytes(ops),
    do:
      Enum.sum_by(ops, fn
        {:put, key, value} -> byte_size(key) + byte_size(value)
        {:delete, key} -> byte_size(key)
      end)

  defp memory(parent, beam, resident) do
    receive do
      :result -> send(parent, {:peak, %{peak_beam_bytes: beam, peak_rss_bytes: resident}})
    after
      5 -> memory(parent, max(beam, :erlang.memory(:total)), max(resident, rss()))
    end
  end

  defp rss do
    [_, kb] = Regex.run(~r/^VmRSS:\s+(\d+) kB/m, File.read!("/proc/self/status"))
    String.to_integer(kb) * 1024
  end

  defp shards,
    do:
      Enum.map(Streams.Cluster.local_shards(), fn n ->
        {:ok, {:local, ctx}} = Streams.Cluster.lookup(n)
        ctx
      end)

  defp output(stats), do: IO.puts(JSON.encode!(stats))
end

DocumentForkMeasurements.run()
