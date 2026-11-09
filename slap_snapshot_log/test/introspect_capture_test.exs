defmodule Slap.SnapshotLog.IntrospectCaptureTest do
  # The named Streams cluster and inspection admission are shared within the node.
  use ExUnit.Case, async: false

  alias Slap.{SnapshotLog, Streams}
  alias Slap.SnapshotLog.{Introspect, Store}

  setup do
    start_supervised!({Streams.Cluster, store: :memory, shards: 2})
    assert :ok = Streams.Cluster.await_ready(5000)
    :ok
  end

  test "snapshot log listings exclude unrelated streams" do
    for base <- ["/listed/a", "/listed/b"], do: assert({:ok, _} = SnapshotLog.append(base, "x"))
    assert {:ok, _, _} = Streams.create("/listed/plain", body: "x")
    assert {:ok, _, _} = Streams.create("/listed/not-a-log/.index", body: "x")

    listed =
      for shard <- 0..1,
          {:ok, %{rows: rows, stopped: :end}} =
            Introspect.list(Streams.Cluster, shard, 1, 5000, []),
          %{decoded: {:ok, %{kind: :log, base: base}}} <- rows,
          do: base

    assert Enum.sort(listed) == ["/listed/a", "/listed/b"]
  end

  test "capture returns component identities, the selected snapshot and bounded input size" do
    base = "/capture/current"
    assert {:ok, first} = SnapshotLog.append(base, "first")
    assert :ok = SnapshotLog.snapshot(base, first, "snapshot")
    assert {:ok, tail} = SnapshotLog.append(base, "later")

    assert {:ok, capture} = capture(base)
    assert capture.current.offset == first
    assert capture.snapshot.offset == first
    assert capture.snapshot.path == SnapshotLog.path(base, {:snapshot, first})
    assert capture.snapshot.closed
    assert capture.from == first
    assert capture.until == tail
    assert capture.tail == tail
    assert capture.encoded_bytes == tail - first + byte_size("snapshot") + 4
    assert capture.owner == node()

    for component <- [capture.control, capture.index, capture.updates, capture.snapshot] do
      assert is_integer(component.sid)
      assert component.shard == shard(base)
    end

    assert {:ok, history} = capture(base, at_offset: first)
    assert history.history
    assert history.snapshot == nil
    assert history.from == 0
    assert history.until == first
    assert history.encoded_bytes == first
    assert {:error, :not_an_entry_boundary} = capture(base, at_offset: tail + 1)

    assert {:ok, _} = SnapshotLog.append(base, "newer")
    assert {:ok, later} = capture(base)
    assert later.identity == capture.identity
    assert later.until > capture.until
  end

  test "history capture survives a missing current snapshot and later publication" do
    base = "/capture/history"
    assert {:ok, first} = SnapshotLog.append(base, "first")
    assert {:ok, tail} = SnapshotLog.append(base, "later")

    assert catch_throw(
             SnapshotLog.snapshot_internal(base, first, "snapshot",
               after_step: fn
                 :index -> throw(:published)
                 _step -> :ok
               end
             )
           ) == :published

    assert {:ok, before} = capture(base, at_offset: first)
    assert :ok = Streams.delete(SnapshotLog.path(base, {:snapshot, first}))
    assert {:error, :capture_changed} = capture(base)
    assert {:ok, history} = capture(base, at_offset: first)
    assert history.snapshot == nil
    assert history.from == 0
    assert history.until == first
    assert history.encoded_bytes == first
    assert history.identity == before.identity

    assert catch_throw(
             SnapshotLog.snapshot_internal(base, tail, "later snapshot",
               after_step: fn
                 :index -> throw(:published)
                 _step -> :ok
               end
             )
           ) == :published

    assert {:ok, later} = capture(base, at_offset: first)
    assert later.identity == history.identity
    assert later.until == first
    assert :ok = SnapshotLog.delete(base)
    assert {:error, _} = capture(base, at_offset: first)
  end

  test "a fork's snapshot identity belongs to its target shard" do
    source = "/capture/source"

    target =
      Enum.find_value(1..100, fn n ->
        path = "/capture/fork-#{n}"
        if shard(path) != shard(source), do: path
      end)

    assert {:ok, at} = SnapshotLog.append(source, "entry")
    assert :ok = SnapshotLog.snapshot(source, at, "snapshot")
    assert {:ok, _} = SnapshotLog.fork(source, target)
    assert {:ok, capture} = capture(target)
    assert capture.snapshot.offset == at
    assert capture.snapshot.path == SnapshotLog.path(target, {:snapshot, at})
    assert capture.snapshot.shard == shard(target)
    assert capture.from == at
    assert capture.until == at
  end

  test "pending and cancelled control states are rejected before component selection" do
    base = "/capture/pending"
    control = base <> "/.control"
    assert {:ok, _, _} = Streams.create(control, content_type: "application/json")

    for {status, error} <- [{"pending", :log_busy}, {"cancelled", :deleted}] do
      assert {:ok, _} =
               Streams.append(control, JSON.encode!(%{"kind" => "fork", "status" => status}))

      assert {:error, ^error} = capture(base)
      assert {:error, ^error} = capture(base, at_offset: 0)
    end
  end

  test "missing logs remain missing, and legacy logs need no control stream" do
    base = "/capture/legacy"
    assert {:error, :stream_not_observed} = capture(base)
    assert {:error, :not_found} = Streams.head(SnapshotLog.path(base, :updates))
    assert {:ok, _, _} = Streams.create(SnapshotLog.path(base, :updates), body: "entry")
    assert {:ok, _, _} = Streams.create(SnapshotLog.path(base, :index))
    assert {:ok, %{control: nil, current: nil, snapshot: nil, from: 0}} = capture(base)
    assert {:ok, _} = Streams.close(SnapshotLog.path(base, :updates))
    assert {:error, :deleted} = capture(base)
    assert {:error, :deleted} = capture(base, at_offset: 0)
  end

  test "capture and index inspection select the current entry as the log does" do
    for {suffix, body, current} <- [
          {"malformed", "{}", :previous},
          {"deleted", ~s({"deleted":true}), :deleted}
        ] do
      base = "/capture/" <> suffix
      assert {:ok, at} = SnapshotLog.append(base, "entry")
      assert :ok = SnapshotLog.snapshot(base, at, "snapshot")
      path = SnapshotLog.path(base, :index)
      assert {:ok, _} = Streams.append(path, body)
      {:ok, %{sid: sid}} = Streams.metadata_fenced(path)

      assert {:ok, %{current: selected}} =
               Introspect.messages(Streams.Cluster, shard(base), 1, 5000,
                 sid: sid,
                 component: :index,
                 base: base
               )

      case current do
        :previous ->
          assert {:ok, %{current: %{offset: ^at}, deleted: nil}} = Store.state(base)
          assert {:ok, %{offset: ^at}} = selected
          assert {:ok, %{current: %{offset: ^at}}} = capture(base)

        :deleted ->
          assert {:ok, %{deleted: deleted}} = Store.state(base)
          assert is_integer(deleted)
          assert {:error, :deleted} = selected
          assert {:error, :deleted} = capture(base)
      end
    end
  end

  test "selection reads the complete index across pages and multipart entries" do
    base = "/capture/pages"
    assert {:ok, at} = SnapshotLog.append(base, "entry")
    assert :ok = SnapshotLog.snapshot(base, at, "snapshot")
    index = SnapshotLog.path(base, :index)
    assert {:ok, %{messages: [{_, body}]}} = Streams.read(index, :start)

    for _ <- 1..201, do: assert({:ok, _} = Streams.append(index, body))

    padded = body |> JSON.decode!() |> Map.put("padding", String.duplicate("x", 256 * 1024))
    assert {:ok, _} = Streams.append(index, JSON.encode!(padded))
    assert {:ok, %{current: %{offset: ^at}, snapshot: %{offset: ^at}}} = capture(base)

    {:ok, %{sid: sid}} = Streams.metadata_fenced(index)
    opts = [sid: sid, component: :index, base: base, limit: 200]

    assert {:ok, %{cursor: cursor, current: {:error, :index_incomplete}}} =
             Introspect.messages(Streams.Cluster, shard(base), 1, 5000, opts)

    assert is_binary(cursor)

    assert {:ok, %{cursor: nil, current: {:error, :index_incomplete}}} =
             Introspect.messages(
               Streams.Cluster,
               shard(base),
               1,
               5000,
               Keyword.put(opts, :cursor, cursor)
             )
  end

  test "index selection requires coverage through the durable tail" do
    base = "/capture/partial-index"
    assert {:ok, first} = SnapshotLog.append(base, "first")
    assert :ok = SnapshotLog.snapshot(base, first, "first snapshot")
    index = SnapshotLog.path(base, :index)
    assert {:ok, %{next_offset: index_boundary}} = Streams.head(index)
    assert {:ok, second} = SnapshotLog.append(base, "second")

    assert catch_throw(
             SnapshotLog.snapshot_internal(base, second, "second snapshot",
               after_step: fn
                 :index -> throw(:published)
                 _step -> :ok
               end
             )
           ) == :published

    {:ok, %{sid: sid}} = Streams.metadata_fenced(index)
    opts = [sid: sid, component: :index, base: base]

    for until <- [0, index_boundary] do
      assert {:ok, %{cursor: nil, current: {:error, :index_incomplete}}} =
               Introspect.messages(
                 Streams.Cluster,
                 shard(base),
                 1,
                 5000,
                 Keyword.put(opts, :until, until)
               )
    end

    assert {:ok, %{current: {:ok, %{offset: ^second}}}} =
             Introspect.messages(Streams.Cluster, shard(base), 1, 5000, opts)
  end

  test "index selection rejects unreadable rows instead of reporting no snapshot" do
    base = "/capture/truncated-index"
    assert {:ok, at} = SnapshotLog.append(base, "entry")
    assert :ok = SnapshotLog.snapshot(base, at, "snapshot")
    {:ok, %{sid: sid}} = Streams.metadata_fenced(SnapshotLog.path(base, :index))

    assert {:ok, %{cursor: nil, current: {:error, :unreadable_index}}} =
             Introspect.messages(Streams.Cluster, shard(base), 1, 5000,
               sid: sid,
               component: :index,
               base: base,
               max_value_bytes: 1
             )
  end

  test "capture does not restart component writers" do
    base = "/capture/inactive"
    assert {:ok, at} = SnapshotLog.append(base, "entry")
    assert :ok = SnapshotLog.snapshot(base, at, "snapshot")
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(shard(base))

    paths = [
      base <> "/.control",
      SnapshotLog.path(base, :index),
      SnapshotLog.path(base, :updates),
      SnapshotLog.path(base, {:snapshot, at})
    ]

    for path <- paths,
        {pid, _} <- Registry.lookup(ctx.registry, {ctx.n, {:stream, path}}),
        do: GenServer.stop(pid, :normal, 5000)

    assert {:ok, %{snapshot: %{offset: ^at}}} = capture(base)
    for path <- paths, do: assert(Registry.lookup(ctx.registry, {ctx.n, {:stream, path}}) == [])
  end

  defp capture(base, opts \\ []),
    do: Introspect.capture(Streams.Cluster, shard(base), 1, 5000, [base: base] ++ opts)

  defp shard(base), do: Streams.Cluster.shard_for(Streams.placement_key(base))
end
