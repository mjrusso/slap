defmodule Slap.SnapshotLogTest do
  use Slap.SnapshotLog.Test.ClusterCase, async: false

  alias Slap.Streams
  alias Slap.Streams.Offset

  defmodule OtherCluster do
    use Slap.Streams.Cluster, otp_app: :slap_snapshot_log
  end

  test "a bound log uses its own Streams cluster" do
    dir = Path.join(System.tmp_dir!(), "snapshot-other-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    start_supervised!(
      {OtherCluster, store: {:local, dir}, shards: 2, settings: %{flush_interval: "2ms"}}
    )

    base = base()
    assert {:ok, at} = SnapshotLog.append(base, "entry", cluster: OtherCluster)
    assert :ok = SnapshotLog.snapshot(base, at, "state", cluster: OtherCluster)

    assert {:reset, %{snapshot: "state", offset: ^at}} =
             SnapshotLog.next(base, nil, cluster: OtherCluster)

    assert {:ok, ^at} = SnapshotLog.tail(base, cluster: OtherCluster)
    assert {:ok, "state"} = SnapshotLog.read_snapshot(base, at, cluster: OtherCluster)
    assert {:ok, [%{offset: ^at}]} = SnapshotLog.snapshots(base, cluster: OtherCluster)
    assert {:error, :not_found} = Streams.head(SnapshotLog.path(base, :updates))
    assert :ok = SnapshotLog.delete(base, cluster: OtherCluster)
  end

  test "next's timeout includes owner connection and routing retries" do
    missing = :"slap-absent@127.0.0.1"

    start_supervised!(
      {OtherCluster,
       store: :memory,
       shards: 1,
       strategy: {Slap.Cluster.Strategy.Static, assignments: %{missing => [0]}}}
    )

    started = System.monotonic_time(:millisecond)
    assert {:error, :timeout} = SnapshotLog.next(base(), nil, timeout: 50, cluster: OtherCluster)
    assert System.monotonic_time(:millisecond) - started < 250
  end

  describe "next/3 and append/2" do
    test "a new log resets to no snapshot; appends are read back in pages" do
      base = base()
      assert {:reset, %{snapshot: nil, offset: 0}} = SnapshotLog.next(base, nil)

      assert {:ok, %{entries: [], offset: 0, up_to_date: true}} =
               SnapshotLog.next(base, 0, wait: 0)

      {:ok, a} = SnapshotLog.append(base, "a")
      {:ok, b} = SnapshotLog.append(base, "b")
      {:ok, c} = SnapshotLog.append(base, "c")
      assert {:ok, ^c} = SnapshotLog.tail(base)

      assert {:ok, %{entries: ["a"], offset: ^a, up_to_date: false}} =
               SnapshotLog.next(base, 0, max_bytes: 1)

      assert {:ok, %{entries: ["b", "c"], offset: ^c, up_to_date: true}} =
               SnapshotLog.next(base, a)

      assert {:ok, %{entries: ["c"]}} = SnapshotLog.next(base, b)
    end

    test "append creates the log" do
      base = base()
      assert {:ok, _} = SnapshotLog.append(base, "a")
      assert {nil, ["a"], _} = load(base)
    end

    test "snapshot and metadata calls accept a timeout" do
      base = base()
      assert {:ok, offset} = SnapshotLog.append(base, "a", timeout: 5_000)
      assert :ok = SnapshotLog.snapshot(base, offset, "state", timeout: 5_000)
      assert {:ok, [%{offset: ^offset}]} = SnapshotLog.snapshots(base, timeout: 5_000)
      assert {:ok, "state"} = SnapshotLog.read_snapshot(base, offset, timeout: 5_000)
      assert {:ok, ^offset} = SnapshotLog.tail(base, timeout: 5_000)
      assert :ok = SnapshotLog.delete(base, timeout: 5_000)

      assert_raise ArgumentError, ~r/:timeout/, fn ->
        SnapshotLog.snapshots(base, timeout: -1)
      end
    end

    test "a producer can retry an uncertain append without adding another entry" do
      base = base()
      producer = {"writer", 0, 0}
      assert {:ok, first} = SnapshotLog.append(base, "a", producer: producer)
      assert {:ok, tail} = SnapshotLog.append(base, "b")
      assert tail > first
      assert {:duplicate, ^tail} = SnapshotLog.append(base, "a", producer: producer)
      assert {nil, ["a", "b"], ^tail} = load(base)
    end

    test "invalid operation data returns a bad request" do
      assert {:error, {:bad_request, :invalid_entry}} = SnapshotLog.append(base(), "")
      assert {:error, {:bad_request, :invalid_entry}} = SnapshotLog.append(base(), :entry)
      assert {:error, {:bad_request, :invalid_snapshot}} = SnapshotLog.snapshot(base(), -1, "")
      assert {:error, {:bad_request, :invalid_snapshot}} = SnapshotLog.snapshot(base(), 0, :state)
      assert {:error, {:bad_request, :invalid_offset}} = SnapshotLog.next(base(), :bad)
      assert {:error, {:bad_request, :invalid_offset}} = SnapshotLog.read_snapshot(base(), :bad)
    end

    test "invalid control options raise" do
      base = base()

      assert_raise ArgumentError, fn -> SnapshotLog.next(base, nil, waait: 0) end
      assert_raise ArgumentError, fn -> SnapshotLog.append(base, "a", timout: 1) end
      assert_raise ArgumentError, fn -> SnapshotLog.append(base, "", timout: 1) end
      assert_raise ArgumentError, fn -> SnapshotLog.snapshot(base, 0, "", historry: []) end
      assert_raise ArgumentError, fn -> SnapshotLog.snapshot(base, -1, "", historry: []) end
      assert_raise ArgumentError, fn -> SnapshotLog.snapshot(base, 0, "", now: 0) end

      assert_raise ArgumentError, fn ->
        SnapshotLog.snapshot(base, 0, "", after_step: fn _ -> :ok end)
      end

      assert_raise ArgumentError, fn -> SnapshotLog.read_snapshot(base, -1, clustr: nil) end
      assert_raise ArgumentError, ~r/:wait/, fn -> SnapshotLog.next(base, 0, wait: -1) end

      assert_raise ArgumentError, ~r/:max_bytes/, fn ->
        SnapshotLog.next(base, 0, max_bytes: 0)
      end

      assert_raise ArgumentError, ~r/:cluster/, fn -> SnapshotLog.tail(base, cluster: 1) end
    end

    test "invalid history is rejected before a snapshot is written" do
      base = base()
      {:ok, offset} = SnapshotLog.append(base, "a")

      assert_raise ArgumentError, ~r/:history/, fn ->
        SnapshotLog.snapshot(base, offset, "a", history: [{0, 1_000}])
      end

      assert {:error, :not_found} = SnapshotLog.read_snapshot(base, offset)
    end

    test "timeout covers initial creation and caps an idle wait" do
      base = base()
      assert {:error, :timeout} = SnapshotLog.next(base, nil, timeout: 0)
      assert {:error, :not_found} = Streams.head(SnapshotLog.path(base, :updates))

      assert {:reset, %{offset: 0}} = SnapshotLog.next(base, nil)
      waiting = Task.async(fn -> SnapshotLog.next(base, 0, wait: 5_000, timeout: 50) end)
      assert {:ok, %{entries: [], offset: 0, up_to_date: true}} = Task.await(waiting, 1_000)
    end

    test "an idle log returns an empty page when the wait uses its timeout" do
      base = base()
      assert {:reset, %{offset: 0}} = SnapshotLog.next(base, nil)

      assert {:ok, %{entries: [], offset: 0, up_to_date: true}} =
               SnapshotLog.next(base, 0, timeout: 50)
    end

    test "at the tail, next/3 waits for an entry" do
      base = base()
      {:reset, %{offset: 0}} = SnapshotLog.next(base, nil)
      waiting = Task.async(fn -> SnapshotLog.next(base, 0, wait: 5_000) end)
      assert Task.yield(waiting, 100) == nil

      {:ok, tail} = SnapshotLog.append(base, "late")
      assert {:ok, %{entries: ["late"], offset: ^tail}} = Task.await(waiting)
    end

    test "a wait ends with :deleted when the log is deleted" do
      base = base()
      {:reset, %{offset: 0}} = SnapshotLog.next(base, nil)
      waiting = Task.async(fn -> SnapshotLog.next(base, 0, wait: 5_000) end)
      assert Task.yield(waiting, 100) == nil

      :ok = SnapshotLog.delete(base)
      assert {:error, :deleted} = Task.await(waiting)
    end

    test "a wait that times out returns no entries" do
      base = base()
      {:reset, %{offset: 0}} = SnapshotLog.next(base, nil)

      assert {:ok, %{entries: [], offset: 0, up_to_date: true}} =
               SnapshotLog.next(base, 0, wait: 50)
    end
  end

  describe "snapshots" do
    test "a snapshot replaces the entries before its offset; older offsets reset to it" do
      base = base()
      {:ok, _} = SnapshotLog.append(base, "a")
      {:ok, at} = SnapshotLog.append(base, "b")
      {:ok, tail} = SnapshotLog.append(base, "c")

      :ok = SnapshotLog.snapshot(base, at, "ab")
      assert {"ab", ["c"], ^tail} = load(base)

      # A consumer still at offset 0 is told to start over from the snapshot.
      assert {:reset, %{snapshot: "ab", offset: ^at}} = SnapshotLog.next(base, 0)
      assert {:ok, %{entries: ["c"]}} = SnapshotLog.next(base, at)
      assert {:ok, [%{offset: ^at}]} = SnapshotLog.snapshots(base)
      assert {:ok, "ab"} = SnapshotLog.read_snapshot(base, at)
    end

    test "the index and snapshot paths use the reference server's format" do
      base = base()
      {:ok, at} = SnapshotLog.append(base, "a")
      :ok = SnapshotLog.snapshot(base, at, "a")

      {:ok, %{messages: [{_, entry}]}} = Streams.read(SnapshotLog.path(base, :index), 0)
      assert %{"snapshotOffset" => encoded, "createdAt" => ms} = JSON.decode!(entry)
      assert encoded == Offset.encode(at) and is_integer(ms)
      assert SnapshotLog.path(base, {:snapshot, at}) =~ ~r"/\.snapshots/\d{16}_\d{16}_snapshot$"
    end
  end

  test "a log's streams are on one shard" do
    base = base()
    {:ok, at} = SnapshotLog.append(base, "a")
    :ok = SnapshotLog.snapshot(base, at, "a")

    for stream <- [:updates, :index, {:snapshot, at}] do
      assert Streams.placement_key(SnapshotLog.path(base, stream)) == base
    end
  end

  test "a deleted log's entries and snapshots are gone, and its base cannot be used again" do
    base = base()
    {:ok, at} = SnapshotLog.append(base, "a")
    :ok = SnapshotLog.snapshot(base, at, "a")
    {:ok, tail} = SnapshotLog.append(base, "b")

    :ok = SnapshotLog.delete(base)
    assert {:error, :not_found} = Streams.head(SnapshotLog.path(base, {:snapshot, at}))

    assert {:ok, %{messages: [], closed: true}} =
             Streams.read(SnapshotLog.path(base, :updates), :start)

    for offset <- [nil, 0, at, tail] do
      assert {:error, :deleted} = SnapshotLog.next(base, offset, wait: 0)
    end

    assert {:error, :deleted} = SnapshotLog.append(base, "c")
    assert {:error, :deleted} = SnapshotLog.snapshot(base, tail, "ab")
    assert {:error, :deleted} = SnapshotLog.snapshots(base)
    assert {:error, :deleted} = SnapshotLog.tail(base)
    assert :ok = SnapshotLog.delete(base)
  end

  test "a log deleted before it was created cannot be created" do
    base = base()
    :ok = SnapshotLog.delete(base)
    assert {:error, :deleted} = SnapshotLog.next(base, nil)
    assert {:error, :deleted} = SnapshotLog.append(base, "a")
  end

  for step <- [:close, :index, :seal, :clean] do
    test "a delete interrupted after #{step} reads as deleted, and a retry finishes it" do
      base = base()
      {:ok, at} = SnapshotLog.append(base, "a")
      :ok = SnapshotLog.snapshot(base, at, "a")
      {:ok, _} = SnapshotLog.append(base, "b")
      {:ok, tail} = SnapshotLog.append(base, "c")

      crash = fn name -> if name == unquote(step), do: throw(:crash), else: :ok end
      assert catch_throw(SnapshotLog.Store.delete(base, after_step: crash)) == :crash

      assert {:error, :deleted} = SnapshotLog.append(base, "c")

      for offset <- [nil, at, tail] do
        assert {:error, :deleted} = SnapshotLog.next(base, offset, wait: 0)
      end

      # A page short of the tail, as well as one that reaches it.
      assert {:error, :deleted} = SnapshotLog.next(base, at, wait: 0, max_bytes: 1)

      assert {:error, :deleted} = SnapshotLog.read_snapshot(base, at)
      assert {:error, :deleted} = SnapshotLog.snapshots(base)

      :ok = SnapshotLog.delete(base)
      assert {:ok, []} = Streams.list(base <> "/.snapshots/")
      assert {:ok, %{messages: []}} = Streams.read(SnapshotLog.path(base, :updates), :start)
    end
  end

  test "a publication that resumes after a delete cannot write its snapshot" do
    base = base()
    {:ok, at} = SnapshotLog.append(base, "a")
    test = self()

    after_step = fn
      :check ->
        send(test, {:paused, self()})
        receive do: (:resume -> :ok)

      :snapshot ->
        throw(:crash)

      _step ->
        :ok
    end

    task =
      Task.async(fn ->
        try do
          SnapshotLog.Compaction.run(base, at, "a", after_step: after_step)
        catch
          :throw, :crash -> :crash
        end
      end)

    assert_receive {:paused, pid}, 5_000
    :ok = SnapshotLog.delete(base)
    send(pid, :resume)

    assert {:error, :deleted} = Task.await(task)
    assert {:error, :not_found} = Streams.head(SnapshotLog.path(base, {:snapshot, at}))
    assert {:ok, []} = Streams.list(base <> "/.snapshots/")
  end

  for step <- [:check, :snapshot] do
    test "a publication paused after #{step} across a delete finds the log deleted" do
      base = base()
      {:ok, at} = SnapshotLog.append(base, "a")
      test = self()

      pause = fn name ->
        if name == unquote(step) do
          send(test, {:paused, self()})
          receive do: (:resume -> :ok)
        end
      end

      task = Task.async(fn -> SnapshotLog.Compaction.run(base, at, "a", after_step: pause) end)
      assert_receive {:paused, pid}, 5_000
      :ok = SnapshotLog.delete(base)
      send(pid, :resume)

      assert {:error, :deleted} = Task.await(task)
      assert {:error, :not_found} = Streams.head(SnapshotLog.path(base, {:snapshot, at}))
      assert {:error, :deleted} = SnapshotLog.next(base, nil)
    end
  end

  test "delete removes retained snapshots, and ones never made current" do
    base = base()
    rules = [{1_000, 60_000}]

    retained =
      for now <- [0, 1_000] do
        {:ok, at} = SnapshotLog.append(base, "a")
        :ok = SnapshotLog.Compaction.run(base, at, "x", history: rules, now: now)
        at
      end

    {:ok, unindexed} = SnapshotLog.append(base, "b")
    crash = fn name -> if name == :snapshot, do: throw(:crash), else: :ok end

    assert catch_throw(SnapshotLog.Compaction.run(base, unindexed, "y", after_step: crash)) ==
             :crash

    assert {:ok, [_, _]} = SnapshotLog.snapshots(base)
    :ok = SnapshotLog.delete(base)

    for at <- [unindexed | retained] do
      assert {:error, :not_found} = Streams.head(SnapshotLog.path(base, {:snapshot, at}))
    end
  end

  test "base paths are stream paths without dot segments" do
    for bad <- ["", "relative", "/a/.b", "/a/", nil] do
      assert {:error, {:bad_request, :invalid_base}} = SnapshotLog.next(bad, nil)
    end
  end
end
