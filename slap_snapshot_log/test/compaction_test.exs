defmodule Slap.SnapshotLog.CompactionTest do
  use Slap.SnapshotLog.Test.ClusterCase, async: false

  alias Slap.SnapshotLog.Compaction
  alias Slap.Streams
  alias Slap.Streams.Offset

  @hour 3_600_000
  @day 24 * @hour

  @steps [:check, :snapshot, :index, :delete, :trim]

  defp append_words(base, words) do
    for word <- words, do: {:ok, _} = SnapshotLog.append(base, word)
    :ok
  end

  defp tail(base) do
    {:ok, tail} = SnapshotLog.tail(base)
    tail
  end

  defp text(base) do
    {snapshot, entries, _tail} = load(base)
    Enum.join(List.wrap(snapshot) ++ entries)
  end

  defp compact(base, opts \\ []), do: Compaction.run(base, tail(base), text(base), opts)

  defp stored?(base, offset), do: match?({:ok, _}, SnapshotLog.read_snapshot(base, offset))

  defp crash_after(step), do: fn name -> if name == step, do: throw(:crash), else: :ok end

  defp paused_after(step, fun) do
    test = self()

    pause = fn name ->
      if name == step do
        send(test, {:paused, self()})
        receive do: (:resume -> :ok)
      end
    end

    task = Task.async(fn -> fun.(pause) end)
    assert_receive {:paused, pid}, 5_000
    {task, pid}
  end

  defp resume({task, pid}) do
    send(pid, :resume)
    Task.await(task)
  end

  test "a compaction replaces the entries with a snapshot, and deletes the previous one" do
    base = base()
    append_words(base, ~w(a b c))
    first = tail(base)
    :ok = compact(base)

    assert {"abc", [], ^first} = load(base)
    assert {:error, :trimmed} = Streams.read(SnapshotLog.path(base, :updates), 0)

    append_words(base, ~w(d e))
    second = tail(base)
    :ok = compact(base)

    assert text(base) == "abcde"
    refute stored?(base, first)
    assert {:ok, [%{offset: ^second}]} = SnapshotLog.snapshots(base)
  end

  test "compacting at the current snapshot's offset does nothing" do
    base = base()
    append_words(base, ~w(a))
    :ok = compact(base)
    {:ok, before} = SnapshotLog.snapshots(base)
    :ok = compact(base)
    assert {:ok, ^before} = SnapshotLog.snapshots(base)
  end

  for step <- @steps do
    test "a compaction interrupted after #{step} leaves a readable log, cleaned up next time" do
      step = unquote(step)
      base = base()
      append_words(base, ~w(a b))
      :ok = compact(base)
      first = tail(base)

      append_words(base, ~w(c d))
      second = tail(base)
      assert catch_throw(compact(base, after_step: crash_after(step))) == :crash
      assert text(base) == "abcd"

      append_words(base, ~w(e))
      third = tail(base)
      :ok = compact(base)

      assert text(base) == "abcde"
      assert {:ok, [%{offset: ^third}]} = SnapshotLog.snapshots(base)
      refute stored?(base, first)
      refute stored?(base, second)
    end
  end

  for step <- [:index, :delete] do
    test "a retry of a compaction interrupted after #{step} finishes its cleanup" do
      base = base()
      append_words(base, ~w(a b))
      :ok = compact(base)
      first = tail(base)

      append_words(base, ~w(c d))
      second = tail(base)
      assert catch_throw(compact(base, after_step: crash_after(unquote(step)))) == :crash

      :ok = SnapshotLog.snapshot(base, second, "abcd")
      refute stored?(base, first)
      assert {:error, :trimmed} = Streams.read(SnapshotLog.path(base, :updates), first)
      assert text(base) == "abcd"
    end
  end

  test "a snapshot after the tail is refused, and changes nothing" do
    base = base()
    append_words(base, ~w(a b))
    :ok = compact(base)
    at = tail(base)

    assert {:error, :offset_beyond_tail} = SnapshotLog.snapshot(base, at + 10, "ab?")
    refute stored?(base, at + 10)
    assert {"ab", [], ^at} = load(base)
  end

  test "a compaction that read the index before another's indexes from a fresh read" do
    base = base()
    append_words(base, ~w(a))
    :ok = compact(base)
    append_words(base, ~w(b))
    lower = tail(base)
    append_words(base, ~w(c))
    higher = tail(base)

    paused = paused_after(:snapshot, &Compaction.run(base, higher, "abc", after_step: &1))
    :ok = SnapshotLog.snapshot(base, lower, "ab")
    assert :ok = resume(paused)

    assert {:ok, [%{offset: ^higher}]} = SnapshotLog.snapshots(base)
    # The snapshot at `lower` is no longer current or retained.
    refute stored?(base, lower)
    assert text(base) == "abc"
  end

  test "of two compactions, the one with the older snapshot is superseded" do
    base = base()
    append_words(base, ~w(a b))
    older = tail(base)
    older_text = text(base)
    append_words(base, ~w(c))
    :ok = compact(base)

    assert {:error, :superseded} = SnapshotLog.snapshot(base, older, older_text)
    refute stored?(base, older)
    assert text(base) == "abc"
  end

  test "a compaction superseded before it writes its snapshot deletes the snapshot" do
    base = base()
    append_words(base, ~w(a))
    lower = tail(base)
    paused = paused_after(:check, &Compaction.run(base, lower, "a", after_step: &1))

    append_words(base, ~w(b))
    :ok = compact(base)
    assert {:error, :superseded} = resume(paused)
    refute stored?(base, lower)
  end

  test "a snapshot written after a cleanup tried to delete it is deleted by the next one" do
    base = base()
    append_words(base, ~w(a))
    lower = tail(base)
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
      Task.async(fn -> catch_throw(Compaction.run(base, lower, "a", after_step: after_step)) end)

    assert_receive {:paused, pid}, 5_000
    append_words(base, ~w(b))
    :ok = compact(base)
    send(pid, :resume)
    assert Task.await(task) == :crash
    assert stored?(base, lower)

    append_words(base, ~w(c))
    :ok = compact(base)
    refute stored?(base, lower)
  end

  test "an index written with snapshot offsets as its Stream-Seq advances" do
    base = base()
    # A snapshot offset, the earlier Stream-Seq, past the index's tail.
    long = String.duplicate("a", 1_000)
    append_words(base, [long, "b"])
    at = tail(base)
    :ok = SnapshotLog.Store.snapshot(base, at, long <> "b")

    entry = JSON.encode!(%{"snapshotOffset" => Offset.encode(at), "createdAt" => 0})

    {:ok, _} =
      Streams.append(SnapshotLog.path(base, :index), entry,
        content_type: "application/json",
        stream_seq: Offset.encode(at)
      )

    append_words(base, ~w(c))
    :ok = compact(base)
    assert text(base) == long <> "bc"
    assert {:ok, [%{offset: offset}]} = SnapshotLog.snapshots(base)
    assert offset == tail(base)
  end

  test "a conflict the index's entries do not explain is an error, not a retry" do
    base = base()
    append_words(base, ~w(a))
    :ok = compact(base)

    {:ok, _} =
      Streams.append(SnapshotLog.path(base, :index), "{}",
        content_type: "application/json",
        stream_seq: "z"
      )

    append_words(base, ~w(b))
    assert {:error, :index_seq_conflict} = compact(base)
    assert text(base) == "ab"
  end

  test "a superseded compaction leaves the snapshots the current one retains" do
    base = base()
    rules = [{@hour, 2 * @hour}]
    append_words(base, ~w(a))
    old = tail(base)
    :ok = Compaction.run(base, old, "a", history: rules, now: 0)
    append_words(base, ~w(b))
    :ok = Compaction.run(base, tail(base), "ab", history: rules, now: @hour)

    # At 3 h the snapshot at `old` has expired; at 1.5 h it has not.
    append_words(base, ~w(c))
    stale = tail(base)

    paused =
      paused_after(
        :snapshot,
        &Compaction.run(base, stale, "abc", history: rules, now: 3 * @hour, after_step: &1)
      )

    append_words(base, ~w(d))
    :ok = Compaction.run(base, tail(base), "abcd", history: rules, now: div(3 * @hour, 2))
    assert {:error, :superseded} = resume(paused)

    assert {:ok, [%{offset: ^old} | _]} = SnapshotLog.snapshots(base)
    assert {:ok, "a"} = SnapshotLog.read_snapshot(base, old)
    refute stored?(base, stale)
  end

  test "a retry at the same offset reuses the snapshot an interrupted compaction wrote" do
    base = base()
    append_words(base, ~w(a b))
    assert catch_throw(compact(base, after_step: crash_after(:snapshot))) == :crash

    :ok = compact(base)
    assert text(base) == "ab"
    assert {:ok, [_]} = SnapshotLog.snapshots(base)
  end

  describe "history" do
    test "overlapping history rules retain the latest snapshot of each period" do
      base = base()
      rules = [{@hour, 2 * @hour}, {@day, 3 * @day}]

      offsets =
        for now <- [
              0,
              div(@hour, 2),
              @day,
              @day + div(@hour, 2),
              2 * @day,
              2 * @day + div(@hour, 2),
              2 * @day + @hour
            ] do
          {:ok, at} = SnapshotLog.append(base, "a")
          :ok = Compaction.run(base, at, text(base), history: rules, now: now)
          at
        end

      [_, day0, _, day1, _, recent, current] = offsets
      assert {:ok, history} = SnapshotLog.snapshots(base)
      assert Enum.map(history, & &1.offset) == [day0, day1, recent, current]
    end

    test "snapshots the policy keeps are retained, readable, and deleted once they expire" do
      base = base()
      rules = [{@hour, 3 * @hour}]

      # Two snapshots an hour, for four hours.
      offsets =
        for half <- 0..7 do
          append_words(base, ["w#{half} "])

          :ok =
            Compaction.run(base, tail(base), text(base),
              history: rules,
              now: half * div(@hour, 2)
            )

          tail(base)
        end

      # At 3.5 h: the latest of hours 1 and 2 (the second of each), and the
      # current one. Hour 0's expired at 3 h.
      [_, _, _, h1, _, h2, _, current] = offsets
      assert {:ok, history} = SnapshotLog.snapshots(base)
      assert Enum.map(history, & &1.offset) == [h1, h2, current]

      for offset <- offsets, do: assert(stored?(base, offset) == offset in [h1, h2, current])
      assert {:ok, "w0 w1 w2 w3 "} = SnapshotLog.read_snapshot(base, h1)
    end
  end
end
