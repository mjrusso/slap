defmodule Slap.KV.BatchTest do
  use Slap.KV.Test.ClusterCase, async: false

  import Slap.KV.Test.WriterControl

  alias Slap.KV.Keys
  alias Slap.SlateDB

  @moduletag :capture_log

  test "puts share a version; delete-only and check-only results have their own contracts" do
    assert {:ok, version} =
             KV.batch("p", [{:put, "a", "1", :absent}, {:put, "b", "2"}, {:delete, "gone"}])

    assert {:ok, %{rows: [{"a", "1", ^version}, {"b", "2", ^version}], cursor: nil}} =
             KV.scan("p", with_versions: true)

    assert {:ok, nil} = KV.batch("p", [{:check, "a", version}, {:check, "gone", :absent}])
    assert {:ok, deleted} = KV.batch("p", [{:delete, "a", version}, {:delete, "b"}])
    assert deleted > version
    assert {:ok, %{rows: []}} = KV.scan("p")
    assert {:ok, later} = KV.batch("p", [{:delete, "already gone"}])
    assert later > deleted
  end

  test "conditions run in order against pre-batch state, without partial changes" do
    {:ok, version} = KV.put("p", "last", "original")

    assert {:error, {:conflict, "last", ^version}} =
             KV.batch("p", [{:put, "first", "new"}, {:delete, "last", 0}, {:check, "missing", 0}])

    assert {:ok, nil} = KV.get("p", "first")
    assert {:ok, %{value: "original", version: ^version}} = KV.get("p", "last")

    assert {:error, {:conflict, "missing", nil}} =
             KV.batch("p", [{:check, "missing", 0}, {:put, "last", "new", :absent}])
  end

  test "invalid operations and controls leave rows untouched" do
    cases = [
      {nil, :invalid_batch},
      {[], :empty_batch},
      {[{:put, "a", "v"} | :tail], :invalid_batch},
      {[{:unknown, "a"}], :invalid_operation},
      {[{:check, "a"}], :invalid_operation},
      {[{:put, "a", :bad}], :invalid_value},
      {[{:delete, "a", :absent}], :invalid_version},
      {[{:check, "a", nil}], :invalid_version},
      {[{:put, "a", "v", nil}], :invalid_version},
      {[{:put, "", "v"}], :invalid_key},
      {[{:put, "a", "v"}, {:check, "a", :absent}], :duplicate_key}
    ]

    for {ops, reason} <- cases, do: assert(KV.batch("p", ops) == {:error, {:bad_request, reason}})
    assert {:error, {:bad_request, :invalid_partition}} = KV.batch("", [{:delete, "a"}])
    assert_raise ArgumentError, fn -> KV.batch("p", [{:delete, "a"}], if_version: 1) end
    assert_raise ArgumentError, fn -> KV.batch("p", [{:delete, "a"}], timeout: -1) end
    assert {:ok, %{rows: []}} = KV.scan("p")
  end

  test "default operation boundary accepts 1000 and rejects 1001" do
    ops = for n <- 1..1_000, do: {:put, Integer.to_string(n), ""}
    assert {:ok, _} = KV.batch("p", ops)
    assert {:error, {:bad_request, :batch_too_large}} = KV.batch("p", [{:delete, "extra"} | ops])

    assert {:error, {:bad_request, :batch_too_large}} =
             KV.batch(
               "p",
               ops ++
                 [{:delete, "extra"}, {:delete, "another"} | :tail]
             )

    assert {:ok, %{rows: rows, cursor: nil}} = KV.scan("p", limit: 1_000)
    assert Enum.count(rows) == 1_000
  end

  test "default byte boundary includes the encoded key" do
    value = :binary.copy("x", 8 * 1024 * 1024 - 4)
    assert {:ok, _} = KV.batch("p", [{:put, "k", value}])
    assert {:error, {:bad_request, :batch_too_large}} = KV.batch("p", [{:put, "k", value <> "x"}])
  end

  @tag child_options: [max_batch_ops: 2, max_batch_bytes: 10]
  test "owner limits include every encoded partition/key and put value byte" do
    assert {:ok, _} = KV.batch("p", [{:put, "a", "12"}, {:delete, "b"}])

    assert {:error, {:bad_request, :batch_too_large}} =
             KV.batch("p", [{:put, "a", "123"}, {:check, "b", :absent}])

    assert {:error, {:bad_request, :batch_too_large}} =
             KV.batch("p", [{:delete, "a"}, {:delete, "b"}, {:delete, "c"}])

    assert {:error, {:bad_request, :batch_too_large}} =
             KV.batch("p", [{:delete, "a"}, {:delete, "b"} | :tail])

    assert {:error, {:bad_request, :batch_too_large}} =
             KV.batch("p", [{:put, "a", "123456"} | :tail])

    assert {:error, {:bad_request, :batch_too_large}} =
             KV.batch("long partition", [{:delete, "a"}])
  end

  test "one iterator keeps its cutoff across native fetches and concurrent batch replacement" do
    ops = for n <- 1..600, do: {:put, String.pad_leading(Integer.to_string(n), 4, "0"), "old"}
    {:ok, old_version} = KV.batch("p", ops)
    test = self()

    reader =
      Task.async(fn ->
        Slap.SlateDB.scan(ctx_for("p").db,
          prefix: Keys.encode("p", ""),
          batch_size: 128,
          with_versions: true
        )
        |> Stream.with_index()
        |> Enum.map(fn
          {row, 0} ->
            send(test, {:first_row, self()})
            receive do: (:continue -> row)

          {row, _} ->
            row
        end)
      end)

    assert_receive {:first_row, pid}, 5_000
    {:ok, new_version} = KV.batch("p", Enum.map(ops, fn {:put, k, _} -> {:put, k, "new"} end))
    send(pid, :continue)
    rows = Task.await(reader)
    assert Enum.count(rows) == 600
    assert Enum.all?(rows, fn {_, v, version} -> v == "old" and version == old_version end)
    assert {:ok, %{rows: current, cursor: nil}} = KV.scan("p", limit: 1_000, with_versions: true)
    assert Enum.all?(current, fn {_, v, version} -> v == "new" and version == new_version end)
  end

  describe "without automatic WAL flushes" do
    @describetag settings: %{flush_interval: nil}

    test "success, conflicts and check-only results all wait for durability" do
      put = pending("p", fn -> KV.batch("p", [{:put, "a", "v"}, {:put, "b", "v"}]) end)
      conflict = pending("p", fn -> KV.batch("p", [{:put, "a", "other", :absent}]) end)
      check = pending("p", fn -> KV.batch("p", [{:check, "missing", :absent}]) end)
      for task <- [put, conflict, check], do: assert(Task.yield(task, 0) == nil)
      assert {:ok, %{rows: []}} = KV.scan("p")
      flush("p")
      assert {:ok, version} = Task.await(put)
      assert {:error, {:conflict, "a", ^version}} = Task.await(conflict)
      assert {:ok, nil} = Task.await(check)
    end

    @tag child_options: [max_inflight_bytes_per_writer: 9]
    test "one full-batch reservation rejects every mutation together" do
      ctx = ctx_for("p")
      first = pending("p", fn -> KV.put("p", "k", "x") end)
      before = SlateDB.last_write_seq(ctx.db)
      assert {:error, :overloaded} = KV.batch("p", [{:put, "a", ""}, {:delete, "b"}])
      assert SlateDB.last_write_seq(ctx.db) == before
      assert {:ok, nil} = SlateDB.get(ctx.db, Keys.encode("p", "a"))
      flush("p")
      assert {:ok, _} = Task.await(first)
    end
  end
end
