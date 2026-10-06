defmodule Slap.SlateDB.FeaturesTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias Slap.SlateDB
  alias Slap.SlateDB.{Admin, CompactionFilter, MergeOperator, Reader, Transaction}

  # The compactor schedules work every 100 ms by default in these tests, but
  # the compaction worker only looks for scheduled work every 5 s. Make both
  # fast, and compact as soon as there are two L0 SSTs.
  @fast_compaction %{
    compactor_options: %{
      poll_interval: "100ms",
      commit_compacted_interval: "100ms",
      scheduler_options: %{min_compaction_sources: "2"},
      worker: %{compactions_poll_interval: "100ms"}
    }
  }

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "slap-slatedb-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  defp eventually(fun, tries \\ 50) do
    cond do
      fun.() -> true
      tries == 0 -> false
      true -> Process.sleep(100) && eventually(fun, tries - 1)
    end
  end

  describe "merge operators" do
    test "u64_add counts without reading" do
      {:ok, db} = SlateDB.open("u64", store: :memory, merge_operator: :u64_add)
      {:ok, _} = SlateDB.increment(db, "c", await_durable: true)
      {:ok, _} = SlateDB.increment(db, "c", 41)
      {:ok, _} = SlateDB.merge(db, "c", MergeOperator.encode_u64(8))
      assert {:ok, bin} = SlateDB.get(db, "c")
      assert MergeOperator.decode_u64(bin) == 50

      {:ok, _} = SlateDB.put(db, "c", MergeOperator.encode_u64(100))
      {:ok, _} = SlateDB.increment(db, "c", 1)
      assert {:ok, <<101::unsigned-little-64>>} = SlateDB.get(db, "c")
      {:ok, _} = SlateDB.delete(db, "c")
      {:ok, _} = SlateDB.increment(db, "c", 2)
      assert {:ok, <<2::unsigned-little-64>>} = SlateDB.get(db, "c")
      :ok = SlateDB.close(db)
    end

    test "the other operators" do
      cases = [
        {:i64_add, [MergeOperator.encode_i64(5), MergeOperator.encode_i64(-8)],
         MergeOperator.encode_i64(-3)},
        {:u64_max, Enum.map([3, 9, 4], &MergeOperator.encode_u64/1), MergeOperator.encode_u64(9)},
        {:u64_min, Enum.map([3, 9, 4], &MergeOperator.encode_u64/1), MergeOperator.encode_u64(3)},
        {:append, ["a", "bc", "d"], "abcd"}
      ]

      for {op, operands, expected} <- cases do
        {:ok, db} = SlateDB.open("#{op}", store: :memory, merge_operator: op)
        for operand <- operands, do: {:ok, _} = SlateDB.merge(db, "k", operand)
        assert {:ok, ^expected} = SlateDB.get(db, "k"), "#{op}"
        :ok = SlateDB.close(db)
      end

      {:ok, db} = SlateDB.open("i64", store: :memory, merge_operator: :i64_add)
      {:ok, _} = SlateDB.increment(db, "k", -5)
      assert {:ok, bin} = SlateDB.get(db, "k")
      assert MergeOperator.decode_i64(bin) == -5
      :ok = SlateDB.close(db)
    end

    test "merges in batches and transactions" do
      {:ok, db} = SlateDB.open("batch", store: :memory, merge_operator: :append)

      {:ok, _} =
        SlateDB.write(db, [{:put, "k", "a"}, {:merge, "k", "b"}, {:merge, "k", "c"}])

      assert {:ok, "abc"} = SlateDB.get(db, "k")

      # SlateDB allows one merge TTL per key in a batch.
      {:ok, _} = SlateDB.write(db, [{:merge, "t", "x", 60_000}, {:merge, "t", "y", 60_000}])
      assert {:ok, %{value: "xy", expire_ts: expire}} = SlateDB.get_key_value(db, "t")
      assert is_integer(expire)

      assert {:error, %SlateDB.Error{kind: :invalid}} =
               SlateDB.write(db, [{:merge, "u", "x", 60_000}, {:merge, "u", "y"}])

      {:ok, tx} = SlateDB.begin(db)
      :ok = Transaction.merge(tx, "k", "d")
      assert {:ok, "abcd"} = Transaction.get(tx, "k")
      {:ok, _} = Transaction.commit(tx)
      assert {:ok, "abcd"} = SlateDB.get(db, "k")
      :ok = SlateDB.close(db)
    end

    test "bad operands and missing operators are rejected" do
      {:ok, db} = SlateDB.open("bad", store: :memory, merge_operator: :u64_add)
      assert {:error, %SlateDB.Error{kind: :invalid}} = SlateDB.merge(db, "k", "abc")

      assert {:error, %SlateDB.Error{kind: :invalid}} =
               SlateDB.write(db, [
                 {:merge, "k", MergeOperator.encode_u64(1)},
                 {:merge, "k", "x"}
               ])

      {:ok, tx} = SlateDB.begin(db)
      assert {:error, %SlateDB.Error{kind: :invalid}} = Transaction.merge(tx, "k", "x")
      assert {:ok, nil} = SlateDB.get(db, "k")
      assert_raise ArgumentError, fn -> SlateDB.increment(db, "k", -1) end
      :ok = SlateDB.close(db)

      {:ok, plain} = SlateDB.open("plain", store: :memory)

      assert {:error, %SlateDB.Error{kind: :invalid, message: message}} =
               SlateDB.merge(plain, "k", "x")

      assert message =~ "merge_operator"
      assert_raise ArgumentError, fn -> SlateDB.increment(plain, "k") end
      :ok = SlateDB.close(plain)

      assert_raise ArgumentError, fn ->
        SlateDB.open("x", store: :memory, merge_operator: :multiply)
      end
    end

    test "operands survive flushes and reopening, which needs the operator" do
      dir = tmp_dir()
      {:ok, db} = SlateDB.open("reopen", store: {:local, dir}, merge_operator: :u64_add)
      {:ok, _} = SlateDB.increment(db, "c", 2)
      :ok = SlateDB.flush(db, type: :memtable)
      {:ok, _} = SlateDB.increment(db, "c", 3)
      :ok = SlateDB.close(db)

      {:ok, db} = SlateDB.open("reopen", store: {:local, dir}, merge_operator: :u64_add)
      assert {:ok, <<5::unsigned-little-64>>} = SlateDB.get(db, "c")
      :ok = SlateDB.close(db)

      # Without the operator, SlateDB cannot read a key that has operands.
      {:ok, db} = SlateDB.open("reopen", store: {:local, dir})
      assert {:error, %SlateDB.Error{kind: :invalid}} = SlateDB.get(db, "c")
      :ok = SlateDB.close(db)
    end
  end

  describe "compaction filter" do
    test "compaction deletes keys under the filter's prefixes" do
      filter = CompactionFilter.new(["dead/"])

      store = {:local, tmp_dir()}

      {:ok, db} =
        SlateDB.open("filter",
          store: store,
          compaction_filter: filter,
          settings: @fast_compaction
        )

      {:ok, _} = SlateDB.write(db, for(i <- 1..10, do: {:put, "dead/#{i}", "x"}))
      {:ok, _} = SlateDB.write(db, for(i <- 1..10, do: {:put, "live/#{i}", "x"}))
      :ok = SlateDB.flush(db, type: :memtable)
      {:ok, _} = SlateDB.put(db, "stream/7/a", "x")
      :ok = SlateDB.flush(db, type: :memtable)

      # The keys stay visible until a compaction has run and the database
      # has picked up its result from the manifest.
      assert eventually(fn -> Enum.to_list(SlateDB.scan(db, prefix: "dead/")) == [] end)
      assert CompactionFilter.tombstoned(filter) == 10
      assert Enum.count(SlateDB.scan(db, prefix: "live/")) == 10

      # "stream/7/a" is now in a sorted run. A prefix added later only
      # reaches it when that run is compacted again, which a full compaction
      # does.
      :ok = CompactionFilter.add(filter, ["stream/7/"])
      {:ok, _} = SlateDB.put(db, "live/11", "x")
      :ok = SlateDB.flush(db, type: :memtable)
      {:ok, admin} = Admin.open("filter", store: store)
      assert {:ok, id} = Admin.compact(admin)
      assert is_binary(id)
      assert eventually(fn -> SlateDB.get(db, "stream/7/a") == {:ok, nil} end)
      assert CompactionFilter.tombstoned(filter) >= 11
      assert Enum.count(SlateDB.scan(db, prefix: "live/")) == 11
      :ok = SlateDB.close(db)
    end

    test "the prefix set can be changed and read" do
      filter = CompactionFilter.new(["b/", "a/"])
      assert CompactionFilter.prefixes(filter) == ["a/", "b/"]
      :ok = CompactionFilter.add(filter, ["c/", "a/x/"])
      :ok = CompactionFilter.remove(filter, ["b/", "not-there"])
      assert CompactionFilter.prefixes(filter) == ["a/", "a/x/", "c/"]
      assert CompactionFilter.tombstoned(filter) == 0

      assert_raise ArgumentError, fn -> CompactionFilter.new([""]) end
      assert_raise ArgumentError, fn -> CompactionFilter.add(filter, [:a]) end
    end

    test "it needs the compactor" do
      assert {:error, %SlateDB.Error{kind: :invalid, message: message}} =
               SlateDB.open("nocompactor",
                 store: :memory,
                 compaction_filter: CompactionFilter.new(["x"]),
                 settings: %{compactor_options: nil}
               )

      assert message =~ "compactor"
    end
  end

  describe "readers" do
    setup do
      store = {:local, tmp_dir()}
      {:ok, db} = SlateDB.open("db", store: store, settings: %{flush_interval: "10s"})
      on_exit(fn -> SlateDB.close(db) end)
      %{db: db, store: store}
    end

    test "a reader follows durable writes", %{db: db, store: store} do
      {:ok, _} = SlateDB.put(db, "a", "1", await_durable: true)
      {:ok, reader} = Reader.open("db", store: store, settings: %{manifest_poll_interval: 50})
      assert {:ok, "1"} = Reader.get(reader, "a")

      {:ok, seq} = SlateDB.put(db, "b", "2", await_durable: true)
      assert eventually(fn -> Reader.get(reader, "b") == {:ok, "2"} end)
      assert Reader.durable_seq(reader) >= seq
      assert {:ok, %{value: "2", seq: ^seq}} = Reader.get_key_value(reader, "b")
      assert [{"b", "2"}, {"a", "1"}] = reader |> Reader.scan(order: :desc) |> Enum.to_list()
      {:ok, iter} = Reader.iterator(reader, gte: "b")
      assert {:ok, [{"b", "2"}]} = SlateDB.Iterator.next_batch(iter)

      :ok = Reader.close(reader)
      :ok = Reader.close(reader)
      assert {:error, %SlateDB.Error{kind: :closed}} = Reader.get(reader, "a")
      assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.Iterator.next_batch(iter)
    end

    test "latest mode and settings strings", %{db: db, store: store} do
      {:ok, _} = SlateDB.put(db, "a", "1", await_durable: true)

      {:ok, reader} =
        Reader.open("db",
          store: store,
          mode: :latest,
          settings: %{"manifest_poll_interval" => "100ms", checkpoint_lifetime: "5m"}
        )

      assert {:ok, "1"} = Reader.get(reader, "a")
      :ok = Reader.close(reader)

      assert_raise ArgumentError, ~r/:mode/, fn ->
        Reader.open("db", store: store, mode: :sometimes)
      end

      assert_raise ArgumentError, fn ->
        Reader.open("db", store: store, settings: %{manifest_poll_interval: "soon"})
      end
    end

    test "a reader pinned to a checkpoint does not move", %{db: db, store: store} do
      {:ok, _} = SlateDB.put(db, "a", "1")
      {:ok, %{id: id}} = SlateDB.create_checkpoint(db, scope: :all)
      {:ok, _} = SlateDB.put(db, "a", "2", await_durable: true)

      {:ok, reader} = Reader.open("db", store: store, checkpoint: id)
      assert {:ok, "1"} = Reader.get(reader, "a")
      :ok = Reader.close(reader)

      assert {:error, %SlateDB.Error{kind: :invalid}} =
               Reader.open("db", store: store, checkpoint: "nope")
    end

    test "a reader needs the merge operator for merged keys", %{store: store} do
      {:ok, db} = SlateDB.open("counters", store: store, merge_operator: :u64_add)
      {:ok, _} = SlateDB.increment(db, "c", 3, await_durable: true)

      {:ok, reader} = Reader.open("counters", store: store, merge_operator: :u64_add)
      assert {:ok, <<3::unsigned-little-64>>} = Reader.get(reader, "c")
      :ok = Reader.close(reader)
      :ok = SlateDB.close(db)
    end
  end

  describe "checkpoints and admin" do
    setup do
      store = {:local, tmp_dir()}
      {:ok, db} = SlateDB.open("db", store: store)
      {:ok, admin} = Admin.open("db", store: store)
      on_exit(fn -> SlateDB.close(db) end)
      %{db: db, admin: admin, store: store}
    end

    test "create, list, refresh and delete checkpoints", %{db: db, admin: admin} do
      {:ok, _} = SlateDB.put(db, "k", "v")

      {:ok, %{id: id1, manifest_id: m1}} =
        SlateDB.create_checkpoint(db, scope: :all, name: "one", lifetime: 60_000)

      assert is_integer(m1)
      {:ok, %{id: id2}} = Admin.create_checkpoint(admin, name: "two")
      {:ok, %{id: id3}} = Admin.create_checkpoint(admin, source: id1)

      {:ok, all} = Admin.list_checkpoints(admin)
      assert Enum.sort(Enum.map(all, & &1.id)) == Enum.sort([id1, id2, id3])

      assert {:ok, [%{id: ^id1, name: "one", expires_at: %DateTime{}}]} =
               Admin.list_checkpoints(admin, name: "one")

      assert :ok = Admin.refresh_checkpoint(admin, id1, nil)
      assert {:ok, [%{expires_at: nil}]} = Admin.list_checkpoints(admin, name: "one")

      assert :ok = Admin.delete_checkpoint(admin, id2)
      assert {:ok, []} = Admin.list_checkpoints(admin, name: "two")
      assert :ok = Admin.run_gc(admin, settings: %{compacted_options: %{min_age: "1h"}})

      assert {:error, %SlateDB.Error{kind: :invalid}} = Admin.delete_checkpoint(admin, "x")

      assert_raise ArgumentError, ~r/:scope/, fn ->
        SlateDB.create_checkpoint(db, scope: :some)
      end

      assert {:error, %SlateDB.Error{kind: :invalid}} =
               Admin.run_gc(admin, settings: %{wal_options: 1})
    end

    test "clone a database at a checkpoint", %{db: db, admin: admin, store: store} do
      {:ok, _} = SlateDB.put(db, "k", "before")
      {:ok, %{id: id}} = SlateDB.create_checkpoint(db, scope: :all)
      {:ok, _} = SlateDB.put(db, "k", "after")

      assert :ok = Admin.clone(admin, "copy", checkpoint: id)
      {:ok, copy} = SlateDB.open("copy", store: store)
      assert {:ok, "before"} = SlateDB.get(copy, "k")

      {:ok, _} = SlateDB.put(copy, "k", "copy")
      assert {:ok, "after"} = SlateDB.get(db, "k")
      :ok = SlateDB.close(copy)
    end

    test "timestamps and sequence numbers", %{db: db, admin: admin} do
      {:ok, seq} = SlateDB.put(db, "k", "v")
      :ok = SlateDB.flush(db, type: :memtable)

      assert {:ok, ts} = Admin.timestamp_for_seq(admin, seq, round_up: true)
      assert ts == nil or match?(%DateTime{}, ts)
      assert {:ok, found} = Admin.seq_for_timestamp(admin, DateTime.utc_now())
      assert found == nil or is_integer(found)
    end
  end
end
