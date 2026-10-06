defmodule Slap.SlateDBTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias Slap.SlateDB
  alias Slap.SlateDB.{Snapshot, Transaction}

  setup do
    {:ok, db} = SlateDB.open("test-db", store: :memory)
    on_exit(fn -> SlateDB.close(db) end)
    %{db: db}
  end

  test "validates settings without opening a database" do
    assert :ok = SlateDB.validate_settings(%{flush_interval: "10ms"})

    assert {:error, %SlateDB.Error{kind: :invalid}} =
             SlateDB.validate_settings(%{flush_interval: "bogus"})

    assert_raise ArgumentError, fn -> SlateDB.validate_settings("bogus") end
  end

  describe "get/put/delete" do
    test "unknown options are rejected before side effects", %{db: db} do
      assert_raise ArgumentError, fn -> SlateDB.close(db, typo: true) end
      assert {:ok, _} = SlateDB.put(db, "still-open", "yes")

      assert_raise ArgumentError, fn -> SlateDB.snapshot(db, typo: true) end
      assert_raise ArgumentError, fn -> SlateDB.probe_store(:memory, "probe", typo: true) end

      {:ok, subscription} = SlateDB.subscribe(db, :test)
      assert_raise ArgumentError, fn -> SlateDB.unsubscribe(subscription, typo: true) end
      assert_raise ArgumentError, fn -> SlateDB.unsubscribe(subscription, flush: "true") end
      assert {:ok, _} = SlateDB.put(db, "notify", "yes", await_durable: true)
      assert_receive {:slap_slatedb_durable, _, :test, _}
      assert :ok = SlateDB.unsubscribe(subscription, flush: true)

      assert_raise ArgumentError, fn -> SlateDB.Telemetry.start_link(db: db, typo: true) end
    end

    test "round-trips binaries", %{db: db} do
      assert {:ok, nil} = SlateDB.get(db, "missing")
      assert {:ok, _} = SlateDB.put(db, "k", "v")
      assert {:ok, "v"} = SlateDB.get(db, "k")
      assert {:ok, _} = SlateDB.delete(db, "k")
      assert {:ok, nil} = SlateDB.get(db, "k")
    end

    test "handles non-UTF-8 and empty values", %{db: db} do
      key = <<0, 255, 1>>
      assert {:ok, _} = SlateDB.put(db, key, <<>>)
      assert {:ok, <<>>} = SlateDB.get(db, key)
      assert {:ok, _} = SlateDB.put(db, key, :crypto.strong_rand_bytes(1_000_000))
      assert {:ok, value} = SlateDB.get(db, key)
      assert byte_size(value) == 1_000_000
    end

    test "rejects an empty key without crashing", %{db: db} do
      assert {:error, %SlateDB.Error{kind: :invalid}} = SlateDB.put(db, "", "v")
      assert {:error, %SlateDB.Error{kind: :invalid}} = SlateDB.get(db, "")
      assert {:error, %SlateDB.Error{kind: :invalid}} = SlateDB.delete(db, "")
    end

    test "ttl sets expire_ts", %{db: db} do
      {:ok, _} = SlateDB.put(db, "ttl", "v", ttl: 60_000)
      {:ok, _} = SlateDB.put(db, "forever", "v")

      assert {:ok, %{key: "ttl", value: "v", create_ts: created, expire_ts: expires}} =
               SlateDB.get_key_value(db, "ttl")

      assert expires == created + 60_000
      assert {:ok, %{expire_ts: nil, seq: seq}} = SlateDB.get_key_value(db, "forever")
      assert is_integer(seq)
      assert {:ok, nil} = SlateDB.get_key_value(db, "missing")
    end
  end

  describe "write/3" do
    test "applies a batch", %{db: db} do
      {:ok, _} = SlateDB.put(db, "c", "old")

      assert {:ok, _} =
               SlateDB.write(db, [
                 {:put, "a", "1"},
                 {:put, "b", "2", 60_000},
                 {:delete, "c"}
               ])

      assert {:ok, "1"} = SlateDB.get(db, "a")
      assert {:ok, "2"} = SlateDB.get(db, "b")
      assert {:ok, nil} = SlateDB.get(db, "c")
    end

    test "rejects the whole batch when one op is bad", %{db: db} do
      assert {:error, %SlateDB.Error{kind: :invalid}} =
               SlateDB.write(db, [{:put, "a", "1"}, {:bogus, "x"}])

      assert {:error, %SlateDB.Error{kind: :invalid}} =
               SlateDB.write(db, [{:put, "a", "1"}, {:delete, ""}])

      assert {:ok, nil} = SlateDB.get(db, "a")
    end
  end

  describe "scan/2" do
    setup %{db: db} do
      rows = for i <- 1..20, do: {"key:" <> String.pad_leading("#{i}", 2, "0"), "v#{i}"}
      {:ok, _} = SlateDB.write(db, Enum.map(rows, fn {k, v} -> {:put, k, v} end))
      {:ok, _} = SlateDB.put(db, "other", "x")
      %{rows: rows}
    end

    test "full scan returns rows in key order", %{db: db, rows: rows} do
      assert Enum.to_list(SlateDB.scan(db)) == Enum.sort([{"other", "x"} | rows])
    end

    test "bounds", %{db: db} do
      keys = fn opts -> db |> SlateDB.scan(opts) |> Enum.map(&elem(&1, 0)) end

      assert keys.(gte: "key:05", lt: "key:08") == ["key:05", "key:06", "key:07"]
      assert keys.(gt: "key:05", lte: "key:08") == ["key:06", "key:07", "key:08"]
      assert keys.(gte: "other") == ["other"]
    end

    test "prefix", %{db: db, rows: rows} do
      assert Enum.to_list(SlateDB.scan(db, prefix: "key:")) == rows
      assert Enum.to_list(SlateDB.scan(db, prefix: "nope")) == []
    end

    test "small batches and early halt", %{db: db, rows: rows} do
      assert Enum.to_list(SlateDB.scan(db, prefix: "key:", batch_size: 3)) == rows
      assert Enum.take(SlateDB.scan(db, batch_size: 2), 3) == Enum.take(rows, 3)
    end

    test "scan can include the stored version", %{db: db} do
      {:ok, version} = SlateDB.put(db, "versioned", "value")

      assert [{"versioned", "value", ^version}] =
               Enum.to_list(SlateDB.scan(db, prefix: "versioned", with_versions: true))
    end

    test "a stream can be run again and sees new data", %{db: db} do
      stream = SlateDB.scan(db, prefix: "new:")
      assert Enum.to_list(stream) == []
      {:ok, _} = SlateDB.put(db, "new:1", "x")
      assert Enum.to_list(stream) == [{"new:1", "x"}]
    end

    test "iterator seek", %{db: db} do
      {:ok, iter} = SlateDB.iterator(db, prefix: "key:")
      assert :ok = SlateDB.Iterator.seek(iter, "key:18")

      assert {:ok, [{"key:18", _}, {"key:19", _}, {"key:20", _}]} =
               SlateDB.Iterator.next_batch(iter, 10)

      assert {:ok, []} = SlateDB.Iterator.next_batch(iter)
    end

    test "iterator fixes row shape and takes batch size per fetch", %{db: db} do
      assert_raise ArgumentError, ~r/:batch_size belongs to scan\/2/, fn ->
        SlateDB.iterator(db, batch_size: 2)
      end

      assert_raise ArgumentError, ~r/:batch_size must be a positive integer/, fn ->
        SlateDB.scan(db, batch_size: 0) |> Enum.to_list()
      end
    end

    test "bad options raise", %{db: db} do
      assert_raise ArgumentError, fn ->
        SlateDB.scan(db, gte: "a", gt: "b") |> Enum.to_list()
      end

      assert_raise ArgumentError, fn -> SlateDB.scan(db, prefix: 1) |> Enum.to_list() end
      assert_raise ArgumentError, ~r/:type/, fn -> SlateDB.flush(db, type: :disk) end
    end
  end

  describe "snapshots" do
    test "do not see later writes", %{db: db} do
      {:ok, _} = SlateDB.put(db, "a", "1")
      {:ok, snap} = SlateDB.snapshot(db)
      {:ok, _} = SlateDB.put(db, "a", "2")
      {:ok, _} = SlateDB.put(db, "b", "3")

      assert {:ok, "1"} = Snapshot.get(snap, "a")
      assert {:ok, nil} = Snapshot.get(snap, "b")
      assert Enum.to_list(Snapshot.scan(snap)) == [{"a", "1"}]
      assert {:ok, "2"} = SlateDB.get(db, "a")
    end

    test "read rows with their metadata", %{db: db} do
      {:ok, seq} = SlateDB.put(db, "a", "1")
      {:ok, snap} = SlateDB.snapshot(db)
      {:ok, _} = SlateDB.put(db, "a", "2")

      assert {:ok, %{key: "a", value: "1", seq: ^seq}} = Snapshot.get_key_value(snap, "a")
      assert {:ok, nil} = Snapshot.get_key_value(snap, "b")
    end
  end

  test "read functions reject another kind of handle", %{db: db} do
    {:ok, _} = SlateDB.put(db, "k", "v")
    {:ok, snap} = SlateDB.snapshot(db)
    {:ok, tx} = SlateDB.begin(db)

    # Called with apply/3: the compiler warns about a direct call with the
    # wrong handle type, which is what this test checks at runtime.
    for {module, fun, args} <- [
          {Snapshot, :get, [db, "k"]},
          {Snapshot, :get_key_value, [tx, "k"]},
          {Snapshot, :iterator, [db]},
          {Transaction, :get, [db, "k"]},
          {Transaction, :scan, [snap]},
          {SlateDB, :get, [snap, "k"]},
          {SlateDB, :scan, [tx]}
        ] do
      assert_raise FunctionClauseError, fn -> apply(module, fun, args) end
    end
  end

  describe "transactions" do
    test "commit makes writes visible", %{db: db} do
      {:ok, tx} = SlateDB.begin(db)
      :ok = Transaction.put(tx, "a", "1")
      :ok = Transaction.put(tx, "b", "2")
      :ok = Transaction.delete(tx, "b")

      assert {:ok, "1"} = Transaction.get(tx, "a")
      assert {:ok, nil} = SlateDB.get(db, "a")
      assert Enum.to_list(Transaction.scan(tx)) == [{"a", "1"}]

      assert {:ok, iter} = Transaction.iterator(tx)
      assert {:ok, [{"a", "1"}]} = SlateDB.Iterator.next_batch(iter)
      assert {:ok, %{key: "a", value: "1", seq: nil}} = Transaction.get_key_value(tx, "a")
      assert {:ok, nil} = Transaction.get_key_value(tx, "b")

      assert {:ok, _} = Transaction.commit(tx)
      assert {:ok, "1"} = SlateDB.get(db, "a")
      assert {:ok, nil} = SlateDB.get(db, "b")
    end

    test "uncommitted writes have no version", %{db: db} do
      {:ok, original_version} = SlateDB.put(db, "original", "0")
      {:ok, tx} = SlateDB.begin(db)
      :ok = Transaction.put(tx, "new", "1")

      assert Enum.to_list(Transaction.scan(tx, with_versions: true)) ==
               [{"new", "1", nil}, {"original", "0", original_version}]

      assert {:ok, iter} = Transaction.iterator(tx, with_versions: true)

      assert {:ok, [{"new", "1", nil}, {"original", "0", ^original_version}]} =
               SlateDB.Iterator.next_batch(iter, 10)

      assert_raise ArgumentError, ~r/:with_versions/, fn ->
        SlateDB.Iterator.next_batch(iter, 10, with_versions: false)
      end

      assert {:ok, %{seq: nil}} = Transaction.get_key_value(tx, "new")
      assert {:ok, %{seq: ^original_version}} = Transaction.get_key_value(tx, "original")
    end

    test "rollback discards writes and the transaction cannot be reused", %{db: db} do
      {:ok, tx} = SlateDB.begin(db)
      :ok = Transaction.put(tx, "a", "1")
      assert :ok = Transaction.rollback(tx)
      assert :ok = Transaction.rollback(tx)
      assert {:ok, nil} = SlateDB.get(db, "a")

      assert {:error, %SlateDB.Error{kind: :invalid}} = Transaction.put(tx, "a", "1")
      assert {:error, %SlateDB.Error{kind: :invalid}} = Transaction.get(tx, "a")
      assert {:error, %SlateDB.Error{kind: :invalid}} = Transaction.commit(tx)
    end

    test "write-write conflict is detected", %{db: db} do
      {:ok, tx1} = SlateDB.begin(db)
      {:ok, tx2} = SlateDB.begin(db)
      :ok = Transaction.put(tx1, "k", "from tx1")
      :ok = Transaction.put(tx2, "k", "from tx2")

      assert {:ok, _} = Transaction.commit(tx1)
      assert {:error, %SlateDB.Error{kind: :conflict}} = Transaction.commit(tx2)
      assert {:ok, "from tx1"} = SlateDB.get(db, "k")
    end

    test "serializable detects read-write conflicts", %{db: db} do
      {:ok, _} = SlateDB.put(db, "balance", "10")
      {:ok, tx} = SlateDB.begin(db, isolation: :serializable)
      {:ok, "10"} = Transaction.get(tx, "balance")
      :ok = Transaction.put(tx, "audit", "saw 10")

      {:ok, _} = SlateDB.put(db, "balance", "20")
      assert {:error, %SlateDB.Error{kind: :conflict}} = Transaction.commit(tx)
    end

    # Slap.Streams' group seal relies on this: a create reads the absent
    # seal key, and a seal committed before the create conflicts with it.
    test "serializable detects a write to a key it read as absent", %{db: db} do
      {:ok, tx} = SlateDB.begin(db, isolation: :serializable)
      {:ok, nil} = Transaction.get(tx, "seal")
      :ok = Transaction.put(tx, "created", "1")

      {:ok, _} = SlateDB.put(db, "seal", "1")
      assert {:error, %SlateDB.Error{kind: :conflict}} = Transaction.commit(tx)
      assert {:ok, nil} = SlateDB.get(db, "created")
    end

    test "transaction/3 commits, rolls back, re-raises and retries", %{db: db} do
      assert {:ok, :done} =
               SlateDB.transaction(db, fn tx ->
                 :ok = Transaction.put(tx, "t", "1")
                 {:ok, :done}
               end)

      assert {:ok, "1"} = SlateDB.get(db, "t")

      assert {:error, :nope} =
               SlateDB.transaction(db, fn tx ->
                 :ok = Transaction.put(tx, "t", "2")
                 {:error, :nope}
               end)

      assert {:ok, "1"} = SlateDB.get(db, "t")

      assert_raise RuntimeError, "boom", fn ->
        SlateDB.transaction(db, fn tx ->
          :ok = Transaction.put(tx, "t", "3")
          raise "boom"
        end)
      end

      assert {:ok, "1"} = SlateDB.get(db, "t")

      # The first attempt conflicts with a write made while it runs. The
      # retry does not, so it commits.
      attempts = :counters.new(1, [])

      assert {:ok, 2} =
               SlateDB.transaction(
                 db,
                 fn tx ->
                   :counters.add(attempts, 1, 1)
                   :ok = Transaction.put(tx, "t", "tx")

                   if :counters.get(attempts, 1) == 1 do
                     {:ok, _} = SlateDB.put(db, "t", "outside")
                   end

                   {:ok, :counters.get(attempts, 1)}
                 end,
                 retries: 1
               )

      assert {:ok, "tx"} = SlateDB.get(db, "t")
    end

    test "transaction/3 rolls back after a throw or exit", %{db: db} do
      parent = self()

      assert catch_throw(
               SlateDB.transaction(db, fn tx ->
                 :ok = Transaction.put(tx, "t", "throw")
                 send(parent, {:transaction, tx})
                 throw(:halt)
               end)
             ) == :halt

      assert_receive {:transaction, thrown}
      assert {:error, %SlateDB.Error{kind: :invalid}} = Transaction.put(thrown, "t", "later")

      assert catch_exit(
               SlateDB.transaction(db, fn tx ->
                 :ok = Transaction.put(tx, "t", "exit")
                 send(parent, {:transaction, tx})
                 exit(:halt)
               end)
             ) == :halt

      assert_receive {:transaction, exited}
      assert {:error, %SlateDB.Error{kind: :invalid}} = Transaction.put(exited, "t", "later")
      assert {:ok, nil} = SlateDB.get(db, "t")
    end
  end

  describe "lifecycle" do
    test "calls after close return a closed error" do
      {:ok, db} = SlateDB.open("closing", store: :memory)
      assert :ok = SlateDB.close(db)

      assert {:error, %SlateDB.Error{kind: :closed, reason: :clean}} =
               SlateDB.get(db, "k")

      assert {:error, %SlateDB.Error{kind: :closed}} = SlateDB.put(db, "k", "v")
    end

    test "data persists in a local store across reopen" do
      dir = Path.join(System.tmp_dir!(), "slap-slatedb-#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, db} = SlateDB.open("persist", store: {:local, dir})
      {:ok, _} = SlateDB.put(db, "k", "v")
      :ok = SlateDB.close(db)

      {:ok, db} = SlateDB.open("persist", store: {:local, dir})
      assert {:ok, "v"} = SlateDB.get(db, "k")
      :ok = SlateDB.close(db)
    end

    test "a second writer fences the first" do
      dir = Path.join(System.tmp_dir!(), "slap-slatedb-#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, first} = SlateDB.open("fenced", store: {:local, dir})
      {:ok, second} = SlateDB.open("fenced", store: {:local, dir})

      assert {:error, %SlateDB.Error{kind: :closed, reason: :fenced}} =
               SlateDB.put(first, "k", "v", await_durable: true)

      assert {:ok, _} = SlateDB.put(second, "k", "v", await_durable: true)
      :ok = SlateDB.close(second)
      assert :ok = SlateDB.close(first)
    end

    test "settings are merged over the defaults" do
      assert {:ok, db} =
               SlateDB.open("settings",
                 store: :memory,
                 settings: %{flush_interval: "10ms", default_ttl_millis: 5_000}
               )

      {:ok, _} = SlateDB.put(db, "k", "v")

      assert {:ok, %{create_ts: created, expire_ts: expires}} =
               SlateDB.get_key_value(db, "k")

      assert expires == created + 5_000
      :ok = SlateDB.close(db)

      assert {:error, %SlateDB.Error{kind: :invalid}} =
               SlateDB.open("bad", store: :memory, settings: %{flush_interval: 5})
    end

    test "bad store options raise" do
      assert_raise ArgumentError, fn -> SlateDB.open("x", store: :nowhere) end

      assert {:error, %SlateDB.Error{kind: :invalid}} =
               SlateDB.open("x", store: {:url, "not a url"})
    end
  end

  describe "concurrency" do
    test "many processes share one database", %{db: db} do
      1..500
      |> Task.async_stream(
        fn i ->
          key = "c:#{i}"
          {:ok, _} = SlateDB.put(db, key, Integer.to_string(i))
          {:ok, value} = SlateDB.get(db, key)
          value
        end,
        max_concurrency: 100
      )
      |> Enum.each(fn {:ok, value} -> assert is_binary(value) end)

      assert Enum.count(SlateDB.scan(db, prefix: "c:")) == 500
    end

    test "a reply is not confused with other messages", %{db: db} do
      send(self(), {:slap_slatedb_reply, make_ref(), {:ok, "stray"}})
      {:ok, _} = SlateDB.put(db, "k", "v")
      assert {:ok, "v"} = SlateDB.get(db, "k")
      assert_received {:slap_slatedb_reply, _, {:ok, "stray"}}
    end

    # About 15 seconds: object_store retries the unreachable endpoint 10
    # times. Run with `mix test --include slow`.
    @tag :slow
    @tag timeout: 120_000
    test "schedulers stay responsive while calls wait on slow I/O" do
      # 10.255.255.1 does not answer, so each open waits on the network until
      # it gives up.
      store_opts = [
        {"aws_endpoint", "http://10.255.255.1:9000"},
        {"allow_http", "true"},
        {"aws_access_key_id", "x"},
        {"aws_secret_access_key", "x"},
        {"aws_region", "us-east-1"},
        {"connect_timeout", "1s"}
      ]

      # Four blocked callers per scheduler. If the NIF blocked its scheduler
      # thread, every scheduler would be stuck and the sleep below would
      # finish late.
      tasks =
        for _ <- 1..(System.schedulers_online() * 4) do
          Task.async(fn ->
            # Without this, SlateDB retries on top of object_store's retries.
            SlateDB.open("db",
              store: {:url, "s3://bucket", store_opts},
              settings: %{object_store_max_retries: 0}
            )
          end)
        end

      Process.sleep(100)
      {elapsed_us, _} = :timer.tc(fn -> Process.sleep(20) end)
      assert elapsed_us < 100_000

      for result <- Task.await_many(tasks, 110_000) do
        assert {:error, %SlateDB.Error{kind: :unavailable}} = result
      end
    end
  end
end
