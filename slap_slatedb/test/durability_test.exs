defmodule Slap.SlateDB.DurabilityTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias Slap.SlateDB
  alias Slap.SlateDB.Transaction

  # The WAL flush timer fires once right after open, and that first tick can
  # come late when the machine is busy. Let it pass, so the next write waits
  # for `flush_interval` and tests can see it before it is durable.
  defp warm_up(db) do
    {:ok, _} = SlateDB.put(db, "warm-up", "x")
    :ok = SlateDB.flush(db)
    Process.sleep(200)
  end

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "slap-slatedb-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  describe "sequence numbers" do
    setup do
      {:ok, db} = SlateDB.open("seq", store: :memory)
      on_exit(fn -> SlateDB.close(db) end)
      %{db: db}
    end

    test "every write returns a higher seq", %{db: db} do
      {:ok, s1} = SlateDB.put(db, "a", "1")
      {:ok, s2} = SlateDB.delete(db, "a")
      {:ok, s3} = SlateDB.write(db, [{:put, "b", "2"}, {:put, "c", "3"}])
      {:ok, s4} = SlateDB.put(db, "d", "4", await_durable: true)
      assert s1 < s2 and s2 < s3 and s3 < s4

      assert {:ok, %{seq: ^s3}} = SlateDB.get_key_value(db, "b")
      assert {:ok, %{seq: ^s3}} = SlateDB.get_key_value(db, "c")
    end

    test "commit returns a seq, or nil when there were no writes", %{db: db} do
      {:ok, before} = SlateDB.put(db, "x", "1")

      {:ok, tx} = SlateDB.begin(db)
      :ok = Transaction.put(tx, "y", "2")
      assert {:ok, seq} = Transaction.commit(tx)
      assert seq > before

      {:ok, empty} = SlateDB.begin(db)
      assert {:ok, nil} = Transaction.commit(empty)
    end
  end

  describe "durable_seq and subscriptions" do
    test "durable_seq covers a write once it is durable" do
      {:ok, db} = SlateDB.open("d", store: :memory, settings: %{flush_interval: "10s"})
      warm_up(db)
      {:ok, seq} = SlateDB.put(db, "k", "v")
      assert SlateDB.last_write_seq(db) == seq
      assert SlateDB.durable_seq(db) < seq
      :ok = SlateDB.flush(db)
      assert SlateDB.durable_seq(db) >= seq
      :ok = SlateDB.close(db)
    end

    test "subscribers hear about durable progress without waiting on writes" do
      {:ok, db} = SlateDB.open("sub", store: :memory, settings: %{flush_interval: "20ms"})
      {:ok, %{ref: ref}} = SlateDB.subscribe(db, :mine)
      assert_receive {:slap_slatedb_durable, ^ref, :mine, initial}

      seqs = for i <- 1..50, do: elem(SlateDB.put(db, "k#{i}", "v"), 1)
      last = List.last(seqs)
      assert last > initial
      assert_durable(ref, last)

      :ok = SlateDB.close(db)
      assert_receive {:slap_slatedb_closed, ^ref, :mine, :clean}
    end

    test "subscriptions that share a tag are told apart by their ref" do
      {:ok, db} = SlateDB.open("same-tag", store: :memory, settings: %{flush_interval: "10s"})
      {:ok, %{ref: first_ref} = first} = SlateDB.subscribe(db, :same)
      {:ok, %{ref: second_ref}} = SlateDB.subscribe(db, :same)
      assert first_ref != second_ref
      assert_receive {:slap_slatedb_durable, ^first_ref, :same, _}
      assert_receive {:slap_slatedb_durable, ^second_ref, :same, _}

      :ok = SlateDB.unsubscribe(first)
      {:ok, seq} = SlateDB.put(db, "k", "v", await_durable: true)
      assert_durable(second_ref, seq)
      refute_received {:slap_slatedb_durable, ^first_ref, :same, _}

      :ok = SlateDB.close(db)
      assert_receive {:slap_slatedb_closed, ^second_ref, :same, :clean}
      refute_received {:slap_slatedb_closed, ^first_ref, :same, _}
    end

    test "unsubscribe with flush: true removes the subscription's queued messages" do
      {:ok, db} = SlateDB.open("flush", store: :memory, settings: %{flush_interval: "10s"})
      {:ok, kept} = SlateDB.subscribe(db, :kept)
      {:ok, flushed} = SlateDB.subscribe(db, :flushed)

      # Take each initial message and queue it again, so both are known to be
      # in the mailbox when unsubscribe runs.
      for %{ref: ref} <- [kept, flushed] do
        assert_receive {:slap_slatedb_durable, ^ref, _, _} = message
        send(self(), message)
      end

      :ok = SlateDB.unsubscribe(kept)
      :ok = SlateDB.unsubscribe(flushed, flush: true)
      {:ok, _} = SlateDB.put(db, "k", "v", await_durable: true)
      :ok = SlateDB.close(db)

      %{ref: kept_ref} = kept
      %{ref: flushed_ref} = flushed
      assert_received {:slap_slatedb_durable, ^kept_ref, :kept, _}
      refute_received {:slap_slatedb_durable, ^kept_ref, _, _}
      refute_received {_, ^flushed_ref, _, _}
    end

    test "updates go to the given pid and stop after unsubscribe" do
      {:ok, db} = SlateDB.open("unsub", store: :memory, settings: %{flush_interval: "10s"})

      listener =
        spawn_link(fn ->
          receive do
            :stop -> :ok
          end
        end)

      {:ok, sub} = SlateDB.subscribe(db, :other, listener)
      {:ok, %{ref: mine_ref} = mine} = SlateDB.subscribe(db, :mine)
      assert_receive {:slap_slatedb_durable, ^mine_ref, :mine, _}

      :ok = SlateDB.unsubscribe(mine)
      :ok = SlateDB.unsubscribe(mine)
      {:ok, _} = SlateDB.put(db, "k", "v", await_durable: true)
      refute_receive {:slap_slatedb_durable, ^mine_ref, :mine, _}, 100

      {:messages, messages} = Process.info(listener, :messages)
      other_ref = sub.ref
      assert Enum.any?(messages, &match?({:slap_slatedb_durable, ^other_ref, :other, _}, &1))

      :ok = SlateDB.unsubscribe(sub)
      send(listener, :stop)
      :ok = SlateDB.close(db)
    end

    test "a subscriber that exits does not break the database" do
      {:ok, db} = SlateDB.open("dead", store: :memory, settings: %{flush_interval: "10ms"})
      pid = spawn(fn -> :ok end)
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, _, _, _}

      {:ok, _} = SlateDB.subscribe(db, :gone, pid)
      {:ok, seq} = SlateDB.put(db, "k", "v", await_durable: true)
      assert SlateDB.durable_seq(db) >= seq
      :ok = SlateDB.close(db)
    end

    test "the subscription monitors the subscriber" do
      {:ok, db} = SlateDB.open("monitor", store: :memory)
      pid = spawn(fn -> receive do: (:stop -> :ok) end)

      {:ok, sub} = SlateDB.subscribe(db, :watched, pid)
      # The NIF monitor shows up as a resource monitoring the process, so an
      # exit ends the subscription at once, not at the next send.
      {:monitored_by, monitors} = Process.info(pid, :monitored_by)
      assert Enum.any?(monitors, &is_reference/1)

      ref = Process.monitor(pid)
      send(pid, :stop)
      assert_receive {:DOWN, ^ref, _, _, _}

      assert :ok = SlateDB.unsubscribe(sub)
      {:ok, seq} = SlateDB.put(db, "k", "v", await_durable: true)
      assert SlateDB.durable_seq(db) >= seq
      :ok = SlateDB.close(db)
    end

    test "close reports a fence it discovers, since unflushed writes are lost" do
      dir = tmp_dir()
      # A long poll interval, so `first` does not notice the fence by itself.
      {:ok, first} =
        SlateDB.open("fence-close",
          store: {:local, dir},
          settings: %{manifest_poll_interval: "1h"}
        )

      {:ok, _} = SlateDB.put(first, "k", "durable", await_durable: true)
      {:ok, second} = SlateDB.open("fence-close", store: {:local, dir})

      # Accepted into memory; the final flush at close is refused.
      {:ok, _} = SlateDB.put(first, "k", "lost")

      assert {:error, %SlateDB.Error{kind: :closed, reason: :fenced}} =
               SlateDB.close(first)

      # Closing again is :ok, as for any closed handle.
      assert :ok = SlateDB.close(first)
      assert {:ok, "durable"} = SlateDB.get(second, "k")
      :ok = SlateDB.close(second)
    end

    test "an idle writer learns that it was fenced" do
      dir = tmp_dir()
      settings = %{manifest_poll_interval: "100ms"}
      {:ok, first} = SlateDB.open("fence", store: {:local, dir}, settings: settings)
      {:ok, %{ref: ref}} = SlateDB.subscribe(first, :first)
      assert_receive {:slap_slatedb_durable, ^ref, :first, _}

      {:ok, second} = SlateDB.open("fence", store: {:local, dir}, settings: settings)

      # No writes on `first`: it finds out from its manifest poll.
      assert_receive {:slap_slatedb_closed, ^ref, :first, :fenced}, 2_000

      assert {:error, %SlateDB.Error{kind: :closed, reason: :fenced}} =
               SlateDB.put(first, "k", "v")

      :ok = SlateDB.close(second)
      assert :ok = SlateDB.close(first)
    end
  end

  defp assert_durable(ref, seq) do
    receive do
      {:slap_slatedb_durable, ^ref, _tag, durable} when durable >= seq -> :ok
      {:slap_slatedb_durable, ^ref, _tag, _} -> assert_durable(ref, seq)
    after
      2_000 -> flunk("durable_seq did not reach #{seq}")
    end
  end

  describe "read options" do
    setup do
      {:ok, db} = SlateDB.open("opts", store: :memory, settings: %{flush_interval: "10s"})
      on_exit(fn -> SlateDB.close(db) end)
      %{db: db}
    end

    test "durability: :remote reads only durable data", %{db: db} do
      warm_up(db)
      {:ok, _} = SlateDB.put(db, "k", "v")
      assert {:ok, "v"} = SlateDB.get(db, "k")
      assert {:ok, nil} = SlateDB.get(db, "k", durability: :remote)
      assert Enum.to_list(SlateDB.scan(db, prefix: "k", durability: :remote)) == []

      :ok = SlateDB.flush(db)
      assert {:ok, "v"} = SlateDB.get(db, "k", durability: :remote)
      assert {:ok, %{value: "v"}} = SlateDB.get_key_value(db, "k", durability: :remote)
      assert Enum.to_list(SlateDB.scan(db, prefix: "k", durability: :remote)) == [{"k", "v"}]
    end

    test "scan order and fetch options", %{db: db} do
      {:ok, _} = SlateDB.write(db, for(k <- ~w(a b c d), do: {:put, k, k}))

      assert db |> SlateDB.scan(order: :desc) |> Enum.map(&elem(&1, 0)) == ~w(d c b a)

      assert db
             |> SlateDB.scan(gte: "b", order: :desc, batch_size: 1)
             |> Enum.map(&elem(&1, 0)) == ~w(d c b)

      rows =
        SlateDB.scan(db,
          cache_blocks: true,
          read_ahead_bytes: 65_536,
          max_fetch_tasks: 4,
          dirty: false
        )

      assert Enum.count(rows) == 4
    end

    test "options work on snapshots and transactions", %{db: db} do
      {:ok, _} = SlateDB.put(db, "k", "v")
      {:ok, snap} = SlateDB.snapshot(db)
      assert {:ok, "v"} = SlateDB.Snapshot.get(snap, "k", cache_blocks: false)
      assert [{"k", "v"}] = snap |> SlateDB.Snapshot.scan(order: :desc) |> Enum.to_list()

      {:ok, tx} = SlateDB.begin(db)
      assert {:ok, "v"} = Transaction.get(tx, "k", durability: :memory)
      assert [{"k", "v"}] = tx |> Transaction.scan(order: :desc) |> Enum.to_list()
      {:ok, nil} = Transaction.commit(tx)
    end

    test "bad options", %{db: db} do
      assert_raise ArgumentError, fn -> SlateDB.get(db, "k", durabilty: :remote) end
      assert_raise ArgumentError, fn -> db |> SlateDB.scan(ordr: :desc) |> Enum.to_list() end

      assert_raise ArgumentError, ~r/:durability/, fn ->
        SlateDB.get(db, "k", durability: :disk)
      end

      assert_raise ArgumentError, ~r/:order/, fn ->
        db |> SlateDB.scan(order: :sideways) |> Enum.to_list()
      end

      assert_raise ArgumentError, ~r/:dirty/, fn -> SlateDB.get(db, "k", dirty: "yes") end

      assert_raise ArgumentError, ~r/max_fetch_tasks/, fn ->
        db |> SlateDB.scan(max_fetch_tasks: 0) |> Enum.to_list()
      end
    end
  end

  describe "caches" do
    test "a shared cache serves several databases" do
      cache = SlateDB.Cache.new(16 * 1024 * 1024)
      dir = tmp_dir()

      dbs =
        for name <- ~w(one two) do
          {:ok, db} = SlateDB.open(name, store: {:local, dir}, cache: cache)
          {:ok, _} = SlateDB.put(db, "name", name)
          :ok = SlateDB.close(db)
          {:ok, db} = SlateDB.open(name, store: {:local, dir}, cache: cache)
          {name, db}
        end

      for {name, db} <- dbs do
        assert {:ok, ^name} = SlateDB.get(db, "name")
        assert {:ok, ^name} = SlateDB.get(db, "name")
        :ok = SlateDB.close(db)
      end
    end

    test "the cache can be disabled" do
      {:ok, db} = SlateDB.open("nocache", store: :memory, cache: :disabled)
      {:ok, _} = SlateDB.put(db, "k", "v")
      assert {:ok, "v"} = SlateDB.get(db, "k")
      :ok = SlateDB.close(db)
    end

    test "a bad cache option raises" do
      assert_raise ArgumentError, fn -> SlateDB.open("x", store: :memory, cache: :big) end
    end

    test "the local disk cache fills through settings" do
      dir = tmp_dir()
      cache_dir = Path.join(dir, "cache")
      settings = %{object_store_cache_options: %{root_folder: cache_dir}}

      {:ok, db} =
        SlateDB.open("disk", store: {:local, Path.join(dir, "data")}, settings: settings)

      {:ok, _} = SlateDB.write(db, for(i <- 1..500, do: {:put, "k#{i}", "v#{i}"}))
      :ok = SlateDB.flush(db)
      :ok = SlateDB.close(db)

      {:ok, db} =
        SlateDB.open("disk", store: {:local, Path.join(dir, "data")}, settings: settings)

      assert Enum.count(SlateDB.scan(db)) == 500
      :ok = SlateDB.close(db)

      assert Path.wildcard(Path.join(cache_dir, "**/*")) |> Enum.any?(&File.regular?/1)
    end
  end
end
