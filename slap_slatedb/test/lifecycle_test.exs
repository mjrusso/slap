defmodule Slap.SlateDB.LifecycleTest do
  # Not async: some tests change application settings and capture logs.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Slap.SlateDB
  alias Slap.SlateDB.{Snapshot, Transaction}

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "slap-slatedb-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end

  defp conflicting_runtime_threads do
    if SlateDB.Native.runtime_init(1) == :ok, do: 2, else: 1
  end

  # See the note in durability_test.exs: let the WAL flush timer's first
  # tick pass, so later writes wait for `flush_interval`.
  defp warm_up(db) do
    {:ok, _} = SlateDB.put(db, "warm-up", "x")
    :ok = SlateDB.flush(db)
    Process.sleep(200)
  end

  # Tests turn SlateDB logging off (see test_helper.exs). These tests check
  # log output, so they turn it back on.
  defp forward_warnings(_context) do
    :ok = SlateDB.set_log_level(:warning)
    on_exit(fn -> SlateDB.set_log_level(:none) end)
  end

  defp gc_until(fun) do
    Enum.reduce_while(1..50, false, fn _, _ ->
      :erlang.garbage_collect()
      Process.sleep(20)
      if fun.(), do: {:halt, true}, else: {:cont, false}
    end)
  end

  test "invalid runtime thread settings fail when the application starts" do
    key = "SLAP_SLATEDB_RUNTIME_THREADS"
    original = System.get_env(key)

    on_exit(fn ->
      if original, do: System.put_env(key, original), else: System.delete_env(key)
    end)

    for value <- ["0", "many", "-2"] do
      System.put_env(key, value)

      assert_raise ArgumentError,
                   ~r/SLAP_SLATEDB_RUNTIME_THREADS must be a positive integer/,
                   fn ->
                     SlateDB.Application.start(:normal, [])
                   end
    end
  end

  test "the environment thread count reaches the NIF" do
    key = "SLAP_SLATEDB_RUNTIME_THREADS"
    original = System.get_env(key)

    on_exit(fn ->
      if original, do: System.put_env(key, original), else: System.delete_env(key)
    end)

    threads = conflicting_runtime_threads()
    System.put_env(key, Integer.to_string(threads))

    assert_raise SlateDB.Error, ~r/restart the VM to change it/, fn ->
      SlateDB.Application.start(:normal, [])
    end
  end

  test "application config takes precedence and nil falls through to the environment" do
    key = "SLAP_SLATEDB_RUNTIME_THREADS"
    original_env = System.get_env(key)
    original_config = Application.fetch_env(:slap_slatedb, :runtime_threads)

    on_exit(fn ->
      if original_env, do: System.put_env(key, original_env), else: System.delete_env(key)

      case original_config do
        {:ok, value} -> Application.put_env(:slap_slatedb, :runtime_threads, value)
        :error -> Application.delete_env(:slap_slatedb, :runtime_threads)
      end
    end)

    threads = conflicting_runtime_threads()
    System.put_env(key, "many")
    Application.put_env(:slap_slatedb, :runtime_threads, threads)

    assert_raise SlateDB.Error, ~r/restart the VM to change it/, fn ->
      SlateDB.Application.start(:normal, [])
    end

    Application.put_env(:slap_slatedb, :runtime_threads, nil)
    System.put_env(key, Integer.to_string(threads))

    assert_raise SlateDB.Error, ~r/restart the VM to change it/, fn ->
      SlateDB.Application.start(:normal, [])
    end

    Application.put_env(:slap_slatedb, :runtime_threads, 0)

    assert_raise ArgumentError, ~r/:runtime_threads must be a positive integer/, fn ->
      SlateDB.Application.start(:normal, [])
    end
  end

  describe "close" do
    test "is idempotent and later calls get :closed" do
      {:ok, db} = SlateDB.open("close", store: :memory)
      {:ok, _} = SlateDB.put(db, "k", "v")
      {:ok, snap} = SlateDB.snapshot(db)
      {:ok, tx} = SlateDB.begin(db)
      {:ok, iter} = SlateDB.iterator(db)

      assert :ok = SlateDB.close(db)
      assert :ok = SlateDB.close(db)

      closed = %SlateDB.Error{
        kind: :closed,
        reason: :clean,
        message: "the database is closed"
      }

      assert {:error, ^closed} = SlateDB.get(db, "k")
      assert {:error, ^closed} = SlateDB.put(db, "k", "v")
      assert {:error, ^closed} = SlateDB.flush(db)
      assert {:error, ^closed} = SlateDB.snapshot(db)
      assert {:error, ^closed} = Snapshot.get(snap, "k")
      assert {:error, ^closed} = Transaction.get(tx, "k")
      assert {:error, ^closed} = Transaction.put(tx, "k", "v")
      assert {:error, ^closed} = Transaction.commit(tx)
      assert :ok = Transaction.rollback(tx)
      assert {:error, ^closed} = SlateDB.Iterator.next_batch(iter)
      assert_raise SlateDB.Error, fn -> db |> SlateDB.scan() |> Enum.to_list() end

      assert is_integer(SlateDB.durable_seq(db))
      assert %{durable_seq: _} = SlateDB.stats(db)
    end

    test "waits for calls in flight, and calls made during close get :closed" do
      {:ok, db} =
        SlateDB.open("inflight", store: :memory, settings: %{flush_interval: "500ms"})

      warm_up(db)

      # This write waits up to 500 ms for the next WAL flush.
      writer = Task.async(fn -> SlateDB.put(db, "k", "v", await_durable: true) end)
      Process.sleep(50)

      closer = Task.async(fn -> SlateDB.close(db) end)
      Process.sleep(50)
      late = Task.async(fn -> SlateDB.get(db, "k") end)

      assert {:ok, seq} = Task.await(writer)
      assert :ok = Task.await(closer)
      assert {:error, %SlateDB.Error{kind: :closed, reason: :clean}} = Task.await(late)
      assert SlateDB.durable_seq(db) >= seq
    end
  end

  describe "handles keep the database alive" do
    setup :forward_warnings

    test "a snapshot keeps the database open until it is dropped too" do
      parent = self()

      # The snapshot lives in its own process, so dropping it is just that
      # process exiting.
      holder =
        spawn(fn ->
          snap =
            (fn ->
               {:ok, db} = SlateDB.open("alive", store: :memory)
               {:ok, _} = SlateDB.put(db, "k", "v")
               {:ok, snap} = SlateDB.snapshot(db)
               snap
             end).()

          :erlang.garbage_collect()
          Process.sleep(200)
          send(parent, {:read, Snapshot.get(snap, "k"), Enum.to_list(Snapshot.scan(snap))})

          receive do
            :exit -> :ok
          end
        end)

      log =
        capture_log(fn ->
          assert_receive {:read, {:ok, "v"}, [{"k", "v"}]}, 2_000
          Process.sleep(200)
        end)

      refute log =~ "garbage collected"

      ref = Process.monitor(holder)

      log =
        capture_log(fn ->
          send(holder, :exit)
          assert_receive {:DOWN, ^ref, _, _, _}
          Process.sleep(300)
        end)

      assert log =~ "garbage collected without Slap.SlateDB.close/1"
    end

    test "dropping the last handle without close logs a warning" do
      log =
        capture_log(fn ->
          (fn ->
             {:ok, db} = SlateDB.open("leak", store: :memory)
             {:ok, _} = SlateDB.put(db, "k", "v")
             :ok
           end).()

          assert gc_until(fn -> false end) == false
        end)

      assert log =~ "garbage collected without Slap.SlateDB.close/1"
    end

    test "a closed database does not warn when collected" do
      log =
        capture_log(fn ->
          (fn ->
             {:ok, db} = SlateDB.open("closed-leak", store: :memory)
             :ok = SlateDB.close(db)
           end).()

          gc_until(fn -> false end)
        end)

      refute log =~ "garbage collected"
    end
  end

  describe "timeouts" do
    test "a slow call times out and its late reply is dropped" do
      {:ok, db} = SlateDB.open("slow", store: :memory, settings: %{flush_interval: "500ms"})
      warm_up(db)

      assert {:error, %SlateDB.Error{kind: :timeout, message: message}} =
               SlateDB.put(db, "k", "v", await_durable: true, timeout: 50)

      assert message =~ "may still complete"

      # The write was not cancelled.
      Process.sleep(700)
      assert {:ok, "v"} = SlateDB.get(db, "k", durability: :remote)
      assert {:messages, []} = Process.info(self(), :messages)
      :ok = SlateDB.close(db)
    end

    test "fast calls with a timeout work, and errors still raise" do
      {:ok, db} = SlateDB.open("fast", store: :memory, timeout: 5_000)
      assert {:ok, _} = SlateDB.put(db, "k", "v", timeout: 1_000)
      assert {:ok, "v"} = SlateDB.get(db, "k", timeout: 1_000)
      assert [{"k", "v"}] = db |> SlateDB.scan(timeout: 1_000) |> Enum.to_list()

      assert {:error, %SlateDB.Error{kind: :invalid}} =
               SlateDB.get(db, "", timeout: 1_000)

      assert_raise ArgumentError, fn ->
        SlateDB.get(db, "k", dirty: "yes", timeout: 1_000)
      end

      assert_raise ArgumentError, fn -> SlateDB.get(db, "k", timeout: -1) end
      assert {:messages, []} = Process.info(self(), :messages)
      :ok = SlateDB.close(db)
    end
  end

  describe "large values" do
    setup do
      {:ok, db} = SlateDB.open("large", store: {:local, tmp_dir()})
      on_exit(fn -> SlateDB.close(db) end)
      %{db: db}
    end

    test "values around the zero-copy threshold round-trip", %{db: db} do
      for size <- [65_535, 65_536, 65_537, 1_000_000] do
        value = :crypto.strong_rand_bytes(size)
        key = "v#{size}"
        {:ok, _} = SlateDB.put(db, key, value)
        assert {:ok, ^value} = SlateDB.get(db, key)
        assert {:ok, %{value: ^value}} = SlateDB.get_key_value(db, key)
      end

      big = :crypto.strong_rand_bytes(200_000)
      {:ok, _} = SlateDB.write(db, [{:put, "b1", big}, {:put, "b2", "small"}])

      assert [{"b1", ^big}, {"b2", "small"}] =
               db |> SlateDB.scan(prefix: "b") |> Enum.to_list()

      # Read back from SSTs rather than the memtable.
      :ok = SlateDB.flush(db)
      assert {:ok, ^big} = SlateDB.get(db, "b1")
    end

    test "keys longer than 65535 bytes round-trip", %{db: db} do
      key = :crypto.strong_rand_bytes(100_000)
      {:ok, _} = SlateDB.put(db, key, "v")
      assert {:ok, "v"} = SlateDB.get(db, key)
    end

    test "a returned large value outlives the database", %{db: db} do
      value = :crypto.strong_rand_bytes(500_000)
      {:ok, _} = SlateDB.put(db, "k", value)
      {:ok, got} = SlateDB.get(db, "k")
      :ok = SlateDB.close(db)
      :erlang.garbage_collect()
      assert got == value
    end

    test "a sub-binary of a larger binary is written correctly", %{db: db} do
      whole = :crypto.strong_rand_bytes(300_000)
      <<_::binary-size(1_000), part::binary-size(100_000), _::binary>> = whole
      {:ok, _} = SlateDB.put(db, "part", part)
      assert {:ok, ^part} = SlateDB.get(db, "part")
    end
  end

  test "large batches are written through the dirty scheduler" do
    {:ok, db} = SlateDB.open("dirty", store: :memory)
    ops = for i <- 1..5_000, do: {:put, "k#{String.pad_leading("#{i}", 5, "0")}", "v#{i}"}
    assert {:ok, seq} = SlateDB.write(db, ops)
    assert {:ok, %{seq: ^seq}} = SlateDB.get_key_value(db, "k02500")
    assert Enum.count(SlateDB.scan(db)) == 5_000

    assert {:error, %SlateDB.Error{kind: :invalid}} =
             SlateDB.write(db, [{:delete, ""} | ops])

    :ok = SlateDB.close(db)
  end

  describe "stats, metrics and logs" do
    setup :forward_warnings

    test "stats report durability lag and the cache" do
      dir = tmp_dir()

      {:ok, db} =
        SlateDB.open("stats", store: {:local, dir}, settings: %{flush_interval: "10s"})

      warm_up(db)

      {:ok, seq} = SlateDB.put(db, "k", "v")
      stats = SlateDB.stats(db)
      assert stats.last_write_seq == seq
      assert stats.durability_lag > 0
      assert SlateDB.durability_lag(db) == stats.durability_lag

      :ok = SlateDB.flush(db)
      assert %{durability_lag: 0} = SlateDB.stats(db)
      assert SlateDB.durability_lag(db) == 0
      :ok = SlateDB.close(db)

      {:ok, db} = SlateDB.open("stats", store: {:local, dir})
      for _ <- 1..5, do: {:ok, "v"} = SlateDB.get(db, "k")
      stats = SlateDB.stats(db)
      assert stats.cache_hits + stats.cache_misses > 0
      assert stats.l0_sst_count >= 1
      assert is_integer(stats.sorted_run_count)

      metrics = SlateDB.metrics(db)

      assert %{value: 5} =
               Enum.find(
                 metrics,
                 &(&1.name == "slatedb.db.request_count" and &1.labels == %{"op" => "get"})
               )

      assert Enum.any?(metrics, &is_map(&1.value))
      :ok = SlateDB.close(db)
    end

    test "log level can be changed" do
      assert_raise ArgumentError, fn -> SlateDB.set_log_level(:loud) end
      :ok = SlateDB.set_log_level(:none)

      log =
        capture_log(fn ->
          (fn ->
             {:ok, _db} = SlateDB.open("quiet", store: :memory)
             :ok
           end).()

          gc_until(fn -> false end)
        end)

      refute log =~ "garbage collected"
    end
  end
end
