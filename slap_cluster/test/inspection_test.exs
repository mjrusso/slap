defmodule Slap.Cluster.InspectionTest do
  # Admission uses one process name for every cluster on this node.
  use ExUnit.Case, async: false

  alias Slap.Cluster.{Inspection, Test.TestCluster}
  alias Slap.Cluster.Inspection.Read
  alias Slap.SlateDB

  setup do
    start_supervised!({TestCluster, store: :memory, shards: 1})
    assert :ok = TestCluster.await_ready(5000)
    {:ok, {:local, ctx}} = TestCluster.lookup(0)
    %{ctx: ctx}
  end

  test "admission remains occupied until its process finishes", %{ctx: ctx} do
    parent = self()

    reader =
      Task.async(fn ->
        Inspection.request(TestCluster, 0, 1, 1000, fn _, _ ->
          send(parent, :entered)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :entered

    assert {:error, :busy} =
             Inspection.request(TestCluster, 0, 1, 1000, fn _, _ -> flunk("read while busy") end)

    assert {:error, :busy} =
             Inspection.exclusive(1, 1000, fn _ -> flunk("compute while reading") end)

    send(reader.pid, :release)
    assert :ok = Task.await(reader)

    assert {:ok, _} =
             Inspection.request(TestCluster, 0, 1, 1000, fn ^ctx, _ -> {:ok, :available} end)
  end

  test "the VM releases admission after the holder dies" do
    parent = self()

    pid =
      spawn(fn ->
        Inspection.request(TestCluster, 0, 1, 1000, fn _, _ ->
          send(parent, :entered)

          receive do
            :never -> :ok
          end
        end)
      end)

    ref = Process.monitor(pid)
    assert_receive :entered
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
    assert :ok = Inspection.request(TestCluster, 0, 1, 1000, fn _, _ -> :ok end)
  end

  test "storage-free work excludes shard reads and releases admission on failure" do
    assert :ok =
             Inspection.exclusive(1, 1000, fn _deadline ->
               assert {:error, :busy} =
                        Inspection.request(TestCluster, 0, 1, 1000, fn _, _ ->
                          flunk("read during compute")
                        end)

               :ok
             end)

    # ArgumentError because Inspection rescues it from lookup; one raised by
    # the work must still propagate and release admission.
    assert_raise ArgumentError, "storage failure", fn ->
      Inspection.exclusive(1, 1000, fn _ -> raise ArgumentError, "storage failure" end)
    end

    assert :ok = Inspection.exclusive(1, 1000, fn _ -> :ok end)
    assert {:error, :invalid_budget} = Inspection.exclusive(1, 0, fn _ -> flunk("invalid") end)

    assert {:error, {:unsupported_api, 1}} =
             Inspection.exclusive(2, 1000, fn _ -> flunk("unsupported") end)
  end

  test "bounded pages skip only oversized keys with the reported prefix", %{ctx: ctx} do
    for {key, value} <- [
          {"a", "one"},
          {"bb", "prefix"},
          {"bb-long", String.duplicate("x", 4096)},
          {"bb-other", "skip"},
          {"bc", "next"}
        ] do
      assert {:ok, _} = SlateDB.put(ctx.db, key, value, await_durable: true)
    end

    assert :ok = SlateDB.flush(ctx.db, type: :memtable)

    opts = [max_key_bytes: 2, max_value_bytes: 8, target_bytes: 1]

    read = fn opts ->
      Inspection.request(TestCluster, 0, 1, 1000, fn ctx, deadline ->
        Read.page(ctx, deadline, opts, fn _, _ -> :raw end)
      end)
    end

    assert {:ok, first} = read.(opts)
    assert [%{key: "a"}] = first.rows

    assert {:ok, second} =
             read.([cursor: first.cursor, cursor_exclusive: first.cursor_exclusive] ++ opts)

    assert [%{key: "bb", oversized_key: false}] = second.rows

    assert {:ok, third} =
             read.([cursor: second.cursor, cursor_exclusive: second.cursor_exclusive] ++ opts)

    assert [%{key: "bb", key_size: 7, value_size: 4096, oversized_key: true, truncated: true}] =
             third.rows

    assert third.cursor == "bc"
    assert third.stopped == :oversized_keys_skipped
    assert :erlang.external_size(third) <= third.hard_max_bytes
    assert {:ok, fourth} = read.(Keyword.put(opts, :cursor, third.cursor))
    assert [%{key: "bc"}] = fourth.rows
  end

  test "invalid limits return errors before reading storage", %{ctx: ctx} do
    for {key, maximum} <- [
          max_key_bytes: 4096,
          max_value_bytes: 524_288,
          target_bytes: 1_048_576,
          limit: 200
        ],
        value <- [0, -1, maximum + 1, :invalid] do
      opts = [{key, value}]
      expected = {:error, {:invalid, "#{key} must be in 1..#{maximum}"}}

      assert Read.page(ctx, Read.deadline(1000), opts, fn _, _ -> flunk("decoded") end) ==
               expected

      assert Read.get(ctx, Read.deadline(1000), "key", opts, fn _, _ -> flunk("decoded") end) ==
               expected
    end
  end

  test "a lookahead page reports the first key after its rows", %{ctx: ctx} do
    assert {:ok, _} =
             SlateDB.write(ctx.db, for(key <- ["a", "b", "c", "d"], do: {:put, key, "v"}),
               await_durable: true
             )

    read = fn opts ->
      Inspection.request(TestCluster, 0, 1, 1000, fn ctx, deadline ->
        Read.page(ctx, deadline, opts, fn _, _ -> :raw end)
      end)
    end

    assert {:ok, %{rows: [%{key: "a"}, %{key: "b"}], stopped: :end, next_key: "c"}} =
             read.(gte: "a", lt: "c", lookahead_lt: "z")

    assert {:ok, %{rows: [%{key: "a"}], stopped: :limit, next_key: "b"}} =
             read.(gte: "a", lt: "c", lookahead_lt: "z", limit: 1)

    assert {:ok, %{rows: [%{key: "c"}, %{key: "d"}], stopped: :limit, next_key: nil}} =
             read.(gte: "c", lookahead_lt: "z", limit: 2)

    assert {:ok, %{stopped: :end, next_key: "c"}} = read.(gte: "a", lt: "c", lookahead_lt: "d")

    assert {:ok, page} = read.(gte: "a", lt: "c")
    refute Map.has_key?(page, :next_key)
  end

  test "a lookahead page stops at a cut key only when its preview is past the bound", %{
    ctx: ctx
  } do
    assert {:ok, _} =
             SlateDB.write(ctx.db, [{:put, "ka1", "v"}, {:put, "kb2", "v"}], await_durable: true)

    read = fn opts ->
      Inspection.request(TestCluster, 0, 1, 1000, fn ctx, deadline ->
        Read.page(ctx, deadline, [max_key_bytes: 2, lookahead_lt: "z"] ++ opts, fn _, _ ->
          :raw
        end)
      end)
    end

    # "kb" sorts after "ka9", so "kb2" is past the bound.
    assert {:ok, %{rows: [%{key: "ka", oversized_key: true}]}} = read.(gte: "k", lt: "ka9")
    assert {:ok, %{rows: [], stopped: :end, next_key: "kb"}} = read.(gte: "kb", lt: "ka9")
    # "ka" cannot be ordered against "ka9", so it stays a row.
    assert {:ok, %{rows: [%{key: "ka"}]}} = read.(gte: "ka", lt: "ka9")
  end

  test "a row built from a fetched value matches a ranged get", %{ctx: ctx} do
    decode = fn _key, value -> byte_size(value) end
    ranges = [nil, {0, 10}, {100, 50}, {1990, 100}, {2000, 10}, {5000, 10}, {0, 4096}]

    for {key, size, extra} <- [
          {"small", 2000, []},
          {"large", 600_000,
           [
             {524_000, 288},
             {524_280, 100},
             {550_000, 10},
             {550_000, 0},
             {599_990, 100},
             {700_000, 10}
           ]}
        ] do
      value = :binary.list_to_bin(Enum.map(0..(size - 1), &rem(&1, 251)))
      assert {:ok, _} = SlateDB.put(ctx.db, key, value, await_durable: true)
      assert {:ok, fetched} = Read.fetch(ctx, Read.deadline(1000), key)

      for range <- ranges ++ extra, max <- [nil, 1, 1000] do
        opts = Enum.reject([value_range: range, max_value_bytes: max], &is_nil(elem(&1, 1)))
        expected = Read.get(ctx, Read.deadline(1000), key, opts, decode)

        {offset, length} = range || {0, size}
        end_byte = min(offset + min(length, max || 524_288), size)

        if end_byte > 524_288 and end_byte > offset do
          assert Read.row(ctx, key, fetched, opts, decode) == {:error, :oversized_value},
                 "#{key}: not rejected for #{inspect(opts)}"
        else
          assert Read.row(ctx, key, fetched, opts, decode) == expected,
                 "#{key}: differs for #{inspect(opts)}"
        end
      end
    end
  end

  test "a named caller is distinguished from occupied admission" do
    Process.register(self(), __MODULE__.NamedCaller)

    try do
      assert {:error, :registered_caller} =
               Inspection.exclusive(1, 1000, fn _ -> flunk("named caller admitted") end)

      assert {:error, :registered_caller} =
               Inspection.request(TestCluster, 0, 1, 1000, fn _, _ ->
                 flunk("named caller read")
               end)

      assert Process.whereis(__MODULE__.NamedCaller) == self()
      assert Process.whereis(Inspection) == nil
    after
      Process.unregister(__MODULE__.NamedCaller)
    end

    assert :ok = Inspection.exclusive(1, 1000, fn _ -> :ok end)
  end

  test "a deadline reached between rows returns a partial page", %{ctx: ctx} do
    assert {:ok, _} =
             SlateDB.write(ctx.db, [{:put, "a", "first"}, {:put, "b", "second"}],
               await_durable: true
             )

    parent = self()

    reader =
      Task.async(fn ->
        deadline = Read.deadline(1000)

        Read.page(ctx, deadline, [], fn key, _ ->
          send(parent, {:decoding, self(), key, deadline})

          receive do
            :release -> :raw
          end
        end)
      end)

    assert_receive {:decoding, pid, "a", deadline}
    delay = max(deadline - System.monotonic_time(:millisecond) + 1, 1)
    Process.send_after(pid, :release, delay)

    assert {:ok, %{rows: [%{key: "a"}], cursor: cursor, stopped: :deadline}} =
             Task.await(reader, 5000)

    assert cursor > "a"
    refute_receive {:decoding, _, "b", _}
  end

  test "prefix pages advance with full-key cursors and respect suffix bounds", %{ctx: ctx} do
    for key <- ["a", "aa", "ab", "ac", "b", <<255>>, <<255, 0>>, <<255, 1>>] do
      assert {:ok, _} = SlateDB.put(ctx.db, key, "value", await_durable: true)
    end

    for {opts, expected} <- [
          {[prefix: "a", gte: "a", lt: "c", cursor: "a"], ["aa", "ab"]},
          {[prefix: <<255>>], [<<255>>, <<255, 0>>, <<255, 1>>]},
          {[prefix: <<>>, gte: "b"], ["b", <<255>>, <<255, 0>>, <<255, 1>>]},
          {[prefix: "a", cursor: "b"], []}
        ] do
      {keys, {cursor, _exclusive}} =
        Enum.map_reduce(1..(length(expected) + 1), {nil, false}, fn _, {cursor, exclusive} ->
          opts = Keyword.merge(opts, limit: 1, max_key_bytes: 2)

          opts =
            if cursor,
              do: Keyword.merge(opts, cursor: cursor, cursor_exclusive: exclusive),
              else: opts

          assert {:ok, page} =
                   Inspection.request(TestCluster, 0, 1, 1000, fn ctx, deadline ->
                     Read.page(ctx, deadline, opts, fn _, _ -> :raw end)
                   end)

          {Enum.map(page.rows, & &1.key), {page.cursor, page.cursor_exclusive}}
        end)

      assert List.flatten(keys) == expected
      assert cursor == nil
    end
  end

  test "expired calls do not read storage and unsupported calls do not take admission", %{
    ctx: ctx
  } do
    assert {:ok, %{rows: [], stopped: :deadline}} =
             Read.page(ctx, System.monotonic_time(:millisecond) - 1, [], fn _, _ ->
               flunk("decoded")
             end)

    assert {:ok, %{cursor: "a", cursor_exclusive: false, stopped: :deadline}} =
             Read.page(
               ctx,
               System.monotonic_time(:millisecond) - 1,
               [prefix: "a", cursor_exclusive: true],
               fn _, _ -> flunk("decoded") end
             )

    assert {:error, {:unsupported_api, 1}} =
             Inspection.request(TestCluster, 0, 2, 1000, fn _, _ -> flunk("read") end)
  end
end
