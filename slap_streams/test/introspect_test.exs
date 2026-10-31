defmodule Slap.Streams.IntrospectTest do
  # ClusterCase uses the named Streams cluster; admission is node-wide.
  use ExUnit.Case, async: false
  use Slap.Streams.Test.ClusterCase

  alias Slap.Cluster.Inspection.Read
  alias Slap.SlateDB
  alias Slap.Streams.Introspect
  alias Slap.Streams.Store.{Codec, Keys, Meta, Tail}

  test "inspection decodes durable records without starting a stream server" do
    path = "/inspection"
    assert {:ok, _, _} = Streams.create(path, body: "hello")
    ctx = ctx_for(path)
    pid = server(path)
    ref = Process.monitor(pid)
    GenServer.stop(pid, :normal, 5000)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

    assert {:ok, %{row: %{decoded: {:ok, %{value: %{sid: sid}}}}}} =
             Introspect.stream(Streams.Cluster, ctx.n, 1, 5000, path: path)

    assert {:ok, %{rows: [%{value: "hello", decoded: {:ok, %{sid: ^sid, offset: 0, part: 0}}}]}} =
             Introspect.messages(Streams.Cluster, ctx.n, 1, 5000, sid: sid)

    assert server(path) == nil
  end

  test "logical plans use stored metadata when the displayed row is truncated" do
    source = "/inspection/limited-source"
    target = "/inspection/limited-target"
    assert {:ok, _, _} = Streams.create(source, body: "inherited")
    assert {:ok, _, _} = Streams.create(target, forked_from: source)
    ctx = ctx_for(target)

    assert {:ok, %{row: row, logical: logical}} =
             Introspect.logical(Streams.Cluster, ctx.n, 1, 5000, path: target, max_value_bytes: 4)

    # The row has no decoded fields to plan from.
    assert %{truncated: true, diagnostic: :truncated_value} = row
    refute Map.has_key?(row, :decoded)

    assert %{read_plan: %{kind: :source, opts: opts}} = logical
    assert opts[:path] == source
  end

  test "logical inspection projects references and validates opaque continuation tokens" do
    source = "/inspection/source"
    target = "/inspection/target"
    assert {:ok, _, _} = Streams.create(source, body: "inherited")
    assert {:ok, _, _} = Streams.create(target, forked_from: source)
    assert {:ok, _} = Streams.append(target, "own")
    ctx = ctx_for(target)

    assert {:ok, %{logical: logical}} =
             Introspect.logical(Streams.Cluster, ctx.n, 1, 5000, path: target)

    assert logical.creation == :reference
    assert %{source: %{path: ^source}, target: %{path: ^target}} = logical.reference
    assert is_binary(logical.continuation)
    assert %{kind: :source, shard: source_shard, opts: source_opts} = logical.read_plan
    assert source_shard == Streams.Cluster.shard_for(Streams.placement_key(source))
    assert source_opts[:path] == source
    assert source_opts[:expected_sid] == logical.reference.source.sid
    assert source_opts[:from] == 0
    assert source_opts[:until] == logical.reference.offset
    assert source_opts[:retained]
    assert source_opts[:reference_budget] == 0

    assert {:ok, %{logical: %{read_plan: %{kind: :messages, opts: own_opts}, continuation: nil}}} =
             Introspect.logical(Streams.Cluster, ctx.n, 1, 5000,
               path: target,
               cursor: logical.continuation
             )

    assert own_opts[:sid] == logical.sid
    assert own_opts[:from] == logical.reference.offset
    assert own_opts[:logical_cursor]

    assert {:ok, %{rows: [%{value: "own"}], cursor: nil}} =
             Introspect.messages(Streams.Cluster, ctx.n, 1, 5000,
               sid: logical.sid,
               from: 0,
               logical_cursor: true,
               cursor: logical.continuation
             )

    assert {:error, :invalid_logical_cursor} =
             Introspect.logical(Streams.Cluster, ctx.n, 1, 5000, path: target, cursor: "invalid")
  end

  test "message fragments carry validation and accounting across pagination" do
    path = "/inspection/fragments"
    body = String.duplicate("x", 512 * 1024) <> "tail"
    assert {:ok, _, _} = Streams.create(path, body: body)
    ctx = ctx_for(path)
    {:ok, %{sid: sid}} = Streams.metadata_fenced(path)

    assert {:ok, first} = Introspect.messages(Streams.Cluster, ctx.n, 1, 5000, sid: sid, limit: 1)
    assert first.fragment_contract == 1
    assert [%{status: :more, before: nil, after: first_token} = start] = first.fragments

    assert {:ok, middle} =
             Introspect.messages(Streams.Cluster, ctx.n, 1, 5000,
               sid: sid,
               limit: 1,
               cursor: first.cursor
             )

    assert [%{status: :more, before: ^first_token, after: middle_token} = continuation] =
             middle.fragments

    assert {:ok, last} =
             Introspect.messages(Streams.Cluster, ctx.n, 1, 5000,
               sid: sid,
               limit: 1,
               cursor: middle.cursor
             )

    assert [%{status: :complete, before: ^middle_token, after: nil} = finish] = last.fragments
    assert last.cursor == nil
    assert finish.next_offset == byte_size(body) + 4

    assert start.encoded_bytes + continuation.encoded_bytes + finish.encoded_bytes ==
             byte_size(body) + 4

    for page <- [first, middle, last],
        do: assert(:erlang.external_size(page) <= page.hard_max_bytes)
  end

  test "a missing part between pages invalidates the final fragment" do
    path = "/inspection/missing-fragment"
    assert {:ok, _, _} = Streams.create(path, body: String.duplicate("x", 512 * 1024 + 1))
    ctx = ctx_for(path)
    {:ok, %{sid: sid}} = Streams.metadata_fenced(path)
    assert {:ok, _} = SlateDB.delete(ctx.db, Keys.msg(sid, 0, 1), await_durable: true)

    assert {:ok, first} = Introspect.messages(Streams.Cluster, ctx.n, 1, 5000, sid: sid, limit: 1)
    assert [%{status: :more}] = first.fragments

    assert {:ok, last} =
             Introspect.messages(Streams.Cluster, ctx.n, 1, 5000,
               sid: sid,
               limit: 1,
               cursor: first.cursor
             )

    assert [%{status: :invalid}] = last.fragments
  end

  test "oversized message keys retain diagnostics and resume after the skipped prefix" do
    path = "/inspection/oversized-message-keys"
    assert {:ok, _, _} = Streams.create(path, body: "first")
    assert {:ok, _} = Streams.append(path, "second")
    ctx = ctx_for(path)
    {:ok, %{sid: sid}} = Streams.metadata_fenced(path)

    for maximum <- [1, 8, 9, 10, 16, 17, 18] do
      assert {:ok, page} =
               Introspect.messages(Streams.Cluster, ctx.n, 1, 5000,
                 sid: sid,
                 max_key_bytes: maximum
               )

      assert [%{oversized_key: true, diagnostic: :oversized_keys_with_prefix_skipped}] = page.rows
      assert [%{status: :invalid}] = page.fragments
      assert :erlang.external_size(page) <= page.hard_max_bytes

      if page.cursor do
        assert {:ok, %{logical: logical}} =
                 Introspect.logical(Streams.Cluster, ctx.n, 1, 5000,
                   path: path,
                   cursor: page.cursor
                 )

        assert {:ok, resumed} =
                 Introspect.messages(Streams.Cluster, ctx.n, 1, 5000,
                   sid: sid,
                   from: logical.from,
                   logical_cursor: true,
                   cursor: page.cursor
                 )

        assert Enum.map(resumed.rows, & &1.value) == if(maximum >= 17, do: ["second"], else: [])
        assert resumed.cursor == nil
      end
    end
  end

  test "malformed message keys retain diagnostics across logical cursor pagination" do
    path = "/inspection/malformed-message-keys"
    assert {:ok, _, _} = Streams.create(path, body: "first")
    assert {:ok, _} = Streams.append(path, "second")
    ctx = ctx_for(path)
    {:ok, %{sid: sid}} = Streams.metadata_fenced(path)
    short = <<4, sid::64, 3::64>>
    extended = Keys.msg(sid, 3, 1) <> <<7>>
    malformed = [short, short <> <<0>>, extended]

    assert {:ok, _} =
             SlateDB.write(ctx.db, Enum.map(malformed, &{:put, &1, "bad"}), await_durable: true)

    cursor =
      Enum.reduce(malformed, short, fn key, cursor ->
        assert {:ok, %{logical: logical}} =
                 Introspect.logical(Streams.Cluster, ctx.n, 1, 5000,
                   path: path,
                   cursor: if(cursor == short, do: nil, else: cursor)
                 )

        assert {:ok, page} =
                 Introspect.messages(Streams.Cluster, ctx.n, 1, 5000,
                   sid: sid,
                   from: logical.from,
                   cursor: cursor,
                   max_key_bytes: 20,
                   limit: 1
                 )

        assert [%{key: ^key, decoded: {:error, :unknown_or_malformed_key}}] = page.rows
        assert [%{status: :invalid}] = page.fragments
        assert :erlang.external_size(page) <= page.hard_max_bytes
        page.cursor
      end)

    assert {:ok, %{rows: [%{value: "second"}], cursor: nil}} =
             Introspect.messages(Streams.Cluster, ctx.n, 1, 5000, sid: sid, cursor: cursor)
  end

  test "malformed, legacy and compressed records retain bytes and diagnostics" do
    for key <- [<<>>, <<4, 1>>, <<11, 0>>, <<13, 1>>] do
      assert {:error, :unknown_or_malformed_key} = Introspect.decode(key, "bad")
    end

    legacy = :erlang.term_to_binary({:meta, 1, %{sid: 1}})

    assert {:error, :metadata_v1_requires_offline_upgrade} =
             Introspect.decode(Keys.meta("old"), legacy)

    future = :erlang.term_to_binary({:meta, 3, %{sid: 1}})

    assert {:error, {:unsupported_version, 3}} =
             Introspect.decode(Keys.meta("future"), future)

    compressed =
      :erlang.term_to_binary({:meta, 2, %{sid: 1, content_type: String.duplicate("a", 1000)}},
        compressed: 9
      )

    assert {:error, :compressed_etf} =
             Introspect.decode(Keys.meta("compressed"), compressed)

    ctx = ctx_for("/bad")

    assert {:ok, _} = SlateDB.put(ctx.db, Keys.meta("/bad"), "bad", await_durable: true)
    assert {:ok, page} = Introspect.list(Streams.Cluster, ctx.n, 1, 5000, [])
    assert Enum.any?(page.rows, &match?(%{value: "bad", decoded: {:error, :malformed_etf}}, &1))

    assert {:ok, %{kind: :meta, value: %{sid: 8}}} =
             Introspect.decode(Keys.meta("new"), Codec.encode(%Meta{sid: 8}))
  end

  test "stream views copy at most the displayed bytes of an oversized record" do
    path = "/inspection/oversized-meta"
    ctx = ctx_for(path)
    record = :binary.copy("x", 600_000)
    assert {:ok, _} = SlateDB.put(ctx.db, Keys.meta(path), record, await_durable: true)

    assert {:ok, %{row: row, records: %{diagnostic: :metadata_unavailable}}} =
             Introspect.stream(Streams.Cluster, ctx.n, 1, 5000, path: path, max_value_bytes: 1000)

    assert %{value_size: 600_000, truncated: true} = row
    assert row.value == binary_part(record, 0, 1000)
    assert :binary.referenced_byte_size(row.value) == 1000

    assert {:error, :oversized_value} =
             Introspect.logical(Streams.Cluster, ctx.n, 1, 5000, path: path)
  end

  test "whole-message reads join parts across native batches and stop at the deadline" do
    ctx = ctx_for("/inspection/batched")
    sid = 1_000_000
    # A scan batch holds 256 rows: the three parts of the last message
    # straddle the first batch boundary.
    {messages, tail} =
      Enum.map_reduce(0..254, 0, fn n, offset ->
        parts = if n == 254, do: ["xxx", "yyy", "zzz"], else: ["m#{n}"]
        {{offset, parts}, offset + 4 + IO.iodata_length(parts)}
      end)

    rows =
      for {offset, parts} <- messages,
          {part, n} <- Enum.with_index(parts),
          do: {:put, Keys.msg(sid, offset, n), part}

    assert {:ok, _} = SlateDB.write(ctx.db, rows, await_durable: true)
    expected = for {offset, parts} <- messages, do: {offset, IO.iodata_to_binary(parts)}

    assert {:ok, ^expected} = Introspect.read_messages(ctx, Read.deadline(5000), sid, 0, tail)

    assert {:ok, []} = Introspect.read_messages(ctx, Read.deadline(5000), sid, tail, tail)
    expired = System.monotonic_time(:millisecond)
    assert {:error, :deadline} = Introspect.read_messages(ctx, expired, sid, 0, tail)
  end

  test "stream and logical views report corrupt and misplaced metadata" do
    records = [
      {:erlang.term_to_binary({:meta, 2, nil}), {:error, :malformed_record}},
      {:erlang.term_to_binary({:meta, 2, %{unknown: 1}}), {:error, :malformed_record}},
      {:erlang.term_to_binary({:meta, 2, %{creation: {:unknown, %{}}}}),
       {:error, :malformed_record}},
      {Codec.encode(%Tail{next_offset: 1}), {:error, :unexpected_record}}
    ]

    for {{record, error}, n} <- Enum.with_index(records) do
      path = "/inspection/corrupt-meta-#{n}"
      ctx = ctx_for(path)
      assert {:ok, _} = SlateDB.put(ctx.db, Keys.meta(path), record, await_durable: true)
      assert ^error = Introspect.decode(Keys.meta(path), record)

      assert {:ok, %{row: %{decoded: ^error}, records: %{diagnostic: :metadata_unavailable}}} =
               Introspect.stream(Streams.Cluster, ctx.n, 1, 5000, path: path)

      assert ^error = Introspect.logical(Streams.Cluster, ctx.n, 1, 5000, path: path)
    end
  end
end
