defmodule Slap.Streams.StoreTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Slap.SlateDB
  alias Slap.Streams.Offset

  alias Slap.Streams.Creation.Target

  alias Slap.Streams.Store.{
    Batch,
    Capture,
    Codec,
    CopyCreation,
    CopyProgress,
    Identity,
    Keys,
    Meta,
    Producer,
    PublishedCopy,
    PublishedReference,
    Read,
    ReferenceCreation,
    Tail
  }

  describe "keys" do
    property "sort like the tuples they encode" do
      encode = fn
        {:msg, {s, o, p}} -> Keys.msg(s, o, p)
        {:tail, {s}} -> Keys.tail(s)
        {:expiry, {d, s}} -> Keys.expiry(d, s)
        {:producer, {s, id}} -> Keys.producer(s, id)
        {:registration, {s, shard, target}} -> Keys.registration(s, %{shard: shard, sid: target})
        {:pin, {s, start, shard, target}} -> Keys.pin(s, start, %{shard: shard, sid: target})
      end

      order = %{tail: 2, producer: 3, msg: 4, expiry: 5, registration: 13, pin: 14}

      check all({ta, a} <- key(), {tb, b} <- key(), max_runs: 10_000) do
        expected = {order[ta], a} <= {order[tb], b}
        assert encode.({ta, a}) <= encode.({tb, b}) == expected
      end
    end

    test "message ranges and decoding" do
      assert Keys.decode_msg(Keys.msg(3, 1234, 2)) == {1234, 2}
      assert Keys.producer_id(Keys.producer(9, "p-1")) == "p-1"
      [gte: lo, lt: hi] = Keys.msg_range(3, 10, 20)
      assert lo <= Keys.msg(3, 10, 5) and Keys.msg(3, 19, 999) < hi and Keys.msg(3, 20, 0) >= hi
    end

    test "row ownership covers stream records and only explicitly shared records" do
      sid = 7
      target = %Identity{path: "/target", shard: 2, sid: 8}

      for key <- [
            Keys.tail(sid),
            Keys.producer(sid, "p"),
            Keys.msg(sid, 10, 0),
            Keys.expiry(100, sid),
            Keys.delete_pending(sid),
            Keys.trim(sid),
            Keys.repair(sid),
            Keys.reclaim(sid),
            Keys.witness(sid),
            Keys.registration(sid, target),
            Keys.pin(sid, 10, target)
          ] do
        assert Read.row_owner({key, ""}) == {:stream, sid}
      end

      assert Read.row_owner({Keys.meta("/source"), Codec.encode(%Meta{sid: sid})}) ==
               {:stream, sid}

      for key <- [Keys.next_sid(), Keys.seal("/group")],
          do: assert(Read.row_owner({key, ""}) == :shared)

      assert_raise CaseClauseError, fn ->
        Read.row_owner({Keys.meta("/invalid"), Codec.encode(%Meta{})})
      end

      for key <- [
            <<>>,
            <<0x00, "unknown">>,
            <<0x0F, sid::64>>,
            Keys.tail(sid) <> <<0>>,
            <<0x04, sid::64>>,
            <<0x05, sid::64>>,
            <<0x0D, sid::64>>,
            <<0x0E, sid::64>>
          ] do
        assert_raise ArgumentError, ~r/unrecognized stream key/, fn ->
          Read.row_owner({key, ""})
        end
      end
    end
  end

  test "codec round-trips stored records" do
    meta = %Meta{sid: 7, content_type: "text/plain", closed: true, closed_by: {"p", 1, 2}}

    for {tag, version, value} <- [
          {:meta, 2, meta},
          {:tail, 1, %Tail{next_offset: 99, last_access_ms: 5}},
          {:producer, 1, %Producer{epoch: 3, last_seq: 4}}
        ] do
      encoded = Codec.encode(value)
      assert {^tag, ^version, _fields} = :erlang.binary_to_term(encoded, [:safe])
      assert Codec.decode(encoded) == value
    end
  end

  test "codec reconstructs tagged creation records with defaulted fields" do
    source = {:identity, %{path: "/source", shard: 1, sid: 7}}
    target = {:identity, %{path: "/target", shard: 2, sid: 8}}

    capture =
      {:capture,
       %{
         source: source,
         target: target,
         start: 0,
         offset: 9,
         depth: 1,
         content_type: "application/json",
         closed: false
       }}

    pending = %{
      source: source,
      target: target,
      request: Target.request(%{forked_from: "/source", body: "[4]"})
    }

    variants = [
      {:reference_pending, ReferenceCreation, pending},
      {:copy_pending, CopyCreation, pending},
      {:copy_pending, CopyCreation,
       Map.put(
         pending,
         :progress,
         {:copy_progress, %{capture: capture, cursor: 5, progressed_ms: 0}}
       )},
      {:reference, PublishedReference,
       %{capture: capture, source_placement_key: "route", requested_offset: 0, sub_offset: 2}},
      {:copy, PublishedCopy,
       %{
         source: source,
         target: target,
         source_placement_key: "route",
         requested_offset: nil,
         offset: 9
       }}
    ]

    for {tag, module, fields} <- variants do
      encoded = :erlang.term_to_binary({:meta, 2, %{sid: 8, creation: {tag, fields}}})
      decoded = Codec.decode(encoded)
      assert decoded.creation.__struct__ == module
      assert decoded.reclaimed_before == 0
      assert Codec.decode(Codec.encode(decoded)) == decoded
      assert_plain_term(:erlang.binary_to_term(Codec.encode(decoded), [:safe]))
    end

    {:capture, fields} = capture
    encoded = :erlang.term_to_binary({:capture, 1, fields})
    assert %Capture{} = cap = Codec.decode(encoded)
    assert cap.source == %Identity{path: "/source", shard: 1, sid: 7}
    assert cap.target == %Identity{path: "/target", shard: 2, sid: 8}
    assert cap.prefix == nil
    assert cap.ttl_s == nil
    assert cap.expires_at_ms == nil

    encoded = :erlang.term_to_binary({:meta, 2, %{creation: {:unknown_creation, %{}}}})
    assert_raise KeyError, fn -> Codec.decode(encoded) end
  end

  # A node that takes over a shard may decode a stream's rows before it has
  # loaded any module that names their fields, and `:safe` decoding rejects
  # atoms the node does not have yet.
  test "codec decodes on a node that has loaded nothing else" do
    meta = %Meta{
      sid: 7,
      closed: true,
      closed_by: {"p", 1, 2},
      last_stream_seq: "5",
      soft_deleted: true
    }

    source = %Identity{path: "/s", shard: 1, sid: 7}
    target = %Identity{path: "/t", shard: 2, sid: 8}

    cap = %Capture{
      source: source,
      target: target,
      start: 0,
      offset: 9,
      prefix: "hello",
      depth: 2,
      content_type: "application/octet-stream",
      closed: false
    }

    request = Target.request(%{forked_from: "/s", body: "initial"})

    creation = %ReferenceCreation{
      source: source,
      target: target,
      request: request
    }

    registered = %{
      meta
      | registration_count: 1,
        pin_start: cap.start,
        creation: %PublishedReference{
          capture: %{cap | prefix: nil},
          source_placement_key: "route",
          requested_offset: 0,
          sub_offset: 3
        }
    }

    pending = %{meta | creation: struct!(CopyCreation, Map.from_struct(creation))}

    published = %{
      meta
      | creation: %PublishedCopy{
          source: source,
          target: target,
          source_placement_key: "route",
          requested_offset: nil,
          offset: 9
        }
    }

    values = [
      meta,
      registered,
      pending,
      %{meta | creation: creation},
      %{
        pending
        | creation: %{
            pending.creation
            | progress: %CopyProgress{capture: cap, cursor: cap.offset, progressed_ms: 0}
          }
      },
      published,
      cap,
      %Tail{next_offset: 1, expiry_key_ms: 2},
      %Producer{epoch: 1}
    ]

    paths = Enum.flat_map(:code.get_path(), &[~c"-pa", &1])
    {:ok, peer, _node} = :peer.start_link(%{connection: :standard_io, args: paths})

    for value <- values do
      encoded = Codec.encode(value)
      assert_plain_term(:erlang.binary_to_term(encoded, [:safe]))

      assert :peer.call(peer, Codec, :decode, [encoded]) == value
    end

    :peer.stop(peer)
  end

  describe "offsets" do
    test "match the official format" do
      assert Offset.encode(0) == "0000000000000000_0000000000000000"
      assert Offset.encode(1234) == "0000000000000000_0000000000001234"
      assert Offset.parse(Offset.encode(1234)) == {:ok, 1234}
      assert Offset.parse("-1") == {:ok, :start}
      assert Offset.parse(nil) == {:ok, :start}
      assert Offset.parse("now") == {:ok, :now}
      assert Offset.advance(10, 5) == 19
    end

    test "reject anything else" do
      for bad <- [
            "abc",
            "1",
            "0000000000000000_000000000000000x",
            "0000000000000001_0000000000000000",
            "0000000000000000_00000000000000001",
            " 0000000000000000_0000000000000000"
          ] do
        assert Offset.parse(bad) == {:error, :bad_offset}, bad
      end
    end

    property "sort lexicographically in offset order" do
      offset = one_of([integer(0..1000), map(binary(length: 5), &:binary.decode_unsigned/1)])

      check all(a <- offset, b <- offset, max_runs: 1_000) do
        assert Offset.encode(a) <= Offset.encode(b) == a <= b
      end
    end
  end

  describe "reading back from SlateDB" do
    setup do
      {:ok, db} = SlateDB.open("store", store: :memory)
      on_exit(fn -> SlateDB.close(db) end)
      %{db: db}
    end

    test "a created stream, its messages, tail and producers", %{db: db} do
      big = :crypto.strong_rand_bytes(600_000)
      messages = [{0, "a"}, {5, big}, {5 + 4 + 600_000, "c"}]
      tail = Offset.advance(5 + 4 + 600_000, 1)
      meta = %Meta{sid: 3, content_type: "text/plain"}

      {:ok, _} =
        SlateDB.write(db, Batch.create("/s", meta, %Tail{next_offset: tail}, messages))

      {:ok, _} =
        SlateDB.write(
          db,
          Batch.append(3, [], %Tail{next_offset: tail}, producers: [{"p", %Producer{epoch: 1}}])
        )

      assert Read.get_meta(db, "/s") == {:ok, meta}
      assert Read.get_meta(db, "/other") == {:ok, nil}
      assert {:ok, %Tail{next_offset: ^tail}} = Read.get_tail(db, 3)
      assert Read.list_producers(db, 3) == %{"p" => %Producer{epoch: 1}}

      assert Read.read_msgs(db, 3, 0, tail, 10_000_000) == {messages, tail}
      assert Read.read_msgs(db, 3, 5, tail, 10_000_000) == {tl(messages), tail}
      assert Read.read_msgs(db, 3, 0, 5, 10_000_000) == {[{0, "a"}], 5}
      assert Read.read_msgs(db, 3, 0, tail, 1) == {[{0, "a"}], 5}
      assert Read.read_msgs(db, 3, 0, tail, 2) == {Enum.take(messages, 2), 5 + 4 + 600_000}
      assert Read.read_msgs(db, 3, tail, tail, 10) == {[], tail}
    end

    test "a logical delete removes the metadata", %{db: db} do
      {:ok, _} = SlateDB.write(db, Batch.create("/s", %Meta{sid: 1}, %Tail{}, []))
      {:ok, _} = SlateDB.write(db, Batch.logical_delete("/s", 1))
      assert Read.get_meta(db, "/s") == {:ok, nil}
      assert {:ok, <<0::64>>} = SlateDB.get(db, Keys.delete_pending(1))
    end

    test "stream id reservations", %{db: db} do
      assert Read.next_sid(db) == {:ok, 1}
      {:ok, _} = SlateDB.write(db, Batch.reserve_sids(1001))
      assert Read.next_sid(db) == {:ok, 1001}
    end
  end

  defp assert_plain_term(fields) when is_map(fields) do
    refute Map.has_key?(fields, :__struct__)

    Enum.each(fields, fn {key, value} ->
      assert_plain_term(key)
      assert_plain_term(value)
    end)
  end

  defp assert_plain_term(tuple) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> Enum.each(&assert_plain_term/1)

  defp assert_plain_term(value) when is_atom(value),
    do: refute(String.starts_with?(Atom.to_string(value), "Elixir."))

  defp assert_plain_term(_value), do: :ok

  # 64-bit values: :binary.decode_unsigned of 8 random bytes, since
  # integer/1 keeps to small values.
  defp u64, do: one_of([integer(0..1000), map(binary(length: 8), &:binary.decode_unsigned/1)])

  defp key do
    max = 0xFFFF_FFFF_FFFF_FFFF

    one_of([
      tuple({constant(:msg), tuple({member_of([0, 1, 7, max]), u64(), integer(0..0xFFFF)})}),
      tuple({constant(:tail), tuple({u64()})}),
      tuple({constant(:expiry), tuple({u64(), u64()})}),
      tuple({constant(:producer), tuple({member_of([1, 2]), binary(max_length: 8)})}),
      tuple({constant(:registration), tuple({u64(), u64(), u64()})}),
      tuple({constant(:pin), tuple({u64(), u64(), u64(), u64()})})
    ])
  end
end
