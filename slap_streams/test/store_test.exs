defmodule Slap.Streams.StoreTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Slap.SlateDB
  alias Slap.Streams.Offset
  alias Slap.Streams.Store.{Batch, Codec, ForkOf, Keys, Meta, Producer, Read, Tail}

  describe "keys" do
    property "sort like the tuples they encode" do
      encode = fn
        {:msg, {s, o, p}} -> Keys.msg(s, o, p)
        {:tail, {s}} -> Keys.tail(s)
        {:expiry, {d, s}} -> Keys.expiry(d, s)
        {:producer, {s, id}} -> Keys.producer(s, id)
      end

      order = %{tail: 2, producer: 3, msg: 4, expiry: 5}

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
  end

  test "codec round-trips and ignores unknown fields" do
    meta = %Meta{sid: 7, content_type: "text/plain", closed: true, closed_by: {"p", 1, 2}}

    for value <- [
          meta,
          %Tail{next_offset: 99, last_access_ms: 5},
          %Producer{epoch: 3, last_seq: 4}
        ] do
      assert Codec.decode(Codec.encode(value)) == value
    end

    future = :erlang.term_to_binary({:tail, 1, %{next_offset: 1, new_field: :x}})
    assert Codec.decode(future) == %Tail{next_offset: 1}
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
      forks: ["/f"],
      soft_deleted: true,
      fork_of: %ForkOf{
        path: "/s",
        offset: 4,
        requested_offset: 2,
        sub_offset: 1,
        requested_content_type: "text/plain",
        requested_ttl_s: 5,
        requested_expires_at_ms: 6
      }
    }

    values = [meta, %Tail{next_offset: 1, expiry_key_ms: 2}, %Producer{epoch: 1}]
    paths = Enum.flat_map(:code.get_path(), &[~c"-pa", &1])
    {:ok, peer, _node} = :peer.start_link(%{connection: :standard_io, args: paths})

    for value <- values do
      assert :peer.call(peer, Codec, :decode, [Codec.encode(value)]) == value
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
      # From an offset in the middle, and bounded by `until`.
      assert Read.read_msgs(db, 3, 5, tail, 10_000_000) == {tl(messages), tail}
      assert Read.read_msgs(db, 3, 0, 5, 10_000_000) == {[{0, "a"}], 5}
      # max_bytes stops after the message that reaches it.
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

  # 64-bit values: :binary.decode_unsigned of 8 random bytes, since
  # integer/1 keeps to small values.
  defp u64, do: one_of([integer(0..1000), map(binary(length: 8), &:binary.decode_unsigned/1)])

  defp key do
    max = 0xFFFF_FFFF_FFFF_FFFF

    one_of([
      tuple({constant(:msg), tuple({member_of([0, 1, 7, max]), u64(), integer(0..0xFFFF)})}),
      tuple({constant(:tail), tuple({u64()})}),
      tuple({constant(:expiry), tuple({u64(), u64()})}),
      tuple({constant(:producer), tuple({member_of([1, 2]), binary(max_length: 8)})})
    ])
  end
end
