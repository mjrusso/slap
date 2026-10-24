defmodule Slap.Streams.UpgradeTest do
  # The cluster has a node-wide name and is stopped during offline conversion.
  use Slap.Streams.Test.ClusterCase, async: false

  alias Slap.Cluster.Config
  alias Slap.SlateDB
  alias Slap.Streams.Jobs.Deleter
  alias Slap.Streams.Store.{Batch, Codec, Keys, Meta, Producer, Read, Tail}
  alias Slap.Streams.Upgrade

  @moduletag :capture_log
  @old_copy Base.decode64!(
              "g2gDdwRtZXRhYQF0AAAADHcGY2xvc2VkdwR0cnVldwNzaWRhAncMY29udGVudF90eXBlbQAAAAp0" <>
                "ZXh0L3BsYWludwpjcmVhdGVkX21zYQF3B2Zvcmtfb2Z0AAAAB3cGb2Zmc2V0YQp3BHBhdGhtAAAA" <>
                "By9zb3VyY2V3EHJlcXVlc3RlZF9vZmZzZXRhCncWcmVxdWVzdGVkX2NvbnRlbnRfdHlwZW0AAAAK" <>
                "dGV4dC9wbGFpbncPcmVxdWVzdGVkX3R0bF9zdwNuaWx3F3JlcXVlc3RlZF9leHBpcmVzX2F0X21z" <>
                "dwNuaWx3CnN1Yl9vZmZzZXRhAHcFdHRsX3N3A25pbHcNZXhwaXJlc19hdF9tc3cDbmlsdwljbG9z" <>
                "ZWRfYnl3A25pbHcPbGFzdF9zdHJlYW1fc2VxdwNuaWx3BWZvcmtzancHY29weWluZ3cFZmFsc2V3" <>
                "DHNvZnRfZGVsZXRlZHcFZmFsc2U="
            )

  test "the offline upgrade preserves completed copies and ordinary stream state", context do
    shard = ctx_for("/copy").n
    producer = %Producer{epoch: 2, last_seq: 3}

    offline(fn dbs ->
      db = Enum.at(dbs, shard)
      {:meta, 1, fields} = :erlang.binary_to_term(@old_copy)
      source = %{fields | sid: 1, soft_deleted: true, forks: ["/copy"], fork_of: nil}

      ordinary = %{
        fields
        | sid: 3,
          closed: false,
          fork_of: nil,
          forks: ["/copy"],
          last_stream_seq: "seq",
          expires_at_ms: 4_102_444_800_000
      }

      ops =
        [
          {:put, Keys.meta("/source"), encode(source)},
          {:put, Keys.meta("/copy"), @old_copy},
          {:put, Keys.meta("/ordinary"), encode(ordinary)},
          {:put, Keys.tail(2), Codec.encode(%Tail{next_offset: 10})},
          {:put, Keys.tail(3),
           Codec.encode(%Tail{next_offset: 10, expiry_key_ms: 4_102_444_800_000})},
          {:put, Keys.expiry(4_102_444_800_000, 3), "/ordinary"},
          {:put, Keys.producer(3, "producer"), Codec.encode(producer)},
          {:put, Keys.trim(2), <<5::64>>},
          {:put, Keys.trim(3), <<5::64>>}
        ] ++
          Batch.messages(1, [{0, "old"}]) ++
          Batch.messages(2, [{0, "a"}, {5, "b"}]) ++
          Batch.messages(3, [{0, "c"}, {5, "d"}]) ++
          Batch.repair("/source", 1) ++ Batch.reserve_sids(4)

      {:ok, _} = SlateDB.write(db, ops)
      :ok = SlateDB.flush(db)
      assert_raise CaseClauseError, fn -> Codec.decode(@old_copy) end
      :ok = Upgrade.run(dbs)
      before_retry = Enum.map(dbs, &rows/1)
      :ok = Upgrade.run(dbs)
      assert Enum.map(dbs, &rows/1) == before_retry
      assert {:ok, nil} = Read.get_meta(db, "/source", durability: :remote)
      assert {:ok, <<0::64>>} = SlateDB.get(db, Keys.delete_pending(1), durability: :remote)
      assert {:ok, <<5::64>>} = SlateDB.get(db, Keys.reclaim(2), durability: :remote)
      assert {:ok, <<5::64>>} = SlateDB.get(db, Keys.reclaim(3), durability: :remote)

      assert {:ok, %{creation: nil, reclaimed_before: 5, closed: true}} =
               Read.get_meta(db, "/copy")

      assert {:ok,
              %{last_stream_seq: "seq", expires_at_ms: 4_102_444_800_000, registration_count: 0}} =
               Read.get_meta(db, "/ordinary")

      assert {:ok, "/ordinary"} = SlateDB.get(db, Keys.expiry(4_102_444_800_000, 3))
      assert Read.list_producers(db, 3) == %{"producer" => producer}
    end)

    start_supervised!({Streams.Cluster, context.cluster_opts})
    assert Streams.read("/copy", 0) == {:error, :trimmed}

    assert {:ok, %{messages: [{5, "b"}], next_offset: 10, closed: true}} =
             Streams.read("/copy", :start)

    assert {:ok, :created, _} = Streams.create("/fork", forked_from: "/copy")
    assert {:ok, :created, _} = Streams.copy("/copy", "/independent")
    :ok = Streams.delete("/copy")
    assert {:ok, %{messages: [{5, "b"}]}} = Streams.read("/fork", :start)
    assert {:ok, %{messages: [{5, "b"}], closed: true}} = Streams.read("/independent", :start)
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(shard)
    Deleter.drain(ctx)
    assert {:ok, nil} = SlateDB.get(ctx.db, Keys.msg(1, 0, 0))
    assert {:ok, nil} = SlateDB.get(ctx.db, Keys.msg(3, 0, 0))
    assert {:ok, "d"} = SlateDB.get(ctx.db, Keys.msg(3, 5, 0))

    assert {:ok, %{next_offset: 15}} =
             Streams.append("/ordinary", "e",
               placement_key: "/copy",
               producer: {"producer", 2, 4},
               stream_seq: "seq2"
             )

    assert {:ok, %{messages: [{5, "d"}, {10, "e"}]}} =
             Streams.read("/ordinary", :start, placement_key: "/copy")
  end

  test "preflight rejects incomplete copies on any shard without converting earlier shards" do
    offline(fn [first, second] = dbs ->
      {:meta, 1, fields} = :erlang.binary_to_term(@old_copy)
      {:ok, _} = SlateDB.write(first, [{:put, Keys.meta("/complete"), @old_copy}])
      incomplete = encode(%{fields | copying: true})
      {:ok, _} = SlateDB.write(second, [{:put, Keys.meta("/incomplete"), incomplete}])
      for db <- dbs, do: :ok = SlateDB.flush(db)

      assert {:error, {:incomplete_copy, "/incomplete"}} = Upgrade.run(dbs)

      assert {:ok, @old_copy} = SlateDB.get(first, Keys.meta("/complete"), durability: :remote)

      assert {:ok, ^incomplete} =
               SlateDB.get(second, Keys.meta("/incomplete"), durability: :remote)

      assert {:ok, nil} = SlateDB.get(first, Keys.reclaim(2))
    end)
  end

  test "upgrade crosses page boundaries and leaves current metadata intact" do
    offline(fn [db | _] = dbs ->
      {:meta, 1, fields} = :erlang.binary_to_term(@old_copy)
      current = %Meta{sid: 300, content_type: "application/json"}
      {:ok, _} = SlateDB.write(db, Batch.put_meta("/current", current))

      for sid <- 1..205 do
        {:ok, _} =
          SlateDB.write(db, [{:put, Keys.meta("/copy-#{sid}"), encode(%{fields | sid: sid})}])
      end

      :ok = SlateDB.flush(db)
      assert :ok = Upgrade.run(dbs)
      assert {:ok, ^current} = Read.get_meta(db, "/current", durability: :remote)
      assert {:ok, current_bytes} = SlateDB.get(db, Keys.meta("/current"), durability: :remote)
      assert {:meta, 2, _fields} = :erlang.binary_to_term(current_bytes, [:safe])
      assert current_bytes == Codec.encode(current)

      for sid <- 1..205 do
        assert {:ok, bytes} = SlateDB.get(db, Keys.meta("/copy-#{sid}"), durability: :remote)
        assert {:meta, 2, _fields} = :erlang.binary_to_term(bytes, [:safe])

        assert {:ok, %{sid: ^sid, creation: nil, closed: true}} =
                 Read.get_meta(db, "/copy-#{sid}", durability: :remote)
      end
    end)
  end

  test "preflight rejects unknown metadata versions before converting other shards" do
    offline(fn [first, second] = dbs ->
      unsupported = :erlang.term_to_binary({:meta, 3, Map.from_struct(%Meta{sid: 3})})
      {:ok, _} = SlateDB.write(first, [{:put, Keys.meta("/legacy"), @old_copy}])
      {:ok, _} = SlateDB.write(second, [{:put, Keys.meta("/future"), unsupported}])
      for db <- dbs, do: :ok = SlateDB.flush(db)

      assert {:error, {:unsupported_metadata, "/future"}} = Upgrade.run(dbs)
      assert {:ok, @old_copy} = SlateDB.get(first, Keys.meta("/legacy"), durability: :remote)
      assert {:ok, ^unsupported} = SlateDB.get(second, Keys.meta("/future"), durability: :remote)
    end)
  end

  defp offline(fun) do
    config = Config.get(Streams.Cluster)
    :ok = stop_supervised(Streams.Cluster)

    dbs =
      for n <- 0..(config.shards - 1) do
        {:ok, db} =
          SlateDB.open(Config.shard_path(config, n),
            store: config.store,
            settings: config.settings
          )

        db
      end

    try do
      fun.(dbs)
    after
      for db <- dbs, do: :ok = SlateDB.close(db)
    end
  end

  defp encode(fields), do: :erlang.term_to_binary({:meta, 1, fields})
  defp rows(db), do: db |> SlateDB.scan(durability: :remote) |> Enum.to_list()
end
