defmodule Slap.KV.IntrospectTest do
  use ExUnit.Case, async: true

  alias Slap.Cluster.Inspection
  alias Slap.KV.{Introspect, Keys}
  alias Slap.SlateDB

  defmodule Cluster do
    use Slap.Cluster, otp_app: :slap_kv
  end

  defmodule AppCluster do
    use Slap.KV.Cluster, otp_app: :slap_kv
  end

  test "an application-defined KV cluster is discovered with the kv service" do
    start_supervised!({AppCluster, store: :memory, shards: 2})

    assert %{shards: 2, services: [:kv]} =
             Enum.find(Slap.Cluster.clusters(), &(&1.cluster == AppCluster))
  end

  test "invalid partitions are rejected before keys are read" do
    start_supervised!({Cluster, store: :memory, shards: 1})
    assert :ok = Cluster.await_ready(5000)

    assert {:error, {:bad_request, :invalid_partition}} =
             Introspect.scan(Cluster, 0, 1, 5000, partition: "")

    assert {:error, {:bad_request, :key_too_long}} =
             Introspect.scan(Cluster, 0, 1, 5000, partition: String.duplicate("x", 65_535))
  end

  test "partition listings filter and deduplicate partitions without loading values" do
    start_supervised!({Cluster, store: :memory, shards: 1})
    assert :ok = Cluster.await_ready(5000)
    {:ok, {:local, ctx}} = Slap.Cluster.lookup(Cluster, 0)
    large = String.duplicate("v", 600 * 1024)

    rows =
      for partition <- ["p", "q", "skip"],
          key <- ["a", "b"],
          do: {:put, Keys.encode(partition, key), large}

    rows = [{:put, <<0>>, "v"}, {:put, Keys.confirm_key(), "v"}, {:put, <<0, 0, 1>>, "v"}] ++ rows
    assert {:ok, _} = SlateDB.write(ctx.db, rows, await_durable: true)

    parse = fn
      :confirm -> :error
      "skip" -> :error
      partition -> {:ok, partition}
    end

    assert {:ok, page} =
             Inspection.request(Cluster, 0, 1, 5000, fn ctx, deadline ->
               Introspect.partitions(ctx, deadline, [], parse)
             end)

    assert [
             %{key: <<0>>, diagnostic: :unknown_key},
             %{key: <<0, 0, 1>>, diagnostic: :unknown_key},
             %{key: <<0, 1, ?p>>, decoded: {:ok, "p"}},
             %{key: <<0, 1, ?q>>, decoded: {:ok, "q"}}
           ] = page.rows
  end

  test "partition listings include the ownership-confirmation row" do
    start_supervised!({Cluster, store: :memory, shards: 1})
    assert :ok = Cluster.await_ready(5000)
    {:ok, {:local, ctx}} = Slap.Cluster.lookup(Cluster, 0)

    rows =
      [{:put, Keys.confirm_key(), "c"}] ++
        for(key <- ["a", "b"], do: {:put, Keys.encode("p", key), "v"})

    assert {:ok, _} = SlateDB.write(ctx.db, rows, await_durable: true)

    assert {:ok, %{rows: rows, stopped: :end}} =
             Introspect.list_partitions(Cluster, 0, 1, 5000, [])

    assert [
             %{decoded: {:ok, %{kind: :confirm}}},
             %{decoded: {:ok, %{kind: :partition, partition: "p"}}}
           ] =
             rows
  end

  test "partition listings diagnose malformed keys and accept valid truncated previews" do
    start_supervised!({Cluster, store: :memory, shards: 1})
    assert :ok = Cluster.await_ready(5000)
    {:ok, {:local, ctx}} = Slap.Cluster.lookup(Cluster, 0)
    malformed = [Keys.confirm_key() <> <<1>>, Keys.encode("p", "")]
    keys = [Keys.confirm_key(), Keys.encode("p", "row"), Keys.encode("q", "row")] ++ malformed
    assert {:ok, _} = SlateDB.write(ctx.db, Enum.map(keys, &{:put, &1, "v"}), await_durable: true)

    for opts <- [[], [max_key_bytes: 3]] do
      assert {:ok, %{rows: rows}} = Introspect.list_partitions(Cluster, 0, 1, 5000, opts)

      assert [
               %{decoded: {:ok, %{kind: :confirm}}},
               %{key: <<0, 0, 1>>, diagnostic: :unknown_key},
               %{key: <<0, 1, ?p>>, diagnostic: :unknown_key},
               %{decoded: {:ok, %{kind: :partition, partition: "p"}}},
               %{decoded: {:ok, %{kind: :partition, partition: "q"}}}
             ] = rows
    end
  end

  test "the key parser validates partition and key boundaries" do
    assert {:ok, %{kind: :confirm}} = Keys.parse(Keys.confirm_key())

    assert {:ok, %{partition: <<0, 255>>, key: <<255>>, kind: :row}} =
             Introspect.decode(Keys.encode(<<0, 255>>, <<255>>), "value")

    for key <- [<<>>, <<0>>, <<0, 0, 1>>, <<0, 3, 1, 2>>, <<0, 1, 1>>] do
      assert {:error, :malformed_key} = Keys.parse(key)
    end
  end
end
