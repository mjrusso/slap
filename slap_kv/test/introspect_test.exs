defmodule Slap.KV.IntrospectTest do
  use ExUnit.Case, async: true

  alias Slap.KV.{Introspect, Keys}

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

  test "the key parser validates partition and key boundaries" do
    assert {:ok, %{kind: :confirm}} = Keys.parse(Keys.confirm_key())

    assert {:ok, %{partition: <<0, 255>>, key: <<255>>, kind: :row}} =
             Introspect.decode(Keys.encode(<<0, 255>>, <<255>>), "value")

    for key <- [<<>>, <<0>>, <<0, 0, 1>>, <<0, 3, 1, 2>>, <<0, 1, 1>>] do
      assert {:error, :malformed_key} = Keys.parse(key)
    end
  end
end
