defmodule Slap.Cluster.HashTest do
  use ExUnit.Case, async: true

  alias Slap.Cluster.Hash
  alias Slap.Cluster.Test.HashVectors

  test "matches the reference XXH64 for every length up to 100 bytes" do
    data = for i <- 0..99, into: <<>>, do: <<rem(i * 7 + 3, 256)>>

    for {expected, len} <- Enum.with_index(HashVectors.by_length()) do
      assert Hash.xxh64(binary_part(data, 0, len)) == expected, "length #{len}"
    end
  end

  test "matches the reference XXH64 for some strings" do
    for {string, expected} <- HashVectors.strings() do
      assert Hash.xxh64(string) == expected, inspect(string)
    end
  end

  test "spreads keys evenly over shards" do
    counts =
      Enum.frequencies_by(1..64_000, &Hash.shard_for("stream/#{&1}", 64))

    assert map_size(counts) == 64
    assert Enum.all?(Map.values(counts), &(&1 in 800..1200))
  end
end
