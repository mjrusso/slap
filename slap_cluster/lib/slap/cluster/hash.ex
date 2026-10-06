defmodule Slap.Cluster.Hash do
  @moduledoc """
  XXH64 (xxHash, 64-bit), for placing keys on shards.

  `shard_for/2` is pinned forever: changing it moves every key to a different
  shard. It matches the reference implementation (checked against the
  `twox-hash` crate in the tests).
  """

  import Bitwise

  @p1 11_400_714_785_074_694_791
  @p2 14_029_467_366_897_019_727
  @p3 1_609_587_929_392_839_161
  @p4 9_650_029_242_287_828_579
  @p5 2_870_177_450_012_600_261
  @mask 0xFFFF_FFFF_FFFF_FFFF

  @doc "The shard for `key`: `xxh64(key, 0) mod shards`."
  @spec shard_for(binary(), pos_integer()) :: non_neg_integer()
  def shard_for(key, shards) when is_binary(key) and is_integer(shards) and shards > 0 do
    rem(xxh64(key), shards)
  end

  @doc "XXH64 of `data` with `seed`, as an unsigned 64-bit integer."
  @spec xxh64(binary(), non_neg_integer()) :: non_neg_integer()
  def xxh64(data, seed \\ 0) when is_binary(data) do
    len = byte_size(data)

    {h, rest} =
      if len >= 32 do
        {v1, v2, v3, v4, rest} =
          stripes(
            data,
            add(seed, add(@p1, @p2)),
            add(seed, @p2),
            seed,
            band(seed - @p1, @mask)
          )

        h = add(add(rotl(v1, 1), rotl(v2, 7)), add(rotl(v3, 12), rotl(v4, 18)))
        {h |> merge(v1) |> merge(v2) |> merge(v3) |> merge(v4), rest}
      else
        {add(seed, @p5), data}
      end

    h
    |> add(len)
    |> tail(rest)
    |> avalanche()
  end

  defp stripes(
         <<a::little-64, b::little-64, c::little-64, d::little-64, rest::binary>>,
         v1,
         v2,
         v3,
         v4
       ) do
    v1 = round(v1, a)
    v2 = round(v2, b)
    v3 = round(v3, c)
    v4 = round(v4, d)

    if byte_size(rest) >= 32,
      do: stripes(rest, v1, v2, v3, v4),
      else: {v1, v2, v3, v4, rest}
  end

  defp tail(h, <<k::little-64, rest::binary>>) do
    h = bxor(h, round(0, k))
    tail(add(mul(rotl(h, 27), @p1), @p4), rest)
  end

  defp tail(h, <<k::little-32, rest::binary>>) do
    h = bxor(h, mul(k, @p1))
    tail(add(mul(rotl(h, 23), @p2), @p3), rest)
  end

  defp tail(h, <<byte, rest::binary>>) do
    h = bxor(h, mul(byte, @p5))
    tail(mul(rotl(h, 11), @p1), rest)
  end

  defp tail(h, <<>>), do: h

  defp avalanche(h) do
    h = mul(bxor(h, h >>> 33), @p2)
    h = mul(bxor(h, h >>> 29), @p3)
    bxor(h, h >>> 32)
  end

  defp round(acc, input), do: mul(rotl(add(acc, mul(input, @p2)), 31), @p1)

  defp merge(acc, v), do: add(mul(bxor(acc, round(0, v)), @p1), @p4)

  defp add(a, b), do: band(a + b, @mask)
  defp mul(a, b), do: band(a * b, @mask)
  defp rotl(x, r), do: band(x <<< r, @mask) ||| x >>> (64 - r)
end
