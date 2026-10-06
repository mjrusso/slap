defmodule Slap.KV.Keys do
  @moduledoc false
  # A row's SlateDB key is the partition's length (16 bits, big-endian), the
  # partition, then the row's key. A partition's rows are contiguous and in
  # key order, and one partition's keys cannot run into another's.

  @max_key 65_535

  # A key no row can have (its partition would be empty), which a partition
  # writer writes to confirm that its node still owns the shard.
  @spec confirm_key() :: binary()
  def confirm_key, do: <<0, 0>>

  @spec encode(binary(), binary()) :: binary()
  def encode(partition, key), do: <<byte_size(partition)::16, partition::binary, key::binary>>

  # The SlateDB bounds of a scan of `partition`: keys that start with
  # `prefix`, at or after `gte`, after `cursor`, and before `lt` (each nil
  # when not given). Returns `:empty` when no key can match.
  @spec range(binary(), binary(), binary() | nil, binary() | nil, binary() | nil) ::
          {:ok, keyword()} | :empty
  def range(partition, prefix, gte, cursor, lt) do
    start = encode(partition, prefix)

    lower =
      [
        {:gte, start},
        gte && {:gte, encode(partition, gte)},
        cursor && {:gt, encode(partition, cursor)}
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.max_by(fn {kind, key} -> {key, kind == :gt} end)

    upper =
      [{:lt, successor(start)}, lt && {:lt, encode(partition, lt)}]
      |> Enum.reject(&is_nil/1)
      |> Enum.min_by(fn {:lt, key} -> key end)

    if elem(lower, 1) < elem(upper, 1), do: {:ok, [lower, upper]}, else: :empty
  end

  # The first key after every key that starts with `prefix`. A prefix always
  # starts with a partition's length, which is below 0xFFFF, so it exists.
  defp successor(prefix) do
    size = byte_size(prefix) - 1

    case prefix do
      <<head::binary-size(^size), 255>> -> successor(head)
      <<head::binary-size(^size), last>> -> <<head::binary, last + 1>>
    end
  end

  @spec decode(binary(), binary()) :: binary()
  def decode(partition, encoded) do
    size = byte_size(partition) + 2
    <<_::binary-size(^size), key::binary>> = encoded
    key
  end

  @spec validate(term(), term()) :: :ok | {:error, {:bad_request, atom()}}
  def validate(partition, _key) when not is_binary(partition) or partition == "",
    do: {:error, {:bad_request, :invalid_partition}}

  def validate(_partition, key) when not is_binary(key) or key == "",
    do: {:error, {:bad_request, :invalid_key}}

  def validate(partition, key) when byte_size(partition) + byte_size(key) + 2 > @max_key,
    do: {:error, {:bad_request, :key_too_long}}

  def validate(_partition, _key), do: :ok

  @spec validate_partition(term()) :: :ok | {:error, {:bad_request, atom()}}
  def validate_partition(partition) when not is_binary(partition) or partition == "",
    do: {:error, {:bad_request, :invalid_partition}}

  def validate_partition(partition) when byte_size(partition) + 2 > @max_key,
    do: {:error, {:bad_request, :key_too_long}}

  def validate_partition(_partition), do: :ok
end
