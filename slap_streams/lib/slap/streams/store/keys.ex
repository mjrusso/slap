defmodule Slap.Streams.Store.Keys do
  @moduledoc false

  @spec next_sid() :: binary()
  def next_sid, do: <<0x00, "next_sid">>

  @spec meta(binary()) :: binary()
  def meta(path) when is_binary(path), do: <<0x01, path::binary>>

  @doc "The path in a meta key."
  @spec meta_path(binary()) :: binary()
  def meta_path(<<0x01, path::binary>>), do: path

  @spec tail(non_neg_integer()) :: binary()
  def tail(sid), do: <<0x02, sid::64>>

  @spec producer(non_neg_integer(), binary()) :: binary()
  def producer(sid, producer_id) when is_binary(producer_id),
    do: <<0x03, sid::64, producer_id::binary>>

  @doc "The prefix of every producer key of `sid`."
  @spec producer_prefix(non_neg_integer()) :: binary()
  def producer_prefix(sid), do: <<0x03, sid::64>>

  @doc "The producer id in a producer key."
  @spec producer_id(binary()) :: binary()
  def producer_id(<<0x03, _sid::64, producer_id::binary>>), do: producer_id

  @spec msg(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: binary()
  def msg(sid, offset, part), do: <<0x04, sid::64, offset::64, part::16>>

  @doc "The scan bounds for messages of `sid` starting in `from..to - 1`."
  @spec msg_range(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: keyword()
  def msg_range(sid, from, to), do: [gte: msg(sid, from, 0), lt: msg(sid, to, 0)]

  @doc "`{offset, part}` of a message key."
  @spec decode_msg(binary()) :: {non_neg_integer(), non_neg_integer()}
  def decode_msg(<<0x04, _sid::64, offset::64, part::16>>), do: {offset, part}

  @spec expiry(non_neg_integer(), non_neg_integer()) :: binary()
  def expiry(deadline_ms, sid), do: <<0x05, deadline_ms::64, sid::64>>

  @doc "`{deadline_ms, sid}` of an expiry key."
  @spec decode_expiry(binary()) :: {non_neg_integer(), non_neg_integer()}
  def decode_expiry(<<0x05, deadline_ms::64, sid::64>>), do: {deadline_ms, sid}

  @doc "The sid in a delete-pending, trim or repair key."
  @spec decode_sid(binary()) :: non_neg_integer()
  def decode_sid(<<type, sid::64>>) when type in [0x06, 0x07, 0x08], do: sid

  @doc "The prefix of every key of `type` (a byte)."
  @spec type_prefix(0..255) :: binary()
  def type_prefix(type), do: <<type>>

  @spec seal(binary()) :: binary()
  def seal(group) when is_binary(group), do: <<0x09, group::binary>>

  @spec delete_pending(non_neg_integer()) :: binary()
  def delete_pending(sid), do: <<0x06, sid::64>>

  @spec trim(non_neg_integer()) :: binary()
  def trim(sid), do: <<0x07, sid::64>>

  @spec repair(non_neg_integer()) :: binary()
  def repair(sid), do: <<0x08, sid::64>>
end
