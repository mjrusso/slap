defmodule Slap.Streams.Store.Keys do
  @moduledoc false

  @spec next_sid() :: binary()
  def next_sid, do: <<0x00, "next_sid">>

  @spec meta(binary()) :: binary()
  def meta(path) when is_binary(path), do: <<0x01, path::binary>>

  @spec meta_path(binary()) :: binary()
  def meta_path(<<0x01, path::binary>>), do: path

  @spec tail(non_neg_integer()) :: binary()
  def tail(sid), do: <<0x02, sid::64>>

  @spec producer(non_neg_integer(), binary()) :: binary()
  def producer(sid, producer_id) when is_binary(producer_id),
    do: <<0x03, sid::64, producer_id::binary>>

  @spec producer_prefix(non_neg_integer()) :: binary()
  def producer_prefix(sid), do: <<0x03, sid::64>>

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

  @spec decode_sid(binary()) :: non_neg_integer()
  def decode_sid(<<type, sid::64>>) when type in [0x06, 0x07, 0x08, 0x0A, 0x0C], do: sid

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
  @spec reclaim(non_neg_integer()) :: binary()
  def reclaim(sid), do: <<0x0A, sid::64>>

  @spec witness(non_neg_integer()) :: binary()
  def witness(sid), do: <<0x0C, sid::64>>

  def registration(sid, target), do: <<0x0D, sid::64, target.shard::64, target.sid::64>>
  def registration_prefix(sid), do: <<0x0D, sid::64>>
  def pin(sid, start, target), do: <<0x0E, sid::64, start::64, target.shard::64, target.sid::64>>
  def pin_prefix(sid), do: <<0x0E, sid::64>>
  def pin_start(<<0x0E, _sid::64, start::64, _target_shard::64, _target_sid::64>>), do: start

  @spec scope(binary()) :: :shared | :meta | {:stream, non_neg_integer()}
  def scope(<<0x00, "next_sid">>), do: :shared
  def scope(<<0x09, _group::binary>>), do: :shared
  def scope(<<0x01, _path::binary>>), do: :meta
  def scope(<<0x03, sid::64, _producer::binary>>), do: {:stream, sid}
  def scope(<<0x04, sid::64, _offset::64, _part::16>>), do: {:stream, sid}
  def scope(<<0x05, _deadline::64, sid::64>>), do: {:stream, sid}

  def scope(<<type, sid::64>>) when type in [0x02, 0x06, 0x07, 0x08, 0x0A, 0x0C],
    do: {:stream, sid}

  def scope(<<0x0D, sid::64, _target_shard::64, _target_sid::64>>), do: {:stream, sid}
  def scope(<<0x0E, sid::64, _start::64, _target_shard::64, _target_sid::64>>), do: {:stream, sid}
  def scope(key), do: raise(ArgumentError, "unrecognized stream key: #{inspect(key)}")
end
