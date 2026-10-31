defmodule Slap.Streams.Store.Keys do
  @moduledoc false

  # Each key starts with its type byte. These are stored; never renumber them.
  @next_sid 0x00
  @meta 0x01
  @tail 0x02
  @producer 0x03
  @msg 0x04
  @expiry 0x05
  @delete_pending 0x06
  @trim 0x07
  @repair 0x08
  @seal 0x09
  @reclaim 0x0A
  @witness 0x0C
  @registration 0x0D
  @pin 0x0E
  @work 0x0F

  @sid_types %{
    @tail => :tail,
    @delete_pending => :delete_pending,
    @trim => :trim,
    @repair => :repair,
    @reclaim => :reclaim,
    @witness => :witness,
    @work => :work
  }

  @prefixes %{
    meta: @meta,
    expiry: @expiry,
    delete_pending: @delete_pending,
    repair: @repair,
    reclaim: @reclaim,
    work: @work
  }

  @spec next_sid() :: binary()
  def next_sid, do: <<@next_sid, "next_sid">>

  @spec meta(binary()) :: binary()
  def meta(path) when is_binary(path), do: <<@meta, path::binary>>

  @spec meta_path(binary()) :: binary()
  def meta_path(<<@meta, path::binary>>), do: path

  @spec tail(non_neg_integer()) :: binary()
  def tail(sid), do: <<@tail, sid::64>>

  @spec producer(non_neg_integer(), binary()) :: binary()
  def producer(sid, producer_id) when is_binary(producer_id),
    do: <<@producer, sid::64, producer_id::binary>>

  @spec producer_prefix(non_neg_integer()) :: binary()
  def producer_prefix(sid), do: <<@producer, sid::64>>

  @spec producer_id(binary()) :: binary()
  def producer_id(<<@producer, _sid::64, producer_id::binary>>), do: producer_id

  @spec msg(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: binary()
  def msg(sid, offset, part), do: <<@msg, sid::64, offset::64, part::16>>

  @doc "The prefix of every message key of `sid`."
  @spec msg_prefix(non_neg_integer()) :: binary()
  def msg_prefix(sid), do: <<@msg, sid::64>>

  @doc "The scan bounds for messages of `sid` starting in `from..to - 1`."
  @spec msg_range(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: keyword()
  def msg_range(sid, from, to), do: [gte: msg(sid, from, 0), lt: msg(sid, to, 0)]

  @doc "`{offset, part}` of a message key."
  @spec decode_msg(binary()) :: {non_neg_integer(), non_neg_integer()}
  def decode_msg(<<@msg, _sid::64, offset::64, part::16>>), do: {offset, part}

  @spec expiry(non_neg_integer(), non_neg_integer()) :: binary()
  def expiry(deadline_ms, sid), do: <<@expiry, deadline_ms::64, sid::64>>

  @doc "`{deadline_ms, sid}` of an expiry key."
  @spec decode_expiry(binary()) :: {non_neg_integer(), non_neg_integer()}
  def decode_expiry(<<@expiry, deadline_ms::64, sid::64>>), do: {deadline_ms, sid}

  @spec decode_sid(binary()) :: non_neg_integer()
  def decode_sid(<<type, sid::64>>) when type != @tail and is_map_key(@sid_types, type), do: sid

  @doc "The prefix of every key of one type, for scans."
  @spec prefix(atom()) :: binary()
  def prefix(kind), do: <<Map.fetch!(@prefixes, kind)>>

  @spec seal(binary()) :: binary()
  def seal(group) when is_binary(group), do: <<@seal, group::binary>>

  @spec delete_pending(non_neg_integer()) :: binary()
  def delete_pending(sid), do: <<@delete_pending, sid::64>>

  @spec trim(non_neg_integer()) :: binary()
  def trim(sid), do: <<@trim, sid::64>>

  @spec repair(non_neg_integer()) :: binary()
  def repair(sid), do: <<@repair, sid::64>>
  @spec reclaim(non_neg_integer()) :: binary()
  def reclaim(sid), do: <<@reclaim, sid::64>>

  @spec witness(non_neg_integer()) :: binary()
  def witness(sid), do: <<@witness, sid::64>>

  def registration(sid, target), do: <<@registration, sid::64, target.shard::64, target.sid::64>>
  def registration_prefix(sid), do: <<@registration, sid::64>>

  def pin(sid, start, target),
    do: <<@pin, sid::64, start::64, target.shard::64, target.sid::64>>

  def pin_prefix(sid), do: <<@pin, sid::64>>
  def pin_start(<<@pin, _sid::64, start::64, _target_shard::64, _target_sid::64>>), do: start

  @spec work(non_neg_integer()) :: binary()
  def work(sid), do: <<@work, sid::64>>

  @spec scope(binary()) :: :shared | :meta | {:stream, non_neg_integer()}

  def scope(key) do
    case parse(key) do
      {:ok, %{sid: sid}} -> {:stream, sid}
      {:ok, %{kind: :meta}} -> :meta
      {:ok, %{kind: kind}} when kind in [:next_sid, :seal] -> :shared
      {:error, _reason} -> raise ArgumentError, "unrecognized stream key: #{inspect(key)}"
    end
  end

  def parse(<<@next_sid, "next_sid">>), do: {:ok, %{kind: :next_sid}}
  def parse(<<@meta, path::binary>>), do: {:ok, %{kind: :meta, path: path}}

  def parse(<<@producer, sid::64, producer::binary>>),
    do: {:ok, %{kind: :producer, sid: sid, producer: producer}}

  def parse(<<@msg, sid::64, offset::64, part::16>>),
    do: {:ok, %{kind: :message, sid: sid, offset: offset, part: part}}

  def parse(<<@expiry, deadline::64, sid::64>>),
    do: {:ok, %{kind: :expiry, sid: sid, deadline_ms: deadline}}

  def parse(<<@seal, group::binary>>), do: {:ok, %{kind: :seal, group: group}}

  def parse(<<@registration, sid::64, shard::64, target::64>>),
    do: {:ok, %{kind: :registration, sid: sid, target: %{shard: shard, sid: target}}}

  def parse(<<@pin, sid::64, start::64, shard::64, target::64>>),
    do: {:ok, %{kind: :pin, sid: sid, start: start, target: %{shard: shard, sid: target}}}

  def parse(<<type, sid::64>>) when is_map_key(@sid_types, type),
    do: {:ok, %{kind: Map.fetch!(@sid_types, type), sid: sid}}

  def parse(_key), do: {:error, :unknown_or_malformed_key}
end
