defmodule Slap.Streams.Store.Read do
  @moduledoc false

  alias Slap.SlateDB
  alias Slap.Streams.Offset
  alias Slap.Streams.Store.{Codec, Keys, Meta, Producer, Tail}

  @spec get_meta(SlateDB.t(), binary()) :: {:ok, Meta.t() | nil} | {:error, term()}
  def get_meta(db, path), do: get(db, Keys.meta(path))

  @doc """
  `{path, %Meta{}}` of every stream whose path starts with `prefix`, in path
  order. Takes `scan/2`'s read options, such as `:durability`. Raises
  `Slap.SlateDB.Error` if the scan fails.
  """
  @spec list_meta(SlateDB.t(), binary(), keyword()) :: [{binary(), Meta.t()}]
  def list_meta(db, prefix, opts \\ []) do
    db
    |> SlateDB.scan(Keyword.put(opts, :prefix, Keys.meta(prefix)))
    |> Enum.map(fn {key, value} -> {Keys.meta_path(key), Codec.decode(value)} end)
  end

  @spec get_tail(SlateDB.t(), non_neg_integer()) :: {:ok, Tail.t() | nil} | {:error, term()}
  def get_tail(db, sid), do: get(db, Keys.tail(sid))

  @doc "Every producer of `sid`, as `%{producer_id => %Producer{}}`."
  @spec list_producers(SlateDB.t(), non_neg_integer()) :: %{binary() => Producer.t()}
  def list_producers(db, sid) do
    db
    |> SlateDB.scan(prefix: Keys.producer_prefix(sid))
    |> Map.new(fn {key, value} -> {Keys.producer_id(key), Codec.decode(value)} end)
  end

  @doc "The trim point of `sid`: reads before it get 410. 0 when untrimmed."
  @spec get_trim(SlateDB.t(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def get_trim(db, sid) do
    case SlateDB.get(db, Keys.trim(sid)) do
      {:ok, nil} -> {:ok, 0}
      {:ok, <<offset::64>>} -> {:ok, offset}
      {:error, _} = error -> error
    end
  end

  @doc "The next unreserved stream id, or 1 for a new shard."
  @spec next_sid(SlateDB.t()) :: {:ok, pos_integer()} | {:error, term()}
  def next_sid(db) do
    case SlateDB.get(db, Keys.next_sid()) do
      {:ok, nil} -> {:ok, 1}
      {:ok, <<n::64>>} -> {:ok, n}
      {:error, _} = error -> error
    end
  end

  @doc """
  Reads the messages of `sid` that start at `from` or later and before
  `until`, joining their parts. Stops after the message that brings the
  total to `max_bytes` or more, but always returns at least one message if
  there is one.

  Returns `{[{offset, bytes}], next_offset}`: the offset to read from next,
  `until` when everything up to it was read.
  """
  @spec read_msgs(SlateDB.t(), non_neg_integer(), Offset.t(), Offset.t(), pos_integer()) ::
          {[{Offset.t(), binary()}], Offset.t()}
  def read_msgs(_db, _sid, from, until, _max_bytes) when from >= until, do: {[], until}

  def read_msgs(db, sid, from, until, max_bytes) do
    # {messages (reversed), bytes so far, current {offset, parts (reversed)} | nil}
    result =
      db
      |> SlateDB.scan(Keys.msg_range(sid, from, until))
      |> Enum.reduce_while({[], 0, nil}, &add_row(&1, &2, max_bytes))

    case result do
      {:full, acc, {offset, bytes}} ->
        {Enum.reverse(acc), Offset.advance(offset, byte_size(bytes))}

      {acc, _total, nil} ->
        {Enum.reverse(acc), until}

      {acc, _total, current} ->
        {msg, _size} = finish(current)
        {Enum.reverse([msg | acc]), until}
    end
  end

  # Rows come in key order: a message's parts, then the next message's.
  defp add_row({key, value}, {acc, total, current}, max_bytes) do
    {offset, _part} = Keys.decode_msg(key)

    case current do
      {^offset, parts} -> {:cont, {acc, total, {offset, [value | parts]}}}
      nil -> {:cont, {acc, total, {offset, [value]}}}
      done -> add_message(done, {acc, total}, max_bytes, {offset, [value]})
    end
  end

  # A message is complete; stop once `max_bytes` are read.
  defp add_message(done, {acc, total}, max_bytes, next) do
    {msg, size} = finish(done)
    acc = [msg | acc]
    total = total + size

    if total >= max_bytes, do: {:halt, {:full, acc, msg}}, else: {:cont, {acc, total, next}}
  end

  defp finish({offset, parts}) do
    bytes = parts |> Enum.reverse() |> IO.iodata_to_binary()
    {{offset, bytes}, byte_size(bytes)}
  end

  defp get(db, key) do
    case SlateDB.get(db, key) do
      {:ok, nil} -> {:ok, nil}
      {:ok, value} -> {:ok, Codec.decode(value)}
      {:error, _} = error -> error
    end
  end
end
