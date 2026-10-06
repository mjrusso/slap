defmodule Slap.Streams.Store.Batch do
  @moduledoc false

  alias Slap.SlateDB
  alias Slap.Streams.Store.{Codec, Keys, Meta, Producer, Tail}

  @part_size 256 * 1024

  @doc """
  A new stream: its metadata, tail and initial messages
  (`[{offset, bytes}]`), and its expiry index entry if it has an expiry.
  """
  @spec create(binary(), Meta.t(), Tail.t(), [{non_neg_integer(), binary()}]) :: [
          SlateDB.write_op()
        ]
  def create(path, %Meta{sid: sid} = meta, %Tail{} = tail, messages) do
    [{:put, Keys.meta(path), Codec.encode(meta)}, {:put, Keys.tail(sid), Codec.encode(tail)}] ++
      messages(sid, messages) ++ expiry(path, sid, nil, tail.expiry_key_ms)
  end

  @doc "The metadata alone, as when a fork is registered or a copy finishes."
  @spec put_meta(binary(), Meta.t()) :: [SlateDB.write_op()]
  def put_meta(path, %Meta{} = meta), do: [{:put, Keys.meta(path), Codec.encode(meta)}]

  @doc "The tail alone, with the expiry index moved from `old` to its deadline."
  @spec touch(binary(), non_neg_integer(), Tail.t(), integer() | nil) :: [SlateDB.write_op()]
  def touch(path, sid, %Tail{} = tail, old) do
    [{:put, Keys.tail(sid), Codec.encode(tail)}] ++ expiry(path, sid, old, tail.expiry_key_ms)
  end

  @doc "Moves the expiry index entry of `sid` from deadline `old` to `new`."
  @spec expiry(binary(), non_neg_integer(), integer() | nil, integer() | nil) ::
          [SlateDB.write_op()]
  def expiry(_path, _sid, same, same), do: []

  def expiry(path, sid, old, new) do
    delete = if old, do: [{:delete, Keys.expiry(old, sid)}], else: []
    put = if new, do: [{:put, Keys.expiry(new, sid), path}], else: []
    delete ++ put
  end

  @doc """
  Appends messages (`[{offset, bytes}]`, possibly none, as for a close) to
  `sid` with its new tail. Options:

    * `:producers` - `[{producer_id, %Producer{}}]` to store.
    * `:meta` - `{path, %Meta{}}` when the metadata changes (a close, or a
      new `Stream-Seq`).
  """
  @spec append(non_neg_integer(), [{non_neg_integer(), binary()}], Tail.t(), keyword()) ::
          [SlateDB.write_op()]
  def append(sid, messages, %Tail{} = tail, opts \\ []) do
    producers =
      for {id, %Producer{} = p} <- Keyword.get(opts, :producers, []),
          do: {:put, Keys.producer(sid, id), Codec.encode(p)}

    meta =
      case Keyword.get(opts, :meta) do
        nil -> []
        {path, %Meta{} = meta} -> [{:put, Keys.meta(path), Codec.encode(meta)}]
      end

    index = Keyword.get(opts, :expiry, [])

    messages(sid, messages) ++
      [{:put, Keys.tail(sid), Codec.encode(tail)}] ++ producers ++ meta ++ index
  end

  @doc """
  A logical delete: the metadata goes (the stream is not found from now on),
  so do its expiry and repair index entries (deadline `expiry_key`), and a
  delete-pending marker is left for `Slap.Streams.Jobs.Deleter`, which removes the
  stream's rows.
  """
  @spec logical_delete(binary(), non_neg_integer(), integer() | nil) :: [SlateDB.write_op()]
  def logical_delete(path, sid, expiry_key \\ nil) do
    [
      {:delete, Keys.meta(path)},
      {:delete, Keys.repair(sid)},
      {:put, Keys.delete_pending(sid), <<0::64>>}
    ] ++ expiry(path, sid, expiry_key, nil)
  end

  @doc """
  A soft delete (a stream with forks): the metadata stays, marked deleted
  (410), and the stream's rows are deleted as for a logical delete. Forks
  have their own copies.
  """
  @spec soft_delete(binary(), Meta.t(), integer() | nil) :: [SlateDB.write_op()]
  def soft_delete(path, %Meta{sid: sid} = meta, expiry_key) do
    put_meta(path, %{meta | soft_deleted: true}) ++
      [{:put, Keys.delete_pending(sid), <<0::64>>} | repair(path, sid)] ++
      expiry(path, sid, expiry_key, nil)
  end

  @doc "Removes the metadata of a soft-deleted stream whose last fork is gone."
  @spec finalize(binary(), non_neg_integer()) :: [SlateDB.write_op()]
  def finalize(path, sid), do: [{:delete, Keys.meta(path)}, {:delete, Keys.repair(sid)}]

  @doc """
  Lists the stream in the repair index (`Slap.Streams.Jobs.Repair`): a fork that
  starts copying, or a soft-deleted stream.
  """
  @spec repair(binary(), non_neg_integer()) :: [SlateDB.write_op()]
  def repair(path, sid), do: [{:put, Keys.repair(sid), path}]

  @doc """
  Trims `sid` before `offset`: reads from earlier offsets get 410, and
  `Slap.Streams.Jobs.Deleter` deletes the rows before it.
  """
  @spec trim(non_neg_integer(), non_neg_integer()) :: [SlateDB.write_op()]
  def trim(sid, offset), do: [{:put, Keys.trim(sid), <<offset::64>>}]

  @doc "Reserves stream ids below `next`."
  @spec reserve_sids(non_neg_integer()) :: [SlateDB.write_op()]
  def reserve_sids(next), do: [{:put, Keys.next_sid(), <<next::64>>}]

  @doc "The ops for messages, each split into parts of at most 256 KiB."
  @spec messages(non_neg_integer(), [{non_neg_integer(), binary()}]) :: [SlateDB.write_op()]
  def messages(sid, messages) do
    for {offset, bytes} <- messages,
        {part, n} <- Enum.with_index(parts(bytes)) do
      {:put, Keys.msg(sid, offset, n), part}
    end
  end

  defp parts(bytes) when byte_size(bytes) <= @part_size, do: [bytes]

  defp parts(<<part::binary-size(@part_size), rest::binary>>), do: [part | parts(rest)]
end
