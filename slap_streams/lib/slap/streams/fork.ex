defmodule Slap.Streams.Fork do
  @moduledoc false

  require Logger

  alias Slap.Streams
  alias Slap.Streams.{ContentType, Offset, Protocol, Stream}
  alias Slap.Streams.Store.{Batch, Codec, ForkOf, Keys, Meta, Read, Tail}

  @default_max_fork_copy_bytes 64 * 1024 * 1024
  @default_fork_copy_grace :timer.hours(1)
  @copy_page_bytes 4 * 1024 * 1024

  @typedoc "What the source answers to `source/3`."
  @type source_info :: %{
          content_type: binary(),
          ttl_s: non_neg_integer() | nil,
          expires_at_ms: integer() | nil,
          offset: non_neg_integer(),
          prefix: binary() | nil,
          trim: non_neg_integer()
        }

  # -- On the fork ----------------------------------------------------------

  @doc """
  Has the source validate and register the fork at `path`. `resuming?` is
  true for a fork that is copying already: if its source was deleted
  meanwhile, it cannot finish, and the error is `{:abandon, :source_gone}`.
  """
  @spec source(Slap.Cluster.Shard.t(), binary(), map(), boolean()) ::
          {:ok, source_info()} | {:error, term()}
  def source(ctx, path, req, resuming?) do
    source = req[:forked_from]

    with :ok <- Protocol.check_expiry(req),
         :ok <- check_not_self(source, path) do
      case Streams.internal(source, {:fork_source, path, req}, cluster: ctx.cluster) do
        {:error, :source_gone} when resuming? -> {:error, {:abandon, :source_gone}}
        other -> other
      end
    end
  end

  defp check_not_self(path, path), do: {:error, {:bad_request, :fork_of_itself}}
  defp check_not_self(_source, _path), do: :ok

  @doc """
  The fork, copying, as `sid`, and the writes that make it so: its
  metadata, an empty tail, and its entry in the repair index.
  """
  @spec start(binary(), map(), source_info(), non_neg_integer(), integer()) ::
          {Stream.t(), [Slap.SlateDB.write_op()]}
  def start(path, req, src, sid, now) do
    {ttl_s, expires_at_ms} = expiry(req, src)

    meta = %Meta{
      sid: sid,
      content_type: src.content_type,
      created_ms: now,
      ttl_s: ttl_s,
      expires_at_ms: expires_at_ms,
      copying: true,
      fork_of: %ForkOf{
        path: req[:forked_from],
        offset: src.offset,
        requested_offset: req[:fork_offset],
        sub_offset: req[:fork_sub_offset] || 0,
        requested_content_type: req[:content_type],
        requested_ttl_s: req[:ttl_s],
        requested_expires_at_ms: req[:expires_at_ms]
      }
    }

    tail = Codec.encode(%Tail{next_offset: 0, last_access_ms: now})

    ops =
      [{:put, Keys.tail(sid), tail} | Batch.put_meta(path, meta)] ++ Batch.repair(path, sid)

    {Stream.copying(path, meta), ops}
  end

  # §4.2: the fork's own TTL or Expires-At, else the source's.
  defp expiry(req, src) do
    cond do
      req[:ttl_s] != nil -> {req[:ttl_s], nil}
      req[:expires_at_ms] != nil -> {nil, req[:expires_at_ms]}
      true -> {src.ttl_s, src.expires_at_ms}
    end
  end

  @doc "Where the copy starts: data the source trimmed is trimmed in the fork too."
  @spec copy_from(source_info()) :: non_neg_integer()
  def copy_from(src), do: min(src.trim, src.offset)

  @doc """
  The source's messages that start in `from..until - 1`, a page at a time:
  `{:ok, page, next}`, where `next` is `until` after the last page.
  """
  @spec read_page(Slap.Cluster.Shard.t(), binary(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [{non_neg_integer(), binary()}], non_neg_integer()} | :done | {:error, term()}
  def read_page(_ctx, _source, from, until) when from >= until, do: :done

  def read_page(ctx, source, from, until) do
    with {:ok, %{messages: messages, next_offset: next}} <-
           Streams.read_internal(source, from,
             max_bytes: @copy_page_bytes,
             peek: true,
             cluster: ctx.cluster
           ) do
      page = for {offset, _} = m <- messages, offset < until, do: m
      {:ok, page, if(messages == [] or next >= until, do: until, else: next)}
    end
  end

  @doc "Makes the copied fork active, with its own first messages (`body`, split)."
  @spec finish(Stream.t(), map(), source_info(), [binary()], integer()) :: Protocol.decision()
  def finish(%Stream{status: :copying, path: path, sid: sid} = copying, req, src, body, now) do
    prefix = if src.prefix, do: [src.prefix], else: []
    {stored, tail} = Protocol.place(prefix ++ body, src.offset)
    meta = %{copying.meta | copying: false, closed: req[:closed] == true}
    expiry_key = Stream.deadline(meta, now)
    tail_row = %Tail{next_offset: tail, last_access_ms: now, expiry_key_ms: expiry_key}
    trim = copy_from(src)

    ops =
      Batch.put_meta(path, meta) ++
        [{:delete, Keys.repair(sid)}] ++
        Batch.messages(sid, stored) ++
        Batch.touch(path, sid, tail_row, nil) ++
        if(trim > 0, do: Batch.trim(sid, trim), else: [])

    stream = %{
      copying
      | status: :active,
        meta: meta,
        tail: tail,
        last_access: now,
        expiry_key: expiry_key,
        trim: trim
    }

    {:write, ops, {:ok, :created, Protocol.info(meta, tail)}, stream, []}
  end

  @doc "Removes a fork that is still copying, and unregisters it from its source."
  @spec drop(Stream.t(), term()) :: Protocol.decision()
  def drop(%Stream{status: :copying, meta: meta} = stream, reply) do
    {:write, Batch.logical_delete(stream.path, stream.sid), reply, Stream.absent(stream.path),
     [:kick_deleter | Protocol.unregister_from(meta.fork_of)]}
  end

  @doc """
  From `Slap.Streams.Jobs.Repair`, for the stream listed in the repair
  index as `sid`. A fork left copying past the grace period is dropped. A
  soft-deleted stream returns its forks, for the sweep to check (not here:
  a fork being deleted may be calling this server to unregister). An entry
  for any other state is stale (the stream was removed, or its path
  reused) and is dropped.
  """
  @spec repair_check(Slap.Cluster.Shard.t(), Stream.t(), non_neg_integer(), integer()) ::
          Protocol.decision()
  def repair_check(ctx, %Stream{sid: sid, status: :copying} = stream, sid, now) do
    grace = Keyword.get(ctx.child_options, :fork_copy_grace, @default_fork_copy_grace)

    if now - stream.meta.created_ms >= grace do
      Logger.warning("fork #{inspect(stream.path)} was left copying; removing it")
      drop(stream, :ok)
    else
      {:reply, :ok}
    end
  end

  def repair_check(_ctx, %Stream{sid: sid, status: :gone} = stream, sid, _now),
    do: {:reply, {:ok, {:forks, stream.meta.forks}}}

  def repair_check(_ctx, stream, sid, _now),
    do: {:write, [{:delete, Keys.repair(sid)}], :ok, stream, []}

  # -- On the source --------------------------------------------------------

  @doc """
  Validates `fork` and registers it (idempotently). The fork offset is
  resolved in the durable `view`, since the fork copies durable data.
  """
  @spec register(Stream.t(), Stream.View.t(), Slap.Cluster.Shard.t(), binary(), map()) ::
          Protocol.decision()
  def register(%Stream{status: :absent}, _view, _ctx, _fork, _req),
    do: {:reply, {:error, :source_not_found}}

  def register(%Stream{status: :gone}, _view, _ctx, _fork, _req),
    do: {:reply, {:error, :source_gone}}

  def register(%Stream{status: :copying}, _view, _ctx, _fork, _req),
    do: {:reply, {:error, :unavailable}}

  def register(%Stream{status: :active, meta: meta} = stream, view, ctx, fork, req) do
    with :ok <- check_content_type(req[:content_type], meta.content_type),
         {:ok, offset, prefix} <- resolve_offset(ctx.db, view, req),
         :ok <- check_copy_size(offset, ctx.child_options) do
      reply =
        {:ok,
         %{
           content_type: meta.content_type,
           ttl_s: meta.ttl_s,
           expires_at_ms: meta.expires_at_ms,
           offset: offset,
           prefix: prefix,
           trim: view.trim
         }}

      if fork in meta.forks do
        {:reply, reply}
      else
        meta = %{meta | forks: [fork | meta.forks]}
        {:write, Batch.put_meta(stream.path, meta), reply, %{stream | meta: meta}, []}
      end
    else
      {:error, _} = error -> {:reply, error}
    end
  end

  # The official server compares the whole type, case-insensitively.
  defp check_content_type(nil, _source), do: :ok

  defp check_content_type(requested, source) do
    if String.downcase(requested) == String.downcase(source),
      do: :ok,
      else: {:error, :content_type_mismatch}
  end

  # The fork offset (default: the tail), and for a sub-offset, the resolved
  # offset (JSON: that many messages further) or the prefix of the next
  # message to copy as the fork's first own message (other types).
  defp resolve_offset(db, view, req) do
    %Stream.View{next_offset: tail, trim: trim} = view
    offset = req[:fork_offset] || tail
    sub = req[:fork_sub_offset] || 0

    cond do
      offset > tail ->
        {:error, {:bad_request, :fork_offset_beyond_source}}

      sub == 0 ->
        {:ok, offset, nil}

      # The data to resolve a sub-offset in has been trimmed.
      offset < trim ->
        {:error, {:bad_request, :fork_offset_trimmed}}

      true ->
        resolve_sub_offset(db, view, offset, sub)
    end
  end

  defp resolve_sub_offset(db, view, offset, sub) do
    %Stream.View{next_offset: tail, sid: sid, meta: meta} = view
    {messages, _} = Read.read_msgs(db, sid, offset, tail, 1)

    if ContentType.json?(meta.content_type),
      do: take_json(db, sid, offset, tail, sub, messages),
      else: binary_prefix(messages, offset, sub)
  end

  defp binary_prefix([{_, first} | _], offset, sub) when byte_size(first) >= sub,
    do: {:ok, offset, binary_part(first, 0, sub)}

  defp binary_prefix(_messages, _offset, _sub),
    do: {:error, {:bad_request, :invalid_fork_sub_offset}}

  # Advances past `n` JSON messages, one read each.
  defp take_json(_db, _sid, offset, _tail, 0, _first), do: {:ok, offset, nil}

  defp take_json(db, sid, _offset, tail, n, [{at, bytes}]) do
    next = Offset.advance(at, byte_size(bytes))
    {messages, _} = if n > 1, do: Read.read_msgs(db, sid, next, tail, 1), else: {[], nil}
    if n == 1, do: {:ok, next, nil}, else: take_json(db, sid, next, tail, n - 1, messages)
  end

  defp take_json(_db, _sid, _offset, _tail, _n, _none),
    do: {:error, {:bad_request, :invalid_fork_sub_offset}}

  defp check_copy_size(offset, opts) do
    max = Keyword.get(opts, :max_fork_copy_bytes, @default_max_fork_copy_bytes)

    if offset > max, do: {:error, :payload_too_large}, else: :ok
  end

  @doc """
  `fork` was deleted. A soft-deleted source with no forks left is removed
  too, and unregisters from its own source.
  """
  @spec unregister(Stream.t(), binary()) :: Protocol.decision()
  def unregister(%Stream{status: :absent}, _fork), do: {:reply, :ok}

  def unregister(%Stream{meta: meta} = stream, fork) do
    new_meta = %{meta | forks: List.delete(meta.forks, fork)}

    cond do
      new_meta == meta ->
        {:reply, :ok}

      stream.status == :gone and new_meta.forks == [] ->
        {:write, Batch.finalize(stream.path, stream.sid), :ok, Stream.absent(stream.path),
         Protocol.unregister_from(meta.fork_of)}

      true ->
        {:write, Batch.put_meta(stream.path, new_meta), :ok, %{stream | meta: new_meta}, []}
    end
  end

  @doc "Unregisters `fork` from `path` unless it is still a fork of `path`."
  @spec check(Slap.Cluster.Shard.t(), binary(), binary()) :: term()
  def check(ctx, path, fork) do
    case Streams.internal(fork, :fork_of, cluster: ctx.cluster) do
      {:ok, ^path} -> :ok
      {:ok, _other} -> Streams.internal(path, {:unregister_fork, fork}, cluster: ctx.cluster)
      {:error, _} -> :ok
    end
  end

  @doc "Unregisters `fork` from `source`, logging a failure."
  @spec unregister_from(Slap.Cluster.Shard.t(), binary(), binary()) :: :ok
  def unregister_from(ctx, source, fork) do
    case Streams.internal(source, {:unregister_fork, fork}, cluster: ctx.cluster) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "could not unregister fork #{inspect(fork)} from #{inspect(source)}: #{inspect(reason)}"
        )
    end
  end
end
