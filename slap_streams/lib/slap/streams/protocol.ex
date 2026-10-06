defmodule Slap.Streams.Protocol do
  @moduledoc false

  alias Slap.SlateDB
  alias Slap.Streams.{ContentType, Json, Offset, Stream}
  alias Slap.Streams.Store.{Batch, ForkOf, Keys, Meta, Producer, Tail}

  @typedoc """
  `{:reply, reply}` writes nothing. `{:write, ops, reply, stream, effects}`
  writes `ops` in one batch, after which `stream` is the stream's state;
  `reply` goes out once the batch is durable. `effects` run once the batch
  is written: `:kick_deleter` wakes the shard's deleter, and
  `{:unregister_from, source}` unregisters the stream from the source it
  was forked from.
  """
  @type decision ::
          {:reply, term()}
          | {:write, [SlateDB.write_op()], term(), Stream.t(), [effect()]}

  @type effect :: :kick_deleter | {:unregister_from, binary()}

  # -- Reads (PROTOCOL.md §5.5, §5.6) ---------------------------------------

  @spec head(Stream.View.t()) :: {:ok, Slap.Streams.info()} | {:error, term()}
  def head(%Stream.View{status: :active} = view), do: {:ok, info(view.meta, view.next_offset)}
  def head(%Stream.View{status: status}), do: Stream.error(status)

  @doc """
  A long-poll from `offset`: answered at once if there is data after it or
  the stream is closed, else `{:wait, offset}` (`:now` resolved).
  """
  @spec wait(Stream.View.t(), non_neg_integer() | :now) ::
          {:reply, term()} | {:wait, non_neg_integer()}
  def wait(%Stream.View{status: :active} = view, offset) do
    offset = if offset == :now, do: view.next_offset, else: offset

    case wake_reason(view, view.sid, offset) do
      nil -> {:wait, offset}
      reason -> {:reply, {:ok, reason}}
    end
  end

  def wait(%Stream.View{status: status}, _offset), do: {:reply, Stream.error(status)}

  @doc """
  Why a waiter on stream `sid` at `offset` is woken by the durable `view`,
  or `nil` while it keeps waiting. A different sid means the stream was
  deleted (and perhaps created again).
  """
  @spec wake_reason(Stream.View.t(), non_neg_integer(), non_neg_integer()) ::
          :data | :closed | :deleted | nil
  def wake_reason(%Stream.View{status: :active, sid: sid} = view, sid, offset) do
    cond do
      view.next_offset > offset -> :data
      view.meta.closed -> :closed
      true -> nil
    end
  end

  def wake_reason(_view, _sid, _offset), do: :deleted

  # -- Create (PROTOCOL.md §5.1, §4.2) ---------------------------------------

  @doc """
  What a create does: `:create` or `:fork` at an absent path, `:resume_fork`
  for a retry that finishes a fork whose copy was interrupted, or the reply
  for a path in use. A soft-deleted stream's path cannot be created again
  (§4.2).
  """
  @spec create_action(Stream.t(), map()) :: {:reply, term()} | :create | :fork | :resume_fork
  def create_action(%Stream{status: :absent}, req),
    do: if(req[:forked_from], do: :fork, else: :create)

  def create_action(%Stream{status: :gone}, _req), do: {:reply, {:error, :conflict}}

  def create_action(%Stream{status: status} = stream, req) do
    cond do
      not config_matches?(stream.meta, req, status) -> {:reply, {:error, :conflict}}
      status == :copying -> :resume_fork
      true -> {:reply, {:ok, :exists, info(stream.meta, stream.tail)}}
    end
  end

  # Content type, expiry and closure must match (§5.1), and for a fork the
  # source, the requested offset and the sub-offset (§4.2), as the official
  # server compares them.
  #
  # A fork whose copy was interrupted holds the values it inherited from its
  # source, which a retry of the same request (leaving them out) would not
  # match: while copying, the retry is compared with the original request.
  defp config_matches?(meta, req, status) do
    {content_type, ttl_s, expires_at_ms} = compared_config(meta, status)

    ContentType.matches?(content_type, req[:content_type]) and ttl_s == req[:ttl_s] and
      expires_at_ms == req[:expires_at_ms] and meta.closed == (req[:closed] == true) and
      fork_matches?(meta.fork_of, req)
  end

  defp compared_config(%Meta{fork_of: %ForkOf{} = f}, :copying),
    do: {f.requested_content_type, f.requested_ttl_s, f.requested_expires_at_ms}

  defp compared_config(meta, _status), do: {meta.content_type, meta.ttl_s, meta.expires_at_ms}

  defp fork_matches?(nil, req), do: req[:forked_from] == nil

  defp fork_matches?(fork, req) do
    fork.path == req[:forked_from] and
      req[:fork_offset] in [nil, fork.requested_offset || fork.offset] and
      (req[:fork_sub_offset] || 0) == fork.sub_offset
  end

  @doc "Checks a create (not a fork) and splits its initial body into messages."
  @spec create_messages(map()) :: {:ok, [binary()]} | {:error, term()}
  def create_messages(req) do
    with :ok <- check_expiry(req),
         do: split(ContentType.normalize(req[:content_type]), req[:body] || "", true)
  end

  @doc "Creates the stream (not a fork) as `sid`, with `messages` from `create_messages/1`."
  @spec create(Stream.t(), map(), [binary()], non_neg_integer(), integer()) :: decision()
  def create(%Stream{status: :absent, path: path}, req, messages, sid, now) do
    meta = %Meta{
      sid: sid,
      content_type: ContentType.normalize(req[:content_type]),
      created_ms: now,
      ttl_s: req[:ttl_s],
      expires_at_ms: req[:expires_at_ms],
      closed: req[:closed] == true
    }

    {stored, tail} = place(messages, 0)
    expiry_key = Stream.deadline(meta, now)
    tail_row = %Tail{next_offset: tail, last_access_ms: now, expiry_key_ms: expiry_key}

    stream = %Stream{
      path: path,
      status: :active,
      meta: meta,
      sid: sid,
      tail: tail,
      producers: %{},
      last_access: now,
      expiry_key: expiry_key,
      trim: 0
    }

    {:write, Batch.create(path, meta, tail_row, stored), {:ok, :created, info(meta, tail)},
     stream, []}
  end

  @spec check_expiry(map()) :: :ok | {:error, term()}
  def check_expiry(req) do
    if req[:ttl_s] != nil and req[:expires_at_ms] != nil,
      do: {:error, {:bad_request, :ttl_and_expires_at}},
      else: :ok
  end

  # -- Append and close (PROTOCOL.md §5.2, §5.2.1, §5.3) -------------------

  @spec append(Stream.t(), map()) :: decision()
  def append(%Stream{status: :active} = stream, req) do
    body = req[:body] || ""
    close = req[:close] == true
    producer = req[:producer]

    cond do
      body == "" and not close -> {:reply, {:error, {:bad_request, :empty_body}}}
      body == "" -> close_only(producer, stream)
      true -> append_body(req, body, close, producer, stream)
    end
  end

  def append(%Stream{status: status}, _req), do: {:reply, Stream.error(status)}

  defp close_only(nil, %Stream{meta: %Meta{closed: true}} = stream),
    do: {:reply, {:ok, result(:closed, stream.tail, true, nil)}}

  defp close_only(nil, stream) do
    meta = %{stream.meta | closed: true}
    {tail, index, key} = tail_and_index(stream, stream.tail)
    ops = Batch.append(stream.sid, [], tail, meta: {stream.path, meta}, expiry: index)

    {:write, ops, {:ok, result(:closed, stream.tail, true, nil)},
     %{stream | meta: meta, expiry_key: key}, []}
  end

  defp close_only({_id, epoch, seq} = producer, %Stream{meta: %Meta{closed: true}} = stream) do
    if stream.meta.closed_by == producer,
      do: {:reply, {:ok, result(:duplicate, stream.tail, true, {epoch, seq})}},
      else: {:reply, {:error, {:closed, stream.tail}}}
  end

  defp close_only({id, epoch, seq} = producer, stream) do
    case validate_producer(stream.producers, id, epoch, seq) do
      {:error, reason} ->
        error_reply(reason, stream)

      {:duplicate, last_seq} ->
        {:reply, {:ok, result(:duplicate, stream.tail, false, {epoch, last_seq})}}

      {:accept, p} ->
        meta = %{stream.meta | closed: true, closed_by: producer}
        {tail, index, key} = tail_and_index(stream, stream.tail)

        ops =
          Batch.append(stream.sid, [], tail,
            producers: [{id, p}],
            meta: {stream.path, meta},
            expiry: index
          )

        stream = %{
          stream
          | meta: meta,
            producers: Map.put(stream.producers, id, p),
            expiry_key: key
        }

        {:write, ops, {:ok, result(:closed, stream.tail, true, {epoch, seq})}, stream, []}
    end
  end

  defp append_body(req, body, close, producer, stream) do
    meta = stream.meta

    with :ok <- check_open(producer, stream),
         :ok <- check_content_type(req[:content_type], meta),
         {:ok, producer_update} <- check_producer(producer, stream),
         :ok <- check_stream_seq(req[:stream_seq], meta),
         {:ok, messages} <- split(meta.content_type, body, false) do
      {stored, new_tail} = place(messages, stream.tail)

      new_meta = %{
        meta
        | last_stream_seq: req[:stream_seq] || meta.last_stream_seq,
          closed: meta.closed or close,
          closed_by: if(close, do: producer, else: meta.closed_by)
      }

      producers =
        case producer_update do
          nil -> []
          {id, p} -> [{id, p}]
        end

      {tail, index, key} = tail_and_index(stream, new_tail)
      meta_op = if new_meta != meta, do: [meta: {stream.path, new_meta}], else: []

      ops =
        Batch.append(stream.sid, stored, tail, [producers: producers, expiry: index] ++ meta_op)

      echo =
        case producer do
          {_id, epoch, seq} -> {epoch, seq}
          nil -> nil
        end

      stream = %{
        stream
        | meta: new_meta,
          tail: new_tail,
          producers: Enum.into(producers, stream.producers),
          expiry_key: key
      }

      {:write, ops, {:ok, result(:appended, new_tail, new_meta.closed, echo)}, stream, []}
    else
      {:duplicate, reply} -> {:reply, reply}
      {:error, reason} -> error_reply(reason, stream)
    end
  end

  # A sequence gap is judged from this server's producer state, which is
  # behind if another node has taken the shard and accepted appends there
  # (a placement without leases, after this node was paused). Before
  # answering one, rewrite the stream's metadata, unchanged: a fenced
  # writer fails that write, so the request gets 503 and is retried on the
  # shard's owner. The other errors cannot come from state that is behind.
  defp error_reply({:producer_seq_gap, _, _} = reason, stream),
    do: {:write, Batch.put_meta(stream.path, stream.meta), {:error, reason}, stream, []}

  defp error_reply(reason, _stream), do: {:reply, {:error, reason}}

  # Closed comes first (§5.2 error precedence). A retry of the request that
  # closed the stream is a duplicate.
  defp check_open(producer, %Stream{meta: %Meta{closed: true}} = stream) do
    case producer do
      {_id, epoch, seq} when stream.meta.closed_by == producer ->
        {:duplicate, {:ok, result(:duplicate, stream.tail, true, {epoch, seq})}}

      _ ->
        {:error, {:closed, stream.tail}}
    end
  end

  defp check_open(_producer, _stream), do: :ok

  defp check_content_type(nil, _meta), do: :ok

  defp check_content_type(type, meta) do
    if ContentType.matches?(meta.content_type, type),
      do: :ok,
      else: {:error, :content_type_mismatch}
  end

  # The producer comes before Stream-Seq, as in the official server, so a
  # retry is deduplicated even if its Stream-Seq would now conflict.
  defp check_producer(nil, _stream), do: {:ok, nil}

  defp check_producer({id, epoch, seq}, stream) do
    case validate_producer(stream.producers, id, epoch, seq) do
      {:accept, p} ->
        {:ok, {id, p}}

      {:duplicate, last_seq} ->
        {:duplicate, {:ok, result(:duplicate, stream.tail, false, {epoch, last_seq})}}

      {:error, _} = error ->
        error
    end
  end

  defp check_stream_seq(nil, _meta), do: :ok
  defp check_stream_seq(_seq, %Meta{last_stream_seq: nil}), do: :ok

  defp check_stream_seq(seq, %Meta{last_stream_seq: last}) do
    if seq > last, do: :ok, else: {:error, :stream_seq_conflict}
  end

  @doc "PROTOCOL.md §5.2.1: what producer `id`'s request `{epoch, seq}` does."
  @spec validate_producer(%{binary() => Producer.t()}, binary(), non_neg_integer(), integer()) ::
          {:accept, Producer.t()} | {:duplicate, non_neg_integer()} | {:error, term()}
  def validate_producer(producers, id, epoch, seq),
    do: check_producer(Map.get(producers, id), epoch, seq)

  defp check_producer(nil, _epoch, seq) when seq != 0, do: {:error, {:producer_seq_gap, 0, seq}}
  defp check_producer(nil, epoch, _seq), do: {:accept, %Producer{epoch: epoch, last_seq: 0}}

  defp check_producer(%Producer{epoch: current}, epoch, _seq) when epoch < current,
    do: {:error, {:stale_epoch, current}}

  defp check_producer(%Producer{epoch: current}, epoch, seq) when epoch > current and seq != 0,
    do: {:error, {:bad_request, :new_epoch_must_start_at_zero}}

  defp check_producer(%Producer{epoch: current}, epoch, _seq) when epoch > current,
    do: {:accept, %Producer{epoch: epoch, last_seq: 0}}

  defp check_producer(%Producer{last_seq: last}, _epoch, seq) when seq <= last,
    do: {:duplicate, last}

  defp check_producer(%Producer{last_seq: last} = p, _epoch, seq) when seq == last + 1,
    do: {:accept, %{p | last_seq: seq}}

  defp check_producer(%Producer{last_seq: last}, _epoch, seq),
    do: {:error, {:producer_seq_gap, last + 1, seq}}

  # -- Expiry (PROTOCOL.md §5.1) --------------------------------------------

  @doc """
  An access at `now`, which resets a sliding TTL: `{:ok, stream}`, or a
  write when the expiry index moves (at most once per tenth of the TTL).
  """
  @spec touch(Stream.t(), integer()) :: {:ok, Stream.t()} | decision()
  def touch(%Stream{status: :active, meta: %Meta{ttl_s: ttl}} = stream, now) when ttl != nil do
    stream = %{stream | last_access: now}

    case moved_expiry_key(stream) do
      nil ->
        {:ok, stream}

      key ->
        tail = %Tail{next_offset: stream.tail, last_access_ms: now, expiry_key_ms: key}

        {:write, Batch.touch(stream.path, stream.sid, tail, stream.expiry_key), nil,
         %{stream | expiry_key: key}, []}
    end
  end

  def touch(stream, now), do: {:ok, %{stream | last_access: now}}

  @doc """
  The tail row to save when the server stops, if a sliding TTL's saved
  deadline is behind the last access.
  """
  @spec save_access(Stream.t()) :: [SlateDB.write_op()]
  def save_access(%Stream{status: :active, meta: %Meta{ttl_s: ttl}} = stream) when ttl != nil do
    case Stream.deadline(stream) do
      same when same == stream.expiry_key ->
        []

      new ->
        tail = %Tail{
          next_offset: stream.tail,
          last_access_ms: stream.last_access,
          expiry_key_ms: new
        }

        Batch.touch(stream.path, stream.sid, tail, stream.expiry_key)
    end
  end

  def save_access(_stream), do: []

  # The tail row for a write at `next_offset`, the expiry index move for a
  # sliding TTL, and the expiry key after it.
  defp tail_and_index(stream, next_offset) do
    {key, index} =
      case moved_expiry_key(stream) do
        nil -> {stream.expiry_key, []}
        new -> {new, Batch.expiry(stream.path, stream.sid, stream.expiry_key, new)}
      end

    tail = %Tail{next_offset: next_offset, last_access_ms: stream.last_access, expiry_key_ms: key}
    {tail, index, key}
  end

  # The new deadline of a sliding TTL, if the index entry should move to it.
  defp moved_expiry_key(%Stream{meta: %Meta{ttl_s: nil}}), do: nil

  defp moved_expiry_key(%Stream{meta: %Meta{ttl_s: ttl}} = stream) do
    new = Stream.deadline(stream)

    if stream.expiry_key == nil or new - stream.expiry_key >= div(ttl * 1000, 10),
      do: new,
      else: nil
  end

  @doc """
  From `Slap.Streams.Jobs.Expiry`: the index says `sid` expires at `at`.
  The stream has been removed already if it has expired. Otherwise the
  entry is stale (the stream is gone or was recreated) or behind (a
  sliding TTL moved on), and is replaced.
  """
  @spec expiry_check(Stream.t(), non_neg_integer(), integer()) :: decision()
  def expiry_check(%Stream{status: :active, sid: sid} = stream, sid, at) do
    key = Stream.deadline(stream)
    tail = %Tail{next_offset: stream.tail, last_access_ms: stream.last_access, expiry_key_ms: key}
    {:write, Batch.touch(stream.path, sid, tail, at), :ok, %{stream | expiry_key: key}, []}
  end

  def expiry_check(stream, sid, at),
    do: {:write, [{:delete, Keys.expiry(at, sid)}], :ok, stream, []}

  # -- Delete (PROTOCOL.md §5.4) and trim -----------------------------------

  @spec delete(Stream.t()) :: decision()
  def delete(%Stream{status: status}) when status in [:absent, :gone],
    do: {:reply, Stream.error(status)}

  def delete(stream), do: remove(stream, :ok)

  @doc """
  Deletes the stream (a delete, or expiry): a soft delete if it has forks,
  else a logical delete, after which a fork unregisters from its source.
  """
  @spec remove(Stream.t(), term()) :: decision()
  def remove(%Stream{meta: %Meta{forks: [_ | _]} = meta} = stream, reply) do
    gone = %{stream | status: :gone, meta: %{meta | soft_deleted: true}, expiry_key: nil}

    {:write, Batch.soft_delete(stream.path, meta, stream.expiry_key), reply, gone,
     [:kick_deleter]}
  end

  def remove(%Stream{meta: meta} = stream, reply) do
    {:write, Batch.logical_delete(stream.path, stream.sid, stream.expiry_key), reply,
     Stream.absent(stream.path), [:kick_deleter | unregister_from(meta.fork_of)]}
  end

  @doc false
  def unregister_from(nil), do: []
  def unregister_from(%{path: source}), do: [{:unregister_from, source}]

  @doc "Marks data before `offset` as gone (410)."
  @spec trim(Stream.t(), non_neg_integer()) :: decision()
  def trim(%Stream{status: :active} = stream, offset) do
    cond do
      offset > stream.tail ->
        {:reply, {:error, {:bad_request, :trim_offset_beyond_tail}}}

      offset <= stream.trim ->
        {:reply, :ok}

      true ->
        {:write, Batch.trim(stream.sid, offset), :ok, %{stream | trim: offset}, [:kick_deleter]}
    end
  end

  def trim(%Stream{status: status}, _offset), do: {:reply, Stream.error(status)}

  # -- Helpers --------------------------------------------------------------

  @doc false
  def split(content_type, body, allow_empty) do
    cond do
      # A create without a body. (An append without one never gets here.)
      body == "" ->
        {:ok, []}

      ContentType.json?(content_type) ->
        case Json.split(body, allow_empty) do
          {:ok, messages} -> {:ok, messages}
          {:error, reason} -> {:error, {:bad_request, reason}}
        end

      true ->
        {:ok, [body]}
    end
  end

  @doc false
  # Gives each message its offset, starting at `from`.
  def place(messages, from) do
    Enum.map_reduce(messages, from, fn bytes, offset ->
      {{offset, bytes}, Offset.advance(offset, byte_size(bytes))}
    end)
  end

  @doc false
  def info(meta, next_offset) do
    %{
      next_offset: next_offset,
      closed: meta.closed,
      content_type: meta.content_type,
      ttl_s: meta.ttl_s,
      expires_at_ms: meta.expires_at_ms
    }
  end

  defp result(result, next_offset, closed, producer),
    do: %{result: result, next_offset: next_offset, closed: closed, producer: producer}
end
