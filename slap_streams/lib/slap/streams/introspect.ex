defmodule Slap.Streams.Introspect do
  @moduledoc false

  alias Slap.Cluster.Inspection
  alias Slap.Cluster.Inspection.{Read, Value}
  alias Slap.SlateDB
  alias Slap.SlateDB.Iterator
  alias Slap.Streams.Introspect.{Cursor, Logical, Messages}
  alias Slap.Streams.Store.{Capture, Codec, Keys, Meta, Producer, Tail}
  alias Slap.Streams.Store.Read, as: Store

  @max_offset 0xFFFFFFFFFFFFFFFF
  @durable [durability: :remote, cache_blocks: false]
  @records %{meta: Meta, tail: Tail, producer: Producer, registration: Capture}

  def shard_for(cluster, version, path) do
    with :ok <- Inspection.check_version(version),
         do: Slap.Cluster.shard_for(cluster, Slap.Streams.placement_key(path))
  end

  def scan(cluster, shard, version, budget, opts),
    do:
      Inspection.request(cluster, shard, version, budget, fn ctx, deadline ->
        Read.page(ctx, deadline, opts, &decode/2)
      end)

  @doc "The scan prefix of the metadata rows of streams whose path starts with `prefix`."
  def path_prefix(prefix), do: Keys.meta(prefix)

  @doc "Returns `{:ok, path}` for a stream's metadata key, or `:error`."
  def path(key) do
    case Keys.parse(key) do
      {:ok, %{kind: :meta, path: path}} -> {:ok, path}
      _other -> :error
    end
  end

  def list(cluster, shard, version, budget, opts),
    do: scan(cluster, shard, version, budget, Keyword.put_new(opts, :prefix, Keys.prefix(:meta)))

  def logical(cluster, shard, version, budget, opts) do
    Inspection.request(cluster, shard, version, budget, fn ctx, deadline ->
      with {:ok, view} <- view(ctx, deadline, opts),
           {:ok, logical} <- Logical.describe(view.state, view.shard, opts),
           do: {:ok, view |> Map.delete(:state) |> Map.put(:logical, logical)}
    end)
  end

  def stream(cluster, shard, version, budget, opts) do
    Inspection.request(cluster, shard, version, budget, fn ctx, deadline ->
      with {:ok, view} <- view(ctx, deadline, opts), do: {:ok, Map.delete(view, :state)}
    end)
  end

  # The metadata row and the tail, trim and deletion rows for display, with
  # the stored state that plans use: rows can be truncated or lose their
  # decoded fields. Both come from one read of each record.
  defp view(ctx, deadline, opts) do
    key = opts |> Keyword.fetch!(:path) |> Keys.meta()

    with {:ok, fetched} <- Read.fetch(ctx, deadline, key),
         {:ok, view} <- Read.row(ctx, key, fetched, opts, &decode/2) do
      {state, records} =
        case stored_state(ctx, deadline, fetched) do
          {:ok, nil, nil} ->
            {{:ok, nil}, %{diagnostic: :not_found_custom_placement_key_possible}}

          {:ok, state, values} ->
            {{:ok, state},
             Map.new(values, fn {name, {key, value}} -> {name, row(ctx, key, value, opts)} end)}

          error ->
            {error, %{diagnostic: :metadata_unavailable}}
        end

      {:ok, Map.merge(view, %{state: state, records: records})}
    end
  end

  @doc """
  A stream's stored metadata, tail and trim, read whole and decoded:
  `{:ok, %{meta: meta, tail: next_offset | nil, trim: offset}}`, or
  `{:ok, nil}` when no stream has `path`. Shares the caller's inspection
  admission.
  """
  def state(ctx, deadline, path) do
    with {:ok, fetched} <- Read.fetch(ctx, deadline, Keys.meta(path)),
         {:ok, state, _values} <- stored_state(ctx, deadline, fetched),
         do: {:ok, state}
  end

  defp stored_state(_ctx, _deadline, nil), do: {:ok, nil, nil}

  defp stored_state(ctx, deadline, fetched) do
    with {:ok, %Meta{sid: sid} = meta} <- stored(fetched, Meta),
         {:ok, values} <- values(ctx, deadline, sid),
         {:ok, offsets} <- offsets(values),
         do: {:ok, Map.put(offsets, :meta, meta), values}
  end

  # The tail, trim and deletion records of `sid`, as `{key, fetched}`.
  defp values(ctx, deadline, sid) do
    [tail: Keys.tail(sid), trim: Keys.trim(sid), deletion: Keys.delete_pending(sid)]
    |> Enum.reduce_while({:ok, %{}}, fn {name, key}, {:ok, values} ->
      case Read.fetch(ctx, deadline, key) do
        {:ok, fetched} -> {:cont, {:ok, Map.put(values, name, {key, fetched})}}
        error -> {:halt, error}
      end
    end)
  end

  defp offsets(%{tail: {_tail_key, tail}, trim: {_trim_key, trim}}) do
    with {:ok, tail} <- stored(tail, Tail),
         {:ok, trim} <- whole(trim),
         {:ok, trim} <- Store.parse_trim(trim),
         do: {:ok, %{tail: next_offset(tail), trim: trim}}
  end

  defp next_offset(%Tail{next_offset: offset}), do: offset
  defp next_offset(nil), do: nil

  defp stored(fetched, module),
    do: with({:ok, binary} <- whole(fetched), do: parse(binary, module))

  defp whole(nil), do: {:ok, nil}
  defp whole(%{value: value, value_size: size}) when byte_size(value) == size, do: {:ok, value}
  defp whole(_fetched), do: {:error, :oversized_value}

  defp parse(nil, _module), do: {:ok, nil}

  defp parse(binary, module) do
    case Codec.parse(binary, &Value.term/1) do
      {:ok, %{__struct__: ^module}} = record -> record
      {:ok, _record} -> {:error, :unexpected_record}
      error -> error
    end
  end

  def messages(cluster, shard, version, budget, opts) do
    Inspection.request(cluster, shard, version, budget, &messages(&1, &2, opts))
  end

  def messages(ctx, deadline, opts) do
    with {:ok, scan_opts} <- Cursor.options(opts, Keyword.fetch!(opts, :sid)),
         {:ok, page} <- message_records(ctx, deadline, scan_opts) do
      {page, state} = Messages.fragments(page, scan_opts[:fragment_state])
      Cursor.page(page, state)
    end
  end

  @doc """
  The whole messages of `sid` from `from` to `until`, read from its durable
  view, with the deadline checked before each native batch. The caller
  bounds `until - from`.
  """
  def read_messages(_ctx, _deadline, _sid, from, until) when from >= until, do: {:ok, []}

  def read_messages(ctx, deadline, sid, from, until) do
    opts = Keys.msg_range(sid, from, until) ++ @durable

    # Each message counts 4 bytes more than its body, so the bodies total
    # less than `until - from` and the cap cannot stop the join early.
    with :ok <- Read.check_deadline(deadline),
         {:ok, iter} <- storage(SlateDB.iterator(ctx.db, opts)),
         {:ok, rows} <- message_rows(iter, deadline, []) do
      case Store.join_msgs(rows, until, until - from) do
        {messages, ^until} -> {:ok, messages}
        {_messages, _next} -> {:error, :incomplete}
      end
    end
  end

  defp message_rows(iter, deadline, batches) do
    with :ok <- Read.check_deadline(deadline) do
      case storage(Iterator.next_batch(iter)) do
        {:ok, []} -> {:ok, batches |> Enum.reverse() |> Enum.concat()}
        {:ok, rows} -> message_rows(iter, deadline, [rows | batches])
        error -> error
      end
    end
  end

  defp storage({:error, %SlateDB.Error{} = error}), do: Read.storage_error(error)
  defp storage(result), do: result

  defp message_records(ctx, deadline, opts) do
    sid = Keyword.fetch!(opts, :sid)

    with {:ok, values} <- values(ctx, deadline, sid),
         {:ok, offsets} <- offsets(values),
         {:ok, tail} <- row(ctx, values.tail, opts),
         {:ok, trim} <- row(ctx, values.trim, opts),
         {:ok, deleted} <- row(ctx, values.deletion, opts),
         {:ok, opts, ending} <- ending(ctx, deadline, sid, opts, offsets.tail),
         {:ok, page} <-
           Read.page(ctx, deadline, page_opts(sid, opts, offsets.tail), &decode/2) do
      page = sid |> message_page(opts, offsets.tail, page) |> continue(sid, ending)

      {:ok,
       Map.merge(page, %{
         sid: sid,
         offsets: offsets,
         tail: tail.row,
         trim: trim.row,
         deleted: deleted.row,
         from: Keyword.get(opts, :from, 0),
         until: Keyword.get(opts, :until),
         interpretation: :own_records
       })}
    end
  end

  # `:before` bounds the page by an offset, or the tail for `:end`.
  # The reverse scan finds a page of trailing rows; start/2 aligns the start
  # to a message boundary, which can precede those rows for a large message.
  defp ending(ctx, deadline, sid, opts, tail) do
    case Keyword.pop(opts, :before) do
      {nil, opts} ->
        {:ok, opts, nil}

      {_before, _opts} when is_nil(tail) ->
        {:error, :tail_unavailable}

      {before, opts} when before == :end or (is_integer(before) and before >= 0) ->
        bound = end_offset(tail, opts)
        from = Keyword.get(opts, :from, 0)
        stop = Logical.ending(before, from, bound)
        # Omit :max_key_bytes so the reverse scan counts full-sized message
        # keys. It must not select more rows than a forward page can hold;
        # start/2 also needs the keys to find a message boundary.
        limits = Keyword.take(opts, [:limit, :target_bytes, :max_value_bytes])

        with {:ok, last} <-
               Read.last(ctx, deadline, limits ++ Keys.msg_range(sid, from, stop), &decode/2) do
          opts = Keyword.merge(opts, [until: stop] ++ start(last, from))
          {:ok, opts, %{stop: stop, bound: bound}}
        end

      {_before, _opts} ->
        {:error, {:invalid, "before must be a nonnegative offset or :end"}}
    end
  end

  defp start(%{stopped: :end}, from), do: [from: from]

  # A message whose first part is not among the rows starts before them. A
  # row whose key is not a message key starts the page at that key, so the
  # page shows its diagnostic.
  defp start(%{rows: rows}, _from) do
    rows
    |> Enum.map(&{&1.key, message_key(&1.key)})
    |> Enum.chunk_by(fn {key, parsed} -> if parsed, do: parsed.offset, else: key end)
    |> first_whole()
  end

  defp first_whole([[{key, nil} | _] | _rest]), do: [cursor: key, cursor_exclusive: false]
  defp first_whole([[{_key, %{part: 0, offset: offset}} | _] | _rest]), do: [from: offset]
  defp first_whole([[{_key, %{offset: offset}} | _]]), do: [from: offset]
  defp first_whole([_partial | rest]), do: first_whole(rest)

  defp message_key(key) do
    case Keys.parse(key) do
      {:ok, %{kind: :message} = parsed} -> parsed
      _malformed -> nil
    end
  end

  defp continue(page, _sid, nil), do: page

  defp continue(%{cursor: nil} = page, sid, %{stop: stop, bound: bound}) when stop < bound,
    do: Map.merge(page, %{cursor: Keys.msg(sid, stop, 0), cursor_exclusive: false, before: stop})

  defp continue(page, _sid, %{stop: stop}), do: Map.put(page, :before, stop)

  # A capture can end inside a message, so the physical boundary of the
  # page's last message is the next message key, found past `until`.
  defp page_opts(sid, opts, nil), do: Keyword.merge(opts, message_bounds(sid, opts, nil))

  defp page_opts(sid, opts, tail) do
    opts
    |> Keyword.merge(message_bounds(sid, opts, tail))
    |> Keyword.put(:lookahead_lt, Keys.msg(sid, tail, 0))
  end

  defp message_page(sid, opts, tail, %{stopped: stopped} = page)
       when stopped in [:end, :limit] and is_integer(tail) do
    {next_key, page} = Map.pop(page, :next_key)
    physical = next_offset(next_key, sid, tail)
    page = Map.put(page, :next_message_offset, physical)

    if is_integer(physical) and physical >= end_offset(tail, opts),
      do: Map.merge(page, %{cursor: nil, cursor_exclusive: false, stopped: :end}),
      else: page
  end

  defp message_page(_sid, _opts, _tail, page),
    do: page |> Map.delete(:next_key) |> Messages.end_at_known_boundary()

  defp next_offset(nil, _sid, tail), do: tail

  defp next_offset(key, sid, _tail) do
    case Keys.parse(key) do
      {:ok, %{kind: :message, sid: ^sid, offset: offset}} -> offset
      _ -> nil
    end
  end

  defp end_offset(nil, _opts), do: nil
  defp end_offset(tail, opts), do: min(Keyword.get(opts, :until, tail), tail)

  defp message_bounds(sid, opts, tail) do
    until = end_offset(tail, opts) || Keyword.get(opts, :until, @max_offset)
    Keys.msg_range(sid, Keyword.get(opts, :from, 0), until)
  end

  def retention(cluster, shard, version, budget, opts) do
    Inspection.request(cluster, shard, version, budget, fn ctx, deadline ->
      sid = Keyword.fetch!(opts, :sid)

      prefix =
        if Keyword.get(opts, :pins, false),
          do: Keys.pin_prefix(sid),
          else: Keys.registration_prefix(sid)

      with {:ok, bound} <- record(ctx, deadline, Keys.reclaim(sid), opts),
           {:ok, trim} <- record(ctx, deadline, Keys.trim(sid), opts),
           {:ok, page} <-
             Read.page(ctx, deadline, Keyword.put(opts, :prefix, prefix), &decode/2) do
        {:ok, Map.merge(page, %{reclamation: bound.row, trim: trim.row, sid: sid})}
      end
    end)
  end

  defp record(ctx, deadline, key, opts) do
    with {:ok, fetched} <- Read.fetch(ctx, deadline, key), do: row(ctx, key, fetched, opts)
  end

  defp row(ctx, {key, fetched}, opts), do: row(ctx, key, fetched, opts)

  # Tail, trim, deletion and reclamation records are small; 512 bytes holds them.
  defp row(ctx, key, fetched, opts),
    do: Read.row(ctx, key, fetched, Keyword.put(opts, :max_value_bytes, 512), &decode/2)

  def decode(key, value) do
    with {:ok, fields} <- Keys.parse(key),
         {:ok, decoded} <- value(fields.kind, value) do
      {:ok, Map.put(fields, :value, decoded)}
    end
  end

  defp value(kind, binary) when is_map_key(@records, kind) do
    with {:ok, record} <- parse(binary, Map.fetch!(@records, kind)), do: {:ok, project(record)}
  end

  defp value(kind, <<number::64>>) when kind in [:next_sid, :delete_pending, :trim, :reclaim],
    do: {:ok, number}

  defp value(kind, binary) when kind in [:next_sid, :delete_pending, :trim, :reclaim],
    do: {:error, {:malformed_integer, byte_size(binary)}}

  defp value(_kind, _binary), do: {:ok, :bytes}

  defp project(%Meta{} = meta), do: Map.merge(Value.plain(meta), Logical.metadata(meta))
  defp project(record), do: Value.plain(record)
end
