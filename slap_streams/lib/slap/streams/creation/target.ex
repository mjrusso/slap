defmodule Slap.Streams.Creation.Target do
  @moduledoc false

  alias Slap.Streams.{Protocol, Retention, Stream}

  alias Slap.Streams.Store.{
    Batch,
    Capture,
    Codec,
    CopyCreation,
    CopyProgress,
    Identity,
    Keys,
    Meta,
    PublishedCopy,
    PublishedReference,
    ReferenceCreation,
    Selection,
    Tail
  }

  @type completed :: {:ok, {:completed, :created | :exists, Slap.Streams.info()}}
  @type outcome(progress) :: {:ok, {:pending, progress}} | completed() | {:error, term()}

  def request(req) do
    %{
      forked_from: req[:forked_from],
      source_placement_key:
        req[:source_placement_key] || Slap.Streams.placement_key(req[:forked_from]),
      fork_offset: req[:fork_offset],
      fork_sub_offset: req[:fork_sub_offset] || 0,
      content_type: req[:content_type],
      ttl_s: req[:ttl_s],
      expires_at_ms: req[:expires_at_ms],
      closed: req[:closed] == true,
      body: req[:body] || ""
    }
  end

  def reservation(%Stream{status: :absent}, _kind, _req), do: {:reply, :needs_source}
  def reservation(%Stream{status: :gone}, _kind, _req), do: {:reply, {:error, :conflict}}

  def reservation(%Stream{status: :pending, meta: %{creation: %CopyCreation{} = c}}, :copy, req) do
    if copy_selection(c.request) == copy_selection(req),
      do: {:reply, {:ok, {:pending, c}}},
      else: {:reply, {:error, :conflict}}
  end

  def reservation(%Stream{status: :pending, meta: %{creation: c}}, kind, req) do
    if kind(c) == kind and c.request == req,
      do: {:reply, {:ok, {:pending, c}}},
      else: {:reply, {:error, :conflict}}
  end

  def reservation(
        %Stream{status: :active, meta: %{creation: %PublishedCopy{} = c}} = s,
        :copy,
        req
      ) do
    if copy_selection(c) == copy_selection(req),
      do: completed(s),
      else: {:reply, {:error, :conflict}}
  end

  def reservation(
        %Stream{status: :active, meta: %{creation: %PublishedReference{} = c}} = s,
        :reference,
        req
      ) do
    if reference_matches?(c, req) and Protocol.config_matches?(s.meta, req),
      do: completed(s),
      else: {:reply, {:error, :conflict}}
  end

  def reservation(_stream, _kind, _req), do: {:reply, {:error, :conflict}}

  def reserve(ctx, %Stream{status: :absent, path: path}, kind, req, source, sid, now) do
    variant = if kind == :reference, do: ReferenceCreation, else: CopyCreation

    c =
      struct!(variant,
        request: req,
        source: source,
        target: Identity.new(ctx, path, sid)
      )

    meta = %Meta{sid: sid, created_ms: now, creation: c}
    stream = Stream.pending(path, meta)

    ops =
      Batch.put_meta(path, meta) ++
        [{:put, Keys.tail(sid), Codec.encode(%Tail{last_access_ms: now})}] ++
        Batch.repair(path, sid)

    {:write, ops, {:ok, {:pending, c}}, stream, []}
  end

  def selection(c) do
    %Selection{
      kind: kind(c),
      source: c.source,
      target: c.target,
      fork_offset: c.request.fork_offset,
      fork_sub_offset: c.request.fork_sub_offset,
      content_type: c.request.content_type
    }
  end

  defp kind(%ReferenceCreation{}), do: :reference
  defp kind(%CopyCreation{}), do: :copy

  defp reference_matches?(%PublishedReference{capture: cap} = c, req) do
    cap.source.path == req.forked_from and c.source_placement_key == req.source_placement_key and
      req.fork_offset in [nil, c.requested_offset || cap.offset] and
      req.fork_sub_offset == c.sub_offset
  end

  defp copy_selection(%PublishedCopy{} = c),
    do: {c.source.path, c.source_placement_key, c.requested_offset}

  defp copy_selection(req), do: {req.forked_from, req.source_placement_key, req.fork_offset}

  def capture(s, target, cap, now) do
    case {creation_state(s, target), cap} do
      {{:published, %PublishedCopy{}}, _cap} ->
        completed(s)

      {{:pending, %CopyCreation{progress: %CopyProgress{capture: ^cap}} = c}, %Capture{}} ->
        {:reply, {:ok, {:pending, c}}}

      {{:pending, %CopyCreation{progress: nil} = c}, %Capture{target: ^target, source: source}}
      when source == c.source ->
        c = %{c | progress: %CopyProgress{capture: cap, cursor: cap.start, progressed_ms: now}}
        meta = %{s.meta | creation: c}
        {:write, Batch.put_meta(s.path, meta), {:ok, {:pending, c}}, %{s | meta: meta}, []}

      _different_creation ->
        {:reply, {:error, :conflict}}
    end
  end

  def page(s, target, from, messages, next, now) do
    case creation_state(s, target) do
      {:pending, %CopyCreation{progress: %CopyProgress{cursor: ^from}} = c} ->
        copy_page(s, c, from, messages, next, now)

      {:pending, %CopyCreation{progress: %CopyProgress{cursor: cursor}}} when from < cursor ->
        {:reply, {:ok, {:pending, cursor}}}

      {:published, %PublishedCopy{offset: offset}} when from < offset ->
        completed(s)

      _different_creation_or_cursor ->
        {:reply, {:error, :conflict}}
    end
  end

  defp copy_page(s, c, from, messages, next, now) do
    if valid_page?(c.progress.capture, from, messages, next) do
      progress = %{c.progress | cursor: next, progressed_ms: now}
      meta = %{s.meta | creation: %{c | progress: progress}}

      {:write, Batch.messages(s.sid, messages) ++ Batch.put_meta(s.path, meta),
       {:ok, {:pending, next}}, %{s | meta: meta}, []}
    else
      {:reply, {:error, :conflict}}
    end
  end

  defp valid_page?(%Capture{offset: until}, from, messages, next) do
    next > from and next <= until and
      Enum.all?(messages, fn {at, _} -> at >= from and at < until end)
  end

  defp valid_page?(_cap, _from, _messages, _next), do: false

  def activate(s, target, cap, now) do
    case {creation_state(s, target), cap} do
      {{:published, _creation}, _cap} ->
        completed(s)

      {{:pending, %ReferenceCreation{source: source} = c},
       %Capture{target: ^target, source: source} = cap} ->
        finish(s, c, cap, now)

      {{:pending, %CopyCreation{progress: %CopyProgress{capture: cap, cursor: cursor}} = c}, nil} ->
        if cursor < cap.offset,
          do: {:reply, {:error, :unavailable}},
          else: finish(s, c, cap, now)

      _different_creation ->
        {:reply, {:error, :conflict}}
    end
  end

  def reconcile(s, target, {:error, _reason} = error) do
    case creation_state(s, target) do
      {:published, _creation} -> completed(s)
      _pending_or_replaced -> {:reply, error}
    end
  end

  defp creation_state(
         %Stream{status: :pending, meta: %{creation: %{__struct__: variant, target: target} = c}},
         target
       )
       when variant in [ReferenceCreation, CopyCreation],
       do: {:pending, c}

  defp creation_state(
         %Stream{status: :active, meta: %{creation: %PublishedCopy{target: target} = c}},
         target
       ),
       do: {:published, c}

  defp creation_state(
         %Stream{
           status: :active,
           meta: %{creation: %PublishedReference{capture: %{target: target}} = c}
         },
         target
       ),
       do: {:published, c}

  defp creation_state(_stream, _target), do: :replaced

  defp completed(s), do: {:reply, {:ok, {:completed, :exists, Protocol.info(s.meta, s.tail)}}}

  defp finish(s, c, cap, now) do
    {ttl, at} = expiry(c.request, cap)

    with true <- at == nil or now < at,
         {:ok, body} <- Protocol.split(cap.content_type, c.request.body, true) do
      publish_body(s, c, cap, now, ttl, at, body)
    else
      false -> Retention.remove(s, {:error, :source_gone})
      {:error, _reason} = error -> Retention.remove(s, error)
    end
  end

  defp publish_body(s, c, cap, now, ttl, at, body) do
    {messages, tail} =
      Protocol.place(if(cap.prefix, do: [cap.prefix | body], else: body), cap.offset)

    published =
      case kind(c) do
        :reference ->
          %PublishedReference{
            capture: %{cap | prefix: nil},
            source_placement_key: c.request.source_placement_key,
            requested_offset: c.request.fork_offset,
            sub_offset: c.request.fork_sub_offset
          }

        :copy ->
          %PublishedCopy{
            source: c.source,
            target: c.target,
            source_placement_key: c.request.source_placement_key,
            requested_offset: c.request.fork_offset,
            offset: cap.offset
          }
      end

    meta = %{
      s.meta
      | creation: published,
        content_type: cap.content_type,
        ttl_s: ttl,
        expires_at_ms: at,
        closed: if(kind(c) == :copy, do: cap.closed, else: c.request.closed)
    }

    {meta, reclaim} = Retention.authorize(meta, cap.start)
    key = Stream.deadline(meta, now)
    tail_row = %Tail{next_offset: tail, last_access_ms: now, expiry_key_ms: key}

    ops = [
      {:delete, Keys.repair(s.sid)}
      | Batch.put_meta(s.path, meta) ++
          Batch.messages(s.sid, messages) ++
          Batch.touch(s.path, s.sid, tail_row, nil) ++ Batch.trim(s.sid, cap.start) ++ reclaim
    ]

    active = %{
      s
      | status: :active,
        meta: meta,
        tail: tail,
        trim: cap.start,
        last_access: now,
        expiry_key: key
    }

    {:write, ops, {:ok, {:completed, :created, Protocol.info(meta, tail)}}, active,
     [{:repair_source, c.source, c.target}]}
  end

  defp expiry(%{ttl_s: ttl}, _cap) when ttl != nil, do: {ttl, nil}
  defp expiry(%{expires_at_ms: at}, _cap) when at != nil, do: {nil, at}
  defp expiry(_req, cap), do: {cap.ttl_s, cap.expires_at_ms}
end
