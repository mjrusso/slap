defmodule Slap.Streams.Retention do
  @moduledoc false

  alias Slap.Streams.Stream

  alias Slap.Streams.Store.{
    Batch,
    Capture,
    CopyCreation,
    CopyProgress,
    Identity,
    Keys,
    Meta,
    PublishedReference,
    Read,
    Selection
  }

  @max_offset 0xFFFFFFFFFFFFFFFF
  @default_max_depth 8
  @default_creation_grace :timer.hours(1)

  def register(stream, view, ctx, %Selection{} = selection, resolved) do
    with :ok <- same_source(stream, ctx, selection.source),
         {:ok, registration} <- Read.get_registration(ctx.db, stream.sid, selection.target) do
      case registration do
        %Capture{} = cap ->
          {:reply, {:ok, cap}}

        nil ->
          register_new(stream, view, ctx, selection, resolved)
      end
    else
      {:error, %Slap.SlateDB.Error{}} -> {:reply, {:error, :unavailable}}
      {:error, _} = error -> {:reply, error}
    end
  end

  defp register_new(%Stream{status: :pending}, _view, _ctx, _selection, _resolved),
    do: {:reply, {:error, :unavailable}}

  defp register_new(%Stream{status: :gone}, _view, _ctx, _selection, _resolved),
    do: {:reply, {:error, :source_gone}}

  defp register_new(stream, view, ctx, selection, resolved) do
    with :ok <- lifecycle_durable(stream, view),
         :ok <- content_type(selection.content_type, view.meta.content_type),
         {:ok, offset, prefix} <- resolved_offset(view, selection, resolved),
         start = min(view.trim, offset),
         :ok <- reclaimable(stream.meta, start, offset),
         depth =
           if(start < offset and selection.kind == :reference,
             do: Meta.depth(view.meta) + 1,
             else: 0
           ),
         :ok <- depth_allowed(depth, ctx.child_options) do
      cap = %Capture{
        source: selection.source,
        target: selection.target,
        start: start,
        offset: offset,
        prefix: prefix,
        content_type: view.meta.content_type,
        ttl_s: view.meta.ttl_s,
        expires_at_ms: view.meta.expires_at_ms,
        closed: view.meta.closed,
        depth: depth
      }

      meta = %{
        stream.meta
        | registration_count: stream.meta.registration_count + 1,
          pin_start: add_pin(stream.meta.pin_start, cap)
      }

      ops =
        Batch.registration(cap) ++
          Batch.put_meta(stream.path, meta) ++ Batch.repair(stream.path, stream.sid)

      {:write, ops, {:ok, cap}, %{stream | meta: meta}, []}
    else
      {:resolve, _} = reply -> {:reply, reply}
      {:error, _} = error -> {:reply, error}
    end
  end

  defp add_pin(pin, %Capture{start: start, offset: offset}) when start < offset,
    do: if(pin == nil, do: start, else: min(pin, start))

  defp add_pin(pin, _cap), do: pin

  defp same_source(%Stream{status: :absent}, _ctx, _id), do: {:error, :source_not_found}
  defp same_source(%{sid: sid}, %{n: shard}, %Identity{sid: sid, shard: shard}), do: :ok
  defp same_source(_s, _ctx, _id), do: {:error, :source_gone}

  defp lifecycle_durable(%{status: status, trim: trim}, %{status: status, trim: trim}), do: :ok
  defp lifecycle_durable(_stream, _view), do: {:error, :unavailable}

  defp reclaimable(_meta, start, offset) when start == offset, do: :ok
  defp reclaimable(%{reclaimed_before: floor}, start, _offset) when start >= floor, do: :ok
  defp reclaimable(_meta, _start, _offset), do: {:error, :unavailable}

  defp depth_allowed(depth, opts) do
    if depth <= Keyword.get(opts, :max_fork_depth, @default_max_depth),
      do: :ok,
      else: {:error, {:bad_request, :max_fork_depth_exceeded}}
  end

  defp content_type(nil, _type), do: :ok

  defp content_type(requested, type) do
    if String.downcase(requested) == String.downcase(type),
      do: :ok,
      else: {:error, :content_type_mismatch}
  end

  defp resolved_offset(view, req, resolved) do
    offset = req.fork_offset || view.next_offset

    cond do
      offset > view.next_offset -> {:error, {:bad_request, :fork_offset_beyond_source}}
      req.fork_sub_offset == 0 -> {:ok, offset, nil}
      offset < view.trim -> {:error, {:bad_request, :fork_offset_trimmed}}
      resolved == nil -> {:resolve, view}
      true -> validate_resolution(view, resolved)
    end
  end

  defp validate_resolution(view, {sid, trim, tail, offset, prefix})
       when sid == view.sid and trim == view.trim and tail <= view.next_offset and offset <= tail,
       do: {:ok, offset, prefix}

  defp validate_resolution(_view, _resolved), do: {:error, :unavailable}

  def registered?(db, %Meta{sid: sid} = meta, cap) when sid == cap.source.sid do
    # Publication moves sub-offset bytes to the target, so compacting the
    # prefix must not invalidate the registration.
    with false <- Meta.pending?(meta),
         {:ok, %Capture{} = registered} <-
           Read.get_registration(db, sid, cap.target, durability: :remote) do
      %{registered | prefix: nil} == %{cap | prefix: nil}
    else
      _ -> false
    end
  end

  def registered?(_db, _meta, _cap), do: false

  def compact_registration(ctx, %Stream{sid: sid, meta: meta} = s, %Identity{sid: sid}, target)
      when meta != nil do
    case Read.get_registration(ctx.db, sid, target) do
      {:ok, %Capture{prefix: prefix} = cap} when prefix != nil ->
        {:write, Batch.registration(%{cap | prefix: nil}), :ok, s, []}

      {:ok, _} ->
        {:reply, :ok}

      error ->
        {:reply, error}
    end
  end

  def compact_registration(_ctx, _s, _source, _target), do: {:reply, :ok}

  def unregister(ctx, %Stream{sid: sid} = s, %Identity{sid: sid}, target) do
    case Read.get_registration(ctx.db, sid, target) do
      {:ok, %Capture{} = cap} -> release_registration(ctx, s, cap)
      {:ok, nil} -> {:reply, :ok}
      error -> {:reply, error}
    end
  end

  def unregister(_ctx, _s, _source, _target), do: {:reply, :ok}

  defp release_registration(ctx, s, %Capture{} = cap) do
    meta = %{
      s.meta
      | registration_count: s.meta.registration_count - 1,
        pin_start: Read.next_pin(ctx.db, s.sid, Keys.pin(s.sid, cap.start, cap.target))
    }

    {meta, reclaim} = authorize(meta, if(s.status == :gone, do: @max_offset, else: s.trim))
    s = %{s | meta: meta}

    if s.status == :gone and meta.registration_count == 0 do
      {:write, ops, reply, next, effects} = finalize(s, :ok)
      {:write, Batch.unregister(cap) ++ ops, reply, next, effects}
    else
      ops =
        Batch.unregister(cap) ++
          Batch.put_meta(s.path, meta) ++
          reclaim ++
          if(meta.registration_count == 0 and s.status == :active,
            do: [{:delete, Keys.repair(s.sid)}],
            else: []
          )

      {:write, ops, :ok, s, [:kick_deleter]}
    end
  end

  defp finalize(s, reply) do
    repair =
      case s.meta.creation do
        %PublishedReference{capture: cap} -> [{:repair_source, cap.source, cap.target}]
        %{source: source, target: target} -> [{:repair_source, source, target}]
        _ -> []
      end

    {:write, Batch.logical_delete(s.path, s.sid, s.expiry_key), reply, Stream.absent(s.path),
     [:kick_deleter | repair]}
  end

  def authorize(meta, logical) do
    floor = if meta.pin_start == nil, do: logical, else: min(logical, meta.pin_start)
    floor = max(meta.reclaimed_before, floor)
    ops = if floor == 0, do: [], else: [{:put, Keys.reclaim(meta.sid), <<floor::64>>}]
    {%{meta | reclaimed_before: floor}, ops}
  end

  def remove(%Stream{meta: %{registration_count: 0}} = s, reply), do: finalize(s, reply)

  def remove(s, reply) do
    {meta, reclaim} = authorize(%{s.meta | soft_deleted: true}, @max_offset)

    ops =
      Batch.put_meta(s.path, meta) ++
        reclaim ++
        Batch.repair(s.path, s.sid) ++
        Batch.expiry(s.path, s.sid, s.expiry_key, nil)

    {:write, ops, reply, %{s | status: :gone, meta: meta, expiry_key: nil}, [:kick_deleter]}
  end

  def repair_check(ctx, %Stream{sid: sid, status: :pending} = s, sid, now) do
    if repair_due?(s.meta, ctx.child_options, now),
      do: remove(s, :ok),
      else: {:reply, :ok}
  end

  def repair_check(_ctx, %Stream{sid: sid, meta: %{registration_count: 0}} = s, sid, _now),
    do: {:write, [{:delete, Keys.repair(sid)}], :ok, s, []}

  def repair_check(_ctx, %Stream{sid: sid} = s, sid, _now) when s.meta != nil,
    do: {:reply, :ok}

  def repair_check(_ctx, s, sid, _now),
    do: {:write, [{:delete, Keys.repair(sid)}], :ok, s, []}

  def repair_due?(
        %Meta{creation: %CopyCreation{progress: %CopyProgress{progressed_ms: at}}},
        opts,
        now
      ),
      do: now - at >= creation_grace(opts)

  def repair_due?(meta, opts, now),
    do: Meta.pending?(meta) and now - meta.created_ms >= creation_grace(opts)

  defp creation_grace(opts), do: Keyword.get(opts, :creation_grace, @default_creation_grace)

  def protection(%Meta{sid: sid} = meta, %Identity{sid: sid} = target) do
    case {Meta.pending?(meta), Meta.reference(meta)} do
      {true, _} -> :protected
      {false, %Capture{target: ^target}} -> :referenced
      _ -> :unprotected
    end
  end

  def protection(_meta, _target), do: :unprotected

  def terminal(s, target) do
    # A stale owner's read may miss a live target. Require a durable write so
    # SlateDB fences that owner before repair releases the source registration.
    case protection(s.meta, target) do
      :unprotected ->
        {:write, [{:delete, Keys.witness(target.sid)}], {:ok, :terminal}, s, []}

      status ->
        {:reply, {:ok, status}}
    end
  end
end
