defmodule Slap.Streams.Test.Model do
  @moduledoc false
  # Reference model derived from PROTOCOL.md §4.2 and §5. Expiry is checked
  # on access and sweep. HEAD checks expiry without resetting a sliding TTL;
  # internal retention and cleanup requests do neither.

  alias Slap.Streams.{ContentType, Json}

  def new(now, max_fork_depth \\ 8),
    do: %{
      streams: %{},
      now: now,
      sealed: MapSet.new(),
      max_fork_depth: max_fork_depth,
      next_id: 0,
      releases: MapSet.new()
    }

  def run(state, {:seal, group}), do: {:ok, %{state | sealed: MapSet.put(state.sealed, group)}}

  def run(state, {:advance, ms}), do: {:ok, %{state | now: state.now + ms}}

  def run(state, {:repair}), do: {:ok, repair(state)}

  def run(state, {:invalid, _request, reason}),
    do: {{:error, {:bad_request, reason}}, state}

  def run(state, {:sweep}) do
    due =
      for {path, %{status: :active} = s} <- state.streams, expired?(s, state.now), do: path

    {:ok, Enum.reduce(due, state, &access(&2, &1, false))}
  end

  # Listing is not a request to the streams it lists: an expired stream not
  # yet removed is listed.
  def run(state, {:list, prefix}) do
    key = Slap.Streams.placement_key(prefix)

    paths =
      for {path, %{status: :active}} <- state.streams,
          String.starts_with?(path, prefix),
          Slap.Streams.placement_key(path) == key,
          do: path

    {{:ok, Enum.sort(paths)}, state}
  end

  # Killing a stream server loses nothing: every acknowledged write is durable.
  def run(state, {:kill, _path}), do: {:ok, state}

  def run(state, {:head, path}) do
    state = access(state, path, false)

    case get_any(state, path) do
      nil -> {{:error, :not_found}, state}
      %{status: :gone} -> {{:error, :gone}, state}
      %{status: :pending} -> {{:error, :unavailable}, state}
      s -> {{:ok, info(s)}, state}
    end
  end

  def run(state, {command, path, _} = c) when command in [:create, :trim],
    do: dispatch(access(state, path, true), c)

  def run(state, {command, path, _, _} = c) when command in [:append, :read],
    do: dispatch(access(state, path, true), c)

  def run(state, {:delete, path}) do
    state = access(state, path, true)

    case get_any(state, path) do
      nil -> {{:error, :not_found}, state}
      %{status: :gone} -> {{:error, :gone}, state}
      _ -> {:ok, remove(state, path)}
    end
  end

  # A copy interrupted once it captured the source's range: a retry finishes it
  # from the capture.
  def run(state, {:interrupt_copy, source, target}) do
    src = get(state, source)
    identity = {source, :latest}
    {state, id} = retain(state, source)
    s = copied(state, src, identity, src.tail, id)

    pending =
      Map.merge(pending(state, :copy, identity, src.id), %{
        captured: s,
        hold: {source, src.id, id}
      })

    {:ok, put(state, target, pending)}
  end

  def run(state, {:copy_page, target}) do
    %{status: :pending} = p = get_any(state, target)
    {:ok, put(state, target, %{p | since: state.now})}
  end

  def run(state, {:copy, path, path, _opts}),
    do: {{:error, {:bad_request, :fork_of_itself}}, state}

  def run(state, {:copy, source, target, opts}) do
    identity = {source, Keyword.get(opts, :at, :latest)}
    state = access(state, target, true)

    case get_any(state, target) do
      %{status: :active, copy_of: ^identity} = s ->
        {{:ok, :exists, info(s)}, state}

      %{status: :pending, request: ^identity, captured: s, hold: {source, src_id, id}} ->
        s = %{s | last_access: state.now}
        {{:ok, :created, info(s)}, state |> put(target, s) |> release({source, src_id}, id)}

      %{status: :pending, kind: :copy, request: ^identity} = pending ->
        retry_source(state, target, pending, source, &copy_stream(&1, source, target, identity))

      nil ->
        create_from_source(state, source, target, &copy_stream(&1, source, target, identity))

      _ ->
        {{:error, :conflict}, state}
    end
  end

  defp copy_stream(state, source, target, identity) do
    {_source, requested} = identity

    case fork_source(state, source) do
      {:ok, src} ->
        offset = if requested == :latest, do: src.tail, else: requested
        copy_range(state, source, target, identity, src, offset)

      {:error, error} ->
        {{:error, error}, state}
    end
  end

  defp copy_range(state, _source, target, identity, src, offset) when offset > src.tail do
    pending = pending(state, :copy, identity, src.id)
    {{:error, {:bad_request, :fork_offset_beyond_source}}, put(state, target, pending)}
  end

  defp copy_range(state, source, target, identity, src, offset) do
    {state, id} = retain(state, source)
    state = release(state, {source, src.id}, id)
    s = copied(state, src, identity, offset, id)

    if sealed?(state, target),
      do: {{:error, :sealed}, state},
      else: {{:ok, :created, info(s)}, put(state, target, s)}
  end

  defp copied(state, src, identity, offset, id) do
    trim = min(src.trim, offset)
    meta = %{src.meta | closed_by: nil, last_stream_seq: nil}

    %{
      stream(meta, state.now, id)
      | tail: offset,
        trim: trim,
        copy_of: identity,
        messages: Enum.filter(src.messages, fn {at, _} -> at >= trim and at < offset end)
    }
  end

  # -- create (§5.1, §4.2) --

  defp dispatch(state, {:create, path, opts}) do
    case get_any(state, path) do
      nil ->
        create_new(state, path, opts)

      %{status: :gone} ->
        {{:error, :conflict}, state}

      %{status: :pending} = pending ->
        retry_pending(state, path, pending, opts)

      s ->
        if same_config?(s, opts),
          do: {{:ok, :exists, info(s)}, state},
          else: {{:error, :conflict}, state}
    end
  end

  defp dispatch(state, {:append, path, body, opts}) do
    case get_any(state, path) do
      nil ->
        {{:error, :not_found}, state}

      %{status: :gone} ->
        {{:error, :gone}, state}

      %{status: :pending} ->
        {{:error, :unavailable}, state}

      s ->
        {result, s} = append(s, body, opts)
        {result, put(state, path, s)}
    end
  end

  defp dispatch(state, {:trim, path, offset}) do
    case get_any(state, path) do
      nil -> {{:error, :not_found}, state}
      %{status: :gone} -> {{:error, :gone}, state}
      %{status: :pending} -> {{:error, :unavailable}, state}
      s when offset > s.tail -> {{:error, {:bad_request, :trim_offset_beyond_tail}}, state}
      s when offset <= s.trim -> {:ok, state}
      s -> {:ok, put(state, path, %{s | trim: offset})}
    end
  end

  # -- read (§5.6) --

  defp dispatch(state, {:read, path, offset, max_bytes}) do
    case get_any(state, path) do
      nil -> {{:error, :not_found}, state}
      %{status: :gone} -> {{:error, :gone}, state}
      %{status: :pending} -> {{:error, :unavailable}, state}
      s -> {read_stream(s, if(offset == :now, do: s.tail, else: offset), max_bytes), state}
    end
  end

  defp retry_pending(state, path, pending, opts) do
    if pending.kind == :reference and request(opts) == pending.request do
      retry_source(state, path, pending, opts[:forked_from], &fork(&1, path, opts))
    else
      {{:error, :conflict}, state}
    end
  end

  defp retry_source(state, path, pending, source, create) do
    state = access(state, source, false)

    case get_any(state, source) do
      %{status: :active, id: id} when id == pending.source_id ->
        {result, next_state} = create.(delete(state, path))

        case get_any(next_state, path) do
          %{status: :pending} = next ->
            {result, put(next_state, path, %{next | since: pending.since})}

          _completed_or_cancelled ->
            {result, next_state}
        end

      nil ->
        {{:error, :source_not_found}, state}

      _gone_or_replaced ->
        {{:error, :source_gone}, state}
    end
  end

  defp request(opts) do
    opts
    |> Map.new()
    |> Map.put_new(:body, "")
    |> Map.put_new(:closed, false)
    |> Map.put_new(:fork_sub_offset, 0)
  end

  defp pending(state, kind, request, source_id),
    do: %{status: :pending, kind: kind, request: request, source_id: source_id, since: state.now}

  # -- read helpers --

  defp read_stream(s, offset, _max_bytes) when offset > s.tail, do: {:error, :offset_beyond_tail}
  defp read_stream(s, offset, _max_bytes) when offset < s.trim, do: {:error, :trimmed}

  defp read_stream(s, offset, max_bytes) do
    available = Enum.filter(s.messages, fn {o, _} -> o >= offset end)
    taken = take(available, max_bytes, 0, [])
    next = next_offset(s, taken, available)
    up_to_date = next == s.tail

    {:ok,
     %{
       messages: taken,
       next_offset: next,
       up_to_date: up_to_date,
       closed: s.meta.closed and up_to_date,
       content_type: s.meta.content_type
     }}
  end

  defp next_offset(s, taken, available) when length(taken) == length(available), do: s.tail

  defp next_offset(_s, taken, available) do
    {next, _} = Enum.at(available, length(taken))
    min(taken |> List.last() |> message_end(), next)
  end

  defp message_end({offset, bytes}), do: offset + 4 + byte_size(bytes)

  # -- create helpers --

  defp create_new(state, path, opts) do
    cond do
      opts[:ttl_s] != nil and opts[:expires_at_ms] != nil ->
        {{:error, {:bad_request, :ttl_and_expires_at}}, state}

      opts[:forked_from] == path ->
        {{:error, {:bad_request, :fork_of_itself}}, state}

      opts[:forked_from] != nil ->
        create_from_source(state, opts[:forked_from], path, &fork(&1, path, opts))

      true ->
        plain_create(state, path, opts)
    end
  end

  defp create_from_source(state, source, target, create) do
    state = access(state, source, false)

    case fork_source(state, source) do
      {:ok, _source} ->
        if sealed?(state, target), do: {{:error, :sealed}, state}, else: create.(state)

      {:error, reason} ->
        {{:error, reason}, state}
    end
  end

  defp plain_create(state, path, opts) do
    ct = ContentType.normalize(opts[:content_type])

    case {messages(ct, opts[:body] || "", true), sealed?(state, path)} do
      {{:error, reason}, _sealed} ->
        {{:error, {:bad_request, reason}}, state}

      {{:ok, _bodies}, true} ->
        {{:error, :sealed}, state}

      {{:ok, bodies}, false} ->
        meta = %{
          content_type: ct,
          ttl_s: opts[:ttl_s],
          expires_at_ms: opts[:expires_at_ms],
          closed: opts[:closed] == true,
          closed_by: nil,
          last_stream_seq: nil
        }

        {state, id} = allocate(state)
        s = add(stream(meta, state.now, id), bodies)
        {{:ok, :created, info(s)}, put(state, path, s)}
    end
  end

  defp sealed?(state, path),
    do: MapSet.member?(state.sealed, Slap.Streams.placement_key(path))

  defp same_config?(%{meta: m, fork_of: fork}, opts) do
    ContentType.media_type(m.content_type) == ContentType.media_type(opts[:content_type]) and
      m.ttl_s == opts[:ttl_s] and m.expires_at_ms == opts[:expires_at_ms] and
      m.closed == (opts[:closed] == true) and same_fork?(fork, opts)
  end

  defp same_fork?(nil, opts), do: opts[:forked_from] == nil

  defp same_fork?(fork, opts) do
    fork.path == opts[:forked_from] and
      fork.source_placement_key ==
        (opts[:source_placement_key] || Slap.Streams.placement_key(opts[:forked_from])) and
      opts[:fork_offset] in [nil, fork.requested_offset || fork.offset] and
      (opts[:fork_sub_offset] || 0) == fork.sub_offset
  end

  # -- forks (§4.2) --

  defp fork(state, path, opts) do
    source = opts[:forked_from]
    state = access(state, source, false)

    case fork_source(state, source) do
      {:ok, src} -> fork_range(state, path, opts, src)
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp fork_range(state, path, opts, src) do
    source = opts[:forked_from]

    with :ok <- fork_content_type(opts[:content_type], src.meta.content_type),
         {:ok, offset, prefix} <- fork_offset(src, opts),
         :ok <-
           if(offset > src.trim and src.depth >= state.max_fork_depth,
             do: {:error, {:bad_request, :max_fork_depth_exceeded}},
             else: :ok
           ) do
      case {messages(src.meta.content_type, opts[:body] || "", true), sealed?(state, path)} do
        {{:error, reason}, _sealed} ->
          {state, id} = retain(state, source)
          {{:error, {:bad_request, reason}}, release(state, {source, src.id}, id)}

        {{:ok, _bodies}, true} ->
          {state, id} = retain(state, source)
          {{:error, :sealed}, release(state, {source, src.id}, id)}

        {{:ok, bodies}, false} ->
          {state, id} = retain(state, source)
          create_fork(state, path, src, opts, {offset, prefix, bodies, id})
      end
    else
      {:error, _reason} = error ->
        pending = pending(state, :reference, request(opts), src.id)
        {error, put(state, path, pending)}
    end
  end

  defp create_fork(state, path, src, opts, {offset, prefix, bodies, id}) do
    {ttl_s, expires_at_ms} = fork_expiry(opts, src)

    meta = %{
      content_type: src.meta.content_type,
      ttl_s: ttl_s,
      expires_at_ms: expires_at_ms,
      closed: opts[:closed] == true,
      closed_by: nil,
      last_stream_seq: nil
    }

    trim = min(src.trim, offset)
    copied = Enum.filter(src.messages, fn {o, _} -> o >= trim and o < offset end)

    s = %{
      stream(meta, state.now, id)
      | messages: copied,
        tail: offset,
        trim: trim,
        depth: if(trim < offset, do: src.depth + 1, else: 0),
        origin: {opts[:forked_from], src.id},
        fork_of: %{
          path: opts[:forked_from],
          source_placement_key:
            opts[:source_placement_key] || Slap.Streams.placement_key(opts[:forked_from]),
          offset: offset,
          requested_offset: opts[:fork_offset],
          sub_offset: opts[:fork_sub_offset] || 0
        }
    }

    s = add(s, List.wrap(prefix) ++ bodies)
    {{:ok, :created, info(s)}, put(state, path, s)}
  end

  # The fork's own TTL or Expires-At, else the source's.
  defp fork_expiry(opts, src) do
    cond do
      opts[:ttl_s] != nil -> {opts[:ttl_s], nil}
      opts[:expires_at_ms] != nil -> {nil, opts[:expires_at_ms]}
      true -> {src.meta.ttl_s, src.meta.expires_at_ms}
    end
  end

  defp fork_source(state, source) do
    case get_any(state, source) do
      nil -> {:error, :source_not_found}
      %{status: :gone} -> {:error, :source_gone}
      %{status: :pending} -> {:error, :unavailable}
      s -> {:ok, s}
    end
  end

  defp fork_content_type(nil, _), do: :ok

  defp fork_content_type(ct, source) do
    if String.downcase(ct) == String.downcase(source),
      do: :ok,
      else: {:error, :content_type_mismatch}
  end

  @overshoot {:error, {:bad_request, :invalid_fork_sub_offset}}

  defp fork_offset(src, opts) do
    offset = opts[:fork_offset] || src.tail
    sub = opts[:fork_sub_offset] || 0

    cond do
      offset > src.tail ->
        {:error, {:bad_request, :fork_offset_beyond_source}}

      sub == 0 ->
        {:ok, offset, nil}

      offset < src.trim ->
        {:error, {:bad_request, :fork_offset_trimmed}}

      true ->
        after_offset = Enum.filter(src.messages, fn {o, _} -> o >= offset end)
        sub_offset(ContentType.json?(src.meta.content_type), after_offset, offset, sub, src.tail)
    end
  end

  # JSON: `sub` messages further. Other types: a prefix of the next message.
  defp sub_offset(true, after_offset, _offset, sub, tail) do
    case Enum.split(after_offset, sub) do
      {taken, rest} when length(taken) == sub ->
        next =
          case rest do
            [{at, _} | _] -> at
            [] -> tail
          end

        {:ok, min(message_end(List.last(taken)), next), nil}

      _ ->
        @overshoot
    end
  end

  defp sub_offset(false, [{_, b} | _], offset, sub, _tail) when byte_size(b) >= sub,
    do: {:ok, offset, binary_part(b, 0, sub)}

  defp sub_offset(false, _after_offset, _offset, _sub, _tail), do: @overshoot

  # -- lifecycle --

  # A request reaching `path`: an expired stream there is removed first.
  defp access(state, path, touch?) do
    state =
      case get_any(state, path) do
        %{status: :active} = s -> if expired?(s, state.now), do: remove(state, path), else: state
        _ -> state
      end

    case get_any(state, path) do
      %{status: :active} = s when touch? -> put(state, path, %{s | last_access: state.now})
      _ -> state
    end
  end

  defp expired?(%{meta: m, last_access: last}, now) do
    (m.expires_at_ms != nil and now >= m.expires_at_ms) or
      (m.ttl_s != nil and now >= last + m.ttl_s * 1000)
  end

  defp remove(state, path) do
    remove(state, path, get_any(state, path))
  end

  defp remove(state, path, %{status: :pending, hold: {source, id, fork}}),
    do: state |> delete(path) |> release({source, id}, fork)

  defp remove(state, path, %{status: :pending}), do: delete(state, path)

  defp remove(state, path, s) do
    if s.forks != [],
      do: put(state, path, %{s | status: :gone}),
      else: state |> delete(path) |> release(s.origin, s.id)
  end

  defp unregister(state, {source, id, fork}) do
    case get_any(state, source) do
      %{id: ^id} = s -> drop_fork(state, source, s, List.delete(s.forks, fork))
      _ -> state
    end
  end

  defp drop_fork(state, _source, %{forks: forks}, forks), do: state

  defp drop_fork(state, source, %{status: :gone} = s, []) do
    state |> delete(source) |> release(s.origin, s.id)
  end

  defp drop_fork(state, source, s, forks), do: put(state, source, %{s | forks: forks})

  defp allocate(state), do: {%{state | next_id: state.next_id + 1}, state.next_id}

  defp retain(state, source) do
    {state, id} = allocate(state)
    s = get_any(state, source)
    {put(state, source, %{s | forks: [id | s.forks]}), id}
  end

  defp release(state, nil, _target), do: state

  defp release(state, {source, id}, target),
    do: %{state | releases: MapSet.put(state.releases, {source, id, target})}

  defp repair(state) do
    state =
      Enum.reduce(state.streams, state, fn
        {path, %{status: :pending, since: since}}, state when state.now - since >= 3_600_000 ->
          remove(state, path)

        _, state ->
          state
      end)

    case MapSet.to_list(state.releases) do
      [] ->
        state

      releases ->
        releases
        |> Enum.reduce(%{state | releases: MapSet.new()}, &unregister(&2, &1))
        |> repair()
    end
  end

  defp stream(meta, now, id) do
    %{
      id: id,
      origin: nil,
      status: :active,
      meta: meta,
      messages: [],
      tail: 0,
      producers: %{},
      trim: 0,
      forks: [],
      fork_of: nil,
      copy_of: nil,
      depth: 0,
      last_access: now
    }
  end

  defp get_any(state, path), do: Map.get(state.streams, path)
  defp put(state, path, s), do: %{state | streams: Map.put(state.streams, path, s)}
  defp delete(state, path), do: %{state | streams: Map.delete(state.streams, path)}

  defp take([], _max, _total, acc), do: Enum.reverse(acc)

  defp take([{_o, b} = m | rest], max, total, acc) do
    total = total + byte_size(b)
    if total >= max, do: Enum.reverse([m | acc]), else: take(rest, max, total, [m | acc])
  end

  defp append(s, body, opts) do
    close = opts[:close] == true
    p = opts[:producer]

    cond do
      body == "" and not close -> {{:error, {:bad_request, :empty_body}}, s}
      body == "" -> close_only(s, p)
      true -> append_body(s, body, close, p, opts)
    end
  end

  defp close_only(s, nil) do
    s = put_in(s.meta.closed, true)
    {ok(:closed, s, nil), s}
  end

  defp close_only(%{meta: %{closed: true}} = s, {_id, e, q} = p) do
    if s.meta.closed_by == p,
      do: {ok(:duplicate, s, {e, q}), s},
      else: {{:error, {:closed, s.tail}}, s}
  end

  defp close_only(s, {id, e, q} = p) do
    case producer(s, id, e, q) do
      {:error, reason} ->
        {{:error, reason}, s}

      {:duplicate, last} ->
        {ok(:duplicate, s, {e, last}), s}

      {:accept, state} ->
        s = %{s | producers: Map.put(s.producers, id, state)}
        s = %{s | meta: %{s.meta | closed: true, closed_by: p}}
        {ok(:closed, s, {e, q}), s}
    end
  end

  defp append_body(%{meta: %{closed: true}} = s, _body, _close, p, _opts) do
    case p do
      {_id, e, q} when p == s.meta.closed_by -> {ok(:duplicate, s, {e, q}), s}
      _ -> {{:error, {:closed, s.tail}}, s}
    end
  end

  defp append_body(s, body, close, p, opts) do
    seq = opts[:stream_seq]

    with :ok <- check_type(opts[:content_type], s.meta.content_type),
         {:accept, pstate} <- check_producer(s, p),
         :ok <- check_seq(seq, s.meta.last_stream_seq),
         {:ok, bodies} <- body_messages(s, body) do
      apply_append(s, bodies, {p, pstate}, seq, close)
    else
      {:duplicate, last} -> {ok(:duplicate, s, {elem(p, 1), last}), s}
      {:error, reason} -> {{:error, reason}, s}
    end
  end

  defp check_type(nil, _stream_type), do: :ok

  defp check_type(type, stream_type) do
    if ContentType.matches?(type, stream_type), do: :ok, else: {:error, :content_type_mismatch}
  end

  defp check_producer(_s, nil), do: {:accept, nil}
  defp check_producer(s, {id, e, q}), do: producer(s, id, e, q)

  defp check_seq(seq, last) when seq != nil and last != nil and seq <= last,
    do: {:error, :stream_seq_conflict}

  defp check_seq(_seq, _last), do: :ok

  defp body_messages(s, body) do
    case messages(s.meta.content_type, body, false) do
      {:ok, bodies} -> {:ok, bodies}
      {:error, reason} -> {:error, {:bad_request, reason}}
    end
  end

  defp apply_append(s, bodies, {p, pstate}, seq, close) do
    s = s |> add(bodies) |> put_producer(p, pstate)
    meta = %{s.meta | last_stream_seq: seq || s.meta.last_stream_seq}
    meta = if close, do: %{meta | closed: true, closed_by: p}, else: meta
    s = %{s | meta: meta}
    echo = if p, do: {elem(p, 1), elem(p, 2)}
    {ok(:appended, s, echo), s}
  end

  defp put_producer(s, nil, _pstate), do: s
  defp put_producer(s, {id, _, _}, pstate), do: %{s | producers: Map.put(s.producers, id, pstate)}

  # §5.2.1 validation.
  defp producer(s, id, e, q) do
    case Map.get(s.producers, id) do
      nil ->
        if q == 0, do: {:accept, {e, 0}}, else: {:error, {:producer_seq_gap, 0, q}}

      {cur, _} when e < cur ->
        {:error, {:stale_epoch, cur}}

      {cur, _} when e > cur ->
        if q == 0,
          do: {:accept, {e, 0}},
          else: {:error, {:bad_request, :new_epoch_must_start_at_zero}}

      {_, last} when q <= last ->
        {:duplicate, last}

      {cur, last} when q == last + 1 ->
        {:accept, {cur, q}}

      {_, last} ->
        {:error, {:producer_seq_gap, last + 1, q}}
    end
  end

  defp messages(ct, body, allow_empty) do
    cond do
      body == "" -> {:ok, []}
      ContentType.json?(ct) -> Json.split(body, allow_empty)
      true -> {:ok, [body]}
    end
  end

  defp add(s, bodies) do
    {new, tail} =
      Enum.map_reduce(bodies, s.tail, fn b, at -> {{at, b}, at + 4 + byte_size(b)} end)

    %{s | messages: s.messages ++ new, tail: tail}
  end

  defp ok(result, s, echo),
    do: {:ok, %{result: result, next_offset: s.tail, closed: s.meta.closed, producer: echo}}

  defp info(s) do
    %{
      next_offset: s.tail,
      closed: s.meta.closed,
      content_type: s.meta.content_type,
      ttl_s: s.meta.ttl_s,
      expires_at_ms: s.meta.expires_at_ms
    }
  end

  @doc false
  # Message offsets and the tail of a stream, for picking read offsets.
  def boundaries(state, path) do
    case get(state, path) do
      nil -> [0]
      s -> [s.tail | Enum.map(s.messages, &elem(&1, 0))]
    end
  end

  @doc false
  # The active stream at `path`, or nil.
  def get(state, path) do
    case get_any(state, path) do
      %{status: :active} = s -> s
      _ -> nil
    end
  end

  @doc false
  def paths(state), do: Map.keys(state.streams)
end
