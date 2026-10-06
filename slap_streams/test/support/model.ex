defmodule Slap.Streams.Test.Model do
  @moduledoc false
  # A reference model of streams, written from PROTOCOL.md §4.2 and §5 as
  # plain data transformations: no processes, no storage, no durability. The
  # property test runs the same commands against it and against Slap.Streams,
  # and the results must match.
  #
  # State: %{streams: %{path => stream}, now: ms, sealed: MapSet of
  # placement groups}, where a stream is
  #   %{status: :active | :gone,
  #     meta: %{content_type, ttl_s, expires_at_ms, closed, closed_by,
  #             last_stream_seq},
  #     messages: [{offset, bytes}], tail, producers: %{id => {epoch, last}},
  #     trim, forks: [path], fork_of: nil | %{path, offset, requested_offset,
  #     sub_offset}, last_access}
  #
  # Expiry is lazy, as in the server: an expired stream is removed when its
  # path is next accessed (by a request, a fork, a fork's removal, or the
  # expiry sweep). Reads, writes and creates reset a sliding TTL; HEAD and
  # requests between streams do not.

  alias Slap.Streams.{ContentType, Json}

  def new(now, max_fork_copy_bytes \\ 64 * 1024 * 1024),
    do: %{streams: %{}, now: now, sealed: MapSet.new(), max_fork_copy_bytes: max_fork_copy_bytes}

  def run(state, {:seal, group}), do: {:ok, %{state | sealed: MapSet.put(state.sealed, group)}}

  def run(state, {:advance, ms}), do: {:ok, %{state | now: state.now + ms}}

  # The sweep visits every expired stream (Slap.Streams.Jobs.Expiry).
  # The repair sweep changes nothing visible here: the model has no copies
  # left unfinished, and forks always unregister.
  def run(state, {:repair}), do: {:ok, state}

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

  # -- create (§5.1, §4.2) --

  defp dispatch(state, {:create, path, opts}) do
    case get_any(state, path) do
      nil ->
        create_new(state, path, opts)

      %{status: :gone} ->
        {{:error, :conflict}, state}

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

      s ->
        {result, s} = append(s, body, opts)
        {result, put(state, path, s)}
    end
  end

  defp dispatch(state, {:trim, path, offset}) do
    case get_any(state, path) do
      nil -> {{:error, :not_found}, state}
      %{status: :gone} -> {{:error, :gone}, state}
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
      s -> {read_stream(s, if(offset == :now, do: s.tail, else: offset), max_bytes), state}
    end
  end

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

  # The tail once everything is taken, else the end of the last message.
  defp next_offset(s, taken, available) when length(taken) == length(available), do: s.tail
  defp next_offset(_s, taken, _available), do: taken |> List.last() |> message_end()

  defp message_end({offset, bytes}), do: offset + 4 + byte_size(bytes)

  # -- create helpers --

  defp create_new(state, path, opts) do
    cond do
      opts[:ttl_s] != nil and opts[:expires_at_ms] != nil ->
        {{:error, {:bad_request, :ttl_and_expires_at}}, state}

      opts[:forked_from] == path ->
        {{:error, {:bad_request, :fork_of_itself}}, state}

      opts[:forked_from] != nil ->
        fork(state, path, opts)

      true ->
        plain_create(state, path, opts)
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

        s = add(stream(meta, state.now), bodies)
        {{:ok, :created, info(s)}, put(state, path, s)}
    end
  end

  # A write that would create a stream in a sealed group fails.
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
      opts[:fork_offset] in [nil, fork.requested_offset || fork.offset] and
      (opts[:fork_sub_offset] || 0) == fork.sub_offset
  end

  # -- forks (§4.2) --

  defp fork(state, path, opts) do
    source = opts[:forked_from]
    state = access(state, source, false)

    with {:ok, src} <- fork_source(state, source),
         :ok <- fork_content_type(opts[:content_type], src.meta.content_type),
         {:ok, offset, prefix} <- fork_offset(src, opts),
         :ok <-
           if(offset > state.max_fork_copy_bytes, do: {:error, :payload_too_large}, else: :ok) do
      forks = if path in src.forks, do: src.forks, else: [path | src.forks]
      state = put(state, source, %{src | forks: forks})

      case {messages(src.meta.content_type, opts[:body] || "", true), sealed?(state, path)} do
        {{:error, reason}, _sealed} ->
          {{:error, {:bad_request, reason}}, unregister(state, source, path)}

        {{:ok, _bodies}, true} ->
          {{:error, :sealed}, unregister(state, source, path)}

        {{:ok, bodies}, false} ->
          create_fork(state, path, src, opts, {offset, prefix, bodies})
      end
    else
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp create_fork(state, path, src, opts, {offset, prefix, bodies}) do
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
      stream(meta, state.now)
      | messages: copied,
        tail: offset,
        trim: trim,
        fork_of: %{
          path: opts[:forked_from],
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
        sub_offset(ContentType.json?(src.meta.content_type), after_offset, offset, sub)
    end
  end

  # JSON: `sub` messages further. Other types: a prefix of the next message.
  defp sub_offset(true, after_offset, _offset, sub) do
    case Enum.drop(after_offset, sub - 1) do
      [{o, b} | _] -> {:ok, o + 4 + byte_size(b), nil}
      [] -> @overshoot
    end
  end

  defp sub_offset(false, [{_, b} | _], offset, sub) when byte_size(b) >= sub,
    do: {:ok, offset, binary_part(b, 0, sub)}

  defp sub_offset(false, _after_offset, _offset, _sub), do: @overshoot

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

  # A stream with forks is soft-deleted; otherwise removed, and a fork is
  # unregistered from its source.
  defp remove(state, path) do
    s = get_any(state, path)

    cond do
      s.forks != [] ->
        put(state, path, %{s | status: :gone, messages: []})

      s.fork_of != nil ->
        state |> delete(path) |> unregister(s.fork_of.path, path)

      true ->
        delete(state, path)
    end
  end

  # A soft-deleted source whose last fork goes is removed, up the chain.
  defp unregister(state, source, fork) do
    state = access(state, source, false)

    case get_any(state, source) do
      nil -> state
      s -> drop_fork(state, source, s, List.delete(s.forks, fork))
    end
  end

  defp drop_fork(state, _source, %{forks: forks}, forks), do: state

  defp drop_fork(state, source, %{status: :gone} = s, []) do
    state = delete(state, source)
    if s.fork_of, do: unregister(state, s.fork_of.path, source), else: state
  end

  defp drop_fork(state, source, s, forks), do: put(state, source, %{s | forks: forks})

  defp stream(meta, now) do
    %{
      status: :active,
      meta: meta,
      messages: [],
      tail: 0,
      producers: %{},
      trim: 0,
      forks: [],
      fork_of: nil,
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
