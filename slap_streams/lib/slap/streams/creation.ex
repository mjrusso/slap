defmodule Slap.Streams.Creation do
  @moduledoc false

  alias Slap.Streams
  alias Slap.Streams.{ContentType, Offset}
  alias Slap.Streams.Creation.Target
  alias Slap.Streams.Store.{CopyCreation, CopyProgress, ReferenceCreation}

  @page_bytes 4 * 1024 * 1024
  @json_page_bytes 64 * 1024

  def run(path, req, kind, opts) do
    deadline = deadline(Keyword.get(opts, :timeout, 30_000))
    req = Target.request(Map.put(req, :source_placement_key, opts[:source_placement_key]))

    with :ok <- if(kind == :copy, do: not_self(path, req.forked_from), else: :ok) do
      path
      |> reserve(req, kind, opts, deadline)
      |> complete(opts, deadline)
      |> public_result()
    end
  end

  @spec public_result(Target.completed() | {:error, term()}) ::
          {:ok, :created | :exists, Streams.info()} | {:error, term()}
  defp public_result({:ok, {:completed, status, info}}), do: {:ok, status, info}
  defp public_result({:error, _} = error), do: error

  defp reserve(path, req, kind, opts, deadline) do
    case target(path, {:reservation, kind, req}, opts, deadline) do
      :needs_source ->
        source_opts = source_options(opts, deadline)

        with :ok <- Streams.Protocol.check_expiry(req),
             :ok <- not_self(path, req.forked_from),
             {:ok, source} <- Streams.internal(req.forked_from, :source_identity, source_opts) do
          target(path, {:reserve, kind, req, source}, opts, deadline)
        end

      result ->
        result
    end
  end

  @spec complete(
          Target.outcome(Slap.Streams.Store.Creation.pending()),
          keyword(),
          integer() | :infinity
        ) :: Target.completed() | {:error, term()}
  defp complete({:ok, {:pending, c}}, opts, deadline) do
    result = with {:ok, cap} <- capture(c, opts, deadline), do: publish(c, cap, opts, deadline)

    case result do
      {:error, _} = failure ->
        call(c.target, {:reconcile_creation, c.target, failure}, opts, deadline)

      {:ok, {:completed, _status, _info}} = completed ->
        completed
    end
  end

  defp complete({:ok, {:completed, _status, _info}} = completed, _opts, _deadline),
    do: completed

  defp complete({:error, _} = error, _opts, _deadline), do: error

  defp capture(%CopyCreation{progress: %CopyProgress{capture: cap}}, _opts, _deadline),
    do: {:ok, cap}

  defp capture(c, opts, deadline) do
    selection = Target.selection(c)

    case call(c.source, {:register, selection, nil}, opts, deadline) do
      {:resolve, view} ->
        resolve_capture(c, selection, view, opts, deadline)

      result ->
        result
    end
  end

  defp resolve_capture(c, selection, view, opts, deadline) do
    case resolve(c, view, opts, deadline) do
      {:ok, offset, prefix} ->
        resolved = {view.sid, view.trim, view.next_offset, offset, prefix}
        call(c.source, {:register, selection, resolved}, opts, deadline)

      {:error, _} = error ->
        case call(c.source, {:register, selection, nil}, opts, deadline) do
          {:ok, cap} -> {:ok, cap}
          _unknown_or_unresolved -> error
        end
    end
  end

  defp resolve(c, view, opts, deadline) do
    offset = c.request.fork_offset || view.next_offset
    sub = c.request.fork_sub_offset

    if ContentType.json?(view.meta.content_type),
      do: json_offset(c, view, offset, sub, opts, deadline),
      else: binary_prefix(c, view, offset, sub, opts, deadline)
  end

  defp binary_prefix(c, view, offset, sub, opts, deadline) do
    with {:ok, %{messages: messages}} <- logical_page(c, view, offset, 1, opts, deadline) do
      case messages do
        [{_, bytes} | _] when byte_size(bytes) >= sub -> {:ok, offset, binary_part(bytes, 0, sub)}
        _ -> {:error, {:bad_request, :invalid_fork_sub_offset}}
      end
    end
  end

  defp json_offset(_c, _view, offset, 0, _opts, _deadline), do: {:ok, offset, nil}

  defp json_offset(c, view, offset, n, opts, deadline) do
    with {:ok, %{messages: messages, next_offset: next}} <-
           logical_page(c, view, offset, @json_page_bytes, opts, deadline) do
      case Enum.split(messages, n) do
        {[], []} ->
          {:error, {:bad_request, :invalid_fork_sub_offset}}

        {taken, []} ->
          json_offset(c, view, next, n - length(taken), opts, deadline)

        {taken, [{next_at, _} | _]} ->
          {at, bytes} = List.last(taken)
          {:ok, min(Offset.advance(at, byte_size(bytes)), next_at), nil}
      end
    end
  end

  defp logical_page(c, view, offset, max_bytes, opts, deadline) do
    Streams.read_identity(
      c.source,
      view,
      offset,
      max_bytes,
      remaining(deadline),
      Keyword.get(opts, :cluster, Streams.Cluster)
    )
  end

  defp publish(%ReferenceCreation{} = c, cap, opts, deadline),
    do: call(c.target, {:activate, c.target, cap}, opts, deadline)

  defp publish(c, cap, opts, deadline) do
    case call(c.target, {:capture, c.target, cap}, opts, deadline) do
      {:ok, {:pending, c}} -> copy(c, opts, deadline)
      {:ok, {:completed, _status, _info}} = completed -> completed
      {:error, _} = error -> error
    end
  end

  defp copy(
         %CopyCreation{progress: %CopyProgress{cursor: cursor, capture: %{offset: until}}} = c,
         opts,
         deadline
       )
       when cursor >= until,
       do: call(c.target, {:activate, c.target}, opts, deadline)

  defp copy(%CopyCreation{progress: progress} = c, opts, deadline) do
    cluster = Keyword.get(opts, :cluster, Streams.Cluster)

    with {:ok, {messages, next}} <-
           Streams.retained(
             progress.capture,
             progress.cursor,
             progress.capture.offset,
             @page_bytes,
             remaining(deadline),
             cluster
           ) do
      case call(c.target, {:copy_page, c.target, progress.cursor, messages, next}, opts, deadline) do
        {:ok, {:pending, next}} ->
          copy(%{c | progress: %{progress | cursor: next}}, opts, deadline)

        {:ok, {:completed, _status, _info}} = completed ->
          completed

        {:error, _} = error ->
          error
      end
    end
  end

  defp target(path, request, opts, deadline),
    do:
      Streams.internal(
        path,
        request,
        Keyword.put(Keyword.take(opts, [:cluster, :placement_key]), :timeout, remaining(deadline))
      )

  defp call(identity, request, opts, deadline),
    do:
      Streams.internal_identity(
        identity,
        request,
        remaining(deadline),
        Keyword.get(opts, :cluster, Streams.Cluster)
      )

  defp source_options(opts, deadline) do
    options = Keyword.take(opts, [:cluster]) |> Keyword.put(:timeout, remaining(deadline))

    if Keyword.has_key?(opts, :source_placement_key),
      do: Keyword.put(options, :placement_key, opts[:source_placement_key]),
      else: options
  end

  defp not_self(path, path), do: {:error, {:bad_request, :fork_of_itself}}
  defp not_self(_path, _source), do: :ok
  defp deadline(:infinity), do: :infinity
  defp deadline(ms), do: System.monotonic_time(:millisecond) + ms
  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)
end
