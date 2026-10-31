defmodule Slap.Streams.Introspect.Messages do
  @moduledoc false

  alias Slap.Streams.Offset

  def annotate(page, decode) do
    tail = page.next_message_offset

    rows =
      Enum.flat_map(assemble(page.rows, tail), fn {rows, result} ->
        annotate_group(rows, result, decode)
      end)

    available = max(page.hard_max_bytes - :erlang.external_size(page) - 64, 0)

    {rows, {_available, limited}} =
      Enum.map_reduce(rows, {available, false}, &bounded_annotation/2)

    page |> Map.put(:rows, rows) |> Map.put(:interpretations_limited, limited)
  end

  # A page that stopped early has no next key, so its last message may be
  # missing rows.
  def end_at_known_boundary(page) do
    groups = Enum.chunk_by(page.rows, &offset/1)

    with [_, _ | _] <- groups,
         [%{key: key} = first | _] <- List.last(groups),
         at when is_integer(at) <- offset(first) do
      Map.merge(page, %{
        rows: groups |> Enum.drop(-1) |> Enum.concat(),
        next_message_offset: at,
        cursor: key,
        cursor_exclusive: false
      })
    else
      _ -> Map.put(page, :next_message_offset, nil)
    end
  end

  defp assemble(rows, tail) do
    Enum.map(groups(rows, tail), fn {rows, boundary} ->
      {rows, body(rows, boundary)}
    end)
  end

  def fragments(page, previous) do
    {fragments, state} =
      Enum.map_reduce(groups(page.rows, page.next_message_offset), previous, fn
        {rows, boundary}, previous -> fragment(page, rows, boundary, previous)
      end)

    page =
      Map.merge(page, %{
        fragment_contract: 1,
        fragments: fragments,
        hard_max_bytes: page.hard_max_bytes + 1024 * length(fragments) + 64
      })

    {page, state}
  end

  defp groups(rows, tail) do
    groups = Enum.chunk_by(rows, &offset/1)
    boundaries = Enum.concat(Enum.map(Enum.drop(groups, 1), &offset(hd(&1))), [tail])
    Enum.zip(groups, boundaries)
  end

  defp fragment(page, rows, boundary, previous) do
    {status, next} =
      if is_nil(previous) or (previous.sid == page.sid and previous.shard == page.shard),
        do: progress(rows, boundary, previous),
        else: {:invalid, nil}

    next = next && Map.merge(next, %{sid: page.sid, shard: page.shard})

    fragment = %{
      offset: offset(hd(rows)),
      row_count: length(rows),
      encoded_bytes: encoded_bytes(rows),
      next_offset: logical_boundary(boundary, page.until),
      before: previous,
      after: next,
      status: status
    }

    {fragment, next}
  end

  defp logical_boundary(boundary, until) when is_integer(boundary) and is_integer(until),
    do: min(boundary, until)

  defp logical_boundary(boundary, _until), do: boundary

  defp progress(rows, boundary, previous) do
    at = offset(hd(rows))

    with :ok <- readable(rows),
         {:ok, first, bytes} <- prefix(previous, at),
         true <- Enum.map(rows, &part/1) == Enum.to_list(first..(first + length(rows) - 1)) do
      bytes = bytes + encoded_bytes(rows)

      cond do
        boundary == at ->
          {:more, %{offset: at, next_part: first + length(rows), bytes: bytes}}

        is_integer(boundary) and at + bytes == boundary ->
          {:complete, nil}

        true ->
          {:invalid, nil}
      end
    else
      _ -> {:invalid, nil}
    end
  end

  defp prefix(nil, _offset), do: {:ok, 0, 0}

  defp prefix(%{offset: offset, next_part: part, bytes: bytes}, offset),
    do: {:ok, part, bytes}

  defp prefix(_previous, _offset), do: {:error, :incomplete_message_parts}

  def readable(rows) do
    if Enum.all?(
         rows,
         &match?(
           %{
             truncated: false,
             oversized_key: false,
             decoded: {:ok, %{offset: offset, part: part}}
           }
           when is_integer(offset) and is_integer(part),
           &1
         )
       ),
       do: :ok,
       else: {:error, :incomplete_message_parts}
  end

  defp encoded_bytes(rows) do
    Enum.reduce(rows, 0, fn row, bytes ->
      if part(row) == 0,
        do: Offset.advance(bytes, byte_size(row.value)),
        else: bytes + byte_size(row.value)
    end)
  end

  defp bounded_annotation(row, {available, limited}) do
    raw = Map.delete(row, :interpretation)
    extra = :erlang.external_size(row) - :erlang.external_size(raw)

    if extra <= available,
      do: {row, {available - extra, limited}},
      else: {raw, {available, true}}
  end

  defp body(rows, boundary) do
    case progress(rows, boundary, nil) do
      {:complete, nil} -> {:ok, rows |> Enum.map(& &1.value) |> IO.iodata_to_binary()}
      _ -> {:error, :incomplete_message_parts}
    end
  end

  defp annotate_group(rows, result, decode) do
    summary = with {:ok, body} <- result, do: decode.(body)

    summary =
      if :erlang.external_size(summary) <= 2048, do: summary, else: {:error, :summary_too_large}

    [first | rest] = rows
    [Map.put(first, :interpretation, summary) | rest]
  end

  defp offset(%{decoded: {:ok, %{offset: offset}}}), do: offset
  defp offset(_row), do: nil
  defp part(%{decoded: {:ok, %{part: part}}}), do: part
  defp part(_row), do: nil
end
