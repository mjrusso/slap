defmodule Slap.Inspect.Overview do
  @moduledoc false

  alias Slap.Inspect.{Bytes, Labels}

  def fields(%{content: _content}), do: []

  def fields(overview) do
    metadata = overview[:metadata] || %{}
    logical = metadata[:logical] || %{}

    stream(metadata) ++
      range(logical) ++
      fork(logical[:reference]) ++
      records(overview, metadata) ++
      log(overview[:log]) ++
      flags(overview)
  end

  defp stream(%{row: %{decoded: {:ok, %{path: path, value: %{sid: sid} = meta}}}} = metadata),
    do:
      [{"Stream", Bytes.name(path)}, {"Stream ID", "#{sid}"}] ++
        Labels.optional("Shard", metadata[:shard]) ++
        Labels.optional("Content type", meta[:content_type])

  defp stream(_metadata), do: []

  defp range(%{from: from, until: until}), do: [{"Offsets read", "#{from} to #{until}"}]
  defp range(_logical), do: []

  defp fork(%{source: source, offset: offset} = reference),
    do: [
      {"Fork of", "#{Bytes.name(source.path)} (sid #{source.sid}, shard #{source.shard})"},
      {"Inherits", "offsets before #{offset}"},
      {"Fork depth", "#{reference[:depth]}"}
    ]

  defp fork(_reference), do: []

  # A logical page can contain the source stream's records. Read the fork's
  # trim and deletion markers from its metadata.
  defp records(_overview, %{logical: _logical} = metadata) do
    records = metadata[:records] || %{}
    removal(stored(records[:trim]), stored(records[:deletion]))
  end

  defp records(overview, _metadata),
    do:
      Labels.optional("Tail", next_offset(overview[:tail])) ++
        removal(overview[:trim], overview[:deleted])

  defp removal(trim, deleted),
    do:
      Labels.optional("Trimmed before", trimmed(trim)) ++
        if(deleted, do: [{"Deletion", "pending"}], else: [])

  defp stored({:ok, %{row: row}}), do: row
  defp stored(_record), do: nil

  defp next_offset(%{decoded: {:ok, %{value: %{next_offset: offset}}}}), do: "offset #{offset}"
  defp next_offset(_row), do: nil

  defp trimmed(%{decoded: {:ok, %{value: offset}}}) when is_integer(offset) and offset > 0,
    do: "offset #{offset}"

  defp trimmed(_row), do: nil

  defp log(%{recognition: recognition} = log) do
    [{"Log", recognition(recognition)}] ++
      current(log[:current]) ++
      snapshots(log[:stored_snapshots]) ++
      Labels.optional("Listing", log[:completeness]) ++
      components(log[:components])
  end

  defp log(_log), do: []

  defp recognition(%{legacy: true} = recognition),
    do:
      "legacy snapshot log" <>
        if(recognition[:index] == :partial, do: " (index partly read)", else: "")

  defp recognition(recognition),
    do: [recognition[:kind], recognition[:state]] |> Enum.reject(&is_nil/1) |> Enum.join(", ")

  defp current(%{offset: offset}), do: [{"Current snapshot", "at offset #{offset}"}]
  defp current(_current), do: [{"Current snapshot", "none"}]

  defp snapshots(snapshots) when is_list(snapshots),
    do: [{"Stored snapshots", "#{length(snapshots)}"}]

  defp snapshots(_snapshots), do: []

  defp components(components) when is_map(components) do
    for {name, %{decoded: {:ok, %{path: path, value: %{sid: sid}}}}} <- Enum.sort(components),
        do: {"Component #{name}", "#{Bytes.name(path)} (sid #{sid})"}
  end

  defp components(_components), do: []

  defp flags(overview) do
    Labels.optional("Soft deleted", overview[:soft_deleted] && "yes") ++
      Labels.optional("Creation", overview[:creation]) ++
      Labels.optional("Messages", messages(overview[:interpretation]))
  end

  defp messages(:logical), do: "logical: inherited messages, then the stream's own"
  defp messages(:own_records), do: "the stream's own records"
  defp messages(_interpretation), do: nil
end
