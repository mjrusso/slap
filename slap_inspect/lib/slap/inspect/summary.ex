defmodule Slap.Inspect.Summary do
  @moduledoc false

  alias Slap.Inspect.{Bytes, Hex, Labels, Overview}

  @doc """
  Renders identifying fields, other decoded fields and interpretations as
  aligned text blocks. Returns an empty string for document content.
  """
  def render(row) do
    {fields, other} = fields(row)
    blocks = Enum.reject([fields, flatten(other, ""), interpretation(row)], &(&1 == []))

    width =
      blocks |> Enum.concat() |> Enum.map(&String.length(elem(&1, 0))) |> Enum.max(fn -> 0 end)

    Enum.map_join(blocks, "\n\n", &aligned(&1, width))
  end

  defp aligned(fields, width) do
    Enum.map_join(fields, "\n", fn {name, value} ->
      String.pad_trailing(name, width + 2) <> value
    end)
  end

  defp fields(%{decoded: {:ok, %{kind: :overview, value: overview}}}),
    do: {Overview.fields(overview), %{}}

  defp fields(%{decoded: {:ok, %{kind: :cluster_view} = listing}}) do
    shards =
      for count <- listing.counts || [],
          do:
            {"Shard #{count.shard}",
             if(count.owner, do: Labels.rows(count.rows), else: "unassigned")}

    {[
       {"Cluster", inspect(listing.cluster)},
       {"View", "#{listing.view}"},
       {"Rows", Labels.spread(listing.counts) || "not counted"}
     ] ++ shards, %{}}
  end

  defp fields(%{decoded: {:ok, %{kind: :files_partition} = partition}} = row),
    do: decoded(partition, row)

  defp fields(%{decoded: {:ok, %{kind: :log, base: base}}}),
    do: {[{"Log", Bytes.name(base)}], %{}}

  defp fields(%{decoded: {:ok, %{kind: :partition, partition: partition}}}),
    do: {[{"Partition", Bytes.name(partition)}], %{}}

  defp fields(%{decoded: {:ok, %{kind: :yjs_document} = document}}),
    do:
      {[
         {"Service", Bytes.name(document.service)},
         {"Document", Bytes.name(document.name)},
         {"Base", Bytes.name(document.base)}
       ], %{}}

  defp fields(%{decoded: {:ok, decoded}} = row) do
    {fields, other} = decoded(decoded, row)
    {fields ++ stored(row), other}
  end

  defp fields(row), do: {[{"Key", Bytes.text(row.key)} | stored(row)], %{}}

  defp decoded(%{kind: :message, offset: offset, part: part, sid: sid} = decoded, row) do
    {[{"Offset", Labels.offset(offset)}] ++
       Labels.optional("Part", part > 0 && part) ++
       [{"Stream", "sid #{sid}"}] ++ Labels.optional("Content type", row[:content_type]),
     other(decoded)}
  end

  defp decoded(%{family: :record, id: id, value: value}, _row) do
    {file, other} = Map.split(value, [:size, :content_type, :storage, :body, :sha256, :metadata])

    {[
       {"File", Bytes.name(id)},
       {"Size", "#{file.size} bytes"}
     ] ++
       Labels.optional("Content type", file[:content_type]) ++
       [{"Storage", "#{file.storage}"}] ++
       object(file.body) ++
       [{"SHA-256", Base.encode16(file.sha256, case: :lower)}] ++ metadata(file[:metadata]),
     other}
  end

  defp decoded(%{family: :intent, id: id, value: value}, _row) do
    {intent, other} = Map.split(value, [:ref, :due_ms, :taken])

    {[
       {"Intent", Bytes.hex_text(id)},
       {"File", Enum.map_join(intent.ref, "/", &Bytes.name/1)},
       {"Due", time(intent.due_ms)},
       {"Taken", inspect(intent.taken)}
     ], other}
  end

  defp decoded(%{kind: :files_partition} = fields, _row),
    do:
      {[
         {"Partition", Bytes.name(fields.partition)},
         {"Namespace", Bytes.name(fields.namespace)},
         {"Family", "#{fields.family}"},
         {"Suffix", Bytes.name(fields.suffix)}
       ], other(fields)}

  defp decoded(%{kind: :row, partition: partition, key: key} = decoded, _row),
    do: {[{"Partition", Bytes.name(partition)}, {"Key", Bytes.name(key)}], other(decoded)}

  defp decoded(%{kind: :meta, path: path, value: %{sid: _} = value}, _row) do
    {meta, other} = Map.split(value, [:sid, :content_type, :created_ms, :closed, :type])

    {[{"Path", Bytes.name(path)}, {"Stream", "sid #{meta.sid}"}] ++
       Labels.optional("Content type", meta[:content_type]) ++
       [{"Created", time(meta[:created_ms])}, {"Closed", "#{meta[:closed]}"}], other}
  end

  defp decoded(%{kind: kind} = decoded, row),
    do: {[{"Kind", "#{kind}"}, {"Key", Bytes.text(row.key)}], other(decoded)}

  defp decoded(decoded, row), do: {[{"Key", Bytes.text(row.key)}], other(decoded)}

  defp other(%{value: value}) when is_map(value) and not is_struct(value), do: value
  defp other(%{value: value}) when is_integer(value), do: %{decoded: value}
  defp other(_decoded), do: %{}

  @doc """
  Checks whether the summary includes the fields needed to replace the value's text.
  """
  def summarizes?(%{decoded: {:ok, %{kind: kind}}})
      when kind in [:log, :yjs_document, :cluster_view],
      do: true

  def summarizes?(%{decoded: {:ok, decoded}}), do: other(decoded) != %{}
  def summarizes?(_row), do: false

  defp stored(row),
    do:
      Labels.optional("Seq", row[:seq]) ++
        Labels.optional("Value", row[:value_size] && "#{row.value_size} bytes#{truncated(row)}")

  defp truncated(%{truncated: true}), do: ", truncated"
  defp truncated(_row), do: ""

  defp object([:object, key]), do: [{"Object", Bytes.name(key)}]
  defp object(_body), do: []

  defp metadata(metadata) when is_map(metadata), do: flatten(metadata, "Metadata ")
  defp metadata(_metadata), do: []

  defp interpretation(%{interpretation: {:ok, fields}}) when is_map(fields),
    do: [{"Interpretation", ""} | flatten(fields, "  ")]

  defp interpretation(%{interpretation: {:error, reason}}),
    do: [{"Interpretation", "error: " <> inspect(reason)}]

  defp interpretation(_row), do: []

  defp flatten(map, prefix) do
    map
    |> Enum.reject(&match?({_key, nil}, &1))
    |> Enum.sort()
    |> Enum.flat_map(fn
      {key, value} when is_map(value) and not is_struct(value) and map_size(value) > 0 ->
        flatten(value, prefix <> name(key, prefix) <> " ")

      {key, value} ->
        [{prefix <> name(key, prefix), value(key, value)}]
    end)
  end

  defp name(key, prefix) when is_atom(key) do
    name =
      key
      |> Atom.to_string()
      |> String.replace(~r/_at_ms$/, "_at")
      |> String.replace(~r/_(ms|s)$/, " (\\1)")
      |> String.replace("_", " ")

    if String.trim(prefix) == "", do: String.capitalize(name), else: name
  end

  defp name(key, _prefix) when is_binary(key), do: Bytes.name(key)
  defp name(key, _prefix), do: inspect(key)

  defp value(key, value) when is_atom(key) and is_integer(value) do
    if String.ends_with?(Atom.to_string(key), "_at_ms"), do: time(value), else: value(value)
  end

  defp value(_key, value), do: value(value)

  defp value(value) when is_binary(value),
    do: if(String.valid?(value), do: Bytes.name(value), else: Bytes.hex_text(value))

  defp value(value) when is_atom(value), do: value |> inspect() |> String.trim_leading(":")

  defp value(value),
    do: value |> Hex.wrap() |> inspect(limit: 20, printable_limit: 256) |> Bytes.display()

  # Stream timestamps may exceed DateTime's supported range.
  defp time(ms) when is_integer(ms) do
    case DateTime.from_unix(ms, :millisecond) do
      {:ok, time} -> DateTime.to_iso8601(time)
      {:error, _reason} -> "#{ms} ms"
    end
  end

  defp time(_ms), do: "unknown"
end
