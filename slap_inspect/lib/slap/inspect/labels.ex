defmodule Slap.Inspect.Labels do
  @moduledoc false

  alias Slap.Inspect.{Bytes, ShardRows}

  @preview 60
  @flags [reference: "fork", pending: "pending", closed: "closed", soft_deleted: "deleted"]

  @doc "Returns a one-line record label using decoded fields and a value preview."
  def label(%{decoded: {:ok, decoded}} = row), do: decoded(decoded, row)
  def label(row), do: Bytes.text(row.key)

  defp decoded(%{kind: :message, offset: offset, part: part}, row), do: message(row, offset, part)

  defp decoded(%{family: :record, id: id, value: %{size: size} = file}, _row),
    do:
      [Bytes.name(id), size(size), file[:content_type], file.storage]
      |> Enum.reject(&is_nil/1)
      |> Enum.join(" · ")

  defp decoded(%{family: :intent, id: id, value: %{ref: ref}}, _row),
    do: "intent #{Bytes.hex_text(id)} · #{Enum.map_join(ref, "/", &Bytes.name/1)}"

  defp decoded(%{family: :object, id: key}, _row), do: "object #{Bytes.name(key)}"

  defp decoded(%{kind: :row, partition: partition, key: key}, %{value: value} = row)
       when is_binary(value),
       do:
         "#{Bytes.name(partition)} / #{Bytes.name(key)} · #{size(row.value_size)} · " <>
           preview(value)

  defp decoded(%{kind: :row, partition: partition, key: key}, _row),
    do: "#{Bytes.name(partition)} / #{Bytes.name(key)}"

  defp decoded(
         %{kind: :files_partition, namespace: namespace, family: family, suffix: suffix},
         _row
       ),
       do: "#{family} #{Bytes.name(namespace)} / #{Bytes.name(suffix)}"

  defp decoded(%{kind: :partition, partition: partition}, _row), do: Bytes.name(partition)
  defp decoded(%{kind: :confirm}, _row), do: "confirm (the shard writer's ownership check)"
  defp decoded(%{kind: :log, base: base}, _row), do: Bytes.name(base)

  defp decoded(%{kind: :yjs_document, service: service, name: name}, _row),
    do: "#{Bytes.name(service)} / #{Bytes.name(name)}"

  defp decoded(%{kind: :overview}, _row), do: "overview"

  defp decoded(%{kind: :cluster_view, cluster: cluster, view: view, counts: counts}, _row),
    do: Enum.join([inspect(cluster), view | List.wrap(spread(counts))], " · ")

  defp decoded(%{kind: :meta, path: path, value: meta}, _row) when is_map(meta) do
    flags = for {key, label} <- @flags, meta[key], do: label
    Enum.join([Bytes.name(path) | List.wrap(meta[:content_type])] ++ flags, " · ")
  end

  defp decoded(%{path: path}, _row), do: Bytes.name(path)
  defp decoded(%{kind: kind} = fields, row), do: "#{kind} #{Bytes.text(fields[:key] || row.key)}"
  defp decoded(_fields, row), do: Bytes.text(row.key)

  defp message(row, offset, part) do
    part = if part == 0, do: [], else: ["part #{part}"]
    Enum.join(["@#{offset}" | part] ++ [size(row.value_size), preview(row.value)], " · ")
  end

  @doc "Formats row counts across shards from `Slap.Inspect.ShardRows.count/4`."
  def spread(nil), do: nil

  def spread(counts) do
    holding = Enum.filter(counts, &ShardRows.rows?(&1.rows))
    missed = Enum.count(counts, &(not is_tuple(&1.rows)))

    found =
      case holding do
        [] when missed > 0 -> "no rows found"
        [] -> "empty"
        _ -> "#{total(holding)} on #{length(holding)} of #{length(counts)} shards"
      end

    if missed > 0, do: found <> " · #{plural(missed, "shard")} not counted", else: found
  end

  defp total(holding) do
    rows = holding |> Enum.map(&elem(&1.rows, 1)) |> Enum.sum()

    if Enum.any?(holding, &match?({:at_least, _rows}, &1.rows)),
      do: "#{rows}+ rows",
      else: plural(rows, "row")
  end

  defp plural(1, noun), do: "1 #{noun}"
  defp plural(count, noun), do: "#{count} #{noun}s"

  @doc "Formats a shard's row count or read status from `Slap.Inspect.ShardRows.count/4`."
  def rows(nil), do: ""
  def rows({:exactly, 0}), do: "empty"
  def rows({:exactly, rows}), do: plural(rows, "row")
  def rows({:at_least, rows}), do: "#{rows}+ rows"
  def rows(:unchecked), do: "not counted in time"
  def rows(:unreadable), do: "unreadable"

  @doc "A stream offset in the wire format."
  def offset(offset),
    do: "0000000000000000_" <> String.pad_leading(Integer.to_string(offset), 16, "0")

  defp size(bytes), do: "#{bytes} B"

  defp preview(value) do
    head =
      if byte_size(value) > @preview,
        do: Bytes.drop_partial_end(binary_part(value, 0, @preview)),
        else: value

    text = if String.valid?(head), do: head |> String.split() |> Enum.join(" "), else: head
    if Bytes.plain?(text), do: Bytes.display(text), else: Bytes.text(head)
  end

  def optional(_name, value) when value in [nil, false], do: []
  def optional(name, value), do: [{name, to_string(value)}]
end
