defmodule Slap.Inspect.Details do
  @moduledoc false

  alias Slap.Inspect.{Bytes, Hex, Request, Summary, Wrap}

  @doc "Returns `{mode, text}`, falling back when the requested mode is unavailable."
  def render(nil, _preview, requested, _width),
    do: {requested, "Select a record to show it here."}

  def render(row, preview, requested, width) do
    %{modes: modes, value: value, bytes: bytes} = view(row, preview)
    mode = shown(modes, requested)

    note =
      if mode in [requested, :summary] or bytes.value == "",
        do: "",
        else: fallback(requested, mode)

    range = if match?({:content, _label, _json}, value), do: "", else: range(bytes, mode)
    body = body(row, value, bytes, mode, max(width, 1))
    sections = [Summary.render(row), note, range, body]
    {mode, Enum.join(Enum.reject(sections, &(&1 == "")), "\n\n")}
  end

  @doc """
  Returns `%{modes: modes, value: value, bytes: bytes}`.
  `modes` lists available display modes in cycling order. `value` is tagged
  document content or JSON, or `:none`. `bytes` comes from `preview` when
  supplied, otherwise from `row`.
  """
  def view(row, preview) do
    bytes = preview || row

    case content(row) do
      # Content is decoded on the server; it has no stored bytes to show.
      {:ok, label, json} ->
        %{modes: [:text, :json, :term], value: {:content, label, json}, bytes: bytes}

      :none ->
        json = json(bytes)

        modes =
          Enum.concat([
            first(row, bytes),
            if(json, do: [:json], else: []),
            if(bytes.value == "", do: [], else: [:bytes]),
            [:term]
          ])

        %{modes: modes, value: json || :none, bytes: bytes}
    end
  end

  defp first(row, bytes) do
    cond do
      readable_text?(bytes) -> [:text]
      Summary.summarizes?(row) -> [:summary]
      true -> []
    end
  end

  def shown(modes, requested), do: Enum.find(fallbacks(requested), &(&1 in modes))

  defp fallbacks(mode) when mode in [:text, :summary], do: [:text, :summary, :bytes, :term]
  defp fallbacks(:json), do: [:json | fallbacks(:text)]
  defp fallbacks(:bytes), do: [:bytes, :text, :term]
  defp fallbacks(:term), do: [:term]

  # Decoding to pairs preserves stored object key order and duplicate keys.
  defp json(bytes) do
    case JSON.decode(bytes.value, [], ordered_objects()) do
      {json, [], ""} -> {:value, json}
      _incomplete -> nil
    end
  end

  defp ordered_objects,
    do: [
      object_start: fn _acc -> [] end,
      object_push: fn key, value, pairs -> [{key, value} | pairs] end,
      object_finish: fn pairs, acc -> {{:object, Enum.reverse(pairs)}, acc} end
    ]

  defp fallback(:text, mode), do: "No UTF-8 text in this range; showing #{mode}."
  defp fallback(:json, mode), do: "Not a complete JSON value; showing #{mode}."

  defp readable_text?(%{value: ""}), do: false

  defp readable_text?(bytes), do: String.valid?(text_range(bytes))

  # Text paging advances by complete characters. Switching from bytes can
  # still leave a partial character at the start of the range.
  defp text_range(bytes) do
    text = text_until_cut(bytes)
    if (bytes[:offset] || 0) > 0, do: Bytes.drop_partial_start(text), else: text
  end

  defp text_until_cut(bytes) do
    size = range_size(bytes, :text)
    text = binary_part(bytes.value, 0, size)

    if (bytes[:offset] || 0) + size < bytes.value_size,
      do: Bytes.drop_partial_end(text),
      else: text
  end

  @doc "The byte count used to advance to the next range."
  def range_step(bytes, :text) do
    case byte_size(text_until_cut(bytes)) do
      0 -> range_size(bytes, :text)
      shown -> shown
    end
  end

  def range_step(bytes, mode), do: range_size(bytes, mode)

  def range_size(%{value: value}, :json), do: byte_size(value)
  def range_size(%{value: value}, _mode), do: min(byte_size(value), Request.range_bytes())

  defp range(_bytes, mode) when mode in [:summary, :term], do: ""

  defp range(%{value_size: total} = bytes, mode) do
    offset = bytes[:offset] || 0
    size = range_size(bytes, mode)
    interval = if size == 0, do: "empty at #{offset}", else: "#{offset}–#{offset + size - 1}"
    ending = if offset + size >= total, do: " · EOF", else: " · m Next range"
    partial = if bytes[:stopped] == :deadline, do: " · Partial read", else: ""
    truncated = if bytes[:truncated], do: " · Truncated", else: ""

    split =
      if mode == :text and byte_size(text_range(bytes)) < byte_size(text_until_cut(bytes)),
        do: " · Split characters hidden",
        else: ""

    "Bytes #{interval} of #{total}" <> ending <> partial <> truncated <> split
  end

  defp range(_bytes, _mode), do: ""

  defp body(_row, _json, _bytes, :summary, _width), do: ""

  defp body(_row, _json, bytes, :bytes, width),
    do:
      Bytes.hex(
        binary_part(bytes.value, 0, range_size(bytes, :bytes)),
        bytes[:offset] || 0,
        width
      )

  defp body(row, json, bytes, :term, width),
    do: term(json, row |> Map.merge(bytes) |> Map.delete(:value), width)

  defp body(_row, json, bytes, :text, width) do
    text =
      case json do
        {:content, label, json} -> label <> "\n\n" <> content_text(json)
        _stored -> text_range(bytes)
      end

    "Text (wrapped)\n" <> wrap(safe_text(text), width)
  end

  defp body(_row, json, _bytes, :json, width) do
    {label, json} =
      case json do
        {:content, label, json} -> {label, json}
        {:value, json} -> {"JSON", json}
      end

    wrap(label <> "\n" <> Bytes.display(pretty_json(json)), width)
  end

  defp content(%{decoded: {:ok, %{value: %{content: {:json, json}} = result}}}),
    do: {:ok, result[:label] || "Document content", JSON.decode!(json)}

  defp content(_row), do: :none

  defp content_text(%{"roots" => roots}) do
    Enum.map_join(roots, "\n\n", fn root ->
      interpretation =
        cond do
          root["inferred"] -> " (#{root["interpretation"]}, inferred)"
          root["interpretation"] -> " (#{root["interpretation"]})"
          true -> ""
        end

      root["name"] <> interpretation <> "\n" <> root_text(root)
    end)
  end

  defp content_text(json), do: pretty_json(json)

  defp root_text(%{"value" => value}) when is_binary(value), do: value
  defp root_text(%{"value" => value}) when not is_nil(value), do: pretty_json(value)

  defp root_text(%{"status" => "wrong_type", "interpretation" => type, "holds" => holds}),
    do:
      "Cannot display as #{type}: this root contains #{holds(holds)}. Press i to choose another type."

  defp root_text(%{"status" => "ambiguous"}),
    do:
      "Type uncertain: this may be text containing only embeds, an array or XML. Press i to choose a type."

  defp root_text(%{"status" => "uninterpreted"} = root) do
    holds = if root["holds"], do: " It contains #{holds(root["holds"])}.", else: ""
    "No root type selected." <> holds <> " Press i to choose a type."
  end

  defp root_text(%{"status" => "absent"}), do: "Not in this document."
  defp root_text(root), do: pretty_json(root)

  defp holds("map"), do: "map entries"
  defp holds("sequence"), do: "a sequence: an array, XML or text containing only embeds"
  defp holds("text"), do: "text"
  defp holds(other), do: other

  defp term(json, fields, width) do
    case json do
      {:content, label, json} ->
        wrap(label, width) <> "\n" <> inspect_fields(json, width, :infinity)

      _stored ->
        legacy_label(fields) <> inspect_fields(fields, width)
    end
  end

  defp inspect_fields(fields, width, printable_limit \\ 256),
    do:
      fields
      |> Hex.wrap()
      |> inspect(
        pretty: true,
        width: width,
        limit: :infinity,
        binaries: :as_strings,
        printable_limit: printable_limit,
        custom_options: [sort_maps: true]
      )
      |> Bytes.display()

  defp legacy_label(%{
         decoded: {:ok, %{value: %{log: %{recognition: %{legacy: true, index: :partial}}}}}
       }),
       do: "legacy snapshot log (index partly read)\n"

  defp legacy_label(_fields), do: ""

  defp pretty_json(value), do: pretty_json(value, 0)

  defp pretty_json({:object, pairs}, level) do
    entries =
      Enum.map(pairs, fn {key, value} ->
        JSON.encode!(key) <> ": " <> pretty_json(value, level + 1)
      end)

    container(entries, "{", "}", level)
  end

  # Reconstructed maps have no stored key order to preserve.
  defp pretty_json(map, level) when is_map(map),
    do: pretty_json({:object, Enum.sort(map)}, level)

  defp pretty_json(list, level) when is_list(list),
    do: container(Enum.map(list, &pretty_json(&1, level + 1)), "[", "]", level)

  defp pretty_json(value, _level), do: JSON.encode!(value)

  defp container([], left, right, _level), do: left <> right

  defp container(entries, left, right, level) do
    indent = String.duplicate("  ", level)

    left <>
      "\n" <>
      indent <> "  " <> Enum.join(entries, ",\n" <> indent <> "  ") <> "\n" <> indent <> right
  end

  defp safe_text(text),
    do: text |> String.replace("\\", "\\\\") |> String.replace("\t", "    ") |> Bytes.display()

  defp wrap(text, width), do: Wrap.lines(text, width)
end
