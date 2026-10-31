defmodule Slap.Inspect.Details do
  @moduledoc false

  alias BackBreeze.Utils
  alias Slap.Inspect.{Bytes, Request}

  def render(nil, _preview, _mode, _width),
    do: "Select a row. Each page is an independent durable read."

  def render(row, preview, mode, width) do
    bytes = preview || row
    range = range(bytes, mode)
    body = body(row, bytes, mode, max(width, 1))
    Enum.join(Enum.reject([range, body], &(&1 == "")), "\n\n")
  end

  def range_size(%{value: value}, :json), do: byte_size(value)
  def range_size(%{value: value}, _mode), do: min(byte_size(value), Request.range_bytes())

  defp range(%{value_size: total} = bytes, mode) do
    offset = bytes[:offset] || 0
    size = range_size(bytes, mode)
    interval = if size == 0, do: "empty at #{offset}", else: "#{offset}–#{offset + size - 1}"
    ending = if offset + size >= total, do: " · EOF", else: " · m Next range"
    partial = if bytes[:stopped] == :deadline, do: " · Partial read", else: ""
    truncated = if bytes[:truncated], do: " · Truncated", else: ""
    "Bytes #{interval} of #{total}" <> ending <> partial <> truncated
  end

  defp range(_bytes, _mode), do: ""

  defp body(row, bytes, :bytes, width) do
    fields = row |> Map.merge(bytes) |> Map.delete(:value)

    metadata(row, fields, width) <>
      "\n\n" <>
      Bytes.hex(binary_part(bytes.value, 0, range_size(bytes, :bytes)), bytes[:offset] || 0)
  end

  defp body(row, bytes, :text, width) do
    text =
      case content(row) do
        {:ok, label, json} -> label <> "\n\n" <> content_text(json)
        :none -> binary_part(bytes.value, 0, range_size(bytes, :text))
      end

    if String.valid?(text),
      do: "Text (wrapped)\n" <> wrap(safe_text(text), width),
      else: "Text unavailable: invalid UTF-8. Press d for JSON or bytes."
  end

  defp body(row, bytes, :json, width) do
    result =
      case content(row) do
        {:ok, label, json} ->
          {:ok, label, json}

        :none ->
          case JSON.decode(bytes.value) do
            {:ok, value} -> {:ok, "JSON", value}
            {:error, _} -> :invalid
          end
      end

    case result do
      {:ok, label, json} -> label <> "\n" <> wrap(Bytes.display(pretty_json(json)), width)
      :invalid -> "JSON unavailable: this range is not a complete JSON value. Press d for bytes."
    end
  end

  defp content(%{decoded: {:ok, %{value: %{content: {:json, json}} = result}}}),
    do: {:ok, result[:label] || "Document content", JSON.decode!(json)}

  defp content(_row), do: :none

  defp content_text(%{"roots" => roots}) do
    Enum.map_join(roots, "\n\n", fn root ->
      value = root["value"]
      interpretation = if root["interpretation"], do: " (#{root["interpretation"]})", else: ""

      root["name"] <>
        interpretation <> "\n" <> if(is_binary(value), do: value, else: pretty_json(root))
    end)
  end

  defp content_text(json), do: pretty_json(json)

  defp metadata(row, fields, width) do
    case content(row) do
      {:ok, label, json} -> label <> "\n" <> inspect_fields(json, width, :infinity)
      :none -> legacy_label(fields) <> inspect_fields(fields, width)
    end
  end

  defp inspect_fields(fields, width, printable_limit \\ 256),
    do:
      fields
      |> inspect(
        pretty: true,
        width: width,
        limit: :infinity,
        binaries: :as_strings,
        printable_limit: printable_limit
      )
      |> Bytes.display()

  defp legacy_label(%{
         decoded: {:ok, %{value: %{log: %{recognition: %{legacy: true, index: :partial}}}}}
       }),
       do: "legacy snapshot log (index partly read)\n"

  defp legacy_label(_fields), do: ""

  defp pretty_json(value), do: pretty_json(value, 0)

  defp pretty_json(map, level) when is_map(map) do
    entries =
      map
      |> Enum.sort()
      |> Enum.map(fn {key, value} ->
        JSON.encode!(key) <> ": " <> pretty_json(value, level + 1)
      end)

    container(entries, "{", "}", level)
  end

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

  defp safe_text(text) do
    for <<codepoint::utf8 <- text>>, into: "" do
      case codepoint do
        ?\n -> "\n"
        ?\t -> "    "
        ?\\ -> "\\\\"
        cp when cp < 32 or cp in 127..159 -> "\\u{" <> Integer.to_string(cp, 16) <> "}"
        cp -> Bytes.display(<<cp::utf8>>)
      end
    end
  end

  defp wrap(text, width),
    do: text |> String.split("\n") |> Enum.map_join("\n", &wrap_line(&1, width))

  defp wrap_line(line, width) do
    {parts, _used} =
      line |> String.graphemes() |> Enum.reduce({[], 0}, &wrap_character(&1, &2, width))

    parts |> Enum.reverse() |> IO.iodata_to_binary()
  end

  defp wrap_character(character, {parts, used}, width) do
    size = Utils.string_length(character)

    if used > 0 and used + size > width,
      do: {[character, "\n" | parts], size},
      else: {[character | parts], used + size}
  end
end
