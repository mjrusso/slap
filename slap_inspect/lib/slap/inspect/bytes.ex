defmodule Slap.Inspect.Bytes do
  @moduledoc false

  alias BackBreeze.{Ucwidth, Utils}

  @hex_bytes 32

  @doc """
  Renders `binary` on one line. Valid UTF-8 is quoted; other binaries show
  any printable ASCII prefix followed by the remaining bytes in hexadecimal.
  """
  def text(binary) when is_binary(binary) do
    if String.valid?(binary), do: quoted(binary), else: mixed(binary, ascii_prefix(binary, 0))
  end

  @doc """
  Renders a name without surrounding quotes when possible, escaping
  characters the terminal cannot display.
  """
  def name(binary) do
    if plain?(binary) and not String.contains?(binary, [" ", "\"", "\\"]),
      do: display(binary),
      else: text(binary)
  end

  @doc """
  Checks for printable UTF-8 without control characters. `String.printable?/1`
  alone accepts ESC, CR, BEL and other terminal controls.
  """
  def plain?(binary), do: String.printable?(binary) and not Regex.match?(~r/[[:cntrl:]]/u, binary)

  @doc "Removes UTF-8 continuation bytes at the start of a byte range."
  def drop_partial_start(binary), do: drop_continuation(binary, 3)

  defp drop_continuation(<<0b10::2, _::6, rest::binary>>, n) when n > 0,
    do: drop_continuation(rest, n - 1)

  defp drop_continuation(binary, _n), do: binary

  @doc "Removes an incomplete UTF-8 character at the end of a byte range."
  def drop_partial_end(binary) do
    size = byte_size(binary)

    Enum.find_value(1..min(3, size)//1, binary, fn cut ->
      if partial?(binary_part(binary, size, -cut)), do: binary_part(binary, 0, size - cut)
    end)
  end

  defp partial?(<<0b110::3, _::5>>), do: true

  defp partial?(<<0b1110::4, _::4, rest::binary>>) when byte_size(rest) < 2,
    do: continuations?(rest)

  defp partial?(<<0b11110::5, _::3, rest::binary>>) when byte_size(rest) < 3,
    do: continuations?(rest)

  defp partial?(_tail), do: false

  defp continuations?(<<0b10::2, _::6, rest::binary>>), do: continuations?(rest)
  defp continuations?(rest), do: rest == <<>>

  defp mixed(binary, 0), do: hex_text(binary)

  defp mixed(binary, size) do
    <<prefix::binary-size(^size), rest::binary>> = binary
    quoted(prefix) <> " " <> hex_text(rest)
  end

  defp quoted(text),
    do: text |> inspect(binaries: :as_strings, printable_limit: 200) |> display()

  defp ascii_prefix(binary, size) do
    case binary do
      <<_::binary-size(^size), byte, _::binary>> when byte in 32..126 ->
        ascii_prefix(binary, size + 1)

      _ ->
        size
    end
  end

  @doc "Formats up to #{@hex_bytes} bytes as hexadecimal with a `0x` prefix."
  def hex_text(binary) when byte_size(binary) > @hex_bytes,
    do: hex_text(binary_part(binary, 0, @hex_bytes)) <> "…"

  def hex_text(binary), do: "0x" <> Base.encode16(binary, case: :lower)

  @doc "Escapes terminal controls except newline, and zero-width code points, as `\\u{…}`."
  def display(text) do
    for <<codepoint::utf8 <- text>>, into: "" do
      if escape?(codepoint),
        do: "\\u{" <> Integer.to_string(codepoint, 16) <> "}",
        else: <<codepoint::utf8>>
    end
  end

  defp escape?(?\n), do: false
  defp escape?(codepoint) when codepoint < 32 or codepoint in 127..159, do: true
  # BackBreeze 0.4 stores one codepoint per cell, overwriting zero-width marks.
  defp escape?(codepoint), do: codepoint > 127 and Ucwidth.width_codepoint(codepoint) == 0

  def head(text, columns) do
    text
    |> String.graphemes()
    |> Enum.reduce_while({"", 0}, fn character, {text, used} ->
      next = used + Utils.string_length(character)
      if next > columns, do: {:halt, {text, used}}, else: {:cont, {text <> character, next}}
    end)
    |> elem(0)
  end

  def tail(text, columns) do
    {characters, _width} =
      text
      |> String.graphemes()
      |> Enum.reverse()
      |> Enum.reduce_while({[], 0}, fn character, {characters, width} ->
        next = width + Utils.string_length(character)

        if next > columns,
          do: {:halt, {characters, width}},
          else: {:cont, {[character | characters], next}}
      end)

    IO.iodata_to_binary(characters)
  end

  @doc """
  Formats a hex dump for `width` columns, starting at byte offset `offset`.
  """
  def hex(binary, offset, width) do
    {per_line, layout} =
      Enum.find([{16, :spaced}, {16, :grouped}, {8, :spaced}, {8, :grouped}], {4, :spaced}, fn
        {count, layout} -> line_width(count, layout) <= width
      end)

    binary
    |> :binary.bin_to_list()
    |> Enum.chunk_every(per_line)
    |> Enum.with_index()
    |> Enum.map_join("\n", fn {bytes, row} ->
      at = offset + row * per_line
      left = at |> Integer.to_string(16) |> String.pad_leading(8, "0")
      data = String.pad_trailing(hex_data(bytes, layout), data_width(per_line, layout))
      left <> "  " <> data <> "  " <> List.to_string(Enum.map(bytes, &printable/1))
    end)
  end

  defp line_width(count, layout), do: 8 + 2 + data_width(count, layout) + 2 + count

  defp data_width(count, :spaced), do: 3 * count - 1
  defp data_width(count, :grouped), do: 2 * count + div(count, 4) - 1

  defp hex_data(bytes, :spaced), do: Enum.map_join(bytes, " ", &hex_byte/1)

  defp hex_data(bytes, :grouped),
    do:
      bytes
      |> Enum.chunk_every(4)
      |> Enum.map_join(" ", &Enum.map_join(&1, "", fn byte -> hex_byte(byte) end))

  defp hex_byte(byte), do: byte |> Integer.to_string(16) |> String.pad_leading(2, "0")

  defp printable(byte) when byte in 32..126, do: byte
  defp printable(_byte), do: ?.
end
