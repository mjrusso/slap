defmodule Slap.Inspect.Bytes do
  @moduledoc false

  alias BackBreeze.{Ucwidth, Utils}

  def text(binary) when is_binary(binary) do
    binary
    |> inspect(binaries: :as_strings, limit: 32, printable_limit: 200)
    |> display()
  end

  # BackBreeze 0.4 stores one codepoint per cell, overwriting zero-width marks.
  def display(text) do
    for <<codepoint::utf8 <- text>>, into: "" do
      if codepoint > 127 and Ucwidth.width_codepoint(codepoint) == 0,
        do: "\\u{" <> Integer.to_string(codepoint, 16) <> "}",
        else: <<codepoint::utf8>>
    end
  end

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

  def hex(binary, offset \\ 0) do
    binary
    |> :binary.bin_to_list()
    |> Enum.chunk_every(16)
    |> Enum.with_index()
    |> Enum.map_join("\n", fn {bytes, row} ->
      at = offset + row * 16
      left = at |> Integer.to_string(16) |> String.pad_leading(8, "0")

      data =
        Enum.map_join(bytes, " ", &(&1 |> Integer.to_string(16) |> String.pad_leading(2, "0")))

      printable = Enum.map(bytes, &printable/1)
      left <> "  " <> String.pad_trailing(data, 47) <> "  " <> List.to_string(printable)
    end)
  end

  defp printable(byte) when byte in 32..126, do: byte
  defp printable(_byte), do: ?.
end
