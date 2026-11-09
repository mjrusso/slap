defmodule Slap.Inspect.Wrap do
  @moduledoc false

  alias BackBreeze.Utils

  def lines(text, width),
    do: text |> String.split("\n") |> Enum.map_join("\n", &wrap_line(&1, width))

  defp wrap_line(line, width) do
    {parts, _used} =
      ~r/ +/
      |> Regex.split(line, include_captures: true, trim: true)
      |> Enum.reduce({[], 0}, &wrap_word(&1, &2, width))

    parts |> Enum.reverse() |> IO.iodata_to_binary()
  end

  defp wrap_word(word, {parts, used}, width) do
    size = Utils.string_length(word)

    cond do
      used + size <= width ->
        {[word | parts], used + size}

      String.trim(word) == "" ->
        {["\n" | parts], 0}

      size > width ->
        start = if used == 0, do: {parts, 0}, else: {["\n" | parts], 0}
        word |> String.graphemes() |> Enum.reduce(start, &wrap_character(&1, &2, width))

      true ->
        {[word, "\n" | parts], size}
    end
  end

  defp wrap_character(character, {parts, used}, width) do
    size = Utils.string_length(character)

    if used > 0 and used + size > width,
      do: {[character, "\n" | parts], size},
      else: {[character | parts], used + size}
  end
end
