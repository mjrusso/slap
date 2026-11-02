defmodule Slap.Inspect.Command do
  @moduledoc false

  def split(text), do: split(text, nil, false, "", [])

  defp split(<<>>, nil, started, word, words),
    do: {:ok, Enum.reverse(if(started, do: [word | words], else: words))}

  defp split(<<>>, _quote, _started, _word, _words),
    do: {:error, "Unterminated quote; close it and submit again"}

  defp split(<<"\\x", bytes::binary>>, quote, _started, word, words) when quote != ?' do
    with <<hex::binary-size(2), rest::binary>> <- bytes,
         {:ok, byte} <- Base.decode16(hex, case: :mixed) do
      split(rest, quote, true, word <> byte, words)
    else
      _ -> {:error, "Invalid byte escape; use two hexadecimal digits"}
    end
  end

  defp split(<<"\\u", hex::binary-size(4), rest::binary>>, quote, _started, word, words)
       when quote != ?' do
    case Base.decode16(hex, case: :mixed) do
      {:ok, <<codepoint::16>>} when codepoint not in 0xD800..0xDFFF ->
        split(rest, quote, true, word <> <<codepoint::utf8>>, words)

      _ ->
        {:error, "Invalid Unicode escape; use four hexadecimal digits"}
    end
  end

  defp split(<<"\\", char::utf8, rest::binary>>, quote, _started, word, words) when quote != ?',
    do: split(rest, quote, true, word <> escaped(char), words)

  defp split("\\", quote, _started, _word, _words) when quote != ?',
    do: {:error, "Trailing backslash; escape it or finish the command"}

  defp split(<<char, rest::binary>>, nil, _started, word, words) when char in [?', ?"],
    do: split(rest, char, true, word, words)

  defp split(<<char, rest::binary>>, char, _started, word, words) when char in [?', ?"],
    do: split(rest, nil, true, word, words)

  defp split(<<char, rest::binary>>, nil, started, word, words) when char in [32, 9, 10, 13],
    do: split(rest, nil, false, "", if(started, do: [word | words], else: words))

  defp split(<<char::utf8, rest::binary>>, quote, _started, word, words),
    do: split(rest, quote, true, word <> <<char::utf8>>, words)

  def quote(value) do
    cond do
      not String.valid?(value) ->
        bytes = for <<byte <- value>>, into: "", do: "\\x" <> Base.encode16(<<byte>>)
        "\"" <> bytes <> "\""

      value != "" and Regex.match?(~r/\A[^\s"'\\\x00-\x1F\x7F-\x9F]+\z/u, value) ->
        value

      true ->
        JSON.encode!(value)
    end
  end

  # quote/1 writes the query editor's syntax, which split/1 reads back;
  # shell_quote/1 writes POSIX sh syntax, for commands run in a shell.
  def shell_quote(word) do
    if Regex.match?(~r/\A[A-Za-z0-9_@%+=:,.\/-]+\z/, word),
      do: word,
      else: "'" <> String.replace(word, "'", "'\\''") <> "'"
  end

  defp escaped(?b), do: "\b"
  defp escaped(?f), do: "\f"
  defp escaped(?n), do: "\n"
  defp escaped(?r), do: "\r"
  defp escaped(?t), do: "\t"
  defp escaped(char), do: <<char::utf8>>
end
