defmodule Slap.Inspect.Command do
  @moduledoc false

  def split(text) do
    case words(text) do
      {:ok, words} -> {:ok, Enum.map(words, & &1.value)}
      {:error, message, _words, _at} -> {:error, message}
    end
  end

  @doc """
  Tokenizes `text` into maps with `value`, `start`, `stop` and `plain` fields.
  `start` and `stop` are byte offsets; `plain` marks words without quotes or
  escapes.

  Returns `{:ok, words}` or `{:error, message, words, at}`. On error, `words`
  contains the completed tokens and `at` is the start of the invalid token.
  """
  def words(text), do: scan(text, 0, nil, nil, [])

  defp scan(<<>>, pos, nil, word, words), do: {:ok, Enum.reverse(close(word, pos, words))}

  defp scan(<<>>, _pos, _quote, word, words),
    do: error("Unterminated quote; close it and submit again", word, words)

  defp scan(<<"\\x", bytes::binary>>, pos, quote, word, words) when quote != ?' do
    with <<hex::binary-size(2), rest::binary>> <- bytes,
         {:ok, byte} <- Base.decode16(hex, case: :mixed) do
      scan(rest, pos + 4, quote, escape(word, pos, byte), words)
    else
      _ -> error("Invalid byte escape; use two hexadecimal digits", open(word, pos), words)
    end
  end

  defp scan(<<"\\u", hex::binary-size(4), rest::binary>>, pos, quote, word, words)
       when quote != ?' do
    case Base.decode16(hex, case: :mixed) do
      {:ok, <<codepoint::16>>} when codepoint not in 0xD800..0xDFFF ->
        scan(rest, pos + 6, quote, escape(word, pos, <<codepoint::utf8>>), words)

      _ ->
        error("Invalid Unicode escape; use four hexadecimal digits", open(word, pos), words)
    end
  end

  defp scan(<<"\\", char::utf8, rest::binary>>, pos, quote, word, words) when quote != ?' do
    size = byte_size(<<char::utf8>>)
    scan(rest, pos + 1 + size, quote, escape(word, pos, escaped(char)), words)
  end

  defp scan("\\", pos, quote, word, words) when quote != ?',
    do: error("Trailing backslash; escape it or finish the command", open(word, pos), words)

  defp scan(<<char, rest::binary>>, pos, nil, word, words) when char in [?', ?"],
    do: scan(rest, pos + 1, char, escape(word, pos, ""), words)

  defp scan(<<char, rest::binary>>, pos, char, word, words) when char in [?', ?"],
    do: scan(rest, pos + 1, nil, word, words)

  defp scan(<<char, rest::binary>>, pos, nil, word, words) when char in [32, 9, 10, 13],
    do: scan(rest, pos + 1, nil, nil, close(word, pos, words))

  defp scan(<<char::utf8, rest::binary>>, pos, quote, word, words) do
    word = open(word, pos)
    character = <<char::utf8>>
    scan(rest, pos + byte_size(character), quote, %{word | value: word.value <> character}, words)
  end

  defp open(nil, pos), do: %{value: "", start: pos, plain: true}
  defp open(word, _pos), do: word

  defp escape(word, pos, value) do
    word = open(word, pos)
    %{word | value: word.value <> value, plain: false}
  end

  defp close(nil, _pos, words), do: words
  defp close(word, pos, words), do: [Map.put(word, :stop, pos) | words]

  defp error(message, word, words), do: {:error, message, Enum.reverse(words), word.start}

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
