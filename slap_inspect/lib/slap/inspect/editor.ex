defmodule Slap.Inspect.Editor do
  @moduledoc false

  alias BackBreeze.Utils
  alias Slap.Inspect.{Bytes, CLI, Command, Operation}

  defstruct text: "", cursor: 0, history: [], index: 0, draft: "", matches: []

  def new(text, history),
    do: %__MODULE__{text: text, cursor: String.length(text), history: history, draft: text}

  def input(editor, key), do: apply_key(%{editor | matches: []}, key)

  defp apply_key(editor, key) when key in ["Tab", "\t"], do: complete(editor)
  defp apply_key(editor, "Home"), do: %{editor | cursor: 0}
  defp apply_key(editor, "End"), do: %{editor | cursor: String.length(editor.text)}
  defp apply_key(editor, "ArrowLeft"), do: %{editor | cursor: max(editor.cursor - 1, 0)}

  defp apply_key(editor, "ArrowRight"),
    do: %{editor | cursor: min(editor.cursor + 1, String.length(editor.text))}

  defp apply_key(editor, "\x15"), do: %{editor | text: "", cursor: 0}

  defp apply_key(editor, "ArrowUp"),
    do: recall(editor, min(editor.index + 1, length(editor.history)))

  defp apply_key(editor, "ArrowDown"), do: recall(editor, max(editor.index - 1, 0))

  defp apply_key(%{cursor: 0} = editor, key) when key in ["Backspace", "\x7f"], do: editor

  defp apply_key(editor, key) when key in ["Backspace", "\x7f"] do
    {left, right} = String.split_at(editor.text, editor.cursor)
    %{editor | text: String.slice(left, 0..-2//1) <> right, cursor: editor.cursor - 1}
  end

  defp apply_key(editor, "Delete") do
    {left, right} = String.split_at(editor.text, editor.cursor)
    %{editor | text: left <> String.slice(right, 1..-1//1)}
  end

  defp apply_key(editor, key) do
    if String.valid?(key) and String.printable?(key) and String.length(key) == 1 do
      {left, right} = String.split_at(editor.text, editor.cursor)
      %{editor | text: left <> key <> right, cursor: String.length(left <> key)}
    else
      editor
    end
  end

  def line(editor, width) do
    {left, right} = String.split_at(editor.text, editor.cursor)
    prefix = "Command: "
    available = max(width - byte_size(prefix) - 2, 1)
    left = Bytes.tail(Bytes.display(left), available - 1)
    right_width = available - Utils.string_length(left) - 1
    matches = if editor.matches == [], do: "", else: "  · " <> Enum.join(editor.matches, " ")
    prefix <> left <> "_" <> Bytes.head(Bytes.display(right) <> matches, right_width)
  end

  defp complete(editor) do
    {left, _right} = String.split_at(editor.text, editor.cursor)

    case word_at(editor.text, byte_size(left)) do
      {:ok, before, word} -> complete(editor, before, word)
      :none -> editor
    end
  end

  defp complete(editor, before, word) do
    matches =
      before
      |> candidates(word.prefix)
      |> Enum.filter(&String.starts_with?(&1, word.prefix))
      |> Enum.sort()

    case matches do
      [] -> editor
      [match] -> replace(editor, word.start, past_space(editor.text, word.stop), match <> " ")
      several -> complete_shared(editor, word, several)
    end
  end

  # Quoted or escaped words are literal values, even when they resemble options.
  defp word_at(text, offset) do
    {words, unparsed} =
      case Command.words(text) do
        {:ok, words} -> {words, byte_size(text) + 1}
        {:error, _message, words, at} -> {words, at}
      end

    {before, rest} = Enum.split_while(words, &(&1.stop < offset))
    before = Enum.map(before, & &1.value)

    case rest do
      _rest when offset >= unparsed ->
        :none

      [%{start: start, plain: true} = word | _] when start <= offset ->
        prefix = binary_part(word.value, 0, offset - start)
        {:ok, before, %{start: start, stop: word.stop, prefix: prefix}}

      [%{start: start} | _] when start <= offset ->
        :none

      _rest ->
        {:ok, before, %{start: offset, stop: offset, prefix: ""}}
    end
  end

  defp complete_shared(editor, word, matches) do
    case shared_prefix(matches) do
      prefix when prefix == word.prefix -> %{editor | matches: matches}
      prefix -> replace(editor, word.start, word.stop, prefix)
    end
  end

  defp candidates([], _word), do: Operation.commands()
  defp candidates([command | _], word), do: options(command, word)

  defp options(command, word) do
    cond do
      not (word == "" or String.starts_with?(word, "-")) -> []
      command in Operation.commands() -> CLI.query_options(command)
      true -> CLI.query_options()
    end
  end

  defp replace(editor, start, stop, text) do
    head = binary_part(editor.text, 0, start) <> text

    %{
      editor
      | text: head <> binary_part(editor.text, stop, byte_size(editor.text) - stop),
        cursor: String.length(head)
    }
  end

  defp past_space(text, stop) do
    case text do
      <<_::binary-size(^stop), " ", _::binary>> -> stop + 1
      _text -> stop
    end
  end

  defp shared_prefix([first | rest]) do
    Enum.reduce(rest, first, fn match, prefix ->
      prefix
      |> String.graphemes()
      |> Enum.zip(String.graphemes(match))
      |> Enum.take_while(fn {a, b} -> a == b end)
      |> Enum.map_join(&elem(&1, 0))
    end)
  end

  defp recall(editor, index) do
    draft = if editor.index == 0, do: editor.text, else: editor.draft
    text = if index == 0, do: draft, else: Enum.at(editor.history, index - 1)
    %{editor | text: text, cursor: String.length(text), index: index, draft: draft}
  end
end
