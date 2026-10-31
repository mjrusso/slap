defmodule Slap.Inspect.Editor do
  @moduledoc false

  alias BackBreeze.Utils
  alias Slap.Inspect.Bytes

  defstruct text: "", cursor: 0, history: [], index: 0, draft: ""

  def new(text, history),
    do: %__MODULE__{text: text, cursor: String.length(text), history: history, draft: text}

  def input(editor, "Home"), do: %{editor | cursor: 0}
  def input(editor, "End"), do: %{editor | cursor: String.length(editor.text)}
  def input(editor, "ArrowLeft"), do: %{editor | cursor: max(editor.cursor - 1, 0)}

  def input(editor, "ArrowRight"),
    do: %{editor | cursor: min(editor.cursor + 1, String.length(editor.text))}

  def input(editor, "\x15"), do: %{editor | text: "", cursor: 0}
  def input(editor, "ArrowUp"), do: recall(editor, min(editor.index + 1, length(editor.history)))
  def input(editor, "ArrowDown"), do: recall(editor, max(editor.index - 1, 0))

  def input(%{cursor: 0} = editor, key) when key in ["Backspace", "\x7f"], do: editor

  def input(editor, key) when key in ["Backspace", "\x7f"] do
    {left, right} = String.split_at(editor.text, editor.cursor)
    %{editor | text: String.slice(left, 0..-2//1) <> right, cursor: editor.cursor - 1}
  end

  def input(editor, "Delete") do
    {left, right} = String.split_at(editor.text, editor.cursor)
    %{editor | text: left <> String.slice(right, 1..-1//1)}
  end

  def input(editor, key) do
    if String.valid?(key) and String.printable?(key) and String.length(key) == 1 do
      {left, right} = String.split_at(editor.text, editor.cursor)
      %{editor | text: left <> key <> right, cursor: String.length(left <> key)}
    else
      editor
    end
  end

  def line(editor, width) do
    {left, right} = String.split_at(editor.text, editor.cursor)
    prefix = "Filter command: "
    available = max(width - byte_size(prefix) - 2, 1)
    left = Bytes.tail(Bytes.display(left), available - 1)
    right_width = available - Utils.string_length(left) - 1
    prefix <> left <> "_" <> Bytes.head(Bytes.display(right), right_width)
  end

  defp recall(editor, index) do
    draft = if editor.index == 0, do: editor.text, else: editor.draft
    text = if index == 0, do: draft, else: Enum.at(editor.history, index - 1)
    %{editor | text: text, cursor: String.length(text), index: index, draft: draft}
  end
end
