defmodule Slap.Inspect.CommandTest do
  use ExUnit.Case, async: true

  alias Slap.Inspect.{Command, Editor}

  test "quoted arguments preserve whitespace, empty values, backslashes and opaque bytes" do
    values = [
      "",
      "plain",
      <<255>>,
      :binary.list_to_bin(Enum.to_list(0..255)),
      "controls:" <> List.to_string(Enum.to_list(0..31)),
      "a b",
      "\"'\\",
      "ending\\",
      "a\nb\tc",
      "\e[31m",
      "界é",
      "literal\\u{301}",
      ~S(literal\xFF)
    ]

    assert {:ok, ^values} = values |> Enum.map_join(" ", &Command.quote/1) |> Command.split()
    assert {:ok, ["a b", "c d"]} = Command.split(~S('a b' c\ d))
    assert {:ok, [~S(\xFF)]} = Command.split(~S('\xFF'))
    assert {:error, _} = Command.split("'unclosed")
    assert {:error, _} = Command.split("trailing\\")

    for malformed <- [~S("\u-001"), ~S("\uD800"), ~S("\uzzxx"), ~S(\x), ~S(\x0), ~S(\xgg)] do
      assert {:error, _} = Command.split(malformed)
    end
  end

  test "editing combining marks keeps the cursor on grapheme boundaries" do
    editor = Editor.new("", [])
    editor = Enum.reduce(["e", "́", "界"], editor, &Editor.input(&2, &1))
    assert Editor.line(editor, 80) =~ ~S(e\u{301}界_)
    editor = Editor.input(editor, "ArrowLeft")
    editor = Editor.input(editor, "Delete")
    assert Editor.line(editor, 80) =~ ~S(e\u{301}_)
    editor = Editor.input(editor, "Backspace")
    assert Editor.line(editor, 80) == "Filter command: _"
  end
end
