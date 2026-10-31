defmodule Slap.Inspect.ViewEditorTest do
  use ExUnit.Case, async: true

  import Slap.Inspect.ViewCase

  alias BackBreeze.Utils
  alias Breeze.Test

  test "editing connection options preserves the query and does not start another read" do
    session = start_view()
    answer(session, {:ok, page()})

    for command <- [
          "scan --cluster Other.Cluster",
          "scan --node other@host",
          "scan --refresh 1000"
        ] do
      edit(session, command)
      Test.input(session, "Enter")
      screen = Test.render!(session)
      assert screen =~ "Unsupported query options"
      assert screen =~ "Filter command: " <> command
      assert screen =~ "durable seq 4"
      refute_receive {:read, _, _}
      Test.input(session, "Escape")
      refute Test.render!(session) =~ String.replace_prefix(command, "scan ", "")
    end
  end

  test "unclosed quotes leave the command editable and preserve the current page" do
    for quote <- ["\"", "'"] do
      session = start_view(size: {140, 36})
      answer(session, {:ok, page()})
      Test.input(session, "ArrowDown")
      command = "kv --partition " <> quote <> "demo"
      edit(session, command)
      Test.input(session, "Enter")
      screen = Test.render!(session)
      assert screen =~ "Filter error:"
      assert screen =~ command <> "_"
      assert screen =~ "durable seq 4"
      refute_receive {:read, _, _}
      Test.input(session, quote)
      Test.input(session, "Enter")
      assert_receive {:read, %{view: :kv, opts: opts}, _worker}
      assert opts[:partition] == "demo"
    end
  end

  test "long commands keep their insertion point visible in narrow terminals" do
    for size <- [{60, 18}, {80, 24}] do
      session = start_view(size: size)
      answer(session, {:ok, page()})

      commands = [
        "yjs --content --base /v1/stream/yjs/demo/docs/notes --root text:text",
        "yjs --content --base /" <> String.duplicate("界", 32) <> " --root text:text"
      ]

      for command <- commands do
        edit(session, command)
        screen = Test.render!(session)
        assert screen =~ "text:text_"
        refute screen =~ "Filter command: yjs"
        prompt = screen |> lines() |> Enum.find(&String.contains?(&1, "Filter command:"))
        assert Utils.string_length(prompt) <= elem(size, 0)
        Test.input(session, "Backspace")
        assert Test.render!(session) =~ "text:tex_"
        Test.input(session, "\x7f")
        assert Test.render!(session) =~ "text:te_"
        Test.input(session, "Escape")
      end

      edit(session, "k")
      assert Test.render!(session) =~ "Filter command: k_"
    end
  end

  test "filter editing receives scroll keys while details are focused" do
    data = %{page() | rows: [%{row() | value: String.duplicate("x", 512), value_size: 512}]}

    for size <- [{80, 24}, {110, 24}] do
      session = start_view(size: size)
      answer(session, {:ok, data})
      Test.input(session, "ArrowDown")
      Test.input(session, "Enter")
      Test.render!(session)
      edit(session, "yjs --base /jk")
      editing = Test.render!(session)
      assert editing =~ "Filter command: yjs --base /jk_"
      for key <- ["ArrowDown", "ArrowRight", "PageDown"], do: Test.input(session, key)
      assert Test.render!(session) == editing
      Test.input(session, "Escape")
      before = Test.render!(session)
      Test.input(session, "j")
      assert Test.render!(session) != before
      Test.input(session, "k")
      assert Test.render!(session) == before
    end
  end
end
