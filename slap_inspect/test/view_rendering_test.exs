defmodule Slap.Inspect.ViewRenderingTest do
  # Formatting call counts are node-wide.
  use ExUnit.Case, async: false

  import Slap.Inspect.ViewCase, only: [start_view: 1, answer: 2]

  alias Breeze.Test
  alias Slap.Inspect.Details

  setup do
    Code.ensure_loaded!(Details)
    :erlang.trace_pattern({Details, :render, 4}, true, [:call_count])
    on_exit(fn -> :erlang.trace_pattern({Details, :render, 4}, false, [:call_count]) end)
    :ok
  end

  test "hidden details stay unformatted and visible details update only with their inputs" do
    session = start_view(request: [opts: [base: "/doc"]])
    answer(session, {:ok, page("first")})
    Test.render!(session)
    assert renders() == 0

    Test.input(session, "Enter")
    assert Test.render!(session) =~ "first"
    assert renders() == 1

    Test.input(session, "/")

    for key <- String.graphemes(" --limit 1") do
      Test.input(session, key)
      Test.render!(session)
    end

    Test.input(session, "Escape")
    Test.render!(session)
    assert renders() == 1

    Test.input(session, "d")
    Test.render!(session)
    assert renders() == 2

    Test.input(session, "r")
    answer(session, {:ok, page("changed")})
    assert Test.render!(session) =~ "changed"
    assert renders() == 3

    resized = %{session | terminal: %{session.terminal | size: %{width: 90, height: 24}}}
    Test.info(resized, :resize)
    Test.render!(resized)
    assert renders() == 4

    Test.input(resized, "b")
    Test.input(resized, "ArrowDown")
    Test.render!(resized)
    assert renders() == 4
    Test.input(resized, "Enter")
    assert Test.render!(resized) =~ "second"
    assert renders() == 5
  end

  test "root chooser changes reuse details after it closes" do
    content = %{content: {:json, JSON.encode!(%{roots: [%{name: "body", value: "document"}]})}}
    session = start_view(request: [view: :yjs_content, opts: [base: "/doc"]])
    answer(session, {:ok, %{owner: :slap@localhost, encoded_bytes: 12}})
    Test.input(session, "y")
    answer(session, {:ok, content})
    Test.input(session, "ArrowDown")
    Test.input(session, "Enter")
    assert Test.render!(session) =~ "document"
    assert renders() == 1

    Test.input(session, "i")

    for key <- ["ArrowRight", "ArrowDown", "ArrowLeft"] do
      Test.input(session, key)
      assert Test.render!(session) =~ "Interpret roots"
    end

    Test.input(session, "Escape")
    assert Test.render!(session) =~ "document"
    assert renders() == 1
  end

  defp renders do
    {:call_count, count} = :erlang.trace_info({Details, :render, 4}, :call_count)
    count
  end

  defp page(value) do
    %{
      owner: :slap@localhost,
      shard: 0,
      rows:
        for {key, value} <- [{"a", value}, {"b", "second"}] do
          %{
            key: key,
            value: "",
            decoded: {:ok, %{value: %{content: {:json, JSON.encode!(%{text: value})}}}}
          }
        end
    }
  end
end
