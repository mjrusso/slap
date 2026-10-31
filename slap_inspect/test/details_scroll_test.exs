defmodule Slap.Inspect.DetailsScrollTest do
  use ExUnit.Case, async: true

  alias Breeze.Viewport
  alias Slap.Inspect.DetailsScroll

  test "Left moves immediately after the content or viewport shrinks its scroll range" do
    attrs = %{:"content-width" => 80}
    {:ok, state, _} = DetailsScroll.init([], attrs, %{offset_x: 200})
    viewport = %Viewport{viewport_width: 60, content_width: 80}
    before = DetailsScroll.handle_modifiers(:root, [layout_element: viewport], state)

    {:noreply, state} =
      DetailsScroll.handle_event(:input, %{"key" => "ArrowLeft", "element" => viewport}, state)

    after_left = DetailsScroll.handle_modifiers(:root, [layout_element: viewport], state)
    assert before[:scroll_x] == 22
    assert after_left[:scroll_x] == 21
  end
end
