defmodule Slap.Inspect.DetailsScroll do
  @moduledoc false
  @behaviour Breeze.Implicit

  alias Breeze.Implicit.Scroll
  alias Breeze.Viewport

  def init(children, attrs, previous) do
    previous = if previous[:content_key] == attrs[:"content-key"], do: previous, else: %{}
    {:ok, state, opts} = Scroll.init(children, attrs, previous)

    state =
      Map.merge(state, %{
        offset_x: Map.get(previous, :offset_x, 0),
        content_key: attrs[:"content-key"],
        width: attrs[:"content-width"],
        disabled: attrs[:"scroll-disabled"] == true
      })

    {:ok, state, Keyword.put(opts, :captures_focus_keys, true)}
  end

  def handle_event(_, %{"key" => key}, state) when key in ["\t", "Tab", "ShiftTab"],
    do: {{:change, :switch_pane}, state}

  def handle_event(_event, _payload, %{disabled: true} = state), do: {:noreply, state}

  def handle_event(_, %{"key" => key, "element" => viewport}, state)
      when key in ["ArrowLeft", "ArrowRight", "[", "]"] do
    step =
      case key do
        "ArrowLeft" -> -1
        "ArrowRight" -> 1
        "[" -> -20
        "]" -> 20
      end

    viewport = horizontal_viewport(viewport, state)
    current = Viewport.clamp_scroll_x(state.offset_x, viewport)
    offset = Viewport.clamp_scroll_x(current + step, viewport)
    {:noreply, %{state | offset_x: offset}}
  end

  def handle_event(event, payload, state), do: Scroll.handle_event(event, payload, state)

  def handle_modifiers(:root, flags, state) do
    viewport = horizontal_viewport(Keyword.get(flags, :layout_element), state)
    offset = Viewport.clamp_scroll_x(state.offset_x, viewport)
    [scroll_x: offset] ++ Scroll.handle_modifiers(:root, flags, state)
  end

  def handle_modifiers(:child, _flags, _state), do: []

  defp horizontal_viewport(element, state) do
    viewport = Viewport.from_dimensions(element)

    # Breeze reports the container width for both metrics, including its two border columns.
    %{
      viewport
      | content_width: state.width,
        viewport_width: max((viewport.viewport_width || 0) - 2, 0)
    }
  end
end
