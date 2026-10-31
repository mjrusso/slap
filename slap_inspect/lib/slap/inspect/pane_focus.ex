defmodule Slap.Inspect.PaneFocus do
  @moduledoc false
  @behaviour Breeze.Implicit

  def init(_children, _attrs, _previous), do: {:ok, %{}, captures_focus_keys: true}

  def handle_event(_, %{"key" => key}, state) when key in ["\t", "Tab", "ShiftTab"],
    do: {{:change, :switch_pane}, state}

  def handle_event(_, _, state), do: {:noreply, state}
  def handle_modifiers(_, _, _), do: []
end
