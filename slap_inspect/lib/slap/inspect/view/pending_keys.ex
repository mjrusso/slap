defmodule Slap.Inspect.View.PendingKeys do
  @moduledoc false

  import Breeze.Component, only: [assign: 2]

  @selection_keys ["ArrowDown", "ArrowUp", "Home", "End", "Enter", "f", "g"]

  defguard selection_key?(key) when key in @selection_keys

  def queue(%{assigns: %{id: id}} = term, key) do
    keys =
      case term.assigns.pending_keys do
        {^id, keys} -> keys
        _other -> []
      end

    assign(term, pending_keys: {id, [key | keys]})
  end

  def replay(
        %{assigns: %{pending_keys: {id, _keys}, id: id, operation: :reading}} = term,
        _input
      ),
      do: term

  def replay(
        %{assigns: %{pending_keys: {id, keys}, id: id, mode: :browse, page: %{}}} = term,
        input
      ),
      do: keys |> Enum.reverse() |> Enum.reduce(assign(term, pending_keys: nil), input)

  def replay(term, _input), do: assign(term, pending_keys: nil)
end
