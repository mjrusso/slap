defmodule Slap.Inspect.Rows do
  @moduledoc false

  # A row's identity: its key, and whether that key was cut short.
  def id(row), do: {row.key, Map.get(row, :oversized_key, false)}

  def find(rows, id), do: Enum.find(rows, &(id(&1) == id))
  def index(rows, id), do: Enum.find_index(rows, &(id(&1) == id))
  def selected(%{rows: rows, selected: selected}), do: find(rows, selected)

  def first_offset(rows) do
    Enum.find_value(rows, fn
      %{decoded: {:ok, %{kind: :message, offset: offset}}} -> offset
      _row -> nil
    end)
  end
end
