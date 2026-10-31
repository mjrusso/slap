defmodule Slap.Inspect.JSON do
  @moduledoc false

  def encode(value), do: value |> plain() |> JSON.encode!()

  defp plain({:json, binary}), do: JSON.decode!(binary)
  defp plain(value) when is_binary(value), do: Base.encode16(value, case: :lower)
  defp plain(value) when is_tuple(value), do: value |> Tuple.to_list() |> plain()
  defp plain(value) when is_list(value), do: Enum.map(value, &plain/1)

  defp plain(value) when is_map(value),
    do: Map.new(value, fn {key, field} -> {key(key), plain(field)} end)

  defp plain(value) when is_boolean(value) or is_nil(value) or is_number(value), do: value
  defp plain(value) when is_atom(value), do: Atom.to_string(value)
  defp plain(_value), do: "unsupported"

  defp key(key) when is_atom(key), do: Atom.to_string(key)
  defp key(key) when is_binary(key), do: Base.encode16(key, case: :lower)
  defp key(key), do: inspect(key)
end
