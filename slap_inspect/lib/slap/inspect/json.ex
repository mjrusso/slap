defmodule Slap.Inspect.JSON do
  @moduledoc false

  # Printable binaries are JSON strings; other binaries are
  # `{"$bytes": "<hex>"}`. A map key that starts with `$` gains another `$`,
  # and a key that is not printable is `"$bytes:<hex>"`, so a key with one
  # leading `$` is always an encoder tag, as in Yjs content.

  @doc "Encodes `value` as JSON, keeping page cursors hexadecimal for `--cursor`."
  def encode(%{cursor: cursor} = page) when is_binary(cursor),
    do: page |> Map.delete(:cursor) |> plain() |> Map.put("cursor", hex(cursor)) |> JSON.encode!()

  def encode(value), do: value |> plain() |> JSON.encode!()

  defp plain({:json, binary}), do: JSON.decode!(binary)

  defp plain(value) when is_binary(value),
    do: if(String.printable?(value), do: value, else: %{"$bytes" => hex(value)})

  defp plain(value) when is_tuple(value), do: value |> Tuple.to_list() |> plain()
  defp plain(value) when is_list(value), do: Enum.map(value, &plain/1)

  defp plain(value) when is_map(value) and not is_struct(value),
    do: Map.new(value, fn {key, field} -> {key(key), plain(field)} end)

  defp plain(value) when is_boolean(value) or is_nil(value) or is_number(value), do: value
  defp plain(value) when is_atom(value), do: Atom.to_string(value)
  defp plain(value), do: inspect(value)

  defp key(key) when is_atom(key), do: key |> Atom.to_string() |> key()

  defp key(key) when is_binary(key) do
    case {String.printable?(key), key} do
      {false, _key} -> "$bytes:" <> hex(key)
      {true, "$" <> _key} -> "$" <> key
      {true, _key} -> key
    end
  end

  defp key(key), do: key |> inspect() |> key()

  defp hex(binary), do: Base.encode16(binary, case: :lower)
end
