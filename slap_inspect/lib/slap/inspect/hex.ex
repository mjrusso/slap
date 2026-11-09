defmodule Slap.Inspect.Hex do
  @moduledoc false

  alias Slap.Inspect.Bytes

  # Breeze sizes the entire details pane to its widest line. Truncate hex
  # values to avoid expensive layouts for long binaries.
  defstruct [:bytes]

  def wrap(binary) when is_binary(binary),
    do: if(String.valid?(binary), do: binary, else: %__MODULE__{bytes: binary})

  def wrap(list) when is_list(list), do: Enum.map(list, &wrap/1)
  def wrap(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> wrap() |> List.to_tuple()

  def wrap(map) when is_map(map) and not is_struct(map),
    do: Map.new(map, fn {key, value} -> {wrap(key), wrap(value)} end)

  def wrap(value), do: value

  defimpl Inspect do
    def inspect(%{bytes: bytes}, _opts), do: Bytes.hex_text(bytes)
  end
end
