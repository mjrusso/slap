defmodule Slap.Cluster.Inspection.Value do
  @moduledoc false

  @limit 512 * 1024

  def term(binary) when byte_size(binary) > @limit, do: {:error, :oversized_value}
  def term(<<131, 80, _::binary>>), do: {:error, :compressed_etf}

  def term(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> {:error, :malformed_etf}
  end

  def plain(value), do: plain(value, 0)
  defp plain(_value, depth) when depth > 24, do: :depth_limit

  defp plain(%module{} = value, depth),
    do: value |> Map.from_struct() |> Map.put(:type, module) |> plain(depth + 1)

  defp plain(value, depth) when is_map(value),
    do: Map.new(value, fn {key, field} -> {plain(key, depth + 1), plain(field, depth + 1)} end)

  defp plain(value, depth) when is_tuple(value), do: plain(Tuple.to_list(value), depth + 1)
  defp plain(value, depth) when is_list(value), do: Enum.map(value, &plain(&1, depth + 1))
  defp plain(value, _depth) when is_binary(value) or is_atom(value) or is_number(value), do: value
  defp plain(_value, _depth), do: :unsupported_term
end
