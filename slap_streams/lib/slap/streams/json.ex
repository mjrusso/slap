defmodule Slap.Streams.Json do
  @moduledoc false

  @ws [?\s, ?\t, ?\n, ?\r]

  @doc """
  Splits `body` into messages. A top-level array gives its elements, any
  other value gives itself (trimmed). An empty array gives `[]` when
  `allow_empty` (a create), and `{:error, :empty_array}` otherwise (an
  append).
  """
  @spec split(binary(), boolean()) :: {:ok, [binary()]} | {:error, :invalid_json | :empty_array}
  def split(body, allow_empty) do
    case JSON.decode(body) do
      {:ok, value} ->
        trimmed = trim(body)

        cond do
          not is_list(value) -> {:ok, [trimmed]}
          value == [] and allow_empty -> {:ok, []}
          value == [] -> {:error, :empty_array}
          true -> {:ok, elements(trimmed)}
        end

      {:error, _} ->
        {:error, :invalid_json}
    end
  end

  @doc "Joins messages into a JSON array, as a read returns them."
  @spec join([binary()]) :: iodata()
  def join([]), do: "[]"
  def join(messages), do: [?[, Enum.intersperse(messages, ?,), ?]]

  # The body is valid JSON and an array here. Walk it once, splitting on
  # commas at depth 1 outside strings.
  defp elements(<<?[, rest::binary>>), do: scan(rest, 1, false, false, [], [])

  defp scan(<<c, rest::binary>>, depth, true = _in_string, escaped, cur, acc) do
    cond do
      escaped -> scan(rest, depth, true, false, [c | cur], acc)
      c == ?\\ -> scan(rest, depth, true, true, [c | cur], acc)
      c == ?" -> scan(rest, depth, false, false, [c | cur], acc)
      true -> scan(rest, depth, true, false, [c | cur], acc)
    end
  end

  defp scan(<<?", rest::binary>>, depth, false, _, cur, acc),
    do: scan(rest, depth, true, false, [?" | cur], acc)

  defp scan(<<c, rest::binary>>, depth, false, _, cur, acc) when c in [?[, ?{],
    do: scan(rest, depth + 1, false, false, [c | cur], acc)

  defp scan(<<?], _rest::binary>>, 1, false, _, cur, acc), do: Enum.reverse([element(cur) | acc])

  defp scan(<<c, rest::binary>>, depth, false, _, cur, acc) when c in [?], ?}],
    do: scan(rest, depth - 1, false, false, [c | cur], acc)

  defp scan(<<?,, rest::binary>>, 1, false, _, cur, acc),
    do: scan(rest, 1, false, false, [], [element(cur) | acc])

  defp scan(<<c, rest::binary>>, depth, false, _, cur, acc),
    do: scan(rest, depth, false, false, [c | cur], acc)

  defp element(reversed), do: reversed |> Enum.reverse() |> IO.iodata_to_binary() |> trim()

  defp trim(binary), do: binary |> trim_leading() |> trim_trailing()

  defp trim_leading(<<c, rest::binary>>) when c in @ws, do: trim_leading(rest)
  defp trim_leading(binary), do: binary

  defp trim_trailing(binary) do
    size = byte_size(binary)

    if size > 0 and :binary.last(binary) in @ws,
      do: trim_trailing(binary_part(binary, 0, size - 1)),
      else: binary
  end
end
