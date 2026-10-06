defmodule Slap.Files.Scan do
  @moduledoc false
  alias Slap.KV

  @page 1_000

  @spec reduce(binary(), acc, ({binary(), binary()}, acc -> acc)) ::
          {:ok, acc} | {:error, term()}
        when acc: term()
  def reduce(partition, acc, fun, opts \\ []), do: reduce(partition, nil, acc, fun, opts)

  defp reduce(partition, cursor, acc, fun, route_opts) do
    opts = if cursor, do: [limit: @page, cursor: cursor], else: [limit: @page]

    case KV.scan(partition, opts ++ route_opts) do
      {:ok, %{rows: rows, cursor: next}} ->
        acc = Enum.reduce(rows, acc, fun)
        if next, do: reduce(partition, next, acc, fun, route_opts), else: {:ok, acc}

      {:error, _} = error ->
        error
    end
  end
end
