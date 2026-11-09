defmodule Slap.Inspect.Prefix do
  @moduledoc false

  def common([]), do: []

  def common([first | rest]) do
    Enum.reduce(rest, first, fn list, prefix ->
      prefix
      |> Enum.zip(list)
      |> Enum.take_while(fn {a, b} -> a == b end)
      |> Enum.map(&elem(&1, 0))
    end)
  end
end
