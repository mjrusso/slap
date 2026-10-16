ExUnit.start()
Slap.SlateDB.set_log_level(:none)

defmodule Slap.ReadmeTest do
  use ExUnit.Case, async: true

  @readme System.argv() |> hd() |> Path.expand()
  @examples Regex.scan(~r/^```elixir\n(.*?)^```/ms, File.read!(@readme), capture: :all_but_first)
            |> Enum.map(&hd/1)
            |> Enum.filter(&Regex.match?(~r/^\s*iex>/m, &1))

  if @examples == [], do: raise(ArgumentError, "no IEx examples in #{@readme}")

  @directory Path.join(Mix.Project.build_path(), "readme_examples")
  @filename @readme
            |> Path.relative_to(Path.expand("../..", __DIR__))
            |> String.replace("/", "_")
  @transcript Path.join(@directory, @filename)

  # Blank lines separate doctests, but the walkthrough is one IEx session.
  @session @examples |> Enum.join("\n") |> String.split("\n", trim: true) |> Enum.join("\n")

  File.mkdir_p!(@directory)
  File.write!(@transcript, @session <> "\n")

  doctest_file(@transcript)
end
