# Prints markdown tables comparing the scenario and sweep results of each
# store, one column per label, from bench/results/{scenarios,sweeps}-*.json
# (or SLAP_BENCH_OUT), then the micro benchmark table.
#
#     mix run --no-start bench/summary.exs >> "$GITHUB_STEP_SUMMARY"

Code.require_file("support/bench.exs", __DIR__)

dir = Bench.out_dir()

table = fn prefix, title ->
  results =
    for file <- Path.wildcard(Path.join(dir, "#{prefix}-*-{smaller,bigger}.json")),
        better = if(String.ends_with?(file, "-smaller.json"), do: "lower", else: "higher"),
        entry <- file |> File.read!() |> JSON.decode!() do
      [label, name] = String.split(entry["name"], ": ", parts: 2)
      %{label: label, name: name, unit: entry["unit"], value: entry["value"], better: better}
    end

  labels = results |> Enum.map(& &1.label) |> Enum.uniq() |> Enum.sort()
  # Keep rows for the same setting ("flush 5 ms: ...") together.
  names =
    results
    |> Enum.map(&{&1.name, &1.unit, &1.better})
    |> Enum.uniq()
    |> Enum.group_by(fn {name, _, _} -> name |> String.split(": ") |> hd() end)
    |> Enum.sort_by(fn {group, _} ->
      number = Regex.run(~r/\d+/, group) |> then(&(&1 && String.to_integer(hd(&1))))
      {String.replace(group, ~r/\d+/, ""), number}
    end)
    |> Enum.flat_map(&elem(&1, 1))

  by_key = Map.new(results, &{{&1.label, &1.name}, &1.value})

  format = fn
    nil -> "–"
    v when is_float(v) -> :erlang.float_to_binary(v, decimals: 1)
    v -> to_string(v)
  end

  header = "| #{title} | Unit | " <> Enum.join(labels, " | ") <> " | Better |"
  rule = "|---|---|" <> String.duplicate("---:|", length(labels)) <> "---|"

  rows =
    for {name, unit, better} <- names do
      values = Enum.map(labels, &format.(by_key[{&1, name}]))
      "| #{name} | #{unit} | " <> Enum.join(values, " | ") <> " | #{better} |"
    end

  if results != [] do
    IO.puts(Enum.join(["### #{title} by store", "", header, rule | rows], "\n"))
    IO.puts("")
  end
end

table.("scenarios", "Scenarios")
table.("sweeps", "Sweeps")

micro = Path.join(dir, "micro.md")
if File.exists?(micro), do: IO.puts(File.read!(micro))
