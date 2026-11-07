# Runs a Livebook notebook's Elixir cells in order against this checkout:
#
#     mix run scripts/livebook_test.exs notebooks/getting-started.livemd
#
# The checkout provides dependencies, so the Mix.install setup cell is skipped.
# Kino's fallback labels, "tuple" and "term", indicate unmatched messages.
Slap.SlateDB.set_log_level(:none)

path = System.argv() |> hd() |> Path.expand()
source = File.read!(path)

[{setup, _line} | cells] =
  for [{start, length}] <-
        Regex.scan(~r/^```elixir\n(.*?)^```/ms, source, capture: :all_but_first, return: :index) do
    preceding = binary_part(source, 0, start)
    {binary_part(source, start, length), length(String.split(preceding, "\n"))}
  end

if !String.contains?(setup, "Mix.install"),
  do: raise(ArgumentError, "#{path}: the first cell is not a Mix.install setup cell")

check_labels! = fn
  %Kino.JS{module: Kino.Mermaid} = kino ->
    {"mermaid", diagram} = Kino.Test.export(kino, 5_000)

    case Regex.scan(~r/^\d+->>\d+: (\w+: (?:tuple|term))$/m, diagram, capture: :all_but_first) do
      [] ->
        :ok

      labels ->
        raise "unlabeled messages in the sequence diagram: #{inspect(List.flatten(labels))}"
    end

  _result ->
    :ok
end

Enum.reduce(cells, {[], Code.env_for_eval(file: path)}, fn {code, line}, {binding, env} ->
  try do
    quoted = Code.string_to_quoted!(code, file: path, line: line)
    {result, binding, env} = Code.eval_quoted_with_env(quoted, binding, env)
    check_labels!.(result)
    {binding, env}
  catch
    kind, reason ->
      IO.puts(:stderr, "#{Path.relative_to_cwd(path)}: the cell at line #{line} failed")
      :erlang.raise(kind, reason, __STACKTRACE__)
  end
end)

IO.puts("#{Path.relative_to_cwd(path)}: #{length(cells)} cells ran")
