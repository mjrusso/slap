defmodule Slap.Inspect.Feedback do
  @moduledoc false

  alias Slap.Inspect.{Operation, Options}

  def error({:invalid_option, key, 0, nil}), do: Options.option(key) <> " must be zero or greater"

  def error({:invalid_option, key, 1, nil}),
    do: Options.option(key) <> " must be greater than zero"

  def error({:invalid_option, key, minimum, maximum}),
    do: Options.option(key) <> " must be in #{minimum}..#{maximum}"

  def error({:invalid, message}), do: message

  def error(:before_unsupported),
    do: "This server cannot read the page before an offset; T and p need a newer Slap"

  def error({:required_options, sets}),
    do:
      "Required: " <>
        Enum.map_join(
          sets,
          " or ",
          &Enum.map_join(&1, " and ", fn key -> Options.option(key) end)
        )

  def error({:invalid_hex, key}),
    do: Options.option(key) <> " needs complete hexadecimal byte pairs"

  def error({:invalid_query_options, options}),
    do:
      "Unsupported query options: " <>
        Enum.map_join(options, ", ", &suggest(&1, Options.query_options()))

  def error({:unsupported_options, command, selectors, options}),
    do:
      Enum.join([command | Enum.map(selectors, &Options.option/1)], " ") <>
        " does not take " <> Enum.map_join(options, ", ", &unused(command, &1))

  def error({:unsupported_api, version}),
    do:
      "The server supports inspection API version #{version}; upgrade slap-inspect or the server to match."

  def error({:log_not_found, shard}),
    do: "No log at this --base on shard #{shard}. Check the path, or omit --shard to route by it."

  def error({:not_served, cluster, view, []}),
    do:
      "#{inspect(cluster)} does not run #{service(view)}, and no cluster here runs #{service(view)}."

  def error({:not_served, cluster, view, others}),
    do:
      "#{inspect(cluster)} does not run #{service(view)}; #{service(view)} runs in " <>
        Enum.map_join(others, ", ", &inspect/1) <> ". ? lists each cluster's view keys."

  def error({:stopped, cluster}),
    do: "#{inspect(cluster)} is no longer running. Open a cluster from the navigation pane."

  def error(:busy), do: "The owner is running another inspection. Press r to retry."
  def error({:disconnected, _node}), do: error(:disconnected)
  def error(:disconnected), do: "Disconnected. Reconnecting every 2 seconds; q quits."
  def error(%{reason: :unassigned}), do: "Unassigned shard. Check placement; press r to retry."
  def error(%{reason: :not_owner}), do: "Owner changed. Press r to retry."

  def error({:history_unavailable, offset}),
    do: "Updates before offset #{offset} are not retained. Press c for current content."

  def error(reason) when reason in [:stream_not_observed, :deleted, :not_found],
    do: "Record unavailable (#{reason}). Press r to retry or Esc to go back."

  def error(reason) when is_binary(reason), do: reason
  def error(reason) when is_atom(reason), do: "Error: #{reason}. Press e for diagnostics."
  def error(_reason), do: "Read failed. Press e for diagnostics; r to retry."

  defp service(view) do
    case Operation.service(view) do
      :kv -> "KV"
      :streams -> "Streams"
    end
  end

  defp unused(command, {key, nil}),
    do: suggest(Options.option(key), Options.query_options(command))

  defp unused(_command, {key, []}), do: Options.option(key)

  defp unused(_command, {key, missing}),
    do: Options.option(key) <> " without " <> Enum.map_join(missing, " and ", &Options.option/1)

  defp suggest(option, options) do
    known = Enum.max_by(options, &String.jaro_distance(&1, option))

    if String.jaro_distance(known, option) >= 0.9,
      do: "#{option} (did you mean #{known}?)",
      else: option
  end
end
