defmodule Slap.Inspect.Feedback do
  @moduledoc false

  def error({:invalid_option, key, 0, nil}), do: option(key) <> " must be zero or greater"
  def error({:invalid_option, key, 1, nil}), do: option(key) <> " must be greater than zero"

  def error({:invalid_option, key, minimum, maximum}),
    do: option(key) <> " must be in #{minimum}..#{maximum}"

  def error({:invalid, message}), do: message

  def error({:required_options, options}),
    do: "Required: " <> Enum.map_join(options, ", ", &option/1)

  def error({:invalid_hex, key}), do: option(key) <> " needs complete hexadecimal byte pairs"

  def error({:invalid_query_options, options}),
    do: "Unsupported query options: " <> Enum.join(options, ", ")

  def error({:unsupported_api, version}),
    do:
      "The server supports inspection API version #{version}; upgrade slap-inspect or the server to match."

  def error(:busy), do: "The owner is running another inspection. Press r to retry."
  def error({:disconnected, _node}), do: error(:disconnected)
  def error(:disconnected), do: "Disconnected. Check the node; press r to retry."
  def error(%{reason: :unassigned}), do: "Unassigned shard. Check placement; press r to retry."
  def error(%{reason: :not_owner}), do: "Owner changed. Press r to retry."

  def error({:history_unavailable, offset}),
    do: "Updates before offset #{offset} are not retained. Press c for current content."

  def error(reason) when reason in [:stream_not_observed, :deleted, :not_found],
    do: "Record unavailable (#{reason}). Press r to retry or Esc to go back."

  def error(reason) when is_binary(reason), do: reason
  def error(reason) when is_atom(reason), do: "Error: #{reason}. Press e for diagnostics."
  def error(_reason), do: "Read failed. Press e for diagnostics; r to retry."

  def option(key), do: "--" <> String.replace(Atom.to_string(key), "_", "-")
end
