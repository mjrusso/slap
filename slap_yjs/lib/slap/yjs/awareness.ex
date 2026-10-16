defmodule Slap.Yjs.Awareness do
  @moduledoc """
  Encodes and decodes awareness (presence) updates in the y-protocols format.
  An update starts with its entry count as a varuint. Each entry contains a
  client id and clock as varuints, then a length-prefixed JSON state;
  `"null"` removes the client's state.

  A host can use `decode/1` to check the awareness messages its clients
  send, and `encode/1` to build an update from the entries it accepts. The
  README's "Authorizing client awareness" section shows how. To publish and
  maintain awareness for a server-side participant, use `Slap.Yjs.Presence`.
  """

  alias Slap.Yjs.Frame

  @type entry :: {client_id :: non_neg_integer(), clock :: non_neg_integer(), state :: term()}

  @doc """
  Decodes an awareness update into entries. A removal has a `nil` state.
  Returns `{:error, :invalid}` for malformed updates.

  A complete awareness message is byte 1 followed by a framed update. This
  function accepts only the unframed update.
  """
  @spec decode(binary()) :: {:ok, [entry()]} | {:error, :invalid}
  def decode(update) when is_binary(update) do
    with {:ok, count, rest} <- Frame.read_varuint(update),
         {:ok, entries, <<>>} <- entries(rest, count, []) do
      {:ok, entries}
    else
      _ -> {:error, :invalid}
    end
  end

  @doc "Encodes entries into an awareness update."
  @spec encode([entry()]) :: binary()
  def encode(entries) do
    IO.iodata_to_binary([
      Frame.varuint(length(entries))
      | Enum.map(entries, fn {client_id, clock, state} ->
          [Frame.varuint(client_id), Frame.varuint(clock), Frame.frame(JSON.encode!(state))]
        end)
    ])
  end

  defp entries(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp entries(binary, n, acc) do
    with {:ok, client_id, rest} <- Frame.read_varuint(binary),
         {:ok, clock, rest} <- Frame.read_varuint(rest),
         {:ok, size, rest} <- Frame.read_varuint(rest),
         <<json::binary-size(^size), rest::binary>> <- rest,
         {:ok, state} <- JSON.decode(json) do
      entries(rest, n - 1, [{client_id, clock, state} | acc])
    end
  end
end
