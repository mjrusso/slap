defmodule Slap.Streams.Offset do
  @moduledoc """
  Offsets on the wire, in the official server's format and accounting, so
  that offsets stay valid for streams migrated from it.

  A token is `"0000000000000000_" <> byte_offset` zero-padded to 16 decimal
  digits. Each stored message advances the offset by `4 + byte_size(message)`,
  the official file store's framing (a 4-byte length prefix), although the
  prefix is not stored here.
  """

  @type t :: non_neg_integer()

  @doc "The token for `byte_offset`."
  @spec encode(t()) :: String.t()
  def encode(byte_offset) when is_integer(byte_offset) and byte_offset >= 0 do
    "0000000000000000_" <> String.pad_leading(Integer.to_string(byte_offset), 16, "0")
  end

  @doc """
  Parses a client offset. `"-1"` (and an absent offset, `nil` or `""`) is
  `:start`, the earliest data the stream still has (after a trim, not 0);
  `"now"` is `:now`.
  """
  @spec parse(String.t() | nil) :: {:ok, t() | :start | :now} | {:error, :bad_offset}
  def parse(nil), do: {:ok, :start}
  def parse(""), do: {:ok, :start}
  def parse("-1"), do: {:ok, :start}
  def parse("now"), do: {:ok, :now}

  def parse(<<read_seq::binary-size(16), "_", byte_offset::binary-size(16)>>) do
    with true <- digits?(read_seq) and digits?(byte_offset),
         0 <- String.to_integer(read_seq) do
      {:ok, String.to_integer(byte_offset)}
    else
      _ -> {:error, :bad_offset}
    end
  end

  def parse(_), do: {:error, :bad_offset}

  @doc "The offset after a message of `size` bytes stored at `offset`."
  @spec advance(t(), non_neg_integer()) :: t()
  def advance(offset, size), do: offset + 4 + size

  defp digits?(<<>>), do: true
  defp digits?(<<c, rest::binary>>) when c in ?0..?9, do: digits?(rest)
  defp digits?(_), do: false
end
