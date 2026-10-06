defmodule Slap.Yjs.Frame do
  @moduledoc """
  The framing of Yjs updates in a `.updates` stream, as lib0's
  `writeVarUint8Array` writes it and the reference `y-durable-streams`
  provider reads it: each update is its byte size as a varuint, then the
  update. Frames are concatenated, so a batch of frames is itself valid
  framing.

  A varuint is little-endian base 128: 7 bits per byte, low groups first,
  the high bit set on every byte but the last.
  """

  import Bitwise

  @doc "Frames one update."
  @spec frame(binary()) :: binary()
  def frame(update) when is_binary(update), do: varuint(byte_size(update)) <> update

  @doc "Frames several updates into one binary."
  @spec frames([binary()]) :: binary()
  def frames(updates), do: updates |> Enum.map(&frame/1) |> IO.iodata_to_binary()

  @doc """
  Splits concatenated frames into their updates. `{:error, :truncated}` if
  the binary ends inside a frame, `{:error, :invalid}` if a length does not
  fit in 53 bits (lib0's limit).
  """
  @spec parse(binary()) :: {:ok, [binary()]} | {:error, :truncated | :invalid}
  def parse(binary) when is_binary(binary), do: parse(binary, [])

  defp parse(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp parse(binary, acc) do
    with {:ok, size, rest} <- read_varuint(binary) do
      case rest do
        <<update::binary-size(^size), rest::binary>> -> parse(rest, [update | acc])
        _ -> {:error, :truncated}
      end
    end
  end

  @doc "Encodes a non-negative integer as a varuint."
  @spec varuint(non_neg_integer()) :: binary()
  def varuint(n) when is_integer(n) and n >= 0 and n < 128, do: <<n>>

  def varuint(n) when is_integer(n) and n >= 128,
    do: <<1::1, band(n, 127)::7>> <> varuint(n >>> 7)

  @doc """
  Decodes a varuint at the start of `binary`: `{:ok, n, rest}`, or an error
  as for `parse/1`.
  """
  @spec read_varuint(binary()) ::
          {:ok, non_neg_integer(), binary()} | {:error, :truncated | :invalid}
  def read_varuint(binary), do: read_varuint(binary, 0, 0)

  # lib0 numbers are at most 2^53 - 1, which takes eight 7-bit groups; the
  # eighth may still exceed it, so the value is checked too.
  @max 2 ** 53 - 1

  defp read_varuint(_binary, _n, shift) when shift > 49, do: {:error, :invalid}

  defp read_varuint(<<0::1, low::7, rest::binary>>, n, shift) do
    case n ||| low <<< shift do
      n when n <= @max -> {:ok, n, rest}
      _ -> {:error, :invalid}
    end
  end

  defp read_varuint(<<1::1, low::7, rest::binary>>, n, shift),
    do: read_varuint(rest, n ||| low <<< shift, shift + 7)

  defp read_varuint(<<>>, _n, _shift), do: {:error, :truncated}
end
