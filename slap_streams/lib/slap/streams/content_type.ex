defmodule Slap.Streams.ContentType do
  @moduledoc false
  # Content types compare by media type, case-insensitively, ignoring
  # parameters, as in the official server. A missing type is
  # application/octet-stream.

  @default "application/octet-stream"

  def normalize(nil), do: @default
  def normalize(""), do: @default
  def normalize(type) when is_binary(type), do: type

  def media_type(type) do
    type
    |> normalize()
    |> String.split(";", parts: 2)
    |> hd()
    |> String.trim()
    |> String.downcase()
  end

  def matches?(a, b), do: media_type(a) == media_type(b)

  def json?(type), do: media_type(type) == "application/json"
end
