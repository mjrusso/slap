defmodule Slap.Files.Codec do
  @moduledoc false

  # Stored as `{tag, version, fields}`. Each tag has its own version, so a
  # change to one format leaves the other's stored values readable.
  @versions %{file: 1, intent: 1}

  def encode(tag, fields), do: :erlang.term_to_binary({tag, Map.fetch!(@versions, tag), fields})

  def decode(tag, binary) do
    {:ok, {^tag, fields}} = decode_envelope(:erlang.binary_to_term(binary, [:safe]))
    fields
  end

  def decode_envelope({tag, version, fields}) do
    case Map.fetch(@versions, tag) do
      {:ok, ^version} -> {:ok, {tag, fields}}
      {:ok, _supported} -> {:error, {:unsupported_version, version}}
      :error -> {:error, :malformed_files_record}
    end
  end

  def decode_envelope(_term), do: {:error, :malformed_files_record}
end
