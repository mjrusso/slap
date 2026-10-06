defmodule Slap.KV.Read do
  @moduledoc false

  alias Slap.KV.Keys
  alias Slap.SlateDB

  @remote [durability: :remote]

  def get(ctx, key) do
    case SlateDB.get_key_value(ctx.db, key, @remote) do
      {:ok, nil} -> {:ok, nil}
      {:ok, %{value: value, seq: version}} -> {:ok, %{value: value, version: version}}
      {:error, _} -> {:error, :unavailable}
    end
  end

  def scan(ctx, partition, range, limit, with_versions) do
    rows =
      ctx.db
      |> SlateDB.scan(
        @remote ++ [batch_size: min(limit + 1, 256), with_versions: with_versions] ++ range
      )
      |> Enum.take(limit + 1)
      |> Enum.map(&decode_row(partition, &1))

    case Enum.split(rows, limit) do
      {page, []} -> {:ok, %{rows: page, cursor: nil}}
      {page, _more} -> {:ok, %{rows: page, cursor: page |> List.last() |> elem(0)}}
    end
  rescue
    # The database failed or closed under the scan (the shard is stopping).
    SlateDB.Error -> {:error, :unavailable}
  end

  defp decode_row(partition, {key, value}), do: {Keys.decode(partition, key), value}

  defp decode_row(partition, {key, value, version}),
    do: {Keys.decode(partition, key), value, version}
end
