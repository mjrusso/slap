defmodule Slap.Files.Introspect do
  @moduledoc false

  alias Slap.Cluster.Inspection
  alias Slap.Cluster.Inspection.{Read, Value}
  alias Slap.Files.{Codec, Config}
  alias Slap.KV.Introspect, as: KV
  alias Slap.SlateDB.ObjectStore

  def scan(cluster, shard, version, budget, opts) do
    Inspection.request(
      cluster,
      shard,
      version,
      budget,
      fn ctx, deadline -> KV.page(ctx, deadline, opts, &decode/2) end
    )
  end

  def partitions(cluster, shard, version, budget, opts) do
    Inspection.request(cluster, shard, version, budget, fn ctx, deadline ->
      KV.partitions(ctx, deadline, opts, &partition/1)
    end)
  end

  defp partition(:confirm), do: {:error, :not_files_partition}

  defp partition(partition) do
    with {:ok, fields} <- Config.parse_partition(partition),
         do: {:ok, Map.merge(fields, %{kind: :files_partition, partition: partition})}
  end

  # Reads the object store, not the shard, so any node can answer.
  def object(cluster, _shard, version, budget, opts) do
    Inspection.exclusive(version, budget, fn deadline ->
      config = Config.get(Keyword.get(opts, :instance, Slap.Files))

      with :ok <- same_cluster(config, cluster),
           {:ok, limits} <- Read.limits(opts),
           do: preview(config, opts, deadline, limits.value)
    end)
  end

  defp preview(config, opts, deadline, max_bytes) do
    {offset, length} = Keyword.get(opts, :value_range, {0, 4096})
    key = Keyword.fetch!(opts, :object_key)
    prefix = Config.object_prefix(config)

    if valid_range?(offset, length, max_bytes) and valid_key?(key, prefix) do
      case ObjectStore.download(config.objects, key, range: {offset, offset + length - 1}) do
        {:ok, nil} ->
          {:ok, %{object: nil, owner: node()}}

        {:ok, {chunks, size, _version}} ->
          {bytes, stopped} = collect(chunks, deadline)

          {:ok,
           %{
             object: %{key: key, value: bytes, value_size: size, offset: offset},
             stopped: stopped,
             owner: node()
           }}

        {:error, error} ->
          Read.storage_error(error)
      end
    else
      {:error, :invalid_object_range}
    end
  end

  defp same_cluster(%{cluster: cluster}, cluster), do: :ok
  defp same_cluster(_config, _cluster), do: {:error, :invalid_object_request}

  defp valid_range?(offset, length, maximum),
    do:
      is_integer(offset) and offset >= 0 and is_integer(length) and length > 0 and
        length <= maximum

  defp valid_key?(key, prefix),
    do: byte_size(key) <= Read.max_key_bytes() and String.starts_with?(key, prefix)

  # Driven by hand so the deadline is checked before each chunk is fetched;
  # `Enum.reduce_while/3` would see it only after the next fetch.
  defp collect(chunks, deadline) do
    {:suspended, _, continuation} =
      Enumerable.reduce(chunks, {:suspend, nil}, fn chunk, _ -> {:suspend, chunk} end)

    collect_next(continuation, deadline, [])
  end

  defp collect_next(continuation, deadline, acc) do
    if Read.expired?(deadline) do
      continuation.({:halt, nil})
      {acc |> Enum.reverse() |> IO.iodata_to_binary(), :deadline}
    else
      case continuation.({:cont, nil}) do
        {:suspended, chunk, next} -> collect_next(next, deadline, [chunk | acc])
        {:done, _} -> {acc |> Enum.reverse() |> IO.iodata_to_binary(), :end}
      end
    end
  end

  def decode(key, binary) do
    with {:ok, %{kind: :row, partition: partition, key: id}} <- KV.decode(key, binary),
         {:ok, fields} <- Config.parse_partition(partition),
         {:ok, value} <- value(fields.family, binary) do
      {:ok, Map.merge(fields, %{id: id, value: Value.plain(value)})}
    end
  end

  defp value(:object, <<>>), do: {:ok, %{registered: true}}

  defp value(family, binary) do
    with {:ok, term} <- Value.term(binary),
         {:ok, {tag, fields}} <- Codec.decode_envelope(term) do
      case {family, tag, fields} do
        {:record, :file, %{body: {kind, body}, size: size, sha256: hash} = fields}
        when kind in [:inline, :object] and is_binary(body) and is_integer(size) and
               is_binary(hash) ->
          {:ok, summary(fields)}

        {:intent, :intent, %{ref: _, keys: _, due_ms: _, taken: _} = fields} ->
          {:ok, fields}

        _ ->
          {:error, :malformed_files_record}
      end
    end
  end

  defp summary(%{body: {:inline, bytes}} = record),
    do: %{record | body: {:inline, %{bytes: byte_size(bytes)}}} |> Map.put(:storage, :inline)

  defp summary(%{body: {:object, _key}} = record), do: Map.put(record, :storage, :object)
end
