defmodule Slap.Cluster.Layout do
  @moduledoc false
  # `<path>/_cluster/layout` records the shard count and hash that the
  # store's keys were routed with. A cluster with another count reads keys on
  # shards it no longer routes them to as missing, so startup refuses a
  # mismatch.
  #
  # A store from before the record has none. If the shard after the
  # configured last one has no database, the store matches when the last
  # shard has one, and is new when shard 0 has none under any name width up
  # to six digits (a million shards) or the configured width.

  alias Slap.Cluster.{Config, Hash}
  alias Slap.SlateDB.ObjectStore

  @key "_cluster/layout"
  @version 1
  @timeout 30_000

  def check(%Config{} = config) do
    with {:ok, store} <- ObjectStore.open(config.path, store: config.store, timeout: @timeout),
         do: compare(store, config, read(store))
  end

  defp compare(store, config, {:ok, nil}), do: record(store, config)
  defp compare(_store, _config, {:error, _} = error), do: error

  defp compare(_store, config, {:ok, stored}) do
    case layout(config) do
      ^stored -> :ok
      configured -> {:error, {:layout_mismatch, %{configured: configured, stored: stored}}}
    end
  end

  defp layout(config), do: %{shards: config.shards, hash: Hash.name()}

  defp read(store) do
    case ObjectStore.get(store, @key, timeout: @timeout) do
      {:ok, nil} -> {:ok, nil}
      {:ok, {body, _version}} -> decode(body)
      {:error, _} = error -> error
    end
  end

  defp decode(body) do
    case JSON.decode(body) do
      {:ok, %{"version" => @version, "shards" => shards, "hash" => hash}}
      when is_integer(shards) and shards > 0 and is_binary(hash) ->
        {:ok, %{shards: shards, hash: hash}}

      _ ->
        {:error, {:invalid_layout, body}}
    end
  end

  defp record(store, config) do
    with :ok <- unrecorded(store, config) do
      body = JSON.encode!(Map.put(layout(config), :version, @version))

      case ObjectStore.put(store, @key, body, mode: :create, timeout: @timeout) do
        {:ok, _version} -> :ok
        # Another node recorded the layout first; compare with what it wrote.
        {:error, :conflict} -> compare(store, config, read(store))
        {:error, _} = error -> error
      end
    end
  end

  defp unrecorded(store, config) do
    width = Config.shard_width(config.shards)
    last = Config.shard_name(config.shards - 1, width)
    next = Config.shard_name(config.shards, width)
    firsts = for w <- 3..max(6, width), do: Config.shard_name(0, w)

    with {:ok, last?} <- exists?(store, last),
         {:ok, next?} <- exists?(store, next),
         {:ok, empty?} <- none?(store, firsts) do
      if not next? and (last? or empty?),
        do: :ok,
        else: {:error, {:layout_mismatch, %{configured: layout(config), stored: :unrecorded}}}
    end
  end

  defp none?(store, names) do
    Enum.reduce_while(names, {:ok, true}, fn name, acc ->
      case exists?(store, name) do
        {:ok, false} -> {:cont, acc}
        {:ok, true} -> {:halt, {:ok, false}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp exists?(store, name) do
    with {:ok, keys} <- ObjectStore.list(store, Path.join(name, "manifest"), timeout: @timeout),
         do: {:ok, keys != []}
  end
end
