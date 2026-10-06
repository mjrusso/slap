defmodule Slap.Files.Object do
  @moduledoc false
  # An object's key includes its namespace, bucket and random id. Its
  # registration is a Slap.KV row in the namespace's object partition,
  # keyed by the id. A writer registers a key after naming it in an intent
  # and before uploading to it, and the
  # sweeper deletes the registration only after it has deleted the object
  # (Slap.Files.Sweeper). So an object without a registration was written
  # after it was deleted, by a store request that completed late; nothing
  # can point a file at it, and reconcile/1 deletes it. The registration
  # is a write the intent covers: it is not applied once the intent is
  # due, so it cannot be applied after the sweeper has unregistered the key.

  alias Slap.Files.{Config, Deadline, Intent, Scan}
  alias Slap.KV
  alias Slap.SlateDB.ObjectStore

  @spec new_key(Slap.Files.ref(), Config.t()) :: String.t()
  def new_key(ref, config),
    do: Config.object_prefix(config, Intent.bucket(ref)) <> Slap.Files.new_id()

  @spec partition(non_neg_integer(), Config.t()) :: binary()
  def partition(bucket, config), do: Config.partition(config, :object, bucket)

  # `due_ms` is when the intent that names the key is due.
  @spec register(String.t(), integer(), Config.t()) :: :ok | {:error, term()}
  def register(key, due_ms, config) do
    {bucket, id} = parse(key, config)

    with {:ok, _version} <-
           Deadline.result(
             KV.put(
               partition(bucket, config),
               id,
               "",
               Config.route_opts(config) ++ Deadline.opts(due_ms, config)
             )
           ),
         do: :ok
  end

  # Once the object is deleted.
  @spec unregister(String.t(), Config.t()) :: :ok | {:error, term()}
  def unregister(key, config) do
    {bucket, id} = parse(key, config)
    KV.delete(partition(bucket, config), id, Config.route_opts(config))
  end

  @spec reconcile(non_neg_integer(), Config.t()) :: :ok | {:error, term()}
  def reconcile(bucket, config) do
    prefix = Config.object_prefix(config, bucket)
    objects = config.objects

    # Objects are listed first: a listed object's registration was written
    # before the listing, and is gone only if the object was deleted.
    with {:ok, keys} <- ObjectStore.list(objects, prefix, Config.object_opts(config)),
         listed = MapSet.new(keys, &String.replace_prefix(&1, prefix, "")),
         {:ok, unregistered} <-
           Scan.reduce(
             partition(bucket, config),
             listed,
             fn {id, _}, ids -> MapSet.delete(ids, id) end,
             Config.route_opts(config)
           ) do
      delete_all(objects, Enum.map(unregistered, &(prefix <> &1)), Config.object_opts(config))
    end
  end

  defp delete_all(objects, keys, opts) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case ObjectStore.delete(objects, key, opts) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp parse(key, %Config{namespace: namespace}) do
    encoded = Base.url_encode64(namespace, padding: false)
    ["objects", "ns", ^encoded, bucket, id] = String.split(key, "/")
    {String.to_integer(bucket), id}
  end
end
