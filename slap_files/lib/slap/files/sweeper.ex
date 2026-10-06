defmodule Slap.Files.Sweeper do
  @moduledoc """
  Deletes objects that are no longer a file's body, from the intents that
  name them (every `sweep_interval_ms`, default 10 s). Each node sweeps
  the intent partitions whose `Slap.KV` shard it owns.

  For each intent that is due, the sweeper takes it (a conditional rewrite,
  so a writer can no longer use it), deletes each object it names that the
  file's record does not point to, and deletes the intent. An intent is due
  when an upload has had `upload_timeout_ms`, or an old body
  `retention_ms`, so that readers that already have its key can finish.

  An upload that is due may still write its object after the sweeper has
  deleted it: nothing bounds when a store request completes. After
  deleting an object, the sweeper deletes its registration, so such an
  object has none. Every `reconcile_interval_ms` (default 1 hour), for
  each bucket whose `Slap.KV` shard it owns, it lists the bucket's objects
  and deletes those without a registration.

  A writer may give up on a write of a file's record that is applied
  later. So a record write covered by an intent has a `Slap.KV` deadline,
  the intent's due time, and the sweeper acts on the intent only
  `max_clock_skew_ms` after that, when no such write can be applied any
  more, by any node's clock. It reads the record with a linearizable read,
  which sees every write that was applied.
  """

  use GenServer
  require Logger

  alias Slap.Files.{Config, Intent, Object, Record}
  alias Slap.SlateDB.ObjectStore

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    config = Keyword.fetch!(opts, :config)
    GenServer.start_link(__MODULE__, config, name: Config.sweeper(config.name))
  end

  @doc "Sweeps now; returns failures by bucket."
  @spec sweep() :: :ok | {:error, [{non_neg_integer(), term()}]}
  def sweep(name \\ Slap.Files), do: GenServer.call(Config.sweeper(name), :sweep, :infinity)

  @doc "Reconciles now; returns failures by bucket."
  @spec reconcile() :: :ok | {:error, [{non_neg_integer(), term()}]}
  def reconcile(name \\ Slap.Files),
    do: GenServer.call(Config.sweeper(name), :reconcile, :infinity)

  @impl true
  def init(config) do
    schedule(:sweep, config)
    schedule(:reconcile, config)
    {:ok, config}
  end

  @impl true
  def handle_call(task, _from, config) when task in [:sweep, :reconcile] do
    {:reply, run(task, config), config}
  end

  @impl true
  def handle_info(task, config) when task in [:sweep, :reconcile] do
    case run(task, config) do
      :ok -> :ok
      {:error, errors} -> Logger.warning("slap_files: #{task}: #{inspect(errors)}")
    end

    schedule(task, config)
    {:noreply, config}
  end

  defp schedule(:sweep, config),
    do: Process.send_after(self(), :sweep, config.sweep_interval_ms)

  defp schedule(:reconcile, config),
    do: Process.send_after(self(), :reconcile, config.reconcile_interval_ms)

  defp run(:sweep, config) do
    now = Config.now(config)

    Intent.buckets()
    |> Enum.filter(&local?(Intent.partition(&1, config), config))
    |> run_buckets(&sweep_bucket(&1, now, config))
  end

  defp run(:reconcile, config) do
    Intent.buckets()
    |> Enum.filter(&local?(Object.partition(&1, config), config))
    |> run_buckets(&Object.reconcile(&1, config))
  end

  defp run_buckets(buckets, fun) do
    errors =
      Enum.reduce(buckets, [], fn bucket, errors ->
        case fun.(bucket) do
          :ok -> errors
          {:error, reason} -> [{bucket, reason} | errors]
        end
      end)

    if errors == [], do: :ok, else: {:error, Enum.reverse(errors)}
  end

  defp sweep_bucket(bucket, now, config) do
    resolve_due = fn intent, result ->
      if result == :ok and due?(intent, now, config),
        do: resolve(intent, now, config),
        else: result
    end

    case Intent.reduce(bucket, :ok, resolve_due, config) do
      {:ok, result} -> result
      {:error, _} = error -> error
    end
  end

  defp local?(partition, config),
    do:
      match?(
        {:ok, {:local, _}},
        Slap.Cluster.lookup(config.cluster, Slap.Cluster.shard_for(config.cluster, partition))
      )

  defp due?(intent, now, config), do: intent.due_ms + config.max_clock_skew_ms <= now

  defp resolve(listed, now, config) do
    with {:ok, %Intent{} = intent} <- Intent.get(listed.bucket, listed.id, config),
         true <- due?(intent, now, config),
         {:ok, intent} <- take(intent),
         {:ok, current} <-
           Record.get(intent.ref, [consistency: :linearizable, cluster: config.cluster], config),
         :ok <- delete_unreferenced(intent.keys, current, config) do
      Intent.done(intent)
    else
      # Gone, not due any more, or taken by a writer meanwhile.
      {:ok, nil} ->
        :ok

      false ->
        :ok

      {:error, :taken} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  # An intent this sweeper took before (it stopped half-way) is its own.
  defp take(%Intent{taken: :sweeper} = intent), do: {:ok, intent}
  defp take(intent), do: Intent.take(intent, taken: :sweeper)

  # An object is unregistered only once it is deleted.
  defp delete_unreferenced(keys, current, config) do
    referenced = with {_version, record} <- current, do: Record.object_key(record)
    objects = config.objects

    keys
    |> Enum.reject(&(&1 == referenced))
    |> Enum.reduce_while(:ok, fn key, :ok ->
      with :ok <- ObjectStore.delete(objects, key, Config.object_opts(config)),
           :ok <- Object.unregister(key, config) do
        {:cont, :ok}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
