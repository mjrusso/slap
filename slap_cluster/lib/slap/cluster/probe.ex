defmodule Slap.Cluster.Probe do
  @moduledoc false
  # The strategy can open writers, so start it only after the store passes
  # the fencing probe (unless probing is disabled) and its layout matches
  # `:shards`.

  use GenServer
  require Logger

  alias Slap.Cluster.{Config, Layout, Telemetry}
  alias Slap.SlateDB

  @retry_base 1_000
  @retry_max 30_000

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl true
  def init(%Config{probe: :background} = config),
    do: {:ok, {config, @retry_base}, {:continue, :probe}}

  def init(config) do
    case check(config) do
      :ok ->
        start_strategy(config)

      {:error, reason} ->
        refuse(config, reason)
        {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:probe, {config, delay}) do
    case check(config) do
      :ok ->
        case start_strategy(config) do
          :ignore -> {:noreply, {config, delay}}
          {:stop, reason} -> {:stop, reason, {config, delay}}
        end

      # A retry would find the same layout.
      {:error, {kind, _} = reason} when kind in [:layout_mismatch, :invalid_layout] ->
        refuse(config, reason)
        {:stop, reason, {config, delay}}

      {:error, reason} ->
        Logger.warning(
          "#{inspect(config.cluster)}: #{describe(config, reason)} No shards are open. " <>
            "Retrying in #{delay} ms."
        )

        Process.send_after(self(), :retry, delay)
        {:noreply, {config, min(delay * 2, @retry_max)}}
    end
  end

  @impl true
  def handle_info(:retry, state), do: {:noreply, state, {:continue, :probe}}

  defp refuse(config, reason),
    do: Logger.error("#{inspect(config.cluster)}: #{describe(config, reason)} Not starting.")

  defp check(config) do
    case probe(config) do
      :ok -> Layout.check(config)
      {:error, reason} -> {:error, {:probe_failed, reason}}
    end
  end

  defp probe(%Config{probe: false, cluster: cluster}) do
    GenServer.cast(Slap.Cluster.host(cluster), {:probe, :skipped})
    :ok
  end

  defp probe(config) do
    result = SlateDB.probe_store(config.store, Config.probe_path(config))
    ok? = match?({:ok, _}, result)
    status = if ok?, do: :ok, else: result
    GenServer.cast(Slap.Cluster.host(config.cluster), {:probe, status})
    Telemetry.execute([:probe], %{}, %{cluster: config.cluster, ok: ok?, result: result})

    case result do
      {:ok, steps} ->
        Logger.debug("#{inspect(config.cluster)}: store probe passed: #{inspect(steps)}")
        :ok

      {:error, {:probe_failed, steps}} ->
        {:error, steps}

      error ->
        error
    end
  end

  defp start_strategy(%Config{cluster: cluster, strategy: {strategy, opts}}) do
    spec = strategy.child_spec(Keyword.put(opts, :cluster, cluster))

    case DynamicSupervisor.start_child(Slap.Cluster.strategy_supervisor(cluster), spec) do
      {:ok, _pid} -> :ignore
      {:ok, _pid, _info} -> :ignore
      :ignore -> :ignore
      {:error, reason} -> {:stop, reason}
    end
  end

  defp describe(config, {:probe_failed, result}),
    do:
      "the store at #{store(config.store)} failed the conditional write probe, so " <>
        "writers could not be fenced safely. Result: #{inspect(result)}."

  defp describe(config, {:layout_mismatch, %{configured: configured, stored: :unrecorded}}),
    do:
      "the store at #{store(config.store)}, path #{inspect(config.path)}, has shard " <>
        "databases for a different count than the configured #{configured.shards}, and " <>
        "no _cluster/layout record. " <> moving()

  defp describe(config, {:layout_mismatch, %{configured: configured, stored: stored}}),
    do:
      "the store at #{store(config.store)}, path #{inspect(config.path)}, was written " <>
        "with #{stored.shards} shards (hash #{stored.hash}); this cluster is configured " <>
        "with #{configured.shards} (hash #{configured.hash}). " <> moving()

  defp describe(config, {:invalid_layout, body}),
    do:
      "the store at #{store(config.store)}, path #{inspect(config.path)}, has an " <>
        "unreadable _cluster/layout record: #{inspect(body)}."

  defp describe(config, reason),
    do:
      "could not check the shard layout of the store at #{store(config.store)}: " <>
        "#{inspect(reason)}."

  defp moving,
    do:
      "Records are not moved between shards, so keys on the other shards would read " <>
        "as missing. Set :shards to the count the data was written with, or copy the " <>
        "data into a new :path."

  # The store without credentials.
  defp store({:url, url, options}) do
    endpoint = Enum.find_value(options, fn {k, v} -> to_string(k) =~ "endpoint" && v end)
    if endpoint, do: "#{url} (endpoint #{endpoint})", else: url
  end

  defp store({:url, url}), do: url
  defp store(other), do: inspect(other)
end
