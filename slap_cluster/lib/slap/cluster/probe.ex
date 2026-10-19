defmodule Slap.Cluster.Probe do
  @moduledoc false
  # The strategy can open writers, so start it only after the store passes
  # the fencing probe, unless probing is disabled.

  use GenServer
  require Logger

  alias Slap.Cluster.{Config, Telemetry}
  alias Slap.SlateDB

  @retry_base 1_000
  @retry_max 30_000

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl true
  def init(%Config{probe: :background} = config),
    do: {:ok, {config, @retry_base}, {:continue, :probe}}

  def init(config) do
    case probe(config) do
      :ok ->
        start_strategy(config)

      {:error, reason} ->
        Logger.error(
          "#{inspect(config.cluster)}: the store at #{describe(config.store)} failed the " <>
            "conditional write probe, so writers could not be fenced safely. Not starting. " <>
            "Result: #{inspect(reason)}"
        )

        {:stop, {:probe_failed, reason}}
    end
  end

  @impl true
  def handle_continue(:probe, {config, delay}) do
    case probe(config) do
      :ok ->
        case start_strategy(config) do
          :ignore -> {:noreply, {config, delay}}
          {:stop, reason} -> {:stop, reason, {config, delay}}
        end

      {:error, reason} ->
        Logger.warning(
          "#{inspect(config.cluster)}: the store at #{describe(config.store)} failed the " <>
            "conditional write probe; no shards are open. Retrying in #{delay} ms. " <>
            "Result: #{inspect(reason)}"
        )

        Process.send_after(self(), :retry, delay)
        {:noreply, {config, min(delay * 2, @retry_max)}}
    end
  end

  @impl true
  def handle_info(:retry, state), do: {:noreply, state, {:continue, :probe}}

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

  # The store without credentials.
  defp describe({:url, url, options}) do
    endpoint = Enum.find_value(options, fn {k, v} -> to_string(k) =~ "endpoint" && v end)
    if endpoint, do: "#{url} (endpoint #{endpoint})", else: url
  end

  defp describe({:url, url}), do: url
  defp describe(other), do: inspect(other)
end
