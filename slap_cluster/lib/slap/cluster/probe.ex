defmodule Slap.Cluster.Probe do
  @moduledoc false
  # Runs `Slap.SlateDB.probe_store/2` once when the cluster starts, before any
  # shard opens. A failure stops the cluster from starting.

  require Logger

  alias Slap.Cluster.{Config, Telemetry}
  alias Slap.SlateDB

  def run(%Config{probe: false}), do: :ignore

  def run(config) do
    path = Config.probe_path(config)
    result = SlateDB.probe_store(config.store, path)
    ok? = match?({:ok, _}, result)
    Telemetry.execute([:probe], %{}, %{cluster: config.cluster, ok: ok?, result: result})

    case result do
      {:ok, steps} ->
        Logger.debug("#{inspect(config.cluster)}: store probe passed: #{inspect(steps)}")
        :ignore

      {:error, reason} ->
        reason = probe_reason(reason)

        Logger.error(
          "#{inspect(config.cluster)}: the store at #{describe(config.store)} failed the " <>
            "conditional write probe, so writers could not be fenced safely. Not starting. " <>
            "Result: #{inspect(reason)}"
        )

        {:error, {:probe_failed, reason}}
    end
  end

  defp probe_reason({:probe_failed, steps}), do: steps
  defp probe_reason(error), do: error

  # The store without credentials.
  defp describe({:url, url, options}) do
    endpoint = Enum.find_value(options, fn {k, v} -> to_string(k) =~ "endpoint" && v end)
    if endpoint, do: "#{url} (endpoint #{endpoint})", else: url
  end

  defp describe({:url, url}), do: url
  defp describe(other), do: inspect(other)
end
