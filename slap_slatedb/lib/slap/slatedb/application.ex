defmodule Slap.SlateDB.Application do
  @moduledoc false
  use Application

  alias Slap.SlateDB
  alias Slap.SlateDB.Native

  @impl true
  def start(_type, _args) do
    case runtime_threads!() |> Native.runtime_init() |> Native.normalize() do
      :ok -> :ok
      {:error, error} -> raise error
    end

    Supervisor.start_link([SlateDB.LogForwarder],
      strategy: :one_for_one,
      name: SlateDB.Supervisor
    )
  end

  defp runtime_threads! do
    case Application.get_env(:slap_slatedb, :runtime_threads) do
      nil -> env_threads!()
      threads -> validate_threads!(threads)
    end
  end

  defp env_threads! do
    case System.get_env("SLAP_SLATEDB_RUNTIME_THREADS") do
      nil ->
        nil

      value ->
        case Integer.parse(String.trim(value)) do
          {threads, ""} when threads > 0 -> threads
          _ -> raise ArgumentError, "SLAP_SLATEDB_RUNTIME_THREADS must be a positive integer"
        end
    end
  end

  defp validate_threads!(threads) when is_integer(threads) and threads > 0, do: threads

  defp validate_threads!(_threads),
    do: raise(ArgumentError, ":runtime_threads must be a positive integer")
end
