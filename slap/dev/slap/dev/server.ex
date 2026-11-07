defmodule Slap.Dev.Server do
  @moduledoc false

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(config) do
    {listener, services} = Keyword.split(config, [:port, :ip])
    {http, services} = split_http(services)

    children = [
      Slap.child_spec(services),
      Supervisor.child_spec(
        {Bandit,
         plug: {Slap.Router, http},
         port: Keyword.get(listener, :port, 4437),
         ip: Keyword.get(listener, :ip, :loopback)},
        id: {__MODULE__, :listener}
      )
    ]

    %{
      id: __MODULE__,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
      type: :supervisor
    }
  end

  defp split_http(config) do
    Enum.reduce([:streams, :kv], {[], config}, fn service, {http, config} ->
      case Keyword.fetch(config, service) do
        {:ok, opts} when is_list(opts) ->
          {router, opts} = Keyword.pop(opts, :http, [])
          {[{service, router} | http], Keyword.put(config, service, opts)}

        _ ->
          {http, config}
      end
    end)
  end
end
