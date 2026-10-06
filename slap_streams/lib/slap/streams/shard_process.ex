defmodule Slap.Streams.ShardProcess do
  @moduledoc false

  defmacro __using__(opts) do
    name = Keyword.fetch!(opts, :name)

    quote do
      use GenServer

      def child_spec(ctx), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [ctx]}}

      def start_link(ctx), do: GenServer.start_link(__MODULE__, ctx, name: name(ctx))

      defp name(ctx), do: Slap.Cluster.via(ctx.cluster, ctx.n, unquote(name))
    end
  end
end
