defmodule Slap.Streams.Test.ClusterCase do
  @moduledoc false

  alias Slap.Streams
  # Starts Slap.Streams.Cluster on a fresh local directory for each test.

  use ExUnit.CaseTemplate

  using do
    quote do
      import Slap.Streams.Test.ClusterCase
      alias Slap.Streams
    end
  end

  setup context do
    dir = Path.join(System.tmp_dir!(), "slap-streams-test-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    settings =
      Map.merge(
        %{flush_interval: "2ms", manifest_poll_interval: "100ms"},
        context[:settings] || %{}
      )

    opts = [
      store: {:local, dir},
      shards: context[:shards] || 2,
      settings: settings,
      child_options: context[:child_options] || []
    ]

    start_supervised!({Streams.Cluster, opts})
    %{dir: dir, cluster_opts: opts}
  end

  @doc "The context of `path`'s shard (a single-node test cluster has every shard open)."
  def ctx_for(path) do
    {:ok, {:local, ctx}} = Streams.Cluster.lookup(Streams.Cluster.shard_for(path))
    ctx
  end

  @doc "The stream server of `path`, if running."
  def server(path) do
    ctx = ctx_for(path)

    case Registry.lookup(ctx.registry, {ctx.n, {:stream, path}}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  @doc "The messages' bytes from a read."
  def bodies(%{messages: messages}), do: Enum.map(messages, &elem(&1, 1))
end
