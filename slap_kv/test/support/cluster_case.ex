defmodule Slap.KV.Test.ClusterCase do
  @moduledoc false
  # Starts Slap.KV.Cluster on a fresh local directory for each test. Tests
  # are not async: the cluster is a named process.

  use ExUnit.CaseTemplate

  alias Slap.KV

  using do
    quote do
      import Slap.KV.Test.ClusterCase
      alias Slap.KV
    end
  end

  setup context do
    dir = Path.join(System.tmp_dir!(), "slap-kv-test-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)

    settings =
      Map.merge(
        %{flush_interval: "2ms", manifest_poll_interval: "100ms"},
        context[:settings] || %{}
      )

    opts = [store: {:local, dir}, shards: context[:shards] || 2, settings: settings]
    start_supervised!({KV.Cluster, opts})
    %{dir: dir, cluster_opts: opts}
  end

  @doc "The context of `partition`'s shard (a single-node test cluster has every shard open)."
  def ctx_for(partition) do
    {:ok, {:local, ctx}} = KV.Cluster.lookup(KV.Cluster.shard_for(partition))
    ctx
  end

  @doc """
  Lets each shard's WAL flush timer tick once. It fires once right after
  the database opens, and that first tick can come late on a busy machine;
  with a long `flush_interval`, writes after it wait for a flush.
  """
  def warm_up do
    for n <- KV.Cluster.local_shards() do
      {:ok, {:local, ctx}} = KV.Cluster.lookup(n)
      {:ok, _} = Slap.SlateDB.put(ctx.db, KV.Keys.encode("warm-up", "x"), "x")
      :ok = Slap.SlateDB.flush(ctx.db)
    end

    Process.sleep(200)
  end

  @doc "The partition writer of `partition`."
  def partition_writer(partition) do
    ctx = ctx_for(partition)
    [{pid, _}] = Registry.lookup(ctx.registry, {ctx.n, {:partition_writer, index(partition)}})
    pid
  end

  @doc "The index of `partition`'s partition writer on its shard."
  def index(partition) do
    {:ok, i} = KV.PartitionWriter.index(ctx_for(partition), partition)
    i
  end

  @doc "The supervisor and child id of `partition`'s partition writer."
  def partition_writer_child(partition) do
    {:dictionary, dictionary} = Process.info(partition_writer(partition), :dictionary)
    {_, [supervisor | _]} = List.keyfind(dictionary, :"$ancestors", 0)
    {supervisor, {KV.PartitionWriter, index(partition)}}
  end
end
