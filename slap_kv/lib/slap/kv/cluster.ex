defmodule Slap.KV.Cluster do
  @moduledoc """
  The shards that hold the rows, a `Slap.Cluster`. A partition's rows are
  on shard `shard_for(partition)`. Each shard runs partition writers,
  each the only process that writes its own
  subset of the shard's partitions. Set
  `child_options: [partition_writers: n]` on the cluster (default 16).

  Run one instance of this built-in cluster per VM. It can own many shards
  and participate in a cluster across VMs. The same VM can also run
  `Slap.Streams.Cluster`.

  To configure a cluster under your application's name, define a module
  with `use Slap.KV.Cluster, otp_app: :my_app`, supervise that module in
  place of `Slap.KV.Cluster`, and pass `cluster: MyApp.KVCluster` to KV
  operations and `Slap.KV.HTTP.Router`. Run one KV cluster per VM; peer
  discovery is node-wide.

  Configure it like any `Slap.Cluster`:

      config :slap_kv, Slap.KV.Cluster,
        store: {:url, "s3://bucket/prefix", aws_endpoint: "https://rustfs:9000"},
        path: "kv",
        shards: 8,
        settings: %{flush_interval: "10ms"}

  Writes are acknowledged once durable, so `flush_interval` is the floor of
  write latency. Give it its own `:path` when it shares a store with
  another cluster.
  """

  use Slap.Cluster,
    otp_app: :slap_kv,
    defaults: [shard_children: {Slap.KV.PartitionWriter, :child_specs, []}]

  @doc "Defines a KV cluster under the given `:otp_app`."
  defmacro __using__(opts) do
    Keyword.validate!(opts, [:otp_app])
    otp_app = Keyword.fetch!(opts, :otp_app)

    quote do
      use Slap.Cluster,
        otp_app: unquote(otp_app),
        defaults: [shard_children: {Slap.KV.PartitionWriter, :child_specs, []}]
    end
  end
end
