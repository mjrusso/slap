defmodule Slap.KV.Cluster do
  @moduledoc """
  The shards that hold the rows, a `Slap.Cluster`. A partition's rows are
  on shard `shard_for(partition)`. Each shard runs partition writers; each
  partition has one writer. Set
  `child_options: [partition_writers: n]` on the cluster (default 16).

  Each shard limits the work waiting for durability. The limits must be
  positive integers. Defaults:

    * `:max_inflight_bytes_per_writer` - 64 MiB.
    * `:max_inflight_bytes_per_shard` - 256 MiB.
    * `:max_inflight_requests_per_writer` - 1,024.
    * `:max_batch_ops` - 1,000.
    * `:max_batch_bytes` - 8 MiB.

  The shard's owner checks a request against them before it reads or
  writes any row. A request's size is its encoded keys (two length bytes,
  the partition and the key) and put values; a linearizable get counts as
  one request of zero bytes. A request counts until it is durable and
  answered. The limits do not cover mailboxes, HTTP decoding, response
  bodies or total BEAM and native memory. The README's "Limits on pending
  work" describes the errors.

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
        shards: 8

  Durable writes wait for SlateDB's next WAL flush. This cluster sets
  `flush_interval` to 10 ms; SlateDB's default is 100 ms. Other settings
  leave the 10 ms default intact. The README describes the latency and
  object-store request trade-off.

  Give the cluster its own `:path` when it shares a store with another
  cluster.
  """

  alias Slap.KV.Admission

  @default_settings %{flush_interval: "10ms"}

  use Slap.Cluster,
    otp_app: :slap_kv,
    defaults: [
      shard_children: {Slap.KV.PartitionWriter, :child_specs, []},
      settings: @default_settings
    ]

  @doc "Returns the accepted `:child_options` keys for a KV cluster."
  @spec child_option_keys() :: [atom()]
  def child_option_keys, do: Admission.option_keys()

  @doc "Defines a KV cluster under the given `:otp_app`."
  defmacro __using__(opts) do
    Keyword.validate!(opts, [:otp_app])
    otp_app = Keyword.fetch!(opts, :otp_app)

    settings = Macro.escape(@default_settings)

    quote do
      use Slap.Cluster,
        otp_app: unquote(otp_app),
        defaults: [
          shard_children: {Slap.KV.PartitionWriter, :child_specs, []},
          settings: unquote(settings)
        ]
    end
  end
end
