defmodule Slap.Streams.Cluster do
  @moduledoc """
  The shards that hold the streams, a `Slap.Cluster`. Each shard allocates
  stream ids and supervises one process per active stream.

  Run one instance of this built-in cluster per VM. It can own many shards
  and participate in a cluster across VMs. `Slap.Streams.Metrics` reports
  node-wide metrics for this instance.

  To configure a cluster under your application's name, define a module
  with `use Slap.Streams.Cluster, otp_app: :my_app`, supervise that module
  in place of `Slap.Streams.Cluster`, and pass `cluster: MyApp.StreamsCluster`
  to Streams operations and `Slap.Streams.HTTP.Router`. Run one Streams
  cluster per VM; metrics and the router's request-body budget are node-wide.

  Configure it like any `Slap.Cluster`:

      config :slap_streams, Slap.Streams.Cluster,
        store: {:url, "s3://streams/db", aws_endpoint: "https://rustfs:9000"},
        shards: 8

  Durable appends wait for SlateDB's next WAL flush. This cluster sets
  `flush_interval` to 10 ms, the value used by the Durable Streams design;
  SlateDB's default is 100 ms. Other settings leave the 10 ms default intact.
  The README describes the latency and object-store request trade-off.

  Service settings go in `child_options:` at start. See
  `child_option_keys/0` for the accepted keys. Values are positive integers
  in milliseconds for timeouts and intervals, bytes for byte limits, and
  entries for `:deleter_page`. `:max_fork_copy_bytes` and `:fork_copy_grace`
  may also be zero.
  """

  @default_settings %{flush_interval: "10ms"}

  use Slap.Cluster,
    otp_app: :slap_streams,
    defaults: [
      shard_children: {Slap.Streams.ShardChildren, :child_specs, []},
      settings: @default_settings
    ]

  @doc false
  def __slap_streams_cluster__, do: true

  @doc false
  def streams_cluster?(cluster),
    do: is_atom(cluster) and function_exported?(cluster, :__slap_streams_cluster__, 0)

  @doc "Returns the accepted `:child_options` keys for a Streams cluster."
  @spec child_option_keys() :: [atom()]
  def child_option_keys do
    [
      :idle_timeout,
      :max_fork_copy_bytes,
      :fork_copy_grace,
      :expiry_interval,
      :repair_interval,
      :deleter_interval,
      :deleter_page,
      :load_interval,
      :max_inflight_bytes_per_stream,
      :max_inflight_bytes_per_shard
    ]
  end

  @doc "Defines a Streams cluster under the given `:otp_app`."
  defmacro __using__(opts) do
    Keyword.validate!(opts, [:otp_app])
    otp_app = Keyword.fetch!(opts, :otp_app)

    settings = Macro.escape(@default_settings)

    quote do
      use Slap.Cluster,
        otp_app: unquote(otp_app),
        defaults: [
          shard_children: {Slap.Streams.ShardChildren, :child_specs, []},
          settings: unquote(settings)
        ]

      @doc false
      def __slap_streams_cluster__, do: true
    end
  end
end
