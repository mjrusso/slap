defmodule JepsenNode.Application do
  @moduledoc """
  Runs one node of the test cluster, with shards and file bodies in S3.

    * 4437: Durable Streams and KV (`/v1/kv/`).
    * 4438: Yjs (`JepsenNode.YjsRouter`).
    * 4439: Files (`JepsenNode.FilesRouter`).
    * 4440: Snapshot logs (`JepsenNode.LogRouter`).
    * 4441: Forks and copies (`JepsenNode.ForkRouter`).

  Configured by:

    * `JEPSEN_STORE` - the store URL, `s3://bucket/prefix`; the endpoint and
      credentials come from the `AWS_*` variables.
    * `JEPSEN_NODES` - every node's name, comma-separated.
    * `JEPSEN_SHARDS` - the number of shards (default 8), for the streams and
      for KV each.
    * `JEPSEN_PLACEMENT` - `object-lease` (the default; leases in the
      bucket) or `distributed` (placement from the connected nodes).
    * `JEPSEN_KV_ADMISSION` - `normal` (defaults) or `low` (one pending
      request per writer, 32 KiB per writer and 64 KiB per shard).
    * `JEPSEN_LEASE_TTL` - ms, for `object-lease` (default 15,000).
  """

  use Application

  @impl true
  def start(_type, _args) do
    nodes =
      "JEPSEN_NODES"
      |> System.fetch_env!()
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_atom/1)

    shards = String.to_integer(System.get_env("JEPSEN_SHARDS", "8"))

    slap = [
      store: {:url, System.fetch_env!("JEPSEN_STORE")},
      streams: [shards: shards, settings: %{flush_interval: "10ms"}],
      yjs: [],
      kv: [
        shards: shards,
        child_options: kv_admission(System.get_env("JEPSEN_KV_ADMISSION", "normal"))
      ],
      files: [
        retention_ms: 2_000,
        upload_timeout_ms: 10_000,
        sweep_interval_ms: 1_000,
        # The containers share the host's clock.
        max_clock_skew_ms: 0
      ],
      strategy: strategy(System.get_env("JEPSEN_PLACEMENT", "object-lease")),
      peers: nodes -- [node()]
    ]

    JepsenNode.Stats.setup()
    JepsenNode.FilesRouter.setup(nodes)
    JepsenNode.LogRouter.setup(nodes)
    JepsenNode.Readiness.setup(nodes)

    children =
      [
        {Slap, slap},
        listener(4437, {Slap.Router, streams: [], kv: []}, :slap_listener)
      ] ++
        JepsenNode.Log.child_specs() ++
        [
          listener(4440, JepsenNode.LogRouter, :log_listener),
          listener(4441, JepsenNode.ForkRouter, :fork_listener),
          listener(4438, JepsenNode.YjsRouter, :yjs_listener),
          listener(4439, JepsenNode.FilesRouter, :files_listener)
        ]

    Supervisor.start_link(children, strategy: :one_for_one, name: JepsenNode.Supervisor)
  end

  defp listener(port, plug, id),
    do: Supervisor.child_spec({Bandit, plug: plug, port: port, ip: :any}, id: id)

  defp kv_admission("normal"), do: []

  defp kv_admission("low") do
    [
      partition_writers: 4,
      max_inflight_bytes_per_writer: 32_768,
      max_inflight_bytes_per_shard: 65_536,
      max_inflight_requests_per_writer: 1
    ]
  end

  defp strategy("distributed"), do: {Slap.Cluster.Strategy.Distributed, []}

  defp strategy("object-lease") do
    ttl = String.to_integer(System.get_env("JEPSEN_LEASE_TTL", "15000"))
    {Slap.Cluster.Strategy.ObjectLease, lease_ttl: ttl}
  end
end
