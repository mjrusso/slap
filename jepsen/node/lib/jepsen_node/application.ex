defmodule JepsenNode.Application do
  @moduledoc """
  One node of the cluster under test: Durable Streams and KV on port 4437
  (the placement strategy below, shards in S3; KV under `/v1/kv/`), the
  Yjs API (`JepsenNode.YjsRouter`) on port 4438, and files
  (`JepsenNode.FilesRouter`, bodies in the same bucket) on port 4439, and
  snapshot logs (`JepsenNode.LogRouter`) on port 4440.
  Configured by:

    * `JEPSEN_STORE` - the store URL, `s3://bucket/prefix`; the endpoint and
      credentials come from the `AWS_*` variables.
    * `JEPSEN_NODES` - every node's name, comma-separated.
    * `JEPSEN_SHARDS` - the number of shards (default 8), for the streams and
      for KV each.
    * `JEPSEN_PLACEMENT` - `object-lease` (the default; leases in the
      bucket) or `distributed` (placement from the connected nodes).
    * `JEPSEN_LEASE_TTL` - ms, for `object-lease` (default 15,000).
  """

  use Application

  alias Slap.Yjs

  @impl true
  def start(_type, _args) do
    nodes =
      "JEPSEN_NODES"
      |> System.fetch_env!()
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_atom/1)

    shards = String.to_integer(System.get_env("JEPSEN_SHARDS", "8"))

    config = [
      store: {:url, System.fetch_env!("JEPSEN_STORE")},
      streams: [shards: shards, settings: %{flush_interval: "10ms"}],
      kv: [shards: shards],
      port: 4437,
      ip: :any,
      strategy: strategy(System.get_env("JEPSEN_PLACEMENT", "object-lease")),
      peers: nodes -- [node()]
    ]

    # Files are cleaned up within a test: old bodies are kept 2 s, uploads
    # expire after 10 s, and every node sweeps each second. The containers
    # share the host's clock.
    files =
      {Slap.Files,
       store: config[:store],
       path: "files",
       retention_ms: 2_000,
       upload_timeout_ms: 10_000,
       sweep_interval_ms: 1_000,
       max_clock_skew_ms: 0}

    JepsenNode.Stats.setup()
    JepsenNode.FilesRouter.setup(nodes)
    JepsenNode.LogRouter.setup(nodes)
    JepsenNode.Readiness.setup(nodes)

    children =
      [{Slap.Server, config}] ++
        JepsenNode.Log.child_specs() ++
        [
          Supervisor.child_spec({Bandit, plug: JepsenNode.LogRouter, port: 4440},
            id: :log_listener
          ),
          Yjs.Docs,
          {Bandit, plug: JepsenNode.YjsRouter, port: 4438},
          files,
          Supervisor.child_spec({Bandit, plug: JepsenNode.FilesRouter, port: 4439},
            id: :files_listener
          )
        ]

    Supervisor.start_link(children, strategy: :one_for_one, name: JepsenNode.Supervisor)
  end

  defp strategy("distributed"), do: {Slap.Cluster.Strategy.Distributed, []}

  defp strategy("object-lease") do
    ttl = String.to_integer(System.get_env("JEPSEN_LEASE_TTL", "15000"))
    {Slap.Cluster.Strategy.ObjectLease, lease_ttl: ttl}
  end
end
