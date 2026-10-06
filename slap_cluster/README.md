# slap_cluster

`slap_cluster` manages a fixed number of SlateDB databases ("shards") in one
store, using [`slap_slatedb`](https://hexdocs.pm/slap_slatedb/). It assigns
each shard to a node, opens its database there, routes calls to its owner, and
reports when writes become durable. If SlateDB fences a writer, the shard
stops and the placement strategy can assign it elsewhere. The application
provides the processes that use each shard and decides what they store.

Use `slap_cluster` when you are building a sharded service with your own data
model and need to route operations to shard owners or move shards between
nodes after failures. It manages one SlateDB writer per shard; your shard
children implement the service's reads, writes, and responses. For one
database without shard placement, use `slap_slatedb` directly. If your data
fits the Streams or partitioned KV APIs, use `slap_streams` or `slap_kv`, which
already build on this package.

<!-- slap-preamble -->
> #### About Slap {: .info}
>
> This package is part of [Slap](https://github.com/mjrusso/slap).
>
> Slap provides Elixir bindings for [SlateDB](https://slatedb.io) (an embedded
> key-value engine optimized for storing data in object storage), as well as
> services built on top of these bindings. The services run inside your
> application, on one node or several.
>
> Services:
>
> - [slap_streams](https://hexdocs.pm/slap_streams/): a [Durable
>   Streams](https://durablestreams.com) server, with HTTP, long polling, and
>   SSE
> - [slap_kv](https://hexdocs.pm/slap_kv/): partitioned key-value storage with
>   conditional writes, ordered scans, and an HTTP API
> - [slap_files](https://hexdocs.pm/slap_files/): file storage with metadata
>   and conditional replacement; small files are kept in `slap_kv`
>   records, which saves object-store PUTs
> - [slap_yjs](https://hexdocs.pm/slap_yjs/): [Yjs](https://yjs.dev) documents
>   for collaborative editing, with replication and presence across nodes
>
> Building blocks:
>
> - [slap_slatedb](https://hexdocs.pm/slap_slatedb/): the SlateDB bindings
> - [slap_cluster](https://hexdocs.pm/slap_cluster/): SlateDB databases run as
>   shards across nodes, with placement, routing, and failover
> - [slap_snapshot_log](https://hexdocs.pm/slap_snapshot_log/): an append-only
>   log of changes that can be condensed into snapshots
>
> Standalone server:
>
> - [slap](https://hexdocs.pm/slap/): an HTTP server for evaluating and
>   benchmarking `slap_streams` and `slap_kv`
<!-- /slap-preamble -->

## Installation

Add `slap_cluster` to the dependencies in your application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_cluster, "~> 0.1.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_cluster/).

## Example

In an application that depends on `slap_cluster`, define a cluster module:

```elixir
defmodule MyApp.Cluster do
  use Slap.Cluster, otp_app: :my_app
end
```

Configure it in `config/config.exs`. This local configuration sketch assumes
the application provides `MyApp.ShardChildren.child_specs/1` to start its
processes for each shard:

```elixir
config :my_app, MyApp.Cluster,
  store: {:local, "/tmp/my-app-cluster"},
  shards: 8,
  shard_children: {MyApp.ShardChildren, :child_specs, []}
```

Add `MyApp.Cluster` to the application's supervision tree after defining
its shard children.

In a shard child, `ctx` is its context, `ops` is a SlateDB write batch, and
`ref` is a token chosen by the child to identify the notification.
The child can request a durability notification for a write:

```elixir
{:ok, seq} = Slap.SlateDB.write(ctx.db, ops)
Slap.Cluster.notify_when_durable(ctx, seq, {:ack, ref})
```

The child later receives `{:slap_cluster_durable, {:ack, ref}}`. A caller can
route an operation to the owner of a key's shard. Here `MyApp.Streams.append/2`
is an application function, and `path` and `body` are its arguments:

```elixir
shard = MyApp.Cluster.shard_for("some/key")
{:ok, result} = MyApp.Cluster.call(shard, {MyApp.Streams, :append, [path, body]})
```

The outer `{:ok, result}` means routing succeeded. `result` is the application
function's return value, including any `{:error, reason}` it returns.

`Slap.Cluster`'s docs cover the options, shard children, durability and
telemetry.

## How it works

The cluster probes the store's create-if-absent behavior before opening
shards. `ObjectLease` also requires `If-Match` support. Each shard has one
database handle and one durability subscription.
Application shard children stop before the handle closes. A fenced or crashed
database stops its shard; the placement strategy decides where to reopen it.
Reopening without that decision could fence a new owner.
Children that need the stop reason use `shard_status/1`; their supervisor exit
reason is `:shutdown` even when the shard was fenced.

When a child calls `notify_when_durable/3`, the shard compares the write's
sequence number with SlateDB's current durable sequence number, not with the
last durability message it handled. A write that is already durable is reported
at once, even if that message is still in the shard's mailbox. `shard_for/1`
uses `xxh64(key) mod shards`; changing the shard count or hash moves existing
keys.

## Several nodes

To run a cluster on several nodes, use a shared object store and a placement
strategy.
For an S3-compatible store such as RustFS, `aws_endpoint` sets its endpoint;
the store must support conditional writes:

```elixir
config :my_app, MyApp.Cluster,
  store:
    {:url, "s3://bucket/prefix",
     aws_endpoint: "http://rustfs:9000", aws_allow_http: true},
  shards: 64,
  settings: %{flush_interval: "10ms"},
  cache: [capacity_bytes: 2 * 1024 * 1024 * 1024],
  shard_children: {MyApp.ShardChildren, :child_specs, []},
  strategy: {Slap.Cluster.Strategy.ObjectLease, lease_ttl: 15_000}
```

Nodes connect through distributed Erlang. `ObjectLease` stores conditional
leases and node heartbeats in the bucket. An owner renews its leases and stops
a shard if renewal fails past its local deadline, before another node can
claim it. A paused owner that misses the deadline is fenced by SlateDB when a
new owner opens the database. A clean stop closes shards before freeing their
leases. Self-fencing runs separately from the renewal loop so a blocked store
call cannot delay it.

`call/4` routes to the owner over `:erpc`. If the reply is `:not_owner` or
`:unassigned`, or the owner cannot be reached before the call is sent,
`call/4` refreshes the placement and retries. A call that was already sent is
not retried, because it may have run; the caller receives
`{:error, {:erpc, :noconnection}}`. Lease data does not create node-name
atoms, so peers must be discovered or configured before routing to them.

## Placement strategies

- **`Local`** runs all shards on one node. The node must restart to recover
  from a failure.
- **`ObjectLease`** uses an object store with `If-Match` (S3, RustFS, GCS or
  Azure) and roughly synchronised clocks. Failover takes about one lease
  TTL. A node cut off from the bucket stops its shards before its leases
  expire.
- **`Distributed`** uses distributed Erlang without leases. A dead node's
  shards move after `:settle` (2 s); an unresponsive node also requires
  `net_ticktime`. A partition can let both sides open a shard. SlateDB
  fences one writer, but that side may serve stale reads until it finds that
  it has been fenced.
- **`Static`** uses a fixed list of nodes. A failed node must return before
  its shards are available again.

`ObjectLease` requires clocks within `:max_clock_skew_ms` (1 s by default); the
local file store cannot provide `If-Match`. Lower `net_ticktime` with
`Distributed` if failure detection must be faster.

## Tests

```sh
mix test                                                     # local directories
SLAP_TEST_S3_ENDPOINT=http://127.0.0.1:9000 mix test      # also 64 shards on RustFS
```

The tests exercise durability, fencing, placement and routing with a counter
server on each shard. Multi-node tests use separate OS processes and a shared
store; `SLAP_TEST_S3_ENDPOINT` enables the RustFS cases. See
`test/support/failover.ex` for the fault test harness.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
