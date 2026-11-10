# slap_cluster

`slap_cluster` runs a fixed number of SlateDB databases, called shards, across
the nodes of an Elixir cluster. The shards share one store, such as a prefix in
an S3 bucket. `slap_cluster` assigns each shard to one node, opens the shard's
database there, and routes calls for the shard to that node. Depending on the
placement strategy, a failed node's shards move to other nodes. SlateDB allows
one writer per database: if two nodes open the same shard, only the newer one
can write. `slap_cluster` also reports when a shard's writes become durable. It
opens the databases with [`slap_slatedb`](https://hexdocs.pm/slap_slatedb/).

An object store must support conditional writes, as S3 does. The cluster checks
this before opening shards. By default, a failed check prevents startup; see
[How it works](#how-it-works) for background retries.

`slap_cluster` does not decide what the shards store. Your application provides
the processes that run with each shard on its node, called shard children, and
they implement the service's reads, writes, and replies. For one database on
one node, use `slap_slatedb` directly. Use `slap_cluster` directly if your data
does not fit an existing service such as
[`slap_streams`](https://hexdocs.pm/slap_streams/) or
[`slap_kv`](https://hexdocs.pm/slap_kv/) (which are themselves built on
`slap_cluster`).

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
> All-in-one:
>
> - [slap](https://hexdocs.pm/slap/): all Slap server libraries, and `Slap`,
>   which starts the services you select under one supervisor
>
> Inspection:
>
> - [slap_inspect](https://hexdocs.pm/slap_inspect/): a read-only TUI and CLI
>   for browsing Slap data; installed separately
>
> The `slap` package pulls in all seven server libraries. If you only need a
> subset, add those packages directly.
<!-- /slap-preamble -->

## Installation

Requires Elixir 1.18 or later.

To depend on `slap_cluster` directly, add it to the dependencies in your
application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_cluster, "~> 0.6.1"}
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
  shards: 1,
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
route an operation to the owner of a key's shard. Here `MyApp.Streams.append/3`
is an application function. The owner calls it with the shard context as an
extra first argument, so it receives `(ctx, path, body)`:

```elixir
shard = MyApp.Cluster.shard_for("some/key")
{:ok, result} = MyApp.Cluster.call(shard, {MyApp.Streams, :append, [path, body]})
```

The outer `{:ok, result}` means routing succeeded. `result` is the application
function's return value, including any `{:error, reason}` it returns.

`Slap.Cluster`'s docs cover the options, shard children, durability and
telemetry.

## Readiness

With `probe: :background`, startup can return before any shard opens. Even
with synchronous probing, failed shard opens retry in the background. Before
boot work that needs the cluster, wait for every configured shard and its
children to finish starting:

```elixir
:ok = MyApp.Cluster.await_ready(30_000)
```

The result is `:ok` or `{:error, :timeout}`. The timeout covers the whole wait,
including remote checks and retries. `await_ready(:infinity)` waits without an
overall deadline. Each attempt has a finite deadline so waits can rediscover
owners after placement changes, even when the previous owner stops responding.

Readiness is checked on every call, including during child restarts. A shard
whose supervisor cannot answer within the inspection deadline counts as
unready. Ignored children, completed temporary tasks, and stopped or deleted
child specifications do not block readiness. Children added through
`Supervisor.start_child/2` are included. Inspection covers direct shard
children. An application-provided supervisor is checked for a live process;
its descendants are outside the readiness check.

For health checks, `MyApp.Cluster.status()` returns
`{:ok, %{ready?: ready?, probe: probe, shards: shards}}`. Each shard has its
owner node, current lifecycle status, and `:last_error`. The local startup
probe has its own status and `:last_error`. Hosts retain probe and shard
failures after recovery, until they restart. `status(timeout)` bounds the whole
query (default 5,000 ms). It returns `{:error, :timeout}` when the query times out,
or `{:error, :unavailable}` when the local cluster cannot be queried.

These functions read lifecycle state without storage I/O. To check current
storage reachability, call `Slap.SlateDB.probe_store/3` separately.

## How it works

The cluster probes the store's create-if-absent behavior before opening
shards. A failed probe normally prevents startup. With `probe: :background`,
the cluster starts without shards and retries until the probe passes.
`ObjectLease` also requires `If-Match` support. Each shard has one database
handle and one durability subscription.

During orderly shutdown, application shard children stop before the database
closes. If a shard process is killed or exceeds its shutdown timeout, database
closure continues independently of the cluster. A replacement on this node
waits for closure before opening another handle. A fenced or crashed
database stops its shard; the placement strategy decides where to reopen it.
Reopening without that decision could fence a new owner.
Set `restart: :temporary, significant: true` on an application child to stop
the shard when that child exits.
Children that need the stop reason use `shard_status/1`; their supervisor exit
reason is `:shutdown` even when the shard was fenced.

When a child calls `notify_when_durable/3`, the shard compares the write's
sequence number with SlateDB's current durable sequence number, not with the
last durability message it handled. A write that is already durable is reported
at once, even if that message is still in the shard's mailbox. `shard_for/1`
uses `xxh64(key) mod shards`; the shard count and hash must stay fixed for
existing data.

## Running another cluster on the same store

Before running `mix run` or `iex -S mix` with the same store and `:path` as a
running application, stop the application or configure the script with a
different store or `:path`. Opening another writer fences the existing shard
writers. With
[`Slap.Cluster.Strategy.Local`](#placement-strategies), both clusters retry
and can keep fencing each other until one stops.

## Choosing the shard count

For a new, lightly loaded, single-node deployment on local storage, start with
one shard and measure your workload. Fewer active shards reduce database and
background-task overhead and can improve batching. Additional shards can help
read concurrency or CPU-intensive workloads.

With `{:local, directory}`, mutations across every database and file object under
that canonical store root share one OS lock, held through synchronization.
Adding shards under the same root does not add concurrent object publication.
See [local-store behavior](https://hexdocs.pm/slap_slatedb/readme.html#object-stores)
for filesystem requirements and upgrade precautions.

A store's shard count cannot change. A key's shard depends on the count, and
records are not moved when the count changes, so with a new count many records
would be looked up on the wrong shard and read as missing. To prevent this, a
cluster records its shard count in `<path>/_cluster/layout` when it first
starts, and refuses to start on that path with a different `:shards`. To change
the count, start a cluster on a new `:path` and copy the data into it.

A store created by `slap_cluster` 0.4.0 or earlier has no `_cluster/layout`. On
its first start, the cluster checks that the store's existing shard databases
match `:shards`. If they do, it records the count; otherwise it refuses to
start. An empty store is recorded as new.

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

Nodes connect through distributed Erlang. Any way of connecting them works,
such as [`libcluster`](https://hexdocs.pm/libcluster/), DNS-based discovery in
Kubernetes, your own `Node.connect/1` calls, or `Slap.Cluster.Peers`, which
keeps a node connected to a fixed list of node names.

`ObjectLease` stores conditional leases and node heartbeats in the bucket. An
owner renews its leases and stops a shard if renewal fails past its local
deadline, before another node can claim it. A paused owner that misses the
deadline is fenced by SlateDB when a new owner opens the database. A clean stop
closes shards before freeing their leases. Self-fencing runs separately from
the renewal loop so a blocked store call cannot delay it.

`call/4` routes to the owner over `:erpc`. If the reply is `:not_owner` or
`:unassigned`, or the owner cannot be reached before the call is sent,
`call/4` refreshes the placement and retries. A call that was already sent is
not retried, because it may have run; the caller receives
`{:error, {:erpc, :noconnection}}`. Lease data does not create node-name
atoms, so peers must be discovered or configured before routing to them.

## Placement strategies

- **`Local`** runs all shards on one node. The node must restart to recover
  from a failure.
- **`ObjectLease`** uses an object store with `If-Match` (S3, RustFS, GCS,
  Azure, or a local store shared on one host) and roughly synchronised clocks.
  Failover takes about one lease TTL. A node cut off from the store stops its
  shards before its leases expire.
- **`Distributed`** uses distributed Erlang without leases. A dead node's
  shards move after `:settle` (2 s); an unresponsive node also requires
  `net_ticktime`. A partition can let both sides open a shard. SlateDB
  fences one writer, but that side may serve stale reads until it finds that
  it has been fenced.
- **`Static`** uses a fixed list of nodes. A failed node must return before
  its shards are available again.

`ObjectLease` requires clocks within `:max_clock_skew_ms` (1 s by default).
Local stores provide `If-Match` for cooperating processes on one host; network
filesystem mounts remain unsupported. A process paused while holding the
local store's write lock can block writes and shard takeover until it resumes
or exits. Lower `net_ticktime` with
`Distributed` if failure detection must be faster.

## Tests

Follow the repository's
[development setup](https://github.com/mjrusso/slap#development), then run
from `slap_cluster/`:

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
