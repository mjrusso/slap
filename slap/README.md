# slap

`slap` provides a Mix-run HTTP server for evaluating, testing, and benchmarking
[Durable Streams](https://github.com/durable-streams/durable-streams) and KV.
It serves either or both services from one listener. It does not serve Files or
Yjs, and it does not authenticate requests.

Applications normally depend on the embeddable service packages instead, so
that they own their supervision tree and HTTP routing; see below. Adding `slap`
as a dependency does not start a listener: run `mix slap.server`, or supervise
`{Slap.Server, opts}`. The Mix task is not available in a release, so a release
that needs this listener supervises `{Slap.Server, opts}`.

The supported setup has one `Slap.Streams.Cluster` and one `Slap.KV.Cluster`
per VM. A cluster can have many shards and span several VMs.

## Embed a service in your application

Choose the package for the service your application needs:

| Need | Package | Integration |
| --- | --- | --- |
| Direct key-value access to SlateDB | [`slap_slatedb`](https://hexdocs.pm/slap_slatedb/) | Open a database and use `Slap.SlateDB`. |
| Custom sharded storage | [`slap_cluster`](https://hexdocs.pm/slap_cluster/) | Define shard children and supervise your module that uses `Slap.Cluster`. |
| Durable Streams | [`slap_streams`](https://hexdocs.pm/slap_streams/) | Supervise `Slap.Streams.Cluster`; call `Slap.Streams` or serve `Slap.Streams.HTTP.Router` as a Plug. |
| Partitioned KV | [`slap_kv`](https://hexdocs.pm/slap_kv/) | Supervise `Slap.KV.Cluster`; call `Slap.KV` or serve `Slap.KV.HTTP.Router` as a Plug. |
| Files | [`slap_files`](https://hexdocs.pm/slap_files/) | Supervise `Slap.KV.Cluster` before `Slap.Files`. |
| Yjs documents | [`slap_yjs`](https://hexdocs.pm/slap_yjs/) | Supervise `Slap.Streams.Cluster` and `Slap.Yjs.Docs`. |
| Snapshot logs | [`slap_snapshot_log`](https://hexdocs.pm/slap_snapshot_log/) | Supervise `Slap.Streams.Cluster` before using the log. |

The embeddable packages do not start an HTTP listener. The Streams and KV
HTTP routers are Plugs that can be mounted in your application's HTTP
pipeline or served by a listener you supervise. Put your authentication and
authorization in front of them. The linked package READMEs show the required
children and in-process APIs.

## Install the Mix-run server

If another Mix project needs this server, add `slap` to its
`mix.exs`:

```elixir
defp deps do
  [
    {:slap, "~> 0.1.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap/).

## Try the standalone server

With Elixir 1.18 or later, from `slap/` in a checkout of the
[repository](https://github.com/mjrusso/slap), run `mix deps.get`, then one of
these commands. Without `SLAP_LOCAL_DEPS=1`, `mix deps.get` uses the packages
published on Hex and records them in `slap/mix.lock`; run
`git restore mix.lock` afterwards to undo that.

```sh
mix slap.server --streams --store memory
mix slap.server --kv --store memory
mix slap.server --streams --kv --store memory
```

Use `iex -S mix slap.server --streams --store memory` to keep an IEx prompt
while the server runs.

The listener binds to `127.0.0.1` by default. Use `--ip` to listen on another
address. The memory store loses its data when the server stops. To try S3,
create the bucket, configure `AWS_REGION` and credentials in the server's
environment, then run:

```sh
mix slap.server --streams --kv --store s3:s3://my-bucket/my-app
```

### Streams

With Streams running, create a JSON stream, then append to it. The JSON
extension stores each element of an appended array as a separate message.

```sh
curl -X PUT -H 'Content-Type: application/json' http://localhost:4437/v1/stream/events
curl -X POST -H 'Content-Type: application/json' \
  --data '[{"type":"created","id":1},{"type":"updated","id":1}]' \
  http://localhost:4437/v1/stream/events
```

Read the stream. The JSON messages are returned as an array.

```sh
curl 'http://localhost:4437/v1/stream/events?offset=-1'
```

Fork the stream at its current end, then append to the fork. The source still
contains only the first two messages.

```sh
curl -X PUT -H 'Content-Type: application/json' \
  -H 'Stream-Forked-From: /v1/stream/events' \
  http://localhost:4437/v1/stream/events-fork
curl -X POST -H 'Content-Type: application/json' --data '{"type":"deleted","id":1}' \
  http://localhost:4437/v1/stream/events-fork
curl 'http://localhost:4437/v1/stream/events-fork?offset=-1'
curl 'http://localhost:4437/v1/stream/events?offset=-1'
```

### KV

With KV running, set three rows, get one, then scan a range of keys in the
`people` partition. The range stops before `c`.

```sh
curl -X PUT --data-binary 'Ada' http://localhost:4437/v1/kv/people/a
curl -X PUT --data-binary 'Grace' http://localhost:4437/v1/kv/people/b
curl -X PUT --data-binary 'Katherine' http://localhost:4437/v1/kv/people/c
curl http://localhost:4437/v1/kv/people/a
curl 'http://localhost:4437/v1/kv/people?gte=a&lt=c'
```

The scan returns JSON with keys and values encoded as unpadded base64url.

`mix help slap.server` lists the options: the store, the listener address,
each service's shard count and flush interval, the long-poll and SSE timeouts,
a pid file, and the placement (`local`, `object-lease`, `distributed` or
`static`) for running as one node of a cluster.

`object-lease` requires an S3 store; `--peers` requires a placement other
than `local`.

In a store, the streams' shards are under `streams/` and KV's under `kv/`,
each with its own leases when the placement uses them.

To supervise the standalone listener in a test or benchmark harness, add
`{Slap.Server, opts}` as a child. It starts the selected clusters and a Bandit
listener together:

```elixir
children = [
  {Slap.Server,
    store: {:url, "s3://bucket/db"},
    streams: [shards: 8],
    kv: [shards: 4],
    port: 4437}
]
```

## Development and testing

This project also holds the tests that run the server as an OS process:
the official Durable Streams conformance suite and benchmarks, the kill -9
crash tests, the cluster tests and the streams HTTP load benchmark.

### Durable Streams conformance and benchmarks

`durable_streams/` runs version 0.3.7 of
[`@durable-streams/server-conformance-tests`](https://www.npmjs.com/package/@durable-streams/server-conformance-tests)
against a running server:

```sh
mix slap.server --streams --store memory --long-poll-timeout 500 &    # the reference servers use 500 ms for this suite
cd durable_streams && pnpm install && pnpm test
```

The official suite covers stream operations, including forks, on the
in-memory and RustFS stores in CI. The suite skips subscriptions, which this
server does not provide.

`pnpm bench` in `durable_streams/` runs the official HTTP benchmarks,
[`@durable-streams/benchmarks`](https://www.npmjs.com/package/@durable-streams/benchmarks)
0.2.7: append-to-long-poll round-trip latency, and append throughput for
100-byte and 1 MB messages. Both suites read the server's URL from
`DURABLE_STREAMS_URL` (default `http://127.0.0.1:4437`).

`scripts/streams_bench.sh` runs the benchmarks against `mix slap.server` and
against the official Caddy server (a pinned release, downloaded and
checked), one after another, and `durable_streams/summary.js` compares the
results. From the repository root:

```sh
just streams-bench                                  # slap-local caddy-file
just streams-bench slap-memory caddy-memory slap-local caddy-file
```

`slap-local` and `caddy-file` both reply to an append once it is on disk, so
they are the comparable pair. `slap-memory` still waits for a SlateDB flush,
which `caddy-memory` does not. The memory store keeps the objects SlateDB writes
in RAM, about twice the bytes appended, and deleting the streams does not free
it within minutes. The 1 MB benchmark appends a fixed time's worth of data, 1.5
GB in a typical run, so `slap-memory` needs about 4 GB of free memory, more on a
faster machine. When started from the Actions tab, the `slap_streams_bench.yml`
workflow runs `slap-local`, `slap-s3` (on RustFS) and `caddy-file`; a run on
main also charts the results.

### Crash test

`scripts/crash_test.exs` runs `mix slap.server` as its own OS process. It:

1. has 16 writers append to their own JSON streams (half with idempotent
   producers)
2. kills the server with `kill -9` at a random moment between 0.3 and 3 s
3. restarts the server on the same store
4. lets the producers retry the appends that were in flight, and all writers
   append more
5. reads every stream back

Every acknowledged append must be present exactly once, at the offset its
acknowledgement gave, so no offset is reused after the crash. Each writer's
messages must be in order, with no message stored twice.

```sh
mix run scripts/crash_test.exs --runs 20                         # local directory
mix run scripts/crash_test.exs --runs 20 --store s3:s3://bucket/crash
```

CI runs the crash test on local storage and RustFS. An append in flight
when the process dies may or may not be stored.

### Streams load

`bench/streams_http_load.exs` measures stream append throughput and latency
against a running server. Choose shard count and `flush_interval` from the
store's sustained PUT rate: each active shard can write one WAL object per
flush interval. Run the benchmark on the intended store before sizing a
deployment.

### Cluster tests

`scripts/cluster.sh start DIR` runs a local cluster of three `mix
slap.server` nodes (separate OS processes, one store, the `distributed`
placement). The Durable Streams conformance suite runs through one of them,
so most requests, long-polls and SSE streams are served by another node.

`scripts/cluster_crash_test.exs` runs 16 writers (half with idempotent
producers) through the nodes of a 3-node cluster and injects a fault into
one node at a random moment, then checks every stream as the single-node
crash test does: every acknowledged append exactly once at its
acknowledged offset, no duplicates, each writer's messages in order.

- **kill** - SIGKILL the node; the others take its shards (when its
  leases expire, or at once with `distributed`); it is restarted and takes
  its share back.
- **pause** - SIGSTOP the node past its leases (or `net_ticktime`): the
  others take its shards, fencing it in SlateDB; on SIGCONT it finds it
  lost them and gives them up.

The test measures how long each shard is unavailable after a kill. With
`distributed` placement, failover waits for node failure detection and the
settle interval. With `object-lease`, it waits for the old lease to expire
and for the shard to reopen. CI runs the Durable Streams conformance suite
through the cluster and injects both faults under each placement.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
