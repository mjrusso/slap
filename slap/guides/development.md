# Development and testing

These commands require a repository checkout. Follow the repository's
[development setup](https://github.com/mjrusso/slap#development), then run
commands from `slap/` unless a different directory is specified.

## Library examples

Use `iex -S mix` to try the [library examples](library-examples.md) against
this checkout. To check their expected results, as CI does, run:

```sh
mix run scripts/readme_test.exs ../README.md
mix run scripts/readme_test.exs guides/library-examples.md
```

## Durable Streams conformance and benchmarks

### Conformance

From the repository root, run:

```sh
just streams-conformance
```

The recipe starts a memory server, waits for its HTTP listener, runs the
[official conformance suite](https://www.npmjs.com/package/@durable-streams/server-conformance-tests),
and stops the server afterward. CI runs the suite on memory and RustFS
stores. It covers stream operations, including forks, and skips subscriptions,
which this server does not provide.

### HTTP benchmarks

The [official HTTP benchmarks](https://www.npmjs.com/package/@durable-streams/benchmarks)
measure append-to-long-poll round-trip latency and append throughput for
100-byte and 1 MB messages. Run them against Slap and the official Caddy
server from the repository root:

```sh
just streams-bench
```

This compares `slap-local` and `caddy-file`. To include memory stores:

```sh
just streams-bench slap-memory caddy-memory slap-local caddy-file
```

`scripts/streams_bench.sh` runs each server in turn and
`durable_streams/summary.js` compares the results. The Caddy binary is pinned
and checksum-verified. Both the conformance and benchmark runners use
`DURABLE_STREAMS_URL` (default `http://127.0.0.1:4437`) when run directly
from `durable_streams/` with `pnpm test` or `pnpm bench`.

`slap-local` and `caddy-file` acknowledge appends after disk persistence.
`slap-memory` waits for a SlateDB flush; `caddy-memory` does not.

`slap-memory` retains SlateDB objects in RAM, and deleting streams does
not immediately release that memory. Allow for all data appended during the
run plus SlateDB overhead. The throughput benchmark runs for a fixed duration,
so faster machines need more memory.

The `slap_streams_bench.yml` workflow compares `slap-local`, `slap-s3` on
RustFS, and `caddy-file`, and charts the results of runs on main.

## Mix.install startup

Test the standalone server's [`Mix.install` command](standalone-server.md)
against this checkout:

```sh
scripts/mix_install_test.sh
```

## Crash test

`scripts/crash_test.exs` starts `mix slap.server` as its own OS process. It:

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

Run on local storage or an S3 prefix:

```sh
mix run scripts/crash_test.exs --runs 20
mix run scripts/crash_test.exs --runs 20 --store s3:s3://bucket/crash
```

CI runs the crash test on local storage and RustFS. An append in flight
when the process dies may or may not be stored.

## Streams load

`bench/streams_http_load.exs` measures append throughput and latency. Start a
Streams server in one terminal:

```sh
mix slap.server --streams --store memory
```

Run the load generator in another terminal:

```sh
mix run bench/streams_http_load.exs --connections 128 --seconds 20
```

Choose shard count and `flush_interval` from the store's sustained PUT rate:
each active shard can write one WAL object per flush interval. Run the
benchmark on the intended store before sizing a deployment.

## Cluster tests

### Conformance through a cluster

`scripts/cluster.sh` starts three server processes sharing a local store,
using `distributed` placement. Run the conformance suite through node 2 on
port 4442, then stop the nodes:

```sh
(
  set -e
  cluster_dir=$(mktemp -d)
  trap 'scripts/cluster.sh stop "$cluster_dir"' EXIT
  scripts/cluster.sh start "$cluster_dir"
  (
    cd durable_streams
    pnpm install --frozen-lockfile
    DURABLE_STREAMS_URL=http://127.0.0.1:4442 pnpm test
  )
)
```

Requests, long-polls and SSE streams for shards owned by another node are
routed to that node.

### Faults under load

`scripts/cluster_crash_test.exs` starts its own three-node cluster, runs 16
writers (half with idempotent producers), and checks the same data guarantees
as the [single-node crash test](#crash-test):

```sh
mix run scripts/cluster_crash_test.exs --runs 6 --placement distributed
```

The test alternates between two faults:

- **kill**: SIGKILL a node, wait for failover, then restart it.
- **pause**: SIGSTOP a node until the others take its shards, then resume it.
  SlateDB fences the old writer.

The test measures how long each shard is unavailable after a kill. With
`distributed` placement, failover waits for node failure detection and the
settle interval. With `object-lease`, it waits for the old lease to expire
and for the shard to reopen. CI runs cluster conformance with `distributed`
placement and tests both faults with `distributed` and `object-lease`.
