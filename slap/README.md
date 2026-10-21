# slap

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
> Complete distribution:
>
> - [slap](https://hexdocs.pm/slap/): all Slap libraries and a standalone HTTP
>   server for evaluating and benchmarking `slap_streams` and `slap_kv`
>
> The `slap` package pulls in all seven libraries. If you only need a
> subset, add those packages directly.
<!-- /slap-preamble -->

The `slap` package includes all seven `slap_*` packages, so you can use
every Slap library with a single dependency. If you only need a subset,
add those packages directly to keep your dependency set smaller.

The `slap` package also provides `mix slap.server`, a standalone HTTP server
for [Durable Streams](https://durablestreams.com)
([`slap_streams`](https://hexdocs.pm/slap_streams/)) and partitioned key-value
storage ([`slap_kv`](https://hexdocs.pm/slap_kv/)). It serves either or both
from one listener, and it does not authenticate requests. Use it to evaluate,
test, and benchmark those services without writing an application.

## Try the libraries

Requires Elixir 1.18 or later and Erlang/OTP 26.2 or later.

Start IEx and install the published `slap` package:

```sh
iex -e 'Mix.install([:slap])'
```

Run the examples below, in order, from this IEx session. These examples use
in-memory storage, which is discarded when the VM exits.

### SlateDB

Open a database, write a durable value, read it, and close the database:

```elixir
iex> {:ok, db} = Slap.SlateDB.open("example", store: :memory); :ok
:ok
iex> {:ok, _seq} = Slap.SlateDB.put(db, "user:1", "Ada", await_durable: true); :ok
:ok
iex> Slap.SlateDB.get(db, "user:1")
{:ok, "Ada"}
iex> Slap.SlateDB.close(db)
:ok
```

### Streams and snapshot logs

Start a Streams cluster, list its local shards through `Slap.Cluster`, then
create, append to, and read a stream:

```elixir
iex> {:ok, _} = Slap.Streams.Cluster.start_link(store: :memory, path: "streams", shards: 1); :ok
:ok
iex> Slap.Cluster.local_shards(Slap.Streams.Cluster)
[0]
iex> {:ok, :created, _} = Slap.Streams.create("/events", content_type: "text/plain"); :ok
:ok
iex> {:ok, %{result: :appended}} = Slap.Streams.append("/events", "created"); :ok
:ok
iex> {:ok, %{messages: messages}} = Slap.Streams.read("/events", 0); messages
[{0, "created"}]
```

Snapshot logs use the running Streams cluster:

```elixir
iex> base = "/v1/stream/examples/log"
"/v1/stream/examples/log"
iex> {:ok, _} = Slap.SnapshotLog.append(base, "one"); :ok
:ok
iex> {:ok, %{entries: ["one"], offset: offset}} = Slap.SnapshotLog.next(base, 0, wait: 0); :ok
:ok
iex> Slap.SnapshotLog.snapshot(base, offset, "state after one")
:ok
```

### Yjs

Start the document supervisor on the Streams cluster and edit a document with
`y_ex`:

```elixir
iex> defmodule Example.DocServer do
...>   use Slap.Yjs.DocServer
...> end; :ok
:ok
iex> {:ok, _} = Slap.Yjs.Docs.start_link(); :ok
:ok
iex> {:ok, server} = Slap.Yjs.Docs.join(Example.DocServer, {"examples", "doc-1"}); :ok
:ok
iex> {:ok, doc} = Slap.Yjs.DocServer.doc(server, 5_000); :ok
:ok
iex> text = Yex.Doc.get_text(doc, "text"); Yex.Text.insert(text, 0, "hello")
:ok
iex> :ok = Slap.Yjs.DocServer.sync(server, 5_000)
:ok
iex> Yex.Text.to_string(text)
"hello"
```

### KV and files

Start a KV cluster, then write and read a row:

```elixir
iex> {:ok, _} = Slap.KV.Cluster.start_link(store: :memory, path: "kv", shards: 1); :ok
:ok
iex> {:ok, version} = Slap.KV.put("users", "42", "Ada", if_version: :absent); :ok
:ok
iex> {:ok, %{value: value, version: ^version}} = Slap.KV.get("users", "42"); value
"Ada"
```

Files use the running KV cluster. Small bodies are stored in KV records:

```elixir
iex> {:ok, _} = Slap.Files.start_link(store: :memory, path: "files"); :ok
:ok
iex> ref = {"examples", "hello.txt"}
{"examples", "hello.txt"}
iex> {:ok, %{storage: :inline}} = Slap.Files.put(ref, "hello", if_version: :absent); :ok
:ok
iex> Slap.Files.read(ref)
{:ok, "hello"}
```

For production configuration and supervision, follow the package links in
[Use Slap in an application](#use-slap-in-an-application).

## Try the standalone server

You need Elixir 1.18 or later and Erlang/OTP 26.2 or later. This command
installs `slap` from Hex with `Mix.install` and starts the server:

```sh
elixir -e 'Mix.install([:slap]); Mix.Task.run("slap.server", System.argv())' -- \
  --streams --kv --store memory
```

`--streams` and `--kv` select the services; pass one or both.

The SlateDB NIF is precompiled for macOS and Linux (glibc and musl) on x86_64
and arm64. The first run downloads it and compiles the dependencies; later
runs reuse them.

The server listens on `http://127.0.0.1:4437`. It serves Streams under
`/v1/stream/` and KV under `/v1/kv/`, and `GET /health` answers 200 once it is
up. Use `--ip` to listen on another address, and `--port` for another port.

The memory store loses its data when the server stops. To use S3, create the
bucket, set `AWS_REGION` and credentials (such as `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`) in the server's environment, then run:

```sh
elixir -e 'Mix.install([:slap]); Mix.Task.run("slap.server", System.argv())' -- \
  --streams --kv --store s3:s3://my-bucket/my-app
```

`--store local:DIR` keeps the data in a local directory.

To keep an IEx prompt while the server runs:

```sh
iex -e 'Mix.install([:slap]); Mix.Task.run("slap.server", System.argv())' -- \
  --streams --kv --store memory
```

### Streams

With the server running, use another terminal for the HTTP examples below.
Create a JSON stream, then append to it. The JSON extension stores each element
of an appended array as a separate message.

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

### Options

Pass `--help` to the server command to list the options: the store, the
listener address, each service's shard count and flush interval, the long-poll
and SSE timeouts, a pid file, and the placement (`local`, `object-lease`,
`distributed` or `static`) for running as one node of a cluster.
`object-lease` requires an S3 store; `--peers` requires a placement other than
`local`.

Each service has its own shards: in a store, the streams' shards are under
`streams/` and KV's under `kv/`, each with its own leases when the placement
uses them. Run one server per VM; a cluster can have many shards and span
several VMs.

### Running from a Mix project

In a Mix project that depends on `slap`, run the task directly:

```sh
mix slap.server --streams --kv --store memory
```

The same server options apply. Run `mix help slap.server` for the full list,
or `iex -S mix slap.server --streams --kv --store memory` to keep an IEx
prompt while the server runs.

### Supervising the server

The Mix task is not available in a release. To run the server in a release,
or in a test or benchmark harness, add `slap` as a dependency and add
`{Slap.Server, opts}` as a child. It starts the selected services and a
Bandit listener together:

```elixir
children = [
  {Slap.Server,
    store: {:url, "s3://bucket/db"},
    streams: [shards: 8],
    kv: [shards: 4],
    port: 4437}
]
```

## Use Slap in an application

Requires Elixir 1.18 or later and Erlang/OTP 26.2 or later, because `slap`
includes `slap_yjs`.

Add `slap` to the dependencies in your application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap, "~> 0.4.0"}
  ]
end
```

Run `mix deps.get`.

`slap` makes every library below available. Applications that use only part
of Slap can depend on those packages directly. In either case, add the
required processes to the application's supervision tree:

| Package | Integration |
| --- | --- |
| [`slap_streams`](https://hexdocs.pm/slap_streams/) | Supervise `Slap.Streams.Cluster`; call `Slap.Streams` or serve `Slap.Streams.HTTP.Router` as a Plug. |
| [`slap_kv`](https://hexdocs.pm/slap_kv/) | Supervise `Slap.KV.Cluster`; call `Slap.KV` or serve `Slap.KV.HTTP.Router` as a Plug. |
| [`slap_files`](https://hexdocs.pm/slap_files/) | Supervise `Slap.KV.Cluster` before `Slap.Files`. |
| [`slap_yjs`](https://hexdocs.pm/slap_yjs/) | Supervise `Slap.Streams.Cluster` and `Slap.Yjs.Docs`. |
| [`slap_snapshot_log`](https://hexdocs.pm/slap_snapshot_log/) | Supervise `Slap.Streams.Cluster` before using the log. |
| [`slap_cluster`](https://hexdocs.pm/slap_cluster/) | Define shard children and supervise your module that uses `Slap.Cluster`. |
| [`slap_slatedb`](https://hexdocs.pm/slap_slatedb/) | Open a database and use `Slap.SlateDB`. |

These packages do not start an HTTP listener. The Streams and KV HTTP routers
are Plugs: mount them in your application's HTTP pipeline, or serve them with a
listener you supervise, and put your authentication and authorization in front
of them. Each package's README shows the children it needs and its in-process
API.

## Development and testing

To run against this checkout, follow the repository's
[development setup](https://github.com/mjrusso/slap#development), then run
commands from `slap/` unless a different directory is specified.
Use `iex -S mix` for the library examples and
`mix slap.server --streams --kv --store memory` for the standalone server.

This project also holds the tests that run the server as an OS process:
the official Durable Streams conformance suite and benchmarks, the kill -9
crash tests, the cluster tests and the streams HTTP load benchmark.

### Library examples

CI checks the IEx examples in the root README and this package's README
against this checkout, including their expected results. Each walkthrough
runs in a separate VM so its bindings and named services stay within that
session. From `slap/`:

```sh
mix run scripts/readme_test.exs ../README.md
mix run scripts/readme_test.exs README.md
```

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
results. These paths are relative to `slap/`. Run the benchmark recipes from
the repository root:

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
faster machine. The `slap_streams_bench.yml` workflow runs `slap-local`,
`slap-s3` (on RustFS) and `caddy-file`; a run on main also charts the results.

### Without a Mix project

`scripts/mix_install_test.sh` runs the [`Mix.install`
command](#try-the-standalone-server) against this checkout. CI runs it.

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
