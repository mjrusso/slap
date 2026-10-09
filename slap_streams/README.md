# slap_streams

`slap_streams` is a server for [Durable Streams](https://durablestreams.com),
an HTTP protocol for append-only streams of messages. Clients read a stream
from an offset, and resume from their last offset after a disconnect or
restart.

`slap_streams` stores streams in SlateDB, and acknowledges an append only once
it is durably stored in object storage. It passes the official Durable Streams
conformance suite, and is built on top of
[`slap_cluster`](https://hexdocs.pm/slap_cluster/) and
[`slap_slatedb`](https://hexdocs.pm/slap_slatedb/).

Use `slap_streams` to deliver a sequence of messages to clients. For example,
an application can append a document's changes to a stream, and each client
keeps reading from its last offset. The package handles durable appends, reads
by offset, waits for new messages, producer deduplication, stream expiry, and
forks. It does not define your message format or apply messages to your
application's state.

`slap_streams` runs inside your application, on one node or several. You start
`Slap.Streams.Cluster`, call `Slap.Streams` in-process, and can mount
`Slap.Streams.HTTP.Router` in your [`Plug`](https://hexdocs.pm/plug/) pipeline.
Streams are divided among SlateDB databases called shards, and a call for a
stream goes to the node that owns its shard.

The [`slap`](https://hexdocs.pm/slap/) package runs `slap_streams` as a
standalone server (`mix slap.server --streams --store memory`).

Section numbers (§) in the API documentation refer to the [Durable Streams
specification](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md).

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

Add `slap_streams` to the dependencies in your application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_streams, "~> 0.2.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_streams/).

## Example

In an application that depends on `slap_streams`, run this in `iex -S mix`
with a fresh local directory. The in-process API is `Slap.Streams`:

```elixir
iex> {:ok, _} = Slap.Streams.Cluster.start_link(store: {:local, "/tmp/slap-streams"}, shards: 8); :ok
:ok

iex> {:ok, :created, %{next_offset: 0}} = Slap.Streams.create("/chat/1", content_type: "application/json"); :created
:created
iex> {:ok, %{result: :appended, next_offset: 12}} =
...>   Slap.Streams.append("/chat/1", ~s([{"m": 1}]), producer: {"client-a", 0, 0}); {:appended, 12}
{:appended, 12}
iex> {:ok, %{messages: messages, up_to_date: true}} = Slap.Streams.read("/chat/1", 0); messages
[{0, "{\"m\": 1}"}]
```

The functions return protocol results, not HTTP responses; `Slap.Streams`
documents the HTTP status for each result. The next offset is 12 because the
JSON element occupies eight bytes and each message adds four bytes of
overhead. Offsets in `Slap.Streams` are these integers; over HTTP they use
the official wire format, and `Slap.Streams.Offset` converts between the two.

## Embedding

Add `Bandit` to the host application's dependencies. Then add these children
to its supervision tree, with the cluster first:

```elixir
children = [
  {Slap.Streams.Cluster, store: {:local, "/tmp/slap-streams"}, shards: 8},
  {Bandit, plug: Slap.Streams.HTTP.Router, port: 4437}
]
```

Bandit serves the router at `/v1/stream/`. A `{:local, dir}` store is for
development, because it never deletes old manifests (see
[Storage after deletes](#storage-after-deletes)); [Using S3](#using-s3)
describes the alternative.

To use an application-owned cluster module:

```elixir
defmodule MyApp.StreamsCluster do
  use Slap.Streams.Cluster, otp_app: :my_app
end

children = [{MyApp.StreamsCluster, store: {:local, "/tmp/slap-streams"}, shards: 8}]
{:ok, :created, _} = Slap.Streams.create("/notes", cluster: MyApp.StreamsCluster)
```

Pass `cluster: MyApp.StreamsCluster` to the router and to any `Slap.Yjs.Docs`
instance using it. Run one Streams cluster per VM: metrics and the HTTP
request-body budget are shared across the VM. A KV cluster can run on the
same VM.

### Mounting the router in an application

A stream's name is its full request path, including any path the router is
mounted under. Served at the root, the stream at `/v1/stream/chat/1` is
`Slap.Streams.head("/v1/stream/chat/1")` in-process. Mounted under
`/streams`, the same stream is at `/streams/v1/stream/chat/1`, and that is
its name.

The router reads the request body itself, so it must run before
`Plug.Parsers`. After it, a request whose content type `Plug.Parsers` parses
reaches the router with its body already read. With Phoenix's default parsers
(JSON, URL-encoded and multipart), an append to a JSON stream then gets 400
("empty body not allowed"). A Phoenix router runs after the endpoint's
`Plug.Parsers`, so mount the Streams router in the endpoint, with a function
plug placed before `plug Plug.Parsers`:

```elixir
# In MyAppWeb.Endpoint, before `plug Plug.Parsers`:
plug :streams

@streams Slap.Streams.HTTP.Router.init([])

defp streams(%Plug.Conn{path_info: ["streams" | rest]} = conn, _opts),
  do: conn |> Plug.forward(rest, Slap.Streams.HTTP.Router, @streams) |> halt()

defp streams(conn, _opts), do: conn
```

In a `Plug.Router` that has no `Plug.Parsers` before it,
`forward "/streams", to: Slap.Streams.HTTP.Router` does the same.

## Using S3

Create the bucket first and configure `AWS_REGION` and credentials in the
environment used to start the application, for example with
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Then, in `iex -S mix` in an
application that depends on `slap_streams`, start a cluster on a new prefix in
that bucket:

```elixir
iex> {:ok, _} = Slap.Streams.Cluster.start_link(
...>   store: {:url, "s3://my-bucket/my-app/streams"},
...>   shards: 8
...> ); :ok
:ok

iex> {:ok, :created, _} = Slap.Streams.create("/notes", content_type: "text/plain"); :created
:created
iex> {:ok, %{result: :appended}} = Slap.Streams.append("/notes", "hello"); :appended
:appended
iex> {:ok, %{messages: messages}} = Slap.Streams.read("/notes", 0); messages
[{0, "hello"}]
```

Use the same store URL and shard count when restarting the application. In a
supervised application, put `Slap.Streams.Cluster` in the supervision tree as
shown above. The example uses the default local placement on one node; see
[Several nodes](#several-nodes) to run on several nodes.

## How it works

- **Placement.** A stream lives on shard `xxh64(key) mod N`, where `key` is its
  placement key (see [Several nodes](#several-nodes)) and `N` is the shard
  count. Each shard runs one process per active stream, its stream server. A
  stream server starts on demand and stops after 5 idle minutes.
- **Writes.** A stream server validates each write against the Durable Streams
  protocol and assigns offsets. It writes the messages, the new tail, producer
  state, and any metadata change in **one** `Slap.SlateDB.write/3` batch. It
  does not wait for that write to become durable before handling the next
  request, so several writes can be in flight at once.
- **Acknowledgements.** The server replies once SlateDB reports the write
  durable. Replies go out in request order. A reply that does not need a write
  (an error, a duplicate, or "already exists") still waits behind the writes in
  flight, so no reply depends on state that could still be lost.
- **Reads.** The server keeps a **durable view** that advances only as writes
  become durable. Reads, `head`, long-polls, and SSE see only that view, so
  they never return unacknowledged data. The read itself runs in the caller's
  process.
- **Failures.** A failed write fails that request and every request in flight,
  and the stream server stops. The next request reloads the stream from
  storage. When a shard stops or is fenced, its stream servers stop first and
  fail requests in flight with `:unavailable`.

## HTTP

`Slap.Streams.HTTP.Router` is a `Plug` over `Slap.Streams`. Streams live under
`/v1/stream/`, and a stream's name is the request path. By default the router
matches the official Go server: the same headers, status codes, cursors, CORS
headers, and security headers. For HTTP request examples, see
[`slap`](https://hexdocs.pm/slap/).

- **Reads** return up to about 1 MiB. `Stream-Up-To-Date` is set on a read
  that reached the tail, and `Stream-Closed` is set only on the read that
  returns a closed stream's final data. The `ETag` is `"sid:start:end"` (with
  `:c` when closed), and a request with a matching `If-None-Match` gets 304.
- **Caching.** Historical reads are cacheable
  (`public, max-age=60, stale-while-revalidate=300`). Reads at `offset=now`
  and HEAD requests are `no-store`.
- **Long-poll.** A long-poll request waits for new data for up to 30 s, then
  gets 204. The stream server notifies it of new data, so nothing polls.
- **SSE** sends each `data` event and its `control` event in one chunk.
  Streams that are not text or JSON are sent as base64. The response ends
  after 60 s, or at the end of a closed stream.
- **TTL and Expires-At.** Reads, waits, and writes reset a sliding TTL; HEAD
  does not. [Lifecycle](#lifecycle) describes when expired streams are
  deleted.
- **Forks** are created with the `Stream-Forked-From`, `Stream-Fork-Offset`,
  and `Stream-Fork-Sub-Offset` headers, which are parsed as the Go server
  parses them.

### Router options

| Option | Default | Effect |
| --- | --- | --- |
| `:long_poll_timeout` | 30,000 ms | How long a long-poll waits for data. |
| `:sse_timeout` | 60,000 ms | How long an SSE response stays open. |
| `:max_read` | 1 MiB | About how many bytes one read returns. |
| `:max_body` | 64 MiB | The largest request body. A larger `Content-Length` gets 413 before the body is read. |
| `:max_path` | 512 bytes | The longest stream path (414 above it). |
| `:max_buffered` | 512 MiB | Request-body bytes the VM holds at once (503 above it). |
| `:trust_forwarded` | `false` | Build a created stream's `Location` from `X-Forwarded-Proto` and `X-Forwarded-Host`. |
| `:private_cache` | `false` | Send `Cache-Control: private` instead of `public`. |
| `:cors` | `"*"` | The allowed origin, or `false` to omit CORS headers. |

### Authentication

The router does not authenticate requests or limit their rate. Put both in
your application's pipeline or in a proxy in front of it. Two things to cover:

- A path-based access check must also check a `PUT`'s `Stream-Forked-From`
  header. Creating a fork copies the source stream, so the request reads the
  source's path.
- Set `:private_cache`, so that a CDN does not serve one user's historical
  reads to another.

### Backpressure

An append gets 503 with `Retry-After: 1` if it would take a stream's bytes in
flight (written but not yet durable) over 64 MiB, or its shard's over 256 MiB.
A stream with nothing in flight always accepts one request. Set the limits with
the cluster `child_options` keys `:max_inflight_bytes_per_stream` and
`:max_inflight_bytes_per_shard`.

### Metrics

`Slap.Streams.Telemetry` emits telemetry events for:

- append latency, from accepting the request to the durable reply
- rejected appends and write failures
- HTTP requests
- each shard's load, every 5 s: stream servers, bytes and requests in flight,
  waiters, pending deletions and expiries, and SlateDB's L0 SSTs, sorted
  runs, and block cache hits

`Slap.Streams.Metrics` keeps these, together with `slap_cluster`'s
durability lag and shard events, as Prometheus metrics; its documentation
lists them. `Slap.Streams.HTTP.Metrics` is a `Plug` that serves them in the
Prometheus text format. Mount it where Prometheus can reach it and the public
cannot.

## Lifecycle

- **Forks are copies.** When a fork is created, the source's stream server
  checks the request (content type, offset, sub-offset, and size) and records
  the fork. The source's messages below the fork offset are then copied to the
  fork, keeping their offsets, followed by the fork's own first messages.
  Requests to the fork get 503 until the copy finishes; retrying the create
  finishes an interrupted copy. A fork whose copy would exceed
  `:max_fork_copy_bytes` (cluster `child_options`, 64 MiB by default) gets 413.
  A fork inherits the source's TTL or Expires-At unless it sets its own.
- **Deletes.** Deleting a stream that has forks soft-deletes it. From then on,
  requests to it get 410, and creating a stream at its path or a fork from it
  gets 409. Its
  data is deleted at once, because each fork has its own copy. A stream
  without forks is removed completely. If it was a fork, it is removed from
  its source's list of forks, and a soft-deleted source with no forks left is
  removed too.
- **Expiry.** Every 10 s (`:expiry_interval`), a background job finds the
  streams whose deadline has passed and has each one's stream server check
  it; a stream that has expired is deleted. Every request to a stream also
  checks its expiry. For a sliding TTL, the stream's last access is stored at
  most once per tenth of the TTL.
- **Repair.** Two states could otherwise stay forever: a fork whose copy was
  interrupted and never retried, and a soft-deleted source that still lists a
  fork that no longer exists. A background job checks for both every minute
  (`:repair_interval`). It removes a fork still copying after an hour
  (`:fork_copy_grace`); the copy cannot be finished, because the fork's
  initial body was only in the request. It removes stale forks from a
  soft-deleted source's list, which lets the source be removed.
- **Deleting messages.** Deleting a stream first writes only a deletion
  marker. A background job then deletes the messages in pages of 10,000,
  recording its progress with each page so that it resumes after a crash.
- **Trim.** `Slap.Streams.trim(path, offset)` deletes the messages before
  `offset`. Reads from an earlier offset get 410 (`:trimmed`), and
  `offset=-1` starts at the earliest message left. The protocol has no request
  for this, so it is available only in-process.
- **List.** `Slap.Streams.list(prefix)` returns the paths of the streams that
  start with `prefix` in `prefix`'s placement group, such as a Yjs document's
  `.snapshots/`. It is available only in-process.
- **Seal.** `Slap.Streams.seal(group)` permanently prevents new streams in a
  placement group. Once `seal/2` returns, creating a stream or a fork in the
  group gets `{:error, :sealed}` (409), and every stream created before is
  durable and listed. [`slap_snapshot_log`](https://hexdocs.pm/slap_snapshot_log/) seals
  a deleted log's group, so that a snapshot write that started before the
  delete cannot land after it. It is available only in-process.

## Storage after deletes

Deleting rows writes tombstones. SlateDB reclaims their space only after
compaction and garbage collection; old sorted runs and checkpoints can delay
reclamation. The local file store does not collect old manifests, so use an
object store when reclaiming deleted data matters. `bench/storage_soak.exs`
measures this behavior.

## Several nodes

With a placement strategy, `slap_cluster` spreads the shards over several
nodes and moves them when nodes join, leave, or fail. Connect the nodes with
distributed Erlang (for example with
[`libcluster`](https://hexdocs.pm/libcluster/)). Any node can serve any
request:

```elixir
{Slap.Streams.Cluster, store: {:url, "s3://streams/ds"}, shards: 64,
             strategy: {Slap.Cluster.Strategy.ObjectLease, lease_ttl: 15_000}}
```

- **Routing.** `Slap.Streams` runs each operation on the shard's owner with
  `Slap.Cluster.call/4`. If the owner cannot be reached, or does not answer
  within the request's timeout plus 5 s, the request gets 503, and it may or
  may not have been applied. Retries from idempotent producers are
  deduplicated. A call is retried on another node only if it was never sent.
- **Long-poll and SSE.** The HTTP process on the node that received the
  request registers with the owner's stream server, which notifies it of new
  data; the process then reads from the owner. If the owner's stream server
  or its node dies, the request ends with 503 instead of waiting for its
  timeout.
- **Forks** use the same routing, so a fork and its source can be on different
  shards and nodes.
- **Placement key.** A stream's placement key is its path up to the first
  segment that starts with `.` (`Slap.Streams.placement_key/1`). So a Yjs
  document's `.updates`, `.index`, and `.snapshots/...` streams
  ([`slap_yjs`](https://hexdocs.pm/slap_yjs/)) are on one shard. The
  `:placement_key` option of `Slap.Streams` overrides it.

### Placement strategies and consistency

Any of `slap_cluster`'s strategies works (`--placement` for
`mix slap.server`):

- **`ObjectLease`** stores leases as objects in the streams' bucket, so it
  needs S3 or another store with `If-Match`. An owner stops its shards before
  its lease expires, and the next owner claims them only after, so reads and
  writes are linearizable.
- **`Distributed`** uses no leases: every node computes the placement from the
  connected nodes. Lower `net_ticktime` for faster failover of a node that
  stops answering.
- **`Static`** uses a fixed list of nodes; **`Local`** runs on one node.

Without leases, reads are not linearizable. A node that was paused, or that
disagrees with others about which nodes are connected, may serve reads from
its old state until it finds that SlateDB has fenced it (about a second).
Its writes fail with 503, because SlateDB rejects writes from a fenced
writer. Before it reports a producer sequence gap, it confirms its ownership
with a write, so that reply also gets 503. The Jepsen suite checks both kinds
of placement.

## Storage

Offsets use the official wire format (`%016d_%016d`) and advance by four
bytes plus the body length per message. An append stores its messages, tail,
producer state, and metadata changes in one SlateDB batch. JSON mode keeps
each array element's original bytes.

## Tests

```sh
mix test                          # SLAP_STREAMS_PROP_RUNS=1000 for more model sequences
```

The model test covers stream operations, crashes, and clock changes against a
pure protocol model. Set `SLAP_STREAMS_PROP_RUNS` to run more sequences;
`SLAP_STREAMS_PROP_STATS=1` prints outcome counts.

The tests that need the server as an OS process live in
[`slap`](https://hexdocs.pm/slap/readme.html#development-and-testing): the
official [Durable Streams conformance suite and
benchmarks](https://hexdocs.pm/slap/readme.html#durable-streams-conformance-and-benchmarks),
the crash tests and the cluster tests.

## Benchmarks

```sh
mix run bench/append_latency.exs
mix run bench/read_scaling.exs
```

These measure the library in-process. `slap` runs the official [Durable
Streams HTTP
benchmarks](https://hexdocs.pm/slap/readme.html#durable-streams-conformance-and-benchmarks)
and an [HTTP append load test](https://hexdocs.pm/slap/readme.html#streams-load)
against a running server.

`bench/append_latency.exs` measures durable append latency on the in-memory
store. Set `SLAP_BENCH_FLUSH_INTERVAL` to change SlateDB's flush interval.

`bench/read_scaling.exs` times first server reads, warm random reads, and narrow
and broad lists as stream count grows. It also times reads at the start,
middle, and tail of a growing stream. It uses one shard, flushes data before
timing, and reports p50, p99, and maximum latency. The first server reads
include process startup and state loading; the other operations use warmed
paths. The default store is a temporary local directory, removed after the
run. Use a fresh object-store prefix to measure that backend:

```sh
mix run bench/read_scaling.exs -- --streams 1000 --messages 1024 --samples 200 \
  --cache disabled --store s3:s3://bucket/read-bench
```

Increase `--streams`, `--messages`, or `--message-bytes` to test larger stores.
`--cache disabled` turns off SlateDB's in-memory cache; operating-system and
object-store caches may still affect results. `--page-bytes` and
`--flush-interval` control read size and SlateDB's flush interval.
`--store memory` avoids local and object-store I/O. Object-store data remains
after the run and must be removed separately.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
