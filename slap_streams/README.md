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
> Server distribution:
>
> - [slap](https://hexdocs.pm/slap/): all Slap server libraries and a standalone
>   HTTP server for evaluating and benchmarking `slap_streams` and `slap_kv`
>
> Inspection:
>
> - [slap_inspect](https://hexdocs.pm/slap_inspect/): a read-only TUI and CLI
>   for browsing Slap data
>
> The `slap` package pulls in all seven server libraries. If you only need a
> subset, add those packages directly.
<!-- /slap-preamble -->

## Installation

Requires Elixir 1.18 or later.

To depend on `slap_streams` directly, add it to the dependencies in your
application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_streams, "~> 0.5.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_streams/).

## Example

In an application that depends on `slap_streams`, run this in `iex -S mix`
with a fresh local directory. The in-process API is `Slap.Streams`:

```elixir
iex> {:ok, _} = Slap.Streams.Cluster.start_link(store: {:local, "/tmp/slap-streams"}, shards: 1); :ok
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
  {Slap.Streams.Cluster, store: {:local, "/tmp/slap-streams"}, shards: 1},
  {Bandit, plug: Slap.Streams.HTTP.Router, port: 4437}
]
```

Bandit serves the router at `/v1/stream/`. For local storage, see
[choosing the shard count](https://hexdocs.pm/slap_cluster/readme.html#choosing-the-shard-count);
before running a script against a store an application is using, see
[running another cluster on the same store](https://hexdocs.pm/slap_cluster/readme.html#running-another-cluster-on-the-same-store).
[Using S3](#using-s3) describes remote storage.

To use an application-owned cluster module:

```elixir
defmodule MyApp.StreamsCluster do
  use Slap.Streams.Cluster, otp_app: :my_app
end

children = [{MyApp.StreamsCluster, store: {:local, "/tmp/slap-streams"}, shards: 1}]
```

Add these children to your supervision tree. Once it is running, pass the
cluster module to each operation:

```elixir
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
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. In an application that depends
on `slap_streams`, start a fresh `iex -S mix` session and open a cluster on a
new prefix in that bucket:

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

A failed store probe normally prevents the cluster and its supervising
application from starting. With `probe: :background`, the cluster starts
without shards and retries the probe. Operations return
`{:error, :unavailable}` until the shards open.

Before boot work that needs every shard, call `Slap.Streams.Cluster.await_ready(30_000)`
and handle `{:error, :timeout}`. `Slap.Streams.Cluster.status()` reports current
readiness and the latest failures without accessing storage; see
[cluster readiness](https://hexdocs.pm/slap_cluster/Slap.Cluster.html#status/2).

## Flush interval

A durable append waits for SlateDB to flush its write-ahead log (WAL) to
object storage. `Slap.Streams.Cluster` sets `flush_interval` to 10 ms;
SlateDB's default is 100 ms. Pass `settings: %{flush_interval: "50ms"}` to
change it. Other settings leave the 10 ms default intact.

Lower intervals reduce the wait for acknowledgements but increase
object-store PUTs. Each shard with pending writes can issue up to one WAL PUT
per interval; idle shards issue none. Choose the interval for the expected
write rate, shard count, and store pricing. For `:memory` and `{:local, dir}`
tests, `settings: %{flush_interval: "2ms"}` reduces the wait without request
charges.

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
  they never return data before it is durable. The read itself runs in the
  caller's process.
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

- A path-based access check must authorize both the target path and the
  `Stream-Forked-From` source of a `PUT`: a fork reads data from its source.
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

- **Forks.** A fork starts with its source's durable messages before the fork
  offset, from the source's trim point on. These messages are the fork's
  *inherited range*. Creating a fork does not copy them, so creation time
  does not depend on how many messages the fork inherits. (With a fork
  sub-offset on a byte stream, it copies the first bytes of the message at the
  fork offset.) The fork reads its inherited range from its source's storage.
  Those reads need the source's shard to be available, and the shards of the
  source's own sources if it is a fork; they do not start stream servers
  there. Reads of the fork's own messages need only its shard. A fork
  inherits TTL or Expires-At unless it sets its own.
- **Fork depth.** A fork's depth is its source's depth plus one, or zero when
  its inherited range is empty. Other streams, including copies, have depth
  zero. `:max_fork_depth` in cluster `child_options` (default 8) limits it; a
  deeper fork returns `{:error, {:bad_request, :max_fork_depth_exceeded}}`
  (HTTP 400).
- **Copies.** `Slap.Streams.copy/3` writes the source's messages into a new
  stream with its own storage, at the same offsets. Use a fork when the
  source's shards will stay available: it does not copy the inherited
  messages. Use a copy when the new stream must not depend on its
  source: a completed copy reads only its own shard and does not keep its
  source's messages, and it can be forked to start a new chain of forks.
  A copy takes time and storage proportional to the messages it copies.
  Copies are available only in-process.
- **Interrupted creation.** A fork or copy stays hidden until it is active.
  Lists omit it, and HEAD, reads, appends, and forks or copies from it return
  `:unavailable`. A copy writes pages from the calling process until its
  `:timeout` (default 30 s), so a large copy can need several calls. When a
  create or copy returns `{:error, :timeout}` or another error, the target
  can stay pending with the progress made so far. Call it again with the same
  arguments to continue; it returns `{:ok, :exists, info}` if the earlier call
  finished after returning. Once a call has selected the inherited range,
  retries keep it, even if the source changes or is deleted. Delete a pending
  target to cancel its creation.
- **Deletes.** Deleting a stream that has forks or pending copies soft-deletes
  it: reads return 410 and creating a stream at its path returns 409, while
  its forks and pending copies can still read their inherited ranges. A
  deleted fork keeps its own inherited range while forks made from it exist.
  Long-polls woken by deletion return `:deleted` through the Elixir API.
- **Expiry.** Every 10 s (`:expiry_interval`), a background job asks each stream
  past its deadline to check expiry and delete itself if expired. Public requests
  also check expiry; internal retention and cleanup requests do not. Sliding-TTL
  access times are persisted at most once per tenth of the TTL.
- **Repair.** Every minute (`:repair_interval`), repair cancels forks pending
  for longer than `:creation_grace` (default one hour) and copies that have
  copied no page for that long. It also releases inherited ranges that no
  stream needs any more. A range whose fork or copy is on an unreachable
  shard stays until repair can check it. Repair reads durable metadata, and
  starts a stream server only when it needs to write.
- **Deleting messages.** Background deletion removes messages in pages of
  10,000 rows and resumes after a crash.
- **Trim.** `Slap.Streams.trim(path, offset)` makes reads below `offset` return
  410 (`:trimmed`). HTTP reads from `-1` (Elixir `:start`) start at the trim
  point. Background cleanup deletes only the messages below both the trim
  point and the start of every inherited range that other streams hold on
  this stream. Trimming a fork does not shrink its inherited range. Trim is
  available only in-process.
- **List.** `Slap.Streams.list(prefix)` returns paths starting with `prefix` in
  its placement group, such as a Yjs document's `.snapshots/`. It is available
  only in-process.
- **Seal.** `Slap.Streams.seal(group)` permanently prevents new streams in a
  placement group. Once it returns, creating a stream, fork or copy in that
  group returns `{:error, :sealed}` (409) without reserving the target. A fork
  or copy that was already pending is removed when it tries to become active,
  and that call returns `{:error, :sealed}`. Existing streams are unchanged,
  and a retry of a creation that had already completed returns
  `{:ok, :exists, info}`.
  [`slap_snapshot_log`](https://hexdocs.pm/slap_snapshot_log/) seals a deleted
  log's group to prevent pending snapshot writes from creating new streams.
  Seal is available only in-process.

## Upgrading existing stream stores

Stores written by 0.4.0 and earlier need an offline upgrade before starting
this version. In 0.4.0 and earlier, a fork copied its source's messages; the
upgrade turns those forks into streams that are not forks. Before upgrading,
finish or delete forks that the old version is still creating.

Stop **every node using the store** and take a backup. With the new package
installed, save this invocation as `upgrade_streams.exs`. Supply all stream
shards using their existing paths and store configuration:

```elixir
shard_paths = ["streams/shard-000", "streams/shard-001"]
store = {:local, "/path/to/existing/store"}

dbs =
  Enum.map(shard_paths, fn path ->
    {:ok, db} = Slap.SlateDB.open(path, store: store)
    db
  end)

try do
  :ok = Slap.Streams.Upgrade.run(dbs)
after
  Enum.each(dbs, fn db -> :ok = Slap.SlateDB.close(db) end)
end
```

Run `mix run --no-start upgrade_streams.exs` before restarting the cluster.
`Slap.Streams.Upgrade.run/1` reads trusted stored ETF records; do not supply
external input. It checks every shard before writing and converts metadata in
durable batches. On failure, close and reopen the handles before retrying;
converted records are skipped. Once conversion starts, use the new version or
restore the backup before returning to the old version.

The upgrade preserves message rows, offsets, trim, producer state, Stream-Seq,
expiry and closed state. Background cleanup finishes old deletion and trim work
after the cluster restarts. Sources no longer keep messages for forks made by
the old version, which hold their own copies.

## Storage after deletes

Deleting rows writes tombstones. SlateDB reclaims their space only after
compaction and garbage collection; old sorted runs and checkpoints can delay
reclamation. `bench/storage_soak.exs` measures storage reclamation on local and
remote stores.

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

Follow the repository's
[development setup](https://github.com/mjrusso/slap#development), then run
from `slap_streams/`:

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

Run these scripts from `slap_streams/` after the development setup above:

```sh
mix run bench/append_latency.exs
mix run bench/read_scaling.exs
mix run bench/forks_and_copies.exs
```

These measure the library in-process. `slap` runs the official [Durable
Streams HTTP
benchmarks](https://hexdocs.pm/slap/readme.html#durable-streams-conformance-and-benchmarks)
and an [HTTP append load test](https://hexdocs.pm/slap/readme.html#streams-load)
against a running server.

`bench/append_latency.exs` measures durable append latency on the in-memory
store. Set `SLAP_BENCH_FLUSH_INTERVAL` to change SlateDB's flush interval.

`bench/forks_and_copies.exs` measures fork creation and copies for 1 KiB,
64 MiB and 1 GiB prefixes, reads through chains of forks, and fork reads after
the source is trimmed. It uses an in-memory store and prints JSON results.

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
