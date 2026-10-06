![Slap logo](assets/slap.png)

***Slap it in a bucket!*** Use object storage as a database.

**Slap** provides Elixir bindings for [SlateDB](https://slatedb.io), and
services built on them:

- a [Durable Streams](https://durablestreams.com) server that passes the
  official conformance suite
- partitioned key-value (KV) storage with conditional writes and ordered scans
- file storage with metadata, conditional replacement, and crash-safe cleanup;
  small files are stored in KV, which saves object-store PUTs
- [Yjs](https://yjs.dev) documents for collaborative editing, with replication
  and presence across nodes

The services are designed to be embedded in your existing application. Each
service can run across several nodes, with a choice of placement strategies and
failover. These services acknowledge a write only once it is durable in the
store. A standalone HTTP server is also provided for evaluation, testing, and
benchmarking.

---

> [!WARNING]
>
> This is new, experimental data storage software. Bugs may cause data loss.
> APIs may change. Keep independent backups and evaluate carefully before use
> in production. Review and contribute to the [Jepsen test
> suite](jepsen/README.md) to expand failure coverage and build confidence in
> data safety. Talk to your doctor about whether or not Slap is right for you.

---

# Slap

## SlateDB

SlateDB is an embedded [log-structured merge-tree key-value
engine](https://slatedb.io/docs/design/overview/). It stores its write-ahead
log and sorted string table files in object storage (S3, Google Cloud Storage,
Azure Blob Storage, or compatible services such as Tigris). A **store** is
where SlateDB keeps its data; the Elixir binding also supports local
directories and memory.

SlateDB batches writes to limit PUT requests and periodically flushes its
in-memory write-ahead log for durability. A shorter flush interval makes writes
durable sooner but uses more PUT requests. See [write and flush
behavior](https://slatedb.io/docs/operations/tuning/). In-memory block caches,
compressed sorted string tables, Bloom filters, and an optional local disk
cache reduce GETs, read latency, and request cost. See [caching
design](https://slatedb.io/docs/design/caching/).

SlateDB permits one writer per database. Other processes can read concurrently,
and a separate process can compact it. If another process opens the database
for writing, SlateDB fences the previous writer: the new writer uses
conditional writes to advance an epoch in the manifest and claim a write-ahead
log position. Any later write from the previous process fails. (The object
store must honor conditional writes.) See [SlateDB's writer
protocol](https://slatedb.io/rfcs/0001-manifest/#writer-protocol).

## Sharding

Slap uses multiple SlateDB databases as **shards**, each with its own writer.
Writes to different shards can run in parallel, increasing throughput. Slap
assigns each shard to one node and routes Streams and KV requests to its
owner. SlateDB's fencing protects against nodes disagreeing about ownership.
KV keeps each **partition**'s rows on one shard; Streams keeps paths in the
same **placement group** on one shard.

## Usage Examples

With Elixir 1.18 or later installed, from the root of this repository:

```sh
cd slap
SLAP_LOCAL_DEPS=0 mix deps.get
SLAP_LOCAL_DEPS=0 iex -S mix
```

`SLAP_LOCAL_DEPS=0` uses the packages published on Hex.

> [!NOTE]
>
> For simplicity, the examples use the memory store (`store: :memory`), which
> does not require object storage. (Data will be lost when the VM exits.)
>
> `Slap.SlateDB.open/2` and the cluster `start_link/1` functions all take the
> same `:store` option. To use S3, pass `store: {:url, "s3://bucket/prefix"}`
> and set `AWS_REGION` and credentials (such as `AWS_ACCESS_KEY_ID` and
> `AWS_SECRET_ACCESS_KEY`) in the environment.
>
> The `slap_slatedb` README lists the other [object
> stores](slap_slatedb/README.md#object-stores), and the [`slap_streams` S3
> walkthrough](slap_streams/README.md#using-s3) runs a Streams cluster on S3.

### SlateDB Bindings

```elixir
iex> {:ok, db} = Slap.SlateDB.open("example", store: :memory); :ok
:ok

iex> {:ok, _seq} = Slap.SlateDB.put(db, "user:1", "Ada", await_durable: true); :ok
:ok
iex> {:ok, "Ada"} = Slap.SlateDB.get(db, "user:1")
{:ok, "Ada"}

iex> {:ok, _seq} = Slap.SlateDB.delete(db, "user:1", await_durable: true); :ok
:ok
iex> {:ok, nil} = Slap.SlateDB.get(db, "user:1")
{:ok, nil}
iex> :ok = Slap.SlateDB.close(db)
:ok
```

See the [`slap_slatedb` README](slap_slatedb/README.md) for more usage
examples.

### Streams and KV

These examples show the Elixir API. The [standalone server
README](slap/README.md#try-the-standalone-server) has HTTP examples and server
options.

#### Streams

Start a Streams cluster.

```elixir
iex> {:ok, _} = Slap.Streams.Cluster.start_link(store: :memory, path: "streams", shards: 1); :ok
:ok
```

Create a JSON stream, then append to it. A JSON array append stores each
element as a separate message.

```elixir
iex> {:ok, :created, _} = Slap.Streams.create("/events", content_type: "application/json"); :created
:created
iex> {:ok, %{result: result, next_offset: offset}} =
...>   Slap.Streams.append("/events", ~s([{"type":"created","id":1},{"type":"updated","id":1}])); {result, offset}
{:appended, 58}
```

Read the stream. The API returns each message with its offset.

```elixir
iex> {:ok, %{messages: events}} = Slap.Streams.read("/events", 0); events
[
  {0, "{\"type\":\"created\",\"id\":1}"},
  {29, "{\"type\":\"updated\",\"id\":1}"}
]
```

Fork the stream at its current end, then append to the fork. The source still
contains only the first two messages.

```elixir
iex> {:ok, :created, _} = Slap.Streams.create("/events-fork", forked_from: "/events"); :created
:created
iex> {:ok, %{result: result, next_offset: offset}} =
...>   Slap.Streams.append("/events-fork", ~s({"type":"deleted","id":1})); {result, offset}
{:appended, 87}
iex> {:ok, %{messages: events}} = Slap.Streams.read("/events-fork", 0); events
[
  {0, "{\"type\":\"created\",\"id\":1}"},
  {29, "{\"type\":\"updated\",\"id\":1}"},
  {58, "{\"type\":\"deleted\",\"id\":1}"}
]
iex> {:ok, %{messages: events}} = Slap.Streams.read("/events", 0); events
[
  {0, "{\"type\":\"created\",\"id\":1}"},
  {29, "{\"type\":\"updated\",\"id\":1}"}
]
```

#### KV

Start a KV cluster.

```elixir
iex> {:ok, _} = Slap.KV.Cluster.start_link(store: :memory, path: "kv", shards: 1); :ok
:ok
```

Set three rows, get one, then scan a range of keys in the `people` partition.
The range stops before `c`.

```elixir
iex> {:ok, version} = Slap.KV.put("people", "a", "Ada"); :ok
:ok
iex> {:ok, _} = Slap.KV.put("people", "b", "Grace"); :ok
:ok
iex> {:ok, _} = Slap.KV.put("people", "c", "Katherine"); :ok
:ok
iex> {:ok, %{value: value, version: ^version}} = Slap.KV.get("people", "a"); value
"Ada"
iex> {:ok, %{rows: rows, cursor: nil}} =
...>   Slap.KV.scan("people", gte: "a", lt: "c"); rows
[{"a", "Ada"}, {"b", "Grace"}]
```

## Packages

Slap is eight Mix packages, each published separately to Hex. Most
applications depend on one of the services:

| Package                         | Provides                                                                                                          |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------|
| [`slap_streams`](slap_streams/) | A [Durable Streams](https://durablestreams.com) server, with HTTP, long polling, and SSE.                         |
| [`slap_kv`](slap_kv/)           | Partitioned key-value storage with conditional writes, ordered scans, and an HTTP API.                            |
| [`slap_files`](slap_files/)     | File storage. Small bodies go in KV records, which saves object-store PUTs; larger ones go in object storage.     |
| [`slap_yjs`](slap_yjs/)         | [Yjs](https://yjs.dev) documents, with replication and presence across nodes.                                     |

The services are built on these packages, which you can also use directly:

| Package                                   | Provides                                                                                                |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------|
| [`slap_slatedb`](slap_slatedb/)           | Elixir bindings for SlateDB: reads, writes, transactions, durability subscriptions, and object access.  |
| [`slap_cluster`](slap_cluster/)           | Placement, routing, and failover for SlateDB shards across nodes.                                       |
| [`slap_snapshot_log`](slap_snapshot_log/) | An append-only log with snapshots that replace its prefix. Each `slap_yjs` document is a snapshot log.  |

[`slap`](slap/README.md#try-the-standalone-server) is a standalone HTTP server
for evaluating, testing, and benchmarking Streams and KV. You generally don't
want to depend on it in your application.

### Embedding

The usage examples above start `Slap.Streams.Cluster` and `Slap.KV.Cluster`
from IEx. In an application, add them as children of your supervisor. The
[`slap_streams`](slap_streams/README.md#embedding),
[`slap_kv`](slap_kv/README.md#example),
[`slap_files`](slap_files/README.md#example),
[`slap_yjs`](slap_yjs/README.md#example), and
[`slap_snapshot_log`](slap_snapshot_log/README.md#example) READMEs show the
children each package needs.

`slap_streams` and `slap_kv` include Plug routers (`Slap.Streams.HTTP.Router`
and `Slap.KV.HTTP.Router`) but don't start an HTTP server. Mount a router in
your Plug or Phoenix application, or serve it with a Plug server such as
Bandit. The routers don't authenticate requests; put your own authentication
in front.

## Benchmarks

CI runs two benchmark workflows:

- [SlateDB benchmarks](.github/workflows/slap_slatedb_bench.yml), when
  `slap_slatedb/` changes. They measure in-memory binding overhead and durable
  operations against RustFS, with and without added network latency.
- [Durable Streams benchmarks](.github/workflows/slap_streams_bench.yml), when
  `slap/`, `slap_streams/`, `slap_cluster/` or `slap_slatedb/` changes. They
  run the official Durable Streams benchmarks against Slap and the official
  Caddy server on the same runner. Run them locally with `just streams-bench`;
  see the [server README](slap/README.md#durable-streams-conformance-and-benchmarks).

Results appear in each run's job summary; on `main`, they are also pushed to
the `gh-pages` branch. Shared runners are noisy, so compare runs over time
rather than reading one result.

To measure HTTP append throughput and latency on this checkout, use two
terminals with `SLAP_LOCAL_DEPS=1` set (the Nix shell sets it). In the first,
from the repository root, start a Streams server:

```sh
cd slap
mix slap.server --streams --store memory
```

In the second, also from the repository root, run the load script:

```sh
cd slap
mix run bench/streams_http_load.exs --connections 128 --seconds 20
```

The values shown are the defaults. The script also takes `--streams`,
`--bytes`, and `--rate`. To measure an S3-compatible store, start the server
with `--store s3:s3://bucket/bench` and set its `AWS_*` variables. Before
sizing a deployment, read [Streams load](slap/README.md#streams-load): the
store's sustained PUT rate limits the shard count and flush interval.

Other scripts measure:

- [SlateDB storage and shard behavior](slap_slatedb/README.md#benchmarks)
- [Stream append latency and read scaling](slap_streams/README.md#benchmarks)
- [Stream storage reclamation](slap_streams/README.md#storage-after-deletes)
- [KV partition writers](slap_kv/README.md#benchmarks)
- [Inline versus object file bodies](slap_files/README.md#benchmarks)

## Jepsen

The [Jepsen suite](jepsen/README.md) runs concurrent operations against five
Slap nodes that share a RustFS store, while it injects network partitions,
process kills, pauses, and object-store faults. It checks Streams, KV, Files,
snapshot logs, and Yjs for lost or duplicated acknowledged writes and for
consistency violations. On pushes and pull requests that touch the packages or
the suite, CI runs every combination of the five workloads, two placement
strategies (object leases and distributed), and four faults: 40 jobs, each
running two 120-second trials.

With object leases, the checkers require every read to see each write that was
acknowledged before the read started. With distributed placement, a node can
serve stale reads during a fault, so the checkers allow stale reads but still
require every acknowledged write to be present once the cluster recovers. The
[Jepsen README](jepsen/README.md#placement) lists the checks for each
placement.

To run a workload, install Docker with Compose, JDK 21, Leiningen, gnuplot,
Graphviz, and `just` (the Nix shell provides all but Docker). Then, from the
repository root:

```sh
just jepsen-up
just jepsen --workload append --time-limit 120
just jepsen-down
```

The [Jepsen README](jepsen/README.md) lists the workloads, fault options,
placement guarantees, and where to inspect results.

## Development

Each project runs its checks (formatting, compiler warnings, Credo, Dialyzer,
dependency cycles, tests) with `mix check`; see [AGENTS.md](AGENTS.md).

To set up: `nix develop` (the flake provides OTP, Elixir, Rust and the Jepsen
tools), then `just deps` and `just check`.

The Nix shell sets `SLAP_LOCAL_DEPS=1`, so the Mix projects use sibling
packages from this checkout. CI and the Jepsen node build set the same
variable. With the variable unset or set to `0`, their dependencies use
versioned Hex packages. The Nix shell also sets `SLAP_SLATEDB_BUILD=1`, which
builds the SlateDB NIF from source (this needs Rust). To check a package for
publication from the Nix shell, run `SLAP_LOCAL_DEPS=0 mix deps.get` and then
`SLAP_LOCAL_DEPS=0 mix hex.publish --dry-run` in its directory after its
dependencies have been published.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
