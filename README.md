![Slap logo](site/assets/slap.png)

[![slap][slap-badge]][slap-hex] [![slap_streams][slap_streams-badge]][slap_streams-hex] [![slap_kv][slap_kv-badge]][slap_kv-hex] [![slap_files][slap_files-badge]][slap_files-hex] [![slap_yjs][slap_yjs-badge]][slap_yjs-hex] [![slap_slatedb][slap_slatedb-badge]][slap_slatedb-hex] [![slap_cluster][slap_cluster-badge]][slap_cluster-hex] [![slap_snapshot_log][slap_snapshot_log-badge]][slap_snapshot_log-hex] [![slap_inspect][slap_inspect-badge]][slap_inspect-hex]

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
failover.

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

## Introduction

### SlateDB

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

### Sharding

Slap uses multiple SlateDB databases as **shards**, each with its own writer.
Writes to different shards can run in parallel, increasing throughput. Slap
assigns each shard to one node and routes Streams and KV requests to its
owner. SlateDB's fencing protects against nodes disagreeing about ownership.
KV keeps each **partition**'s rows on one shard; Streams keeps paths in the
same **placement group** on one shard.

## Packages

Applications can depend on individual server libraries or on `slap`, which
includes all seven:

| Package                         | Provides                                                                                                      | Hex                                                                                         |
|---------------------------------|---------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| [`slap_streams`](slap_streams/) | A [Durable Streams](https://durablestreams.com) server, with HTTP, long polling, and SSE.                     | [![Hex][slap_streams-hex-badge]][slap_streams-hex] [![Docs][docs-badge]][slap_streams-docs] |
| [`slap_kv`](slap_kv/)           | Partitioned key-value storage with conditional writes, ordered scans, and an HTTP API.                        | [![Hex][slap_kv-hex-badge]][slap_kv-hex] [![Docs][docs-badge]][slap_kv-docs]                |
| [`slap_files`](slap_files/)     | File storage. Small bodies go in KV records, which saves object-store PUTs; larger ones go in object storage. | [![Hex][slap_files-hex-badge]][slap_files-hex] [![Docs][docs-badge]][slap_files-docs]       |
| [`slap_yjs`](slap_yjs/)         | [Yjs](https://yjs.dev) documents, with replication and presence across nodes.                                 | [![Hex][slap_yjs-hex-badge]][slap_yjs-hex] [![Docs][docs-badge]][slap_yjs-docs]             |

The services are built on these packages, which you can also use directly:

| Package                                   | Provides                                                                                               | Hex                                                                                                        |
|-------------------------------------------|--------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| [`slap_slatedb`](slap_slatedb/)           | Elixir bindings for SlateDB: reads, writes, transactions, durability subscriptions, and object access. | [![Hex][slap_slatedb-hex-badge]][slap_slatedb-hex] [![Docs][docs-badge]][slap_slatedb-docs]                |
| [`slap_cluster`](slap_cluster/)           | Placement, routing, and failover for SlateDB shards across nodes.                                      | [![Hex][slap_cluster-hex-badge]][slap_cluster-hex] [![Docs][docs-badge]][slap_cluster-docs]                |
| [`slap_snapshot_log`](slap_snapshot_log/) | An append-only log with snapshots that replace its prefix. Each `slap_yjs` document is a snapshot log. | [![Hex][slap_snapshot_log-hex-badge]][slap_snapshot_log-hex] [![Docs][docs-badge]][slap_snapshot_log-docs] |

`slap` installs them together:

| Package         | Provides                                                                     | Hex                                                                 |
|-----------------|------------------------------------------------------------------------------|---------------------------------------------------------------------|
| [`slap`](slap/) | All Slap server libraries, and `Slap`, which starts the services you select. | [![Hex][slap-hex-badge]][slap-hex] [![Docs][docs-badge]][slap-docs] |

`slap_inspect` browses the data of a running cluster. It is installed
separately and does not depend on the server libraries:

| Package                         | Provides                                        | Hex                                                                                         |
|---------------------------------|-------------------------------------------------|---------------------------------------------------------------------------------------------|
| [`slap_inspect`](slap_inspect/) | A read-only TUI and CLI for browsing Slap data. | [![Hex][slap_inspect-hex-badge]][slap_inspect-hex] [![Docs][docs-badge]][slap_inspect-docs] |

## Getting started

Requires Elixir 1.18 or later and Erlang/OTP 26.2 or later.

[![Run in Livebook](https://livebook.dev/badge/v1/blue.svg)](https://livebook.dev/run?url=https%3A%2F%2Fgithub.com%2Fmjrusso%2Fslap%2Fblob%2Fmain%2Fslap%2Fnotebooks%2Fgetting-started.livemd)

Start with the [getting started notebook](slap/notebooks/getting-started.livemd)
to try SlateDB, Streams, snapshot logs, Yjs, KV, and files from Elixir. It
uses in-memory storage and runs in Livebook or IEx.

In an application, add `slap` to your dependencies and start the services you
need under your supervisor:

```elixir
children = [
  {Slap, store: {:url, "s3://bucket/prefix"}, streams: [], kv: [], files: []}
]
```

Use `store: :memory` to run without object storage. For S3, set `AWS_REGION`
and credentials (such as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`)
in the environment. See the [application setup](slap/README.md#use-slap-in-an-application)
for dependencies and options, and the `slap_slatedb` README for other
[object stores](slap_slatedb/README.md#object-stores).

To try the Streams and KV HTTP APIs, run
[`examples/server.exs`](slap/examples/server.exs); the [standalone server
guide](slap/guides/standalone-server.md) shows how. The HTTP routers don't
authenticate requests; put your own authentication in front.

The [kitchen sink demo](slap/README.md#kitchen-sink-demo) runs every service
with changing in-memory data and prints [Slap Inspect](slap_inspect/)
commands to browse it.

The [Exploring Slap's internals](slap/notebooks/internals.livemd) notebook
([run in Livebook][internals-livebook]) traces writes, displays stored
data, and demonstrates shard recovery. The
[architecture guide](slap/guides/architecture.md) explains the package and
service dependencies.

## Benchmarks

These workflows run the benchmarks in GitHub Actions weekly and on each push
to `main` that changes the code they measure; start them from the Actions tab
to run them on another branch:

- [SlateDB benchmarks](.github/workflows/slap_slatedb_bench.yml) measure
  in-memory binding overhead and durable operations against RustFS, with and
  without added network latency.
- [Durable Streams benchmarks](.github/workflows/slap_streams_bench.yml) run
  the official Durable Streams benchmarks against Slap and the official Caddy
  server on the same runner. Run them locally with `just streams-bench`; see
  the [server development
  guide](slap/guides/development.md#durable-streams-conformance-and-benchmarks).

Results appear in each run's job summary; a run on `main` also pushes them to
the `gh-pages` branch, which GitHub Pages serves as charts of every run on
`main`:

- SlateDB: <https://mjrusso.github.io/slap/dev/bench/>
- Durable Streams: <https://mjrusso.github.io/slap/dev/streams-bench/>

Shared runners are noisy, so expect non-trivial variance between runs.

To measure HTTP append throughput and latency on this checkout, see [Streams
load](slap/guides/development.md#streams-load). The store's sustained PUT rate
limits the shard count and flush interval, so run it on the intended store
before sizing a deployment.

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
consistency violations. To run it in GitHub Actions, start the [Jepsen
workflow](.github/workflows/jepsen.yml) from the Actions tab. It runs every
combination of the five workloads, two placement strategies (object leases and
distributed), and four faults: 40 jobs, each running two 120-second trials.

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
builds the SlateDB NIF from source (this needs Rust).

[RELEASING.md](RELEASING.md) describes how to release the packages to Hex.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).

[internals-livebook]: https://livebook.dev/run?url=https%3A%2F%2Fgithub.com%2Fmjrusso%2Fslap%2Fblob%2Fmain%2Fslap%2Fnotebooks%2Finternals.livemd
[slap-badge]: https://img.shields.io/hexpm/v/slap?label=slap
[slap-hex-badge]: https://img.shields.io/hexpm/v/slap.svg
[slap-hex]: https://hex.pm/packages/slap
[slap-docs]: https://hexdocs.pm/slap
[slap_streams-badge]: https://img.shields.io/hexpm/v/slap_streams?label=slap_streams
[slap_streams-hex-badge]: https://img.shields.io/hexpm/v/slap_streams.svg
[slap_streams-hex]: https://hex.pm/packages/slap_streams
[slap_streams-docs]: https://hexdocs.pm/slap_streams
[slap_kv-badge]: https://img.shields.io/hexpm/v/slap_kv?label=slap_kv
[slap_kv-hex-badge]: https://img.shields.io/hexpm/v/slap_kv.svg
[slap_kv-hex]: https://hex.pm/packages/slap_kv
[slap_kv-docs]: https://hexdocs.pm/slap_kv
[slap_files-badge]: https://img.shields.io/hexpm/v/slap_files?label=slap_files
[slap_files-hex-badge]: https://img.shields.io/hexpm/v/slap_files.svg
[slap_files-hex]: https://hex.pm/packages/slap_files
[slap_files-docs]: https://hexdocs.pm/slap_files
[slap_yjs-badge]: https://img.shields.io/hexpm/v/slap_yjs?label=slap_yjs
[slap_yjs-hex-badge]: https://img.shields.io/hexpm/v/slap_yjs.svg
[slap_yjs-hex]: https://hex.pm/packages/slap_yjs
[slap_yjs-docs]: https://hexdocs.pm/slap_yjs
[slap_slatedb-badge]: https://img.shields.io/hexpm/v/slap_slatedb?label=slap_slatedb
[slap_slatedb-hex-badge]: https://img.shields.io/hexpm/v/slap_slatedb.svg
[slap_slatedb-hex]: https://hex.pm/packages/slap_slatedb
[slap_slatedb-docs]: https://hexdocs.pm/slap_slatedb
[slap_cluster-badge]: https://img.shields.io/hexpm/v/slap_cluster?label=slap_cluster
[slap_cluster-hex-badge]: https://img.shields.io/hexpm/v/slap_cluster.svg
[slap_cluster-hex]: https://hex.pm/packages/slap_cluster
[slap_cluster-docs]: https://hexdocs.pm/slap_cluster
[slap_snapshot_log-badge]: https://img.shields.io/hexpm/v/slap_snapshot_log?label=slap_snapshot_log
[slap_snapshot_log-hex-badge]: https://img.shields.io/hexpm/v/slap_snapshot_log.svg
[slap_snapshot_log-hex]: https://hex.pm/packages/slap_snapshot_log
[slap_snapshot_log-docs]: https://hexdocs.pm/slap_snapshot_log
[slap_inspect-badge]: https://img.shields.io/hexpm/v/slap_inspect?label=slap_inspect
[slap_inspect-hex-badge]: https://img.shields.io/hexpm/v/slap_inspect.svg
[slap_inspect-hex]: https://hex.pm/packages/slap_inspect
[slap_inspect-docs]: https://hexdocs.pm/slap_inspect
[docs-badge]: https://img.shields.io/badge/hex-docs-blue.svg
