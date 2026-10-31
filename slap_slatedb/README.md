# slap_slatedb

`slap_slatedb` provides Elixir bindings for [SlateDB](https://slatedb.io) 0.17,
an embedded key-value engine that keeps its data in object storage, such as
S3. Your application opens a database and calls it directly; there is no
database server to run. SlateDB batches writes to limit object-store requests.
A database can also use a local directory or memory. The bindings use the
Erlang VM's native implemented function (NIF) interface to call Rust code,
built with [Rustler](https://github.com/rusterlium/rustler).

Use `slap_slatedb` when you want direct control over keys, scans,
transactions, and durability. It does not distribute databases across nodes
or serve an HTTP API. To run many databases across several nodes, use
[`slap_cluster`](https://hexdocs.pm/slap_cluster/). For a ready-made
partitioned key-value store with conditional writes, use
[`slap_kv`](https://hexdocs.pm/slap_kv/).

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

Requires Elixir 1.18 or later and Erlang/OTP 25 or later.

To depend on `slap_slatedb` directly, add it to the dependencies in your
application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_slatedb, "~> 0.2.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_slatedb/).

Prebuilt NIFs cover macOS and Linux, with glibc or musl, on arm64 and x86_64;
[`rustler_precompiled`](https://github.com/philss/rustler_precompiled) downloads
the one for your platform from GitHub during compilation and verifies its
checksum. You don't need Rust.

On other platforms, or for builds without network access such as Nix sandbox
builds, compile the NIF from source: install Rust 1.91 or later, add
`{:rustler, "~> 0.38"}` to your dependencies, and set `SLAP_SLATEDB_BUILD=1`
when you compile.

## Example

In an application that depends on `slap_slatedb`, run this in `iex -S mix`
with a fresh local directory:

```elixir
iex> {:ok, db} = Slap.SlateDB.open("my-db", store: {:local, "/tmp/slatedb"}); :ok
:ok

iex> {:ok, _seq} = Slap.SlateDB.put(db, "user:1", "ada", await_durable: true); :ok
:ok
iex> {:ok, "ada"} = Slap.SlateDB.get(db, "user:1")
{:ok, "ada"}
iex> :ok = Slap.SlateDB.close(db)
:ok
```

`await_durable: true` waits for the write to reach the store before replying.
Without it, the returned sequence identifies a write that may still be in
flight. See [Durability and write ordering](#durability-and-write-ordering)
before acknowledging data to a caller.

## Capabilities

- **Reads and writes:** `get/3`, `put/4`, `delete/3` and batched `write/3`;
  range and prefix scans; per-key time to live (TTL), enforced during
  compaction; and snapshots for stable reads.
- **Transactions:** snapshot and serializable isolation, with conflict retries
  through `transaction/3`. Built-in merge operators and a prefix compaction
  filter are available.
- **Durability:** write sequence numbers, `durable_seq/1`, and
  `subscribe/3` for durability notifications.
- **Object storage:** `Slap.SlateDB.ObjectStore` supports conditional puts,
  streaming uploads and lazy downloads. `Slap.SlateDB.Admin` manages
  checkpoints, clones, compaction and garbage collection.
- **Observability:** `stats/1`, `metrics/1` and `Slap.SlateDB.Telemetry`.

The module documentation describes options and return values for each API.

`snapshot/1` gives a read-only view of an open database; it is not a persisted
checkpoint. Use `create_checkpoint/2` for a durable view that can be reopened,
or `Slap.SlateDB.Admin.clone/3` to create an independently writable database
from a checkpoint or the latest state. SlateDB's range projection and union
operations for rescaling are not exposed by this binding.

## How it works

Use one writer handle per database. You can share that handle between Elixir
processes. Opening another writer for the same stored database fences the
previous writer: it loses permission to make further durable writes. On
shared storage, use `Slap.SlateDB.Reader` for additional read-only handles,
including in other VMs, without taking over the writer's ownership.

Writes first enter an in-memory buffer. SlateDB batches them into write-ahead
log (WAL) objects in the store. The buffer is also flushed into immutable
sorted string tables (SSTs), files containing key/value records ordered by
key. Keys and values can be arbitrary binaries. With persistent storage, a
durable write survives restarting the writer because it is already recorded
in the WAL or tables.

Flushed tables enter level zero (L0). Compaction merges and rewrites SSTs into
sorted runs, groups of tables with non-overlapping key ranges. A manifest is
metadata that records which tables and WAL data make up a database state.
Checkpoints retain a manifest and its referenced data so that state can be
reopened later.

Garbage collection (GC) removes obsolete objects after the current state and
retained checkpoints no longer need them. Compaction creates replacement
tables; GC reclaims the old files.

### Native calls

I/O NIFs copy their arguments, start asynchronous Rust work on a shared Tokio
runtime, and send the result to the calling Elixir process. A panic becomes an
`:internal` error reply. Opening a store and converting large batches run off
the normal Erlang schedulers.

`close/2` waits for calls already in flight. Calls made after the handle closes
fail with a `:closed` error. Snapshots, transactions, and iterators hold the
database open. If the final handle is dropped without `close/2`, the destructor
closes it in the background and logs a warning.

Binaries of at least 64 KiB share SlateDB buffers with Elixir. This avoids a
copy, but a small slice can keep a larger cached block alive; use
`:binary.copy/1` when retaining such a value.

A call with a timeout uses a helper process so late replies can be discarded.
The database operation continues after the caller times out. See
[Durability and write ordering](#durability-and-write-ordering) before using
timeouts with writes.

### Runtime threads

The Tokio runtime is shared by every SlateDB database in the VM. By default
it uses one worker thread per CPU. Set
`config :slap_slatedb, runtime_threads: 4` in `config/runtime.exs` or earlier,
or set `SLAP_SLATEDB_RUNTIME_THREADS=4` before starting the VM. Application
config takes precedence when it is not `nil`. `System.put_env/2` in
`config/runtime.exs` works too; a call in your application's `start/2` is too
late because dependencies start first. Invalid values fail startup. Restart
the VM to change the count.

More threads can increase concurrency, memory use and CPU contention. They
do not shorten object-store requests or WAL flush intervals. Start with the
default and measure throughput and latency with your expected workload and
store. For example, run `mix run bench/scenarios.exs` once with the default
and again with `SLAP_SLATEDB_RUNTIME_THREADS=4`; each run starts a new VM.

## Object stores

SlateDB uses conditional writes to fence stale writers, so an object store
must support them. S3, Azure Blob Storage, and RustFS do. Check other stores,
including Google Cloud Storage, with `probe_store/2` before use; see the notes
below.

The `:store` option of `open/2` selects where the database lives:

```elixir
# In memory; each open/2 call creates a separate store.
Slap.SlateDB.open("my-db", store: :memory)

# A local directory.
Slap.SlateDB.open("my-db", store: {:local, "/var/lib/slatedb"})

# Amazon S3, configured from AWS_REGION, AWS_ACCESS_KEY_ID and
# AWS_SECRET_ACCESS_KEY.
Slap.SlateDB.open("my-db", store: {:url, "s3://my-bucket/prefix"})

# An S3-compatible store.
Slap.SlateDB.open("my-db",
  store:
    {:url, "s3://my-bucket/prefix",
     aws_endpoint: "http://127.0.0.1:9000",
     aws_allow_http: true,
     aws_region: "us-east-1",
     aws_access_key_id: "rustfsadmin",
     aws_secret_access_key: "rustfsadmin"}
)

# Azure Blob Storage, configured from AZURE_STORAGE_ACCOUNT_NAME and
# AZURE_STORAGE_ACCOUNT_KEY.
Slap.SlateDB.open("my-db", store: {:url, "az://my-container/prefix"})

# Google Cloud Storage, with a service account key file. Without one, the
# store uses application default credentials. See the GCS fencing note below.
Slap.SlateDB.open("my-db",
  store:
    {:url, "gs://my-bucket/prefix",
     google_service_account: "/etc/slatedb/service-account.json"}
)
```

With `{:local, directory}`, object writes and completed uploads sync file contents
before returning success. On Unix, object creation and deletion also sync directory
entries, and opening a store syncs its directory and ancestors. Synchronization can
increase write and open latency. Persistence requires a filesystem and storage
device that honor sync requests. Other platforms do not sync directory entries.

This applies to database storage and `Slap.SlateDB.ObjectStore`. Database writes
still return before durability by default; use `await_durable: true`,
`durable_seq/1`, or `flush/1` to wait for or confirm durability.

Local stores support conditional writes (`:create` and `{:update, version}`),
which SlateDB's garbage collector and `slap_cluster`'s `ObjectLease` strategy
need. Conditional updates require a filesystem that records modification times
to the microsecond; on other filesystems they return `:unsupported`.

Every write to a local store holds one exclusive lock on the store's root
directory until the write is synced, so writes to different databases and
objects under one root run one at a time. Reads do not take the lock. Adding
shards under one root does not increase write throughput; see
[choosing the shard count](https://hexdocs.pm/slap_cluster/readme.html#choosing-the-shard-count).

The lock works only if every writer takes it:

- Every process that uses the data must open the same root directory. A
  symlink to the root is resolved to it. A root inside another root is not
  supported.
- Before upgrading, stop processes running older Slap versions. They, other
  `object_store` clients and programs that write to the directory directly do
  not take the lock.
- Do not delete the `.slap-lock#0` file in the root. Its presence does not mean
  a lock is held. Its modification time keeps object versions from repeating.

On Linux and macOS, opening a local store inspects its filesystem. Recognized
network filesystems, including NFS and SMB/CIFS, are rejected. Unknown or ambiguous
types, including FUSE and layered mounts, produce a warning once per root per
process. Test locking and syncing on such a filesystem before using it.

A write whose sync fails returns an error even if the new object is already
visible. After a failed conditional update, read the object's current version
before retrying.

`{:url, url, options}` builds a store for `s3://` (Amazon S3 and compatible
stores), `az://` / `abfs://` / `azure://` (Azure Blob Storage) or `gs://`
(Google Cloud Storage) URLs:

- **Configuration** comes from environment variables (`AWS_*`, `AZURE_*` or
  `GOOGLE_*`), then from `options`, which win. Elixir reads the environment
  with `System.get_env/0` and passes it to the NIF, so `System.put_env/2`
  changes are seen. The builders' own `from_env()` reads the OS environment,
  which does not see them.
- **Unknown option keys are errors.** `aws_regoin: "..."` returns
  `{:error, %Slap.SlateDB.Error{kind: :invalid}}` instead of being ignored.
  Unknown environment variables are skipped, as `from_env` does.
- **Conditional puts are always on.** SlateDB fences stale writers with
  conditional puts on its manifest. For S3, object_store already defaults to
  ETag conditional puts, and the binding also sets them explicitly. A
  `conditional_put` value other than `"etag"`, from the environment or from
  options, is an error. Azure and GCS support conditional puts without
  configuration.
- **Tested against emulators:** the `:s3` tests check fencing against RustFS,
  which answers `412 Precondition Failed` to a conflicting `If-Match` or
  `If-None-Match` PUT. The `:azure` test checks it against Azurite.
- **GCS fencing is unverified here.** object_store sends the documented
  `x-goog-if-generation-match` precondition, but no test in this repository
  has exercised it against GCS.
- **Probe stores before use.** `probe_store/2` checks whether a store rejects
  conflicting conditional puts. A store that ignores preconditions silently
  fails to fence a second writer. Run the probe against GCS and S3-compatible
  stores other than RustFS:

  ```elixir
  {:ok, _steps} =
    Slap.SlateDB.probe_store({:url, "gs://my-bucket/prefix"}, "probe/check")
  ```

## Durability and write ordering

The `put`, `delete`, `merge`, `increment`, and `write` calls return `{:ok, seq}`.
A write is durable once `durable_seq/1` is at least its `seq`. A process can
write with `await_durable: false` and confirm several writes from one
`{:slap_slatedb_durable, ref, tag, durable_seq}` notification.

Each call waits for its reply, and SlateDB assigns the sequence number before
the reply is sent. **One process's writes are applied in the order it makes
them**, each with a higher `seq`. Concurrent writes from different processes
have no defined order. Code that calls the NIF directly without waiting for
replies bypasses this ordering guarantee.

A write that returns `{:error, %{kind: :timeout}}` is still running and may
be applied after the process's next write. If your code depends on write
order, leave `:timeout` at its default of `:infinity`. After any write error,
stop using the handle, close it, reopen the database, and reload state from
storage.

A fenced writer detects the fence when it next writes or flushes, or when it
next polls the manifest (every `manifest_poll_interval`, 1 s by default).
Subscribers then get `{:slap_slatedb_closed, ref, tag, :fenced}`, so an idle
owner finds out that it lost ownership without writing.

Closing a fenced handle returns `:ok` if the handle had already detected the
fence. If the fence is detected during the close, `close/2` returns
`{:error, %{kind: :closed, reason: :fenced}}`, because writes that were not
yet durable are lost. Both are normal shutdown results for a writer that
lost ownership.

## Clones, checkpoints and garbage collection

A clone is a separate database that you can write to. It starts with the
source's data and shares the source's SST files instead of copying them.
`Slap.SlateDB.Admin.clone/3` clones the source at a checkpoint you pass, or
at its latest state. It copies the WAL data the clone needs into the clone's
path. Source and clone must be in the same store, under the same URL prefix.
`:memory` stores cannot be used, because each `:memory` open is a separate
store.

If you pass a checkpoint, you can delete it once `clone/3` returns. If you do
not, SlateDB creates a temporary one. Either way, SlateDB adds checkpoints of
its own to the source, with no expiry, so that garbage collection keeps the
files the clone shares. Do not delete those checkpoints.

A clone can be cloned in turn, so a database can read SST files owned by any
of its ancestors. Do not delete an ancestor database's files while a
descendant exists. Compaction rewrites a clone's shared files into files the
clone owns. Garbage collection removes SlateDB's checkpoint from the source
once neither the clone's current state nor any checkpoint of the clone needs
the source's files. A checkpoint of the clone can therefore keep the source's
checkpoint after the clone's current state no longer needs it.

Checkpoints with finite lifetimes stop protecting data when they expire.
Once GC removes an expired checkpoint, opening a new reader or clone with
its ID returns a `Slap.SlateDB.Error`. Keep a checkpoint alive for as long as
its readers need it.

## Limits and caveats

- SlateDB 0.17 enforces TTL during compaction, not on reads. An expired row
  may still be returned; `get_key_value/3` includes its `expire_ts` for an
  exact cutoff.
- Numeric merge operators require an eight-byte base value and the same
  `:merge_operator` on every process that opens the database. An invalid base
  can fail reads and compaction. A batch cannot merge the same key with
  different TTLs.
- A compaction filter deletes keys only when compaction rewrites them.
  `Slap.SlateDB.Admin.compact/2` submits work on sorted runs and returns before
  it completes; it does not directly compact L0 files.
- A `Slap.SlateDB.Reader` sees durable writes after its next manifest poll
  (its own `manifest_poll_interval`, 10 s by default, unlike the writer's
  1 s). Its default `:managed` mode maintains a checkpoint to protect its
  reads from GC. `mode: :latest` creates no checkpoint, so reads can fail if
  GC removes a file they still need.
- `seq_for_timestamp/3` and `timestamp_for_seq/3` use sampled points in the
  manifest; they are approximate.
- Database and snapshot handles can be shared across processes. Use a
  transaction or iterator from one process at a time.

## Unsupported SlateDB APIs

Compared with SlateDB's official UniFFI bindings, this binding does not expose:

- merge operators and compaction filters written in Elixir
- filter policies, segments and prefix extractors
- the WAL reader
- submitting arbitrary compaction specs, reading compaction state, and
  `delete_db`

SlateDB calls custom merge operators, compaction filters and filter policies
synchronously from its own threads during reads, flushes and compactions,
where no Elixir process is waiting. A NIF cannot call an Elixir function, so
it would have to send a message and block the Tokio thread until a reply
arrives. This deadlocks if the replying
process is itself waiting on the database. The binding provides built-in
operators and a prefix filter that you configure from Elixir. Logs and
metrics can be sent to Elixir without waiting for a reply.

## Build and test

Follow the repository's
[development setup](https://github.com/mjrusso/slap#development), then run
the commands in this section from `slap_slatedb/`.

```sh
export SLAP_SLATEDB_BUILD=1
mix test
mix test --include slow --include crash
```

The `:slow` test checks that slow I/O does not block schedulers. The crash
test also runs against S3 when `SLAP_TEST_S3_ENDPOINT` is set.

SlateDB's own log records are off during tests. Set
`SLAP_TEST_LOG_LEVEL=warning` (or `debug`, `info`, `error`) to see them.

The `:s3` tests run against a real S3-compatible server when
`SLAP_TEST_S3_ENDPOINT` is set. They check S3 configuration from the
environment, durable writes, fencing through conditional puts, and clone and
checkpoint retention through garbage collection. With
[RustFS](https://github.com/rustfs/rustfs) in Docker:

```sh
docker run -d --name rustfs -p 9000:9000 \
  -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin rustfs/rustfs
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user rustfsadmin:rustfsadmin \
  -X PUT http://127.0.0.1:9000/slatedb-test
SLAP_TEST_S3_ENDPOINT=http://127.0.0.1:9000 mix test
```

`SLAP_TEST_S3_BUCKET`, `_REGION`, `_KEY` and `_SECRET` override the
defaults (`slatedb-test`, `us-east-1`, `rustfsadmin`, `rustfsadmin`).

The retention suite tests SlateDB **0.17.0** on local directories and RustFS.
It covers checkpoint deletion and expiry, removal of the temporary checkpoint
`clone/3` creates, clones of clones, removal of SlateDB's checkpoints from
sources, the WAL data copied into clones, and source writes before and after
cloning. It compares full scans from fresh readers with caches disabled,
checks that specific obsolete SSTs are deleted from each source database, and
checks that a checkpoint of a clone keeps the source's checkpoint until it is
deleted. GC runs with writers and readers closed. The suite does not cover
removing ancestor storage, GC during writes, readers used after checkpoint
expiry, other S3 implementations, or other SlateDB versions. Local cases run
in ordinary checks; setting the S3 endpoint also enables the RustFS cases:

```sh
mix test test/clone_gc_test.exs
SLAP_TEST_S3_ENDPOINT=http://127.0.0.1:9000 mix test test/clone_gc_test.exs
```

The `:stress` cases require a durable source write to finish during the clone
call. That completion depends on scheduling and I/O latency, so these cases
are excluded from ordinary checks. Include the local stress case, or both
stores with RustFS configured:

```sh
mix test test/clone_gc_test.exs --include stress:local
SLAP_TEST_S3_ENDPOINT=http://127.0.0.1:9000 mix test test/clone_gc_test.exs --include stress
```

The `stress` input on a manual run of the `slap_slatedb` workflow enables both
local and RustFS stress cases in CI.

The fixtures close writers, compactors and readers before running GC with
zero minimum object ages. They keep boundary files enabled and WAL fence
collection disabled. These GC settings are for controlled tests.

The `:azure` test runs against [Azurite](https://github.com/Azure/Azurite)
when `SLAP_TEST_AZURITE` is set:

```sh
docker run -d --name azurite -p 10000:10000 mcr.microsoft.com/azure-storage/azurite \
  azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck --loose
python3 scripts/azurite_container.py slatedb
SLAP_TEST_AZURITE=1 mix test
```

There is no GCS integration test. object_store uses GCS's XML API, and each
emulator tested (fake-gcs-server, storage-testbench, floci-gcp and localgcp,
as of October 2026) lacks at least one required behavior: XML-API uploads,
generation preconditions, or the `ETag` and `x-goog-generation` response
headers. Tests cover GCS store configuration only.

## Benchmarks

Run these scripts from `slap_slatedb/` after the development setup above.

Run `MIX_ENV=bench mix run bench/micro.exs` to measure binding overhead on the
in-memory store. `mix run bench/scenarios.exs` measures durable writes, reads,
and scans against `SLAP_BENCH_STORE=memory|local|s3`; S3 uses
`SLAP_BENCH_S3_ENDPOINT`. `mix run bench/sweeps.exs` measures latency and PUT
rate across flush intervals and shard counts. The scripts write results under
`bench/results/`.

A durable write waits for a WAL flush. Lowering `flush_interval` can reduce
latency but increases object-store PUTs; each active database can issue up to
one WAL PUT per interval. Measure against the store and machine you intend to
use before choosing the interval and shard count.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
