# slap_slatedb

`slap_slatedb` provides Elixir bindings for [SlateDB](https://slatedb.io) 0.16,
an embedded key-value engine that keeps its data in object storage, such as
S3. Your application opens a database and calls it directly; there is no
database server to run. SlateDB batches writes to limit object-store requests.
A database can also use a local directory or memory. The bindings are a Rust
NIF, built with [Rustler](https://github.com/rusterlium/rustler).

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
> Standalone server:
>
> - [slap](https://hexdocs.pm/slap/): an HTTP server for evaluating and
>   benchmarking `slap_streams` and `slap_kv`
<!-- /slap-preamble -->

## Installation

Add `slap_slatedb` to the dependencies in your application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_slatedb, "~> 0.1.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_slatedb/).

Prebuilt NIFs cover macOS and Linux, with glibc or musl, on arm64 and x86_64;
the package downloads the one for your platform, so you don't need Rust. On
other platforms, build the NIF from source: install Rust 1.91 or later, add
`{:rustler, "~> 0.38"}` to your dependencies, and set `SLAP_SLATEDB_BUILD=1`
when you compile.

## Example

Run this in `iex -S mix` with a fresh local directory.

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
  range and prefix scans; TTL on puts (enforced during compaction); and
  snapshots for stable reads.
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

## Build and test

You need Elixir 1.18 or later on OTP 25 or later. Building the NIF from
source also needs Rust 1.91 or later.

```sh
export SLAP_SLATEDB_BUILD=1
mix test
mix test --include slow --include crash
```

The `:slow` test checks that slow I/O does not block schedulers. The crash
test also runs against S3 when `SLAP_TEST_S3_ENDPOINT` is set.

The package depends on
[`rustler_precompiled`](https://github.com/philss/rustler_precompiled),
which downloads a prebuilt NIF for the platform from the GitHub release and
checks it against `checksum-Elixir.Slap.SlateDB.Native.exs`. With
`SLAP_SLATEDB_BUILD=1` it builds from source with `rustler` instead, as CI and
development do.

SlateDB's own log records are off during tests. Set
`SLAP_TEST_LOG_LEVEL=warning` (or `debug`, `info`, `error`) to see them.

The `:s3` tests run against a real S3-compatible server when
`SLAP_TEST_S3_ENDPOINT` is set. They check S3 configuration from the
environment, durable writes, and fencing through conditional puts. With
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

## How it works

I/O NIFs copy their arguments, start work on a shared Tokio runtime, and send
the result to the calling Elixir process. A panic becomes an `:internal` error
reply. Opening a store and converting large batches run off the normal
schedulers.

### Runtime threads

The Tokio runtime is shared by every SlateDB database in the VM. By default
it uses one worker thread per CPU. Set
`config :slap_slatedb, runtime_threads: 4` in `config/runtime.exs` or earlier,
or set `SLAP_SLATEDB_RUNTIME_THREADS=4` before starting the VM. Application
config takes precedence when it is not `nil`. `System.put_env/2` in
`config/runtime.exs` works too; a call in your application's `start/2` is too
late because dependencies start first. Invalid values fail startup. Restart
the VM to change the count.

More threads can help when many operations have work ready at once, but use
more memory and can add CPU contention. Fewer threads use fewer resources but
can limit concurrent work. Thread count does not remove object-store latency
or the wait for a durable WAL flush. Start with the default, then compare
throughput and latency on your hardware with your expected concurrency and
store. For example, run `mix run bench/scenarios.exs` once with the default
and again with `SLAP_SLATEDB_RUNTIME_THREADS=4`; each run starts a new VM.

`close/2` waits for calls in flight, then rejects new calls. Snapshots,
transactions, and iterators hold the database open. If the final handle is
dropped without `close/2`, the destructor closes it in the background and logs
a warning.

Binaries of at least 64 KiB share SlateDB buffers with Elixir. This avoids a
copy, but a small slice can keep a larger cached block alive; use
`:binary.copy/1` when retaining such a value.

A call with a timeout uses a helper process so late replies can be discarded.
The database operation continues after the caller times out. See
[Durability and write ordering](#durability-and-write-ordering) before using
timeouts with writes.

## Object stores

SlateDB uses conditional writes to stop a second writer, so an object store
must support them. S3, Azure Blob Storage, and RustFS do. Check other stores,
including Google Cloud Storage, with `probe_store/2` before use; see the notes
below.

The `:store` option of `open/2` selects where the database lives:

```elixir
# In memory, lost when the VM exits.
Slap.SlateDB.open("my-db", store: :memory)

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
- **Conditional puts are always on.** SlateDB fences a second writer with
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

Direct `put`, `delete`, `merge`, `increment`, and `write` calls return `{:ok,
seq}`. A write is durable once `durable_seq/1` is at least its `seq`. So a
process can write with `await_durable: false` and confirm many writes at once
from one `{:slap_slatedb_durable, ref, tag, durable_seq}` message, instead of
waiting on each write.

Each call waits for its reply, and SlateDB assigns the sequence number before
the reply is sent. So **one process's writes are applied in the order it makes
them**, each with a higher `seq`. Writes from different processes at the same
time have no defined order between them. Only code that calls the NIF
directly, without waiting for replies, could reorder one
process's writes.

**Timeouts break this.** A write that returns `{:error, %{kind: :timeout}}`
is still running and may be applied after the process's next write. If your
code depends on write order, do not pass `:timeout` to writes (the default
is `:infinity`), and treat any write error as the end of the handle: stop,
reopen, and reload state from storage.

A fenced writer detects the fence when it next writes or flushes, or when it
next polls the manifest (every `manifest_poll_interval`, 1 s by default).
Subscribers then get `{:slap_slatedb_closed, ref, tag, :fenced}`, so an idle
owner finds out that it lost ownership without writing.

Closing a fenced handle returns `:ok` if the handle had already detected the
fence. If the fence is detected during the close, `close/2` returns
`{:error, %{kind: :closed, reason: :fenced}}`, because writes that were not
yet durable are lost. Treat both as
a normal shutdown of a writer that lost ownership.

## Benchmarks

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

## Limits and caveats

- SlateDB 0.16 enforces TTL during compaction, not on reads. An expired row
  may still be returned; `get_key_value/3` includes its `expire_ts` for an
  exact cutoff.
- Keys longer than 65,535 bytes are rejected with `:invalid`.
- Numeric merge operators require an eight-byte base value and the same
  `:merge_operator` on every process that opens the database. An invalid base
  can fail reads and compaction. A batch cannot merge the same key with
  different TTLs.
- A compaction filter deletes keys only when compaction rewrites them.
  `Slap.SlateDB.Admin.compact/2` can force that work.
- A `Slap.SlateDB.Reader` sees durable writes after its next manifest poll
  (its own `manifest_poll_interval`, 10 s by default, unlike the writer's
  1 s).
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

Custom merge operators, compaction filters and filter policies are left out
on purpose. SlateDB calls them synchronously from its own threads, during
reads, flushes and compactions, where no Elixir process is waiting. A NIF
cannot call an Elixir function, so it would have to send a message and block
the Tokio thread until a reply arrives. That is slow, and it deadlocks if the
replying process is itself waiting on the database. So this binding offers
built-in operators and a prefix filter, chosen and configured from Elixir,
instead. Logs and metrics avoid the problem because they go to Elixir
without waiting for a reply.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
