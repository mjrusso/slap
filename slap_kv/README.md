# slap_kv

`slap_kv` provides a partitioned key-value store, with data stored durably in
object storage.

`slap_kv` is built on [`slap_cluster`](https://hexdocs.pm/slap_cluster/) and
[`slap_slatedb`](https://hexdocs.pm/slap_slatedb/). Each row is addressed by a
partition and a key. A partition's rows are stored together, in key order, in
one of a fixed number of SlateDB databases, called shards. Writes are
acknowledged once they are durable, reads return only durable rows, and a write
can be made conditional on a row's version.

Use `slap_kv` when you need records grouped by a partition, such as a user's
settings or a document's metadata. It routes each partition to a shard,
supports listing keys in order, and uses versions to detect concurrent updates.
This saves you from building shard ownership and conditional writes on top of
`slap_slatedb`. If you need direct SlateDB transactions or a different key
layout, use `slap_slatedb` instead. `slap_kv` does not support cross-partition
scans or cross-partition transactions.

`slap_kv` runs inside your application, on one node or several. You start
`Slap.KV.Cluster`, call `Slap.KV` in-process, and can mount
`Slap.KV.HTTP.Router` in your [`Plug`](https://hexdocs.pm/plug/) pipeline. A
call for a partition goes to the node that owns its shard. The
[`slap`](https://hexdocs.pm/slap/) package's
[standalone server](https://hexdocs.pm/slap/standalone-server.html) script
serves `slap_kv` over HTTP.

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

To depend on `slap_kv` directly, add it to the dependencies in your
application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_kv, "~> 0.6.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_kv/).

## Example

In an application that depends on `slap_kv`, run this in `iex -S mix`
with a fresh local directory:

```elixir
iex> {:ok, _} = Slap.KV.Cluster.start_link(store: {:local, "/tmp/slap-kv"}, path: "kv", shards: 1); :ok
:ok

iex> {:ok, v1} = Slap.KV.put("user:42", "profile", "Ada", if_version: :absent); :ok
:ok
iex> {:ok, %{value: value, version: ^v1}} = Slap.KV.get("user:42", "profile"); value
"Ada"
iex> {:error, {:conflict, ^v1}} = Slap.KV.put("user:42", "profile", "other", if_version: :absent); :conflict
:conflict
iex> {:ok, v2} = Slap.KV.put("user:42", "profile", "Ada Lovelace", if_version: v1); :ok
:ok
iex> {:ok, %{rows: rows, cursor: nil}} = Slap.KV.scan("user:42", prefix: "pro"); rows
[{"profile", "Ada Lovelace"}]
iex> :ok = Slap.KV.delete("user:42", "profile", if_version: v2)
:ok
```

For local storage, see [choosing the shard count](https://hexdocs.pm/slap_cluster/readme.html#choosing-the-shard-count);
before running a script against a store an application is using, see
[running another cluster on the same store](https://hexdocs.pm/slap_cluster/readme.html#running-another-cluster-on-the-same-store).

An application can define its own cluster module and pass it to KV calls
and the HTTP router:

```elixir
defmodule MyApp.KVCluster do
  use Slap.KV.Cluster, otp_app: :my_app
end

children = [{MyApp.KVCluster, store: {:local, "/tmp/slap-kv"}, shards: 1}]
```

Add these children to your supervision tree. Once it is running, pass the
cluster module to each operation:

```elixir
{:ok, _} = Slap.KV.put("users", "42", "Ada", cluster: MyApp.KVCluster)
```

Supervise one KV cluster per VM. A Streams cluster can run on the same VM.
Run one `Slap.Cluster.Peers` per VM; its connections to other nodes serve
every cluster on that VM.

A failed store probe normally prevents the cluster and its supervising
application from starting. With `probe: :background`, the cluster starts
without shards and retries the probe. Operations return
`{:error, :unavailable}` until the shards open.

Before boot work that needs every shard, call `Slap.KV.Cluster.await_ready(30_000)`
and handle `{:error, :timeout}`. `Slap.KV.Cluster.status()` reports current
readiness and the latest failures without accessing storage; see
[cluster readiness](https://hexdocs.pm/slap_cluster/Slap.Cluster.html#status/2).

## Flush interval

A durable write waits for SlateDB to flush its write-ahead log (WAL) to
object storage. `Slap.KV.Cluster` sets `flush_interval` to 10 ms; SlateDB's
default is 100 ms. Pass `settings: %{flush_interval: "50ms"}` to change it.
Other settings leave the 10 ms default intact.

Lower intervals reduce the wait for acknowledgements but increase
object-store PUTs. Each shard with pending writes can issue up to one WAL PUT
per interval; idle shards issue none. Choose the interval for the expected
write rate, shard count, and store pricing. For `:memory` and `{:local, dir}`
tests, `settings: %{flush_interval: "2ms"}` reduces the wait without request
charges.

## Semantics

- **Versions.** A row's version is the SlateDB sequence number of the
  write that stored it, so it changes with every write and only increases
  within a shard. `put/4` and `delete/3` accept `if_version: version`, and
  `put/4` also accepts `if_version: :absent`. When the condition fails, the
  call returns `{:error, {:conflict, current}}`, where `current` is the row's
  current version, or `nil` if the row does not exist.
- **Durability.** A write returns once SlateDB reports it durable. Reads
  (`get/3`, `scan/2`) use SlateDB's `:remote` durability: they see only
  durable rows, so they never return a write that could still be lost.
- **Uncertain writes.** After an `:unavailable` or `:timeout` reply, the
  write may still be applied; nothing retries it on its own. Pass
  `deadline:` with the write: once the deadline plus the largest difference
  between the nodes' clocks has passed, the write can no longer be applied.
  A read with `consistency: :linearizable` then returns a state that the
  write can no longer change. The row alone shows whether the write was
  applied only if no other writer could have changed it. To tell in every
  case, write a receipt in the same batch: a row under a key unique to the
  request, in the same partition, that nothing else deletes. After the
  deadline, the write was applied if and only if a linearizable read finds
  the receipt.
- **Scans.** Each page reads one consistent view of the durable rows.
  Pages are separate reads, so rows written between pages can appear in
  later pages if their keys come later. Passing `cursor: nil` starts at the
  beginning of the selected range; a returned `cursor: nil` means iteration
  is complete. A binary cursor identifies the last key returned and remains
  valid after that row is deleted. Supplied `:prefix`, `:gte`, and `:lt`
  values must be binaries.
  Scan rows omit versions by default; `with_versions: true` includes them.
  There are no scans or writes across partitions.

## Atomic batches

`Slap.KV.batch/3` applies operations on distinct keys in one partition atomically:

```elixir
{:ok, version} = Slap.KV.batch("user:42", [
  {:put, "profile", "Ada", :absent},
  {:put, "theme", "dark"},
  {:delete, "draft"},
  {:check, "disabled", :absent}
])
```

The rules for operations:

- A put or delete may end with a version condition: an integer version, or,
  for a put, `:absent`.
- A check must have a condition: an integer version or `:absent`.
- Each key may appear only once in a batch, so a key cannot be both checked
  and changed. A condition of `nil` is invalid.
- Conditions are checked in list order against the rows as they were before
  the batch. The first one that fails returns
  `{:error, {:conflict, key, current_version_or_nil}}`, and nothing is
  written.

A batch that puts or deletes rows returns `{:ok, version}`, and every row it
puts gets that version. A batch of only checks returns `{:ok, nil}`; like a
conflict, it replies only after a write of its own confirms that its node
still owns the shard. Options are `:deadline`, `:timeout` and `:cluster`, as
for single writes. A batch is not applied at or after its deadline. A timeout
does not cancel the batch: after `:timeout` or `:unavailable`, resolve its
outcome as for single writes (see [Uncertain writes](#semantics)) before
retrying.

## Limits on pending work

Each shard limits how much work can wait for durability at once. A write,
batch or linearizable get counts against the limits from the moment its
partition writer accepts it until its result is durable and it has replied.
A request that would exceed them is rejected before any row is read or
written. Set the limits as
positive integers in the cluster's `child_options`:

```elixir
child_options: [
  partition_writers: 16,
  max_inflight_bytes_per_writer: 64 * 1024 * 1024,
  max_inflight_bytes_per_shard: 256 * 1024 * 1024,
  max_inflight_requests_per_writer: 1_024,
  max_batch_ops: 1_000,
  max_batch_bytes: 8 * 1024 * 1024
]
```

These are the defaults. A request's size in bytes is the sum, over its
operations, of the encoded key (two length bytes, the partition and the key)
and, for a put, the value. A batch counts as one request. A linearizable get
counts as one request of zero bytes. Durable gets and scans are not limited.

- A request larger than either byte limit returns
  `{:error, {:bad_request, :request_too_large}}`, even when nothing else is
  pending.
- A batch with more than `max_batch_ops` operations or `max_batch_bytes`
  bytes returns `{:error, {:bad_request, :batch_too_large}}`.
- A request that would fit once pending work finishes returns
  `{:error, :overloaded}`.

In all three cases the request was not applied.

A caller's timeout or exit does not free the work's capacity: the work is
still pending. If a partition writer crashes, its pending writes count until
they are durable or the shard's database closes. If it crashes while
submitting a write to SlateDB, so that whether the write was submitted is
unknown, the shard stops, and it accepts requests again only after its
database has closed.

These limits bound pending request bytes and counts. They do not bound
process mailboxes, HTTP decoding, distribution queues, response bodies, or
total BEAM or native memory. `Slap.KV.Telemetry` reports rejections and
pending work.

## How it works

- **Placement.** A partition is on shard `xxh64(partition) mod N`
  (`Slap.KV.Cluster.shard_for/1`). Operations run on the shard's owner
  through `Slap.Cluster.call/4`.
- **Writers.** Each shard's database has one SlateDB writer: the handle its
  owner opened. `slap_cluster` places the owner on one node, and SlateDB
  fences any other. On that node, each partition has one partition writer,
  the only process that writes the partition's rows. Each shard runs
  `partition_writers` partition writers (a cluster `child_options` key, 16 by
  default), and all of them write through the shard's one handle. A partition
  belongs to the partition writer
  `:erlang.phash2(partition, partition_writers)`. A shard reads the setting
  when it starts its partition writers, and keeps that count until it starts
  them again.
- **Conditions and pending writes.** A partition writer checks a condition with
  a normal read, which sees its own writes even before they are durable. It
  writes without waiting and replies once the write is durable, so several
  writes can be in flight at once. Replies go out in request order, behind the
  writes in flight.
- **Conflicts are confirmed.** Under a placement without leases, a node can
  lose a shard after a pause without knowing it. Its partition writers then
  judge conditions from old state. So before reporting a conflict, a
  partition writer writes a reserved key and waits for that write to be
  durable. On a fenced handle the write fails, and the request gets
  `:unavailable` instead of a conflict that may be wrong. Successful writes
  need no confirmation, because they fail on a fenced handle anyway. Reads on
  such a node may be stale until it finds that it has been fenced, as with
  streams.
- **Writer recovery.** A write error fails every request in flight with
  `:unavailable` and stops the partition writer. Its supervisor starts a new
  one, which first waits until every write made through the shard's database
  is durable. So the new writer never checks a condition against state that
  could still be lost.

## HTTP

`Slap.KV.HTTP.Router` serves KV over HTTP, under `/v1/kv` by default. To try
it, download and run the
[standalone server](https://hexdocs.pm/slap/standalone-server.html) script
from [`slap`](https://hexdocs.pm/slap/) in another terminal:

```sh
curl -fsSLo server.exs https://raw.githubusercontent.com/mjrusso/slap/main/slap/examples/server.exs
elixir server.exs
```

It keeps its data in memory, which is lost when the server stops. Then:

```sh
curl -X PUT --data-binary 'hello' -H 'If-None-Match: *' -i http://localhost:4437/v1/kv/p/greeting
curl -i http://localhost:4437/v1/kv/p/greeting
curl 'http://localhost:4437/v1/kv/p?prefix=gr&limit=10'
```

The PUT returns 204 and an `ETag` containing the new version; the GET
returns 200 with the value and the same `ETag`. To replace or delete the
row, send that value in `If-Match`. A stale version returns 412 with the
current `ETag`.

Partitions and keys are percent-encoded path segments. In scan responses,
keys, values, and the cursor are unpadded base64url. The router does not
authenticate requests.

Size errors return 413 without `Retry-After`. A 503 response with body
`overloaded: request was not applied` is a definite rejection and includes
`Retry-After: 1`. Other 503 responses and transport failures leave the write's
outcome unknown.

For an atomic batch, send JSON with unpadded base64url keys and values:

```sh
curl -i -X POST http://localhost:4437/v1/kv/p \
  -H 'Content-Type: application/json' \
  --data '{"ops":[{"op":"put","key":"YQ","value":"MQ","if_absent":true},{"op":"delete","key":"Yg"}]}'
```

Operations are `put` (key and value), `delete` (key), or `check` (key and a
required condition). Conditions are `if_version: N` or `if_absent: true`;
`if_absent` is allowed only for put and check. A batch is rejected if it has
unknown fields, an operation has both conditions, or the request has an
`If-Match` or `If-None-Match` header. The only optional
top-level field is integer `deadline` in Unix milliseconds.

Mutation success is 200 with `{"version": N}` and ETag `"N"`. Check-only success
is 200 with `{"version": null}` and no ETag. A conflict is 412 with
`{"key": "base64url", "version": N}` (`null` when missing). Passed batch deadlines
return 400 with `deadline_exceeded`.

The router's `max_batch_body` bounds raw JSON (16 MiB by default), separately
from owner-enforced decoded limits. `max_value` also bounds each decoded put
value (8 MiB by default).

## Storage

A row's SlateDB key is the partition's length (16 bits), the partition,
then the key, so a partition is one contiguous key range
and scans never cross into another partition. Keys may be up to SlateDB's
limit of 65,535 bytes once encoded. Deletes are SlateDB deletes
(tombstones, removed by compaction).

## Limits

- **A conditional write of a row that is not in memory waits for a read from
  the store**, and its partition writer's other writes wait behind it.
  Unconditional writes do not read. Partition writers are independent, so
  this delays only the partitions that share that writer; see
  [Benchmarks](#benchmarks).
- **No TTLs or cross-partition operations.**

## Benchmarks

Follow the repository's
[development setup](https://github.com/mjrusso/slap#development), then run
from `slap_kv/`:

```sh
mix run bench/partition_writers.exs --store memory
```

`bench/partition_writers.exs` measures throughput, latency, and writer mailbox
length with unconditional, warm conditional, and cold conditional writes. A
cold conditional write waits for a store read and blocks its partition writer;
increasing `partition_writers` spreads that delay across partitions. Measure
on the intended store before changing the default of 16.

`bench/durability_admission.exs` runs 64 bounded clients by default, mixing
64 KiB puts, tiny puts, failed conditions and linearizable gets. It reports
elapsed time, request outcomes, reserved bytes/counts, mailbox samples,
BEAM memory and SlateDB's native buffer metrics as JSON. Native buffer gauges
cover only the memory SlateDB reports; they are not total native allocations.
Use an externally delayed object store to measure slow durability. The report
also includes the result of a final write after the clients finish.

```sh
mix run bench/durability_admission.exs --store s3://bucket/benchmark --flush-interval 10ms
```

## Tests

Run from `slap_kv/` after the development setup above:

```sh
mix test          # SLAP_KV_PROP_RUNS=1000 for more model sequences
```

The model test covers atomic batches, conditional writes, scans, writer
replacement, and cluster restarts. `test/durability_test.exs` checks durable
replies, pending writes, and fencing. Set `SLAP_KV_PROP_RUNS` to run more model
sequences.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
