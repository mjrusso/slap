# slap_kv

`slap_kv` provides a partitioned key-value store that persists data using
SlateDB, built on [`slap_cluster`](https://hexdocs.pm/slap_cluster/) and
[`slap_slatedb`](https://hexdocs.pm/slap_slatedb/). Rows are addressed by a
partition and a key; a partition's rows are on one shard, in key order. Writes
are acknowledged once durable, reads return only durable rows, and writes can
be conditional on a row's version.

Use `slap_kv` when you need records grouped by a partition, such as a user's
settings or a document's metadata. It routes each partition to a shard, lets
you list its keys in order, and uses versions to detect concurrent updates.
This saves you from building shard ownership and conditional writes on top of
`slap_slatedb`. If you need direct SlateDB transactions or a different key
layout, use `slap_slatedb` instead. `slap_kv` has no cross-partition scan or
transaction.

`slap_kv` is meant to be embedded: your application starts `Slap.KV.Cluster`,
calls `Slap.KV` in-process, and can mount `Slap.KV.HTTP.Router` in its
[`Plug`](https://hexdocs.pm/plug/) pipeline. The
[`slap`](https://hexdocs.pm/slap/) package runs `slap_kv` as a standalone
server (`mix slap.server --kv --store memory`).

## Installation

Add `slap_kv` to the dependencies in your application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_kv, "~> 0.1.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_kv/).

## Example

In an application that depends on `slap_kv`, run this in `iex -S mix`
with a fresh local directory:

```elixir
iex> {:ok, _} = Slap.KV.Cluster.start_link(store: {:local, "/tmp/slap-kv"}, path: "kv", shards: 8); :ok
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

An application can define its own cluster module and pass it to KV calls
and the HTTP router:

```elixir
defmodule MyApp.KVCluster do
  use Slap.KV.Cluster, otp_app: :my_app
end

children = [{MyApp.KVCluster, store: {:local, "/tmp/slap-kv"}, shards: 8}]
{:ok, _} = Slap.KV.put("users", "42", "Ada", cluster: MyApp.KVCluster)
```

Supervise one KV cluster per VM. A Streams cluster can run on the same VM.
Run one `Slap.Cluster.Peers` per VM; its connections to other nodes serve
every cluster on that VM.

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
- **Uncertain writes.** An `:unavailable` or `:timeout` reply means the
  write may still become durable. Read the row to find out; nothing retries
  a write on its own.
- **Scans.** Each page is a separate read of durable rows, not a snapshot
  across pages. The opaque cursor identifies the last key returned. Scan
  rows omit versions by default; `with_versions: true` includes them. There
  are no scans or writes across partitions.

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
it, run the standalone server from [`slap`](https://hexdocs.pm/slap/) in
another terminal:

```sh
mix slap.server --kv --store memory --kv-shards 4
```

The memory store loses its data when the server stops. Then:

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
keys, values, and the cursor are unpadded base64url. A 503 response (with
`Retry-After`) means that the shard was unavailable or the request timed out;
a write may still have been applied. The router does not authenticate
requests.

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
- **No batches, TTLs or cross-partition operations** yet.
- **No backpressure:** a shard's writes in flight are not capped.

## Benchmarks

`bench/partition_writers.exs` measures throughput, latency, and writer mailbox
length with unconditional, warm conditional, and cold conditional writes. A
cold conditional write waits for a store read and blocks its partition writer;
increasing `partition_writers` spreads that delay across partitions. Measure
on the intended store before changing the default of 16.

## Tests

```sh
mix test          # SLAP_KV_PROP_RUNS=1000 for more model sequences
```

The model test covers conditional writes, scans, writer replacement, and
cluster restarts. `test/durability_test.exs` checks durable replies, pending
writes, and fencing. Set `SLAP_KV_PROP_RUNS` to run more model sequences.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
