# slap_files

`slap_files` is a service for storing files in object storage.

A file has a ref, `{partition, id}`, such as `{document_id, attachment_id}`; a
body (its contents); and a content type, size, checksum, and your own metadata.
You can list the files in a partition, such as a document's attachments, and
make a replacement conditional on the file not having changed since you read
it. When a file is replaced or deleted, its old body is deleted for you, and so
are objects left behind by an interrupted upload or delete.

Each file is one record in [`slap_kv`](https://hexdocs.pm/slap_kv/). With the
default `storage: :auto`, a body up to 16 KiB is stored in the record, and a
larger one is uploaded as a separate object (with the record holding the
object's key). SlateDB batches `slap_kv` writes into shared PUTs, so storing
many small files takes fewer PUTs than storing each one as its own object.

Use `slap_files` when files are replaced or deleted while other processes or
nodes may be reading them, and you want their metadata, listing, conditional
replacement, and cleanup handled for you. If object keys are enough for your
application and you already handle those, use object storage directly instead
of `slap_files`.

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

Add `slap_files` to the dependencies in your application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_files, "~> 0.1.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_files/).

## Example

In an application that depends on `slap_files`, start `Slap.KV.Cluster`
before `Slap.Files`. For example, in `iex -S mix` with a fresh local
directory:

```elixir
iex> store = {:local, "/tmp/slap-files"}
{:local, "/tmp/slap-files"}
iex> {:ok, _} = Slap.KV.Cluster.start_link(store: store, path: "kv", shards: 8); :ok
:ok
iex> {:ok, _} = Slap.Files.start_link(store: store, path: "files"); :ok
:ok

iex> ref = {"doc-42", Slap.Files.new_id()}; :ok
:ok
iex> {:ok, %Slap.Files.File{version: v1, storage: :inline}} =
...>   Slap.Files.put(ref, "hello", content_type: "text/plain", if_version: :absent); :inline
:inline
iex> Slap.Files.read(ref)
{:ok, "hello"}

iex> body = :binary.copy("x", 20_000); byte_size(body)
20000
iex> {:ok, %{version: v2, storage: :object}} = Slap.Files.put(ref, body, if_version: v1); :object
:object

iex> {:ok, ^body} = Slap.Files.read(ref); byte_size(body)
20000
iex> {:ok, %{files: files, cursor: nil}} = Slap.Files.list("doc-42"); length(files)
1
iex> :ok = Slap.Files.delete(ref, if_version: v2)
:ok
```

In an application, put both processes in its supervision tree in the same
order. `Slap.Files.stream/1` returns the metadata and a lazy body stream.

## API

- `put(ref, body, opts)` creates or replaces a file and returns it once it is
  durable. `body` is a binary, or an enumerable of binaries that is read once
  (such as a request body). Options:
  - `if_version:`: `:absent`, or the version the file must have
  - `storage:`: `:auto` (the default), `:inline`, or `:object`
  - `content_type:` and `metadata:` (a map of strings to strings)
  - `expected_sha256:`: the SHA-256 the body must match

  Without `if_version:`, a put with the same body, content type, metadata, and
  storage mode returns the existing file instead of creating a new version.
  With `if_version:`, the version is checked even when the content is
  identical.
- `get(ref)`, `read(ref)`, `stream(ref)`: metadata, the whole body, or the
  body as a lazy stream.
- `list(partition, prefix:, gte:, lt:, limit:, cursor:)`: a partition's
  files in id order, with their versions.
- `delete(ref, if_version:)`.

A partition is a stable group of files, such as a document's attachments,
that is listed together and lives on one `slap_kv` shard. There is no listing
across partitions, so to find files that nothing references, an application
lists the partitions it knows.

## Storage

The default instance uses the `"default"` namespace in `slap_kv` partitions and
object keys. Named instances require an explicit, stable `namespace:`.

- `storage: :auto` stores a body inline up to `inline_max_bytes` (default
  16 KiB), and as an object above it; `:inline` and `:object` force the
  choice. An inline body is at most `inline_limit` (default 1 MiB), since
  `get` and every page of `list` read the whole record.
- An inline file is one `slap_kv` row: a put or delete is one conditional
  write.
- An object body gets a new namespaced key on every put and is
  never overwritten. `ObjectStore.upload/4` streams it: one PUT below
  5 MiB, a multipart upload at 5 MiB or above.

## Cleanup

An old object body remains available for `retention_ms` (default 5 minutes)
after a replacement or delete, so a reader that already opened it can finish.
Interrupted uploads and writes may leave objects temporarily; background
sweeps and reconciliation remove them. Nodes' clocks must be within
`max_clock_skew_ms` (default 30 seconds) of each other for cleanup to be safe.

### How cleanup works

Files are hashed into 64 buckets. Each bucket has a `slap_kv` partition of
intents and a `slap_kv` partition of object registrations, and its objects
share a key prefix.

- An **intent** names the object keys of one file that may have to be
  deleted, and the time at which they become due.
- A **registration** records an object's key. It is written before the object
  exists.

The steps:

- **Upload.** Before an upload starts, the writer opens an intent that names
  the new object key, due after `upload_timeout_ms` (1 hour by default). It
  then registers the key and uploads the body. Once the body is written, the
  writer takes the intent: a conditional rewrite that makes it due after
  `retention_ms` and adds the old body's key, if there is one. Finally it
  points the file's record at the new body, with a conditional write whose
  `slap_kv` deadline is the intent's due time.
- **Replace or delete.** Before the record stops pointing to the old body, an
  intent names the old body's key, due after `retention_ms`. A reader that
  already has the old body can finish reading it.
- **Sweep.** Every `sweep_interval_ms`, each node runs `Slap.Files.Sweeper`
  over the intent partitions on the `slap_kv` shards it owns. The sweeper
  waits until an intent is `max_clock_skew_ms` (30 s by default) past its due
  time, when no record write the intent covers can still be applied. It then
  takes the intent (a conditional rewrite), reads the file's record with a
  linearizable read, deletes each object the intent names that the record
  does not point to, and deletes the intent.
- **Reconcile.** A store request can complete late, so an upload that was due
  may write its object after the sweep deleted it. The sweep deletes an
  object's registration only after the object, so such an object has no
  registration. Every `reconcile_interval_ms` (1 hour by default), each node
  lists the objects in the buckets on the `slap_kv` shards it owns, then their
  registrations, and deletes every object without one. Listing the objects
  first makes this safe, because a registration is written before its object
  exists.

A writer and the sweeper both take the intent before acting, so only one of
them acts. An upload slower than `upload_timeout_ms` loses to the sweep and
returns `{:error, :expired}`, instead of pointing a file at a deleted object.
A write or delete that fails part-way leaves an intent, and the sweep
finishes the cleanup. Reconciliation deletes anything an upload wrote too
late.

## Limits

- **Needs a running `Slap.KV.Cluster`.** Each upload adds `slap_kv` writes
  (an intent, a registration, and the record), and each node runs background
  sweeps and reconciliation.
- **Old bodies are kept only for `retention_ms`**, for reads in progress. There
  is no version history.
- **No HTTP file server or access control.**
- **No transactions with other databases.** If another database refers to a
  file, create the file before adding the reference, and remove the reference
  before deleting the file.

## Benchmarks

`bench/inline.exs` compares inline and object bodies on RustFS. Inline writes
need one durable `slap_kv` write; object writes also upload the body and manage
an intent. Inline bodies save object-store PUTs, but they increase record size
and make listings read more data. The total cost also depends on `slap_kv`
reads, writes, and compaction, and on your object store's pricing. Measure with
representative file sizes before changing `inline_max_bytes`.

## Tests

```sh
mix test          # SLAP_FILES_PROP_RUNS=500 for more model sequences
```

The model test covers writes, deletes, reads, sweeps, and clock changes.
`test/sweeper_test.exs` checks interrupted uploads and cleanup.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
