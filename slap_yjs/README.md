# slap_yjs

`slap_yjs` stores [Yjs](https://yjs.dev) documents in Durable Streams
([`slap_streams`](https://hexdocs.pm/slap_streams/)), as snapshot logs
([`slap_snapshot_log`](https://hexdocs.pm/slap_snapshot_log/)), and shares
presence between the nodes that serve each document.

Use `slap_yjs` when an Elixir application uses `y_ex` for collaborative
documents and needs a durable update history shared by document servers on
different nodes. Your application provides the client connection, forwards
sync and awareness messages, and handles authentication. The package does not
start a WebSocket or HTTP server. For a log of application-defined changes
rather than Yjs updates, use `slap_snapshot_log`.

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

Add `slap_yjs` to the dependencies in your application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_yjs, "~> 0.1.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_yjs/). This package requires OTP 26.2 or
later (see [Requirements](#requirements)).

## Example

In an application that depends on `slap_yjs`, add these children to its
supervision tree, in this order:

```elixir
children = [
  {Slap.Streams.Cluster, store: {:local, "/tmp/slap-yjs"}, shards: 8},
  Slap.Yjs.Docs
]
```

For an application-owned Streams cluster, use the pattern in the
`slap_streams` README and pass `cluster: MyApp.StreamsCluster` when starting
`Slap.Yjs.Docs` and calling `Slap.Yjs.Store`. Run one Streams cluster per VM.

Define a document server module:

```elixir
defmodule MyApp.DocServer do
  use Slap.Yjs.DocServer
end
```

With the supervision tree running, join the document from IEx, which acts as
the client process:

```elixir
iex> {:ok, server} = Slap.Yjs.Docs.join(MyApp.DocServer, {"my-service", "doc-1"}); :ok
:ok
iex> is_reference(Process.monitor(server))
true

iex> :ok = Slap.Yjs.DocServer.sync(server, 5_000)
:ok
iex> {:ok, snapshots} = Slap.Yjs.Store.snapshots({"my-service", "doc-1"}); is_list(snapshots)
true
```

Pass each Yjs sync or awareness message received from the client to
`MyApp.DocServer.process_message_v1(server, message, self())`.
`sync/2` waits for local updates to be stored and for the server to catch up
with other nodes.

## Client behavior

- A client joins its node's document server, sends its sync and awareness
  messages to it, and receives `{:slap_yjs_update, doc_id, update}` and
  `{:slap_yjs_awareness, doc_id, update}` from any node. The server stops
  after 30 s without subscribers. If the client's monitor of the server
  reports `:DOWN`, the client joins again and resyncs.
- Awareness (presence) is not stored. A client's awareness state is removed
  when the client exits, and a server's when the server or its node stops.
- An append that fails for a reason other than a transient error
  stops the server with `{:slap_yjs_append_failed, reason}`. Clients join
  again and resend their state.

## How it works

- **Layout.** `Slap.Yjs.Store` keeps a document in the reference
  `y-durable-streams` server's layout, with retained-history metadata in the
  snapshot index, so its HTTP clients can read the same documents later:

  ```
  /v1/stream/yjs/<service>/docs/<doc>/.updates                     lib0-framed updates
  /v1/stream/yjs/<service>/docs/<doc>/.index                       snapshot metadata or a deletion marker
  /v1/stream/yjs/<service>/docs/<doc>/.snapshots/<offset>_snapshot a snapshot published at <offset>
  ```

  Snapshot metadata contains `"snapshotOffset"`, `"createdAt"`, and
  `"retained"`. A stream's placement key is its path up to the first segment
  that starts with `.`, so a document's streams are on one shard.
- **One server per node, following the stream.** Each node with clients for
  a document runs a server for it (`Slap.Yjs.Docs`); no server owns the
  document. A server loads the current snapshot and the updates after it. It
  appends its clients' updates to `.updates`, and follows `.updates` through
  `Slap.SnapshotLog.next/3`, applying every update, including its own. Yjs
  updates commute, and applying one twice has no effect, so every server
  converges on the same state. When a shard moves, the servers stay where
  they are: appends and reads get `:unavailable` briefly and are retried.
- **Awareness.** A document's servers form a `:pg` group, monitor it, and
  send each other their clients' awareness updates; a server that joins
  gets the others' current states.
- **Batching.** A server frames updates (`Slap.Yjs.Frame`) into a buffer and
  appends the buffer after 10 ms, or sooner once it holds 256 KiB. One append
  runs at a time; updates buffered meanwhile go in the next one. A failed
  append is retried while the shard is unavailable.
- **Pending updates.** An update that depends on one the server has not seen
  yet waits in the document as pending. yrs does not retry a pending update
  when a later update fills the gap, so the server applies pending updates
  again after the stored updates it reads. What a client's pending update
  adds is then appended. Snapshots include updates that are still pending.
- **Compaction.** A server publishes the document's state as a snapshot
  when the updates it has read since the last snapshot reach
  `max(1 MiB, half the snapshot's size)`, and when it stops. The snapshot is
  at the offset the server has read up to. Publishing it
  (`Slap.SnapshotLog.snapshot/4`) indexes it, deletes the snapshots that are
  no longer kept, and trims `.updates` and `.index`. Each server runs one
  compaction at a time, in a task.

  The state can contain more than the updates before that offset: pending
  updates, and local updates not appended yet. The snapshot log allows this
  for entries that are safe to apply twice, as Yjs updates are. Servers on
  several nodes may compact at once, and any step can be interrupted;
  `Slap.SnapshotLog` explains why both are safe.
- **History.** A policy such as "hourly for a week, daily for 90 days"
  (`{every_ms, keep_ms}` rules) keeps the latest snapshot of each period.
  `Slap.Yjs.Store.snapshots/1` lists them (`Slap.SnapshotLog.snapshots/1`).
- **Shutdown.** A normal stop appends what is buffered, waits for the
  appends, and compacts. Start `Slap.Yjs.Docs` after `Slap.Streams.Cluster`,
  so that the servers stop first.

## Requirements

OTP 26.2 or later (`mix.exs` checks it): the document servers monitor
their `:pg` group, and before 26.2 a process that both monitors and joins
a group crashes the `:pg` scope when it exits.

## Tests

```sh
mix test
```

Set `SLAP_TEST_S3_ENDPOINT` and `SLAP_TEST_S3_BUCKET` to include the RustFS
tests. The multi-node convergence test uses `:peer` and needs `epmd`;
`SLAP_YJS_PROP_SEED` replays its edit sequence.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
