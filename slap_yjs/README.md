# slap_yjs

`slap_yjs` durably stores [Yjs documents](https://yjs.dev), and keeps each
document in sync across the nodes of an Elixir cluster. Yjs is a CRDT library:
its shared data types merge concurrent edits from many clients without
conflicts, and its protocol exchanges those edits between them. Each document
is stored as a snapshot log, using
[`slap_snapshot_log`](https://hexdocs.pm/slap_snapshot_log/). Clients connected
to different nodes edit the same document and see each other's cursors and
other Yjs awareness state.

Use `slap_yjs` when your application uses [`y_ex`](https://hexdocs.pm/y_ex/),
the Elixir bindings for Yjs. Your application is responsible for providing the
client connection, forwarding sync and awareness messages, and handling
authentication; this package does not start a WebSocket or HTTP server.

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
> Complete distribution:
>
> - [slap](https://hexdocs.pm/slap/): all Slap libraries and a standalone HTTP
>   server for evaluating and benchmarking `slap_streams` and `slap_kv`
>
> The `slap` package pulls in all seven libraries. If you only need a
> subset, add those packages directly.
<!-- /slap-preamble -->

## Installation

Requires Elixir 1.18 or later and Erlang/OTP 26.2 or later (see
[Requirements](#requirements)).

To depend on `slap_yjs` directly, add it to the dependencies in your
application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_yjs, "~> 0.3.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_yjs/).

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

Start your application with `iex -S mix`, then join the document. IEx acts
as the client process:

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

Pass each Yjs sync or awareness message from the client to
`MyApp.DocServer.process_message_v1(server, message, self())`. Encode each
`{:slap_yjs_update, _, _}` and `{:slap_yjs_awareness, _, _}` message the
client process receives with `Slap.Yjs.DocServer.encode_message/1`, and
send it to the client. `sync/2` waits for local updates to be stored and
for the server to catch up with other nodes.

To read or edit a document from server code, get its `Yex.Doc` with
`Slap.Yjs.DocServer.doc/2` and use y_ex's functions on it. To pass settings
to your document server module, use the `:assigns` option of
`Slap.Yjs.Docs.join/3`.

## Client behavior

- A client joins its node's document server, sends its sync and awareness
  messages to it, and receives `{:slap_yjs_update, doc_id, update}` and
  `{:slap_yjs_awareness, doc_id, update}` from any node. The server stops
  after 30 s without subscribers. If the client's monitor of the server
  reports `:DOWN`, the client joins again and resyncs.
- A server relays a client's update to its other clients before storing
  it, which takes up to 10 ms (`:flush_after`) plus one append. If the
  server stops first, the update is lost unless a client that has it
  resyncs. Clients on other nodes receive only stored updates.
- Awareness (presence) is not stored. A client's awareness state is removed
  when the client exits, and a server's when the server or its node stops.
- An append that fails for a reason other than a transient error
  stops the server with `{:slap_yjs_append_failed, reason}`. Clients join
  again and resend their state.
- A deleted document's id cannot be reused.

## Awareness and presence

Yjs refers to ephemeral participant state as **awareness**, such as a
participant's name, cursor position, or selection. Awareness is not stored
as document content.

- Use `Slap.Yjs.Awareness` to decode client awareness messages, inspect and
  filter their entries, and encode the accepted entries for forwarding.

- Use `Slap.Yjs.Presence` to publish awareness for a server-side participant.
  It handles renewal, reconnection, and removal.

### Authorizing client awareness

Document servers relay awareness messages without authorizing their client ids
or state. An authenticated host should authorize each message before passing
it to `process_message_v1/3`. Apply the check where each node receives
messages from clients, so states relayed between document servers have
already passed it.

One awareness update can contain entries for several clients because
y-websocket clients forward states they receive and removals for states that
timed out. Filter each entry independently. For each connection, accept:

- states that identify the authenticated user, using the first accepted
  client id as the only allowed id for that connection;
- removals for that client id.

The following function returns `{message, client_id}`. `message` is `nil` if
it accepts no entries, and `client_id` is `nil` until it accepts a state. This
example expects states to identify their user as
`%{"user" => %{"id" => user_id}}`.

```elixir
def authorize_awareness(<<1, framed::binary>>, user_id, client_id) do
  with {:ok, [update]} <- Slap.Yjs.Frame.parse(framed),
       {:ok, entries} <- Slap.Yjs.Awareness.decode(update) do
    case Enum.flat_map_reduce(entries, client_id, &accept(&1, &2, user_id)) do
      {[], client_id} -> {nil, client_id}
      {kept, client_id} ->
        {<<1>> <> Slap.Yjs.Frame.frame(Slap.Yjs.Awareness.encode(kept)), client_id}
    end
  else
    _ -> {nil, client_id}
  end
end

defp accept({id, _clock, %{"user" => %{"id" => user_id}}} = entry, client_id, user_id)
     when client_id in [nil, id],
     do: {[entry], id}

defp accept({id, _clock, nil} = entry, id, _user_id), do: {[entry], id}
defp accept(_entry, client_id, _user_id), do: {[], client_id}
```

Client ids are not authenticated. A connection can choose an id that another
client uses if it is the connection's first accepted id. Its accepted state
still identifies its authenticated user. While both clients are connected,
their updates replace each other under the shared id.

### Server-side presence

Start `Slap.Yjs.Presence` for a server-side participant such as an agent. It
subscribes to the document, publishes and renews its state, rejoins after the
document server stops, and removes its state when the presence process stops.

With the supervision tree and `MyApp.DocServer` from the example above:

```elixir
{:ok, presence} =
  Slap.Yjs.Presence.start_link(
    module: MyApp.DocServer,
    doc_id: {"my-service", "doc-1"},
    state: %{"user" => %{"name" => "Assistant"}, "busy" => false}
  )

:ok =
  Slap.Yjs.Presence.update(presence, %{
    "user" => %{"name" => "Assistant"},
    "busy" => true
  })
```

`update/2` replaces the participant's whole state. Browser clients receive
this state through the same Yjs awareness protocol used for other participants.
Your client UI decides how to display fields such as `"user"` and `"busy"`.
The state is renewed every 15 seconds by default; see `Slap.Yjs.Presence`
for configuration options.

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

Follow the repository's
[development setup](https://github.com/mjrusso/slap#development), then run
from `slap_yjs/`:

```sh
mix test
```

Set `SLAP_TEST_S3_ENDPOINT` and `SLAP_TEST_S3_BUCKET` to include the RustFS
tests. The multi-node convergence test uses `:peer` and needs `epmd`;
`SLAP_YJS_PROP_SEED` replays its edit sequence.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
