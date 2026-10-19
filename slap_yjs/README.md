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

Before forwarding awareness, [claim and authorize its client id](#authorizing-client-awareness).
Pass each Yjs sync or authorized awareness message from the client to
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
- Awareness (presence) is not stored. A client's exit removes the awareness
  states it still owns. A server's departure removes the states attributed
  to it on its peers.
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

Each awareness entry has a client id chosen by the browser. That id is separate
from the authenticated user id. Checking the name or user id in the state does
not establish who may update the entry: another connection could submit its own
state under the same client id and overwrite it.

Local publishers must claim their awareness id with `Slap.Yjs.DocServer.claim/4`
before submitting states or removals through `process_message_v1/3`. The caller
must be subscribed and pass `self()` as the message origin. A claim belongs to
that subscriber until it leaves or exits. Another connection of the same
principal may take over; the previous connection's states and removals are
then rejected, and its departure cannot remove the replacement's state.

For example, a reconnecting browser may keep client id `42` while its old
connection is still alive. The new connection claims `42` with the same
authenticated principal. Ownership transfers immediately, even if the browser
resends a state with the clock the server already has. Yjs may ignore that
equal-clock state, but the old connection's later exit still cannot delete the
replacement's presence.

The host supplies the authenticated principal and still authorizes or stamps
state fields. One awareness update can contain entries for several clients:
y-websocket clients forward states they receive and removals for timed-out
states. Filter entries independently and bind each connection to its first
accepted id. Claim only when establishing that binding; submitting later
states checks ownership without reclaiming the id.

This example expects states to identify their user as
`%{"user" => %{"id" => user_id}}`. It returns `{message, client_id}`;
`message` is `nil` when no entries are accepted.

Start a new subscription with `client_id` set to `nil`. Save the returned
value after each call and pass it to the next call:

- `nil`: no claim has been confirmed; a later state can try again.
- An integer: the connection's claimed awareness id.
- `:not_owner`: the connection lost ownership. Further entries are discarded
  without trying to claim again.

```elixir
def authorize_awareness(server, message, user_id, client_id) do
  with {:ok, 1, framed} <- Slap.Yjs.Frame.read_varuint(message),
       {:ok, [update]} <- Slap.Yjs.Frame.parse(framed),
       {:ok, entries} <- Slap.Yjs.Awareness.decode(update) do
    case Enum.flat_map_reduce(entries, client_id, &accept(&1, &2, user_id, server)) do
      {[], client_id} -> {nil, client_id}
      {kept, client_id} ->
        {<<1>> <> Slap.Yjs.Frame.frame(Slap.Yjs.Awareness.encode(kept)), client_id}
    end
  else
    _ -> {nil, client_id}
  end
end

defp accept({id, _clock, %{"user" => %{"id" => user_id}}} = entry, nil, user_id, server) do
  case Slap.Yjs.DocServer.claim(server, id, user_id) do
    :ok -> {[entry], id}
    {:error, :not_owner} -> {[], :not_owner}
    {:error, _reason} -> {[], nil}
  end
end

defp accept({id, _clock, %{"user" => %{"id" => user_id}}} = entry, id, user_id, _server),
  do: {[entry], id}

defp accept({id, _clock, nil} = entry, id, _user_id, _server), do: {[entry], id}
defp accept(_entry, client_id, _user_id, _server), do: {[], client_id}
```

Forward a non-nil `message` with
`MyApp.DocServer.process_message_v1(server, message, self())`. The server
checks ownership again before applying the entries. If it returns
`{:error, :not_owner}`, set the connection's `client_id` to `:not_owner` too.
The next call to the example function will then discard the entries.
An accepted update can still be ignored under awareness clock rules.

A takeover prevents the old subscriber from reclaiming that id for the rest
of its subscription. Unsubscribing and subscribing again starts a new
subscription, even for the same pid. A server restart clears all claims and
requires clients to subscribe again. Reset `client_id` to `nil` only when
starting a new subscription.

### Multi-node ownership limitation

Claims protect submissions to one document server. Awareness relayed from another
document server is accepted without these ownership checks. Two servers can each
accept a claim for the same client id, including claims by different principals.
Their updates can conflict with each other or with server-side presence. Hosts
must authenticate and filter awareness on every node; local claims do not provide
exclusive ownership across nodes.

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
    owner: self(),
    state: fn client_id ->
      %{"user" => %{"name" => "Assistant"}, "colorIndex" => rem(client_id, 8), "busy" => false}
    end
  )
```

The initial state can be a map or a function of the generated client id. The
function runs once in each presence process, before its first publication.
Rejoining keeps the same id and the latest state, including any updates.

With `owner: self()`, the caller's exit stops the presence, including normal
exit and while the presence is retrying a join. Presence's default
`restart: :transient` child policy prevents a supervisor restart after owner
exit. The owner may differ from the process that starts or supervises the
presence. To remove presence on an explicit leave while the owner remains
alive, retain the presence pid and call `GenServer.stop(presence)`.

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
