# slap_snapshot_log

`slap_snapshot_log` provides an append-only log of opaque entries, with
snapshots that replace its prefix, stored in Durable Streams
([`slap_streams`](https://hexdocs.pm/slap_streams/)) below a base path. A
consumer rebuilds its state from the current snapshot and the entries after it,
follows the log to stay current, and publishes snapshots of what it has
applied; publishing a snapshot trims the entries it covers.

[`slap_yjs`](https://hexdocs.pm/slap_yjs/) stores Yjs documents this way.

Use `slap_snapshot_log` when an application's state comes from an ordered log of
changes, but replaying every change from the beginning would become too slow.
For example, a counter can append one increment per entry and periodically store
its total as a snapshot. A new reader loads that total, then applies only later
entries. The package stores and trims the log; your application defines the
entry format, how entries change state, and how to encode a snapshot. Use
`slap_streams` directly if you need to retain and replay every entry or do not
need snapshots: publishing a snapshot here trims the entries it covers.

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

Add `slap_snapshot_log` to the dependencies in your application's `mix.exs`:

```elixir
defp deps do
  [
    {:slap_snapshot_log, "~> 0.1.0"}
  ]
end
```

Run `mix deps.get`. API documentation is on
[HexDocs](https://hexdocs.pm/slap_snapshot_log/).

## Example

In an application that depends on `slap_snapshot_log`, start
`Slap.Streams.Cluster` before using the log. For example, in `iex -S mix`
with a fresh local directory:

```elixir
iex> {:ok, _} = Slap.Streams.Cluster.start_link(store: {:local, "/tmp/slap-snapshot-log"}, shards: 8); :ok
:ok
iex> base = "/v1/stream/docs/42"
"/v1/stream/docs/42"

iex> {:reset, %{snapshot: nil, offset: 0}} = Slap.SnapshotLog.next(base, nil); :reset
:reset
iex> {:ok, _} = Slap.SnapshotLog.append(base, "entry"); :ok
:ok
iex> {:ok, %{entries: entries, offset: offset}} = Slap.SnapshotLog.next(base, 0); entries
["entry"]
iex> state_bytes = "entry"
"entry"
iex> :ok = Slap.SnapshotLog.snapshot(base, offset, state_bytes)
:ok
```

The application defines how to encode its state in `state_bytes`. See
`slap_streams` for cluster configuration. An application-owned Streams
cluster can be passed as `cluster:` to log operations. Run one Streams
cluster per VM.

## Reading

`next(base, offset, opts)` returns one of:

- `{:ok, %{entries: entries, offset: next, up_to_date: boolean}}`: a page
  of entries from `offset`, and the offset to read from next. At the tail,
  it waits up to `:wait` ms (30,000 by default) for an entry; `wait: 0`
  returns at once.
- `{:reset, %{snapshot: bytes | nil, offset: offset}}`: when `offset` is
  nil, or the entries from `offset` have been trimmed. The consumer
  replaces its state with the snapshot (an empty state if the snapshot is
  nil) and continues reading from `offset`.
- `{:error, reason}`: a `Slap.Streams` error, or `:deleted` when the log is
  deleted, before or during a wait. Transient read errors are retried for up to
  `:timeout` ms (default 60,000).

A consumer handles a reset the same way at start and later: another
process's snapshot can trim entries the consumer has not read yet.

## Appending

`append(base, entry)` returns `{:ok, offset}`, the offset after the entry,
once the entry is durable. It creates the log if needed. It does not retry:
on `{:error, :unavailable}` or `{:error, :timeout}` the entry may still be
stored. To retry without appending the entry twice, pass the same
`producer: {id, epoch, seq}` to each attempt. A retry returns
`{:duplicate, current_tail}` if that sequence was already written.

## Snapshots

`snapshot(base, offset, bytes, opts)` publishes a snapshot that replaces
the entries before `offset`, makes it current, and trims those entries.
Snapshot publication, listing, reading, tail lookup, and deletion accept
`timeout:` to bound each Streams call (default 30 s).

A snapshot at `offset` must contain the effect of every entry before
`offset`. It may contain more only if applying those entries again, when
the entries from `offset` on are replayed, is harmless. A consumer whose
entries cannot be applied twice snapshots only the entries it has read from
the log. A consumer whose entries are idempotent, such as Yjs updates, may
snapshot its live state. The log cannot check this.

Several processes can publish at once. Only the snapshot at the highest
offset becomes current; the others return `{:error, :superseded}`. Each
step of a publication can be interrupted: the log stays readable, and a
later publication, or a retry at the same offset, deletes what the
interrupted one left. A snapshot at an offset after the tail is
`{:error, :offset_beyond_tail}`.

The `:history` option keeps older snapshots: a list of `{every_ms,
keep_ms}` rules, each keeping the latest snapshot of every `every_ms`
period for `keep_ms`. For example,
`history: [{3_600_000, 7 * 24 * 3_600_000}]` keeps the latest snapshot
from each hour for up to a week. `snapshots/1` lists the stored snapshots
and `read_snapshot/2` reads one.

## Deleting

`delete(base)` deletes the log permanently. Its entries and snapshots are
deleted, and every later call on the base returns `{:error, :deleted}`. That
includes a `next/3` waiting at the tail, an `append/2`, and a snapshot
publication that started before the delete.

The base cannot be reused. These stay after a delete, so that those calls
are refused:

- `.index`, ending with a `{"deleted": true}` entry
- an empty, closed `.updates`
- a seal on the base's placement group (`Slap.Streams.seal/2`), so that no
  snapshot can be written there

The log reads as deleted from the delete's first step. Retrying `delete/1`
finishes an interrupted delete.

## Storage

```
<base>/.updates                      the entries, one message each
<base>/.index                        JSON snapshot metadata or a deletion marker
<base>/.snapshots/<offset>_snapshot  a snapshot
```

Snapshot metadata contains `"snapshotOffset"`, `"createdAt"`, and `"retained"`;
before deletion, the last snapshot entry is current. This is the reference
`y-durable-streams` server's layout (`"retained"` is an addition). The streams
share `base` as their placement key, so they are on one shard. `base` must be a
stream path with no segment that starts with `.`.

## Tests

```sh
mix test
```

The tests cover reading, appending, snapshot publication (including
interrupted and competing publications), snapshot history, and deletion.

## License

Slap is released under the terms of the [Apache License 2.0](LICENSE).

Copyright (c) 2026, [Michael Russo](https://mjrusso.com).
