# Changelog

## 0.6.0

- Uses `slap_slatedb` 0.4.0, which upgrades SlateDB from 0.17 to 0.18.
  Rollback to SlateDB 0.17 is not guaranteed; see the
  [upgrade precautions](https://hexdocs.pm/slap_slatedb/changelog.html#0-4-0).

## 0.5.0

- Breaking change: if using an existing store written by `slap_streams` 0.4.0
  or earlier, perform the offline metadata upgrade before starting a cluster
  with this release. See the
  [upgrade instructions](https://hexdocs.pm/slap_streams/readme.html#upgrading-existing-stream-stores).
- Adds `Slap.SnapshotLog.fork/3` to create a log from another log's snapshot
  and entries without copying them. Repeat the call with the same source and
  target after a timeout. Reading inherited snapshots and entries requires the
  source's shard to be available. Inherited data remains stored even after the
  fork publishes a new snapshot.
- `append/3` and `next/3` return `{:error, :busy}` while a fork is pending.
  They can finish an interrupted ordinary log creation; pass `nil` as the
  offset to `next/3` to do so.
- Adds read-only snapshot-log inspection for
  [Slap Inspect](https://hexdocs.pm/slap_inspect/).

## 0.4.0

- Updates `slap_streams` and `slap_cluster` dependencies to 0.4.0.

## 0.3.0

- Uses `slap_streams` 0.3.0 and `slap_cluster` 0.3.0.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_snapshot_log`.
