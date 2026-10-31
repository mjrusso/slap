# Changelog

## Unreleased

- Requires the offline Streams store upgrade for stores written by
  `slap_streams` 0.4.0 and earlier; see the
  [upgrade instructions](../slap_streams/README.md#upgrading-existing-stream-stores).
- Adds `Slap.SnapshotLog.fork/3`, which creates a log that starts with
  another log's snapshot and entries without copying them. If a fork times
  out, call `fork/3` again with the same source and target to finish it.
- Calls on a log that is still being created or forked return
  `{:error, :busy}`, except `append/3` and `next/3`, which finish an
  interrupted creation.
- Adds read-only snapshot-log inspection to Slap Inspect.

## 0.4.0

- Updates `slap_streams` and `slap_cluster` dependencies to 0.4.0.

## 0.3.0

- Uses `slap_streams` 0.3.0 and `slap_cluster` 0.3.0.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_snapshot_log`.
