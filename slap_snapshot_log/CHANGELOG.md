# Changelog

## Unreleased

- Adds `Slap.SnapshotLog.fork/3` to create independently editable logs by
  reference. Retry the same source and target after a timeout to resolve the
  original capture. Pending targets, and forks from a pending source, return
  `:busy`. Exhausted publication retries return `:retries_exhausted`. Forks
  retain ancestor storage and use the stream reference depth limit.

## 0.4.0

- Updates `slap_streams` and `slap_cluster` dependencies to 0.4.0.

## 0.3.0

- Uses `slap_streams` 0.3.0 and `slap_cluster` 0.3.0.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_snapshot_log`.
