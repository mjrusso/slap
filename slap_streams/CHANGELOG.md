# Changelog

## Unreleased

- Create forks by reference, with no limit on inherited data size. Replace
  `:max_fork_copy_bytes` with `:max_fork_depth` (default 8; HTTP 400 when exceeded).
- Rename `:fork_copy_grace` to `:creation_grace` for pending forks and copies.
- Forks retain inherited data after source trim, deletion and expiry, including
  through deleted forks. Reading that data requires the ancestor shards to be
  available.
- Add `Slap.Streams.copy/3` for independent copies that preserve offsets.
  Retry the same request to resume an interrupted copy. See the
  [copy options and expiry rules](README.md#lifecycle).

### Required store upgrade

Stores written by 0.4.0 and earlier require an offline metadata upgrade before
starting this version. Stop all nodes, take a backup, and follow the
[upgrade instructions](README.md#upgrading-existing-stream-stores).

## 0.4.0

- Fix in-flight reads returning `:gone` (410) for streams that were removed
  completely. These reads now return `:not_found` (404), including after row
  cleanup or recreation at the same path. Soft-deleted streams still return
  `:gone`; long-polls woken by deletion still return `:deleted`.

## 0.3.0

- `Slap.Streams.Cluster` now sets SlateDB's `flush_interval` to 10 ms instead
  of using SlateDB's 100 ms default. Other settings leave this default intact;
  pass `settings: %{flush_interval: "100ms"}` to restore the old interval.
- With `probe: :background`, the cluster starts before its store is available.
  Operations return `{:error, :unavailable}` until its shards open.

## 0.2.0

- Add a `[:slap, :streams, :wait, :registered]` telemetry event, emitted
  when a read with `:wait` starts waiting.
- When a write by the deleter fails, the log now shows the `Slap.SlateDB.Error` message instead of a `MatchError`.
- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_streams`.
