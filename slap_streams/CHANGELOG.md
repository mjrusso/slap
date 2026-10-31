# Changelog

## Unreleased

- Adds read-only stream and retention inspection for
  [Slap Inspect](https://hexdocs.pm/slap_inspect/).
- Forks now read their inherited messages from the source's storage instead
  of copying them, so creation time does not depend on how many messages a
  fork inherits. Reading those
  messages needs the source's shards to be available. The source keeps them
  after it is trimmed, deleted or expires, until the fork and every fork made
  from it are removed.
- Breaking change: replaces `:max_fork_copy_bytes` with `:max_fork_depth`
  (default 8), which limits chains of forks. A deeper fork returns HTTP 400.
- Breaking change: renames `:fork_copy_grace` to `:creation_grace`. It applies
  to pending forks and copies.
- Adds `Slap.Streams.copy/3`, which copies a stream into one with its own
  storage and the same offsets. Call it again with the same arguments to
  continue a copy that timed out.
- Fixes bodies of exactly 1 GiB being treated as empty on OTP 29.1.1, which
  could close a stream and discard the body.

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
