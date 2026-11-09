# Changelog

## Unreleased

- Adds backward message paging for `slap_inspect`, bounded by `:from` and
  `:until`. For forks, backward reads use the fork's own messages when they
  overlap the requested range, or its source's messages otherwise.
  Backward reads reject continuation cursors.

## 0.6.0

- Uses `slap_slatedb` 0.4.0, which upgrades SlateDB from 0.17 to 0.18.
  Rollback to SlateDB 0.17 is not guaranteed; see the
  [upgrade precautions](https://hexdocs.pm/slap_slatedb/changelog.html#0-4-0).

## 0.5.0

- Breaking change: if using an existing store written by `slap_streams` 0.4.0
  or earlier, perform the offline metadata upgrade before starting a cluster
  with this release. Finish or delete pending forks with the old version,
  stop all nodes using that store, take a backup, and follow the
  [upgrade instructions](README.md#upgrading-existing-stream-stores).
  Once conversion starts, returning to the old version requires restoring
  the backup. Existing forks keep their own message copies.
- Breaking change: replaces `:max_fork_copy_bytes` with `:max_fork_depth`
  (default 8), which limits chains of forks. A deeper fork returns HTTP 400.
- Breaking change: renames `:fork_copy_grace` to `:creation_grace`. Repair
  cancels forks pending longer than this duration and copies that have made
  no progress for this long.
- Fixes bodies of exactly 1 GiB being treated as empty on OTP 29.1.1, which
  could close a stream and discard the body.
- Forks read inherited messages from the source's storage. Creation time no
  longer depends on the number of inherited messages, but reading them requires
  the source's shards to be available. The source keeps those messages after
  trim, deletion or expiry until the fork and all its descendants are removed.
- Resume a fork left pending after a timeout by calling
  `Slap.Streams.create/2` with the same options, or delete it to cancel.
  Reads and appends return `:unavailable` until creation finishes.
- Adds `Slap.Streams.copy/3`, which copies a stream into one with its own
  storage and the same offsets. Call it again with the same arguments after a
  timeout. Completed copies are independent of the source; producer and
  Stream-Seq state start empty.
- Adds read-only stream and retention inspection for
  [Slap Inspect](https://hexdocs.pm/slap_inspect/).

## 0.4.0

- Fixes in-flight reads returning `:gone` (410) for streams removed
  completely. They now return `:not_found` (404), including after row
  cleanup or recreation at the same path. Soft-deleted streams still return
  `:gone`; long-polls woken by deletion still return `:deleted`.

## 0.3.0

- `Slap.Streams.Cluster` sets the default SlateDB `flush_interval` to 10 ms. Pass
  `settings: %{flush_interval: "100ms"}` to restore the previous default.
- With `probe: :background`, the cluster starts before its store is available.
  Operations return `{:error, :unavailable}` until its shards open.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.
- Adds the `[:slap, :streams, :wait, :registered]` telemetry event when a read
  with `:wait` starts waiting.
- Logs the `Slap.SlateDB.Error` message when a deleter write fails, replacing
  the `MatchError` that previously hid it.

## 0.1.0

Initial release of `slap_streams`.
