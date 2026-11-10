# Changelog

## 0.6.1

- Lists a shard's Files partitions for `slap_inspect` without reading values.

## 0.6.0

- Uses `slap_slatedb` 0.4.0, which upgrades SlateDB from 0.17 to 0.18.
  Rollback to SlateDB 0.17 is not guaranteed; see the
  [upgrade precautions](https://hexdocs.pm/slap_slatedb/changelog.html#0-4-0).

## 0.5.0

- File operations can return `{:error, :overloaded}` or
  `{:error, {:bad_request, :request_too_large}}` under `slap_kv` 0.5.0's new
  limits on pending writes and linearizable reads. Configure the limits in the
  KV cluster's `child_options`; see
  [Limits on pending work](https://hexdocs.pm/slap_kv/readme.html#limits-on-pending-work).
- Adds read-only inspection of file metadata and object previews for
  [Slap Inspect](https://hexdocs.pm/slap_inspect/).

## 0.4.0

- `Slap.Files.list/2` accepts `cursor: nil` to start at the beginning of the
  selected range. A returned `cursor: nil` marks the end of iteration.
  `prefix: nil`, `gte: nil`, and `lt: nil` remain invalid.

## 0.3.0

- Uses `slap_kv` 0.3.0 and `slap_cluster` 0.3.0.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.
- `Slap.Files.read/2` and `Slap.Files.stream/2` accept `:range` as
  `{first, last}` (inclusive), `{first, :eof}`, or `{:last, n}`. For files
  stored as objects, only the requested bytes are downloaded. `stream/2`
  returns the range clamped to the file's size for HTTP `Content-Range`.
  A range containing no bytes returns
  `{:error, {:range_not_satisfiable, size}}`.
- `read/2` and `stream/2` accept `:if_version`. Reuse the first range request's
  version on later requests to read every range from the same file version.

## 0.1.0

Initial release of `slap_files`.
