# Changelog

## Unreleased

- `Slap.KV.scan/2` accepts `cursor: nil` to start at the beginning of the
  selected range. A returned `cursor: nil` marks the end of iteration.
  `prefix: nil`, `gte: nil`, and `lt: nil` remain invalid.

## 0.3.0

- `Slap.KV.Cluster` now sets SlateDB's `flush_interval` to 10 ms instead of
  using SlateDB's 100 ms default. Other settings leave this default intact;
  pass `settings: %{flush_interval: "100ms"}` to restore the old interval.
- With `probe: :background`, the cluster starts before its store is available.
  Operations return `{:error, :unavailable}` until its shards open.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_kv`.
