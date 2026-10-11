# Changelog

## Unreleased

- Adds `slap_snapshot_log`, `slap_yjs`, and `slap_files` as dependencies. The
  standalone server continues to serve Streams and KV.
- Service `:settings` now merge with the cluster defaults. `flush_interval`
  remains 10 ms unless explicitly set.

## 0.2.0

- `slap.server` runs without a Mix project, via `Mix.install`.
- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap`.
