# Changelog

## 0.4.0

- Updates `slap_cluster`, `slap_streams`, `slap_snapshot_log`, `slap_yjs`,
  `slap_kv`, and `slap_files` dependencies to 0.4.0.
- Breaking change for Yjs users: local awareness publishers must subscribe
  and claim their client ids before submitting awareness messages. See the
  `slap_yjs` changelog for the ownership requirements and multi-node limitation.

## 0.3.0

- Adds `slap_snapshot_log`, `slap_yjs`, and `slap_files` as dependencies.
  Installing `slap` now installs all seven library packages. The standalone
  server continues to serve Streams and KV.
- Because it now depends on `slap_yjs`, `slap` requires Erlang/OTP 26.2 or
  later.
- Service `:settings` now merge with the cluster defaults. `flush_interval`
  remains 10 ms unless explicitly set.

## 0.2.0

- `slap.server` runs without a Mix project, via `Mix.install`.
- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap`.
