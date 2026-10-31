# Changelog

## Unreleased

- Adds `Slap.KV.batch/3` and JSON `POST /v1/kv/{partition}`, which apply
  several puts, deletes and condition checks within one partition
  atomically.
- By default, each shard now limits how many writes and linearizable reads,
  and how many bytes of them, can wait for durability. A request over the
  limits fails with `{:error, :overloaded}` (HTTP 503), or with
  `{:error, {:bad_request, :request_too_large}}` (HTTP 413) if it could never
  fit; in both cases it was not applied. Set the limits in the cluster's
  `child_options`; see
  [Limits on pending work](README.md#limits-on-pending-work).
- Adds telemetry events for rejected requests and for pending work; see
  `Slap.KV.Telemetry`.
- Adds read-only partition inspection for
  [Slap Inspect](https://hexdocs.pm/slap_inspect/).

## 0.4.0

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
