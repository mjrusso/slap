# Changelog

## Unreleased

- Adds a KV partition listing for `slap_inspect` that reads keys only and
  includes the shard's ownership-confirmation row. Malformed keys appear
  with an `:unknown_key` diagnostic.

## 0.6.0

- Uses `slap_slatedb` 0.4.0, which upgrades SlateDB from 0.17 to 0.18.
  Rollback to SlateDB 0.17 is not guaranteed; see the
  [upgrade precautions](https://hexdocs.pm/slap_slatedb/changelog.html#0-4-0).

## 0.5.0

- Each shard now limits the requests and bytes waiting for durability from
  writes and linearizable reads. A request that exceeds current capacity
  returns `{:error, :overloaded}` (HTTP 503). One that exceeds a byte limit by
  itself returns `{:error, {:bad_request, :request_too_large}}` (HTTP 413).
  Rejected requests are not applied. Configure the limits in the cluster's
  `child_options`; see
  [Limits on pending work](README.md#limits-on-pending-work).
- Adds `Slap.KV.batch/3` and JSON `POST /v1/kv/{partition}` for atomic puts,
  deletes and condition checks within one partition. Batches default to at most
  1,000 operations and 8 MiB of encoded keys and values. Larger batches return
  `{:error, {:bad_request, :batch_too_large}}` (HTTP 413).
- Adds read-only partition inspection for
  [Slap Inspect](https://hexdocs.pm/slap_inspect/).
- Adds telemetry events for rejected requests and pending work; see
  `Slap.KV.Telemetry`.

## 0.4.0

- `Slap.KV.scan/2` accepts `cursor: nil` to start at the beginning of the
  selected range. A returned `cursor: nil` marks the end of iteration.
  `prefix: nil`, `gte: nil`, and `lt: nil` remain invalid.

## 0.3.0

- `Slap.KV.Cluster` sets the default SlateDB `flush_interval` to 10 ms. Pass
  `settings: %{flush_interval: "100ms"}` to restore the previous default.
- With `probe: :background`, the cluster starts before its store is available.
  Operations return `{:error, :unavailable}` until its shards open.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_kv`.
