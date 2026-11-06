# Changelog

## 0.6.0

- Uses `slap_slatedb` 0.4.0, which upgrades SlateDB from 0.17 to 0.18.
  Rollback to SlateDB 0.17 is not guaranteed; see the
  [upgrade precautions](https://hexdocs.pm/slap_slatedb/changelog.html#0-4-0).

## 0.5.1

- Add guides for the kitchen sink demo, library examples, standalone server,
  and development tools, included in the Hex package and HexDocs. Move the
  shared package overview into a guide.
- Distinguish package and monorepo GitHub links in package metadata.

## 0.5.0

- Breaking change: if using an existing Streams store written by
  `slap_streams` 0.4.0 or earlier, perform the offline metadata upgrade before
  starting a cluster with this release. Stop all nodes using that store,
  take a backup, and follow the
  [upgrade instructions](https://hexdocs.pm/slap_streams/readme.html#upgrading-existing-stream-stores).
- If using local storage (`{:local, directory}`), writes now share one
  exclusive OS lock per store root. Before starting this release, stop all
  processes running older Slap versions that access the same store; older
  versions do not take this lock. Writes to local object storage now sync to
  disk; see the
  [`slap_slatedb` changelog](https://hexdocs.pm/slap_slatedb/changelog.html#0-3-0).
- Breaking change: the Streams options `:max_fork_copy_bytes` and
  `:fork_copy_grace` are replaced by `:max_fork_depth` and `:creation_grace`;
  see the [`slap_streams` changelog](https://hexdocs.pm/slap_streams/changelog.html).
- Breaking change: y_ex 0.13 returns Yjs buffer values as
  `%Yex.Buffer{data: bytes}`. See the
  [`slap_yjs` changelog](https://hexdocs.pm/slap_yjs/changelog.html).
- Services refuse to start if the configured shard count differs from the
  count used to create the store; see the
  [`slap_cluster` changelog](https://hexdocs.pm/slap_cluster/changelog.html).
- KV writes and linearizable reads now have default limits on requests waiting
  for durability. The KV HTTP API returns 503 (`overloaded`) if other pending
  requests leave too little capacity, or 413 (`request_too_large`) if the
  request exceeds a byte limit by itself.
  Configure these limits through `kv: [child_options: ...]`; see
  [Limits on pending work](https://hexdocs.pm/slap_kv/readme.html#limits-on-pending-work).
- Serves atomic JSON KV batches at `POST /v1/kv/{partition}`.
- Adds server-side support for [Slap Inspect](https://hexdocs.pm/slap_inspect/)
  across all services. Install the inspector separately.
- Adds a kitchen sink demo (`examples/kitchen_sink.exs`) that runs every
  service with in-memory sample data and prints Slap Inspect commands.
- Updates the six server libraries previously at 0.4.0 to 0.5.0 and
  `slap_slatedb` to 0.3.0.

## 0.4.0

- Breaking change: local Yjs awareness publishers must subscribe and claim
  their client IDs before submitting awareness messages. See the
  `slap_yjs` changelog for the ownership requirements and multi-node limitation.
- Updates `slap_cluster`, `slap_streams`, `slap_snapshot_log`, `slap_yjs`,
  `slap_kv`, and `slap_files` dependencies to 0.4.0.

## 0.3.0

- Requires Erlang/OTP 26.2 or later through the new `slap_yjs` dependency.
- Service `:settings` now merge with the cluster defaults. `flush_interval`
  remains 10 ms unless explicitly set.
- Adds `slap_snapshot_log`, `slap_yjs`, and `slap_files` as dependencies.
  Installing `slap` installs all seven server libraries. The standalone HTTP
  server continues to serve Streams and KV.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.
- `slap.server` runs without a Mix project, via `Mix.install`.

## 0.1.0

Initial release of `slap`.
