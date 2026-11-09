# Changelog

## Unreleased

- **Breaking:** JSON output now encodes printable stored binaries as strings
  and other stored binaries as `{"$bytes": "<hex>"}`. Previously, these were
  hexadecimal strings. Page cursors remain hexadecimal. See
  [Commands](README.md#commands) for map key encoding.
- Rejects options that a command does not use, and suggests the closest
  option for a typo. The command editor completes command names and
  options with Tab.

## 0.1.0

Initial release of `slap-inspect`, a read-only terminal UI and JSON CLI for raw
storage, streams, KV rows, files, snapshot logs and Yjs documents.

- Requires a running server with `slap_cluster` 0.5.0 or later. Service views
  also require the package for that service at 0.5.0 or later. Reads use the
  shard owners' open database handles; the inspector itself has no dependency
  on the server libraries or the SlateDB NIF.
- Connects through distributed Erlang and discovers the clusters and services
  running on a node. Browse several clusters in one terminal session or select
  one with `--cluster`.
- Supports paginated reads, byte-range previews, links between records,
  optional timed refresh and reconstruction of Yjs content and retained history.
  JSON listings print the next page's command to stderr.
