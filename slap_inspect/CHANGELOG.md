# Changelog

## 0.2.0

- **Breaking:** JSON output now encodes printable stored binaries as strings
  and other stored binaries as `{"$bytes": "<hex>"}`. Previously, these were
  hexadecimal strings. Page cursors remain hexadecimal. See
  [Commands](README.md#commands) for map key encoding.
- Adds a home view showing each cluster's views and row counts by shard.
  The terminal opens home by default and updates the cluster menu when home
  is refreshed.
- Lists KV and Files partitions with `kv` and `files`, snapshot logs with
  `log`, and Yjs documents with `yjs`. In the terminal, `f` opens a listed
  partition, log or document.
- Shows shard owners, status and row counts in the shard picker. Empty
  listings can switch to a shard with rows and keep it selected on refresh.
- Adds record labels and summaries, and an Elixir term view for details.
  Details open as text when available. The JSON view preserves stored
  object key order and repeated keys.
- Reads a log's current snapshot and subsequent entries with
  `log --content --base PATH`, without requiring Yjs on the server.
- Adds `T` to read near a stream's tail and `p` to page backward in the
  `messages` and `logical` views. Both respect `--from` and `--until`.
- Infers Yjs map and text roots when no type is requested. Reports root type
  mismatches and distinguishes them from ambiguous sequence content.
- Adds Tab completion for command names and options, and suggestions for
  misspelled options. Rejects options a command does not use. Errors for
  unavailable services list the clusters that provide them.
- Reconnects automatically after a lost connection. Turning timed refresh
  back on restores the configured interval. Reads show elapsed time and a
  warning if they stall.
- Selects the first row on new pages, restores selection when returning
  from a link, and opens details directly for single-record reads.
- Wraps text at word boundaries and preserves multibyte characters across
  byte ranges. Hex dumps, help and view shortcuts adapt to narrow terminals.
- Escapes terminal control characters in labels and previews. Shows invalid
  UTF-8 as hexadecimal, preserving any readable prefix in labels.
- Asks for confirmation before reading log or Yjs content over 1 MiB.
- Preserves custom read budgets when switching views.
- Reports truncated keys in filtered listings and malformed keys in
  backward message reads. Missing logs report `log_not_found` with the shard
  checked.

The new partition, log and Yjs document listings and backward message paging
require updated server packages. See
[Reads and errors](README.md#reads-and-errors) for compatibility errors.

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
