# Changelog

## 0.6.1

- Fixes installation in Livebook Desktop by checking the loaded Kernel
  version instead of reading the `OTP_VERSION` file, which bundled runtimes
  may omit. OTP 26.2 or later is still required.

## 0.6.0

- Uses `slap_slatedb` 0.4.0, which upgrades SlateDB from 0.17 to 0.18.
  Rollback to SlateDB 0.17 is not guaranteed; see the
  [upgrade precautions](https://hexdocs.pm/slap_slatedb/changelog.html#0-4-0).

## 0.5.0

- Breaking change: if using an existing store written by `slap_streams` 0.4.0
  or earlier, perform the offline metadata upgrade before starting a cluster
  with this release. See the
  [upgrade instructions](https://hexdocs.pm/slap_streams/readme.html#upgrading-existing-stream-stores).
- Breaking change: upgrades y_ex to 0.13. Buffer values now read as
  `%Yex.Buffer{data: bytes}`; update code that expects byte lists. Buffers in
  text formatting attributes and embeds still read as lists after
  synchronization.
- Adds `Slap.Yjs.Store.fork/3` to create an independently editable document
  from another document's stored state without copying it. Call
  `Slap.Yjs.DocServer.sync/2` on known document servers first to include their
  buffered updates. Repeat the fork call with the same source and target after
  a timeout. Reading inherited state requires the source's shard to be available.
- `Slap.Yjs.Docs.join/3` returns `{:error, :busy}` for a document that is
  still being forked.
- Adds inspection of Yjs update frames, document content and retained history
  for [Slap Inspect](https://hexdocs.pm/slap_inspect/).

## 0.4.0

- Breaking change: local awareness publishers must subscribe and claim their
  client IDs through `Slap.Yjs.DocServer.claim/4` before using their PID as the
  message origin. Other local connections cannot overwrite a claimed entry by
  submitting the same client ID. Hosts still authenticate principals and
  authorize state fields.
- Claims are enforced per document server. Other document servers can accept
  the same client ID, and relayed awareness is not checked against local
  claims. The `:pg` relay does not coordinate claims across nodes. See the
  [multi-node ownership limitation](README.md#multi-node-ownership-limitation).
- Same-principal reconnects transfer cleanup ownership even when the awareness
  clock is unchanged. An old connection's exit no longer removes its replacement's
  presence. Replaced subscribers cannot publish or remove states, or reclaim
  the ID within the same subscription.
- `Slap.Yjs.Presence` claims its ID before publishing, preventing other local
  clients from claiming it or changing its state.
- `Slap.Yjs.Presence` accepts an optional `:owner` PID. Owner exit stops the
  presence, including normal exit and exit while retrying a join. Its default
  `restart: :transient` policy prevents a supervisor restart.
- `Slap.Yjs.Presence` accepts `state: fn client_id -> map end` to derive initial
  state from its generated ID. The function runs once per presence process;
  rejoining preserves the ID and latest state.

## 0.3.0

- Adds `Slap.Yjs.Presence` to publish and renew awareness for a server-side
  participant. It rejoins after the document server stops; transient failures
  retry with backoff, preserving the client ID and latest state. Stopping the
  presence process removes its state.
- Presence rejects invalid states before starting or updating a process. A
  rejected update preserves the running process and its previous state.
- Adds `Slap.Yjs.Awareness` for decoding and encoding y-protocols awareness
  entries. Hosts can use it to authorize client messages before passing them
  to a document server.

## 0.2.0

- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.
- `Slap.Yjs.DocServer.doc/2` returns the server's `Yex.Doc`, so server code
  can read and edit the document with y_ex's API. Its edits are relayed and
  stored like a client's updates.
- `Slap.Yjs.DocServer.encode_message/1` encodes the update and awareness
  messages a subscriber receives as y-protocols messages for its client.
- `Slap.Yjs.Docs.join/3` takes `:assigns`, a map that becomes
  `state.assigns` of the document server it starts. Callbacks in your
  document server module can read settings from it. If the server is
  already running, the option has no effect.

## 0.1.0

Initial release of `slap_yjs`.
