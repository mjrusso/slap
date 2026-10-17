# Changelog

## Unreleased

- Local awareness publishers must subscribe and claim their client ids through
  `Slap.Yjs.DocServer.claim/4` before submitting messages with their pid as the
  origin. This prevents another local connection from overwriting a claimed
  entry merely by submitting the same browser-chosen client id. Hosts still
  authenticate principals and authorize state fields.
- Same-principal takeovers transfer cleanup ownership independently of awareness
  clocks. A reconnecting client can resend an equal-clock state without its old
  connection's later exit deleting its presence. Replaced subscribers cannot
  submit states or removals, or reclaim the id within the same subscription.
- `Slap.Yjs.Presence` claims its id before publishing, preventing local clients
  from claiming, overwriting, or removing its state.
- Ownership enforcement is local to each document server. Different servers can
  accept conflicting claims, and relayed awareness is not checked against local
  claims. The current `:pg` awareness relay does not coordinate claims across
  nodes. See the README's
  [multi-node ownership limitation](README.md#multi-node-ownership-limitation).

## 0.3.0

- Presence rejects incompatible states in the caller before starting or
  updating a process. An invalid update leaves the presence process running
  with its previous state.
- Add `Slap.Yjs.Presence`, which publishes and renews awareness for a
  server-side participant. It rejoins after the document server stops, and
  stopping the presence process removes its state. Transient rejoin failures
  retry with backoff, preserving the client id and latest state.
- Add `Slap.Yjs.Awareness` for decoding and encoding y-protocols awareness
  entries. Hosts can use it to authorize client messages before passing them
  to a document server.

## 0.2.0

- `Slap.Yjs.DocServer.doc/2` returns the server's `Yex.Doc`, so server code
  can read and edit the document with y_ex's API. Its edits are relayed and
  stored like a client's updates.
- `Slap.Yjs.DocServer.encode_message/1` encodes the update and awareness
  messages a subscriber receives as y-protocols messages for its client.
- `Slap.Yjs.Docs.join/3` takes `:assigns`, a map that becomes
  `state.assigns` of the document server it starts. Callbacks in your
  document server module can read settings from it. If the server is
  already running, the option has no effect.
- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_yjs`.
