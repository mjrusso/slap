# Changelog

## Unreleased

- `Slap.Yjs.DocServer.doc/2` returns the server's `Yex.Doc`, so server code
  can read and edit the document with y_ex's API. Its edits are relayed and
  stored like a client's updates.
- `Slap.Yjs.DocServer.encode_message/1` encodes the update and awareness
  messages a subscriber receives as y-protocols messages for its client.

## 0.1.0

Initial release of `slap_yjs`.
