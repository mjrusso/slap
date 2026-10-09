# Changelog

## 0.2.0

- Ranged reads: `Slap.Files.read/2` and `Slap.Files.stream/2` take a
  `:range` and return only those bytes of the file, as an HTTP server needs
  for `Range` requests. A range is `{first, last}` (inclusive, as in HTTP),
  `{first, :eof}`, or `{:last, n}`. For a body stored as an object, only the
  requested bytes are downloaded. `stream/2` also returns the range clamped
  to the file's size, which is what a `Content-Range` header reports. A
  range that contains no byte of the file returns
  `{:error, {:range_not_satisfiable, size}}`.
- `read/2` and `stream/2` take `:if_version`. Passing the version returned
  by a client's first range request to the later ones makes every range
  come from the same version of the file.
- Uses `slap_slatedb` 0.2.0, which upgrades SlateDB from 0.16 to 0.17.
  Databases written by 0.1.0 open unchanged.

## 0.1.0

Initial release of `slap_files`.
