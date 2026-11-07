# slap_inspect

The `slap_inspect` package provides `slap-inspect`, a read-only TUI and CLI for
browsing Slap data: streams, KV rows, files, snapshot logs and Yjs documents,
including retained Yjs history.

The terminal UI is designed for exploring data: it pages through records, opens
their details and follows links between them. With `--json`, the same commands
print one page as JSON and exit.

It has no dependency on the server libraries or the SlateDB NIF.

[Install the inspector](#installation), [connect to a server](#connecting),
then [browse its data](#commands). To try it with sample data, run the
[kitchen sink demo](https://hexdocs.pm/slap/readme.html#kitchen-sink-demo).

<!-- slap-preamble -->
> #### About Slap {: .info}
>
> This package is part of [Slap](https://github.com/mjrusso/slap).
>
> Slap provides Elixir bindings for [SlateDB](https://slatedb.io) (an embedded
> key-value engine optimized for storing data in object storage), as well as
> services built on top of these bindings. The services run inside your
> application, on one node or several.
>
> Services:
>
> - [slap_streams](https://hexdocs.pm/slap_streams/): a [Durable
>   Streams](https://durablestreams.com) server, with HTTP, long polling, and
>   SSE
> - [slap_kv](https://hexdocs.pm/slap_kv/): partitioned key-value storage with
>   conditional writes, ordered scans, and an HTTP API
> - [slap_files](https://hexdocs.pm/slap_files/): file storage with metadata
>   and conditional replacement; small files are kept in `slap_kv`
>   records, which saves object-store PUTs
> - [slap_yjs](https://hexdocs.pm/slap_yjs/): [Yjs](https://yjs.dev) documents
>   for collaborative editing, with replication and presence across nodes
>
> Building blocks:
>
> - [slap_slatedb](https://hexdocs.pm/slap_slatedb/): the SlateDB bindings
> - [slap_cluster](https://hexdocs.pm/slap_cluster/): SlateDB databases run as
>   shards across nodes, with placement, routing, and failover
> - [slap_snapshot_log](https://hexdocs.pm/slap_snapshot_log/): an append-only
>   log of changes that can be condensed into snapshots
>
> All-in-one:
>
> - [slap](https://hexdocs.pm/slap/): all Slap server libraries, and `Slap`,
>   which starts the services you select under one supervisor
>
> Inspection:
>
> - [slap_inspect](https://hexdocs.pm/slap_inspect/): a read-only TUI and CLI
>   for browsing Slap data; installed separately
>
> The `slap` package pulls in all seven server libraries. If you only need a
> subset, add those packages directly.
<!-- /slap-preamble -->

## Installation

Install the inspector on the machine you connect from. The inspector requires
Elixir 1.18 or later. The Hex package `slap_inspect` installs the
`slap-inspect` command. You do not need to add it to your application's
dependencies.

```sh
mix local.hex --force
mix local.rebar --force
mix escript.install hex slap_inspect
```

Mix installs escripts in `~/.mix/escripts`; add that directory to your `PATH`
if needed. If you use asdf, run `asdf reshim elixir` afterwards. Run `mix
escript.install hex slap_inspect` again to update. To install from `main`:

```sh
mix escript.install github mjrusso/slap branch main --sparse slap_inspect
```

`slap-inspect --help` lists the options and commands.

## Connecting

The inspector connects to a server through distributed Erlang, so the server
must have inspection support and a node name, set with `--sname` or `--name`.

| Option | Value |
| --- | --- |
| `--node` | The server's node name, such as `my_app@my-host`. `Node.self()` in the server's console prints it. |
| `--cookie-file` | Optional. A file containing the server's Erlang cookie. Without it, the inspector uses `~/.erlang.cookie`. |
| `--cluster` | Optional. The module of the cluster to read, such as `MyApp.KV.Cluster`. See [Clusters](#clusters). |

```sh
slap-inspect --node my_app@my-host --cookie-file ~/.slap-cookie
```

The inspector uses long names when the host in `--node` contains a dot, as in
`my_app@127.0.0.1`; pass `--longnames` for a long name without one, such as
`my_app@localhost`. The inspector must reach the distribution ports of the
node it connects to and of every node that owns a shard it reads.

A server started without `--cookie` uses the `~/.erlang.cookie` of the user
that runs it, so the inspector needs no `--cookie-file` when it runs as that
user on that machine. Otherwise, save the value of `Node.get_cookie()`
without the leading colon to a file, and `chmod 600` it. With the wrong cookie
or an unreachable node, the inspector exits with `connection_failed`.

The inspector connects as a hidden node, so it does not appear in
`Node.list()`. Anyone who has the cookie and can reach the distribution ports
can run any code on the server: protect both as you would a remote console.

### Clusters

Without `--cluster`, the inspector reads every cluster running on the node it
connects to, including clusters with no shards open there. Clusters running
only on other nodes are not included. A Streams cluster holds streams,
snapshot logs and Yjs documents; a KV cluster holds KV rows and files. `scan`
and `get` read any cluster. A cluster whose shard children report no services,
such as one built directly on `slap_cluster`, offers only those two.

The terminal lists each cluster's views in the navigation pane and opens a
command in the first cluster that serves it.

With `--json`, exactly one cluster must serve the command. If several do, the
inspector exits with `ambiguous_cluster` and names them; on a node with
several clusters, this is always the case for `scan` and `get`. If none does,
it exits with `no_cluster_for_view`, or `no_clusters` if the node runs none.
Pass `--cluster` to choose one.

With `--cluster`, the inspector reads only that cluster, and exits with
`cluster_not_running` if it does not run on the node.

### Standalone Slap server

[`slap`](https://hexdocs.pm/slap/)'s
[standalone server](https://hexdocs.pm/slap/standalone-server.html) script,
`server.exs`, runs `Slap.Streams.Cluster` and `Slap.KV.Cluster`. When the
server and the inspector run on one machine as the same user, they share
`~/.erlang.cookie`. Start the script with a node name:

```sh
elixir --name slap@127.0.0.1 server.exs --store local:./data
```

Then, from another terminal:

```sh
slap-inspect --node slap@127.0.0.1
```

## Commands

Add a command after the connection options; without one, the inspector opens
`scan`. For example:

```sh
slap-inspect --node my_app@my-host --json kv --partition users
```

| Command | Reads |
| --- | --- |
| `clusters` | The node's clusters, with their shard counts, local shards, services and views. Always prints JSON, and does not take `--cluster`. |
| `scan --shard N --prefix HEX` | Raw stored rows. |
| `get --shard N --key HEX` | One stored value. |
| `streams` / `stream --path PATH` | Stream metadata. |
| `messages --shard N --sid ID` | The message records stored under one stream ID. |
| `logical --path PATH` | A stream's messages, including those a fork inherits. |
| `retention --shard N --sid ID` | Source registrations. |
| `kv --partition NAME` | KV rows; `--key-prefix HEX` filters keys. |
| `files --partition PARTITION` | File records, intents and object registrations. |
| `object --object-key KEY` | A file's object body. |
| `log --base PATH` | Snapshot-log state and components. |
| `yjs --base PATH` | The selected snapshot, and the number and size of update frames. |
| `yjs --content --base PATH --root text:body` | Yjs content with `body` read as text. |

A listing reads one shard: the first, unless `--shard N`, or `s` in the
terminal, selects another. An empty listing therefore does not mean the
cluster is empty. Commands that take `--path` or `--partition` find the shard
themselves.

JSON encodes every binary as a hexadecimal string, including paths, partition
names and keys. [Yjs content](#yjs-content) is the exception: its values are
written as JSON. Errors are written to stderr as JSON, and the inspector exits
nonzero.

### Streams, snapshot logs and Yjs

`--sid` is the numeric stream ID in stream metadata. `--from N` and
`--until N` limit `messages`, `logical`, `log --component` and `yjs` to the
offsets from `--from` (inclusive) to `--until` (exclusive). Pass
`--placement-key` for a stream created with a custom placement key.

`logical` lists the records a fork inherits, then its own, with one cursor
across them; its `lineage` field names the source streams. A path can be
reused after its stream is deleted: `--expected-sid N` makes the lookup fail
if the path now names another stream. Following a registration with `f` sets
`--expected-sid`.

A source registration protects the messages that a fork or copy still reads:
trimming the source does not delete them. `retention` lists a stream's
registrations; `--pins` lists them by the first offset each protects, and
`--path PATH` adds the stream's metadata summary.

For a snapshot log, `--base` is the path passed to `Slap.SnapshotLog`. For a
Yjs document, `Slap.Yjs.Store.base/2` returns it from the document ID and the
server's store options; with the default prefix, `{"demo", "notes"}` has the
base `/v1/stream/yjs/demo/docs/notes`.

`log --base PATH --component NAME` pages one component: `control`, `index` or
`updates`. `yjs --shard N --sid ID` pages a Yjs update stream by itself.

### Files

For `files`, `--partition` names a KV partition. File records for the
Files partition `users` are in `files-ns:<namespace>:record:users`, where
`<namespace>` is the Files namespace in unpadded URL-safe Base64; with the
default namespace, `files-ns:ZGVmYXVsdA:record:users`. Upload intents and
object registrations are spread over 64 partitions per namespace, not by
Files partition: `files-ns:<namespace>:intent:N` and
`files-ns:<namespace>:object:N`, for N from 0 to 63.

File summaries show inline bodies only as byte counts. For object previews
from a named Files instance, pass `--instance MyApp.Files`.

### Keys, ranges and pages

`--prefix`, `--key`, `--key-prefix` and `--cursor` take hexadecimal byte
pairs, optionally separated by spaces, as in `--prefix "00 ff"`. KV partition
names are text.

A page that has more rows includes a cursor. Pass it back with `--cursor HEX`,
and with `--cursor-exclusive` if the page reports `cursor_exclusive: true`.
With `--json`, the inspector also writes the command for the next page to
stderr, after `Next page: `.

Terminal pages default to 200 rows and a 1 MiB target, and `--json` listings
to 50 rows and 256 KiB. A page includes the row that crosses its target.
Single-record reads, Yjs content, `log --base` without `--component`, and
`yjs --base` use the terminal defaults with `--json` too. Values are cut at
512 KiB and keys at 4 KiB. `--limit`, `--target-bytes`, `--max-value-bytes`
and `--max-key-bytes` lower these limits. They bound the reply; SlateDB still
loads whole rows into native memory.

A key cut by `--max-key-bytes` ends its page, and the next page starts after
every key that begins with the shown bytes, so those keys are skipped. A cut
key cannot be fetched or followed; raise `--max-key-bytes` to read it, up to
4 KiB.

`--offset N --length N` selects a byte range for `get` or `object`. Without
`--length`, a range is 4096 bytes, or `--max-value-bytes` if that is smaller.
`object` rejects a length above `--max-value-bytes`; `get` rejects it only
when `--max-value-bytes` is given, and otherwise returns at most 512 KiB.
Object ranges need a positive length; `get` also accepts zero.

Each page names the shard, its owner and the shard's durable sequence number
(`durable_seq`); the page holds only writes up to it. JSON output can exceed
the page's `hard_max_bytes`, the largest reply the owner can send, because the
inspector adds assembled `messages`, the `log` summary, and the `logical`
view's `metadata` and `lineage`.

## Terminal

The footer lists the keys for the current screen, and `?` lists them all
with the full query.

| Key                           | Action                                            |
|-------------------------------|---------------------------------------------------|
| Arrows                        | Select rows, or scroll and pan details.           |
| PageUp / PageDown, Home / End | Scroll details by a page, or to the top / bottom. |
| `[` / `]`                     | Pan details by twenty columns.                    |
| Tab                           | Switch between records and details.               |
| Enter                         | Open details.                                     |
| Escape / `b`                  | Go back.                                          |
| `n` / `p`                     | Next / previous page.                             |
| `m` / `M`                     | Next / previous byte range of a value.            |
| `d`                           | Show details as bytes, text or JSON.              |
| `/`                           | Edit the command.                                 |
| `1`–`9`                       | Open a view listed in the navigation pane.        |
| `s`                           | Choose a shard.                                   |
| `f`                           | Follow a linked stream or object.                 |
| `g`                           | Show a stream's retention.                        |
| `c`                           | Reconstruct the content of a log or Yjs document. |
| `i`                           | Choose how Yjs roots are read.                    |
| `r`                           | Read again.                                       |
| `t`                           | Turn timed refresh off, or on at five seconds.    |
| `e`                           | Diagnostics for the last read, if it failed.      |
| `?`                           | Help.                                             |
| `q`                           | Quit.                                             |

Timed refresh rereads the current page and keeps the selection when its key
is still present. It is off unless the inspector starts with `--refresh MS`.
When it is on, `t` turns it off; when it is off, `t` turns it on at five
seconds, even if `--refresh` set another interval. It pauses while a read is
running or a dialog or the command editor is open, and it skips Yjs content.
If a refresh fails, the previous results stay on screen, labelled
`Last good data`.

The command editor accepts query options; connection, output and refresh
options are set at startup. Outside single quotes, a backslash escapes the
next character: `\n`, `\r`, `\t`, `\b`, `\f`, two-digit `\xHH` and four-digit
`\uXXXX` are recognized, and any other escaped character stands for itself.
The editor shows binary paths with byte escapes, so submitting the displayed
command keeps their bytes.

The byte and text views show up to 4096 bytes at a time. The terminal shows
zero-width code points as `\u{HEX}`: a key holding `e` followed by U+0301
appears as `"e\u{301}"`, and a key holding the characters `\u{301}` appears
with a doubled backslash.

## Yjs content

`yjs --content --base PATH` reconstructs a stored document: the shard owners
read its updates, and the node the inspector connects to applies them. The
terminal shows the owner and the input size and asks for confirmation;
`--json` does not ask.

`--root TYPE:NAME` reads a root as `text`, `array`, `map` or `xml_fragment`;
repeat it for more roots. Use the types the application uses; others can
return empty values. Roots without a type are listed without values, and
missing roots are reported as absent. Text omits formatting. In the terminal,
`i` changes the types.

Buffers in arrays and maps are written as `{"$bytes": "<hex>"}`. A user map
key that starts with `$` gains another `$` at every nesting level: `"$bytes"`
becomes `"$$bytes"`. Remove one leading `$` to recover a user key; a key with
exactly one leading `$` is an encoder tag. Root names are unchanged.

Without `--at-offset`, the content includes every update before the reported
offset, and may include later state from its snapshot. `--at-offset N` replays
the updates before `N` without a snapshot, where `N` is the start offset of a
stored update or the log's tail. It needs every earlier update to be
readable; for a fork, that includes inherited updates, which its registration
keeps readable.

| Option               | Default   | Maximum   |
|----------------------|-----------|-----------|
| `--max-input-bytes`  | 8 MiB     | 32 MiB    |
| `--max-entries`      | 50,000    | 200,000   |
| `--max-output-bytes` | 1 MiB     | 4 MiB     |
| `--budget`           | 10,000 ms | 30,000 ms |

Input sizes count each message's body plus its four-byte header, including
the snapshot. Native operations can run past the budget, memory use can
exceed the input size, and the output limit is checked after conversion. If
the selected snapshot keeps changing, reconstruction stops after three
attempts with `incomplete`.

## Reads and errors

Inspection requires a running cluster with the store open. The inspector
reads through the database handle of the node that owns the shard (its
owner). It never opens a database or starts a writer.

Reads return data that SlateDB has written to object storage. A durable
write can appear before its caller receives confirmation, including when
that confirmation is lost. Separate pages and object lookups do not share
one snapshot: data can change between reads, so a missing row or object alone
does not establish corruption. Malformed and truncated records are shown
with their bytes and a diagnostic.

Reads have a five-second budget by default; Yjs content reconstruction has
ten seconds. Set `--budget MS` to change it, up to 30,000 ms. The budget is
checked between storage calls, so a call in progress can run past it.

If a read stalls, the terminal waits for it to finish. You can submit another
request while waiting; only the latest waiting request is kept. With
`--json`, the inspector exits with `owner_read_stalled`. Quitting or
disconnecting does not cancel the read on the owner.

Each node runs one inspection request at a time across all its clusters.
Another request to that node fails with `busy` while the first is running.
In the terminal, press `r` to retry after the node finishes its current read.

If the server reports `owner_does_not_support_inspection`, it needs a version
with inspection support. If it reports `unsupported_api`, upgrade the
inspector or server so their inspection API versions match; the error
includes the server's inspection API version.

Remote exceptions include diagnostics: the node, the function called, the
kind (`error`, `exit` or `throw`), a summary of the reason, and up to eight
stack frames without argument values. In the terminal, press `e` to view
error diagnostics.

## Development

From the repository root:

```sh
nix develop
just deps slap_inspect
cd slap_inspect
```

Run from source with `mix slap.inspect` in place of `slap-inspect`:

```sh
mix slap.inspect --node my_app@my-host --cluster MyApp.Streams.Cluster streams
```

Check changes with `mix format` and `mix check`. Package tests run without
the server libraries or the SlateDB NIF; the tests against real servers are
the [`slap` project's inspection tests](https://github.com/mjrusso/slap/blob/main/slap/test/inspect_remote_test.exs),
which need that project's dependencies, including the NIF.

To build and run the executable:

```sh
MIX_ENV=prod mix escript.build
./slap-inspect --help
```
