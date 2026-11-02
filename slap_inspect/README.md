# Slap Inspect

`slap-inspect` is a read-only TUI and CLI for browsing Slap data: streams, KV
rows, files, snapshot logs and Yjs documents, including retained Yjs history.

The terminal UI is for exploring: it pages through records, opens their
details and follows links between them. With `--json`, the same commands print
one page as JSON and exit, so scripts can use them.

It has no dependency on the server libraries or the SlateDB NIF.

[Install the inspector](#installation), [connect to a server](#connecting),
then [browse its data](#browse-a-streams-cluster). To try it with sample data,
run the [kitchen sink demo](https://hexdocs.pm/slap/readme.html#kitchen-sink-demo).

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
> Server distribution:
>
> - [slap](https://hexdocs.pm/slap/): all Slap server libraries and a standalone
>   HTTP server for evaluating and benchmarking `slap_streams` and `slap_kv`
>
> Inspection:
>
> - [slap_inspect](https://hexdocs.pm/slap_inspect/): a read-only TUI and CLI
>   for browsing Slap data
>
> The `slap` package pulls in all seven server libraries. If you only need a
> subset, add those packages directly.
<!-- /slap-preamble -->

## Installation

Install on the machine you will connect from. You need Elixir 1.18 or later.
`slap_inspect` is the Hex package name; `slap-inspect` is the command it
installs. You do not need to add it to your application's Mix dependencies.

```sh
mix local.hex --force
mix local.rebar --force
mix escript.install hex slap_inspect
```

Add Mix's escript directory to your `PATH` if needed. The default is
`~/.mix/escripts`:

```sh
export PATH="$HOME/.mix/escripts:$PATH"
```

If you use asdf, run `asdf reshim elixir` after installation. To update the
inspector, run `mix escript.install hex slap_inspect` again.

To list the connection options and commands, run:

```sh
slap-inspect --help
```

To install from `main` (requires Git):

```sh
mix escript.install github mjrusso/slap branch main --sparse slap_inspect
```

## Connecting

Run `slap-inspect` in a separate terminal. The server must have inspection
support and an Erlang node name, set with `--sname` or `--name` when it starts.
The inspector connects through distributed Erlang, using the node name and
cookie rather than the server's HTTP address. Supply these options:

| Option | Value |
| --- | --- |
| `--node` | The server's full Erlang node name, such as `my_app@my-host`. To find it, run `Node.self()` in the server's IEx console. |
| `--cookie-file` | Optional. A file on the inspector's machine containing the server's Erlang cookie as text. Without it, the inspector uses Erlang's default cookie, `~/.erlang.cookie`. |
| `--cluster` | Optional. The module used to start the cluster to read, such as `MyApp.Streams.Cluster` or `MyApp.KV.Cluster`. Without it, the inspector reads every cluster running on the node. |

The inspector uses long names when the host in `--node` contains a dot, as in
`my_app@127.0.0.1`. For a long name without a dot, such as
`my_app@localhost`, pass `--longnames`. The inspector's machine must be able
to reach the distributed Erlang ports of the entry node and the nodes that own
its shards. The inspector finds the shard owners
automatically.

A server started without `--cookie` uses the default cookie of the user that
runs it. If the inspector runs as that user on the same machine, omit
`--cookie-file`. Otherwise, find the cookie with `Node.get_cookie()` in the
server's IEx console. Save the atom's name to a file, without a leading colon
or quotes, and restrict the file's permissions with
`chmod 600 ~/.slap-cookie`. With the wrong cookie, or a node it cannot reach,
the inspector exits with `connection_failed`.

Open the inspector, replacing the node name with your application's:

```sh
slap-inspect --node my_app@my-host --cookie-file ~/.slap-cookie
```

The inspector lists the clusters running on that node, including clusters
with no shards open there. Clusters on other nodes are not listed. The
navigation pane shows each cluster's views under its module name: Streams,
snapshot logs and Yjs documents are in a Streams cluster, and KV and Files
data are in a KV cluster. Any cluster can be read with `scan` and `get`.

Keys 1 to 9 open the first nine views, and the header names the cluster that
is open. Each listing reads one cluster. When you open another cluster, the
inspector keeps the first cluster's query, selection and history, and
restores them when you return.

A command opens the first listed cluster that serves it:

```sh
slap-inspect --node my_app@my-host --cookie-file ~/.slap-cookie streams
```

`--cluster` reads one cluster and does not list the others. It offers the
views that cluster serves, and exits with `cluster_not_running` if the cluster
does not run on the node. Use it in scripts, or with a server whose
`slap_cluster` predates `Slap.Cluster.clusters/0`: without `--cluster`, the
inspector reports `discovery_unsupported` for such a server, and with it, the
inspector offers every view:

```sh
slap-inspect --node my_app@my-host --cookie-file ~/.slap-cookie --cluster MyApp.Streams.Cluster streams
```

Add `--json` to print one page as JSON and exit. For example, to print the
first page of the KV partition `users`:

```sh
slap-inspect --node my_app@my-host --cookie-file ~/.slap-cookie --json kv --partition users
```

Without `--cluster`, `--json` reads the one cluster that serves the command.
If several clusters serve it, the inspector exits with an `ambiguous_cluster`
error that names them; with more than one cluster, this is always the case for
`scan` and `get`. Pass `--cluster` to choose one. The error is `no_clusters`
when the node runs none, and `no_cluster_for_view` when none serves the
command.

The inspector connects as a hidden Erlang node, so it does not appear in the
server's `Node.list()`. Anyone with the cookie who can reach the server's
distribution ports can run any code on it. Protect the cookie file and the
ports as you would access to a remote console.

### Standalone Slap server

If you use `mix slap.server`, its cluster modules are `Slap.Streams.Cluster`
and `Slap.KV.Cluster`, and one inspector session browses both. An unnamed server must be restarted with `--sname`
or `--name` before the inspector can connect.

A local server and the inspector both use Erlang's default cookie,
`~/.erlang.cookie`, which Erlang creates if it does not exist. In a Mix
project that depends on `slap`, start the server with:

```sh
elixir --name slap@127.0.0.1 -S mix slap.server --streams --kv --store local:./data
```

Without a Mix project, install and start the server with:

```sh
elixir --name slap@127.0.0.1 \
  -e 'Mix.install([:slap]); Mix.Task.run("slap.server", System.argv())' -- \
  --streams --kv --store local:./data
```

Then connect from another terminal on the same machine:

```sh
slap-inspect --node slap@127.0.0.1
```

## Browse a Streams cluster

After connecting with the `streams` command:

1. Use Up/Down to select a stream and Enter to open its metadata.
   Press `d` to switch between bytes, text and JSON.
2. Press `f` on stream metadata to read its messages. For a fork, this
   includes the messages it inherits from its source. Use `n` and `p` to page
   through the results, and Escape or `b` to return.
3. Press `s` to choose another shard. A listing reads one shard at a time;
   an empty listing does not mean the whole cluster is empty.
4. Press `r` to read again, `?` for help, or `q` to quit.

The [kitchen sink demo](https://hexdocs.pm/slap/readme.html#kitchen-sink-demo)
supplies streams to browse. A server using an empty store has no application
data until some is written through its HTTP or Elixir APIs.

## Commands

Add a command after the connection options. It opens the terminal UI; with
`--json`, it writes one JSON object and exits. JSON encodes raw binaries as
hexadecimal strings and preserves reconstructed document content as JSON.
Errors exit nonzero. Omitting the command opens the raw `scan` view.

`scan` and `get` return raw keys and values. Use `streams`, `kv` or `files`
for decoded records.

Listings default to the first shard. Use `--shard N` to select another,
or press `s` in the terminal. Path and partition lookups route automatically.

| Command | Reads |
| --- | --- |
| `clusters` | The node's clusters, with their shard counts, local shards, services and views. Always prints JSON, and does not take `--cluster`. |
| `scan --shard N --prefix HEX` | Raw stored rows. |
| `get --shard N --key HEX` | One stored value. |
| `streams` / `stream --path PATH` | Stream metadata. |
| `messages --shard N --sid ID` | The message records stored under one stream ID. |
| `logical --path PATH` | A stream's messages, including those a fork inherits. |
| `retention --shard N --sid ID` | Source registrations; add `--pins` to list them by start offset. |
| `kv --partition NAME` | KV rows; `--key-prefix HEX` filters keys. |
| `files --partition PARTITION` | File records, intents and object registrations. |
| `object --object-key KEY` | A file's object body. |
| `log --base PATH` | Snapshot-log state and components. |
| `yjs --base PATH` | Snapshot selection and update frame counts and sizes. |
| `yjs --content --base PATH --root text:body` | Yjs content with `body` read as text. |

### Streams, snapshot logs and Yjs

`--sid` is the numeric stream ID shown in stream metadata. `--from N` and
`--until N` limit `messages`, `logical` and `log` to the stream offsets from
`--from` (inclusive) to `--until` (exclusive).

A source registration is a row stored with a stream for each fork or copy
that reads its messages. While the row exists, the messages it covers are not
deleted, even after the stream is trimmed. `retention` lists these rows;
`--pins` lists them by the first offset they protect.

For snapshot logs, `--base` is the path passed to `Slap.SnapshotLog`. For Yjs,
obtain it with `Slap.Yjs.Store.base/2`, using the document ID and store
options configured on the server. With the default prefix, `{"demo", "notes"}`
has the base `/v1/stream/yjs/demo/docs/notes`.

Use `log --base PATH --component index` to page a log component; `control`
and `updates` also work. For Yjs updates alone, use `yjs --shard N --sid ID`.
Add `--path PATH` to `retention` to include the stream's metadata summary.

For a stream created with a custom placement key, pass `--placement-key`.

A path can be reused after its stream is deleted. Pass `--expected-sid N` to
make a `logical` lookup fail if the path now names a stream with another ID.
Following a registration with `f` adds this option to the query.

`logical` lists the records a fork inherits first, then its own records. The
pages share one cursor: pass the hexadecimal cursor from a page back unchanged
to read the next one. `lineage` in the result lists the source streams the
inherited records came from.

Each page names the node that owns the shard, the shard number, and the
shard's durable sequence number (`durable_seq`). A read includes only writes
up to that sequence number.

### Files

For Files, `--partition` is the KV partition that stores the data, not the
Files partition name. File records for the Files partition `users` are in
`files-ns:<namespace>:record:users`, where `<namespace>` is the Files
namespace in unpadded URL-safe Base64. With the default namespace, that is
`files-ns:ZGVmYXVsdA:record:users`. Upload intents and object registrations
are in partitions with `intent` and `object` in place of `record`.

For object previews from a named Files instance, pass
`--instance MyApp.Files`.

### Raw keys, ranges and pagination

`--prefix`, `--key`, `--key-prefix` and `--cursor` take hexadecimal byte
pairs, optionally separated by spaces. Quote values containing spaces, as
in `--prefix "00 ff"`. KV partition names are literal text.
Resume a returned cursor with `--cursor HEX`; also pass `--cursor-exclusive`
if the page reports `cursor_exclusive: true`.

`--offset N --length N` selects a byte range for `get` or `object`. Offsets
must be nonnegative; lengths cannot exceed `--max-value-bytes` (at most
512 KiB). Object ranges require a positive length; `get` also accepts zero.

A page's `hard_max_bytes` is the size limit on the owner's reply. JSON output
can be larger, because the inspector adds fields after the reply arrives:
assembled `messages`, the `log` summary, and the `logical` view's `metadata`
and `lineage`.

## Terminal controls

| Key | Action |
| --- | --- |
| Arrows | Select rows or scroll details. |
| PageUp / PageDown | Scroll details by a page. |
| Tab | Change pane focus. |
| Enter | Open details. |
| Escape / `b` | Go back. |
| Home / End | Scroll details to the top / bottom. |
| `n` / `p` | Next / previous page. |
| `/` | Edit the current command and filters. |
| `?` | Keyboard help, including the full query. |
| `e` | Diagnostics for the last error. |
| `s` | List and select available shards. |
| `d` | Cycle bytes, wrapped text and JSON details. |
| `[` / `]` | Pan details left / right by twenty columns. |
| `1`–`9` | Open a view listed in the navigation pane, grouped by cluster. |
| `r` | Refresh. |
| `t` | Turn timed refresh off, or on at five seconds. |
| `f` | Follow a linked stream or object. |
| `g` | Inspect stream retention. |
| `c` | Reconstruct content from a log or Yjs view. Press `y` to confirm or `n` to cancel. |
| `i` | Choose root interpretations for loaded Yjs content. |
| `m` / `M` | Load the next / previous byte range. |
| `q` | Quit. |

`--refresh MS` starts the inspector with timed refresh at that interval.
Pressing `t` turns it off; pressing `t` again turns it on at five seconds,
not at the `--refresh` interval. Only the active page refreshes;
selection is kept when the same key is still present. The header shows the
active command, refresh interval and last successful read time. Failed
refreshes keep the previous results and label them `Last good data`.
Timed refresh skips Yjs content, which displays `Manual reconstruction`;
press `r` to reconstruct it again after confirmation.

Set connection, output and refresh options when starting the inspector. The
command editor accepts query options only.

In the command editor, Left/Right, Home/End and Backspace/Delete move the
cursor and edit text. Up/Down recalls submitted commands from this session;
Ctrl-U clears the input. Enter submits and Escape discards the edit. Quoted
arguments support escaped quotes, backslashes, `\n`, `\r`, `\t`, two-digit
`\xHH` byte escapes and four-digit `\uXXXX` escapes. Opaque binary paths use
byte escapes so submitting the displayed command preserves their bytes.
Single quotes preserve their contents literally. View shortcuts open the
command editor when the selected view needs arguments. The status line lists
the required options, and the editor adds the first one to the command.
Commands submitted in the editor read the open cluster.

Enter opens the first record when none is selected. Tab switches between
records and details; the pane label and border identify focus. If a refreshed
record disappears, focus returns to the list with an explanation. Help and
diagnostics return to the same selection and scroll position with Escape.
Long record labels stay on one line; open details to inspect the record.
Help shows the full query when it is clipped in the header.

## Reading data

Details start in the byte view. Press `d` for text with line breaks and
wrapping, then again for formatted JSON. Unavailable interpretations show an
explanation; the byte view remains available. The byte view and raw text
view show up to 4096 bytes at a time. The range label identifies the displayed
interval, total size, truncation, partial reads and EOF. Press `m` to advance
and `M` to return to the previous offset and length through a new durable
read. Changing ranges starts at the top of the details. Manual and timed
refresh reread the displayed range and preserve its scroll position.
Yjs text mode labels each interpreted root with its interpretation and shows
text roots with real line breaks; it does not change the stored document.

The terminal displays zero-width Unicode codepoints as `\u{HEX}` escapes so
combining marks and format characters remain visible. For example, a key
containing `e` followed by U+0301 appears as `"e\u{301}"`; a key containing
the literal characters `\u{301}` appears with a doubled backslash. These escapes
affect terminal display only.

### Yjs content and history

Use `yjs --content --base PATH` to reconstruct a stored document. The terminal
shows the owner and input size in wire bytes before asking for confirmation. JSON
mode runs immediately. The inspector reads the document from its shard owners
and the entry node reconstructs it.

Use `--root TYPE:NAME` to choose how each root is read: `text`, `array`, `map`
or `xml_fragment`. Repeat it for multiple roots. Roots without a type are
listed without values; missing roots are reported as absent. Text omits
formatting. Buffers in arrays and maps use `{"$bytes": "<hex>"}`.
User map keys starting with `$` gain another `$`, at every nesting level:
`"$bytes"` becomes `"$$bytes"` and `"$$x"` becomes `"$$$x"`. Remove one leading
`$` to recover a user key; keys with exactly one leading `$` are encoder tags.
Root names are unchanged.
Use the application's root types; other choices may return empty values.

In the terminal, press `i` to choose interpretations without restarting.
Up/Down selects a root; Left/Right cycles its type, including none. Enter
reviews the reconstruction cost before confirmation; Escape discards edits.

Current content includes all updates before the reported offset, but its
snapshot may include later state. For history, use `--at-offset N` with a
stored update entry's starting offset or the log's tail. It replays updates
before `N` without a snapshot and requires all earlier updates to remain
readable. For a fork, this includes inherited updates: the fork's registration
keeps them readable after its source is trimmed.

| Option | Default | Maximum |
| --- | --- | --- |
| `--max-input-bytes` | 8 MiB | 32 MiB |
| `--max-entries` | 50,000 | 200,000 |
| `--max-output-bytes` | 1 MiB | 4 MiB |
| `--budget` | 10,000 ms | 30,000 ms |

Input sizes count wire bytes: each message's body plus its four-byte header,
including the snapshot. The confirmation and input-limit errors use this count.

Native operations can run past the budget. Memory use can exceed encoded
input size, and the JSON output limit is checked after conversion.
If the selected snapshot keeps changing, collection stops after three
attempts and returns `:incomplete`.

### Stored records

Reads use the owner's existing handle without opening databases or starting
application writers, so a store can be inspected only while a running
cluster has it open. Reads return only data that SlateDB has written to
object storage. That data can include writes whose callers have not yet
received a reply, or never will. Pages and object lookups can observe
different states; a missing row or object alone does not establish
corruption.

Pages default to 200 rows and a 1 MiB target, with previews capped at
4 KiB per key and 512 KiB per value. Use `--limit`, `--target-bytes`,
`--max-key-bytes` and `--max-value-bytes` to lower these limits. SlateDB still
loads full rows in native memory. Malformed or truncated records are shown
with their bytes and a diagnostic.

A key longer than `--max-key-bytes` is shown cut to that length, and the page
ends at it. The next page starts after every key that begins with the shown
bytes, so those keys are skipped, and a cut key cannot be fetched or
followed. If you lowered `--max-key-bytes`, raise it to read these keys. Keys
longer than 4 KiB, the maximum, are always cut.

File summaries include metadata and storage kind. Inline bodies are shown
as byte counts; their encoded bytes remain in the raw row.

### Errors and stalled reads

Each node allows one inspection request at a time across its clusters.
Ordinary reads default to a five-second budget (`--budget MS`, maximum 30000),
checked between storage calls; a call already in progress may take longer.
While a read is stalled, the terminal keeps at most one more request waiting;
navigating again replaces it. JSON mode exits with a stalled-read error.
Quitting or disconnecting does not cancel the owner's read. Press `r` to retry
a busy owner, or enable timed refresh.

Remote failures report the node, requested module/function/arity, failure kind
(`error`, `exit` or `throw`), a reason summary, and up to eight stack frames.
Frames include arity and source location without argument values. Before JSON
encoding, diagnostics are capped at 8 KiB, including at most 1 KiB for the
reason. A server without inspection support reports
`owner_does_not_support_inspection`; one with a different inspection API
version reports `unsupported_api` and the version it supports. Upgrade
`slap-inspect` or the server so they match. Transport timeouts and
disconnections are reported separately.

## Development

From the repository root:

```sh
nix develop
just deps slap_inspect
cd slap_inspect
```

Run from source with the Mix task:

```sh
mix slap.inspect --help
mix slap.inspect --node my_app@my-host --cookie-file ~/.slap-cookie --cluster MyApp.Streams.Cluster streams
```

For commands elsewhere in this guide or printed by the demo, replace
`slap-inspect` with `mix slap.inspect` when running from source.

Format and check changes with:

```sh
mix format
mix check
```

Package tests run without the server libraries or the SlateDB NIF.
Tests that connect to real servers live in the
[`slap` project's inspection tests](https://github.com/mjrusso/slap/blob/main/slap/test/inspect_remote_test.exs)
and require that project's dependencies, including the SlateDB NIF.

### Testing the executable

Build the executable locally:

```sh
MIX_ENV=prod mix escript.build
./slap-inspect --help
```

Use `./slap-inspect` in place of `slap-inspect` in the examples above. The
executable bundles the inspector, its dependencies and Elixir; running it
requires Erlang/OTP. Rebuild it after changing the source.
