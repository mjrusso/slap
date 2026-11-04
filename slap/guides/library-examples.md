# Library examples

Requires Elixir 1.18 or later and Erlang/OTP 26.2 or later.

Start IEx and install the published `slap` package:

```sh
iex -e 'Mix.install([:slap])'
```

Run the examples below, in order, from this IEx session. These examples use
in-memory storage, which is discarded when the VM exits.

## SlateDB

Open a database, write a durable value, read it, and close the database:

```elixir
iex> {:ok, db} = Slap.SlateDB.open("example", store: :memory); :ok
:ok
iex> {:ok, _seq} = Slap.SlateDB.put(db, "user:1", "Ada", await_durable: true); :ok
:ok
iex> Slap.SlateDB.get(db, "user:1")
{:ok, "Ada"}
iex> Slap.SlateDB.close(db)
:ok
```

## Streams and snapshot logs

Start a Streams cluster, then create, append to, and read a stream:

```elixir
iex> {:ok, _} = Slap.Streams.Cluster.start_link(store: :memory, path: "streams", shards: 1); :ok
:ok
iex> {:ok, :created, _} = Slap.Streams.create("/events", content_type: "text/plain"); :ok
:ok
iex> {:ok, %{result: :appended}} = Slap.Streams.append("/events", "created"); :ok
:ok
iex> {:ok, %{messages: messages}} = Slap.Streams.read("/events", 0); messages
[{0, "created"}]
```

Snapshot logs use the running Streams cluster. The caller must supply
snapshot bytes representing the effects of every entry before the snapshot
offset. The log cannot verify that state. Publishing a snapshot trims those
entries:

```elixir
iex> base = "/v1/stream/examples/log"
"/v1/stream/examples/log"
iex> {:ok, _} = Slap.SnapshotLog.append(base, "one"); :ok
:ok
iex> {:ok, %{entries: ["one"], offset: offset}} = Slap.SnapshotLog.next(base, 0, wait: 0); :ok
:ok
iex> Slap.SnapshotLog.snapshot(base, offset, "state after one")
:ok
iex> {:reset, %{snapshot: snapshot, offset: ^offset}} = Slap.SnapshotLog.next(base, nil); snapshot
"state after one"
```

A consumer starts with `next(base, nil)` to get the current snapshot and the
offset from which to read later entries.

## Yjs

Start the document supervisor on the Streams cluster and edit a document with
`y_ex`:

```elixir
iex> defmodule Example.DocServer do
...>   use Slap.Yjs.DocServer
...> end; :ok
:ok
iex> {:ok, _} = Slap.Yjs.Docs.start_link(); :ok
:ok
iex> {:ok, server} = Slap.Yjs.Docs.join(Example.DocServer, {"examples", "doc-1"}); :ok
:ok
iex> {:ok, doc} = Slap.Yjs.DocServer.doc(server, 5_000); :ok
:ok
iex> text = Yex.Doc.get_text(doc, "text"); Yex.Text.insert(text, 0, "hello")
:ok
```

Call `sync/2` to wait for the document's buffered changes to be stored, then
read the text:

```elixir
iex> :ok = Slap.Yjs.DocServer.sync(server, 5_000)
:ok
iex> Yex.Text.to_string(text)
"hello"
```

## KV and files

Start a KV cluster, then write and read a row:

```elixir
iex> {:ok, _} = Slap.KV.Cluster.start_link(store: :memory, path: "kv", shards: 1); :ok
:ok
iex> {:ok, version} = Slap.KV.put("users", "42", "Ada", if_version: :absent); :ok
:ok
iex> {:ok, %{value: value, version: ^version}} = Slap.KV.get("users", "42"); value
"Ada"
```

Files use the running KV cluster. Small bodies are stored in KV records:

```elixir
iex> {:ok, _} = Slap.Files.start_link(store: :memory, path: "files"); :ok
:ok
iex> ref = {"examples", "hello.txt"}
{"examples", "hello.txt"}
iex> {:ok, %{storage: :inline}} = Slap.Files.put(ref, "hello", if_version: :absent); :ok
:ok
iex> Slap.Files.read(ref)
{:ok, "hello"}
```

For production configuration and supervision, follow the package links in
[Use Slap in an application](../README.md#use-slap-in-an-application).
