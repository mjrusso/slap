# Standalone server

`mix slap.server` serves Streams, KV, or both from one HTTP listener. It does
not authenticate requests. Use it to evaluate, test and benchmark the services
without writing an application.

Requires Elixir 1.18 or later and Erlang/OTP 26.2 or later. The SlateDB NIF is
precompiled for macOS and Linux (glibc and musl) on x86_64 and arm64. The first
run downloads it and compiles the dependencies; later runs reuse them.

This command installs `slap` from Hex and starts the server:

```sh
elixir -e 'Mix.install([:slap]); Mix.Task.run("slap.server", System.argv())' -- \
  --streams --kv --store memory
```

`--streams` and `--kv` select the services; pass one or both. The server
listens on `http://127.0.0.1:4437`, with Streams under `/v1/stream/` and KV
under `/v1/kv/`. `GET /health` answers 200 once the listener is up.
The memory store loses its data when the server stops.

## Streams

With the server running, use another terminal for the HTTP examples below.
Create a JSON stream, then append to it. The JSON extension stores each element
of an appended array as a separate message.

```sh
curl -X PUT -H 'Content-Type: application/json' http://localhost:4437/v1/stream/events
curl -X POST -H 'Content-Type: application/json' \
  --data '[{"type":"created","id":1},{"type":"updated","id":1}]' \
  http://localhost:4437/v1/stream/events
```

Read the stream. The JSON messages are returned as an array.

```sh
curl 'http://localhost:4437/v1/stream/events?offset=-1'
```

Fork the stream at its current end, then append to the fork. The source still
contains only the first two messages.

```sh
curl -X PUT -H 'Content-Type: application/json' \
  -H 'Stream-Forked-From: /v1/stream/events' \
  http://localhost:4437/v1/stream/events-fork
curl -X POST -H 'Content-Type: application/json' --data '{"type":"deleted","id":1}' \
  http://localhost:4437/v1/stream/events-fork
curl 'http://localhost:4437/v1/stream/events-fork?offset=-1'
curl 'http://localhost:4437/v1/stream/events?offset=-1'
```

## KV

With KV running, set three rows, get one, then scan a range of keys in the
`people` partition. The range stops before `c`.

```sh
curl -X PUT --data-binary 'Ada' http://localhost:4437/v1/kv/people/a
curl -X PUT --data-binary 'Grace' http://localhost:4437/v1/kv/people/b
curl -X PUT --data-binary 'Katherine' http://localhost:4437/v1/kv/people/c
curl http://localhost:4437/v1/kv/people/a
curl 'http://localhost:4437/v1/kv/people?gte=a&lt=c'
```

The scan returns JSON with keys and values encoded as unpadded base64url.

## Storage

For S3, create the bucket and set `AWS_REGION` and credentials (such as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) in the server's environment.
Start the server with:

```sh
elixir -e 'Mix.install([:slap]); Mix.Task.run("slap.server", System.argv())' -- \
  --streams --kv --store s3:s3://my-bucket/my-app
```

`--store local:DIR` keeps the data in a local directory. See
[choosing the shard count](https://hexdocs.pm/slap_cluster/readme.html#choosing-the-shard-count)
for local-storage guidance and
[running another cluster on the same store](https://hexdocs.pm/slap_cluster/readme.html#running-another-cluster-on-the-same-store)
before running a script against a store an application is using. Existing
data requires its original shard count.

## Options

Pass `--help` to the startup command for the full option list. Use `--ip` to
listen on another address and `--port` for another port.

`--placement object-lease` requires an S3 store. `--peers` requires a
placement other than `local`.

Each service has its own shards: Streams under `streams/` and KV under
`kv/`, with separate leases when the placement uses them. Run one server per
Erlang VM; a cluster can span several Erlang VMs on one or several hosts.

To keep an IEx prompt with the `Mix.install` command, replace `elixir` with
`iex`.

## Running from a Mix project

In a Mix project that depends on `slap`, run the task directly:

```sh
mix slap.server --streams --kv --store memory
```

The same server options apply. Run `mix help slap.server` for the full list,
or `iex -S mix slap.server --streams --kv --store memory` to keep an IEx
prompt while the server runs.

## Inspection

Slap Inspect requires a named Erlang node. See its
[standalone server connection guide](https://hexdocs.pm/slap_inspect/readme.html#standalone-slap-server)
for startup and connection commands.

## Supervising the server

The Mix task is not available in a release. Add `slap` to your application's
dependencies and supervise `{Slap.Server, opts}` to start the selected
services and a Bandit listener:

```elixir
children = [
  {Slap.Server,
    store: {:url, "s3://bucket/db"},
    streams: [shards: 8],
    kv: [shards: 4],
    port: 4437}
]
```
