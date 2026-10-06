#!/bin/bash
# Starts or stops a local cluster of `mix slap.server` nodes, for the
# Durable Streams conformance suite and manual tests. Each node is its own
# OS process with a name (n1@127.0.0.1, ...), sharing a store, with the
# Distributed placement (no leases, no database).
#
#   scripts/cluster.sh start DIR [NODES=3] [BASE_PORT=4440]
#   scripts/cluster.sh stop DIR
#
# Node i serves HTTP on BASE_PORT + i and logs to DIR/n<i>.log. Environment:
#   SLAP_STORE    the store (default local:DIR/store)
#   SLAP_SHARDS   shards (default 8)
#   SLAP_EXTRA    extra slap.server options (default --long-poll-timeout 500)
set -e
cmd=$1 dir=$2 nodes=${3:-3} base=${4:-4440}
cd "$(dirname "$0")/.."

case $cmd in
  start)
    mkdir -p "$dir"
    epmd -daemon
    names=$(for i in $(seq 1 "$nodes"); do printf "n%d@127.0.0.1," "$i"; done)
    for i in $(seq 1 "$nodes"); do
      nohup elixir --erl "-kernel net_ticktime 4" --name "n$i@127.0.0.1" --cookie slap-cluster \
        -S mix slap.server --streams --placement distributed \
        --port $((base + i)) --store "${SLAP_STORE:-local:$dir/store}" \
        --streams-shards "${SLAP_SHARDS:-8}" --streams-flush-interval 10ms \
        --peers "${names%,}" \
        --pid-file "$dir/n$i.pid" ${SLAP_EXTRA:---long-poll-timeout 500} \
        > "$dir/n$i.log" 2>&1 &
    done
    # Ready when every node answers and a stream on each shard can be created.
    for i in $(seq 1 "$nodes"); do
      for t in $(seq 1 120); do
        curl -s -o /dev/null --noproxy '*' "http://127.0.0.1:$((base + i))/v1/stream/_ready" && break
        sleep 1
      done
    done
    for t in $(seq 1 120); do
      ok=1
      for s in $(seq 1 32); do
        code=$(curl -s -o /dev/null -w "%{http_code}" --noproxy '*' -X PUT \
          "http://127.0.0.1:$((base + 1))/v1/stream/_ready/$s")
        [ "$code" = 201 ] || [ "$code" = 200 ] || { ok=0; break; }
      done
      [ $ok = 1 ] && echo "cluster ready: $nodes nodes on ports $((base + 1))-$((base + nodes))" && exit 0
      sleep 1
    done
    echo "cluster did not become ready" >&2
    exit 1
    ;;
  stop)
    # The next cluster uses the same node names, and a node cannot start while
    # another node with its name is running. A node with SSE streams open
    # takes tens of seconds to stop.
    pids=
    for f in "$dir"/n*.pid; do [ -f "$f" ] && pids="$pids $(cat "$f")" || true; done
    for pid in $pids; do kill "$pid" 2>/dev/null || true; done
    for pid in $pids; do
      for t in $(seq 1 600); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
      kill -9 "$pid" 2>/dev/null || true
    done
    ;;
  *)
    echo "usage: $0 start|stop DIR [NODES] [BASE_PORT]" >&2
    exit 2
    ;;
esac
