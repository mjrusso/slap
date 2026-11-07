#!/usr/bin/env bash
# Runs examples/server.exs outside a Mix project, with `Mix.install` of this
# checkout, and checks `GET /health`.
set -euo pipefail
cd "$(dirname "$0")/.."
work=$(mktemp -d)
port=4438

# examples/server.exs skips its own Mix.install when one has already run, so
# this test does not check the script's Mix.install line. Mix.install leaves
# out optional dependencies; SLAP_SLATEDB_BUILD=1 needs Rustler to build the
# NIF.
MIX_INSTALL_DIR=$work/install elixir -e "
  Mix.install([
    {:slap, path: \"$PWD\"},
    {:bandit, \"~> 1.12\"},
    {:rustler, \"~> 0.38\", runtime: false}
  ])
  Code.eval_file(\"examples/server.exs\")
" -- --port $port > "$work/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; rm -rf "$work"' EXIT

# Long enough for a fresh install to compile every dependency and the NIF.
for _ in $(seq 1 900); do
  if curl -sf -o /dev/null http://127.0.0.1:$port/health; then
    echo "examples/server.exs started without a Mix project"
    exit 0
  fi
  if ! kill -0 $server 2>/dev/null; then break; fi
  sleep 1
done

echo "examples/server.exs failed to start without a Mix project; its log:" >&2
tail -50 "$work/server.log" >&2
exit 1
