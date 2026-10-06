#!/usr/bin/env bash
# Runs the official Durable Streams benchmarks (durable_streams/, `pnpm
# bench`) against `mix slap.server` and against the official Caddy server,
# one target at a time on port 4437, and writes each target's results to
# OUT_DIR/<target>.json, removing earlier results there.
#
#   scripts/streams_bench.sh OUT_DIR TARGET...
#
# Targets:
#   slap-memory    mix slap.server, in-memory store
#   slap-local     mix slap.server, local directory store
#   slap-s3        mix slap.server, on SLAP_BENCH_S3_URL (s3://bucket/prefix)
#                  with the AWS_* variables
#   caddy-memory   the Caddy server, in-memory store
#   caddy-file     the Caddy server, file store (fsynced before replying)
#
# Both servers use their default flush and long-poll settings. The Caddy
# server is the pinned release below, cached in $XDG_CACHE_HOME/slap.
set -euo pipefail
out=$1
shift
cd "$(dirname "$0")/.."
mkdir -p "$out"
rm -f "$out"/*.json
out=$(cd "$out" && pwd)
port=4437
url=http://127.0.0.1:$port
work=$(mktemp -d)
server=""
trap '[ -n "$server" ] && kill $server 2>/dev/null; rm -rf "$work"' EXIT

caddy_version=0.3.0

caddy_binary() {
  local platform sha256 dir tarball
  platform=$(uname -s | tr '[:upper:]' '[:lower:]')_$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')
  case $platform in
    linux_amd64) sha256=120b03d91ad67adfb265bac72e2d06cae07c333621598b32ee3b0ae21d11acb9 ;;
    linux_arm64) sha256=7e908cb5bac87d7299e9529779993afe3bf223ad1e58b044e6face7e111c3914 ;;
    darwin_amd64) sha256=7c86670e8bd26ad10bb230edb864e7f428fba13cde45fcdcc65ba74affcbb444 ;;
    darwin_arm64) sha256=359c583bf7de24a37ed355be8c5cd75ef169136460242831cd3a71e179e0455f ;;
    *) echo "no Caddy server release for $platform" >&2 && exit 1 ;;
  esac
  dir=${XDG_CACHE_HOME:-$HOME/.cache}/slap/durable-streams-server-$caddy_version
  if [ ! -x "$dir/durable-streams-server" ]; then
    tarball=durable-streams-server_${caddy_version}_$platform.tar.gz
    curl -sfL -o "$work/$tarball" \
      "https://github.com/durable-streams/durable-streams/releases/download/v$caddy_version/$tarball"
    echo "$sha256  $work/$tarball" | sha256sum -c - >&2
    mkdir -p "$dir"
    tar -xzf "$work/$tarball" -C "$dir" durable-streams-server
  fi
  echo "$dir/durable-streams-server"
}

start_caddy() {
  local options=$1 caddy
  caddy=$(caddy_binary)
  cat > "$work/Caddyfile" <<EOF
{
	admin off
	auto_https off
}

:$port {
	route /v1/stream/* {
		durable_streams $options
	}
}
EOF
  "$caddy" run --config "$work/Caddyfile" --adapter caddyfile > "$work/server.log" 2>&1 &
  server=$!
}

start_slap() {
  mix slap.server --streams --port $port --store "$1" > "$work/server.log" 2>&1 &
  server=$!
}

# Ready once a stream can be created: the listener alone answers before
# slap's shards are open.
wait_ready() {
  for _ in $(seq 1 120); do
    curl -sf -o /dev/null --noproxy '*' -X PUT -H 'content-type: application/octet-stream' \
      "$url/v1/stream/_ready" && return 0
    sleep 1
  done
  echo "the server did not become ready; its log:" >&2
  cat "$work/server.log" >&2
  return 1
}

mix compile
(cd durable_streams && pnpm install --frozen-lockfile)

for target in "$@"; do
  rm -rf "$work/data"
  mkdir -p "$work/data"
  case $target in
    slap-memory) start_slap memory ;;
    slap-local) start_slap "local:$work/data" ;;
    slap-s3) start_slap "s3:${SLAP_BENCH_S3_URL:?set SLAP_BENCH_S3_URL for slap-s3}/$(date +%s)" ;;
    caddy-memory) start_caddy "" ;;
    caddy-file) start_caddy "{
			data_dir $work/data
		}" ;;
    *) echo "unknown target: $target" >&2 && exit 2 ;;
  esac
  wait_ready
  echo "== $target" >&2
  (cd durable_streams &&
    DURABLE_STREAMS_URL=$url BENCH_ENVIRONMENT=$target pnpm bench &&
    mv benchmark-results.json "$out/$target.json")
  kill $server
  wait $server 2>/dev/null || true
  server=""
done
