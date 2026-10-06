# Mix recipes run in every project, in dependency order, or in the one given
# (for example `just test slap_yjs`). Run them in `nix develop`.

projects := "slap_slatedb slap_cluster slap_streams slap_snapshot_log slap_yjs slap_kv slap_files slap"

default:
    @just --list

# Installs Hex and Rebar into .nix-mix, and fetches each project's dependencies.
deps project="":
    mix local.hex --if-missing --force
    mix local.rebar --if-missing --force
    just _each "{{ project }}" "mix deps.get"

# Runs a Durable Streams server on http://localhost:4437 (in memory).
dev:
    cd slap && mix slap.server --streams --store memory

# Everything CI runs for a project (see AGENTS.md): `mix check`, and for
# slap_slatedb also the NIF's Rust checks.
check project="":
    just _each "{{ project }}" "mix check"
    if [ -z "{{ project }}" ] || [ "{{ project }}" = slap_slatedb ]; then just check-rust; fi
    if [ -z "{{ project }}" ]; then just readme-preamble-check; fi

# The SlateDB NIF's Rust checks, as CI runs them.
check-rust:
    cd slap_slatedb/native/slatedb_nif && cargo fmt --check
    cd slap_slatedb/native/slatedb_nif && cargo clippy --release --all-targets -- -D warnings
    cd slap_slatedb/native/slatedb_nif && cargo test --release

# Runs the official Durable Streams conformance suite on slap, as CI does.
streams-conformance:
    #!/usr/bin/env bash
    set -euo pipefail
    cd slap
    log="${TMPDIR:-/tmp}/slap-streams-conformance-server.log"
    mix compile
    # The reference servers run this suite with a 500 ms long-poll timeout.
    mix slap.server --streams --port 4437 --store memory --long-poll-timeout 500 \
      --streams-flush-interval 10ms > "$log" 2>&1 &
    server=$!
    trap 'kill $server; echo "server log: $log"' EXIT
    for i in $(seq 1 60); do
      curl -sf -o /dev/null http://127.0.0.1:4437/health && break
      sleep 1
    done
    cd durable_streams
    pnpm install --frozen-lockfile
    DURABLE_STREAMS_URL=http://127.0.0.1:4437 pnpm test

streams-load:
    cd slap && mix run bench/streams_http_load.exs

# Runs the official Durable Streams benchmarks on slap and the Caddy server.
streams-bench *targets="slap-local caddy-file":
    cd slap && scripts/streams_bench.sh bench/results {{ targets }}
    cd slap/durable_streams && node summary.js ../bench/results

test project="":
    just _each "{{ project }}" "mix test"

lint project="":
    just _each "{{ project }}" "MIX_ENV=test mix format --check-formatted"
    just _each "{{ project }}" "MIX_ENV=test mix deps.unlock --check-unused"
    just _each "{{ project }}" "MIX_ENV=test mix credo"
    if [ -z "{{ project }}" ]; then just readme-preamble-check; fi

# Reports Reach's smell findings without failing on them.
smells project="":
    just _each "{{ project }}" "MIX_ENV=test mix reach.check --smells"

typecheck project="":
    just _each "{{ project }}" "MIX_ENV=test mix dialyzer"

# Writes scripts/readme_preamble.md into each package README.
readme-preamble:
    scripts/readme_preamble.sh write "{{ projects }}"

# Checks that each package README contains scripts/readme_preamble.md, as CI does.
readme-preamble-check:
    scripts/readme_preamble.sh check "{{ projects }}"

format:
    nixpkgs-fmt flake.nix
    cd slap_slatedb/native/slatedb_nif && cargo fmt
    just _each "" "mix format"

ci: check

# Shows each package's version, tag, Hex and CHANGELOG state, and commits since its last tag.
release-status *packages:
    @scripts/release.sh status "{{ projects }}" {{ packages }}

# Tags HEAD `<package>-v<version>` for each untagged package version, after confirming.
release-tags *packages:
    @scripts/release.sh tags "{{ projects }}" {{ packages }}

# Publishes to Hex each tagged package version not on Hex yet (RELEASING.md).
publish *packages:
    @scripts/release.sh publish "{{ projects }}" {{ packages }}

# Replaces each package's HexDocs for its released version with docs built from HEAD.
publish-docs *packages:
    @scripts/release.sh docs "{{ projects }}" {{ packages }}

_each project command:
    #!/usr/bin/env bash
    set -euo pipefail
    for p in {{ if project == "" { projects } else { project } }}; do
      echo "==> $p: {{ command }}"
      (cd "$p" && {{ command }})
    done

# Builds the Jepsen node image and starts the nodes and RustFS.
jepsen-up:
    docker build -f jepsen/docker/Dockerfile -t slap-jepsen-node .
    docker compose -f jepsen/docker/compose.yml up -d

jepsen-down:
    docker compose -f jepsen/docker/compose.yml down

jepsen-test:
    docker build --target test --output type=cacheonly -f jepsen/docker/Dockerfile .
    cd jepsen && lein test

# Runs a Jepsen test, for example `just jepsen --workload yjs --time-limit 300`.
jepsen *args:
    cd jepsen && lein run test {{ args }}
