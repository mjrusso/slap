FROM rust:1.98.1-trixie@sha256:a8a5f0a1e5fe7dfe1d352591e4a1c7dd2c08fd70475cae872cf3458ba0df0546 AS rust

FROM hexpm/elixir:1.20.2-erlang-29.0.3-debian-trixie-20260623-slim@sha256:ac5a38d5ae181e9558b15ce32286bc4604b1835fb35ead161fa3b301df69eac7 AS build

ENV MIX_ENV=prod \
    SLAP_LOCAL_DEPS=1 \
    SLAP_SLATEDB_BUILD=1 \
    CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH="/usr/local/cargo/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=rust /usr/local/cargo /usr/local/cargo
COPY --from=rust /usr/local/rustup /usr/local/rustup

RUN mix local.hex --force && mix local.rebar --force

WORKDIR /src
COPY slap_slatedb ./slap_slatedb
COPY slap_cluster ./slap_cluster
COPY slap_streams ./slap_streams
COPY slap_snapshot_log ./slap_snapshot_log
COPY slap_yjs ./slap_yjs
COPY slap_kv ./slap_kv
COPY slap_files ./slap_files
COPY slap ./slap
COPY jepsen/node/mix.exs jepsen/node/mix.lock ./jepsen/node/

WORKDIR /src/jepsen/node
RUN mix deps.get --only prod --check-locked && mix deps.compile
COPY jepsen/node/lib ./lib
RUN mix compile --warnings-as-errors && mix release jepsen_node

FROM build AS test
ENV MIX_ENV=test
COPY jepsen/node/test ./test
RUN mix deps.get --only test --check-locked && mix test --no-start --warnings-as-errors

FROM debian:trixie-20260623-slim@sha256:28de0877c2189802884ccd20f15ee41c203573bd87bb6b883f5f46362d24c5c2

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates coreutils findutils gawk grep iproute2 iptables \
    libncurses6 libssl3t64 libstdc++6 procps sed sudo util-linux \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/jepsen/node/_build/prod/rel/jepsen_node /opt/jepsen_node

ENV RELEASE_TMP=/tmp \
    ELIXIR_ERL_OPTIONS=+fnu \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

CMD ["sleep", "infinity"]
