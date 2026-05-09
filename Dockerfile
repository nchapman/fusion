# syntax=docker/dockerfile:1.6
FROM rust:1.85-slim AS builder
WORKDIR /build
# `build-essential` provides cc + make for tikv-jemalloc-sys to compile its
# bundled jemalloc. `pkg-config` is kept for any future native deps.
RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* ./
COPY src ./src
# Cargo refuses to parse the manifest if any declared `[[bench]]` target file
# is missing — even for `cargo build` which doesn't compile benches.
COPY benches ./benches
RUN --mount=type=cache,target=/build/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release && \
    cp target/release/fusion /usr/local/bin/fusion

FROM debian:bookworm-slim
# `gosu` drops privileges in the entrypoint after we've fixed up UIDs/perms
# as root. `passwd` provides usermod/groupmod for the PUID/PGID dance.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates gosu passwd libcap2-bin && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -r -s /usr/sbin/nologin -u 1000 fusion
COPY --from=builder /usr/local/bin/fusion /usr/local/bin/fusion
# Permit non-root to bind privileged ports (portmap on 111, optional NFS on
# 2049). Required because the entrypoint drops privs to PUID:PGID via gosu;
# without this, only PUID=0 could bind below 1024.
RUN setcap 'cap_net_bind_service=+ep' /usr/local/bin/fusion
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 111 = portmap (RPC discovery, used by Infuse and other clients without a
# port override). 11111 = default NFS bind; production deployments commonly
# set `server.bind: 0.0.0.0:2049` in config.yaml.
EXPOSE 111/tcp 11111/tcp

ENV RUST_LOG=info
# Entrypoint runs as root so it can usermod + chown, then drops to PUID:PGID
# via gosu. Set PUID=0 PGID=0 to skip the drop entirely.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["--config", "/etc/fusion/config.yaml"]
