# syntax=docker/dockerfile:1.6
FROM rust:1.85-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config && rm -rf /var/lib/apt/lists/*

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
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -r -s /usr/sbin/nologin -u 1000 fusion
COPY --from=builder /usr/local/bin/fusion /usr/local/bin/fusion

# Default bind is 0.0.0.0:11111 (non-privileged). For production NFS on
# port 2049 you'll need CAP_NET_BIND_SERVICE on the binary or
# `sysctl net.ipv4.ip_unprivileged_port_start=2049` on the host, plus
# `server.bind: 0.0.0.0:2049` in the config.
EXPOSE 11111/tcp

ENV RUST_LOG=info
USER fusion
ENTRYPOINT ["/usr/local/bin/fusion"]
CMD ["--config", "/etc/fusion/config.yaml"]
