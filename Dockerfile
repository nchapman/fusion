# syntax=docker/dockerfile:1.6
FROM rust:1.82-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* ./
COPY src ./src
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

# Default NFSv3 port. 2049 < 1024 is privileged on Linux; either run with
# CAP_NET_BIND_SERVICE on the binary, use a higher port, or accept root.
# We default to running as `fusion` and using port 2049 only when the
# host kernel has been configured to allow it (e.g. sysctl
# net.ipv4.ip_unprivileged_port_start=2049).
EXPOSE 2049/tcp

ENV RUST_LOG=info
USER fusion
ENTRYPOINT ["/usr/local/bin/fusion"]
CMD ["--config", "/etc/fusion/config.yaml"]
