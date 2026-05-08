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
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/fusion /usr/local/bin/fusion

# Default NFSv3 port. Override with --network host on macOS.
EXPOSE 2049/tcp

ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/fusion"]
CMD ["--config", "/etc/fusion/config.yaml"]
