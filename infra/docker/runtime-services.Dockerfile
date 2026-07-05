# syntax=docker/dockerfile:1.7
FROM rust:1.96-bookworm AS build
WORKDIR /app
COPY . .

# API and realtime-worker share most dependencies. Building them in one stage prevents
# parallel Compose builds from storing two complete Rust release trees. The target tree
# remains a BuildKit cache and only the finished binaries enter the image graph.
RUN --mount=type=cache,id=cesta-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cesta-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=cesta-rust-target,target=/app/target,sharing=locked \
    cargo build --locked --release -p cesta-api -p realtime-worker && \
    install -D /app/target/release/cesta-api /out/cesta-api && \
    install -D /app/target/release/realtime-worker /out/realtime-worker

FROM debian:bookworm-slim AS runtime
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

FROM runtime AS api
COPY --from=build /out/cesta-api /usr/local/bin/cesta-api
EXPOSE 8070
CMD ["cesta-api"]

FROM runtime AS realtime-worker
COPY --from=build /out/realtime-worker /usr/local/bin/realtime-worker
CMD ["realtime-worker"]
