# syntax=docker/dockerfile:1.7
FROM rust:1.96-bookworm AS build
WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cesta-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cesta-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=cesta-rust-target,target=/app/target,sharing=locked \
    cargo build --locked --release -p data-pipeline && \
    install -D /app/target/release/data-pipeline /out/data-pipeline

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/data-pipeline /usr/local/bin/data-pipeline
ENTRYPOINT ["data-pipeline"]
