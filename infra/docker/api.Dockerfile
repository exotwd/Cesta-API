# syntax=docker/dockerfile:1.7
FROM rust:1.96-bookworm AS build
WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cesta-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cesta-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=cesta-rust-target,target=/app/target,sharing=locked \
    cargo build --locked --release -p cesta-api && \
    install -D /app/target/release/cesta-api /out/cesta-api

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/cesta-api /usr/local/bin/cesta-api
EXPOSE 8070
CMD ["cesta-api"]

