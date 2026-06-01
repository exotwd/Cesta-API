FROM rust:1.96-bookworm AS build
WORKDIR /app
COPY . .
RUN cargo build --release -p data-pipeline

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/data-pipeline /usr/local/bin/data-pipeline
ENTRYPOINT ["data-pipeline"]
