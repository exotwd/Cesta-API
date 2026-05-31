FROM rust:1.96-bookworm AS build
WORKDIR /app
COPY . .
RUN cargo build --release -p cesta-api

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/cesta-api /usr/local/bin/cesta-api
EXPOSE 8070
CMD ["cesta-api"]

