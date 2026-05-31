# Cesta API

Cesta API is an API-only backend foundation for Czech public transport data. It is designed for future mobile apps, web apps, public QR departure boards and admin tools, but this repository intentionally contains no frontend application.

## What Is Implemented

- Rust workspace with backend API, data-pipeline CLI and realtime-worker services.
- Shared transport domain model crates.
- Fixture-backed routing core with a simple Connection Scan Algorithm.
- GTFS importer crate that parses core GTFS files from zip archives and validates common data-quality issues.
- GGU latest downloader/import CLI foundation for `https://data.jr.ggu.cz/results/latest/`.
- API endpoints for health, metadata, auth, user data, stops, departures, journeys, realtime status, offline packages, tickets, public boards and admin import/data-quality status.
- PostgreSQL/PostGIS migrations for accounts, transport data, imports, validation and offline packages.
- Docker Compose for PostgreSQL/PostGIS, Redis, API, data pipeline and realtime worker.
- GitHub Actions CI for formatting, tests and clippy.

## What Is Mocked

- Realtime data uses explicit mock updates and reports `mock = true`.
- Ticket recommendation endpoints return mock recommendations and do not implement payment.
- The API uses fixture transport data only when `USE_MOCK_DATA=true`; with `USE_MOCK_DATA=false`, stop search, stop detail and departures read imported PostgreSQL data.
- Offline package records are metadata-only placeholders until package generation is wired to imported data.

## What Uses Real Data

- The `data-pipeline` service can download GGU latest GTFS and log files, archive them without overwrites, compute SHA-256 checksums, parse GTFS core files and export agencies, stops, routes, trips, stop times and validation issues to PostgreSQL.
- `/metadata/data-status`, `/stops/search`, `/stops/{id}` and `/departures` read imported database data when `USE_MOCK_DATA=false`.
- API response shapes include data freshness and warnings so mock or unavailable data is not hidden.

## Run Locally

```powershell
cp .env.example .env
docker compose up --build
```

If a host port is already taken, change only the host-side port. Containers still use `postgres:5432` and `redis:6379` internally:

```env
POSTGRES_HOST_PORT=5433
REDIS_HOST_PORT=6380
DATABASE_URL=postgres://cesta:cesta@postgres:5432/cesta
REDIS_URL=redis://redis:6379
API_PORT=8070
```

On ARM Linux servers, if `postgis/postgis:16-3.4` exits with `exec format error`, use an ARM-compatible PostGIS image:

```env
POSTGRES_IMAGE=imresamu/postgis-arm64:16-3.4-alpine3.21
```

Useful local commands:

```powershell
cargo test
cargo run -p cesta-api
cargo run -p data-pipeline -- import-and-validate ggu-latest --limit-rows 1000
cargo run -p data-pipeline -- summarize latest
cargo run -p realtime-worker
```

On Windows, native `cargo run` requires Visual Studio Build Tools with the C++ workload because the default Rust toolchain uses MSVC `link.exe`. If that is not installed, use Docker Compose:

```powershell
docker compose up --build
```

The API listens on `http://localhost:8070` by default.

## First Admin

Set these variables before starting the API:

```powershell
$env:ADMIN_BOOTSTRAP_EMAIL="admin@example.com"
$env:ADMIN_BOOTSTRAP_PASSWORD="change-me-locally"
```

The current bootstrap path is documented and represented in migrations; production bootstrap should be finalized with an explicit database task before deployment.

Admin database stats are available after logging in:

```powershell
$login = Invoke-RestMethod -Method Post http://localhost:8070/auth/login -ContentType "application/json" -Body '{"email":"admin@example.com","password":"change-me-locally"}'
Invoke-RestMethod http://localhost:8070/admin/database/stats -Headers @{ Authorization = "Bearer $($login.access_token)" }
```

## Example Calls

```powershell
Invoke-RestMethod http://localhost:8070/health
$stops = Invoke-RestMethod "http://localhost:8070/stops/search?q=a"
Invoke-RestMethod ("http://localhost:8070/departures?stopId=" + [uri]::EscapeDataString($stops.stops[0].id) + "&limit=5")
Invoke-RestMethod -Method Post http://localhost:8070/journeys/search -ContentType "application/json" -Body '{"from":{"type":"stop","id":"stop-praha-hl-n"},"to":{"type":"stop","id":"stop-brno-hl-n"},"datetime":"2026-07-06T21:05:00+02:00","mode":"depart_at","transport_modes":["train"],"max_transfers":4,"walking_speed":"normal","prefer_reliable_transfers":true,"offline_compatible":false}'
```

## GGU Latest Import

```powershell
docker compose --profile tools run --rm data-pipeline import-and-validate ggu-latest --limit-rows 1000
```

This downloads real GGU latest files into `storage/raw/...`, parses GTFS core files and exports imported rows to PostgreSQL. If the latest local GGU run still matches the remote `ETag`, `Last-Modified` or content length, the pipeline reuses the existing files instead of downloading them again. Database export also skips a source when the same feed checksum was already imported successfully, and it refuses to start a duplicate export while the same source is already running. Use `--force-db-export` only when you intentionally want to rewrite an unchanged feed. Full national imports can be large. Use `--limit-rows` for development and remove it for production-style runs.

After import, restart the API if it was already running:

```powershell
docker compose up --build api
```

## OpenAPI

The API exposes a static OpenAPI foundation at:

```text
GET /openapi.json
```

## Next Connections

- Wire API repositories to PostgreSQL queries for imported schedules.
- Replace fixture routing snapshots with generated per-service-day snapshots.
- Add official PID/GTFS-RT realtime integrations.
- Implement geodata reconciliation and manual match workflows.
- Add production bootstrap/admin management commands.
