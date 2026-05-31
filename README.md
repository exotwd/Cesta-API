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
- The API uses fixture transport data when `USE_MOCK_DATA=true` or when no database import is available.
- Offline package records are metadata-only placeholders until package generation is wired to imported data.

## What Uses Real Data

- The `data-pipeline` service can download GGU latest GTFS and log files, archive them without overwrites, compute SHA-256 checksums and parse GTFS core files.
- Database migrations are ready for imported entities with source tracking and import run metadata.
- API response shapes include data freshness and warnings so mock or unavailable data is not hidden.

## Run Locally

```powershell
cp .env.example .env
docker compose up --build
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

The API listens on `http://localhost:8080` by default.

## First Admin

Set these variables before starting the API:

```powershell
$env:ADMIN_BOOTSTRAP_EMAIL="admin@example.com"
$env:ADMIN_BOOTSTRAP_PASSWORD="change-me-locally"
```

The current bootstrap path is documented and represented in migrations; production bootstrap should be finalized with an explicit database task before deployment.

## Example Calls

```powershell
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod "http://localhost:8080/stops/search?q=Praha"
Invoke-RestMethod "http://localhost:8080/departures?stopId=stop-praha-hl-n&limit=5"
Invoke-RestMethod -Method Post http://localhost:8080/journeys/search -ContentType "application/json" -Body '{"from":{"type":"stop","id":"stop-praha-hl-n"},"to":{"type":"stop","id":"stop-brno-hl-n"},"datetime":"2026-07-06T21:05:00+02:00","mode":"depart_at","transport_modes":["train"],"max_transfers":4,"walking_speed":"normal","prefer_reliable_transfers":true,"offline_compatible":false}'
```

## GGU Latest Import

```powershell
cargo run -p data-pipeline -- download ggu-latest
cargo run -p data-pipeline -- import ggu-latest --limit-rows 1000
cargo run -p data-pipeline -- validate latest
cargo run -p data-pipeline -- import-and-validate ggu-latest --limit-rows 1000
```

Full national imports can be large. Use `--limit-rows` for development and remove it for production-style runs.

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
