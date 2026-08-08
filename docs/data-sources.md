# Data Sources

Current schedule sources:

- PID GTFS: `https://data.pid.cz/PID_GTFS.zip` (checked every 6 hours and imported only when changed)
- PID current and seven-day route geometry: `https://data.pid.cz/geodata/Linky_7d_WGS84.json`

Schedule downloads use conditional HTTP validators and SHA-256 before database export. PostgreSQL keeps the configured recent import audits and validation findings. Historical GGU imports and their source tracking remain stored for auditability, but GGU feeds are disabled and excluded from public data and routing.

Current realtime sources:

- PID Golemio GTFS-Realtime trip updates plus the richer GeoJSON vehicle-position API, polled every 20 seconds. IDs match PID static GTFS. The GeoJSON adapter adds the public line, destination, vehicle type, registration number, wheelchair accessibility, air conditioning, USB chargers, speed, operator and tracking state. If the richer endpoint fails, the worker falls back to GTFS-Realtime positions.

The IDS JMK and DÚK adapters remain implemented but are not polled while `NON_PID_REALTIME_ENABLED=false`. Their source feeds are disabled in the PID-only policy.

`PID_API_TOKEN` is sent as `X-Access-Token` when configured. No credential is committed. Golemio documents a default limit of 20 requests per 8 seconds; the default 20-second poll interval stays comfortably below it. Every record retains source identifiers, attribution, license metadata, fetch time and validity. Synchronization health is available from `GET /data-sources/status`.

Source and terms references:

- PID open data and attribution: `https://pid.cz/o-systemu/opendata/`
- Golemio public-transport API: `https://api.golemio.cz/pid/docs/openapi/`
- IDS JMK open data and CC BY 4.0 notice: `https://www.idsjmk.cz/a/kontakty.html`

Planned sources:

- IDOL
- official rail and regional GTFS/GTFS-RT feeds
