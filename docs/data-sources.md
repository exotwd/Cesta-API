# Data Sources

Current schedule sources:

- PID GTFS: `https://data.pid.cz/PID_GTFS.zip` (checked every 6 hours and imported only when changed;
  includes service calendars, stop hierarchy, coordinated pickup/drop-off rules, general
  minimum-transfer times and trip shapes)
- OpenStreetMap pedestrian graph through the configured Valhalla or OSRM walking router. Production
  uses a local ARM64 Valhalla service with persistent prebuilt tiles; Cesta persistently caches
  bounded route results and rejects ferry or other non-walking steps.
- PID current and seven-day route geometry: `https://data.pid.cz/geodata/Linky_7d_WGS84.json`
- IDS JMK GTFS: `https://kordis-jmk.cz/gtfs/gtfs.zip` (includes Brno urban services operated by
  DPMB as well as the rest of IDS JMK). The publisher exposes one aggregate agency, so the import
  deliberately retains the complete feed instead of guessing an operator split. A full import is
  rejected when its declared service horizon is already in the past.

Schedule downloads use conditional HTTP validators and SHA-256 before database export. PostgreSQL keeps the configured recent import audits and validation findings. Historical GGU imports and their source tracking remain stored for auditability, but GGU feeds are disabled and excluded from public data and routing.

Schedule imports preserve unchanged stop times and shape points instead of rewriting them for each
import run. Before and throughout database writes, the pipeline also checks the filesystem containing
PostgreSQL and refuses to import when less than 5 GiB is available. Production uses
`/mnt/cesta-data`; development or alternate deployments can set `DATABASE_STORAGE_PATH` and
`MIN_DATABASE_FREE_BYTES`. Regular vacuum runs both before and after a full import, and the large
schedule tables use aggressive autovacuum thresholds. This makes obsolete row space reusable
without requiring the temporary disk capacity of `VACUUM FULL`. An unchanged download is not
skipped when its core trips or stop times are missing, so the scheduler can automatically rebuild
regenerable timetable tables after storage recovery.

Current realtime sources:

- PID Golemio GTFS-Realtime trip updates plus the richer GeoJSON vehicle-position API, polled every 20 seconds. IDs match PID static GTFS. The GeoJSON adapter adds the public line, destination, vehicle type, registration number, wheelchair accessibility, air conditioning, USB chargers, speed, operator and tracking state. If the richer endpoint fails, the worker falls back to GTFS-Realtime positions.
- IDS JMK GTFS-Realtime vehicle positions: `https://kordis-jmk.cz/gtfs/gtfsReal.dat`, polled every
  15 seconds. Trip, route and stop IDs are scoped to the matching `ids_jmk_gtfs` rows. Public line,
  destination and mode are attached only through exact static-feed matches. The source currently
  publishes vehicle positions, not GTFS-Realtime TripUpdates, so Cesta does not invent stop delays.

Vehicle positions and compact trip-delay summaries are refreshed in independent loops so the larger
stop-level snapshot cannot block current map or routing data. The full stop-level import continues in
the background and its `syncing` state is exposed separately by `GET /realtime/status`.

Realtime records are a current-state cache rather than an audit log. PostgreSQL stores that table as
unlogged data, rebuilds it from the upstream feeds after a database restart, and vacuums it
aggressively. This prevents high-frequency position and delay updates from accumulating WAL or
permanent table bloat. The realtime worker also stops persistence below the same configurable
database free-space reserve used by schedule imports.

DÚK remains opt-in while redistribution terms are unresolved. IDS JMK is enabled independently with
`IDS_JMK_ENABLED=true` by default.

`PID_API_TOKEN` is sent as `X-Access-Token` when configured. No credential is committed. Golemio documents a default limit of 20 requests per 8 seconds; the default 20-second poll interval stays comfortably below it. Every record retains source identifiers, attribution, license metadata, fetch time and validity. Synchronization health is available from `GET /data-sources/status`.

Source and terms references:

- PID open data and attribution: `https://pid.cz/o-systemu/opendata/`
- Golemio public-transport API: `https://api.golemio.cz/pid/docs/openapi/`
- IDS JMK open data and CC BY 4.0 notice: `https://www.idsjmk.cz/a/kontakty.html`

Planned sources:

- IDOL
- official rail and regional GTFS/GTFS-RT feeds
