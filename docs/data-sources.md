# Data Sources

Current schedule sources:

- GGU latest results: `https://data.jr.ggu.cz/results/latest/`
- JDF merged GTFS: `JDF_merged_GTFS.zip`
- CZPTT GTFS: `CZPTT_GTFS.zip`
- raw JDF archive: `JDF_merged.zip`
- conversion and fixing logs: `jdf-to-gtfs.log.json`, `czptt-to-gtfs.log.json`, `fixing.log.json`, `merging.log.json`, `main.log`
- PID GTFS: `https://data.pid.cz/PID_GTFS.zip` (checked every 6 hours and imported only when changed)

Schedule downloads use conditional HTTP validators when available and SHA-256 before database
export. Unchanged GGU files are hard-linked into mixed runs, while obsolete timestamped raw runs are
deleted according to `RAW_IMPORT_RUNS_TO_KEEP` (default `3`). PostgreSQL import audit and validation
history are retained.
- PID current and seven-day route geometry: `https://data.pid.cz/geodata/Linky_7d_WGS84.json`

Current realtime sources:

- PID Golemio GTFS-Realtime trip updates plus the richer GeoJSON vehicle-position API, polled every 20 seconds. IDs match PID static GTFS. The GeoJSON adapter adds the public line, destination, vehicle type, registration number, wheelchair accessibility, air conditioning, USB chargers, speed, operator and tracking state. If the richer endpoint fails, the worker falls back to GTFS-Realtime positions.
- Official IDS JMK GTFS-Realtime at `https://kordis-jmk.cz/gtfs/gtfsReal.dat`, polled every 30 seconds. It is published as open data under CC BY 4.0. Standard GTFS-Realtime fields are normalized without inventing unavailable vehicle equipment.
- The DÚK `GetTraffic` adapter remains implemented but is disabled by default (`DUK_ENABLED=false`) because redistribution terms for a third-party passenger application have not been verified. Do not enable it in production without written confirmation.

`PID_API_TOKEN` is sent as `X-Access-Token` when configured. No credential is committed. Golemio documents a default limit of 20 requests per 8 seconds; the default 20-second poll interval stays comfortably below it. Every record retains source identifiers, attribution, license metadata, fetch time and validity. Synchronization health is available from `GET /data-sources/status`.

Source and terms references:

- PID open data and attribution: `https://pid.cz/o-systemu/opendata/`
- Golemio public-transport API: `https://api.golemio.cz/pid/docs/openapi/`
- IDS JMK open data and CC BY 4.0 notice: `https://www.idsjmk.cz/a/kontakty.html`

Planned sources:

- IDOL
- official rail and regional GTFS/GTFS-RT feeds
