# Data Sources

Current schedule sources:

- GGU latest results: `https://data.jr.ggu.cz/results/latest/`
- JDF merged GTFS: `JDF_merged_GTFS.zip`
- CZPTT GTFS: `CZPTT_GTFS.zip`
- raw JDF archive: `JDF_merged.zip`
- conversion and fixing logs: `jdf-to-gtfs.log.json`, `czptt-to-gtfs.log.json`, `fixing.log.json`, `merging.log.json`, `main.log`
- PID GTFS: `https://data.pid.cz/PID_GTFS.zip` (checked every 6 hours and imported only when changed)
- PID current and seven-day route geometry: `https://data.pid.cz/geodata/Linky_7d_WGS84.json`

Current realtime sources:

- PID GTFS-Realtime trip updates and vehicle positions from Golemio, polled every 20 seconds. IDs match the PID static GTFS and update concrete trips and stops.
- IDS JMK ArcGIS vehicle positions, polled every 30 seconds. Delay values are normalized from minutes to seconds.
- DUK `GetTraffic` vehicle positions, polled every 30 seconds. Delay values are normalized from minutes to seconds.

`PID_API_TOKEN` is sent as `X-Access-Token` when configured. No credential is committed. Every record retains source identifiers, fetch time and validity. Synchronization health is available from `GET /data-sources/status`.

Planned sources:

- IDOL
- official rail and regional GTFS/GTFS-RT feeds
