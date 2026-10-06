# Backend API

The API exposes:

- `GET /health`, including separate `routing_schedule_ready` and `routing_realtime_ready` signals
- auth endpoints under `/auth`
- user data endpoints under `/me`
- stops under `/stops`, including ranked stop suggestions with canonical-name and alias metadata at
  `GET /stops/search?q=...&limit=10`
- a complete snapshot for local stop-name search at `GET /stops/catalog`
- departures under `/departures`
- journey search at `POST /journeys/search`
- realtime source status under `/realtime/status`
- current PID trip updates under `/realtime/trip/{trip_id}`
- current vehicle positions under `/vehicles` and `/realtime/vehicles`
- offline package metadata under `/offline`
- ticket recommendation placeholders under `/tickets`
- authenticated ČD searches, orders, add-ons, checkout, documents and refunds under `/ticketing`
- admin imports and data quality under `/admin`
- public board data under `/public/boards`

Every schedule/realtime response should include data-status metadata and warnings where data is mock, stale, unavailable or partial.

## Local stop search catalog

`GET /stops/catalog` downloads all active, named `stop` and `station` records from enabled sources in ID order. It returns `schema_version: 1`, `count`, `stops`, and `data_status`. Each compact record includes the stable stop ID, public and normalized name, canonical name, aliases, municipality, region, coordinates, modes, platform, location and place types, parent stop area ID, and source feed ID. A development fixture response declares `data_status.source: "mock"`.

Store the `ETag` response header with the downloaded catalog. On the next refresh, send `If-None-Match: <stored ETag>` to the same endpoint. `304 Not Modified` has no body, so keep the local catalog. `200 OK` contains a complete replacement; save its body and new `ETag` together. If a request fails, keep the previous catalog. The endpoint sends `Cache-Control: no-cache` so clients revalidate before replacing stored data. This catalog is for local stop lookup; it does not contain timetables or replace the ranked `/stops/search` response.

Clients may send `Accept-Encoding: gzip` to reduce the catalog download size. The ETag is a weak validator because the JSON and gzip representations describe the same catalog.

PID boarding points with different public names remain separate stop suggestions even when the
source places them in one interchange complex. The routing graph connects such points with verified
walking transfers instead of treating every service at the complex as boardable from every point.
Where PID publishes both Palackého náměstí platform pairs with the same base name, platforms I/J are
exposed as `Palackého náměstí (nábřeží)`; the stored source name and PID stop IDs remain unchanged.
The stop suggestion `modes` field includes only modes served by boarding points with that exact
public name. Journey endpoint resolution follows the same rule and never widens tram or other
service across differently named stops.

`POST /journeys/search` accepts `stop`, `city`, and `{ "type": "coordinate", "lat": ..., "lon": ... }`
points. Search `mode` must be `depart_at`; `arrive_by` and unknown modes return HTTP 400 with
`code: unsupported_search_mode` before routing. Clients must not offer arrival-by search until it is implemented.

`GET /health` reports `journey_search_latency.p50_ms`, `p95_ms`, `sample_count`, `failed`, and
`retained_limit` over the latest 50 searches, including failures, using nearest-rank percentiles.
Empty samples return null latencies. The bounded sample resets on restart and is operational
diagnostics, not a long-term service-level metric. Admin routing diagnostics also include
`p50_total_ms` and `p95_total_ms` alongside stage timings.

Journey search accepts the above
points. Coordinate points do not require `id`. Every returned journey leg contains GeoJSON
`geometry`: a clipped GTFS shape for transit or the verified pedestrian path for walking.
Transit legs also contain `direction`, resolved from the headsign valid at the boarding stop,
the trip-wide headsign, or finally the actual terminal stop of the trip. This is deliberately not
derived from the leg's `to_stop_id`, which may only be a transfer stop. Walking legs return `null`.
