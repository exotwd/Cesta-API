# Backend API

The API exposes:

- `GET /health`, including separate `routing_schedule_ready` and `routing_realtime_ready` signals
- auth endpoints under `/auth`
- user data endpoints under `/me`
- stops under `/stops`, including ranked stop suggestions with canonical-name and alias metadata at
  `GET /stops/search?q=...&limit=10`
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

`POST /journeys/search` accepts `stop`, `city`, and `{ "type": "coordinate", "lat": ..., "lon": ... }`
points. Coordinate points do not require `id`. Every returned journey leg contains GeoJSON
`geometry`: a clipped GTFS shape for transit or the verified pedestrian path for walking.
