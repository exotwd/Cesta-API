# Routing Spec

Phase 1 uses a simple Connection Scan Algorithm over fixture or prepared imported snapshots.

Required routing behavior:

- earliest-arrival stop-to-stop search
- max transfer limit
- mode filters
- walking transfers
- up to five reasonable journeys
- warnings for incomplete data, short transfers, unavailable realtime and uncertain stop locations

Future work:

- arrive-by search
- multi-criteria ranking

## Administrator tuning

Database-backed searches read the singleton `routing_algorithm_config` profile for every request,
so a validated admin update affects new searches immediately without restarting the API or running
an import. `GET /admin/routing-algorithm` returns the active profile and defaults,
`PUT /admin/routing-algorithm` replaces it, and `DELETE /admin/routing-algorithm` restores defaults.
All three endpoints require an `admin` or `data_admin` access token.

The API keeps route search fast by serving RAPTOR from memory. On cache miss it first tries a
serialized timetable snapshot from `ROUTING_SNAPSHOT_DIR` (default
`storage/processed/routing`) keyed by service date, latest successful imports, and enabled source
state. Enabled feeds use calendar-confirmed service on the requested date. A latest successful
import that has no calendar data remains searchable as an explicitly unverified legacy fallback. If the
snapshot is missing or stale, the API rebuilds the timetable from PostgreSQL, writes a replacement
snapshot, and stores it in the in-memory cache. A background warmer refreshes today and tomorrow
every minute so new imports are picked up before most user searches; it never runs an import on API
startup.

RAPTOR first searches only calendar-verified trips, preventing a faster legacy trip from suppressing
a real service during round scanning. It reruns with legacy trips enabled only when the verified
search returns no journey, and any resulting fallback is returned with a response warning.

For departure-at searches, RAPTOR uses a bounded rRAPTOR-style range probe. The requested
departure time is searched first. If it produces too few distinct candidates, evenly spaced
coverage probes and real departures from resolved origin stops within
`range_search_window_seconds` are searched two at a time, up to `max_range_departures`, stopping as
soon as the candidate floor is reached. Each small batch uses bounded concurrency. Candidates from
those probes are merged, deduplicated and ranked after RAPTOR; weighted scoring is not used inside
the RAPTOR round scan. Evening searches also skip next-service-day RAPTOR when the current service
day already produced enough candidates.

RFC3339 journey timestamps are converted to `Europe/Prague` before the service date and seconds
since midnight are derived. Offset-less date-times remain Prague-local wall times for backward
compatibility. A final API-boundary guard removes any same-day candidate whose first departure is
earlier than the requested Prague-local time, so stale or malformed timetable data cannot surface
an already-departed connection.

RAPTOR timetables include imported transfers plus implicit same-station/platform interchange
footpaths derived from stop areas, railway station IDs, and conservative station-like
name/municipality/coordinate grouping. This lets transfers between platform-level stop records work
without adding route-specific exceptions.

Nearby origin and destination walking access is cached by routing-data revision, endpoint stop set,
direction and walking speed when `endpoint_access_cache_enabled` is true. Cache misses use the same
PostGIS radius query as before, so repeated searches avoid endpoint transfer latency without
changing option coverage.

Within a RAPTOR probe, route-queue scratch storage is reused between rounds and request-only
walking links use a sparse index. Static journey metadata queries run concurrently. Ticketing
references are installed in the process-local store before the response and are persisted to
PostgreSQL asynchronously, keeping database fsync latency outside the public route-search critical
path while preserving the existing opaque-reference API.

On API startup, snapshot files with a lower format version than the running API are deleted before
warmup. Current-version files, files from a newer version, and unrelated files are preserved.

`GET /admin/routing-algorithm` also reports `snapshot_status`: configured snapshot directory,
latest-import key, file sizes, per-date in-memory status, and the current background warmup stage.

The endpoint also reports bounded in-memory `search_diagnostics` for the latest 50 route searches.
It includes total latency, per-stage timings, cache-hit detail, stage averages and maxima, and the
currently observed bottleneck. Diagnostics reset when the API process restarts and are not exposed
on the public journey response.

The admin page separates controls into:

- candidate generation: direct/transfer query limits, valid transfer time window, transfer-query
  timeout, the next-service-day threshold, bounded range-search controls and endpoint access cache;
- ranking: `arrival_time × arrival_time_weight + duration × duration_weight + transfers ×
  transfer_penalty_seconds`, with the lowest score first;
- result selection: response limit, dominance pruning, simplest-journey coverage, transfer-count
  coverage, and carrier diversity.

Defaults use arrival time as the only score input and apply no transfer penalty. The fastest
connection can therefore be a transfer. The response labels the configured score winner
`doporuceno`, the actual earliest arrival `nejrychlejsi`, and the fewest-transfer result
`nejjednodussi`.

Carrier diversity is a proxy for preserving potentially cheaper alternatives; it is not fare
ranking. No journey is described as cheapest until verified fare data is imported.
- accessibility routing
- historical reliability
- realtime rerouting
