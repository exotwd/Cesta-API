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

- Range RAPTOR
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
  timeout, and the next-service-day threshold;
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
