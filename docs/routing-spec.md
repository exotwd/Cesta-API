# Routing Spec

Production uses round-based RAPTOR over indexed, date-specific imported timetables.
The Connection Scan Algorithm remains available for explicit development fixtures.

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
startup. If a timetable is already in memory but its disk snapshot is missing, every warmup pass
retries the atomic snapshot write. Filesystem failures are reported in `snapshot_status.warmup`
without taking the in-memory routing timetable out of service.

RAPTOR first searches only calendar-verified trips, preventing a faster legacy trip from suppressing
a real service during round scanning. It reruns with legacy trips enabled only when the verified
search returns no journey, and any resulting fallback is returned with a response warning.

For departure-at searches, RAPTOR uses a bounded rRAPTOR-style profile. The requested departure
time is searched first, followed by the real boardable departure events within
`range_search_window_seconds`, up to `max_range_departures`. For coordinate origins, each vehicle
departure is shifted backwards by its verified pedestrian-access duration, so the profile is based
on when the passenger must actually leave rather than when the vehicle leaves the stop. Dense event
sets are sampled across the complete time window instead of taking only the earliest events. Every
selected profile event is searched; finding a few route patterns no longer terminates the range
early. Each small batch uses bounded concurrency. Candidates are merged, deduplicated and ranked
after RAPTOR; weighted scoring is not used inside the RAPTOR round scan. Evening searches also skip
next-service-day RAPTOR when the current service day already produced enough candidates.

If all bounded departure probes still produce too few reasonable route patterns, up to two
additional bounded passes exclude the winning route and then the first route of the best alternative.
This exposes genuinely different itineraries, including metro combinations, without turning the
search into an unbounded combinatorial alternatives scan. Patterns outside the same 15-minute
quality window do not satisfy the diversity target or receive a reserved result slot.

RAPTOR's earliest-arrival labels do not enumerate every useful direct line. An additional indexed
scan therefore intersects the expanded origin and destination boarding positions and enumerates
forward direct services within the same departure window, independently of sampled range events.
It honors mode, verified service, pickup/drop-off and realtime eligibility and retains scheduled
leg times. It follows only the explicitly expanded endpoints; coordinate walking and nearby access
remain part of regular RAPTOR. `max_direct_candidates` caps these additional results, with one
earliest departure per line reserved before filling the cap with subsequent departures.

Final Pareto selection compares departure time, expected arrival, transfer count and walking
distance. A candidate is removed only when another departs no earlier, arrives no later, has no
more transfers and requires no more walking, with at least one strict improvement. The configured
primary ranked result reserves the first slot, including for a one-result limit. Simple journeys
(fewest transfers, then least walking) and bounded reasonable route/carrier alternatives reserve
slots before the remaining frontier is sampled across its full departure-time span. Each route
and carrier reservation is limited to at most four candidates and approximately one third of the
result limit; distinct route alternatives must satisfy the 15-minute arrival/duration quality
window. `dominate_only_same_carrier` restricts comparisons to matching known carrier signatures
when enabled. A best option for another carrier may survive as a potential fare exception.

Bounded selection runs before expensive shape loading, with dominance disabled. Up to twice the
public result limit is kept for geometry validation, providing fallbacks for invalid shapes without
fetching and clipping geometry for every raw range-search candidate. Final dominance and ranking
run after geometry validation, so an invalid faster candidate does not prematurely discard its
valid fallback.

When final selection returns no journey, `related.routing_diagnostics` identifies the failure
stage and reports expanded endpoint IDs, coordinate-access status, timetable size, active mode and
transfer filters, and candidate counts after service validation, deduplication, geometry validation
and dominance pruning. Successful public responses omit this diagnostic block.

When a range probe reaches the first transit leg through an endpoint walking link, the returned
walk is scheduled backwards from that vehicle's departure using its computed walking duration.
This reports the latest feasible walking departure instead of the arbitrary probe time. Final
deduplication ignores the absolute probe time of otherwise identical leading walks while retaining
their endpoints and duration, so one transit itinerary is not repeated for every range probe.

RFC3339 journey timestamps are converted to `Europe/Prague` before the service date and seconds
since midnight are derived. Offset-less date-times remain Prague-local wall times for backward
compatibility. A final API-boundary guard removes any same-day candidate whose first departure is
earlier than the requested Prague-local time, so stale or malformed timetable data cannot surface
an already-departed connection.

RAPTOR timetables include PID's general `transfer_type=2` minimum-change times, including official
same-stop change times. Candidate station complexes come from `stop_area_id`, `parent_station_id`,
source-native station relationships and conservative name/proximity grouping. Cross-mode implicit
edges are admitted only after the pedestrian engine finds a walking-only route; source proximity
never creates an edge by itself. Trip-pair `transfer_type=1` guarantees are counted in the import
summary but are not widened into generic links.

PID interchange-complex membership creates walking-transfer candidates; it never makes all member
stops equivalent journey endpoints. Differently named boarding points such as Karlovo náměstí,
Palackého náměstí and Moráň therefore retain their own served routes while still allowing a verified
walk between them.
Cross-name and cross-mode links always require a walking-only route from the pedestrian engine.
Same-name, same-mode platform links may use their measured straight-line distance, capped by the
same maximum interchange distance. Changing these rules increments the serialized timetable format
so an older transfer graph cannot remain active after deployment.

Among valid GTFS pickup and drop-off values, `1` is the prohibited action. PID values `2` and `3`
remain routable because they permit service with advance or driver coordination; the API exposes
the original values in related stop-time and stop-call metadata.

An explicit stop endpoint is expanded deterministically to its active station children, stop-area
members, PID complex members, railway platforms and co-located same-name siblings. Those IDs are
direct routing origins/destinations and never require a street-routing request to reach their own
platforms. Coordinate endpoints use a bounded indexed PostGIS lookup to find several nearby
boarding candidates. Air distance is only candidate generation: every coordinate-access edge must
then be returned by the configured Valhalla or OSRM pedestrian engine, must contain only walking
steps, and must stay within the walking limit. Ferry or other vehicle steps, missing routes,
excessive network distance and invalid endpoint snaps are rejected with diagnostic codes. Results
and negative results are stored in `pedestrian_route_cache` by rounded endpoints and router graph
revision, while concurrent request misses share a process-wide concurrency limit. A local
pedestrian engine owns a persistent, prebuilt OSM graph; Cesta never rebuilds that graph per query.
If that engine is unavailable while station transfers are verified, the incomplete timetable is
not published as a snapshot.

Fresh PID trip-summary delays are loaded once before the production API starts listening, refreshed
into a process-local routing cache every 60 seconds and passed into RAPTOR. `/health` reports
scheduled and realtime routing readiness separately and remains degraded in production until both
caches are ready. Detailed stop-level realtime remains part of response enrichment, but it is not
bulk-loaded on the route-search path. Route-search requests only read the process-local cache and
never wait for the realtime table; if the cache is not ready or is older than 90 seconds, scheduled
routing is returned immediately with an explicit warning. Effective delayed arrival and departure
times determine whether a transfer is catchable and which candidate arrives first. Journey-leg
`departure_time` and `arrival_time` remain scheduled service-day seconds; the existing `realtime`
object carries delay and estimated timestamps to the app. Top-level journey arrival and duration
reflect the effective times used for routing when delay data is available.

Every returned leg is enriched from its route, trip and stop records with `line`, `mode_name`, `route_name`,
`destination`, `display_name`, `from_stop_name` and `to_stop_name`. Internal route, trip and stop IDs
remain stable correlation keys but are not intended as user-visible labels. Each transit leg has its
GTFS `shape_id` geometry clipped between boarding and alighting in trip direction. Each walking leg
has the pedestrian-router geometry used to validate its edge. A candidate is rejected if any leg
lacks usable geometry; direct stop-to-stop fallback lines are never synthesized. `LineString` is the
normal representation and `MultiLineString` is accepted for genuinely discontinuous source paths.

Within a RAPTOR probe, route-queue scratch storage is reused between rounds and request-only
walking links use a sparse index. Static journey metadata queries run concurrently. Ticketing
references are installed in the process-local store before the response and are persisted to
PostgreSQL asynchronously, keeping database fsync latency outside the public route-search critical
path while preserving the existing opaque-reference API.

Trips are grouped by route and stop pattern, then split into non-overtaking chains before the
timetable is cached. This preserves RAPTOR's FIFO route assumption when an express service passes a
slower service on the same pattern. Boarding lookup uses the per-stop departure index directly for
routes without realtime changes; when realtime is present, only the affected route's bounded delay
window is scanned, so an unrelated delayed trip cannot widen every route scan.

On API startup and after every background warmup pass, processed snapshot retention removes lower
format versions, stale temporary files, duplicate data revisions for the same service date, and the
least recently written surplus snapshots. `ROUTING_SNAPSHOT_FILES_TO_KEEP` defaults to `8` and is
clamped to at least `2`; the newest snapshots for today and tomorrow are always protected. Files
from a newer API version and unrelated/manual files are never removed. Every deleted file is a
derived cache artifact and is rebuilt from PostgreSQL on demand.

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
