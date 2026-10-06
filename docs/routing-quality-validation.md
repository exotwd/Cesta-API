# Routing quality regression: Karlovo náměstí / Strossmayerovo náměstí

## Reproducible public query

The reference request uses `POST https://api.pojedes.cz/journeys/search`, Prague-local
2026-10-05, station `pid_gtfs:U237S1` and stop `pid_gtfs:U717Z1P`. Explicit stops
expand to boarding points, so the actual transit leg may use another platform ID
at the same named stop. Changing platform does not imply a different journey.

```sh
curl --fail-with-body --max-time 60 \
  -X POST https://api.pojedes.cz/journeys/search \
  -H 'Content-Type: application/json' \
  --data '{"from":{"type":"stop","id":"pid_gtfs:U237S1"},"to":{"type":"stop","id":"pid_gtfs:U717Z1P"},"datetime":"2026-10-05T10:00:00+02:00","mode":"depart_at","transport_modes":["metro","tram","bus","train","trolleybus","ferry"],"max_transfers":4,"walking_speed":"normal","prefer_reliable_transfers":true,"offline_compatible":false}'
```

This is a public journey lookup, not a database mutation, import, purchase or
account test. Run probes sequentially or with small bounded concurrency.

## Observed baseline before the October 4 routing change

Measured on 2026-10-04 around 15:47–15:51 UTC, against the running public API.
Times below are Europe/Prague on October 5. A direct tram means one transit leg
on line 6, zero transfers and zero walking; a transfer itinerary ending on line 6
does not count. Rank is the 1-based position in the public response.

| Requested time | All modes, max transfers 4 | First returned itinerary | Walking | Direct 6 control, max transfers 0 | Public request latency |
| --- | --- | --- | --- | --- | --- |
| 08:00 | 20 results; direct 6 at rank 4 | B → 26; 08:04:20–08:16; 700 s | 224 m | 08:04–08:20; 960 s; rank 1 | 0.664 s |
| 10:00 | 20 results; direct 6 absent | B → 6; 10:01:30–10:13; 690 s | 224 m | 10:07–10:23; 960 s; rank 2 | 0.404 s |
| 12:00 | 20 results; direct 6 absent | B → 6; 12:01:30–12:13; 690 s | 224 m | 12:07–12:23; 960 s; rank 2 | 0.330 s |
| 14:00 | 20 results; direct 6 absent | B → 6; 14:00:30–14:13; 750 s | 224 m | 14:07–14:23; 960 s; rank 2 | 0.422 s |

The control queries with only `tram` and `max_transfers: 4` also return direct
line 6 at rank 2 for each of these times. The all-mode zero-transfer control at
10:00/12:00/14:00 first returns line 17 with a 349 m endpoint walk and a 1120 s
journey; the no-walk line 6 follows. Scheduled line 6 takes 16 minutes in this
snapshot, so this fixture must not assert an unsupported 12-minute duration.

The all-mode max-transfer-4 responses at 10:00/12:00/14:00 report dominance
rejections of 21/19/30 candidates. They also explicitly report use of scheduled
times because the realtime routing cache was not ready. A future-day scheduled
query is not evidence that a specific realtime run is on time.

Reverse direction is useful because the failure is asymmetric:

| Requested time | Reverse all modes, max transfers 4 | Reverse direct 6 control, max transfers 0 |
| --- | --- | --- |
| 08:00 | First retained direct 6 at rank 8, 08:37–08:52 | 08:05–08:20; 900 s; rank 2 |
| 10:00 | First retained direct 6 at rank 4, 10:16–10:31 | 10:06–10:21; 900 s; rank 2 |
| 12:00 | First retained direct 6 at rank 5, 12:16–12:31 | 12:06–12:21; 900 s; rank 2 |
| 14:00 | First retained direct 6 at rank 3, 14:06–14:21 | 14:06–14:21; 900 s; rank 2 |

Latencies are individual public HTTP observations, include enrichment and cache
effects, and are not a statistically representative p95 benchmark. Several cold
reverse/tram requests took 1–3 s; repeated warm forward controls took 0.2–0.4 s.

## Regression matrix and acceptance checks

Use the Cartesian product of both directions, departure hours 08/10/12/14,
transport modes `all`/`tram`, and max transfers 0/4: 32 requests. Hold source
revision, service date, timezone and request preferences constant. Record returned
count, lines, transfer count, walking distance, first direct-trip departure,
arrival, rank and HTTP latency before and after deployment.

For this snapshot, the earliest eligible line 6 with no walking in a zero-transfer
control must remain discoverable in the all-mode search permitting transfers.
Both the fast transfer option and the simpler direct option are useful, and the
response limit must not allocate every slot to later transfer trips. Also check
that there are no duplicate transit itineraries, departures before the request,
disallowed transport modes or transfer counts above the request limit, and that
all legs have valid source geometry. Treat a real timetable or service-calendar
change as changed evidence, not a reason to hardcode line 6 into routing.

The retained local snapshot is
`storage/processed/routing/raptor-v13-2026-10-05-1791103857436-304c9f483405a53d.json`.
Its metadata is version 13, service date 2026-10-05, latest import
`2026-10-04T08:50:57.436550Z`, revision token `304c9f483405a53d`, 24,677 stops,
10,758 non-overtaking route patterns and 71,930 trips, with no unverified services.
The JSON envelope's `timetable` can be deserialized into routing-core's
`RaptorTimetable` for a DB-independent real-data check. The snapshot is a derived
operator artifact and must not be added to Git.

Run the isolated real-data routing and ranking regression without a database connection:

```sh
CESTA_ROUTING_REGRESSION_SNAPSHOT=storage/processed/routing/raptor-v13-2026-10-05-1791103857436-304c9f483405a53d.json \
  cargo test -p cesta-api --lib real_pid_snapshot_keeps_direct_trams_in_multimode_search -- --ignored --nocapture
```

This checks 32 combinations against the same immutable timetable. It does not perform HTTP
enrichment or add request-specific nearby walking links. The separate synthetic regression
`adaptive_search_keeps_exact_direct_route_when_walked_route_arrives_first` covers a verified
nearby walk competing with a direct tram and a faster metro interchange.

After deployment, run the full public matrix including actual response enrichment:

```sh
python3 tools/check_routing_quality.py --base-url https://api.pojedes.cz --date 2026-10-05
```

The validator checks all 32 responses for the earliest direct line 6 retained from the
zero-transfer control, bounded results, no duplicate visible journeys, no departed journeys,
mode and transfer limits, dated transit run IDs, valid WGS84 line geometries and correct fastest
labels. It prints observed ranks, durations and HTTP latency and exits unsuccessfully on a
regression. It uses the Prague timezone for each requested date.

## Implemented routing changes and limits

The direct scan uses the in-memory endpoint/route indexes and real service-verified trips;
it is independent of the bounded departure sampling and reserves distinct direct lines under
`max_direct_candidates`. It uses realtime-adjusted eligibility and aggregate times while
retaining the normal scheduled leg contract. The final Pareto criteria now include transfers
and walking distance, and the primary result, low-walking simple option and bounded reasonable
route alternatives reserve slots before departure-range sampling. Geometry preselection keeps
bounded alternatives until invalid shapes have been rejected.

Compared with the original time-only postprocessing this preserves actual travel tradeoffs,
at the cost of a somewhat larger candidate set. The direct scan adds only indexed stop-to-stop
work and makes direct-line discovery independent of probe sampling. It deliberately does not
claim exhaustive multicriteria or range routing: general RAPTOR still keeps earliest-arrival
labels within each round and the departure profile remains sampled. Full McRAPTOR label bags
and descending label-reusing rRAPTOR would improve completeness for arbitrary walking tradeoffs
and crowded departure intervals but need a separate latency/memory evaluation. These changes
also do not implement arrive-by or arbitrary intermediate-stay searches.

## Primary algorithm references

[Delling, Pajor and Werneck, Round-Based Public Transit Routing (2012)](https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/raptor_alenex.pdf)
defines arrival-time/transfer Pareto routing. Section 4.2 gathers every source
departure in the interval, processes departures latest first and reuses
per-round labels. It explicitly distinguishes those labels from global earliest
arrival pruning, which cannot be carried between earlier departures. Cesta's
bounded independently initialized departure sampling is an approximation:
bounded work is useful for latency, but it does not guarantee the complete range
frontier. Final pruning must preserve transfer and walking tradeoffs returned by
the core rather than undoing them with a time-only comparison.

[Spojenka's algorithm description](https://www.spojenka.cz/#faq) identifies RAPTOR,
bidirectional search, transfer limits and walking-plus-reserve transfer timing.
Its actual engine source is on [GitLab](https://gitlab.mff.cuni.cz/rehorc/spojenka-engine),
not an assumed GitHub repository. Inspected revision:
`7d7a55ee5bec2887f7f9d463c0fbd558dca72834` (2026-10-03).
[Raptor.h](https://gitlab.mff.cuni.cz/rehorc/spojenka-engine/-/blob/7d7a55ee5bec2887f7f9d463c0fbd558dca72834/SearchEngine/search/Raptor.h)
distinguishes physical arrival from transfer-adjusted readiness, retains arrival
by trip and by walking separately, and can reverse-search to shorten an itinerary.
[JourneySearch.cpp](https://gitlab.mff.cuni.cz/rehorc/spojenka-engine/-/blob/7d7a55ee5bec2887f7f9d463c0fbd558dca72834/SearchEngine/search/JourneySearch.cpp)
collects successive journey sets, including simpler alternatives, instead of
spending the complete output limit on a single flat time frontier. These are
design references; no Spojenka source code is copied into Cesta.
