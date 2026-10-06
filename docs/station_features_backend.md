# Station plans, formations and metro boarding API

The API exposes three assistance contracts. Production responses are emitted only from imported,
source-attributed data. Development fixtures carry `mock: true` and must not be used for navigation.

## Stable identities

Stops retain their source-scoped `id` as `stop_id`. Stations additionally expose `station_id`,
`complex_id`, `has_station_layout`, and `station_layout_version`. A railway station and a metro
station in the same interchange must have different station IDs; a curator may assign the same
complex ID to group them.

For scheduled services the API derives opaque, stable IDs from the source-scoped `trip_id`, service
date, stop ID, and stop sequence:

- `run_id` identifies one dated run.
- `call_id` identifies one call of that run.

The same algorithm is used in departures, journey legs, stop calls, and realtime vehicles. Formation
imports must populate `service_runs` and `run_calls` with those published IDs.

## `GET /stations/{stationId}/layout`

`level={levelId}` is optional. The response uses camel-case envelope fields and snake-case element
properties, WGS84 GeoJSON coordinates in longitude/latitude order, stable element IDs, nullable
accessibility, and current facility availability. The requested and returned station IDs always
match exactly. Missing station, version, or level data returns `404`.

Static layouts are versioned in `station_layouts`, `station_layout_levels`, and
`station_layout_elements`. Time-limited overrides live separately in `station_facility_status`.
Responses include an ETag and a short cache lifetime because facility availability can change.

## `GET /runs/{runId}/formation`

`atCallId={callId}` selects the valid consist segment. The endpoint rejects a call belonging to a
different run. Vehicles preserve `array_index` ordering when positions are absent. Public coach
`number` is never substituted for `position`. Formation status is either `planned` or `confirmed`.

When `atCallId` is omitted, orientation is true only for one unsegmented formation covering the
whole run. Expired formations and ambiguous segmented formations are returned without a direction
claim (`orientationKnown: false`). Missing data returns `404`.

## `GET /journeys/{journeyId}/legs/{legIndex}/boarding-guidance`

Profiles are `fastest` and `wheelchair`. Rules are linked to the exact search journey and zero-based
leg index. The endpoint checks the active arrival-station layout version, target element, rule
validity, facility availability, and wheelchair verification before returning `status: available`.
Otherwise it returns an explicit unavailable result or `404` when no verified rule exists.

Initial production data should remain at `precision: zone` (`front`, `middle`, or `rear`). Coach and
door precision require a verified train type and stopping position. `station_path_edges` stores the
walking graph; inaccessible stairs and unavailable lifts must be excluded before a guidance row is
published.

## Import order

1. Import and review one railway layout and one metro interchange in both directions.
2. Activate exactly one layout version per station and import current facility observations.
3. Import dated runs, calls, formation segments, and ordered vehicles from an authorized source.
4. Import verified metro rules and walking edges, then bind computed results to journey IDs.
5. Validate geometry, attribution, licence, opposite directions, changed platforms, failed lifts,
   service-day boundaries, train reversal, and attached/detached coaches before release.

