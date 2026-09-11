-- GTFS shape distances are optional, but preserving them makes shape ordering and
-- diagnostics auditable without changing the existing point geometry storage.
ALTER TABLE shapes
  ADD COLUMN IF NOT EXISTS distance_traveled double precision;

-- A pedestrian engine owns the expensive OSM graph. Cesta stores only bounded
-- point-to-point results, including negative results, so request handling never
-- rebuilds or downloads a walking graph.
CREATE TABLE IF NOT EXISTS pedestrian_route_cache (
  from_lat_e5 integer NOT NULL,
  from_lon_e5 integer NOT NULL,
  to_lat_e5 integer NOT NULL,
  to_lon_e5 integer NOT NULL,
  router_revision text NOT NULL,
  status text NOT NULL CHECK (status IN ('ok', 'no_route', 'non_walking_segment', 'invalid_geometry')),
  distance_meters integer,
  duration_seconds integer,
  geometry jsonb,
  failure_detail text,
  checked_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (from_lat_e5, from_lon_e5, to_lat_e5, to_lon_e5, router_revision),
  CHECK (
    (status = 'ok' AND distance_meters >= 0 AND duration_seconds >= 0 AND geometry IS NOT NULL)
    OR status <> 'ok'
  )
);

CREATE INDEX IF NOT EXISTS pedestrian_route_cache_expiry_idx
  ON pedestrian_route_cache (expires_at);

CREATE INDEX IF NOT EXISTS trips_shape_id_idx
  ON trips (shape_id)
  WHERE shape_id IS NOT NULL;

-- These legacy covering indexes predate the bounded trip-summary lookup. They
-- are fully superseded by realtime_updates_pid_trip_summary_routing_idx for
-- journey routing and realtime_updates_valid_trip_idx for trip/stop-call
-- lookups. Since fetched_at changes on every poll, retaining them also
-- amplifies every realtime UPSERT and can consume tens of gigabytes without
-- improving current queries.
DROP INDEX IF EXISTS realtime_updates_routing_latest_idx;
DROP INDEX IF EXISTS realtime_updates_routing_delay_idx;

-- Pareto dominance is objective across carriers. Diversity policies are applied
-- only after dominated candidates have been removed.
UPDATE routing_algorithm_config
SET dominate_only_same_carrier = false
WHERE id = 1;
