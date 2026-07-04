ALTER TABLE realtime_updates
  ADD COLUMN IF NOT EXISTS source_feed_id text REFERENCES source_feeds(id),
  ADD COLUMN IF NOT EXISTS source_entity_id text,
  ADD COLUMN IF NOT EXISTS vehicle_id text,
  ADD COLUMN IF NOT EXISTS bearing double precision,
  ADD COLUMN IF NOT EXISTS service_date date;

UPDATE realtime_updates
SET source_entity_id = id::text
WHERE source_entity_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS realtime_updates_source_entity_idx
  ON realtime_updates (source, source_entity_id)
  WHERE source_entity_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS realtime_updates_valid_trip_idx
  ON realtime_updates (trip_id, fetched_at DESC)
  WHERE trip_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS realtime_updates_valid_vehicle_idx
  ON realtime_updates (source, vehicle_id, fetched_at DESC)
  WHERE vehicle_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS data_source_syncs (
  source_id text PRIMARY KEY,
  source_url text NOT NULL,
  data_kind text NOT NULL,
  status text NOT NULL,
  last_attempt_at timestamptz NOT NULL,
  last_success_at timestamptz,
  source_timestamp timestamptz,
  records_received integer NOT NULL DEFAULT 0,
  records_written integer NOT NULL DEFAULT 0,
  error_message text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS route_geometries (
  source_feed_id text NOT NULL REFERENCES source_feeds(id),
  source_feature_id text NOT NULL,
  route_id text,
  source_route_id text NOT NULL,
  validity date[] NOT NULL DEFAULT '{}',
  geometry jsonb NOT NULL,
  geom geometry(Geometry, 4326),
  properties jsonb NOT NULL DEFAULT '{}'::jsonb,
  fetched_at timestamptz NOT NULL,
  PRIMARY KEY (source_feed_id, source_feature_id)
);

CREATE INDEX IF NOT EXISTS route_geometries_route_idx
  ON route_geometries (route_id);
CREATE INDEX IF NOT EXISTS route_geometries_geom_gist
  ON route_geometries USING gist (geom);

INSERT INTO source_feeds (id, name, url, type, mode_scope, priority)
VALUES
  ('pid_gtfs', 'PID GTFS', 'https://data.pid.cz/PID_GTFS.zip', 'gtfs', 'pid_all_modes', 10),
  ('pid_lines_geodata', 'PID line geometry', 'https://data.pid.cz/geodata/Linky_7d_WGS84.json', 'geojson', 'pid_routes', 10),
  ('pid_realtime', 'PID GTFS Realtime', 'https://api.golemio.cz/v2/vehiclepositions/gtfsrt/trip_updates.pb', 'gtfs_realtime', 'pid_all_modes', 10),
  ('ids_jmk_realtime', 'IDS JMK vehicle positions', 'https://gis.brno.cz/ags1/rest/services/Hosted/Kordis_26_polohy/FeatureServer/0', 'arcgis_realtime', 'ids_jmk', 30),
  ('duk_realtime', 'DUK vehicle positions', 'https://tabule.portabo.cz/api/v1-tabule/cis/GetTraffic/0', 'json_realtime', 'duk', 30),
  ('mock_realtime', 'Development mock realtime', 'mock://realtime-worker', 'mock', 'development_only', 1000)
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  url = EXCLUDED.url,
  type = EXCLUDED.type,
  mode_scope = EXCLUDED.mode_scope,
  priority = EXCLUDED.priority;
