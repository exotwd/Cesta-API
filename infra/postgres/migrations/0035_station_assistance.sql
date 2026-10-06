ALTER TABLE stops
  ADD COLUMN IF NOT EXISTS station_id text,
  ADD COLUMN IF NOT EXISTS complex_id text,
  ADD COLUMN IF NOT EXISTS has_station_layout boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS station_layout_version text;

UPDATE stops
SET station_id = 'station:' || CASE
      WHEN location_type = 'station' THEN id
      WHEN parent_station_id IS NOT NULL THEN parent_station_id
      ELSE id
    END
WHERE station_id IS NULL
  AND (
    location_type = 'station'
    OR parent_station_id IS NOT NULL
    OR modes && ARRAY['train', 'metro']::text[]
  );

UPDATE stops
SET complex_id = station_id
WHERE complex_id IS NULL AND station_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS stops_station_id_idx
  ON stops (station_id) WHERE station_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS stops_complex_id_idx
  ON stops (complex_id) WHERE complex_id IS NOT NULL;

CREATE OR REPLACE FUNCTION assign_stop_station_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.station_id IS NULL AND (
    NEW.location_type = 'station'
    OR NEW.parent_station_id IS NOT NULL
    OR NEW.modes && ARRAY['train', 'metro']::text[]
  ) THEN
    NEW.station_id := 'station:' || CASE
      WHEN NEW.location_type = 'station' THEN NEW.id
      WHEN NEW.parent_station_id IS NOT NULL THEN NEW.parent_station_id
      ELSE NEW.id
    END;
  END IF;
  IF NEW.complex_id IS NULL AND NEW.station_id IS NOT NULL THEN
    NEW.complex_id := NEW.station_id;
  END IF;
  IF NEW.station_id IS NOT NULL THEN
    SELECT layout.version
    INTO NEW.station_layout_version
    FROM station_layouts layout
    WHERE layout.station_id = NEW.station_id AND layout.active
    LIMIT 1;
    NEW.has_station_layout := NEW.station_layout_version IS NOT NULL;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS stops_assign_station_identity ON stops;
CREATE TRIGGER stops_assign_station_identity
BEFORE INSERT OR UPDATE OF id, location_type, parent_station_id, modes, station_id, complex_id
ON stops FOR EACH ROW EXECUTE FUNCTION assign_stop_station_identity();

CREATE OR REPLACE VIEW enabled_source_stops AS
SELECT
  stop.id,
  COALESCE(preferred.import_run_id, stop.import_run_id) AS import_run_id,
  COALESCE(preferred.source_feed_id, stop.source_feed_id) AS source_feed_id,
  stop.name, stop.normalized_name, stop.municipality, stop.district, stop.region,
  stop.lat, stop.lon, stop.geom, stop.coordinate_confidence, stop.coordinate_source,
  stop.stop_area_id, stop.platform_code, stop.modes,
  COALESCE(preferred.priority, stop.source_priority) AS source_priority,
  stop.is_active, stop.created_at, stop.city_id, stop.city_assignment_source,
  stop.location_type, stop.parent_station_id, stop.wheelchair_boarding,
  stop.station_id, stop.complex_id, stop.has_station_layout, stop.station_layout_version
FROM stops AS stop
LEFT JOIN LATERAL (
  SELECT source_id.source_feed_id, source_id.import_run_id, source_id.priority
  FROM stop_source_ids AS source_id
  JOIN source_feeds AS source_feed
    ON source_feed.id = source_id.source_feed_id AND source_feed.enabled = true
  WHERE source_id.stop_id = stop.id
  ORDER BY source_id.priority ASC, source_id.source_feed_id ASC
  LIMIT 1
) AS preferred ON true
WHERE preferred.source_feed_id IS NOT NULL
   OR stop.source_feed_id IS NULL
   OR EXISTS (
     SELECT 1 FROM source_feeds AS direct_feed
     WHERE direct_feed.id = stop.source_feed_id AND direct_feed.enabled = true
   );

CREATE TABLE IF NOT EXISTS station_layouts (
  station_id text NOT NULL,
  version text NOT NULL,
  complex_id text,
  name text NOT NULL,
  updated_at timestamptz NOT NULL,
  source text NOT NULL,
  attribution text NOT NULL,
  active boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (station_id, version),
  CHECK (btrim(station_id) <> ''),
  CHECK (btrim(version) <> ''),
  CHECK (btrim(name) <> ''),
  CHECK (btrim(source) <> ''),
  CHECK (btrim(attribution) <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS station_layouts_one_active_version
  ON station_layouts (station_id) WHERE active;

CREATE TABLE IF NOT EXISTS station_layout_levels (
  station_id text NOT NULL,
  layout_version text NOT NULL,
  level_id text NOT NULL,
  name text NOT NULL,
  level_index integer NOT NULL,
  PRIMARY KEY (station_id, layout_version, level_id),
  FOREIGN KEY (station_id, layout_version)
    REFERENCES station_layouts(station_id, version) ON DELETE CASCADE,
  UNIQUE (station_id, layout_version, level_index),
  CHECK (btrim(level_id) <> ''),
  CHECK (btrim(name) <> '')
);

CREATE TABLE IF NOT EXISTS station_layout_elements (
  station_id text NOT NULL,
  layout_version text NOT NULL,
  element_id text NOT NULL,
  kind text NOT NULL,
  level_id text,
  label text,
  platform text,
  track text,
  wheelchair_accessible boolean,
  default_available boolean,
  geometry jsonb NOT NULL,
  properties jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (station_id, layout_version, element_id),
  FOREIGN KEY (station_id, layout_version)
    REFERENCES station_layouts(station_id, version) ON DELETE CASCADE,
  FOREIGN KEY (station_id, layout_version, level_id)
    REFERENCES station_layout_levels(station_id, layout_version, level_id),
  CHECK (btrim(element_id) <> ''),
  CHECK (kind IN (
    'entrance', 'platform', 'corridor', 'stairs', 'escalator',
    'elevator', 'service', 'pathway', 'gate', 'other'
  )),
  CHECK (jsonb_typeof(geometry) = 'object'),
  CHECK (geometry->>'type' IN (
    'Point', 'MultiPoint', 'LineString', 'MultiLineString',
    'Polygon', 'MultiPolygon'
  )),
  CHECK (jsonb_typeof(geometry->'coordinates') = 'array'),
  CHECK (jsonb_typeof(properties) = 'object')
);

CREATE TABLE IF NOT EXISTS station_facility_status (
  station_id text NOT NULL,
  element_id text NOT NULL,
  available boolean,
  observed_at timestamptz NOT NULL,
  valid_until timestamptz NOT NULL,
  source text NOT NULL,
  PRIMARY KEY (station_id, element_id, observed_at),
  CHECK (valid_until >= observed_at),
  CHECK (btrim(source) <> '')
);

CREATE INDEX IF NOT EXISTS station_facility_status_current_idx
  ON station_facility_status (station_id, element_id, valid_until DESC, observed_at DESC);

CREATE TABLE IF NOT EXISTS station_path_edges (
  station_id text NOT NULL,
  layout_version text NOT NULL,
  edge_id text NOT NULL,
  from_element_id text NOT NULL,
  to_element_id text NOT NULL,
  walking_seconds integer NOT NULL,
  direction text NOT NULL DEFAULT 'both',
  uses_stairs boolean NOT NULL DEFAULT false,
  uses_elevator_element_id text,
  wheelchair_accessible boolean,
  PRIMARY KEY (station_id, layout_version, edge_id),
  FOREIGN KEY (station_id, layout_version, from_element_id)
    REFERENCES station_layout_elements(station_id, layout_version, element_id) ON DELETE CASCADE,
  FOREIGN KEY (station_id, layout_version, to_element_id)
    REFERENCES station_layout_elements(station_id, layout_version, element_id) ON DELETE CASCADE,
  FOREIGN KEY (station_id, layout_version, uses_elevator_element_id)
    REFERENCES station_layout_elements(station_id, layout_version, element_id),
  CHECK (walking_seconds >= 0),
  CHECK (direction IN ('both', 'forward', 'reverse')),
  CHECK (from_element_id <> to_element_id)
);

CREATE TABLE IF NOT EXISTS service_runs (
  run_id text PRIMARY KEY,
  trip_id text NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
  service_date date NOT NULL,
  source text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (trip_id, service_date),
  CHECK (btrim(run_id) <> ''),
  CHECK (btrim(source) <> '')
);

CREATE TABLE IF NOT EXISTS run_calls (
  call_id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES service_runs(run_id) ON DELETE CASCADE,
  stop_id text NOT NULL REFERENCES stops(id),
  stop_sequence integer NOT NULL,
  scheduled_arrival integer,
  scheduled_departure integer,
  platform text,
  UNIQUE (run_id, stop_sequence),
  CHECK (btrim(call_id) <> ''),
  CHECK (stop_sequence >= 0)
);

CREATE INDEX IF NOT EXISTS run_calls_run_stop_idx
  ON run_calls (run_id, stop_id, stop_sequence);

CREATE TABLE IF NOT EXISTS train_formations (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  run_id text NOT NULL REFERENCES service_runs(run_id) ON DELETE CASCADE,
  valid_from_call_sequence integer,
  valid_to_call_sequence integer,
  status text NOT NULL,
  orientation_known boolean NOT NULL DEFAULT false,
  direction_label text,
  updated_at timestamptz NOT NULL,
  valid_until timestamptz NOT NULL,
  source text NOT NULL,
  CHECK (status IN ('planned', 'confirmed')),
  CHECK (valid_to_call_sequence IS NULL OR valid_from_call_sequence IS NULL
    OR valid_to_call_sequence >= valid_from_call_sequence),
  CHECK (valid_until >= updated_at),
  CHECK (btrim(source) <> '')
);

CREATE INDEX IF NOT EXISTS train_formations_lookup_idx
  ON train_formations (run_id, updated_at DESC, valid_until DESC);

CREATE TABLE IF NOT EXISTS formation_vehicles (
  formation_id uuid NOT NULL REFERENCES train_formations(id) ON DELETE CASCADE,
  array_index integer NOT NULL,
  position integer,
  number text,
  class text,
  vehicle_type text,
  is_locomotive boolean NOT NULL DEFAULT false,
  wheelchair_accessible boolean,
  features text[] NOT NULL DEFAULT '{}',
  destination text,
  PRIMARY KEY (formation_id, array_index),
  CHECK (array_index >= 0),
  CHECK (position IS NULL OR position > 0),
  CHECK (features <@ ARRAY[
    'wheelchair', 'bicycle', 'restaurant', 'bistro', 'wifi',
    'quiet', 'power_socket', 'air_conditioning'
  ]::text[])
);

CREATE TABLE IF NOT EXISTS metro_boarding_rules (
  rule_id text PRIMARY KEY,
  arrival_station_id text NOT NULL,
  line_id text NOT NULL,
  direction_id text NOT NULL,
  arrival_platform text NOT NULL,
  profile text NOT NULL,
  train_zone text NOT NULL,
  reason_code text NOT NULL,
  precision text NOT NULL DEFAULT 'zone',
  coach_position_from_front integer,
  door_side_relative_to_travel text,
  target_element_id text NOT NULL,
  layout_version text NOT NULL,
  verified_at timestamptz NOT NULL,
  valid_until timestamptz NOT NULL,
  source text NOT NULL,
  CHECK (profile IN ('fastest', 'wheelchair')),
  CHECK (train_zone IN ('front', 'middle', 'rear')),
  CHECK (reason_code IN ('closest_to_transfer', 'closest_to_exit', 'closest_to_elevator')),
  CHECK (precision IN ('zone', 'coach', 'door')),
  CHECK (coach_position_from_front IS NULL OR coach_position_from_front > 0),
  CHECK (door_side_relative_to_travel IS NULL OR door_side_relative_to_travel IN ('left', 'right')),
  CHECK (valid_until >= verified_at)
);

CREATE TABLE IF NOT EXISTS journey_boarding_guidance (
  journey_id text NOT NULL,
  leg_index integer NOT NULL,
  profile text NOT NULL,
  rule_id text NOT NULL REFERENCES metro_boarding_rules(rule_id),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (journey_id, leg_index, profile),
  CHECK (leg_index >= 0),
  CHECK (profile IN ('fastest', 'wheelchair'))
);

CREATE OR REPLACE FUNCTION refresh_station_layout_summary()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  changed_station_id text;
BEGIN
  IF TG_OP = 'DELETE' THEN
    changed_station_id := OLD.station_id;
  ELSE
    changed_station_id := NEW.station_id;
  END IF;
  UPDATE stops
  SET has_station_layout = EXISTS (
        SELECT 1 FROM station_layouts layout
        WHERE layout.station_id = stops.station_id AND layout.active
      ),
      station_layout_version = (
        SELECT layout.version FROM station_layouts layout
        WHERE layout.station_id = stops.station_id AND layout.active
        LIMIT 1
      )
  WHERE stops.station_id = changed_station_id;
  IF TG_OP = 'UPDATE' AND OLD.station_id <> NEW.station_id THEN
    UPDATE stops
    SET has_station_layout = EXISTS (
          SELECT 1 FROM station_layouts layout
          WHERE layout.station_id = OLD.station_id AND layout.active
        ),
        station_layout_version = (
          SELECT layout.version FROM station_layouts layout
          WHERE layout.station_id = OLD.station_id AND layout.active
          LIMIT 1
        )
    WHERE stops.station_id = OLD.station_id;
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS station_layout_summary_after_change ON station_layouts;
CREATE TRIGGER station_layout_summary_after_change
AFTER INSERT OR UPDATE OR DELETE ON station_layouts
FOR EACH ROW EXECUTE FUNCTION refresh_station_layout_summary();

UPDATE stops stop
SET has_station_layout = EXISTS (
      SELECT 1 FROM station_layouts layout
      WHERE layout.station_id = stop.station_id AND layout.active
    ),
    station_layout_version = (
      SELECT layout.version FROM station_layouts layout
      WHERE layout.station_id = stop.station_id AND layout.active
      LIMIT 1
    )
WHERE stop.station_id IS NOT NULL;
