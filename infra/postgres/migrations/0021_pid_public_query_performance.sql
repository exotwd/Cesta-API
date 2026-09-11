-- Public transport endpoints currently serve PID only. Keep the projection
-- index-friendly: indirect source mappings are historical provenance, not
-- public routing or map records.
CREATE OR REPLACE VIEW enabled_source_stops AS
SELECT
  stop.id,
  stop.import_run_id,
  stop.source_feed_id,
  stop.name,
  stop.normalized_name,
  stop.municipality,
  stop.district,
  stop.region,
  stop.lat,
  stop.lon,
  stop.geom,
  stop.coordinate_confidence,
  stop.coordinate_source,
  stop.stop_area_id,
  stop.platform_code,
  stop.modes,
  stop.source_priority,
  stop.is_active,
  stop.created_at,
  stop.city_id,
  stop.city_assignment_source,
  stop.location_type,
  stop.parent_station_id,
  stop.wheelchair_boarding
FROM stops AS stop
WHERE stop.source_feed_id = 'pid_gtfs';

CREATE INDEX IF NOT EXISTS stops_pid_active_geom_gist
  ON stops USING gist (geom)
  WHERE source_feed_id = 'pid_gtfs'
    AND is_active = true
    AND geom IS NOT NULL;
