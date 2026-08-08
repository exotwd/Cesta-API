-- Keep historical imports and their provenance, but serve and refresh only PID feeds.
UPDATE source_feeds
SET enabled = id IN ('pid_gtfs', 'pid_lines_geodata', 'pid_realtime');

CREATE OR REPLACE VIEW enabled_source_stops AS
SELECT
  stop.id,
  COALESCE(preferred.import_run_id, stop.import_run_id) AS import_run_id,
  COALESCE(preferred.source_feed_id, stop.source_feed_id) AS source_feed_id,
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
  COALESCE(preferred.priority, stop.source_priority) AS source_priority,
  stop.is_active,
  stop.created_at,
  stop.city_id,
  stop.city_assignment_source,
  stop.location_type,
  stop.parent_station_id,
  stop.wheelchair_boarding
FROM stops AS stop
LEFT JOIN LATERAL (
  SELECT source_id.source_feed_id, source_id.import_run_id, source_id.priority
  FROM stop_source_ids AS source_id
  JOIN source_feeds AS source_feed
    ON source_feed.id = source_id.source_feed_id
   AND source_feed.enabled = true
  WHERE source_id.stop_id = stop.id
  ORDER BY source_id.priority ASC, source_id.source_feed_id ASC
  LIMIT 1
) AS preferred ON true
WHERE preferred.source_feed_id IS NOT NULL
   OR stop.source_feed_id IS NULL
   OR EXISTS (
     SELECT 1
     FROM source_feeds AS direct_feed
     WHERE direct_feed.id = stop.source_feed_id
       AND direct_feed.enabled = true
   );
