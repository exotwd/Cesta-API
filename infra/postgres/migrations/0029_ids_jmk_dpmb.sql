-- IDS JMK is the official public-data channel for DPMB schedules and live
-- vehicle positions. Keep the full IDS feed because its GTFS does not expose a
-- reliable per-operator split; DPMB services are part of this coordinated feed.
INSERT INTO source_feeds (
  id, name, url, type, mode_scope, priority, enabled,
  license_id, attribution, terms_url, redistribution_allowed
)
VALUES
  (
    'ids_jmk_gtfs',
    'IDS JMK GTFS (includes DPMB)',
    'https://kordis-jmk.cz/gtfs/gtfs.zip',
    'gtfs',
    'ids_jmk_all_modes_including_dpmb',
    20,
    true,
    'CC-BY-4.0',
    'KORDIS JMK, a.s.; data from KORDIS JMK and DPMB',
    'https://www.idsjmk.cz/a/kontakty.html',
    true
  ),
  (
    'ids_jmk_realtime',
    'IDS JMK GTFS Realtime (includes DPMB)',
    'https://kordis-jmk.cz/gtfs/gtfsReal.dat',
    'gtfs_realtime',
    'ids_jmk_vehicle_positions_including_dpmb',
    20,
    true,
    'CC-BY-4.0',
    'KORDIS JMK, a.s.; data from KORDIS JMK and DPMB',
    'https://www.idsjmk.cz/a/kontakty.html',
    true
  )
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  url = EXCLUDED.url,
  type = EXCLUDED.type,
  mode_scope = EXCLUDED.mode_scope,
  priority = EXCLUDED.priority,
  enabled = EXCLUDED.enabled,
  license_id = EXCLUDED.license_id,
  attribution = EXCLUDED.attribution,
  terms_url = EXCLUDED.terms_url,
  redistribution_allowed = EXCLUDED.redistribution_allowed;

-- The previous performance view intentionally admitted only PID. Restore an
-- enabled-feed projection now that two independently tracked schedule sources
-- are served.
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

CREATE INDEX IF NOT EXISTS stops_ids_jmk_active_geom_gist
  ON stops USING gist (geom)
  WHERE source_feed_id = 'ids_jmk_gtfs'
    AND is_active = true
    AND geom IS NOT NULL;
