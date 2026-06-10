CREATE INDEX IF NOT EXISTS stops_geom_gist ON stops USING gist (geom);
CREATE INDEX IF NOT EXISTS stops_normalized_name_trgm ON stops USING gin (normalized_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS stop_areas_geom_gist ON stop_areas USING gist (geom);
CREATE INDEX IF NOT EXISTS routes_source_id_idx ON routes (source_id);
CREATE INDEX IF NOT EXISTS trips_route_id_idx ON trips (route_id);
CREATE INDEX IF NOT EXISTS trips_service_id_idx ON trips (service_id);
CREATE INDEX IF NOT EXISTS trips_feed_import_idx ON trips (source_feed_id, import_run_id);
CREATE INDEX IF NOT EXISTS stop_times_trip_sequence_idx ON stop_times (trip_id, stop_sequence);
CREATE INDEX IF NOT EXISTS stop_times_stop_departure_idx ON stop_times (stop_id, departure_time);
CREATE INDEX IF NOT EXISTS stop_times_stop_arrival_idx ON stop_times (stop_id, arrival_time);
CREATE INDEX IF NOT EXISTS stop_times_trip_stop_sequence_idx ON stop_times (trip_id, stop_id, stop_sequence);
CREATE INDEX IF NOT EXISTS stops_area_active_idx ON stops (stop_area_id) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS stops_name_coord_active_idx ON stops (normalized_name, lat, lon) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS realtime_updates_trip_id_idx ON realtime_updates (trip_id);
CREATE INDEX IF NOT EXISTS realtime_updates_stop_id_idx ON realtime_updates (stop_id);
CREATE INDEX IF NOT EXISTS validation_issues_severity_idx ON validation_issues (severity);
CREATE INDEX IF NOT EXISTS import_runs_created_at_idx ON import_runs (started_at);
CREATE INDEX IF NOT EXISTS user_sessions_user_id_idx ON user_sessions (user_id);
CREATE INDEX IF NOT EXISTS saved_places_user_id_idx ON saved_places (user_id);
CREATE INDEX IF NOT EXISTS favorite_stops_user_stop_idx ON favorite_stops (user_id, stop_id);
CREATE INDEX IF NOT EXISTS favorite_routes_user_route_idx ON favorite_routes (user_id, route_id);

INSERT INTO source_feeds (id, name, url, type, mode_scope, priority)
VALUES
  ('ggu_jdf_gtfs_latest', 'GGU JDF merged GTFS latest', 'https://data.jr.ggu.cz/results/latest/JDF_merged_GTFS.zip', 'gtfs', 'bus_and_regional_public_transport', 30),
  ('ggu_czptt_gtfs_latest', 'GGU CZPTT GTFS latest', 'https://data.jr.ggu.cz/results/latest/CZPTT_GTFS.zip', 'gtfs', 'rail', 20),
  ('ggu_jdf_raw_latest', 'GGU JDF raw merged latest', 'https://data.jr.ggu.cz/results/latest/JDF_merged.zip', 'jdf_raw', null, 40)
ON CONFLICT (id) DO NOTHING;
