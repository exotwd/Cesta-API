-- RAPTOR only needs current PID trip-summary delays for one service day. Keep this index small
-- enough to remain hot while the realtime worker continuously upserts vehicle and stop updates.
CREATE INDEX CONCURRENTLY IF NOT EXISTS realtime_updates_pid_trip_summary_routing_idx
  ON realtime_updates (service_date, fetched_at DESC)
  INCLUDE (trip_id, delay_seconds, valid_until)
  WHERE source = 'pid_gtfs_rt'
    AND source_entity_id >= 'trip-summary:'
    AND source_entity_id < 'trip-summary;'
    AND trip_id IS NOT NULL
    AND stop_id IS NULL
    AND delay_seconds IS NOT NULL;
