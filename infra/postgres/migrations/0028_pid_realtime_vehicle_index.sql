-- The realtime table is large and continuously written. Build this map-query index
-- concurrently from the schedule updater so API startup remains available.
DROP INDEX CONCURRENTLY IF EXISTS realtime_updates_pid_vehicle_latest_idx;

CREATE INDEX CONCURRENTLY realtime_updates_pid_vehicle_latest_idx
  ON realtime_updates (source_feed_id, vehicle_id, fetched_at DESC)
  WHERE source_feed_id = 'pid_realtime'
    AND vehicle_id IS NOT NULL
    AND vehicle_position IS NOT NULL;
