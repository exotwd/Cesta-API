-- Journey routing loads fresh delays for one service day before running RAPTOR.
-- Keep this partial and covering so the hot read does not scan vehicle-only rows.
CREATE INDEX CONCURRENTLY IF NOT EXISTS realtime_updates_routing_delay_idx
  ON realtime_updates (service_date, fetched_at DESC)
  INCLUDE (trip_id, stop_id, delay_seconds, valid_until)
  WHERE service_date IS NOT NULL
    AND trip_id IS NOT NULL
    AND delay_seconds IS NOT NULL;
