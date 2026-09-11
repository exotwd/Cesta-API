-- Journey endpoint resolution expands a station to its active child platforms. Without this
-- index, the `id = $1 OR parent_station_id = $1` lookup can scan the full stops table.
CREATE INDEX CONCURRENTLY IF NOT EXISTS stops_active_parent_station_idx
  ON stops (parent_station_id)
  WHERE is_active = true
    AND parent_station_id IS NOT NULL;
