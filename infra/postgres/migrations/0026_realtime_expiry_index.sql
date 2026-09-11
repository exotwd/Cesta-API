-- Expired GTFS-RT entities are removed in small batches. Without an index the
-- bounded cleanup query still has to scan the whole table and times out once
-- realtime_updates grows large.
DROP INDEX IF EXISTS realtime_updates_routing_latest_idx_ccnew;

CREATE INDEX IF NOT EXISTS realtime_updates_valid_until_idx
  ON realtime_updates (valid_until)
  WHERE valid_until IS NOT NULL;
