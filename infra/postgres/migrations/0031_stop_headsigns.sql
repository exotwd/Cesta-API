-- A stop-level headsign overrides trips.headsign at the specific boarding stop.
-- This is required for loop, lasso and other services whose displayed direction
-- changes during one GTFS trip.
ALTER TABLE stop_times
  ADD COLUMN IF NOT EXISTS stop_headsign text;
