ALTER TABLE routing_algorithm_config
  ADD COLUMN IF NOT EXISTS range_search_window_seconds integer NOT NULL DEFAULT 5400 CHECK (range_search_window_seconds BETWEEN 0 AND 21600),
  ADD COLUMN IF NOT EXISTS max_range_departures integer NOT NULL DEFAULT 10 CHECK (max_range_departures BETWEEN 1 AND 96),
  ADD COLUMN IF NOT EXISTS endpoint_access_cache_enabled boolean NOT NULL DEFAULT true;
