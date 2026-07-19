CREATE TABLE IF NOT EXISTS routing_algorithm_config (
  id smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  max_results integer NOT NULL DEFAULT 5 CHECK (max_results BETWEEN 1 AND 20),
  max_direct_candidates integer NOT NULL DEFAULT 20 CHECK (max_direct_candidates BETWEEN 1 AND 500),
  max_transfer_candidates integer NOT NULL DEFAULT 40 CHECK (max_transfer_candidates BETWEEN 1 AND 1000),
  min_transfer_seconds integer NOT NULL DEFAULT 300 CHECK (min_transfer_seconds BETWEEN 60 AND 3600),
  max_transfer_wait_seconds integer NOT NULL DEFAULT 7200 CHECK (max_transfer_wait_seconds BETWEEN 300 AND 21600),
  transfer_search_timeout_seconds integer NOT NULL DEFAULT 6 CHECK (transfer_search_timeout_seconds BETWEEN 1 AND 60),
  next_day_search_from_seconds integer NOT NULL DEFAULT 64800 CHECK (next_day_search_from_seconds BETWEEN 0 AND 86399),
  range_search_window_seconds integer NOT NULL DEFAULT 5400 CHECK (range_search_window_seconds BETWEEN 0 AND 21600),
  max_range_departures integer NOT NULL DEFAULT 10 CHECK (max_range_departures BETWEEN 1 AND 96),
  endpoint_access_cache_enabled boolean NOT NULL DEFAULT true,
  arrival_time_weight double precision NOT NULL DEFAULT 1 CHECK (arrival_time_weight BETWEEN 0 AND 10),
  duration_weight double precision NOT NULL DEFAULT 0 CHECK (duration_weight BETWEEN 0 AND 10),
  transfer_penalty_seconds integer NOT NULL DEFAULT 0 CHECK (transfer_penalty_seconds BETWEEN 0 AND 14400),
  preserve_simplest boolean NOT NULL DEFAULT true,
  preserve_each_transfer_count boolean NOT NULL DEFAULT true,
  preserve_carrier_diversity boolean NOT NULL DEFAULT true,
  remove_dominated boolean NOT NULL DEFAULT true,
  dominate_only_same_carrier boolean NOT NULL DEFAULT true,
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by text
);

INSERT INTO routing_algorithm_config (id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;
