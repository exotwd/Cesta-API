CREATE UNLOGGED TABLE IF NOT EXISTS schedule_import_seen_keys (
  import_run_id uuid NOT NULL,
  entity_type text NOT NULL CHECK (entity_type IN ('shape', 'stop_time', 'trip')),
  entity_id text NOT NULL,
  sequence integer NOT NULL,
  PRIMARY KEY (import_run_id, entity_type, entity_id, sequence)
);

COMMENT ON TABLE schedule_import_seen_keys IS
  'Ephemeral keys seen during a GTFS import. Unlogged by design so unchanged schedule rows do not need a new import_run_id and do not generate replacement WAL.';

-- Realtime rows are an ephemeral current-state cache that is continuously rebuilt from the
-- upstream feeds. The previous logged table accumulated tens of gigabytes of heap and index bloat
-- from updates every few seconds. Reset it in its own committed migration so the old 81 GB
-- relfilenode is released before the follow-up migration changes table persistence.
TRUNCATE TABLE realtime_updates;
COMMENT ON TABLE realtime_updates IS
  'Ephemeral current realtime state, automatically repopulated by realtime-worker.';

ALTER TABLE realtime_updates SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold = 10000,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_analyze_threshold = 10000
);

ALTER TABLE trips SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold = 10000,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_analyze_threshold = 10000
);

ALTER TABLE stop_times SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold = 10000,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_analyze_threshold = 10000
);

ALTER TABLE shapes SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold = 10000,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_analyze_threshold = 10000
);

ALTER TABLE schedule_import_seen_keys SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold = 10000,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_analyze_threshold = 10000
);
