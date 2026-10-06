ALTER TABLE realtime_updates SET UNLOGGED;

COMMENT ON TABLE realtime_updates IS
  'Ephemeral current realtime state. Unlogged so high-frequency feed refreshes cannot exhaust PostgreSQL WAL storage; automatically repopulated by realtime-worker.';
