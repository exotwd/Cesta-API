-- Preserve provenance for official PID transfer times imported from transfers.txt.
ALTER TABLE transfers
  ADD COLUMN IF NOT EXISTS import_run_id uuid REFERENCES import_runs(id),
  ADD COLUMN IF NOT EXISTS source_feed_id text REFERENCES source_feeds(id),
  ADD COLUMN IF NOT EXISTS transfer_type smallint;

CREATE INDEX IF NOT EXISTS idx_transfers_source_import
  ON transfers (source_feed_id, import_run_id);
