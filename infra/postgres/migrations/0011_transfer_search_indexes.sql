-- Transfer routing resolves active services and the latest successful import on every search.
-- These indexes keep those small lookup CTEs from scanning their full history.
CREATE INDEX IF NOT EXISTS calendars_active_range_idx
  ON calendars (start_date, end_date, service_id);

CREATE INDEX IF NOT EXISTS calendar_dates_active_service_idx
  ON calendar_dates (date, exception_type, service_id);

CREATE INDEX IF NOT EXISTS import_runs_latest_successful_feed_idx
  ON import_runs ((summary->>'feed_id'), finished_at DESC, started_at DESC)
  WHERE status = 'success' AND summary ? 'feed_id';
