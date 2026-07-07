-- Stop search combines exact IDs, normalized names and original display names.
-- Partial trigram indexes keep every OR branch indexable without indexing inactive history.
CREATE INDEX IF NOT EXISTS stops_normalized_name_trgm_active
  ON stops USING gin (normalized_name gin_trgm_ops)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS stops_name_trgm_active
  ON stops USING gin (name gin_trgm_ops)
  WHERE is_active = true;

-- Search enrichment resolves source ownership by canonical stop ID.
CREATE INDEX IF NOT EXISTS stop_source_ids_stop_priority_idx
  ON stop_source_ids (stop_id, priority, source_feed_id);
