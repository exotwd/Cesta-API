-- Autocomplete must avoid scanning imported stop history for top and prefix suggestions.
CREATE INDEX IF NOT EXISTS stops_active_priority_name_idx
  ON stops (source_priority, name, platform_code, id)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS stops_active_normalized_prefix_idx
  ON stops (normalized_name text_pattern_ops, source_priority, name, id)
  WHERE is_active = true;
