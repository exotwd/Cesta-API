-- Do not expose a schedule source until a complete successful import exists.
-- The scheduler enables it immediately after the validated export commits.
UPDATE source_feeds
SET enabled = EXISTS (
  SELECT 1
  FROM import_runs
  WHERE status = 'success'
    AND summary->>'feed_id' = 'ids_jmk_gtfs'
)
WHERE id = 'ids_jmk_gtfs';
