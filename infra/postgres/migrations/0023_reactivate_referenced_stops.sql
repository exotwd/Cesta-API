-- A stop referenced by the latest enabled-feed schedule must remain routable even when the
-- upstream stops file omits or supersedes its platform row.
WITH referenced_stops AS (
  SELECT DISTINCT ON (stop_time.stop_id)
    stop_time.stop_id,
    stop_time.import_run_id
  FROM stop_times AS stop_time
  JOIN source_feeds AS source_feed
    ON source_feed.id = stop_time.source_feed_id
   AND source_feed.enabled = true
  JOIN import_runs AS import_run
    ON import_run.id = stop_time.import_run_id
  ORDER BY stop_time.stop_id,
           import_run.finished_at DESC NULLS LAST,
           import_run.started_at DESC,
           import_run.id DESC
)
UPDATE stops AS stop
SET import_run_id = referenced_stop.import_run_id,
    is_active = true
FROM referenced_stops AS referenced_stop
WHERE stop.id = referenced_stop.stop_id
  AND (stop.import_run_id IS DISTINCT FROM referenced_stop.import_run_id
       OR stop.is_active = false);
