-- Enable GGU feeds for routing and public queries
UPDATE source_feeds
SET enabled = true
WHERE id IN ('ggu_jdf_gtfs_latest', 'ggu_czptt_gtfs_latest', 'ggu_jdf_raw_latest');
