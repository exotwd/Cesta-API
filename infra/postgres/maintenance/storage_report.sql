-- Read-only PostgreSQL storage report. Run with psql against the Cesta database.
SELECT
  current_database() AS database_name,
  pg_database_size(current_database()) AS bytes,
  pg_size_pretty(pg_database_size(current_database())) AS total_size;

SELECT
  schemaname,
  relname AS table_name,
  pg_relation_size(relid) AS table_bytes,
  pg_indexes_size(relid) AS index_bytes,
  pg_total_relation_size(relid) AS total_bytes,
  pg_size_pretty(pg_relation_size(relid)) AS table_size,
  pg_size_pretty(pg_indexes_size(relid)) AS indexes_size,
  pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
  n_live_tup,
  n_dead_tup,
  last_autovacuum,
  last_autoanalyze
FROM pg_catalog.pg_statio_user_tables
JOIN pg_catalog.pg_stat_user_tables USING (relid, schemaname, relname)
ORDER BY pg_total_relation_size(relid) DESC;

SELECT
  relname AS table_name,
  indexrelname AS index_name,
  pg_relation_size(indexrelid) AS index_bytes,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
  idx_scan
FROM pg_catalog.pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC;
