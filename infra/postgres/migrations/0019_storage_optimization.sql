-- stop_times already has a primary-key B-tree on (trip_id, stop_sequence).
-- Keeping the same non-unique index duplicates one of the largest timetable indexes
-- without providing an additional query path.
DROP INDEX CONCURRENTLY IF EXISTS stop_times_trip_sequence_idx;
