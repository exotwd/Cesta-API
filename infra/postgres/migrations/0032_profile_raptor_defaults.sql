-- A dense urban network needs enough real departure events and result slots to
-- represent the departure/arrival Pareto frontier. Preserve explicit operator
-- tuning while upgrading the previous product defaults.
UPDATE routing_algorithm_config
SET max_results = CASE WHEN max_results = 5 THEN 20 ELSE max_results END,
    max_range_departures = CASE WHEN max_range_departures = 10 THEN 48 ELSE max_range_departures END,
    updated_at = now()
WHERE max_results = 5 OR max_range_departures = 10;
