-- Automatically merge only exact, cross-feed aliases whose public and operational
-- metadata agree. Nearby or same-feed records remain review candidates.
CREATE OR REPLACE FUNCTION cesta_apply_safe_stop_deduplication()
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  mappings_created bigint := 0;
  merge_result jsonb := '{}'::jsonb;
BEGIN
  WITH exact_groups AS (
    SELECT
      stop.normalized_name,
      round(stop.lat::numeric, 5) AS lat_key,
      round(stop.lon::numeric, 5) AS lon_key,
      (array_agg(stop.id ORDER BY stop.source_priority ASC, stop.id ASC))[1]
        AS canonical_stop_id,
      array_agg(stop.id ORDER BY stop.source_priority ASC, stop.id ASC)
        AS stop_ids
    FROM stops AS stop
    WHERE stop.is_active = true
      AND stop.source_feed_id IS NOT NULL
      AND stop.lat IS NOT NULL
      AND stop.lon IS NOT NULL
      AND btrim(stop.name) <> ''
      AND btrim(stop.normalized_name) <> ''
      AND stop.location_type IN ('stop', 'station')
    GROUP BY stop.normalized_name,
             round(stop.lat::numeric, 5), round(stop.lon::numeric, 5)
    HAVING count(*) > 1
       AND count(DISTINCT stop.source_feed_id) = count(*)
       AND count(DISTINCT COALESCE(stop.platform_code, '')) = 1
       AND count(DISTINCT stop.location_type) = 1
       AND count(DISTINCT COALESCE(stop.parent_station_id, '')) = 1
       AND count(DISTINCT array_to_string(stop.modes, ',')) = 1
       AND count(DISTINCT COALESCE(stop.city_id, '')) = 1
  ), safe_groups AS (
    SELECT exact_group.*
    FROM exact_groups AS exact_group
    WHERE NOT EXISTS (
      SELECT 1
      FROM stop_times AS call
      WHERE call.stop_id = ANY(exact_group.stop_ids)
      GROUP BY call.trip_id
      HAVING count(DISTINCT call.stop_id) > 1
    )
  ), mappings AS (
    SELECT
      duplicate_stop_id AS stop_id,
      safe_group.canonical_stop_id AS target_stop_id
    FROM safe_groups AS safe_group
    CROSS JOIN LATERAL unnest(safe_group.stop_ids) AS duplicate_stop_id
    WHERE duplicate_stop_id <> safe_group.canonical_stop_id
  )
  INSERT INTO manual_stop_matches (
    stop_id, target_stop_id, confidence, note
  )
  SELECT
    mapping.stop_id,
    mapping.target_stop_id,
    'confirmed_duplicate',
    'Automatically confirmed exact cross-feed duplicate'
  FROM mappings AS mapping
  WHERE NOT EXISTS (
    SELECT 1
    FROM manual_stop_matches AS existing
    WHERE existing.stop_id = mapping.stop_id
      AND existing.confidence = 'confirmed_duplicate'
  );
  GET DIAGNOSTICS mappings_created = ROW_COUNT;

  IF mappings_created > 0 THEN
    SELECT cesta_apply_confirmed_stop_merges() INTO merge_result;
  END IF;

  RETURN jsonb_build_object(
    'mappings_created', mappings_created,
    'merge_result', merge_result
  );
END;
$$;

CREATE OR REPLACE FUNCTION cesta_apply_safe_data_repairs()
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  normalized_stop_names bigint := 0;
  assigned_stop_cities bigint := 0;
  corrected_realtime_validity bigint := 0;
  stop_deduplication jsonb := '{}'::jsonb;
  automatic_stop_merges bigint := 0;
BEGIN
  UPDATE stops
  SET normalized_name = trim(
    regexp_replace(lower(unaccent(name)), '[^a-z0-9]+', ' ', 'g')
  )
  WHERE btrim(name) <> ''
    AND btrim(normalized_name) = '';
  GET DIAGNOSTICS normalized_stop_names = ROW_COUNT;

  WITH exact_city_matches AS (
    SELECT
      stop.id AS stop_id,
      min(city.id) AS city_id
    FROM stops AS stop
    JOIN cities AS city
      ON city.country_code = 'CZ'
     AND city.normalized_name = trim(
       regexp_replace(lower(unaccent(stop.municipality)), '[^a-z0-9]+', ' ', 'g')
     )
    WHERE stop.is_active = true
      AND stop.city_id IS NULL
      AND COALESCE(btrim(stop.municipality), '') <> ''
    GROUP BY stop.id
    HAVING count(*) = 1
  )
  UPDATE stops AS stop
  SET city_id = exact_match.city_id,
      city_assignment_source = 'exact_municipality_repair'
  FROM exact_city_matches AS exact_match
  WHERE stop.id = exact_match.stop_id;
  GET DIAGNOSTICS assigned_stop_cities = ROW_COUNT;

  UPDATE realtime_updates
  SET valid_until = fetched_at
  WHERE valid_until IS NOT NULL
    AND valid_until < fetched_at;
  GET DIAGNOSTICS corrected_realtime_validity = ROW_COUNT;

  SELECT cesta_apply_safe_stop_deduplication() INTO stop_deduplication;
  automatic_stop_merges := COALESCE(
    (stop_deduplication->>'mappings_created')::bigint,
    0
  );

  RETURN jsonb_build_object(
    'normalized_stop_names', normalized_stop_names,
    'assigned_stop_cities', assigned_stop_cities,
    'corrected_realtime_validity', corrected_realtime_validity,
    'automatic_stop_merges', automatic_stop_merges,
    'stop_deduplication', stop_deduplication,
    'records_changed', normalized_stop_names + assigned_stop_cities
      + corrected_realtime_validity + automatic_stop_merges
  );
END;
$$;
