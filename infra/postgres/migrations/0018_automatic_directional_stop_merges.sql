-- Automatically flatten nearby physical stop aliases that serve the same public
-- place in the same dominant travel direction. Every member must be within 120 m
-- of one canonical stop, so DBSCAN chaining cannot collapse a long stop corridor.
CREATE OR REPLACE FUNCTION cesta_apply_safe_nearby_direction_deduplication()
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  mappings_created bigint := 0;
  merge_result jsonb := '{}'::jsonb;
BEGIN
  WITH normalized_stops AS (
    SELECT
      stop.*,
      trim(regexp_replace(
        lower(unaccent(COALESCE(stop.municipality, ''))),
        '[^a-z0-9]+', ' ', 'g'
      )) AS municipality_key
    FROM stops AS stop
    WHERE stop.is_active = true
      AND stop.geom IS NOT NULL
      AND stop.lat IS NOT NULL
      AND stop.lon IS NOT NULL
      AND btrim(stop.name) <> ''
      AND btrim(stop.normalized_name) <> ''
      AND stop.location_type IN ('stop', 'station')
  ), physical_stops AS (
    SELECT
      normalized_stop.*,
      CASE
        WHEN municipality_key <> ''
         AND normalized_name LIKE municipality_key || ' %'
          THEN substr(normalized_name, char_length(municipality_key) + 2)
        ELSE normalized_name
      END AS public_name,
      COALESCE(city_id, NULLIF(municipality_key, ''), '') AS locality_key
    FROM normalized_stops AS normalized_stop
  ), candidate_pairs AS (
    SELECT left_stop.id AS left_stop_id, right_stop.id AS right_stop_id
    FROM physical_stops AS left_stop
    JOIN physical_stops AS right_stop
      ON right_stop.id > left_stop.id
     AND right_stop.public_name = left_stop.public_name
     AND right_stop.locality_key = left_stop.locality_key
     AND right_stop.location_type = left_stop.location_type
     AND ST_DWithin(left_stop.geom, right_stop.geom, 120)
    WHERE left_stop.public_name <> ''
  ), candidate_stop_ids AS (
    SELECT left_stop_id AS stop_id FROM candidate_pairs
    UNION
    SELECT right_stop_id FROM candidate_pairs
  ), direction_counts AS (
    SELECT
      call.stop_id,
      mod(floor((degrees(ST_Azimuth(
        origin.geom::geometry,
        next_stop.geom::geometry
      )) + 22.5) / 45.0)::integer, 8) AS direction_bucket,
      count(*) AS sample_count
    FROM stop_times AS call
    JOIN candidate_stop_ids AS candidate ON candidate.stop_id = call.stop_id
    JOIN stops AS origin ON origin.id = call.stop_id
    JOIN LATERAL (
      SELECT destination.geom
      FROM stop_times AS next_call
      JOIN stops AS destination ON destination.id = next_call.stop_id
      WHERE next_call.trip_id = call.trip_id
        AND next_call.stop_sequence > call.stop_sequence
        AND destination.geom IS NOT NULL
      ORDER BY next_call.stop_sequence ASC
      LIMIT 1
    ) AS next_stop ON true
    WHERE origin.geom IS NOT NULL
      AND ST_Distance(origin.geom, next_stop.geom) > 5
    GROUP BY call.stop_id, direction_bucket
  ), ranked_directions AS (
    SELECT *, row_number() OVER (
      PARTITION BY stop_id
      ORDER BY sample_count DESC, direction_bucket ASC
    ) AS rank
    FROM direction_counts
  ), directional_stops AS (
    SELECT physical_stop.*, direction.direction_bucket
    FROM physical_stops AS physical_stop
    JOIN ranked_directions AS direction
      ON direction.stop_id = physical_stop.id AND direction.rank = 1
  ), clustered_stops AS (
    SELECT
      directional_stop.*,
      ST_ClusterDBSCAN(
        ST_Transform(directional_stop.geom::geometry, 3857),
        eps := 200,
        minpoints := 2
      ) OVER (
        PARTITION BY directional_stop.public_name,
                     directional_stop.locality_key,
                     directional_stop.location_type,
                     directional_stop.direction_bucket
      ) AS cluster_id
    FROM directional_stops AS directional_stop
  ), stop_usage AS (
    SELECT call.stop_id, count(*) AS stop_time_count
    FROM stop_times AS call
    JOIN candidate_stop_ids AS candidate ON candidate.stop_id = call.stop_id
    GROUP BY call.stop_id
  ), canonical_candidates AS (
    SELECT
      candidate.public_name,
      candidate.locality_key,
      candidate.location_type,
      candidate.direction_bucket,
      candidate.cluster_id,
      candidate.id AS canonical_stop_id,
      candidate.source_priority,
      COALESCE(usage.stop_time_count, 0) AS stop_time_count,
      row_number() OVER (
        PARTITION BY candidate.public_name, candidate.locality_key,
                     candidate.location_type, candidate.direction_bucket,
                     candidate.cluster_id
        ORDER BY candidate.source_priority ASC,
                 COALESCE(usage.stop_time_count, 0) DESC,
                 candidate.id ASC
      ) AS canonical_rank
    FROM clustered_stops AS candidate
    JOIN clustered_stops AS member
      ON member.public_name = candidate.public_name
     AND member.locality_key = candidate.locality_key
     AND member.location_type = candidate.location_type
     AND member.direction_bucket = candidate.direction_bucket
     AND member.cluster_id = candidate.cluster_id
    LEFT JOIN stop_usage AS usage ON usage.stop_id = candidate.id
    WHERE candidate.cluster_id IS NOT NULL
    GROUP BY candidate.public_name, candidate.locality_key,
             candidate.location_type, candidate.direction_bucket,
             candidate.cluster_id, candidate.id, candidate.source_priority,
             usage.stop_time_count
    HAVING count(*) > 1
       AND max(ST_Distance(candidate.geom, member.geom)) <= 120
  ), grouped_stops AS (
    SELECT
      canonical.public_name,
      canonical.locality_key,
      canonical.location_type,
      canonical.direction_bucket,
      canonical.cluster_id,
      canonical.canonical_stop_id,
      array_agg(member.id ORDER BY member.id) AS stop_ids
    FROM canonical_candidates AS canonical
    JOIN clustered_stops AS member
      ON member.public_name = canonical.public_name
     AND member.locality_key = canonical.locality_key
     AND member.location_type = canonical.location_type
     AND member.direction_bucket = canonical.direction_bucket
     AND member.cluster_id = canonical.cluster_id
    WHERE canonical.canonical_rank = 1
    GROUP BY canonical.public_name, canonical.locality_key,
             canonical.location_type, canonical.direction_bucket,
             canonical.cluster_id, canonical.canonical_stop_id
  ), safe_groups AS (
    SELECT grouped_stop.*
    FROM grouped_stops AS grouped_stop
    WHERE NOT EXISTS (
      SELECT 1
      FROM stop_times AS call
      WHERE call.stop_id = ANY(grouped_stop.stop_ids)
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
    'Automatically confirmed nearby same-direction duplicate'
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
  exact_stop_deduplication jsonb := '{}'::jsonb;
  nearby_direction_deduplication jsonb := '{}'::jsonb;
  automatic_exact_stop_merges bigint := 0;
  automatic_nearby_stop_merges bigint := 0;
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

  SELECT cesta_apply_safe_stop_deduplication()
  INTO exact_stop_deduplication;
  automatic_exact_stop_merges := COALESCE(
    (exact_stop_deduplication->>'mappings_created')::bigint,
    0
  );

  SELECT cesta_apply_safe_nearby_direction_deduplication()
  INTO nearby_direction_deduplication;
  automatic_nearby_stop_merges := COALESCE(
    (nearby_direction_deduplication->>'mappings_created')::bigint,
    0
  );

  RETURN jsonb_build_object(
    'normalized_stop_names', normalized_stop_names,
    'assigned_stop_cities', assigned_stop_cities,
    'corrected_realtime_validity', corrected_realtime_validity,
    'automatic_exact_stop_merges', automatic_exact_stop_merges,
    'automatic_nearby_stop_merges', automatic_nearby_stop_merges,
    'exact_stop_deduplication', exact_stop_deduplication,
    'nearby_direction_deduplication', nearby_direction_deduplication,
    'records_changed', normalized_stop_names + assigned_stop_cities
      + corrected_realtime_validity + automatic_exact_stop_merges
      + automatic_nearby_stop_merges
  );
END;
$$;

-- Clean the already imported dataset once when this migration is deployed. Future
-- imports call cesta_apply_safe_data_repairs() through the normal audited pipeline.
DO $migration$
DECLARE
  initial_summary jsonb := '{}'::jsonb;
  initial_mapping_count bigint := 0;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('cesta-data-repair'));
  SELECT cesta_apply_safe_nearby_direction_deduplication()
  INTO initial_summary;
  initial_mapping_count := COALESCE(
    (initial_summary->>'mappings_created')::bigint,
    0
  );

  IF initial_mapping_count > 0 THEN
    INSERT INTO data_repair_runs (
      repair_type, status, summary, finished_at
    )
    VALUES (
      'automatic_nearby_direction_migration',
      'completed',
      initial_summary,
      now()
    );
  END IF;
END;
$migration$;
