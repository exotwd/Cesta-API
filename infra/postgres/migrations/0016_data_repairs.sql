CREATE TABLE IF NOT EXISTS data_repair_runs (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  repair_type text NOT NULL,
  status text NOT NULL,
  requested_by uuid REFERENCES users(id),
  summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE INDEX IF NOT EXISTS data_repair_runs_created_at_idx
  ON data_repair_runs (created_at DESC);

CREATE OR REPLACE FUNCTION cesta_apply_safe_data_repairs()
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  normalized_stop_names bigint := 0;
  assigned_stop_cities bigint := 0;
  corrected_realtime_validity bigint := 0;
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

  RETURN jsonb_build_object(
    'normalized_stop_names', normalized_stop_names,
    'assigned_stop_cities', assigned_stop_cities,
    'corrected_realtime_validity', corrected_realtime_validity,
    'records_changed', normalized_stop_names + assigned_stop_cities + corrected_realtime_validity
  );
END;
$$;

CREATE OR REPLACE FUNCTION cesta_apply_confirmed_stop_merges()
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  merge_record record;
  mappings_applied bigint := 0;
  source_rows_moved bigint := 0;
  source_ids_moved bigint := 0;
  stop_times_moved bigint := 0;
BEGIN
  FOR merge_record IN
    SELECT DISTINCT ON (manual_match.stop_id)
      manual_match.stop_id AS source_stop_id,
      manual_match.target_stop_id
    FROM manual_stop_matches AS manual_match
    JOIN stops AS source_stop ON source_stop.id = manual_match.stop_id
    JOIN stops AS target_stop ON target_stop.id = manual_match.target_stop_id
    WHERE manual_match.confidence = 'confirmed_duplicate'
      AND manual_match.target_stop_id IS NOT NULL
      AND manual_match.stop_id <> manual_match.target_stop_id
    ORDER BY manual_match.stop_id, manual_match.created_at DESC, manual_match.id DESC
  LOOP
    UPDATE stops AS target
    SET modes = ARRAY(
        SELECT DISTINCT mode
          FROM unnest(target.modes || source.modes) AS combined_mode(mode)
          ORDER BY mode
        ),
        city_id = COALESCE(target.city_id, source.city_id),
        city_assignment_source = COALESCE(target.city_assignment_source, source.city_assignment_source),
        municipality = COALESCE(target.municipality, source.municipality),
        district = COALESCE(target.district, source.district),
        region = COALESCE(target.region, source.region),
        stop_area_id = COALESCE(target.stop_area_id, source.stop_area_id),
        wheelchair_boarding = CASE
          WHEN target.wheelchair_boarding = 'unknown' THEN source.wheelchair_boarding
          ELSE target.wheelchair_boarding
        END
    FROM stops AS source
    WHERE target.id = merge_record.target_stop_id
      AND source.id = merge_record.source_stop_id;

    INSERT INTO transfers (
      from_stop_id, to_stop_id, min_transfer_seconds, distance_meters,
      walking_geometry, confidence, accessibility_level, source
    )
    SELECT
      CASE WHEN transfer.from_stop_id = merge_record.source_stop_id
        THEN merge_record.target_stop_id ELSE transfer.from_stop_id END,
      CASE WHEN transfer.to_stop_id = merge_record.source_stop_id
        THEN merge_record.target_stop_id ELSE transfer.to_stop_id END,
      transfer.min_transfer_seconds,
      transfer.distance_meters,
      transfer.walking_geometry,
      transfer.confidence,
      transfer.accessibility_level,
      transfer.source
    FROM transfers AS transfer
    WHERE (transfer.from_stop_id = merge_record.source_stop_id
        OR transfer.to_stop_id = merge_record.source_stop_id)
      AND (CASE WHEN transfer.from_stop_id = merge_record.source_stop_id
        THEN merge_record.target_stop_id ELSE transfer.from_stop_id END)
        <> (CASE WHEN transfer.to_stop_id = merge_record.source_stop_id
        THEN merge_record.target_stop_id ELSE transfer.to_stop_id END)
    ON CONFLICT (from_stop_id, to_stop_id) DO NOTHING;

    DELETE FROM transfers
    WHERE from_stop_id = merge_record.source_stop_id
       OR to_stop_id = merge_record.source_stop_id;

    DELETE FROM favorite_stops AS favorite
    USING favorite_stops AS existing
    WHERE favorite.stop_id = merge_record.source_stop_id
      AND existing.user_id = favorite.user_id
      AND existing.stop_id = merge_record.target_stop_id;

    UPDATE favorite_stops
    SET stop_id = merge_record.target_stop_id
    WHERE stop_id = merge_record.source_stop_id;

    DELETE FROM favorite_routes AS favorite
    USING favorite_routes AS existing
    WHERE (favorite.from_stop_id = merge_record.source_stop_id
        OR favorite.to_stop_id = merge_record.source_stop_id)
      AND existing.id <> favorite.id
      AND existing.user_id = favorite.user_id
      AND existing.route_id = favorite.route_id
      AND existing.from_stop_id IS NOT DISTINCT FROM
        CASE WHEN favorite.from_stop_id = merge_record.source_stop_id
          THEN merge_record.target_stop_id ELSE favorite.from_stop_id END
      AND existing.to_stop_id IS NOT DISTINCT FROM
        CASE WHEN favorite.to_stop_id = merge_record.source_stop_id
          THEN merge_record.target_stop_id ELSE favorite.to_stop_id END;

    UPDATE favorite_routes
    SET from_stop_id = CASE WHEN from_stop_id = merge_record.source_stop_id
          THEN merge_record.target_stop_id ELSE from_stop_id END,
        to_stop_id = CASE WHEN to_stop_id = merge_record.source_stop_id
          THEN merge_record.target_stop_id ELSE to_stop_id END
    WHERE from_stop_id = merge_record.source_stop_id
       OR to_stop_id = merge_record.source_stop_id;

    UPDATE user_profiles
    SET home_stop_id = CASE WHEN home_stop_id = merge_record.source_stop_id
          THEN merge_record.target_stop_id ELSE home_stop_id END,
        work_stop_id = CASE WHEN work_stop_id = merge_record.source_stop_id
          THEN merge_record.target_stop_id ELSE work_stop_id END
    WHERE home_stop_id = merge_record.source_stop_id
       OR work_stop_id = merge_record.source_stop_id;

    UPDATE saved_places
    SET stop_id = merge_record.target_stop_id,
        updated_at = now()
    WHERE stop_id = merge_record.source_stop_id;

    UPDATE stop_times
    SET stop_id = merge_record.target_stop_id
    WHERE stop_id = merge_record.source_stop_id;
    GET DIAGNOSTICS source_rows_moved = ROW_COUNT;
    stop_times_moved := stop_times_moved + source_rows_moved;

    UPDATE realtime_updates
    SET stop_id = merge_record.target_stop_id
    WHERE stop_id = merge_record.source_stop_id;

    UPDATE stops
    SET parent_station_id = merge_record.target_stop_id
    WHERE parent_station_id = merge_record.source_stop_id;

    UPDATE stop_source_ids
    SET stop_id = merge_record.target_stop_id,
        suppressed_as_duplicate = true
    WHERE stop_id = merge_record.source_stop_id;
    GET DIAGNOSTICS source_rows_moved = ROW_COUNT;
    source_ids_moved := source_ids_moved + source_rows_moved;

    UPDATE stops
    SET is_active = false
    WHERE id = merge_record.source_stop_id
      AND is_active = true;

    mappings_applied := mappings_applied + 1;
  END LOOP;

  RETURN jsonb_build_object(
    'mappings_applied', mappings_applied,
    'source_ids_moved', source_ids_moved,
    'stop_times_moved', stop_times_moved
  );
END;
$$;
