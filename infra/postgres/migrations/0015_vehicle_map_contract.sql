ALTER TABLE stops
  ADD COLUMN IF NOT EXISTS location_type text NOT NULL DEFAULT 'stop',
  ADD COLUMN IF NOT EXISTS parent_station_id text,
  ADD COLUMN IF NOT EXISTS wheelchair_boarding text NOT NULL DEFAULT 'unknown';

ALTER TABLE realtime_updates
  ADD COLUMN IF NOT EXISTS route_short_name text,
  ADD COLUMN IF NOT EXISTS destination text,
  ADD COLUMN IF NOT EXISTS vehicle_type text,
  ADD COLUMN IF NOT EXISTS speed_kmh double precision,
  ADD COLUMN IF NOT EXISTS wheelchair_accessible boolean,
  ADD COLUMN IF NOT EXISTS air_conditioned boolean,
  ADD COLUMN IF NOT EXISTS usb_chargers boolean,
  ADD COLUMN IF NOT EXISTS occupancy_status text,
  ADD COLUMN IF NOT EXISTS vehicle_registration_number text,
  ADD COLUMN IF NOT EXISTS operator_name text,
  ADD COLUMN IF NOT EXISTS tracking boolean,
  ADD COLUMN IF NOT EXISTS state text;

ALTER TABLE source_feeds
  ADD COLUMN IF NOT EXISTS license_id text,
  ADD COLUMN IF NOT EXISTS attribution text,
  ADD COLUMN IF NOT EXISTS terms_url text,
  ADD COLUMN IF NOT EXISTS redistribution_allowed boolean;

CREATE INDEX IF NOT EXISTS realtime_updates_vehicle_position_gist
  ON realtime_updates USING gist (vehicle_position)
  WHERE vehicle_position IS NOT NULL;

UPDATE source_feeds
SET
  license_id = 'CC-BY',
  attribution = 'Pražská integrovaná doprava / Golemio',
  terms_url = 'https://pid.cz/o-systemu/opendata/',
  redistribution_allowed = true
WHERE id IN ('pid_gtfs', 'pid_realtime', 'pid_lines_geodata');

UPDATE source_feeds
SET
  url = 'https://kordis-jmk.cz/gtfs/gtfsReal.dat',
  type = 'gtfs_realtime',
  license_id = 'CC-BY-4.0',
  attribution = 'KORDIS JMK, a.s.',
  terms_url = 'https://www.idsjmk.cz/a/kontakty.html',
  redistribution_allowed = true
WHERE id = 'ids_jmk_realtime';

UPDATE source_feeds
SET
  license_id = NULL,
  attribution = 'Doprava Ústeckého kraje',
  terms_url = NULL,
  redistribution_allowed = false
WHERE id = 'duk_realtime';
