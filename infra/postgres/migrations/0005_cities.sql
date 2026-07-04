CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE TABLE IF NOT EXISTS cities (
  id text PRIMARY KEY,
  official_municipality_id text NOT NULL,
  name text NOT NULL,
  normalized_name text NOT NULL,
  region text,
  country_code text NOT NULL,
  lat double precision,
  lon double precision,
  importance integer NOT NULL DEFAULT 0,
  source_url text NOT NULL DEFAULT 'https://github.com/33bcdd/souradnice-mest',
  source_reference_date date,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (country_code, official_municipality_id)
);

ALTER TABLE cities
  ADD COLUMN IF NOT EXISTS source_url text NOT NULL DEFAULT 'https://github.com/33bcdd/souradnice-mest',
  ADD COLUMN IF NOT EXISTS source_reference_date date;

ALTER TABLE stops
  ADD COLUMN IF NOT EXISTS city_id text REFERENCES cities(id),
  ADD COLUMN IF NOT EXISTS city_assignment_source text;

CREATE INDEX IF NOT EXISTS cities_normalized_name_trgm
  ON cities USING gin (normalized_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS cities_country_official_id_idx
  ON cities (country_code, official_municipality_id);
CREATE INDEX IF NOT EXISTS stops_city_active_idx
  ON stops (city_id) WHERE is_active = true;

INSERT INTO cities (
  id, official_municipality_id, name, normalized_name, region,
  country_code, lat, lon, importance
)
VALUES
  ('city:CZ:554782', '554782', 'Praha', 'praha', 'Hlavni mesto Praha', 'CZ', 50.0755, 14.4378, 100),
  ('city:CZ:582786', '582786', 'Brno', 'brno', 'Jihomoravsky kraj', 'CZ', 49.1951, 16.6068, 90),
  ('city:CZ:544256', '544256', 'Ceske Budejovice', 'ceske budejovice', 'Jihocesky kraj', 'CZ', 48.9747, 14.4749, 70),
  ('city:CZ:586846', '586846', 'Jihlava', 'jihlava', 'Kraj Vysocina', 'CZ', 49.3961, 15.5912, 65),
  ('city:CZ:541630', '541630', 'Vsetin', 'vsetin', 'Zlinsky kraj', 'CZ', 49.3387, 17.9962, 60)
ON CONFLICT (id) DO NOTHING;

UPDATE stops AS stop
SET city_id = city.id,
    city_assignment_source = 'name_fallback'
FROM cities AS city
WHERE stop.city_id IS NULL
  AND city.country_code = 'CZ'
  AND (
    trim(regexp_replace(lower(unaccent(COALESCE(stop.municipality, ''))), '[^a-z0-9]+', ' ', 'g')) = city.normalized_name
    OR trim(regexp_replace(lower(unaccent(stop.name)), '[^a-z0-9]+', ' ', 'g')) = city.normalized_name
    OR trim(regexp_replace(lower(unaccent(stop.name)), '[^a-z0-9]+', ' ', 'g')) LIKE city.normalized_name || ' %'
  );
