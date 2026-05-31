CREATE TABLE IF NOT EXISTS import_runs (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  source text NOT NULL,
  status text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  summary jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS source_feeds (
  id text PRIMARY KEY,
  name text NOT NULL,
  url text NOT NULL,
  type text NOT NULL,
  mode_scope text,
  priority integer NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agencies (
  id text PRIMARY KEY,
  import_run_id uuid REFERENCES import_runs(id),
  source_feed_id text REFERENCES source_feeds(id),
  source_id text NOT NULL,
  name text NOT NULL,
  url text,
  timezone text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS operators (
  id text PRIMARY KEY,
  import_run_id uuid REFERENCES import_runs(id),
  source_feed_id text REFERENCES source_feeds(id),
  source_id text,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stop_areas (
  id text PRIMARY KEY,
  name text NOT NULL,
  geom geography(Point, 4326),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stops (
  id text PRIMARY KEY,
  import_run_id uuid REFERENCES import_runs(id),
  source_feed_id text REFERENCES source_feeds(id),
  name text NOT NULL,
  normalized_name text NOT NULL,
  municipality text,
  district text,
  region text,
  lat double precision,
  lon double precision,
  geom geography(Point, 4326),
  coordinate_confidence text NOT NULL DEFAULT 'unresolved',
  coordinate_source text,
  stop_area_id text REFERENCES stop_areas(id),
  platform_code text,
  modes text[] NOT NULL DEFAULT '{}',
  source_priority integer NOT NULL DEFAULT 100,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stop_source_ids (
  stop_id text NOT NULL REFERENCES stops(id),
  source_feed_id text NOT NULL REFERENCES source_feeds(id),
  original_source_id text NOT NULL,
  import_run_id uuid REFERENCES import_runs(id),
  priority integer NOT NULL,
  confidence text,
  suppressed_as_duplicate boolean NOT NULL DEFAULT false,
  PRIMARY KEY (source_feed_id, original_source_id)
);

CREATE TABLE IF NOT EXISTS routes (
  id text PRIMARY KEY,
  import_run_id uuid REFERENCES import_runs(id),
  source_feed_id text REFERENCES source_feeds(id),
  source_id text NOT NULL,
  agency_id text REFERENCES agencies(id),
  operator_id text REFERENCES operators(id),
  short_name text,
  long_name text,
  mode text NOT NULL,
  gtfs_route_type integer,
  color text,
  text_color text,
  source_priority integer NOT NULL,
  suppressed_as_duplicate boolean NOT NULL DEFAULT false,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trips (
  id text PRIMARY KEY,
  import_run_id uuid REFERENCES import_runs(id),
  source_feed_id text REFERENCES source_feeds(id),
  source_id text NOT NULL,
  route_id text NOT NULL REFERENCES routes(id),
  service_id text NOT NULL,
  headsign text,
  direction_id smallint,
  shape_id text,
  restrictions jsonb NOT NULL DEFAULT '{}'::jsonb,
  raw_source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_priority integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stop_times (
  trip_id text NOT NULL REFERENCES trips(id),
  stop_id text NOT NULL REFERENCES stops(id),
  stop_sequence integer NOT NULL,
  arrival_time integer NOT NULL,
  departure_time integer NOT NULL,
  pickup_type smallint,
  drop_off_type smallint,
  timepoint boolean,
  platform text,
  raw_notes text,
  import_run_id uuid REFERENCES import_runs(id),
  source_feed_id text REFERENCES source_feeds(id),
  source_priority integer NOT NULL DEFAULT 100,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (trip_id, stop_sequence)
);

CREATE TABLE IF NOT EXISTS calendars (
  service_id text PRIMARY KEY,
  monday boolean NOT NULL,
  tuesday boolean NOT NULL,
  wednesday boolean NOT NULL,
  thursday boolean NOT NULL,
  friday boolean NOT NULL,
  saturday boolean NOT NULL,
  sunday boolean NOT NULL,
  start_date date NOT NULL,
  end_date date NOT NULL,
  import_run_id uuid REFERENCES import_runs(id),
  source_feed_id text REFERENCES source_feeds(id)
);

CREATE TABLE IF NOT EXISTS calendar_dates (
  service_id text NOT NULL,
  date date NOT NULL,
  exception_type smallint NOT NULL,
  import_run_id uuid REFERENCES import_runs(id),
  source_feed_id text REFERENCES source_feeds(id),
  PRIMARY KEY (service_id, date)
);

CREATE TABLE IF NOT EXISTS transfers (
  from_stop_id text NOT NULL REFERENCES stops(id),
  to_stop_id text NOT NULL REFERENCES stops(id),
  min_transfer_seconds integer NOT NULL,
  distance_meters integer,
  walking_geometry jsonb,
  confidence text NOT NULL,
  accessibility_level text,
  source text NOT NULL,
  PRIMARY KEY (from_stop_id, to_stop_id)
);

CREATE TABLE IF NOT EXISTS shapes (
  shape_id text NOT NULL,
  shape_pt_sequence integer NOT NULL,
  geom geography(Point, 4326) NOT NULL,
  import_run_id uuid REFERENCES import_runs(id),
  source_feed_id text REFERENCES source_feeds(id),
  PRIMARY KEY (shape_id, shape_pt_sequence)
);

CREATE TABLE IF NOT EXISTS realtime_updates (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  trip_id text,
  route_id text,
  stop_id text,
  delay_seconds integer,
  estimated_arrival timestamptz,
  estimated_departure timestamptz,
  cancellation_status text,
  platform_change text,
  vehicle_position geography(Point, 4326),
  source text NOT NULL,
  fetched_at timestamptz NOT NULL,
  valid_until timestamptz,
  confidence text NOT NULL,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS manual_stop_matches (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  stop_id text NOT NULL REFERENCES stops(id),
  lat double precision,
  lon double precision,
  target_stop_id text,
  confidence text NOT NULL,
  note text,
  created_by uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS validation_issues (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  import_run_id uuid REFERENCES import_runs(id),
  source_feed_id text REFERENCES source_feeds(id),
  severity text NOT NULL,
  code text NOT NULL,
  message text NOT NULL,
  source_file text,
  affected_entity text,
  raw_payload jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS offline_packages (
  id text PRIMARY KEY,
  name_cs text NOT NULL,
  version text NOT NULL,
  checksum text,
  valid_from date,
  valid_until date,
  size_bytes bigint,
  storage_path text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ticket_products_mock (
  id text PRIMARY KEY,
  name_cs text NOT NULL,
  provider text NOT NULL,
  mock boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

