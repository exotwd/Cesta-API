ALTER TABLE users
  ADD COLUMN IF NOT EXISTS auth_version bigint NOT NULL DEFAULT 1;

ALTER TABLE user_profiles
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS version bigint NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS auth_attempts (
  id bigserial PRIMARY KEY,
  identifier_hash text NOT NULL,
  action text NOT NULL CHECK (action IN ('login', 'password_reset_request', 'password_reset_complete')),
  succeeded boolean NOT NULL DEFAULT false,
  attempted_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS auth_attempts_rate_limit_idx
  ON auth_attempts (identifier_hash, action, attempted_at DESC);

CREATE TABLE IF NOT EXISTS password_reset_tokens (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  used_at timestamptz,
  failed_attempts integer NOT NULL DEFAULT 0 CHECK (failed_attempts >= 0)
);
CREATE INDEX IF NOT EXISTS password_reset_tokens_user_idx
  ON password_reset_tokens (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS saved_routes (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 120),
  origin jsonb NOT NULL,
  destination jsonb NOT NULL,
  via jsonb,
  via_dwell_seconds integer NOT NULL DEFAULT 0 CHECK (via_dwell_seconds BETWEEN 0 AND 86400),
  transport_modes text[] NOT NULL DEFAULT '{}',
  preferences jsonb NOT NULL DEFAULT '{}'::jsonb,
  position integer NOT NULL DEFAULT 0 CHECK (position BETWEEN 0 AND 999),
  pinned boolean NOT NULL DEFAULT false,
  commute jsonb,
  version bigint NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  CHECK (jsonb_typeof(origin) = 'object'),
  CHECK (jsonb_typeof(destination) = 'object'),
  CHECK (via IS NULL OR jsonb_typeof(via) = 'object'),
  CHECK (jsonb_typeof(preferences) = 'object'),
  CHECK (commute IS NULL OR jsonb_typeof(commute) = 'object')
);
CREATE INDEX IF NOT EXISTS saved_routes_sync_idx
  ON saved_routes (user_id, updated_at, id);
CREATE UNIQUE INDEX IF NOT EXISTS saved_routes_active_name_idx
  ON saved_routes (user_id, lower(name)) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS mobile_devices (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  platform text NOT NULL CHECK (platform IN ('ios', 'android')),
  push_token text NOT NULL,
  token_hash text NOT NULL UNIQUE,
  app_version text,
  locale text,
  timezone text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS mobile_devices_user_idx ON mobile_devices (user_id, enabled);

CREATE TABLE IF NOT EXISTS journey_subscriptions (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  device_id uuid NOT NULL REFERENCES mobile_devices(id) ON DELETE CASCADE,
  run_id text NOT NULL,
  service_date date NOT NULL,
  trip_id text NOT NULL REFERENCES trips(id),
  boarding_call_id text NOT NULL,
  boarding_stop_id text NOT NULL REFERENCES stops(id),
  boarding_stop_sequence integer NOT NULL,
  alighting_call_id text,
  alighting_stop_id text REFERENCES stops(id),
  alighting_stop_sequence integer,
  connection_run_id text,
  connection_service_date date,
  connection_trip_id text REFERENCES trips(id),
  connection_call_id text,
  minimum_transfer_seconds integer CHECK (minimum_transfer_seconds BETWEEN 0 AND 7200),
  significant_delay_seconds integer NOT NULL DEFAULT 300 CHECK (significant_delay_seconds BETWEEN 60 AND 7200),
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  ended_at timestamptz,
  UNIQUE (device_id, run_id, boarding_call_id)
);
CREATE INDEX IF NOT EXISTS journey_subscriptions_realtime_idx
  ON journey_subscriptions (trip_id, service_date, ended_at, expires_at);

CREATE TABLE IF NOT EXISTS push_deliveries (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  subscription_id uuid REFERENCES journey_subscriptions(id) ON DELETE CASCADE,
  device_id uuid NOT NULL REFERENCES mobile_devices(id) ON DELETE CASCADE,
  event_key text NOT NULL,
  event_type text NOT NULL CHECK (event_type IN ('trip_cancelled', 'platform_changed', 'significant_delay', 'connection_at_risk')),
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sending', 'delivered', 'retry', 'failed')),
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  last_error_code text,
  created_at timestamptz NOT NULL DEFAULT now(),
  delivered_at timestamptz,
  UNIQUE (device_id, event_key)
);
CREATE INDEX IF NOT EXISTS push_deliveries_queue_idx
  ON push_deliveries (available_at, created_at) WHERE status IN ('pending', 'retry');

CREATE OR REPLACE FUNCTION touch_saved_route() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  IF ROW(NEW.name, NEW.origin, NEW.destination, NEW.via, NEW.via_dwell_seconds,
         NEW.transport_modes, NEW.preferences, NEW.position, NEW.pinned, NEW.commute,
         NEW.deleted_at) IS DISTINCT FROM
     ROW(OLD.name, OLD.origin, OLD.destination, OLD.via, OLD.via_dwell_seconds,
         OLD.transport_modes, OLD.preferences, OLD.position, OLD.pinned, OLD.commute,
         OLD.deleted_at) THEN
    NEW.version := OLD.version + 1;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS saved_routes_touch ON saved_routes;
CREATE TRIGGER saved_routes_touch BEFORE UPDATE ON saved_routes
FOR EACH ROW EXECUTE FUNCTION touch_saved_route();

