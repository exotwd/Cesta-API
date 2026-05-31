CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  email text NOT NULL UNIQUE,
  password_hash text NOT NULL,
  display_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_login_at timestamptz,
  is_email_verified boolean NOT NULL DEFAULT false,
  deleted_at timestamptz
);

CREATE TABLE IF NOT EXISTS user_profiles (
  user_id uuid PRIMARY KEY REFERENCES users(id),
  home_stop_id text,
  work_stop_id text,
  preferred_walking_speed text NOT NULL DEFAULT 'normal',
  prefer_fewer_transfers boolean NOT NULL DEFAULT false,
  prefer_reliable_transfers boolean NOT NULL DEFAULT true,
  default_departure_mode text NOT NULL DEFAULT 'depart_at',
  language text NOT NULL DEFAULT 'cs',
  accessibility_preferences jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS saved_places (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  name text NOT NULL,
  type text NOT NULL,
  stop_id text,
  lat double precision,
  lon double precision,
  address text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS favorite_stops (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  stop_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (user_id, stop_id)
);

CREATE TABLE IF NOT EXISTS favorite_routes (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  route_id text NOT NULL,
  from_stop_id text,
  to_stop_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (user_id, route_id, from_stop_id, to_stop_id)
);

CREATE TABLE IF NOT EXISTS notification_preferences (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  type text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  config jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (user_id, type)
);

CREATE TABLE IF NOT EXISTS user_sessions (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  refresh_token_hash text NOT NULL,
  device_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  revoked_at timestamptz
);

CREATE TABLE IF NOT EXISTS user_roles (
  user_id uuid NOT NULL REFERENCES users(id),
  role text NOT NULL,
  PRIMARY KEY (user_id, role)
);

