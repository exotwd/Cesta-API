CREATE TABLE IF NOT EXISTS cd_ticketing_locations (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  upstream_type integer NOT NULL CHECK (upstream_type BETWEEN 1 AND 3),
  upstream_key integer NOT NULL CHECK (upstream_key > 0),
  payload jsonb NOT NULL,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (user_id, upstream_type, upstream_key)
);

CREATE TABLE IF NOT EXISTS cd_ticketing_searches (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  upstream_handle integer NOT NULL,
  request jsonb NOT NULL,
  payload jsonb NOT NULL,
  connection_map jsonb NOT NULL DEFAULT '{}'::jsonb,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cd_ticketing_searches_owner_idx ON cd_ticketing_searches(user_id, id);

CREATE TABLE IF NOT EXISTS cd_ticketing_orders (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  search_id uuid NOT NULL REFERENCES cd_ticketing_searches(id),
  connection_id uuid NOT NULL,
  upstream_conn_id integer NOT NULL,
  upstream_booking_id text NOT NULL UNIQUE,
  status text NOT NULL CHECK (status IN ('draft','checkout_pending','paid','issued','cancelled','issuance_failed','refunded','refund_pending')),
  selected_offer_type integer,
  amount_hellers integer CHECK (amount_hellers IS NULL OR amount_hellers >= 0),
  currency text NOT NULL DEFAULT 'CZK' CHECK (currency = 'CZK'),
  customer jsonb,
  quote jsonb NOT NULL,
  checkout_provider text,
  checkout_session_id text,
  version bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cd_ticketing_orders_owner_idx ON cd_ticketing_orders(user_id, id);

CREATE TABLE IF NOT EXISTS cd_ticketing_documents (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  order_id uuid NOT NULL REFERENCES cd_ticketing_orders(id),
  upstream_document_id text NOT NULL,
  document_type integer,
  content_type text CHECK (content_type IS NULL OR content_type IN ('application/pdf','image/png')),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (order_id, upstream_document_id)
);

CREATE TABLE IF NOT EXISTS cd_ticketing_tickets (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  order_id uuid NOT NULL REFERENCES cd_ticketing_orders(id),
  upstream_ticket_id text NOT NULL,
  api_offer_type integer,
  payload jsonb NOT NULL,
  returned boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (order_id, upstream_ticket_id)
);

CREATE TABLE IF NOT EXISTS cd_ticketing_refunds (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid NOT NULL REFERENCES users(id),
  ticket_id uuid NOT NULL REFERENCES cd_ticketing_tickets(id),
  status text NOT NULL CHECK (status IN ('requested','processing','settled','rejected')),
  amount_hellers integer CHECK (amount_hellers IS NULL OR amount_hellers >= 0),
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cd_ticketing_idempotency (
  user_id uuid NOT NULL REFERENCES users(id),
  operation text NOT NULL,
  idempotency_key text NOT NULL,
  request_hash text NOT NULL,
  status_code integer,
  response jsonb,
  locked_until timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, operation, idempotency_key)
);

CREATE TABLE IF NOT EXISTS cd_ticketing_rate_limits (
  user_id uuid NOT NULL REFERENCES users(id),
  bucket text NOT NULL,
  window_start timestamptz NOT NULL,
  request_count integer NOT NULL CHECK (request_count > 0),
  PRIMARY KEY (user_id, bucket, window_start)
);

CREATE TABLE IF NOT EXISTS cd_ticketing_audit_events (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id uuid REFERENCES users(id),
  order_id uuid REFERENCES cd_ticketing_orders(id),
  event_type text NOT NULL,
  outcome text NOT NULL,
  sanitized_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cd_ticketing_refund_cursor (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  last_event_id integer,
  last_event_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO cd_ticketing_refund_cursor(singleton) VALUES (true) ON CONFLICT DO NOTHING;
