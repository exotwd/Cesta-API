CREATE TABLE IF NOT EXISTS cd_ticketing_journey_refs (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  journey_snapshot jsonb NOT NULL,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cd_ticketing_journey_refs_expiry_idx
  ON cd_ticketing_journey_refs(expires_at);

CREATE INDEX IF NOT EXISTS cd_ticketing_orders_user_created_idx
  ON cd_ticketing_orders(user_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS cd_ticketing_documents_order_idx
  ON cd_ticketing_documents(user_id, order_id, created_at);
CREATE INDEX IF NOT EXISTS cd_ticketing_tickets_order_idx
  ON cd_ticketing_tickets(user_id, order_id, created_at);
CREATE INDEX IF NOT EXISTS cd_ticketing_refunds_ticket_idx
  ON cd_ticketing_refunds(user_id, ticket_id, updated_at DESC);
