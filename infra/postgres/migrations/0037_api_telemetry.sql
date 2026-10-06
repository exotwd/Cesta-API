-- Only aggregate technical measurements. No IPs, accounts, queries, locations or tokens.
CREATE TABLE IF NOT EXISTS api_telemetry_minutes (
  minute timestamptz NOT NULL,
  kind text NOT NULL CHECK (kind IN ('request','stage','journey','operation')),
  endpoint text NOT NULL,
  method text NOT NULL,
  status integer NOT NULL,
  observations bigint NOT NULL,
  latency_sum_ms bigint NOT NULL,
  latency_max_ms bigint NOT NULL,
  latency_buckets bigint[] NOT NULL CHECK (cardinality(latency_buckets)=10),
  response_bytes bigint NOT NULL,
  sized_responses bigint NOT NULL,
  results bigint NOT NULL,
  empty_results bigint NOT NULL,
  warned_results bigint NOT NULL,
  realtime_fallbacks bigint NOT NULL,
  PRIMARY KEY (minute,kind,endpoint,method,status)
);
CREATE TABLE IF NOT EXISTS api_telemetry_batches (
  id uuid PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS api_telemetry_batches_created_idx ON api_telemetry_batches(created_at);
