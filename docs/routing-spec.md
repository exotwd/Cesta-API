# Routing Spec

Phase 1 uses a simple Connection Scan Algorithm over fixture or prepared imported snapshots.

Required routing behavior:

- earliest-arrival stop-to-stop search
- max transfer limit
- mode filters
- walking transfers
- up to five reasonable journeys
- warnings for incomplete data, short transfers, unavailable realtime and uncertain stop locations

Future work:

- Range RAPTOR
- arrive-by search
- multi-criteria ranking

## Administrator tuning

Database-backed searches read the singleton `routing_algorithm_config` profile for every request,
so a validated admin update affects new searches immediately without restarting the API or running
an import. `GET /admin/routing-algorithm` returns the active profile and defaults,
`PUT /admin/routing-algorithm` replaces it, and `DELETE /admin/routing-algorithm` restores defaults.
All three endpoints require an `admin` or `data_admin` access token.

The admin page separates controls into:

- candidate generation: direct/transfer query limits, valid transfer time window, transfer-query
  timeout, and the next-service-day threshold;
- ranking: `arrival_time × arrival_time_weight + duration × duration_weight + transfers ×
  transfer_penalty_seconds`, with the lowest score first;
- result selection: response limit, dominance pruning, simplest-journey coverage, transfer-count
  coverage, and carrier diversity.

Defaults use arrival time as the only score input and apply no transfer penalty. The fastest
connection can therefore be a transfer. The response labels the configured score winner
`doporuceno`, the actual earliest arrival `nejrychlejsi`, and the fewest-transfer result
`nejjednodussi`.

Carrier diversity is a proxy for preserving potentially cheaper alternatives; it is not fare
ranking. No journey is described as cheapest until verified fare data is imported.
- accessibility routing
- historical reliability
- realtime rerouting
