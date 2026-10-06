# Realtime Spec

Realtime states:

- scheduled
- estimated
- delayed
- cancelled
- platform_changed
- unknown

Realtime confidence:

- exact
- estimated
- stale
- unavailable

Realtime must never overwrite base schedules. API responses must distinguish scheduled-only data from live, stale, partial and unavailable realtime data.

PID trip updates are joined to `pid_gtfs` trips and stops by official GTFS identifiers. Journey legs expose delay, estimated times, cancellation, platform change, vehicle position, source and validity without modifying scheduled times.

IDS JMK vehicle positions use the exact trip, route and stop identifiers from the matching IDS JMK
GTFS schedule and are available through `GET /vehicles` (with `/realtime/vehicles` as a compatibility
alias). The feed currently contains VehiclePositions but no TripUpdates, so absence of a delay is
reported as unknown rather than zero. Opt-in DÚK data remains source-scoped unless a reliable
schedule mapping exists and is never guessed onto an unrelated trip.

Vehicle map responses use one normalized contract. Unknown provider capabilities remain `null`; `null` must never be interpreted as `false`. Viewport filtering uses `bbox=west,south,east,north`. The API only serves last successfully persisted observations and never calls an upstream provider in response to a map movement.

The worker evaluates active exact-run subscriptions after each persisted GTFS-Realtime batch. It
enqueues deduplicated cancellation, platform-change, significant-delay and connection-risk events
and delivers them through FCM HTTP v1 or APNs HTTP/2. Temporary provider and transport failures use
bounded exponential retry; an unconfigured provider keeps its queue retryable without exhausting
the attempt budget. Provider responses identifying an invalid token disable the device and end its
subscriptions. Required secrets stay outside the repository: `FCM_PROJECT_ID` with
`FCM_SERVICE_ACCOUNT_JSON`, or `APNS_TEAM_ID`, `APNS_KEY_ID`, `APNS_PRIVATE_KEY` and
`APNS_BUNDLE_ID` (`APNS_USE_SANDBOX=true` only for development devices).
