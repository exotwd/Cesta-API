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

IDS JMK and DUK data remains source-scoped unless a reliable schedule mapping exists. It is available through `GET /realtime/vehicles` and is never guessed onto an unrelated trip.
