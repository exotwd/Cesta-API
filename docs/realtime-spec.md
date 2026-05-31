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

