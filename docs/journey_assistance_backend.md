# Journey preferences, offline journeys and recovery

Saved routes, offline bundles, recovery search, and the Android widget reuse `POST /journeys/search`;
they do not require separate backend endpoints.

## Request preferences

The optional `journey_preferences` object accepts:

```json
{
  "profile": "standard",
  "step_free": false,
  "prefer_fewer_stairs": false,
  "minimum_transfer_buffer_seconds": 600
}
```

Profiles are `standard`, `wheelchair`, `stroller`, and `luggage`. The transfer buffer is limited to
0–1800 seconds and is added during RAPTOR boarding eligibility after the required interchange
movement. It is not a post-ranking preference.

This deployment currently routes `standard` and `luggage` requests with a transfer buffer. It
explicitly rejects wheelchair/stroller, `step_free`, and `prefer_fewer_stairs` requests with
`journey_accessibility_unverified` until the imported station graph can verify the complete route.
It never silently presents an unverified route as satisfying an accessibility request.

Every returned journey carries:

```json
{"accessibility":{"verified":false,"step_free":null,"reason":"complete_journey_not_verified"}}
```

Only a future result containing both `verified: true` and `step_free: true` may be shown as a
verified step-free journey.

## Offline response requirements

With `include_intermediate_stops: true`, each leg contains ordered `stop_calls`, stable station/run/
call identities, scheduled times, platforms, and station-layout availability. Journey geometry is
real GeoJSON: transit shapes or verified pedestrian-router geometry. Scheduled fields remain
separate from realtime annotations so clients can strip live and ticketing state before storage.

Station layouts are downloaded separately through the versioned layout endpoint. A missing plan is
not a journey-search failure. Server-side push tracking, ticket validity decisions, reservation
changes, and iOS WidgetKit remain separate future capabilities.

