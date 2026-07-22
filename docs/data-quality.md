# Data Quality

The importer treats generated source files as inputs that require validation.

Initial validation checks:

- missing required GTFS files
- malformed rows
- missing or invalid stop coordinates
- duplicate stop candidates
- unsupported route types
- routes without trips
- trips without stop times
- negative or impossible travel times
- invalid calendars
- conversion log warnings and errors

The administrator interface also provides an explicit database validation run. This audit does
not replace importer validation. It checks the last successfully imported database state for:

- missing stop names or coordinates
- coordinates outside valid latitude and longitude ranges
- missing stop, route, trip and stop-time source tracking
- active routes without trips
- trips without stop times
- trip service IDs without calendars or calendar exceptions
- negative, reversed or excessively large stop times
- invalid calendar ranges or calendars with no active weekdays
- enabled source feeds without a successful import

Administrator validation findings use `source_file = admin_database_validation`. Each run replaces
only findings with that marker, retains importer-generated findings, and stores aggregate counts
plus sample record IDs in `raw_payload`.

## Repair workflow

The administrator data-quality page separates repairs into two safety levels:

- Safe automatic repairs rebuild a missing normalized stop name only when a public name exists,
  assign a stop to a city only when its municipality matches exactly one Czech city, set an
  impossible realtime `valid_until` value to the row's fetch time, and merge exact cross-feed stop
  aliases only when name, coordinate, platform, type, mode and locality agree. A group is excluded
  whenever one trip calls at more than one selected record.
- Exact-coordinate duplicate repairs remain available for administrator review.
- Nearby same-direction candidates are proposed up to 120 metres apart. Direction is derived from
  the next scheduled stop across stop-time samples. Confirmation revalidates public name (with a
  municipality prefix removed), locality, physical stop type, distance and direction, and rejects
  records that occur in the same trip.

Every repair is recorded in `data_repair_runs`. Automatically exact and administrator-confirmed
duplicate mappings are stored as `manual_stop_matches` with `confidence = confirmed_duplicate`.
Applying a mapping moves stop times,
transfers, realtime references and account stop references to the canonical record, retains all
`stop_source_ids` with `suppressed_as_duplicate = true`, and deactivates rather than deletes the
source stop. The data pipeline re-applies conservative repairs and confirmed mappings after each
import, preventing a later source refresh from silently undoing an administrator decision.

The repair system intentionally does not invent empty public stop names, synthesize calendars,
rewrite invalid timetable times, delete orphan trips or routes, disable feeds, or automatically
merge nearby/same-feed candidates. Empty names and non-physical GTFS nodes are hidden from public
stop search, nearby and map-bound responses until their source data is corrected. Uncertain merge
cases require an explicit, reviewed administrator decision.

API responses must expose freshness, realtime availability and warnings rather than pretending uncertain data is certain.
