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

API responses must expose freshness, realtime availability and warnings rather than pretending uncertain data is certain.
