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

API responses must expose freshness, realtime availability and warnings rather than pretending uncertain data is certain.

