# Source Priority

Configured priority when multiple feeds are enabled:

1. official regional GTFS with realtime/geodata
2. official or high-quality rail data, including CZPTT-derived data
3. GGU/JDF-derived national feed for gaps
4. geodata enrichment sources
5. manual correction layer

Imported entities must retain source feed, original source ID, import run, priority, confidence and duplicate-suppression metadata.

Only PID feeds are currently enabled. GGU and other non-PID transport feeds remain disabled while their imported history and source tracking are retained.
