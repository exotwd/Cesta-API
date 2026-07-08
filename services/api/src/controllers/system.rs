use crate::*;

pub(crate) async fn health(State(state): State<AppState>) -> Json<Value> {
    let database = match &state.db {
        Some(pool) => match sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(pool)
            .await
        {
            Ok(_) => json!({"status": "up"}),
            Err(error) => {
                tracing::warn!(%error, "health check database probe failed");
                json!({"status": "down"})
            }
        },
        None => json!({"status": "not_configured", "data_mode": "development_fixtures"}),
    };
    let status = if database["status"] == "down" {
        "degraded"
    } else {
        "ok"
    };
    Json(json!({"status": status, "service": "cesta-api", "database": database}))
}

pub(crate) async fn openapi() -> Json<Value> {
    let mut specification = json!({
        "openapi": "3.1.0",
        "info": {"title": "Cesta API", "version": "0.1.0"},
        "paths": {
            "/health": {"get": {
                "summary": "Health check",
                "description": "Reports API and database availability. Development fixture mode reports the database as not configured.",
                "responses": {"200": {
                    "description": "Current service health",
                    "headers": {"X-Request-Id": {"schema": {"type": "string", "format": "uuid"}}},
                    "content": {"application/json": {"schema": {
                        "type": "object",
                        "required": ["status", "service", "database"],
                        "properties": {
                            "status": {"type": "string", "enum": ["ok", "degraded"]},
                            "service": {"type": "string", "const": "cesta-api"},
                            "database": {"type": "object"}
                        }
                    }}}
                }}
            }},
            "/auth/register": {"post": {"summary": "Register user"}},
            "/auth/login": {"post": {"summary": "Login user"}},
            "/stops/search": {"get": {
                "summary": "Search stops and cities",
                "description": "Returns ranked stop suggestions. The canonical search parameter is q; query, text and term are accepted as compatibility aliases. When includeCities (or include_cities) is true, cities and stops are returned together in results and separately for backwards compatibility. Related source, stop-area and route enrichment is omitted by default for autocomplete latency; request includeRelated=true when needed.",
                "parameters": [
                    {"name": "q", "in": "query", "required": false, "schema": {"type": "string"}},
                    {"name": "limit", "in": "query", "required": false, "schema": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10}},
                    {"name": "includeCities", "in": "query", "required": false, "schema": {"type": "boolean", "default": false}},
                    {"name": "includeRelated", "in": "query", "required": false, "schema": {"type": "boolean", "default": false}}
                ],
                "responses": {"200": {
                    "description": "Ranked place suggestions",
                    "content": {"application/json": {"schema": {"$ref": "#/components/schemas/PlaceSearchResponse"}}}
                }}
            }},
            "/stops/in-bounds": {"get": {
                "summary": "List stops in map bounds",
                "description": "Returns active stops inside the visible rectangular map bounds, ordered by ID for cursor pagination. Repeat the same bounds with nextCursor as cursor until nextCursor is null.",
                "parameters": [
                    {"name": "south", "in": "query", "required": true, "schema": {"type": "number", "minimum": -90, "maximum": 90}},
                    {"name": "west", "in": "query", "required": true, "schema": {"type": "number", "minimum": -180, "maximum": 180}},
                    {"name": "north", "in": "query", "required": true, "schema": {"type": "number", "minimum": -90, "maximum": 90}},
                    {"name": "east", "in": "query", "required": true, "schema": {"type": "number", "minimum": -180, "maximum": 180}},
                    {"name": "limit", "in": "query", "required": false, "schema": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 500}},
                    {"name": "cursor", "in": "query", "required": false, "schema": {"type": "string"}}
                ],
                "responses": {
                    "200": {
                        "description": "Stops in the requested viewport",
                        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/StopsInBoundsResponse"}}}
                    },
                    "400": {"description": "Invalid or reversed bounds"}
                }
            }},
            "/departures": {"get": {"summary": "Stop departures"}},
            "/journeys/search": {"post": {
                "summary": "Search journeys",
                "description": "Returns ranked journey candidates. City points expand to active physical stops. Each leg may include PID realtime delay, estimates, cancellation, vehicle position, source and freshness metadata.",
                "requestBody": {
                    "required": true,
                    "content": {"application/json": {"schema": {
                        "type": "object",
                        "required": ["from", "to", "datetime", "mode", "transport_modes", "max_transfers", "walking_speed", "prefer_reliable_transfers", "offline_compatible"],
                        "properties": {
                            "from": {"$ref": "#/components/schemas/JourneyPoint"},
                            "to": {"$ref": "#/components/schemas/JourneyPoint"},
                            "datetime": {"type": "string"},
                            "mode": {"type": "string"},
                            "transport_modes": {"type": "array", "items": {"type": "string"}},
                            "max_transfers": {"type": "integer", "minimum": 0},
                            "walking_speed": {"type": "string"},
                            "prefer_reliable_transfers": {"type": "boolean"},
                            "offline_compatible": {"type": "boolean"},
                            "include_intermediate_stops": {
                                "type": "boolean",
                                "default": false,
                                "description": "When true, every journey leg contains ordered stop_calls including its origin, all intermediate stops and its destination. The camelCase alias includeIntermediateStops is also accepted."
                            }
                        }
                    }}}
                }
            }},
            "/realtime/vehicles": {"get": {
                "summary": "Current public transport vehicle positions",
                "description": "Returns fresh PID, IDS JMK and DUK vehicle positions with source-specific delay information.",
                "parameters": [
                    {"name": "source", "in": "query", "schema": {"type": "string"}},
                    {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 2000}}
                ],
                "responses": {"200": {"description": "Current vehicle positions"}}
            }},
            "/data-sources/status": {"get": {
                "summary": "Automatic data-source synchronization status",
                "responses": {"200": {"description": "Freshness, record counts and latest errors for every automatic source"}}
            }},
            "/admin": {"get": {
                "summary": "Cesta data administration interface",
                "description": "Serves the embedded administrator interface. Admin JSON endpoints require an admin or data_admin access token."
            }},
            "/admin/data": {"get": {"summary": "List available administrator data entities"}},
            "/admin/data/{entity}": {"get": {
                "summary": "Browse a paginated administrator data entity",
                "parameters": [
                    {"name": "entity", "in": "path", "required": true, "schema": {"type": "string"}},
                    {"name": "page", "in": "query", "schema": {"type": "integer", "minimum": 1, "default": 1}},
                    {"name": "page_size", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50}},
                    {"name": "q", "in": "query", "schema": {"type": "string"}}
                ]
            }},
            "/admin/related/{entity}/{id}": {"get": {
                "summary": "Get linked administrator data for a stop, route or trip",
                "description": "Returns the selected record plus entity-aware linked routes, trips, stops and service data.",
                "parameters": [
                    {"name": "entity", "in": "path", "required": true, "schema": {"type": "string", "enum": ["stops", "routes", "trips"]}},
                    {"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}
                ]
            }},
            "/admin/map/stops": {"get": {
                "summary": "List active stops for the administrator map",
                "description": "Returns at most 5000 stops filtered by source, search text and optional map bounds."
            }},
            "/admin/imports": {"get": {"summary": "List import runs"}},
            "/admin/imports/{id}": {"get": {"summary": "Get an import run and its validation issues"}},
            "/admin/imports/ggu-latest/start": {"post": {"summary": "Start GGU latest import"}},
            "/admin/database/stats": {"get": {"summary": "Database row counts and table sizes"}},
            "/admin/data-quality": {"get": {"summary": "Validation, duplicate and unresolved-stop metrics"}},
            "/admin/data-quality/validate": {"post": {
                "summary": "Run administrator database validation",
                "description": "Checks imported transport data for missing, invalid and disconnected records. Replaces only findings from the previous administrator validation run."
            }},
            "/admin/unmatched-stops": {"get": {"summary": "List active stops with unresolved coordinates"}},
            "/admin/source-feeds": {"get": {"summary": "List configured source feeds"}},
            "/admin/source-feeds/{id}": {"patch": {"summary": "Update a source feed configuration"}},
            "/admin/routing-algorithm": {
                "get": {"summary": "Read the active journey-search algorithm configuration"},
                "put": {
                    "summary": "Replace and immediately activate the journey-search algorithm configuration",
                    "description": "Validates and persists candidate-generation limits, transfer constraints, scoring weights, dominance pruning and result-diversity guarantees. Requires an admin or data_admin token.",
                    "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RoutingAlgorithmConfig"}}}},
                    "responses": {"200": {"description": "Validated configuration is active for new searches"}, "400": {"description": "Invalid or unsafe parameter combination"}}
                },
                "delete": {"summary": "Reset the journey-search algorithm to safe defaults"}
            },
            "/public/boards/{stopId}": {"get": {"summary": "Public departure board data"}}
        },
        "components": {
            "schemas": {
                "PlaceType": {
                    "type": "string",
                    "enum": ["city", "railway_station", "railway_stop", "bus_station", "bus_stop", "tram_stop", "metro_station", "ferry_terminal", "airport", "stop"]
                },
                "CitySearchResult": {
                    "type": "object",
                    "required": ["id", "name", "place_type", "country_code", "modes"],
                    "properties": {
                        "id": {"type": "string", "pattern": "^city:[A-Z]{2}:.+$"},
                        "name": {"type": "string"},
                        "place_type": {"type": "string", "enum": ["city"]},
                        "region": {"type": ["string", "null"]},
                        "country_code": {"type": "string"},
                        "lat": {"type": ["number", "null"]},
                        "lon": {"type": ["number", "null"]},
                        "modes": {"type": "array", "items": {"type": "string"}}
                    }
                },
                "StopSearchResult": {
                    "type": "object",
                    "required": ["id", "name", "place_type", "modes"],
                    "properties": {
                        "id": {"type": "string"},
                        "name": {"type": "string"},
                        "place_type": {"$ref": "#/components/schemas/PlaceType"},
                        "municipality": {"type": ["string", "null"]},
                        "region": {"type": ["string", "null"]},
                        "lat": {"type": ["number", "null"]},
                        "lon": {"type": ["number", "null"]},
                        "modes": {"type": "array", "items": {"type": "string"}}
                    }
                },
                "PlaceSearchResponse": {
                    "type": "object",
                    "required": ["stops"],
                    "properties": {
                        "results": {
                            "type": "array",
                            "items": {"oneOf": [
                                {"$ref": "#/components/schemas/CitySearchResult"},
                                {"$ref": "#/components/schemas/StopSearchResult"}
                            ]}
                        },
                        "cities": {"type": "array", "items": {"$ref": "#/components/schemas/CitySearchResult"}},
                        "stops": {"type": "array", "items": {"$ref": "#/components/schemas/StopSearchResult"}}
                    }
                },
                "StopsInBoundsResponse": {
                    "type": "object",
                    "required": ["stops", "nextCursor", "data_status"],
                    "properties": {
                        "stops": {"type": "array", "items": {"$ref": "#/components/schemas/Stop"}},
                        "nextCursor": {"type": ["string", "null"]},
                        "data_status": {"type": "object"}
                    }
                },
                "Stop": {
                    "type": "object",
                    "required": ["id", "source_ids", "name", "normalized_name", "modes", "coordinate_confidence", "is_active"],
                    "properties": {
                        "id": {"type": "string"},
                        "source_ids": {"type": "array", "items": {"$ref": "#/components/schemas/StopSourceRef"}},
                        "name": {"type": "string"},
                        "normalized_name": {"type": "string"},
                        "municipality": {"type": ["string", "null"]},
                        "district": {"type": ["string", "null"]},
                        "region": {"type": ["string", "null"]},
                        "lat": {"type": ["number", "null"]},
                        "lon": {"type": ["number", "null"]},
                        "geom": {"type": ["object", "null"]},
                        "coordinate_confidence": {"type": "string", "enum": ["exact", "high", "medium", "low", "unresolved"]},
                        "coordinate_source": {"type": ["string", "null"]},
                        "stop_area_id": {"type": ["string", "null"]},
                        "platform_code": {"type": ["string", "null"]},
                        "modes": {"type": "array", "items": {"type": "string"}},
                        "is_active": {"type": "boolean"}
                    }
                },
                "StopSourceRef": {
                    "type": "object",
                    "required": ["feed_id", "original_id", "priority", "suppressed_as_duplicate"],
                    "properties": {
                        "feed_id": {"type": "string"},
                        "original_id": {"type": "string"},
                        "import_run_id": {"type": ["string", "null"], "format": "uuid"},
                        "priority": {"type": "integer"},
                        "confidence": {"type": ["string", "null"]},
                        "suppressed_as_duplicate": {"type": "boolean"}
                    }
                },
                "RoutingAlgorithmConfig": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["max_results", "max_direct_candidates", "max_transfer_candidates", "min_transfer_seconds", "max_transfer_wait_seconds", "transfer_search_timeout_seconds", "next_day_search_from_seconds", "arrival_time_weight", "duration_weight", "transfer_penalty_seconds", "preserve_simplest", "preserve_each_transfer_count", "preserve_carrier_diversity", "remove_dominated", "dominate_only_same_carrier"],
                    "properties": {
                        "max_results": {"type": "integer", "minimum": 1, "maximum": 20, "default": 5},
                        "max_direct_candidates": {"type": "integer", "minimum": 1, "maximum": 500, "default": 20},
                        "max_transfer_candidates": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 40},
                        "min_transfer_seconds": {"type": "integer", "minimum": 60, "maximum": 3600, "default": 300},
                        "max_transfer_wait_seconds": {"type": "integer", "minimum": 300, "maximum": 21600, "default": 7200},
                        "transfer_search_timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 60, "default": 6},
                        "next_day_search_from_seconds": {"type": "integer", "minimum": 0, "maximum": 86399, "default": 64800},
                        "arrival_time_weight": {"type": "number", "minimum": 0, "maximum": 10, "default": 1},
                        "duration_weight": {"type": "number", "minimum": 0, "maximum": 10, "default": 0},
                        "transfer_penalty_seconds": {"type": "integer", "minimum": 0, "maximum": 14400, "default": 0},
                        "preserve_simplest": {"type": "boolean", "default": true},
                        "preserve_each_transfer_count": {"type": "boolean", "default": true},
                        "preserve_carrier_diversity": {"type": "boolean", "default": true},
                        "remove_dominated": {"type": "boolean", "default": true},
                        "dominate_only_same_carrier": {"type": "boolean", "default": true}
                    }
                },
                "JourneyPoint": {
                    "type": "object",
                    "required": ["type", "id"],
                    "properties": {
                        "type": {"type": "string", "enum": ["stop", "city"]},
                        "id": {"type": "string"},
                        "lat": {"type": ["number", "null"]},
                        "lon": {"type": ["number", "null"]}
                    }
                },
                "JourneyLegRealtime": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string", "enum": ["realtime", "cancelled"]},
                        "delay_seconds": {"type": ["integer", "null"]},
                        "estimated_departure": {"type": ["string", "null"], "format": "date-time"},
                        "estimated_arrival": {"type": ["string", "null"], "format": "date-time"},
                        "cancellation_status": {"type": ["string", "null"]},
                        "vehicle_id": {"type": ["string", "null"]},
                        "vehicle_position": {"type": ["object", "null"]},
                        "source": {"type": "string"},
                        "fetched_at": {"type": "string", "format": "date-time"},
                        "valid_until": {"type": ["string", "null"], "format": "date-time"}
                    }
                },
                "JourneyStopCall": {
                    "type": "object",
                    "required": ["trip_id", "stop_id", "stop_sequence", "name", "scheduled_arrival_seconds", "scheduled_departure_seconds", "scheduled_arrival", "scheduled_departure", "is_origin", "is_destination", "is_intermediate", "realtime"],
                    "properties": {
                        "trip_id": {"type": "string"},
                        "stop_id": {"type": "string"},
                        "stop_sequence": {"type": ["integer", "null"]},
                        "name": {"type": "string"},
                        "municipality": {"type": ["string", "null"]},
                        "lat": {"type": ["number", "null"]},
                        "lon": {"type": ["number", "null"]},
                        "platform": {"type": ["string", "null"]},
                        "scheduled_arrival_seconds": {"type": "integer"},
                        "scheduled_departure_seconds": {"type": "integer"},
                        "scheduled_arrival": {"type": "string"},
                        "scheduled_departure": {"type": "string"},
                        "pickup_type": {"type": ["integer", "null"]},
                        "drop_off_type": {"type": ["integer", "null"]},
                        "timepoint": {"type": ["boolean", "null"]},
                        "is_origin": {"type": "boolean"},
                        "is_destination": {"type": "boolean"},
                        "is_intermediate": {"type": "boolean"},
                        "realtime": {"$ref": "#/components/schemas/JourneyStopCallRealtime"}
                    }
                },
                "JourneyStopCallRealtime": {
                    "type": "object",
                    "required": ["status"],
                    "properties": {
                        "status": {"type": "string", "enum": ["scheduled", "realtime", "cancelled", "unavailable"]},
                        "delay_seconds": {"type": ["integer", "null"]},
                        "estimated_arrival": {"type": ["string", "null"], "format": "date-time"},
                        "estimated_departure": {"type": ["string", "null"], "format": "date-time"},
                        "platform_change": {"type": ["string", "null"]},
                        "source": {"type": ["string", "null"]},
                        "fetched_at": {"type": ["string", "null"], "format": "date-time"},
                        "valid_until": {"type": ["string", "null"], "format": "date-time"}
                    }
                }
            }
        }
    });
    ticketing::augment_openapi(&mut specification);
    Json(specification)
}

pub(crate) async fn data_status(State(state): State<AppState>) -> Json<Value> {
    if let Some(pool) = &state.db {
        return Json(database_status(pool).await.unwrap_or_else(|error| {
            json!({
                "schedule": "unknown",
                "realtime": "unavailable",
                "source": "database",
                "database_available": false,
                "warnings": [format!("database status query failed: {error}")]
            })
        }));
    }

    Json(json!({
        "schedule": if state.use_mock_data { "mock" } else { "unknown" },
        "realtime": "unavailable",
        "source": if state.use_mock_data { "mock" } else { "database" },
        "warnings": if state.use_mock_data { vec!["development fixture data is in use"] } else { Vec::<&str>::new() }
    }))
}

pub(crate) async fn sources() -> Json<Value> {
    Json(json!({
        "sources": [
            {"id":"ggu_jdf_gtfs_latest","url":"https://data.jr.ggu.cz/results/latest/JDF_merged_GTFS.zip","priority":30,"type":"gtfs"},
            {"id":"ggu_czptt_gtfs_latest","url":"https://data.jr.ggu.cz/results/latest/CZPTT_GTFS.zip","priority":20,"type":"gtfs"},
            {"id":"ggu_jdf_raw_latest","url":"https://data.jr.ggu.cz/results/latest/JDF_merged.zip","priority":40,"type":"jdf_raw"}
        ]
    }))
}
