use crate::*;

pub(crate) async fn health(State(state): State<AppState>) -> Json<Value> {
    let database = match &state.db {
        Some(pool) => match time::timeout(
            std::time::Duration::from_millis(750),
            sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool),
        )
        .await
        {
            Ok(Ok(_)) => json!({"status":"up"}),
            _ => json!({"status":"down"}),
        },
        None => json!({"status": "not_configured", "data_mode": "development_fixtures"}),
    };
    let service_date = Utc::now()
        .with_timezone(&chrono_tz::Europe::Prague)
        .date_naive();
    let routing_schedule_ready =
        state
            .raptor_cache
            .read()
            .await
            .iter()
            .any(|((cached_service_date, _), cell)| {
                *cached_service_date == service_date && cell.get().is_some()
            });
    let realtime = state.routing_realtime_cache.read().await;
    let realtime_entry = realtime.as_ref().filter(|entry| {
        entry.service_date == service_date
            && entry.loaded_at.elapsed()
                < std::time::Duration::from_secs(ROUTING_REALTIME_CACHE_TTL_SECONDS)
    });
    let routing_realtime_ready = realtime_entry.is_some();
    let routing = json!({
        "service_date": service_date,
        "schedule_ready": routing_schedule_ready,
        "realtime_ready": routing_realtime_ready,
        "realtime_trip_count": realtime_entry.map(|entry| entry.data.trip_count()).unwrap_or(0),
        "realtime_cache_age_seconds": realtime_entry.map(|entry| entry.loaded_at.elapsed().as_secs())
    });
    let operations = if let Some(pool) = &state.db {
        let aggregate = sqlx::query_scalar::<_, Value>(
            r#"SELECT jsonb_build_object(
              'schedule_import', (SELECT jsonb_build_object('finished_at',finished_at,'source',source,'feed_id',summary->>'feed_id')
                FROM import_runs WHERE status='success' ORDER BY finished_at DESC NULLS LAST LIMIT 1),
              'import_failures_24h', (SELECT count(*) FROM import_runs WHERE status='failed' AND started_at>now()-interval '24 hours'),
              'realtime_latest_success_at', (SELECT max(last_success_at) FROM data_source_syncs WHERE data_kind LIKE '%realtime%' OR data_kind='vehicle_positions'),
              'source_sync_failures', (SELECT count(*) FROM data_source_syncs WHERE status='failed'),
              'push_queue', jsonb_build_object(
                'pending', (SELECT count(*) FROM push_deliveries WHERE status IN ('pending','retry')),
                'failed', (SELECT count(*) FROM push_deliveries WHERE status='failed'),
                'oldest_available_at', (SELECT min(available_at) FROM push_deliveries WHERE status IN ('pending','retry'))),
              'business_24h', jsonb_build_object(
                'orders', (SELECT count(*) FROM cd_ticketing_orders WHERE created_at>now()-interval '24 hours'),
                'issuance_failed', (SELECT count(*) FROM cd_ticketing_orders WHERE status='issuance_failed' AND updated_at>now()-interval '24 hours'),
                'refund_pending', (SELECT count(*) FROM cd_ticketing_orders WHERE status='refund_pending'))
            )"#,
        ).fetch_one(pool);
        match time::timeout(std::time::Duration::from_millis(750), aggregate).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                tracing::warn!(%error, "health operational aggregate failed");
                json!({"status":"unavailable"})
            }
            Err(_) => {
                tracing::warn!(
                    timeout_millis = 750,
                    "health operational aggregate timed out"
                );
                json!({"status":"timeout"})
            }
        }
    } else {
        json!({"status":"not_configured"})
    };
    let route_searches = state.route_search_diagnostics.read().await;
    let mut latencies = route_searches
        .iter()
        .map(|timing| timing.total_ms)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let recent_route_search = json!({
        "sample_count": route_searches.len(),
        "failed": route_searches.iter().filter(|timing| !timing.success).count(),
        "average_ms": if route_searches.is_empty() { None } else {
            Some(route_searches.iter().map(|timing| timing.total_ms as u128).sum::<u128>() / route_searches.len() as u128)
        },
        "maximum_ms": latencies.last(),
        "p50_ms": latency_percentile(&latencies, 50),
        "p95_ms": latency_percentile(&latencies, 95),
        "retained_limit": ROUTE_SEARCH_TIMING_HISTORY
    });
    let production_routing_not_ready =
        state.db.is_some() && (!routing_schedule_ready || !routing_realtime_ready);
    let status = if database["status"] == "down" || production_routing_not_ready {
        "degraded"
    } else {
        "ok"
    };
    Json(json!({
        "status": status,
        "service": "cesta-api",
        "database": database,
        "routing_schedule_ready": routing_schedule_ready,
        "routing_realtime_ready": routing_realtime_ready,
        "routing": routing,
        "operations": operations,
        "journey_search_latency": recent_route_search,
        "integrations": {
            "payment_provider_configured": env::var_os("PAYMENT_PROVIDER_BASE_URL").is_some(),
            "password_reset_delivery_configured": env::var_os("PASSWORD_RESET_DELIVERY_URL").is_some(),
            "fcm_configured": env::var_os("FCM_PROJECT_ID").is_some()
                && env::var_os("FCM_SERVICE_ACCOUNT_JSON").is_some(),
            "apns_configured": env::var_os("APNS_BUNDLE_ID").is_some()
                && env::var_os("APNS_TEAM_ID").is_some()
                && env::var_os("APNS_KEY_ID").is_some()
                && env::var_os("APNS_PRIVATE_KEY").is_some()
        }
    }))
}

pub(crate) async fn readiness(State(state): State<AppState>) -> Response {
    let payload = health(State(state)).await.0;
    let status = if payload["status"] == "ok" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(payload)).into_response()
}

pub(crate) async fn openapi() -> Json<Value> {
    let mut specification = json!({
        "openapi": "3.1.0",
        "info": {"title": "Cesta API", "version": "0.1.0"},
        "paths": {
            "/health": {"get": {
                "summary": "Health check",
                "description": "Reports API, database, scheduled-routing and realtime-routing readiness. Production status remains degraded until both routing caches are ready. Development fixture mode reports the database as not configured.",
                "responses": {"200": {
                    "description": "Current service health",
                    "headers": {"X-Request-Id": {"schema": {"type": "string", "format": "uuid"}}},
                    "content": {"application/json": {"schema": {
                        "type": "object",
                        "required": ["status", "service", "database", "routing_schedule_ready", "routing_realtime_ready", "routing"],
                        "properties": {
                            "status": {"type": "string", "enum": ["ok", "degraded"]},
                            "service": {"type": "string", "const": "cesta-api"},
                            "database": {"type": "object"},
                            "routing_schedule_ready": {"type": "boolean"},
                            "routing_realtime_ready": {"type": "boolean"},
                            "routing": {"type": "object"},
                            "journey_search_latency": {
                                "type": "object",
                                "description": "Bounded recent searches, including failures. p50_ms and p95_ms use nearest-rank percentiles; null for an empty sample. Resets on restart.",
                                "properties": {
                                    "sample_count": {"type": "integer"},
                                    "retained_limit": {"type": "integer"},
                                    "failed": {"type": "integer"},
                                    "average_ms": {"type": ["number", "null"]},
                                    "maximum_ms": {"type": ["integer", "null"]},
                                    "p50_ms": {"type": ["integer", "null"]},
                                    "p95_ms": {"type": ["integer", "null"]}
                                }
                            }
                        }
                    }}}
                }}
            }},
            "/ready": {"get": {
                "summary":"Readiness check",
                "description":"Returns 503 until the database plus current schedule and realtime routing caches are ready. The response also exposes aggregate import, data-age, push-queue, ticketing and journey-search latency signals without customer data.",
                "responses":{"200":{"description":"Ready"},"503":{"description":"Not ready"}}
            }},
            "/auth/register": {"post": {"summary": "Register user"}},
            "/auth/login": {"post": {"summary": "Login user"}},
            "/auth/refresh": {"post": {"summary": "Rotate a single-use refresh token"}},
            "/auth/logout": {"post": {"summary": "Revoke a refresh token"}},
            "/auth/me": {
                "get": {"summary":"Read the authenticated account","security":[{"bearerAuth":[]}]},
                "patch": {"summary":"Update the authenticated account","security":[{"bearerAuth":[]}]},
                "delete": {"summary":"Pseudonymize the account, remove synced personal data, and revoke every session","security":[{"bearerAuth":[]}]}
            },
            "/auth/change-password": {"post": {"summary":"Change the password and revoke every session","security":[{"bearerAuth":[]}]}},
            "/auth/password-reset": {"post": {
                "summary":"Request a one-time password reset",
                "description":"Always returns the same accepted response whether or not the account exists. Delivery and attempts are rate limited."
            }},
            "/auth/password-reset/complete": {"post": {"summary":"Consume an expiring one-time reset token and revoke every session"}},
            "/me/profile": {
                "get":{"summary":"Read the persistent travel profile","security":[{"bearerAuth":[]}]},
                "patch":{"summary":"Update the persistent travel profile","security":[{"bearerAuth":[]}]}
            },
            "/me/saved-routes": {
                "get":{"summary":"Synchronize active routes and deletion tombstones","security":[{"bearerAuth":[]}],"parameters":[{"name":"since","in":"query","schema":{"type":"string","format":"date-time"}},{"name":"include_deleted","in":"query","schema":{"type":"boolean"}}]},
                "post":{"summary":"Create a versioned saved route","security":[{"bearerAuth":[]}]}
            },
            "/me/saved-routes/{id}": {
                "patch":{"summary":"Update a saved route with optimistic concurrency","security":[{"bearerAuth":[]}]},
                "delete":{"summary":"Create a synchronized deletion tombstone","security":[{"bearerAuth":[]}]}
            },
            "/me/devices": {
                "get":{"summary":"List the authenticated account's push devices","security":[{"bearerAuth":[]}]},
                "post":{"summary":"Register an FCM or APNs device token without returning the token","security":[{"bearerAuth":[]}]}
            },
            "/me/devices/{id}": {"delete":{"summary":"Disable an owned device and end its subscriptions","security":[{"bearerAuth":[]}]}},
            "/me/journey-subscriptions": {
                "get":{"summary":"List journey subscriptions for the authenticated account","security":[{"bearerAuth":[]}]},
                "post":{"summary":"Subscribe using a dated run, exact GTFS trip and exact boarding call","description":"run_id and call_id are recomputed server-side from service_date, trip_id, stop_id and stop_sequence; a similar search result cannot be substituted.","security":[{"bearerAuth":[]}]}
            },
            "/me/journey-subscriptions/{id}": {"delete":{"summary":"End an owned journey subscription","security":[{"bearerAuth":[]}]}},
            "/stops/search": {"get": {
                "summary": "Search stops and cities",
                "description": "Returns ranked stop suggestions with canonical_name, aliases and parent_stop_area_id metadata. Common railway suffixes such as 'hl. n.', 'hlavni nadrazi' and 'zel. st.' are accepted even when an upstream rail feed omits the suffix from the station name. The canonical search parameter is q; query, text and term are accepted as compatibility aliases. When includeCities (or include_cities) is true, cities and stops are returned together in results and separately for backwards compatibility. Related source, stop-area and route enrichment is omitted by default for autocomplete latency; request includeRelated=true when needed.",
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
            "/stops/catalog": {"get": {
                "summary": "Download searchable stops for local autocomplete",
                "description": "Returns one complete, ID-sorted snapshot of active physical stops from enabled sources. Store its ETag and send it as If-None-Match on later requests. A 304 response means the locally stored snapshot is current; a 200 response replaces it in full. Development fixtures are identified by data_status.source=mock.",
                "parameters": [
                    {"name": "If-None-Match", "in": "header", "required": false, "schema": {"type": "string"}}
                ],
                "responses": {
                    "200": {
                        "description": "Complete stop catalog",
                        "headers": {
                            "ETag": {"schema": {"type": "string"}},
                            "Cache-Control": {"schema": {"type": "string"}}
                        },
                        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/StopCatalogResponse"}}}
                    },
                    "304": {
                        "description": "Catalog unchanged; no response body",
                        "headers": {"ETag": {"schema": {"type": "string"}}}
                    },
                    "500": {"description": "Catalog could not be read; keep the previously stored snapshot"}
                }
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
            "/departures": {"get": {
                "summary": "Stop departures",
                "description": "Returns the nearest scheduled departures at or after the requested time. A selected stop includes the directional platforms represented by the same autocomplete suggestion, without mixing distinct transport places such as a tram stop and ferry terminal. Station and stop-area identifiers also include departures from their active child platforms. When time is omitted or invalid, the current Europe/Prague local time is used, including the applicable CET or CEST offset.",
                "parameters": [
                    {"name": "stopId", "in": "query", "required": true, "schema": {"type": "string"}},
                    {"name": "time", "in": "query", "required": false, "description": "Optional departure time (HH:MM:SS or a date-time containing HH:MM:SS). Defaults to the current Europe/Prague local time.", "schema": {"type": "string"}},
                    {"name": "limit", "in": "query", "required": false, "schema": {"type": "integer", "minimum": 1, "default": 10}}
                ]
            }},
            "/stations/{stationId}/layout": {"get": {
                "summary": "Get a verified, versioned station layout",
                "parameters": [
                    {"name": "stationId", "in": "path", "required": true, "schema": {"type": "string"}},
                    {"name": "level", "in": "query", "required": false, "schema": {"type": "string"}}
                ],
                "responses": {
                    "200": {"description": "Exact requested station layout", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/StationLayout"}}}},
                    "204": {"description": "No layout data"},
                    "304": {"description": "Cached version is current"},
                    "404": {"description": "Station or requested level has no verified layout"}
                }
            }},
            "/runs/{runId}/formation": {"get": {
                "summary": "Get the formation of a dated run at a specific call",
                "parameters": [
                    {"name": "runId", "in": "path", "required": true, "schema": {"type": "string"}},
                    {"name": "atCallId", "in": "query", "required": false, "schema": {"type": "string"}}
                ],
                "responses": {"200": {"description": "Planned or confirmed formation", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/TrainFormation"}}}}, "404": {"description": "No matching formation"}}
            }},
            "/journeys/{journeyId}/legs/{legIndex}/boarding-guidance": {"get": {
                "summary": "Get verified metro boarding guidance",
                "parameters": [
                    {"name": "journeyId", "in": "path", "required": true, "schema": {"type": "string"}},
                    {"name": "legIndex", "in": "path", "required": true, "schema": {"type": "integer", "minimum": 0}},
                    {"name": "profile", "in": "query", "required": true, "schema": {"type": "string", "enum": ["fastest", "wheelchair"]}}
                ],
                "responses": {"200": {"description": "Available or explicitly unavailable guidance", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/BoardingGuidance"}}}}, "404": {"description": "No verified guidance"}}
            }},
            "/journeys/search": {"post": {
                "summary": "Search journeys",
                "description": "Returns ranked journey candidates. City points expand to active physical stops. Coordinate points and selected stops are connected to reasonable nearby boarding points through the configured pedestrian graph, while autocomplete identities remain separate. Direct services between expanded stop endpoints are enumerated within the configured departure window independently of sampled RAPTOR probes. Final Pareto pruning compares departure, expected arrival, transfer count and walking distance; a faster transfer or a longer access walk cannot by itself eliminate a useful direct service. The primary ranked result and bounded simple and distinct-route alternatives reserve slots before departure-range sampling. Dominance is applied after real geometry validation. Realtime delays are used when checking transfers and ranking expected arrival. Every returned leg has real GeoJSON geometry: GTFS shapes for transit and pedestrian-router geometry for walking.",
                "requestBody": {
                    "required": true,
                    "content": {"application/json": {"schema": {
                        "type": "object",
                        "required": ["from", "to", "datetime", "mode", "transport_modes", "max_transfers", "walking_speed", "prefer_reliable_transfers", "offline_compatible"],
                        "properties": {
                            "from": {"$ref": "#/components/schemas/JourneyPoint"},
                            "to": {"$ref": "#/components/schemas/JourneyPoint"},
                            "datetime": {
                                "type": "string",
                                "description": "Departure date and time. RFC3339 offsets and Z timestamps are converted to Europe/Prague before selecting the public-transport service day; offset-less values are interpreted as Prague-local wall time."
                            },
                            "mode": {"type": "string", "enum": ["depart_at"], "description": "Only departure-at routing is implemented. arrive_by and unknown modes return HTTP 400 unsupported_search_mode."},
                            "transport_modes": {"type": "array", "items": {"type": "string"}},
                            "max_transfers": {"type": "integer", "minimum": 0},
                            "walking_speed": {"type": "string"},
                            "prefer_reliable_transfers": {"type": "boolean"},
                            "offline_compatible": {"type": "boolean"},
                            "include_intermediate_stops": {
                                "type": "boolean",
                                "default": false,
                                "description": "When true, every journey leg contains ordered stop_calls including its origin, all intermediate stops and its destination. The camelCase alias includeIntermediateStops is also accepted."
                            },
                            "journey_preferences": {"$ref": "#/components/schemas/JourneyPreferences"}
                        }
                    }, "example": {
                        "from": {"type": "coordinate", "lat": 50.089458, "lon": 14.428683},
                        "to": {"type": "stop", "id": "pid_gtfs:U897S1"},
                        "datetime": "2026-09-01T05:35:00+02:00",
                        "mode": "depart_at",
                        "transport_modes": ["metro", "tram", "bus", "train", "trolleybus", "ferry"],
                        "max_transfers": 4,
                        "walking_speed": "normal",
                        "prefer_reliable_transfers": true,
                        "offline_compatible": false,
                        "include_intermediate_stops": true
                    }}}
                }
            }},
            "/vehicles": {"get": {
                "summary": "Unified current public transport vehicle positions",
                "description": "Returns normalized fresh vehicle positions from licensed providers. PID includes Golemio vehicle equipment when available; IDS JMK includes DPMB and uses the official GTFS-Realtime feed. DÚK is disabled by default until redistribution terms are confirmed.",
                "parameters": [
                    {"name": "source", "in": "query", "schema": {"type": "string"}},
                    {"name": "provider", "in": "query", "schema": {"type": "string", "enum": ["pid", "ids_jmk", "duk"]}},
                    {"name": "bbox", "in": "query", "description": "Visible map bounds as west,south,east,north", "schema": {"type": "string", "example": "14.30,49.95,14.70,50.20"}},
                    {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 10000, "default": 2000}}
                ],
                "responses": {
                    "200": {"description": "Current normalized vehicle positions", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/VehiclesResponse"}}}},
                    "400": {"description": "Invalid bbox"}
                }
            }},
            "/data-sources/status": {"get": {
                "summary": "Automatic data-source synchronization status",
                "responses": {"200": {"description": "Freshness, record counts and latest errors for every automatic source"}}
            }},
            "/realtime/status": {"get": {
                "summary": "Current realtime synchronization status",
                "description": "Returns enabled realtime sources with current, syncing or stale freshness state. PID trip summaries and vehicle positions are reported independently from the full stop-level import.",
                "responses": {"200": {"description": "Current realtime source state"}}
            }},
            "/realtime/trip/{trip_id}": {"get": {
                "summary": "Current realtime updates for a trip",
                "parameters": [{
                    "name": "trip_id", "in": "path", "required": true,
                    "schema": {"type": "string"},
                    "description": "Source-scoped static trip ID, for example pid_gtfs:58_3600_260629"
                }],
                "responses": {"200": {"description": "Fresh stop-level and trip-summary updates"}}
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
            "/admin/imports/pid/start": {"post": {"summary": "Start PID schedule synchronization"}},
            "/admin/database/stats": {"get": {
                "summary": "Current database and routing-cache usage",
                "description": "Returns current PostgreSQL storage composition, estimated live and dead rows, largest indexes, connection and cache statistics, recent imports, source coverage and routing snapshot usage. Requires an admin or data_admin token.",
                "responses": {
                    "200": {"description": "Current administrator usage overview", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/AdminDatabaseStats"}}}},
                    "401": {"description": "Administrator authentication required"},
                    "403": {"description": "Authenticated account lacks an administrator role"}
                }
            }},
            "/admin/data-quality": {"get": {"summary": "Validation, duplicate and unresolved-stop metrics"}},
            "/admin/data-quality/validate": {"post": {
                "summary": "Run administrator database validation",
                "description": "Checks imported transport data for missing, invalid and disconnected records. Replaces only findings from the previous administrator validation run."
            }},
            "/admin/data-quality/repairs": {"get": {
                "summary": "Preview safe repairs and review duplicate-stop candidates",
                "description": "Returns counts for conservative automatic repairs, exact-coordinate groups, nearby same-direction candidates derived from scheduled stop sequences, and recent audited repair runs.",
                "parameters": [
                    {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 100, "default": 25}},
                    {"name": "offset", "in": "query", "schema": {"type": "integer", "minimum": 0, "default": 0}}
                ]
            }},
            "/admin/data-quality/repairs/automatic": {"post": {
                "summary": "Apply conservative automatic data repairs",
                "description": "Rebuilds missing normalized stop names, assigns unique exact municipality matches, expires inconsistent realtime rows, merges exact cross-feed aliases, and automatically flattens nearby same-name physical stops with the same locality, type and measured direction. Directional groups require one canonical stop within 120 metres of every member and no same-trip conflict. Records an audit run.",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {
                    "type": "object", "required": ["confirmation"],
                    "properties": {"confirmation": {"const": "apply_safe_repairs"}},
                    "additionalProperties": false
                }}}}
            }},
            "/admin/data-quality/duplicates/merge": {"post": {
                "summary": "Confirm and merge duplicate stop records",
                "description": "Exact-coordinate mode requires identical normalized names and coordinates. Nearby-same-direction mode accepts reviewed physical stops up to 120 metres apart only when public name, locality, type and measured travel direction agree. Both modes reject stops called by the same trip, preserve source IDs and persist mappings across imports.",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {
                    "type": "object",
                    "required": ["canonical_stop_id", "duplicate_stop_ids", "confirmation"],
                    "properties": {
                        "canonical_stop_id": {"type": "string", "minLength": 1},
                        "duplicate_stop_ids": {"type": "array", "minItems": 1, "maxItems": 25, "uniqueItems": true, "items": {"type": "string", "minLength": 1}},
                        "confirmation": {"const": "merge_duplicate_stops"},
                        "note": {"type": ["string", "null"]},
                        "strategy": {"type": "string", "enum": ["exact_coordinates", "nearby_same_direction"], "default": "exact_coordinates"}
                    },
                    "additionalProperties": false
                }}}}
            }},
            "/admin/unmatched-stops": {"get": {"summary": "List active stops with unresolved coordinates"}},
            "/admin/source-feeds": {"get": {"summary": "List configured source feeds"}},
            "/admin/source-feeds/{id}": {"patch": {"summary": "Update a source feed configuration"}},
            "/admin/routing-algorithm": {
                "get": {
                    "summary": "Read the active journey-search algorithm configuration and RAPTOR cache status",
                    "responses": {"200": {"description": "Active configuration and snapshot warmup status", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RoutingAlgorithmPayload"}}}}}
                },
                "put": {
                    "summary": "Replace and immediately activate the journey-search algorithm configuration",
                    "description": "Validates and persists candidate-generation limits, transfer constraints, scoring weights, dominance pruning and result-diversity guarantees. Requires an admin or data_admin token.",
                    "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RoutingAlgorithmConfig"}}}},
                    "responses": {"200": {"description": "Validated configuration is active for new searches", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RoutingAlgorithmPayload"}}}}, "400": {"description": "Invalid or unsafe parameter combination"}}
                },
                "delete": {
                    "summary": "Reset the journey-search algorithm to safe defaults",
                    "responses": {"200": {"description": "Default configuration is active for new searches", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RoutingAlgorithmPayload"}}}}}
                }
            },
            "/public/boards/{stopId}": {"get": {"summary": "Public departure board data"}}
        },
        "components": {
            "schemas": {
                "PlaceType": {
                    "type": "string",
                    "enum": ["city", "railway_station", "railway_stop", "bus_station", "bus_stop", "tram_stop", "metro_station", "ferry_terminal", "airport", "station_entrance", "generic_node", "boarding_area", "stop"]
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
                    "required": ["id", "name", "canonical_name", "aliases", "place_type", "modes"],
                    "properties": {
                        "id": {"type": "string"},
                        "name": {"type": "string"},
                        "canonical_name": {"type": "string"},
                        "aliases": {"type": "array", "items": {"type": "string"}},
                        "parent_stop_area_id": {"type": ["string", "null"]},
                        "place_type": {"$ref": "#/components/schemas/PlaceType"},
                        "municipality": {"type": ["string", "null"]},
                        "region": {"type": ["string", "null"]},
                        "lat": {"type": ["number", "null"]},
                        "lon": {"type": ["number", "null"]},
                        "modes": {"type": "array", "items": {"type": "string"}, "description": "Transport modes served by boarding points with this exact public stop name."},
                        "station_id": {"type": ["string", "null"]},
                        "complex_id": {"type": ["string", "null"]},
                        "has_station_layout": {"type": "boolean"},
                        "station_layout_version": {"type": ["string", "null"]}
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
                "StopCatalogResponse": {
                    "type": "object",
                    "required": ["schema_version", "count", "stops", "data_status"],
                    "properties": {
                        "schema_version": {"type": "integer", "const": 1},
                        "count": {"type": "integer", "minimum": 0},
                        "stops": {"type": "array", "items": {"$ref": "#/components/schemas/StopCatalogEntry"}},
                        "data_status": {"type": "object"}
                    }
                },
                "StopCatalogEntry": {
                    "type": "object",
                    "required": ["id", "name", "normalized_name", "canonical_name", "aliases", "modes", "location_type", "place_type"],
                    "properties": {
                        "id": {"type": "string"},
                        "name": {"type": "string"},
                        "normalized_name": {"type": "string"},
                        "canonical_name": {"type": "string"},
                        "aliases": {"type": "array", "items": {"type": "string"}},
                        "municipality": {"type": ["string", "null"]},
                        "region": {"type": ["string", "null"]},
                        "lat": {"type": ["number", "null"]},
                        "lon": {"type": ["number", "null"]},
                        "modes": {"type": "array", "items": {"type": "string"}},
                        "platform_code": {"type": ["string", "null"]},
                        "location_type": {"type": "string", "enum": ["stop", "station"]},
                        "place_type": {"$ref": "#/components/schemas/PlaceType"},
                        "parent_stop_area_id": {"type": ["string", "null"]},
                        "source_feed_id": {"type": ["string", "null"]}
                        ,"station_id": {"type": ["string", "null"]}
                        ,"complex_id": {"type": ["string", "null"]}
                        ,"has_station_layout": {"type": "boolean"}
                        ,"station_layout_version": {"type": ["string", "null"]}
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
                    "required": ["id", "source_ids", "name", "canonical_name", "aliases", "normalized_name", "location_type", "wheelchair_boarding", "modes", "coordinate_confidence", "is_active", "place_type", "marker_type", "map_visible"],
                    "properties": {
                        "id": {"type": "string"},
                        "source_ids": {"type": "array", "items": {"$ref": "#/components/schemas/StopSourceRef"}},
                        "name": {"type": "string"},
                        "canonical_name": {"type": "string"},
                        "aliases": {"type": "array", "items": {"type": "string"}},
                        "parent_stop_area_id": {"type": ["string", "null"]},
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
                        "location_type": {"type": "string", "enum": ["stop", "station", "entrance_exit", "generic_node", "boarding_area"]},
                        "parent_station_id": {"type": ["string", "null"]},
                        "station_id": {"type": ["string", "null"]},
                        "complex_id": {"type": ["string", "null"]},
                        "has_station_layout": {"type": "boolean"},
                        "station_layout_version": {"type": ["string", "null"]},
                        "wheelchair_boarding": {"type": "string", "enum": ["unknown", "accessible", "inaccessible"]},
                        "modes": {"type": "array", "items": {"type": "string"}, "description": "Transport modes served by boarding points with this exact public stop name."},
                        "place_type": {"$ref": "#/components/schemas/PlaceType"},
                        "marker_type": {"$ref": "#/components/schemas/PlaceType"},
                        "map_visible": {"type": "boolean"},
                        "is_active": {"type": "boolean"}
                    }
                },
                "VehiclesResponse": {
                    "type": "object",
                    "required": ["vehicles"],
                    "properties": {
                        "vehicles": {"type": "array", "items": {"$ref": "#/components/schemas/Vehicle"}},
                        "warnings": {"type": "array", "items": {"type": "string"}}
                    }
                },
                "Vehicle": {
                    "type": "object",
                    "required": ["id", "provider", "source", "vehicleId", "latitude", "longitude", "route", "accessibility", "amenities", "updatedAt", "confidence"],
                    "properties": {
                        "id": {"type": "string", "example": "pid:registration:8826"},
                        "provider": {"type": "string", "enum": ["pid", "ids_jmk", "duk", "unknown"]},
                        "source": {"$ref": "#/components/schemas/VehicleSource"},
                        "vehicleId": {"type": "string"},
                        "registrationNumber": {"type": ["string", "null"]},
                        "latitude": {"type": "number"},
                        "longitude": {"type": "number"},
                        "heading": {"type": ["number", "null"]},
                        "speedKmh": {"type": ["number", "null"]},
                        "route": {"type": "object", "additionalProperties": true},
                        "vehicleType": {"type": ["string", "null"], "enum": ["bus", "tram", "metro", "train", "trolleybus", "ferry", "cable_car", null]},
                        "accessibility": {"type": "object", "properties": {"wheelchairAccessible": {"type": ["boolean", "null"]}}},
                        "amenities": {"type": "object", "properties": {"airConditioned": {"type": ["boolean", "null"]}, "usbChargers": {"type": ["boolean", "null"]}}},
                        "occupancyStatus": {"type": ["string", "null"]},
                        "operatorName": {"type": ["string", "null"]},
                        "tracking": {"type": ["boolean", "null"]},
                        "state": {"type": ["string", "null"]},
                        "delaySeconds": {"type": ["integer", "null"]},
                        "updatedAt": {"type": "string", "format": "date-time"},
                        "validUntil": {"type": ["string", "null"], "format": "date-time"},
                        "confidence": {"type": "string"}
                    }
                },
                "VehicleSource": {
                    "type": "object",
                    "properties": {
                        "feedId": {"type": ["string", "null"]},
                        "url": {"type": ["string", "null"], "format": "uri"},
                        "license": {"type": ["string", "null"]},
                        "attribution": {"type": ["string", "null"]},
                        "termsUrl": {"type": ["string", "null"], "format": "uri"},
                        "redistributionAllowed": {"type": ["boolean", "null"]}
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
                "JourneyPreferences": {
                    "type": "object", "additionalProperties": false,
                    "required": ["profile"],
                    "properties": {
                        "profile": {"type": "string", "enum": ["standard", "wheelchair", "stroller", "luggage"]},
                        "step_free": {"type": "boolean", "default": false},
                        "prefer_fewer_stairs": {"type": "boolean", "default": false},
                        "minimum_transfer_buffer_seconds": {"type": "integer", "minimum": 0, "maximum": 1800, "default": 0}
                    }
                },
                "StationLayout": {
                    "type": "object",
                    "required": ["stationId", "name", "version", "updatedAt", "source", "attribution", "levels", "elements"],
                    "properties": {
                        "stationId": {"type": "string"}, "complexId": {"type": ["string", "null"]},
                        "name": {"type": "string"}, "version": {"type": "string"},
                        "updatedAt": {"type": "string", "format": "date-time"},
                        "source": {"type": "string"}, "attribution": {"type": "string"},
                        "levels": {"type": "array", "items": {"type": "object"}},
                        "elements": {"type": "array", "items": {"type": "object"}}
                    }
                },
                "TrainFormation": {
                    "type": "object",
                    "required": ["runId", "status", "orientationKnown", "updatedAt", "validUntil", "source", "vehicles"],
                    "properties": {
                        "runId": {"type": "string"}, "atCallId": {"type": ["string", "null"]},
                        "status": {"type": "string", "enum": ["planned", "confirmed"]},
                        "orientationKnown": {"type": "boolean"}, "directionLabel": {"type": ["string", "null"]},
                        "updatedAt": {"type": "string", "format": "date-time"},
                        "validUntil": {"type": "string", "format": "date-time"},
                        "source": {"type": "string"}, "vehicles": {"type": "array", "items": {"type": "object"}}
                    }
                },
                "BoardingGuidance": {
                    "type": "object", "required": ["status"],
                    "properties": {
                        "status": {"type": "string", "enum": ["available", "unavailable"]},
                        "trainZone": {"type": "string", "enum": ["front", "middle", "rear"]},
                        "reasonCode": {"type": "string", "enum": ["closest_to_transfer", "closest_to_exit", "closest_to_elevator"]},
                        "precision": {"type": "string", "enum": ["zone", "coach", "door"]},
                        "basis": {"type": "string"}, "targetElementId": {"type": "string"},
                        "layoutVersion": {"type": "string"}, "validUntil": {"type": "string", "format": "date-time"},
                        "coachPositionFromFront": {"type": ["integer", "null"], "minimum": 1},
                        "doorSideRelativeToTravel": {"type": ["string", "null"], "enum": ["left", "right", null]}
                    }
                },
                "RoutingAlgorithmConfig": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["max_results", "max_direct_candidates", "max_transfer_candidates", "min_transfer_seconds", "max_transfer_wait_seconds", "transfer_search_timeout_seconds", "next_day_search_from_seconds", "range_search_window_seconds", "max_range_departures", "endpoint_access_cache_enabled", "arrival_time_weight", "duration_weight", "transfer_penalty_seconds", "preserve_simplest", "preserve_each_transfer_count", "preserve_carrier_diversity", "remove_dominated", "dominate_only_same_carrier"],
                    "properties": {
                        "max_results": {"type": "integer", "minimum": 1, "maximum": 20, "default": 20},
                        "max_direct_candidates": {"type": "integer", "minimum": 1, "maximum": 500, "default": 20, "description": "Bounds exact-stop direct services enumerated from the indexed in-memory timetable in addition to RAPTOR, preserving distinct direct lines before filling with later departures."},
                        "max_transfer_candidates": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 40},
                        "min_transfer_seconds": {"type": "integer", "minimum": 60, "maximum": 3600, "default": 300},
                        "max_transfer_wait_seconds": {"type": "integer", "minimum": 300, "maximum": 21600, "default": 7200},
                        "transfer_search_timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 60, "default": 6},
                        "next_day_search_from_seconds": {"type": "integer", "minimum": 0, "maximum": 86399, "default": 64800},
                        "range_search_window_seconds": {"type": "integer", "minimum": 0, "maximum": 21600, "default": 5400},
                        "max_range_departures": {"type": "integer", "minimum": 1, "maximum": 96, "default": 48},
                        "endpoint_access_cache_enabled": {"type": "boolean", "default": true},
                        "arrival_time_weight": {"type": "number", "minimum": 0, "maximum": 10, "default": 1},
                        "duration_weight": {"type": "number", "minimum": 0, "maximum": 10, "default": 0},
                        "transfer_penalty_seconds": {"type": "integer", "minimum": 0, "maximum": 14400, "default": 0},
                        "preserve_simplest": {"type": "boolean", "default": true},
                        "preserve_each_transfer_count": {"type": "boolean", "default": true},
                        "preserve_carrier_diversity": {"type": "boolean", "default": true},
                        "remove_dominated": {"type": "boolean", "default": true},
                        "dominate_only_same_carrier": {"type": "boolean", "default": false, "description": "When true, compare Pareto dominance only for matching known carrier signatures; unknown or distinct carriers remain incomparable."}
                    }
                },
                "JourneyPoint": {
                    "type": "object",
                    "required": ["type"],
                    "properties": {
                        "type": {"type": "string", "enum": ["stop", "city", "coordinate"]},
                        "id": {"type": "string"},
                        "lat": {"type": ["number", "null"], "minimum": -90, "maximum": 90},
                        "lon": {"type": ["number", "null"], "minimum": -180, "maximum": 180}
                    },
                    "oneOf": [
                        {"title": "Stop point", "required": ["type", "id"], "properties": {"type": {"const": "stop"}}},
                        {"title": "City point", "required": ["type", "id"], "properties": {"type": {"const": "city"}}},
                        {"title": "Coordinate point", "required": ["type", "lat", "lon"], "properties": {"type": {"const": "coordinate"}}}
                    ],
                    "examples": [
                        {"type": "stop", "id": "pid_gtfs:U480S1"},
                        {"type": "city", "id": "city:CZ:554782"},
                        {"type": "coordinate", "lat": 50.089458, "lon": 14.428683}
                    ]
                },
                "GeoJsonLineString": {
                    "type": "object",
                    "required": ["type", "coordinates"],
                    "properties": {
                        "type": {"const": "LineString"},
                        "coordinates": {"type": "array", "minItems": 2, "items": {"type": "array", "prefixItems": [{"type": "number", "minimum": -180, "maximum": 180}, {"type": "number", "minimum": -90, "maximum": 90}], "minItems": 2, "maxItems": 2}}
                    }
                },
                "GeoJsonMultiLineString": {
                    "type": "object",
                    "required": ["type", "coordinates"],
                    "properties": {
                        "type": {"const": "MultiLineString"},
                        "coordinates": {"type": "array", "minItems": 1, "items": {"type": "array", "minItems": 2, "items": {"type": "array", "prefixItems": [{"type": "number", "minimum": -180, "maximum": 180}, {"type": "number", "minimum": -90, "maximum": 90}], "minItems": 2, "maxItems": 2}}}
                    }
                },
                "JourneyLegGeometry": {
                    "oneOf": [
                        {"$ref": "#/components/schemas/GeoJsonLineString"},
                        {"$ref": "#/components/schemas/GeoJsonMultiLineString"}
                    ]
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
                "JourneyLeg": {
                    "type": "object",
                    "required": ["from_stop_id", "to_stop_id", "departure_time", "arrival_time", "mode", "warnings", "direction", "display_name", "geometry"],
                    "properties": {
                        "from_stop_id": {"type": "string"},
                        "to_stop_id": {"type": "string"},
                        "from_stop_name": {"type": ["string", "null"]},
                        "to_stop_name": {"type": ["string", "null"]},
                        "route_id": {"type": ["string", "null"]},
                        "trip_id": {"type": ["string", "null"]},
                        "run_id": {"type": ["string", "null"]},
                        "departure_call_id": {"type": ["string", "null"]},
                        "from_station_id": {"type": ["string", "null"]},
                        "to_station_id": {"type": ["string", "null"]},
                        "line": {"type": ["string", "null"], "description": "Public short line name, with source prefixes removed only as a fallback."},
                        "mode_name": {"type": ["string", "null"], "description": "Human-readable Czech transport type such as Tramvaj, Autobus or Vlak."},
                        "route_name": {"type": ["string", "null"]},
                        "destination": {"type": ["string", "null"], "description": "Backward-compatible alias of direction for transit legs; destination stop name for walking legs."},
                        "direction": {"type": ["string", "null"], "description": "Passenger-facing direction shown for this boarding: stop_headsign, then trip_headsign, then the trip's actual terminal stop. Null for walking legs."},
                        "display_name": {"type": "string", "description": "Ready-to-display connection label such as 'Autobus 991 směr Nádraží Hostivař'."},
                        "departure_time": {"type": "integer", "minimum": 0, "description": "Scheduled service-day seconds."},
                        "arrival_time": {"type": "integer", "minimum": 0, "description": "Scheduled service-day seconds."},
                        "mode": {"type": "string"},
                        "warnings": {"type": "array", "items": {"type": "string"}},
                        "geometry": {"$ref": "#/components/schemas/JourneyLegGeometry", "description": "GTFS shape clipped in travel direction for transit, or verified pedestrian-router geometry for walking. Never a stop-to-stop fallback line."},
                        "realtime": {"$ref": "#/components/schemas/JourneyLegRealtime"}
                    }
                },
                "JourneyStopCall": {
                    "type": "object",
                    "required": ["trip_id", "stop_id", "stop_sequence", "name", "scheduled_arrival_seconds", "scheduled_departure_seconds", "scheduled_arrival", "scheduled_departure", "is_origin", "is_destination", "is_intermediate", "realtime"],
                    "properties": {
                        "trip_id": {"type": "string"},
                        "run_id": {"type": ["string", "null"]},
                        "call_id": {"type": ["string", "null"]},
                        "stop_id": {"type": "string"},
                        "station_id": {"type": ["string", "null"]},
                        "complex_id": {"type": ["string", "null"]},
                        "has_station_layout": {"type": "boolean"},
                        "station_layout_version": {"type": ["string", "null"]},
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
    let vehicles_path = specification["paths"]["/vehicles"].clone();
    specification["paths"]["/realtime/vehicles"] = vehicles_path;
    let schemas = specification["components"]["schemas"]
        .as_object_mut()
        .expect("OpenAPI schemas object");
    schemas.insert(
        "AdminDatabaseStats".to_string(),
        json!({
            "type": "object",
            "required": ["database_available"],
            "properties": {
                "database_available": {"type": "boolean"},
                "generated_at": {"type": ["string", "null"], "format": "date-time"},
                "database": {
                    "type": ["object", "null"],
                    "properties": {
                        "name": {"type": "string"},
                        "total_size_bytes": {"type": "integer", "minimum": 0},
                        "total_size_pretty": {"type": "string"}
                    }
                },
                "storage": {
                    "type": ["object", "null"],
                    "properties": {
                        "database_size_bytes": {"type": "integer", "minimum": 0},
                        "user_relations_bytes": {"type": "integer", "minimum": 0},
                        "table_data_bytes": {"type": "integer", "minimum": 0},
                        "index_bytes": {"type": "integer", "minimum": 0},
                        "auxiliary_bytes": {"type": "integer", "minimum": 0},
                        "database_other_bytes": {"type": "integer", "minimum": 0},
                        "routing_snapshot_bytes": {"type": "integer", "minimum": 0},
                        "note": {"type": "string"}
                    }
                },
                "runtime": {
                    "type": ["object", "null"],
                    "properties": {
                        "total_connections": {"type": "integer", "minimum": 0},
                        "max_connections": {"type": "integer", "minimum": 1},
                        "active_connections": {"type": "integer", "minimum": 0},
                        "idle_connections": {"type": "integer", "minimum": 0},
                        "cache_hit_percent": {"type": "number", "minimum": 0, "maximum": 100},
                        "commit_percent": {"type": "number", "minimum": 0, "maximum": 100},
                        "temp_files": {"type": "integer", "minimum": 0},
                        "temp_bytes": {"type": "integer", "minimum": 0},
                        "deadlocks": {"type": "integer", "minimum": 0},
                        "stats_reset": {"type": ["string", "null"], "format": "date-time"},
                        "server_started_at": {"type": "string", "format": "date-time"},
                        "uptime_seconds": {"type": "integer", "minimum": 0}
                    }
                },
                "totals": {
                    "type": ["object", "null"],
                    "properties": {
                        "tracked_rows": {"type": "integer", "minimum": 0},
                        "tracked_rows_are_estimated": {"type": "boolean"},
                        "dead_rows": {"type": "integer", "minimum": 0},
                        "dead_row_percent": {"type": "number", "minimum": 0, "maximum": 100}
                    }
                },
                "tables": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["table", "rows", "rows_are_estimated", "dead_rows", "table_size_bytes", "indexes_size_bytes", "total_size_bytes"],
                        "properties": {
                            "table": {"type": "string"},
                            "rows": {"type": "integer", "minimum": 0},
                            "rows_are_estimated": {"type": "boolean"},
                            "dead_rows": {"type": "integer", "minimum": 0},
                            "table_size_bytes": {"type": "integer", "minimum": 0},
                            "indexes_size_bytes": {"type": "integer", "minimum": 0},
                            "auxiliary_size_bytes": {"type": "integer", "minimum": 0},
                            "total_size_bytes": {"type": "integer", "minimum": 0},
                            "table_size_pretty": {"type": "string"},
                            "indexes_size_pretty": {"type": "string"},
                            "total_size_pretty": {"type": "string"},
                            "last_autovacuum": {"type": ["string", "null"], "format": "date-time"},
                            "last_autoanalyze": {"type": ["string", "null"], "format": "date-time"}
                        }
                    }
                },
                "largest_indexes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["table", "index", "size_bytes", "size_pretty", "scans"],
                        "properties": {
                            "table": {"type": "string"},
                            "index": {"type": "string"},
                            "size_bytes": {"type": "integer", "minimum": 0},
                            "size_pretty": {"type": "string"},
                            "scans": {"type": "integer", "minimum": 0}
                        }
                    }
                },
                "routing_snapshots": {"$ref": "#/components/schemas/RoutingSnapshotStatus"}
            }
        }),
    );
    schemas.insert(
        "RoutingAlgorithmPayload".to_string(),
        json!({
            "type": "object",
            "required": ["configuration", "defaults", "database_available", "snapshot_status", "search_diagnostics"],
            "properties": {
                "configuration": {"$ref": "#/components/schemas/RoutingAlgorithmConfig"},
                "defaults": {"$ref": "#/components/schemas/RoutingAlgorithmConfig"},
                "database_available": {"type": "boolean"},
                "updated_at": {"type": ["string", "null"], "format": "date-time"},
                "updated_by": {"type": ["string", "null"]},
                "snapshot_status": {"$ref": "#/components/schemas/RoutingSnapshotStatus"},
                "search_diagnostics": {"$ref": "#/components/schemas/RouteSearchDiagnostics"},
                "activation": {"type": "string"},
                "scoring_formula": {"type": "string"},
                "fare_note": {"type": "string"}
            }
        }),
    );
    schemas.insert(
        "RouteSearchDiagnostics".to_string(),
        json!({
            "type": "object",
            "required": ["retained_limit", "sample_count", "average_total_ms", "max_total_ms", "stage_aggregates", "recent", "implemented_improvements"],
            "properties": {
                "retained_limit": {"type": "integer", "minimum": 1},
                "sample_count": {"type": "integer", "minimum": 0},
                "average_total_ms": {"type": "integer", "minimum": 0},
                "max_total_ms": {"type": "integer", "minimum": 0},
                "bottleneck": {"type": ["object", "null"], "additionalProperties": true},
                "stage_aggregates": {"type": "array", "items": {"type": "object", "additionalProperties": true}},
                "recent": {"type": "array", "items": {"type": "object", "additionalProperties": true}},
                "implemented_improvements": {"type": "array", "items": {"type": "string"}}
            }
        }),
    );
    schemas.insert(
        "RoutingSnapshotStatus".to_string(),
        json!({
            "type": "object",
            "required": ["database_available", "directory", "snapshot_version", "warmup_interval_seconds", "total_size_bytes", "snapshots", "warmup"],
            "properties": {
                "database_available": {"type": "boolean"},
                "directory": {"type": "string"},
                "latest_import": {"type": ["string", "null"], "format": "date-time"},
                "latest_import_error": {"type": ["string", "null"]},
                "snapshot_version": {"type": "integer"},
                "warmup_interval_seconds": {"type": "integer"},
                "total_size_bytes": {"type": "integer", "minimum": 0},
                "snapshots": {"type": "array", "items": {"$ref": "#/components/schemas/RoutingSnapshotFile"}},
                "warmup": {"$ref": "#/components/schemas/RoutingWarmupStatus"}
            }
        }),
    );
    schemas.insert(
        "RoutingSnapshotFile".to_string(),
        json!({
            "type": "object",
            "required": ["service_date", "path", "exists", "memory_cached"],
            "properties": {
                "service_date": {"type": "string", "format": "date"},
                "file_name": {"type": ["string", "null"]},
                "path": {"type": "string"},
                "exists": {"type": "boolean"},
                "size_bytes": {"type": ["integer", "null"], "minimum": 0},
                "modified_at": {"type": ["string", "null"], "format": "date-time"},
                "memory_cached": {"type": "boolean"}
            }
        }),
    );
    schemas.insert(
        "RoutingWarmupStatus".to_string(),
        json!({
            "type": "object",
            "required": ["active", "stage", "total_dates"],
            "properties": {
                "active": {"type": "boolean"},
                "stage": {"type": "string"},
                "service_date": {"type": ["string", "null"], "format": "date"},
                "current_index": {"type": ["integer", "null"]},
                "total_dates": {"type": "integer"},
                "started_at": {"type": ["string", "null"], "format": "date-time"},
                "finished_at": {"type": ["string", "null"], "format": "date-time"},
                "elapsed_seconds": {"type": ["integer", "null"], "minimum": 0},
                "error": {"type": ["string", "null"]}
            }
        }),
    );
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
                "warnings": [safe_data_warning(error, "Transport data status unavailable")]
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
            {"id":"pid_gtfs","url":"https://data.pid.cz/PID_GTFS.zip","priority":10,"type":"gtfs"},
            {"id":"pid_lines_geodata","url":"https://data.pid.cz/geodata/Linky_7d_WGS84.json","priority":10,"type":"geojson"},
            {"id":"pid_realtime","url":"https://api.golemio.cz/v2/vehiclepositions/gtfsrt/trip_updates.pb","priority":10,"type":"gtfs_realtime"},
            {"id":"ids_jmk_gtfs","url":"https://kordis-jmk.cz/gtfs/gtfs.zip","priority":20,"type":"gtfs","includes":"DPMB"},
            {"id":"ids_jmk_realtime","url":"https://kordis-jmk.cz/gtfs/gtfsReal.dat","priority":20,"type":"gtfs_realtime","capabilities":["vehicle_positions"],"includes":"DPMB"}
        ]
    }))
}
