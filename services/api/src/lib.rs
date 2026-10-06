#![recursion_limit = "256"]

use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use chrono::{DateTime, Duration, NaiveDateTime, NaiveTime, Timelike, Utc};
use routing_core::{
    RaptorRealtimeData, RaptorRealtimeUpdate, RaptorRequest, RaptorSearchStats, RaptorStopTime,
    RaptorTimetable, RaptorTrip, SearchRequest as RoutingSearchRequest, direct_journeys,
    earliest_arrivals, fixture_snapshot, raptor_with_stats_excluding_routes,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use tokio::{
    sync::{OnceCell, RwLock},
    time,
};
use transit_model::{
    AccessibilityStatus, CoordinateConfidence, Journey, JourneyLeg, OfflinePackage, RealtimeStatus,
    Stop, StopLocationType, TicketOption, Transfer, TransportMode, normalize_czech_name,
};
use uuid::Uuid;

mod cd;
mod config;
mod controllers;
mod error;
mod http;
mod infrastructure;
mod operations;
#[cfg(test)]
mod security_tests;
mod repositories;
mod services;
mod telemetry;
mod ticketing;

use config::AppConfig;
use controllers::{account::*, system::*, transit::*};
use error::ApiError;
use repositories::users::{find_by_email as user_by_email_db, find_by_id as user_by_id_db};
use services::auth::{
    auth_response, create_user_record, current_user, hash_token, public_user, require_admin,
};

const MAX_JOURNEY_RESULTS: usize = 20;
const MAX_DIRECT_JOURNEY_CANDIDATES: i64 = 20;
const MAX_TRANSFER_JOURNEY_CANDIDATES: i64 = 40;
const SERVICE_DAY_SECONDS: u32 = 24 * 3600;
const NEXT_SERVICE_DAY_SEARCH_FROM_SECONDS: u32 = 18 * 3600;
const MIN_TRANSFER_SECONDS: u32 = 5 * 60;
const MAX_TRANSFER_WAIT_SECONDS: u32 = 2 * 3600;
const TRANSFER_SEARCH_TIMEOUT_SECONDS: u64 = 6;
const STOP_SEARCH_TIMEOUT_SECONDS: u64 = 3;
const NEARBY_JOURNEY_STOP_RADIUS_M: f64 = 700.0;
const MAX_NEARBY_JOURNEY_STOPS_PER_ENDPOINT: i64 = 24;
const RANGE_SEARCH_WINDOW_SECONDS: u32 = 90 * 60;
const MAX_RANGE_DEPARTURES: usize = 48;
const RAPTOR_TIMETABLE_SNAPSHOT_VERSION: u32 = 13;
const RAPTOR_RANGE_SEARCH_CONCURRENCY: usize = 2;
const RAPTOR_INITIAL_RANGE_DEPARTURES: usize = 1;
const RAPTOR_RANGE_EXPANSION_BATCH_DEPARTURES: usize = 2;
const RAPTOR_RANGE_EXPANSION_MIN_CANDIDATES: usize = 3;
const RAPTOR_ALTERNATIVE_ROUTE_PASSES: usize = 2;
const REASONABLE_ALTERNATIVE_SLACK_SECONDS: u32 = 15 * 60;
const RAPTOR_WARMUP_INTERVAL_SECONDS: u64 = 60;
const ROUTE_SEARCH_TIMING_HISTORY: usize = 50;
const ROUTING_ENDPOINT_ACCESS_BUDGET_MILLIS: u64 = 4_000;
const MAX_ENDPOINT_WALKING_DISTANCE_M: u32 = 1_500;
const MAX_INTERCHANGE_WALKING_DISTANCE_M: u32 = 1_000;
const MIN_STATION_INTERCHANGE_SECONDS: u32 = 180;
const MAX_WALKING_SNAP_DISTANCE_M: f64 = 80.0;
const MAX_TRANSIT_SHAPE_SNAP_DISTANCE_M: f64 = 250.0;
const ROUTING_REALTIME_REFRESH_INTERVAL_SECONDS: u64 = 60;
const ROUTING_REALTIME_CACHE_TTL_SECONDS: u64 = 90;
const ROUTING_REALTIME_STATEMENT_TIMEOUT_MILLIS: u64 = 15_000;
const JOURNEY_ROUTING_REALTIME_QUERY: &str = r#"
        SELECT trip_id, stop_id, delay_seconds
        FROM realtime_updates
        WHERE source = 'pid_gtfs_rt'
          AND source_entity_id >= 'trip-summary:'
          AND source_entity_id < 'trip-summary;'
          AND trip_id IS NOT NULL
          AND stop_id IS NULL
          AND delay_seconds IS NOT NULL
          AND valid_until >= now()
          AND service_date = $1
        LIMIT 10000
        "#;
const ADMIN_DEFAULT_PAGE_SIZE: usize = 50;
const ADMIN_MAX_PAGE_SIZE: usize = 200;
const ADMIN_MAX_MAP_STOPS: usize = 5000;
const ADMIN_VALIDATION_SOURCE_FILE: &str = "admin_database_validation";
const REALTIME_SOURCE_STATUS_IDS: &[&str] = &["pid_gtfs_rt", "ids_jmk_positions"];
const STOP_SEARCH_SOURCE_IDS_QUERY: &str = r#"
        SELECT source_id.stop_id, source_id.source_feed_id, source_id.original_source_id,
               source_id.import_run_id, source_id.priority, source_id.confidence,
               source_id.suppressed_as_duplicate
        FROM stop_source_ids AS source_id
        JOIN source_feeds AS source_feed
          ON source_feed.id = source_id.source_feed_id
         AND source_feed.enabled = true
        WHERE source_id.stop_id = ANY($1)
        ORDER BY source_id.stop_id ASC, source_id.priority ASC,
                 source_id.source_feed_id ASC
        "#;

type RaptorCacheKey = (chrono::NaiveDate, String);
type RaptorCacheCell = Arc<OnceCell<Arc<RaptorTimetable>>>;
type RaptorCache = Arc<RwLock<HashMap<RaptorCacheKey, RaptorCacheCell>>>;
type EndpointAccessCacheCell = Arc<OnceCell<EndpointAccessResult>>;
type EndpointAccessCache = Arc<RwLock<HashMap<EndpointAccessCacheKey, EndpointAccessCacheCell>>>;
type RoutingRealtimeCache = Arc<RwLock<Option<RoutingRealtimeCacheEntry>>>;
type RoutingWarmupStatus = Arc<RwLock<RoutingWarmupState>>;
type RouteSearchDiagnostics = Arc<RwLock<VecDeque<RouteSearchTiming>>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EndpointAccessCacheKey {
    revision_token: String,
    selected_stop_ids: Vec<String>,
    access_to_origin: bool,
    walking_speed_centimeters_per_second: u32,
}

#[derive(Debug, Clone, Default)]
struct EndpointAccessResult {
    transfers: Vec<Transfer>,
    diagnostics: Vec<String>,
}

#[derive(Clone)]
struct PedestrianRouter {
    client: reqwest::Client,
    engine: PedestrianRouterEngine,
    base_url: String,
    revision: String,
    permits: Arc<tokio::sync::Semaphore>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PedestrianRouterEngine {
    Osrm,
    Valhalla,
}

#[derive(Debug, Clone)]
struct WalkingRoute {
    distance_meters: u32,
    duration_seconds: u32,
    geometry: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalkingRouteRejection {
    NoRoute,
    NonWalkingSegment,
    InvalidGeometry,
    ExceededDistance,
    RouterUnavailable,
}

impl WalkingRouteRejection {
    fn diagnostic_code(self) -> &'static str {
        match self {
            Self::NoRoute => "nonexistent_walking_route",
            Self::NonWalkingSegment => "non_walking_segment",
            Self::InvalidGeometry => "invalid_walking_geometry",
            Self::ExceededDistance => "walking_distance_exceeded",
            Self::RouterUnavailable => "walking_router_unavailable",
        }
    }
}

#[derive(Debug, Clone)]
struct RoutingRealtimeCacheEntry {
    service_date: chrono::NaiveDate,
    loaded_at: time::Instant,
    data: Arc<RaptorRealtimeData>,
}

#[derive(Debug)]
struct RoutingRealtimeSnapshot {
    data: Arc<RaptorRealtimeData>,
    cache_hit: bool,
}

#[derive(Debug, Clone, Serialize)]
struct RouteSearchStageTiming {
    stage: String,
    elapsed_ms: u64,
    detail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RouteSearchTiming {
    started_at: DateTime<Utc>,
    service_date: chrono::NaiveDate,
    total_ms: u64,
    success: bool,
    result_count: usize,
    stages: Vec<RouteSearchStageTiming>,
}

struct RouteSearchTimingBuilder {
    started_at: DateTime<Utc>,
    started: time::Instant,
    service_date: chrono::NaiveDate,
    stages: Vec<RouteSearchStageTiming>,
}

impl RouteSearchTimingBuilder {
    fn new(service_date: chrono::NaiveDate) -> Self {
        Self {
            started_at: Utc::now(),
            started: time::Instant::now(),
            service_date,
            stages: Vec::new(),
        }
    }

    fn push(&mut self, stage: &str, started: time::Instant, detail: Option<String>) {
        self.stages.push(RouteSearchStageTiming {
            stage: stage.to_string(),
            elapsed_ms: elapsed_millis(started),
            detail,
        });
    }

    fn finish(self, success: bool, result_count: usize) -> RouteSearchTiming {
        RouteSearchTiming {
            started_at: self.started_at,
            service_date: self.service_date,
            total_ms: elapsed_millis(self.started),
            success,
            result_count,
            stages: self.stages,
        }
    }
}

fn elapsed_millis(started: time::Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

#[derive(Debug, Clone, Serialize)]
struct RoutingWarmupState {
    active: bool,
    stage: String,
    service_date: Option<chrono::NaiveDate>,
    current_index: Option<u32>,
    total_dates: u32,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    error: Option<String>,
}

impl Default for RoutingWarmupState {
    fn default() -> Self {
        Self {
            active: false,
            stage: "idle".to_string(),
            service_date: None,
            current_index: None,
            total_dates: 2,
            started_at: None,
            finished_at: None,
            error: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RaptorTimetableSnapshot {
    version: u32,
    service_date: chrono::NaiveDate,
    latest_import: Option<DateTime<Utc>>,
    revision_token: String,
    timetable: RaptorTimetable,
}

#[derive(Debug, Clone)]
struct RoutingDataRevision {
    latest_import: Option<DateTime<Utc>>,
    token: String,
}

struct AdminEntitySpec {
    key: &'static str,
    table: &'static str,
    label: &'static str,
    row_expression: &'static str,
    order_by: &'static str,
    map_available: bool,
}

struct DataValidationCheck {
    code: &'static str,
    severity: &'static str,
    entity: &'static str,
    description: &'static str,
    table: &'static str,
    id_expression: &'static str,
    predicate: &'static str,
}

#[rustfmt::skip]
const ADMIN_ENTITY_SPECS: &[AdminEntitySpec] = &[
    AdminEntitySpec { key: "import_runs", table: "import_runs", label: "Import runs", row_expression: "to_jsonb(t)", order_by: "started_at DESC", map_available: false },
    AdminEntitySpec { key: "source_feeds", table: "source_feeds", label: "Source feeds", row_expression: "to_jsonb(t)", order_by: "priority ASC, id ASC", map_available: false },
    AdminEntitySpec { key: "agencies", table: "agencies", label: "Agencies", row_expression: "to_jsonb(t)", order_by: "name ASC, id ASC", map_available: false },
    AdminEntitySpec { key: "operators", table: "operators", label: "Operators", row_expression: "to_jsonb(t)", order_by: "name ASC, id ASC", map_available: false },
    AdminEntitySpec { key: "cities", table: "cities", label: "Cities", row_expression: "to_jsonb(t)", order_by: "importance DESC, name ASC, id ASC", map_available: false },
    AdminEntitySpec { key: "stop_areas", table: "stop_areas", label: "Stop areas", row_expression: "to_jsonb(t) - 'geom'", order_by: "name ASC, id ASC", map_available: true },
    AdminEntitySpec { key: "stops", table: "stops", label: "Stops", row_expression: "to_jsonb(t) - 'geom'", order_by: "name ASC, platform_code ASC NULLS FIRST, id ASC", map_available: true },
    AdminEntitySpec { key: "stop_source_ids", table: "stop_source_ids", label: "Stop source IDs", row_expression: "to_jsonb(t)", order_by: "stop_id ASC, priority ASC", map_available: false },
    AdminEntitySpec { key: "routes", table: "routes", label: "Routes", row_expression: "to_jsonb(t)", order_by: "source_priority ASC, short_name ASC NULLS LAST, id ASC", map_available: false },
    AdminEntitySpec { key: "trips", table: "trips", label: "Trips", row_expression: "to_jsonb(t)", order_by: "source_priority ASC, id ASC", map_available: false },
    AdminEntitySpec { key: "stop_times", table: "stop_times", label: "Stop times", row_expression: "to_jsonb(t)", order_by: "trip_id ASC, stop_sequence ASC", map_available: false },
    AdminEntitySpec { key: "calendars", table: "calendars", label: "Calendars", row_expression: "to_jsonb(t)", order_by: "service_id ASC", map_available: false },
    AdminEntitySpec { key: "calendar_dates", table: "calendar_dates", label: "Calendar exceptions", row_expression: "to_jsonb(t)", order_by: "date DESC, service_id ASC", map_available: false },
    AdminEntitySpec { key: "transfers", table: "transfers", label: "Transfers", row_expression: "to_jsonb(t)", order_by: "from_stop_id ASC, to_stop_id ASC", map_available: false },
    AdminEntitySpec { key: "shapes", table: "shapes", label: "Shapes", row_expression: "to_jsonb(t) - 'geom'", order_by: "shape_id ASC, shape_pt_sequence ASC", map_available: true },
    AdminEntitySpec { key: "realtime_updates", table: "realtime_updates", label: "Realtime updates", row_expression: "to_jsonb(t) - 'vehicle_position'", order_by: "fetched_at DESC, id DESC", map_available: false },
    AdminEntitySpec { key: "data_source_syncs", table: "data_source_syncs", label: "Data source syncs", row_expression: "to_jsonb(t)", order_by: "last_attempt_at DESC, source_id ASC", map_available: false },
    AdminEntitySpec { key: "route_geometries", table: "route_geometries", label: "Route geometries", row_expression: "to_jsonb(t) - 'geom'", order_by: "source_route_id ASC, source_feature_id ASC", map_available: false },
    AdminEntitySpec { key: "manual_stop_matches", table: "manual_stop_matches", label: "Manual stop matches", row_expression: "to_jsonb(t)", order_by: "created_at DESC, id DESC", map_available: true },
    AdminEntitySpec { key: "data_repair_runs", table: "data_repair_runs", label: "Data repair runs", row_expression: "to_jsonb(t)", order_by: "created_at DESC, id DESC", map_available: false },
    AdminEntitySpec { key: "validation_issues", table: "validation_issues", label: "Validation issues", row_expression: "to_jsonb(t)", order_by: "created_at DESC, id DESC", map_available: false },
    AdminEntitySpec { key: "offline_packages", table: "offline_packages", label: "Offline packages", row_expression: "to_jsonb(t)", order_by: "created_at DESC, id ASC", map_available: false },
    AdminEntitySpec { key: "ticket_products_mock", table: "ticket_products_mock", label: "Mock ticket products", row_expression: "to_jsonb(t)", order_by: "id ASC", map_available: false },
    AdminEntitySpec { key: "users", table: "users", label: "Users", row_expression: "to_jsonb(t) - 'password_hash'", order_by: "created_at DESC, id DESC", map_available: false },
    AdminEntitySpec { key: "user_profiles", table: "user_profiles", label: "User profiles", row_expression: "to_jsonb(t)", order_by: "user_id ASC", map_available: false },
    AdminEntitySpec { key: "saved_places", table: "saved_places", label: "Saved places", row_expression: "to_jsonb(t)", order_by: "updated_at DESC, id DESC", map_available: true },
    AdminEntitySpec { key: "favorite_stops", table: "favorite_stops", label: "Favorite stops", row_expression: "to_jsonb(t)", order_by: "created_at DESC, id DESC", map_available: false },
    AdminEntitySpec { key: "favorite_routes", table: "favorite_routes", label: "Favorite routes", row_expression: "to_jsonb(t)", order_by: "created_at DESC, id DESC", map_available: false },
    AdminEntitySpec { key: "notification_preferences", table: "notification_preferences", label: "Notification preferences", row_expression: "to_jsonb(t)", order_by: "user_id ASC, type ASC", map_available: false },
    AdminEntitySpec { key: "user_sessions", table: "user_sessions", label: "User sessions", row_expression: "to_jsonb(t) - 'refresh_token_hash'", order_by: "created_at DESC, id DESC", map_available: false },
    AdminEntitySpec { key: "user_roles", table: "user_roles", label: "User roles", row_expression: "to_jsonb(t)", order_by: "user_id ASC, role ASC", map_available: false },
];

#[rustfmt::skip]
const DATA_VALIDATION_CHECKS: &[DataValidationCheck] = &[
    DataValidationCheck { code: "city_missing_required_data", severity: "error", entity: "cities", description: "Cities must retain a stable official municipality identifier, country and normalized name", table: "cities", id_expression: "id", predicate: "btrim(official_municipality_id) = '' OR btrim(country_code) = '' OR btrim(name) = '' OR btrim(normalized_name) = ''" },
    DataValidationCheck { code: "city_invalid_coordinates", severity: "error", entity: "cities", description: "City coordinates must be within valid latitude and longitude ranges", table: "cities", id_expression: "id", predicate: "lat IS NOT NULL AND lon IS NOT NULL AND (lat < -90 OR lat > 90 OR lon < -180 OR lon > 180)" },
    DataValidationCheck { code: "stop_missing_name", severity: "error", entity: "stops", description: "Active stops must have a name and normalized name", table: "stops", id_expression: "id", predicate: "is_active = true AND (btrim(name) = '' OR btrim(normalized_name) = '')" },
    DataValidationCheck { code: "stop_missing_city", severity: "warning", entity: "stops", description: "Active stops should be assigned to a stable city identifier", table: "stops", id_expression: "id", predicate: "is_active = true AND city_id IS NULL" },
    DataValidationCheck { code: "stop_missing_coordinates", severity: "warning", entity: "stops", description: "Active stops should have latitude and longitude", table: "stops", id_expression: "id", predicate: "is_active = true AND (lat IS NULL OR lon IS NULL)" },
    DataValidationCheck { code: "stop_invalid_coordinates", severity: "error", entity: "stops", description: "Stop coordinates must be within valid latitude and longitude ranges", table: "stops", id_expression: "id", predicate: "lat IS NOT NULL AND lon IS NOT NULL AND (lat < -90 OR lat > 90 OR lon < -180 OR lon > 180)" },
    DataValidationCheck { code: "stop_missing_source_tracking", severity: "error", entity: "stops", description: "Active stops must retain their source feed and original source identifier", table: "stops", id_expression: "id", predicate: "is_active = true AND (source_feed_id IS NULL OR NOT EXISTS (SELECT 1 FROM stop_source_ids source_ids WHERE source_ids.stop_id = stops.id))" },
    DataValidationCheck { code: "route_missing_name", severity: "warning", entity: "routes", description: "Active routes should have a short or long public name", table: "routes", id_expression: "id", predicate: "is_active = true AND COALESCE(btrim(short_name), '') = '' AND COALESCE(btrim(long_name), '') = ''" },
    DataValidationCheck { code: "route_missing_source_tracking", severity: "error", entity: "routes", description: "Routes must retain their source feed and source identifier", table: "routes", id_expression: "id", predicate: "source_feed_id IS NULL OR btrim(source_id) = ''" },
    DataValidationCheck { code: "route_without_trips", severity: "warning", entity: "routes", description: "Active routes should contain at least one trip", table: "routes", id_expression: "id", predicate: "is_active = true AND NOT EXISTS (SELECT 1 FROM trips WHERE trips.route_id = routes.id)" },
    DataValidationCheck { code: "trip_missing_source_tracking", severity: "error", entity: "trips", description: "Trips must retain their source feed, source identifier and service identifier", table: "trips", id_expression: "id", predicate: "source_feed_id IS NULL OR btrim(source_id) = '' OR btrim(service_id) = ''" },
    DataValidationCheck { code: "realtime_missing_source_tracking", severity: "error", entity: "realtime_updates", description: "Realtime records must retain their source feed and external entity identifier", table: "realtime_updates", id_expression: "id::text", predicate: "source_feed_id IS NULL OR COALESCE(btrim(source_entity_id), '') = ''" },
    DataValidationCheck { code: "realtime_invalid_validity", severity: "warning", entity: "realtime_updates", description: "Realtime validity must not end before the source fetch timestamp", table: "realtime_updates", id_expression: "id::text", predicate: "valid_until IS NOT NULL AND valid_until < fetched_at" },
    DataValidationCheck { code: "trip_without_stop_times", severity: "error", entity: "trips", description: "Trips must contain at least one stop time", table: "trips", id_expression: "id", predicate: "NOT EXISTS (SELECT 1 FROM stop_times WHERE stop_times.trip_id = trips.id)" },
    DataValidationCheck { code: "trip_without_service_calendar", severity: "warning", entity: "trips", description: "Trip service identifiers should exist in calendars or calendar exceptions", table: "trips", id_expression: "id", predicate: "NOT EXISTS (SELECT 1 FROM calendars WHERE calendars.service_id = trips.service_id) AND NOT EXISTS (SELECT 1 FROM calendar_dates WHERE calendar_dates.service_id = trips.service_id)" },
    DataValidationCheck { code: "stop_time_invalid_time", severity: "error", entity: "stop_times", description: "Stop times must be non-negative, ordered and within a two-day service window", table: "stop_times", id_expression: "trip_id || ':' || stop_sequence::text", predicate: "arrival_time < 0 OR departure_time < arrival_time OR arrival_time > 172800 OR departure_time > 172800" },
    DataValidationCheck { code: "stop_time_missing_source_tracking", severity: "warning", entity: "stop_times", description: "Stop times should retain their source feed and import run", table: "stop_times", id_expression: "trip_id || ':' || stop_sequence::text", predicate: "source_feed_id IS NULL OR import_run_id IS NULL" },
    DataValidationCheck { code: "calendar_invalid_range", severity: "error", entity: "calendars", description: "Calendars must have a valid date range and at least one active weekday", table: "calendars", id_expression: "service_id", predicate: "end_date < start_date OR NOT (monday OR tuesday OR wednesday OR thursday OR friday OR saturday OR sunday)" },
    DataValidationCheck { code: "enabled_source_without_successful_import", severity: "warning", entity: "source_feeds", description: "Enabled source feeds should have a successful import", table: "source_feeds", id_expression: "id", predicate: "enabled = true AND NOT EXISTS (SELECT 1 FROM import_runs WHERE import_runs.status = 'success' AND import_runs.summary->>'feed_id' = source_feeds.id)" },
];

#[derive(Clone)]
struct AppState {
    config: Arc<AppConfig>,
    users: Arc<RwLock<HashMap<Uuid, UserRecord>>>,
    refresh_tokens: Arc<RwLock<HashMap<String, Uuid>>>,
    saved_places: Arc<RwLock<HashMap<Uuid, Vec<SavedPlace>>>>,
    favorite_stops: Arc<RwLock<HashMap<Uuid, Vec<FavoriteStop>>>>,
    stops: Arc<Vec<Stop>>,
    cities: Arc<Vec<City>>,
    db: Option<PgPool>,
    jwt_secret: String,
    use_mock_data: bool,
    ticketing: ticketing::TicketingService,
    raptor_cache: RaptorCache,
    endpoint_access_cache: EndpointAccessCache,
    pedestrian_router: PedestrianRouter,
    routing_realtime_cache: RoutingRealtimeCache,
    routing_warmup_status: RoutingWarmupStatus,
    route_search_diagnostics: RouteSearchDiagnostics,
    security: http::security::Security,
    telemetry: telemetry::Telemetry,
    stop_catalog_cache: Arc<tokio::sync::Mutex<Option<controllers::transit::CachedStopCatalog>>>,
}

#[derive(Debug, Clone, Serialize)]
struct City {
    id: String,
    name: String,
    normalized_name: String,
    region: Option<String>,
    country_code: String,
    lat: Option<f64>,
    lon: Option<f64>,
    importance: i32,
}

#[derive(Debug, Clone)]
struct UserRecord {
    id: Uuid,
    email: String,
    password_hash: String,
    display_name: Option<String>,
    roles: Vec<String>,
    created_at: chrono::DateTime<Utc>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    auth_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Claims {
    sub: String,
    email: String,
    roles: Vec<String>,
    exp: usize,
    #[serde(default)]
    auth_version: i64,
}

#[derive(Debug, Deserialize)]
struct RegisterRequest {
    email: String,
    password: String,
    display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
    device_name: Option<String>,
}

#[derive(Debug, Serialize)]
struct AuthResponse {
    access_token: String,
    refresh_token: String,
    token_type: String,
    expires_in_seconds: i64,
    user: PublicUser,
}

#[derive(Debug, Serialize)]
struct PublicUser {
    id: Uuid,
    email: String,
    display_name: Option<String>,
    roles: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RefreshRequest {
    refresh_token: String,
}

#[derive(Debug, Deserialize)]
struct ChangePasswordRequest {
    current_password: String,
    new_password: String,
}

#[derive(Debug, Deserialize)]
struct PasswordResetRequest {
    email: String,
}

#[derive(Debug, Deserialize)]
struct PasswordResetCompleteRequest {
    token: String,
    new_password: String,
}

#[derive(Debug, Deserialize)]
struct ProfileUpdateRequest {
    preferred_walking_speed: Option<String>,
    prefer_fewer_transfers: Option<bool>,
    prefer_reliable_transfers: Option<bool>,
    default_departure_mode: Option<String>,
    language: Option<String>,
    accessibility_preferences: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SavedRouteRequest {
    name: String,
    origin: Value,
    destination: Value,
    via: Option<Value>,
    #[serde(default)]
    via_dwell_seconds: i32,
    #[serde(default)]
    transport_modes: Vec<String>,
    #[serde(default)]
    preferences: Value,
    #[serde(default)]
    position: i32,
    #[serde(default)]
    pinned: bool,
    commute: Option<Value>,
    expected_version: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SavedRouteListQuery {
    since: Option<DateTime<Utc>>,
    #[serde(default)]
    include_deleted: bool,
}

#[derive(Debug, Deserialize)]
struct SavedRouteDeleteQuery {
    expected_version: i64,
}

#[derive(Debug, Deserialize)]
struct DeviceRegistrationRequest {
    platform: String,
    push_token: String,
    timezone: String,
    app_version: Option<String>,
    locale: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JourneySubscriptionRequest {
    device_id: Uuid,
    run_id: String,
    service_date: chrono::NaiveDate,
    trip_id: String,
    boarding_call_id: String,
    boarding_stop_id: String,
    boarding_stop_sequence: i32,
    alighting_call_id: Option<String>,
    alighting_stop_id: Option<String>,
    alighting_stop_sequence: Option<i32>,
    connection_run_id: Option<String>,
    connection_service_date: Option<chrono::NaiveDate>,
    connection_trip_id: Option<String>,
    connection_call_id: Option<String>,
    minimum_transfer_seconds: Option<i32>,
    #[serde(default = "default_significant_delay_seconds")]
    significant_delay_seconds: i32,
    expires_at: DateTime<Utc>,
}

fn default_significant_delay_seconds() -> i32 {
    300
}

#[derive(Debug, Deserialize)]
struct SavedPlaceRequest {
    name: String,
    #[serde(rename = "type")]
    place_type: String,
    stop_id: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SavedPlace {
    id: Uuid,
    user_id: Uuid,
    name: String,
    #[serde(rename = "type")]
    place_type: String,
    stop_id: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    address: Option<String>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct FavoriteStopRequest {
    stop_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FavoriteStop {
    id: Uuid,
    user_id: Uuid,
    stop_id: String,
    created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct StopSearchQuery {
    #[serde(default, alias = "query", alias = "text", alias = "term")]
    q: Option<String>,
    limit: Option<usize>,
    #[serde(rename = "includeCities", alias = "include_cities", default)]
    include_cities: bool,
    #[serde(rename = "includeRelated", alias = "include_related", default)]
    include_related: bool,
}

#[derive(Debug, Deserialize)]
struct RealtimeVehiclesQuery {
    source: Option<String>,
    provider: Option<String>,
    bbox: Option<String>,
    limit: Option<usize>,
}

impl RealtimeVehiclesQuery {
    fn parsed_bbox(&self) -> Result<Option<[f64; 4]>, ApiError> {
        let Some(raw) = self.bbox.as_deref() else {
            return Ok(None);
        };
        let values = raw
            .split(',')
            .map(|value| value.trim().parse::<f64>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ApiError {
                code: "validation_error".to_string(),
                message: "bbox must contain west,south,east,north numbers".to_string(),
            })?;
        let [west, south, east, north] = values.as_slice() else {
            return Err(ApiError {
                code: "validation_error".to_string(),
                message: "bbox must contain exactly west,south,east,north".to_string(),
            });
        };
        if !west.is_finite()
            || !south.is_finite()
            || !east.is_finite()
            || !north.is_finite()
            || !(-180.0..=180.0).contains(west)
            || !(-180.0..=180.0).contains(east)
            || !(-90.0..=90.0).contains(south)
            || !(-90.0..=90.0).contains(north)
            || west >= east
            || south >= north
        {
            return Err(ApiError {
                code: "validation_error".to_string(),
                message: "bbox coordinates are invalid or reversed".to_string(),
            });
        }
        Ok(Some([*west, *south, *east, *north]))
    }
}

#[derive(Debug, Deserialize)]
struct NearbyQuery {
    lat: f64,
    lon: f64,
    radius: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct StopsInBoundsQuery {
    south: f64,
    west: f64,
    north: f64,
    east: f64,
    limit: Option<usize>,
    cursor: Option<String>,
}

impl StopsInBoundsQuery {
    fn validate(&self) -> Result<(), ApiError> {
        let valid_latitude = |value: f64| value.is_finite() && (-90.0..=90.0).contains(&value);
        let valid_longitude = |value: f64| value.is_finite() && (-180.0..=180.0).contains(&value);

        if !valid_latitude(self.south) || !valid_latitude(self.north) {
            return Err(ApiError {
                code: "validation_error".to_string(),
                message: "south and north must be finite latitudes between -90 and 90".to_string(),
            });
        }
        if !valid_longitude(self.west) || !valid_longitude(self.east) {
            return Err(ApiError {
                code: "validation_error".to_string(),
                message: "west and east must be finite longitudes between -180 and 180".to_string(),
            });
        }
        if self.south >= self.north {
            return Err(ApiError {
                code: "validation_error".to_string(),
                message: "south must be less than north".to_string(),
            });
        }
        if self.west >= self.east {
            return Err(ApiError {
                code: "validation_error".to_string(),
                message: "west must be less than east".to_string(),
            });
        }
        if self.limit.is_some_and(|limit| !(1..=1000).contains(&limit)) {
            return Err(ApiError {
                code: "validation_error".to_string(),
                message: "limit must be between 1 and 1000".to_string(),
            });
        }
        if self.cursor.as_ref().is_some_and(|cursor| cursor.is_empty()) {
            return Err(ApiError {
                code: "validation_error".to_string(),
                message: "cursor must not be empty".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct DeparturesQuery {
    #[serde(rename = "stopId")]
    stop_id: String,
    time: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct StationLayoutQuery {
    level: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FormationQuery {
    #[serde(default, rename = "atCallId", alias = "at_call_id")]
    at_call_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BoardingGuidanceQuery {
    profile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JourneyPreferences {
    profile: String,
    #[serde(default)]
    step_free: bool,
    #[serde(default)]
    prefer_fewer_stairs: bool,
    #[serde(default)]
    minimum_transfer_buffer_seconds: u32,
}

#[derive(Debug, Deserialize)]
struct JourneySearchBody {
    from: JourneyPoint,
    to: JourneyPoint,
    datetime: String,
    mode: String,
    transport_modes: Vec<TransportMode>,
    max_transfers: u32,
    walking_speed: String,
    prefer_reliable_transfers: bool,
    offline_compatible: bool,
    #[serde(default, alias = "includeIntermediateStops")]
    include_intermediate_stops: bool,
    #[serde(default, alias = "journeyPreferences")]
    journey_preferences: Option<JourneyPreferences>,
}

#[derive(Debug, Clone)]
struct JourneyStopCall {
    trip_id: String,
    stop_id: String,
    stop_sequence: i32,
    scheduled_arrival: i32,
    scheduled_departure: i32,
    pickup_type: Option<i16>,
    drop_off_type: Option<i16>,
    timepoint: Option<bool>,
    stop_time_platform: Option<String>,
    stop_name: String,
    municipality: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    platform_code: Option<String>,
    station_id: Option<String>,
    complex_id: Option<String>,
    has_station_layout: bool,
    station_layout_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct JourneyPoint {
    #[serde(rename = "type")]
    point_type: String,
    id: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct AdminDataQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    q: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AdminMapQuery {
    q: Option<String>,
    source_feed_id: Option<String>,
    min_lat: Option<f64>,
    min_lon: Option<f64>,
    max_lat: Option<f64>,
    max_lon: Option<f64>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct AdminSourceFeedPatch {
    name: Option<String>,
    url: Option<String>,
    mode_scope: Option<String>,
    priority: Option<i32>,
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AdminRepairQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdminSafeRepairRequest {
    confirmation: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdminDuplicateStopMergeRequest {
    canonical_stop_id: String,
    duplicate_stop_ids: Vec<String>,
    confirmation: String,
    note: Option<String>,
    strategy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct RoutingAlgorithmConfig {
    max_results: i32,
    max_direct_candidates: i32,
    max_transfer_candidates: i32,
    min_transfer_seconds: i32,
    max_transfer_wait_seconds: i32,
    transfer_search_timeout_seconds: i32,
    next_day_search_from_seconds: i32,
    range_search_window_seconds: i32,
    max_range_departures: i32,
    endpoint_access_cache_enabled: bool,
    arrival_time_weight: f64,
    duration_weight: f64,
    transfer_penalty_seconds: i32,
    preserve_simplest: bool,
    preserve_each_transfer_count: bool,
    preserve_carrier_diversity: bool,
    remove_dominated: bool,
    dominate_only_same_carrier: bool,
}

impl Default for RoutingAlgorithmConfig {
    fn default() -> Self {
        Self {
            max_results: MAX_JOURNEY_RESULTS as i32,
            max_direct_candidates: MAX_DIRECT_JOURNEY_CANDIDATES as i32,
            max_transfer_candidates: MAX_TRANSFER_JOURNEY_CANDIDATES as i32,
            min_transfer_seconds: MIN_TRANSFER_SECONDS as i32,
            max_transfer_wait_seconds: MAX_TRANSFER_WAIT_SECONDS as i32,
            transfer_search_timeout_seconds: TRANSFER_SEARCH_TIMEOUT_SECONDS as i32,
            next_day_search_from_seconds: NEXT_SERVICE_DAY_SEARCH_FROM_SECONDS as i32,
            range_search_window_seconds: RANGE_SEARCH_WINDOW_SECONDS as i32,
            max_range_departures: MAX_RANGE_DEPARTURES as i32,
            endpoint_access_cache_enabled: true,
            arrival_time_weight: 1.0,
            duration_weight: 0.0,
            transfer_penalty_seconds: 0,
            preserve_simplest: true,
            preserve_each_transfer_count: true,
            preserve_carrier_diversity: true,
            remove_dominated: true,
            dominate_only_same_carrier: false,
        }
    }
}

impl RoutingAlgorithmConfig {
    fn validate(&self) -> Result<(), ApiError> {
        let checks = [
            ("max_results", self.max_results, 1, 20),
            ("max_direct_candidates", self.max_direct_candidates, 1, 500),
            (
                "max_transfer_candidates",
                self.max_transfer_candidates,
                1,
                1000,
            ),
            ("min_transfer_seconds", self.min_transfer_seconds, 60, 3600),
            (
                "max_transfer_wait_seconds",
                self.max_transfer_wait_seconds,
                300,
                21600,
            ),
            (
                "transfer_search_timeout_seconds",
                self.transfer_search_timeout_seconds,
                1,
                60,
            ),
            (
                "next_day_search_from_seconds",
                self.next_day_search_from_seconds,
                0,
                86399,
            ),
            (
                "range_search_window_seconds",
                self.range_search_window_seconds,
                0,
                21600,
            ),
            ("max_range_departures", self.max_range_departures, 1, 96),
            (
                "transfer_penalty_seconds",
                self.transfer_penalty_seconds,
                0,
                14400,
            ),
        ];
        for (field, value, minimum, maximum) in checks {
            if !(minimum..=maximum).contains(&value) {
                return Err(invalid_field(field, minimum, maximum));
            }
        }
        if self.max_transfer_wait_seconds < self.min_transfer_seconds {
            return Err(ApiError {
                code: "validation_error".to_string(),
                message: "max_transfer_wait_seconds must be greater than or equal to min_transfer_seconds"
                    .to_string(),
            });
        }
        for (field, value) in [
            ("arrival_time_weight", self.arrival_time_weight),
            ("duration_weight", self.duration_weight),
        ] {
            if !value.is_finite() || !(0.0..=10.0).contains(&value) {
                return Err(ApiError {
                    code: "validation_error".to_string(),
                    message: format!("{field} must be a finite number between 0 and 10"),
                });
            }
        }
        if self.arrival_time_weight == 0.0 && self.duration_weight == 0.0 {
            return Err(ApiError {
                code: "validation_error".to_string(),
                message: "arrival_time_weight and duration_weight cannot both be zero".to_string(),
            });
        }
        Ok(())
    }
}

fn invalid_field(field: &str, minimum: i32, maximum: i32) -> ApiError {
    ApiError {
        code: "validation_error".to_string(),
        message: format!("{field} must be between {minimum} and {maximum}"),
    }
}

fn init_tracing(production: bool) -> anyhow::Result<()> {
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = tracing_subscriber::EnvFilter::from_default_env();
    if production {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .finish()
            .try_init()?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .finish()
            .try_init()?;
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => tracing::error!(%error, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutdown signal received; draining active requests");
}

pub async fn run() -> anyhow::Result<()> {
    let config = AppConfig::from_env()?;
    init_tracing(config.production)?;
    match prune_raptor_snapshots(
        &config.routing_snapshot_dir,
        config.routing_snapshot_files_to_keep,
    )
    .await
    {
        Ok(removed) if removed > 0 => tracing::info!(
            removed,
            current_version = RAPTOR_TIMETABLE_SNAPSHOT_VERSION,
            files_to_keep = config.routing_snapshot_files_to_keep,
            directory = %config.routing_snapshot_dir.display(),
            "deleted unused RAPTOR snapshots"
        ),
        Ok(_) => {}
        Err(error) => tracing::warn!(
            %error,
            directory = %config.routing_snapshot_dir.display(),
            "failed to clean RAPTOR snapshots"
        ),
    }
    let app = app_state_with_config(config.clone()).await?;
    tokio::spawn(operations::monitor(app.clone()));
    let router = build_router(app);
    tracing::info!(address = %config.bind_address, "starting Cesta API");
    let listener = tokio::net::TcpListener::bind(config.bind_address).await?;
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

#[cfg(test)]
async fn app_state() -> anyhow::Result<AppState> {
    app_state_with_config(AppConfig::from_env()?).await
}

async fn app_state_with_config(config: AppConfig) -> anyhow::Result<AppState> {
    let db = if config.use_mock_data {
        None
    } else {
        Some(
            infrastructure::database::connect_with_retry(
                config
                    .database_url
                    .as_deref()
                    .expect("configuration validates DATABASE_URL"),
                &config.database_pool,
            )
            .await?,
        )
    };

    let cd_client: Option<Arc<dyn cd::CdApi>> =
        match (env::var("CD_TICKET_API_USER").ok(), cd_private_key()?) {
            (Some(user), Some(private_key)) if !user.is_empty() => Some(Arc::new(
                cd::HttpCdClient::new(cd::CdConfig {
                    base_url: env::var("CD_TICKET_API_BASE_URL")
                        .unwrap_or_else(|_| "https://ticket-api.cd.cz/v1".to_string()),
                    partner_user: cd::Secret::new(user),
                    private_key_pem: cd::Secret::new(private_key),
                    description: env::var("CD_TICKET_API_DESCRIPTION")
                        .unwrap_or_else(|_| "Cesta API".to_string()),
                    language: match env::var("CD_TICKET_API_LANGUAGE").as_deref() {
                        Ok("en") => cd::Language::En,
                        Ok("de") => cd::Language::De,
                        _ => cd::Language::Cs,
                    },
                    timeout: std::time::Duration::from_secs(
                        env::var("CD_TICKET_API_TIMEOUT_SECONDS")
                            .ok()
                            .and_then(|value| value.parse().ok())
                            .unwrap_or(15),
                    ),
                })
                .map_err(|error| anyhow::anyhow!("invalid ČD Ticket API configuration: {error}"))?,
            )),
            _ => None,
        };
    let payment_provider: Arc<dyn ticketing::PaymentProvider> = match (
        env::var("PAYMENT_PROVIDER_BASE_URL").ok(),
        env::var("PAYMENT_PROVIDER_API_KEY").ok(),
        env::var("MOBILE_CHECKOUT_RETURN_URL").ok(),
        env::var("MOBILE_CHECKOUT_CANCEL_URL").ok(),
    ) {
        (Some(base_url), Some(api_key), Some(return_url), Some(cancel_url))
            if !base_url.is_empty()
                && !api_key.is_empty()
                && !return_url.is_empty()
                && !cancel_url.is_empty() =>
        {
            Arc::new(
                ticketing::HttpPaymentProvider::new(
                    base_url,
                    api_key,
                    return_url,
                    cancel_url,
                    std::time::Duration::from_secs(10),
                )
                .map_err(|error| {
                    anyhow::anyhow!("invalid payment provider configuration: {error}")
                })?,
            )
        }
        _ => Arc::new(ticketing::DisabledPaymentProvider),
    };
    let ticketing = ticketing::TicketingService::new(cd_client, payment_provider, db.clone());
    let pedestrian_router = PedestrianRouter {
        client: reqwest::Client::builder()
            .timeout(config.pedestrian_router_timeout)
            .user_agent("Cesta-API/0.1 pedestrian-routing")
            .build()
            .context("could not create pedestrian routing HTTP client")?,
        engine: match config.pedestrian_router_engine.as_str() {
            "osrm" => PedestrianRouterEngine::Osrm,
            _ => PedestrianRouterEngine::Valhalla,
        },
        base_url: config.pedestrian_router_url.clone(),
        revision: config.pedestrian_router_revision.clone(),
        permits: Arc::new(tokio::sync::Semaphore::new(
            config.pedestrian_router_concurrency,
        )),
    };

    let routing_realtime_cache = Arc::new(RwLock::new(None));
    let routing_realtime_initially_ready = if let Some(pool) = &db {
        refresh_routing_realtime(pool, &routing_realtime_cache).await
    } else {
        false
    };
    services::auth::dummy_password_hash();
    let state = AppState {
        config: Arc::new(config.clone()),
        users: Arc::new(RwLock::new(HashMap::new())),
        refresh_tokens: Arc::new(RwLock::new(HashMap::new())),
        saved_places: Arc::new(RwLock::new(HashMap::new())),
        favorite_stops: Arc::new(RwLock::new(HashMap::new())),
        stops: Arc::new(fixture_stops()),
        cities: Arc::new(fixture_cities()),
        security: http::security::Security::new(
            config.max_concurrent_requests,
            config.max_concurrent_searches,
        ),
        telemetry: telemetry::Telemetry::new(db.clone()),
        stop_catalog_cache: Arc::new(tokio::sync::Mutex::new(None)),
        db,
        jwt_secret: config.jwt_secret.clone(),
        use_mock_data: config.use_mock_data,
        ticketing,
        raptor_cache: Arc::new(RwLock::new(HashMap::new())),
        endpoint_access_cache: Arc::new(RwLock::new(HashMap::new())),
        pedestrian_router,
        routing_realtime_cache,
        routing_warmup_status: Arc::new(RwLock::new(RoutingWarmupState::default())),
        route_search_diagnostics: Arc::new(RwLock::new(VecDeque::new())),
    };
    if let Some(pool) = state.db.clone() {
        let cache = state.raptor_cache.clone();
        let routing_realtime_cache = state.routing_realtime_cache.clone();
        let snapshot_dir = state.config.routing_snapshot_dir.clone();
        let snapshot_files_to_keep = state.config.routing_snapshot_files_to_keep;
        let warmup_status = state.routing_warmup_status.clone();
        let pedestrian_router = state.pedestrian_router.clone();
        tokio::spawn(warm_routing_realtime(
            pool.clone(),
            routing_realtime_cache,
            routing_realtime_initially_ready,
        ));
        tokio::spawn(async move {
            warm_raptor_timetables(
                pool,
                cache,
                snapshot_dir,
                snapshot_files_to_keep,
                warmup_status,
                pedestrian_router,
            )
            .await
        });
    }
    state
        .ticketing
        .start_refund_reconciliation(std::time::Duration::from_secs(
            env::var("CD_TICKET_REFUND_RECONCILE_SECONDS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(300),
        ));

    if let (Ok(email), Ok(password)) = (
        env::var("ADMIN_BOOTSTRAP_EMAIL"),
        env::var("ADMIN_BOOTSTRAP_PASSWORD"),
    ) && !email.is_empty()
        && !password.is_empty()
    {
        let user = if let Some(db) = &state.db {
            if let Some(existing) = user_by_email_db(db, &email).await? {
                existing
            } else {
                let created = create_user_record(
                    &email,
                    &password,
                    Some("Admin".to_string()),
                    vec!["admin".to_string(), "data_admin".to_string()],
                )?;
                let mut transaction = db.begin().await?;
                sqlx::query("INSERT INTO users(id,email,password_hash,display_name,created_at) VALUES($1,$2,$3,$4,$5)").bind(created.id).bind(&created.email).bind(&created.password_hash).bind(&created.display_name).bind(created.created_at).execute(&mut *transaction).await?;
                for role in &created.roles {
                    sqlx::query("INSERT INTO user_roles(user_id,role) VALUES($1,$2)")
                        .bind(created.id)
                        .bind(role)
                        .execute(&mut *transaction)
                        .await?;
                }
                sqlx::query("INSERT INTO user_profiles(user_id) VALUES($1)")
                    .bind(created.id)
                    .execute(&mut *transaction)
                    .await?;
                transaction.commit().await?;
                created
            }
        } else {
            create_user_record(
                &email,
                &password,
                Some("Admin".to_string()),
                vec!["admin".to_string(), "data_admin".to_string()],
            )?
        };
        state.users.write().await.insert(user.id, user);
    }
    Ok(state)
}

fn cd_private_key() -> anyhow::Result<Option<String>> {
    if let Ok(pem) = env::var("CD_TICKET_API_PRIVATE_KEY_PEM")
        && !pem.is_empty()
    {
        return Ok(Some(pem.replace("\\n", "\n")));
    }
    if let Ok(path) = env::var("CD_TICKET_API_PRIVATE_KEY_FILE")
        && !path.is_empty()
    {
        return Ok(Some(std::fs::read_to_string(path)?));
    }
    Ok(None)
}

fn build_router(state: AppState) -> Router {
    http::routes::build(state)
}

async fn admin_app() -> Html<&'static str> {
    Html(include_str!("../admin/index.html"))
}

async fn admin_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../admin/admin.css"),
    )
}

async fn admin_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        include_str!("../admin/admin.js"),
    )
}

async fn admin_entities(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({
        "entities": ADMIN_ENTITY_SPECS
            .iter()
            .map(|entity| json!({
                "key": entity.key,
                "label": entity.label,
                "map_available": entity.map_available
            }))
            .collect::<Vec<_>>()
    })))
}

async fn admin_entity_rows(
    Path(entity_key): Path<String>,
    Query(query): Query<AdminDataQuery>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let entity = ADMIN_ENTITY_SPECS
        .iter()
        .find(|entity| entity.key == entity_key)
        .ok_or_else(not_found)?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({
            "entity": entity.key,
            "label": entity.label,
            "rows": [],
            "pagination": {"page": 1, "page_size": 0, "total_rows": 0, "total_pages": 0},
            "database_available": false
        })));
    };

    let page = query.page.unwrap_or(1).max(1);
    let page_size = query
        .page_size
        .unwrap_or(ADMIN_DEFAULT_PAGE_SIZE)
        .clamp(1, ADMIN_MAX_PAGE_SIZE);
    let offset = (page - 1).saturating_mul(page_size);
    let search = query.q.unwrap_or_default().trim().to_string();

    let (total_rows, rows) = if search.is_empty() {
        let count_sql = format!("SELECT COUNT(*) FROM {}", entity.table);
        let total_rows = sqlx::query_scalar::<_, i64>(&count_sql)
            .fetch_one(pool)
            .await
            .map_err(internal_error)?;
        let rows_sql = format!(
            "SELECT {} AS row FROM {} t ORDER BY {} LIMIT $1 OFFSET $2",
            entity.row_expression, entity.table, entity.order_by
        );
        let rows = sqlx::query(&rows_sql)
            .bind(page_size as i64)
            .bind(offset as i64)
            .fetch_all(pool)
            .await
            .map_err(internal_error)?;
        (total_rows, rows)
    } else {
        let count_sql = format!(
            "SELECT COUNT(*) FROM {} t WHERE ({})::text ILIKE $1",
            entity.table, entity.row_expression
        );
        let rows_sql = format!(
            "SELECT {} AS row FROM {} t WHERE ({})::text ILIKE $1 ORDER BY {} LIMIT $2 OFFSET $3",
            entity.row_expression, entity.table, entity.row_expression, entity.order_by
        );
        let search_pattern = format!("%{search}%");
        let total_rows = sqlx::query_scalar::<_, i64>(&count_sql)
            .bind(&search_pattern)
            .fetch_one(pool)
            .await
            .map_err(internal_error)?;
        let rows = sqlx::query(&rows_sql)
            .bind(&search_pattern)
            .bind(page_size as i64)
            .bind(offset as i64)
            .fetch_all(pool)
            .await
            .map_err(internal_error)?;
        (total_rows, rows)
    };

    let total_pages = if total_rows == 0 {
        0
    } else {
        (total_rows as usize).div_ceil(page_size)
    };
    Ok(Json(json!({
        "entity": entity.key,
        "label": entity.label,
        "rows": rows
            .into_iter()
            .map(|row| row.get::<Value, _>("row"))
            .collect::<Vec<_>>(),
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_rows": total_rows,
            "total_pages": total_pages
        },
        "database_available": true
    })))
}

async fn admin_related_data(
    Path((entity, id)): Path<(String, String)>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({
            "database_available": false,
            "entity": entity,
            "id": id,
            "sections": []
        })));
    };

    let payload = match entity.as_str() {
        "stops" => admin_stop_related_data(pool, &id).await,
        "routes" => admin_route_related_data(pool, &id).await,
        "trips" => admin_trip_related_data(pool, &id).await,
        _ => {
            return Ok(Json(json!({
                "database_available": true,
                "supported": false,
                "entity": entity,
                "id": id,
                "sections": []
            })));
        }
    }
    .map_err(internal_error)?
    .ok_or_else(not_found)?;

    Ok(Json(payload))
}

async fn admin_stop_related_data(pool: &PgPool, id: &str) -> Result<Option<Value>, sqlx::Error> {
    let record =
        sqlx::query_scalar::<_, Value>("SELECT to_jsonb(stops) - 'geom' FROM stops WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    let Some(record) = record else {
        return Ok(None);
    };

    let station_stops_sql = r#"
        WITH selected AS (
          SELECT id, stop_area_id, normalized_name, lat, lon
          FROM stops
          WHERE id = $1
        )
        SELECT s.id, s.name, s.municipality, s.platform_code, s.modes,
               s.coordinate_confidence, s.source_feed_id, s.is_active
        FROM stops s
        CROSS JOIN selected
        WHERE s.id = selected.id
           OR (
             selected.stop_area_id IS NOT NULL
             AND s.stop_area_id = selected.stop_area_id
           )
           OR (
             selected.stop_area_id IS NULL
             AND selected.lat IS NOT NULL
             AND selected.lon IS NOT NULL
             AND s.normalized_name = selected.normalized_name
             AND s.lat IS NOT NULL
             AND s.lon IS NOT NULL
             AND abs(s.lat - selected.lat) < 0.00005
             AND abs(s.lon - selected.lon) < 0.00005
           )
        ORDER BY s.name ASC, s.platform_code ASC NULLS FIRST, s.id ASC
    "#;
    let station_stop_rows = sqlx::query(station_stops_sql)
        .bind(id)
        .fetch_all(pool)
        .await?;
    let station_stop_ids = station_stop_rows
        .iter()
        .map(|row| row.get::<String, _>("id"))
        .collect::<Vec<_>>();
    let station_stops = station_stop_rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<String, _>("id"),
                "name": row.get::<String, _>("name"),
                "municipality": row.get::<Option<String>, _>("municipality"),
                "platform_code": row.get::<Option<String>, _>("platform_code"),
                "modes": row.get::<Vec<String>, _>("modes"),
                "coordinate_confidence": row.get::<String, _>("coordinate_confidence"),
                "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
                "is_active": row.get::<bool, _>("is_active")
            })
        })
        .collect::<Vec<_>>();

    let route_rows = sqlx::query(
        r#"
        SELECT r.id, r.source_feed_id, r.source_id, r.agency_id, r.operator_id,
               r.short_name, r.long_name, r.mode, r.gtfs_route_type, r.color,
               r.text_color, r.source_priority, r.is_active,
               COUNT(DISTINCT t.id) AS trip_count,
               MIN(st.arrival_time) AS first_service_time,
               MAX(st.departure_time) AS last_service_time
        FROM stop_times st
        JOIN trips t ON t.id = st.trip_id
        JOIN routes r ON r.id = t.route_id
        WHERE st.stop_id = ANY($1)
          AND r.is_active = true
        GROUP BY r.id
        ORDER BY r.source_priority ASC, r.short_name ASC NULLS LAST, r.long_name ASC NULLS LAST, r.id ASC
        LIMIT 1000
        "#,
    )
    .bind(&station_stop_ids)
    .fetch_all(pool)
    .await?;
    let routes = route_rows
        .into_iter()
        .map(|row| {
            let mut route = route_row_json(&row);
            route["trip_count"] = json!(row.get::<i64, _>("trip_count"));
            route["first_service_time"] = json!(row.get::<Option<i32>, _>("first_service_time"));
            route["last_service_time"] = json!(row.get::<Option<i32>, _>("last_service_time"));
            route
        })
        .collect::<Vec<_>>();

    let trip_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT st.trip_id)
        FROM stop_times st
        JOIN trips t ON t.id = st.trip_id
        JOIN routes r ON r.id = t.route_id
        WHERE st.stop_id = ANY($1)
          AND r.is_active = true
        "#,
    )
    .bind(&station_stop_ids)
    .fetch_one(pool)
    .await?;
    let trip_rows = sqlx::query(
        r#"
        SELECT t.id, t.route_id, t.service_id, t.headsign, t.direction_id,
               t.source_feed_id, r.short_name, r.long_name, r.mode,
               MIN(st.arrival_time) AS arrival_time,
               MIN(st.departure_time) AS departure_time,
               MIN(st.platform) AS platform
        FROM stop_times st
        JOIN trips t ON t.id = st.trip_id
        JOIN routes r ON r.id = t.route_id
        WHERE st.stop_id = ANY($1)
          AND r.is_active = true
        GROUP BY t.id, r.id
        ORDER BY MIN(st.departure_time) ASC, r.short_name ASC NULLS LAST, t.id ASC
        LIMIT 250
        "#,
    )
    .bind(&station_stop_ids)
    .fetch_all(pool)
    .await?;
    let trips = trip_rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<String, _>("id"),
                "route_id": row.get::<String, _>("route_id"),
                "route_name": row.get::<Option<String>, _>("short_name")
                    .or_else(|| row.get::<Option<String>, _>("long_name")),
                "mode": row.get::<String, _>("mode"),
                "headsign": row.get::<Option<String>, _>("headsign"),
                "service_id": row.get::<String, _>("service_id"),
                "direction_id": row.get::<Option<i16>, _>("direction_id"),
                "arrival_time": row.get::<Option<i32>, _>("arrival_time"),
                "departure_time": row.get::<Option<i32>, _>("departure_time"),
                "platform": row.get::<Option<String>, _>("platform"),
                "source_feed_id": row.get::<Option<String>, _>("source_feed_id")
            })
        })
        .collect::<Vec<_>>();

    Ok(Some(json!({
        "database_available": true,
        "supported": true,
        "entity": "stops",
        "id": id,
        "record": record,
        "summary": [
            {"label": "Station stops", "value": station_stops.len()},
            {"label": "Routes", "value": routes.len()},
            {"label": "Trips", "value": trip_count}
        ],
        "sections": [
            {
                "key": "routes",
                "label": "Routes through this stop",
                "description": "Routes serving this station, including equivalent platform records.",
                "entity": "routes",
                "id_field": "id",
                "columns": ["short_name", "long_name", "mode", "trip_count", "first_service_time", "last_service_time"],
                "rows": routes,
                "total": routes.len(),
                "truncated": routes.len() == 1000
            },
            {
                "key": "trips",
                "label": "Trips serving this stop",
                "description": "First 250 scheduled trips ordered by time at this station.",
                "entity": "trips",
                "id_field": "id",
                "columns": ["departure_time", "route_name", "headsign", "platform", "service_id"],
                "rows": trips,
                "total": trip_count,
                "truncated": trip_count > trips.len() as i64
            },
            {
                "key": "station_stops",
                "label": "Station and platform records",
                "description": "Stop records grouped by stop area or matching station coordinates.",
                "entity": "stops",
                "id_field": "id",
                "columns": ["name", "platform_code", "modes", "coordinate_confidence", "source_feed_id"],
                "rows": station_stops,
                "total": station_stops.len(),
                "truncated": false
            }
        ]
    })))
}

async fn admin_trip_related_data(pool: &PgPool, id: &str) -> Result<Option<Value>, sqlx::Error> {
    let trip_row = sqlx::query(
        r#"
        SELECT id, source_feed_id, source_id, route_id, service_id, headsign,
               direction_id, shape_id, restrictions, raw_source_metadata, source_priority
        FROM trips
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    let Some(trip_row) = trip_row else {
        return Ok(None);
    };
    let record = trip_row_json(&trip_row);
    let route_id = trip_row.get::<String, _>("route_id");
    let service_id = trip_row.get::<String, _>("service_id");

    let route = sqlx::query(
        r#"
        SELECT id, source_feed_id, source_id, agency_id, operator_id, short_name, long_name,
               mode, gtfs_route_type, color, text_color, source_priority, is_active
        FROM routes
        WHERE id = $1
        "#,
    )
    .bind(&route_id)
    .fetch_optional(pool)
    .await?
    .map(|row| route_row_json(&row));

    let stop_rows = sqlx::query(
        r#"
        SELECT st.trip_id, st.stop_id, st.stop_sequence, st.arrival_time, st.departure_time,
               st.pickup_type, st.drop_off_type, st.timepoint, st.stop_headsign,
               st.platform, st.raw_notes,
               st.source_feed_id, st.source_priority,
               s.name AS stop_name, s.municipality, s.platform_code, s.stop_area_id
        FROM stop_times st
        JOIN stops s ON s.id = st.stop_id
        WHERE st.trip_id = $1
        ORDER BY st.stop_sequence ASC
        "#,
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    let stops = stop_rows
        .into_iter()
        .map(|row| {
            let mut stop_time = stop_time_row_json(&row);
            stop_time["stop_name"] = json!(row.get::<String, _>("stop_name"));
            stop_time["municipality"] = json!(row.get::<Option<String>, _>("municipality"));
            stop_time["platform_code"] = json!(row.get::<Option<String>, _>("platform_code"));
            stop_time["stop_area_id"] = json!(row.get::<Option<String>, _>("stop_area_id"));
            stop_time
        })
        .collect::<Vec<_>>();

    let calendar = sqlx::query_scalar::<_, Value>(
        "SELECT to_jsonb(calendars) FROM calendars WHERE service_id = $1",
    )
    .bind(&service_id)
    .fetch_optional(pool)
    .await?;
    let calendar_dates = sqlx::query_scalar::<_, Value>(
        r#"
        SELECT to_jsonb(calendar_dates)
        FROM calendar_dates
        WHERE service_id = $1
        ORDER BY date ASC
        LIMIT 200
        "#,
    )
    .bind(&service_id)
    .fetch_all(pool)
    .await?;

    Ok(Some(json!({
        "database_available": true,
        "supported": true,
        "entity": "trips",
        "id": id,
        "record": record,
        "summary": [
            {"label": "Stops", "value": stops.len()},
            {"label": "Route", "value": route.as_ref().and_then(|value| value.get("short_name")).cloned().unwrap_or(json!(route_id))},
            {"label": "Service", "value": service_id}
        ],
        "sections": [
            {
                "key": "stop_sequence",
                "label": "Stop sequence",
                "description": "Complete ordered calling pattern for this trip.",
                "entity": "stops",
                "id_field": "stop_id",
                "columns": ["stop_sequence", "arrival_time", "departure_time", "stop_name", "platform"],
                "rows": stops,
                "total": stops.len(),
                "truncated": false,
                "display": "timeline"
            },
            {
                "key": "route",
                "label": "Route",
                "description": "The route used by this trip.",
                "entity": "routes",
                "id_field": "id",
                "columns": ["short_name", "long_name", "mode", "source_feed_id"],
                "rows": route.into_iter().collect::<Vec<_>>(),
                "total": 1,
                "truncated": false
            },
            {
                "key": "service",
                "label": "Service calendar",
                "description": "Regular calendar and date-specific exceptions for this trip.",
                "entity": null,
                "id_field": null,
                "columns": [],
                "rows": [],
                "total": calendar_dates.len() + usize::from(calendar.is_some()),
                "truncated": calendar_dates.len() == 200,
                "display": "calendar",
                "calendar": calendar,
                "calendar_dates": calendar_dates
            }
        ]
    })))
}

async fn admin_route_related_data(pool: &PgPool, id: &str) -> Result<Option<Value>, sqlx::Error> {
    let route_row = sqlx::query(
        r#"
        SELECT id, source_feed_id, source_id, agency_id, operator_id, short_name, long_name,
               mode, gtfs_route_type, color, text_color, source_priority, is_active
        FROM routes
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    let Some(route_row) = route_row else {
        return Ok(None);
    };
    let record = route_row_json(&route_row);

    let trip_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM trips WHERE route_id = $1")
        .bind(id)
        .fetch_one(pool)
        .await?;
    let trip_rows = sqlx::query(
        r#"
        SELECT t.id, t.source_feed_id, t.source_id, t.route_id, t.service_id, t.headsign,
               t.direction_id, t.shape_id, t.restrictions, t.raw_source_metadata,
               t.source_priority, COUNT(st.stop_id) AS stop_count,
               MIN(st.departure_time) AS departure_time,
               MAX(st.arrival_time) AS arrival_time
        FROM trips t
        LEFT JOIN stop_times st ON st.trip_id = t.id
        WHERE t.route_id = $1
        GROUP BY t.id
        ORDER BY MIN(st.departure_time) ASC NULLS LAST, t.headsign ASC NULLS LAST, t.id ASC
        LIMIT 300
        "#,
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    let trips = trip_rows
        .into_iter()
        .map(|row| {
            let mut trip = trip_row_json(&row);
            trip["stop_count"] = json!(row.get::<i64, _>("stop_count"));
            trip["departure_time"] = json!(row.get::<Option<i32>, _>("departure_time"));
            trip["arrival_time"] = json!(row.get::<Option<i32>, _>("arrival_time"));
            trip
        })
        .collect::<Vec<_>>();

    let stop_rows = sqlx::query(
        r#"
        SELECT s.id, s.name, s.municipality, s.platform_code, s.modes,
               s.coordinate_confidence, s.source_feed_id,
               COUNT(DISTINCT st.trip_id) AS trip_count,
               MIN(st.stop_sequence) AS first_sequence
        FROM trips t
        JOIN stop_times st ON st.trip_id = t.id
        JOIN stops s ON s.id = st.stop_id
        WHERE t.route_id = $1
        GROUP BY s.id
        ORDER BY MIN(st.stop_sequence) ASC, s.name ASC, s.platform_code ASC NULLS FIRST
        LIMIT 1000
        "#,
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    let stops = stop_rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<String, _>("id"),
                "name": row.get::<String, _>("name"),
                "municipality": row.get::<Option<String>, _>("municipality"),
                "platform_code": row.get::<Option<String>, _>("platform_code"),
                "modes": row.get::<Vec<String>, _>("modes"),
                "coordinate_confidence": row.get::<String, _>("coordinate_confidence"),
                "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
                "trip_count": row.get::<i64, _>("trip_count"),
                "first_sequence": row.get::<i32, _>("first_sequence")
            })
        })
        .collect::<Vec<_>>();

    Ok(Some(json!({
        "database_available": true,
        "supported": true,
        "entity": "routes",
        "id": id,
        "record": record,
        "summary": [
            {"label": "Trips", "value": trip_count},
            {"label": "Served stops", "value": stops.len()},
            {"label": "Mode", "value": record.get("mode").cloned().unwrap_or(Value::Null)}
        ],
        "sections": [
            {
                "key": "trips",
                "label": "Trips on this route",
                "description": "First 300 trips ordered by their first departure.",
                "entity": "trips",
                "id_field": "id",
                "columns": ["departure_time", "arrival_time", "headsign", "service_id", "stop_count"],
                "rows": trips,
                "total": trip_count,
                "truncated": trip_count > trips.len() as i64
            },
            {
                "key": "stops",
                "label": "Stops served",
                "description": "Distinct stops served by trips assigned to this route.",
                "entity": "stops",
                "id_field": "id",
                "columns": ["first_sequence", "name", "municipality", "platform_code", "trip_count"],
                "rows": stops,
                "total": stops.len(),
                "truncated": stops.len() == 1000
            }
        ]
    })))
}

async fn admin_map_stops(
    Query(query): Query<AdminMapQuery>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({
            "stops": [],
            "database_available": false,
            "truncated": false
        })));
    };

    let search = query.q.unwrap_or_default().trim().to_string();
    let search_pattern = format!("%{search}%");
    let source_feed_id = query
        .source_feed_id
        .filter(|value| !value.trim().is_empty());
    let limit = query
        .limit
        .unwrap_or(ADMIN_MAX_MAP_STOPS)
        .clamp(1, ADMIN_MAX_MAP_STOPS);
    let rows = sqlx::query(
        r#"
        SELECT id, source_feed_id, name, normalized_name, municipality, region,
               lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
               platform_code, modes, source_priority
        FROM stops
        WHERE is_active = true
          AND lat IS NOT NULL
          AND lon IS NOT NULL
          AND ($1::text IS NULL OR source_feed_id = $1)
          AND (
            $2 = ''
            OR id ILIKE $3
            OR name ILIKE $3
            OR normalized_name ILIKE $3
            OR municipality ILIKE $3
          )
          AND ($4::double precision IS NULL OR lat >= $4)
          AND ($5::double precision IS NULL OR lon >= $5)
          AND ($6::double precision IS NULL OR lat <= $6)
          AND ($7::double precision IS NULL OR lon <= $7)
        ORDER BY source_priority ASC, name ASC, platform_code ASC NULLS FIRST
        LIMIT $8
        "#,
    )
    .bind(source_feed_id)
    .bind(&search)
    .bind(&search_pattern)
    .bind(query.min_lat)
    .bind(query.min_lon)
    .bind(query.max_lat)
    .bind(query.max_lon)
    .bind(limit as i64)
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;

    let stops = rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<String, _>("id"),
                "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
                "name": row.get::<String, _>("name"),
                "normalized_name": row.get::<String, _>("normalized_name"),
                "municipality": row.get::<Option<String>, _>("municipality"),
                "region": row.get::<Option<String>, _>("region"),
                "lat": row.get::<f64, _>("lat"),
                "lon": row.get::<f64, _>("lon"),
                "coordinate_confidence": row.get::<String, _>("coordinate_confidence"),
                "coordinate_source": row.get::<Option<String>, _>("coordinate_source"),
                "stop_area_id": row.get::<Option<String>, _>("stop_area_id"),
                "platform_code": row.get::<Option<String>, _>("platform_code"),
                "modes": row.get::<Vec<String>, _>("modes"),
                "source_priority": row.get::<i32, _>("source_priority")
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "truncated": stops.len() == limit,
        "limit": limit,
        "stops": stops,
        "database_available": true
    })))
}

async fn admin_imports(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({"imports": [], "database_available": false})));
    };
    let rows = sqlx::query(
        r#"
        SELECT id, source, status, started_at, finished_at, summary
        FROM import_runs
        ORDER BY started_at DESC
        LIMIT 200
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;
    Ok(Json(json!({
        "imports": rows.into_iter().map(import_run_row_json).collect::<Vec<_>>(),
        "database_available": true
    })))
}

async fn admin_import(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let import_id = Uuid::parse_str(&id).map_err(|_| not_found())?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({"id": id, "database_available": false})));
    };
    let row = sqlx::query(
        r#"
        SELECT id, source, status, started_at, finished_at, summary
        FROM import_runs
        WHERE id = $1
        "#,
    )
    .bind(import_id)
    .fetch_optional(pool)
    .await
    .map_err(internal_error)?
    .ok_or_else(not_found)?;
    let issue_rows = sqlx::query(
        r#"
        SELECT id, source_feed_id, severity, code, message, source_file,
               affected_entity, raw_payload, created_at
        FROM validation_issues
        WHERE import_run_id = $1
        ORDER BY created_at DESC, id DESC
        LIMIT 500
        "#,
    )
    .bind(import_id)
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;
    Ok(Json(json!({
        "import": import_run_row_json(row),
        "validation_issues": issue_rows.into_iter().map(validation_issue_row_json).collect::<Vec<_>>()
    })))
}

async fn admin_import_latest(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({"latest": null, "database_available": false})));
    };
    let row = sqlx::query(
        r#"
        SELECT id, source, status, started_at, finished_at, summary
        FROM import_runs
        ORDER BY started_at DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await
    .map_err(internal_error)?;
    Ok(Json(json!({
        "latest": row.map(import_run_row_json),
        "database_available": true
    })))
}

async fn admin_import_start(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({
        "status": "accepted",
        "command": "cargo run -p data-pipeline -- sync-pid",
        "warning": "API does not run synchronization inline; use the schedule updater or a worker/job runner"
    })))
}

async fn admin_database_stats(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        let routing_snapshots = routing_snapshot_status(
            None,
            &state.raptor_cache,
            &state.config.routing_snapshot_dir,
            &state.routing_warmup_status,
        )
        .await;
        return Ok(Json(json!({
            "database_available": false,
            "mock": state.use_mock_data,
            "warning": "database is not configured; set USE_MOCK_DATA=false and DATABASE_URL",
            "routing_snapshots": routing_snapshots
        })));
    };

    let (database_stats, routing_snapshots) = tokio::join!(
        database_admin_stats(pool),
        routing_snapshot_status(
            Some(pool),
            &state.raptor_cache,
            &state.config.routing_snapshot_dir,
            &state.routing_warmup_status,
        )
    );
    let mut database_stats = database_stats.map_err(internal_error)?;
    database_stats["storage"]["routing_snapshot_bytes"] =
        routing_snapshots["total_size_bytes"].clone();
    database_stats["routing_snapshots"] = routing_snapshots;
    Ok(Json(database_stats))
}

async fn admin_data_quality(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({
            "database_available": false,
            "mock": state.use_mock_data
        })));
    };

    let severity_rows = sqlx::query(
        r#"
        SELECT severity, COUNT(*) AS count
        FROM validation_issues
        WHERE code <> 'database_validation_completed'
        GROUP BY severity
        ORDER BY severity
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;
    let code_rows = sqlx::query(
        r#"
        SELECT code, severity, COUNT(*) AS count
        FROM validation_issues
        WHERE code <> 'database_validation_completed'
        GROUP BY code, severity
        ORDER BY count DESC, code ASC
        LIMIT 100
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;
    let latest_issue_rows = sqlx::query(
        r#"
        SELECT id, import_run_id, source_feed_id, severity, code, message,
               source_file, affected_entity, raw_payload, created_at
        FROM validation_issues
        WHERE code <> 'database_validation_completed'
        ORDER BY created_at DESC, id DESC
        LIMIT 100
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;
    let unresolved_stops: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM stops
        WHERE is_active = true
          AND (lat IS NULL OR lon IS NULL OR coordinate_confidence = 'unresolved')
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(internal_error)?;
    let duplicate_groups: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM (
          SELECT normalized_name, round(lat::numeric, 5), round(lon::numeric, 5)
          FROM stops
          WHERE is_active = true AND lat IS NOT NULL AND lon IS NOT NULL
          GROUP BY normalized_name, round(lat::numeric, 5), round(lon::numeric, 5)
          HAVING COUNT(*) > 1
        ) duplicates
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(internal_error)?;
    let last_database_validation = sqlx::query_scalar::<_, Option<Value>>(
        r#"
        SELECT raw_payload
        FROM validation_issues
        WHERE source_file = $1
          AND code = 'database_validation_completed'
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        "#,
    )
    .bind(ADMIN_VALIDATION_SOURCE_FILE)
    .fetch_optional(pool)
    .await
    .map_err(internal_error)?
    .flatten();

    Ok(Json(json!({
        "database_available": true,
        "validation_issue_counts": severity_rows.into_iter().map(|row| json!({
            "severity": row.get::<String, _>("severity"),
            "count": row.get::<i64, _>("count")
        })).collect::<Vec<_>>(),
        "issue_codes": code_rows.into_iter().map(|row| json!({
            "code": row.get::<String, _>("code"),
            "severity": row.get::<String, _>("severity"),
            "count": row.get::<i64, _>("count")
        })).collect::<Vec<_>>(),
        "unresolved_stops": unresolved_stops,
        "duplicate_stop_groups": duplicate_groups,
        "last_database_validation": last_database_validation,
        "latest_issues": latest_issue_rows.into_iter().map(validation_issue_row_json).collect::<Vec<_>>()
    })))
}

async fn admin_run_data_validation(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({
            "database_available": false,
            "message": "Database validation requires a configured transport database"
        })));
    };

    let validation_run_id = Uuid::new_v4();
    let started_at = Utc::now();
    let mut transaction = pool.begin().await.map_err(internal_error)?;
    sqlx::query("DELETE FROM validation_issues WHERE source_file = $1")
        .bind(ADMIN_VALIDATION_SOURCE_FILE)
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;

    let mut results = Vec::with_capacity(DATA_VALIDATION_CHECKS.len());
    let mut affected_records = 0_i64;
    let mut failed_checks = 0_usize;

    for check in DATA_VALIDATION_CHECKS {
        let query = format!(
            r#"
            WITH invalid AS (
              SELECT ({})::text AS identifier
              FROM {}
              WHERE {}
            ),
            samples AS (
              SELECT identifier
              FROM invalid
              ORDER BY identifier
              LIMIT 20
            )
            SELECT
              (SELECT COUNT(*) FROM invalid) AS count,
              COALESCE((SELECT array_agg(identifier) FROM samples), ARRAY[]::text[]) AS sample_ids
            "#,
            check.id_expression, check.table, check.predicate
        );
        let row = sqlx::query(&query)
            .fetch_one(&mut *transaction)
            .await
            .map_err(internal_error)?;
        let count = row.get::<i64, _>("count");
        let sample_ids = row.get::<Vec<String>, _>("sample_ids");
        let status = if count == 0 { "passed" } else { "failed" };
        let result = json!({
            "code": check.code,
            "severity": check.severity,
            "entity": check.entity,
            "description": check.description,
            "status": status,
            "count": count,
            "sample_ids": sample_ids
        });

        if count > 0 {
            affected_records += count;
            failed_checks += 1;
            sqlx::query(
                r#"
                INSERT INTO validation_issues (
                  import_run_id, source_feed_id, severity, code, message,
                  source_file, affected_entity, raw_payload
                )
                VALUES (NULL, NULL, $1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(check.severity)
            .bind(check.code)
            .bind(format!("{count} records failed: {}", check.description))
            .bind(ADMIN_VALIDATION_SOURCE_FILE)
            .bind(check.entity)
            .bind(&result)
            .execute(&mut *transaction)
            .await
            .map_err(internal_error)?;
        }
        results.push(result);
    }

    let finished_at = Utc::now();
    let summary = json!({
        "validation_run_id": validation_run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "checks_total": DATA_VALIDATION_CHECKS.len(),
        "checks_passed": DATA_VALIDATION_CHECKS.len() - failed_checks,
        "checks_failed": failed_checks,
        "affected_records": affected_records,
        "results": results
    });
    sqlx::query(
        r#"
        INSERT INTO validation_issues (
          import_run_id, source_feed_id, severity, code, message,
          source_file, affected_entity, raw_payload
        )
        VALUES (NULL, NULL, 'info', 'database_validation_completed', $1, $2, 'database', $3)
        "#,
    )
    .bind(format!(
        "Database validation completed with {failed_checks} failed checks and {affected_records} affected records"
    ))
    .bind(ADMIN_VALIDATION_SOURCE_FILE)
    .bind(&summary)
    .execute(&mut *transaction)
    .await
    .map_err(internal_error)?;
    transaction.commit().await.map_err(internal_error)?;

    Ok(Json(json!({
        "database_available": true,
        "validation": summary
    })))
}

async fn admin_data_repairs(
    Query(query): Query<AdminRepairQuery>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({
            "database_available": false,
            "safe_repairs": [],
            "duplicate_groups": [],
            "nearby_direction_groups": [],
            "recent_runs": []
        })));
    };
    let limit = query.limit.unwrap_or(25).clamp(1, 100);
    let offset = query.offset.unwrap_or(0).max(0);
    let repairable = sqlx::query(
        r#"
        WITH exact_city_matches AS (
          SELECT stop.id
          FROM stops AS stop
          JOIN cities AS city
            ON city.country_code = 'CZ'
           AND city.normalized_name = trim(
             regexp_replace(lower(unaccent(stop.municipality)), '[^a-z0-9]+', ' ', 'g')
           )
          WHERE stop.is_active = true
            AND stop.city_id IS NULL
            AND COALESCE(btrim(stop.municipality), '') <> ''
          GROUP BY stop.id
          HAVING count(*) = 1
        ), exact_duplicate_groups AS (
          SELECT array_agg(stop.id) AS stop_ids
          FROM stops AS stop
          WHERE stop.is_active = true
            AND stop.source_feed_id IS NOT NULL
            AND stop.lat IS NOT NULL
            AND stop.lon IS NOT NULL
            AND btrim(stop.name) <> ''
            AND btrim(stop.normalized_name) <> ''
            AND stop.location_type IN ('stop', 'station')
          GROUP BY stop.normalized_name,
                   round(stop.lat::numeric, 5), round(stop.lon::numeric, 5)
          HAVING count(*) > 1
             AND count(DISTINCT stop.source_feed_id) = count(*)
             AND count(DISTINCT COALESCE(stop.platform_code, '')) = 1
             AND count(DISTINCT stop.location_type) = 1
             AND count(DISTINCT COALESCE(stop.parent_station_id, '')) = 1
             AND count(DISTINCT array_to_string(stop.modes, ',')) = 1
             AND count(DISTINCT COALESCE(stop.city_id, '')) = 1
        ), safe_duplicate_groups AS (
          SELECT duplicate_group.stop_ids
          FROM exact_duplicate_groups AS duplicate_group
          WHERE NOT EXISTS (
            SELECT 1
            FROM stop_times AS call
            WHERE call.stop_id = ANY(duplicate_group.stop_ids)
            GROUP BY call.trip_id
            HAVING count(DISTINCT call.stop_id) > 1
          )
        )
        SELECT
          (SELECT count(*) FROM stops
           WHERE btrim(name) <> '' AND btrim(normalized_name) = '') AS normalized_stop_names,
          (SELECT count(*) FROM exact_city_matches) AS exact_city_assignments,
          (SELECT count(*) FROM realtime_updates
           WHERE valid_until IS NOT NULL AND valid_until < fetched_at) AS realtime_validity,
          COALESCE((SELECT sum(cardinality(stop_ids) - 1)
                    FROM safe_duplicate_groups), 0)::bigint AS automatic_stop_merges
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(internal_error)?;
    let duplicate_rows = sqlx::query(
        r#"
        WITH duplicate_keys AS (
          SELECT
            normalized_name,
            round(lat::numeric, 5) AS lat_key,
            round(lon::numeric, 5) AS lon_key,
            count(*) AS stop_count
          FROM stops
          WHERE is_active = true
            AND lat IS NOT NULL
            AND lon IS NOT NULL
            AND btrim(normalized_name) <> ''
          GROUP BY normalized_name, round(lat::numeric, 5), round(lon::numeric, 5)
          HAVING count(*) > 1
          ORDER BY count(*) DESC, normalized_name ASC
          LIMIT $1
          OFFSET $2
        ),
        stop_details AS (
          SELECT
            stop.*,
            (SELECT count(*) FROM stop_times WHERE stop_id = stop.id) AS stop_time_count,
            COALESCE((
              SELECT jsonb_agg(
                jsonb_build_object(
                  'source_feed_id', source_id.source_feed_id,
                  'original_source_id', source_id.original_source_id,
                  'priority', source_id.priority
                ) ORDER BY source_id.priority, source_id.source_feed_id
              )
              FROM stop_source_ids AS source_id
              WHERE source_id.stop_id = stop.id
            ), '[]'::jsonb) AS retained_source_ids
          FROM duplicate_keys AS key
          JOIN stops AS stop
            ON stop.is_active = true
           AND stop.normalized_name = key.normalized_name
           AND round(stop.lat::numeric, 5) = key.lat_key
           AND round(stop.lon::numeric, 5) = key.lon_key
        )
        SELECT
          key.normalized_name,
          key.lat_key::double precision AS latitude,
          key.lon_key::double precision AS longitude,
          key.stop_count,
          (array_agg(stop.id ORDER BY stop.source_priority ASC, stop.id ASC))[1]
            AS suggested_canonical_stop_id,
          count(DISTINCT stop.source_feed_id) = count(*)
            AND count(DISTINCT COALESCE(stop.platform_code, '')) = 1
            AND count(DISTINCT stop.location_type) = 1
            AND count(DISTINCT COALESCE(stop.parent_station_id, '')) = 1
            AND count(DISTINCT array_to_string(stop.modes, ',')) = 1
            AND count(DISTINCT COALESCE(stop.city_id, '')) = 1
            AS high_confidence_candidate,
          jsonb_agg(
            jsonb_build_object(
              'id', stop.id,
              'name', stop.name,
              'municipality', stop.municipality,
              'platform_code', stop.platform_code,
              'location_type', stop.location_type,
              'parent_station_id', stop.parent_station_id,
              'modes', stop.modes,
              'source_feed_id', stop.source_feed_id,
              'source_priority', stop.source_priority,
              'stop_times', stop.stop_time_count,
              'source_ids', stop.retained_source_ids
            ) ORDER BY stop.source_priority ASC, stop.id ASC
          ) AS stops
        FROM duplicate_keys AS key
        JOIN stop_details AS stop
          ON stop.normalized_name = key.normalized_name
         AND round(stop.lat::numeric, 5) = key.lat_key
         AND round(stop.lon::numeric, 5) = key.lon_key
        GROUP BY key.normalized_name, key.lat_key, key.lon_key, key.stop_count
        ORDER BY key.stop_count DESC, key.normalized_name ASC
        "#,
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;
    let nearby_direction_rows = sqlx::query(
        r#"
        WITH normalized_stops AS (
          SELECT
            stop.*,
            trim(regexp_replace(
              lower(unaccent(COALESCE(stop.municipality, ''))),
              '[^a-z0-9]+', ' ', 'g'
            )) AS municipality_key
          FROM stops AS stop
          WHERE stop.is_active = true
            AND stop.geom IS NOT NULL
            AND stop.lat IS NOT NULL
            AND stop.lon IS NOT NULL
            AND btrim(stop.name) <> ''
            AND btrim(stop.normalized_name) <> ''
            AND stop.location_type IN ('stop', 'station')
        ), physical_stops AS (
          SELECT
            normalized_stop.*,
            CASE
              WHEN municipality_key <> ''
               AND normalized_name LIKE municipality_key || ' %'
                THEN substr(normalized_name, char_length(municipality_key) + 2)
              ELSE normalized_name
            END AS public_name,
            COALESCE(city_id, NULLIF(municipality_key, ''), '') AS locality_key
          FROM normalized_stops AS normalized_stop
        ), base_pairs AS (
          SELECT
            left_stop.id AS left_stop_id,
            right_stop.id AS right_stop_id,
            left_stop.public_name,
            ST_Distance(left_stop.geom, right_stop.geom) AS distance_m
          FROM physical_stops AS left_stop
          JOIN physical_stops AS right_stop
            ON right_stop.id > left_stop.id
           AND right_stop.public_name = left_stop.public_name
           AND right_stop.locality_key = left_stop.locality_key
           AND ST_DWithin(left_stop.geom, right_stop.geom, 120)
          WHERE left_stop.public_name <> ''
            AND NOT (
              round(left_stop.lat::numeric, 5) = round(right_stop.lat::numeric, 5)
              AND round(left_stop.lon::numeric, 5) = round(right_stop.lon::numeric, 5)
            )
            AND NOT EXISTS (
              SELECT 1
              FROM stop_times AS left_call
              JOIN stop_times AS right_call
                ON right_call.trip_id = left_call.trip_id
              WHERE left_call.stop_id = left_stop.id
                AND right_call.stop_id = right_stop.id
            )
          ORDER BY distance_m ASC, left_stop.id ASC, right_stop.id ASC
          LIMIT 500
        ), candidate_stop_ids AS (
          SELECT left_stop_id AS stop_id FROM base_pairs
          UNION
          SELECT right_stop_id FROM base_pairs
        ), direction_counts AS (
          SELECT
            call.stop_id,
            mod(floor((degrees(ST_Azimuth(
              origin.geom::geometry,
              next_stop.geom::geometry
            )) + 22.5) / 45.0)::integer, 8) AS direction_bucket,
            count(*) AS sample_count
          FROM stop_times AS call
          JOIN candidate_stop_ids AS candidate ON candidate.stop_id = call.stop_id
          JOIN stops AS origin ON origin.id = call.stop_id
          JOIN LATERAL (
            SELECT destination.geom
            FROM stop_times AS next_call
            JOIN stops AS destination ON destination.id = next_call.stop_id
            WHERE next_call.trip_id = call.trip_id
              AND next_call.stop_sequence > call.stop_sequence
              AND destination.geom IS NOT NULL
            ORDER BY next_call.stop_sequence ASC
            LIMIT 1
          ) AS next_stop ON true
          WHERE origin.geom IS NOT NULL
            AND ST_Distance(origin.geom, next_stop.geom) > 5
          GROUP BY call.stop_id, direction_bucket
        ), ranked_directions AS (
          SELECT *, row_number() OVER (
            PARTITION BY stop_id
            ORDER BY sample_count DESC, direction_bucket ASC
          ) AS rank
          FROM direction_counts
        ), dominant_directions AS (
          SELECT stop_id, direction_bucket, sample_count
          FROM ranked_directions
          WHERE rank = 1
        ), compatible_pairs AS (
          SELECT
            pair.*,
            left_direction.direction_bucket AS left_direction_bucket,
            left_direction.sample_count AS left_direction_samples,
            right_direction.direction_bucket AS right_direction_bucket,
            right_direction.sample_count AS right_direction_samples
          FROM base_pairs AS pair
          JOIN dominant_directions AS left_direction
            ON left_direction.stop_id = pair.left_stop_id
          JOIN dominant_directions AS right_direction
            ON right_direction.stop_id = pair.right_stop_id
          WHERE least(
            abs(left_direction.direction_bucket - right_direction.direction_bucket),
            8 - abs(left_direction.direction_bucket - right_direction.direction_bucket)
          ) <= 1
          ORDER BY pair.distance_m ASC, pair.public_name ASC,
                   pair.left_stop_id ASC, pair.right_stop_id ASC
          LIMIT 50
        ), stop_usage AS (
          SELECT call.stop_id, count(*) AS stop_time_count
          FROM stop_times AS call
          WHERE call.stop_id IN (
            SELECT left_stop_id FROM compatible_pairs
            UNION
            SELECT right_stop_id FROM compatible_pairs
          )
          GROUP BY call.stop_id
        )
        SELECT
          pair.public_name AS normalized_name,
          (left_stop.lat + right_stop.lat) / 2.0 AS latitude,
          (left_stop.lon + right_stop.lon) / 2.0 AS longitude,
          pair.distance_m,
          CASE
            WHEN left_stop.source_priority < right_stop.source_priority THEN left_stop.id
            WHEN right_stop.source_priority < left_stop.source_priority THEN right_stop.id
            WHEN COALESCE(left_usage.stop_time_count, 0) >= COALESCE(right_usage.stop_time_count, 0)
              THEN left_stop.id
            ELSE right_stop.id
          END AS suggested_canonical_stop_id,
          pair.distance_m <= 30
            AND pair.left_direction_bucket = pair.right_direction_bucket
            AND left_stop.location_type = right_stop.location_type
            AS high_confidence_candidate,
          pair.left_direction_bucket = pair.right_direction_bucket
            AND left_stop.location_type = right_stop.location_type
            AS automatic_candidate,
          jsonb_build_array(
            jsonb_build_object(
              'id', left_stop.id,
              'name', left_stop.name,
              'municipality', left_stop.municipality,
              'platform_code', left_stop.platform_code,
              'location_type', left_stop.location_type,
              'parent_station_id', left_stop.parent_station_id,
              'modes', left_stop.modes,
              'source_feed_id', left_stop.source_feed_id,
              'source_priority', left_stop.source_priority,
              'stop_times', COALESCE(left_usage.stop_time_count, 0),
              'direction_bucket', pair.left_direction_bucket,
              'direction', (ARRAY['N','NE','E','SE','S','SW','W','NW'])[pair.left_direction_bucket + 1],
              'direction_samples', pair.left_direction_samples,
              'source_ids', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                  'source_feed_id', source_id.source_feed_id,
                  'original_source_id', source_id.original_source_id,
                  'priority', source_id.priority
                ) ORDER BY source_id.priority, source_id.source_feed_id)
                FROM stop_source_ids AS source_id
                WHERE source_id.stop_id = left_stop.id
              ), '[]'::jsonb)
            ),
            jsonb_build_object(
              'id', right_stop.id,
              'name', right_stop.name,
              'municipality', right_stop.municipality,
              'platform_code', right_stop.platform_code,
              'location_type', right_stop.location_type,
              'parent_station_id', right_stop.parent_station_id,
              'modes', right_stop.modes,
              'source_feed_id', right_stop.source_feed_id,
              'source_priority', right_stop.source_priority,
              'stop_times', COALESCE(right_usage.stop_time_count, 0),
              'direction_bucket', pair.right_direction_bucket,
              'direction', (ARRAY['N','NE','E','SE','S','SW','W','NW'])[pair.right_direction_bucket + 1],
              'direction_samples', pair.right_direction_samples,
              'source_ids', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                  'source_feed_id', source_id.source_feed_id,
                  'original_source_id', source_id.original_source_id,
                  'priority', source_id.priority
                ) ORDER BY source_id.priority, source_id.source_feed_id)
                FROM stop_source_ids AS source_id
                WHERE source_id.stop_id = right_stop.id
              ), '[]'::jsonb)
            )
          ) AS stops
        FROM compatible_pairs AS pair
        JOIN physical_stops AS left_stop ON left_stop.id = pair.left_stop_id
        JOIN physical_stops AS right_stop ON right_stop.id = pair.right_stop_id
        LEFT JOIN stop_usage AS left_usage ON left_usage.stop_id = left_stop.id
        LEFT JOIN stop_usage AS right_usage ON right_usage.stop_id = right_stop.id
        ORDER BY pair.distance_m ASC, pair.public_name ASC
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;
    let nearby_direction_groups = combine_nearby_direction_pairs(
        nearby_direction_rows
            .into_iter()
            .map(|row| {
                json!({
                    "normalized_name": row.get::<String, _>("normalized_name"),
                    "latitude": row.get::<f64, _>("latitude"),
                    "longitude": row.get::<f64, _>("longitude"),
                    "distance_m": row.get::<f64, _>("distance_m"),
                    "stop_count": 2,
                    "candidate_kind": "nearby_same_direction",
                    "merge_strategy": "nearby_same_direction",
                    "suggested_canonical_stop_id": row.get::<String, _>("suggested_canonical_stop_id"),
                    "high_confidence_candidate": row.get::<bool, _>("high_confidence_candidate"),
                    "automatic_candidate": row.get::<bool, _>("automatic_candidate"),
                    "stops": row.get::<Value, _>("stops")
                })
            })
            .collect(),
    );
    let automatic_nearby_stop_merges = nearby_direction_groups
        .iter()
        .filter(|group| group["automatic_candidate"].as_bool().unwrap_or(false))
        .map(|group| group["stop_count"].as_i64().unwrap_or(1).saturating_sub(1))
        .sum::<i64>();
    let recent_runs = sqlx::query_scalar::<_, Value>(
        r#"
        SELECT to_jsonb(run)
        FROM data_repair_runs AS run
        ORDER BY created_at DESC, id DESC
        LIMIT 20
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;

    Ok(Json(json!({
        "database_available": true,
        "duplicate_offset": offset,
        "duplicate_limit": limit,
        "safe_repairs": [
            {
                "code": "normalize_stop_names",
                "label": "Rebuild missing normalized stop names",
                "count": repairable.get::<i64, _>("normalized_stop_names"),
                "description": "Derives only the search form from an existing non-empty public name."
            },
            {
                "code": "assign_exact_stop_cities",
                "label": "Assign exact municipality matches",
                "count": repairable.get::<i64, _>("exact_city_assignments"),
                "description": "Assigns a city only when the stop municipality matches one Czech city exactly."
            },
            {
                "code": "correct_realtime_validity",
                "label": "Expire inconsistent realtime rows",
                "count": repairable.get::<i64, _>("realtime_validity"),
                "description": "Sets an impossible validity end to its fetch time, keeping the stale row out of live results."
            },
            {
                "code": "merge_exact_cross_feed_stops",
                "label": "Merge exact cross-feed stop aliases",
                "count": repairable.get::<i64, _>("automatic_stop_merges"),
                "description": "Automatically merges only different-feed records with the same name, coordinate, platform, type, modes and locality, unless one trip calls at both stops."
            },
            {
                "code": "merge_nearby_same_direction_stops",
                "label": "Merge nearby stops in the same direction",
                "count": automatic_nearby_stop_merges,
                "description": "Automatically merges same-name physical stops in one locality and direction when every record is within 120 metres of the selected canonical stop and no trip calls at two records."
            }
        ],
        "duplicate_groups": duplicate_rows.into_iter().map(|row| json!({
            "normalized_name": row.get::<String, _>("normalized_name"),
            "latitude": row.get::<f64, _>("latitude"),
            "longitude": row.get::<f64, _>("longitude"),
            "stop_count": row.get::<i64, _>("stop_count"),
            "suggested_canonical_stop_id": row.get::<String, _>("suggested_canonical_stop_id"),
            "high_confidence_candidate": row.get::<bool, _>("high_confidence_candidate"),
            "stops": row.get::<Value, _>("stops")
        })).collect::<Vec<_>>(),
        "nearby_direction_groups": nearby_direction_groups,
        "recent_runs": recent_runs
    })))
}

async fn admin_apply_safe_data_repairs(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(body): Json<AdminSafeRepairRequest>,
) -> Result<Json<Value>, ApiError> {
    let user = require_admin(&state, &headers).await?;
    if body.confirmation != "apply_safe_repairs" {
        return Err(ApiError {
            code: "validation_error".to_string(),
            message: "confirmation must be apply_safe_repairs".to_string(),
        });
    }
    let pool = state.db.as_ref().ok_or_else(|| ApiError {
        code: "database_unavailable".to_string(),
        message: "Data repair requires a configured transport database".to_string(),
    })?;
    let repair_run_id = Uuid::new_v4();
    let mut transaction = pool.begin().await.map_err(internal_error)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('cesta-data-repair'))")
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;
    sqlx::query(
        r#"
        INSERT INTO data_repair_runs (id, repair_type, status, requested_by)
        VALUES ($1, 'safe_automatic', 'running', $2)
        "#,
    )
    .bind(repair_run_id)
    .bind(user.id)
    .execute(&mut *transaction)
    .await
    .map_err(internal_error)?;
    let summary = sqlx::query_scalar::<_, Value>("SELECT cesta_apply_safe_data_repairs()")
        .fetch_one(&mut *transaction)
        .await
        .map_err(internal_error)?;
    sqlx::query(
        r#"
        UPDATE data_repair_runs
        SET status = 'completed', summary = $2, finished_at = now()
        WHERE id = $1
        "#,
    )
    .bind(repair_run_id)
    .bind(&summary)
    .execute(&mut *transaction)
    .await
    .map_err(internal_error)?;
    transaction.commit().await.map_err(internal_error)?;
    state.raptor_cache.write().await.clear();
    state.endpoint_access_cache.write().await.clear();

    Ok(Json(json!({
        "database_available": true,
        "repair_run_id": repair_run_id,
        "status": "completed",
        "summary": summary
    })))
}

async fn admin_merge_duplicate_stops(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(body): Json<AdminDuplicateStopMergeRequest>,
) -> Result<Json<Value>, ApiError> {
    let user = require_admin(&state, &headers).await?;
    let strategy = body.strategy.as_deref().unwrap_or("exact_coordinates");
    let canonical_stop_id = body.canonical_stop_id.trim();
    let duplicate_stop_ids = body
        .duplicate_stop_ids
        .iter()
        .map(|id| id.trim().to_string())
        .collect::<HashSet<_>>();
    if body.confirmation != "merge_duplicate_stops"
        || canonical_stop_id.is_empty()
        || duplicate_stop_ids.is_empty()
        || duplicate_stop_ids.len() > 25
        || duplicate_stop_ids.contains(canonical_stop_id)
        || duplicate_stop_ids.iter().any(|id| id.is_empty())
        || !matches!(strategy, "exact_coordinates" | "nearby_same_direction")
    {
        return Err(ApiError {
            code: "validation_error".to_string(),
            message: "Choose one canonical stop and between 1 and 25 distinct duplicate stops"
                .to_string(),
        });
    }
    let duplicate_stop_ids = duplicate_stop_ids.into_iter().collect::<Vec<_>>();
    let pool = state.db.as_ref().ok_or_else(|| ApiError {
        code: "database_unavailable".to_string(),
        message: "Duplicate repair requires a configured transport database".to_string(),
    })?;
    let mut selected_ids = duplicate_stop_ids.clone();
    selected_ids.push(canonical_stop_id.to_string());
    let mut transaction = pool.begin().await.map_err(internal_error)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('cesta-data-repair'))")
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;
    let selected_rows = sqlx::query(
        r#"
        SELECT id, name, normalized_name, municipality, city_id, location_type,
               lat, lon,
               round(lat::numeric, 5)::double precision AS lat_key,
               round(lon::numeric, 5)::double precision AS lon_key,
               is_active
        FROM stops
        WHERE id = ANY($1)
        FOR UPDATE
        "#,
    )
    .bind(&selected_ids)
    .fetch_all(&mut *transaction)
    .await
    .map_err(internal_error)?;
    if selected_rows.len() != selected_ids.len()
        || selected_rows
            .iter()
            .any(|row| !row.get::<bool, _>("is_active"))
    {
        return Err(ApiError {
            code: "validation_error".to_string(),
            message: "Every selected stop must exist and still be active".to_string(),
        });
    }
    let canonical = selected_rows
        .iter()
        .find(|row| row.get::<String, _>("id") == canonical_stop_id)
        .expect("selected canonical stop exists");
    let normalized_name = canonical.get::<String, _>("normalized_name");
    let latitude = canonical.get::<Option<f64>, _>("lat_key");
    let longitude = canonical.get::<Option<f64>, _>("lon_key");
    match strategy {
        "exact_coordinates" => {
            if normalized_name.trim().is_empty()
                || latitude.is_none()
                || longitude.is_none()
                || selected_rows.iter().any(|row| {
                    row.get::<String, _>("normalized_name") != normalized_name
                        || row.get::<Option<f64>, _>("lat_key") != latitude
                        || row.get::<Option<f64>, _>("lon_key") != longitude
                })
            {
                return Err(ApiError {
                    code: "unsafe_stop_merge".to_string(),
                    message: "Exact-coordinate merges require matching normalized names and coordinates rounded to five decimal places".to_string(),
                });
            }
        }
        "nearby_same_direction" => {
            let canonical_name = canonical_stop_name_parts(
                &canonical.get::<String, _>("name"),
                canonical
                    .get::<Option<String>, _>("municipality")
                    .as_deref(),
            );
            let canonical_city = canonical.get::<Option<String>, _>("city_id");
            let canonical_municipality = canonical
                .get::<Option<String>, _>("municipality")
                .as_deref()
                .map(normalize_search_text);
            let canonical_location_type = canonical.get::<String, _>("location_type");
            let canonical_position = canonical
                .get::<Option<f64>, _>("lat")
                .zip(canonical.get::<Option<f64>, _>("lon"));
            let incompatible_stop = selected_rows.iter().any(|row| {
                let municipality = row.get::<Option<String>, _>("municipality");
                let public_name =
                    canonical_stop_name_parts(&row.get::<String, _>("name"), municipality.as_deref());
                let municipality = municipality.as_deref().map(normalize_search_text);
                let city = row.get::<Option<String>, _>("city_id");
                let position = row
                    .get::<Option<f64>, _>("lat")
                    .zip(row.get::<Option<f64>, _>("lon"));
                public_name.is_empty()
                    || public_name != canonical_name
                    || row.get::<String, _>("location_type") != canonical_location_type
                    || matches!((&canonical_city, &city), (Some(left), Some(right)) if left != right)
                    || matches!((&canonical_municipality, &municipality), (Some(left), Some(right)) if left != right)
                    || !matches!((canonical_position, position), (Some((left_lat, left_lon)), Some((right_lat, right_lon))) if haversine_m(left_lat, left_lon, right_lat, right_lon) <= 120.0)
            });
            if incompatible_stop {
                return Err(ApiError {
                    code: "unsafe_stop_merge".to_string(),
                    message: "Nearby-direction merges require the same public name, locality and stop type within 120 metres".to_string(),
                });
            }

            let direction_rows = sqlx::query(
                r#"
                WITH direction_counts AS (
                  SELECT
                    call.stop_id,
                    mod(floor((degrees(ST_Azimuth(
                      origin.geom::geometry,
                      next_stop.geom::geometry
                    )) + 22.5) / 45.0)::integer, 8) AS direction_bucket,
                    count(*) AS sample_count
                  FROM stop_times AS call
                  JOIN stops AS origin ON origin.id = call.stop_id
                  JOIN LATERAL (
                    SELECT destination.geom
                    FROM stop_times AS next_call
                    JOIN stops AS destination ON destination.id = next_call.stop_id
                    WHERE next_call.trip_id = call.trip_id
                      AND next_call.stop_sequence > call.stop_sequence
                      AND destination.geom IS NOT NULL
                    ORDER BY next_call.stop_sequence ASC
                    LIMIT 1
                  ) AS next_stop ON true
                  WHERE call.stop_id = ANY($1)
                    AND origin.geom IS NOT NULL
                    AND ST_Distance(origin.geom, next_stop.geom) > 5
                  GROUP BY call.stop_id, direction_bucket
                ), ranked AS (
                  SELECT *, row_number() OVER (
                    PARTITION BY stop_id
                    ORDER BY sample_count DESC, direction_bucket ASC
                  ) AS rank
                  FROM direction_counts
                )
                SELECT selected_stop_id AS stop_id, ranked.direction_bucket
                FROM unnest($1::text[]) AS selected_stop_id
                LEFT JOIN ranked
                  ON ranked.stop_id = selected_stop_id AND ranked.rank = 1
                "#,
            )
            .bind(&selected_ids)
            .fetch_all(&mut *transaction)
            .await
            .map_err(internal_error)?;
            let directions = direction_rows
                .into_iter()
                .map(|row| {
                    (
                        row.get::<String, _>("stop_id"),
                        row.get::<Option<i32>, _>("direction_bucket"),
                    )
                })
                .collect::<HashMap<_, _>>();
            let canonical_direction = directions.get(canonical_stop_id).copied().flatten();
            if canonical_direction.is_none()
                || selected_ids.iter().any(|id| {
                    !directions
                        .get(id)
                        .copied()
                        .flatten()
                        .zip(canonical_direction)
                        .is_some_and(|(candidate, canonical)| {
                            direction_buckets_compatible(candidate, canonical)
                        })
                })
            {
                return Err(ApiError {
                    code: "unsafe_stop_merge".to_string(),
                    message: "Every nearby stop must have a compatible measured travel direction"
                        .to_string(),
                });
            }
        }
        _ => unreachable!("merge strategy was validated"),
    }
    let same_trip_conflict: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM stop_times
          WHERE stop_id = ANY($1)
          GROUP BY trip_id
          HAVING count(DISTINCT stop_id) > 1
        )
        "#,
    )
    .bind(&selected_ids)
    .fetch_one(&mut *transaction)
    .await
    .map_err(internal_error)?;
    if same_trip_conflict {
        return Err(ApiError {
            code: "unsafe_stop_merge".to_string(),
            message: "Stops used by the same trip cannot be merged because they are distinct calls"
                .to_string(),
        });
    }
    let conflicting_mapping: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM manual_stop_matches
          WHERE confidence = 'confirmed_duplicate'
            AND (
              stop_id = $1
              OR target_stop_id = ANY($2)
              OR (stop_id = ANY($2) AND target_stop_id IS DISTINCT FROM $1)
            )
        )
        "#,
    )
    .bind(canonical_stop_id)
    .bind(&duplicate_stop_ids)
    .fetch_one(&mut *transaction)
    .await
    .map_err(internal_error)?;
    if conflicting_mapping {
        return Err(ApiError {
            code: "conflicting_stop_merge".to_string(),
            message: "A selected stop already participates in a different confirmed merge"
                .to_string(),
        });
    }
    let note = body
        .note
        .as_deref()
        .map(str::trim)
        .filter(|note| !note.is_empty());
    sqlx::query(
        r#"
        INSERT INTO manual_stop_matches (
          stop_id, target_stop_id, confidence, note, created_by
        )
        SELECT duplicate_stop_id, $1, 'confirmed_duplicate', $3, $4
        FROM unnest($2::text[]) AS duplicate_stop_id
        WHERE NOT EXISTS (
          SELECT 1
          FROM manual_stop_matches AS existing
          WHERE existing.stop_id = duplicate_stop_id
            AND existing.target_stop_id = $1
            AND existing.confidence = 'confirmed_duplicate'
        )
        "#,
    )
    .bind(canonical_stop_id)
    .bind(&duplicate_stop_ids)
    .bind(note.unwrap_or("Confirmed from the duplicate-stop repair review"))
    .bind(user.id)
    .execute(&mut *transaction)
    .await
    .map_err(internal_error)?;
    let merge_summary =
        sqlx::query_scalar::<_, Value>("SELECT cesta_apply_confirmed_stop_merges()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(internal_error)?;
    let repair_run_id = Uuid::new_v4();
    let repair_summary = json!({
        "canonical_stop_id": canonical_stop_id,
        "duplicate_stop_ids": duplicate_stop_ids,
        "strategy": strategy,
        "merge_result": merge_summary
    });
    sqlx::query(
        r#"
        INSERT INTO data_repair_runs (
          id, repair_type, status, requested_by, summary, finished_at
        )
        VALUES ($1, 'confirmed_duplicate_merge', 'completed', $2, $3, now())
        "#,
    )
    .bind(repair_run_id)
    .bind(user.id)
    .bind(&repair_summary)
    .execute(&mut *transaction)
    .await
    .map_err(internal_error)?;
    transaction.commit().await.map_err(internal_error)?;
    state.raptor_cache.write().await.clear();
    state.endpoint_access_cache.write().await.clear();

    Ok(Json(json!({
        "database_available": true,
        "repair_run_id": repair_run_id,
        "status": "completed",
        "summary": repair_summary
    })))
}

async fn admin_unmatched_stops(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({"stops": [], "database_available": false})));
    };
    let rows = sqlx::query(
        r#"
        SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
               lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
               platform_code, location_type, parent_station_id, station_id, complex_id,
               has_station_layout, station_layout_version, wheelchair_boarding,
               modes, source_priority, is_active
        FROM stops
        WHERE is_active = true
          AND (lat IS NULL OR lon IS NULL OR coordinate_confidence = 'unresolved')
        ORDER BY source_priority ASC, name ASC, id ASC
        LIMIT 1000
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;
    let stops = rows
        .into_iter()
        .map(stop_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(internal_error)?;
    let truncated = stops.len() == 1000;
    Ok(Json(json!({
        "stops": stops,
        "database_available": true,
        "truncated": truncated
    })))
}

async fn admin_manual_stop_match(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(
        json!({"status": "accepted", "warning": "manual match persistence is pending"}),
    ))
}

async fn admin_source_feeds(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(sources().await);
    };
    let rows = sqlx::query(
        r#"
        SELECT id, name, url, type, mode_scope, priority, enabled, created_at
        FROM source_feeds
        ORDER BY priority ASC, id ASC
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(internal_error)?;
    Ok(Json(json!({
        "sources": rows.into_iter().map(source_feed_row_json).collect::<Vec<_>>(),
        "database_available": true
    })))
}

async fn admin_source_feed_patch(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(body): Json<AdminSourceFeedPatch>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({"id": id, "database_available": false})));
    };
    let row = sqlx::query(
        r#"
        UPDATE source_feeds
        SET
          name = COALESCE($2, name),
          url = COALESCE($3, url),
          mode_scope = CASE WHEN $4::text IS NULL THEN mode_scope ELSE NULLIF($4, '') END,
          priority = COALESCE($5, priority),
          enabled = COALESCE($6, enabled)
        WHERE id = $1
        RETURNING id, name, url, type, mode_scope, priority, enabled, created_at
        "#,
    )
    .bind(&id)
    .bind(body.name.filter(|value| !value.trim().is_empty()))
    .bind(body.url.filter(|value| !value.trim().is_empty()))
    .bind(body.mode_scope)
    .bind(body.priority)
    .bind(body.enabled)
    .fetch_optional(pool)
    .await
    .map_err(internal_error)?
    .ok_or_else(not_found)?;
    Ok(Json(json!({
        "source": source_feed_row_json(row),
        "status": "updated"
    })))
}

async fn admin_routing_algorithm(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        let snapshot_status = routing_snapshot_status(
            None,
            &state.raptor_cache,
            &state.config.routing_snapshot_dir,
            &state.routing_warmup_status,
        )
        .await;
        let search_diagnostics =
            route_search_diagnostics_payload(&state.route_search_diagnostics).await;
        return Ok(Json(routing_algorithm_payload(
            RoutingAlgorithmConfig::default(),
            false,
            None,
            None,
            snapshot_status,
            search_diagnostics,
        )));
    };
    let (configuration, updated_at, updated_by) = routing_algorithm_config_db(pool)
        .await
        .map_err(internal_error)?;
    let snapshot_status = routing_snapshot_status(
        Some(pool),
        &state.raptor_cache,
        &state.config.routing_snapshot_dir,
        &state.routing_warmup_status,
    )
    .await;
    let search_diagnostics =
        route_search_diagnostics_payload(&state.route_search_diagnostics).await;
    Ok(Json(routing_algorithm_payload(
        configuration,
        true,
        updated_at,
        updated_by,
        snapshot_status,
        search_diagnostics,
    )))
}

async fn admin_routing_algorithm_update(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(configuration): Json<RoutingAlgorithmConfig>,
) -> Result<Json<Value>, ApiError> {
    let user = require_admin(&state, &headers).await?;
    configuration.validate()?;
    let Some(pool) = &state.db else {
        return Err(ApiError {
            code: "database_unavailable".to_string(),
            message: "Routing configuration cannot be persisted while the database is unavailable"
                .to_string(),
        });
    };
    persist_routing_algorithm_config(pool, &configuration, &user.email)
        .await
        .map_err(internal_error)?;
    let snapshot_status = routing_snapshot_status(
        Some(pool),
        &state.raptor_cache,
        &state.config.routing_snapshot_dir,
        &state.routing_warmup_status,
    )
    .await;
    let search_diagnostics =
        route_search_diagnostics_payload(&state.route_search_diagnostics).await;
    Ok(Json(routing_algorithm_payload(
        configuration,
        true,
        Some(Utc::now()),
        Some(user.email),
        snapshot_status,
        search_diagnostics,
    )))
}

async fn admin_routing_algorithm_reset(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    let user = require_admin(&state, &headers).await?;
    let configuration = RoutingAlgorithmConfig::default();
    let Some(pool) = &state.db else {
        return Err(ApiError {
            code: "database_unavailable".to_string(),
            message: "Routing configuration cannot be reset while the database is unavailable"
                .to_string(),
        });
    };
    persist_routing_algorithm_config(pool, &configuration, &user.email)
        .await
        .map_err(internal_error)?;
    let snapshot_status = routing_snapshot_status(
        Some(pool),
        &state.raptor_cache,
        &state.config.routing_snapshot_dir,
        &state.routing_warmup_status,
    )
    .await;
    let search_diagnostics =
        route_search_diagnostics_payload(&state.route_search_diagnostics).await;
    Ok(Json(routing_algorithm_payload(
        configuration,
        true,
        Some(Utc::now()),
        Some(user.email),
        snapshot_status,
        search_diagnostics,
    )))
}

async fn routing_algorithm_config_db(
    pool: &PgPool,
) -> Result<
    (
        RoutingAlgorithmConfig,
        Option<DateTime<Utc>>,
        Option<String>,
    ),
    sqlx::Error,
> {
    let row = sqlx::query(
        r#"
        SELECT max_results, max_direct_candidates, max_transfer_candidates,
               min_transfer_seconds, max_transfer_wait_seconds,
               transfer_search_timeout_seconds, next_day_search_from_seconds,
               range_search_window_seconds, max_range_departures,
               endpoint_access_cache_enabled,
               arrival_time_weight, duration_weight, transfer_penalty_seconds,
               preserve_simplest, preserve_each_transfer_count,
               preserve_carrier_diversity, remove_dominated,
               dominate_only_same_carrier, updated_at, updated_by
        FROM routing_algorithm_config
        WHERE id = 1
        "#,
    )
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok((RoutingAlgorithmConfig::default(), None, None));
    };
    Ok((
        RoutingAlgorithmConfig {
            max_results: row.get("max_results"),
            max_direct_candidates: row.get("max_direct_candidates"),
            max_transfer_candidates: row.get("max_transfer_candidates"),
            min_transfer_seconds: row.get("min_transfer_seconds"),
            max_transfer_wait_seconds: row.get("max_transfer_wait_seconds"),
            transfer_search_timeout_seconds: row.get("transfer_search_timeout_seconds"),
            next_day_search_from_seconds: row.get("next_day_search_from_seconds"),
            range_search_window_seconds: row.get("range_search_window_seconds"),
            max_range_departures: row.get("max_range_departures"),
            endpoint_access_cache_enabled: row.get("endpoint_access_cache_enabled"),
            arrival_time_weight: row.get("arrival_time_weight"),
            duration_weight: row.get("duration_weight"),
            transfer_penalty_seconds: row.get("transfer_penalty_seconds"),
            preserve_simplest: row.get("preserve_simplest"),
            preserve_each_transfer_count: row.get("preserve_each_transfer_count"),
            preserve_carrier_diversity: row.get("preserve_carrier_diversity"),
            remove_dominated: row.get("remove_dominated"),
            dominate_only_same_carrier: row.get("dominate_only_same_carrier"),
        },
        row.get("updated_at"),
        row.get("updated_by"),
    ))
}

async fn persist_routing_algorithm_config(
    pool: &PgPool,
    configuration: &RoutingAlgorithmConfig,
    updated_by: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO routing_algorithm_config (
          id, max_results, max_direct_candidates, max_transfer_candidates,
          min_transfer_seconds, max_transfer_wait_seconds,
          transfer_search_timeout_seconds, next_day_search_from_seconds,
          range_search_window_seconds, max_range_departures,
          endpoint_access_cache_enabled,
          arrival_time_weight, duration_weight, transfer_penalty_seconds,
          preserve_simplest, preserve_each_transfer_count,
          preserve_carrier_diversity, remove_dominated,
          dominate_only_same_carrier, updated_at, updated_by
        ) VALUES (
          1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
          $11, $12, $13, $14, $15, $16, $17, $18, now(), $19
        )
        ON CONFLICT (id) DO UPDATE SET
          max_results = EXCLUDED.max_results,
          max_direct_candidates = EXCLUDED.max_direct_candidates,
          max_transfer_candidates = EXCLUDED.max_transfer_candidates,
          min_transfer_seconds = EXCLUDED.min_transfer_seconds,
          max_transfer_wait_seconds = EXCLUDED.max_transfer_wait_seconds,
          transfer_search_timeout_seconds = EXCLUDED.transfer_search_timeout_seconds,
          next_day_search_from_seconds = EXCLUDED.next_day_search_from_seconds,
          range_search_window_seconds = EXCLUDED.range_search_window_seconds,
          max_range_departures = EXCLUDED.max_range_departures,
          endpoint_access_cache_enabled = EXCLUDED.endpoint_access_cache_enabled,
          arrival_time_weight = EXCLUDED.arrival_time_weight,
          duration_weight = EXCLUDED.duration_weight,
          transfer_penalty_seconds = EXCLUDED.transfer_penalty_seconds,
          preserve_simplest = EXCLUDED.preserve_simplest,
          preserve_each_transfer_count = EXCLUDED.preserve_each_transfer_count,
          preserve_carrier_diversity = EXCLUDED.preserve_carrier_diversity,
          remove_dominated = EXCLUDED.remove_dominated,
          dominate_only_same_carrier = EXCLUDED.dominate_only_same_carrier,
          updated_at = now(),
          updated_by = EXCLUDED.updated_by
        "#,
    )
    .bind(configuration.max_results)
    .bind(configuration.max_direct_candidates)
    .bind(configuration.max_transfer_candidates)
    .bind(configuration.min_transfer_seconds)
    .bind(configuration.max_transfer_wait_seconds)
    .bind(configuration.transfer_search_timeout_seconds)
    .bind(configuration.next_day_search_from_seconds)
    .bind(configuration.range_search_window_seconds)
    .bind(configuration.max_range_departures)
    .bind(configuration.endpoint_access_cache_enabled)
    .bind(configuration.arrival_time_weight)
    .bind(configuration.duration_weight)
    .bind(configuration.transfer_penalty_seconds)
    .bind(configuration.preserve_simplest)
    .bind(configuration.preserve_each_transfer_count)
    .bind(configuration.preserve_carrier_diversity)
    .bind(configuration.remove_dominated)
    .bind(configuration.dominate_only_same_carrier)
    .bind(updated_by)
    .execute(pool)
    .await?;
    Ok(())
}

fn routing_algorithm_payload(
    configuration: RoutingAlgorithmConfig,
    database_available: bool,
    updated_at: Option<DateTime<Utc>>,
    updated_by: Option<String>,
    snapshot_status: Value,
    search_diagnostics: Value,
) -> Value {
    json!({
        "configuration": configuration,
        "defaults": RoutingAlgorithmConfig::default(),
        "database_available": database_available,
        "updated_at": updated_at,
        "updated_by": updated_by,
        "snapshot_status": snapshot_status,
        "search_diagnostics": search_diagnostics,
        "activation": "New journey searches read this profile immediately; running searches are not changed.",
        "scoring_formula": "arrival_time × arrival_time_weight + duration × duration_weight + transfers × transfer_penalty_seconds",
        "fare_note": "No real fare data is imported. Carrier diversity preserves potentially cheaper operators without claiming a cheapest fare."
    })
}

async fn record_route_search_timing(
    diagnostics: &RouteSearchDiagnostics,
    timing: RouteSearchTiming,
) {
    let mut recent = diagnostics.write().await;
    recent.push_front(timing);
    recent.truncate(ROUTE_SEARCH_TIMING_HISTORY);
}

async fn append_route_search_timing(
    diagnostics: &RouteSearchDiagnostics,
    started_at: DateTime<Utc>,
    stage: &str,
    elapsed_ms: u64,
    detail: Option<String>,
    success: bool,
) {
    let mut recent = diagnostics.write().await;
    if let Some(search) = recent
        .iter_mut()
        .find(|search| search.started_at == started_at)
    {
        search.total_ms = search.total_ms.saturating_add(elapsed_ms);
        search.success &= success;
        search.stages.push(RouteSearchStageTiming {
            stage: stage.to_string(),
            elapsed_ms,
            detail,
        });
    }
}

fn latency_percentile(sorted: &[u64], percentile: usize) -> Option<u64> {
    if sorted.is_empty() || !(1..=100).contains(&percentile) {
        return None;
    }
    sorted
        .get((sorted.len() * percentile).div_ceil(100) - 1)
        .copied()
}

async fn route_search_diagnostics_payload(diagnostics: &RouteSearchDiagnostics) -> Value {
    let recent = diagnostics.read().await;
    let mut latencies = recent
        .iter()
        .map(|search| search.total_ms)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let mut stage_totals = HashMap::<String, (u64, u64, u64)>::new();
    let mut total_sum = 0_u64;
    let mut total_max = 0_u64;
    for search in recent.iter() {
        total_sum = total_sum.saturating_add(search.total_ms);
        total_max = total_max.max(search.total_ms);
        for stage in &search.stages {
            let entry = stage_totals.entry(stage.stage.clone()).or_default();
            entry.0 = entry.0.saturating_add(stage.elapsed_ms);
            entry.1 = entry.1.max(stage.elapsed_ms);
            entry.2 += 1;
        }
    }
    let mut stages = stage_totals
        .into_iter()
        .map(|(stage, (sum, max, samples))| {
            json!({
                "stage": stage,
                "average_ms": sum.checked_div(samples).unwrap_or(0),
                "max_ms": max,
                "samples": samples
            })
        })
        .collect::<Vec<_>>();
    stages.sort_by_key(|stage| std::cmp::Reverse(stage["average_ms"].as_u64().unwrap_or(0)));
    let bottleneck = stages.first().cloned();
    json!({
        "retained_limit": ROUTE_SEARCH_TIMING_HISTORY,
        "sample_count": recent.len(),
        "average_total_ms": if recent.is_empty() { 0 } else { total_sum / recent.len() as u64 },
        "max_total_ms": total_max,
        "p50_total_ms": latency_percentile(&latencies, 50),
        "p95_total_ms": latency_percentile(&latencies, 95),
        "bottleneck": bottleneck,
        "stage_aggregates": stages,
        "recent": recent.iter().take(10).collect::<Vec<_>>(),
        "implemented_improvements": [
            "Resolve origin and destination concurrently",
            "Reuse one routing-data revision for all service days in a request",
            "Pre-index RAPTOR route departures by stop for faster catchable-trip lookup",
            "Use numeric stop indexes and array labels inside RAPTOR scans",
            "Build bounded rRAPTOR-style profiles from real boardable departure events",
            "Shift coordinate-origin departure events by verified pedestrian access time",
            "Search the complete bounded departure profile instead of stopping after a few route patterns",
            "Preserve departure, arrival, transfers and walking as Pareto criteria",
            "Enumerate bounded exact-stop direct services independently of sampled range departures",
            "Reserve primary, simple and reasonable distinct-route alternatives before range sampling",
            "Keep bounded geometry fallbacks before applying final dominance",
            "Reuse route-sized scratch indexes across RAPTOR rounds",
            "Store sparse request-only walking links without stop-sized allocations",
            "Run each small range-probe batch with bounded concurrency",
            "Skip next-service-day RAPTOR when current service-day candidates are sufficient",
            "Add implicit same-station/platform interchange footpaths to RAPTOR timetables",
            "Bound endpoint spatial candidates with indexed lateral lookups",
            "Cache endpoint nearby walking access and coalesce concurrent misses by routing-data revision",
            "Apply fresh stop-level realtime delays inside RAPTOR transfer checks and arrival ranking",
            "Fetch related entities concurrently, then fetch realtime and intermediate stops concurrently",
            "Persist ticketing references after response annotation instead of blocking route search on database fsync",
            "Convert offset timestamps to Europe/Prague and reject any already-departed same-day candidate"
        ]
    })
}

async fn routing_snapshot_status(
    pool: Option<&PgPool>,
    cache: &RaptorCache,
    routing_snapshot_dir: &FsPath,
    warmup_status: &RoutingWarmupStatus,
) -> Value {
    let (revision, latest_import_error) = match pool {
        Some(pool) => match routing_data_revision(pool).await {
            Ok(value) => (Some(value), None),
            Err(error) => (None, Some(error.to_string())),
        },
        None => (None, None),
    };
    let latest_import = revision.as_ref().and_then(|value| value.latest_import);
    let today = chrono::Local::now().date_naive();
    let dates = [
        today,
        today
            .checked_add_days(chrono::Days::new(1))
            .unwrap_or(today),
    ];
    let memory_cached_by_date = {
        let cache = cache.read().await;
        dates
            .iter()
            .map(|date| {
                (
                    *date,
                    revision.as_ref().is_some_and(|revision| {
                        cache.contains_key(&(*date, revision.token.clone()))
                    }),
                )
            })
            .collect::<HashMap<_, _>>()
    };
    let mut snapshots = Vec::new();
    let mut total_size_bytes: u64 = 0;
    for service_date in dates {
        let path = revision.as_ref().map(|revision| {
            raptor_timetable_snapshot_path(routing_snapshot_dir, service_date, revision)
        });
        let Some(path) = path else {
            continue;
        };
        let metadata = tokio::fs::metadata(&path).await.ok();
        let size_bytes = metadata.as_ref().map(|metadata| metadata.len());
        if let Some(size_bytes) = size_bytes {
            total_size_bytes = total_size_bytes.saturating_add(size_bytes);
        }
        let modified_at = metadata
            .as_ref()
            .and_then(|metadata| metadata.modified().ok())
            .map(DateTime::<Utc>::from);
        snapshots.push(json!({
            "service_date": service_date,
            "file_name": path.file_name().and_then(|value| value.to_str()),
            "path": path.display().to_string(),
            "exists": metadata.is_some(),
            "size_bytes": size_bytes,
            "modified_at": modified_at,
            "memory_cached": memory_cached_by_date.get(&service_date).copied().unwrap_or(false)
        }));
    }
    let warmup = warmup_status.read().await.clone();
    let elapsed_seconds = match (warmup.started_at, warmup.finished_at, warmup.active) {
        (Some(started_at), Some(finished_at), false) => {
            Some((finished_at - started_at).num_seconds().max(0))
        }
        (Some(started_at), _, _) => Some((Utc::now() - started_at).num_seconds().max(0)),
        _ => None,
    };
    json!({
        "database_available": pool.is_some(),
        "directory": routing_snapshot_dir,
        "latest_import": latest_import,
        "latest_import_error": latest_import_error,
        "snapshot_version": RAPTOR_TIMETABLE_SNAPSHOT_VERSION,
        "warmup_interval_seconds": RAPTOR_WARMUP_INTERVAL_SECONDS,
        "total_size_bytes": total_size_bytes,
        "snapshots": snapshots,
        "warmup": {
            "active": warmup.active,
            "stage": warmup.stage,
            "service_date": warmup.service_date,
            "current_index": warmup.current_index,
            "total_dates": warmup.total_dates,
            "started_at": warmup.started_at,
            "finished_at": warmup.finished_at,
            "elapsed_seconds": elapsed_seconds,
            "error": warmup.error
        }
    })
}

async fn public_board(Path(stop_id): Path<String>) -> Json<Value> {
    Json(public_board_payload(&stop_id))
}

async fn public_board_qr(Path(stop_id): Path<String>) -> Json<Value> {
    Json(
        json!({"stop_id": stop_id, "board_url": format!("https://cesta.local/public/boards/{stop_id}"), "theme": "default", "mock": true}),
    )
}

fn unauthorized() -> ApiError {
    ApiError {
        code: "unauthorized".to_string(),
        message: "Authentication required".to_string(),
    }
}

fn not_found() -> ApiError {
    ApiError {
        code: "not_found".to_string(),
        message: "Resource not found".to_string(),
    }
}

fn internal_error(_error: impl std::fmt::Display) -> ApiError {
    // SQL errors may contain literal user input or credentials. Do not emit their text.
    tracing::error!("Internal API operation failed");
    ApiError {
        code: "internal_error".into(),
        message: "An internal service error occurred".into(),
    }
}
fn safe_data_warning(_error: impl std::fmt::Display, message: &str) -> String {
    tracing::warn!(context = message, "Data operation unavailable");
    message.to_owned()
}
fn service_unavailable(_error: impl std::fmt::Display) -> ApiError {
    tracing::error!("Transport data query unavailable");
    ApiError {
        code: "upstream_unavailable".into(),
        message: "Transport data is temporarily unavailable".into(),
    }
}

fn package_by_id(id: &str) -> Result<OfflinePackage, ApiError> {
    offline_pack::development_packages()
        .into_iter()
        .find(|package| package.id == id)
        .ok_or_else(not_found)
}

fn import_run_row_json(row: sqlx::postgres::PgRow) -> Value {
    json!({
        "id": row.get::<Uuid, _>("id"),
        "source": row.get::<String, _>("source"),
        "status": row.get::<String, _>("status"),
        "started_at": row.get::<chrono::DateTime<Utc>, _>("started_at"),
        "finished_at": row.get::<Option<chrono::DateTime<Utc>>, _>("finished_at"),
        "summary": row.get::<Value, _>("summary")
    })
}

fn validation_issue_row_json(row: sqlx::postgres::PgRow) -> Value {
    json!({
        "id": row.get::<Uuid, _>("id"),
        "import_run_id": row.get::<Option<Uuid>, _>("import_run_id"),
        "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
        "severity": row.get::<String, _>("severity"),
        "code": row.get::<String, _>("code"),
        "message": row.get::<String, _>("message"),
        "source_file": row.get::<Option<String>, _>("source_file"),
        "affected_entity": row.get::<Option<String>, _>("affected_entity"),
        "raw_payload": row.get::<Option<Value>, _>("raw_payload"),
        "created_at": row.get::<chrono::DateTime<Utc>, _>("created_at")
    })
}

fn source_feed_row_json(row: sqlx::postgres::PgRow) -> Value {
    json!({
        "id": row.get::<String, _>("id"),
        "name": row.get::<String, _>("name"),
        "url": row.get::<String, _>("url"),
        "type": row.get::<String, _>("type"),
        "mode_scope": row.get::<Option<String>, _>("mode_scope"),
        "priority": row.get::<i32, _>("priority"),
        "enabled": row.get::<bool, _>("enabled"),
        "created_at": row.get::<chrono::DateTime<Utc>, _>("created_at")
    })
}

async fn database_status(pool: &PgPool) -> Result<Value, sqlx::Error> {
    let latest = sqlx::query(
        r#"
        SELECT run.id, run.source, run.status, run.started_at, run.finished_at, run.summary
        FROM import_runs AS run
        JOIN source_feeds AS feed
          ON feed.id = run.summary->>'feed_id'
         AND feed.enabled = true
        WHERE run.status = 'success'
        ORDER BY finished_at DESC NULLS LAST, started_at DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await?;
    let stop_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM enabled_source_stops WHERE is_active = true")
            .fetch_one(pool)
            .await?;
    let route_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM routes JOIN source_feeds feed ON feed.id = routes.source_feed_id AND feed.enabled = true WHERE routes.is_active = true",
    )
        .fetch_one(pool)
        .await?;
    let trip_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM trips JOIN source_feeds feed ON feed.id = trips.source_feed_id AND feed.enabled = true",
    )
    .fetch_one(pool)
    .await?;
    let stop_time_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM stop_times JOIN trips ON trips.id = stop_times.trip_id JOIN source_feeds feed ON feed.id = trips.source_feed_id AND feed.enabled = true",
    )
        .fetch_one(pool)
        .await?;
    let realtime_sources = sqlx::query(
        r#"
        SELECT source_id, status, last_success_at, source_timestamp,
               records_received, records_written, error_message
        FROM data_source_syncs
        WHERE source_id = ANY($1)
        ORDER BY source_id ASC
        "#,
    )
    .bind(REALTIME_SOURCE_STATUS_IDS)
    .fetch_all(pool)
    .await?;
    let current_realtime_sources = realtime_sources
        .iter()
        .filter(|row| {
            row.get::<String, _>("status") == "success"
                && row
                    .get::<Option<DateTime<Utc>>, _>("source_timestamp")
                    .is_some_and(|timestamp| timestamp > Utc::now() - Duration::minutes(5))
        })
        .count();
    let has_successful_import = latest.is_some();

    Ok(json!({
        "schedule": if has_successful_import { "current" } else { "unknown" },
        "realtime": if current_realtime_sources == REALTIME_SOURCE_STATUS_IDS.len() {
            "full"
        } else if current_realtime_sources > 0 {
            "partial"
        } else if realtime_sources.is_empty() {
            "unavailable"
        } else {
            "stale"
        },
        "realtime_sources": realtime_sources.into_iter().map(|row| json!({
            "source_id": row.get::<String, _>("source_id"),
            "status": row.get::<String, _>("status"),
            "last_success_at": row.get::<Option<DateTime<Utc>>, _>("last_success_at"),
            "source_timestamp": row.get::<Option<DateTime<Utc>>, _>("source_timestamp"),
            "records_received": row.get::<i32, _>("records_received"),
            "records_written": row.get::<i32, _>("records_written"),
            "error_message": row.get::<Option<String>, _>("error_message")
        })).collect::<Vec<_>>(),
        "source": "database",
        "database_available": true,
        "latest_import": latest.map(|row| json!({
            "id": row.get::<Uuid, _>("id"),
            "source": row.get::<String, _>("source"),
            "status": row.get::<String, _>("status"),
            "started_at": row.get::<chrono::DateTime<Utc>, _>("started_at"),
            "finished_at": row.get::<Option<chrono::DateTime<Utc>>, _>("finished_at"),
            "summary": row.get::<Value, _>("summary")
        })),
        "counts": {
            "stops": stop_count,
            "routes": route_count,
            "trips": trip_count,
            "stop_times": stop_time_count
        },
        "warnings": if has_successful_import { Vec::<String>::new() } else { vec!["no successful import has been loaded yet".to_string()] }
    }))
}

async fn database_admin_stats(pool: &PgPool) -> Result<Value, sqlx::Error> {
    let database_row = sqlx::query(
        r#"
        SELECT
          current_database() AS database_name,
          pg_database_size(current_database()) AS total_size_bytes,
          pg_size_pretty(pg_database_size(current_database())) AS total_size_pretty
        "#,
    )
    .fetch_one(pool)
    .await?;

    let table_rows = sqlx::query(
        r#"
        SELECT
          relname AS table_name,
          n_live_tup::bigint AS estimated_rows,
          n_dead_tup::bigint AS dead_rows,
          pg_relation_size(relid)::bigint AS table_size_bytes,
          pg_indexes_size(relid)::bigint AS indexes_size_bytes,
          GREATEST(
            pg_total_relation_size(relid)
              - pg_relation_size(relid)
              - pg_indexes_size(relid),
            0
          )::bigint AS auxiliary_size_bytes,
          pg_total_relation_size(relid)::bigint AS total_size_bytes,
          pg_size_pretty(pg_relation_size(relid)) AS table_size_pretty,
          pg_size_pretty(pg_indexes_size(relid)) AS indexes_size_pretty,
          pg_size_pretty(pg_total_relation_size(relid)) AS total_size_pretty,
          last_vacuum,
          last_autovacuum,
          last_analyze,
          last_autoanalyze
        FROM pg_catalog.pg_stat_user_tables
        WHERE schemaname = 'public'
        ORDER BY pg_total_relation_size(relid) DESC, relname ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut tables = Vec::new();
    let mut total_rows = 0_i64;
    let mut total_dead_rows = 0_i64;
    let mut table_data_bytes = 0_i64;
    let mut index_bytes = 0_i64;
    let mut auxiliary_bytes = 0_i64;
    let mut user_relation_bytes = 0_i64;
    for row in table_rows {
        let estimated_rows = row.get::<i64, _>("estimated_rows").max(0);
        let dead_rows = row.get::<i64, _>("dead_rows").max(0);
        let row_table_bytes = row.get::<i64, _>("table_size_bytes").max(0);
        let row_index_bytes = row.get::<i64, _>("indexes_size_bytes").max(0);
        let row_auxiliary_bytes = row.get::<i64, _>("auxiliary_size_bytes").max(0);
        let row_total_bytes = row.get::<i64, _>("total_size_bytes").max(0);
        total_rows = total_rows.saturating_add(estimated_rows);
        total_dead_rows = total_dead_rows.saturating_add(dead_rows);
        table_data_bytes = table_data_bytes.saturating_add(row_table_bytes);
        index_bytes = index_bytes.saturating_add(row_index_bytes);
        auxiliary_bytes = auxiliary_bytes.saturating_add(row_auxiliary_bytes);
        user_relation_bytes = user_relation_bytes.saturating_add(row_total_bytes);

        tables.push(json!({
            "table": row.get::<String, _>("table_name"),
            "rows": estimated_rows,
            "rows_are_estimated": true,
            "dead_rows": dead_rows,
            "total_size_bytes": row_total_bytes,
            "table_size_bytes": row_table_bytes,
            "indexes_size_bytes": row_index_bytes,
            "auxiliary_size_bytes": row_auxiliary_bytes,
            "table_size_pretty": row.get::<String, _>("table_size_pretty"),
            "indexes_size_pretty": row.get::<String, _>("indexes_size_pretty"),
            "total_size_pretty": row.get::<String, _>("total_size_pretty"),
            "last_vacuum": row.get::<Option<DateTime<Utc>>, _>("last_vacuum"),
            "last_autovacuum": row.get::<Option<DateTime<Utc>>, _>("last_autovacuum"),
            "last_analyze": row.get::<Option<DateTime<Utc>>, _>("last_analyze"),
            "last_autoanalyze": row.get::<Option<DateTime<Utc>>, _>("last_autoanalyze")
        }));
    }

    let source_rows = sqlx::query(
        r#"
        SELECT
          sf.id,
          sf.name,
          sf.type,
          sf.priority,
          COALESCE((latest.summary->>'stops')::bigint, 0) AS stops,
          COALESCE((latest.summary->>'routes')::bigint, 0) AS routes,
          COALESCE((latest.summary->>'trips')::bigint, 0) AS trips,
          COALESCE((latest.summary->>'stop_times')::bigint, 0) AS stop_times,
          COALESCE((latest.summary->>'validation_issues')::bigint, 0) AS validation_issues
        FROM source_feeds sf
        LEFT JOIN LATERAL (
          SELECT run.summary
          FROM import_runs run
          WHERE run.status = 'success'
            AND (
              run.summary->>'feed_id' = sf.id
              OR run.source LIKE sf.id || ':%'
            )
          ORDER BY run.finished_at DESC NULLS LAST, run.started_at DESC
          LIMIT 1
        ) latest ON true
        ORDER BY sf.priority, sf.id
        "#,
    )
    .fetch_all(pool)
    .await?;
    let source_feeds = source_rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<String, _>("id"),
                "name": row.get::<String, _>("name"),
                "type": row.get::<String, _>("type"),
                "priority": row.get::<i32, _>("priority"),
                "counts": {
                    "stops": row.get::<i64, _>("stops"),
                    "routes": row.get::<i64, _>("routes"),
                    "trips": row.get::<i64, _>("trips"),
                    "stop_times": row.get::<i64, _>("stop_times"),
                    "validation_issues": row.get::<i64, _>("validation_issues")
                }
            })
        })
        .collect::<Vec<_>>();

    let latest_import_rows = sqlx::query(
        r#"
        SELECT id, source, status, started_at, finished_at, summary
        FROM import_runs
        ORDER BY started_at DESC
        LIMIT 10
        "#,
    )
    .fetch_all(pool)
    .await?;
    let latest_imports = latest_import_rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<Uuid, _>("id"),
                "source": row.get::<String, _>("source"),
                "status": row.get::<String, _>("status"),
                "started_at": row.get::<chrono::DateTime<Utc>, _>("started_at"),
                "finished_at": row.get::<Option<chrono::DateTime<Utc>>, _>("finished_at"),
                "summary": row.get::<Value, _>("summary")
            })
        })
        .collect::<Vec<_>>();

    let issue_rows = sqlx::query(
        "SELECT severity, COUNT(*) AS count FROM validation_issues GROUP BY severity ORDER BY severity",
    )
    .fetch_all(pool)
    .await?;
    let validation_issues = issue_rows
        .into_iter()
        .map(|row| {
            json!({
                "severity": row.get::<String, _>("severity"),
                "count": row.get::<i64, _>("count")
            })
        })
        .collect::<Vec<_>>();

    let unresolved_stop_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM stops
        WHERE is_active = true
          AND (lat IS NULL OR lon IS NULL OR coordinate_confidence = 'unresolved')
        "#,
    )
    .fetch_one(pool)
    .await?;

    let index_rows = sqlx::query(
        r#"
        SELECT
          relname AS table_name,
          indexrelname AS index_name,
          pg_relation_size(indexrelid)::bigint AS size_bytes,
          pg_size_pretty(pg_relation_size(indexrelid)) AS size_pretty,
          idx_scan::bigint AS scans
        FROM pg_catalog.pg_stat_user_indexes
        WHERE schemaname = 'public'
        ORDER BY pg_relation_size(indexrelid) DESC, indexrelname ASC
        LIMIT 20
        "#,
    )
    .fetch_all(pool)
    .await?;
    let largest_indexes = index_rows
        .into_iter()
        .map(|row| {
            json!({
                "table": row.get::<String, _>("table_name"),
                "index": row.get::<String, _>("index_name"),
                "size_bytes": row.get::<i64, _>("size_bytes"),
                "size_pretty": row.get::<String, _>("size_pretty"),
                "scans": row.get::<i64, _>("scans")
            })
        })
        .collect::<Vec<_>>();

    let runtime_row = sqlx::query(
        r#"
        SELECT
          stats.numbackends::bigint AS total_connections,
          current_setting('max_connections')::bigint AS max_connections,
          (
            SELECT COUNT(*)::bigint
            FROM pg_catalog.pg_stat_activity activity
            WHERE activity.datname = current_database()
              AND activity.state = 'active'
          ) AS active_connections,
          (
            SELECT COUNT(*)::bigint
            FROM pg_catalog.pg_stat_activity activity
            WHERE activity.datname = current_database()
              AND activity.state = 'idle'
          ) AS idle_connections,
          stats.xact_commit::bigint AS committed_transactions,
          stats.xact_rollback::bigint AS rolled_back_transactions,
          stats.blks_read::bigint AS blocks_read,
          stats.blks_hit::bigint AS blocks_hit,
          stats.temp_files::bigint AS temp_files,
          stats.temp_bytes::bigint AS temp_bytes,
          stats.deadlocks::bigint AS deadlocks,
          stats.stats_reset,
          pg_postmaster_start_time() AS server_started_at
        FROM pg_catalog.pg_stat_database stats
        WHERE stats.datname = current_database()
        "#,
    )
    .fetch_one(pool)
    .await?;
    let blocks_read = runtime_row.get::<i64, _>("blocks_read").max(0);
    let blocks_hit = runtime_row.get::<i64, _>("blocks_hit").max(0);
    let total_block_accesses = blocks_read.saturating_add(blocks_hit);
    let cache_hit_percent = if total_block_accesses == 0 {
        100.0
    } else {
        blocks_hit as f64 * 100.0 / total_block_accesses as f64
    };
    let committed_transactions = runtime_row.get::<i64, _>("committed_transactions").max(0);
    let rolled_back_transactions = runtime_row.get::<i64, _>("rolled_back_transactions").max(0);
    let transaction_count = committed_transactions.saturating_add(rolled_back_transactions);
    let commit_percent = if transaction_count == 0 {
        100.0
    } else {
        committed_transactions as f64 * 100.0 / transaction_count as f64
    };
    let server_started_at = runtime_row.get::<DateTime<Utc>, _>("server_started_at");
    let database_size_bytes = database_row.get::<i64, _>("total_size_bytes").max(0);
    let database_other_bytes = database_size_bytes.saturating_sub(user_relation_bytes);
    let row_slots = total_rows.saturating_add(total_dead_rows);
    let dead_row_percent = if row_slots == 0 {
        0.0
    } else {
        total_dead_rows as f64 * 100.0 / row_slots as f64
    };

    Ok(json!({
        "database_available": true,
        "generated_at": Utc::now(),
        "database": {
            "name": database_row.get::<String, _>("database_name"),
            "total_size_bytes": database_size_bytes,
            "total_size_pretty": database_row.get::<String, _>("total_size_pretty")
        },
        "storage": {
            "database_size_bytes": database_size_bytes,
            "user_relations_bytes": user_relation_bytes,
            "table_data_bytes": table_data_bytes,
            "index_bytes": index_bytes,
            "auxiliary_bytes": auxiliary_bytes,
            "database_other_bytes": database_other_bytes,
            "routing_snapshot_bytes": 0,
            "note": "Database usage excludes routing snapshot files. Host disk capacity is not available from PostgreSQL."
        },
        "runtime": {
            "total_connections": runtime_row.get::<i64, _>("total_connections"),
            "max_connections": runtime_row.get::<i64, _>("max_connections"),
            "active_connections": runtime_row.get::<i64, _>("active_connections"),
            "idle_connections": runtime_row.get::<i64, _>("idle_connections"),
            "committed_transactions": committed_transactions,
            "rolled_back_transactions": rolled_back_transactions,
            "commit_percent": commit_percent,
            "cache_hit_percent": cache_hit_percent,
            "temp_files": runtime_row.get::<i64, _>("temp_files"),
            "temp_bytes": runtime_row.get::<i64, _>("temp_bytes"),
            "deadlocks": runtime_row.get::<i64, _>("deadlocks"),
            "stats_reset": runtime_row.get::<Option<DateTime<Utc>>, _>("stats_reset"),
            "server_started_at": server_started_at,
            "uptime_seconds": (Utc::now() - server_started_at).num_seconds().max(0)
        },
        "totals": {
            "tracked_rows": total_rows,
            "tracked_rows_are_estimated": true,
            "dead_rows": total_dead_rows,
            "dead_row_percent": dead_row_percent,
            "unresolved_active_stops": unresolved_stop_count
        },
        "tables": tables,
        "largest_indexes": largest_indexes,
        "source_feeds": source_feeds,
        "latest_imports": latest_imports,
        "validation_issues": validation_issues
    }))
}

#[allow(clippy::too_many_arguments)]
async fn query_journeys_db(
    pool: &PgPool,
    raptor_cache: &RaptorCache,
    endpoint_access_cache: &EndpointAccessCache,
    pedestrian_router: &PedestrianRouter,
    routing_realtime_cache: &RoutingRealtimeCache,
    routing_snapshot_dir: &FsPath,
    diagnostics: &RouteSearchDiagnostics,
    telemetry: &telemetry::Telemetry,
    body: &JourneySearchBody,
    departure_time: u32,
    service_date: chrono::NaiveDate,
) -> Result<(Vec<Value>, Vec<String>, Value, DateTime<Utc>), sqlx::Error> {
    let mut timing = RouteSearchTimingBuilder::new(service_date);
    let search_started_at = timing.started_at;
    let result = query_journeys_profiled_db(
        pool,
        raptor_cache,
        endpoint_access_cache,
        pedestrian_router,
        routing_realtime_cache,
        routing_snapshot_dir,
        body,
        departure_time,
        service_date,
        &mut timing,
    )
    .await;
    let result_count = result
        .as_ref()
        .map(|(journeys, _, _)| journeys.len())
        .unwrap_or(0);
    let completed = timing.finish(result.is_ok(), result_count);
    tracing::info!(
        elapsed_ms = completed.total_ms,
        success = completed.success,
        results = completed.result_count,
        service_date = %completed.service_date,
        "profiled route search completed"
    );
    telemetry.stages(&completed);
    record_route_search_timing(diagnostics, completed).await;
    result.map(|(journeys, warnings, related)| (journeys, warnings, related, search_started_at))
}

#[allow(clippy::too_many_arguments)]
async fn query_journeys_profiled_db(
    pool: &PgPool,
    raptor_cache: &RaptorCache,
    endpoint_access_cache: &EndpointAccessCache,
    pedestrian_router: &PedestrianRouter,
    routing_realtime_cache: &RoutingRealtimeCache,
    routing_snapshot_dir: &FsPath,
    body: &JourneySearchBody,
    departure_time: u32,
    service_date: chrono::NaiveDate,
    timing: &mut RouteSearchTimingBuilder,
) -> Result<(Vec<Value>, Vec<String>, Value), sqlx::Error> {
    let mut warnings = Vec::new();
    let stage_started = time::Instant::now();
    let (routing_config, _, _) = routing_algorithm_config_db(pool).await?;
    timing.push("routing_config", stage_started, None);

    let stage_started = time::Instant::now();
    let (from_result, to_result) = tokio::join!(
        resolve_journey_point_db(pool, &body.from),
        resolve_journey_point_db(pool, &body.to)
    );
    let (from_stop_ids, from_warning) = from_result?;
    let (to_stop_ids, to_warning) = to_result?;
    timing.push(
        "resolve_endpoints",
        stage_started,
        Some(format!(
            "{} origin, {} destination stops",
            from_stop_ids.len(),
            to_stop_ids.len()
        )),
    );
    warnings.extend(from_warning);
    warnings.extend(to_warning);

    if from_stop_ids.is_empty() || to_stop_ids.is_empty() {
        warnings.push("one or both journey stops could not be resolved".to_string());
        return Ok((
            Vec::new(),
            warnings,
            json!({
                "query_context": journey_query_context(body, departure_time, &from_stop_ids, &to_stop_ids, 0),
                "routing_diagnostics": {
                    "failure_stage": "endpoint_resolution",
                    "endpoint_resolution": {
                        "from_expanded_stop_ids": from_stop_ids,
                        "to_expanded_stop_ids": to_stop_ids
                    },
                    "final_candidate_count": 0
                }
            }),
        ));
    }

    let stage_started = time::Instant::now();
    let routing_revision = routing_data_revision(pool).await?;
    timing.push("routing_revision", stage_started, None);

    let nearby_future = async {
        let started = time::Instant::now();
        let pool = pool.clone();
        let endpoint_access_cache = endpoint_access_cache.clone();
        let pedestrian_router = pedestrian_router.clone();
        let routing_revision = routing_revision.clone();
        let from_point = body.from.clone();
        let from_stop_ids = from_stop_ids.clone();
        let to_point = body.to.clone();
        let to_stop_ids = to_stop_ids.clone();
        let walking_speed_mps = walking_speed_meters_per_second(&body.walking_speed);
        let cache_enabled = routing_config.endpoint_access_cache_enabled;
        let task = tokio::spawn(async move {
            nearby_journey_transfers_db(
                &pool,
                &endpoint_access_cache,
                &pedestrian_router,
                &routing_revision,
                cache_enabled,
                &from_point,
                &from_stop_ids,
                &to_point,
                &to_stop_ids,
                walking_speed_mps,
            )
            .await
        });
        match time::timeout(
            std::time::Duration::from_millis(ROUTING_ENDPOINT_ACCESS_BUDGET_MILLIS),
            task,
        )
        .await
        {
            Ok(Ok(result)) => (result, elapsed_millis(started), false),
            Ok(Err(error)) => {
                tracing::error!(%error, "nearby endpoint cache task failed");
                (
                    Ok::<NearbyJourneyTransfers, sqlx::Error>(NearbyJourneyTransfers::default()),
                    elapsed_millis(started),
                    true,
                )
            }
            Err(_) => (
                Ok::<NearbyJourneyTransfers, sqlx::Error>(NearbyJourneyTransfers::default()),
                elapsed_millis(started),
                true,
            ),
        }
    };
    let realtime_future = async {
        let started = time::Instant::now();
        let (result, fallback) =
            match journey_routing_realtime_cached(routing_realtime_cache, service_date).await {
                Some(result) => (result, false),
                None => (
                    RoutingRealtimeSnapshot {
                        data: Arc::new(RaptorRealtimeData::default()),
                        cache_hit: false,
                    },
                    true,
                ),
            };
        (result, elapsed_millis(started), fallback)
    };
    let (nearby_result, realtime_result) = tokio::join!(nearby_future, realtime_future);
    let nearby_transfers = nearby_result.0?;
    warnings.extend(nearby_transfers.diagnostics.iter().cloned());
    if nearby_result.2 {
        warnings.push(format!(
            "nearby walking access exceeded the {ROUTING_ENDPOINT_ACCESS_BUDGET_MILLIS}ms latency budget; the cache is warming in the background"
        ));
    }
    timing.stages.push(RouteSearchStageTiming {
        stage: "nearby_transfers".to_string(),
        elapsed_ms: nearby_result.1,
        detail: Some(if nearby_result.2 {
            "latency budget reached; background cache warmup continues".to_string()
        } else {
            format!(
                "{} walking links; {} cache hits, {} misses",
                nearby_transfers.transfers.len(),
                nearby_transfers.cache_hits,
                nearby_transfers.cache_misses
            )
        }),
    });
    let routing_realtime = realtime_result.0;
    if realtime_result.2 {
        warnings
            .push("realtime routing cache was not ready; scheduled times were used".to_string());
    }
    timing.stages.push(RouteSearchStageTiming {
        stage: "routing_realtime".to_string(),
        elapsed_ms: realtime_result.1,
        detail: Some(format!(
            "{} delayed trips available to RAPTOR; {}",
            routing_realtime.data.trip_count(),
            if realtime_result.2 {
                "background cache unavailable, scheduled-time fallback"
            } else if routing_realtime.cache_hit {
                "background cache hit"
            } else {
                "cache miss"
            }
        )),
    });

    let mode_filters = body
        .transport_modes
        .iter()
        .filter_map(transport_mode_to_db)
        .collect::<Vec<_>>();
    let transfer_buffer_seconds = body
        .journey_preferences
        .as_ref()
        .map_or(0, |preferences| preferences.minimum_transfer_buffer_seconds);
    let current_service_day_result = service_day_journeys_db(
        pool,
        raptor_cache,
        pedestrian_router,
        routing_snapshot_dir,
        &routing_revision,
        &from_stop_ids,
        &to_stop_ids,
        departure_time,
        &mode_filters,
        body.max_transfers,
        transfer_buffer_seconds,
        service_date,
        &routing_config,
        &nearby_transfers.transfers,
        routing_realtime.data.clone(),
    )
    .await;
    let include_next_service_day = should_search_next_service_day(
        departure_time,
        routing_config.next_day_search_from_seconds as u32,
    );

    let (mut journeys, transfer_search_status, current_timing) = current_service_day_result?;
    let current_service_day_candidate_count = journeys.len();
    let mut legacy_search_attempted = current_timing.legacy_search_attempted;
    timing.stages.push(RouteSearchStageTiming {
        stage: "current_timetable_access".to_string(),
        elapsed_ms: current_timing.timetable_ms,
        detail: Some(if current_timing.memory_cache_hit {
            format!(
                "memory cache hit; {} trips across {} route patterns; largest pattern has {} trips",
                current_timing.trip_count,
                current_timing.route_count,
                current_timing.max_route_trip_count
            )
        } else {
            format!(
                "cache miss, disk load, build, or concurrent wait; {} trips across {} route patterns; largest pattern has {} trips",
                current_timing.trip_count,
                current_timing.route_count,
                current_timing.max_route_trip_count
            )
        }),
    });
    timing.stages.push(RouteSearchStageTiming {
        stage: "current_raptor".to_string(),
        elapsed_ms: current_timing.raptor_ms,
        detail: Some(format!(
            "{} candidates from {} departure probes{}; {} rounds, {} route scans, {} marked stops; {}",
            journeys.len(),
            current_timing.range_departure_count,
            if current_timing.range_expanded {
                " after adaptive expansion"
            } else {
                ""
            },
            current_timing.raptor_rounds,
            current_timing.raptor_routes_scanned,
            current_timing.raptor_marked_stops,
            if current_timing.legacy_search_attempted {
                "legacy fallback attempted"
            } else {
                "verified-only search succeeded"
            },
        )),
    });
    let stage_started = time::Instant::now();
    let departed_candidate_count = discard_departed_journeys(&mut journeys, departure_time);
    timing.push(
        "past_departure_guard",
        stage_started,
        Some(format!(
            "discarded {departed_candidate_count} candidates before the requested Prague-local time"
        )),
    );
    if departed_candidate_count > 0 {
        warnings.push(format!(
            "discarded {departed_candidate_count} already-departed journey candidates"
        ));
    }
    if journeys.is_empty() {
        let stage_started = time::Instant::now();
        let mut direct_journeys = direct_journeys_db(
            pool,
            &from_stop_ids,
            &to_stop_ids,
            departure_time,
            &mode_filters,
            service_date,
            routing_config.max_results.max(1) as i64,
        )
        .await?;
        let direct_count = direct_journeys.len();
        journeys.append(&mut direct_journeys);
        timing.push(
            "direct_pid_fallback",
            stage_started,
            Some(format!("{direct_count} direct PID candidates")),
        );
        if direct_count > 0 {
            warnings.push(
                "used the direct PID schedule fallback because RAPTOR returned no same-day route"
                    .to_string(),
            );
        }
    }
    append_transfer_search_warning(
        &mut warnings,
        transfer_search_status,
        false,
        routing_config.transfer_search_timeout_seconds,
    );

    let mut next_service_day_result = None;
    if include_next_service_day {
        if should_search_next_service_day_for_candidates(journeys.len(), &routing_config) {
            let stage_started = time::Instant::now();
            next_service_day_result = match cached_service_day_journeys_db(
                raptor_cache,
                &routing_revision,
                &from_stop_ids,
                &to_stop_ids,
                0,
                &mode_filters,
                body.max_transfers,
                transfer_buffer_seconds,
                service_date.succ_opt().unwrap_or(service_date),
                &routing_config,
                &nearby_transfers.transfers,
                routing_realtime.data.clone(),
            )
            .await?
            {
                Some(next) => Some(next),
                None => {
                    timing.push(
                        "next_timetable_access",
                        stage_started,
                        Some(
                            "skipped cold next service-day timetable; background warmup will prepare it"
                                .to_string(),
                        ),
                    );
                    warnings.push(
                        "next service-day search was skipped because its routing timetable is still warming"
                            .to_string(),
                    );
                    None
                }
            };
        } else {
            timing.push(
                "next_raptor",
                time::Instant::now(),
                Some(format!(
                    "skipped because current service day produced {} candidates",
                    journeys.len()
                )),
            );
        }
    }

    if let Some(next_service_day_result) = next_service_day_result {
        let (next_service_day_journeys, next_transfer_search_status, next_timing) =
            next_service_day_result;
        legacy_search_attempted |= next_timing.legacy_search_attempted;
        timing.stages.push(RouteSearchStageTiming {
            stage: "next_timetable_access".to_string(),
            elapsed_ms: next_timing.timetable_ms,
            detail: Some(if next_timing.memory_cache_hit {
                format!(
                    "memory cache hit; {} trips across {} route patterns; largest pattern has {} trips",
                    next_timing.trip_count,
                    next_timing.route_count,
                    next_timing.max_route_trip_count
                )
            } else {
                format!(
                    "cache miss, disk load, build, or concurrent wait; {} trips across {} route patterns; largest pattern has {} trips",
                    next_timing.trip_count,
                    next_timing.route_count,
                    next_timing.max_route_trip_count
                )
        }),
    });
        timing.stages.push(RouteSearchStageTiming {
            stage: "next_raptor".to_string(),
            elapsed_ms: next_timing.raptor_ms,
            detail: Some(format!(
                "{} candidates from {} departure probes{}; {} rounds, {} route scans, {} marked stops; {}",
                next_service_day_journeys.len(),
                next_timing.range_departure_count,
                if next_timing.range_expanded {
                    " after adaptive expansion"
                } else {
                    ""
                },
                next_timing.raptor_rounds,
                next_timing.raptor_routes_scanned,
                next_timing.raptor_marked_stops,
                if next_timing.legacy_search_attempted {
                    "legacy fallback attempted"
                } else {
                    "verified-only search succeeded"
                }
            )),
        });
        append_transfer_search_warning(
            &mut warnings,
            next_transfer_search_status,
            true,
            routing_config.transfer_search_timeout_seconds,
        );
        let mut next_service_day_journeys = next_service_day_journeys
            .into_iter()
            .map(|journey| shift_journey_service_day(journey, SERVICE_DAY_SECONDS))
            .collect::<Vec<_>>();
        if !next_service_day_journeys.is_empty() {
            journeys.append(&mut next_service_day_journeys);
            warnings.push(
                "included next service-day journeys because early-morning departures occur after the requested time"
                    .to_string(),
            );
        }
    }
    if journeys.is_empty() && !include_next_service_day {
        let stage_started = time::Instant::now();
        let next_service_day_result = cached_service_day_journeys_db(
            raptor_cache,
            &routing_revision,
            &from_stop_ids,
            &to_stop_ids,
            0,
            &mode_filters,
            body.max_transfers,
            transfer_buffer_seconds,
            service_date.succ_opt().unwrap_or(service_date),
            &routing_config,
            &nearby_transfers.transfers,
            routing_realtime.data.clone(),
        )
        .await?;
        if let Some((next_service_day_journeys, next_transfer_search_status, next_timing)) =
            next_service_day_result
        {
            legacy_search_attempted |= next_timing.legacy_search_attempted;
            timing.stages.push(RouteSearchStageTiming {
                stage: "next_timetable_access".to_string(),
                elapsed_ms: next_timing.timetable_ms,
                detail: Some(if next_timing.memory_cache_hit {
                    format!(
                        "memory cache hit; {} trips across {} route patterns; largest pattern has {} trips",
                        next_timing.trip_count,
                        next_timing.route_count,
                        next_timing.max_route_trip_count
                    )
                } else {
                    format!(
                        "cache miss, disk load, build, or concurrent wait; {} trips across {} route patterns; largest pattern has {} trips",
                        next_timing.trip_count,
                        next_timing.route_count,
                        next_timing.max_route_trip_count
                    )
                }),
            });
            timing.stages.push(RouteSearchStageTiming {
                stage: "next_raptor".to_string(),
                elapsed_ms: next_timing.raptor_ms,
                detail: Some(format!(
                    "{} candidates from {} departure probes{}; {} rounds, {} route scans, {} marked stops; {}",
                    next_service_day_journeys.len(),
                    next_timing.range_departure_count,
                    if next_timing.range_expanded {
                        " after adaptive expansion"
                    } else {
                        ""
                    },
                    next_timing.raptor_rounds,
                    next_timing.raptor_routes_scanned,
                    next_timing.raptor_marked_stops,
                    if next_timing.legacy_search_attempted {
                        "legacy fallback attempted"
                    } else {
                        "verified-only search succeeded"
                    }
                )),
            });
            append_transfer_search_warning(
                &mut warnings,
                next_transfer_search_status,
                true,
                routing_config.transfer_search_timeout_seconds,
            );
            let mut next_service_day_journeys =
                next_service_day_journey_results(next_service_day_journeys, departure_time);
            if !next_service_day_journeys.is_empty() {
                journeys.append(&mut next_service_day_journeys);
                warnings.push(
                    "included next service-day journeys because no later service was available on the requested day"
                        .to_string(),
                );
            }
        } else {
            timing.push(
                "next_timetable_access",
                stage_started,
                Some(
                    "skipped cold next service-day timetable after same-day no-result".to_string(),
                ),
            );
            warnings.push(
                "no same-day journey was found; next service-day fallback was skipped because its routing timetable is still warming"
                    .to_string(),
            );
        }
    }
    let candidate_count = journeys.len();
    let stage_started = time::Instant::now();
    let legacy_trip_ids = if legacy_search_attempted {
        legacy_journey_trip_ids_db(pool, &journeys).await?
    } else {
        HashSet::new()
    };
    let (preferred_journeys, verified_candidate_count, legacy_candidate_count) =
        prefer_calendar_verified_journeys(journeys, &legacy_trip_ids);
    journeys = preferred_journeys;
    let service_valid_candidate_count = journeys.len();
    if verified_candidate_count > 0 && legacy_candidate_count > 0 {
        warnings.push(format!(
            "discarded {legacy_candidate_count} calendar-unverified journey candidates because verified alternatives were available"
        ));
    }
    timing.push(
        "legacy_service_validation",
        stage_started,
        Some(if legacy_search_attempted {
            format!(
                "{verified_candidate_count} verified, {legacy_candidate_count} legacy candidates"
            )
        } else {
            format!(
                "database validation skipped; RAPTOR returned {verified_candidate_count} verified candidates"
            )
        }),
    );
    let stage_started = time::Instant::now();
    journeys = dedupe_relevant_journeys_db(pool, journeys, &routing_config).await?;
    let deduplicated_candidate_count = journeys.len();
    timing.push(
        "dedupe_candidates",
        stage_started,
        Some(format!(
            "{candidate_count} to {} candidates",
            journeys.len()
        )),
    );
    let removed_candidates = candidate_count.saturating_sub(journeys.len());
    if removed_candidates > 0 {
        warnings.push(format!(
            "removed {removed_candidates} duplicate or invalid journey candidates"
        ));
    }
    let stage_started = time::Instant::now();
    let carrier_keys = journey_carrier_keys_db(pool, &journeys).await?;
    let geometry_preselection_config = geometry_preselection_config(&routing_config);
    journeys = ranked_journey_results_with_carriers(
        journeys,
        &carrier_keys,
        &geometry_preselection_config,
    );
    timing.push(
        "carrier_lookup_and_preselect",
        stage_started,
        Some(format!(
            "{} candidates selected for geometry validation",
            journeys.len()
        )),
    );
    let stage_started = time::Instant::now();
    let geometry_diagnostics =
        attach_journey_geometries_db(pool, pedestrian_router, &mut journeys).await?;
    let geometry_valid_candidate_count = journeys.len();
    warnings.extend(geometry_diagnostics);
    timing.push(
        "leg_geometries",
        stage_started,
        Some(format!(
            "{} candidates have complete real geometry",
            journeys.len()
        )),
    );
    let stage_started = time::Instant::now();
    let dominance_removed = if routing_config.remove_dominated {
        journeys.len().saturating_sub(
            remove_dominated_journeys(journeys.clone(), &carrier_keys, &routing_config).len(),
        )
    } else {
        0
    };
    journeys = ranked_journey_results_with_carriers(journeys, &carrier_keys, &routing_config);
    if dominance_removed > 0 {
        warnings.push(format!("candidate_rejected:dominance:{dominance_removed}"));
    }
    let final_candidate_count = journeys.len();
    timing.push(
        "final_rank",
        stage_started,
        Some(format!("{} final journeys", journeys.len())),
    );

    let selected_legacy_services = journeys
        .iter()
        .flat_map(|journey| journey.legs.iter())
        .filter_map(|leg| leg.trip_id.as_ref())
        .filter(|trip_id| legacy_trip_ids.contains(*trip_id))
        .collect::<HashSet<_>>()
        .len();
    if selected_legacy_services > 0 {
        warnings.push(format!(
            "no calendar-verified journey was available; using {selected_legacy_services} services from the latest import of feeds without calendar data"
        ));
    }

    if journeys.is_empty() {
        warnings.push("no database journeys found for the resolved stops".to_string());
    }

    let related_future = async {
        let started = time::Instant::now();
        (
            journey_related_data_db(pool, &journeys, body.offline_compatible).await,
            started,
        )
    };
    let realtime_future = async {
        let started = time::Instant::now();
        (
            journey_realtime_updates_db(pool, &journeys, service_date).await,
            started,
        )
    };
    let (mut related, realtime_updates, stop_calls) = if body.include_intermediate_stops {
        let stop_calls_future = async {
            let started = time::Instant::now();
            (journey_stop_calls_db(pool, &journeys).await, started)
        };
        let (related_result, realtime_result, stop_calls_result) =
            tokio::join!(related_future, realtime_future, stop_calls_future);
        timing.push("related_data", related_result.1, None);
        timing.push("realtime_updates", realtime_result.1, None);
        timing.push("intermediate_stops", stop_calls_result.1, None);
        (
            related_result.0?,
            realtime_result.0?,
            Some(stop_calls_result.0?),
        )
    } else {
        let (related_result, realtime_result) = tokio::join!(related_future, realtime_future);
        timing.push("related_data", related_result.1, None);
        timing.push("realtime_updates", realtime_result.1, None);
        (related_result.0?, realtime_result.0?, None)
    };
    let stage_started = time::Instant::now();
    let mut journey_values = journeys_with_realtime(&journeys, &realtime_updates);
    attach_journey_display_metadata(&mut journey_values, &related);
    if let Some(stop_calls) = &stop_calls {
        attach_stop_calls(
            &journeys,
            &mut journey_values,
            stop_calls,
            &realtime_updates,
        );
    }
    attach_journey_assistance_identity(&mut journey_values, &related, service_date);
    related["realtime_updates"] = Value::Array(realtime_updates);
    related["realtime_status"] = json!(journeys_realtime_status(&journey_values));
    related["intermediate_stops_included"] = json!(body.include_intermediate_stops);
    related["query_context"] = journey_query_context(
        body,
        departure_time,
        &from_stop_ids,
        &to_stop_ids,
        nearby_transfers.transfers.len(),
    );
    if journey_values.is_empty() {
        related["routing_diagnostics"] = json!({
            "failure_stage": "candidate_selection",
            "endpoint_resolution": {
                "from_expanded_stop_ids": from_stop_ids,
                "to_expanded_stop_ids": to_stop_ids
            },
            "access": {
                "nearby_transfer_count": nearby_transfers.transfers.len(),
                "timed_out": nearby_result.2,
                "rejections": nearby_transfers.diagnostics
            },
            "timetable": {
                "service_date": service_date,
                "trip_count": current_timing.trip_count,
                "route_pattern_count": current_timing.route_count
            },
            "filters": {
                "allowed_modes": mode_filters,
                "max_transfers": body.max_transfers
            },
            "candidates": {
                "current_service_day_generated": current_service_day_candidate_count,
                "generated_before_service_validation": candidate_count,
                "after_service_validation": service_valid_candidate_count,
                "after_deduplication": deduplicated_candidate_count,
                "after_geometry_validation": geometry_valid_candidate_count,
                "dominance_rejections": dominance_removed
            },
            "final_candidate_count": final_candidate_count
        });
    }
    timing.push("response_assembly", stage_started, None);

    Ok((journey_values, warnings, related))
}

fn journey_uses_legacy_trip(journey: &Journey, legacy_trip_ids: &HashSet<String>) -> bool {
    journey
        .legs
        .iter()
        .filter_map(|leg| leg.trip_id.as_ref())
        .any(|trip_id| legacy_trip_ids.contains(trip_id))
}

fn prefer_calendar_verified_journeys(
    mut journeys: Vec<Journey>,
    legacy_trip_ids: &HashSet<String>,
) -> (Vec<Journey>, usize, usize) {
    let legacy_count = journeys
        .iter()
        .filter(|journey| journey_uses_legacy_trip(journey, legacy_trip_ids))
        .count();
    let verified_count = journeys.len().saturating_sub(legacy_count);
    if verified_count > 0 {
        journeys.retain(|journey| !journey_uses_legacy_trip(journey, legacy_trip_ids));
    }
    (journeys, verified_count, legacy_count)
}

async fn legacy_journey_trip_ids_db(
    pool: &PgPool,
    journeys: &[Journey],
) -> Result<HashSet<String>, sqlx::Error> {
    let trip_ids = journeys
        .iter()
        .flat_map(|journey| journey.legs.iter())
        .filter_map(|leg| leg.trip_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if trip_ids.is_empty() {
        return Ok(HashSet::new());
    }
    Ok(sqlx::query_scalar(
        r#"
        SELECT trip.id
        FROM trips trip
        WHERE trip.id = ANY($1)
          AND NOT EXISTS (SELECT 1 FROM calendars WHERE source_feed_id = trip.source_feed_id)
          AND NOT EXISTS (SELECT 1 FROM calendar_dates WHERE source_feed_id = trip.source_feed_id)
        "#,
    )
    .bind(trip_ids)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum TransferSearchStatus {
    Complete,
    TimedOut,
    Failed,
}

#[derive(Debug, Default)]
struct NearbyJourneyTransfers {
    transfers: Vec<Transfer>,
    cache_hits: usize,
    cache_misses: usize,
    diagnostics: Vec<String>,
}

#[allow(clippy::too_many_arguments)]
async fn nearby_journey_transfers_db(
    pool: &PgPool,
    endpoint_access_cache: &EndpointAccessCache,
    pedestrian_router: &PedestrianRouter,
    routing_revision: &RoutingDataRevision,
    cache_enabled: bool,
    from_point: &JourneyPoint,
    from_stop_ids: &[String],
    to_point: &JourneyPoint,
    to_stop_ids: &[String],
    walking_speed_mps: f64,
) -> Result<NearbyJourneyTransfers, sqlx::Error> {
    let origin_transfers = async {
        if journey_point_uses_nearby_access(from_point) {
            nearby_endpoint_transfers_cached_db(
                pool,
                endpoint_access_cache,
                pedestrian_router,
                routing_revision,
                cache_enabled,
                from_point,
                from_stop_ids,
                true,
                walking_speed_mps,
            )
            .await
        } else {
            Ok((EndpointAccessResult::default(), false))
        }
    };
    let destination_transfers = async {
        if journey_point_uses_nearby_access(to_point) {
            nearby_endpoint_transfers_cached_db(
                pool,
                endpoint_access_cache,
                pedestrian_router,
                routing_revision,
                cache_enabled,
                to_point,
                to_stop_ids,
                false,
                walking_speed_mps,
            )
            .await
        } else {
            Ok((EndpointAccessResult::default(), false))
        }
    };
    let (origin_transfers, destination_transfers) =
        tokio::join!(origin_transfers, destination_transfers);
    let (mut origin, origin_cache_hit) = origin_transfers?;
    let (destination, destination_cache_hit) = destination_transfers?;
    origin.transfers.extend(destination.transfers);
    origin.diagnostics.extend(destination.diagnostics);
    remove_direct_endpoint_walks(&mut origin.transfers, from_stop_ids, to_stop_ids);

    origin.transfers.sort_by(|left, right| {
        left.from_stop_id
            .cmp(&right.from_stop_id)
            .then_with(|| left.to_stop_id.cmp(&right.to_stop_id))
            .then_with(|| left.min_transfer_seconds.cmp(&right.min_transfer_seconds))
    });
    origin.transfers.dedup_by(|left, right| {
        left.from_stop_id == right.from_stop_id && left.to_stop_id == right.to_stop_id
    });

    let cache_hits = usize::from(origin_cache_hit) + usize::from(destination_cache_hit);
    let cache_misses =
        usize::from(journey_point_uses_nearby_access(from_point) && !origin_cache_hit)
            + usize::from(journey_point_uses_nearby_access(to_point) && !destination_cache_hit);
    Ok(NearbyJourneyTransfers {
        transfers: origin.transfers,
        cache_hits,
        cache_misses,
        diagnostics: origin.diagnostics,
    })
}

fn remove_direct_endpoint_walks(
    transfers: &mut Vec<Transfer>,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
) {
    let origins = from_stop_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let destinations = to_stop_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    transfers.retain(|transfer| {
        !(origins.contains(transfer.from_stop_id.as_str())
            && destinations.contains(transfer.to_stop_id.as_str()))
    });
}

fn journey_point_uses_nearby_access(point: &JourneyPoint) -> bool {
    matches!(point.point_type.as_str(), "coordinate" | "stop")
}

#[allow(clippy::too_many_arguments)]
async fn nearby_endpoint_transfers_cached_db(
    pool: &PgPool,
    endpoint_access_cache: &EndpointAccessCache,
    pedestrian_router: &PedestrianRouter,
    routing_revision: &RoutingDataRevision,
    cache_enabled: bool,
    point: &JourneyPoint,
    selected_stop_ids: &[String],
    access_to_origin: bool,
    walking_speed_mps: f64,
) -> Result<(EndpointAccessResult, bool), sqlx::Error> {
    let key = endpoint_access_cache_key(
        routing_revision,
        selected_stop_ids,
        access_to_origin,
        walking_speed_mps,
    );
    if !cache_enabled {
        return nearby_endpoint_transfers_db(
            pool,
            pedestrian_router,
            point,
            selected_stop_ids,
            access_to_origin,
            walking_speed_mps,
        )
        .await
        .map(|transfers| (transfers, false));
    }

    let cell = {
        let mut cache = endpoint_access_cache.write().await;
        cache.retain(|known, _| known.revision_token == routing_revision.token);
        cache
            .entry(key)
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    };
    let cache_hit = cell.get().is_some();
    let transfers = cell
        .get_or_try_init(|| async {
            nearby_endpoint_transfers_db(
                pool,
                pedestrian_router,
                point,
                selected_stop_ids,
                access_to_origin,
                walking_speed_mps,
            )
            .await
        })
        .await?
        .clone();
    Ok((transfers, cache_hit))
}

fn endpoint_access_cache_key(
    routing_revision: &RoutingDataRevision,
    selected_stop_ids: &[String],
    access_to_origin: bool,
    walking_speed_mps: f64,
) -> EndpointAccessCacheKey {
    let mut selected_stop_ids = selected_stop_ids.to_vec();
    selected_stop_ids.sort();
    selected_stop_ids.dedup();
    EndpointAccessCacheKey {
        revision_token: routing_revision.token.clone(),
        selected_stop_ids,
        access_to_origin,
        walking_speed_centimeters_per_second: (walking_speed_mps * 100.0).round().max(0.0) as u32,
    }
}

async fn nearby_endpoint_transfers_db(
    pool: &PgPool,
    pedestrian_router: &PedestrianRouter,
    point: &JourneyPoint,
    selected_stop_ids: &[String],
    access_to_origin: bool,
    walking_speed_mps: f64,
) -> Result<EndpointAccessResult, sqlx::Error> {
    if selected_stop_ids.is_empty() {
        return Ok(EndpointAccessResult::default());
    }

    let rows = if point.point_type == "coordinate" {
        let (lat, lon) = point
            .lat
            .zip(point.lon)
            .expect("coordinate point was validated");
        sqlx::query(
            r#"
            WITH selected AS (
              SELECT $1::text AS id,
                     ST_SetSRID(ST_MakePoint($2, $3), 4326)::geography AS geom
            )
            SELECT selected.id AS selected_id, candidate.id AS candidate_id,
                   ST_Y(selected.geom::geometry) AS selected_lat,
                   ST_X(selected.geom::geometry) AS selected_lon,
                   candidate.lat AS candidate_lat, candidate.lon AS candidate_lon,
                   ST_Distance(selected.geom, candidate.geom)::integer AS air_distance_meters
            FROM selected
            CROSS JOIN LATERAL (
              SELECT stop.id, stop.lat, stop.lon, stop.geom
              FROM enabled_source_stops stop
              WHERE stop.is_active = true AND stop.geom IS NOT NULL
                AND stop.location_type IN ('stop', 'station')
                AND ST_DWithin(selected.geom, stop.geom, $4)
              ORDER BY selected.geom <-> stop.geom
              LIMIT $5
            ) candidate
            ORDER BY air_distance_meters, candidate.id
            "#,
        )
        .bind(&selected_stop_ids[0])
        .bind(lon)
        .bind(lat)
        .bind(NEARBY_JOURNEY_STOP_RADIUS_M)
        .bind(MAX_NEARBY_JOURNEY_STOPS_PER_ENDPOINT)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            r#"
            WITH selected AS MATERIALIZED (
              SELECT id, lat, lon, geom
              FROM enabled_source_stops
              WHERE id = ANY($1) AND is_active = true AND geom IS NOT NULL
              ORDER BY array_position($1::text[], id) NULLS LAST
              LIMIT 1
            )
            SELECT selected.id AS selected_id, candidate.id AS candidate_id,
                   selected.lat AS selected_lat, selected.lon AS selected_lon,
                   candidate.lat AS candidate_lat, candidate.lon AS candidate_lon,
                   ST_Distance(selected.geom, candidate.geom)::integer AS air_distance_meters
            FROM selected
            CROSS JOIN LATERAL (
              SELECT stop.id, stop.lat, stop.lon, stop.geom
              FROM enabled_source_stops stop
              WHERE stop.is_active = true AND stop.geom IS NOT NULL
                AND stop.location_type IN ('stop', 'station')
                AND stop.id <> ALL($1)
                AND ST_DWithin(selected.geom, stop.geom, $2)
              ORDER BY selected.geom <-> stop.geom
              LIMIT $3
            ) candidate
            ORDER BY air_distance_meters, candidate.id
            "#,
        )
        .bind(selected_stop_ids.to_vec())
        .bind(NEARBY_JOURNEY_STOP_RADIUS_M)
        .bind(MAX_NEARBY_JOURNEY_STOPS_PER_ENDPOINT)
        .fetch_all(pool)
        .await?
    };

    let candidates = rows
        .into_iter()
        .map(|row| WalkingCandidate {
            selected_id: row.get("selected_id"),
            candidate_id: row.get("candidate_id"),
            selected: (row.get("selected_lat"), row.get("selected_lon")),
            candidate: (row.get("candidate_lat"), row.get("candidate_lon")),
        })
        .collect::<Vec<_>>();
    verified_walking_transfers(
        pool,
        pedestrian_router,
        candidates,
        access_to_origin,
        walking_speed_mps,
        MAX_ENDPOINT_WALKING_DISTANCE_M,
        "endpoint_access",
        Some(MAX_NEARBY_JOURNEY_STOPS_PER_ENDPOINT as usize),
    )
    .await
}

#[derive(Debug, Clone)]
struct WalkingCandidate {
    selected_id: String,
    candidate_id: String,
    selected: (f64, f64),
    candidate: (f64, f64),
}

#[allow(clippy::too_many_arguments)]
async fn verified_walking_transfers(
    pool: &PgPool,
    pedestrian_router: &PedestrianRouter,
    candidates: Vec<WalkingCandidate>,
    access_to_origin: bool,
    walking_speed_mps: f64,
    max_distance_meters: u32,
    source: &str,
    limit: Option<usize>,
) -> Result<EndpointAccessResult, sqlx::Error> {
    let mut candidates = VecDeque::from(candidates);
    let mut tasks = tokio::task::JoinSet::new();
    let in_flight_limit = pedestrian_router.permits.available_permits().max(1);
    while tasks.len() < in_flight_limit {
        let Some(candidate) = candidates.pop_front() else {
            break;
        };
        spawn_verified_walking_candidate(
            &mut tasks,
            pool.clone(),
            pedestrian_router.clone(),
            candidate,
            access_to_origin,
            max_distance_meters,
        );
    }

    let mut result = EndpointAccessResult::default();
    while let Some(task) = tasks.join_next().await {
        let (candidate, route) = task.map_err(|error| {
            sqlx::Error::Protocol(format!("walking router task failed: {error}"))
        })?;
        let (from_stop_id, to_stop_id) = if access_to_origin {
            (candidate.selected_id, candidate.candidate_id)
        } else {
            (candidate.candidate_id, candidate.selected_id)
        };
        match route? {
            Ok(route) => result.transfers.push(Transfer {
                from_stop_id,
                to_stop_id,
                min_transfer_seconds: walking_route_seconds(
                    route.distance_meters,
                    walking_speed_mps,
                ),
                distance_meters: Some(route.distance_meters),
                walking_geometry: Some(route.geometry),
                confidence: CoordinateConfidence::High,
                accessibility_level: None,
                source: format!("pedestrian_graph_{source}"),
            }),
            Err(reason) => result.diagnostics.push(format!(
                "walking_candidate_rejected:{}:{}->{}",
                reason.diagnostic_code(),
                from_stop_id,
                to_stop_id
            )),
        }
        if let Some(candidate) = candidates.pop_front() {
            spawn_verified_walking_candidate(
                &mut tasks,
                pool.clone(),
                pedestrian_router.clone(),
                candidate,
                access_to_origin,
                max_distance_meters,
            );
        }
    }
    result
        .transfers
        .sort_by_key(|transfer| transfer.distance_meters.unwrap_or(u32::MAX));
    if let Some(limit) = limit {
        result.transfers.truncate(limit);
    }
    Ok(result)
}

type WalkingCandidateTaskResult = (
    WalkingCandidate,
    Result<Result<WalkingRoute, WalkingRouteRejection>, sqlx::Error>,
);

fn spawn_verified_walking_candidate(
    tasks: &mut tokio::task::JoinSet<WalkingCandidateTaskResult>,
    pool: PgPool,
    router: PedestrianRouter,
    candidate: WalkingCandidate,
    access_to_origin: bool,
    max_distance_meters: u32,
) {
    tasks.spawn(async move {
        let _permit = router
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("pedestrian router semaphore is open");
        let (from, to) = if access_to_origin {
            (candidate.selected, candidate.candidate)
        } else {
            (candidate.candidate, candidate.selected)
        };
        let route = walking_route_cached_db(&pool, &router, from, to, max_distance_meters).await;
        (candidate, route)
    });
}

async fn walking_route_cached_db(
    pool: &PgPool,
    router: &PedestrianRouter,
    from: (f64, f64),
    to: (f64, f64),
    max_distance_meters: u32,
) -> Result<Result<WalkingRoute, WalkingRouteRejection>, sqlx::Error> {
    let key = walking_cache_key(from, to);
    if let Some(row) = sqlx::query(
        r#"
        SELECT status, distance_meters, duration_seconds, geometry
        FROM pedestrian_route_cache
        WHERE from_lat_e5 = $1 AND from_lon_e5 = $2
          AND to_lat_e5 = $3 AND to_lon_e5 = $4
          AND router_revision = $5 AND expires_at > now()
        "#,
    )
    .bind(key.0)
    .bind(key.1)
    .bind(key.2)
    .bind(key.3)
    .bind(&router.revision)
    .fetch_optional(pool)
    .await?
    {
        let status = row.get::<String, _>("status");
        if status == "ok" {
            let route = WalkingRoute {
                distance_meters: row.get::<i32, _>("distance_meters").max(0) as u32,
                duration_seconds: row.get::<i32, _>("duration_seconds").max(0) as u32,
                geometry: row.get("geometry"),
            };
            return Ok(if route.distance_meters <= max_distance_meters {
                Ok(route)
            } else {
                Err(WalkingRouteRejection::ExceededDistance)
            });
        }
        return Ok(Err(match status.as_str() {
            "non_walking_segment" => WalkingRouteRejection::NonWalkingSegment,
            "invalid_geometry" => WalkingRouteRejection::InvalidGeometry,
            _ => WalkingRouteRejection::NoRoute,
        }));
    }

    if router.base_url.is_empty() {
        return Ok(Err(WalkingRouteRejection::RouterUnavailable));
    }
    let payload = match pedestrian_router_payload(router, from, to).await {
        Ok(payload) => payload,
        Err(reason) => return Ok(Err(reason)),
    };
    let decision = match router.engine {
        PedestrianRouterEngine::Osrm => walking_route_from_osrm_payload(&payload, from, to),
        PedestrianRouterEngine::Valhalla => walking_route_from_valhalla_payload(&payload, from, to),
    };
    let (status, route, detail, ttl_seconds) = match &decision {
        Ok(route) => ("ok", Some(route.clone()), None, 30 * 24 * 3600),
        Err(WalkingRouteRejection::NonWalkingSegment) => (
            "non_walking_segment",
            None,
            Some("route contains ferry or another non-walking mode"),
            24 * 3600,
        ),
        Err(WalkingRouteRejection::InvalidGeometry) => (
            "invalid_geometry",
            None,
            Some("router geometry or endpoint snap is invalid"),
            6 * 3600,
        ),
        Err(_) => ("no_route", None, Some("no pedestrian route"), 6 * 3600),
    };
    sqlx::query(
        r#"
        INSERT INTO pedestrian_route_cache (
          from_lat_e5, from_lon_e5, to_lat_e5, to_lon_e5, router_revision,
          status, distance_meters, duration_seconds, geometry, failure_detail, expires_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                now() + ($11::bigint * interval '1 second'))
        ON CONFLICT (from_lat_e5, from_lon_e5, to_lat_e5, to_lon_e5, router_revision)
        DO UPDATE SET status = EXCLUDED.status,
          distance_meters = EXCLUDED.distance_meters,
          duration_seconds = EXCLUDED.duration_seconds,
          geometry = EXCLUDED.geometry,
          failure_detail = EXCLUDED.failure_detail,
          checked_at = now(), expires_at = EXCLUDED.expires_at
        "#,
    )
    .bind(key.0)
    .bind(key.1)
    .bind(key.2)
    .bind(key.3)
    .bind(&router.revision)
    .bind(status)
    .bind(route.as_ref().map(|route| route.distance_meters as i32))
    .bind(route.as_ref().map(|route| route.duration_seconds as i32))
    .bind(route.as_ref().map(|route| route.geometry.clone()))
    .bind(detail)
    .bind(ttl_seconds)
    .execute(pool)
    .await?;

    Ok(match decision {
        Ok(route) if route.distance_meters <= max_distance_meters => Ok(route),
        Ok(_) => Err(WalkingRouteRejection::ExceededDistance),
        Err(reason) => Err(reason),
    })
}

async fn pedestrian_router_payload(
    router: &PedestrianRouter,
    from: (f64, f64),
    to: (f64, f64),
) -> Result<Value, WalkingRouteRejection> {
    const MAX_ATTEMPTS: usize = 3;

    for attempt in 0..MAX_ATTEMPTS {
        let request = match router.engine {
            PedestrianRouterEngine::Osrm => {
                let url = format!(
                    "{}/{},{};{},{}",
                    router.base_url, from.1, from.0, to.1, to.0
                );
                router.client.get(url).query(&[
                    ("overview", "full"),
                    ("geometries", "geojson"),
                    ("steps", "true"),
                ])
            }
            PedestrianRouterEngine::Valhalla => router.client.post(&router.base_url).json(&json!({
                "locations": [
                    {"lat": from.0, "lon": from.1},
                    {"lat": to.0, "lon": to.1}
                ],
                "costing": "pedestrian",
                "costing_options": {"pedestrian": {"use_ferry": 0}},
                "units": "kilometers"
            })),
        };
        match request.send().await {
            Ok(response) if response.status().is_success() => {
                return response.json::<Value>().await.map_err(|error| {
                    tracing::warn!(%error, "pedestrian router returned invalid JSON");
                    WalkingRouteRejection::RouterUnavailable
                });
            }
            Ok(response)
                if (response.status().as_u16() == 429 || response.status().is_server_error())
                    && attempt + 1 < MAX_ATTEMPTS =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(250 * (1_u64 << attempt)))
                    .await;
            }
            Ok(response) => {
                tracing::warn!(
                    status = %response.status(),
                    engine = ?router.engine,
                    "pedestrian router returned an HTTP error"
                );
                return Err(WalkingRouteRejection::RouterUnavailable);
            }
            Err(error) if attempt + 1 < MAX_ATTEMPTS => {
                tracing::warn!(attempt = attempt + 1, %error, "pedestrian router request failed; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(250 * (1_u64 << attempt)))
                    .await;
            }
            Err(error) => {
                tracing::warn!(%error, engine = ?router.engine, "pedestrian router request failed");
                return Err(WalkingRouteRejection::RouterUnavailable);
            }
        }
    }
    Err(WalkingRouteRejection::RouterUnavailable)
}

fn walking_cache_key(from: (f64, f64), to: (f64, f64)) -> (i32, i32, i32, i32) {
    (
        (from.0 * 100_000.0).round() as i32,
        (from.1 * 100_000.0).round() as i32,
        (to.0 * 100_000.0).round() as i32,
        (to.1 * 100_000.0).round() as i32,
    )
}

fn walking_route_from_osrm_payload(
    payload: &Value,
    from: (f64, f64),
    to: (f64, f64),
) -> Result<WalkingRoute, WalkingRouteRejection> {
    if payload["code"].as_str() != Some("Ok") {
        return Err(WalkingRouteRejection::NoRoute);
    }
    let route = payload["routes"]
        .as_array()
        .and_then(|routes| routes.first())
        .ok_or(WalkingRouteRejection::NoRoute)?;
    let step_modes = route["legs"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|leg| leg["steps"].as_array().into_iter().flatten())
        .map(|step| step["mode"].as_str())
        .collect::<Vec<_>>();
    if step_modes.is_empty() || step_modes.iter().any(|mode| *mode != Some("walking")) {
        return Err(WalkingRouteRejection::NonWalkingSegment);
    }
    let geometry = route["geometry"].clone();
    let coordinates = geometry["coordinates"]
        .as_array()
        .filter(|coordinates| coordinates.len() >= 2)
        .ok_or(WalkingRouteRejection::InvalidGeometry)?;
    let first = geojson_position(coordinates.first().unwrap())
        .ok_or(WalkingRouteRejection::InvalidGeometry)?;
    let last = geojson_position(coordinates.last().unwrap())
        .ok_or(WalkingRouteRejection::InvalidGeometry)?;
    if geometry["type"].as_str() != Some("LineString")
        || haversine_m(from.0, from.1, first.1, first.0) > MAX_WALKING_SNAP_DISTANCE_M
        || haversine_m(to.0, to.1, last.1, last.0) > MAX_WALKING_SNAP_DISTANCE_M
    {
        return Err(WalkingRouteRejection::InvalidGeometry);
    }
    let distance_meters = route["distance"]
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or(WalkingRouteRejection::InvalidGeometry)?
        .ceil() as u32;
    let duration_seconds = route["duration"]
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or(distance_meters as f64 / 1.25)
        .ceil() as u32;
    Ok(WalkingRoute {
        distance_meters,
        duration_seconds,
        geometry,
    })
}

fn walking_route_from_valhalla_payload(
    payload: &Value,
    from: (f64, f64),
    to: (f64, f64),
) -> Result<WalkingRoute, WalkingRouteRejection> {
    let trip = payload
        .get("trip")
        .filter(|trip| trip["status"].as_i64() == Some(0))
        .ok_or(WalkingRouteRejection::NoRoute)?;
    let legs = trip["legs"]
        .as_array()
        .filter(|legs| !legs.is_empty())
        .ok_or(WalkingRouteRejection::NoRoute)?;
    if trip["summary"]["has_ferry"].as_bool() == Some(true)
        || legs
            .iter()
            .any(|leg| leg["summary"]["has_ferry"].as_bool() == Some(true))
    {
        return Err(WalkingRouteRejection::NonWalkingSegment);
    }
    let maneuver_modes = legs
        .iter()
        .flat_map(|leg| leg["maneuvers"].as_array().into_iter().flatten())
        .map(|maneuver| maneuver["travel_mode"].as_str())
        .collect::<Vec<_>>();
    if maneuver_modes.is_empty()
        || maneuver_modes
            .iter()
            .any(|mode| *mode != Some("pedestrian"))
    {
        return Err(WalkingRouteRejection::NonWalkingSegment);
    }

    let mut coordinates = Vec::new();
    for leg in legs {
        let encoded = leg["shape"]
            .as_str()
            .ok_or(WalkingRouteRejection::InvalidGeometry)?;
        for position in decode_polyline6(encoded)? {
            if coordinates.last() != Some(&position) {
                coordinates.push(position);
            }
        }
    }
    if coordinates.len() < 2 {
        return Err(WalkingRouteRejection::InvalidGeometry);
    }
    let first = geojson_position(coordinates.first().unwrap())
        .ok_or(WalkingRouteRejection::InvalidGeometry)?;
    let last = geojson_position(coordinates.last().unwrap())
        .ok_or(WalkingRouteRejection::InvalidGeometry)?;
    if haversine_m(from.0, from.1, first.1, first.0) > MAX_WALKING_SNAP_DISTANCE_M
        || haversine_m(to.0, to.1, last.1, last.0) > MAX_WALKING_SNAP_DISTANCE_M
    {
        return Err(WalkingRouteRejection::InvalidGeometry);
    }

    let distance_meters = (trip["summary"]["length"]
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or(WalkingRouteRejection::InvalidGeometry)?
        * 1000.0)
        .ceil() as u32;
    let duration_seconds = trip["summary"]["time"]
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or(distance_meters as f64 / 1.25)
        .ceil() as u32;
    Ok(WalkingRoute {
        distance_meters,
        duration_seconds,
        geometry: json!({"type": "LineString", "coordinates": coordinates}),
    })
}

fn decode_polyline6(encoded: &str) -> Result<Vec<Value>, WalkingRouteRejection> {
    let bytes = encoded.as_bytes();
    let mut index = 0_usize;
    let mut latitude = 0_i64;
    let mut longitude = 0_i64;
    let mut coordinates = Vec::new();
    while index < bytes.len() {
        let latitude_delta = decode_polyline_value(bytes, &mut index)?;
        let longitude_delta = decode_polyline_value(bytes, &mut index)?;
        latitude = latitude
            .checked_add(latitude_delta)
            .ok_or(WalkingRouteRejection::InvalidGeometry)?;
        longitude = longitude
            .checked_add(longitude_delta)
            .ok_or(WalkingRouteRejection::InvalidGeometry)?;
        coordinates.push(json!([
            longitude as f64 / 1_000_000.0,
            latitude as f64 / 1_000_000.0
        ]));
    }
    Ok(coordinates)
}

fn decode_polyline_value(bytes: &[u8], index: &mut usize) -> Result<i64, WalkingRouteRejection> {
    let mut result = 0_i64;
    let mut shift = 0_u32;
    loop {
        let byte = bytes
            .get(*index)
            .copied()
            .filter(|byte| *byte >= 63)
            .ok_or(WalkingRouteRejection::InvalidGeometry)?
            - 63;
        *index += 1;
        result |= i64::from(byte & 0x1f)
            .checked_shl(shift)
            .ok_or(WalkingRouteRejection::InvalidGeometry)?;
        if byte < 0x20 {
            break;
        }
        shift += 5;
        if shift > 60 {
            return Err(WalkingRouteRejection::InvalidGeometry);
        }
    }
    Ok(if result & 1 == 0 {
        result >> 1
    } else {
        -(result >> 1) - 1
    })
}

fn geojson_position(value: &Value) -> Option<(f64, f64)> {
    let values = value.as_array()?;
    let lon = values.first()?.as_f64()?;
    let lat = values.get(1)?.as_f64()?;
    (lon.is_finite() && lat.is_finite()).then_some((lon, lat))
}

fn walking_speed_meters_per_second(value: &str) -> f64 {
    match value.trim().to_ascii_lowercase().as_str() {
        "slow" | "relaxed" | "accessible" => 0.9,
        "fast" => 1.6,
        _ => 1.25,
    }
}

fn walking_route_seconds(distance_meters: u32, walking_speed_mps: f64) -> u32 {
    let speed = walking_speed_mps.clamp(0.5, 2.5);
    (distance_meters as f64 / speed).ceil().max(30.0) as u32
}

fn append_transfer_search_warning(
    warnings: &mut Vec<String>,
    status: TransferSearchStatus,
    next_service_day: bool,
    timeout_seconds: i32,
) {
    let prefix = if next_service_day {
        "next service-day transfer search"
    } else {
        "transfer search"
    };
    match status {
        TransferSearchStatus::Complete => {}
        TransferSearchStatus::TimedOut => warnings.push(format!(
            "{prefix} exceeded the configured {timeout_seconds}s timeout; direct journeys are still included"
        )),
        TransferSearchStatus::Failed => warnings.push(format!(
            "{prefix} failed; direct journeys are still included"
        )),
    }
}

#[allow(clippy::too_many_arguments)]
async fn service_day_journeys_db(
    pool: &PgPool,
    raptor_cache: &RaptorCache,
    pedestrian_router: &PedestrianRouter,
    routing_snapshot_dir: &FsPath,
    revision: &RoutingDataRevision,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
    departure_time: u32,
    mode_filters: &[String],
    max_transfers: u32,
    transfer_buffer_seconds: u32,
    service_date: chrono::NaiveDate,
    routing_config: &RoutingAlgorithmConfig,
    extra_transfers: &[Transfer],
    realtime: Arc<RaptorRealtimeData>,
) -> Result<(Vec<Journey>, TransferSearchStatus, ServiceDaySearchTiming), sqlx::Error> {
    let timetable_started = time::Instant::now();
    let (timetable, memory_cache_hit) = raptor_timetable_cached_for_revision_db(
        pool,
        raptor_cache,
        pedestrian_router,
        routing_snapshot_dir,
        service_date,
        revision,
    )
    .await?;
    let timetable_ms = elapsed_millis(timetable_started);
    service_day_journeys_for_timetable(
        timetable,
        memory_cache_hit,
        timetable_ms,
        from_stop_ids,
        to_stop_ids,
        departure_time,
        mode_filters,
        max_transfers,
        transfer_buffer_seconds,
        service_date,
        routing_config,
        extra_transfers,
        realtime,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn cached_service_day_journeys_db(
    raptor_cache: &RaptorCache,
    revision: &RoutingDataRevision,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
    departure_time: u32,
    mode_filters: &[String],
    max_transfers: u32,
    transfer_buffer_seconds: u32,
    service_date: chrono::NaiveDate,
    routing_config: &RoutingAlgorithmConfig,
    extra_transfers: &[Transfer],
    realtime: Arc<RaptorRealtimeData>,
) -> Result<Option<(Vec<Journey>, TransferSearchStatus, ServiceDaySearchTiming)>, sqlx::Error> {
    let timetable_started = time::Instant::now();
    let Some(timetable) =
        raptor_timetable_memory_cached_for_revision(raptor_cache, service_date, revision).await
    else {
        return Ok(None);
    };
    let timetable_ms = elapsed_millis(timetable_started);
    service_day_journeys_for_timetable(
        timetable,
        true,
        timetable_ms,
        from_stop_ids,
        to_stop_ids,
        departure_time,
        mode_filters,
        max_transfers,
        transfer_buffer_seconds,
        service_date,
        routing_config,
        extra_transfers,
        realtime,
    )
    .await
    .map(Some)
}

#[allow(clippy::too_many_arguments)]
async fn service_day_journeys_for_timetable(
    timetable: Arc<RaptorTimetable>,
    memory_cache_hit: bool,
    timetable_ms: u64,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
    departure_time: u32,
    mode_filters: &[String],
    max_transfers: u32,
    transfer_buffer_seconds: u32,
    service_date: chrono::NaiveDate,
    routing_config: &RoutingAlgorithmConfig,
    extra_transfers: &[Transfer],
    realtime: Arc<RaptorRealtimeData>,
) -> Result<(Vec<Journey>, TransferSearchStatus, ServiceDaySearchTiming), sqlx::Error> {
    let modes = mode_filters
        .iter()
        .map(|mode| db_mode_to_model(mode))
        .collect::<Vec<_>>();
    let raptor_started = time::Instant::now();
    let mut search_result = run_adaptive_raptor_searches(
        timetable.clone(),
        from_stop_ids,
        to_stop_ids,
        extra_transfers,
        departure_time,
        max_transfers,
        routing_config.min_transfer_seconds as u32,
        transfer_buffer_seconds,
        &modes,
        false,
        routing_config,
        realtime.clone(),
    )
    .await?;
    let legacy_search_attempted =
        search_result.journeys.is_empty() && timetable.has_unverified_services();
    if legacy_search_attempted {
        search_result = run_adaptive_raptor_searches(
            timetable.clone(),
            from_stop_ids,
            to_stop_ids,
            extra_transfers,
            departure_time,
            max_transfers,
            routing_config.min_transfer_seconds as u32,
            transfer_buffer_seconds,
            &modes,
            true,
            routing_config,
            realtime,
        )
        .await?;
    }
    let raptor_ms = elapsed_millis(raptor_started);
    tracing::debug!(
        timetable_ms,
        raptor_ms,
        candidates = search_result.journeys.len(),
        trips = timetable.trip_count(),
        route_patterns = timetable.route_count(),
        max_route_trips = timetable.max_route_trip_count(),
        service_date = %service_date,
        "RAPTOR journey search completed"
    );
    let _ = routing_config;
    Ok((
        search_result.journeys,
        TransferSearchStatus::Complete,
        ServiceDaySearchTiming {
            timetable_ms,
            raptor_ms,
            memory_cache_hit,
            trip_count: timetable.trip_count(),
            route_count: timetable.route_count(),
            max_route_trip_count: timetable.max_route_trip_count(),
            range_departure_count: search_result.departure_count,
            range_expanded: search_result.expanded,
            raptor_rounds: search_result.stats.rounds,
            raptor_routes_scanned: search_result.stats.routes_scanned,
            raptor_marked_stops: search_result.stats.marked_stops,
            legacy_search_attempted,
        },
    ))
}

fn routing_cpu_permits() -> Arc<tokio::sync::Semaphore> {
    static PERMITS: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> = std::sync::OnceLock::new();
    PERMITS
        .get_or_init(|| {
            Arc::new(tokio::sync::Semaphore::new(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(2)
                    .clamp(1, 4),
            ))
        })
        .clone()
}

#[derive(Debug)]
struct AdaptiveRaptorSearchResult {
    journeys: Vec<Journey>,
    stats: RaptorSearchStats,
    departure_count: usize,
    expanded: bool,
}

fn geometry_preselection_config(configuration: &RoutingAlgorithmConfig) -> RoutingAlgorithmConfig {
    let mut preselection = configuration.clone();
    preselection.max_results = configuration
        .max_results
        .saturating_mul(2)
        .clamp(configuration.max_results, 40);
    // A timetable winner may have unusable geometry. Keep bounded fallbacks
    // until actual geometry has been validated, then perform final dominance.
    preselection.remove_dominated = false;
    preselection
}

#[allow(clippy::too_many_arguments)]
async fn run_adaptive_raptor_searches(
    timetable: Arc<RaptorTimetable>,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
    extra_transfers: &[Transfer],
    departure_time: u32,
    max_transfers: u32,
    min_transfer_seconds: u32,
    transfer_buffer_seconds: u32,
    modes: &[TransportMode],
    allow_unverified_services: bool,
    routing_config: &RoutingAlgorithmConfig,
    realtime: Arc<RaptorRealtimeData>,
) -> Result<AdaptiveRaptorSearchResult, sqlx::Error> {
    let max_departures = routing_config.max_range_departures.max(1) as usize;
    let initial_departures = timetable.departure_times_from_stops(
        from_stop_ids,
        extra_transfers,
        departure_time,
        routing_config.range_search_window_seconds.max(0) as u32,
        max_departures.min(RAPTOR_INITIAL_RANGE_DEPARTURES),
        modes,
        allow_unverified_services,
    );
    let (mut journeys, mut stats) = run_raptor_searches(
        timetable.clone(),
        from_stop_ids,
        to_stop_ids,
        extra_transfers,
        &initial_departures,
        max_transfers,
        min_transfer_seconds,
        transfer_buffer_seconds,
        modes,
        allow_unverified_services,
        realtime.clone(),
        &HashSet::new(),
    )
    .await?;
    let mut searched_departures = initial_departures.iter().copied().collect::<HashSet<_>>();
    let mut departure_count = initial_departures.len();
    let mut expanded = false;

    let direct_request = RaptorRequest {
        from_stop_ids: from_stop_ids.to_vec(),
        to_stop_ids: to_stop_ids.to_vec(),
        extra_transfers: Vec::new(),
        departure_time,
        max_transfers: 0,
        min_transfer_seconds,
        transfer_buffer_seconds,
        modes: modes.to_vec(),
        allow_unverified_services,
        realtime: realtime.clone(),
    };
    let direct_timetable = timetable.clone();
    let direct_window_seconds = routing_config.range_search_window_seconds.max(0) as u32;
    let max_direct_candidates = routing_config.max_direct_candidates.max(1) as usize;
    let cpu_permit = routing_cpu_permits()
        .acquire_owned()
        .await
        .map_err(|_| sqlx::Error::Protocol("Routing worker pool closed".into()))?;
    let mut direct_candidates = tokio::task::spawn_blocking(move || {
        let _cpu_permit = cpu_permit;
        direct_journeys(
            direct_timetable.as_ref(),
            &direct_request,
            direct_window_seconds,
            max_direct_candidates,
        )
    })
    .await
    .map_err(|error| sqlx::Error::Protocol(format!("direct journey worker failed: {error}")))?;
    journeys.append(&mut direct_candidates);

    if searched_departures.len() < max_departures {
        let remaining_departures = timetable
            .departure_times_from_stops(
                from_stop_ids,
                extra_transfers,
                departure_time,
                routing_config.range_search_window_seconds.max(0) as u32,
                max_departures,
                modes,
                allow_unverified_services,
            )
            .into_iter()
            .filter(|departure| searched_departures.insert(*departure))
            .collect::<Vec<_>>();
        for departure_batch in remaining_departures.chunks(RAPTOR_RANGE_EXPANSION_BATCH_DEPARTURES)
        {
            let (mut extra_journeys, extra_stats) = run_raptor_searches(
                timetable.clone(),
                from_stop_ids,
                to_stop_ids,
                extra_transfers,
                departure_batch,
                max_transfers,
                min_transfer_seconds,
                transfer_buffer_seconds,
                modes,
                allow_unverified_services,
                realtime.clone(),
                &HashSet::new(),
            )
            .await?;
            journeys.append(&mut extra_journeys);
            stats.rounds += extra_stats.rounds;
            stats.routes_scanned += extra_stats.routes_scanned;
            stats.marked_stops += extra_stats.marked_stops;
            departure_count += departure_batch.len();
            expanded = true;
        }
    }

    if should_expand_raptor_range(distinct_raptor_candidate_count(&journeys), routing_config) {
        let mut excluded_route_ids = journeys
            .iter()
            .min_by_key(|journey| (journey.arrival_time, journey.transfer_count))
            .into_iter()
            .flat_map(|journey| journey.legs.iter())
            .filter_map(|leg| leg.route_id.clone())
            .collect::<HashSet<_>>();
        if !excluded_route_ids.is_empty() {
            let mut alternative_departures = searched_departures.into_iter().collect::<Vec<_>>();
            alternative_departures.sort_unstable();
            for _ in 0..RAPTOR_ALTERNATIVE_ROUTE_PASSES {
                let (mut alternatives, alternative_stats) = run_raptor_searches(
                    timetable.clone(),
                    from_stop_ids,
                    to_stop_ids,
                    extra_transfers,
                    &alternative_departures,
                    max_transfers,
                    min_transfer_seconds,
                    transfer_buffer_seconds,
                    modes,
                    allow_unverified_services,
                    realtime.clone(),
                    &excluded_route_ids,
                )
                .await?;
                let next_route_to_exclude = alternatives
                    .iter()
                    .min_by_key(|journey| (journey.arrival_time, journey.transfer_count))
                    .into_iter()
                    .flat_map(|journey| journey.legs.iter())
                    .filter_map(|leg| leg.route_id.as_ref())
                    .find(|route_id| !excluded_route_ids.contains(*route_id))
                    .cloned();
                journeys.append(&mut alternatives);
                stats.rounds += alternative_stats.rounds;
                stats.routes_scanned += alternative_stats.routes_scanned;
                stats.marked_stops += alternative_stats.marked_stops;
                departure_count += alternative_departures.len();
                expanded = true;
                if !should_expand_raptor_range(
                    distinct_raptor_candidate_count(&journeys),
                    routing_config,
                ) {
                    break;
                }
                let Some(route_id) = next_route_to_exclude else {
                    break;
                };
                excluded_route_ids.insert(route_id);
            }
        }
    }

    Ok(AdaptiveRaptorSearchResult {
        journeys,
        stats,
        departure_count,
        expanded,
    })
}

fn distinct_raptor_candidate_count(journeys: &[Journey]) -> usize {
    let Some(best) = journeys
        .iter()
        .min_by_key(|journey| (journey.arrival_time, journey.transfer_count))
    else {
        return 0;
    };
    let best_route = journey_route_signature(best);
    journeys
        .iter()
        .filter(|journey| {
            let route = journey_route_signature(journey);
            route == best_route
                || reasonable_distinct_route_alternative(journey, best, &route, &best_route)
        })
        .map(journey_route_signature)
        .collect::<HashSet<_>>()
        .len()
}

fn should_expand_raptor_range(
    candidate_count: usize,
    routing_config: &RoutingAlgorithmConfig,
) -> bool {
    candidate_count < range_expansion_candidate_floor(routing_config)
}

fn should_search_next_service_day_for_candidates(
    candidate_count: usize,
    routing_config: &RoutingAlgorithmConfig,
) -> bool {
    candidate_count < range_expansion_candidate_floor(routing_config)
}

fn range_expansion_candidate_floor(routing_config: &RoutingAlgorithmConfig) -> usize {
    (routing_config.max_results as usize).clamp(1, RAPTOR_RANGE_EXPANSION_MIN_CANDIDATES)
}

#[allow(clippy::too_many_arguments)]
async fn run_raptor_searches(
    timetable: Arc<RaptorTimetable>,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
    extra_transfers: &[Transfer],
    departure_times: &[u32],
    max_transfers: u32,
    min_transfer_seconds: u32,
    transfer_buffer_seconds: u32,
    modes: &[TransportMode],
    allow_unverified_services: bool,
    realtime: Arc<RaptorRealtimeData>,
    excluded_route_ids: &HashSet<String>,
) -> Result<(Vec<Journey>, RaptorSearchStats), sqlx::Error> {
    let mut journeys = Vec::new();
    let mut stats = RaptorSearchStats::default();
    let mut join_set = tokio::task::JoinSet::new();
    let mut departure_times = departure_times.iter().copied();

    loop {
        while join_set.len() < RAPTOR_RANGE_SEARCH_CONCURRENCY {
            let Some(departure_time) = departure_times.next() else {
                break;
            };
            let request = RaptorRequest {
                from_stop_ids: from_stop_ids.to_vec(),
                to_stop_ids: to_stop_ids.to_vec(),
                extra_transfers: extra_transfers.to_vec(),
                departure_time,
                max_transfers,
                min_transfer_seconds,
                transfer_buffer_seconds,
                modes: modes.to_vec(),
                allow_unverified_services,
                realtime: realtime.clone(),
            };
            let timetable = timetable.clone();
            let excluded_route_ids = excluded_route_ids.clone();
            let cpu_permit = routing_cpu_permits()
                .acquire_owned()
                .await
                .map_err(|_| sqlx::Error::Protocol("Routing worker pool closed".into()))?;
            join_set.spawn_blocking(move || {
                let _cpu_permit = cpu_permit;
                raptor_with_stats_excluding_routes(timetable.as_ref(), request, &excluded_route_ids)
            });
        }

        let Some(result) = join_set.join_next().await else {
            break;
        };
        let mut found = result
            .map_err(|error| sqlx::Error::Protocol(format!("RAPTOR worker failed: {error}")))?;
        stats.rounds += found.stats.rounds;
        stats.routes_scanned += found.stats.routes_scanned;
        stats.marked_stops += found.stats.marked_stops;
        journeys.append(&mut found.journeys);
    }
    Ok((journeys, stats))
}

#[derive(Debug, Clone, Copy)]
struct ServiceDaySearchTiming {
    timetable_ms: u64,
    raptor_ms: u64,
    memory_cache_hit: bool,
    trip_count: usize,
    route_count: usize,
    max_route_trip_count: usize,
    range_departure_count: usize,
    range_expanded: bool,
    raptor_rounds: usize,
    raptor_routes_scanned: usize,
    raptor_marked_stops: usize,
    legacy_search_attempted: bool,
}

async fn raptor_timetable_cached_db(
    pool: &PgPool,
    cache: &RaptorCache,
    pedestrian_router: &PedestrianRouter,
    routing_snapshot_dir: &FsPath,
    service_date: chrono::NaiveDate,
) -> Result<Arc<RaptorTimetable>, sqlx::Error> {
    let revision = routing_data_revision(pool).await?;
    let (timetable, _) = raptor_timetable_cached_for_revision_db(
        pool,
        cache,
        pedestrian_router,
        routing_snapshot_dir,
        service_date,
        &revision,
    )
    .await?;
    let snapshot_path =
        raptor_timetable_snapshot_path(routing_snapshot_dir, service_date, &revision);
    ensure_raptor_timetable_snapshot(&snapshot_path, service_date, &revision, timetable.as_ref())
        .await
        .map_err(|error| {
            sqlx::Error::Protocol(format!(
                "RAPTOR snapshot persistence failed for {service_date}: {error}"
            ))
        })?;
    Ok(timetable)
}

async fn raptor_timetable_cached_for_revision_db(
    pool: &PgPool,
    cache: &RaptorCache,
    pedestrian_router: &PedestrianRouter,
    routing_snapshot_dir: &FsPath,
    service_date: chrono::NaiveDate,
    revision: &RoutingDataRevision,
) -> Result<(Arc<RaptorTimetable>, bool), sqlx::Error> {
    let key = (service_date, revision.token.clone());
    let (cell, memory_cache_hit) = {
        let mut cache = cache.write().await;
        cache.retain(|(date, token), _| *date != service_date || *token == revision.token);
        let cell = cache
            .entry(key)
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();
        let hit = cell.get().is_some();
        (cell, hit)
    };
    let snapshot_path =
        raptor_timetable_snapshot_path(routing_snapshot_dir, service_date, revision);
    let timetable = cell
        .get_or_try_init(|| async {
            let started_at = time::Instant::now();
            if let Some(timetable) =
                load_raptor_timetable_snapshot(&snapshot_path, service_date, revision).await
            {
                tracing::info!(
                    elapsed_ms = started_at.elapsed().as_millis(),
                    trips = timetable.trip_count(),
                    service_date = %service_date,
                    path = %snapshot_path.display(),
                    "loaded RAPTOR timetable snapshot"
                );
                return Ok::<Arc<RaptorTimetable>, sqlx::Error>(Arc::new(timetable));
            }

            let db_started_at = time::Instant::now();
            let timetable = raptor_timetable_db(pool, pedestrian_router, service_date).await?;
            tracing::info!(
                elapsed_ms = db_started_at.elapsed().as_millis(),
                trips = timetable.trip_count(),
                service_date = %service_date,
                "built RAPTOR timetable from database"
            );
            if let Err(error) =
                write_raptor_timetable_snapshot(&snapshot_path, service_date, revision, &timetable)
                    .await
            {
                tracing::warn!(
                    %error,
                    service_date = %service_date,
                    path = %snapshot_path.display(),
                    "failed to persist RAPTOR timetable snapshot; routing will continue from memory"
                );
            }
            Ok::<Arc<RaptorTimetable>, sqlx::Error>(Arc::new(timetable))
        })
        .await
        .cloned()?;
    Ok((timetable, memory_cache_hit))
}

async fn raptor_timetable_memory_cached_for_revision(
    cache: &RaptorCache,
    service_date: chrono::NaiveDate,
    revision: &RoutingDataRevision,
) -> Option<Arc<RaptorTimetable>> {
    let key = (service_date, revision.token.clone());
    cache
        .read()
        .await
        .get(&key)
        .and_then(|cell| cell.get().cloned())
}

async fn warm_raptor_timetables(
    pool: PgPool,
    cache: RaptorCache,
    routing_snapshot_dir: PathBuf,
    routing_snapshot_files_to_keep: usize,
    warmup_status: RoutingWarmupStatus,
    pedestrian_router: PedestrianRouter,
) {
    loop {
        let service_date = Utc::now()
            .with_timezone(&chrono_tz::Europe::Prague)
            .date_naive();
        let pass_started_at = Utc::now();
        let mut last_error = None;
        for offset_days in 0..=1 {
            let warmup_date = service_date
                .checked_add_days(chrono::Days::new(offset_days))
                .unwrap_or(service_date);
            {
                let mut status = warmup_status.write().await;
                *status = RoutingWarmupState {
                    active: true,
                    stage: "loading_or_building_snapshot".to_string(),
                    service_date: Some(warmup_date),
                    current_index: Some(offset_days as u32 + 1),
                    total_dates: 2,
                    started_at: Some(pass_started_at),
                    finished_at: None,
                    error: None,
                };
            }
            if let Err(error) = raptor_timetable_cached_db(
                &pool,
                &cache,
                &pedestrian_router,
                &routing_snapshot_dir,
                warmup_date,
            )
            .await
            {
                last_error = Some(error.to_string());
                tracing::warn!(%error, service_date = %warmup_date, "background RAPTOR timetable warmup failed");
            }
        }
        {
            let mut status = warmup_status.write().await;
            let started_at = status.started_at;
            *status = RoutingWarmupState {
                active: false,
                stage: if last_error.is_some() {
                    "idle_after_error".to_string()
                } else {
                    "idle".to_string()
                },
                service_date: None,
                current_index: None,
                total_dates: 2,
                started_at,
                finished_at: Some(Utc::now()),
                error: last_error,
            };
        }
        match prune_raptor_snapshots(&routing_snapshot_dir, routing_snapshot_files_to_keep).await {
            Ok(removed) if removed > 0 => tracing::info!(
                removed,
                files_to_keep = routing_snapshot_files_to_keep,
                directory = %routing_snapshot_dir.display(),
                "deleted unused RAPTOR snapshots"
            ),
            Ok(_) => {}
            Err(error) => tracing::warn!(
                %error,
                directory = %routing_snapshot_dir.display(),
                "failed to clean RAPTOR snapshots"
            ),
        }
        tokio::time::sleep(std::time::Duration::from_secs(
            RAPTOR_WARMUP_INTERVAL_SECONDS,
        ))
        .await;
    }
}

async fn refresh_routing_realtime(pool: &PgPool, cache: &RoutingRealtimeCache) -> bool {
    let service_date = Utc::now()
        .with_timezone(&chrono_tz::Europe::Prague)
        .date_naive();
    match journey_routing_realtime_db(pool, service_date).await {
        Ok(data) => {
            let trip_count = data.trip_count();
            *cache.write().await = Some(RoutingRealtimeCacheEntry {
                service_date,
                loaded_at: time::Instant::now(),
                data,
            });
            tracing::debug!(
                %service_date,
                trip_count,
                "refreshed background routing realtime cache"
            );
            true
        }
        Err(error) => {
            tracing::warn!(
                %error,
                %service_date,
                "background routing realtime refresh failed; retaining previous cache"
            );
            false
        }
    }
}

async fn warm_routing_realtime(pool: PgPool, cache: RoutingRealtimeCache, initially_ready: bool) {
    if initially_ready {
        tokio::time::sleep(std::time::Duration::from_secs(
            ROUTING_REALTIME_REFRESH_INTERVAL_SECONDS,
        ))
        .await;
    }
    loop {
        refresh_routing_realtime(&pool, &cache).await;
        tokio::time::sleep(std::time::Duration::from_secs(
            ROUTING_REALTIME_REFRESH_INTERVAL_SECONDS,
        ))
        .await;
    }
}

async fn routing_data_revision(pool: &PgPool) -> Result<RoutingDataRevision, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT
          (SELECT max(finished_at) FROM import_runs WHERE status = 'success') AS latest_import,
          (SELECT max(finished_at) FROM data_repair_runs WHERE status = 'completed') AS latest_repair,
          COALESCE((
            SELECT jsonb_agg(
              jsonb_build_array(feed.id, feed.enabled, latest.import_run_id)
              ORDER BY feed.id
            )
            FROM source_feeds feed
            LEFT JOIN LATERAL (
              SELECT run.id AS import_run_id
              FROM import_runs run
              WHERE run.status = 'success'
                AND run.summary->>'feed_id' = feed.id
              ORDER BY run.finished_at DESC NULLS LAST, run.started_at DESC, run.id DESC
              LIMIT 1
            ) latest ON true
          ), '[]'::jsonb) AS source_state
        "#,
    )
    .fetch_one(pool)
    .await?;
    let latest_import = row.get::<Option<DateTime<Utc>>, _>("latest_import");
    let latest_repair = row.get::<Option<DateTime<Utc>>, _>("latest_repair");
    let source_state = row.get::<Value, _>("source_state");
    let mut digest = Sha256::new();
    if let Some(latest_import) = latest_import {
        digest.update(latest_import.timestamp_millis().to_be_bytes());
    }
    if let Some(latest_repair) = latest_repair {
        digest.update(latest_repair.timestamp_millis().to_be_bytes());
    }
    digest.update(source_state.to_string().as_bytes());
    let token = hex::encode(digest.finalize())[..16].to_string();
    Ok(RoutingDataRevision {
        latest_import,
        token,
    })
}

fn raptor_timetable_snapshot_path(
    routing_snapshot_dir: &FsPath,
    service_date: chrono::NaiveDate,
    revision: &RoutingDataRevision,
) -> PathBuf {
    let import_token = revision
        .latest_import
        .map(|value| value.timestamp_millis().to_string())
        .unwrap_or_else(|| "no-successful-import".to_string());
    routing_snapshot_dir.join(format!(
        "raptor-v{RAPTOR_TIMETABLE_SNAPSHOT_VERSION}-{service_date}-{import_token}-{}.json",
        revision.token
    ))
}

fn raptor_snapshot_file_metadata(file_name: &str) -> Option<(u32, chrono::NaiveDate, bool)> {
    let remainder = file_name.strip_prefix("raptor-v")?;
    let (version, suffix) = remainder.split_once('-')?;
    let temporary = suffix.ends_with(".json.tmp");
    if !suffix.ends_with(".json") && !temporary {
        return None;
    }
    let service_date = chrono::NaiveDate::parse_from_str(suffix.get(..10)?, "%Y-%m-%d").ok()?;
    Some((version.parse().ok()?, service_date, temporary))
}

#[derive(Debug)]
struct RaptorSnapshotCandidate {
    path: PathBuf,
    service_date: chrono::NaiveDate,
    modified: std::time::SystemTime,
}

async fn prune_raptor_snapshots(
    routing_snapshot_dir: &FsPath,
    files_to_keep: usize,
) -> Result<usize, std::io::Error> {
    let mut entries = match tokio::fs::read_dir(routing_snapshot_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut obsolete_paths = HashSet::new();
    let mut candidates = Vec::new();
    let now = std::time::SystemTime::now();
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some((version, service_date, temporary)) = raptor_snapshot_file_metadata(file_name)
        else {
            continue;
        };
        if version > RAPTOR_TIMETABLE_SNAPSHOT_VERSION {
            continue;
        }
        if version < RAPTOR_TIMETABLE_SNAPSHOT_VERSION {
            obsolete_paths.insert(entry.path());
            continue;
        }
        let metadata = entry.metadata().await?;
        let modified = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);
        if temporary {
            let stale = now.duration_since(modified).unwrap_or_default()
                >= std::time::Duration::from_secs(60 * 60);
            if stale {
                obsolete_paths.insert(entry.path());
            }
            continue;
        }
        candidates.push(RaptorSnapshotCandidate {
            path: entry.path(),
            service_date,
            modified,
        });
    }

    candidates.sort_by(|left, right| {
        left.service_date
            .cmp(&right.service_date)
            .then_with(|| right.modified.cmp(&left.modified))
            .then_with(|| right.path.cmp(&left.path))
    });
    let mut dates = HashSet::new();
    let mut newest_by_date = Vec::new();
    for candidate in candidates {
        if dates.insert(candidate.service_date) {
            newest_by_date.push(candidate);
        } else {
            obsolete_paths.insert(candidate.path);
        }
    }

    let today = Utc::now()
        .with_timezone(&chrono_tz::Europe::Prague)
        .date_naive();
    let tomorrow = today.succ_opt().unwrap_or(today);
    let protected_count = newest_by_date
        .iter()
        .filter(|candidate| candidate.service_date == today || candidate.service_date == tomorrow)
        .count();
    let ordinary_to_keep = files_to_keep.max(2).saturating_sub(protected_count);
    let mut ordinary = newest_by_date
        .into_iter()
        .filter(|candidate| candidate.service_date != today && candidate.service_date != tomorrow)
        .collect::<Vec<_>>();
    ordinary.sort_by(|left, right| {
        right
            .modified
            .cmp(&left.modified)
            .then_with(|| right.path.cmp(&left.path))
    });
    for candidate in ordinary.into_iter().skip(ordinary_to_keep) {
        obsolete_paths.insert(candidate.path);
    }

    let mut removed = 0;
    for path in obsolete_paths {
        match tokio::fs::remove_file(&path).await {
            Ok(()) => removed += 1,
            Err(error) => tracing::warn!(
                %error,
                path = %path.display(),
                "failed to delete obsolete RAPTOR snapshot"
            ),
        }
    }
    Ok(removed)
}

async fn load_raptor_timetable_snapshot(
    path: &FsPath,
    service_date: chrono::NaiveDate,
    revision: &RoutingDataRevision,
) -> Option<RaptorTimetable> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::debug!(%error, path = %path.display(), "RAPTOR timetable snapshot not available");
            return None;
        }
    };
    let snapshot = match serde_json::from_slice::<RaptorTimetableSnapshot>(&bytes) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(%error, path = %path.display(), "RAPTOR timetable snapshot is unreadable");
            return None;
        }
    };
    if snapshot.version != RAPTOR_TIMETABLE_SNAPSHOT_VERSION
        || snapshot.service_date != service_date
        || snapshot.latest_import != revision.latest_import
        || snapshot.revision_token != revision.token
    {
        tracing::warn!(
            path = %path.display(),
            "RAPTOR timetable snapshot metadata did not match requested cache key"
        );
        return None;
    }
    Some(snapshot.timetable)
}

async fn write_raptor_timetable_snapshot(
    path: &FsPath,
    service_date: chrono::NaiveDate,
    revision: &RoutingDataRevision,
    timetable: &RaptorTimetable,
) -> anyhow::Result<()> {
    let snapshot = RaptorTimetableSnapshot {
        version: RAPTOR_TIMETABLE_SNAPSHOT_VERSION,
        service_date,
        latest_import: revision.latest_import,
        revision_token: revision.token.clone(),
        timetable: timetable.clone(),
    };
    let bytes = serde_json::to_vec(&snapshot)
        .map_err(|error| anyhow::anyhow!("failed to serialize timetable: {error}"))?;
    let parent = path.parent().ok_or_else(|| {
        anyhow::anyhow!("snapshot path '{}' has no parent directory", path.display())
    })?;
    tokio::fs::create_dir_all(parent).await.map_err(|error| {
        anyhow::anyhow!(
            "failed to create snapshot directory '{}': {error}",
            parent.display()
        )
    })?;
    let temporary_path = path.with_extension("json.tmp");
    tokio::fs::write(&temporary_path, bytes)
        .await
        .map_err(|error| {
            anyhow::anyhow!(
                "failed to write temporary snapshot '{}': {error}",
                temporary_path.display()
            )
        })?;
    if let Err(error) = tokio::fs::rename(&temporary_path, path).await {
        let _ = tokio::fs::remove_file(path).await;
        if let Err(second_error) = tokio::fs::rename(&temporary_path, path).await {
            let _ = tokio::fs::remove_file(&temporary_path).await;
            return Err(anyhow::anyhow!(
                "failed to publish snapshot '{}': {error}; retry failed: {second_error}",
                path.display()
            ));
        }
    }
    tracing::info!(
        path = %path.display(),
        service_date = %service_date,
        trips = timetable.trip_count(),
        "wrote RAPTOR timetable snapshot"
    );
    Ok(())
}

async fn ensure_raptor_timetable_snapshot(
    path: &FsPath,
    service_date: chrono::NaiveDate,
    revision: &RoutingDataRevision,
    timetable: &RaptorTimetable,
) -> anyhow::Result<bool> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.is_file() && metadata.len() > 0 => return Ok(false),
        Ok(_) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) => {}
        Err(error) => {
            return Err(anyhow::anyhow!(
                "failed to inspect snapshot '{}': {error}",
                path.display()
            ));
        }
    }
    write_raptor_timetable_snapshot(path, service_date, revision, timetable).await?;
    Ok(true)
}

async fn raptor_timetable_db(
    pool: &PgPool,
    pedestrian_router: &PedestrianRouter,
    service_date: chrono::NaiveDate,
) -> Result<RaptorTimetable, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        WITH active_services AS (
          SELECT calendar.service_id
          FROM calendars calendar
          WHERE $1::date BETWEEN calendar.start_date AND calendar.end_date
            AND CASE EXTRACT(ISODOW FROM $1::date)::integer
              WHEN 1 THEN calendar.monday WHEN 2 THEN calendar.tuesday
              WHEN 3 THEN calendar.wednesday WHEN 4 THEN calendar.thursday
              WHEN 5 THEN calendar.friday WHEN 6 THEN calendar.saturday
              WHEN 7 THEN calendar.sunday
            END
            AND NOT EXISTS (
              SELECT 1 FROM calendar_dates exception
              WHERE exception.service_id = calendar.service_id
                AND exception.date = $1::date AND exception.exception_type = 2
            )
          UNION
          SELECT service_id FROM calendar_dates
          WHERE date = $1::date AND exception_type = 1
        ),
        latest_import_runs AS (
          SELECT DISTINCT ON (summary->>'feed_id')
            summary->>'feed_id' AS source_feed_id, id AS import_run_id
          FROM import_runs
          WHERE status = 'success' AND summary ? 'feed_id'
          ORDER BY summary->>'feed_id', finished_at DESC NULLS LAST, started_at DESC
        )
        SELECT trip.id AS trip_id, route.id AS route_id, route.mode,
               route.gtfs_route_type, stop_time.stop_id, stop_time.stop_sequence,
               stop_time.arrival_time, stop_time.departure_time,
               stop_time.pickup_type, stop_time.drop_off_type,
               trip.service_id IN (SELECT service_id FROM active_services) AS service_verified
        FROM trips trip
        JOIN routes route ON route.id = trip.route_id AND route.is_active = true
        JOIN source_feeds feed ON feed.id = trip.source_feed_id AND feed.enabled = true
        JOIN stop_times stop_time ON stop_time.trip_id = trip.id
        JOIN latest_import_runs latest
          ON latest.source_feed_id = trip.source_feed_id
         AND latest.import_run_id = trip.import_run_id
        WHERE (
          trip.service_id IN (SELECT service_id FROM active_services)
          OR (
            NOT EXISTS (
              SELECT 1 FROM calendars
              WHERE source_feed_id = trip.source_feed_id
            )
            AND NOT EXISTS (
              SELECT 1 FROM calendar_dates
              WHERE source_feed_id = trip.source_feed_id
            )
          )
        )
        ORDER BY trip.id, stop_time.stop_sequence
        "#,
    )
    .bind(service_date)
    .fetch_all(pool)
    .await?;

    let mut trips = Vec::<RaptorTrip>::new();
    for row in rows {
        let trip_id = row.get::<String, _>("trip_id");
        if trips.last().is_none_or(|trip| trip.trip_id != trip_id) {
            let mode = db_route_mode_to_model(
                &row.get::<String, _>("mode"),
                row.get::<Option<i32>, _>("gtfs_route_type"),
            );
            trips.push(RaptorTrip {
                trip_id: trip_id.clone(),
                route_id: row.get("route_id"),
                mode,
                service_verified: row.get("service_verified"),
                stop_times: Vec::new(),
            });
        }
        trips.last_mut().unwrap().stop_times.push(RaptorStopTime {
            stop_id: row.get("stop_id"),
            arrival_time: row.get::<i32, _>("arrival_time") as u32,
            departure_time: row.get::<i32, _>("departure_time") as u32,
            // GTFS values 2 and 3 still permit boarding/alighting (with advance
            // coordination). PID uses value 3 for most regional stop calls.
            pickup_allowed: gtfs_stop_action_allowed(row.get::<Option<i16>, _>("pickup_type")),
            drop_off_allowed: gtfs_stop_action_allowed(row.get::<Option<i16>, _>("drop_off_type")),
        });
    }

    let mut transfers: Vec<Transfer> = sqlx::query(
        r#"
        SELECT transfer.from_stop_id, transfer.to_stop_id,
               transfer.min_transfer_seconds, transfer.distance_meters,
               transfer.walking_geometry, transfer.confidence,
               transfer.accessibility_level, transfer.source
        FROM transfers transfer
        JOIN enabled_source_stops origin ON origin.id = transfer.from_stop_id AND origin.is_active = true
        JOIN enabled_source_stops destination ON destination.id = transfer.to_stop_id AND destination.is_active = true
        "#,
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| Transfer {
        from_stop_id: row.get("from_stop_id"),
        to_stop_id: row.get("to_stop_id"),
        min_transfer_seconds: row.get::<i32, _>("min_transfer_seconds") as u32,
        distance_meters: row.get::<Option<i32>, _>("distance_meters").map(|value| value as u32),
        walking_geometry: row.get("walking_geometry"),
        confidence: db_confidence_to_model(&row.get::<String, _>("confidence")),
        accessibility_level: row.get("accessibility_level"),
        source: row.get("source"),
    })
    .collect();
    let mut route_ids_by_stop = HashMap::<String, HashSet<String>>::new();
    for trip in &trips {
        for stop_time in &trip.stop_times {
            route_ids_by_stop
                .entry(stop_time.stop_id.clone())
                .or_default()
                .insert(trip.route_id.clone());
        }
    }
    transfers
        .extend(implicit_station_transfers_db(pool, pedestrian_router, &route_ids_by_stop).await?);
    transfers.sort_by(|left, right| {
        left.from_stop_id
            .cmp(&right.from_stop_id)
            .then_with(|| left.to_stop_id.cmp(&right.to_stop_id))
            .then_with(|| {
                (left.source == "implicit_station_interchange")
                    .cmp(&(right.source == "implicit_station_interchange"))
            })
            .then_with(|| left.min_transfer_seconds.cmp(&right.min_transfer_seconds))
    });
    transfers.dedup_by(|left, right| {
        left.from_stop_id == right.from_stop_id && left.to_stop_id == right.to_stop_id
    });
    Ok(RaptorTimetable::new(trips, transfers))
}

fn gtfs_stop_action_allowed(value: Option<i16>) -> bool {
    matches!(value.unwrap_or(0), 0 | 2 | 3)
}

async fn implicit_station_transfers_db(
    pool: &PgPool,
    pedestrian_router: &PedestrianRouter,
    route_ids_by_stop: &HashMap<String, HashSet<String>>,
) -> Result<Vec<Transfer>, sqlx::Error> {
    if route_ids_by_stop.is_empty() {
        return Ok(Vec::new());
    }
    let stop_ids = route_ids_by_stop.keys().cloned().collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT id, name, municipality, lat, lon, stop_area_id, parent_station_id,
               platform_code, modes
        FROM enabled_source_stops
        WHERE is_active = true AND id = ANY($1)
        "#,
    )
    .bind(stop_ids)
    .fetch_all(pool)
    .await?;

    let mut stops_by_signature =
        HashMap::<String, Vec<(String, String, (f64, f64), Vec<String>, HashSet<String>)>>::new();
    for row in rows {
        let id = row.get::<String, _>("id");
        let source_name = row.get::<String, _>("name");
        let platform_code = row.get::<Option<String>, _>("platform_code");
        let public_name = pid_public_stop_name(&id, &source_name, platform_code.as_deref());
        let modes = row.get::<Vec<String>, _>("modes");
        let signature = implicit_station_transfer_signature(
            &id,
            &source_name,
            row.get::<Option<String>, _>("municipality").as_deref(),
            row.get::<Option<f64>, _>("lat"),
            row.get::<Option<f64>, _>("lon"),
            row.get::<Option<String>, _>("stop_area_id").as_deref(),
            row.get::<Option<String>, _>("parent_station_id").as_deref(),
            platform_code.as_deref(),
            &modes,
        );
        if let (Some(signature), Some(lat), Some(lon)) = (
            signature,
            row.get::<Option<f64>, _>("lat"),
            row.get::<Option<f64>, _>("lon"),
        ) {
            stops_by_signature.entry(signature).or_default().push((
                id.clone(),
                canonical_stop_name_parts(
                    &public_name,
                    row.get::<Option<String>, _>("municipality").as_deref(),
                ),
                (lat, lon),
                modes,
                route_ids_by_stop.get(&id).cloned().unwrap_or_default(),
            ));
        }
    }

    let mut candidates = Vec::new();
    let mut internal_transfers = Vec::new();
    for mut group in stops_by_signature.into_values() {
        group.sort_by(|left, right| left.0.cmp(&right.0));
        group.dedup_by(|left, right| left.0 == right.0);
        if group.len() < 2 || group.len() > 80 {
            continue;
        }
        for (from_stop_id, from_name, from_coordinate, from_modes, from_route_ids) in &group {
            for (to_stop_id, to_name, to_coordinate, to_modes, to_route_ids) in &group {
                if from_stop_id == to_stop_id
                    || !station_interchange_needs_connector(
                        from_modes,
                        from_route_ids,
                        to_modes,
                        to_route_ids,
                    )
                {
                    continue;
                }
                let distance_meters = haversine_m(
                    from_coordinate.0,
                    from_coordinate.1,
                    to_coordinate.0,
                    to_coordinate.1,
                )
                .round()
                .max(0.0) as u32;
                if from_name == to_name
                    && from_modes == to_modes
                    && distance_meters <= MAX_INTERCHANGE_WALKING_DISTANCE_M
                {
                    internal_transfers.push(Transfer {
                        from_stop_id: from_stop_id.clone(),
                        to_stop_id: to_stop_id.clone(),
                        min_transfer_seconds: MIN_STATION_INTERCHANGE_SECONDS,
                        distance_meters: Some(distance_meters),
                        walking_geometry: Some(json!({
                            "type": "LineString",
                            "coordinates": [
                                [from_coordinate.1, from_coordinate.0],
                                [to_coordinate.1, to_coordinate.0]
                            ]
                        })),
                        confidence: CoordinateConfidence::High,
                        accessibility_level: None,
                        source: "implicit_station_internal".to_string(),
                    });
                    continue;
                }
                candidates.push(WalkingCandidate {
                    selected_id: from_stop_id.clone(),
                    candidate_id: to_stop_id.clone(),
                    selected: *from_coordinate,
                    candidate: *to_coordinate,
                });
            }
        }
    }
    let mut result = verified_walking_transfers(
        pool,
        pedestrian_router,
        candidates,
        true,
        1.25,
        MAX_INTERCHANGE_WALKING_DISTANCE_M,
        "station_interchange",
        None,
    )
    .await?;
    if result
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.contains("walking_router_unavailable"))
    {
        return Err(sqlx::Error::Protocol(
            "pedestrian router unavailable while verifying station transfer graph".to_string(),
        ));
    }
    for transfer in &mut result.transfers {
        transfer.min_transfer_seconds = transfer
            .min_transfer_seconds
            .max(MIN_STATION_INTERCHANGE_SECONDS);
    }
    result.transfers.extend(internal_transfers);
    for diagnostic in result.diagnostics {
        tracing::info!(
            reason = %diagnostic,
            "unconnected_complex: station interchange candidate rejected"
        );
    }
    Ok(result.transfers)
}

fn station_interchange_needs_connector(
    from_modes: &[String],
    from_route_ids: &HashSet<String>,
    to_modes: &[String],
    to_route_ids: &HashSet<String>,
) -> bool {
    from_modes != to_modes || from_route_ids != to_route_ids
}

#[allow(clippy::too_many_arguments)]
fn implicit_station_transfer_signature(
    stop_id: &str,
    name: &str,
    municipality: Option<&str>,
    lat: Option<f64>,
    lon: Option<f64>,
    stop_area_id: Option<&str>,
    parent_station_id: Option<&str>,
    platform_code: Option<&str>,
    modes: &[String],
) -> Option<String> {
    if let Some(source_complex_id) = pid_stop_complex_id(stop_id) {
        return Some(format!("source:{source_complex_id}"));
    }
    if let Some(stop_area_id) = stop_area_id.filter(|value| !value.trim().is_empty()) {
        return Some(format!("area:{stop_area_id}"));
    }
    if let Some(parent_station_id) = parent_station_id.filter(|value| !value.trim().is_empty()) {
        return Some(format!("parent:{parent_station_id}"));
    }
    let station_like = platform_code.is_some()
        || railway_station_stop_base(stop_id).is_some()
        || modes.iter().any(|mode| {
            matches!(
                mode.as_str(),
                "train" | "rail" | "metro" | "subway" | "tram"
            )
        });
    if !station_like {
        return None;
    }

    let (Some(lat), Some(lon)) = (lat, lon) else {
        return railway_station_stop_base(stop_id).map(|station| format!("rail:{station}"));
    };
    Some(format!(
        "station:{}:{}:{}:{}",
        canonical_stop_name_parts(name, municipality),
        municipality.map(normalize_search_text).unwrap_or_default(),
        (lat * 100.0).round() as i32,
        (lon * 100.0).round() as i32
    ))
}

fn should_search_next_service_day(departure_time: u32, threshold_seconds: u32) -> bool {
    departure_time >= threshold_seconds
}

fn discard_departed_journeys(journeys: &mut Vec<Journey>, requested_departure_time: u32) -> usize {
    let previous_len = journeys.len();
    journeys.retain(|journey| journey.departure_time >= requested_departure_time);
    previous_len.saturating_sub(journeys.len())
}

fn journey_query_context(
    body: &JourneySearchBody,
    departure_time: u32,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
    nearby_transfer_count: usize,
) -> Value {
    json!({
        "requested_datetime": body.datetime,
        "departure_time": departure_time,
        "max_transfers": body.max_transfers,
        "transport_modes": body.transport_modes,
        "include_intermediate_stops": body.include_intermediate_stops,
        "journey_preferences": body.journey_preferences,
        "from_stop_ids": from_stop_ids,
        "to_stop_ids": to_stop_ids,
        "nearby_walking_transfer_count": nearby_transfer_count
    })
}

fn validate_journey_preferences(body: &JourneySearchBody) -> Result<(), ApiError> {
    let Some(preferences) = &body.journey_preferences else {
        return Ok(());
    };
    if !matches!(
        preferences.profile.as_str(),
        "standard" | "wheelchair" | "stroller" | "luggage"
    ) {
        return Err(ApiError {
            code: "unsupported_journey_profile".to_string(),
            message:
                "journey_preferences.profile must be standard, wheelchair, stroller, or luggage"
                    .to_string(),
        });
    }
    if preferences.minimum_transfer_buffer_seconds > 30 * 60 {
        return Err(ApiError {
            code: "validation_error".to_string(),
            message: "minimum_transfer_buffer_seconds must be between 0 and 1800".to_string(),
        });
    }
    if matches!(preferences.profile.as_str(), "wheelchair" | "stroller")
        || preferences.step_free
        || preferences.prefer_fewer_stairs
    {
        return Err(ApiError {
            code: "journey_accessibility_unverified".to_string(),
            message: "This deployment cannot yet verify a complete step-free route; the requested accessibility profile was not applied"
                .to_string(),
        });
    }
    Ok(())
}

fn operational_run_id(trip_id: &str, service_date: chrono::NaiveDate) -> String {
    let digest = Sha256::digest(format!("{service_date}\0{trip_id}").as_bytes());
    format!("run:{service_date}:{}", &hex::encode(digest)[..24])
}

fn operational_call_id(run_id: &str, stop_id: &str, stop_sequence: i64) -> String {
    let digest = Sha256::digest(format!("{run_id}\0{stop_sequence}\0{stop_id}").as_bytes());
    format!("call:{}", &hex::encode(digest)[..32])
}

fn attach_journey_assistance_identity(
    journeys: &mut [Value],
    related: &Value,
    service_date: chrono::NaiveDate,
) {
    let stops = related["stops"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|stop| Some((stop["id"].as_str()?.to_string(), stop)))
        .collect::<HashMap<_, _>>();
    let stop_times = related["stop_times"].as_array();

    for journey in journeys {
        journey["accessibility"] = json!({
            "verified": false,
            "step_free": null,
            "reason": "complete_journey_not_verified"
        });
        let Some(legs) = journey["legs"].as_array_mut() else {
            continue;
        };
        for leg in legs {
            let from_stop_id = leg["from_stop_id"].as_str().map(str::to_string);
            let to_stop_id = leg["to_stop_id"].as_str().map(str::to_string);
            let trip_id = leg["trip_id"].as_str().map(str::to_string);
            let from_stop = from_stop_id.as_deref().and_then(|id| stops.get(id));
            let to_stop = to_stop_id.as_deref().and_then(|id| stops.get(id));
            leg["from_station_id"] = from_stop
                .and_then(|stop| stop.get("station_id"))
                .cloned()
                .unwrap_or(Value::Null);
            leg["to_station_id"] = to_stop
                .and_then(|stop| stop.get("station_id"))
                .cloned()
                .unwrap_or(Value::Null);

            if let Some(trip_id) = trip_id {
                let run_id = operational_run_id(&trip_id, service_date);
                leg["run_id"] = json!(run_id);
                let departure_sequence = stop_times
                    .into_iter()
                    .flatten()
                    .find(|stop_time| {
                        stop_time["trip_id"].as_str() == Some(trip_id.as_str())
                            && stop_time["stop_id"].as_str() == from_stop_id.as_deref()
                    })
                    .and_then(|stop_time| stop_time["stop_sequence"].as_i64());
                leg["departure_call_id"] = departure_sequence
                    .zip(from_stop_id.as_deref())
                    .map(|(sequence, stop_id)| {
                        json!(operational_call_id(&run_id, stop_id, sequence))
                    })
                    .unwrap_or(Value::Null);

                if let Some(calls) = leg["stop_calls"].as_array_mut() {
                    for call in calls {
                        let stop_id = call["stop_id"].as_str().map(str::to_string);
                        let sequence = call["stop_sequence"].as_i64();
                        call["run_id"] = json!(run_id);
                        call["call_id"] = sequence
                            .zip(stop_id.as_deref())
                            .map(|(sequence, stop_id)| {
                                json!(operational_call_id(&run_id, stop_id, sequence))
                            })
                            .unwrap_or(Value::Null);
                        if call.get("station_id").is_none() {
                            call["station_id"] = stop_id
                                .as_deref()
                                .and_then(|id| stops.get(id))
                                .and_then(|stop| stop.get("station_id"))
                                .cloned()
                                .unwrap_or(Value::Null);
                        }
                    }
                }
            } else {
                leg["run_id"] = Value::Null;
                leg["departure_call_id"] = Value::Null;
            }
        }
    }
}

fn next_service_day_journey_results(
    journeys: Vec<Journey>,
    requested_departure_time: u32,
) -> Vec<Journey> {
    journeys
        .into_iter()
        .filter(|journey| journey.departure_time < requested_departure_time)
        .map(|journey| shift_journey_service_day(journey, SERVICE_DAY_SECONDS))
        .collect()
}

fn shift_journey_service_day(mut journey: Journey, offset_seconds: u32) -> Journey {
    journey.departure_time = journey.departure_time.saturating_add(offset_seconds);
    journey.arrival_time = journey.arrival_time.saturating_add(offset_seconds);
    journey.duration_seconds = journey.arrival_time.saturating_sub(journey.departure_time);
    if !journey.labels.iter().any(|label| label == "dalsi den") {
        journey.labels.push("dalsi den".to_string());
    }
    for leg in &mut journey.legs {
        leg.departure_time = leg.departure_time.saturating_add(offset_seconds);
        leg.arrival_time = leg.arrival_time.saturating_add(offset_seconds);
    }
    journey
}

async fn dedupe_relevant_journeys_db(
    pool: &PgPool,
    journeys: Vec<Journey>,
    routing_config: &RoutingAlgorithmConfig,
) -> Result<Vec<Journey>, sqlx::Error> {
    let stop_ids = journeys
        .iter()
        .flat_map(|journey| journey.legs.iter())
        .flat_map(|leg| [leg.from_stop_id.clone(), leg.to_stop_id.clone()])
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let route_ids = journeys
        .iter()
        .flat_map(|journey| journey.legs.iter())
        .filter_map(|leg| leg.route_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let stop_rows = if stop_ids.is_empty() {
        Vec::new()
    } else {
        sqlx::query(
            r#"
            SELECT id, name, normalized_name, municipality, lat, lon, stop_area_id
            FROM stops
            WHERE id = ANY($1)
            "#,
        )
        .bind(stop_ids)
        .fetch_all(pool)
        .await?
    };
    let stop_signatures = stop_rows
        .into_iter()
        .map(|row| {
            let id = row.get::<String, _>("id");
            let municipality_value = row.get::<Option<String>, _>("municipality");
            let municipality = municipality_value
                .as_deref()
                .map(normalize_search_text)
                .unwrap_or_default();
            let normalized_name = canonical_stop_name_parts(
                &row.get::<String, _>("name"),
                municipality_value.as_deref(),
            );
            let signature = if let Some(station) = railway_station_stop_base(&id) {
                format!("rail:{station}")
            } else if let Some(stop_area_id) = row.get::<Option<String>, _>("stop_area_id") {
                format!("area:{stop_area_id}")
            } else {
                match (
                    row.get::<Option<f64>, _>("lat"),
                    row.get::<Option<f64>, _>("lon"),
                ) {
                    (Some(lat), Some(lon)) => format!(
                        "{normalized_name}:{municipality}:{}:{}",
                        (lat * 100.0).round() as i32,
                        (lon * 100.0).round() as i32
                    ),
                    _ => format!("{normalized_name}:{municipality}"),
                }
            };
            (id, signature)
        })
        .collect::<HashMap<_, _>>();

    let route_priorities = if route_ids.is_empty() {
        HashMap::new()
    } else {
        sqlx::query("SELECT id, source_priority FROM routes WHERE id = ANY($1)")
            .bind(route_ids)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>("id"),
                    row.get::<i32, _>("source_priority"),
                )
            })
            .collect::<HashMap<_, _>>()
    };

    Ok(dedupe_relevant_journeys(
        journeys,
        &stop_signatures,
        &route_priorities,
        routing_config,
    ))
}

fn dedupe_relevant_journeys(
    mut journeys: Vec<Journey>,
    stop_signatures: &HashMap<String, String>,
    route_priorities: &HashMap<String, i32>,
    routing_config: &RoutingAlgorithmConfig,
) -> Vec<Journey> {
    journeys.retain(|journey| journey_is_relevant(journey, stop_signatures, routing_config));
    journeys.sort_by_key(|journey| {
        journey
            .legs
            .iter()
            .map(|leg| {
                leg.route_id
                    .as_ref()
                    .and_then(|route_id| route_priorities.get(route_id))
                    .copied()
                    .unwrap_or(1_000)
            })
            .sum::<i32>()
    });

    let mut seen = HashSet::new();
    journeys.retain(|journey| seen.insert(visible_journey_key(journey, stop_signatures)));
    journeys
}

fn journey_is_relevant(
    journey: &Journey,
    stop_signatures: &HashMap<String, String>,
    routing_config: &RoutingAlgorithmConfig,
) -> bool {
    let Some(first_leg) = journey.legs.first() else {
        return false;
    };
    let Some(last_leg) = journey.legs.last() else {
        return false;
    };
    if first_leg.departure_time != journey.departure_time
        || last_leg.arrival_time != journey.arrival_time
        || journey.arrival_time < journey.departure_time
        || journey.duration_seconds != journey.arrival_time - journey.departure_time
    {
        return false;
    }
    let mut trip_ids = HashSet::new();
    for (index, leg) in journey.legs.iter().enumerate() {
        if leg.arrival_time < leg.departure_time {
            return false;
        }
        if !is_walking_leg(leg)
            && stop_signature(&leg.from_stop_id, stop_signatures)
                == stop_signature(&leg.to_stop_id, stop_signatures)
        {
            return false;
        }
        if let Some(trip_id) = &leg.trip_id
            && !trip_ids.insert(trip_id)
        {
            return false;
        }
        if let Some(next_leg) = journey.legs.get(index + 1) {
            let wait = next_leg.departure_time.saturating_sub(leg.arrival_time);
            if next_leg.departure_time < leg.arrival_time
                || stop_signature(&leg.to_stop_id, stop_signatures)
                    != stop_signature(&next_leg.from_stop_id, stop_signatures)
            {
                return false;
            }
            let max_wait = routing_config.max_transfer_wait_seconds.max(0) as u32;
            let min_wait = routing_config.min_transfer_seconds.max(0) as u32;
            if is_walking_leg(leg) || is_walking_leg(next_leg) {
                if wait > max_wait {
                    return false;
                }
            } else if wait > max_wait
                || (wait < min_wait
                    && !next_leg
                        .warnings
                        .iter()
                        .chain(leg.warnings.iter())
                        .any(|warning| {
                            warning == "official_minimum_change_time"
                                || warning == "realtime_routing_applied"
                        }))
            {
                return false;
            }
        }
    }
    true
}

fn is_walking_leg(leg: &JourneyLeg) -> bool {
    leg.route_id.is_none() && leg.trip_id.is_none()
}

fn visible_journey_key(journey: &Journey, stop_signatures: &HashMap<String, String>) -> String {
    let leading_walk_count = journey
        .legs
        .iter()
        .take_while(|leg| is_walking_leg(leg))
        .count();
    journey
        .legs
        .iter()
        .enumerate()
        .map(|(index, leg)| {
            if index < leading_walk_count {
                format!(
                    "walk:{}:{}:{}",
                    stop_signature(&leg.from_stop_id, stop_signatures),
                    stop_signature(&leg.to_stop_id, stop_signatures),
                    leg.arrival_time.saturating_sub(leg.departure_time)
                )
            } else {
                format!(
                    "{}:{}:{}:{}",
                    stop_signature(&leg.from_stop_id, stop_signatures),
                    stop_signature(&leg.to_stop_id, stop_signatures),
                    leg.departure_time,
                    leg.arrival_time
                )
            }
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn stop_signature<'a>(stop_id: &'a str, stop_signatures: &'a HashMap<String, String>) -> &'a str {
    stop_signatures
        .get(stop_id)
        .map(String::as_str)
        .unwrap_or(stop_id)
}

async fn journey_carrier_keys_db(
    pool: &PgPool,
    journeys: &[Journey],
) -> Result<HashMap<String, String>, sqlx::Error> {
    let route_ids = journeys
        .iter()
        .flat_map(|journey| journey.legs.iter())
        .filter_map(|leg| leg.route_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if route_ids.is_empty() {
        return Ok(HashMap::new());
    }

    Ok(sqlx::query(
        r#"
        SELECT id,
               COALESCE(
                 'operator:' || operator_id,
                 'agency:' || agency_id,
                 'feed:' || source_feed_id,
                 'route:' || id
               ) AS carrier_key
        FROM routes
        WHERE id = ANY($1)
        "#,
    )
    .bind(route_ids)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        (
            row.get::<String, _>("id"),
            row.get::<String, _>("carrier_key"),
        )
    })
    .collect())
}

#[cfg(test)]
fn ranked_journey_results(journeys: Vec<Journey>) -> Vec<Journey> {
    ranked_journey_results_with_carriers(
        journeys,
        &HashMap::new(),
        &RoutingAlgorithmConfig::default(),
    )
}

fn ranked_journey_results_with_carriers(
    mut journeys: Vec<Journey>,
    carrier_keys: &HashMap<String, String>,
    configuration: &RoutingAlgorithmConfig,
) -> Vec<Journey> {
    if configuration.remove_dominated {
        journeys = remove_dominated_journeys(journeys, carrier_keys, configuration);
    }
    journeys.sort_by_key(|journey| journey_rank(journey, configuration));

    let mut seen = HashSet::new();
    let candidates = journeys
        .into_iter()
        .filter(|journey| {
            let key = journey_identity_key(journey);
            seen.insert(key)
        })
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return Vec::new();
    }

    let mut selected = Vec::new();
    let mut selected_keys = HashSet::new();
    let max_results = configuration.max_results as usize;

    // The configured primary result must survive even a one-result limit.
    push_ranked_journey(
        &mut selected,
        &mut selected_keys,
        &candidates[0],
        max_results,
    );

    // Reserve useful alternatives before a large Pareto frontier fills the
    // result limit. A direct ride with little walking is a distinct option
    // from boarding a nearby service after a long access walk.
    if configuration.preserve_simplest
        && let Some(simplest) = candidates.iter().min_by_key(|journey| {
            (
                journey.transfer_count,
                journey.walking_distance_meters,
                journey.arrival_time,
                journey.duration_seconds,
                journey.departure_time,
            )
        })
    {
        push_ranked_journey(&mut selected, &mut selected_keys, simplest, max_results);
    }

    let mut transfer_counts = candidates
        .iter()
        .map(|journey| journey.transfer_count)
        .collect::<Vec<_>>();
    transfer_counts.sort_unstable();
    transfer_counts.dedup();
    for transfer_count in transfer_counts {
        if !configuration.preserve_each_transfer_count {
            break;
        }
        if let Some(best_for_transfer_count) = candidates
            .iter()
            .filter(|journey| journey.transfer_count == transfer_count)
            .min_by_key(|journey| journey_rank(journey, configuration))
        {
            push_ranked_journey(
                &mut selected,
                &mut selected_keys,
                best_for_transfer_count,
                max_results,
            );
        }
    }

    let alternative_slot_limit = max_results.div_ceil(3).clamp(1, 4);
    if configuration.preserve_carrier_diversity {
        let mut best_by_carrier = HashMap::<String, &Journey>::new();
        for journey in &candidates {
            let Some(signature) = journey_carrier_signature(journey, carrier_keys) else {
                continue;
            };
            let replace = best_by_carrier.get(&signature).is_none_or(|known| {
                journey_rank(journey, configuration) < journey_rank(known, configuration)
            });
            if replace {
                best_by_carrier.insert(signature, journey);
            }
        }
        let mut carrier_candidates = best_by_carrier.into_values().collect::<Vec<_>>();
        carrier_candidates.sort_by_key(|journey| journey_rank(journey, configuration));
        for best_for_carrier in carrier_candidates.into_iter().take(alternative_slot_limit) {
            push_ranked_journey(
                &mut selected,
                &mut selected_keys,
                best_for_carrier,
                max_results,
            );
        }
    }

    let fastest = candidates
        .iter()
        .min_by_key(|journey| (journey.arrival_time, journey.transfer_count))
        .expect("non-empty candidates checked above");
    let fastest_route = journey_route_signature(fastest);
    let mut best_by_route = HashMap::<String, &Journey>::new();
    for journey in &candidates {
        let signature = journey_route_signature(journey);
        if signature != fastest_route
            && !reasonable_distinct_route_alternative(journey, fastest, &signature, &fastest_route)
        {
            continue;
        }
        let replace = best_by_route.get(&signature).is_none_or(|known| {
            journey_rank(journey, configuration) < journey_rank(known, configuration)
        });
        if replace {
            best_by_route.insert(signature, journey);
        }
    }
    let mut route_candidates = best_by_route.into_values().collect::<Vec<_>>();
    route_candidates.sort_by_key(|journey| journey_rank(journey, configuration));
    if configuration.preserve_simplest || configuration.preserve_each_transfer_count {
        let selected_routes = selected
            .iter()
            .map(journey_route_signature)
            .collect::<HashSet<_>>();
        for best_for_route in route_candidates
            .into_iter()
            .filter(|journey| !selected_routes.contains(&journey_route_signature(journey)))
            .take(alternative_slot_limit)
        {
            push_ranked_journey(
                &mut selected,
                &mut selected_keys,
                best_for_route,
                max_results,
            );
        }
    }

    let mut time_frontier = candidates
        .iter()
        .filter(|candidate| {
            !candidates
                .iter()
                .any(|other| journey_dominates(other, candidate, carrier_keys, configuration))
        })
        .collect::<Vec<_>>();
    time_frontier.sort_by_key(|journey| {
        (
            journey.departure_time,
            journey.arrival_time,
            journey.transfer_count,
            journey.walking_distance_meters,
            journey.duration_seconds,
        )
    });
    let reserved_frontier_slots = time_frontier
        .iter()
        .filter(|journey| selected_keys.contains(&journey_identity_key(journey)))
        .count();
    let remaining_slots = max_results.saturating_sub(selected.len());
    let sampled_frontier = evenly_spaced_journey_refs(
        &time_frontier,
        remaining_slots.saturating_add(reserved_frontier_slots),
    )
    .into_iter()
    .filter(|journey| !selected_keys.contains(&journey_identity_key(journey)))
    .collect::<Vec<_>>();
    for journey in evenly_spaced_journey_refs(&sampled_frontier, remaining_slots) {
        push_ranked_journey(&mut selected, &mut selected_keys, journey, max_results);
    }

    for journey in &candidates {
        push_ranked_journey(&mut selected, &mut selected_keys, journey, max_results);
    }

    let simplest_key = selected
        .iter()
        .min_by_key(|journey| {
            (
                journey.transfer_count,
                journey.walking_distance_meters,
                journey.arrival_time,
                journey.duration_seconds,
                journey.departure_time,
            )
        })
        .map(journey_identity_key);
    let fastest_key = selected
        .iter()
        .min_by_key(|journey| {
            (
                journey.arrival_time,
                journey.duration_seconds,
                journey.transfer_count,
                journey.departure_time,
            )
        })
        .map(journey_identity_key);
    selected.sort_by_key(|journey| journey_rank(journey, configuration));
    selected
        .into_iter()
        .enumerate()
        .map(|(index, mut journey)| {
            journey.id = format!("journey-{}", index + 1);
            journey.labels.retain(|label| {
                label != "doporuceno" && label != "nejrychlejsi" && label != "nejjednodussi"
            });
            if index == 0 {
                journey.labels.push("doporuceno".to_string());
            }
            if fastest_key.as_ref() == Some(&journey_identity_key(&journey)) {
                journey.labels.push("nejrychlejsi".to_string());
            }
            if simplest_key.as_ref() == Some(&journey_identity_key(&journey)) {
                journey.labels.push("nejjednodussi".to_string());
            }
            journey
        })
        .collect()
}

fn remove_dominated_journeys(
    journeys: Vec<Journey>,
    carrier_keys: &HashMap<String, String>,
    configuration: &RoutingAlgorithmConfig,
) -> Vec<Journey> {
    let time_frontier_keys = journeys
        .iter()
        .filter(|candidate| {
            !journeys
                .iter()
                .any(|other| journey_dominates(other, candidate, carrier_keys, configuration))
        })
        .map(journey_identity_key)
        .collect::<HashSet<_>>();
    let minimum_frontier_transfers = journeys
        .iter()
        .filter(|journey| time_frontier_keys.contains(&journey_identity_key(journey)))
        .map(|journey| journey.transfer_count)
        .min()
        .unwrap_or(u32::MAX);
    let mut exception_keys = HashSet::new();
    if configuration.preserve_simplest
        && let Some(simplest) = journeys
            .iter()
            .filter(|journey| journey.transfer_count < minimum_frontier_transfers)
            .min_by_key(|journey| (journey.transfer_count, journey_rank(journey, configuration)))
    {
        exception_keys.insert(journey_identity_key(simplest));
    }
    if configuration.preserve_each_transfer_count {
        let mut best_by_transfer_count = HashMap::<u32, &Journey>::new();
        for journey in journeys
            .iter()
            .filter(|journey| journey.transfer_count < minimum_frontier_transfers)
        {
            let replace = best_by_transfer_count
                .get(&journey.transfer_count)
                .is_none_or(|known| {
                    journey_rank(journey, configuration) < journey_rank(known, configuration)
                });
            if replace {
                best_by_transfer_count.insert(journey.transfer_count, journey);
            }
        }
        exception_keys.extend(
            best_by_transfer_count
                .into_values()
                .map(journey_identity_key),
        );
    }
    if configuration.preserve_carrier_diversity {
        let mut best_by_carrier = HashMap::<String, &Journey>::new();
        for journey in &journeys {
            let Some(carrier) = journey_carrier_signature(journey, carrier_keys) else {
                continue;
            };
            let replace = best_by_carrier.get(&carrier).is_none_or(|known| {
                journey_rank(journey, configuration) < journey_rank(known, configuration)
            });
            if replace {
                best_by_carrier.insert(carrier, journey);
            }
        }
        exception_keys.extend(best_by_carrier.into_values().map(journey_identity_key));
    }

    journeys
        .iter()
        .filter(|candidate| {
            time_frontier_keys.contains(&journey_identity_key(candidate))
                || exception_keys.contains(&journey_identity_key(candidate))
        })
        .cloned()
        .collect()
}

fn time_dominates(better: &Journey, candidate: &Journey) -> bool {
    better.departure_time >= candidate.departure_time
        && better.arrival_time <= candidate.arrival_time
        && better.transfer_count <= candidate.transfer_count
        && better.walking_distance_meters <= candidate.walking_distance_meters
        && (better.departure_time > candidate.departure_time
            || better.arrival_time < candidate.arrival_time
            || better.transfer_count < candidate.transfer_count
            || better.walking_distance_meters < candidate.walking_distance_meters)
}

fn journey_dominates(
    better: &Journey,
    candidate: &Journey,
    carrier_keys: &HashMap<String, String>,
    configuration: &RoutingAlgorithmConfig,
) -> bool {
    if configuration.dominate_only_same_carrier {
        let better_carrier = journey_carrier_signature(better, carrier_keys);
        let candidate_carrier = journey_carrier_signature(candidate, carrier_keys);
        if better_carrier.is_none() || better_carrier != candidate_carrier {
            return false;
        }
    }
    time_dominates(better, candidate)
}

fn evenly_spaced_journey_refs<'a>(journeys: &[&'a Journey], limit: usize) -> Vec<&'a Journey> {
    if limit == 0 {
        return Vec::new();
    }
    if journeys.len() <= limit {
        return journeys.to_vec();
    }
    if limit <= 1 {
        return journeys.first().copied().into_iter().collect();
    }
    (0..limit)
        .map(|sample_index| {
            let journey_index = sample_index * (journeys.len() - 1) / (limit - 1);
            journeys[journey_index]
        })
        .collect()
}

fn reasonable_distinct_route_alternative(
    candidate: &Journey,
    better: &Journey,
    candidate_route: &str,
    better_route: &str,
) -> bool {
    candidate_route != better_route
        && candidate.arrival_time
            <= better
                .arrival_time
                .saturating_add(REASONABLE_ALTERNATIVE_SLACK_SECONDS)
        && candidate.duration_seconds
            <= better
                .duration_seconds
                .saturating_add(REASONABLE_ALTERNATIVE_SLACK_SECONDS)
        && candidate.transfer_count <= better.transfer_count.saturating_add(2)
}

fn journey_carrier_signature(
    journey: &Journey,
    carrier_keys: &HashMap<String, String>,
) -> Option<String> {
    let mut keys = journey
        .legs
        .iter()
        .filter_map(|leg| leg.route_id.as_ref())
        .filter_map(|route_id| carrier_keys.get(route_id))
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    (!keys.is_empty()).then(|| keys.join("|"))
}

fn push_ranked_journey(
    selected: &mut Vec<Journey>,
    selected_keys: &mut HashSet<String>,
    journey: &Journey,
    max_results: usize,
) {
    if selected.len() >= max_results {
        return;
    }

    let key = journey_identity_key(journey);
    if selected_keys.insert(key) {
        selected.push(journey.clone());
    }
}

fn journey_rank(
    journey: &Journey,
    configuration: &RoutingAlgorithmConfig,
) -> (u64, u32, u32, u32, u32) {
    let score = journey.arrival_time as f64 * configuration.arrival_time_weight
        + journey.duration_seconds as f64 * configuration.duration_weight
        + journey.transfer_count as f64 * configuration.transfer_penalty_seconds as f64;
    (
        (score * 1000.0).round().max(0.0) as u64,
        journey.arrival_time,
        journey.duration_seconds,
        journey.transfer_count,
        journey.departure_time,
    )
}

fn journey_identity_key(journey: &Journey) -> String {
    journey
        .legs
        .iter()
        .map(|leg| {
            format!(
                "{}:{}:{}:{}:{}",
                public_route_key(leg.route_id.as_deref()),
                canonical_journey_stop_id(&leg.from_stop_id),
                canonical_journey_stop_id(&leg.to_stop_id),
                leg.departure_time,
                leg.arrival_time
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn journey_route_signature(journey: &Journey) -> String {
    journey
        .legs
        .iter()
        .map(|leg| {
            leg.route_id.as_deref().map_or_else(
                || "walk".to_string(),
                |route_id| format!("route:{}", public_route_key(Some(route_id))),
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn public_route_key(route_id: Option<&str>) -> String {
    route_id
        .unwrap_or_default()
        .split('-')
        .filter(|part| !(part.len() == 4 && part.chars().all(|ch| ch.is_ascii_digit())))
        .collect::<Vec<_>>()
        .join("-")
}

fn canonical_journey_stop_id(stop_id: &str) -> String {
    railway_station_stop_base(stop_id).unwrap_or_else(|| stop_id.to_string())
}

fn railway_station_stop_base(stop_id: &str) -> Option<String> {
    let (marker_index, marker, canonical_marker) =
        if let Some(marker_index) = stop_id.rfind("SR70ST-CZ-") {
            (marker_index, "SR70ST-CZ-", "SR70S-CZ-")
        } else {
            (stop_id.rfind("SR70S-CZ-")?, "SR70S-CZ-", "SR70S-CZ-")
        };
    let marker_end = marker_index + marker.len();
    let station_and_platform = &stop_id[marker_end..];
    let mut parts = station_and_platform.split('-');
    let station_code = parts.next()?;
    if station_code.is_empty() || !station_code.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    if let Some(platform) = parts.next() {
        let looks_like_platform = parts.next().is_none()
            && !platform.is_empty()
            && platform.len() <= 4
            && platform
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_digit())
            && platform.chars().all(|ch| ch.is_ascii_alphanumeric());
        if !looks_like_platform {
            return None;
        }
    }

    Some(format!(
        "{}{canonical_marker}{station_code}",
        &stop_id[..marker_index]
    ))
}

fn escaped_like_prefix(value: &str) -> String {
    format!(
        "{}%",
        value
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    )
}

#[allow(clippy::collapsible_if)]
async fn resolve_journey_point_db(
    pool: &PgPool,
    point: &JourneyPoint,
) -> Result<(Vec<String>, Vec<String>), sqlx::Error> {
    if point.point_type == "coordinate" {
        let (lat, lon) = point
            .lat
            .zip(point.lon)
            .expect("coordinate point was validated");
        return Ok((vec![coordinate_stop_id(lat, lon)], Vec::new()));
    }
    let candidate = point
        .id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&point.point_type);
    let mut warnings = Vec::new();

    if point.point_type == "city" {
        let stop_ids = sqlx::query_scalar::<_, String>(
            "SELECT id FROM enabled_source_stops WHERE is_active = true AND city_id = $1 ORDER BY id",
        )
        .bind(candidate)
        .fetch_all(pool)
        .await?;
        if stop_ids.is_empty() {
            warnings.push(format!("city '{candidate}' has no active assigned stops"));
        }
        return Ok((stop_ids, warnings));
    }

    if let Some(stop) = get_routing_stop_db(pool, candidate).await? {
        let stop_ids = equivalent_stop_ids_db(pool, &stop).await?;
        if stop_ids.len() > 1 {
            warnings.push(format!(
                "expanded stop '{}' to {} boardable complex members",
                stop.name,
                stop_ids.len()
            ));
        }
        return Ok((stop_ids, warnings));
    }

    let normalized = normalize_search_text(candidate);
    if !normalized.is_empty() {
        if let Some(stop) = search_stops_db(pool, candidate, &normalized, 1)
            .await?
            .into_iter()
            .next()
        {
            warnings.push(format!(
                "resolved stop query '{candidate}' to '{}'",
                stop.name
            ));
            let stop_ids = equivalent_stop_ids_db(pool, &stop).await?;
            if stop_ids.len() > 1 {
                warnings.push(format!(
                    "expanded stop '{}' to {} boardable complex members",
                    stop.name,
                    stop_ids.len()
                ));
            }
            return Ok((stop_ids, warnings));
        }
    }

    warnings.push(format!("could not resolve stop query '{candidate}'"));
    Ok((Vec::new(), warnings))
}

async fn validate_journey_point_db(pool: &PgPool, point: &JourneyPoint) -> Result<(), ApiError> {
    match point.point_type.as_str() {
        "stop" => validate_required_journey_point_id(point),
        "city" => {
            let city_id = point
                .id
                .as_deref()
                .filter(|id| id.starts_with("city:") && !id.trim().is_empty())
                .ok_or_else(|| invalid_city_id(point.id.as_deref()))?;
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM cities WHERE id = $1)")
                    .bind(city_id)
                    .fetch_one(pool)
                    .await
                    .map_err(internal_error)?;
            if exists {
                Ok(())
            } else {
                Err(invalid_city_id(Some(city_id)))
            }
        }
        "coordinate" => validate_coordinate_journey_point(point),
        other => Err(ApiError {
            code: "invalid_journey_point_type".to_string(),
            message: format!(
                "journey point type '{other}' is not supported; use 'stop', 'city' or 'coordinate'"
            ),
        }),
    }
}

fn validate_required_journey_point_id(point: &JourneyPoint) -> Result<(), ApiError> {
    if point.id.as_deref().is_some_and(|id| !id.trim().is_empty()) {
        Ok(())
    } else {
        Err(ApiError {
            code: "invalid_journey_point_id".to_string(),
            message: format!(
                "journey point type '{}' requires a non-empty id",
                point.point_type
            ),
        })
    }
}

fn validate_coordinate_journey_point(point: &JourneyPoint) -> Result<(), ApiError> {
    let (lat, lon) = point.lat.zip(point.lon).ok_or_else(|| ApiError {
        code: "invalid_coordinate".to_string(),
        message: "coordinate journey points require numeric lat and lon".to_string(),
    })?;
    if !lat.is_finite()
        || !lon.is_finite()
        || !(-90.0..=90.0).contains(&lat)
        || !(-180.0..=180.0).contains(&lon)
    {
        return Err(ApiError {
            code: "invalid_coordinate".to_string(),
            message: "lat must be between -90 and 90 and lon between -180 and 180".to_string(),
        });
    }
    Ok(())
}

fn coordinate_stop_id(lat: f64, lon: f64) -> String {
    format!("coordinate:{lat:.6},{lon:.6}")
}

fn coordinate_from_stop_id(stop_id: &str) -> Option<(f64, f64)> {
    let (lat, lon) = stop_id.strip_prefix("coordinate:")?.split_once(',')?;
    Some((lat.parse().ok()?, lon.parse().ok()?))
}

fn invalid_city_id(city_id: Option<&str>) -> ApiError {
    ApiError {
        code: "invalid_city_id".to_string(),
        message: format!(
            "city ID '{}' is invalid or unknown",
            city_id.unwrap_or_default()
        ),
    }
}

async fn equivalent_stop_ids_db(pool: &PgPool, stop: &Stop) -> Result<Vec<String>, sqlx::Error> {
    let mut ids = vec![stop.id.clone()];

    if stop.parent_station_id.is_some() || stop.location_type == StopLocationType::Station {
        let parent_station_id = stop.parent_station_id.as_deref().unwrap_or(&stop.id);
        let mut station_ids = sqlx::query_scalar::<_, String>(
            r#"
            SELECT stop.id
            FROM stops AS stop
            WHERE stop.is_active = true
              AND (stop.id = $1 OR stop.parent_station_id = $1)
              AND stop.normalized_name = $2
              AND (
                stop.source_feed_id IS NULL
                OR EXISTS (
                  SELECT 1 FROM source_feeds AS direct_feed
                  WHERE direct_feed.id = stop.source_feed_id
                    AND direct_feed.enabled = true
                )
                OR EXISTS (
                  SELECT 1
                  FROM stop_source_ids AS source_id
                  JOIN source_feeds AS source_feed
                    ON source_feed.id = source_id.source_feed_id
                   AND source_feed.enabled = true
                  WHERE source_id.stop_id = stop.id
                )
              )
            LIMIT 250
            "#,
        )
        .bind(parent_station_id)
        .bind(pid_source_stop_query(&stop.normalized_name))
        .fetch_all(pool)
        .await?;
        ids.append(&mut station_ids);
    }

    if let Some(stop_area_id) = &stop.stop_area_id {
        let mut area_ids = sqlx::query_scalar::<_, String>(
            r#"
            SELECT stop.id
            FROM stops AS stop
            WHERE stop.is_active = true
              AND stop.stop_area_id = $1
              AND stop.normalized_name = $2
              AND (
                stop.source_feed_id IS NULL
                OR EXISTS (
                  SELECT 1 FROM source_feeds AS direct_feed
                  WHERE direct_feed.id = stop.source_feed_id
                    AND direct_feed.enabled = true
                )
                OR EXISTS (
                  SELECT 1
                  FROM stop_source_ids AS source_id
                  JOIN source_feeds AS source_feed
                    ON source_feed.id = source_id.source_feed_id
                   AND source_feed.enabled = true
                  WHERE source_id.stop_id = stop.id
                )
              )
            LIMIT 250
            "#,
        )
        .bind(stop_area_id)
        .bind(pid_source_stop_query(&stop.normalized_name))
        .fetch_all(pool)
        .await?;
        ids.append(&mut area_ids);
    }

    if let Some(station_base) = railway_station_stop_base(&stop.id) {
        let station_prefix = escaped_like_prefix(&format!("{station_base}-"));
        let station_ids = sqlx::query_scalar::<_, String>(
            r#"
            SELECT stop.id
            FROM stops AS stop
            WHERE stop.is_active = true
              AND (stop.id = $1 OR stop.id LIKE $2 ESCAPE '\')
              AND stop.normalized_name = $3
              AND (
                stop.source_feed_id IS NULL
                OR EXISTS (
                  SELECT 1 FROM source_feeds AS direct_feed
                  WHERE direct_feed.id = stop.source_feed_id
                    AND direct_feed.enabled = true
                )
                OR EXISTS (
                  SELECT 1
                  FROM stop_source_ids AS source_id
                  JOIN source_feeds AS source_feed
                    ON source_feed.id = source_id.source_feed_id
                   AND source_feed.enabled = true
                  WHERE source_id.stop_id = stop.id
                )
              )
            LIMIT 250
            "#,
        )
        .bind(&station_base)
        .bind(station_prefix)
        .bind(pid_source_stop_query(&stop.normalized_name))
        .fetch_all(pool)
        .await?;
        ids.extend(
            station_ids.into_iter().filter(|id| {
                railway_station_stop_base(id).as_deref() == Some(station_base.as_str())
            }),
        );
    }

    if stop.lat.is_some() && stop.lon.is_some() {
        let sibling_rows = sqlx::query(
            r#"
            SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
                   lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
                   platform_code, location_type, parent_station_id, station_id, complex_id,
                   has_station_layout, station_layout_version, wheelchair_boarding,
                   modes, source_priority, is_active
            FROM stops
            WHERE is_active = true
              AND normalized_name = $1
              AND (
                source_feed_id IS NULL
                OR EXISTS (
                  SELECT 1 FROM source_feeds direct_feed
                  WHERE direct_feed.id = stops.source_feed_id
                    AND direct_feed.enabled = true
                )
                OR EXISTS (
                  SELECT 1
                  FROM stop_source_ids source_id
                  JOIN source_feeds source_feed
                    ON source_feed.id = source_id.source_feed_id
                   AND source_feed.enabled = true
                  WHERE source_id.stop_id = stops.id
                )
              )
            ORDER BY source_priority ASC, platform_code ASC NULLS FIRST, id ASC
            LIMIT 250
            "#,
        )
        .bind(pid_source_stop_query(&stop.normalized_name))
        .fetch_all(pool)
        .await?;
        for sibling in sibling_rows {
            let sibling = stop_from_row(sibling)?;
            if stops_are_same_suggestion(stop, &sibling) {
                ids.push(sibling.id);
            }
        }
    }

    ids.sort();
    ids.dedup();
    Ok(ids)
}

async fn direct_journeys_db(
    pool: &PgPool,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
    departure_time: u32,
    mode_filters: &[String],
    service_date: chrono::NaiveDate,
    candidate_limit: i64,
) -> Result<Vec<Journey>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        WITH active_services AS (
          SELECT calendar.service_id
          FROM calendars calendar
          WHERE $6::date BETWEEN calendar.start_date AND calendar.end_date
            AND CASE EXTRACT(ISODOW FROM $6::date)::integer
              WHEN 1 THEN calendar.monday
              WHEN 2 THEN calendar.tuesday
              WHEN 3 THEN calendar.wednesday
              WHEN 4 THEN calendar.thursday
              WHEN 5 THEN calendar.friday
              WHEN 6 THEN calendar.saturday
              WHEN 7 THEN calendar.sunday
            END
            AND NOT EXISTS (
              SELECT 1 FROM calendar_dates exception
              WHERE exception.service_id = calendar.service_id
                AND exception.date = $6::date
                AND exception.exception_type = 2
            )
          UNION
          SELECT exception.service_id
          FROM calendar_dates exception
          WHERE exception.date = $6::date AND exception.exception_type = 1
        ),
        latest_import_runs AS (
          SELECT DISTINCT ON (summary->>'feed_id')
            summary->>'feed_id' AS source_feed_id,
            id AS import_run_id
          FROM import_runs
          WHERE status = 'success'
            AND summary ? 'feed_id'
          ORDER BY summary->>'feed_id', finished_at DESC NULLS LAST, started_at DESC
        ),
        candidate_legs AS (
          SELECT
            st_from.trip_id,
            r.id AS route_id,
            st_from.stop_id AS from_stop_id,
            st_to.stop_id AS to_stop_id,
            st_from.departure_time,
            st_to.arrival_time,
            r.source_priority,
            t.service_id IN (SELECT service_id FROM active_services) AS service_verified,
            CASE
              WHEN lower(r.mode) IN ('train', 'rail') OR r.gtfs_route_type = 2 OR r.gtfs_route_type BETWEEN 100 AND 199 OR r.gtfs_route_type BETWEEN 400 AND 499 OR lower(r.id) LIKE '%train%' OR lower(r.source_id) LIKE '%train%' THEN 'train'
              WHEN lower(r.mode) = 'tram' OR r.gtfs_route_type = 0 OR r.gtfs_route_type BETWEEN 900 AND 999 THEN 'tram'
              WHEN lower(r.mode) = 'metro' OR r.gtfs_route_type = 1 THEN 'metro'
              WHEN lower(r.mode) = 'bus' OR r.gtfs_route_type = 3 OR r.gtfs_route_type BETWEEN 200 AND 299 OR r.gtfs_route_type BETWEEN 700 AND 799 THEN 'bus'
              WHEN lower(r.mode) = 'ferry' OR r.gtfs_route_type = 4 OR r.gtfs_route_type BETWEEN 1000 AND 1099 THEN 'ferry'
              WHEN lower(r.mode) IN ('cable_car', 'cablecar') OR r.gtfs_route_type = 5 OR r.gtfs_route_type BETWEEN 1300 AND 1399 THEN 'cable_car'
              WHEN lower(r.mode) = 'trolleybus' OR r.gtfs_route_type = 11 OR r.gtfs_route_type BETWEEN 800 AND 899 THEN 'trolleybus'
              ELSE 'unknown'
            END AS public_mode
          FROM stop_times st_from
          JOIN stop_times st_to
            ON st_to.trip_id = st_from.trip_id
           AND st_to.stop_sequence > st_from.stop_sequence
          JOIN trips t ON t.id = st_from.trip_id
          JOIN source_feeds feed
            ON feed.id = t.source_feed_id
           AND feed.enabled = true
          LEFT JOIN latest_import_runs lir
            ON lir.source_feed_id = t.source_feed_id
           AND lir.import_run_id = t.import_run_id
          JOIN routes r ON r.id = t.route_id
          WHERE st_from.stop_id = ANY($1)
            AND st_to.stop_id = ANY($2)
            AND st_from.departure_time >= $3
            AND (
              t.service_id IN (SELECT service_id FROM active_services)
              OR NOT EXISTS (SELECT 1 FROM calendars WHERE source_feed_id = t.source_feed_id)
                 AND NOT EXISTS (SELECT 1 FROM calendar_dates WHERE source_feed_id = t.source_feed_id)
            )
            AND (
              lir.import_run_id IS NOT NULL
              OR NOT EXISTS (
                SELECT 1 FROM latest_import_runs latest_for_feed
                WHERE latest_for_feed.source_feed_id = t.source_feed_id
              )
            )
            AND COALESCE(st_from.pickup_type, 0) IN (0, 2, 3)
            AND COALESCE(st_to.drop_off_type, 0) IN (0, 2, 3)
        )
        SELECT
          trip_id,
          route_id,
          from_stop_id,
          to_stop_id,
          departure_time,
          arrival_time,
          source_priority,
          service_verified,
          public_mode AS mode
        FROM candidate_legs
        WHERE public_mode <> 'unknown'
          AND ($4 = false OR public_mode = ANY($5))
        ORDER BY service_verified DESC, arrival_time ASC, departure_time ASC, source_priority ASC
        LIMIT $7
        "#,
    )
    .bind(from_stop_ids.to_vec())
    .bind(to_stop_ids.to_vec())
    .bind(departure_time as i32)
    .bind(!mode_filters.is_empty())
    .bind(mode_filters.to_vec())
    .bind(service_date)
    .bind(candidate_limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            let departure_time = row.get::<i32, _>("departure_time") as u32;
            let arrival_time = row.get::<i32, _>("arrival_time") as u32;
            Journey {
                id: format!("journey-{}", index + 1),
                legs: vec![JourneyLeg {
                    from_stop_id: row.get("from_stop_id"),
                    to_stop_id: row.get("to_stop_id"),
                    route_id: Some(row.get("route_id")),
                    trip_id: Some(row.get("trip_id")),
                    departure_time,
                    arrival_time,
                    mode: db_mode_to_model(&row.get::<String, _>("mode")),
                    warnings: Vec::new(),
                    geometry: None,
                }],
                departure_time,
                arrival_time,
                duration_seconds: arrival_time.saturating_sub(departure_time),
                transfer_count: 0,
                walking_distance_meters: 0,
                realtime_status: RealtimeStatus::Unavailable,
                risk_score: 0.0,
                labels: vec!["nejrychlejsi".to_string()],
            }
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
#[allow(dead_code)] // Retained temporarily for rollback comparison while RAPTOR is deployed.
async fn one_transfer_journeys_db(
    pool: &PgPool,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
    departure_time: u32,
    mode_filters: &[String],
    service_date: chrono::NaiveDate,
    min_transfer_seconds: i32,
    max_transfer_wait_seconds: i32,
    candidate_limit: i64,
) -> Result<Vec<Journey>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        WITH active_services AS (
          SELECT calendar.service_id
          FROM calendars calendar
          WHERE $8::date BETWEEN calendar.start_date AND calendar.end_date
            AND CASE EXTRACT(ISODOW FROM $8::date)::integer
              WHEN 1 THEN calendar.monday
              WHEN 2 THEN calendar.tuesday
              WHEN 3 THEN calendar.wednesday
              WHEN 4 THEN calendar.thursday
              WHEN 5 THEN calendar.friday
              WHEN 6 THEN calendar.saturday
              WHEN 7 THEN calendar.sunday
            END
            AND NOT EXISTS (
              SELECT 1 FROM calendar_dates exception
              WHERE exception.service_id = calendar.service_id
                AND exception.date = $8::date
                AND exception.exception_type = 2
            )
          UNION
          SELECT exception.service_id
          FROM calendar_dates exception
          WHERE exception.date = $8::date AND exception.exception_type = 1
        ),
        latest_import_runs AS (
          SELECT DISTINCT ON (summary->>'feed_id')
            summary->>'feed_id' AS source_feed_id,
            id AS import_run_id
          FROM import_runs
          WHERE status = 'success'
            AND summary ? 'feed_id'
          ORDER BY summary->>'feed_id', finished_at DESC NULLS LAST, started_at DESC
        ),
        origin_departures AS MATERIALIZED (
          SELECT DISTINCT ON (stop_time.trip_id)
            stop_time.trip_id,
            stop_time.stop_id,
            stop_time.stop_sequence,
            stop_time.departure_time
          FROM stop_times stop_time
          JOIN trips endpoint_trip ON endpoint_trip.id = stop_time.trip_id
          LEFT JOIN latest_import_runs endpoint_import
            ON endpoint_import.source_feed_id = endpoint_trip.source_feed_id
           AND endpoint_import.import_run_id = endpoint_trip.import_run_id
          WHERE stop_time.stop_id = ANY($1)
            AND stop_time.departure_time >= $3
            AND COALESCE(stop_time.pickup_type, 0) IN (0, 2, 3)
            AND (
              endpoint_trip.service_id IN (SELECT service_id FROM active_services)
              OR NOT EXISTS (
                SELECT 1 FROM calendars
                WHERE source_feed_id = endpoint_trip.source_feed_id
              ) AND NOT EXISTS (
                SELECT 1 FROM calendar_dates
                WHERE source_feed_id = endpoint_trip.source_feed_id
              )
            )
            AND (
              endpoint_import.import_run_id IS NOT NULL
              OR NOT EXISTS (
                SELECT 1 FROM latest_import_runs latest_for_feed
                WHERE latest_for_feed.source_feed_id = endpoint_trip.source_feed_id
              )
            )
          ORDER BY stop_time.trip_id, stop_time.departure_time ASC,
                   stop_time.stop_sequence ASC
        ),
        destination_arrivals AS MATERIALIZED (
          SELECT DISTINCT ON (stop_time.trip_id)
            stop_time.trip_id,
            stop_time.stop_id,
            stop_time.stop_sequence,
            stop_time.arrival_time
          FROM stop_times stop_time
          JOIN trips endpoint_trip ON endpoint_trip.id = stop_time.trip_id
          LEFT JOIN latest_import_runs endpoint_import
            ON endpoint_import.source_feed_id = endpoint_trip.source_feed_id
           AND endpoint_import.import_run_id = endpoint_trip.import_run_id
          WHERE stop_time.stop_id = ANY($2)
            AND stop_time.arrival_time >= $3 + $6
            AND COALESCE(stop_time.drop_off_type, 0) IN (0, 2, 3)
            AND (
              endpoint_trip.service_id IN (SELECT service_id FROM active_services)
              OR NOT EXISTS (
                SELECT 1 FROM calendars
                WHERE source_feed_id = endpoint_trip.source_feed_id
              ) AND NOT EXISTS (
                SELECT 1 FROM calendar_dates
                WHERE source_feed_id = endpoint_trip.source_feed_id
              )
            )
            AND (
              endpoint_import.import_run_id IS NOT NULL
              OR NOT EXISTS (
                SELECT 1 FROM latest_import_runs latest_for_feed
                WHERE latest_for_feed.source_feed_id = endpoint_trip.source_feed_id
              )
            )
          ORDER BY stop_time.trip_id, stop_time.arrival_time ASC,
                   stop_time.stop_sequence ASC
        ),
        first_legs AS (
          SELECT
            st_from.trip_id AS first_trip_id,
            r.id AS first_route_id,
            st_from.stop_id AS first_from_stop_id,
            st_mid.stop_id AS transfer_arrival_stop_id,
            st_from.departure_time AS first_departure_time,
            st_mid.arrival_time AS first_arrival_time,
            r.source_priority AS first_source_priority,
            t.service_id IN (SELECT service_id FROM active_services) AS first_service_verified,
            CASE
              WHEN s_mid.stop_area_id IS NOT NULL THEN 'area:' || s_mid.stop_area_id
              WHEN s_mid.parent_station_id IS NOT NULL THEN 'parent:' || s_mid.parent_station_id
              WHEN s_mid.lat IS NOT NULL AND s_mid.lon IS NOT NULL
                THEN 'geo:' || s_mid.normalized_name || ':' || round(s_mid.lat::numeric, 2)::text || ':' || round(s_mid.lon::numeric, 2)::text
              WHEN s_mid.id ~ 'SR70S-CZ-[0-9]+-[0-9][[:alnum:]]{0,3}$'
                THEN 'rail:' || regexp_replace(s_mid.id, '-[0-9][[:alnum:]]{0,3}$', '')
              WHEN s_mid.id ~ 'SR70S-CZ-[0-9]+$' THEN 'rail:' || s_mid.id
              ELSE 'stop:' || s_mid.id
            END AS transfer_key,
            CASE
              WHEN lower(r.mode) IN ('train', 'rail') OR r.gtfs_route_type = 2 OR r.gtfs_route_type BETWEEN 100 AND 199 OR r.gtfs_route_type BETWEEN 400 AND 499 OR lower(r.id) LIKE '%train%' OR lower(r.source_id) LIKE '%train%' THEN 'train'
              WHEN lower(r.mode) = 'tram' OR r.gtfs_route_type = 0 OR r.gtfs_route_type BETWEEN 900 AND 999 THEN 'tram'
              WHEN lower(r.mode) = 'metro' OR r.gtfs_route_type = 1 THEN 'metro'
              WHEN lower(r.mode) = 'bus' OR r.gtfs_route_type = 3 OR r.gtfs_route_type BETWEEN 200 AND 299 OR r.gtfs_route_type BETWEEN 700 AND 799 THEN 'bus'
              WHEN lower(r.mode) = 'ferry' OR r.gtfs_route_type = 4 OR r.gtfs_route_type BETWEEN 1000 AND 1099 THEN 'ferry'
              WHEN lower(r.mode) IN ('cable_car', 'cablecar') OR r.gtfs_route_type = 5 OR r.gtfs_route_type BETWEEN 1300 AND 1399 THEN 'cable_car'
              WHEN lower(r.mode) = 'trolleybus' OR r.gtfs_route_type = 11 OR r.gtfs_route_type BETWEEN 800 AND 899 THEN 'trolleybus'
              ELSE 'unknown'
            END AS first_mode
          FROM origin_departures st_from
          JOIN stop_times st_mid
            ON st_mid.trip_id = st_from.trip_id
           AND st_mid.stop_sequence > st_from.stop_sequence
          JOIN stops s_mid
            ON s_mid.id = st_mid.stop_id
           AND s_mid.is_active = true
          JOIN trips t ON t.id = st_from.trip_id
          LEFT JOIN latest_import_runs lir
            ON lir.source_feed_id = t.source_feed_id
           AND lir.import_run_id = t.import_run_id
          JOIN routes r ON r.id = t.route_id
          WHERE (
              t.service_id IN (SELECT service_id FROM active_services)
              OR NOT EXISTS (SELECT 1 FROM calendars WHERE source_feed_id = t.source_feed_id)
                 AND NOT EXISTS (SELECT 1 FROM calendar_dates WHERE source_feed_id = t.source_feed_id)
            )
            AND (
              lir.import_run_id IS NOT NULL
              OR NOT EXISTS (
                SELECT 1 FROM latest_import_runs latest_for_feed
                WHERE latest_for_feed.source_feed_id = t.source_feed_id
              )
            )
            AND COALESCE(st_mid.drop_off_type, 0) IN (0, 2, 3)
        ),
        filtered_first_legs AS MATERIALIZED (
          SELECT *
          FROM first_legs
          WHERE first_mode <> 'unknown'
            AND ($4 = false OR first_mode = ANY($5))
          ORDER BY first_departure_time ASC, first_arrival_time ASC
          LIMIT 4000
        ),
        second_legs AS (
          SELECT
            st_transfer.trip_id AS second_trip_id,
            r2.id AS second_route_id,
            st_transfer.stop_id AS transfer_departure_stop_id,
            st_to.stop_id AS second_to_stop_id,
            st_transfer.departure_time AS second_departure_time,
            st_to.arrival_time AS second_arrival_time,
            r2.source_priority AS second_source_priority,
            t2.service_id IN (SELECT service_id FROM active_services) AS second_service_verified,
            CASE
              WHEN s_transfer.stop_area_id IS NOT NULL THEN 'area:' || s_transfer.stop_area_id
              WHEN s_transfer.parent_station_id IS NOT NULL THEN 'parent:' || s_transfer.parent_station_id
              WHEN s_transfer.lat IS NOT NULL AND s_transfer.lon IS NOT NULL
                THEN 'geo:' || s_transfer.normalized_name || ':' || round(s_transfer.lat::numeric, 2)::text || ':' || round(s_transfer.lon::numeric, 2)::text
              WHEN s_transfer.id ~ 'SR70S-CZ-[0-9]+-[0-9][[:alnum:]]{0,3}$'
                THEN 'rail:' || regexp_replace(s_transfer.id, '-[0-9][[:alnum:]]{0,3}$', '')
              WHEN s_transfer.id ~ 'SR70S-CZ-[0-9]+$' THEN 'rail:' || s_transfer.id
              ELSE 'stop:' || s_transfer.id
            END AS transfer_key,
            CASE
              WHEN lower(r2.mode) IN ('train', 'rail') OR r2.gtfs_route_type = 2 OR r2.gtfs_route_type BETWEEN 100 AND 199 OR r2.gtfs_route_type BETWEEN 400 AND 499 OR lower(r2.id) LIKE '%train%' OR lower(r2.source_id) LIKE '%train%' THEN 'train'
              WHEN lower(r2.mode) = 'tram' OR r2.gtfs_route_type = 0 OR r2.gtfs_route_type BETWEEN 900 AND 999 THEN 'tram'
              WHEN lower(r2.mode) = 'metro' OR r2.gtfs_route_type = 1 THEN 'metro'
              WHEN lower(r2.mode) = 'bus' OR r2.gtfs_route_type = 3 OR r2.gtfs_route_type BETWEEN 200 AND 299 OR r2.gtfs_route_type BETWEEN 700 AND 799 THEN 'bus'
              WHEN lower(r2.mode) = 'ferry' OR r2.gtfs_route_type = 4 OR r2.gtfs_route_type BETWEEN 1000 AND 1099 THEN 'ferry'
              WHEN lower(r2.mode) IN ('cable_car', 'cablecar') OR r2.gtfs_route_type = 5 OR r2.gtfs_route_type BETWEEN 1300 AND 1399 THEN 'cable_car'
              WHEN lower(r2.mode) = 'trolleybus' OR r2.gtfs_route_type = 11 OR r2.gtfs_route_type BETWEEN 800 AND 899 THEN 'trolleybus'
              ELSE 'unknown'
            END AS second_mode
          FROM destination_arrivals st_to
          JOIN stop_times st_transfer
            ON st_transfer.trip_id = st_to.trip_id
           AND st_transfer.stop_sequence < st_to.stop_sequence
          JOIN stops s_transfer
            ON s_transfer.id = st_transfer.stop_id
           AND s_transfer.is_active = true
          JOIN trips t2 ON t2.id = st_transfer.trip_id
          LEFT JOIN latest_import_runs lir2
            ON lir2.source_feed_id = t2.source_feed_id
           AND lir2.import_run_id = t2.import_run_id
          JOIN routes r2 ON r2.id = t2.route_id
          WHERE st_transfer.departure_time >= $3 + $6
            AND (
              t2.service_id IN (SELECT service_id FROM active_services)
              OR NOT EXISTS (SELECT 1 FROM calendars WHERE source_feed_id = t2.source_feed_id)
                 AND NOT EXISTS (SELECT 1 FROM calendar_dates WHERE source_feed_id = t2.source_feed_id)
            )
            AND (
              lir2.import_run_id IS NOT NULL
              OR NOT EXISTS (
                SELECT 1 FROM latest_import_runs latest_for_feed
                WHERE latest_for_feed.source_feed_id = t2.source_feed_id
              )
            )
            AND COALESCE(st_transfer.pickup_type, 0) IN (0, 2, 3)
        ),
        filtered_second_legs AS MATERIALIZED (
          SELECT *
          FROM second_legs
          WHERE second_mode <> 'unknown'
            AND ($4 = false OR second_mode = ANY($5))
          ORDER BY second_arrival_time ASC, second_departure_time DESC
          LIMIT 4000
        ),
        candidate_journeys AS (
          SELECT
            first_legs.first_trip_id,
            first_legs.first_route_id,
            first_legs.first_from_stop_id,
            first_legs.transfer_arrival_stop_id,
            first_legs.first_departure_time,
            first_legs.first_arrival_time,
            first_legs.first_source_priority,
            first_legs.first_service_verified,
            first_legs.first_mode,
            second_legs.second_trip_id,
            second_legs.second_route_id,
            second_legs.transfer_departure_stop_id,
            second_legs.second_to_stop_id,
            second_legs.second_departure_time,
            second_legs.second_arrival_time,
            second_legs.second_source_priority,
            second_legs.second_service_verified,
            second_legs.second_mode
          FROM filtered_first_legs first_legs
          JOIN filtered_second_legs second_legs
            ON first_legs.first_trip_id <> second_legs.second_trip_id
           AND second_legs.second_departure_time >= first_legs.first_arrival_time + $6
           AND second_legs.second_departure_time <= first_legs.first_arrival_time + $7
           AND first_legs.transfer_key = second_legs.transfer_key
        )
        SELECT *
        FROM candidate_journeys
        WHERE first_mode <> 'unknown'
          AND ($4 = false OR first_mode = ANY($5))
        ORDER BY (first_service_verified AND second_service_verified) DESC,
                 second_arrival_time ASC, first_departure_time ASC,
                 first_source_priority + second_source_priority ASC
        LIMIT $9
        "#,
    )
    .bind(from_stop_ids.to_vec())
    .bind(to_stop_ids.to_vec())
    .bind(departure_time as i32)
    .bind(!mode_filters.is_empty())
    .bind(mode_filters.to_vec())
    .bind(min_transfer_seconds)
    .bind(max_transfer_wait_seconds)
    .bind(service_date)
    .bind(candidate_limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let departure_time = row.get::<i32, _>("first_departure_time") as u32;
            let first_arrival_time = row.get::<i32, _>("first_arrival_time") as u32;
            let second_departure_time = row.get::<i32, _>("second_departure_time") as u32;
            let arrival_time = row.get::<i32, _>("second_arrival_time") as u32;
            Journey {
                id: String::new(),
                legs: vec![
                    JourneyLeg {
                        from_stop_id: row.get("first_from_stop_id"),
                        to_stop_id: row.get("transfer_arrival_stop_id"),
                        route_id: Some(row.get("first_route_id")),
                        trip_id: Some(row.get("first_trip_id")),
                        departure_time,
                        arrival_time: first_arrival_time,
                        mode: db_mode_to_model(&row.get::<String, _>("first_mode")),
                        warnings: Vec::new(),
                        geometry: None,
                    },
                    JourneyLeg {
                        from_stop_id: row.get("transfer_departure_stop_id"),
                        to_stop_id: row.get("second_to_stop_id"),
                        route_id: Some(row.get("second_route_id")),
                        trip_id: Some(row.get("second_trip_id")),
                        departure_time: second_departure_time,
                        arrival_time,
                        mode: db_mode_to_model(&row.get::<String, _>("second_mode")),
                        warnings: Vec::new(),
                        geometry: None,
                    },
                ],
                departure_time,
                arrival_time,
                duration_seconds: arrival_time.saturating_sub(departure_time),
                transfer_count: 1,
                walking_distance_meters: 0,
                realtime_status: RealtimeStatus::Unavailable,
                risk_score: 0.0,
                labels: vec!["s prestupem".to_string()],
            }
        })
        .collect())
}

async fn attach_journey_geometries_db(
    pool: &PgPool,
    pedestrian_router: &PedestrianRouter,
    journeys: &mut Vec<Journey>,
) -> Result<Vec<String>, sqlx::Error> {
    let trip_ids = journeys
        .iter()
        .flat_map(|journey| journey.legs.iter())
        .filter_map(|leg| leg.trip_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let stop_ids = journeys
        .iter()
        .flat_map(|journey| journey.legs.iter())
        .flat_map(|leg| [&leg.from_stop_id, &leg.to_stop_id])
        .filter(|stop_id| coordinate_from_stop_id(stop_id).is_none())
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let shape_rows = if trip_ids.is_empty() {
        Vec::new()
    } else {
        sqlx::query(
            r#"
            SELECT trip.id AS trip_id, shape.shape_pt_sequence,
                   ST_Y(shape.geom::geometry) AS lat,
                   ST_X(shape.geom::geometry) AS lon
            FROM trips trip
            JOIN shapes shape ON shape.shape_id = trip.shape_id
            WHERE trip.id = ANY($1)
            ORDER BY trip.id, shape.shape_pt_sequence
            "#,
        )
        .bind(&trip_ids)
        .fetch_all(pool)
        .await?
    };
    let mut shapes_by_trip = HashMap::<String, Vec<(f64, f64)>>::new();
    for row in shape_rows {
        shapes_by_trip
            .entry(row.get("trip_id"))
            .or_default()
            .push((row.get("lat"), row.get("lon")));
    }

    let stop_rows = if stop_ids.is_empty() {
        Vec::new()
    } else {
        sqlx::query("SELECT id, lat, lon FROM stops WHERE id = ANY($1)")
            .bind(&stop_ids)
            .fetch_all(pool)
            .await?
    };
    let mut coordinates = stop_rows
        .into_iter()
        .filter_map(|row| {
            Some((
                row.get::<String, _>("id"),
                (
                    row.get::<Option<f64>, _>("lat")?,
                    row.get::<Option<f64>, _>("lon")?,
                ),
            ))
        })
        .collect::<HashMap<_, _>>();
    for journey in journeys.iter() {
        for leg in &journey.legs {
            for stop_id in [&leg.from_stop_id, &leg.to_stop_id] {
                if let Some(coordinate) = coordinate_from_stop_id(stop_id) {
                    coordinates.insert(stop_id.clone(), coordinate);
                }
            }
        }
    }

    let mut diagnostics = Vec::new();
    let mut complete = Vec::with_capacity(journeys.len());
    for mut journey in journeys.drain(..) {
        let mut valid = true;
        for leg in &mut journey.legs {
            let Some(&from) = coordinates.get(&leg.from_stop_id) else {
                diagnostics.push(format!(
                    "candidate_rejected:geometry_endpoint_missing:{}",
                    leg.from_stop_id
                ));
                valid = false;
                break;
            };
            let Some(&to) = coordinates.get(&leg.to_stop_id) else {
                diagnostics.push(format!(
                    "candidate_rejected:geometry_endpoint_missing:{}",
                    leg.to_stop_id
                ));
                valid = false;
                break;
            };
            if is_walking_leg(leg) {
                if leg.geometry.as_ref().is_some_and(|geometry| {
                    geometry_matches_endpoints(geometry, from, to, MAX_WALKING_SNAP_DISTANCE_M)
                }) {
                    continue;
                }
                let _permit = pedestrian_router
                    .permits
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("pedestrian router semaphore is open");
                match walking_route_cached_db(
                    pool,
                    pedestrian_router,
                    from,
                    to,
                    MAX_ENDPOINT_WALKING_DISTANCE_M,
                )
                .await?
                {
                    Ok(route) => {
                        leg.geometry = Some(route.geometry);
                        if !leg
                            .warnings
                            .iter()
                            .any(|warning| warning.starts_with("walking_source:"))
                        {
                            leg.warnings.push(
                                "walking_source:pedestrian_graph_geometry_repair".to_string(),
                            );
                        }
                    }
                    Err(reason) => {
                        diagnostics.push(format!(
                            "candidate_rejected:{}:{}->{}",
                            reason.diagnostic_code(),
                            leg.from_stop_id,
                            leg.to_stop_id
                        ));
                        valid = false;
                        break;
                    }
                }
            } else {
                let Some(trip_id) = leg.trip_id.as_deref() else {
                    diagnostics.push("candidate_rejected:transit_leg_without_trip".to_string());
                    valid = false;
                    break;
                };
                let Some(shape) = shapes_by_trip.get(trip_id) else {
                    diagnostics.push(format!("candidate_rejected:missing_gtfs_shape:{trip_id}"));
                    valid = false;
                    break;
                };
                let Some(geometry) = clip_gtfs_shape(shape, from, to) else {
                    diagnostics.push(format!(
                        "candidate_rejected:unclippable_gtfs_shape:{trip_id}:{}->{}",
                        leg.from_stop_id, leg.to_stop_id
                    ));
                    valid = false;
                    break;
                };
                leg.geometry = Some(geometry);
            }
        }
        if valid && journey.legs.iter().all(|leg| leg.geometry.is_some()) {
            complete.push(journey);
        }
    }
    *journeys = complete;
    Ok(diagnostics)
}

fn clip_gtfs_shape(shape: &[(f64, f64)], from: (f64, f64), to: (f64, f64)) -> Option<Value> {
    if shape.len() < 2 {
        return None;
    }
    let mut best_from = (f64::INFINITY, 0usize);
    let mut best_pair = (f64::INFINITY, 0usize, 0usize, f64::INFINITY, f64::INFINITY);
    for to_index in 1..shape.len() {
        let from_index = to_index - 1;
        let from_distance = haversine_m(from.0, from.1, shape[from_index].0, shape[from_index].1);
        if from_distance < best_from.0 {
            best_from = (from_distance, from_index);
        }
        let to_distance = haversine_m(to.0, to.1, shape[to_index].0, shape[to_index].1);
        let score = best_from.0 + to_distance;
        if score < best_pair.0 {
            best_pair = (score, best_from.1, to_index, best_from.0, to_distance);
        }
    }
    if best_pair.3 > MAX_TRANSIT_SHAPE_SNAP_DISTANCE_M
        || best_pair.4 > MAX_TRANSIT_SHAPE_SNAP_DISTANCE_M
        || best_pair.1 >= best_pair.2
    {
        return None;
    }
    let mut coordinates = shape[best_pair.1..=best_pair.2]
        .iter()
        .map(|(lat, lon)| json!([lon, lat]))
        .collect::<Vec<_>>();
    *coordinates.first_mut()? = json!([from.1, from.0]);
    *coordinates.last_mut()? = json!([to.1, to.0]);
    Some(json!({"type": "LineString", "coordinates": coordinates}))
}

fn geometry_matches_endpoints(
    geometry: &Value,
    from: (f64, f64),
    to: (f64, f64),
    tolerance_meters: f64,
) -> bool {
    let endpoints = match geometry["type"].as_str() {
        Some("LineString") => geometry["coordinates"].as_array().and_then(|coordinates| {
            Some((
                geojson_position(coordinates.first()?)?,
                geojson_position(coordinates.last()?)?,
            ))
        }),
        Some("MultiLineString") => geometry["coordinates"].as_array().and_then(|lines| {
            let first_line = lines.first()?.as_array()?;
            let last_line = lines.last()?.as_array()?;
            Some((
                geojson_position(first_line.first()?)?,
                geojson_position(last_line.last()?)?,
            ))
        }),
        _ => None,
    };
    endpoints.is_some_and(|(first, last)| {
        haversine_m(from.0, from.1, first.1, first.0) <= tolerance_meters
            && haversine_m(to.0, to.1, last.1, last.0) <= tolerance_meters
    })
}

async fn journey_related_data_db(
    pool: &PgPool,
    journeys: &[Journey],
    include_route_geometries: bool,
) -> Result<Value, sqlx::Error> {
    let mut stop_ids = HashSet::new();
    let mut route_ids = HashSet::new();
    let mut trip_ids = HashSet::new();

    for journey in journeys {
        for leg in &journey.legs {
            stop_ids.insert(leg.from_stop_id.clone());
            stop_ids.insert(leg.to_stop_id.clone());
            if let Some(route_id) = &leg.route_id {
                route_ids.insert(route_id.clone());
            }
            if let Some(trip_id) = &leg.trip_id {
                trip_ids.insert(trip_id.clone());
            }
        }
    }

    let stop_ids = stop_ids.into_iter().collect::<Vec<_>>();
    let route_ids = route_ids.into_iter().collect::<Vec<_>>();
    let trip_ids = trip_ids.into_iter().collect::<Vec<_>>();
    let mut source_feed_ids = HashSet::new();
    let mut agency_ids = HashSet::new();

    let stops_future = async {
        if stop_ids.is_empty() {
            return Ok::<_, sqlx::Error>(Vec::new());
        }
        sqlx::query(
            r#"
            SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
                   lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
                   platform_code, location_type, parent_station_id, station_id, complex_id,
                   has_station_layout, station_layout_version, wheelchair_boarding,
                   modes, source_priority, is_active
            FROM stops
            WHERE id = ANY($1)
            ORDER BY name ASC, platform_code ASC NULLS FIRST
            "#,
        )
        .bind(&stop_ids)
        .fetch_all(pool)
        .await
    };
    let routes_future = async {
        if route_ids.is_empty() {
            return Ok::<_, sqlx::Error>(Vec::new());
        }
        sqlx::query(
            r#"
            SELECT id, source_feed_id, source_id, agency_id, operator_id, short_name, long_name,
                   mode, gtfs_route_type, color, text_color, source_priority, is_active
            FROM routes
            WHERE id = ANY($1)
            ORDER BY source_priority ASC, short_name ASC NULLS LAST, id ASC
            "#,
        )
        .bind(&route_ids)
        .fetch_all(pool)
        .await
    };
    let trips_future = async {
        if trip_ids.is_empty() {
            return Ok::<_, sqlx::Error>(Vec::new());
        }
        sqlx::query(
            r#"
            SELECT trip.id, trip.source_feed_id, trip.source_id, trip.route_id,
                   trip.service_id, trip.headsign, trip.direction_id, trip.shape_id,
                   trip.restrictions, trip.raw_source_metadata, trip.source_priority,
                   terminal.stop_id AS terminal_stop_id,
                   terminal_stop.name AS terminal_stop_name,
                   terminal_stop.platform_code AS terminal_stop_platform
            FROM trips trip
            LEFT JOIN LATERAL (
              SELECT stop_time.stop_id
              FROM stop_times stop_time
              WHERE stop_time.trip_id = trip.id
              ORDER BY stop_time.stop_sequence DESC
              LIMIT 1
            ) terminal ON true
            LEFT JOIN stops terminal_stop ON terminal_stop.id = terminal.stop_id
            WHERE trip.id = ANY($1)
            ORDER BY trip.source_priority ASC, trip.id ASC
            "#,
        )
        .bind(&trip_ids)
        .fetch_all(pool)
        .await
    };
    let stop_times_future = async {
        if trip_ids.is_empty() || stop_ids.is_empty() {
            return Ok::<_, sqlx::Error>(Vec::new());
        }
        sqlx::query(
            r#"
            SELECT trip_id, stop_id, stop_sequence, arrival_time, departure_time,
                   pickup_type, drop_off_type, timepoint, stop_headsign, platform, raw_notes,
                   source_feed_id, source_priority
            FROM stop_times
            WHERE trip_id = ANY($1)
              AND stop_id = ANY($2)
            ORDER BY trip_id ASC, stop_sequence ASC
            "#,
        )
        .bind(&trip_ids)
        .bind(&stop_ids)
        .fetch_all(pool)
        .await
    };
    let route_geometries_future = async {
        if route_ids.is_empty() || !include_route_geometries {
            return Ok::<_, sqlx::Error>(Vec::new());
        }
        sqlx::query(
            r#"
            SELECT source_feed_id, source_feature_id, route_id, source_route_id,
                   validity, geometry, properties, fetched_at
            FROM route_geometries
            WHERE route_id = ANY($1)
              AND (cardinality(validity) = 0 OR CURRENT_DATE = ANY(validity))
            ORDER BY route_id ASC, source_feature_id ASC
            "#,
        )
        .bind(&route_ids)
        .fetch_all(pool)
        .await
    };
    let (stop_rows, route_rows, trip_rows, stop_time_rows, route_geometry_rows) = tokio::try_join!(
        stops_future,
        routes_future,
        trips_future,
        stop_times_future,
        route_geometries_future
    )?;

    let stops = stop_rows
        .into_iter()
        .map(stop_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    for stop in &stops {
        for source_id in &stop.source_ids {
            source_feed_ids.insert(source_id.feed_id.clone());
        }
    }
    let routes = route_rows
        .into_iter()
        .map(|row| {
            if let Some(feed_id) = row.get::<Option<String>, _>("source_feed_id") {
                source_feed_ids.insert(feed_id);
            }
            if let Some(agency_id) = row.get::<Option<String>, _>("agency_id") {
                agency_ids.insert(agency_id);
            }
            route_row_json(&row)
        })
        .collect::<Vec<_>>();
    let trips = trip_rows
        .into_iter()
        .map(|row| {
            if let Some(feed_id) = row.get::<Option<String>, _>("source_feed_id") {
                source_feed_ids.insert(feed_id);
            }
            let mut trip = trip_row_json(&row);
            let terminal_stop_id = row.get::<Option<String>, _>("terminal_stop_id");
            let terminal_source_name = row.get::<Option<String>, _>("terminal_stop_name");
            let terminal_platform = row.get::<Option<String>, _>("terminal_stop_platform");
            let terminal_stop_name = terminal_stop_id
                .as_deref()
                .zip(terminal_source_name.as_deref())
                .map(|(id, name)| pid_public_stop_name(id, name, terminal_platform.as_deref()));
            trip["terminal_stop_id"] = terminal_stop_id.map_or(Value::Null, Value::String);
            trip["terminal_stop_name"] = terminal_stop_name.map_or(Value::Null, Value::String);
            trip
        })
        .collect::<Vec<_>>();
    let stop_times = stop_time_rows
        .into_iter()
        .map(|row| {
            if let Some(feed_id) = row.get::<Option<String>, _>("source_feed_id") {
                source_feed_ids.insert(feed_id);
            }
            stop_time_row_json(&row)
        })
        .collect::<Vec<_>>();
    let route_geometries = route_geometry_rows
        .into_iter()
        .map(|row| {
            json!({
                "source_feed_id": row.get::<String, _>("source_feed_id"),
                "source_feature_id": row.get::<String, _>("source_feature_id"),
                "route_id": row.get::<Option<String>, _>("route_id"),
                "source_route_id": row.get::<String, _>("source_route_id"),
                "validity": row.get::<Vec<chrono::NaiveDate>, _>("validity"),
                "geometry": row.get::<Value, _>("geometry"),
                "properties": row.get::<Value, _>("properties"),
                "fetched_at": row.get::<DateTime<Utc>, _>("fetched_at")
            })
        })
        .collect::<Vec<_>>();

    let (agencies, source_feeds) = tokio::join!(
        fetch_agencies_json(pool, agency_ids.into_iter().collect()),
        fetch_source_feeds_json(pool, source_feed_ids.into_iter().collect())
    );
    let agencies = agencies?;
    let source_feeds = source_feeds?;

    Ok(json!({
        "stops": stops,
        "routes": routes,
        "trips": trips,
        "stop_times": stop_times,
        "route_geometries": route_geometries,
        "agencies": agencies,
        "source_feeds": source_feeds
    }))
}

async fn journey_routing_realtime_db(
    pool: &PgPool,
    service_date: chrono::NaiveDate,
) -> Result<Arc<RaptorRealtimeData>, sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query(&format!(
        "SET LOCAL statement_timeout = '{ROUTING_REALTIME_STATEMENT_TIMEOUT_MILLIS}ms'"
    ))
    .execute(&mut *transaction)
    .await?;
    let rows = sqlx::query(JOURNEY_ROUTING_REALTIME_QUERY)
        .bind(service_date)
        .fetch_all(&mut *transaction)
        .await?;
    transaction.commit().await?;

    Ok(Arc::new(RaptorRealtimeData::from_updates(
        rows.into_iter().map(|row| RaptorRealtimeUpdate {
            trip_id: row.get("trip_id"),
            stop_id: row.get("stop_id"),
            delay_seconds: row.get("delay_seconds"),
        }),
    )))
}

async fn journey_routing_realtime_cached(
    cache: &RoutingRealtimeCache,
    service_date: chrono::NaiveDate,
) -> Option<RoutingRealtimeSnapshot> {
    let cached = cache.read().await;
    if let Some(entry) = cached.as_ref()
        && entry.service_date == service_date
        && entry.loaded_at.elapsed()
            < std::time::Duration::from_secs(ROUTING_REALTIME_CACHE_TTL_SECONDS)
    {
        return Some(RoutingRealtimeSnapshot {
            data: entry.data.clone(),
            cache_hit: true,
        });
    }
    None
}

async fn journey_realtime_updates_db(
    pool: &PgPool,
    journeys: &[Journey],
    service_date: chrono::NaiveDate,
) -> Result<Vec<Value>, sqlx::Error> {
    let trip_ids = journeys
        .iter()
        .flat_map(|journey| journey.legs.iter())
        .filter_map(|leg| leg.trip_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if trip_ids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(sqlx::query(
        r#"
        SELECT source, source_feed_id, source_entity_id, trip_id, route_id, stop_id,
               CASE WHEN raw_payload->>'stop_sequence' ~ '^[0-9]+$'
                 THEN (raw_payload->>'stop_sequence')::integer ELSE NULL END AS stop_sequence,
               delay_seconds, estimated_arrival, estimated_departure,
               cancellation_status, platform_change, vehicle_id, bearing,
               ST_Y(vehicle_position::geometry) AS latitude,
               ST_X(vehicle_position::geometry) AS longitude,
               fetched_at, valid_until, service_date, confidence
        FROM realtime_updates
        WHERE trip_id = ANY($1)
          AND (valid_until IS NULL OR valid_until >= now())
          AND (service_date IS NULL OR service_date BETWEEN $2::date AND $2::date + 1)
        ORDER BY fetched_at DESC
        LIMIT 10000
        "#,
    )
    .bind(trip_ids)
    .bind(service_date)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "source": row.get::<String, _>("source"),
            "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
            "source_entity_id": row.get::<Option<String>, _>("source_entity_id"),
            "trip_id": row.get::<Option<String>, _>("trip_id"),
            "route_id": row.get::<Option<String>, _>("route_id"),
            "stop_id": row.get::<Option<String>, _>("stop_id"),
            "stop_sequence": row.get::<Option<i32>, _>("stop_sequence"),
            "delay_seconds": row.get::<Option<i32>, _>("delay_seconds"),
            "estimated_arrival": row.get::<Option<DateTime<Utc>>, _>("estimated_arrival"),
            "estimated_departure": row.get::<Option<DateTime<Utc>>, _>("estimated_departure"),
            "cancellation_status": row.get::<Option<String>, _>("cancellation_status"),
            "platform_change": row.get::<Option<String>, _>("platform_change"),
            "vehicle_id": row.get::<Option<String>, _>("vehicle_id"),
            "vehicle_position": match (
                row.get::<Option<f64>, _>("latitude"),
                row.get::<Option<f64>, _>("longitude")
            ) {
                (Some(lat), Some(lon)) => Some(json!({"lat": lat, "lon": lon})),
                _ => None
            },
            "bearing": row.get::<Option<f64>, _>("bearing"),
            "fetched_at": row.get::<DateTime<Utc>, _>("fetched_at"),
            "valid_until": row.get::<Option<DateTime<Utc>>, _>("valid_until"),
            "service_date": row.get::<Option<chrono::NaiveDate>, _>("service_date"),
            "confidence": row.get::<String, _>("confidence")
        })
    })
    .collect())
}

async fn journey_stop_calls_db(
    pool: &PgPool,
    journeys: &[Journey],
) -> Result<HashMap<String, Vec<JourneyStopCall>>, sqlx::Error> {
    let trip_ids = journeys
        .iter()
        .flat_map(|journey| journey.legs.iter())
        .filter_map(|leg| leg.trip_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if trip_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT st.trip_id, st.stop_id, st.stop_sequence, st.arrival_time,
               st.departure_time, st.pickup_type, st.drop_off_type, st.timepoint,
               st.platform AS stop_time_platform, s.name AS stop_name,
               s.municipality, s.lat, s.lon, s.platform_code,
               s.station_id, s.complex_id, s.has_station_layout, s.station_layout_version
        FROM stop_times st
        JOIN stops s ON s.id = st.stop_id
        WHERE st.trip_id = ANY($1)
        ORDER BY st.trip_id ASC, st.stop_sequence ASC
        "#,
    )
    .bind(trip_ids)
    .fetch_all(pool)
    .await?;

    let mut calls = HashMap::<String, Vec<JourneyStopCall>>::new();
    for row in rows {
        let call = JourneyStopCall {
            trip_id: row.get("trip_id"),
            stop_id: row.get("stop_id"),
            stop_sequence: row.get("stop_sequence"),
            scheduled_arrival: row.get("arrival_time"),
            scheduled_departure: row.get("departure_time"),
            pickup_type: row.get("pickup_type"),
            drop_off_type: row.get("drop_off_type"),
            timepoint: row.get("timepoint"),
            stop_time_platform: row.get("stop_time_platform"),
            stop_name: row.get("stop_name"),
            municipality: row.get("municipality"),
            lat: row.get("lat"),
            lon: row.get("lon"),
            platform_code: row.get("platform_code"),
            station_id: row.get("station_id"),
            complex_id: row.get("complex_id"),
            has_station_layout: row.get("has_station_layout"),
            station_layout_version: row.get("station_layout_version"),
        };
        calls.entry(call.trip_id.clone()).or_default().push(call);
    }
    Ok(calls)
}

fn attach_stop_calls(
    journeys: &[Journey],
    journey_values: &mut [Value],
    calls_by_trip: &HashMap<String, Vec<JourneyStopCall>>,
    realtime_updates: &[Value],
) {
    for (journey_index, journey) in journeys.iter().enumerate() {
        for (leg_index, leg) in journey.legs.iter().enumerate() {
            let Some(trip_id) = leg.trip_id.as_deref() else {
                journey_values[journey_index]["legs"][leg_index]["stop_calls"] = json!([]);
                continue;
            };
            let Some(trip_calls) = calls_by_trip.get(trip_id) else {
                journey_values[journey_index]["legs"][leg_index]["stop_calls"] = json!([]);
                continue;
            };
            let start = trip_calls
                .iter()
                .position(|call| {
                    call.stop_id == leg.from_stop_id
                        && service_time_matches(call.scheduled_departure, leg.departure_time)
                })
                .or_else(|| {
                    trip_calls
                        .iter()
                        .position(|call| call.stop_id == leg.from_stop_id)
                });
            let Some(start) = start else {
                journey_values[journey_index]["legs"][leg_index]["stop_calls"] = json!([]);
                continue;
            };
            let end = trip_calls
                .iter()
                .enumerate()
                .skip(start)
                .find(|(_, call)| {
                    call.stop_id == leg.to_stop_id
                        && service_time_matches(call.scheduled_arrival, leg.arrival_time)
                })
                .map(|(index, _)| index)
                .or_else(|| {
                    trip_calls
                        .iter()
                        .enumerate()
                        .skip(start)
                        .find(|(_, call)| call.stop_id == leg.to_stop_id)
                        .map(|(index, _)| index)
                });
            let Some(end) = end.filter(|end| *end >= start) else {
                journey_values[journey_index]["legs"][leg_index]["stop_calls"] = json!([]);
                continue;
            };
            let service_day_offset = if (trip_calls[start].scheduled_departure as u32)
                .saturating_add(SERVICE_DAY_SECONDS)
                == leg.departure_time
            {
                SERVICE_DAY_SECONDS
            } else {
                0
            };

            let stop_calls = trip_calls[start..=end]
                .iter()
                .enumerate()
                .map(|(offset, call)| {
                    let is_origin = offset == 0;
                    let is_destination = start + offset == end;
                    let scheduled_arrival =
                        (call.scheduled_arrival.max(0) as u32).saturating_add(service_day_offset);
                    let scheduled_departure = (call.scheduled_departure.max(0) as u32)
                        .saturating_add(service_day_offset);
                    json!({
                        "trip_id": call.trip_id,
                        "stop_id": call.stop_id,
                        "stop_sequence": call.stop_sequence,
                        "name": call.stop_name,
                        "municipality": call.municipality,
                        "lat": call.lat,
                        "lon": call.lon,
                        "platform": call.stop_time_platform.as_ref().or(call.platform_code.as_ref()),
                        "station_id": call.station_id,
                        "complex_id": call.complex_id,
                        "has_station_layout": call.has_station_layout,
                        "station_layout_version": call.station_layout_version,
                        "scheduled_arrival_seconds": scheduled_arrival,
                        "scheduled_departure_seconds": scheduled_departure,
                        "scheduled_arrival": transit_model::seconds_to_time(scheduled_arrival),
                        "scheduled_departure": transit_model::seconds_to_time(scheduled_departure),
                        "pickup_type": call.pickup_type,
                        "drop_off_type": call.drop_off_type,
                        "timepoint": call.timepoint,
                        "is_origin": is_origin,
                        "is_destination": is_destination,
                        "is_intermediate": !is_origin && !is_destination,
                        "realtime": stop_call_realtime(call, realtime_updates)
                    })
                })
                .collect::<Vec<_>>();
            journey_values[journey_index]["legs"][leg_index]["intermediate_stop_count"] =
                json!(stop_calls.len().saturating_sub(2));
            journey_values[journey_index]["legs"][leg_index]["stop_calls"] =
                Value::Array(stop_calls);
        }
    }
}

fn stop_call_realtime(call: &JourneyStopCall, updates: &[Value]) -> Value {
    let trip_updates = updates
        .iter()
        .filter(|update| update["trip_id"].as_str() == Some(&call.trip_id))
        .collect::<Vec<_>>();
    let exact = trip_updates
        .iter()
        .copied()
        .find(|update| {
            update["stop_id"].as_str() == Some(&call.stop_id)
                && update["stop_sequence"].as_i64() == Some(call.stop_sequence as i64)
        })
        .or_else(|| {
            trip_updates
                .iter()
                .copied()
                .find(|update| update["stop_id"].as_str() == Some(&call.stop_id))
        });
    let cancellation = trip_updates
        .iter()
        .find_map(|update| update["cancellation_status"].as_str());
    let Some(update) = exact else {
        return json!({
            "status": if cancellation.is_some() { "cancelled" } else { "scheduled" },
            "delay_seconds": null,
            "estimated_arrival": null,
            "estimated_departure": null,
            "cancellation_status": cancellation
        });
    };
    json!({
        "status": if cancellation.is_some() { "cancelled" } else { "realtime" },
        "delay_seconds": update["delay_seconds"],
        "estimated_arrival": update["estimated_arrival"],
        "estimated_departure": update["estimated_departure"],
        "cancellation_status": cancellation,
        "platform_change": update["platform_change"],
        "source": update["source"],
        "fetched_at": update["fetched_at"],
        "valid_until": update["valid_until"],
        "confidence": update["confidence"]
    })
}

fn service_time_matches(database_time: i32, journey_time: u32) -> bool {
    let database_time = database_time.max(0) as u32;
    database_time == journey_time
        || database_time.saturating_add(SERVICE_DAY_SECONDS) == journey_time
}

fn journeys_with_realtime(journeys: &[Journey], updates: &[Value]) -> Vec<Value> {
    journeys
        .iter()
        .map(|journey| {
            let mut value = serde_json::to_value(journey).unwrap_or_else(|_| json!({}));
            let mut realtime_legs = 0usize;
            for (index, leg) in journey.legs.iter().enumerate() {
                let Some(trip_id) = leg.trip_id.as_deref() else {
                    continue;
                };
                let trip_updates = updates
                    .iter()
                    .filter(|update| update["trip_id"].as_str() == Some(trip_id))
                    .collect::<Vec<_>>();
                if trip_updates.is_empty() {
                    continue;
                }
                let departure = trip_updates
                    .iter()
                    .copied()
                    .find(|update| update["stop_id"].as_str() == Some(&leg.from_stop_id));
                let arrival = trip_updates
                    .iter()
                    .copied()
                    .find(|update| update["stop_id"].as_str() == Some(&leg.to_stop_id));
                let fallback = trip_updates[0];
                let delay_seconds = departure
                    .and_then(|update| update["delay_seconds"].as_i64())
                    .or_else(|| arrival.and_then(|update| update["delay_seconds"].as_i64()))
                    .or_else(|| fallback["delay_seconds"].as_i64());
                let cancellation = trip_updates
                    .iter()
                    .find_map(|update| update["cancellation_status"].as_str());
                let position_update = trip_updates
                    .iter()
                    .copied()
                    .find(|update| !update["vehicle_position"].is_null())
                    .unwrap_or(fallback);
                let realtime = json!({
                    "status": if cancellation.is_some() { "cancelled" } else { "realtime" },
                    "delay_seconds": delay_seconds,
                    "estimated_departure": departure.and_then(|update| update["estimated_departure"].as_str()),
                    "estimated_arrival": arrival.and_then(|update| update["estimated_arrival"].as_str()),
                    "cancellation_status": cancellation,
                    "platform_change": departure.and_then(|update| update["platform_change"].as_str()),
                    "vehicle_id": position_update["vehicle_id"],
                    "vehicle_position": position_update["vehicle_position"],
                    "bearing": position_update["bearing"],
                    "source": fallback["source"],
                    "fetched_at": fallback["fetched_at"],
                    "valid_until": fallback["valid_until"],
                    "confidence": fallback["confidence"]
                });
                value["legs"][index]["realtime"] = realtime;
                if let Some(delay) = delay_seconds
                    && let Some(warnings) = value["legs"][index]["warnings"].as_array_mut()
                {
                    warnings.push(json!(format!("delay_seconds:{delay}")));
                }
                realtime_legs += 1;
            }
            value["realtime_status"] = json!(match realtime_legs {
                0 => "unavailable",
                count if count == journey.legs.len() => "full",
                _ => "partial",
            });
            if journey.legs.len() > 1 {
                for index in 0..journey.legs.len() - 1 {
                    let delay = value["legs"][index]["realtime"]["delay_seconds"]
                        .as_i64()
                        .unwrap_or(0);
                    let connection_margin = journey.legs[index + 1]
                        .departure_time
                        .saturating_sub(journey.legs[index].arrival_time) as i64;
                    if delay > connection_margin.saturating_sub(MIN_TRANSFER_SECONDS as i64) {
                        value["risk_score"] = json!(1.0);
                        if let Some(warnings) = value["legs"][index]["warnings"].as_array_mut() {
                            warnings.push(json!("connection_at_risk"));
                        }
                    }
                }
            }
            value
        })
        .collect()
}

fn attach_journey_display_metadata(journeys: &mut [Value], related: &Value) {
    let routes = related["routes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|route| Some((route["id"].as_str()?.to_string(), route)))
        .collect::<HashMap<_, _>>();
    let trips = related["trips"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|trip| Some((trip["id"].as_str()?.to_string(), trip)))
        .collect::<HashMap<_, _>>();
    let stops = related["stops"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|stop| Some((stop["id"].as_str()?.to_string(), stop)))
        .collect::<HashMap<_, _>>();
    let stop_times = related["stop_times"].as_array();

    for journey in journeys {
        let Some(legs) = journey["legs"].as_array_mut() else {
            continue;
        };
        for leg in legs {
            let from_name = leg["from_stop_id"]
                .as_str()
                .and_then(|id| stops.get(id))
                .and_then(|stop| nonempty_json_string(&stop["name"]));
            let to_name = leg["to_stop_id"]
                .as_str()
                .and_then(|id| stops.get(id))
                .and_then(|stop| nonempty_json_string(&stop["name"]));
            let route = leg["route_id"].as_str().and_then(|id| routes.get(id));
            let trip = leg["trip_id"].as_str().and_then(|id| trips.get(id));
            let line = route
                .and_then(|route| nonempty_json_string(&route["short_name"]))
                .or_else(|| route.and_then(|route| nonempty_json_string(&route["long_name"])))
                .or_else(|| route.and_then(|route| nonempty_json_string(&route["source_id"])))
                .map(humanize_line_identifier);
            let route_name = route
                .and_then(|route| nonempty_json_string(&route["long_name"]))
                .map(str::to_string)
                .or_else(|| line.clone());
            let stop_headsign = (|| {
                let trip_id = leg["trip_id"].as_str()?;
                let from_stop_id = leg["from_stop_id"].as_str()?;
                let departure_time = leg["departure_time"].as_u64()? as u32;
                stop_times
                    .into_iter()
                    .flatten()
                    .find(|stop_time| {
                        stop_time["trip_id"].as_str() == Some(trip_id)
                            && stop_time["stop_id"].as_str() == Some(from_stop_id)
                            && stop_time["departure_time"].as_i64().is_some_and(|time| {
                                service_time_matches(time as i32, departure_time)
                            })
                    })
                    .and_then(|stop_time| nonempty_json_string(&stop_time["stop_headsign"]))
            })();
            let direction = trip.and_then(|trip| {
                stop_headsign
                    .or_else(|| nonempty_json_string(&trip["headsign"]))
                    .or_else(|| nonempty_json_string(&trip["terminal_stop_name"]))
                    .map(str::to_string)
            });
            let destination = direction.clone().or_else(|| {
                trip.is_none()
                    .then(|| to_name.map(str::to_string))
                    .flatten()
            });
            let mode_name = leg["mode"].as_str().and_then(human_transport_mode_name);
            let display_name = match (mode_name, line.as_deref(), direction.as_deref()) {
                (Some(mode), Some(line), Some(direction)) => {
                    format!("{mode} {line} směr {direction}")
                }
                (Some(mode), Some(line), None) => format!("{mode} {line}"),
                (Some(mode), None, Some(direction)) => format!("{mode} směr {direction}"),
                (Some(mode), None, None) => mode.to_string(),
                (None, _, _) => to_name
                    .map(|destination| format!("Pěšky do {destination}"))
                    .unwrap_or_else(|| "Pěšky".to_string()),
            };

            leg["line"] = line.map_or(Value::Null, Value::String);
            leg["mode_name"] = mode_name.map_or(Value::Null, |name| json!(name));
            leg["route_name"] = route_name.map_or(Value::Null, Value::String);
            leg["destination"] = destination.map_or(Value::Null, Value::String);
            leg["direction"] = direction.map_or(Value::Null, Value::String);
            leg["display_name"] = Value::String(display_name);
            leg["from_stop_name"] = from_name.map_or(Value::Null, |name| json!(name));
            leg["to_stop_name"] = to_name.map_or(Value::Null, |name| json!(name));
        }
    }
}

fn nonempty_json_string(value: &Value) -> Option<&str> {
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn humanize_line_identifier(value: &str) -> String {
    let source_id = value.rsplit(':').next().unwrap_or(value).trim();
    source_id
        .strip_prefix('L')
        .filter(|rest| !rest.is_empty() && rest.chars().all(|character| character.is_ascii_digit()))
        .unwrap_or(source_id)
        .to_string()
}

fn human_transport_mode_name(mode: &str) -> Option<&'static str> {
    match mode {
        "train" => Some("Vlak"),
        "tram" => Some("Tramvaj"),
        "bus" => Some("Autobus"),
        "metro" => Some("Metro"),
        "trolleybus" => Some("Trolejbus"),
        "ferry" => Some("Přívoz"),
        "cable_car" => Some("Lanovka"),
        _ => None,
    }
}

fn journeys_realtime_status(journeys: &[Value]) -> &'static str {
    if journeys
        .iter()
        .any(|journey| journey["realtime_status"] == "full")
    {
        "full"
    } else if journeys
        .iter()
        .any(|journey| journey["realtime_status"] == "partial")
    {
        "partial"
    } else {
        "unavailable"
    }
}

async fn stop_search_related_data_db(pool: &PgPool, stops: &[Stop]) -> Result<Value, sqlx::Error> {
    let stop_ids = stops.iter().map(|stop| stop.id.clone()).collect::<Vec<_>>();
    let stop_area_ids = stops
        .iter()
        .filter_map(|stop| stop.stop_area_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut source_feed_ids = stops
        .iter()
        .flat_map(|stop| {
            stop.source_ids
                .iter()
                .map(|source_id| source_id.feed_id.clone())
        })
        .collect::<HashSet<_>>();
    if stop_ids.is_empty() {
        return Ok(json!({
            "source_ids": [],
            "stop_areas": [],
            "routes": [],
            "source_feeds": []
        }));
    }

    let source_ids_query = sqlx::query(STOP_SEARCH_SOURCE_IDS_QUERY)
        .bind(&stop_ids)
        .fetch_all(pool);

    let stop_areas_query = async {
        if stop_area_ids.is_empty() {
            Ok(Vec::new())
        } else {
            sqlx::query(
                r#"
                SELECT id, name,
                       CASE WHEN geom IS NULL THEN NULL ELSE ST_Y(geom::geometry) END AS lat,
                       CASE WHEN geom IS NULL THEN NULL ELSE ST_X(geom::geometry) END AS lon
                FROM stop_areas
                WHERE id = ANY($1)
                ORDER BY name ASC
                "#,
            )
            .bind(&stop_area_ids)
            .fetch_all(pool)
            .await
        }
    };

    let routes_query = sqlx::query(
        r#"
        SELECT DISTINCT r.id, r.source_feed_id, r.source_id, r.agency_id, r.operator_id,
               r.short_name, r.long_name, r.mode, r.gtfs_route_type, r.color, r.text_color,
               r.source_priority, r.is_active
        FROM stop_times st
        JOIN trips t ON t.id = st.trip_id
        JOIN routes r ON r.id = t.route_id
        JOIN source_feeds feed ON feed.id = t.source_feed_id AND feed.enabled = true
        WHERE st.stop_id = ANY($1)
        ORDER BY r.source_priority ASC, r.short_name ASC NULLS LAST, r.id ASC
        LIMIT 200
        "#,
    )
    .bind(&stop_ids)
    .fetch_all(pool);

    let (source_id_rows, stop_area_rows, route_rows) =
        tokio::try_join!(source_ids_query, stop_areas_query, routes_query)?;

    let source_ids = source_id_rows
        .into_iter()
        .map(|row| {
            let feed_id = row.get::<String, _>("source_feed_id");
            source_feed_ids.insert(feed_id.clone());
            json!({
                "stop_id": row.get::<String, _>("stop_id"),
                "source_feed_id": feed_id,
                "original_source_id": row.get::<String, _>("original_source_id"),
                "import_run_id": row.get::<Option<Uuid>, _>("import_run_id"),
                "priority": row.get::<i32, _>("priority"),
                "confidence": row.get::<Option<String>, _>("confidence"),
                "suppressed_as_duplicate": row.get::<bool, _>("suppressed_as_duplicate")
            })
        })
        .collect::<Vec<_>>();

    let stop_areas = stop_area_rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<String, _>("id"),
                "name": row.get::<String, _>("name"),
                "lat": row.get::<Option<f64>, _>("lat"),
                "lon": row.get::<Option<f64>, _>("lon")
            })
        })
        .collect::<Vec<_>>();

    let routes = route_rows
        .into_iter()
        .map(|row| {
            if let Some(feed_id) = row.get::<Option<String>, _>("source_feed_id") {
                source_feed_ids.insert(feed_id);
            }
            route_row_json(&row)
        })
        .collect::<Vec<_>>();
    let source_feeds = fetch_source_feeds_json(pool, source_feed_ids.into_iter().collect()).await?;

    Ok(json!({
        "source_ids": source_ids,
        "stop_areas": stop_areas,
        "routes": routes,
        "source_feeds": source_feeds
    }))
}

async fn search_stops_db(
    pool: &PgPool,
    raw_query: &str,
    normalized_query: &str,
    limit: usize,
) -> Result<Vec<Stop>, sqlx::Error> {
    let normalized = pid_source_stop_query(&normalize_czech_name(raw_query));
    let candidate_limit = stop_search_candidate_limit(limit);
    let raw_candidate_limit = (candidate_limit * 4).min(400);
    if normalized.is_empty() {
        let rows = sqlx::query(
            r#"
            WITH candidates AS MATERIALIZED (
                SELECT stop.id, stop.source_priority, stop.name, stop.platform_code
                FROM stops AS stop
                WHERE stop.is_active = true
                  AND btrim(stop.name) <> ''
                  AND btrim(stop.normalized_name) <> ''
                  AND stop.location_type IN ('stop', 'station')
                ORDER BY stop.source_priority ASC, stop.name ASC,
                         stop.platform_code ASC NULLS FIRST, stop.id ASC
                LIMIT $1
            )
            SELECT stop.id,
                   COALESCE(preferred.source_feed_id, stop.source_feed_id) AS source_feed_id,
                   stop.name, stop.normalized_name,
                   stop.municipality, stop.district, stop.region, stop.lat, stop.lon,
                   stop.coordinate_confidence, stop.coordinate_source, stop.stop_area_id,
                   stop.platform_code, stop.location_type, stop.parent_station_id,
                   stop.station_id, stop.complex_id, stop.has_station_layout,
                   stop.station_layout_version, stop.wheelchair_boarding, stop.modes,
                   COALESCE(preferred.priority, stop.source_priority) AS source_priority,
                   stop.is_active
            FROM candidates AS candidate
            JOIN stops AS stop ON stop.id = candidate.id
            LEFT JOIN source_feeds AS direct_feed
              ON direct_feed.id = stop.source_feed_id
             AND direct_feed.enabled = true
            LEFT JOIN LATERAL (
                SELECT source_id.source_feed_id, source_id.priority
                FROM stop_source_ids AS source_id
                JOIN source_feeds AS source_feed
                  ON source_feed.id = source_id.source_feed_id
                 AND source_feed.enabled = true
                WHERE source_id.stop_id = stop.id
                  AND direct_feed.id IS NULL
                  AND stop.source_feed_id IS NOT NULL
                ORDER BY source_id.priority ASC, source_id.source_feed_id ASC
                LIMIT 1
            ) AS preferred ON true
            WHERE preferred.source_feed_id IS NOT NULL
               OR stop.source_feed_id IS NULL
               OR direct_feed.id IS NOT NULL
            ORDER BY candidate.source_priority ASC, candidate.name ASC,
                     candidate.platform_code ASC NULLS FIRST, candidate.id ASC
            LIMIT $2
            "#,
        )
        .bind(raw_candidate_limit)
        .bind(candidate_limit)
        .fetch_all(pool)
        .await?;
        let stops = rows
            .into_iter()
            .map(stop_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        return ranked_stop_suggestions_db(pool, stops, normalized_query, limit).await;
    }

    let prefix = format!("{normalized}%");
    let direct_prefix_rows = sqlx::query(
        r#"
        SELECT stop.id, stop.source_feed_id, stop.name, stop.normalized_name,
               stop.municipality, stop.district, stop.region, stop.lat, stop.lon,
               stop.coordinate_confidence, stop.coordinate_source, stop.stop_area_id,
               stop.platform_code, stop.location_type, stop.parent_station_id,
               stop.station_id, stop.complex_id, stop.has_station_layout,
               stop.station_layout_version, stop.wheelchair_boarding, stop.modes,
               stop.source_priority, stop.is_active
        FROM stops AS stop
        LEFT JOIN source_feeds AS direct_feed
          ON direct_feed.id = stop.source_feed_id
         AND direct_feed.enabled = true
        WHERE stop.is_active = true
          AND stop.name <> ''
          AND stop.normalized_name <> ''
          AND stop.location_type IN ('stop', 'station')
          AND (
            stop.normalized_name LIKE $1
            OR (
              stop.location_type = 'station'
              AND stop.normalized_name LIKE '%' || $3
            )
          )
          AND (stop.source_feed_id IS NULL OR direct_feed.id IS NOT NULL)
        ORDER BY (stop.normalized_name LIKE $1) DESC,
                 similarity(stop.normalized_name, $3) DESC,
                 stop.source_priority ASC, stop.name ASC, stop.id ASC
        LIMIT $2
        "#,
    )
    .bind(&prefix)
    .bind(candidate_limit)
    .bind(&normalized)
    .fetch_all(pool)
    .await?;
    if !direct_prefix_rows.is_empty() {
        let stops = direct_prefix_rows
            .into_iter()
            .map(stop_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        return ranked_stop_suggestions_db(pool, stops, normalized_query, limit).await;
    }

    let prefix_rows = sqlx::query(
        r#"
        WITH candidates AS MATERIALIZED (
            (
                SELECT stop.id, stop.normalized_name, stop.source_priority,
                       stop.name, stop.platform_code
                FROM stops AS stop
                WHERE stop.id = $2
                  AND stop.is_active = true
                  AND stop.name <> ''
                  AND stop.normalized_name <> ''
                  AND stop.location_type IN ('stop', 'station')
                LIMIT 1
            )
            UNION ALL
            (
                SELECT stop.id, stop.normalized_name, stop.source_priority,
                       stop.name, stop.platform_code
                FROM stops AS stop
                WHERE stop.is_active = true
                  AND stop.name <> ''
                  AND stop.normalized_name = $1
                  AND stop.location_type IN ('stop', 'station')
                LIMIT $5
            )
            UNION ALL
            (
                SELECT stop.id, stop.normalized_name, stop.source_priority,
                       stop.name, stop.platform_code
                FROM stops AS stop
                WHERE stop.is_active = true
                  AND stop.name <> ''
                  AND stop.normalized_name <> ''
                  AND stop.location_type IN ('stop', 'station')
                  AND stop.normalized_name LIKE $3
                LIMIT $4
            )
        )
        SELECT stop.id,
               COALESCE(preferred.source_feed_id, stop.source_feed_id) AS source_feed_id,
               stop.name, stop.normalized_name,
               stop.municipality, stop.district, stop.region, stop.lat, stop.lon,
               stop.coordinate_confidence, stop.coordinate_source, stop.stop_area_id,
               stop.platform_code, stop.location_type, stop.parent_station_id,
               stop.station_id, stop.complex_id, stop.has_station_layout,
               stop.station_layout_version, stop.wheelchair_boarding, stop.modes,
               COALESCE(preferred.priority, stop.source_priority) AS source_priority,
               stop.is_active
        FROM candidates AS candidate
        JOIN stops AS stop ON stop.id = candidate.id
        LEFT JOIN source_feeds AS direct_feed
          ON direct_feed.id = stop.source_feed_id
         AND direct_feed.enabled = true
        LEFT JOIN LATERAL (
            SELECT source_id.source_feed_id, source_id.priority
            FROM stop_source_ids AS source_id
            JOIN source_feeds AS source_feed
              ON source_feed.id = source_id.source_feed_id
             AND source_feed.enabled = true
            WHERE source_id.stop_id = stop.id
              AND direct_feed.id IS NULL
              AND stop.source_feed_id IS NOT NULL
            ORDER BY source_id.priority ASC, source_id.source_feed_id ASC
            LIMIT 1
        ) AS preferred ON true
        WHERE preferred.source_feed_id IS NOT NULL
           OR stop.source_feed_id IS NULL
           OR direct_feed.id IS NOT NULL
        ORDER BY (candidate.id = $2) DESC, (candidate.normalized_name = $1) DESC,
                 candidate.normalized_name ASC,
                 candidate.source_priority ASC, candidate.name ASC,
                 candidate.platform_code ASC NULLS FIRST, candidate.id ASC
        LIMIT $5
        "#,
    )
    .bind(&normalized)
    .bind(raw_query.trim())
    .bind(&prefix)
    .bind(candidate_limit)
    .bind(candidate_limit)
    .fetch_all(pool)
    .await?;
    if !prefix_rows.is_empty() {
        let stops = prefix_rows
            .into_iter()
            .map(stop_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        return ranked_stop_suggestions_db(pool, stops, normalized_query, limit).await;
    }

    let like = format!("%{normalized}%");
    let raw_like = format!("%{}%", raw_query.trim());
    let first_token = normalized_query
        .split_whitespace()
        .next()
        .unwrap_or_default();
    let first_token_like = format!("%{first_token}%");
    let rows = sqlx::query(
        r#"
        WITH candidates AS MATERIALIZED (
            SELECT stop.id,
                   CASE WHEN stop.id = $4 THEN 0
                        WHEN stop.normalized_name = $1 THEN 1
                        WHEN stop.location_type = 'station'
                         AND stop.normalized_name LIKE '%' || $1 THEN 2
                        ELSE 3 END AS match_rank,
                   similarity(stop.normalized_name, $1) AS normalized_similarity,
                   similarity(stop.name, $6) AS name_similarity,
                   stop.platform_code, stop.source_priority, stop.name
            FROM stops AS stop
            WHERE stop.is_active = true
              AND btrim(stop.name) <> ''
              AND btrim(stop.normalized_name) <> ''
              AND stop.location_type IN ('stop', 'station')
              AND (
                stop.id = $4
                OR stop.normalized_name LIKE $2
                OR stop.name ILIKE $3
                OR stop.normalized_name LIKE $5
                OR stop.name ILIKE $5
                OR stop.normalized_name % $1
                OR stop.name % $6
              )
            ORDER BY match_rank, normalized_similarity DESC, name_similarity DESC,
                     stop.platform_code IS NULL DESC, stop.source_priority ASC, stop.name ASC
            LIMIT $7
        )
        SELECT stop.id,
               COALESCE(preferred.source_feed_id, stop.source_feed_id) AS source_feed_id,
               stop.name, stop.normalized_name,
               stop.municipality, stop.district, stop.region, stop.lat, stop.lon,
               stop.coordinate_confidence, stop.coordinate_source, stop.stop_area_id,
               stop.platform_code, stop.location_type, stop.parent_station_id,
               stop.station_id, stop.complex_id, stop.has_station_layout,
               stop.station_layout_version, stop.wheelchair_boarding, stop.modes,
               COALESCE(preferred.priority, stop.source_priority) AS source_priority,
               stop.is_active
        FROM candidates AS candidate
        JOIN stops AS stop ON stop.id = candidate.id
        LEFT JOIN source_feeds AS direct_feed
          ON direct_feed.id = stop.source_feed_id
         AND direct_feed.enabled = true
        LEFT JOIN LATERAL (
            SELECT source_id.source_feed_id, source_id.priority
            FROM stop_source_ids AS source_id
            JOIN source_feeds AS source_feed
              ON source_feed.id = source_id.source_feed_id
             AND source_feed.enabled = true
            WHERE source_id.stop_id = stop.id
              AND direct_feed.id IS NULL
              AND stop.source_feed_id IS NOT NULL
            ORDER BY source_id.priority ASC, source_id.source_feed_id ASC
            LIMIT 1
        ) AS preferred ON true
        WHERE preferred.source_feed_id IS NOT NULL
           OR stop.source_feed_id IS NULL
           OR direct_feed.id IS NOT NULL
        ORDER BY candidate.match_rank, candidate.normalized_similarity DESC,
                 candidate.name_similarity DESC, candidate.platform_code IS NULL DESC,
                 candidate.source_priority ASC, candidate.name ASC
        LIMIT $8
        "#,
    )
    .bind(&normalized)
    .bind(&like)
    .bind(&raw_like)
    .bind(raw_query.trim())
    .bind(&first_token_like)
    .bind(raw_query.trim())
    .bind(raw_candidate_limit)
    .bind(candidate_limit)
    .fetch_all(pool)
    .await?;

    let stops = rows
        .into_iter()
        .map(stop_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    ranked_stop_suggestions_db(pool, stops, normalized_query, limit).await
}

async fn stop_catalog_db(pool: &PgPool) -> Result<Vec<Stop>, sqlx::Error> {
    sqlx::query(
        r#"
        SELECT stop.id,
               COALESCE(preferred.source_feed_id, stop.source_feed_id) AS source_feed_id,
               stop.name, stop.normalized_name, stop.municipality, stop.district,
               stop.region, stop.lat, stop.lon, stop.coordinate_confidence,
               stop.coordinate_source, stop.stop_area_id, stop.platform_code,
               stop.location_type, stop.parent_station_id, stop.station_id, stop.complex_id,
               stop.has_station_layout, stop.station_layout_version,
               stop.wheelchair_boarding, stop.modes,
               COALESCE(preferred.priority, stop.source_priority) AS source_priority,
               stop.is_active
        FROM stops AS stop
        LEFT JOIN source_feeds AS direct_feed
          ON direct_feed.id = stop.source_feed_id AND direct_feed.enabled = true
        LEFT JOIN LATERAL (
            SELECT source_id.source_feed_id, source_id.priority
            FROM stop_source_ids AS source_id
            JOIN source_feeds AS feed
              ON feed.id = source_id.source_feed_id AND feed.enabled = true
            WHERE source_id.stop_id = stop.id
              AND direct_feed.id IS NULL
              AND stop.source_feed_id IS NOT NULL
            ORDER BY source_id.priority ASC, source_id.source_feed_id ASC
            LIMIT 1
        ) AS preferred ON true
        WHERE stop.is_active = true
          AND stop.name <> ''
          AND stop.normalized_name <> ''
          AND stop.location_type IN ('stop', 'station')
          AND (stop.source_feed_id IS NULL OR direct_feed.id IS NOT NULL
               OR preferred.source_feed_id IS NOT NULL)
        ORDER BY stop.id ASC
        "#,
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(stop_from_row)
    .collect()
}

fn stop_search_candidate_limit(limit: usize) -> i64 {
    (limit.max(1) * 6).clamp(20, 100) as i64
}

async fn ranked_stop_suggestions_db(
    pool: &PgPool,
    mut stops: Vec<Stop>,
    normalized_query: &str,
    limit: usize,
) -> Result<Vec<Stop>, sqlx::Error> {
    let mut complex_ids = stops
        .iter()
        .filter_map(|stop| pid_stop_complex_id(&stop.id))
        .collect::<Vec<_>>();
    complex_ids.sort();
    complex_ids.dedup();
    let complex_members = pid_stop_complex_members_db(pool, &complex_ids).await?;
    let mut members_by_complex = HashMap::<String, Vec<Stop>>::new();
    for member in complex_members {
        if let Some(complex_id) = pid_stop_complex_id(&member.id) {
            members_by_complex
                .entry(complex_id)
                .or_default()
                .push(member);
        }
    }
    for stop in &mut stops {
        if let Some(members) =
            pid_stop_complex_id(&stop.id).and_then(|complex_id| members_by_complex.get(&complex_id))
        {
            merge_interchange_modes(stop, members);
        }
    }
    Ok(ranked_stop_suggestions(
        stops.iter(),
        normalized_query,
        limit,
    ))
}

async fn pid_stop_complex_members_db(
    pool: &PgPool,
    complex_ids: &[String],
) -> Result<Vec<Stop>, sqlx::Error> {
    if complex_ids.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query(
        r#"
        SELECT stop.id, stop.source_feed_id, stop.name, stop.normalized_name,
               stop.municipality, stop.district, stop.region, stop.lat, stop.lon,
               stop.coordinate_confidence, stop.coordinate_source, stop.stop_area_id,
               stop.platform_code, stop.location_type, stop.parent_station_id,
               stop.station_id, stop.complex_id, stop.has_station_layout,
               stop.station_layout_version, stop.wheelchair_boarding, stop.modes,
               stop.source_priority, stop.is_active
        FROM unnest($1::text[]) AS complex(id)
        CROSS JOIN LATERAL (
            SELECT stop.id, stop.source_feed_id, stop.name, stop.normalized_name,
                   stop.municipality, stop.district, stop.region, stop.lat, stop.lon,
                   stop.coordinate_confidence, stop.coordinate_source, stop.stop_area_id,
                   stop.platform_code, stop.location_type, stop.parent_station_id,
                   stop.station_id, stop.complex_id, stop.has_station_layout,
                   stop.station_layout_version, stop.wheelchair_boarding, stop.modes,
                   stop.source_priority, stop.is_active
            FROM stops AS stop
            WHERE stop.id >= complex.id
              AND stop.id < complex.id || 'Z~'
              AND (stop.id LIKE complex.id || 'S%' OR stop.id LIKE complex.id || 'Z%')
              AND stop.is_active = true
              AND stop.location_type IN ('stop', 'station')
            ORDER BY stop.id
            LIMIT 1000
        ) AS stop
        JOIN source_feeds AS source_feed
          ON source_feed.id = stop.source_feed_id
         AND source_feed.enabled = true
        ORDER BY stop.source_priority ASC, stop.platform_code ASC NULLS FIRST, stop.id ASC
        LIMIT 1000
        "#,
    )
    .bind(complex_ids)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(stop_from_row)
    .collect()
}

async fn search_cities_db(
    pool: &PgPool,
    raw_query: &str,
    normalized_query: &str,
    limit: usize,
) -> Result<Vec<City>, sqlx::Error> {
    let normalized = normalized_query.to_string();
    let prefix = format!("{normalized}%");
    let contains = format!("%{normalized}%");
    let rows = sqlx::query(
        r#"
        SELECT id, name, normalized_name, region, country_code, lat, lon, importance
        FROM cities
        WHERE $1 = ''
           OR id = $2
           OR normalized_name = $1
           OR normalized_name LIKE $3
           OR normalized_name LIKE $4
           OR normalized_name % $1
        ORDER BY
          CASE
            WHEN id = $2 THEN 0
            WHEN normalized_name = $1 THEN 1
            WHEN normalized_name LIKE $3 THEN 2
            ELSE 3
          END,
          similarity(normalized_name, $1) DESC,
          importance DESC,
          name ASC
        LIMIT $5
        "#,
    )
    .bind(&normalized)
    .bind(raw_query.trim())
    .bind(&prefix)
    .bind(&contains)
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| City {
            id: row.get("id"),
            name: row.get("name"),
            normalized_name: row.get("normalized_name"),
            region: row.get("region"),
            country_code: row.get("country_code"),
            lat: row.get("lat"),
            lon: row.get("lon"),
            importance: row.get("importance"),
        })
        .collect())
}

async fn nearby_stops_db(
    pool: &PgPool,
    lat: f64,
    lon: f64,
    radius: f64,
) -> Result<Vec<Stop>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
               lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
               platform_code, location_type, parent_station_id, station_id, complex_id,
               has_station_layout, station_layout_version, wheelchair_boarding,
               modes, source_priority, is_active
        FROM enabled_source_stops
        WHERE is_active = true
          AND btrim(name) <> ''
          AND btrim(normalized_name) <> ''
          AND location_type IN ('stop', 'station')
          AND geom IS NOT NULL
          AND ST_DWithin(geom, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, $3)
        ORDER BY geom <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
        LIMIT 50
        "#,
    )
    .bind(lon)
    .bind(lat)
    .bind(radius)
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(stop_from_row).collect()
}

async fn stops_in_bounds_db(
    pool: &PgPool,
    bounds: &StopsInBoundsQuery,
    limit: usize,
) -> Result<Vec<Stop>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
               lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
               platform_code, location_type, parent_station_id, station_id, complex_id,
               has_station_layout, station_layout_version, wheelchair_boarding,
               modes, source_priority, is_active
        FROM enabled_source_stops
        WHERE is_active = true
          AND btrim(name) <> ''
          AND btrim(normalized_name) <> ''
          AND location_type IN ('stop', 'station')
          AND geom IS NOT NULL
          AND geom && ST_MakeEnvelope($1, $2, $3, $4, 4326)::geography
          AND ST_Covers(ST_MakeEnvelope($1, $2, $3, $4, 4326), geom::geometry)
          AND ($5::text IS NULL OR id > $5)
        ORDER BY id ASC
        LIMIT $6
        "#,
    )
    .bind(bounds.west)
    .bind(bounds.south)
    .bind(bounds.east)
    .bind(bounds.north)
    .bind(bounds.cursor.as_deref())
    .bind((limit + 1) as i64)
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(stop_from_row).collect()
}

async fn get_stop_db(pool: &PgPool, id: &str) -> Result<Option<Stop>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
               lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
               platform_code, location_type, parent_station_id, station_id, complex_id,
               has_station_layout, station_layout_version, wheelchair_boarding,
               modes, source_priority, is_active
        FROM enabled_source_stops
        WHERE id = $1 AND is_active = true
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    row.map(stop_from_row).transpose()
}

async fn get_routing_stop_db(pool: &PgPool, id: &str) -> Result<Option<Stop>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
               lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
               platform_code, location_type, parent_station_id, station_id, complex_id,
               has_station_layout, station_layout_version, wheelchair_boarding,
               modes, source_priority, is_active
        FROM stops
        WHERE id = $1
          AND is_active = true
          AND (
            source_feed_id IS NULL
            OR EXISTS (
              SELECT 1 FROM source_feeds direct_feed
              WHERE direct_feed.id = stops.source_feed_id AND direct_feed.enabled = true
            )
            OR EXISTS (
              SELECT 1
              FROM stop_source_ids source_id
              JOIN source_feeds source_feed
                ON source_feed.id = source_id.source_feed_id AND source_feed.enabled = true
              WHERE source_id.stop_id = stops.id
            )
          )
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    row.map(stop_from_row).transpose()
}

async fn departures_db(
    pool: &PgPool,
    stop_id: &str,
    earliest_seconds: u32,
    limit: usize,
    service_date: chrono::NaiveDate,
) -> Result<Vec<Value>, sqlx::Error> {
    let suggestion_stop_ids = match get_routing_stop_db(pool, stop_id).await? {
        Some(stop) => equivalent_stop_ids_db(pool, &stop).await?,
        None => vec![stop_id.to_string()],
    };
    let rows = sqlx::query(
        r#"
        WITH selected_stop AS (
          SELECT id, parent_station_id, stop_area_id
          FROM stops
          WHERE id = $1
          LIMIT 1
        ),
        matching_stops AS (
          SELECT candidate.id
          FROM stops candidate
          CROSS JOIN selected_stop selected
          WHERE candidate.is_active = true
            AND (
              candidate.id = ANY($2)
              OR candidate.id = selected.id
              OR candidate.parent_station_id = COALESCE(selected.parent_station_id, selected.id)
              OR (
                selected.parent_station_id IS NOT NULL
                AND candidate.id = selected.parent_station_id
              )
              OR (
                selected.stop_area_id IS NOT NULL
                AND candidate.stop_area_id = selected.stop_area_id
              )
            )
        )
        SELECT
          st.trip_id,
          st.stop_id,
          st.stop_sequence,
          st.departure_time,
          st.arrival_time,
          t.headsign,
          r.id AS route_id,
          r.short_name,
          r.long_name,
          r.mode,
          realtime.delay_seconds,
          realtime.estimated_arrival,
          realtime.estimated_departure,
          realtime.cancellation_status,
          realtime.platform_change,
          realtime.source AS realtime_source,
          realtime.fetched_at AS realtime_fetched_at,
          realtime.valid_until AS realtime_valid_until
          , boarding_stop.station_id
          , boarding_stop.complex_id
          , boarding_stop.has_station_layout
          , boarding_stop.station_layout_version
        FROM stop_times st
        JOIN stops boarding_stop ON boarding_stop.id = st.stop_id
        JOIN trips t ON t.id = st.trip_id
        JOIN routes r ON r.id = t.route_id
        JOIN source_feeds feed ON feed.id = t.source_feed_id AND feed.enabled = true
        LEFT JOIN LATERAL (
          SELECT delay_seconds, estimated_arrival, estimated_departure,
                 cancellation_status, platform_change, source, fetched_at, valid_until
          FROM realtime_updates realtime
          WHERE realtime.trip_id = st.trip_id
            AND (realtime.stop_id = st.stop_id OR realtime.stop_id IS NULL)
            AND (realtime.valid_until IS NULL OR realtime.valid_until >= now())
            AND (realtime.service_date IS NULL OR realtime.service_date = CURRENT_DATE)
          ORDER BY (realtime.stop_id = st.stop_id) DESC, realtime.fetched_at DESC
          LIMIT 1
        ) realtime ON true
        WHERE st.stop_id IN (SELECT id FROM matching_stops)
          AND st.departure_time >= $3
          AND COALESCE(st.pickup_type, 0) IN (0, 2, 3)
        ORDER BY st.departure_time ASC
        LIMIT $4
        "#,
    )
    .bind(stop_id)
    .bind(&suggestion_stop_ids)
    .bind(earliest_seconds as i32)
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let trip_id = row.get::<String, _>("trip_id");
            let stop_id = row.get::<String, _>("stop_id");
            let stop_sequence = row.get::<i32, _>("stop_sequence") as i64;
            let run_id = operational_run_id(&trip_id, service_date);
            let call_id = operational_call_id(&run_id, &stop_id, stop_sequence);
            let departure_time = row.get::<i32, _>("departure_time") as u32;
            let delay_seconds = row.get::<Option<i32>, _>("delay_seconds");
            let cancellation_status = row.get::<Option<String>, _>("cancellation_status");
            json!({
                "trip_id": trip_id,
                "run_id": run_id,
                "call_id": call_id,
                "stop_id": stop_id,
                "station_id": row.get::<Option<String>, _>("station_id"),
                "complex_id": row.get::<Option<String>, _>("complex_id"),
                "has_station_layout": row.get::<bool, _>("has_station_layout"),
                "station_layout_version": row.get::<Option<String>, _>("station_layout_version"),
                "route_id": row.get::<String, _>("route_id"),
                "line": row.get::<Option<String>, _>("short_name")
                    .or_else(|| row.get::<Option<String>, _>("long_name"))
                    .unwrap_or_else(|| row.get::<String, _>("route_id")),
                "destination": row.get::<Option<String>, _>("headsign"),
                "mode": row.get::<String, _>("mode"),
                "scheduled_departure": transit_model::seconds_to_time(departure_time),
                "scheduled_arrival": transit_model::seconds_to_time(row.get::<i32, _>("arrival_time") as u32),
                "realtime_departure": row.get::<Option<DateTime<Utc>>, _>("estimated_departure"),
                "realtime_arrival": row.get::<Option<DateTime<Utc>>, _>("estimated_arrival"),
                "delay_seconds": delay_seconds,
                "status": if cancellation_status.is_some() {
                    "cancelled"
                } else if delay_seconds.is_some() {
                    "realtime"
                } else {
                    "scheduled"
                },
                "cancellation_status": cancellation_status,
                "platform_change": row.get::<Option<String>, _>("platform_change"),
                "realtime_source": row.get::<Option<String>, _>("realtime_source"),
                "realtime_fetched_at": row.get::<Option<DateTime<Utc>>, _>("realtime_fetched_at"),
                "realtime_valid_until": row.get::<Option<DateTime<Utc>>, _>("realtime_valid_until")
            })
        })
        .collect())
}

fn stop_from_row(row: sqlx::postgres::PgRow) -> Result<Stop, sqlx::Error> {
    let lat = row.get::<Option<f64>, _>("lat");
    let lon = row.get::<Option<f64>, _>("lon");
    let source_feed_id = row
        .get::<Option<String>, _>("source_feed_id")
        .unwrap_or_else(|| "database".to_string());
    let source_priority = row.get::<i32, _>("source_priority");
    let id = row.get::<String, _>("id");
    let source_name = row.get::<String, _>("name");
    let platform_code = row.get::<Option<String>, _>("platform_code");
    let name = pid_public_stop_name(&id, &source_name, platform_code.as_deref());
    Ok(Stop {
        id: id.clone(),
        source_ids: vec![transit_model::SourceRef {
            feed_id: source_feed_id.clone(),
            original_id: id,
            import_run_id: None,
            priority: source_priority,
            confidence: None,
            suppressed_as_duplicate: false,
        }],
        normalized_name: normalize_czech_name(&name),
        name,
        municipality: row.get("municipality"),
        district: row.get("district"),
        region: row.get("region"),
        lat,
        lon,
        geom: lat
            .zip(lon)
            .map(|(lat, lon)| geo_types::Point::new(lon, lat)),
        coordinate_confidence: db_confidence_to_model(
            &row.get::<String, _>("coordinate_confidence"),
        ),
        coordinate_source: row.get("coordinate_source"),
        stop_area_id: row.get("stop_area_id"),
        platform_code,
        location_type: row
            .try_get::<String, _>("location_type")
            .map(|value| db_stop_location_type_to_model(&value))
            .unwrap_or(StopLocationType::Stop),
        parent_station_id: row
            .try_get::<Option<String>, _>("parent_station_id")
            .unwrap_or(None),
        station_id: row
            .try_get::<Option<String>, _>("station_id")
            .unwrap_or(None),
        complex_id: row
            .try_get::<Option<String>, _>("complex_id")
            .unwrap_or(None),
        has_station_layout: row
            .try_get::<bool, _>("has_station_layout")
            .unwrap_or(false),
        station_layout_version: row
            .try_get::<Option<String>, _>("station_layout_version")
            .unwrap_or(None),
        wheelchair_boarding: row
            .try_get::<String, _>("wheelchair_boarding")
            .map(|value| db_accessibility_to_model(&value))
            .unwrap_or(AccessibilityStatus::Unknown),
        modes: row
            .get::<Vec<String>, _>("modes")
            .into_iter()
            .map(|mode| db_mode_to_model(&mode))
            .collect(),
        is_active: row.get("is_active"),
    })
}

fn db_stop_location_type_to_model(value: &str) -> StopLocationType {
    match value {
        "station" => StopLocationType::Station,
        "entrance_exit" => StopLocationType::EntranceExit,
        "generic_node" => StopLocationType::GenericNode,
        "boarding_area" => StopLocationType::BoardingArea,
        _ => StopLocationType::Stop,
    }
}

fn db_accessibility_to_model(value: &str) -> AccessibilityStatus {
    match value {
        "accessible" => AccessibilityStatus::Accessible,
        "inaccessible" => AccessibilityStatus::Inaccessible,
        _ => AccessibilityStatus::Unknown,
    }
}

fn route_row_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": row.get::<String, _>("id"),
        "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
        "source_id": row.get::<String, _>("source_id"),
        "agency_id": row.get::<Option<String>, _>("agency_id"),
        "operator_id": row.get::<Option<String>, _>("operator_id"),
        "short_name": row.get::<Option<String>, _>("short_name"),
        "long_name": row.get::<Option<String>, _>("long_name"),
        "mode": row.get::<String, _>("mode"),
        "gtfs_route_type": row.get::<Option<i32>, _>("gtfs_route_type"),
        "color": row.get::<Option<String>, _>("color"),
        "text_color": row.get::<Option<String>, _>("text_color"),
        "source_priority": row.get::<i32, _>("source_priority"),
        "is_active": row.get::<bool, _>("is_active")
    })
}

fn trip_row_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": row.get::<String, _>("id"),
        "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
        "source_id": row.get::<String, _>("source_id"),
        "route_id": row.get::<String, _>("route_id"),
        "service_id": row.get::<String, _>("service_id"),
        "headsign": row.get::<Option<String>, _>("headsign"),
        "direction_id": row.get::<Option<i16>, _>("direction_id"),
        "shape_id": row.get::<Option<String>, _>("shape_id"),
        "restrictions": row.get::<Value, _>("restrictions"),
        "raw_source_metadata": row.get::<Value, _>("raw_source_metadata"),
        "source_priority": row.get::<i32, _>("source_priority")
    })
}

fn stop_time_row_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "trip_id": row.get::<String, _>("trip_id"),
        "stop_id": row.get::<String, _>("stop_id"),
        "stop_sequence": row.get::<i32, _>("stop_sequence"),
        "arrival_time": row.get::<i32, _>("arrival_time"),
        "departure_time": row.get::<i32, _>("departure_time"),
        "pickup_type": row.get::<Option<i16>, _>("pickup_type"),
        "drop_off_type": row.get::<Option<i16>, _>("drop_off_type"),
        "timepoint": row.get::<Option<bool>, _>("timepoint"),
        "stop_headsign": row.get::<Option<String>, _>("stop_headsign"),
        "platform": row.get::<Option<String>, _>("platform"),
        "raw_notes": row.get::<Option<String>, _>("raw_notes"),
        "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
        "source_priority": row.get::<i32, _>("source_priority")
    })
}

async fn fetch_agencies_json(
    pool: &PgPool,
    agency_ids: Vec<String>,
) -> Result<Vec<Value>, sqlx::Error> {
    if agency_ids.is_empty() {
        return Ok(Vec::new());
    }

    Ok(sqlx::query(
        r#"
        SELECT id, source_feed_id, source_id, name, url, timezone
        FROM agencies
        WHERE id = ANY($1)
        ORDER BY name ASC
        "#,
    )
    .bind(&agency_ids)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "id": row.get::<String, _>("id"),
            "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
            "source_id": row.get::<String, _>("source_id"),
            "name": row.get::<String, _>("name"),
            "url": row.get::<Option<String>, _>("url"),
            "timezone": row.get::<Option<String>, _>("timezone")
        })
    })
    .collect())
}

async fn fetch_source_feeds_json(
    pool: &PgPool,
    source_feed_ids: Vec<String>,
) -> Result<Vec<Value>, sqlx::Error> {
    if source_feed_ids.is_empty() {
        return Ok(Vec::new());
    }

    Ok(sqlx::query(
        r#"
        SELECT id, name, url, type, mode_scope, priority, enabled
        FROM source_feeds
        WHERE id = ANY($1) AND enabled = true
        ORDER BY priority ASC, id ASC
        "#,
    )
    .bind(&source_feed_ids)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "id": row.get::<String, _>("id"),
            "name": row.get::<String, _>("name"),
            "url": row.get::<String, _>("url"),
            "type": row.get::<String, _>("type"),
            "mode_scope": row.get::<Option<String>, _>("mode_scope"),
            "priority": row.get::<i32, _>("priority"),
            "enabled": row.get::<bool, _>("enabled")
        })
    })
    .collect())
}

fn database_data_status() -> Value {
    json!({
        "source": "database",
        "schedule": "current",
        "realtime": "source_dependent",
        "warnings": Vec::<String>::new()
    })
}

fn database_data_status_with_realtime(realtime: &str) -> Value {
    json!({
        "source": "database",
        "schedule": "current",
        "realtime": realtime,
        "warnings": Vec::<String>::new()
    })
}

fn resolve_journey_point_fixture(
    stops: &[Stop],
    cities: &[City],
    point: &JourneyPoint,
) -> Option<String> {
    if point.point_type == "coordinate" {
        let (lat, lon) = point.lat.zip(point.lon)?;
        return stops
            .iter()
            .filter_map(|stop| {
                stop.lat.zip(stop.lon).map(|(stop_lat, stop_lon)| {
                    (haversine_m(lat, lon, stop_lat, stop_lon), stop.id.clone())
                })
            })
            .min_by(|left, right| left.0.total_cmp(&right.0))
            .map(|(_, id)| id);
    }
    let candidate = point
        .id
        .as_deref()
        .filter(|value| !value.trim().is_empty())?;

    if point.point_type == "city" {
        return fixture_city_stop_id(cities, candidate).map(str::to_string);
    }

    if stops.iter().any(|stop| stop.id == candidate) {
        return Some(canonical_stop_id(stops, candidate));
    }

    ranked_stop_suggestions(stops.iter(), &normalize_search_text(candidate), 1)
        .into_iter()
        .next()
        .map(|stop| stop.id)
}

fn validate_journey_point_fixture(cities: &[City], point: &JourneyPoint) -> Result<(), ApiError> {
    match point.point_type.as_str() {
        "stop" => validate_required_journey_point_id(point),
        "city" => {
            let city_id = point
                .id
                .as_deref()
                .filter(|id| id.starts_with("city:") && !id.trim().is_empty())
                .ok_or_else(|| invalid_city_id(point.id.as_deref()))?;
            if cities.iter().any(|city| city.id == city_id) {
                Ok(())
            } else {
                Err(invalid_city_id(Some(city_id)))
            }
        }
        "coordinate" => validate_coordinate_journey_point(point),
        other => Err(ApiError {
            code: "invalid_journey_point_type".to_string(),
            message: format!(
                "journey point type '{other}' is not supported; use 'stop', 'city' or 'coordinate'"
            ),
        }),
    }
}

fn fixture_city_stop_id<'a>(cities: &'a [City], city_id: &str) -> Option<&'a str> {
    cities.iter().find(|city| city.id == city_id)?;
    match city_id {
        "city:CZ:554782" => Some("stop-praha-hl-n"),
        "city:CZ:582786" => Some("stop-brno-hl-n"),
        "city:CZ:586846" => Some("stop-jihlava"),
        _ => None,
    }
}

fn canonical_stop_id(stops: &[Stop], stop_id: &str) -> String {
    let Some(stop) = stops.iter().find(|stop| stop.id == stop_id) else {
        return stop_id.to_string();
    };

    if stop.platform_code.is_none() {
        return stop.id.clone();
    }

    stops
        .iter()
        .find(|candidate| {
            candidate.platform_code.is_none() && stops_are_same_suggestion(candidate, stop)
        })
        .map(|candidate| candidate.id.clone())
        .unwrap_or_else(|| stop.id.clone())
}

fn transport_mode_to_db(mode: &TransportMode) -> Option<String> {
    let value = match mode {
        TransportMode::Train => "train",
        TransportMode::Tram => "tram",
        TransportMode::Bus => "bus",
        TransportMode::Metro => "metro",
        TransportMode::Trolleybus => "trolleybus",
        TransportMode::Ferry => "ferry",
        TransportMode::CableCar => "cable_car",
        TransportMode::Unknown => return None,
    };
    Some(value.to_string())
}

fn db_mode_to_model(mode: &str) -> TransportMode {
    match mode {
        "train" => TransportMode::Train,
        "tram" => TransportMode::Tram,
        "bus" => TransportMode::Bus,
        "metro" => TransportMode::Metro,
        "trolleybus" => TransportMode::Trolleybus,
        "ferry" => TransportMode::Ferry,
        "cable_car" => TransportMode::CableCar,
        _ => TransportMode::Unknown,
    }
}

fn db_route_mode_to_model(mode: &str, gtfs_route_type: Option<i32>) -> TransportMode {
    match gtfs_route_type {
        Some(0 | 900..=999) => TransportMode::Tram,
        Some(1) => TransportMode::Metro,
        Some(2 | 100..=199 | 400..=499) => TransportMode::Train,
        Some(3 | 200..=299 | 700..=799) => TransportMode::Bus,
        Some(4 | 1000..=1099) => TransportMode::Ferry,
        Some(5 | 1300..=1399) => TransportMode::CableCar,
        Some(11 | 800..=899) => TransportMode::Trolleybus,
        _ => db_mode_to_model(mode),
    }
}

fn db_confidence_to_model(confidence: &str) -> CoordinateConfidence {
    match confidence {
        "exact" => CoordinateConfidence::Exact,
        "high" => CoordinateConfidence::High,
        "medium" => CoordinateConfidence::Medium,
        "low" => CoordinateConfidence::Low,
        _ => CoordinateConfidence::Unresolved,
    }
}

fn parse_query_time_seconds(value: &str) -> Option<u32> {
    if let Some(time) = value.rsplit('T').next().and_then(|part| part.get(..8)) {
        return transit_model::parse_gtfs_time(time);
    }
    transit_model::parse_gtfs_time(value)
}

fn current_prague_time_seconds() -> u32 {
    prague_time_seconds_at(Utc::now())
}

fn prague_time_seconds_at(now: DateTime<Utc>) -> u32 {
    now.with_timezone(&chrono_tz::Europe::Prague)
        .time()
        .num_seconds_from_midnight()
}

fn mock_status(use_mock_data: bool) -> Value {
    json!({
        "source": if use_mock_data { "mock" } else { "database" },
        "schedule": if use_mock_data { "mock" } else { "current" },
        "realtime": "unavailable",
        "warnings": if use_mock_data { vec!["development fixture data is in use"] } else { Vec::<&str>::new() }
    })
}

fn stop_search_score(stop: &Stop, query: &str) -> Option<i32> {
    let score = stop_search_score_for_query(stop, query);
    let railway_alias_score = railway_station_query_base(stop, query)
        .and_then(|query| stop_search_score_for_query(stop, query));
    score
        .max(railway_alias_score)
        .max(railway_station_locality_omission_score(stop, query))
}

fn railway_station_locality_omission_score(stop: &Stop, query: &str) -> Option<i32> {
    if !stop.modes.contains(&TransportMode::Train)
        || query.split_whitespace().count() < 2
        || !query.contains("nadrazi")
    {
        return None;
    }
    let name = normalize_search_text(&stop.name);
    (name != query && name.ends_with(query))
        .then_some(9_500 - (name.len() as i32 - query.len() as i32).max(0))
}

fn railway_station_query_base<'a>(stop: &Stop, query: &'a str) -> Option<&'a str> {
    let station_like =
        railway_station_stop_base(&stop.id).is_some() || stop.modes.contains(&TransportMode::Train);
    if !station_like {
        return None;
    }

    [
        " hlavni nadrazi",
        " hlavni stanice",
        " zeleznicni stanice",
        " railway station",
        " train station",
        " hlavni n",
        " hl n",
        " zel st",
        " nadrazi",
    ]
    .into_iter()
    .find_map(|suffix| {
        query
            .strip_suffix(suffix)
            .map(str::trim_end)
            .filter(|base| !base.is_empty())
    })
}

fn stop_search_score_for_query(stop: &Stop, query: &str) -> Option<i32> {
    if query.is_empty() {
        return Some(if stop.is_active { 10 } else { 0 });
    }

    let name = normalize_search_text(&stop.name);
    let normalized_name = normalize_search_text(&stop.normalized_name);
    let searchable_text = searchable_stop_text(stop);
    let name_tokens = name.split_whitespace().collect::<Vec<_>>();
    let query_tokens = query.split_whitespace().collect::<Vec<_>>();
    let mut score = None;

    if name == query || normalized_name == query {
        score = Some(10_000);
    } else if name.starts_with(query) || normalized_name.starts_with(query) {
        score = Some(9_000 - (name.len() as i32 - query.len() as i32).abs());
    } else if let Some(position) = searchable_text.find(query) {
        score = Some(8_000 - position as i32);
    }

    if tokens_match_in_order_by_prefix(&query_tokens, &name_tokens) {
        score = score.max(Some(
            7_500
                + query_tokens
                    .iter()
                    .map(|token| token.len() as i32)
                    .sum::<i32>(),
        ));
    } else if tokens_match_unordered_by_prefix(&query_tokens, &name_tokens) {
        score = score.max(Some(
            7_000
                + query_tokens
                    .iter()
                    .map(|token| token.len() as i32)
                    .sum::<i32>(),
        ));
    }

    if let Some(fuzzy_score) = fuzzy_token_score(&query_tokens, &name_tokens) {
        score = score.max(Some(fuzzy_score));
    }

    if query.chars().count() >= 3 {
        let initials = stop_name_initials(&name_tokens);
        if initials.starts_with(query) {
            score = score.max(Some(6_900 + query.len() as i32));
        }

        let distance = levenshtein(query, &name);
        let max_len = query.chars().count().max(name.chars().count());
        let ratio = 1.0 - (distance as f64 / max_len as f64);
        if ratio >= 0.62 || distance <= typo_distance_threshold(query.chars().count()) {
            score = score.max(Some(6_000 + (ratio * 500.0) as i32 - distance as i32));
        }
    }

    score.map(|value| {
        value
            + if stop.is_active { 10 } else { 0 }
            + if stop.platform_code.is_none() { 20 } else { 0 }
    })
}

fn city_search_score(city: &City, query: &str) -> Option<i32> {
    if query.is_empty() {
        return Some(city.importance);
    }

    let name = normalize_search_text(&city.name);
    let normalized_name = normalize_search_text(&city.normalized_name);
    let mut score = if name == query || normalized_name == query {
        Some(10_000)
    } else if name.starts_with(query) || normalized_name.starts_with(query) {
        Some(9_000 - (name.len() as i32 - query.len() as i32).abs())
    } else {
        normalized_name
            .find(query)
            .map(|position| 8_000 - position as i32)
    };

    if query.chars().count() >= 3 {
        let distance = levenshtein(query, &name);
        let max_len = query.chars().count().max(name.chars().count());
        let ratio = 1.0 - (distance as f64 / max_len as f64);
        if ratio >= 0.62 || distance <= typo_distance_threshold(query.chars().count()) {
            score = score.max(Some(6_000 + (ratio * 500.0) as i32 - distance as i32));
        }
    }

    score.map(|value| value + city.importance)
}

fn ranked_city_suggestions<'a>(
    cities: impl Iterator<Item = &'a City>,
    normalized_query: &str,
    limit: usize,
) -> Vec<City> {
    let mut cities = cities
        .filter_map(|city| city_search_score(city, normalized_query).map(|score| (score, city)))
        .collect::<Vec<_>>();
    cities.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .cmp(left_score)
            .then_with(|| left.name.cmp(&right.name))
    });
    cities
        .into_iter()
        .take(limit)
        .map(|(_, city)| city.clone())
        .collect()
}

fn city_search_json(city: &City) -> Value {
    json!({
        "id": city.id,
        "name": city.name,
        "normalized_name": city.normalized_name,
        "place_type": "city",
        "region": city.region,
        "country_code": city.country_code,
        "lat": city.lat,
        "lon": city.lon,
        "modes": []
    })
}

fn stop_search_json(stop: &Stop) -> Value {
    let mut value = serde_json::to_value(stop).unwrap_or_else(|_| json!({}));
    let place_type = stop_place_type(stop);
    let aliases = stop
        .name
        .split(" / ")
        .map(str::trim)
        .filter(|alias| !alias.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    value["place_type"] = json!(place_type);
    value["marker_type"] = json!(place_type);
    value["canonical_name"] = json!(aliases.first().unwrap_or(&stop.name));
    value["aliases"] = json!(aliases);
    value["parent_stop_area_id"] = json!(
        stop.stop_area_id
            .as_ref()
            .or(stop.parent_station_id.as_ref())
    );
    value["map_visible"] = json!(!matches!(
        stop.location_type,
        StopLocationType::GenericNode | StopLocationType::BoardingArea
    ));
    value
}

fn stop_catalog_json(stop: &Stop) -> Value {
    let aliases = stop
        .name
        .split(" / ")
        .map(str::trim)
        .filter(|alias| !alias.is_empty())
        .collect::<Vec<_>>();
    json!({
        "id": stop.id,
        "name": stop.name,
        "normalized_name": stop.normalized_name,
        "canonical_name": aliases.first().copied().unwrap_or(&stop.name),
        "aliases": aliases,
        "municipality": stop.municipality,
        "region": stop.region,
        "lat": stop.lat,
        "lon": stop.lon,
        "modes": stop.modes,
        "platform_code": stop.platform_code,
        "location_type": stop.location_type,
        "place_type": stop_place_type(stop),
        "parent_stop_area_id": stop.stop_area_id.as_ref().or(stop.parent_station_id.as_ref()),
        "station_id": stop.station_id,
        "complex_id": stop.complex_id,
        "has_station_layout": stop.has_station_layout,
        "station_layout_version": stop.station_layout_version,
        "source_feed_id": stop.source_ids.first().map(|source| source.feed_id.as_str())
    })
}

fn stop_place_type(stop: &Stop) -> &'static str {
    match stop.location_type {
        StopLocationType::EntranceExit => return "station_entrance",
        StopLocationType::GenericNode => return "generic_node",
        StopLocationType::BoardingArea => return "boarding_area",
        StopLocationType::Stop | StopLocationType::Station => {}
    }
    let normalized_name = normalize_search_text(&stop.name);
    if normalized_name.contains("letiste") || normalized_name.contains("airport") {
        return "airport";
    }
    if stop.modes.contains(&TransportMode::Metro) {
        return "metro_station";
    }
    if stop.modes.contains(&TransportMode::Tram) {
        return "tram_stop";
    }
    if stop.modes.contains(&TransportMode::Ferry) {
        return "ferry_terminal";
    }
    if stop.modes.contains(&TransportMode::Train) {
        return if stop.platform_code.is_none() {
            "railway_station"
        } else {
            "railway_stop"
        };
    }
    if stop.modes.contains(&TransportMode::Bus) || stop.modes.contains(&TransportMode::Trolleybus) {
        return if normalized_name.contains("autobusove nadrazi")
            || normalized_name.contains("bus station")
        {
            "bus_station"
        } else {
            "bus_stop"
        };
    }
    "stop"
}

fn canonical_stop_name(stop: &Stop) -> String {
    canonical_stop_name_parts(&stop.name, stop.municipality.as_deref())
}

fn canonical_stop_name_parts(name: &str, municipality: Option<&str>) -> String {
    let name = normalize_search_text(name);
    let municipality = municipality.map(normalize_search_text).unwrap_or_default();
    name.strip_prefix(&format!("{municipality} "))
        .filter(|_| !municipality.is_empty())
        .unwrap_or(&name)
        .to_string()
}

fn stops_are_same_suggestion(left: &Stop, right: &Stop) -> bool {
    if left.id == right.id {
        return true;
    }
    if canonical_stop_name(left) != canonical_stop_name(right) {
        return false;
    }
    if stop_modes_require_separate_suggestions(left, right) {
        return false;
    }
    // PID uses one U-number for every station/interchange complex, while the
    // following S/Z suffix identifies individual stations and directional
    // platforms. Some large interchanges span more than the generic proximity
    // threshold, but they must still be a single autocomplete suggestion.
    if let (Some(left_complex), Some(right_complex)) = (
        pid_stop_complex_id(&left.id),
        pid_stop_complex_id(&right.id),
    ) && left_complex == right_complex
    {
        return true;
    }
    if left.stop_area_id.is_some() && left.stop_area_id == right.stop_area_id {
        return true;
    }
    if left.parent_station_id.is_some() && left.parent_station_id == right.parent_station_id {
        return true;
    }
    if let (Some(left_base), Some(right_base)) = (
        railway_station_stop_base(&left.id),
        railway_station_stop_base(&right.id),
    ) && left_base == right_base
    {
        return true;
    }
    let left_municipality = left.municipality.as_deref().map(normalize_search_text);
    let right_municipality = right.municipality.as_deref().map(normalize_search_text);
    if matches!((&left_municipality, &right_municipality), (Some(left), Some(right)) if left != right)
    {
        return false;
    }

    match (left.lat.zip(left.lon), right.lat.zip(right.lon)) {
        (Some((left_lat, left_lon)), Some((right_lat, right_lon))) => {
            haversine_m(left_lat, left_lon, right_lat, right_lon) <= 300.0
        }
        _ => left_municipality.is_some() && left_municipality == right_municipality,
    }
}

fn stop_modes_require_separate_suggestions(left: &Stop, right: &Stop) -> bool {
    let modal_kind = |stop: &Stop| {
        let railway = stop.modes.contains(&TransportMode::Train);
        let urban = stop.modes.iter().any(|mode| {
            matches!(
                mode,
                TransportMode::Metro
                    | TransportMode::Tram
                    | TransportMode::Trolleybus
                    | TransportMode::Bus
            )
        });
        let ferry = stop.modes.contains(&TransportMode::Ferry);
        let non_ferry = stop
            .modes
            .iter()
            .any(|mode| !matches!(mode, TransportMode::Ferry | TransportMode::Unknown));
        (railway, urban, ferry, non_ferry)
    };
    let (left_railway, left_urban, left_ferry, left_non_ferry) = modal_kind(left);
    let (right_railway, right_urban, right_ferry, right_non_ferry) = modal_kind(right);
    (left_railway && !left_urban && right_urban && !right_railway)
        || (right_railway && !right_urban && left_urban && !left_railway)
        || (left_ferry && !left_non_ferry && right_non_ferry && !right_ferry)
        || (right_ferry && !right_non_ferry && left_non_ferry && !left_ferry)
}

fn merge_stop_suggestion(canonical: &mut Stop, sibling: &Stop) {
    for mode in &sibling.modes {
        if !canonical.modes.contains(mode) {
            canonical.modes.push(mode.clone());
        }
    }
    if canonical.modes.len() > 1 {
        canonical
            .modes
            .retain(|mode| !matches!(mode, TransportMode::Unknown));
    }
    canonical.modes.sort_by_key(stop_search_mode_rank);
}

fn merge_interchange_modes(stop: &mut Stop, members: &[Stop]) {
    for member in members {
        if canonical_stop_name(stop) == canonical_stop_name(member)
            && !stop_modes_require_separate_suggestions(stop, member)
        {
            merge_stop_suggestion(stop, member);
        }
    }
}

fn pid_stop_complex_id(stop_id: &str) -> Option<String> {
    let remainder = stop_id.strip_prefix("pid_gtfs:U")?;
    let digit_count = remainder
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digit_count == 0 || !matches!(remainder.as_bytes().get(digit_count), Some(b'S' | b'Z')) {
        return None;
    }
    Some(format!("pid_gtfs:U{}", &remainder[..digit_count]))
}

fn pid_public_stop_name(stop_id: &str, source_name: &str, platform_code: Option<&str>) -> String {
    if pid_stop_complex_id(stop_id).as_deref() == Some("pid_gtfs:U237")
        && normalize_search_text(source_name) == "palackeho namesti"
        && matches!(platform_code, Some("I" | "J"))
    {
        "Palackého náměstí (nábřeží)".to_string()
    } else {
        source_name.to_string()
    }
}

fn pid_source_stop_query(normalized_query: &str) -> String {
    if normalized_query.starts_with("palackeho namesti nabr") {
        "palackeho namesti".to_string()
    } else {
        normalized_query.to_string()
    }
}

fn stop_search_mode_rank(mode: &TransportMode) -> u8 {
    match mode {
        TransportMode::Train => 0,
        TransportMode::Metro => 1,
        TransportMode::Tram => 2,
        TransportMode::Trolleybus => 3,
        TransportMode::Bus => 4,
        TransportMode::Ferry => 5,
        TransportMode::CableCar => 6,
        TransportMode::Unknown => 7,
    }
}

fn stop_suggestion_mode_rank(stop: &Stop) -> u8 {
    u8::from(
        stop.modes.contains(&TransportMode::Ferry)
            && !stop
                .modes
                .iter()
                .any(|mode| !matches!(mode, TransportMode::Ferry | TransportMode::Unknown)),
    )
}

fn searchable_stop_text(stop: &Stop) -> String {
    [
        Some(stop.name.as_str()),
        Some(stop.normalized_name.as_str()),
        stop.municipality.as_deref(),
        stop.district.as_deref(),
        stop.region.as_deref(),
        stop.platform_code.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(normalize_search_text)
    .filter(|part| !part.is_empty())
    .collect::<Vec<_>>()
    .join(" ")
}

fn normalize_search_text(value: &str) -> String {
    let mut normalized = String::new();
    let mut previous_was_space = true;

    for character in value.trim().to_lowercase().chars() {
        if let Some(folded) = fold_czech_character(character) {
            normalized.push(folded);
            previous_was_space = false;
        } else if character.is_ascii_alphanumeric() {
            normalized.push(character);
            previous_was_space = false;
        } else if !previous_was_space {
            normalized.push(' ');
            previous_was_space = true;
        }
    }

    if normalized.ends_with(' ') {
        normalized.pop();
    }

    normalized
}

fn fold_czech_character(character: char) -> Option<char> {
    match character {
        '\u{00e1}' | '\u{00e0}' | '\u{00e2}' | '\u{00e4}' => Some('a'),
        '\u{010d}' => Some('c'),
        '\u{010f}' => Some('d'),
        '\u{00e9}' | '\u{011b}' | '\u{00e8}' | '\u{00ea}' | '\u{00eb}' => Some('e'),
        '\u{00ed}' | '\u{00ec}' | '\u{00ee}' | '\u{00ef}' => Some('i'),
        '\u{0148}' => Some('n'),
        '\u{00f3}' | '\u{00f2}' | '\u{00f4}' | '\u{00f6}' => Some('o'),
        '\u{0159}' => Some('r'),
        '\u{0161}' => Some('s'),
        '\u{0165}' => Some('t'),
        '\u{00fa}' | '\u{016f}' | '\u{00f9}' | '\u{00fb}' | '\u{00fc}' => Some('u'),
        '\u{00fd}' | '\u{00ff}' => Some('y'),
        '\u{017e}' => Some('z'),
        _ => None,
    }
}

fn tokens_match_in_order_by_prefix(query_tokens: &[&str], name_tokens: &[&str]) -> bool {
    if query_tokens.is_empty() {
        return false;
    }

    let mut search_from = 0;
    for query_token in query_tokens {
        let Some(position) = name_tokens[search_from..]
            .iter()
            .position(|name_token| name_token.starts_with(query_token))
        else {
            return false;
        };
        search_from += position + 1;
    }
    true
}

fn tokens_match_unordered_by_prefix(query_tokens: &[&str], name_tokens: &[&str]) -> bool {
    !query_tokens.is_empty()
        && query_tokens.iter().all(|query_token| {
            name_tokens
                .iter()
                .any(|name_token| name_token.starts_with(query_token))
        })
}

fn fuzzy_token_score(query_tokens: &[&str], name_tokens: &[&str]) -> Option<i32> {
    if query_tokens.is_empty() || name_tokens.is_empty() {
        return None;
    }

    let mut search_from = 0;
    let mut distance_total = 0;
    let mut matched_characters = 0;

    for query_token in query_tokens {
        let threshold = typo_distance_threshold(query_token.chars().count());
        let mut best_match = None;

        for (offset, name_token) in name_tokens[search_from..].iter().enumerate() {
            let distance = levenshtein(query_token, name_token);
            if distance <= threshold {
                best_match = match best_match {
                    Some((best_offset, best_distance)) if best_distance <= distance => {
                        Some((best_offset, best_distance))
                    }
                    _ => Some((offset, distance)),
                };
            }
        }

        let (offset, distance) = best_match?;
        search_from += offset + 1;
        distance_total += distance as i32;
        matched_characters += query_token.chars().count() as i32;
    }

    Some(6_500 + matched_characters * 10 - distance_total * 35)
}

fn typo_distance_threshold(length: usize) -> usize {
    match length {
        0..=2 => 0,
        3..=5 => 1,
        6..=9 => 2,
        _ => 3,
    }
}

fn stop_name_initials(tokens: &[&str]) -> String {
    tokens
        .iter()
        .filter_map(|token| token.chars().next())
        .collect()
}

fn levenshtein(left: &str, right: &str) -> usize {
    let right_len = right.chars().count();
    let mut costs = (0..=right_len).collect::<Vec<_>>();

    for (left_index, left_char) in left.chars().enumerate() {
        let mut previous_diagonal = left_index;
        costs[0] = left_index + 1;

        for (right_index, right_char) in right.chars().enumerate() {
            let insertion = costs[right_index + 1] + 1;
            let deletion = costs[right_index] + 1;
            let substitution = previous_diagonal + usize::from(left_char != right_char);
            previous_diagonal = costs[right_index + 1];
            costs[right_index + 1] = insertion.min(deletion).min(substitution);
        }
    }

    costs[right_len]
}

fn fixture_stops() -> Vec<Stop> {
    vec![
        fixture_stop("stop-praha", "Praha", 50.0755, 14.4378, TransportMode::Bus),
        fixture_stop(
            "stop-praha-hl-n",
            "Praha hlavni nadrazi",
            50.083,
            14.435,
            TransportMode::Train,
        ),
        fixture_stop(
            "stop-brno-hl-n",
            "Brno hlavni nadrazi",
            49.191,
            16.612,
            TransportMode::Train,
        ),
        fixture_stop(
            "stop-jihlava",
            "Jihlava autobusove nadrazi",
            49.396,
            15.591,
            TransportMode::Bus,
        ),
    ]
}

fn fixture_cities() -> Vec<City> {
    vec![
        City {
            id: "city:CZ:554782".to_string(),
            name: "Praha".to_string(),
            normalized_name: "praha".to_string(),
            region: Some("Hlavni mesto Praha".to_string()),
            country_code: "CZ".to_string(),
            lat: Some(50.0755),
            lon: Some(14.4378),
            importance: 100,
        },
        City {
            id: "city:CZ:582786".to_string(),
            name: "Brno".to_string(),
            normalized_name: "brno".to_string(),
            region: Some("Jihomoravsky kraj".to_string()),
            country_code: "CZ".to_string(),
            lat: Some(49.1951),
            lon: Some(16.6068),
            importance: 90,
        },
        City {
            id: "city:CZ:544256".to_string(),
            name: "Ceske Budejovice".to_string(),
            normalized_name: "ceske budejovice".to_string(),
            region: Some("Jihocesky kraj".to_string()),
            country_code: "CZ".to_string(),
            lat: Some(48.9747),
            lon: Some(14.4749),
            importance: 70,
        },
        City {
            id: "city:CZ:586846".to_string(),
            name: "Jihlava".to_string(),
            normalized_name: "jihlava".to_string(),
            region: Some("Kraj Vysocina".to_string()),
            country_code: "CZ".to_string(),
            lat: Some(49.3961),
            lon: Some(15.5912),
            importance: 65,
        },
    ]
}

fn fixture_stop(id: &str, name: &str, lat: f64, lon: f64, mode: TransportMode) -> Stop {
    let municipality = if id.starts_with("stop-praha") {
        Some("Praha".to_string())
    } else if id.starts_with("stop-brno") {
        Some("Brno".to_string())
    } else if id.starts_with("stop-jihlava") {
        Some("Jihlava".to_string())
    } else {
        None
    };
    Stop {
        id: id.to_string(),
        source_ids: vec![transit_model::SourceRef {
            feed_id: "fixture".to_string(),
            original_id: id.to_string(),
            import_run_id: None,
            priority: 999,
            confidence: Some(CoordinateConfidence::Exact),
            suppressed_as_duplicate: false,
        }],
        name: name.to_string(),
        normalized_name: normalize_czech_name(name),
        municipality,
        district: None,
        region: None,
        lat: Some(lat),
        lon: Some(lon),
        geom: Some(geo_types::Point::new(lon, lat)),
        coordinate_confidence: CoordinateConfidence::Exact,
        coordinate_source: Some("fixture".to_string()),
        stop_area_id: None,
        platform_code: None,
        location_type: StopLocationType::Stop,
        parent_station_id: None,
        station_id: Some(format!("station:{id}")),
        complex_id: Some(format!("complex:{id}")),
        has_station_layout: true,
        station_layout_version: Some("mock-v1".to_string()),
        wheelchair_boarding: AccessibilityStatus::Unknown,
        modes: vec![mode],
        is_active: true,
    }
}

fn fixture_departures() -> Vec<Value> {
    vec![
        json!({"line":"R9","destination":"Brno hlavni nadrazi","scheduled_departure":"08:00:00","realtime_departure":null,"delay_seconds":null,"status":"scheduled"}),
        json!({"line":"300","destination":"Jihlava autobusove nadrazi","scheduled_departure":"09:00:00","realtime_departure":null,"delay_seconds":null,"status":"scheduled"}),
    ]
}

fn public_board_payload(stop_id: &str) -> Value {
    json!({
        "stop_id": stop_id,
        "stop_name": stop_id,
        "server_time": Utc::now(),
        "last_update": Utc::now(),
        "departures": fixture_departures(),
        "data_freshness": {"schedule":"mock","realtime":"unavailable"},
        "theme": {"line_badges": true},
        "mock": true
    })
}

fn mock_ticket() -> TicketOption {
    TicketOption {
        id: "mock-basic".to_string(),
        name_cs: "Informacni doporuceni jizdenky".to_string(),
        provider: "mock".to_string(),
        price_czk: None,
        mock: true,
    }
}

fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let earth_radius_m = 6_371_000.0_f64;
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let a = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
    2.0 * earth_radius_m * a.sqrt().asin()
}

fn direction_buckets_compatible(left: i32, right: i32) -> bool {
    let difference = (left - right).abs();
    difference.min(8 - difference) <= 1
}

fn combine_nearby_direction_pairs(pairs: Vec<Value>) -> Vec<Value> {
    let mut groups: Vec<Value> = Vec::new();
    for pair in pairs {
        let canonical_id = pair["suggested_canonical_stop_id"]
            .as_str()
            .unwrap_or_default();
        let normalized_name = pair["normalized_name"].as_str().unwrap_or_default();
        let existing = groups.iter_mut().find(|group| {
            group["suggested_canonical_stop_id"].as_str() == Some(canonical_id)
                && group["normalized_name"].as_str() == Some(normalized_name)
        });
        let Some(group) = existing else {
            groups.push(pair);
            continue;
        };

        let stop_count = {
            let incoming_stops = pair["stops"].as_array().cloned().unwrap_or_default();
            let group_stops = group["stops"]
                .as_array_mut()
                .expect("candidate stops array");
            for stop in incoming_stops {
                let stop_id = stop["id"].as_str();
                if !group_stops
                    .iter()
                    .any(|existing| existing["id"].as_str() == stop_id)
                {
                    group_stops.push(stop);
                }
            }
            group_stops.len()
        };
        group["stop_count"] = json!(stop_count);
        group["distance_m"] = json!(
            group["distance_m"]
                .as_f64()
                .unwrap_or_default()
                .max(pair["distance_m"].as_f64().unwrap_or_default())
        );
        group["high_confidence_candidate"] = json!(
            group["high_confidence_candidate"]
                .as_bool()
                .unwrap_or(false)
                && pair["high_confidence_candidate"].as_bool().unwrap_or(false)
        );
        group["automatic_candidate"] = json!(
            group["automatic_candidate"].as_bool().unwrap_or(false)
                && pair["automatic_candidate"].as_bool().unwrap_or(false)
        );
    }

    groups.sort_by(|left, right| {
        right["stop_count"]
            .as_u64()
            .cmp(&left["stop_count"].as_u64())
            .then_with(|| {
                left["normalized_name"]
                    .as_str()
                    .cmp(&right["normalized_name"].as_str())
            })
    });
    let mut retained_stop_sets: Vec<HashSet<String>> = Vec::new();
    groups
        .into_iter()
        .filter(|group| {
            let stop_ids = group["stops"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|stop| stop["id"].as_str().map(str::to_string))
                .collect::<HashSet<_>>();
            if retained_stop_sets
                .iter()
                .any(|retained| stop_ids.is_subset(retained))
            {
                return false;
            }
            retained_stop_sets.push(stop_ids);
            true
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn stop_search_related_source_query_qualifies_joined_columns() {
        let query = STOP_SEARCH_SOURCE_IDS_QUERY.to_ascii_lowercase();

        assert!(query.contains("select source_id.stop_id"));
        assert!(query.contains("source_id.priority"));
        assert!(query.contains("where source_id.stop_id = any($1)"));
        assert!(query.contains("order by source_id.stop_id asc, source_id.priority asc"));
        assert!(!query.contains("select stop_id,"));
        assert!(!query.contains("order by stop_id asc, priority asc"));
    }

    #[test]
    fn operational_run_and_call_ids_are_dated_and_stable() {
        let date = chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap();
        let run = operational_run_id("pid_gtfs:trip-123", date);
        let call = operational_call_id(&run, "pid_gtfs:stop", 7);

        assert!(run.starts_with("run:2026-10-01:"));
        assert_eq!(run, operational_run_id("pid_gtfs:trip-123", date));
        assert!(call.starts_with("call:"));
        assert_ne!(call, operational_call_id(&run, "pid_gtfs:stop", 8));
    }

    #[test]
    fn accessibility_profile_is_rejected_until_end_to_end_verification_exists() {
        let body = JourneySearchBody {
            from: JourneyPoint {
                point_type: "stop".into(),
                id: Some("a".into()),
                lat: None,
                lon: None,
            },
            to: JourneyPoint {
                point_type: "stop".into(),
                id: Some("b".into()),
                lat: None,
                lon: None,
            },
            datetime: "2026-10-01T08:00:00+02:00".into(),
            mode: "depart_at".into(),
            transport_modes: vec![TransportMode::Train],
            max_transfers: 1,
            walking_speed: "normal".into(),
            prefer_reliable_transfers: true,
            offline_compatible: true,
            include_intermediate_stops: true,
            journey_preferences: Some(JourneyPreferences {
                profile: "wheelchair".into(),
                step_free: true,
                prefer_fewer_stairs: true,
                minimum_transfer_buffer_seconds: 600,
            }),
        };

        let error = validate_journey_preferences(&body).unwrap_err();
        assert_eq!(error.code, "journey_accessibility_unverified");
    }

    #[test]
    fn mock_layout_preserves_requested_station_identity_and_level() {
        let layout = mock_station_layout("station:test", Some("mock-platform")).unwrap();
        assert_eq!(layout["stationId"], "station:test");
        assert_eq!(layout["mock"], true);
        assert!(mock_station_layout("station:test", Some("missing")).is_err());
    }

    #[test]
    fn routing_realtime_query_uses_the_validity_indexable_path() {
        let query = JOURNEY_ROUTING_REALTIME_QUERY.to_ascii_lowercase();

        assert!(query.contains("and valid_until >= now()"));
        assert!(!query.contains("valid_until is null"));
    }

    #[test]
    fn raptor_snapshot_path_includes_format_and_data_revision() {
        let revision = RoutingDataRevision {
            latest_import: DateTime::from_timestamp_millis(1_783_479_136_328),
            token: "0123456789abcdef".to_string(),
        };
        let path = raptor_timetable_snapshot_path(
            FsPath::new("routing"),
            chrono::NaiveDate::from_ymd_opt(2026, 7, 8).unwrap(),
            &revision,
        );

        assert_eq!(
            path,
            PathBuf::from("routing/raptor-v13-2026-07-08-1783479136328-0123456789abcdef.json")
        );
    }

    #[tokio::test]
    async fn obsolete_raptor_snapshots_are_deleted_without_touching_other_files() {
        let directory = std::env::temp_dir().join(format!("cesta-raptor-prune-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        for file_name in [
            "raptor-v1-2026-07-08-old.json",
            "raptor-v2-2026-07-08-old.json.tmp",
            "raptor-v3-2026-07-08-old.json",
            "raptor-v4-2026-07-08-old.json",
            "raptor-v5-2026-07-08-old.json",
            "raptor-v6-2026-07-08-old.json",
            "raptor-v7-2026-07-08-old.json",
            "raptor-v8-2026-07-08-old.json",
            "raptor-v9-2026-07-08-current.json",
            "raptor-v13-2026-07-08-newer.json",
            "notes.json",
        ] {
            tokio::fs::write(directory.join(file_name), b"test")
                .await
                .unwrap();
        }

        assert_eq!(prune_raptor_snapshots(&directory, 8).await.unwrap(), 9);
        assert!(!directory.join("raptor-v1-2026-07-08-old.json").exists());
        assert!(!directory.join("raptor-v2-2026-07-08-old.json.tmp").exists());
        assert!(!directory.join("raptor-v3-2026-07-08-old.json").exists());
        assert!(!directory.join("raptor-v4-2026-07-08-old.json").exists());
        assert!(!directory.join("raptor-v5-2026-07-08-old.json").exists());
        assert!(!directory.join("raptor-v6-2026-07-08-old.json").exists());
        assert!(!directory.join("raptor-v7-2026-07-08-old.json").exists());
        assert!(!directory.join("raptor-v8-2026-07-08-old.json").exists());
        assert!(!directory.join("raptor-v9-2026-07-08-current.json").exists());
        assert!(directory.join("raptor-v13-2026-07-08-newer.json").exists());
        assert!(directory.join("notes.json").exists());
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn warmup_recreates_a_missing_snapshot_from_memory() {
        let directory =
            std::env::temp_dir().join(format!("cesta-raptor-persistence-{}", Uuid::new_v4()));
        let revision = RoutingDataRevision {
            latest_import: DateTime::from_timestamp_millis(1_783_479_136_328),
            token: "0123456789abcdef".to_string(),
        };
        let service_date = chrono::NaiveDate::from_ymd_opt(2026, 7, 24).unwrap();
        let path = raptor_timetable_snapshot_path(&directory, service_date, &revision);
        let timetable = RaptorTimetable::default();

        assert!(
            ensure_raptor_timetable_snapshot(&path, service_date, &revision, &timetable)
                .await
                .unwrap()
        );
        assert!(path.is_file());
        assert!(
            !ensure_raptor_timetable_snapshot(&path, service_date, &revision, &timetable)
                .await
                .unwrap()
        );
        assert!(
            load_raptor_timetable_snapshot(&path, service_date, &revision)
                .await
                .is_some()
        );

        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn warmup_reports_snapshot_directory_write_failures() {
        let directory =
            std::env::temp_dir().join(format!("cesta-raptor-write-error-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let blocked_parent = directory.join("not-a-directory");
        tokio::fs::write(&blocked_parent, b"file").await.unwrap();
        let path = blocked_parent.join("snapshot.json");
        let revision = RoutingDataRevision {
            latest_import: None,
            token: "write-error".to_string(),
        };
        let service_date = chrono::NaiveDate::from_ymd_opt(2026, 7, 24).unwrap();

        let error = ensure_raptor_timetable_snapshot(
            &path,
            service_date,
            &revision,
            &RaptorTimetable::default(),
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to create snapshot directory")
        );
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn raptor_snapshot_retention_keeps_one_revision_per_date_and_bounds_cache() {
        let directory =
            std::env::temp_dir().join(format!("cesta-raptor-retention-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        for file_name in [
            "raptor-v13-2026-06-01-import-old.json",
            "raptor-v13-2026-06-01-import-new.json",
            "raptor-v13-2026-06-02-import.json",
            "raptor-v13-2026-06-03-import.json",
            "raptor-v13-2026-06-04-import.json",
            "raptor-v13-2026-06-05-import.json",
        ] {
            tokio::fs::write(directory.join(file_name), b"test")
                .await
                .unwrap();
        }
        tokio::fs::write(directory.join("manual-export.json"), b"keep")
            .await
            .unwrap();

        assert_eq!(prune_raptor_snapshots(&directory, 8).await.unwrap(), 1);
        assert_ne!(
            directory
                .join("raptor-v13-2026-06-01-import-old.json")
                .exists(),
            directory
                .join("raptor-v13-2026-06-01-import-new.json")
                .exists()
        );
        assert_eq!(prune_raptor_snapshots(&directory, 2).await.unwrap(), 3);
        let mut retained_snapshots = tokio::fs::read_dir(&directory).await.unwrap();
        let mut retained_count = 0;
        while let Some(entry) = retained_snapshots.next_entry().await.unwrap() {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("raptor-v13-"))
            {
                retained_count += 1;
            }
        }
        assert_eq!(retained_count, 2);
        assert!(directory.join("manual-export.json").exists());
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn health_endpoint() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("x-request-id"));
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_eq!(response.headers()["x-frame-options"], "DENY");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["status"], "ok");
        assert_eq!(payload["routing_schedule_ready"], false);
        assert_eq!(payload["routing_realtime_ready"], false);
        assert_eq!(payload["routing"]["schedule_ready"], false);
        assert_eq!(payload["routing"]["realtime_ready"], false);
    }

    #[tokio::test]
    async fn stop_catalog_download_and_revalidation() {
        let state = app_state().await.unwrap();
        let app = build_router(state.clone());
        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/stops/catalog")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(first.headers()[header::CACHE_CONTROL], "no-cache");
        let etag = first.headers()[header::ETAG].clone();
        let body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["schema_version"], 1);
        assert_eq!(payload["count"], state.stops.len());
        assert_eq!(payload["data_status"]["source"], "mock");
        assert!(
            payload["stops"]
                .as_array()
                .unwrap()
                .windows(2)
                .all(|pair| { pair[0]["id"].as_str().unwrap() < pair[1]["id"].as_str().unwrap() })
        );
        assert_eq!(payload["stops"][0]["source_feed_id"], "fixture");

        let compressed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/stops/catalog")
                    .header(header::ACCEPT_ENCODING, "gzip")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(compressed.status(), StatusCode::OK);
        assert_eq!(compressed.headers()[header::CONTENT_ENCODING], "gzip");
        assert_eq!(compressed.headers()[header::ETAG], etag);
        assert!(
            to_bytes(compressed.into_body(), usize::MAX)
                .await
                .unwrap()
                .len()
                < body.len()
        );

        let unchanged = app
            .oneshot(
                Request::builder()
                    .uri("/stops/catalog")
                    .header(header::IF_NONE_MATCH, &etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unchanged.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(unchanged.headers()[header::ETAG], etag);
        assert!(
            to_bytes(unchanged.into_body(), usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );

        let mut changed_state = state;
        let mut changed_stops = changed_state.stops.as_ref().clone();
        changed_stops[0].name.push_str(" nový název");
        changed_state.stops = Arc::new(changed_stops);
        let changed = build_router(changed_state)
            .oneshot(
                Request::builder()
                    .uri("/stops/catalog")
                    .header(header::IF_NONE_MATCH, &etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(changed.status(), StatusCode::OK);
        assert_ne!(changed.headers()[header::ETAG], etag);
    }

    #[tokio::test]
    async fn openapi_documents_city_search_and_journey_points() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert!(payload["paths"]["/stops/catalog"]["get"].is_object());
        assert!(
            payload["paths"]["/stops/search"]["get"]["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .any(|parameter| parameter["name"] == "includeCities")
        );
        assert!(
            payload["paths"]["/stops/search"]["get"]["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .any(|parameter| parameter["name"] == "includeRelated")
        );
        assert_eq!(
            payload["components"]["schemas"]["JourneyPoint"]["properties"]["type"]["enum"],
            json!(["stop", "city", "coordinate"])
        );
        assert_eq!(
            payload["components"]["schemas"]["JourneyPoint"]["required"],
            json!(["type"])
        );
        assert!(
            payload["components"]["schemas"]["PlaceType"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "city")
        );
        assert!(payload["paths"]["/realtime/vehicles"].is_object());
        assert!(payload["paths"]["/vehicles"].is_object());
        assert!(payload["components"]["schemas"]["Vehicle"].is_object());
        assert!(payload["paths"]["/data-sources/status"].is_object());
        assert!(payload["paths"]["/admin/imports/pid/start"]["post"].is_object());
        assert!(payload["paths"]["/admin/imports/ggu-latest/start"].is_null());
        assert!(payload["paths"]["/stops/in-bounds"]["get"].is_object());
        assert!(payload["components"]["schemas"]["StopsInBoundsResponse"].is_object());
        assert!(payload["components"]["schemas"]["JourneyLegRealtime"].is_object());
        assert!(
            payload["components"]["schemas"]["JourneyLeg"]["properties"]["display_name"]
                .is_object()
        );
        assert_eq!(
            payload["components"]["schemas"]["JourneyLeg"]["properties"]["geometry"]["$ref"],
            "#/components/schemas/JourneyLegGeometry"
        );
        assert_eq!(
            payload["paths"]["/journeys/search"]["post"]["requestBody"]["content"]["application/json"]
                ["schema"]["properties"]["include_intermediate_stops"]["default"],
            false
        );
        assert!(payload["components"]["schemas"]["JourneyStopCall"].is_object());
        assert!(payload["paths"]["/stations/{stationId}/layout"]["get"].is_object());
        assert!(payload["paths"]["/runs/{runId}/formation"]["get"].is_object());
        assert!(
            payload["paths"]["/journeys/{journeyId}/legs/{legIndex}/boarding-guidance"]["get"]
                .is_object()
        );
        assert!(payload["components"]["schemas"]["JourneyPreferences"].is_object());
        assert!(payload["paths"]["/admin/routing-algorithm"]["put"].is_object());
        assert!(payload["paths"]["/admin/data-quality/repairs"]["get"].is_object());
        assert!(payload["paths"]["/admin/data-quality/repairs/automatic"]["post"].is_object());
        assert!(payload["paths"]["/admin/data-quality/duplicates/merge"]["post"].is_object());
        assert_eq!(
            payload["paths"]["/admin/database/stats"]["get"]["responses"]["200"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/AdminDatabaseStats"
        );
        assert!(
            payload["components"]["schemas"]["AdminDatabaseStats"]["properties"]["storage"]
                .is_object()
        );
        assert!(
            payload["components"]["schemas"]["AdminDatabaseStats"]["properties"]["largest_indexes"]
                .is_object()
        );
        assert_eq!(
            payload["paths"]["/admin/data-quality/duplicates/merge"]["post"]["requestBody"]["content"]
                ["application/json"]["schema"]["properties"]["strategy"]["enum"],
            json!(["exact_coordinates", "nearby_same_direction"])
        );
        assert!(payload["components"]["schemas"]["RouteSearchDiagnostics"].is_object());
    }

    #[tokio::test]
    async fn metadata_sources_lists_enabled_official_feeds() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metadata/sources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        let source_ids = payload["sources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|source| source["id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            source_ids,
            vec![
                "pid_gtfs",
                "pid_lines_geodata",
                "pid_realtime",
                "ids_jmk_gtfs",
                "ids_jmk_realtime"
            ]
        );
    }

    #[tokio::test]
    async fn stops_in_bounds_returns_only_visible_fixture_stops() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stops/in-bounds?south=50.0&west=14.3&north=50.2&east=14.6")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["stops"].as_array().unwrap().len(), 2);
        assert_eq!(payload["stops"][0]["id"], "stop-praha");
        assert_eq!(payload["stops"][0]["marker_type"], "bus_stop");
        assert_eq!(payload["stops"][0]["location_type"], "stop");
        assert_eq!(payload["stops"][0]["wheelchair_boarding"], "unknown");
        assert_eq!(payload["stops"][0]["map_visible"], true);
        assert_eq!(payload["stops"][1]["id"], "stop-praha-hl-n");
        assert!(payload["nextCursor"].is_null());
        assert_eq!(payload["data_status"]["source"], "mock");
    }

    #[tokio::test]
    async fn stops_in_bounds_cursor_pages_without_skipping_stops() {
        let app = build_router(app_state().await.unwrap());
        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/stops/in-bounds?south=50.0&west=14.3&north=50.2&east=14.6&limit=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_payload: Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_payload["stops"][0]["id"], "stop-praha");
        assert_eq!(first_payload["nextCursor"], "stop-praha");

        let second = app
            .oneshot(
                Request::builder()
                    .uri("/stops/in-bounds?south=50.0&west=14.3&north=50.2&east=14.6&limit=1&cursor=stop-praha")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_payload: Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_payload["stops"][0]["id"], "stop-praha-hl-n");
        assert!(second_payload["nextCursor"].is_null());
    }

    #[tokio::test]
    async fn stops_in_bounds_rejects_reversed_bounds() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stops/in-bounds?south=50.2&west=14.3&north=50.0&east=14.6")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["code"], "validation_error");
        assert_eq!(payload["message"], "south must be less than north");
    }

    #[tokio::test]
    async fn vehicles_rejects_reversed_bbox() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/vehicles?bbox=14.7,50.2,14.3,50.0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["code"], "validation_error");
    }

    #[tokio::test]
    async fn protected_endpoint_blocked_without_token() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/auth/me")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn refresh_tokens_are_single_use() {
        let app = build_router(app_state().await.unwrap());
        let register_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/register")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "email": "refresh-test@example.cz",
                            "password": "secure-password"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(register_response.status(), StatusCode::OK);
        let register_body = to_bytes(register_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let registered: Value = serde_json::from_slice(&register_body).unwrap();
        let refresh_token = registered["refresh_token"].as_str().unwrap();
        let refresh_body = json!({"refresh_token": refresh_token}).to_string();

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/refresh")
                    .header("content-type", "application/json")
                    .body(Body::from(refresh_body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let reused = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/refresh")
                    .header("content-type", "application/json")
                    .body(Body::from(refresh_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reused.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_interface_is_served_for_login() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("Administrator sign in"));
        assert!(html.contains("/admin/assets/admin.js"));
        assert!(html.contains("Routing algorithm"));
        assert!(html.contains("Safe automatic repairs"));
        assert!(html.contains("Duplicate-stop review"));
        assert!(html.contains("Nearby stops in the same direction"));
        assert!(html.contains("Automatically repaired when safe"));
        assert!(html.contains("Current storage usage"));
        assert!(html.contains("Largest indexes"));
        assert!(html.contains("Table rows are PostgreSQL estimates"));
    }

    #[tokio::test]
    async fn admin_data_endpoint_requires_admin_token() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/data")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_database_usage_requires_admin_token() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/database/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_routing_algorithm_requires_admin_token() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/routing-algorithm")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_validation_endpoint_requires_admin_token() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/data-quality/validate")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_repair_endpoints_require_admin_token() {
        for (method, uri, body) in [
            ("GET", "/admin/data-quality/repairs", Body::empty()),
            (
                "POST",
                "/admin/data-quality/repairs/automatic",
                Body::from(r#"{"confirmation":"apply_safe_repairs"}"#),
            ),
            (
                "POST",
                "/admin/data-quality/duplicates/merge",
                Body::from(
                    r#"{"canonical_stop_id":"a","duplicate_stop_ids":["b"],"confirmation":"merge_duplicate_stops"}"#,
                ),
            ),
        ] {
            let app = build_router(app_state().await.unwrap());
            let response = app
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .header("content-type", "application/json")
                        .body(body)
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
        }
    }

    #[tokio::test]
    async fn admin_related_data_endpoint_requires_admin_token() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/related/stops/stop-praha-hl-n")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn database_validation_covers_core_schedule_and_source_tracking() {
        let codes = DATA_VALIDATION_CHECKS
            .iter()
            .map(|check| check.code)
            .collect::<HashSet<_>>();

        for required in [
            "stop_missing_coordinates",
            "stop_missing_source_tracking",
            "route_without_trips",
            "trip_without_stop_times",
            "trip_without_service_calendar",
            "stop_time_invalid_time",
            "calendar_invalid_range",
            "enabled_source_without_successful_import",
        ] {
            assert!(
                codes.contains(required),
                "missing validation check {required}"
            );
        }
    }

    #[tokio::test]
    async fn stop_search_ranks_closest_typo_match_first() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stops/search?q=Praha%20hlavny%20nadrazy")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["stops"][0]["id"], "stop-praha-hl-n");
    }

    #[tokio::test]
    async fn stop_search_supports_abbreviated_tokens() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stops/search?q=brno%20hl%20n")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["stops"][0]["id"], "stop-brno-hl-n");
    }

    #[tokio::test]
    async fn stop_search_collapses_platform_level_duplicates() {
        let mut platform = fixture_stop(
            "stop-praha-hl-n-platform-1",
            "Praha hlavni nadrazi",
            50.083,
            14.435,
            TransportMode::Train,
        );
        platform.platform_code = Some("1".to_string());
        let station = fixture_stop(
            "stop-praha-hl-n",
            "Praha hlavni nadrazi",
            50.083,
            14.435,
            TransportMode::Train,
        );
        let stops = [platform, station];

        let suggestions = ranked_stop_suggestions(stops.iter(), "praha", 6);

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].id, "stop-praha-hl-n");
    }

    #[test]
    fn stop_search_exposes_canonical_name_and_aliases() {
        let stop = fixture_stop(
            "pid_gtfs:U462S1",
            "Nádraží Veleslavín / Praha-Veleslavín",
            50.095955,
            14.348551,
            TransportMode::Metro,
        );

        let payload = stop_search_json(&stop);

        assert_eq!(payload["canonical_name"], "Nádraží Veleslavín");
        assert_eq!(
            payload["aliases"],
            json!(["Nádraží Veleslavín", "Praha-Veleslavín"])
        );
    }

    #[test]
    fn stop_search_keeps_railway_and_urban_same_name_stops_separate() {
        let mut station = fixture_stop(
            "central-station",
            "Central",
            50.0,
            14.0,
            TransportMode::Unknown,
        );
        station.location_type = StopLocationType::Station;
        station.modes.clear();

        let mut metro = fixture_stop(
            "central-metro",
            "Central",
            50.0001,
            14.0,
            TransportMode::Metro,
        );
        metro.platform_code = Some("M1".to_string());
        let mut bus = fixture_stop("central-bus", "Central", 50.0002, 14.0, TransportMode::Bus);
        bus.platform_code = Some("B".to_string());
        let mut train = fixture_stop(
            "central-train",
            "Central",
            50.0003,
            14.0,
            TransportMode::Train,
        );
        train.platform_code = Some("1".to_string());
        let stops = [station, metro, bus, train];

        let suggestions = ranked_stop_suggestions(stops.iter(), "central", 10);

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].id, "central-station");
        assert_eq!(
            suggestions[0].modes,
            vec![TransportMode::Metro, TransportMode::Bus]
        );
        assert_eq!(suggestions[1].id, "central-train");
        assert_eq!(suggestions[1].modes, vec![TransportMode::Train]);
    }

    #[test]
    fn pid_railway_and_tram_platform_are_separate_suggestions() {
        let mut railway = fixture_stop(
            "pid_gtfs:U142S2",
            "Hlavní nádraží",
            50.083096,
            14.436194,
            TransportMode::Train,
        );
        railway.location_type = StopLocationType::Station;
        railway.platform_code = None;
        let tram = fixture_stop(
            "pid_gtfs:U142Z2P",
            "Hlavní nádraží",
            50.085292,
            14.435092,
            TransportMode::Tram,
        );

        let suggestions =
            ranked_stop_suggestions([&railway, &tram].into_iter(), "hlavni nadrazi", 10);

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].modes, vec![TransportMode::Train]);
        assert_eq!(suggestions[1].modes, vec![TransportMode::Tram]);
    }

    #[test]
    fn pid_ferry_and_tram_platforms_are_separate_suggestions() {
        let ferry = fixture_stop(
            "pid_gtfs:U19Z11P",
            "Belárie",
            50.013126,
            14.397527,
            TransportMode::Ferry,
        );
        let tram_a = fixture_stop(
            "pid_gtfs:U19Z1P",
            "Belárie",
            50.015944,
            14.397394,
            TransportMode::Tram,
        );
        let tram_b = fixture_stop(
            "pid_gtfs:U19Z2P",
            "Belárie",
            50.015840,
            14.398274,
            TransportMode::Tram,
        );

        let suggestions =
            ranked_stop_suggestions([&ferry, &tram_a, &tram_b].into_iter(), "belarie", 10);

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].id, tram_a.id);
        assert_eq!(suggestions[0].modes, vec![TransportMode::Tram]);
        assert_eq!(suggestions[1].id, ferry.id);
        assert_eq!(suggestions[1].modes, vec![TransportMode::Ferry]);
    }

    #[test]
    fn stop_search_collapses_distant_platforms_in_the_same_pid_complex() {
        let mut station = fixture_stop(
            "pid_gtfs:U1141S1",
            "Zličín",
            50.053264,
            14.291140,
            TransportMode::Metro,
        );
        station.location_type = StopLocationType::Station;
        station.platform_code = None;
        let mut directional_platform = fixture_stop(
            "pid_gtfs:U1141Z9P",
            "Zličín",
            50.055874,
            14.288173,
            TransportMode::Bus,
        );
        directional_platform.platform_code = Some("9".to_string());

        assert!(
            haversine_m(
                station.lat.unwrap(),
                station.lon.unwrap(),
                directional_platform.lat.unwrap(),
                directional_platform.lon.unwrap()
            ) > 300.0
        );

        let suggestions =
            ranked_stop_suggestions([&station, &directional_platform].into_iter(), "zlicin", 10);

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].id, "pid_gtfs:U1141S1");
        assert_eq!(
            suggestions[0].modes,
            vec![TransportMode::Metro, TransportMode::Bus]
        );
    }

    #[test]
    fn stop_search_collapses_same_name_platforms_with_one_parent_station() {
        let mut line_a = fixture_stop(
            "metro-a-platform",
            "Můstek",
            50.0838,
            14.4242,
            TransportMode::Metro,
        );
        line_a.parent_station_id = Some("mustek-interchange".to_string());
        line_a.platform_code = Some("A".to_string());
        let mut line_b = fixture_stop(
            "metro-b-platform",
            "Můstek",
            50.0870,
            14.4242,
            TransportMode::Metro,
        );
        line_b.parent_station_id = Some("mustek-interchange".to_string());
        line_b.platform_code = Some("B".to_string());

        let suggestions = ranked_stop_suggestions([&line_a, &line_b].into_iter(), "mustek", 10);

        assert_eq!(suggestions.len(), 1);
    }

    #[test]
    fn pid_public_stop_names_keep_interchange_boarding_points_distinct() {
        assert_eq!(
            pid_public_stop_name("pid_gtfs:U237Z6P", "Palackého náměstí", Some("K")),
            "Palackého náměstí"
        );
        assert_eq!(
            pid_public_stop_name("pid_gtfs:U237Z7P", "Palackého náměstí", Some("I")),
            "Palackého náměstí (nábřeží)"
        );
        assert_eq!(
            pid_public_stop_name("pid_gtfs:U237S1", "Karlovo náměstí", None),
            "Karlovo náměstí"
        );
        assert_eq!(
            pid_source_stop_query("palackeho namesti nabrezi"),
            "palackeho namesti"
        );
    }

    #[test]
    fn pid_interchange_modes_never_cross_public_stop_names() {
        let mut palackeho = fixture_stop(
            "pid_gtfs:U237Z5P",
            "Palackého náměstí",
            50.073231,
            14.415247,
            TransportMode::Tram,
        );
        let metro = fixture_stop(
            "pid_gtfs:U237Z101P",
            "Karlovo náměstí",
            50.074664,
            14.416855,
            TransportMode::Metro,
        );
        let trolleybus = fixture_stop(
            "pid_gtfs:U237Z6P",
            "Palackého náměstí",
            50.073265,
            14.414462,
            TransportMode::Trolleybus,
        );

        merge_interchange_modes(&mut palackeho, &[metro, trolleybus]);

        assert_eq!(palackeho.id, "pid_gtfs:U237Z5P");
        assert_eq!(palackeho.name, "Palackého náměstí");
        assert_eq!(
            palackeho.modes,
            vec![TransportMode::Tram, TransportMode::Trolleybus]
        );
    }

    #[test]
    fn shared_stop_area_never_merges_different_public_names() {
        let mut karlovo = fixture_stop(
            "pid_gtfs:U237Z101P",
            "Karlovo náměstí",
            50.074664,
            14.416855,
            TransportMode::Metro,
        );
        let mut palackeho = fixture_stop(
            "pid_gtfs:U237Z5P",
            "Palackého náměstí",
            50.073231,
            14.415247,
            TransportMode::Tram,
        );
        karlovo.stop_area_id = Some("pid_gtfs:U237".to_string());
        palackeho.stop_area_id = Some("pid_gtfs:U237".to_string());

        assert!(!stops_are_same_suggestion(&karlovo, &palackeho));
    }

    #[test]
    fn pid_stop_complex_id_requires_a_pid_station_or_platform_marker() {
        assert_eq!(
            pid_stop_complex_id("pid_gtfs:U237Z6P").as_deref(),
            Some("pid_gtfs:U237")
        );
        assert_eq!(
            pid_stop_complex_id("pid_gtfs:U237S1E7").as_deref(),
            Some("pid_gtfs:U237")
        );
        assert_eq!(pid_stop_complex_id("pid_gtfs:U237"), None);
        assert_eq!(pid_stop_complex_id("other:U237Z6P"), None);
    }

    #[test]
    fn stop_search_collapses_municipality_prefixed_source_aliases() {
        let mut short_name = fixture_stop(
            "pid-belarie",
            "Belárie",
            50.0350,
            14.4180,
            TransportMode::Tram,
        );
        short_name.municipality = Some("Praha".to_string());
        let mut qualified_name = fixture_stop(
            "national-belarie",
            "Praha, Belárie",
            50.0353,
            14.4182,
            TransportMode::Tram,
        );
        qualified_name.municipality = Some("Praha".to_string());

        let suggestions =
            ranked_stop_suggestions([&short_name, &qualified_name].into_iter(), "belarie", 10);

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].id, "pid-belarie");
    }

    #[test]
    fn stop_search_keeps_same_named_stops_in_different_places() {
        let mut prague = fixture_stop(
            "prague-namesti",
            "Náměstí",
            50.0755,
            14.4378,
            TransportMode::Bus,
        );
        prague.municipality = Some("Praha".to_string());
        let mut brno = fixture_stop(
            "brno-namesti",
            "Náměstí",
            49.1951,
            16.6068,
            TransportMode::Bus,
        );
        brno.municipality = Some("Brno".to_string());

        let suggestions = ranked_stop_suggestions([&prague, &brno].into_iter(), "namesti", 10);

        assert_eq!(suggestions.len(), 2);
    }

    #[test]
    fn exact_stop_search_does_not_fill_results_with_unrelated_name_fragments() {
        let exact = fixture_stop(
            "pid-mustek",
            "Můstek",
            50.0839,
            14.4233,
            TransportMode::Metro,
        );
        let unrelated = fixture_stop(
            "regional-lyzarsky-mustek",
            "Nýdek,Gora,lyžařský můstek",
            49.6560,
            18.7560,
            TransportMode::Bus,
        );

        let suggestions = ranked_stop_suggestions([&exact, &unrelated].into_iter(), "mustek", 10);

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].id, "pid-mustek");
    }

    #[test]
    fn direction_bucket_matching_handles_north_wraparound() {
        assert!(direction_buckets_compatible(0, 0));
        assert!(direction_buckets_compatible(0, 1));
        assert!(direction_buckets_compatible(0, 7));
        assert!(!direction_buckets_compatible(0, 2));
        assert!(!direction_buckets_compatible(1, 7));
        assert!(!direction_buckets_compatible(0, 4));
    }

    #[test]
    fn nearby_direction_pairs_are_combined_for_one_canonical_stop() {
        let pair = |canonical: &str, left: &str, right: &str| {
            json!({
                "normalized_name": "mustek",
                "suggested_canonical_stop_id": canonical,
                "stop_count": 2,
                "distance_m": 15.0,
                "high_confidence_candidate": true,
                "automatic_candidate": true,
                "stops": [{"id": left}, {"id": right}]
            })
        };

        let groups = combine_nearby_direction_pairs(vec![
            pair("a", "a", "b"),
            pair("a", "a", "c"),
            pair("b", "b", "c"),
        ]);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0]["suggested_canonical_stop_id"], "a");
        assert_eq!(groups[0]["stop_count"], 3);
        assert_eq!(groups[0]["automatic_candidate"], true);
    }

    #[test]
    fn stop_search_candidate_limit_stays_small_for_autocomplete() {
        assert_eq!(stop_search_candidate_limit(1), 20);
        assert_eq!(stop_search_candidate_limit(10), 60);
        assert_eq!(stop_search_candidate_limit(50), 100);
    }

    #[tokio::test]
    async fn place_search_returns_praha_city_and_main_station() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stops/search?q=Praha&limit=20&includeCities=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        let results = payload["results"].as_array().unwrap();
        assert!(
            results.iter().any(|result| {
                result["id"] == "city:CZ:554782" && result["place_type"] == "city"
            })
        );
        assert!(results.iter().any(|result| {
            result["id"] == "stop-praha-hl-n" && result["place_type"] == "railway_station"
        }));
    }

    #[tokio::test]
    async fn place_search_finds_ceske_budejovice_without_diacritics() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stops/search?q=Ceske%20Budejovice&includeCities=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            payload["results"].as_array().unwrap().iter().any(|result| {
                result["id"] == "city:CZ:544256" && result["place_type"] == "city"
            })
        );
    }

    #[tokio::test]
    async fn city_and_same_named_stop_have_distinct_ids() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stops/search?q=Praha&includeCities=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        let results = payload["results"].as_array().unwrap();
        assert!(
            results
                .iter()
                .any(|result| result["id"] == "city:CZ:554782")
        );
        assert!(results.iter().any(|result| result["id"] == "stop-praha"));
    }

    #[tokio::test]
    async fn include_cities_false_preserves_stop_only_response() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stops/search?q=Praha&includeCities=false")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert!(payload.get("results").is_none());
        assert!(payload.get("cities").is_none());
        assert!(
            payload["stops"].as_array().unwrap().iter().all(|result| {
                result["place_type"] != "city" && result["id"] != "city:CZ:554782"
            })
        );
    }

    #[tokio::test]
    async fn stop_suggester_accepts_common_query_parameter_aliases() {
        for parameter in ["query", "text", "term"] {
            let app = build_router(app_state().await.unwrap());
            let response = app
                .oneshot(
                    Request::builder()
                        .uri(format!("/stops/search?{parameter}=Brno&limit=1"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let payload: Value = serde_json::from_slice(&body).unwrap();
            let stops = payload["stops"].as_array().unwrap();
            assert_eq!(stops.len(), 1);
            assert_eq!(stops[0]["id"], "stop-brno-hl-n");
        }
    }

    #[tokio::test]
    async fn stop_suggester_accepts_snake_case_city_flag() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stops/search?query=Praha&include_cities=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert!(payload["results"].as_array().is_some_and(|results| {
            results.iter().any(|result| result["place_type"] == "city")
        }));
    }

    #[test]
    fn latency_percentiles_use_nearest_rank_and_handle_empty_samples() {
        assert_eq!(latency_percentile(&[], 95), None);
        assert_eq!(latency_percentile(&[240], 50), Some(240));
        let values = (1..=20).collect::<Vec<u64>>();
        assert_eq!(latency_percentile(&values, 50), Some(10));
        assert_eq!(latency_percentile(&values, 95), Some(19));
        assert_eq!(latency_percentile(&values, 100), Some(20));
    }

    #[tokio::test]
    async fn journey_search_rejects_arrival_and_unknown_modes_before_routing() {
        let app = build_router(app_state().await.unwrap());
        for mode in ["arrive_by", "unexpected"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/journeys/search")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            json!({
                                "from": {"type":"stop", "id":"stop-praha-hl-n"},
                                "to": {"type":"stop", "id":"stop-brno-hl-n"},
                                "datetime":"2026-07-06T07:05:00+02:00", "mode":mode,
                                "transport_modes":["train"], "max_transfers":4,
                                "walking_speed":"normal", "prefer_reliable_transfers":true,
                                "offline_compatible":false
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let payload: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(payload["code"], "unsupported_search_mode");
            assert!(payload.get("journeys").is_none());
        }
    }

    fn journey_search_request(
        from_type: &str,
        from_id: &str,
        to_type: &str,
        to_id: &str,
    ) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/journeys/search")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "from": {"type": from_type, "id": from_id},
                    "to": {"type": to_type, "id": to_id},
                    "datetime": "2026-07-06T07:05:00+02:00",
                    "mode": "depart_at",
                    "transport_modes": ["train"],
                    "max_transfers": 4,
                    "walking_speed": "normal",
                    "prefer_reliable_transfers": true,
                    "offline_compatible": false
                })
                .to_string(),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn journey_from_city_to_stop_uses_concrete_stop() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(journey_search_request(
                "city",
                "city:CZ:554782",
                "stop",
                "stop-brno-hl-n",
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["journeys"][0]["legs"][0]["from_stop_id"],
            "stop-praha-hl-n"
        );
    }

    #[tokio::test]
    async fn journey_search_accepts_camel_case_intermediate_stop_request() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/journeys/search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "from": {"type": "stop", "id": "stop-praha-hl-n"},
                            "to": {"type": "stop", "id": "stop-brno-hl-n"},
                            "datetime": "2026-07-06T07:05:00+02:00",
                            "mode": "depart_at",
                            "transport_modes": ["train"],
                            "max_transfers": 4,
                            "walking_speed": "normal",
                            "prefer_reliable_transfers": true,
                            "offline_compatible": false,
                            "includeIntermediateStops": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["journeys"][0]["legs"][0]["stop_calls"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn journey_from_stop_to_city_uses_concrete_stop() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(journey_search_request(
                "stop",
                "stop-praha-hl-n",
                "city",
                "city:CZ:582786",
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["journeys"][0]["legs"][0]["to_stop_id"],
            "stop-brno-hl-n"
        );
    }

    #[tokio::test]
    async fn journey_between_cities_uses_concrete_stops() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(journey_search_request(
                "city",
                "city:CZ:554782",
                "city",
                "city:CZ:582786",
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["journeys"][0]["legs"][0]["from_stop_id"],
            "stop-praha-hl-n"
        );
        assert_eq!(
            payload["journeys"][0]["legs"][0]["to_stop_id"],
            "stop-brno-hl-n"
        );
    }

    #[tokio::test]
    async fn invalid_city_id_returns_readable_bad_request() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(journey_search_request(
                "city",
                "city:CZ:does-not-exist",
                "stop",
                "stop-brno-hl-n",
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["code"], "invalid_city_id");
        assert!(payload["message"].as_str().unwrap().contains("unknown"));
    }

    #[test]
    fn railway_platform_ids_resolve_to_the_same_station_base() {
        let station = "ggu_czptt_gtfs_latest:-SR70S-CZ-35442";
        assert_eq!(railway_station_stop_base(station).as_deref(), Some(station));
        assert_eq!(
            railway_station_stop_base("ggu_czptt_gtfs_latest:-SR70ST-CZ-35442").as_deref(),
            Some(station)
        );
        assert_eq!(
            railway_station_stop_base("ggu_czptt_gtfs_latest:-SR70S-CZ-35442-4b").as_deref(),
            Some(station)
        );
        assert_eq!(
            canonical_journey_stop_id("ggu_czptt_gtfs_latest:-SR70S-CZ-35442-2"),
            station
        );
    }

    #[test]
    fn implicit_station_transfer_signature_groups_platform_like_stops() {
        assert_eq!(
            implicit_station_transfer_signature(
                "feed:-SR70S-CZ-33722-2",
                "Olomouc hl.n.",
                Some("Olomouc"),
                Some(49.592),
                Some(17.277),
                None,
                None,
                None,
                &["train".to_string()],
            ),
            implicit_station_transfer_signature(
                "other-feed-platform",
                "Olomouc hl.n.",
                Some("Olomouc"),
                Some(49.593),
                Some(17.278),
                None,
                None,
                Some("5"),
                &["train".to_string()],
            )
        );
        assert_eq!(
            implicit_station_transfer_signature(
                "ordinary-bus-stop",
                "Olomouc hl.n.",
                Some("Olomouc"),
                Some(49.593),
                Some(17.278),
                None,
                None,
                None,
                &["bus".to_string()],
            ),
            None
        );
    }

    #[test]
    fn implicit_station_transfer_signature_prefers_source_station_relationship() {
        let first = implicit_station_transfer_signature(
            "pid_gtfs:U142Z101P",
            "Kačerov",
            Some("Praha"),
            Some(50.041),
            Some(14.460),
            None,
            Some("pid_gtfs:U142S1"),
            Some("1"),
            &["bus".to_string()],
        );
        let second = implicit_station_transfer_signature(
            "pid_gtfs:U142Z301",
            "Kačerov",
            Some("Praha"),
            Some(50.042),
            Some(14.461),
            None,
            Some("pid_gtfs:U142S1"),
            Some("metro"),
            &["metro".to_string()],
        );
        assert_eq!(first.as_deref(), Some("source:pid_gtfs:U142"));
        assert_eq!(first, second);
    }

    #[test]
    fn station_interchange_connects_different_lines_of_the_same_mode() {
        let metro = vec!["metro".to_string()];
        let line_a = HashSet::from(["pid_gtfs:L991".to_string()]);
        let line_b = HashSet::from(["pid_gtfs:L992".to_string()]);

        assert!(station_interchange_needs_connector(
            &metro, &line_a, &metro, &line_b
        ));
        assert!(!station_interchange_needs_connector(
            &metro, &line_a, &metro, &line_a
        ));
    }

    #[test]
    fn pid_coordinated_stop_actions_remain_routable() {
        assert!(gtfs_stop_action_allowed(None));
        assert!(gtfs_stop_action_allowed(Some(0)));
        assert!(!gtfs_stop_action_allowed(Some(1)));
        assert!(gtfs_stop_action_allowed(Some(2)));
        assert!(gtfs_stop_action_allowed(Some(3)));
        assert!(!gtfs_stop_action_allowed(Some(9)));
    }

    #[test]
    fn railway_station_base_rejects_unrelated_or_malformed_ids() {
        assert_eq!(railway_station_stop_base("ordinary-stop-2"), None);
        assert_eq!(
            railway_station_stop_base("ggu_czptt_gtfs_latest:-SR70S-CZ-station-2"),
            None
        );
        assert_eq!(
            canonical_journey_stop_id("ordinary-stop-2"),
            "ordinary-stop-2"
        );
    }

    #[test]
    fn stop_search_accepts_main_station_suffix_missing_from_rail_feed_name() {
        let mut station = fixture_stop(
            "ggu_czptt_gtfs_latest:-SR70ST-CZ-35442",
            "Vsetin",
            49.335427,
            17.99336,
            TransportMode::Train,
        );
        station.location_type = StopLocationType::Station;
        station.modes.clear();
        let ordinary_stop = fixture_stop(
            "ordinary-vsetin",
            "Vsetin",
            49.335427,
            17.99336,
            TransportMode::Bus,
        );

        assert!(stop_search_score(&station, "vsetin hl n").is_some());
        assert!(
            ranked_stop_suggestions([&station].into_iter(), "vsetin hlavni nadrazi", 1)
                .first()
                .is_some_and(|suggestion| suggestion.id == station.id)
        );
        assert!(stop_search_score(&ordinary_stop, "vsetin hl n").is_none());
    }

    #[test]
    fn short_main_station_query_keeps_city_qualified_railway_match() {
        let railway = fixture_stop(
            "pid_gtfs:U142S2",
            "Praha hlavní nádraží",
            50.083096,
            14.436194,
            TransportMode::Train,
        );
        let tram = fixture_stop(
            "pid_gtfs:U142Z2P",
            "Hlavní nádraží",
            50.085292,
            14.435092,
            TransportMode::Tram,
        );

        let suggestions =
            ranked_stop_suggestions([&railway, &tram].into_iter(), "hlavni nadrazi", 10);

        assert_eq!(suggestions.len(), 2);
        assert!(suggestions.iter().any(|stop| stop.id == railway.id));
        assert!(suggestions.iter().any(|stop| stop.id == tram.id));
    }

    #[test]
    fn selected_stops_and_coordinates_use_nearby_access_but_cities_do_not() {
        let point = |point_type: &str| JourneyPoint {
            point_type: point_type.to_string(),
            id: Some("test".to_string()),
            lat: Some(50.0),
            lon: Some(14.0),
        };

        assert!(journey_point_uses_nearby_access(&point("stop")));
        assert!(journey_point_uses_nearby_access(&point("coordinate")));
        assert!(!journey_point_uses_nearby_access(&point("city")));
    }

    #[test]
    fn endpoint_access_does_not_short_circuit_transit_with_an_unreturned_direct_walk() {
        let transfer = |from: &str, to: &str| Transfer {
            from_stop_id: from.to_string(),
            to_stop_id: to.to_string(),
            min_transfer_seconds: 300,
            distance_meters: Some(400),
            walking_geometry: None,
            confidence: CoordinateConfidence::High,
            accessibility_level: None,
            source: "test_endpoint_access".to_string(),
        };
        let mut transfers = vec![
            transfer("railway", "tram"),
            transfer("railway", "destination"),
            transfer("bus", "destination"),
        ];

        remove_direct_endpoint_walks(
            &mut transfers,
            &["railway".to_string()],
            &["destination".to_string()],
        );

        assert_eq!(transfers.len(), 2);
        assert!(
            transfers
                .iter()
                .any(|transfer| transfer.to_stop_id == "tram")
        );
        assert!(
            transfers
                .iter()
                .any(|transfer| transfer.from_stop_id == "bus")
        );
    }

    #[test]
    fn station_prefix_escapes_sql_like_wildcards() {
        assert_eq!(
            escaped_like_prefix("ggu_czptt_100%-SR70S-CZ-35442"),
            "ggu\\_czptt\\_100\\%-SR70S-CZ-35442%"
        );
    }

    #[tokio::test]
    async fn journey_search_resolves_stop_names_before_routing() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/journeys/search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "from": {"type": "stop", "id": "Praha hl. n."},
                            "to": {"type": "stop", "id": "Brno hl. n."},
                            "datetime": "2026-07-06T07:05:00+02:00",
                            "mode": "depart_at",
                            "transport_modes": ["train"],
                            "max_transfers": 4,
                            "walking_speed": "normal",
                            "prefer_reliable_transfers": true,
                            "offline_compatible": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["journeys"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn journey_search_falls_back_to_fixture_service_day_when_requested_time_is_too_late() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/journeys/search")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "from": {"type": "stop", "id": "stop-praha-hl-n"},
                            "to": {"type": "stop", "id": "stop-brno-hl-n"},
                            "datetime": "2026-07-06T21:05:00+02:00",
                            "mode": "depart_at",
                            "transport_modes": ["train"],
                            "max_transfers": 4,
                            "walking_speed": "normal",
                            "prefer_reliable_transfers": true,
                            "offline_compatible": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["journeys"].as_array().unwrap().len(), 1);
        assert!(
            payload["warnings"]
                .as_array()
                .unwrap()
                .iter()
                .any(|warning| {
                    warning
                        .as_str()
                        .is_some_and(|value| value.contains("earliest service-day journeys"))
                })
        );
    }

    fn test_journey(
        id: &str,
        transfer_count: u32,
        departure_time: u32,
        arrival_time: u32,
    ) -> Journey {
        let legs = if transfer_count == 0 {
            vec![JourneyLeg {
                from_stop_id: "praha".to_string(),
                to_stop_id: "vsetin".to_string(),
                route_id: Some(format!("route-{id}")),
                trip_id: Some(format!("trip-{id}")),
                departure_time,
                arrival_time,
                mode: TransportMode::Train,
                warnings: Vec::new(),
                geometry: None,
            }]
        } else {
            vec![
                JourneyLeg {
                    from_stop_id: "praha".to_string(),
                    to_stop_id: format!("transfer-{id}"),
                    route_id: Some(format!("feeder-route-{id}")),
                    trip_id: Some(format!("feeder-trip-{id}")),
                    departure_time,
                    arrival_time: departure_time + 3600,
                    mode: TransportMode::Train,
                    warnings: Vec::new(),
                    geometry: None,
                },
                JourneyLeg {
                    from_stop_id: format!("transfer-{id}"),
                    to_stop_id: "vsetin".to_string(),
                    route_id: Some(format!("route-{id}")),
                    trip_id: Some(format!("trip-{id}")),
                    departure_time: departure_time + 3900,
                    arrival_time,
                    mode: TransportMode::Train,
                    warnings: Vec::new(),
                    geometry: None,
                },
            ]
        };

        Journey {
            id: id.to_string(),
            legs,
            departure_time,
            arrival_time,
            duration_seconds: arrival_time.saturating_sub(departure_time),
            transfer_count,
            walking_distance_meters: 0,
            realtime_status: RealtimeStatus::Unavailable,
            risk_score: 0.0,
            labels: Vec::new(),
        }
    }

    #[test]
    fn calendar_verified_journeys_replace_legacy_candidates_when_available() {
        let verified = test_journey("verified", 0, 1_000, 2_000);
        let legacy = test_journey("legacy", 0, 900, 1_500);
        let legacy_trip_ids = HashSet::from(["trip-legacy".to_string()]);

        let (journeys, verified_count, legacy_count) =
            prefer_calendar_verified_journeys(vec![legacy, verified], &legacy_trip_ids);

        assert_eq!(verified_count, 1);
        assert_eq!(legacy_count, 1);
        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].id, "verified");
    }

    #[test]
    fn legacy_journeys_remain_as_last_resort_without_verified_candidates() {
        let legacy = test_journey("legacy", 0, 900, 1_500);
        let legacy_trip_ids = HashSet::from(["trip-legacy".to_string()]);

        let (journeys, verified_count, legacy_count) =
            prefer_calendar_verified_journeys(vec![legacy], &legacy_trip_ids);

        assert_eq!(verified_count, 0);
        assert_eq!(legacy_count, 1);
        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].id, "legacy");
    }

    #[test]
    fn ranked_journeys_prefer_earliest_arrival_over_earliest_departure() {
        let slow_direct = Journey {
            id: "old".to_string(),
            legs: vec![JourneyLeg {
                from_stop_id: "a".to_string(),
                to_stop_id: "c".to_string(),
                route_id: Some("slow".to_string()),
                trip_id: Some("slow-trip".to_string()),
                departure_time: 4 * 3600,
                arrival_time: 9 * 3600,
                mode: TransportMode::Train,
                warnings: Vec::new(),
                geometry: None,
            }],
            departure_time: 4 * 3600,
            arrival_time: 9 * 3600,
            duration_seconds: 5 * 3600,
            transfer_count: 0,
            walking_distance_meters: 0,
            realtime_status: RealtimeStatus::Unavailable,
            risk_score: 0.0,
            labels: vec!["nejrychlejsi".to_string()],
        };
        let faster_transfer = Journey {
            id: "old-2".to_string(),
            legs: vec![
                JourneyLeg {
                    from_stop_id: "a".to_string(),
                    to_stop_id: "b".to_string(),
                    route_id: Some("feeder".to_string()),
                    trip_id: Some("feeder-trip".to_string()),
                    departure_time: 5 * 3600,
                    arrival_time: 6 * 3600,
                    mode: TransportMode::Train,
                    warnings: Vec::new(),
                    geometry: None,
                },
                JourneyLeg {
                    from_stop_id: "b".to_string(),
                    to_stop_id: "c".to_string(),
                    route_id: Some("fast".to_string()),
                    trip_id: Some("fast-trip".to_string()),
                    departure_time: 6 * 3600 + 10 * 60,
                    arrival_time: 8 * 3600,
                    mode: TransportMode::Train,
                    warnings: Vec::new(),
                    geometry: None,
                },
            ],
            departure_time: 5 * 3600,
            arrival_time: 8 * 3600,
            duration_seconds: 3 * 3600,
            transfer_count: 1,
            walking_distance_meters: 0,
            realtime_status: RealtimeStatus::Unavailable,
            risk_score: 0.0,
            labels: vec!["s prestupem".to_string()],
        };

        let ranked = ranked_journey_results(vec![slow_direct, faster_transfer]);

        assert_eq!(ranked[0].id, "journey-1");
        assert_eq!(ranked[0].arrival_time, 8 * 3600);
        assert_eq!(ranked[0].transfer_count, 1);
        assert!(ranked[0].labels.iter().any(|label| label == "nejrychlejsi"));
        assert!(!ranked[1].labels.iter().any(|label| label == "nejrychlejsi"));
    }

    #[test]
    fn ranked_journeys_remove_strictly_worse_connections() {
        let useful = test_journey("useful", 0, 8 * 3600, 9 * 3600);
        let useless = test_journey("useless", 1, 7 * 3600, 10 * 3600);

        let ranked = ranked_journey_results(vec![useless, useful]);

        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].departure_time, 8 * 3600);
        assert_eq!(ranked[0].arrival_time, 9 * 3600);
        assert_eq!(ranked[0].transfer_count, 0);
    }

    #[test]
    fn ranked_journeys_keep_direct_departures_when_faster_routes_need_transfers() {
        let mut journeys = (0..4)
            .map(|index| {
                let mut direct = test_journey(
                    &format!("direct-6-{index}"),
                    0,
                    10 * 3600 + index * 8 * 60,
                    10 * 3600 + 16 * 60 + index * 8 * 60,
                );
                direct.legs[0].route_id = Some("tram-6".to_string());
                direct
            })
            .collect::<Vec<_>>();
        journeys.extend((0..4).map(|index| {
            test_journey(
                &format!("faster-transfer-{index}"),
                1,
                10 * 3600 + 60 + index * 8 * 60,
                10 * 3600 + 15 * 60 + index * 8 * 60,
            )
        }));
        let configuration = RoutingAlgorithmConfig {
            preserve_simplest: false,
            preserve_each_transfer_count: false,
            preserve_carrier_diversity: false,
            ..RoutingAlgorithmConfig::default()
        };

        let ranked =
            ranked_journey_results_with_carriers(journeys, &HashMap::new(), &configuration);

        assert_eq!(ranked.len(), 8);
        assert_eq!(
            ranked
                .iter()
                .filter(|journey| journey.transfer_count == 0)
                .count(),
            4
        );
    }

    #[test]
    fn ranked_journeys_preserve_direct_tram_against_faster_walked_tram() {
        let mut journeys = (0..25)
            .map(|index| {
                let mut walked = test_journey(
                    &format!("walked-17-{index}"),
                    0,
                    10 * 3600 + index * 60,
                    10 * 3600 + 10 * 60 + index * 60,
                );
                walked.legs[0].route_id = Some("tram-17".to_string());
                walked.walking_distance_meters = 700;
                walked
            })
            .collect::<Vec<_>>();
        let mut direct = test_journey("direct-6", 0, 10 * 3600 + 60, 10 * 3600 + 17 * 60);
        direct.legs[0].route_id = Some("tram-6".to_string());
        journeys.push(direct);

        let ranked = ranked_journey_results(journeys);
        let direct = ranked
            .iter()
            .find(|journey| journey.legs[0].route_id.as_deref() == Some("tram-6"))
            .expect("a direct tram must survive a frontier of faster walking alternatives");

        assert!(direct.labels.iter().any(|label| label == "nejjednodussi"));
        assert_eq!(ranked[0].walking_distance_meters, 700);
        assert!(ranked[0].labels.iter().any(|label| label == "nejrychlejsi"));
    }

    #[test]
    fn ranked_journeys_one_result_limit_keeps_primary_rank() {
        let faster = test_journey("faster-transfer", 1, 10 * 3600, 11 * 3600);
        let simpler = test_journey("slower-direct", 0, 10 * 3600, 12 * 3600);
        let configuration = RoutingAlgorithmConfig {
            max_results: 1,
            ..RoutingAlgorithmConfig::default()
        };

        let ranked = ranked_journey_results_with_carriers(
            vec![simpler, faster],
            &HashMap::new(),
            &configuration,
        );

        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].arrival_time, 11 * 3600);
        assert_eq!(ranked[0].transfer_count, 1);
    }

    #[test]
    fn ranked_journeys_bound_route_reservations_to_preserve_departure_range() {
        let journeys = (0..20)
            .map(|index| {
                test_journey(
                    &format!("route-{index}"),
                    0,
                    10 * 3600 + index * 60,
                    10 * 3600 + 10 * 60 + index * 60,
                )
            })
            .collect::<Vec<_>>();
        let configuration = RoutingAlgorithmConfig {
            max_results: 6,
            preserve_carrier_diversity: false,
            ..RoutingAlgorithmConfig::default()
        };

        let ranked =
            ranked_journey_results_with_carriers(journeys, &HashMap::new(), &configuration);

        assert_eq!(ranked.len(), 6);
        assert_eq!(ranked[0].departure_time, 10 * 3600);
        assert!(
            ranked
                .iter()
                .any(|journey| journey.departure_time == 10 * 3600 + 19 * 60)
        );
    }

    #[test]
    fn journey_dominance_requires_all_travel_criteria_to_improve() {
        let direct = test_journey("direct", 0, 10 * 3600, 11 * 3600);
        let transfer = test_journey("transfer", 1, 10 * 3600, 11 * 3600);
        let mut walked = direct.clone();
        walked.walking_distance_meters = 300;
        let identical = direct.clone();

        assert!(time_dominates(&direct, &transfer));
        assert!(!time_dominates(&transfer, &direct));
        assert!(time_dominates(&direct, &walked));
        assert!(!time_dominates(&walked, &direct));
        assert!(!time_dominates(&direct, &identical));
    }

    #[test]
    fn journey_dominance_honors_same_carrier_configuration() {
        let faster = test_journey("faster", 0, 10 * 3600, 11 * 3600);
        let slower = test_journey("slower", 0, 10 * 3600, 12 * 3600);
        let configuration = RoutingAlgorithmConfig {
            dominate_only_same_carrier: true,
            preserve_simplest: false,
            preserve_each_transfer_count: false,
            preserve_carrier_diversity: false,
            ..RoutingAlgorithmConfig::default()
        };
        let mut carriers = HashMap::from([
            ("route-faster".to_string(), "operator-a".to_string()),
            ("route-slower".to_string(), "operator-b".to_string()),
        ]);

        assert_eq!(
            remove_dominated_journeys(
                vec![faster.clone(), slower.clone()],
                &carriers,
                &configuration
            )
            .len(),
            2
        );
        carriers.insert("route-slower".to_string(), "operator-a".to_string());
        assert_eq!(
            remove_dominated_journeys(
                vec![faster.clone(), slower.clone()],
                &carriers,
                &configuration
            )
            .len(),
            1
        );
        assert_eq!(
            remove_dominated_journeys(vec![faster, slower], &HashMap::new(), &configuration).len(),
            2
        );
    }

    #[test]
    fn ranked_journeys_represent_the_full_departure_arrival_frontier() {
        let journeys = (0..5)
            .map(|index| {
                test_journey(
                    &format!("frontier-{index}"),
                    0,
                    8 * 3600 + index * 10 * 60,
                    9 * 3600 + index * 10 * 60,
                )
            })
            .collect::<Vec<_>>();
        let configuration = RoutingAlgorithmConfig {
            max_results: 3,
            preserve_simplest: false,
            preserve_each_transfer_count: false,
            preserve_carrier_diversity: false,
            ..RoutingAlgorithmConfig::default()
        };

        let ranked =
            ranked_journey_results_with_carriers(journeys, &HashMap::new(), &configuration);
        let departure_times = ranked
            .iter()
            .map(|journey| journey.departure_time)
            .collect::<Vec<_>>();

        assert_eq!(
            departure_times,
            vec![8 * 3600, 8 * 3600 + 20 * 60, 8 * 3600 + 40 * 60]
        );
    }

    #[test]
    fn carrier_diversity_keeps_one_potential_fare_exception() {
        let mut journeys = (0..6)
            .map(|index| {
                test_journey(
                    &format!("carrier-a-{index}"),
                    1,
                    5 * 3600 + index * 60,
                    8 * 3600 + index * 60,
                )
            })
            .collect::<Vec<_>>();
        journeys.push(test_journey("carrier-b", 1, 5 * 3600, 8 * 3600 + 30 * 60));

        let mut carrier_keys = HashMap::new();
        for index in 0..6 {
            carrier_keys.insert(
                format!("feeder-route-carrier-a-{index}"),
                "carrier-a".to_string(),
            );
            carrier_keys.insert(format!("route-carrier-a-{index}"), "carrier-a".to_string());
        }
        carrier_keys.insert(
            "feeder-route-carrier-b".to_string(),
            "carrier-b".to_string(),
        );
        carrier_keys.insert("route-carrier-b".to_string(), "carrier-b".to_string());

        let ranked = ranked_journey_results_with_carriers(
            journeys,
            &carrier_keys,
            &RoutingAlgorithmConfig::default(),
        );

        assert_eq!(ranked.len(), 7);
        assert!(ranked.iter().any(|journey| {
            journey
                .legs
                .iter()
                .any(|leg| leg.route_id.as_deref() == Some("route-carrier-b"))
        }));
    }

    #[test]
    fn route_diversity_does_not_restore_objectively_dominated_detours() {
        let mut journeys = (0..5)
            .map(|index| {
                let mut journey = test_journey(
                    &format!("direct-{index}"),
                    0,
                    8 * 3600 + 55 * 60 + index * 60,
                    9 * 3600 + index * 60,
                );
                journey.legs[0].route_id = Some("route-direct".to_string());
                journey
            })
            .collect::<Vec<_>>();
        let mut tram_transfer = test_journey("tram-15-8", 1, 8 * 3600 + 54 * 60, 9 * 3600 + 5 * 60);
        tram_transfer.legs[0].route_id = Some("route-15".to_string());
        tram_transfer.legs[1].route_id = Some("route-8".to_string());
        let mut metro_transfer =
            test_journey("metro-c-tram-34", 1, 8 * 3600 + 56 * 60, 9 * 3600 + 6 * 60);
        metro_transfer.legs[0].route_id = Some("route-c".to_string());
        metro_transfer.legs[1].route_id = Some("route-34".to_string());
        journeys.extend([tram_transfer, metro_transfer]);

        let ranked = ranked_journey_results(journeys);
        let signatures = ranked
            .iter()
            .map(journey_route_signature)
            .collect::<HashSet<_>>();

        assert_eq!(ranked.len(), 5);
        assert!(!signatures.contains("route:route-15|route:route-8"));
        assert!(!signatures.contains("route:route-c|route:route-34"));
    }

    #[test]
    fn routing_transfer_penalty_is_tunable_without_mislabeling_fastest() {
        let transfer = test_journey("transfer", 1, 5 * 3600, 8 * 3600);
        let direct = test_journey("direct", 0, 5 * 3600, 9 * 3600);
        let configuration = RoutingAlgorithmConfig {
            transfer_penalty_seconds: 2 * 3600,
            ..RoutingAlgorithmConfig::default()
        };

        let ranked = ranked_journey_results_with_carriers(
            vec![transfer, direct],
            &HashMap::new(),
            &configuration,
        );

        assert_eq!(ranked[0].transfer_count, 0);
        assert!(ranked[0].labels.iter().any(|label| label == "doporuceno"));
        let fastest = ranked
            .iter()
            .find(|journey| journey.arrival_time == 8 * 3600)
            .unwrap();
        assert!(fastest.labels.iter().any(|label| label == "nejrychlejsi"));
    }

    #[test]
    fn routing_configuration_rejects_unsafe_combinations() {
        let invalid_window = RoutingAlgorithmConfig {
            min_transfer_seconds: 1800,
            max_transfer_wait_seconds: 900,
            ..RoutingAlgorithmConfig::default()
        };
        assert!(invalid_window.validate().is_err());

        let no_time_objective = RoutingAlgorithmConfig {
            arrival_time_weight: 0.0,
            duration_weight: 0.0,
            ..RoutingAlgorithmConfig::default()
        };
        assert!(no_time_objective.validate().is_err());
        assert!(RoutingAlgorithmConfig::default().validate().is_ok());
    }

    #[test]
    fn adaptive_raptor_expands_only_for_thin_candidate_sets() {
        let configuration = RoutingAlgorithmConfig::default();

        assert!(should_expand_raptor_range(0, &configuration));
        assert!(should_expand_raptor_range(2, &configuration));
        assert!(!should_expand_raptor_range(3, &configuration));
        assert!(!should_search_next_service_day_for_candidates(
            4,
            &configuration
        ));
    }

    #[test]
    fn transfer_search_warnings_distinguish_timeout_from_database_failure() {
        let mut warnings = Vec::new();

        append_transfer_search_warning(&mut warnings, TransferSearchStatus::TimedOut, false, 30);
        append_transfer_search_warning(&mut warnings, TransferSearchStatus::Failed, true, 30);
        append_transfer_search_warning(&mut warnings, TransferSearchStatus::Complete, false, 30);

        assert_eq!(
            warnings,
            vec![
                "transfer search exceeded the configured 30s timeout; direct journeys are still included",
                "next service-day transfer search failed; direct journeys are still included",
            ]
        );
    }

    #[test]
    fn endpoint_access_cache_key_is_stable_for_same_stop_set() {
        let revision = RoutingDataRevision {
            latest_import: None,
            token: "revision".to_string(),
        };
        let left = endpoint_access_cache_key(
            &revision,
            &["b".to_string(), "a".to_string(), "a".to_string()],
            true,
            1.25,
        );
        let right =
            endpoint_access_cache_key(&revision, &["a".to_string(), "b".to_string()], true, 1.25);

        assert_eq!(left, right);
    }

    #[test]
    fn next_service_day_journeys_shift_early_departures_after_evening_search() {
        let next_morning = test_journey("next-morning", 0, 4 * 3600, 8 * 3600);
        let same_evening = test_journey("same-evening", 0, 20 * 3600, 23 * 3600);

        let journeys =
            next_service_day_journey_results(vec![next_morning, same_evening], 19 * 3600 + 24 * 60);

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].departure_time, SERVICE_DAY_SECONDS + 4 * 3600);
        assert_eq!(journeys[0].arrival_time, SERVICE_DAY_SECONDS + 8 * 3600);
        assert_eq!(journeys[0].duration_seconds, 4 * 3600);
        assert!(journeys[0].labels.iter().any(|label| label == "dalsi den"));
        assert_eq!(
            journeys[0].legs[0].departure_time,
            SERVICE_DAY_SECONDS + 4 * 3600
        );
    }

    #[test]
    fn next_service_day_query_only_runs_for_evening_departures() {
        let threshold = NEXT_SERVICE_DAY_SEARCH_FROM_SECONDS;
        assert!(!should_search_next_service_day(
            17 * 3600 + 59 * 60,
            threshold
        ));
        assert!(should_search_next_service_day(18 * 3600, threshold));
        assert!(should_search_next_service_day(
            19 * 3600 + 24 * 60,
            threshold
        ));
    }

    #[test]
    fn journey_service_date_comes_from_requested_local_date() {
        assert_eq!(
            parse_journey_service_date("2026-07-04T00:15:00+02:00")
                .unwrap()
                .to_string(),
            "2026-07-04"
        );
    }

    #[test]
    fn utc_journey_datetime_is_converted_to_prague_local_service_time() {
        assert_eq!(
            parse_journey_departure_seconds("2026-07-19T15:55:23Z").unwrap(),
            17 * 3600 + 55 * 60 + 23
        );
        assert_eq!(
            parse_journey_departure_seconds("2026-01-19T15:55:23Z").unwrap(),
            16 * 3600 + 55 * 60 + 23
        );
        assert_eq!(
            parse_journey_service_date("2026-07-18T22:15:00Z").unwrap(),
            chrono::NaiveDate::from_ymd_opt(2026, 7, 19).unwrap()
        );
    }

    #[test]
    fn default_departure_time_uses_prague_local_time() {
        let summer = DateTime::parse_from_rfc3339("2026-07-22T10:15:30Z")
            .unwrap()
            .with_timezone(&Utc);
        let winter = DateTime::parse_from_rfc3339("2026-01-22T10:15:30Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(prague_time_seconds_at(summer), 12 * 3600 + 15 * 60 + 30);
        assert_eq!(prague_time_seconds_at(winter), 11 * 3600 + 15 * 60 + 30);
    }

    #[test]
    fn already_departed_candidates_are_removed_at_the_api_boundary() {
        let mut journeys = vec![
            test_journey("departed", 0, 10_000, 11_000),
            test_journey("current", 0, 12_000, 13_000),
        ];

        assert_eq!(discard_departed_journeys(&mut journeys, 12_000), 1);
        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].id, "current");
    }

    #[test]
    fn adaptive_candidate_count_ignores_duplicate_range_results() {
        let first = test_journey("first", 0, 10_000, 11_000);
        let mut duplicate = first.clone();
        duplicate.id = "duplicate-id".to_string();
        duplicate.departure_time += 600;
        duplicate.arrival_time += 600;
        duplicate.legs[0].departure_time += 600;
        duplicate.legs[0].arrival_time += 600;
        let different = test_journey("different", 0, 10_200, 11_200);
        let unreasonable = test_journey("unreasonable", 0, 20_000, 21_000);

        assert_eq!(
            distinct_raptor_candidate_count(&[first, duplicate, different, unreasonable]),
            2
        );
    }

    #[tokio::test]
    async fn adaptive_search_keeps_exact_direct_route_when_walked_route_arrives_first() {
        let time = |minutes: u32| 10 * 3600 + minutes * 60;
        let trip =
            |id: &str, route: &str, mode, from: &str, to: &str, departure, arrival| RaptorTrip {
                trip_id: id.to_string(),
                route_id: route.to_string(),
                mode,
                service_verified: true,
                stop_times: vec![
                    RaptorStopTime {
                        stop_id: from.to_string(),
                        arrival_time: departure,
                        departure_time: departure,
                        pickup_allowed: true,
                        drop_off_allowed: true,
                    },
                    RaptorStopTime {
                        stop_id: to.to_string(),
                        arrival_time: arrival,
                        departure_time: arrival,
                        pickup_allowed: true,
                        drop_off_allowed: true,
                    },
                ],
            };
        let timetable = Arc::new(RaptorTimetable::new(
            vec![
                trip(
                    "direct-6",
                    "6",
                    TransportMode::Tram,
                    "karlovo",
                    "stross",
                    time(7),
                    time(23),
                ),
                trip(
                    "walked-17",
                    "17",
                    TransportMode::Tram,
                    "palackeho",
                    "stross",
                    time(6),
                    time(20),
                ),
                trip(
                    "metro-b",
                    "B",
                    TransportMode::Metro,
                    "karlovo",
                    "transfer",
                    time(5),
                    time(8),
                ),
                trip(
                    "connecting-6",
                    "6",
                    TransportMode::Tram,
                    "transfer",
                    "stross",
                    time(10),
                    time(14),
                ),
            ],
            Vec::new(),
        ));
        let access = vec![Transfer {
            from_stop_id: "karlovo".to_string(),
            to_stop_id: "palackeho".to_string(),
            min_transfer_seconds: 280,
            distance_meters: Some(349),
            walking_geometry: None,
            confidence: CoordinateConfidence::Exact,
            accessibility_level: None,
            source: "verified-pedestrian-router".to_string(),
        }];
        let configuration = RoutingAlgorithmConfig {
            max_results: 3,
            max_range_departures: 1,
            range_search_window_seconds: 90 * 60,
            ..RoutingAlgorithmConfig::default()
        };
        let result = run_adaptive_raptor_searches(
            timetable,
            &["karlovo".to_string()],
            &["stross".to_string()],
            &access,
            time(0),
            4,
            60,
            0,
            &[TransportMode::Tram, TransportMode::Metro],
            false,
            &configuration,
            Arc::new(RaptorRealtimeData::default()),
        )
        .await
        .unwrap();
        let ranked =
            ranked_journey_results_with_carriers(result.journeys, &HashMap::new(), &configuration);
        assert_eq!(ranked[0].arrival_time, time(14));
        assert!(
            ranked.iter().any(|journey| {
                journey.legs.len() == 1
                    && journey.legs[0].trip_id.as_deref() == Some("direct-6")
                    && journey.transfer_count == 0
                    && journey.walking_distance_meters == 0
            }),
            "a direct tram must remain available alongside the faster metro interchange and walked tram"
        );
    }

    #[test]
    fn geometry_shortlist_keeps_valid_fallback_until_dominator_is_validated() {
        let invalid = test_journey("invalid-geometry", 0, 8 * 3600 + 60, 9 * 3600);
        let valid = test_journey("valid-geometry", 0, 8 * 3600, 9 * 3600 + 60);
        let configuration = RoutingAlgorithmConfig {
            max_results: 1,
            ..RoutingAlgorithmConfig::default()
        };
        let mut shortlist = ranked_journey_results_with_carriers(
            vec![invalid, valid],
            &HashMap::new(),
            &geometry_preselection_config(&configuration),
        );
        assert_eq!(shortlist.len(), 2);
        // Simulate the geometry validator rejecting the faster timetable candidate.
        shortlist
            .retain(|journey| journey.legs[0].trip_id.as_deref() != Some("trip-invalid-geometry"));
        let ranked =
            ranked_journey_results_with_carriers(shortlist, &HashMap::new(), &configuration);
        assert_eq!(ranked.len(), 1);
        assert_eq!(
            ranked[0].legs[0].trip_id.as_deref(),
            Some("trip-valid-geometry")
        );
    }

    #[tokio::test]
    #[ignore = "explicit real-data regression; requires CESTA_ROUTING_REGRESSION_SNAPSHOT"]
    async fn real_pid_snapshot_keeps_direct_trams_in_multimode_search() {
        let path = std::env::var("CESTA_ROUTING_REGRESSION_SNAPSHOT")
            .expect("set CESTA_ROUTING_REGRESSION_SNAPSHOT to the documented 2026-10-05 snapshot");
        let snapshot: RaptorTimetableSnapshot =
            serde_json::from_reader(std::io::BufReader::new(std::fs::File::open(path).unwrap()))
                .unwrap();
        assert_eq!(
            snapshot.service_date,
            chrono::NaiveDate::from_ymd_opt(2026, 10, 5).unwrap()
        );
        let timetable = Arc::new(snapshot.timetable);
        let karlovo = [
            "U237S1",
            "U237Z101P",
            "U237Z102P",
            "U237Z10P",
            "U237Z1P",
            "U237Z2P",
            "U237Z3P",
            "U237Z9P",
        ]
        .map(|id| format!("pid_gtfs:{id}"));
        let stross =
            ["U717Z1P", "U717Z2P", "U717Z4P", "U717Z5P"].map(|id| format!("pid_gtfs:{id}"));
        let configuration = RoutingAlgorithmConfig::default();
        for (from, to) in [(&karlovo[..], &stross[..]), (&stross[..], &karlovo[..])] {
            for hour in [8, 10, 12, 14] {
                for modes in [Vec::new(), vec![TransportMode::Tram]] {
                    let departure = hour * 3600;
                    let request = RaptorRequest {
                        from_stop_ids: from.to_vec(),
                        to_stop_ids: to.to_vec(),
                        extra_transfers: Vec::new(),
                        departure_time: departure,
                        max_transfers: 0,
                        min_transfer_seconds: 300,
                        transfer_buffer_seconds: 0,
                        modes: modes.clone(),
                        allow_unverified_services: false,
                        realtime: Arc::new(RaptorRealtimeData::default()),
                    };
                    let expected = direct_journeys(&timetable, &request, 90 * 60, 20)
                        .into_iter()
                        .find(|journey| journey.legs[0].route_id.as_deref() == Some("pid_gtfs:L6"))
                        .expect("the reference timetable must contain a direct line 6");
                    for max_transfers in [0, 4] {
                        let started = std::time::Instant::now();
                        let result = run_adaptive_raptor_searches(
                            timetable.clone(),
                            from,
                            to,
                            &[],
                            departure,
                            max_transfers,
                            300,
                            0,
                            &modes,
                            false,
                            &configuration,
                            Arc::new(RaptorRealtimeData::default()),
                        )
                        .await
                        .unwrap();
                        let earliest_arrival = result
                            .journeys
                            .iter()
                            .map(|journey| journey.arrival_time)
                            .min()
                            .unwrap();
                        let ranked = ranked_journey_results_with_carriers(
                            result.journeys,
                            &HashMap::new(),
                            &configuration,
                        );
                        let direct_rank = ranked.iter().position(|journey| {
                            journey.legs.len() == 1 && journey.legs[0].trip_id == expected.legs[0].trip_id
                                && journey.legs[0].from_stop_id == expected.legs[0].from_stop_id
                                && journey.legs[0].to_stop_id == expected.legs[0].to_stop_id
                        }).expect("earliest exact direct tram must survive multimode routing and ranking");
                        assert_eq!(ranked[0].arrival_time, earliest_arrival);
                        assert!(
                            ranked
                                .iter()
                                .all(|journey| journey.departure_time >= departure
                                    && journey.transfer_count <= max_transfers)
                        );
                        assert!(ranked.len() <= configuration.max_results as usize);
                        assert_eq!(
                            ranked
                                .iter()
                                .map(journey_identity_key)
                                .collect::<HashSet<_>>()
                                .len(),
                            ranked.len()
                        );
                        eprintln!(
                            "real PID: {} -> {}, {hour}:00, {:?}, max_transfers={max_transfers}, direct6_rank={}, direct6_duration={}, elapsed={:?}",
                            from[0],
                            to[0],
                            modes,
                            direct_rank + 1,
                            expected.duration_seconds,
                            started.elapsed()
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    #[ignore = "explicit large-network performance regression"]
    async fn adaptive_large_timetable_search_stays_below_latency_budget() {
        let route_count = 5_000;
        let trips_per_route = 20;
        let mut trips = Vec::with_capacity(route_count * trips_per_route);
        for route in 0..route_count {
            for trip in 0..trips_per_route {
                let departure = 6 * 3600 + trip as u32 * 300 + (route % 60) as u32;
                let stop_time = |stop_id: String, arrival_time, departure_time| RaptorStopTime {
                    stop_id,
                    arrival_time,
                    departure_time,
                    pickup_allowed: true,
                    drop_off_allowed: true,
                };
                trips.push(RaptorTrip {
                    trip_id: format!("trip-{route}-{trip}"),
                    route_id: format!("route-{route}"),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("origin".to_string(), departure, departure),
                        stop_time(format!("middle-{route}"), departure + 600, departure + 620),
                        stop_time(
                            "destination".to_string(),
                            departure + 1_200,
                            departure + 1_200,
                        ),
                    ],
                });
            }
        }
        let timetable = Arc::new(RaptorTimetable::new(trips, Vec::new()));
        let started = std::time::Instant::now();
        let result = run_adaptive_raptor_searches(
            timetable,
            &["origin".to_string()],
            &["destination".to_string()],
            &[],
            7 * 3600,
            3,
            300,
            0,
            &[TransportMode::Train],
            false,
            &RoutingAlgorithmConfig::default(),
            Arc::new(RaptorRealtimeData::default()),
        )
        .await
        .unwrap();
        let elapsed = started.elapsed();
        eprintln!(
            "adaptive large timetable: {} trips, {} route patterns, {} probes, search {:?}",
            route_count * trips_per_route,
            route_count,
            result.departure_count,
            elapsed
        );

        assert!(!result.journeys.is_empty());
        assert!(
            elapsed < std::time::Duration::from_millis(1_500),
            "adaptive large timetable search took {elapsed:?}"
        );
    }

    #[test]
    fn ranked_journeys_keep_simplest_direct_route_when_transfers_are_faster() {
        let mut journeys = (0..6)
            .map(|index| {
                test_journey(
                    &format!("fast-transfer-{index}"),
                    1,
                    5 * 3600 + index * 60,
                    8 * 3600 + index * 60,
                )
            })
            .collect::<Vec<_>>();
        journeys.push(test_journey("direct", 0, 4 * 3600, 9 * 3600));

        let ranked = ranked_journey_results(journeys);

        assert_eq!(ranked.len(), 7);
        assert!(ranked.iter().any(|journey| journey.transfer_count == 0));
        let direct = ranked
            .iter()
            .find(|journey| journey.transfer_count == 0)
            .unwrap();
        assert!(direct.labels.iter().any(|label| label == "nejjednodussi"));
        assert_eq!(ranked[0].transfer_count, 1);
        assert!(ranked[0].labels.iter().any(|label| label == "nejrychlejsi"));
    }

    #[test]
    fn ranked_journeys_reserve_simplest_slot_when_frontier_fills_limit() {
        let mut journeys = (0..25)
            .map(|index| {
                test_journey(
                    &format!("frontier-transfer-{index}"),
                    1,
                    5 * 3600 + index * 60,
                    7 * 3600 + index * 60,
                )
            })
            .collect::<Vec<_>>();
        journeys.push(test_journey("direct", 0, 4 * 3600, 9 * 3600));

        let ranked = ranked_journey_results(journeys);

        assert_eq!(
            ranked.len(),
            RoutingAlgorithmConfig::default().max_results as usize
        );
        let direct = ranked
            .iter()
            .find(|journey| journey.transfer_count == 0)
            .expect("the configured simplest alternative must reserve a result slot");
        assert!(direct.labels.iter().any(|label| label == "nejjednodussi"));
        assert_eq!(ranked[0].transfer_count, 1);
    }

    #[test]
    fn ranked_journeys_dedupe_platform_variants_of_same_visible_connection() {
        let first = Journey {
            id: "first".to_string(),
            legs: vec![
                JourneyLeg {
                    from_stop_id: "ggu_czptt_gtfs_latest:-SR70S-CZ-35442-2".to_string(),
                    to_stop_id: "ggu_czptt_gtfs_latest:-SR70S-CZ-33722-7".to_string(),
                    route_id: Some("ggu_czptt_gtfs_latest:-CZTRAINR-2025-EC-122".to_string()),
                    trip_id: Some("first-ec-trip".to_string()),
                    departure_time: 16 * 3600 + 52 * 60,
                    arrival_time: 18 * 3600 + 60,
                    mode: TransportMode::Train,
                    warnings: Vec::new(),
                    geometry: None,
                },
                JourneyLeg {
                    from_stop_id: "ggu_czptt_gtfs_latest:-SR70S-CZ-33722-2".to_string(),
                    to_stop_id: "ggu_czptt_gtfs_latest:-SR70S-CZ-57076-13b".to_string(),
                    route_id: Some("ggu_czptt_gtfs_latest:-CZTRAINR-2025-SC-500".to_string()),
                    trip_id: Some("first-sc-trip".to_string()),
                    departure_time: 18 * 3600 + 11 * 60,
                    arrival_time: 20 * 3600 + 19 * 60,
                    mode: TransportMode::Train,
                    warnings: Vec::new(),
                    geometry: None,
                },
            ],
            departure_time: 16 * 3600 + 52 * 60,
            arrival_time: 20 * 3600 + 19 * 60,
            duration_seconds: 3 * 3600 + 27 * 60,
            transfer_count: 1,
            walking_distance_meters: 0,
            realtime_status: RealtimeStatus::Unavailable,
            risk_score: 0.0,
            labels: vec!["s prestupem".to_string()],
        };
        let mut duplicate = first.clone();
        duplicate.id = "duplicate".to_string();
        duplicate.legs[0].to_stop_id = "ggu_czptt_gtfs_latest:-SR70S-CZ-33722-4".to_string();
        duplicate.legs[1].from_stop_id = "ggu_czptt_gtfs_latest:-SR70S-CZ-33722-8".to_string();
        duplicate.legs[0].route_id =
            Some("ggu_czptt_gtfs_latest:-CZTRAINR-2026-EC-122".to_string());

        let ranked = ranked_journey_results(vec![first, duplicate]);

        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].id, "journey-1");
    }

    #[test]
    fn journey_legs_include_matching_realtime_delay_and_position() {
        let journey = Journey {
            id: "pid-journey".to_string(),
            legs: vec![JourneyLeg {
                from_stop_id: "pid_gtfs:U1Z1P".to_string(),
                to_stop_id: "pid_gtfs:U2Z1P".to_string(),
                route_id: Some("pid_gtfs:L991".to_string()),
                trip_id: Some("pid_gtfs:trip-1".to_string()),
                departure_time: 3600,
                arrival_time: 4200,
                mode: TransportMode::Metro,
                warnings: Vec::new(),
                geometry: None,
            }],
            departure_time: 3600,
            arrival_time: 4200,
            duration_seconds: 600,
            transfer_count: 0,
            walking_distance_meters: 0,
            realtime_status: RealtimeStatus::Unavailable,
            risk_score: 0.0,
            labels: Vec::new(),
        };
        let updates = vec![json!({
            "trip_id": "pid_gtfs:trip-1",
            "stop_id": "pid_gtfs:U1Z1P",
            "delay_seconds": 120,
            "estimated_departure": "2026-07-04T12:02:00Z",
            "estimated_arrival": null,
            "cancellation_status": null,
            "platform_change": null,
            "vehicle_id": "vehicle-1",
            "vehicle_position": {"lat": 50.08, "lon": 14.43},
            "bearing": 90.0,
            "source": "pid_gtfs_rt",
            "fetched_at": "2026-07-04T12:00:00Z",
            "valid_until": "2026-07-04T12:01:30Z",
            "confidence": "estimated"
        })];

        let enriched = journeys_with_realtime(&[journey], &updates);

        assert_eq!(enriched[0]["realtime_status"], "full");
        assert_eq!(enriched[0]["legs"][0]["realtime"]["delay_seconds"], 120);
        assert_eq!(
            enriched[0]["legs"][0]["realtime"]["vehicle_id"],
            "vehicle-1"
        );
    }

    #[test]
    fn journey_legs_include_human_readable_connection_metadata() {
        let mut journeys = vec![json!({
            "legs": [{
                "from_stop_id": "pid_gtfs:U1Z1P",
                "to_stop_id": "pid_gtfs:U2Z1P",
                "route_id": "pid_gtfs:L991",
                "trip_id": "pid_gtfs:trip-1",
                "departure_time": 3600,
                "mode": "bus"
            }]
        })];
        let related = json!({
            "stops": [
                {"id": "pid_gtfs:U1Z1P", "name": "Muzeum"},
                {"id": "pid_gtfs:U2Z1P", "name": "Nádraží Hostivař"}
            ],
            "routes": [{
                "id": "pid_gtfs:L991",
                "source_id": "L991",
                "short_name": "991",
                "long_name": "Praha – Nádraží Hostivař"
            }],
            "trips": [{"id": "pid_gtfs:trip-1", "headsign": "Nádraží Hostivař"}],
            "stop_times": [{
                "trip_id": "pid_gtfs:trip-1",
                "stop_id": "pid_gtfs:U1Z1P",
                "departure_time": 3600,
                "stop_headsign": "Centrum přes Jižní Město"
            }]
        });

        attach_journey_display_metadata(&mut journeys, &related);

        let leg = &journeys[0]["legs"][0];
        assert_eq!(leg["line"], "991");
        assert_eq!(leg["mode_name"], "Autobus");
        assert_eq!(leg["direction"], "Centrum přes Jižní Město");
        assert_eq!(leg["destination"], "Centrum přes Jižní Město");
        assert_eq!(
            leg["display_name"],
            "Autobus 991 směr Centrum přes Jižní Město"
        );
        assert_eq!(leg["from_stop_name"], "Muzeum");
        assert_eq!(leg["to_stop_name"], "Nádraží Hostivař");
    }

    #[test]
    fn journey_direction_falls_back_to_actual_trip_terminal_not_transfer_stop() {
        let mut journeys = vec![json!({
            "legs": [{
                "from_stop_id": "origin",
                "to_stop_id": "transfer",
                "route_id": "route",
                "trip_id": "trip",
                "departure_time": 3600,
                "mode": "train"
            }]
        })];
        let related = json!({
            "stops": [
                {"id": "origin", "name": "Výchozí"},
                {"id": "transfer", "name": "Přestupní"}
            ],
            "routes": [{"id": "route", "short_name": "R9"}],
            "trips": [{
                "id": "trip",
                "headsign": null,
                "terminal_stop_name": "Skutečná konečná"
            }],
            "stop_times": []
        });

        attach_journey_display_metadata(&mut journeys, &related);

        assert_eq!(journeys[0]["legs"][0]["direction"], "Skutečná konečná");
        assert_ne!(journeys[0]["legs"][0]["direction"], "Přestupní");
    }

    #[test]
    fn source_independent_deduplication_keeps_preferred_connection() {
        let mut official = test_journey("official", 0, 3_600, 7_200);
        official.legs[0].from_stop_id = "pid-origin".to_string();
        official.legs[0].to_stop_id = "pid-destination".to_string();
        let mut aggregate = test_journey("aggregate", 0, 3_600, 7_200);
        aggregate.legs[0].from_stop_id = "ggu-origin".to_string();
        aggregate.legs[0].to_stop_id = "ggu-destination".to_string();
        let stop_signatures = HashMap::from([
            ("pid-origin".to_string(), "origin".to_string()),
            ("ggu-origin".to_string(), "origin".to_string()),
            ("pid-destination".to_string(), "destination".to_string()),
            ("ggu-destination".to_string(), "destination".to_string()),
        ]);
        let route_priorities = HashMap::from([
            ("route-official".to_string(), 10),
            ("route-aggregate".to_string(), 30),
        ]);

        let deduplicated = dedupe_relevant_journeys(
            vec![aggregate, official],
            &stop_signatures,
            &route_priorities,
            &RoutingAlgorithmConfig::default(),
        );

        assert_eq!(deduplicated.len(), 1);
        assert_eq!(deduplicated[0].id, "official");
    }

    #[test]
    fn deduplication_ignores_probe_time_for_identical_leading_walks() {
        let journey = |id: &str, walk_departure: u32| Journey {
            id: id.to_string(),
            legs: vec![
                JourneyLeg {
                    from_stop_id: "selected-origin".to_string(),
                    to_stop_id: "nearby-stop".to_string(),
                    route_id: None,
                    trip_id: None,
                    departure_time: walk_departure,
                    arrival_time: walk_departure + 120,
                    mode: TransportMode::Unknown,
                    warnings: vec!["walking_transfer:150".to_string()],
                    geometry: None,
                },
                JourneyLeg {
                    from_stop_id: "nearby-stop".to_string(),
                    to_stop_id: "destination".to_string(),
                    route_id: Some("route".to_string()),
                    trip_id: Some("same-trip".to_string()),
                    departure_time: 4_000,
                    arrival_time: 5_000,
                    mode: TransportMode::Bus,
                    warnings: Vec::new(),
                    geometry: None,
                },
            ],
            departure_time: walk_departure,
            arrival_time: 5_000,
            duration_seconds: 5_000 - walk_departure,
            transfer_count: 0,
            walking_distance_meters: 150,
            realtime_status: RealtimeStatus::Unavailable,
            risk_score: 0.0,
            labels: Vec::new(),
        };
        let first_probe = journey("first-probe", 3_500);
        let second_probe = journey("second-probe", 3_600);

        let deduplicated = dedupe_relevant_journeys(
            vec![first_probe, second_probe],
            &HashMap::new(),
            &HashMap::new(),
            &RoutingAlgorithmConfig::default(),
        );

        assert_eq!(deduplicated.len(), 1);
    }

    #[test]
    fn relevance_filter_rejects_impossible_transfer() {
        let mut journey = test_journey("bad-transfer", 1, 3_600, 10_800);
        journey.legs[1].departure_time = journey.legs[0].arrival_time + 60;
        let signatures = HashMap::from([
            ("praha".to_string(), "praha".to_string()),
            ("vsetin".to_string(), "vsetin".to_string()),
            ("transfer-bad-transfer".to_string(), "transfer".to_string()),
        ]);

        assert!(!journey_is_relevant(
            &journey,
            &signatures,
            &RoutingAlgorithmConfig::default()
        ));

        journey.legs[1]
            .warnings
            .push("official_minimum_change_time".to_string());
        assert!(journey_is_relevant(
            &journey,
            &signatures,
            &RoutingAlgorithmConfig::default()
        ));
    }

    #[test]
    fn relevance_filter_allows_immediate_walking_interchange() {
        let journey = Journey {
            id: "walk-transfer".to_string(),
            legs: vec![
                JourneyLeg {
                    from_stop_id: "a".to_string(),
                    to_stop_id: "b".to_string(),
                    route_id: Some("route-a".to_string()),
                    trip_id: Some("trip-a".to_string()),
                    departure_time: 3_600,
                    arrival_time: 4_200,
                    mode: TransportMode::Train,
                    warnings: Vec::new(),
                    geometry: None,
                },
                JourneyLeg {
                    from_stop_id: "b".to_string(),
                    to_stop_id: "c".to_string(),
                    route_id: None,
                    trip_id: None,
                    departure_time: 4_200,
                    arrival_time: 4_320,
                    mode: TransportMode::Unknown,
                    warnings: vec!["walking_transfer:120".to_string()],
                    geometry: None,
                },
                JourneyLeg {
                    from_stop_id: "c".to_string(),
                    to_stop_id: "d".to_string(),
                    route_id: Some("route-b".to_string()),
                    trip_id: Some("trip-b".to_string()),
                    departure_time: 4_320,
                    arrival_time: 5_400,
                    mode: TransportMode::Train,
                    warnings: Vec::new(),
                    geometry: None,
                },
            ],
            departure_time: 3_600,
            arrival_time: 5_400,
            duration_seconds: 1_800,
            transfer_count: 1,
            walking_distance_meters: 120,
            realtime_status: RealtimeStatus::Unavailable,
            risk_score: 0.0,
            labels: Vec::new(),
        };
        let signatures = HashMap::from([
            ("a".to_string(), "a".to_string()),
            ("b".to_string(), "b".to_string()),
            ("c".to_string(), "c".to_string()),
            ("d".to_string(), "d".to_string()),
        ]);

        assert!(journey_is_relevant(
            &journey,
            &signatures,
            &RoutingAlgorithmConfig::default()
        ));
    }

    #[test]
    fn stop_calls_include_ordered_intermediate_stops_and_endpoints() {
        let journey = test_journey("calls", 0, 3_600, 5_400);
        let calls = [
            ("praha", "Praha", 1, 3_600),
            ("middle", "Intermediate", 2, 4_500),
            ("vsetin", "Vsetin", 3, 5_400),
        ]
        .into_iter()
        .map(
            |(stop_id, stop_name, stop_sequence, time)| JourneyStopCall {
                trip_id: "trip-calls".to_string(),
                stop_id: stop_id.to_string(),
                stop_sequence,
                scheduled_arrival: time,
                scheduled_departure: time,
                pickup_type: Some(0),
                drop_off_type: Some(0),
                timepoint: Some(true),
                stop_time_platform: None,
                stop_name: stop_name.to_string(),
                municipality: None,
                lat: None,
                lon: None,
                platform_code: None,
                station_id: Some(format!("station:{stop_id}")),
                complex_id: Some(format!("complex:{stop_id}")),
                has_station_layout: false,
                station_layout_version: None,
            },
        )
        .collect::<Vec<_>>();
        let calls_by_trip = HashMap::from([("trip-calls".to_string(), calls)]);
        let mut values = journeys_with_realtime(std::slice::from_ref(&journey), &[]);

        attach_stop_calls(&[journey], &mut values, &calls_by_trip, &[]);

        assert_eq!(
            values[0]["legs"][0]["stop_calls"].as_array().unwrap().len(),
            3
        );
        assert_eq!(values[0]["legs"][0]["intermediate_stop_count"], 1);
        assert_eq!(
            values[0]["legs"][0]["stop_calls"][1]["is_intermediate"],
            true
        );
    }

    #[test]
    fn coordinate_journey_point_does_not_require_id_and_validates_wgs84_bounds() {
        let valid = JourneyPoint {
            point_type: "coordinate".to_string(),
            id: None,
            lat: Some(50.089458),
            lon: Some(14.428683),
        };
        assert!(validate_coordinate_journey_point(&valid).is_ok());
        assert_eq!(
            coordinate_stop_id(valid.lat.unwrap(), valid.lon.unwrap()),
            "coordinate:50.089458,14.428683"
        );

        let invalid = JourneyPoint {
            point_type: "coordinate".to_string(),
            id: None,
            lat: Some(91.0),
            lon: Some(14.0),
        };
        assert_eq!(
            validate_coordinate_journey_point(&invalid)
                .unwrap_err()
                .code,
            "invalid_coordinate"
        );
    }

    #[test]
    fn belarie_dostihova_ferry_is_not_a_walking_edge() {
        let payload = json!({
            "code": "Ok",
            "routes": [{
                "distance": 464.1,
                "duration": 371.4,
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[14.397527, 50.013126], [14.396679, 50.012320], [14.393909, 50.012527]]
                },
                "legs": [{"steps": [
                    {"mode": "walking"},
                    {"mode": "ferry", "name": "Přívoz"},
                    {"mode": "walking"}
                ]}]
            }]
        });

        assert_eq!(
            walking_route_from_osrm_payload(
                &payload,
                (50.013126, 14.397527),
                (50.012527, 14.393909)
            )
            .unwrap_err(),
            WalkingRouteRejection::NonWalkingSegment
        );
    }

    #[test]
    fn valhalla_pedestrian_shape_is_decoded_to_geojson() {
        let payload = json!({
            "trip": {
                "status": 0,
                "summary": {"has_ferry": false, "length": 0.294, "time": 215.355},
                "legs": [{
                    "summary": {"has_ferry": false},
                    "shape": r"esdp~AgtvoZlIjUz@bGtJlS}@lAs@v@u@jAx@|BqGzNnB\d@oApCEJp@tD?rBAg@_KAwDCqI_A{D_Rol@oHmYeBxA",
                    "maneuvers": [{"travel_mode": "pedestrian"}]
                }]
            }
        });

        let route = walking_route_from_valhalla_payload(
            &payload,
            (50.088783, 14.430023),
            (50.088880, 14.430477),
        )
        .unwrap();
        assert_eq!(route.distance_meters, 294);
        assert_eq!(route.duration_seconds, 216);
        assert_eq!(route.geometry["type"], "LineString");
        assert!(route.geometry["coordinates"].as_array().unwrap().len() > 2);
    }

    #[test]
    fn valhalla_ferry_summary_is_not_a_walking_edge() {
        let payload = json!({
            "trip": {
                "status": 0,
                "summary": {"has_ferry": true, "length": 0.4, "time": 300.0},
                "legs": [{"summary": {"has_ferry": true}}]
            }
        });
        assert_eq!(
            walking_route_from_valhalla_payload(
                &payload,
                (50.013126, 14.397527),
                (50.012527, 14.393909),
            )
            .unwrap_err(),
            WalkingRouteRejection::NonWalkingSegment
        );
    }

    #[test]
    fn pid_source_complex_groups_distinct_named_boarding_points_for_walking_transfers() {
        let surface = implicit_station_transfer_signature(
            "pid_gtfs:U480Z1P",
            "Náměstí Republiky",
            Some("Praha"),
            Some(50.088783),
            Some(14.430023),
            None,
            None,
            Some("1"),
            &["tram".to_string()],
        );
        let metro = implicit_station_transfer_signature(
            "pid_gtfs:U480Z101P",
            "Náměstí Republiky",
            Some("Praha"),
            Some(50.088880),
            Some(14.430477),
            None,
            Some("pid_gtfs:U480S1"),
            Some("101"),
            &["metro".to_string()],
        );
        let other_name = implicit_station_transfer_signature(
            "pid_gtfs:U480Z3P",
            "Masarykovo nádraží",
            Some("Praha"),
            Some(50.087685),
            Some(14.432513),
            None,
            None,
            Some("3"),
            &["tram".to_string()],
        );

        assert_eq!(surface, metro);
        assert_eq!(surface, other_name);
    }

    #[test]
    fn gtfs_shape_is_clipped_between_stops_in_trip_direction() {
        let shape = vec![
            (50.0, 14.0),
            (50.001, 14.001),
            (50.002, 14.002),
            (50.003, 14.003),
        ];
        let geometry = clip_gtfs_shape(&shape, (50.001, 14.001), (50.003, 14.003)).unwrap();
        assert_eq!(geometry["type"], "LineString");
        assert_eq!(geometry["coordinates"][0], json!([14.001, 50.001]));
        assert_eq!(geometry["coordinates"][2], json!([14.003, 50.003]));
        assert!(clip_gtfs_shape(&shape, (50.003, 14.003), (50.001, 14.001)).is_none());
    }
}
