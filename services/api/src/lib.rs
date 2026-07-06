use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, header},
    middleware::Next,
    response::{Html, IntoResponse, Response},
};
use chrono::{DateTime, Duration, NaiveDateTime, NaiveTime, Timelike, Utc};
use routing_core::{SearchRequest as RoutingSearchRequest, earliest_arrivals, fixture_snapshot};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use tokio::{sync::RwLock, time};
use transit_model::{
    CoordinateConfidence, Journey, JourneyLeg, OfflinePackage, RealtimeStatus, Stop, TicketOption,
    TransportMode, normalize_czech_name,
};
use uuid::Uuid;

mod cd;
mod config;
mod controllers;
mod error;
mod http;
mod infrastructure;
mod repositories;
mod services;
mod ticketing;

use config::AppConfig;
use controllers::{account::*, system::*, transit::*};
use error::ApiError;
use repositories::users::{find_by_email as user_by_email_db, find_by_id as user_by_id_db};
use services::auth::{
    auth_response, create_user_record, current_user, hash_token, public_user, require_admin,
    verify_password,
};

const DB_STAT_TABLES: &[&str] = &[
    "import_runs",
    "source_feeds",
    "agencies",
    "cities",
    "stops",
    "stop_source_ids",
    "routes",
    "trips",
    "stop_times",
    "validation_issues",
    "realtime_updates",
    "data_source_syncs",
    "route_geometries",
    "manual_stop_matches",
    "offline_packages",
];
const MAX_JOURNEY_RESULTS: usize = 5;
const MAX_DIRECT_JOURNEY_CANDIDATES: i64 = 20;
const MAX_TRANSFER_JOURNEY_CANDIDATES: i64 = 40;
const SERVICE_DAY_SECONDS: u32 = 24 * 3600;
const NEXT_SERVICE_DAY_SEARCH_FROM_SECONDS: u32 = 18 * 3600;
const MIN_TRANSFER_SECONDS: u32 = 5 * 60;
const MAX_TRANSFER_WAIT_SECONDS: u32 = 2 * 3600;
const TRANSFER_SEARCH_TIMEOUT_SECONDS: u64 = 6;
const ADMIN_DEFAULT_PAGE_SIZE: usize = 50;
const ADMIN_MAX_PAGE_SIZE: usize = 200;
const ADMIN_MAX_MAP_STOPS: usize = 5000;
const ADMIN_VALIDATION_SOURCE_FILE: &str = "admin_database_validation";
const PID_SOURCE_STATUS_ID: &str = "pid_gtfs_rt";

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Claims {
    sub: String,
    email: String,
    roles: Vec<String>,
    exp: usize,
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
    q: Option<String>,
    limit: Option<usize>,
    #[serde(rename = "includeCities", default)]
    include_cities: bool,
}

#[derive(Debug, Deserialize)]
struct RealtimeVehiclesQuery {
    source: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct NearbyQuery {
    lat: f64,
    lon: f64,
    radius: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DeparturesQuery {
    #[serde(rename = "stopId")]
    stop_id: String,
    time: Option<String>,
    limit: Option<usize>,
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
}

#[derive(Debug, Deserialize)]
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
            arrival_time_weight: 1.0,
            duration_weight: 0.0,
            transfer_penalty_seconds: 0,
            preserve_simplest: true,
            preserve_each_transfer_count: true,
            preserve_carrier_diversity: true,
            remove_dominated: true,
            dominate_only_same_carrier: true,
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
    let app = app_state_with_config(config.clone()).await?;
    let router = build_router(app);
    tracing::info!(address = %config.bind_address, "starting Cesta API");
    let listener = tokio::net::TcpListener::bind(config.bind_address).await?;
    axum::serve(listener, router)
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

    let state = AppState {
        config: Arc::new(config.clone()),
        users: Arc::new(RwLock::new(HashMap::new())),
        refresh_tokens: Arc::new(RwLock::new(HashMap::new())),
        saved_places: Arc::new(RwLock::new(HashMap::new())),
        favorite_stops: Arc::new(RwLock::new(HashMap::new())),
        stops: Arc::new(fixture_stops()),
        cities: Arc::new(fixture_cities()),
        db,
        jwt_secret: config.jwt_secret.clone(),
        use_mock_data: config.use_mock_data,
        ticketing,
    };
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

async fn auth_marker(
    State(_state): State<AppState>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    next.run(request).await
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
               st.pickup_type, st.drop_off_type, st.timepoint, st.platform, st.raw_notes,
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
        "command": "cargo run -p data-pipeline -- import-and-validate ggu-latest",
        "warning": "API does not run the full import inline; use a worker/job runner"
    })))
}

async fn admin_database_stats(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    let Some(pool) = &state.db else {
        return Ok(Json(json!({
            "database_available": false,
            "mock": state.use_mock_data,
            "warning": "database is not configured; set USE_MOCK_DATA=false and DATABASE_URL"
        })));
    };

    Ok(Json(
        database_admin_stats(pool).await.map_err(internal_error)?,
    ))
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
               platform_code, modes, source_priority, is_active
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
        return Ok(Json(routing_algorithm_payload(
            RoutingAlgorithmConfig::default(),
            false,
            None,
            None,
        )));
    };
    let (configuration, updated_at, updated_by) = routing_algorithm_config_db(pool)
        .await
        .map_err(internal_error)?;
    Ok(Json(routing_algorithm_payload(
        configuration,
        true,
        updated_at,
        updated_by,
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
    Ok(Json(routing_algorithm_payload(
        configuration,
        true,
        Some(Utc::now()),
        Some(user.email),
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
    Ok(Json(routing_algorithm_payload(
        configuration,
        true,
        Some(Utc::now()),
        Some(user.email),
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
          arrival_time_weight, duration_weight, transfer_penalty_seconds,
          preserve_simplest, preserve_each_transfer_count,
          preserve_carrier_diversity, remove_dominated,
          dominate_only_same_carrier, updated_at, updated_by
        ) VALUES (
          1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
          $11, $12, $13, $14, $15, now(), $16
        )
        ON CONFLICT (id) DO UPDATE SET
          max_results = EXCLUDED.max_results,
          max_direct_candidates = EXCLUDED.max_direct_candidates,
          max_transfer_candidates = EXCLUDED.max_transfer_candidates,
          min_transfer_seconds = EXCLUDED.min_transfer_seconds,
          max_transfer_wait_seconds = EXCLUDED.max_transfer_wait_seconds,
          transfer_search_timeout_seconds = EXCLUDED.transfer_search_timeout_seconds,
          next_day_search_from_seconds = EXCLUDED.next_day_search_from_seconds,
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
) -> Value {
    json!({
        "configuration": configuration,
        "defaults": RoutingAlgorithmConfig::default(),
        "database_available": database_available,
        "updated_at": updated_at,
        "updated_by": updated_by,
        "activation": "New journey searches read this profile immediately; running searches are not changed.",
        "scoring_formula": "arrival_time × arrival_time_weight + duration × duration_weight + transfers × transfer_penalty_seconds",
        "fare_note": "No real fare data is imported. Carrier diversity preserves potentially cheaper operators without claiming a cheapest fare."
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

fn internal_error(error: impl std::fmt::Display) -> ApiError {
    ApiError {
        code: "internal_error".to_string(),
        message: error.to_string(),
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
        SELECT id, source, status, started_at, finished_at, summary
        FROM import_runs
        WHERE status = 'success'
        ORDER BY finished_at DESC NULLS LAST, started_at DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await?;
    let stop_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM stops WHERE is_active = true")
        .fetch_one(pool)
        .await?;
    let route_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM routes WHERE is_active = true")
        .fetch_one(pool)
        .await?;
    let trip_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trips")
        .fetch_one(pool)
        .await?;
    let stop_time_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM stop_times")
        .fetch_one(pool)
        .await?;
    let realtime_sources = sqlx::query(
        r#"
        SELECT source_id, status, last_success_at, source_timestamp,
               records_received, records_written, error_message
        FROM data_source_syncs
        WHERE data_kind IN ('gtfs_realtime', 'vehicle_positions')
        ORDER BY source_id ASC
        "#,
    )
    .fetch_all(pool)
    .await?;
    let pid_realtime_current = realtime_sources.iter().any(|row| {
        row.get::<String, _>("source_id") == PID_SOURCE_STATUS_ID
            && row.get::<String, _>("status") == "success"
            && row
                .get::<Option<DateTime<Utc>>, _>("source_timestamp")
                .is_some_and(|timestamp| timestamp > Utc::now() - Duration::minutes(5))
    });
    let has_successful_import = latest.is_some();

    Ok(json!({
        "schedule": if has_successful_import { "current" } else { "unknown" },
        "realtime": if pid_realtime_current { "full" } else if realtime_sources.is_empty() { "unavailable" } else { "stale" },
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

    let mut tables = Vec::new();
    let mut total_rows = 0_i64;
    for table in DB_STAT_TABLES {
        let row_count = table_row_count(pool, table).await?;
        total_rows += row_count;
        let size = sqlx::query(
            r#"
            SELECT
              pg_total_relation_size($1::regclass) AS total_size_bytes,
              pg_relation_size($1::regclass) AS table_size_bytes,
              pg_indexes_size($1::regclass) AS indexes_size_bytes,
              pg_size_pretty(pg_total_relation_size($1::regclass)) AS total_size_pretty
            "#,
        )
        .bind(*table)
        .fetch_one(pool)
        .await?;

        tables.push(json!({
            "table": table,
            "rows": row_count,
            "total_size_bytes": size.get::<i64, _>("total_size_bytes"),
            "table_size_bytes": size.get::<i64, _>("table_size_bytes"),
            "indexes_size_bytes": size.get::<i64, _>("indexes_size_bytes"),
            "total_size_pretty": size.get::<String, _>("total_size_pretty")
        }));
    }

    let source_rows = sqlx::query(
        r#"
        SELECT
          sf.id,
          sf.name,
          sf.type,
          sf.priority,
          COALESCE(stop_counts.count, 0) AS stops,
          COALESCE(route_counts.count, 0) AS routes,
          COALESCE(trip_counts.count, 0) AS trips,
          COALESCE(stop_time_counts.count, 0) AS stop_times,
          COALESCE(issue_counts.count, 0) AS validation_issues
        FROM source_feeds sf
        LEFT JOIN (
          SELECT source_feed_id, COUNT(*) AS count FROM stops GROUP BY source_feed_id
        ) stop_counts ON stop_counts.source_feed_id = sf.id
        LEFT JOIN (
          SELECT source_feed_id, COUNT(*) AS count FROM routes GROUP BY source_feed_id
        ) route_counts ON route_counts.source_feed_id = sf.id
        LEFT JOIN (
          SELECT source_feed_id, COUNT(*) AS count FROM trips GROUP BY source_feed_id
        ) trip_counts ON trip_counts.source_feed_id = sf.id
        LEFT JOIN (
          SELECT source_feed_id, COUNT(*) AS count FROM stop_times GROUP BY source_feed_id
        ) stop_time_counts ON stop_time_counts.source_feed_id = sf.id
        LEFT JOIN (
          SELECT source_feed_id, COUNT(*) AS count FROM validation_issues GROUP BY source_feed_id
        ) issue_counts ON issue_counts.source_feed_id = sf.id
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

    Ok(json!({
        "database_available": true,
        "database": {
            "name": database_row.get::<String, _>("database_name"),
            "total_size_bytes": database_row.get::<i64, _>("total_size_bytes"),
            "total_size_pretty": database_row.get::<String, _>("total_size_pretty")
        },
        "totals": {
            "tracked_rows": total_rows,
            "unresolved_active_stops": unresolved_stop_count
        },
        "tables": tables,
        "source_feeds": source_feeds,
        "latest_imports": latest_imports,
        "validation_issues": validation_issues
    }))
}

async fn table_row_count(pool: &PgPool, table: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(pool)
        .await
}

async fn query_journeys_db(
    pool: &PgPool,
    body: &JourneySearchBody,
    departure_time: u32,
    service_date: chrono::NaiveDate,
) -> Result<(Vec<Value>, Vec<String>, Value), sqlx::Error> {
    let mut warnings = Vec::new();
    let (routing_config, _, _) = routing_algorithm_config_db(pool).await?;
    let (from_stop_ids, from_warning) = resolve_journey_point_db(pool, &body.from).await?;
    let (to_stop_ids, to_warning) = resolve_journey_point_db(pool, &body.to).await?;
    warnings.extend(from_warning);
    warnings.extend(to_warning);

    if from_stop_ids.is_empty() || to_stop_ids.is_empty() {
        warnings.push("one or both journey stops could not be resolved".to_string());
        return Ok((
            Vec::new(),
            warnings,
            json!({"query_context": journey_query_context(body, departure_time, &from_stop_ids, &to_stop_ids)}),
        ));
    }

    let mode_filters = body
        .transport_modes
        .iter()
        .filter_map(transport_mode_to_db)
        .collect::<Vec<_>>();
    let current_service_day_search = service_day_journeys_db(
        pool,
        &from_stop_ids,
        &to_stop_ids,
        departure_time,
        &mode_filters,
        body.max_transfers,
        service_date,
        &routing_config,
    );
    let include_next_service_day = should_search_next_service_day(
        departure_time,
        routing_config.next_day_search_from_seconds as u32,
    );
    let (current_service_day_result, next_service_day_result) = if include_next_service_day {
        let next_service_day_search = service_day_journeys_db(
            pool,
            &from_stop_ids,
            &to_stop_ids,
            0,
            &mode_filters,
            body.max_transfers,
            service_date.succ_opt().unwrap_or(service_date),
            &routing_config,
        );
        let (current, next) = tokio::join!(current_service_day_search, next_service_day_search);
        (current, Some(next))
    } else {
        (current_service_day_search.await, None)
    };

    let (mut journeys, transfer_search_status) = current_service_day_result?;
    append_transfer_search_warning(&mut warnings, transfer_search_status, false);

    if let Some(next_service_day_result) = next_service_day_result {
        let (next_service_day_journeys, next_transfer_search_status) = next_service_day_result?;
        append_transfer_search_warning(&mut warnings, next_transfer_search_status, true);
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
        let (next_service_day_journeys, next_transfer_search_status) = service_day_journeys_db(
            pool,
            &from_stop_ids,
            &to_stop_ids,
            0,
            &mode_filters,
            body.max_transfers,
            service_date.succ_opt().unwrap_or(service_date),
            &routing_config,
        )
        .await?;
        append_transfer_search_warning(&mut warnings, next_transfer_search_status, true);
        let mut next_service_day_journeys =
            next_service_day_journey_results(next_service_day_journeys, departure_time);
        if !next_service_day_journeys.is_empty() {
            journeys.append(&mut next_service_day_journeys);
            warnings.push(
                "included next service-day journeys because no later service was available on the requested day"
                    .to_string(),
            );
        }
    }
    let candidate_count = journeys.len();
    journeys = dedupe_relevant_journeys_db(pool, journeys).await?;
    let removed_candidates = candidate_count.saturating_sub(journeys.len());
    if removed_candidates > 0 {
        warnings.push(format!(
            "removed {removed_candidates} duplicate or invalid journey candidates"
        ));
    }
    let carrier_keys = journey_carrier_keys_db(pool, &journeys).await?;
    journeys = ranked_journey_results_with_carriers(journeys, &carrier_keys, &routing_config);

    let unverified_services = unverified_journey_service_count(pool, &journeys).await?;
    if unverified_services > 0 {
        warnings.push(format!(
            "{unverified_services} selected trip services come from a legacy import without calendar data; refresh that source to verify the requested date"
        ));
    }

    if journeys.is_empty() {
        warnings.push("no database journeys found for the resolved stops".to_string());
    }

    let mut related = journey_related_data_db(pool, &journeys).await?;
    let realtime_updates = journey_realtime_updates_db(pool, &journeys, service_date).await?;
    let stop_calls = if body.include_intermediate_stops {
        Some(journey_stop_calls_db(pool, &journeys).await?)
    } else {
        None
    };
    let mut journey_values = journeys_with_realtime(&journeys, &realtime_updates);
    if let Some(stop_calls) = &stop_calls {
        attach_stop_calls(
            &journeys,
            &mut journey_values,
            stop_calls,
            &realtime_updates,
        );
    }
    related["realtime_updates"] = Value::Array(realtime_updates);
    related["realtime_status"] = json!(journeys_realtime_status(&journey_values));
    related["intermediate_stops_included"] = json!(body.include_intermediate_stops);
    related["query_context"] =
        journey_query_context(body, departure_time, &from_stop_ids, &to_stop_ids);

    Ok((journey_values, warnings, related))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferSearchStatus {
    Complete,
    TimedOut,
    Failed,
}

fn append_transfer_search_warning(
    warnings: &mut Vec<String>,
    status: TransferSearchStatus,
    next_service_day: bool,
) {
    let prefix = if next_service_day {
        "next service-day transfer search"
    } else {
        "transfer search"
    };
    match status {
        TransferSearchStatus::Complete => {}
        TransferSearchStatus::TimedOut => warnings.push(format!(
            "{prefix} timed out; direct journeys are still included"
        )),
        TransferSearchStatus::Failed => warnings.push(format!(
            "{prefix} failed; direct journeys are still included"
        )),
    }
}

async fn unverified_journey_service_count(
    pool: &PgPool,
    journeys: &[Journey],
) -> Result<i64, sqlx::Error> {
    let trip_ids = journeys
        .iter()
        .flat_map(|journey| journey.legs.iter())
        .filter_map(|leg| leg.trip_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if trip_ids.is_empty() {
        return Ok(0);
    }
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM trips trip
        WHERE trip.id = ANY($1)
          AND NOT EXISTS (SELECT 1 FROM calendars WHERE source_feed_id = trip.source_feed_id)
          AND NOT EXISTS (SELECT 1 FROM calendar_dates WHERE source_feed_id = trip.source_feed_id)
        "#,
    )
    .bind(trip_ids)
    .fetch_one(pool)
    .await
}

#[allow(clippy::too_many_arguments)]
async fn service_day_journeys_db(
    pool: &PgPool,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
    departure_time: u32,
    mode_filters: &[String],
    max_transfers: u32,
    service_date: chrono::NaiveDate,
    routing_config: &RoutingAlgorithmConfig,
) -> Result<(Vec<Journey>, TransferSearchStatus), sqlx::Error> {
    let direct_search = direct_journeys_db(
        pool,
        from_stop_ids,
        to_stop_ids,
        departure_time,
        mode_filters,
        service_date,
        routing_config.max_direct_candidates as i64,
    );
    let transfer_search = async {
        if max_transfers == 0 {
            return Ok::<_, sqlx::Error>((Vec::new(), TransferSearchStatus::Complete));
        }

        match time::timeout(
            std::time::Duration::from_secs(routing_config.transfer_search_timeout_seconds as u64),
            one_transfer_journeys_db(
                pool,
                from_stop_ids,
                to_stop_ids,
                departure_time,
                mode_filters,
                service_date,
                routing_config.min_transfer_seconds,
                routing_config.max_transfer_wait_seconds,
                routing_config.max_transfer_candidates as i64,
            ),
        )
        .await
        {
            Ok(Ok(journeys)) => Ok((journeys, TransferSearchStatus::Complete)),
            Ok(Err(error)) => {
                tracing::warn!(%error, "transfer journey database query failed");
                Ok((Vec::new(), TransferSearchStatus::Failed))
            }
            Err(_) => Ok((Vec::new(), TransferSearchStatus::TimedOut)),
        }
    };

    let (direct_result, transfer_result) = tokio::join!(direct_search, transfer_search);
    let mut journeys = direct_result?;
    let (mut transfer_journeys, transfer_search_status) = transfer_result?;
    journeys.append(&mut transfer_journeys);
    Ok((journeys, transfer_search_status))
}

fn should_search_next_service_day(departure_time: u32, threshold_seconds: u32) -> bool {
    departure_time >= threshold_seconds
}

fn journey_query_context(
    body: &JourneySearchBody,
    departure_time: u32,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
) -> Value {
    json!({
        "requested_datetime": body.datetime,
        "departure_time": departure_time,
        "max_transfers": body.max_transfers,
        "transport_modes": body.transport_modes,
        "include_intermediate_stops": body.include_intermediate_stops,
        "from_stop_ids": from_stop_ids,
        "to_stop_ids": to_stop_ids
    })
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
    ))
}

fn dedupe_relevant_journeys(
    mut journeys: Vec<Journey>,
    stop_signatures: &HashMap<String, String>,
    route_priorities: &HashMap<String, i32>,
) -> Vec<Journey> {
    journeys.retain(|journey| journey_is_relevant(journey, stop_signatures));
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

fn journey_is_relevant(journey: &Journey, stop_signatures: &HashMap<String, String>) -> bool {
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
        if leg.arrival_time < leg.departure_time
            || stop_signature(&leg.from_stop_id, stop_signatures)
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
                || !(MIN_TRANSFER_SECONDS..=MAX_TRANSFER_WAIT_SECONDS).contains(&wait)
                || stop_signature(&leg.to_stop_id, stop_signatures)
                    != stop_signature(&next_leg.from_stop_id, stop_signatures)
            {
                return false;
            }
        }
    }
    true
}

fn visible_journey_key(journey: &Journey, stop_signatures: &HashMap<String, String>) -> String {
    journey
        .legs
        .iter()
        .map(|leg| {
            format!(
                "{}:{}:{}:{}",
                stop_signature(&leg.from_stop_id, stop_signatures),
                stop_signature(&leg.to_stop_id, stop_signatures),
                leg.departure_time,
                leg.arrival_time
            )
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
    push_ranked_journey(
        &mut selected,
        &mut selected_keys,
        &candidates[0],
        configuration.max_results as usize,
    );

    if configuration.preserve_simplest
        && let Some(simplest) = candidates.iter().min_by_key(|journey| {
            (
                journey.transfer_count,
                journey.arrival_time,
                journey.duration_seconds,
                journey.departure_time,
            )
        })
    {
        push_ranked_journey(
            &mut selected,
            &mut selected_keys,
            simplest,
            configuration.max_results as usize,
        );
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
                configuration.max_results as usize,
            );
        }
    }

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
        for best_for_carrier in carrier_candidates {
            push_ranked_journey(
                &mut selected,
                &mut selected_keys,
                best_for_carrier,
                configuration.max_results as usize,
            );
        }
    }

    for journey in &candidates {
        push_ranked_journey(
            &mut selected,
            &mut selected_keys,
            journey,
            configuration.max_results as usize,
        );
    }

    let simplest_key = selected
        .iter()
        .min_by_key(|journey| {
            (
                journey.transfer_count,
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
    journeys
        .iter()
        .enumerate()
        .filter(|(candidate_index, candidate)| {
            !journeys.iter().enumerate().any(|(other_index, other)| {
                other_index != *candidate_index
                    && (!configuration.dominate_only_same_carrier
                        || journey_carrier_signature(other, carrier_keys)
                            == journey_carrier_signature(candidate, carrier_keys))
                    && other.departure_time >= candidate.departure_time
                    && other.arrival_time <= candidate.arrival_time
                    && other.transfer_count <= candidate.transfer_count
                    && (other.departure_time > candidate.departure_time
                        || other.arrival_time < candidate.arrival_time
                        || other.transfer_count < candidate.transfer_count)
            })
        })
        .map(|(_, journey)| journey.clone())
        .collect()
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
    let marker_index = stop_id.rfind("SR70S-CZ-")?;
    let marker_end = marker_index + "SR70S-CZ-".len();
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

    Some(format!("{}{}", &stop_id[..marker_end], station_code))
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
    let candidate = point
        .id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&point.point_type);
    let mut warnings = Vec::new();

    if point.point_type == "city" {
        let stop_ids = sqlx::query_scalar::<_, String>(
            "SELECT id FROM stops WHERE is_active = true AND city_id = $1 ORDER BY id",
        )
        .bind(candidate)
        .fetch_all(pool)
        .await?;
        if stop_ids.is_empty() {
            warnings.push(format!("city '{candidate}' has no active assigned stops"));
        }
        return Ok((stop_ids, warnings));
    }

    if let Some(stop) = get_stop_db(pool, candidate).await? {
        return Ok((equivalent_stop_ids_db(pool, &stop).await?, warnings));
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
            return Ok((equivalent_stop_ids_db(pool, &stop).await?, warnings));
        }
    }

    warnings.push(format!("could not resolve stop query '{candidate}'"));
    Ok((Vec::new(), warnings))
}

async fn validate_journey_point_db(pool: &PgPool, point: &JourneyPoint) -> Result<(), ApiError> {
    match point.point_type.as_str() {
        "stop" => Ok(()),
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
        other => Err(ApiError {
            code: "invalid_journey_point_type".to_string(),
            message: format!("journey point type '{other}' is not supported; use 'stop' or 'city'"),
        }),
    }
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

    if let Some(stop_area_id) = &stop.stop_area_id {
        let mut area_ids = sqlx::query_scalar::<_, String>(
            "SELECT id FROM stops WHERE is_active = true AND stop_area_id = $1 LIMIT 250",
        )
        .bind(stop_area_id)
        .fetch_all(pool)
        .await?;
        ids.append(&mut area_ids);
    }

    if let Some(station_base) = railway_station_stop_base(&stop.id) {
        let station_prefix = escaped_like_prefix(&format!("{station_base}-"));
        let station_ids = sqlx::query_scalar::<_, String>(
            r#"
            SELECT id
            FROM stops
            WHERE is_active = true
              AND (id = $1 OR id LIKE $2 ESCAPE '\')
            LIMIT 250
            "#,
        )
        .bind(&station_base)
        .bind(station_prefix)
        .fetch_all(pool)
        .await?;
        ids.extend(
            station_ids.into_iter().filter(|id| {
                railway_station_stop_base(id).as_deref() == Some(station_base.as_str())
            }),
        );
    }

    if let Some((lat, lon)) = stop.lat.zip(stop.lon) {
        let sibling_rows = sqlx::query(
            r#"
            SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
                   lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
                   platform_code, modes, source_priority, is_active
            FROM stops
            WHERE is_active = true
              AND lat IS NOT NULL
              AND lon IS NOT NULL
              AND abs(lat - $1) < 0.003
              AND abs(lon - $2) < 0.005
            LIMIT 250
            "#,
        )
        .bind(lat)
        .bind(lon)
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
            AND COALESCE(st_from.pickup_type, 0) = 0
            AND COALESCE(st_to.drop_off_type, 0) = 0
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
            AND COALESCE(stop_time.pickup_type, 0) = 0
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
            AND COALESCE(stop_time.drop_off_type, 0) = 0
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
            AND COALESCE(st_mid.drop_off_type, 0) = 0
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
            AND COALESCE(st_transfer.pickup_type, 0) = 0
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

async fn journey_related_data_db(
    pool: &PgPool,
    journeys: &[Journey],
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

    let stops = if stop_ids.is_empty() {
        Vec::new()
    } else {
        let rows = sqlx::query(
            r#"
            SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
                   lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
                   platform_code, modes, source_priority, is_active
            FROM stops
            WHERE id = ANY($1)
            ORDER BY name ASC, platform_code ASC NULLS FIRST
            "#,
        )
        .bind(&stop_ids)
        .fetch_all(pool)
        .await?;
        rows.into_iter()
            .map(stop_from_row)
            .collect::<Result<Vec<_>, _>>()?
    };
    for stop in &stops {
        for source_id in &stop.source_ids {
            source_feed_ids.insert(source_id.feed_id.clone());
        }
    }

    let routes = if route_ids.is_empty() {
        Vec::new()
    } else {
        let rows = sqlx::query(
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
        .await?;
        rows.into_iter()
            .map(|row| {
                if let Some(feed_id) = row.get::<Option<String>, _>("source_feed_id") {
                    source_feed_ids.insert(feed_id);
                }
                if let Some(agency_id) = row.get::<Option<String>, _>("agency_id") {
                    agency_ids.insert(agency_id);
                }
                route_row_json(&row)
            })
            .collect::<Vec<_>>()
    };

    let trips = if trip_ids.is_empty() {
        Vec::new()
    } else {
        let rows = sqlx::query(
            r#"
            SELECT id, source_feed_id, source_id, route_id, service_id, headsign,
                   direction_id, shape_id, restrictions, raw_source_metadata, source_priority
            FROM trips
            WHERE id = ANY($1)
            ORDER BY source_priority ASC, id ASC
            "#,
        )
        .bind(&trip_ids)
        .fetch_all(pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                if let Some(feed_id) = row.get::<Option<String>, _>("source_feed_id") {
                    source_feed_ids.insert(feed_id);
                }
                trip_row_json(&row)
            })
            .collect::<Vec<_>>()
    };

    let stop_times = if trip_ids.is_empty() || stop_ids.is_empty() {
        Vec::new()
    } else {
        sqlx::query(
            r#"
            SELECT trip_id, stop_id, stop_sequence, arrival_time, departure_time,
                   pickup_type, drop_off_type, timepoint, platform, raw_notes,
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
        .await?
        .into_iter()
        .map(|row| {
            if let Some(feed_id) = row.get::<Option<String>, _>("source_feed_id") {
                source_feed_ids.insert(feed_id);
            }
            stop_time_row_json(&row)
        })
        .collect::<Vec<_>>()
    };

    let route_geometries = if route_ids.is_empty() {
        Vec::new()
    } else {
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
        .await?
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
        .collect::<Vec<_>>()
    };

    let agencies = fetch_agencies_json(pool, agency_ids.into_iter().collect()).await?;
    let source_feeds = fetch_source_feeds_json(pool, source_feed_ids.into_iter().collect()).await?;

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
               s.municipality, s.lat, s.lon, s.platform_code
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

    let source_ids = sqlx::query(
        r#"
        SELECT stop_id, source_feed_id, original_source_id, import_run_id, priority,
               confidence, suppressed_as_duplicate
        FROM stop_source_ids
        WHERE stop_id = ANY($1)
        ORDER BY stop_id ASC, priority ASC, source_feed_id ASC
        "#,
    )
    .bind(&stop_ids)
    .fetch_all(pool)
    .await?
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

    let stop_areas = if stop_area_ids.is_empty() {
        Vec::new()
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
        .await?
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<String, _>("id"),
                "name": row.get::<String, _>("name"),
                "lat": row.get::<Option<f64>, _>("lat"),
                "lon": row.get::<Option<f64>, _>("lon")
            })
        })
        .collect::<Vec<_>>()
    };

    let route_rows = sqlx::query(
        r#"
        SELECT DISTINCT r.id, r.source_feed_id, r.source_id, r.agency_id, r.operator_id,
               r.short_name, r.long_name, r.mode, r.gtfs_route_type, r.color, r.text_color,
               r.source_priority, r.is_active
        FROM stop_times st
        JOIN trips t ON t.id = st.trip_id
        JOIN routes r ON r.id = t.route_id
        WHERE st.stop_id = ANY($1)
        ORDER BY r.source_priority ASC, r.short_name ASC NULLS LAST, r.id ASC
        LIMIT 200
        "#,
    )
    .bind(&stop_ids)
    .fetch_all(pool)
    .await?;
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
    let normalized = normalize_czech_name(raw_query);
    let like = format!("%{normalized}%");
    let raw_like = format!("%{}%", raw_query.trim());
    let first_token = normalized_query
        .split_whitespace()
        .next()
        .unwrap_or_default();
    let first_token_like = format!("%{first_token}%");
    let rows = sqlx::query(
        r#"
        SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
               lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
               platform_code, modes, source_priority, is_active
        FROM stops
        WHERE is_active = true
          AND (
            $1 = ''
            OR id = $4
            OR normalized_name LIKE $2
            OR name ILIKE $3
            OR normalized_name LIKE $5
            OR name ILIKE $5
            OR normalized_name % $1
            OR name % $6
          )
        ORDER BY
          CASE WHEN id = $4 THEN 0 WHEN normalized_name = $1 THEN 1 ELSE 2 END,
          similarity(normalized_name, $1) DESC,
          similarity(name, $6) DESC,
          platform_code IS NULL DESC,
          source_priority ASC,
          name ASC
        LIMIT 250
        "#,
    )
    .bind(&normalized)
    .bind(&like)
    .bind(&raw_like)
    .bind(raw_query.trim())
    .bind(&first_token_like)
    .bind(raw_query.trim())
    .fetch_all(pool)
    .await?;

    let stops = rows
        .into_iter()
        .map(stop_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ranked_stop_suggestions(
        stops.iter(),
        normalized_query,
        limit,
    ))
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
               platform_code, modes, source_priority, is_active
        FROM stops
        WHERE is_active = true
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

async fn get_stop_db(pool: &PgPool, id: &str) -> Result<Option<Stop>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT id, source_feed_id, name, normalized_name, municipality, district, region,
               lat, lon, coordinate_confidence, coordinate_source, stop_area_id,
               platform_code, modes, source_priority, is_active
        FROM stops
        WHERE id = $1 AND is_active = true
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
) -> Result<Vec<Value>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT
          st.trip_id,
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
        FROM stop_times st
        JOIN trips t ON t.id = st.trip_id
        JOIN routes r ON r.id = t.route_id
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
        WHERE st.stop_id = $1
          AND st.departure_time >= $2
          AND COALESCE(st.pickup_type, 0) = 0
        ORDER BY st.departure_time ASC
        LIMIT $3
        "#,
    )
    .bind(stop_id)
    .bind(earliest_seconds as i32)
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let departure_time = row.get::<i32, _>("departure_time") as u32;
            let delay_seconds = row.get::<Option<i32>, _>("delay_seconds");
            let cancellation_status = row.get::<Option<String>, _>("cancellation_status");
            json!({
                "trip_id": row.get::<String, _>("trip_id"),
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
        name: row.get("name"),
        normalized_name: row.get("normalized_name"),
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
        platform_code: row.get("platform_code"),
        modes: row
            .get::<Vec<String>, _>("modes")
            .into_iter()
            .map(|mode| db_mode_to_model(&mode))
            .collect(),
        is_active: row.get("is_active"),
    })
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
        WHERE id = ANY($1)
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
        "stop" => Ok(()),
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
        other => Err(ApiError {
            code: "invalid_journey_point_type".to_string(),
            message: format!("journey point type '{other}' is not supported; use 'stop' or 'city'"),
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

fn mock_status(use_mock_data: bool) -> Value {
    json!({
        "source": if use_mock_data { "mock" } else { "database" },
        "schedule": if use_mock_data { "mock" } else { "current" },
        "realtime": "unavailable",
        "warnings": if use_mock_data { vec!["development fixture data is in use"] } else { Vec::<&str>::new() }
    })
}

fn stop_search_score(stop: &Stop, query: &str) -> Option<i32> {
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
    value["place_type"] = json!(stop_place_type(stop));
    value
}

fn stop_place_type(stop: &Stop) -> &'static str {
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
    if left.stop_area_id.is_some() && left.stop_area_id == right.stop_area_id {
        return true;
    }
    if let (Some(left_base), Some(right_base)) = (
        railway_station_stop_base(&left.id),
        railway_station_stop_base(&right.id),
    ) && left_base == right_base
    {
        return true;
    }
    if canonical_stop_name(left) != canonical_stop_name(right) {
        return false;
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

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    use super::*;

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
        assert!(
            payload["paths"]["/stops/search"]["get"]["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .any(|parameter| parameter["name"] == "includeCities")
        );
        assert_eq!(
            payload["components"]["schemas"]["JourneyPoint"]["properties"]["type"]["enum"],
            json!(["stop", "city"])
        );
        assert!(
            payload["components"]["schemas"]["PlaceType"]["enum"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == "city")
        );
        assert!(payload["paths"]["/realtime/vehicles"].is_object());
        assert!(payload["paths"]["/data-sources/status"].is_object());
        assert!(payload["components"]["schemas"]["JourneyLegRealtime"].is_object());
        assert_eq!(
            payload["paths"]["/journeys/search"]["post"]["requestBody"]["content"]["application/json"]
                ["schema"]["properties"]["include_intermediate_stops"]["default"],
            false
        );
        assert!(payload["components"]["schemas"]["JourneyStopCall"].is_object());
        assert!(payload["paths"]["/admin/routing-algorithm"]["put"].is_object());
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
            railway_station_stop_base("ggu_czptt_gtfs_latest:-SR70S-CZ-35442-4b").as_deref(),
            Some(station)
        );
        assert_eq!(
            canonical_journey_stop_id("ggu_czptt_gtfs_latest:-SR70S-CZ-35442-2"),
            station
        );
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
    fn ranked_journeys_preserve_a_different_carrier_option() {
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

        assert_eq!(ranked.len(), MAX_JOURNEY_RESULTS);
        assert!(ranked.iter().any(|journey| {
            journey
                .legs
                .iter()
                .any(|leg| leg.route_id.as_deref() == Some("route-carrier-b"))
        }));
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
    fn transfer_search_warnings_distinguish_timeout_from_database_failure() {
        let mut warnings = Vec::new();

        append_transfer_search_warning(&mut warnings, TransferSearchStatus::TimedOut, false);
        append_transfer_search_warning(&mut warnings, TransferSearchStatus::Failed, true);
        append_transfer_search_warning(&mut warnings, TransferSearchStatus::Complete, false);

        assert_eq!(
            warnings,
            vec![
                "transfer search timed out; direct journeys are still included",
                "next service-day transfer search failed; direct journeys are still included",
            ]
        );
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

        assert_eq!(ranked.len(), MAX_JOURNEY_RESULTS);
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
        );

        assert_eq!(deduplicated.len(), 1);
        assert_eq!(deduplicated[0].id, "official");
    }

    #[test]
    fn relevance_filter_rejects_impossible_transfer() {
        let mut journey = test_journey("bad-transfer", 1, 3_600, 7_200);
        journey.legs[1].departure_time = journey.legs[0].arrival_time + 60;
        let signatures = HashMap::from([
            ("praha".to_string(), "praha".to_string()),
            ("vsetin".to_string(), "vsetin".to_string()),
            ("transfer-bad-transfer".to_string(), "transfer".to_string()),
        ]);

        assert!(!journey_is_relevant(&journey, &signatures));
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
}
