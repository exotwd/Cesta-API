use std::{
    collections::{HashMap, HashSet},
    env,
    net::SocketAddr,
    sync::Arc,
};

use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
};
use chrono::{Duration, NaiveDateTime, NaiveTime, Timelike, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use routing_core::{SearchRequest as RoutingSearchRequest, earliest_arrivals, fixture_snapshot};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use tokio::{sync::RwLock, time};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use transit_model::{
    CoordinateConfidence, Journey, JourneyLeg, OfflinePackage, RealtimeStatus, Stop, TicketOption,
    TransportMode, normalize_czech_name,
};
use uuid::Uuid;

const DB_STAT_TABLES: &[&str] = &[
    "import_runs",
    "source_feeds",
    "agencies",
    "stops",
    "stop_source_ids",
    "routes",
    "trips",
    "stop_times",
    "validation_issues",
    "realtime_updates",
    "manual_stop_matches",
    "offline_packages",
];
const MAX_JOURNEY_RESULTS: usize = 5;
const SERVICE_DAY_SECONDS: u32 = 24 * 3600;
const MIN_TRANSFER_SECONDS: u32 = 5 * 60;
const MAX_TRANSFER_WAIT_SECONDS: u32 = 2 * 3600;

#[derive(Clone)]
struct AppState {
    users: Arc<RwLock<HashMap<Uuid, UserRecord>>>,
    refresh_tokens: Arc<RwLock<HashMap<String, Uuid>>>,
    saved_places: Arc<RwLock<HashMap<Uuid, Vec<SavedPlace>>>>,
    favorite_stops: Arc<RwLock<HashMap<Uuid, Vec<FavoriteStop>>>>,
    stops: Arc<Vec<Stop>>,
    db: Option<PgPool>,
    jwt_secret: String,
    use_mock_data: bool,
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

#[derive(Debug, Serialize)]
struct ApiError {
    code: String,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.code.as_str() {
            "unauthorized" => StatusCode::UNAUTHORIZED,
            "forbidden" => StatusCode::FORBIDDEN,
            "not_found" => StatusCode::NOT_FOUND,
            "conflict" => StatusCode::CONFLICT,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, Json(self)).into_response()
    }
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
}

#[derive(Debug, Deserialize)]
struct JourneyPoint {
    #[serde(rename = "type")]
    point_type: String,
    id: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let port = env::var("API_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8070);
    let app = app_state().await?;
    let router = build_router(app);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!(%addr, "starting Cesta API");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

async fn app_state() -> anyhow::Result<AppState> {
    let use_mock_data = env::var("USE_MOCK_DATA")
        .map(|value| value == "true")
        .unwrap_or(true);
    let db = if use_mock_data {
        None
    } else {
        let database_url = env::var("DATABASE_URL")?;
        Some(connect_database_with_retry(&database_url).await?)
    };

    let state = AppState {
        users: Arc::new(RwLock::new(HashMap::new())),
        refresh_tokens: Arc::new(RwLock::new(HashMap::new())),
        saved_places: Arc::new(RwLock::new(HashMap::new())),
        favorite_stops: Arc::new(RwLock::new(HashMap::new())),
        stops: Arc::new(fixture_stops()),
        db,
        jwt_secret: env::var("JWT_SECRET").unwrap_or_else(|_| "dev-only-change-me".to_string()),
        use_mock_data,
    };

    if let (Ok(email), Ok(password)) = (
        env::var("ADMIN_BOOTSTRAP_EMAIL"),
        env::var("ADMIN_BOOTSTRAP_PASSWORD"),
    ) {
        if !email.is_empty() && !password.is_empty() {
            let user = create_user_record(
                &email,
                &password,
                Some("Admin".to_string()),
                vec!["admin".to_string(), "data_admin".to_string()],
            )?;
            state.users.write().await.insert(user.id, user);
        }
    }
    Ok(state)
}

async fn connect_database_with_retry(database_url: &str) -> anyhow::Result<PgPool> {
    let mut last_error = None;
    for attempt in 1..=30 {
        match PgPool::connect(database_url).await {
            Ok(pool) => return Ok(pool),
            Err(error) => {
                tracing::warn!(attempt, %error, "database is not ready yet");
                last_error = Some(error);
                time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    Err(anyhow::anyhow!(
        "database connection failed after retries: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    ))
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/openapi.json", get(openapi))
        .route("/metadata/data-status", get(data_status))
        .route("/metadata/sources", get(sources))
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        .route("/auth/refresh", post(refresh))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(auth_me).patch(update_me).delete(delete_me))
        .route("/auth/change-password", post(change_password))
        .route("/me/profile", get(profile).patch(profile))
        .route(
            "/me/saved-places",
            get(list_saved_places).post(create_saved_place),
        )
        .route(
            "/me/saved-places/{id}",
            patch(update_saved_place).delete(delete_saved_place),
        )
        .route(
            "/me/favorite-stops",
            get(list_favorite_stops).post(add_favorite_stop),
        )
        .route("/me/favorite-stops/{id}", delete(delete_favorite_stop))
        .route(
            "/me/favorite-routes",
            get(empty_user_collection).post(empty_user_collection),
        )
        .route("/me/favorite-routes/{id}", delete(empty_user_collection))
        .route(
            "/me/notification-preferences",
            get(notification_preferences).patch(notification_preferences),
        )
        .route("/stops/search", get(search_stops))
        .route("/stops/nearby", get(nearby_stops))
        .route("/stops/{id}", get(stop_detail))
        .route("/stop-areas/{id}", get(stop_area))
        .route("/departures", get(departures))
        .route("/departures/board/{stop_id}", get(board_departures))
        .route("/departures/board/{stop_id}/qr", get(board_qr))
        .route("/journeys/search", post(journey_search))
        .route("/realtime/trip/{trip_id}", get(realtime_trip))
        .route("/realtime/status", get(realtime_status))
        .route("/offline/packages", get(offline_packages))
        .route(
            "/offline/packages/{id}/metadata",
            get(offline_package_metadata),
        )
        .route(
            "/offline/packages/{id}/download",
            get(offline_package_download),
        )
        .route("/offline/packages/{id}/delta", get(offline_package_delta))
        .route("/tickets/recommendation", get(ticket_recommendation))
        .route("/tickets/quote", post(ticket_quote))
        .route("/admin/imports", get(admin_imports))
        .route("/admin/imports/{id}", get(admin_import))
        .route("/admin/imports/latest", get(admin_import_latest))
        .route("/admin/imports/ggu-latest/start", post(admin_import_start))
        .route("/admin/database/stats", get(admin_database_stats))
        .route("/admin/data-quality", get(admin_data_quality))
        .route("/admin/unmatched-stops", get(admin_unmatched_stops))
        .route("/admin/manual-stop-match", post(admin_manual_stop_match))
        .route("/admin/source-feeds", get(admin_source_feeds))
        .route("/admin/source-feeds/{id}", patch(admin_source_feed_patch))
        .route("/public/boards/{stop_id}", get(public_board))
        .route("/public/boards/{stop_id}/qr-metadata", get(public_board_qr))
        .layer(middleware::from_fn_with_state(state.clone(), auth_marker))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn auth_marker(
    State(_state): State<AppState>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    next.run(request).await
}

async fn health() -> Json<Value> {
    Json(json!({"status":"ok","service":"cesta-api"}))
}

async fn openapi() -> Json<Value> {
    Json(json!({
        "openapi": "3.1.0",
        "info": {"title": "Cesta API", "version": "0.1.0"},
        "paths": {
            "/health": {"get": {"summary": "Health check"}},
            "/auth/register": {"post": {"summary": "Register user"}},
            "/auth/login": {"post": {"summary": "Login user"}},
            "/stops/search": {"get": {
                "summary": "Search stops",
                "description": "Returns closest ranked stop suggestions using normalized, abbreviation and typo-tolerant matching.",
                "parameters": [
                    {"name": "q", "in": "query", "required": false, "schema": {"type": "string"}},
                    {"name": "limit", "in": "query", "required": false, "schema": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10}}
                ]
            }},
            "/departures": {"get": {"summary": "Stop departures"}},
            "/journeys/search": {"post": {"summary": "Search journeys"}},
            "/admin/imports/ggu-latest/start": {"post": {"summary": "Start GGU latest import"}},
            "/admin/database/stats": {"get": {"summary": "Database row counts and table sizes"}},
            "/public/boards/{stopId}": {"get": {"summary": "Public departure board data"}}
        }
    }))
}

async fn data_status(State(state): State<AppState>) -> Json<Value> {
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

async fn sources() -> Json<Value> {
    Json(json!({
        "sources": [
            {"id":"ggu_jdf_gtfs_latest","url":"https://data.jr.ggu.cz/results/latest/JDF_merged_GTFS.zip","priority":30,"type":"gtfs"},
            {"id":"ggu_czptt_gtfs_latest","url":"https://data.jr.ggu.cz/results/latest/CZPTT_GTFS.zip","priority":20,"type":"gtfs"},
            {"id":"ggu_jdf_raw_latest","url":"https://data.jr.ggu.cz/results/latest/JDF_merged.zip","priority":40,"type":"jdf_raw"}
        ]
    }))
}

async fn register(
    State(state): State<AppState>,
    Json(body): Json<RegisterRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let mut users = state.users.write().await;
    if users
        .values()
        .any(|user| user.email == body.email && user.deleted_at.is_none())
    {
        return Err(ApiError {
            code: "conflict".to_string(),
            message: "Email is already registered".to_string(),
        });
    }
    let user = create_user_record(
        &body.email,
        &body.password,
        body.display_name,
        vec!["user".to_string()],
    )
    .map_err(internal_error)?;
    let response = auth_response(&state, &user).await?;
    users.insert(user.id, user);
    Ok(Json(response))
}

async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let _device_name = body.device_name;
    let users = state.users.read().await;
    let user = users
        .values()
        .find(|user| user.email == body.email && user.deleted_at.is_none())
        .ok_or_else(unauthorized)?;
    verify_password(&body.password, &user.password_hash)?;
    Ok(Json(auth_response(&state, user).await?))
}

async fn refresh(
    State(state): State<AppState>,
    Json(body): Json<RefreshRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let token_hash = hash_token(&body.refresh_token);
    let user_id = state
        .refresh_tokens
        .read()
        .await
        .get(&token_hash)
        .copied()
        .ok_or_else(unauthorized)?;
    let users = state.users.read().await;
    let user = users.get(&user_id).ok_or_else(unauthorized)?;
    Ok(Json(auth_response(&state, user).await?))
}

async fn logout(
    State(state): State<AppState>,
    Json(body): Json<RefreshRequest>,
) -> Result<Json<Value>, ApiError> {
    state
        .refresh_tokens
        .write()
        .await
        .remove(&hash_token(&body.refresh_token));
    Ok(Json(json!({"status":"logged_out"})))
}

async fn auth_me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<PublicUser>, ApiError> {
    let user = current_user(&state, &headers).await?;
    Ok(Json(public_user(&user)))
}

async fn update_me(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<PublicUser>, ApiError> {
    let current = current_user(&state, &headers).await?;
    let mut users = state.users.write().await;
    let user = users.get_mut(&current.id).ok_or_else(unauthorized)?;
    if let Some(display_name) = body.get("display_name").and_then(Value::as_str) {
        user.display_name = Some(display_name.to_string());
    }
    Ok(Json(public_user(user)))
}

async fn delete_me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let current = current_user(&state, &headers).await?;
    if let Some(user) = state.users.write().await.get_mut(&current.id) {
        user.deleted_at = Some(Utc::now());
    }
    Ok(Json(json!({"status":"deleted"})))
}

async fn change_password() -> Json<Value> {
    Json(
        json!({"status":"not_implemented","warning":"password change endpoint is reserved for the database-backed auth flow"}),
    )
}

async fn profile(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    Ok(Json(json!({
        "user_id": user.id,
        "preferred_walking_speed": "normal",
        "prefer_fewer_transfers": false,
        "prefer_reliable_transfers": true,
        "default_departure_mode": "depart_at",
        "language": "cs",
        "accessibility_preferences": {}
    })))
}

async fn list_saved_places(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let places = state
        .saved_places
        .read()
        .await
        .get(&user.id)
        .cloned()
        .unwrap_or_default();
    Ok(Json(json!({"saved_places": places})))
}

async fn create_saved_place(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SavedPlaceRequest>,
) -> Result<Json<SavedPlace>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let now = Utc::now();
    let place = SavedPlace {
        id: Uuid::new_v4(),
        user_id: user.id,
        name: body.name,
        place_type: body.place_type,
        stop_id: body.stop_id,
        lat: body.lat,
        lon: body.lon,
        address: body.address,
        created_at: now,
        updated_at: now,
    };
    state
        .saved_places
        .write()
        .await
        .entry(user.id)
        .or_default()
        .push(place.clone());
    Ok(Json(place))
}

async fn update_saved_place() -> Json<Value> {
    Json(
        json!({"status":"not_implemented","warning":"PATCH saved place is reserved for repository-backed update"}),
    )
}

async fn delete_saved_place(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    state
        .saved_places
        .write()
        .await
        .entry(user.id)
        .or_default()
        .retain(|place| place.id != id);
    Ok(Json(json!({"status":"deleted"})))
}

async fn list_favorite_stops(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let favorites = state
        .favorite_stops
        .read()
        .await
        .get(&user.id)
        .cloned()
        .unwrap_or_default();
    Ok(Json(json!({"favorite_stops": favorites})))
}

async fn add_favorite_stop(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<FavoriteStopRequest>,
) -> Result<Json<FavoriteStop>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let favorite = FavoriteStop {
        id: Uuid::new_v4(),
        user_id: user.id,
        stop_id: body.stop_id,
        created_at: Utc::now(),
    };
    state
        .favorite_stops
        .write()
        .await
        .entry(user.id)
        .or_default()
        .push(favorite.clone());
    Ok(Json(favorite))
}

async fn delete_favorite_stop(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    state
        .favorite_stops
        .write()
        .await
        .entry(user.id)
        .or_default()
        .retain(|favorite| favorite.id != id);
    Ok(Json(json!({"status":"deleted"})))
}

async fn empty_user_collection() -> Json<Value> {
    Json(json!({"items":[],"warning":"endpoint shape is implemented; persistence is pending"}))
}

async fn notification_preferences() -> Json<Value> {
    Json(json!({"notification_preferences":[],"warning":"notification persistence is pending"}))
}

async fn search_stops(
    State(state): State<AppState>,
    Query(query): Query<StopSearchQuery>,
) -> Json<Value> {
    let q = query.q.unwrap_or_default();
    let normalized = normalize_search_text(&q);
    let limit = query.limit.unwrap_or(10).clamp(1, 50);
    if let Some(pool) = &state.db {
        return match search_stops_db(pool, &q, &normalized, limit).await {
            Ok(stops) => Json(json!({"stops": stops, "data_status": database_data_status()})),
            Err(error) => Json(json!({
                "stops": [],
                "data_status": {
                    "source": "database",
                    "schedule": "unknown",
                    "realtime": "unavailable",
                    "warnings": [format!("database stop search failed: {error}")]
                }
            })),
        };
    }

    let stops = ranked_stop_suggestions(state.stops.iter(), &normalized, limit);
    Json(json!({"stops": stops, "data_status": mock_status(state.use_mock_data)}))
}

fn ranked_stop_suggestions<'a>(
    stops: impl Iterator<Item = &'a Stop>,
    normalized_query: &str,
    limit: usize,
) -> Vec<Stop> {
    let mut scored_stops = stops
        .enumerate()
        .filter_map(|(index, stop)| {
            stop_search_score(stop, normalized_query).map(|score| (score, index, stop.clone()))
        })
        .collect::<Vec<_>>();
    scored_stops.sort_by(
        |(left_score, left_index, left_stop), (right_score, right_index, right_stop)| {
            right_score
                .cmp(left_score)
                .then_with(|| left_stop.name.cmp(&right_stop.name))
                .then_with(|| left_index.cmp(right_index))
        },
    );

    let mut seen = HashMap::new();
    scored_stops
        .into_iter()
        .map(|(_, _, stop)| stop)
        .filter(|stop| {
            let key = stop_suggestion_key(stop);
            if seen.contains_key(&key) {
                false
            } else {
                seen.insert(key, ());
                true
            }
        })
        .take(limit)
        .collect()
}

async fn nearby_stops(
    State(state): State<AppState>,
    Query(query): Query<NearbyQuery>,
) -> Json<Value> {
    let radius = query.radius.unwrap_or(1000.0);
    if let Some(pool) = &state.db {
        return match nearby_stops_db(pool, query.lat, query.lon, radius).await {
            Ok(stops) => Json(
                json!({"stops": stops, "radius": radius, "data_status": database_data_status()}),
            ),
            Err(error) => Json(json!({
                "stops": [],
                "radius": radius,
                "data_status": {
                    "source": "database",
                    "schedule": "unknown",
                    "realtime": "unavailable",
                    "warnings": [format!("database nearby stop query failed: {error}")]
                }
            })),
        };
    }

    let stops = state
        .stops
        .iter()
        .filter(|stop| {
            stop.lat.zip(stop.lon).is_some_and(|(lat, lon)| {
                let distance_m = haversine_m(query.lat, query.lon, lat, lon);
                distance_m <= radius
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    Json(json!({"stops": stops, "radius": radius, "data_status": mock_status(state.use_mock_data)}))
}

async fn stop_detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if let Some(pool) = &state.db {
        let stop = get_stop_db(pool, &id)
            .await
            .map_err(internal_error)?
            .ok_or_else(not_found)?;
        return Ok(Json(
            json!({"stop": stop, "data_status": database_data_status()}),
        ));
    }

    let stop = state
        .stops
        .iter()
        .find(|stop| stop.id == id)
        .ok_or_else(not_found)?;
    Ok(Json(
        json!({"stop": stop, "data_status": mock_status(state.use_mock_data)}),
    ))
}

async fn stop_area(Path(id): Path<String>) -> Json<Value> {
    Json(json!({"id": id, "warning": "stop area detail is pending imported stop-area data"}))
}

async fn departures(
    State(state): State<AppState>,
    Query(query): Query<DeparturesQuery>,
) -> Json<Value> {
    let limit = query.limit.unwrap_or(10);
    if let Some(pool) = &state.db {
        let earliest = query
            .time
            .as_deref()
            .and_then(parse_query_time_seconds)
            .unwrap_or(0);
        return match departures_db(pool, &query.stop_id, earliest, limit).await {
            Ok(departures) => Json(json!({
                "stop_id": query.stop_id,
                "departures": departures,
                "data_status": database_data_status()
            })),
            Err(error) => Json(json!({
                "stop_id": query.stop_id,
                "departures": [],
                "data_status": {
                    "source": "database",
                    "schedule": "unknown",
                    "realtime": "unavailable",
                    "warnings": [format!("database departures query failed: {error}")]
                }
            })),
        };
    }

    Json(json!({
        "stop_id": query.stop_id,
        "departures": fixture_departures().into_iter().take(limit).collect::<Vec<_>>(),
        "data_status": {
            "schedule": if state.use_mock_data { "mock" } else { "current" },
            "realtime": "unavailable",
            "warnings": if state.use_mock_data { vec!["fixture departures are in use"] } else { Vec::<&str>::new() }
        }
    }))
}

async fn board_departures(Path(stop_id): Path<String>) -> Json<Value> {
    Json(public_board_payload(&stop_id))
}

async fn board_qr(Path(stop_id): Path<String>) -> Json<Value> {
    Json(
        json!({"stop_id": stop_id, "qr_url": format!("https://cesta.local/boards/{stop_id}"), "mock": true}),
    )
}

async fn journey_search(
    State(state): State<AppState>,
    Json(body): Json<JourneySearchBody>,
) -> Result<Json<Value>, ApiError> {
    let departure_time = parse_journey_departure_seconds(&body.datetime)?;
    let _request_metadata = (
        &body.mode,
        &body.walking_speed,
        body.prefer_reliable_transfers,
        body.offline_compatible,
        body.from.lat,
        body.from.lon,
        body.to.lat,
        body.to.lon,
    );

    if let Some(pool) = &state.db {
        return match query_journeys_db(pool, &body, departure_time).await {
            Ok((journeys, warnings)) => Ok(Json(json!({
                "journeys": journeys,
                "data_status": database_data_status(),
                "warnings": warnings
            }))),
            Err(error) => Ok(Json(json!({
                "journeys": [],
                "data_status": {
                    "source": "database",
                    "schedule": "unknown",
                    "realtime": "unavailable",
                    "warnings": [format!("database journey search failed: {error}")]
                },
                "warnings": [format!("database journey search failed: {error}")]
            }))),
        };
    }

    let from_stop_id =
        resolve_journey_point_fixture(&state.stops, &body.from).unwrap_or_else(|| {
            body.from
                .id
                .clone()
                .unwrap_or_else(|| body.from.point_type.clone())
        });
    let to_stop_id = resolve_journey_point_fixture(&state.stops, &body.to).unwrap_or_else(|| {
        body.to
            .id
            .clone()
            .unwrap_or_else(|| body.to.point_type.clone())
    });
    let mut journeys = earliest_arrivals(
        &fixture_snapshot(),
        RoutingSearchRequest {
            from_stop_id: from_stop_id.clone(),
            to_stop_id: to_stop_id.clone(),
            departure_time,
            max_transfers: body.max_transfers,
            modes: body.transport_modes.clone(),
        },
    );
    let mut warnings = if state.use_mock_data {
        vec!["routing uses fixture snapshot until imported snapshots are wired".to_string()]
    } else {
        Vec::new()
    };
    if journeys.is_empty() && departure_time > 0 {
        journeys = earliest_arrivals(
            &fixture_snapshot(),
            RoutingSearchRequest {
                from_stop_id,
                to_stop_id,
                departure_time: 0,
                max_transfers: body.max_transfers,
                modes: body.transport_modes,
            },
        );
        if !journeys.is_empty() {
            warnings.push(
                "no departures were found after the requested time; returned earliest service-day journeys"
                    .to_string(),
            );
        }
    }
    Ok(Json(json!({
        "journeys": journeys,
        "data_status": {
            "schedule": if state.use_mock_data { "mock" } else { "current" },
            "realtime": "unavailable",
            "offline_compatible": true,
            "valid_until": "2026-12-31"
        },
        "warnings": warnings
    })))
}

fn parse_journey_departure_seconds(datetime: &str) -> Result<u32, ApiError> {
    if let Ok(value) = chrono::DateTime::parse_from_rfc3339(datetime) {
        return Ok(seconds_since_midnight(value.time()));
    }

    if let Ok(value) = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%dT%H:%M:%S") {
        return Ok(seconds_since_midnight(value.time()));
    }

    if let Ok(value) = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S") {
        return Ok(seconds_since_midnight(value.time()));
    }

    if let Ok(value) = NaiveTime::parse_from_str(datetime, "%H:%M:%S") {
        return Ok(seconds_since_midnight(value));
    }

    Err(ApiError {
        code: "invalid_datetime".to_string(),
        message: "datetime must be RFC3339, YYYY-MM-DDTHH:MM:SS, YYYY-MM-DD HH:MM:SS, or HH:MM:SS"
            .to_string(),
    })
}

fn seconds_since_midnight(time: NaiveTime) -> u32 {
    time.num_seconds_from_midnight()
}

async fn realtime_trip(Path(trip_id): Path<String>) -> Json<Value> {
    Json(
        json!({"trip_id": trip_id, "updates": [], "realtime_status": "unavailable", "mock": false}),
    )
}

async fn realtime_status() -> Json<Value> {
    Json(
        json!({"status":"unavailable","sources":[],"mock_worker_available":true,"warning":"real realtime feeds are not connected yet"}),
    )
}

async fn offline_packages() -> Json<Value> {
    Json(json!({"packages": offline_pack::development_packages()}))
}

async fn offline_package_metadata(Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    let package = package_by_id(&id)?;
    Ok(Json(offline_pack::package_manifest(&package)))
}

async fn offline_package_download(Path(id): Path<String>) -> Json<Value> {
    Json(
        json!({"id": id, "status":"not_available", "warning":"offline package binary generation is pending"}),
    )
}

async fn offline_package_delta(Path(id): Path<String>) -> Json<Value> {
    Json(
        json!({"id": id, "status":"not_available", "warning":"delta packages are planned for a later phase"}),
    )
}

async fn ticket_recommendation() -> Json<Value> {
    Json(
        json!({"options": [mock_ticket()], "mock": true, "warning": "ticket purchase and payment are out of scope"}),
    )
}

async fn ticket_quote() -> Json<Value> {
    Json(json!({"quote": mock_ticket(), "mock": true, "payment_enabled": false}))
}

async fn admin_imports(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(
        json!({"imports": [], "warning": "database import run repository is pending"}),
    ))
}

async fn admin_import(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({"id": id, "status": "unknown"})))
}

async fn admin_import_latest(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(
        json!({"latest": null, "warning": "run data is produced by data-pipeline summarize latest"}),
    ))
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
    Ok(Json(json!({
        "validation_issue_counts": {"warning": 0, "error": 0},
        "unmatched_stops": 0,
        "duplicate_candidates": 0,
        "latest_log_summary": null,
        "mock": state.use_mock_data
    })))
}

async fn admin_unmatched_stops(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({"stops": []})))
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
    Ok(sources().await)
}

async fn admin_source_feed_patch(
    Path(id): Path<String>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({"id": id, "status":"not_implemented"})))
}

async fn public_board(Path(stop_id): Path<String>) -> Json<Value> {
    Json(public_board_payload(&stop_id))
}

async fn public_board_qr(Path(stop_id): Path<String>) -> Json<Value> {
    Json(
        json!({"stop_id": stop_id, "board_url": format!("https://cesta.local/public/boards/{stop_id}"), "theme": "default", "mock": true}),
    )
}

fn create_user_record(
    email: &str,
    password: &str,
    display_name: Option<String>,
    roles: Vec<String>,
) -> anyhow::Result<UserRecord> {
    let salt = SaltString::generate(&mut OsRng);
    let password_hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .to_string();
    Ok(UserRecord {
        id: Uuid::new_v4(),
        email: email.to_string(),
        password_hash,
        display_name,
        roles,
        created_at: Utc::now(),
        deleted_at: None,
    })
}

fn verify_password(password: &str, password_hash: &str) -> Result<(), ApiError> {
    let parsed = PasswordHash::new(password_hash).map_err(|_| unauthorized())?;
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .map_err(|_| unauthorized())
}

async fn auth_response(state: &AppState, user: &UserRecord) -> Result<AuthResponse, ApiError> {
    let expires_at = Utc::now() + Duration::minutes(15);
    let claims = Claims {
        sub: user.id.to_string(),
        email: user.email.clone(),
        roles: user.roles.clone(),
        exp: expires_at.timestamp() as usize,
    };
    let access_token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(state.jwt_secret.as_bytes()),
    )
    .map_err(internal_error)?;
    let refresh_token = Uuid::new_v4().to_string();
    state
        .refresh_tokens
        .write()
        .await
        .insert(hash_token(&refresh_token), user.id);
    Ok(AuthResponse {
        access_token,
        refresh_token,
        token_type: "Bearer".to_string(),
        expires_in_seconds: 900,
        user: public_user(user),
    })
}

async fn current_user(state: &AppState, headers: &HeaderMap) -> Result<UserRecord, ApiError> {
    let token = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(unauthorized)?;
    let claims = decode::<Claims>(
        token,
        &DecodingKey::from_secret(state.jwt_secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| unauthorized())?
    .claims;
    let id = Uuid::parse_str(&claims.sub).map_err(|_| unauthorized())?;
    state
        .users
        .read()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(unauthorized)
}

async fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<UserRecord, ApiError> {
    let user = current_user(state, headers).await?;
    if user
        .roles
        .iter()
        .any(|role| role == "admin" || role == "data_admin")
    {
        Ok(user)
    } else {
        Err(ApiError {
            code: "forbidden".to_string(),
            message: "Admin role is required".to_string(),
        })
    }
}

fn public_user(user: &UserRecord) -> PublicUser {
    let _created_at = user.created_at;
    PublicUser {
        id: user.id,
        email: user.email.clone(),
        display_name: user.display_name.clone(),
        roles: user.roles.clone(),
    }
}

fn hash_token(value: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(value.as_bytes())
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
    let has_successful_import = latest.is_some();

    Ok(json!({
        "schedule": if has_successful_import { "current" } else { "unknown" },
        "realtime": "unavailable",
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
) -> Result<(Vec<Journey>, Vec<String>), sqlx::Error> {
    let mut warnings = Vec::new();
    let (from_stop_ids, from_warning) = resolve_journey_point_db(pool, &body.from).await?;
    let (to_stop_ids, to_warning) = resolve_journey_point_db(pool, &body.to).await?;
    warnings.extend(from_warning);
    warnings.extend(to_warning);

    if from_stop_ids.is_empty() || to_stop_ids.is_empty() {
        warnings.push("one or both journey stops could not be resolved".to_string());
        return Ok((Vec::new(), warnings));
    }

    let mode_filters = body
        .transport_modes
        .iter()
        .filter_map(transport_mode_to_db)
        .collect::<Vec<_>>();
    let mut journeys = direct_journeys_db(
        pool,
        &from_stop_ids,
        &to_stop_ids,
        departure_time,
        &mode_filters,
    )
    .await?;
    if body.max_transfers > 0 {
        journeys.append(
            &mut one_transfer_journeys_db(
                pool,
                &from_stop_ids,
                &to_stop_ids,
                departure_time,
                &mode_filters,
            )
            .await?,
        );
    }

    if departure_time > 0 {
        let mut next_service_day_journeys =
            direct_journeys_db(pool, &from_stop_ids, &to_stop_ids, 0, &mode_filters).await?;
        if body.max_transfers > 0 {
            next_service_day_journeys.append(
                &mut one_transfer_journeys_db(pool, &from_stop_ids, &to_stop_ids, 0, &mode_filters)
                    .await?,
            );
        }
        let mut next_service_day_journeys =
            next_service_day_journey_results(next_service_day_journeys, departure_time);
        if !next_service_day_journeys.is_empty() {
            journeys.append(&mut next_service_day_journeys);
            warnings.push(
                "included next service-day journeys because early-morning departures occur after the requested time"
                    .to_string(),
            );
        }
    }
    journeys = ranked_journey_results(journeys);

    if journeys.is_empty() {
        warnings.push("no database journeys found for the resolved stops".to_string());
    }

    Ok((journeys, warnings))
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

fn ranked_journey_results(mut journeys: Vec<Journey>) -> Vec<Journey> {
    journeys.sort_by_key(journey_arrival_rank);

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
    push_ranked_journey(&mut selected, &mut selected_keys, &candidates[0]);

    if let Some(simplest) = candidates.iter().min_by_key(|journey| {
        (
            journey.transfer_count,
            journey.arrival_time,
            journey.duration_seconds,
            journey.departure_time,
        )
    }) {
        push_ranked_journey(&mut selected, &mut selected_keys, simplest);
    }

    let mut transfer_counts = candidates
        .iter()
        .map(|journey| journey.transfer_count)
        .collect::<Vec<_>>();
    transfer_counts.sort_unstable();
    transfer_counts.dedup();

    for transfer_count in transfer_counts {
        if let Some(best_for_transfer_count) = candidates
            .iter()
            .filter(|journey| journey.transfer_count == transfer_count)
            .min_by_key(|journey| journey_arrival_rank(journey))
        {
            push_ranked_journey(&mut selected, &mut selected_keys, best_for_transfer_count);
        }
    }

    for journey in &candidates {
        push_ranked_journey(&mut selected, &mut selected_keys, journey);
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
    selected.sort_by_key(journey_arrival_rank);
    selected
        .into_iter()
        .enumerate()
        .map(|(index, mut journey)| {
            journey.id = format!("journey-{}", index + 1);
            journey
                .labels
                .retain(|label| label != "nejrychlejsi" && label != "nejjednodussi");
            if index == 0 {
                journey.labels.push("nejrychlejsi".to_string());
            }
            if simplest_key.as_ref() == Some(&journey_identity_key(&journey)) {
                journey.labels.push("nejjednodussi".to_string());
            }
            journey
        })
        .collect()
}

fn push_ranked_journey(
    selected: &mut Vec<Journey>,
    selected_keys: &mut HashSet<String>,
    journey: &Journey,
) {
    if selected.len() >= MAX_JOURNEY_RESULTS {
        return;
    }

    let key = journey_identity_key(journey);
    if selected_keys.insert(key) {
        selected.push(journey.clone());
    }
}

fn journey_arrival_rank(journey: &Journey) -> (u32, u32, u32, u32) {
    (
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
    let Some((base, suffix)) = stop_id.rsplit_once('-') else {
        return stop_id.to_string();
    };

    let looks_like_platform = stop_id.contains("SR70S-CZ-")
        && suffix.len() <= 4
        && suffix.chars().next().is_some_and(|ch| ch.is_ascii_digit())
        && suffix.chars().all(|ch| ch.is_ascii_alphanumeric());
    if looks_like_platform {
        base.to_string()
    } else {
        stop_id.to_string()
    }
}

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
    } else if let Some((lat, lon)) = stop.lat.zip(stop.lon) {
        let mut sibling_ids = sqlx::query_scalar::<_, String>(
            r#"
            SELECT id
            FROM stops
            WHERE is_active = true
              AND normalized_name = $1
              AND lat IS NOT NULL
              AND lon IS NOT NULL
              AND abs(lat - $2) < 0.00005
              AND abs(lon - $3) < 0.00005
            LIMIT 250
            "#,
        )
        .bind(&stop.normalized_name)
        .bind(lat)
        .bind(lon)
        .fetch_all(pool)
        .await?;
        ids.append(&mut sibling_ids);
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
) -> Result<Vec<Journey>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        WITH latest_import_runs AS (
          SELECT DISTINCT ON (summary->>'feed_id')
            summary->>'feed_id' AS source_feed_id,
            id AS import_run_id
          FROM import_runs
          WHERE status = 'success'
            AND summary ? 'feed_id'
          ORDER BY summary->>'feed_id', finished_at DESC NULLS LAST, started_at DESC
        ),
        current_train_route_variants AS (
          SELECT route_id
          FROM (
            SELECT
              id AS route_id,
              row_number() OVER (
                PARTITION BY source_feed_id, regexp_replace(source_id, 'CZTRAINR-[0-9]{4}-', 'CZTRAINR-')
                ORDER BY substring(source_id from 'CZTRAINR-([0-9]{4})-')::integer DESC, id DESC
              ) AS route_variant_rank
            FROM routes
            WHERE COALESCE(source_id, '') ~ 'CZTRAINR-[0-9]{4}-'
          ) ranked_route_variants
          WHERE route_variant_rank = 1
        ),
        candidate_legs AS (
          SELECT
            st_from.trip_id,
            r.id AS route_id,
            st_from.stop_id AS from_stop_id,
            st_to.stop_id AS to_stop_id,
            st_from.departure_time,
            st_to.arrival_time,
            lir.import_run_id IS NOT NULL AS from_latest_import,
            CASE
              WHEN lower(r.mode) IN ('train', 'rail') OR r.gtfs_route_type = 2 OR lower(r.id) LIKE '%train%' OR lower(r.source_id) LIKE '%train%' THEN 'train'
              WHEN lower(r.mode) = 'tram' OR r.gtfs_route_type = 0 THEN 'tram'
              WHEN lower(r.mode) = 'metro' OR r.gtfs_route_type = 1 THEN 'metro'
              WHEN lower(r.mode) = 'bus' OR r.gtfs_route_type = 3 THEN 'bus'
              WHEN lower(r.mode) = 'ferry' OR r.gtfs_route_type = 4 THEN 'ferry'
              WHEN lower(r.mode) IN ('cable_car', 'cablecar') OR r.gtfs_route_type = 5 THEN 'cable_car'
              WHEN lower(r.mode) = 'trolleybus' OR r.gtfs_route_type = 11 THEN 'trolleybus'
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
              COALESCE(r.source_id, '') !~ 'CZTRAINR-[0-9]{4}-'
              OR r.id IN (SELECT route_id FROM current_train_route_variants)
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
          public_mode AS mode
        FROM candidate_legs
        WHERE public_mode <> 'unknown'
          AND ($4 = false OR public_mode = ANY($5))
          AND (
            from_latest_import
            OR NOT EXISTS (
              SELECT 1 FROM candidate_legs latest_candidates WHERE latest_candidates.from_latest_import
            )
          )
        ORDER BY arrival_time ASC, departure_time ASC
        LIMIT 5
        "#,
    )
    .bind(from_stop_ids.to_vec())
    .bind(to_stop_ids.to_vec())
    .bind(departure_time as i32)
    .bind(!mode_filters.is_empty())
    .bind(mode_filters.to_vec())
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

async fn one_transfer_journeys_db(
    pool: &PgPool,
    from_stop_ids: &[String],
    to_stop_ids: &[String],
    departure_time: u32,
    mode_filters: &[String],
) -> Result<Vec<Journey>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        WITH latest_import_runs AS (
          SELECT DISTINCT ON (summary->>'feed_id')
            summary->>'feed_id' AS source_feed_id,
            id AS import_run_id
          FROM import_runs
          WHERE status = 'success'
            AND summary ? 'feed_id'
          ORDER BY summary->>'feed_id', finished_at DESC NULLS LAST, started_at DESC
        ),
        current_train_route_variants AS (
          SELECT route_id
          FROM (
            SELECT
              id AS route_id,
              row_number() OVER (
                PARTITION BY source_feed_id, regexp_replace(source_id, 'CZTRAINR-[0-9]{4}-', 'CZTRAINR-')
                ORDER BY substring(source_id from 'CZTRAINR-([0-9]{4})-')::integer DESC, id DESC
              ) AS route_variant_rank
            FROM routes
            WHERE COALESCE(source_id, '') ~ 'CZTRAINR-[0-9]{4}-'
          ) ranked_route_variants
          WHERE route_variant_rank = 1
        ),
        first_legs AS (
          SELECT
            st_from.trip_id AS first_trip_id,
            r.id AS first_route_id,
            st_from.stop_id AS first_from_stop_id,
            st_mid.stop_id AS transfer_arrival_stop_id,
            st_from.departure_time AS first_departure_time,
            st_mid.arrival_time AS first_arrival_time,
            s_mid.normalized_name AS transfer_normalized_name,
            s_mid.lat AS transfer_lat,
            s_mid.lon AS transfer_lon,
            lir.import_run_id IS NOT NULL AS first_from_latest_import,
            CASE
              WHEN lower(r.mode) IN ('train', 'rail') OR r.gtfs_route_type = 2 OR lower(r.id) LIKE '%train%' OR lower(r.source_id) LIKE '%train%' THEN 'train'
              WHEN lower(r.mode) = 'tram' OR r.gtfs_route_type = 0 THEN 'tram'
              WHEN lower(r.mode) = 'metro' OR r.gtfs_route_type = 1 THEN 'metro'
              WHEN lower(r.mode) = 'bus' OR r.gtfs_route_type = 3 THEN 'bus'
              WHEN lower(r.mode) = 'ferry' OR r.gtfs_route_type = 4 THEN 'ferry'
              WHEN lower(r.mode) IN ('cable_car', 'cablecar') OR r.gtfs_route_type = 5 THEN 'cable_car'
              WHEN lower(r.mode) = 'trolleybus' OR r.gtfs_route_type = 11 THEN 'trolleybus'
              ELSE 'unknown'
            END AS first_mode
          FROM stop_times st_from
          JOIN stop_times st_mid
            ON st_mid.trip_id = st_from.trip_id
           AND st_mid.stop_sequence > st_from.stop_sequence
          JOIN stops s_mid ON s_mid.id = st_mid.stop_id
          JOIN trips t ON t.id = st_from.trip_id
          LEFT JOIN latest_import_runs lir
            ON lir.source_feed_id = t.source_feed_id
           AND lir.import_run_id = t.import_run_id
          JOIN routes r ON r.id = t.route_id
          WHERE st_from.stop_id = ANY($1)
            AND st_from.departure_time >= $3
            AND (
              COALESCE(r.source_id, '') !~ 'CZTRAINR-[0-9]{4}-'
              OR r.id IN (SELECT route_id FROM current_train_route_variants)
            )
            AND COALESCE(st_from.pickup_type, 0) = 0
            AND COALESCE(st_mid.drop_off_type, 0) = 0
          ORDER BY st_mid.arrival_time ASC
          LIMIT 500
        ),
        transfer_stops AS (
          SELECT
            first_legs.*,
            s_transfer.id AS transfer_departure_stop_id
          FROM first_legs
          JOIN stops s_transfer
            ON s_transfer.is_active = true
           AND (
             s_transfer.id = first_legs.transfer_arrival_stop_id
             OR (
               s_transfer.normalized_name = first_legs.transfer_normalized_name
               AND s_transfer.lat IS NOT NULL
               AND s_transfer.lon IS NOT NULL
               AND first_legs.transfer_lat IS NOT NULL
               AND first_legs.transfer_lon IS NOT NULL
               AND abs(s_transfer.lat - first_legs.transfer_lat) < 0.00005
               AND abs(s_transfer.lon - first_legs.transfer_lon) < 0.00005
             )
           )
          WHERE first_mode <> 'unknown'
            AND ($4 = false OR first_mode = ANY($5))
        ),
        candidate_journeys AS (
          SELECT
            transfer_stops.first_trip_id,
            transfer_stops.first_route_id,
            transfer_stops.first_from_stop_id,
            transfer_stops.transfer_arrival_stop_id,
            transfer_stops.first_departure_time,
            transfer_stops.first_arrival_time,
            transfer_stops.first_mode,
            st_transfer.trip_id AS second_trip_id,
            r2.id AS second_route_id,
            st_transfer.stop_id AS transfer_departure_stop_id,
            st_to.stop_id AS second_to_stop_id,
            st_transfer.departure_time AS second_departure_time,
            st_to.arrival_time AS second_arrival_time,
            transfer_stops.first_from_latest_import
              AND lir2.import_run_id IS NOT NULL AS from_latest_import,
            CASE
              WHEN lower(r2.mode) IN ('train', 'rail') OR r2.gtfs_route_type = 2 OR lower(r2.id) LIKE '%train%' OR lower(r2.source_id) LIKE '%train%' THEN 'train'
              WHEN lower(r2.mode) = 'tram' OR r2.gtfs_route_type = 0 THEN 'tram'
              WHEN lower(r2.mode) = 'metro' OR r2.gtfs_route_type = 1 THEN 'metro'
              WHEN lower(r2.mode) = 'bus' OR r2.gtfs_route_type = 3 THEN 'bus'
              WHEN lower(r2.mode) = 'ferry' OR r2.gtfs_route_type = 4 THEN 'ferry'
              WHEN lower(r2.mode) IN ('cable_car', 'cablecar') OR r2.gtfs_route_type = 5 THEN 'cable_car'
              WHEN lower(r2.mode) = 'trolleybus' OR r2.gtfs_route_type = 11 THEN 'trolleybus'
              ELSE 'unknown'
            END AS second_mode
          FROM transfer_stops
          JOIN stop_times st_transfer ON st_transfer.stop_id = transfer_stops.transfer_departure_stop_id
          JOIN stop_times st_to
            ON st_to.trip_id = st_transfer.trip_id
           AND st_to.stop_sequence > st_transfer.stop_sequence
          JOIN trips t2 ON t2.id = st_transfer.trip_id
          LEFT JOIN latest_import_runs lir2
            ON lir2.source_feed_id = t2.source_feed_id
           AND lir2.import_run_id = t2.import_run_id
          JOIN routes r2 ON r2.id = t2.route_id
          WHERE st_to.stop_id = ANY($2)
            AND st_transfer.trip_id <> transfer_stops.first_trip_id
            AND st_transfer.departure_time >= transfer_stops.first_arrival_time + $6
            AND st_transfer.departure_time <= transfer_stops.first_arrival_time + $7
            AND (
              COALESCE(r2.source_id, '') !~ 'CZTRAINR-[0-9]{4}-'
              OR r2.id IN (SELECT route_id FROM current_train_route_variants)
            )
            AND COALESCE(st_transfer.pickup_type, 0) = 0
            AND COALESCE(st_to.drop_off_type, 0) = 0
        )
        SELECT *
        FROM candidate_journeys
        WHERE second_mode <> 'unknown'
          AND ($4 = false OR second_mode = ANY($5))
          AND (
            from_latest_import
            OR NOT EXISTS (
              SELECT 1 FROM candidate_journeys latest_candidates WHERE latest_candidates.from_latest_import
            )
          )
        ORDER BY second_arrival_time ASC, first_departure_time ASC
        LIMIT 20
        "#,
    )
    .bind(from_stop_ids.to_vec())
    .bind(to_stop_ids.to_vec())
    .bind(departure_time as i32)
    .bind(!mode_filters.is_empty())
    .bind(mode_filters.to_vec())
    .bind(MIN_TRANSFER_SECONDS as i32)
    .bind(MAX_TRANSFER_WAIT_SECONDS as i32)
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
          r.mode
        FROM stop_times st
        JOIN trips t ON t.id = st.trip_id
        JOIN routes r ON r.id = t.route_id
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
                "realtime_departure": null,
                "delay_seconds": null,
                "status": "scheduled"
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

fn database_data_status() -> Value {
    json!({
        "source": "database",
        "schedule": "current",
        "realtime": "unavailable",
        "warnings": Vec::<String>::new()
    })
}

fn resolve_journey_point_fixture(stops: &[Stop], point: &JourneyPoint) -> Option<String> {
    let candidate = point
        .id
        .as_deref()
        .filter(|value| !value.trim().is_empty())?;

    if stops.iter().any(|stop| stop.id == candidate) {
        return Some(canonical_stop_id(stops, candidate));
    }

    ranked_stop_suggestions(stops.iter(), &normalize_search_text(candidate), 1)
        .into_iter()
        .next()
        .map(|stop| stop.id)
}

fn canonical_stop_id(stops: &[Stop], stop_id: &str) -> String {
    let Some(stop) = stops.iter().find(|stop| stop.id == stop_id) else {
        return stop_id.to_string();
    };

    if stop.platform_code.is_none() {
        return stop.id.clone();
    }

    let key = stop_suggestion_key(stop);
    stops
        .iter()
        .find(|candidate| {
            candidate.platform_code.is_none() && stop_suggestion_key(candidate) == key
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

fn stop_suggestion_key(stop: &Stop) -> String {
    let name = normalize_search_text(&stop.name);
    if let Some(stop_area_id) = &stop.stop_area_id {
        return format!("area:{stop_area_id}:{name}");
    }

    match stop.lat.zip(stop.lon) {
        Some((lat, lon)) => format!("{name}:{lat:.5}:{lon:.5}"),
        None => format!(
            "{name}:{}",
            stop.municipality.as_deref().unwrap_or_default()
        ),
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

fn fixture_stop(id: &str, name: &str, lat: f64, lon: f64, mode: TransportMode) -> Stop {
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
        municipality: None,
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
        http::Request,
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
        let stops = vec![platform, station];

        let suggestions = ranked_stop_suggestions(stops.iter(), "praha", 6);

        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].id, "stop-praha-hl-n");
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
}
