use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};

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
use chrono::{Duration, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use routing_core::{SearchRequest as RoutingSearchRequest, earliest_arrivals, fixture_snapshot};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use transit_model::{
    CoordinateConfidence, OfflinePackage, Stop, TicketOption, TransportMode, normalize_czech_name,
};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    users: Arc<RwLock<HashMap<Uuid, UserRecord>>>,
    refresh_tokens: Arc<RwLock<HashMap<String, Uuid>>>,
    saved_places: Arc<RwLock<HashMap<Uuid, Vec<SavedPlace>>>>,
    favorite_stops: Arc<RwLock<HashMap<Uuid, Vec<FavoriteStop>>>>,
    stops: Arc<Vec<Stop>>,
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
        .unwrap_or(8080);
    let app = app_state().await?;
    let router = build_router(app);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!(%addr, "starting Cesta API");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

async fn app_state() -> anyhow::Result<AppState> {
    let state = AppState {
        users: Arc::new(RwLock::new(HashMap::new())),
        refresh_tokens: Arc::new(RwLock::new(HashMap::new())),
        saved_places: Arc::new(RwLock::new(HashMap::new())),
        favorite_stops: Arc::new(RwLock::new(HashMap::new())),
        stops: Arc::new(fixture_stops()),
        jwt_secret: env::var("JWT_SECRET").unwrap_or_else(|_| "dev-only-change-me".to_string()),
        use_mock_data: env::var("USE_MOCK_DATA").map(|value| value == "true").unwrap_or(true),
    };

    if let (Ok(email), Ok(password)) = (
        env::var("ADMIN_BOOTSTRAP_EMAIL"),
        env::var("ADMIN_BOOTSTRAP_PASSWORD"),
    ) {
        if !email.is_empty() && !password.is_empty() {
            let user = create_user_record(&email, &password, Some("Admin".to_string()), vec!["admin".to_string(), "data_admin".to_string()])?;
            state.users.write().await.insert(user.id, user);
        }
    }
    Ok(state)
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
        .route("/me/saved-places", get(list_saved_places).post(create_saved_place))
        .route("/me/saved-places/{id}", patch(update_saved_place).delete(delete_saved_place))
        .route("/me/favorite-stops", get(list_favorite_stops).post(add_favorite_stop))
        .route("/me/favorite-stops/{id}", delete(delete_favorite_stop))
        .route("/me/favorite-routes", get(empty_user_collection).post(empty_user_collection))
        .route("/me/favorite-routes/{id}", delete(empty_user_collection))
        .route("/me/notification-preferences", get(notification_preferences).patch(notification_preferences))
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
        .route("/offline/packages/{id}/metadata", get(offline_package_metadata))
        .route("/offline/packages/{id}/download", get(offline_package_download))
        .route("/offline/packages/{id}/delta", get(offline_package_delta))
        .route("/tickets/recommendation", get(ticket_recommendation))
        .route("/tickets/quote", post(ticket_quote))
        .route("/admin/imports", get(admin_imports))
        .route("/admin/imports/{id}", get(admin_import))
        .route("/admin/imports/latest", get(admin_import_latest))
        .route("/admin/imports/ggu-latest/start", post(admin_import_start))
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

async fn auth_marker(State(_state): State<AppState>, request: axum::http::Request<axum::body::Body>, next: Next) -> Response {
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
            "/stops/search": {"get": {"summary": "Search stops"}},
            "/departures": {"get": {"summary": "Stop departures"}},
            "/journeys/search": {"post": {"summary": "Search journeys"}},
            "/admin/imports/ggu-latest/start": {"post": {"summary": "Start GGU latest import"}},
            "/public/boards/{stopId}": {"get": {"summary": "Public departure board data"}}
        }
    }))
}

async fn data_status(State(state): State<AppState>) -> Json<Value> {
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
    if users.values().any(|user| user.email == body.email && user.deleted_at.is_none()) {
        return Err(ApiError { code: "conflict".to_string(), message: "Email is already registered".to_string() });
    }
    let user = create_user_record(&body.email, &body.password, body.display_name, vec!["user".to_string()])
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
    state.refresh_tokens.write().await.remove(&hash_token(&body.refresh_token));
    Ok(Json(json!({"status":"logged_out"})))
}

async fn auth_me(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<PublicUser>, ApiError> {
    let user = current_user(&state, &headers).await?;
    Ok(Json(public_user(&user)))
}

async fn update_me(State(state): State<AppState>, headers: HeaderMap, Json(body): Json<Value>) -> Result<Json<PublicUser>, ApiError> {
    let current = current_user(&state, &headers).await?;
    let mut users = state.users.write().await;
    let user = users.get_mut(&current.id).ok_or_else(unauthorized)?;
    if let Some(display_name) = body.get("display_name").and_then(Value::as_str) {
        user.display_name = Some(display_name.to_string());
    }
    Ok(Json(public_user(user)))
}

async fn delete_me(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, ApiError> {
    let current = current_user(&state, &headers).await?;
    if let Some(user) = state.users.write().await.get_mut(&current.id) {
        user.deleted_at = Some(Utc::now());
    }
    Ok(Json(json!({"status":"deleted"})))
}

async fn change_password() -> Json<Value> {
    Json(json!({"status":"not_implemented","warning":"password change endpoint is reserved for the database-backed auth flow"}))
}

async fn profile(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, ApiError> {
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

async fn list_saved_places(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let places = state.saved_places.read().await.get(&user.id).cloned().unwrap_or_default();
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
    state.saved_places.write().await.entry(user.id).or_default().push(place.clone());
    Ok(Json(place))
}

async fn update_saved_place() -> Json<Value> {
    Json(json!({"status":"not_implemented","warning":"PATCH saved place is reserved for repository-backed update"}))
}

async fn delete_saved_place(State(state): State<AppState>, headers: HeaderMap, Path(id): Path<Uuid>) -> Result<Json<Value>, ApiError> {
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

async fn list_favorite_stops(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let favorites = state.favorite_stops.read().await.get(&user.id).cloned().unwrap_or_default();
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
    state.favorite_stops.write().await.entry(user.id).or_default().push(favorite.clone());
    Ok(Json(favorite))
}

async fn delete_favorite_stop(State(state): State<AppState>, headers: HeaderMap, Path(id): Path<Uuid>) -> Result<Json<Value>, ApiError> {
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

async fn search_stops(State(state): State<AppState>, Query(query): Query<StopSearchQuery>) -> Json<Value> {
    let q = query.q.unwrap_or_default();
    let normalized = normalize_czech_name(&q);
    let stops = state
        .stops
        .iter()
        .filter(|stop| normalized.is_empty() || stop.normalized_name.contains(&normalized))
        .cloned()
        .collect::<Vec<_>>();
    Json(json!({"stops": stops, "data_status": mock_status(state.use_mock_data)}))
}

async fn nearby_stops(State(state): State<AppState>, Query(query): Query<NearbyQuery>) -> Json<Value> {
    let radius = query.radius.unwrap_or(1000.0);
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

async fn stop_detail(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    let stop = state.stops.iter().find(|stop| stop.id == id).ok_or_else(not_found)?;
    Ok(Json(json!({"stop": stop, "data_status": mock_status(state.use_mock_data)})))
}

async fn stop_area(Path(id): Path<String>) -> Json<Value> {
    Json(json!({"id": id, "warning": "stop area detail is pending imported stop-area data"}))
}

async fn departures(State(state): State<AppState>, Query(query): Query<DeparturesQuery>) -> Json<Value> {
    let _time = query.time;
    let limit = query.limit.unwrap_or(10);
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
    Json(json!({"stop_id": stop_id, "qr_url": format!("https://cesta.local/boards/{stop_id}"), "mock": true}))
}

async fn journey_search(State(state): State<AppState>, Json(body): Json<JourneySearchBody>) -> Json<Value> {
    let _request_metadata = (&body.datetime, &body.mode, &body.walking_speed, body.prefer_reliable_transfers, body.offline_compatible, body.from.lat, body.from.lon, body.to.lat, body.to.lon);
    let from_stop_id = body.from.id.unwrap_or_else(|| body.from.point_type);
    let to_stop_id = body.to.id.unwrap_or_else(|| body.to.point_type);
    let journeys = earliest_arrivals(
        &fixture_snapshot(),
        RoutingSearchRequest {
            from_stop_id,
            to_stop_id,
            departure_time: 7 * 3600,
            max_transfers: body.max_transfers,
            modes: body.transport_modes,
        },
    );
    Json(json!({
        "journeys": journeys,
        "data_status": {
            "schedule": if state.use_mock_data { "mock" } else { "current" },
            "realtime": "unavailable",
            "offline_compatible": true,
            "valid_until": "2026-12-31"
        },
        "warnings": if state.use_mock_data { vec!["routing uses fixture snapshot until imported snapshots are wired"] } else { Vec::<&str>::new() }
    }))
}

async fn realtime_trip(Path(trip_id): Path<String>) -> Json<Value> {
    Json(json!({"trip_id": trip_id, "updates": [], "realtime_status": "unavailable", "mock": false}))
}

async fn realtime_status() -> Json<Value> {
    Json(json!({"status":"unavailable","sources":[],"mock_worker_available":true,"warning":"real realtime feeds are not connected yet"}))
}

async fn offline_packages() -> Json<Value> {
    Json(json!({"packages": offline_pack::development_packages()}))
}

async fn offline_package_metadata(Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    let package = package_by_id(&id)?;
    Ok(Json(offline_pack::package_manifest(&package)))
}

async fn offline_package_download(Path(id): Path<String>) -> Json<Value> {
    Json(json!({"id": id, "status":"not_available", "warning":"offline package binary generation is pending"}))
}

async fn offline_package_delta(Path(id): Path<String>) -> Json<Value> {
    Json(json!({"id": id, "status":"not_available", "warning":"delta packages are planned for a later phase"}))
}

async fn ticket_recommendation() -> Json<Value> {
    Json(json!({"options": [mock_ticket()], "mock": true, "warning": "ticket purchase and payment are out of scope"}))
}

async fn ticket_quote() -> Json<Value> {
    Json(json!({"quote": mock_ticket(), "mock": true, "payment_enabled": false}))
}

async fn admin_imports(headers: HeaderMap, State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({"imports": [], "warning": "database import run repository is pending"})))
}

async fn admin_import(Path(id): Path<String>, headers: HeaderMap, State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({"id": id, "status": "unknown"})))
}

async fn admin_import_latest(headers: HeaderMap, State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({"latest": null, "warning": "run data is produced by data-pipeline summarize latest"})))
}

async fn admin_import_start(headers: HeaderMap, State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({
        "status": "accepted",
        "command": "cargo run -p data-pipeline -- import-and-validate ggu-latest",
        "warning": "API does not run the full import inline; use a worker/job runner"
    })))
}

async fn admin_data_quality(headers: HeaderMap, State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({
        "validation_issue_counts": {"warning": 0, "error": 0},
        "unmatched_stops": 0,
        "duplicate_candidates": 0,
        "latest_log_summary": null,
        "mock": state.use_mock_data
    })))
}

async fn admin_unmatched_stops(headers: HeaderMap, State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({"stops": []})))
}

async fn admin_manual_stop_match(headers: HeaderMap, State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({"status": "accepted", "warning": "manual match persistence is pending"})))
}

async fn admin_source_feeds(headers: HeaderMap, State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(sources().await)
}

async fn admin_source_feed_patch(Path(id): Path<String>, headers: HeaderMap, State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({"id": id, "status":"not_implemented"})))
}

async fn public_board(Path(stop_id): Path<String>) -> Json<Value> {
    Json(public_board_payload(&stop_id))
}

async fn public_board_qr(Path(stop_id): Path<String>) -> Json<Value> {
    Json(json!({"stop_id": stop_id, "board_url": format!("https://cesta.local/public/boards/{stop_id}"), "theme": "default", "mock": true}))
}

fn create_user_record(email: &str, password: &str, display_name: Option<String>, roles: Vec<String>) -> anyhow::Result<UserRecord> {
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
    state.users.read().await.get(&id).cloned().ok_or_else(unauthorized)
}

async fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<UserRecord, ApiError> {
    let user = current_user(state, headers).await?;
    if user.roles.iter().any(|role| role == "admin" || role == "data_admin") {
        Ok(user)
    } else {
        Err(ApiError { code: "forbidden".to_string(), message: "Admin role is required".to_string() })
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
    ApiError { code: "unauthorized".to_string(), message: "Authentication required".to_string() }
}

fn not_found() -> ApiError {
    ApiError { code: "not_found".to_string(), message: "Resource not found".to_string() }
}

fn internal_error(error: impl std::fmt::Display) -> ApiError {
    ApiError { code: "internal_error".to_string(), message: error.to_string() }
}

fn package_by_id(id: &str) -> Result<OfflinePackage, ApiError> {
    offline_pack::development_packages()
        .into_iter()
        .find(|package| package.id == id)
        .ok_or_else(not_found)
}

fn mock_status(use_mock_data: bool) -> Value {
    json!({
        "source": if use_mock_data { "mock" } else { "database" },
        "schedule": if use_mock_data { "mock" } else { "current" },
        "realtime": "unavailable",
        "warnings": if use_mock_data { vec!["development fixture data is in use"] } else { Vec::<&str>::new() }
    })
}

fn fixture_stops() -> Vec<Stop> {
    vec![
        fixture_stop("stop-praha-hl-n", "Praha hlavni nadrazi", 50.083, 14.435, TransportMode::Train),
        fixture_stop("stop-brno-hl-n", "Brno hlavni nadrazi", 49.191, 16.612, TransportMode::Train),
        fixture_stop("stop-jihlava", "Jihlava autobusove nadrazi", 49.396, 15.591, TransportMode::Bus),
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
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
    2.0 * earth_radius_m * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn health_endpoint() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn protected_endpoint_blocked_without_token() {
        let app = build_router(app_state().await.unwrap());
        let response = app
            .oneshot(Request::builder().uri("/auth/me").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}

