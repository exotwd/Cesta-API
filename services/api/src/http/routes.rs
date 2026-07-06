use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::{HeaderName, HeaderValue},
    middleware,
    routing::{delete, get, patch, post},
};
use tower_http::{
    cors::{AllowOrigin, Any, CorsLayer},
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
};

use crate::*;

pub(crate) fn build(state: AppState) -> Router {
    let request_id_header = HeaderName::from_static("x-request-id");
    let config = state.config.clone();

    system_routes()
        .merge(account_routes())
        .merge(transit_routes())
        .merge(admin_routes())
        .merge(public_routes())
        .merge(ticketing::router())
        .layer(middleware::from_fn_with_state(state.clone(), auth_marker))
        .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
        .layer(TraceLayer::new_for_http().make_span_with(
            move |request: &axum::http::Request<axum::body::Body>| {
                let request_id = request
                    .headers()
                    .get(&request_id_header)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("unassigned");
                tracing::info_span!(
                    "http_request",
                    request_id,
                    method = %request.method(),
                    path = %request.uri().path()
                )
            },
        ))
        .layer(SetRequestIdLayer::new(
            HeaderName::from_static("x-request-id"),
            MakeRequestUuid,
        ))
        .layer(DefaultBodyLimit::max(config.request_body_limit_bytes))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("x-content-type-options"),
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("x-frame-options"),
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_static("referrer-policy"),
            HeaderValue::from_static("no-referrer"),
        ))
        .layer(cors_layer(&config.cors_allowed_origins))
        .with_state(state)
}

fn system_routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/openapi.json", get(openapi))
        .route("/metadata/data-status", get(data_status))
        .route("/metadata/sources", get(sources))
}

fn account_routes() -> Router<AppState> {
    Router::new()
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
}

fn transit_routes() -> Router<AppState> {
    Router::new()
        .route("/stops/search", get(search_stops))
        .route("/stops/nearby", get(nearby_stops))
        .route("/stops/{id}", get(stop_detail))
        .route("/stop-areas/{id}", get(stop_area))
        .route("/departures", get(departures))
        .route("/departures/board/{stop_id}", get(board_departures))
        .route("/departures/board/{stop_id}/qr", get(board_qr))
        .route("/journeys/search", post(journey_search))
        .route("/realtime/vehicles", get(realtime_vehicles))
        .route("/data-sources/status", get(data_sources_status))
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
}

fn admin_routes() -> Router<AppState> {
    Router::new()
        .route("/admin", get(admin_app))
        .route("/admin/", get(admin_app))
        .route("/admin/assets/admin.css", get(admin_css))
        .route("/admin/assets/admin.js", get(admin_js))
        .route("/admin/data", get(admin_entities))
        .route("/admin/data/{entity}", get(admin_entity_rows))
        .route("/admin/related/{entity}/{id}", get(admin_related_data))
        .route("/admin/map/stops", get(admin_map_stops))
        .route("/admin/imports", get(admin_imports))
        .route("/admin/imports/{id}", get(admin_import))
        .route("/admin/imports/latest", get(admin_import_latest))
        .route("/admin/imports/ggu-latest/start", post(admin_import_start))
        .route("/admin/database/stats", get(admin_database_stats))
        .route("/admin/data-quality", get(admin_data_quality))
        .route(
            "/admin/data-quality/validate",
            post(admin_run_data_validation),
        )
        .route("/admin/unmatched-stops", get(admin_unmatched_stops))
        .route("/admin/manual-stop-match", post(admin_manual_stop_match))
        .route("/admin/source-feeds", get(admin_source_feeds))
        .route("/admin/source-feeds/{id}", patch(admin_source_feed_patch))
        .route(
            "/admin/routing-algorithm",
            get(admin_routing_algorithm)
                .put(admin_routing_algorithm_update)
                .delete(admin_routing_algorithm_reset),
        )
}

fn public_routes() -> Router<AppState> {
    Router::new()
        .route("/public/boards/{stop_id}", get(public_board))
        .route("/public/boards/{stop_id}/qr-metadata", get(public_board_qr))
}

fn cors_layer(origins: &[String]) -> CorsLayer {
    if origins.iter().any(|origin| origin == "*") {
        return CorsLayer::permissive();
    }

    let origins = origins
        .iter()
        .map(|origin| {
            origin
                .parse::<HeaderValue>()
                .expect("CORS origins are validated during startup")
        })
        .collect::<Vec<_>>();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods(Any)
        .allow_headers(Any)
}
