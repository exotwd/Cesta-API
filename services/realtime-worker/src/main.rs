use chrono::{Duration, Utc};
use transit_model::{RealtimeConfidence, RealtimeUpdate};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let update = RealtimeUpdate {
        id: "mock-update-1".to_string(),
        trip_id: Some("trip-rail-1".to_string()),
        route_id: Some("route-r9".to_string()),
        stop_id: Some("stop-praha-hl-n".to_string()),
        delay_seconds: Some(120),
        estimated_arrival: None,
        estimated_departure: Some(Utc::now() + Duration::minutes(2)),
        cancellation_status: None,
        platform_change: None,
        vehicle_position: None,
        source: "mock".to_string(),
        fetched_at: Utc::now(),
        valid_until: Some(Utc::now() + Duration::minutes(5)),
        confidence: RealtimeConfidence::Estimated,
    };
    tracing::info!(mock = true, update = %serde_json::to_string(&update).unwrap(), "realtime worker emitted mock update");
}

