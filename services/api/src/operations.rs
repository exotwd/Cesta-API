use crate::{ApiError, AppState};
use axum::{Json, extract::State, http::HeaderMap};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::Row;
use std::time::Duration;

fn alerts(health: &Value, sources: &[Value], now: DateTime<Utc>) -> Vec<String> {
    let mut alerts = Vec::new();
    if health["database"]["status"] == "down" {
        alerts.push("database_unavailable".into());
    }
    if health["status"] == "degraded" {
        alerts.push("routing_not_ready".into());
    }
    if matches!(
        health["operations"]["status"].as_str(),
        Some("timeout" | "unavailable")
    ) {
        alerts.push("operational_probe_unavailable".into());
    }
    if health["operations"]["import_failures_24h"]
        .as_u64()
        .unwrap_or(0)
        > 0
    {
        alerts.push("import_failures_24h".into());
    }
    if health["operations"]["push_queue"]["failed"]
        .as_u64()
        .unwrap_or(0)
        > 0
    {
        alerts.push("push_delivery_failures".into());
    }
    if health["journey_search_latency"]["sample_count"]
        .as_u64()
        .unwrap_or(0)
        >= 20
        && health["journey_search_latency"]["p95_ms"]
            .as_u64()
            .unwrap_or(0)
            > 2000
    {
        alerts.push("journey_latency_high".into());
    }
    for source in sources {
        let Some(id) = source["source_id"].as_str() else {
            continue;
        };
        // Source IDs are operator-configured, never request parameters.
        if source["status"] == "failed" {
            alerts.push(format!("source_failed:{id}"));
        }
        let max_age = if source["data_kind"] == "schedule" {
            7 * 3600
        } else {
            180
        };
        let timestamp = source["last_success_at"]
            .as_str()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok());
        if timestamp.is_none_or(|value| now.signed_duration_since(value).num_seconds() > max_age) {
            alerts.push(format!("source_stale:{id}"));
        }
    }
    alerts.sort();
    alerts.dedup();
    alerts
}
pub(crate) async fn monitor(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("monitor HTTP client");
    let mut previous = Vec::new();
    let mut last_sent = std::time::Instant::now() - Duration::from_secs(1800);
    loop {
        interval.tick().await;
        let health = match tokio::time::timeout(
            Duration::from_secs(5),
            crate::controllers::system::health(State(state.clone())),
        )
        .await
        {
            Ok(value) => value.0,
            Err(_) => json!({"status":"degraded","database":{"status":"down"}}),
        };
        let mut source_probe_failed = false;
        let sources = if let Some(pool) = &state.db {
            match tokio::time::timeout(Duration::from_secs(2),sqlx::query("SELECT source_id,data_kind,status,last_success_at FROM data_source_syncs WHERE data_kind='schedule' OR data_kind LIKE '%realtime%' OR data_kind='vehicle_positions'").fetch_all(pool)).await {
                Ok(Ok(rows))=>rows.into_iter().map(|row|json!({"source_id":row.get::<String,_>("source_id"),"data_kind":row.get::<String,_>("data_kind"),"status":row.get::<String,_>("status"),"last_success_at":row.get::<Option<DateTime<Utc>>,_>("last_success_at")})).collect::<Vec<_>>(),
                _=>{source_probe_failed=true; Vec::new()}
            }
        } else {
            Vec::new()
        };
        let now = Utc::now();
        let mut current = alerts(&health, &sources, now);
        if source_probe_failed {
            current.push("source_probe_unavailable".into());
        }
        let telemetry = state.telemetry.snapshot();
        if telemetry["dropped_observations"].as_u64().unwrap_or(0) > 0 {
            current.push("telemetry_observations_dropped".into());
        }
        if let Some(timestamp) = telemetry["last_persisted_at"]
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            && now.signed_duration_since(timestamp).num_seconds() > 120
        {
            current.push("telemetry_persistence_stale".into());
        }
        if telemetry["persistence_configured"] == true
            && telemetry["uptime_seconds"].as_u64().unwrap_or(0) > 120
            && telemetry["last_persisted_at"].is_null()
        {
            current.push("telemetry_persistence_unavailable".into());
        }
        for alert in &current {
            state.telemetry.operation(alert);
        }
        let payload = json!({"checked_at":now,"status":if current.is_empty(){"ok"}else{"warning"},"alerts":current,"sources":sources,"health":health,"alert_webhook_configured":std::env::var_os("OPERATIONS_ALERT_WEBHOOK_URL").is_some()});
        *state
            .telemetry
            .operations
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = payload.clone();
        if current != previous
            || (!current.is_empty() && last_sent.elapsed() >= Duration::from_secs(1800))
        {
            tracing::warn!(alerts=?current,"API operational alert state changed");
            if let Ok(url) = std::env::var("OPERATIONS_ALERT_WEBHOOK_URL") {
                match client.post(url).json(&payload).send().await {
                    Ok(response) if response.status().is_success() => {
                        previous = current.clone();
                        last_sent = std::time::Instant::now();
                    }
                    _ => tracing::warn!(
                        "Operational alert delivery failed; will retry on next monitor pass"
                    ),
                }
            } else {
                previous = current;
                last_sent = std::time::Instant::now();
            }
        }
    }
}
pub(crate) async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    crate::require_admin(&state, &headers).await?;
    Ok(Json(
        state
            .telemetry
            .operations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone(),
    ))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_sources_and_failures_are_distinct_from_empty_journeys() {
        let now = Utc::now();
        let health = json!({"status":"ok","journey_search_latency":{"sample_count":50,"p95_ms":2500},"operations":{"import_failures_24h":1}});
        let source = json!({"source_id":"pid_gtfs_rt","data_kind":"gtfs_realtime","status":"success","last_success_at":now-chrono::Duration::seconds(181)});
        let result = alerts(&health, &[source], now);
        assert!(result.contains(&"journey_latency_high".into()));
        assert!(result.contains(&"source_stale:pid_gtfs_rt".into()));
        assert!(result.contains(&"import_failures_24h".into()));
        assert!(!result.contains(&"routing_not_ready".into()));
    }
}
