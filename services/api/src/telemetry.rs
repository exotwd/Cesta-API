//! Bounded, asynchronous minute aggregates. Nothing here records request input or identities.
use crate::{ApiError, AppState, RouteSearchTiming};
use axum::{
    body::{Body, HttpBody},
    extract::{MatchedPath, State},
    http::Request,
    middleware::Next,
    response::Response,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;

const CHANNEL_CAPACITY: usize = 4096;
const MAX_PENDING_KEYS: usize = 8192;
pub(crate) const LATENCY_BOUNDS_MS: [u64; 10] =
    [25, 50, 100, 250, 500, 1000, 2000, 5000, 10000, u64::MAX];
#[derive(Clone, Hash, Eq, PartialEq, Serialize)]
struct Key {
    kind: String,
    endpoint: String,
    method: String,
    status: u16,
}
#[derive(Clone, Default, Serialize)]
struct Stats {
    observations: u64,
    latency_sum_ms: u64,
    latency_max_ms: u64,
    latency_buckets: [u64; 10],
    response_bytes: u64,
    sized_responses: u64,
    results: u64,
    empty_results: u64,
    warned_results: u64,
    realtime_fallbacks: u64,
}
impl Stats {
    fn sample(ms: u64) -> Self {
        let mut value = Self {
            observations: 1,
            latency_sum_ms: ms,
            latency_max_ms: ms,
            ..Self::default()
        };
        value.latency_buckets[LATENCY_BOUNDS_MS
            .iter()
            .position(|bound| ms <= *bound)
            .unwrap_or(9)] = 1;
        value
    }
    fn merge(&mut self, other: &Self) {
        self.observations += other.observations;
        self.latency_sum_ms += other.latency_sum_ms;
        self.latency_max_ms = self.latency_max_ms.max(other.latency_max_ms);
        for (a, b) in self.latency_buckets.iter_mut().zip(other.latency_buckets) {
            *a += b;
        }
        self.response_bytes += other.response_bytes;
        self.sized_responses += other.sized_responses;
        self.results += other.results;
        self.empty_results += other.empty_results;
        self.warned_results += other.warned_results;
        self.realtime_fallbacks += other.realtime_fallbacks;
    }
}
#[derive(Clone)]
pub(crate) struct Telemetry {
    live: Arc<Mutex<HashMap<Key, Stats>>>,
    sender: Option<mpsc::Sender<(i64, Key, Stats)>>,
    dropped: Arc<AtomicU64>,
    persisted_at: Arc<Mutex<Option<DateTime<Utc>>>>,
    started_at: std::time::Instant,
    pub(crate) operations: Arc<Mutex<Value>>,
}
impl Telemetry {
    pub(crate) fn new(db: Option<PgPool>) -> Self {
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        let value = Self {
            live: Arc::new(Mutex::new(HashMap::new())),
            sender: db.as_ref().map(|_| sender),
            dropped: Arc::new(AtomicU64::new(0)),
            persisted_at: Arc::new(Mutex::new(None)),
            started_at: std::time::Instant::now(),
            operations: Arc::new(Mutex::new(json!({"status":"starting","alerts":[]}))),
        };
        if let Some(pool) = db {
            tokio::spawn(persist_loop(pool, receiver, value.clone()));
        }
        value
    }
    fn record(&self, key: Key, stats: Stats) {
        self.live
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(key.clone())
            .or_default()
            .merge(&stats);
        if let Some(sender) = &self.sender
            && sender
                .try_send((Utc::now().timestamp().div_euclid(60) * 60, key, stats))
                .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn stages(&self, timing: &RouteSearchTiming) {
        for stage in &timing.stages {
            self.record(
                Key {
                    kind: "stage".into(),
                    endpoint: stage.stage.clone(),
                    method: "POST".into(),
                    status: 0,
                },
                Stats::sample(stage.elapsed_ms),
            );
        }
    }
    pub(crate) fn journey(&self, count: usize, warnings: bool, fallback: bool) {
        self.record(
            Key {
                kind: "journey".into(),
                endpoint: "/journeys/search".into(),
                method: "POST".into(),
                status: 200,
            },
            Stats {
                results: count as u64,
                empty_results: u64::from(count == 0),
                warned_results: u64::from(warnings),
                realtime_fallbacks: u64::from(fallback),
                ..Stats::sample(0)
            },
        );
    }
    pub(crate) fn operation(&self, code: &str) {
        self.record(
            Key {
                kind: "operation".into(),
                endpoint: code.into(),
                method: "MONITOR".into(),
                status: 0,
            },
            Stats::sample(0),
        );
    }
    pub(crate) fn snapshot(&self) -> Value {
        let live = self.live.lock().unwrap_or_else(|error| error.into_inner());
        let mut rows = live.iter().map(|(key,stats)| json!({"kind":key.kind,"endpoint":key.endpoint,"method":key.method,"status":key.status,"stats":stats,"p95_upper_bound_ms":percentile(&stats.latency_buckets,95)})).collect::<Vec<_>>();
        rows.sort_by_cached_key(|row| row.to_string());
        json!({"scope":"since_process_start","uptime_seconds":self.started_at.elapsed().as_secs(),"persistence_configured":self.sender.is_some(),"rows":rows,"latency_bucket_upper_bounds_ms":[25,50,100,250,500,1000,2000,5000,10000,null],"dropped_observations":self.dropped.load(Ordering::Relaxed),"last_persisted_at":*self.persisted_at.lock().unwrap_or_else(|error| error.into_inner()),"operations":self.operations.lock().unwrap_or_else(|error| error.into_inner()).clone()})
    }
    pub(crate) fn prometheus(&self) -> String {
        let live = self.live.lock().unwrap_or_else(|error| error.into_inner());
        let mut output = String::from(
            "# TYPE cesta_api_requests_total counter\n# TYPE cesta_api_request_duration_ms histogram\n",
        );
        for (key, stats) in live.iter().filter(|(key, _)| key.kind == "request") {
            let labels = format!(
                "endpoint=\"{}\",method=\"{}\",status=\"{}\"",
                key.endpoint, key.method, key.status
            );
            output.push_str(&format!(
                "cesta_api_requests_total{{{labels}}} {}\n",
                stats.observations
            ));
            let mut count = 0;
            for (bound, bucket) in LATENCY_BOUNDS_MS.iter().zip(stats.latency_buckets) {
                count += bucket;
                let le = if *bound == u64::MAX {
                    "+Inf".into()
                } else {
                    bound.to_string()
                };
                output.push_str(&format!(
                    "cesta_api_request_duration_ms_bucket{{{labels},le=\"{le}\"}} {count}\n"
                ));
            }
            output.push_str(&format!("cesta_api_request_duration_ms_sum{{{labels}}} {}\ncesta_api_request_duration_ms_count{{{labels}}} {}\n", stats.latency_sum_ms, stats.observations));
        }
        output.push_str(&format!("# TYPE cesta_api_telemetry_dropped_total counter\ncesta_api_telemetry_dropped_total {}\n",self.dropped.load(Ordering::Relaxed)));
        output
    }
}
fn percentile(buckets: &[u64; 10], percent: u64) -> Option<u64> {
    let count = buckets.iter().sum::<u64>();
    if count == 0 {
        return None;
    }
    let rank = (count * percent).div_ceil(100);
    let mut seen = 0;
    for (index, bucket) in buckets.iter().enumerate() {
        seen += bucket;
        if seen >= rank {
            return (index < 9).then_some(LATENCY_BOUNDS_MS[index]);
        }
    }
    None
}
pub(crate) async fn observe(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let endpoint = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str())
        .unwrap_or("unmatched")
        .to_string();
    let method = match request.method().as_str() {
        "GET" | "POST" | "PATCH" | "DELETE" | "PUT" | "HEAD" | "OPTIONS" => {
            request.method().as_str()
        }
        _ => "OTHER",
    }
    .to_string();
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    let mut stats = Stats::sample(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
    if let Some(bytes) = response.body().size_hint().exact() {
        stats.response_bytes = bytes;
        stats.sized_responses = 1;
    }
    state.telemetry.record(
        Key {
            kind: "request".into(),
            endpoint,
            method,
            status: response.status().as_u16(),
        },
        stats,
    );
    response
}
type Pending = HashMap<(i64, Key), Stats>;
async fn persist_loop(
    pool: PgPool,
    mut receiver: mpsc::Receiver<(i64, Key, Stats)>,
    telemetry: Telemetry,
) {
    let mut pending = Pending::new();
    let mut batch: Option<(uuid::Uuid, Pending)> = None;
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_prune = std::time::Instant::now() - Duration::from_secs(3600);
    loop {
        tokio::select! {
            Some((minute,key,stats)) = receiver.recv() => {
                if pending.len() < MAX_PENDING_KEYS || pending.contains_key(&(minute,key.clone())) { pending.entry((minute,key)).or_default().merge(&stats); }
                else { telemetry.dropped.fetch_add(stats.observations,Ordering::Relaxed); }
            }
            _ = interval.tick() => {
                if batch.is_none() && !pending.is_empty() { batch = Some((uuid::Uuid::new_v4(),std::mem::take(&mut pending))); }
                if let Some((id,rows)) = &batch {
                    match tokio::time::timeout(Duration::from_secs(5),persist_batch(&pool,*id,rows)).await {
                        Ok(Ok(())) => { batch = None; *telemetry.persisted_at.lock().unwrap_or_else(|error| error.into_inner()) = Some(Utc::now()); }
                        _ => tracing::warn!("API telemetry persistence failed; bounded aggregates retained for retry"),
                    }
                }
                if last_prune.elapsed() >= Duration::from_secs(3600) {
                    let cleanup = async {
                        sqlx::query("DELETE FROM api_telemetry_minutes WHERE minute < now()-interval '30 days'").execute(&pool).await?;
                        sqlx::query("DELETE FROM api_telemetry_batches WHERE created_at < now()-interval '31 days'").execute(&pool).await?;
                        sqlx::query("DELETE FROM auth_attempts WHERE attempted_at < now()-interval '2 days'").execute(&pool).await?;
                        Ok::<(),sqlx::Error>(())
                    };
                    if matches!(tokio::time::timeout(Duration::from_secs(5),cleanup).await,Ok(Ok(()))) { last_prune = std::time::Instant::now(); }
                }
            }
        }
    }
}
async fn persist_batch(pool: &PgPool, id: uuid::Uuid, rows: &Pending) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET LOCAL statement_timeout='4000ms'")
        .execute(&mut *transaction)
        .await?;
    if sqlx::query("INSERT INTO api_telemetry_batches(id) VALUES($1) ON CONFLICT DO NOTHING")
        .bind(id)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
        == 0
    {
        return transaction.commit().await;
    }
    // One batch is atomic; its UUID makes retries safe even if the commit acknowledgement was lost.
    for ((minute, key), stats) in rows {
        sqlx::query(r#"INSERT INTO api_telemetry_minutes(minute,kind,endpoint,method,status,observations,latency_sum_ms,latency_max_ms,latency_buckets,response_bytes,sized_responses,results,empty_results,warned_results,realtime_fallbacks)
          VALUES(to_timestamp($1),$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
          ON CONFLICT(minute,kind,endpoint,method,status) DO UPDATE SET
          observations=api_telemetry_minutes.observations+EXCLUDED.observations,
          latency_sum_ms=api_telemetry_minutes.latency_sum_ms+EXCLUDED.latency_sum_ms,
          latency_max_ms=GREATEST(api_telemetry_minutes.latency_max_ms,EXCLUDED.latency_max_ms),
          latency_buckets=ARRAY(SELECT a+b FROM unnest(api_telemetry_minutes.latency_buckets,EXCLUDED.latency_buckets) AS t(a,b)),
          response_bytes=api_telemetry_minutes.response_bytes+EXCLUDED.response_bytes,
          sized_responses=api_telemetry_minutes.sized_responses+EXCLUDED.sized_responses,
          results=api_telemetry_minutes.results+EXCLUDED.results,empty_results=api_telemetry_minutes.empty_results+EXCLUDED.empty_results,
          warned_results=api_telemetry_minutes.warned_results+EXCLUDED.warned_results,realtime_fallbacks=api_telemetry_minutes.realtime_fallbacks+EXCLUDED.realtime_fallbacks"#)
          .bind(*minute as f64).bind(&key.kind).bind(&key.endpoint).bind(&key.method).bind(i32::from(key.status))
          .bind(stats.observations as i64).bind(stats.latency_sum_ms as i64).bind(stats.latency_max_ms as i64).bind(stats.latency_buckets.map(|n| n as i64).to_vec())
          .bind(stats.response_bytes as i64).bind(stats.sized_responses as i64).bind(stats.results as i64).bind(stats.empty_results as i64).bind(stats.warned_results as i64).bind(stats.realtime_fallbacks as i64)
          .execute(&mut *transaction).await?;
    }
    transaction.commit().await
}
#[derive(Deserialize)]
pub(crate) struct AnalyticsQuery {
    hours: Option<i32>,
}
pub(crate) async fn analytics(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    axum::extract::Query(query): axum::extract::Query<AnalyticsQuery>,
) -> Result<axum::Json<Value>, ApiError> {
    crate::require_admin(&state, &headers).await?;
    let hours = query.hours.unwrap_or(24);
    if !(1..=720).contains(&hours) {
        return Err(ApiError {
            code: "validation_error".into(),
            message: "hours must be between 1 and 720".into(),
        });
    }
    let Some(pool) = &state.db else {
        return Ok(axum::Json(state.telemetry.snapshot()));
    };
    let mut transaction = pool.begin().await.map_err(crate::internal_error)?;
    sqlx::query("SET LOCAL statement_timeout='4000ms'")
        .execute(&mut *transaction)
        .await
        .map_err(crate::internal_error)?;
    let rows = sqlx::query_scalar::<_,Value>(r#"SELECT jsonb_build_object('kind',kind,'endpoint',endpoint,'method',method,'status',status,
        'observations',sum(observations)::bigint,'latency_sum_ms',sum(latency_sum_ms)::bigint,'latency_max_ms',max(latency_max_ms),
        'latency_buckets',jsonb_build_array(sum(latency_buckets[1]),sum(latency_buckets[2]),sum(latency_buckets[3]),sum(latency_buckets[4]),sum(latency_buckets[5]),sum(latency_buckets[6]),sum(latency_buckets[7]),sum(latency_buckets[8]),sum(latency_buckets[9]),sum(latency_buckets[10])),
        'response_bytes',sum(response_bytes)::bigint,'sized_responses',sum(sized_responses)::bigint,'results',sum(results)::bigint,
        'empty_results',sum(empty_results)::bigint,'warned_results',sum(warned_results)::bigint,'realtime_fallbacks',sum(realtime_fallbacks)::bigint)
        FROM api_telemetry_minutes WHERE minute >= date_trunc('minute',now()-make_interval(hours=>$1))
        GROUP BY kind,endpoint,method,status ORDER BY kind,endpoint,method,status LIMIT 2000"#)
        .bind(hours).fetch_all(&mut *transaction).await.map_err(crate::internal_error)?;
    transaction.commit().await.map_err(crate::internal_error)?;
    Ok(axum::Json(
        json!({"scope":"persisted_minutes","hours":hours,"retention_days":30,"flush_interval_seconds":15,"rows":rows,"live":state.telemetry.snapshot()}),
    ))
}
pub(crate) async fn metrics(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    crate::require_admin(&state, &headers).await?;
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.telemetry.prometheus(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histograms_merge_and_percentiles_are_explicit_upper_bounds() {
        let mut stats = Stats::sample(30);
        stats.merge(&Stats::sample(300));
        assert_eq!(stats.observations, 2);
        assert_eq!(stats.latency_sum_ms, 330);
        assert_eq!(percentile(&stats.latency_buckets, 95), Some(500));
        assert_eq!(percentile(&Stats::sample(20000).latency_buckets, 95), None);
    }
    #[test]
    fn fixture_analytics_contain_only_aggregate_measurements() {
        let telemetry = Telemetry::new(None);
        telemetry.journey(0, true, true);
        let payload = telemetry.snapshot();
        assert_eq!(payload["rows"][0]["stats"]["empty_results"], 1);
        assert!(!payload.to_string().contains("user_id"));
        assert!(telemetry.prometheus().contains("telemetry_dropped_total"));
    }
}
