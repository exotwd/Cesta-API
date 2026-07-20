use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, patch, post},
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use uuid::Uuid;

use crate::cd::{
    self, BookingRequest, CdApi, CdError, ConnectionParameters, CustomerInfo, PageRequest,
    Passenger, SchemaRequest, SearchConnectionsRequest,
};
use crate::{ApiError, AppState, current_user};

const QUOTE_CREATE_FLAGS: i32 = 65536 | 131072 | 1;
const SEARCH_LIMIT: usize = 30;
const MUTATION_LIMIT: usize = 20;
type IdempotencyLockMap = HashMap<(Uuid, String, String), Arc<Mutex<()>>>;
type RateLimitMap = HashMap<(Uuid, &'static str), VecDeque<Instant>>;

#[derive(Clone)]
pub struct TicketingService {
    cd: Option<Arc<dyn CdApi>>,
    payment: Arc<dyn PaymentProvider>,
    db: Option<PgPool>,
    memory: Arc<RwLock<MemoryStore>>,
    order_locks: Arc<RwLock<HashMap<Uuid, Arc<Mutex<()>>>>>,
    idempotency_locks: Arc<RwLock<IdempotencyLockMap>>,
    reference_cache: Arc<RwLock<HashMap<String, (Instant, Value)>>>,
    rate: Arc<Mutex<RateLimitMap>>,
}

#[derive(Default)]
struct MemoryStore {
    journey_refs: HashMap<Uuid, JourneyReferenceRecord>,
    locations: HashMap<Uuid, LocationRecord>,
    searches: HashMap<Uuid, SearchRecord>,
    orders: HashMap<Uuid, OrderRecord>,
    documents: HashMap<Uuid, DocumentRecord>,
    tickets: HashMap<Uuid, TicketRecord>,
    refunds: HashMap<Uuid, RefundRecord>,
    idempotency: HashMap<(Uuid, String, String), IdempotentResult>,
}

#[derive(Clone)]
struct JourneyReferenceRecord {
    id: Uuid,
    snapshot: Value,
    expires_at: DateTime<Utc>,
}

#[derive(Clone)]
struct LocationRecord {
    user_id: Uuid,
    upstream_type: i32,
    upstream_key: i32,
    #[allow(dead_code)]
    payload: Value,
}
#[derive(Clone)]
struct SearchRecord {
    id: Uuid,
    user_id: Uuid,
    handle: i32,
    request: SearchRequest,
    raw: Value,
    connections: HashMap<Uuid, i32>,
    expires_at: DateTime<Utc>,
}
#[derive(Clone)]
struct OrderRecord {
    id: Uuid,
    user_id: Uuid,
    search_id: Uuid,
    connection_id: Uuid,
    conn_id: i32,
    booking_id: String,
    status: String,
    selected_offer_type: Option<i32>,
    amount_hellers: Option<i64>,
    customer: Option<CustomerRequest>,
    quote: Value,
    checkout_session_id: Option<String>,
    version: i64,
}
#[derive(Clone)]
struct DocumentRecord {
    id: Uuid,
    user_id: Uuid,
    order_id: Uuid,
    upstream_id: String,
    document_type: Option<i32>,
}
#[derive(Clone)]
struct TicketRecord {
    id: Uuid,
    user_id: Uuid,
    order_id: Uuid,
    upstream_id: String,
    payload: Value,
    returned: bool,
}
#[derive(Clone)]
struct RefundRecord {
    id: Uuid,
    user_id: Uuid,
    ticket_id: Uuid,
    status: String,
    amount_hellers: Option<i64>,
    payload: Value,
}
#[derive(Clone)]
struct IdempotentResult {
    request_hash: String,
    status: StatusCode,
    body: Value,
}

struct OrderMutationGuard {
    _local: OwnedMutexGuard<()>,
    _database: Option<Transaction<'static, Postgres>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchRequest {
    pub from_location_id: Uuid,
    pub to_location_id: Uuid,
    pub via_location_id: Option<Uuid>,
    pub change_location_id: Option<Uuid>,
    pub date_time: Option<String>,
    #[serde(default)]
    pub previous: bool,
    #[serde(default = "default_max_count")]
    pub max_count: i32,
    #[serde(default)]
    pub max_changes: Option<i32>,
    #[serde(default)]
    pub passenger_groups: Vec<PassengerRequest>,
    #[serde(default = "default_class")]
    pub class: i32,
}
fn default_max_count() -> i32 {
    5
}
fn default_class() -> i32 {
    2
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PassengerRequest {
    pub passenger_type_id: i32,
    pub count: i32,
    pub age: Option<i32>,
    #[serde(default)]
    pub reduction_ids: Vec<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocationQuery {
    pub q: String,
    pub r#type: Option<i32>,
    pub limit: Option<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PageQuery {
    pub cursor: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OrderCreateRequest {
    pub search_id: Uuid,
    pub connection_id: Uuid,
    #[serde(default)]
    pub passenger_groups: Vec<PassengerRequest>,
    #[serde(default = "default_class")]
    pub class: i32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TicketingIntentRequest {
    pub journey_reference: Uuid,
    #[serde(default)]
    pub segment_indexes: Vec<usize>,
    #[serde(default)]
    pub passenger_groups: Vec<PassengerRequest>,
    #[serde(default = "default_class")]
    pub class: i32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OrdersQuery {
    pub status: Option<String>,
    pub cursor: Option<Uuid>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OfferRequest {
    pub offer_type: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CustomerRequest {
    pub email: String,
    pub name: Option<String>,
    pub in_card_number: Option<i64>,
    pub birth_date: Option<String>,
    pub company_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReservationRequest {
    pub trains: Vec<ReservationSelection>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReservationSelection {
    pub train_id: i32,
    pub res_type: i32,
    pub count: Option<i32>,
    pub coach: Option<i32>,
    #[serde(default)]
    pub places: Vec<i32>,
    pub compartment: Option<i32>,
    pub place: Option<i32>,
    pub berth_male_count: Option<i32>,
    pub berth_female_count: Option<i32>,
    pub berth_together: Option<bool>,
    pub couchette_berth_male_positions: Option<ReservationPositions>,
    pub berth_female_positions: Option<ReservationPositions>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReservationPositions {
    pub top: Option<i32>,
    pub middle: Option<i32>,
    pub bottom: Option<i32>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BicycleRequest {
    pub count: i32,
    #[serde(default)]
    pub trains: Vec<BicycleSelection>,
    #[serde(default)]
    pub price_only: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BicycleSelection {
    pub train_id: i32,
    pub bike_type: i32,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DogRequest {
    pub count: i32,
    pub direction: i32,
    #[serde(default)]
    pub price_only: bool,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CoachQuery {
    pub train_id: i32,
    pub coach: Option<String>,
    #[serde(default = "default_schema_width")]
    pub width: i32,
    pub max_height: Option<i32>,
}
fn default_schema_width() -> i32 {
    800
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckoutRequest {
    pub customer: CustomerRequest,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RefundRequest {
    pub email: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckoutSession {
    pub id: String,
    pub redirect_url: String,
    pub return_url: String,
    pub cancel_url: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct PaymentOrder {
    pub order_id: Uuid,
    pub amount_hellers: i64,
    pub currency: &'static str,
}

#[derive(Debug, Error)]
pub enum PaymentError {
    #[error("payment provider is not configured")]
    NotConfigured,
    #[error("payment provider unavailable")]
    Unavailable,
    #[error("payment is not settled")]
    NotSettled,
    #[error("payment amount or currency mismatch")]
    Mismatch,
}
use thiserror::Error;

#[async_trait]
pub trait PaymentProvider: Send + Sync {
    async fn create_checkout(&self, order: &PaymentOrder) -> Result<CheckoutSession, PaymentError>;
    async fn verify_settled(
        &self,
        session_id: &str,
        order: &PaymentOrder,
    ) -> Result<(), PaymentError>;
}

pub struct DisabledPaymentProvider;
#[async_trait]
impl PaymentProvider for DisabledPaymentProvider {
    async fn create_checkout(&self, _: &PaymentOrder) -> Result<CheckoutSession, PaymentError> {
        Err(PaymentError::NotConfigured)
    }
    async fn verify_settled(&self, _: &str, _: &PaymentOrder) -> Result<(), PaymentError> {
        Err(PaymentError::NotConfigured)
    }
}

#[derive(Clone)]
pub struct HttpPaymentProvider {
    client: reqwest::Client,
    base_url: String,
    api_key: cd::Secret,
    return_url: String,
    cancel_url: String,
}
impl HttpPaymentProvider {
    pub fn new(
        base_url: String,
        api_key: String,
        return_url: String,
        cancel_url: String,
        timeout: Duration,
    ) -> Result<Self, PaymentError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| PaymentError::Unavailable)?;
        Ok(Self {
            client,
            base_url,
            api_key: cd::Secret::new(api_key),
            return_url,
            cancel_url,
        })
    }
}
fn valid_provider_session_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.:".contains(c))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProviderStatus {
    id: String,
    status: String,
    amount_hellers: i64,
    currency: String,
    redirect_url: Option<String>,
    expires_at: Option<DateTime<Utc>>,
}

#[async_trait]
impl PaymentProvider for HttpPaymentProvider {
    async fn create_checkout(&self, o: &PaymentOrder) -> Result<CheckoutSession, PaymentError> {
        let callback = |base: &str| -> Result<String, PaymentError> {
            let mut url = reqwest::Url::parse(base).map_err(|_| PaymentError::NotConfigured)?;
            url.query_pairs_mut()
                .append_pair("orderId", &o.order_id.to_string());
            Ok(url.to_string())
        };
        let return_url = callback(&self.return_url)?;
        let cancel_url = callback(&self.cancel_url)?;
        let r=self.client.post(format!("{}/checkout-sessions",self.base_url.trim_end_matches('/'))).bearer_auth(self.api_key.expose()).json(&json!({"merchantReference":o.order_id,"amountHellers":o.amount_hellers,"currency":o.currency,"returnUrl":return_url,"cancelUrl":cancel_url})).send().await.map_err(|_|PaymentError::Unavailable)?;
        if !r.status().is_success() {
            return Err(PaymentError::Unavailable);
        }
        let p: ProviderStatus = r.json().await.map_err(|_| PaymentError::Unavailable)?;
        if !valid_provider_session_id(&p.id)
            || p.amount_hellers != o.amount_hellers
            || p.currency != "CZK"
        {
            return Err(PaymentError::Mismatch);
        }
        Ok(CheckoutSession {
            id: p.id,
            redirect_url: p.redirect_url.ok_or(PaymentError::Unavailable)?,
            return_url,
            cancel_url,
            expires_at: p.expires_at,
        })
    }
    async fn verify_settled(&self, id: &str, o: &PaymentOrder) -> Result<(), PaymentError> {
        if !valid_provider_session_id(id) {
            return Err(PaymentError::Mismatch);
        }
        let r = self
            .client
            .get(format!(
                "{}/checkout-sessions/{id}",
                self.base_url.trim_end_matches('/')
            ))
            .bearer_auth(self.api_key.expose())
            .send()
            .await
            .map_err(|_| PaymentError::Unavailable)?;
        if !r.status().is_success() {
            return Err(PaymentError::Unavailable);
        }
        let p: ProviderStatus = r.json().await.map_err(|_| PaymentError::Unavailable)?;
        if p.id != id || p.amount_hellers != o.amount_hellers || p.currency != "CZK" {
            return Err(PaymentError::Mismatch);
        }
        if p.status != "settled" {
            return Err(PaymentError::NotSettled);
        }
        Ok(())
    }
}

impl TicketingService {
    pub fn new(
        cd: Option<Arc<dyn CdApi>>,
        payment: Arc<dyn PaymentProvider>,
        db: Option<PgPool>,
    ) -> Self {
        Self {
            cd,
            payment,
            db,
            memory: Default::default(),
            order_locks: Default::default(),
            idempotency_locks: Default::default(),
            reference_cache: Default::default(),
            rate: Default::default(),
        }
    }
    fn client(&self) -> Result<&Arc<dyn CdApi>, ApiError> {
        self.cd
            .as_ref()
            .ok_or_else(|| api_error("ticketing_unavailable", "ČD ticketing is not configured"))
    }
    async fn limit(&self, user: Uuid, bucket: &'static str, max: usize) -> Result<(), ApiError> {
        if let Some(db) = &self.db {
            let count: i32 = sqlx::query_scalar("INSERT INTO cd_ticketing_rate_limits(user_id,bucket,window_start,request_count) VALUES($1,$2,date_trunc('minute',now()),1) ON CONFLICT(user_id,bucket,window_start) DO UPDATE SET request_count=cd_ticketing_rate_limits.request_count+1 RETURNING request_count")
                .bind(user).bind(bucket).fetch_one(db).await.map_err(db_error)?;
            if count > max as i32 {
                return Err(api_error("rate_limited", "Too many ticketing requests"));
            }
            return Ok(());
        }
        let now = Instant::now();
        let mut all = self.rate.lock().await;
        let queue = all.entry((user, bucket)).or_default();
        while queue
            .front()
            .is_some_and(|t| now.duration_since(*t) > Duration::from_secs(60))
        {
            queue.pop_front();
        }
        if queue.len() >= max {
            return Err(api_error("rate_limited", "Too many ticketing requests"));
        }
        queue.push_back(now);
        Ok(())
    }
    async fn order_lock(&self, id: Uuid) -> Arc<Mutex<()>> {
        let mut locks = self.order_locks.write().await;
        locks
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn mutation_guard(&self, id: Uuid) -> Result<OrderMutationGuard, ApiError> {
        let local = self.order_lock(id).await.lock_owned().await;
        let database = if let Some(db) = &self.db {
            let mut transaction = db.begin().await.map_err(db_error)?;
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
                .bind(id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(db_error)?;
            Some(transaction)
        } else {
            None
        };
        Ok(OrderMutationGuard {
            _local: local,
            _database: database,
        })
    }

    pub async fn annotate_journeys(
        &self,
        journeys: &mut [Value],
        related: &Value,
        service_date: NaiveDate,
    ) -> Result<(), ApiError> {
        let stop_names = related
            .get("stops")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|stop| {
                Some((
                    stop.get("id")?.as_str()?.to_string(),
                    stop.get("name")?.as_str()?.to_string(),
                ))
            })
            .collect::<HashMap<_, _>>();
        let route_names = related
            .get("routes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|route| {
                Some((
                    route.get("id")?.as_str()?.to_string(),
                    route
                        .get("short_name")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                ))
            })
            .collect::<HashMap<_, _>>();
        let mut records = Vec::new();
        for journey in journeys {
            let legs = journey
                .get("legs")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let candidate_indexes = legs
                .iter()
                .enumerate()
                .filter_map(|(index, leg)| {
                    (leg.get("mode").and_then(Value::as_str) == Some("train")).then_some(index)
                })
                .collect::<Vec<_>>();
            let segments=legs.iter().enumerate().map(|(index,_leg)|json!({
                "legIndex":index,
                "provider":if candidate_indexes.contains(&index){Some("cd")}else{None},
                "availability":if candidate_indexes.contains(&index){"authentication_required"}else{"unsupported_mode"},
                "ticketable":Value::Null
            })).collect::<Vec<_>>();
            if candidate_indexes.is_empty() {
                journey["ticketing"] = json!({"provider":Value::Null,"journeyReference":Value::Null,"authenticationRequired":true,"availability":"not_supported","indicativePriceHellers":Value::Null,"currency":"CZK","completeJourneyTicketable":false,"segments":segments});
                continue;
            }
            let reference = Uuid::new_v4();
            let snapshot_legs=legs.iter().enumerate().map(|(index,leg)|{
                let from_id=leg.get("from_stop_id").and_then(Value::as_str).unwrap_or_default();
                let to_id=leg.get("to_stop_id").and_then(Value::as_str).unwrap_or_default();
                let route_id=leg.get("route_id").and_then(Value::as_str);
                json!({"index":index,"mode":leg.get("mode"),"fromName":stop_names.get(from_id),"toName":stop_names.get(to_id),"routeName":route_id.and_then(|id|route_names.get(id)).and_then(|value|value.clone()),"departureSeconds":leg.get("departure_time"),"arrivalSeconds":leg.get("arrival_time")})
            }).collect::<Vec<_>>();
            let record = JourneyReferenceRecord {
                id: reference,
                snapshot: json!({"journeyId":journey.get("id"),"serviceDate":service_date,"departureSeconds":journey.get("departure_time"),"arrivalSeconds":journey.get("arrival_time"),"legs":snapshot_legs,"candidateSegmentIndexes":candidate_indexes}),
                expires_at: Utc::now() + chrono::Duration::minutes(30),
            };
            records.push(record);
            let all_supported = legs.iter().all(|leg| {
                matches!(
                    leg.get("mode").and_then(Value::as_str),
                    Some("train" | "walk")
                )
            });
            journey["ticketing"] = json!({"provider":"cd","journeyReference":reference,"authenticationRequired":true,"availability":"authentication_required","indicativePriceHellers":Value::Null,"currency":"CZK","completeJourneyTicketable":Value::Null,"scope":if all_supported{"complete_journey_candidate"}else{"segments"},"segments":segments});
        }
        if !records.is_empty() {
            let mut memory = self.memory.write().await;
            let now = Utc::now();
            memory
                .journey_refs
                .retain(|_, record| record.expires_at > now);
            for record in &records {
                memory.journey_refs.insert(record.id, record.clone());
            }
            drop(memory);
            if let Some(db) = &self.db {
                // The same-process reference is usable immediately. Persist the opaque
                // references off the route-search critical path so database fsync or pool
                // contention cannot add a second to every public journey response.
                let db = db.clone();
                tokio::spawn(async move {
                    if let Err(error) = persist_journey_references(&db, &records).await {
                        tracing::error!(%error, "failed to persist journey ticketing references");
                    }
                });
            }
        }
        Ok(())
    }

    async fn journey_reference(&self, id: Uuid) -> Result<JourneyReferenceRecord, ApiError> {
        if let Some(record) = self.memory.read().await.journey_refs.get(&id).cloned()
            && record.expires_at > Utc::now()
        {
            return Ok(record);
        }
        if let Some(db)=&self.db
            && let Some(row)=sqlx::query("SELECT journey_snapshot,expires_at FROM cd_ticketing_journey_refs WHERE id=$1 AND expires_at>now()").bind(id).fetch_optional(db).await.map_err(db_error)?{
                return Ok(JourneyReferenceRecord{id,snapshot:row.get("journey_snapshot"),expires_at:row.get("expires_at")});
            }
        Err(api_error(
            "journey_reference_expired",
            "Journey ticketing reference is invalid or expired",
        ))
    }

    pub fn start_refund_reconciliation(&self, interval: Duration) {
        if self.db.is_none() || self.cd.is_none() {
            return;
        }
        let service = self.clone();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval.max(Duration::from_secs(30)));
            loop {
                timer.tick().await;
                if let Err(code) = service.reconcile_refunds_once().await {
                    tracing::warn!(error_code = code, "ČD refund reconciliation failed");
                }
            }
        });
    }

    async fn reconcile_refunds_once(&self) -> Result<(), &'static str> {
        let db = self.db.as_ref().ok_or("storage_unavailable")?;
        let client = self.cd.as_ref().ok_or("ticketing_unavailable")?;
        let row = sqlx::query("SELECT last_event_id,last_event_at FROM cd_ticketing_refund_cursor WHERE singleton=true").fetch_one(db).await.map_err(|_|"cursor_read_failed")?;
        let last_id: Option<i32> = row.get("last_event_id");
        let last_at: Option<DateTime<Utc>> = row.get("last_event_at");
        let date = last_at.map(|value| value.format("%Y-%m-%d %H:%M").to_string());
        let (_, payload) = client
            .refund_changes(date.as_deref(), last_id)
            .await
            .map_err(|_| "upstream_refund_changes_failed")?;
        let mut cursor = last_id;
        let mut cursor_at = last_at;
        for event in payload
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let event_id = event
                .get("id")
                .and_then(Value::as_i64)
                .and_then(|v| i32::try_from(v).ok())
                .ok_or("invalid_refund_event")?;
            let ticket_id = event
                .get("ticketId")
                .and_then(Value::as_str)
                .ok_or("invalid_refund_event")?;
            let rejected = event
                .get("refundRejected")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            sqlx::query("UPDATE cd_ticketing_refunds r SET status=$2,payload=$3,amount_hellers=COALESCE(($3->>'refundAmount')::integer,amount_hellers),updated_at=now() FROM cd_ticketing_tickets t WHERE r.ticket_id=t.id AND t.upstream_ticket_id=$1").bind(ticket_id).bind(if rejected{"rejected"}else{"settled"}).bind(&event).execute(db).await.map_err(|_|"refund_update_failed")?;
            if !rejected {
                sqlx::query("UPDATE cd_ticketing_tickets SET returned=true,updated_at=now() WHERE upstream_ticket_id=$1").bind(ticket_id).execute(db).await.map_err(|_|"ticket_update_failed")?;
            }
            if let Some(owner)=sqlx::query("SELECT user_id,order_id FROM cd_ticketing_tickets WHERE upstream_ticket_id=$1 LIMIT 1").bind(ticket_id).fetch_optional(db).await.map_err(|_|"replacement_owner_failed")? {
                let user_id:Uuid=owner.get("user_id");
                let order_id:Uuid=owner.get("order_id");
                for document in event.get("newTickets").and_then(Value::as_array).cloned().unwrap_or_default(){
                    if let Some(document_id)=document.get("documentId").and_then(Value::as_str){
                        sqlx::query("INSERT INTO cd_ticketing_documents(id,user_id,order_id,upstream_document_id,document_type) VALUES($1,$2,$3,$4,$5) ON CONFLICT(order_id,upstream_document_id) DO NOTHING").bind(Uuid::new_v4()).bind(user_id).bind(order_id).bind(document_id).bind(document.get("documentType").and_then(Value::as_i64).and_then(|v|i32::try_from(v).ok())).execute(db).await.map_err(|_|"replacement_document_failed")?;
                    }
                    for ticket in document.get("ticketsIds").and_then(Value::as_array).cloned().unwrap_or_default(){
                        if let Some(new_id)=ticket.get("ticketId").and_then(Value::as_str){
                            sqlx::query("INSERT INTO cd_ticketing_tickets(id,user_id,order_id,upstream_ticket_id,payload,returned) VALUES($1,$2,$3,$4,$5,false) ON CONFLICT(order_id,upstream_ticket_id) DO UPDATE SET payload=EXCLUDED.payload,updated_at=now()").bind(Uuid::new_v4()).bind(user_id).bind(order_id).bind(new_id).bind(&ticket).execute(db).await.map_err(|_|"replacement_ticket_failed")?;
                        }
                    }
                }
            }
            cursor = Some(cursor.map_or(event_id, |old| old.max(event_id)));
            cursor_at = event
                .get("dateTime")
                .and_then(Value::as_str)
                .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
                .map(|v| v.with_timezone(&Utc))
                .or(cursor_at);
        }
        sqlx::query("UPDATE cd_ticketing_refund_cursor SET last_event_id=$1,last_event_at=$2,updated_at=now() WHERE singleton=true").bind(cursor).bind(cursor_at).execute(db).await.map_err(|_|"cursor_write_failed")?;
        Ok(())
    }
}

async fn persist_journey_references(
    db: &PgPool,
    records: &[JourneyReferenceRecord],
) -> Result<(), sqlx::Error> {
    let ids = records.iter().map(|record| record.id).collect::<Vec<_>>();
    let snapshots = records
        .iter()
        .map(|record| record.snapshot.clone())
        .collect::<Vec<_>>();
    let expirations = records
        .iter()
        .map(|record| record.expires_at)
        .collect::<Vec<_>>();
    sqlx::query(
        r#"
        INSERT INTO cd_ticketing_journey_refs (id, journey_snapshot, expires_at)
        SELECT * FROM UNNEST($1::uuid[], $2::jsonb[], $3::timestamptz[])
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(ids)
    .bind(snapshots)
    .bind(expirations)
    .execute(db)
    .await?;
    Ok(())
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/ticketing/locations", get(locations))
        .route("/ticketing/reference/passengers", get(passengers))
        .route("/ticketing/intents", post(create_intent))
        .route("/ticketing/searches", post(create_search))
        .route("/ticketing/searches/{search_id}", get(search_page))
        .route(
            "/ticketing/searches/{search_id}/connections/{connection_id}",
            get(connection_detail),
        )
        .route("/ticketing/orders", get(list_orders).post(create_order))
        .route(
            "/ticketing/orders/{order_id}",
            get(get_order).delete(cancel_order),
        )
        .route("/ticketing/orders/{order_id}/offer", patch(select_offer))
        .route(
            "/ticketing/orders/{order_id}/quote-refresh",
            post(refresh_quote),
        )
        .route("/ticketing/orders/{order_id}/add-ons", get(add_ons))
        .route(
            "/ticketing/orders/{order_id}/reservations",
            patch(set_reservations),
        )
        .route("/ticketing/orders/{order_id}/bicycles", patch(set_bicycles))
        .route("/ticketing/orders/{order_id}/dogs", patch(set_dogs))
        .route(
            "/ticketing/orders/{order_id}/coach-schema",
            get(coach_schema),
        )
        .route(
            "/ticketing/orders/{order_id}/checkout-session",
            post(checkout),
        )
        .route("/ticketing/orders/{order_id}/complete", post(complete))
        .route(
            "/ticketing/orders/{order_id}/documents",
            get(order_documents),
        )
        .route("/ticketing/documents/{document_id}", get(document))
        .route(
            "/ticketing/tickets/{ticket_id}/refund-quote",
            get(refund_quote),
        )
        .route("/ticketing/tickets/{ticket_id}/refunds", post(refund))
        .route(
            "/ticketing/tickets/{ticket_id}/refunds/latest",
            get(refund_latest),
        )
}

pub fn augment_openapi(specification: &mut Value) {
    let Some(paths) = specification
        .get_mut("paths")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    let bearer = json!([{"bearerAuth":[]}]);
    let idempotency = json!({"name":"Idempotency-Key","in":"header","required":true,"schema":{"type":"string","minLength":8,"maxLength":128}});
    let mut add = |path: &str, method: &str, summary: &str, mutation: bool| {
        let operation = json!({"summary":summary,"tags":["ČD ticketing"],"security":bearer,"parameters":if mutation{json!([idempotency.clone()])}else{json!([])},"responses":{"200":{"description":"Successful user-scoped ticketing response"},"400":{"description":"Validation or upstream rejection"},"401":{"description":"Authentication required"},"404":{"description":"Resource not found or not owned by caller"},"429":{"description":"Rate limit exceeded"},"503":{"description":"Configured integration unavailable"}}});
        paths
            .entry(path.to_string())
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .expect("path object")
            .insert(method.to_string(), operation);
    };
    add("/ticketing/locations", "get", "Suggest ČD locations", false);
    add(
        "/ticketing/reference/passengers",
        "get",
        "List passenger and reduction reference data",
        false,
    );
    add(
        "/ticketing/intents",
        "post",
        "Create a draft order from an opaque Cesta journey reference",
        true,
    );
    add(
        "/ticketing/searches",
        "post",
        "Create a connection search",
        false,
    );
    add(
        "/ticketing/searches/{searchId}",
        "get",
        "Page an owned connection search",
        false,
    );
    add(
        "/ticketing/searches/{searchId}/connections/{connectionId}",
        "get",
        "Get owned connection details",
        false,
    );
    add(
        "/ticketing/orders",
        "post",
        "Create a draft order and price quote",
        true,
    );
    add(
        "/ticketing/orders",
        "get",
        "List the authenticated user's ticketing orders",
        false,
    );
    add(
        "/ticketing/orders/{orderId}",
        "get",
        "Get an owned order",
        false,
    );
    add(
        "/ticketing/orders/{orderId}",
        "delete",
        "Cancel an owned draft order",
        true,
    );
    add(
        "/ticketing/orders/{orderId}/offer",
        "patch",
        "Select a price offer",
        true,
    );
    add(
        "/ticketing/orders/{orderId}/quote-refresh",
        "post",
        "Refresh a draft quote",
        true,
    );
    add(
        "/ticketing/orders/{orderId}/add-ons",
        "get",
        "Get reservation, bicycle, and dog state",
        false,
    );
    add(
        "/ticketing/orders/{orderId}/reservations",
        "patch",
        "Set reservations",
        true,
    );
    add(
        "/ticketing/orders/{orderId}/bicycles",
        "patch",
        "Price or set bicycles",
        true,
    );
    add(
        "/ticketing/orders/{orderId}/dogs",
        "patch",
        "Price or set dogs",
        true,
    );
    add(
        "/ticketing/orders/{orderId}/coach-schema",
        "get",
        "Get coach and seat schema",
        false,
    );
    add(
        "/ticketing/orders/{orderId}/checkout-session",
        "post",
        "Create a provider checkout session",
        true,
    );
    add(
        "/ticketing/orders/{orderId}/complete",
        "post",
        "Verify payment and issue documents",
        true,
    );
    add(
        "/ticketing/orders/{orderId}/documents",
        "get",
        "List owned documents and tickets",
        false,
    );
    add(
        "/ticketing/documents/{documentId}",
        "get",
        "Stream an owned PDF or PNG document",
        false,
    );
    add(
        "/ticketing/tickets/{ticketId}/refund-quote",
        "get",
        "Get refund eligibility and amount",
        false,
    );
    add(
        "/ticketing/tickets/{ticketId}/refunds",
        "post",
        "Submit an idempotent refund",
        true,
    );
    add(
        "/ticketing/tickets/{ticketId}/refunds/latest",
        "get",
        "Get latest refund status",
        false,
    );
    for (path, method, schema) in [
        ("/ticketing/intents", "post", "TicketingIntentRequest"),
        ("/ticketing/searches", "post", "TicketingSearchRequest"),
        ("/ticketing/orders", "post", "TicketingOrderCreateRequest"),
        (
            "/ticketing/orders/{orderId}/offer",
            "patch",
            "TicketingOfferRequest",
        ),
        (
            "/ticketing/orders/{orderId}/reservations",
            "patch",
            "TicketingReservationRequest",
        ),
        (
            "/ticketing/orders/{orderId}/bicycles",
            "patch",
            "TicketingBicycleRequest",
        ),
        (
            "/ticketing/orders/{orderId}/dogs",
            "patch",
            "TicketingDogRequest",
        ),
        (
            "/ticketing/orders/{orderId}/checkout-session",
            "post",
            "TicketingCheckoutRequest",
        ),
        (
            "/ticketing/tickets/{ticketId}/refunds",
            "post",
            "TicketingRefundRequest",
        ),
    ] {
        if let Some(operation) = paths
            .get_mut(path)
            .and_then(|value| value.get_mut(method))
            .and_then(Value::as_object_mut)
        {
            operation.insert("requestBody".into(), json!({"required":true,"content":{"application/json":{"schema":{"$ref":format!("#/components/schemas/{schema}")}}}}));
        }
    }
    for (path, method, status, schema) in [
        (
            "/ticketing/locations",
            "get",
            "200",
            "TicketingLocationSuggestions",
        ),
        (
            "/ticketing/reference/passengers",
            "get",
            "200",
            "TicketingPassengerCatalogue",
        ),
        ("/ticketing/intents", "post", "201", "TicketingIntentResult"),
        (
            "/ticketing/searches",
            "post",
            "201",
            "TicketingConnectionPage",
        ),
        (
            "/ticketing/searches/{searchId}",
            "get",
            "200",
            "TicketingConnectionPage",
        ),
        (
            "/ticketing/searches/{searchId}/connections/{connectionId}",
            "get",
            "200",
            "TicketingConnection",
        ),
        (
            "/ticketing/orders",
            "get",
            "200",
            "TicketingOrderCollection",
        ),
        ("/ticketing/orders", "post", "201", "TicketingOrder"),
        (
            "/ticketing/orders/{orderId}",
            "get",
            "200",
            "TicketingOrder",
        ),
        (
            "/ticketing/orders/{orderId}",
            "delete",
            "200",
            "TicketingOrder",
        ),
        (
            "/ticketing/orders/{orderId}/offer",
            "patch",
            "200",
            "TicketingOrder",
        ),
        (
            "/ticketing/orders/{orderId}/quote-refresh",
            "post",
            "200",
            "TicketingOrder",
        ),
        (
            "/ticketing/orders/{orderId}/add-ons",
            "get",
            "200",
            "TicketingAddOns",
        ),
        (
            "/ticketing/orders/{orderId}/reservations",
            "patch",
            "200",
            "TicketingAddOns",
        ),
        (
            "/ticketing/orders/{orderId}/bicycles",
            "patch",
            "200",
            "TicketingAddOns",
        ),
        (
            "/ticketing/orders/{orderId}/dogs",
            "patch",
            "200",
            "TicketingAddOns",
        ),
        (
            "/ticketing/orders/{orderId}/coach-schema",
            "get",
            "200",
            "TicketingCoachSchema",
        ),
        (
            "/ticketing/orders/{orderId}/checkout-session",
            "post",
            "201",
            "TicketingCheckoutResult",
        ),
        (
            "/ticketing/orders/{orderId}/complete",
            "post",
            "200",
            "TicketingOrder",
        ),
        (
            "/ticketing/orders/{orderId}/documents",
            "get",
            "200",
            "TicketingIssuedDocuments",
        ),
        (
            "/ticketing/tickets/{ticketId}/refund-quote",
            "get",
            "200",
            "TicketingRefundQuote",
        ),
        (
            "/ticketing/tickets/{ticketId}/refunds",
            "post",
            "202",
            "TicketingRefund",
        ),
        (
            "/ticketing/tickets/{ticketId}/refunds/latest",
            "get",
            "200",
            "TicketingRefundStatus",
        ),
    ] {
        if let Some(responses) = paths
            .get_mut(path)
            .and_then(|value| value.get_mut(method))
            .and_then(|value| value.get_mut("responses"))
            .and_then(Value::as_object_mut)
        {
            responses.insert(status.into(),json!({"description":"Stable normalized response","content":{"application/json":{"schema":{"$ref":format!("#/components/schemas/{schema}")}}}}));
        }
    }
    paths.insert("/auth/register".into(),json!({"post":{"summary":"Register a Cesta account","requestBody":{"required":true,"content":{"application/json":{"schema":{"$ref":"#/components/schemas/AuthRegisterRequest"}}}},"responses":{"200":{"description":"Access and refresh tokens","content":{"application/json":{"schema":{"$ref":"#/components/schemas/AuthResponse"}}}},"409":{"description":"Email already registered"}}}}));
    paths.insert("/auth/login".into(),json!({"post":{"summary":"Log in to a Cesta account","requestBody":{"required":true,"content":{"application/json":{"schema":{"$ref":"#/components/schemas/AuthLoginRequest"}}}},"responses":{"200":{"description":"Access and refresh tokens","content":{"application/json":{"schema":{"$ref":"#/components/schemas/AuthResponse"}}}},"401":{"description":"Invalid credentials"}}}}));
    paths.insert("/auth/refresh".into(),json!({"post":{"summary":"Rotate a Cesta session using a refresh token","description":"Refresh tokens are opaque, stored only as SHA-256 hashes, valid for 30 days, and a successful refresh returns a new token pair.","requestBody":{"required":true,"content":{"application/json":{"schema":{"$ref":"#/components/schemas/AuthRefreshRequest"}}}},"responses":{"200":{"description":"New token pair","content":{"application/json":{"schema":{"$ref":"#/components/schemas/AuthResponse"}}}},"401":{"description":"Invalid, expired, or revoked refresh token"}}}}));
    paths.insert("/auth/logout".into(),json!({"post":{"summary":"Revoke a Cesta refresh token","requestBody":{"required":true,"content":{"application/json":{"schema":{"$ref":"#/components/schemas/AuthRefreshRequest"}}}},"responses":{"200":{"description":"Session revoked"}}}}));
    if let Some(operation) = paths
        .get_mut("/journeys/search")
        .and_then(|value| value.get_mut("post"))
        .and_then(Value::as_object_mut)
    {
        operation.insert("responses".into(),json!({"200":{"description":"Cesta journeys with stable ticketing metadata. Live prices require authentication and POST /ticketing/intents.","content":{"application/json":{"schema":{"$ref":"#/components/schemas/JourneySearchWithTicketingResponse"}}}}}));
    }
    if let Some(components) = specification
        .get_mut("components")
        .and_then(Value::as_object_mut)
    {
        components.insert(
            "securitySchemes".into(),
            json!({"bearerAuth":{"type":"http","scheme":"bearer","bearerFormat":"JWT"}}),
        );
        if let Some(schemas) = components.get_mut("schemas").and_then(Value::as_object_mut) {
            schemas.insert("TicketingPassengerGroup".into(),json!({"type":"object","additionalProperties":false,"required":["passengerTypeId","count"],"properties":{"passengerTypeId":{"type":"integer","minimum":1},"count":{"type":"integer","minimum":1,"maximum":10},"age":{"type":["integer","null"],"minimum":1,"maximum":200},"reductionIds":{"type":"array","items":{"type":"integer","minimum":1}}}}));
            schemas.insert("TicketingSearchRequest".into(),json!({"type":"object","additionalProperties":false,"required":["fromLocationId","toLocationId"],"properties":{"fromLocationId":{"type":"string","format":"uuid"},"toLocationId":{"type":"string","format":"uuid"},"viaLocationId":{"type":["string","null"],"format":"uuid"},"changeLocationId":{"type":["string","null"],"format":"uuid"},"dateTime":{"type":["string","null"]},"previous":{"type":"boolean","default":false},"maxCount":{"type":"integer","minimum":1,"maximum":10,"default":5},"maxChanges":{"type":["integer","null"],"minimum":0,"maximum":8},"class":{"type":"integer","enum":[1,2],"default":2},"passengerGroups":{"type":"array","maxItems":8,"items":{"$ref":"#/components/schemas/TicketingPassengerGroup"}}}}));
            schemas.insert("TicketingOrderCreateRequest".into(),json!({"type":"object","additionalProperties":false,"required":["searchId","connectionId"],"properties":{"searchId":{"type":"string","format":"uuid"},"connectionId":{"type":"string","format":"uuid"},"class":{"type":"integer","enum":[1,2],"default":2},"passengerGroups":{"type":"array","items":{"$ref":"#/components/schemas/TicketingPassengerGroup"}}}}));
            schemas.insert("TicketingOfferRequest".into(),json!({"type":"object","additionalProperties":false,"required":["offerType"],"properties":{"offerType":{"type":"integer","enum":[1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,20,21,22,23,25]}}}));
            schemas.insert("TicketingReservationRequest".into(),json!({"type":"object","additionalProperties":false,"required":["trains"],"properties":{"trains":{"type":"array","maxItems":20,"items":{"type":"object","additionalProperties":false,"required":["trainId","resType"],"properties":{"trainId":{"type":"integer","minimum":0},"resType":{"type":"integer"},"count":{"type":["integer","null"],"minimum":0,"maximum":10},"coach":{"type":["integer","null"]},"places":{"type":"array","maxItems":10,"items":{"type":"integer","minimum":1}},"compartment":{"type":["integer","null"]},"place":{"type":["integer","null"]},"berthMaleCount":{"type":["integer","null"]},"berthFemaleCount":{"type":["integer","null"]},"berthTogether":{"type":["boolean","null"]}}}}}}));
            schemas.insert("TicketingBicycleRequest".into(),json!({"type":"object","additionalProperties":false,"required":["count"],"properties":{"count":{"type":"integer","minimum":0,"maximum":6},"priceOnly":{"type":"boolean","default":false},"trains":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["trainId","bikeType"],"properties":{"trainId":{"type":"integer","minimum":0},"bikeType":{"type":"integer","enum":[1,2,4,8,16,32,64,256]}}}}}}));
            schemas.insert("TicketingDogRequest".into(),json!({"type":"object","additionalProperties":false,"required":["count","direction"],"properties":{"count":{"type":"integer","minimum":0,"maximum":6},"direction":{"type":"integer","enum":[1,2,4]},"priceOnly":{"type":"boolean","default":false}}}));
            schemas.insert("TicketingCustomer".into(),json!({"type":"object","additionalProperties":false,"required":["email"],"properties":{"email":{"type":"string","format":"email","maxLength":254},"name":{"type":["string","null"],"maxLength":120},"inCardNumber":{"type":["integer","null"]},"birthDate":{"type":["string","null"]},"companyName":{"type":["string","null"],"maxLength":160}}}));
            schemas.insert("TicketingCheckoutRequest".into(),json!({"type":"object","additionalProperties":false,"required":["customer"],"properties":{"customer":{"$ref":"#/components/schemas/TicketingCustomer"}}}));
            schemas.insert("TicketingRefundRequest".into(),json!({"type":"object","additionalProperties":false,"required":["email"],"properties":{"email":{"type":"string","format":"email","maxLength":254}}}));
            schemas.insert("TicketingIntentRequest".into(),json!({"type":"object","additionalProperties":false,"required":["journeyReference"],"properties":{"journeyReference":{"type":"string","format":"uuid"},"segmentIndexes":{"type":"array","uniqueItems":true,"items":{"type":"integer","minimum":0}},"class":{"type":"integer","enum":[1,2],"default":2},"passengerGroups":{"type":"array","items":{"$ref":"#/components/schemas/TicketingPassengerGroup"}}}}));
            schemas.insert("TicketingLocation".into(),json!({"type":"object","additionalProperties":true,"required":["id","name","type"],"properties":{"id":{"type":"string","format":"uuid"},"name":{"type":"string"},"type":{"type":"integer","enum":[1,2,3]},"typeName":{"type":["string","null"]},"state":{"type":["string","null"]},"region":{"type":["string","null"]}}}));
            schemas.insert("TicketingLocationSuggestions".into(),json!({"type":"object","required":["locations"],"properties":{"locations":{"type":"array","items":{"$ref":"#/components/schemas/TicketingLocation"}}}}));
            schemas.insert("TicketingPassengerCatalogue".into(),json!({"type":"object","additionalProperties":true,"required":["passengers"],"properties":{"passengers":{}}}));
            schemas.insert("TicketingConnection".into(),json!({"type":"object","additionalProperties":true,"required":["id"],"properties":{"id":{"type":"string","format":"uuid"},"trains":{"type":"array","items":{"type":"object","additionalProperties":true}},"remarks":{"type":["object","null"],"additionalProperties":true},"priceOffers":{"type":["object","null"],"additionalProperties":true}}}));
            schemas.insert("TicketingConnectionPage".into(),json!({"type":"object","required":["searchId","connections"],"properties":{"searchId":{"type":"string","format":"uuid"},"allowPrevious":{"type":["boolean","null"]},"allowNext":{"type":["boolean","null"]},"connections":{"type":"array","items":{"$ref":"#/components/schemas/TicketingConnection"}}}}));
            schemas.insert("TicketingOrder".into(),json!({"type":"object","additionalProperties":false,"required":["id","searchId","connectionId","status","currency","quote","version"],"properties":{"id":{"type":"string","format":"uuid"},"searchId":{"type":"string","format":"uuid"},"connectionId":{"type":"string","format":"uuid"},"status":{"type":"string","enum":["draft","checkout_pending","paid","issued","cancelled","issuance_failed","refunded","refund_pending"]},"selectedOfferType":{"type":["integer","null"]},"amountHellers":{"type":["integer","null"],"minimum":0},"currency":{"const":"CZK"},"quote":{"type":"object","additionalProperties":true},"version":{"type":"integer"},"documents":{"type":"array","items":{"$ref":"#/components/schemas/TicketingDocument"}},"tickets":{"type":"array","items":{"$ref":"#/components/schemas/TicketingTicket"}}}}));
            schemas.insert("TicketingOrderCollection".into(),json!({"type":"object","required":["orders"],"properties":{"orders":{"type":"array","items":{"$ref":"#/components/schemas/TicketingOrder"}},"nextCursor":{"type":["string","null"],"format":"uuid"}}}));
            schemas.insert("TicketingIntentResult".into(),json!({"type":"object","required":["journeyReference","searchId","connectionId","order","correlation"],"properties":{"journeyReference":{"type":"string","format":"uuid"},"searchId":{"type":"string","format":"uuid"},"connectionId":{"type":"string","format":"uuid"},"order":{"$ref":"#/components/schemas/TicketingOrder"},"connection":{"oneOf":[{"$ref":"#/components/schemas/TicketingConnection"},{"type":"null"}]},"correlation":{"type":"object","required":["method","toleranceMinutes"],"properties":{"method":{"const":"server_side_schedule_and_train_evidence"},"toleranceMinutes":{"type":"integer"}}}}}));
            schemas.insert("TicketingAddOns".into(),json!({"type":"object","additionalProperties":true,"properties":{"dogs":{"type":["object","null"]},"reservations":{"type":["object","null"]},"bikes":{"type":["object","null"]}}}));
            schemas.insert("TicketingCoachSchema".into(),json!({"type":"object","additionalProperties":true,"properties":{"coachSchemas":{"type":"array","items":{"type":"object","additionalProperties":true}},"legend":{"type":"array","items":{"type":"object","additionalProperties":true}}}}));
            schemas.insert("TicketingDocument".into(),json!({"type":"object","required":["id"],"properties":{"id":{"type":"string","format":"uuid"},"documentType":{"type":["integer","null"]},"contentType":{"type":["string","null"],"enum":["application/pdf","image/png",null]}}}));
            schemas.insert("TicketingTicket".into(),json!({"type":"object","additionalProperties":false,"required":["id","orderId","returned","details"],"properties":{"id":{"type":"string","format":"uuid"},"orderId":{"type":"string","format":"uuid"},"returned":{"type":"boolean"},"details":{"type":"object","additionalProperties":true},"latestRefund":{"oneOf":[{"$ref":"#/components/schemas/TicketingRefund"},{"type":"null"}]}}}));
            schemas.insert("TicketingIssuedDocuments".into(),json!({"type":"object","required":["documents","tickets"],"properties":{"documents":{"type":"array","items":{"$ref":"#/components/schemas/TicketingDocument"}},"tickets":{"type":"array","items":{"$ref":"#/components/schemas/TicketingTicket"}}}}));
            schemas.insert("TicketingCheckoutSession".into(),json!({"type":"object","required":["id","redirectUrl","returnUrl","cancelUrl"],"properties":{"id":{"type":"string"},"redirectUrl":{"type":"string","format":"uri"},"returnUrl":{"type":"string","format":"uri"},"cancelUrl":{"type":"string","format":"uri"},"expiresAt":{"type":["string","null"],"format":"date-time"}}}));
            schemas.insert("TicketingCheckoutResult".into(),json!({"type":"object","required":["order","checkoutSession"],"properties":{"order":{"$ref":"#/components/schemas/TicketingOrder"},"checkoutSession":{"$ref":"#/components/schemas/TicketingCheckoutSession"}}}));
            schemas.insert("TicketingRefundQuote".into(),json!({"type":"object","additionalProperties":true,"properties":{"price2Refund":{"type":["integer","null"],"minimum":0},"errors":{"type":"array","items":{"type":"object","additionalProperties":true}}}}));
            schemas.insert("TicketingRefund".into(),json!({"type":"object","additionalProperties":false,"required":["id","ticketId","status","details"],"properties":{"id":{"type":"string","format":"uuid"},"ticketId":{"type":"string","format":"uuid"},"status":{"type":"string","enum":["requested","processing","settled","rejected"]},"amountHellers":{"type":["integer","null"],"minimum":0},"details":{"type":"object","additionalProperties":true}}}));
            schemas.insert("TicketingRefundStatus".into(),json!({"type":"object","required":["refund","upstreamStatus"],"properties":{"refund":{"oneOf":[{"$ref":"#/components/schemas/TicketingRefund"},{"type":"null"}]},"upstreamStatus":{"type":"object","additionalProperties":true}}}));
            schemas.insert("AuthRegisterRequest".into(),json!({"type":"object","additionalProperties":false,"required":["email","password"],"properties":{"email":{"type":"string","format":"email"},"password":{"type":"string","minLength":8},"display_name":{"type":["string","null"]}}}));
            schemas.insert("AuthLoginRequest".into(),json!({"type":"object","additionalProperties":false,"required":["email","password"],"properties":{"email":{"type":"string","format":"email"},"password":{"type":"string"},"device_name":{"type":["string","null"]}}}));
            schemas.insert("AuthRefreshRequest".into(),json!({"type":"object","additionalProperties":false,"required":["refresh_token"],"properties":{"refresh_token":{"type":"string"}}}));
            schemas.insert("PublicUser".into(),json!({"type":"object","required":["id","email","roles"],"properties":{"id":{"type":"string","format":"uuid"},"email":{"type":"string","format":"email"},"display_name":{"type":["string","null"]},"roles":{"type":"array","items":{"type":"string"}}}}));
            schemas.insert("AuthResponse".into(),json!({"type":"object","required":["access_token","refresh_token","token_type","expires_in_seconds","user"],"properties":{"access_token":{"type":"string","description":"JWT access token valid for 900 seconds"},"refresh_token":{"type":"string","description":"Opaque refresh token; store in platform secure storage"},"token_type":{"const":"Bearer"},"expires_in_seconds":{"const":900},"user":{"$ref":"#/components/schemas/PublicUser"}}}));
            schemas.insert("JourneyTicketingSegment".into(),json!({"type":"object","required":["legIndex","availability","ticketable"],"properties":{"legIndex":{"type":"integer","minimum":0},"provider":{"type":["string","null"],"enum":["cd",null]},"availability":{"type":"string","enum":["authentication_required","unsupported_mode"]},"ticketable":{"type":"null","description":"Actual sellability is resolved server-side by POST /ticketing/intents"}}}));
            schemas.insert("JourneyTicketingMetadata".into(),json!({"type":"object","required":["provider","journeyReference","authenticationRequired","availability","indicativePriceHellers","currency","completeJourneyTicketable","segments"],"properties":{"provider":{"type":["string","null"],"enum":["cd",null]},"journeyReference":{"type":["string","null"],"format":"uuid"},"authenticationRequired":{"const":true},"availability":{"type":"string","enum":["authentication_required","not_supported"]},"indicativePriceHellers":{"type":"null","description":"Public route search does not load live ČD fares"},"currency":{"const":"CZK"},"completeJourneyTicketable":{"type":["boolean","null"]},"scope":{"type":["string","null"],"enum":["complete_journey_candidate","segments",null]},"segments":{"type":"array","items":{"$ref":"#/components/schemas/JourneyTicketingSegment"}}}}));
            schemas.insert("JourneySearchWithTicketingResponse".into(),json!({"type":"object","required":["journeys","data_status","warnings"],"properties":{"journeys":{"type":"array","items":{"type":"object","additionalProperties":true,"required":["ticketing"],"properties":{"ticketing":{"$ref":"#/components/schemas/JourneyTicketingMetadata"}}}},"related":{"type":["object","null"],"additionalProperties":true},"data_status":{"type":"object","additionalProperties":true},"warnings":{"type":"array","items":{"type":"string"}}}}));
        }
    }
}

async fn locations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<LocationQuery>,
) -> Result<Json<Value>, ApiError> {
    let u = current_user(&state, &headers).await?;
    state.ticketing.limit(u.id, "search", SEARCH_LIMIT).await?;
    let query = q.q.trim();
    if query.len() < 2 || query.len() > 80 {
        return Err(validation("q must contain 2 to 80 characters"));
    }
    if q.r#type.is_some_and(|v| !(1..=3).contains(&v)) {
        return Err(validation("type must be 1, 2, or 3"));
    }
    let limit = q.limit.unwrap_or(10);
    if !(1..=30).contains(&limit) {
        return Err(validation("limit must be between 1 and 30"));
    }
    let cache_key = format!(
        "locations:{:?}:{}:{}",
        q.r#type,
        limit,
        query.to_lowercase()
    );
    let cached = state
        .ticketing
        .reference_cache
        .read()
        .await
        .get(&cache_key)
        .filter(|(at, _)| at.elapsed() < Duration::from_secs(600))
        .map(|(_, value)| value.clone());
    let raw = if let Some(value) = cached {
        value
    } else {
        let (_, value) = state
            .ticketing
            .client()?
            .search_locations(query, q.r#type, limit)
            .await
            .map_err(cd_error)?;
        state
            .ticketing
            .reference_cache
            .write()
            .await
            .insert(cache_key, (Instant::now(), value.clone()));
        value
    };
    let mut out = Vec::new();
    for item in raw
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let t = i32_field(&item, "type")?;
        let key = i32_field(&item, "key")?;
        let id = Uuid::new_v4();
        state.ticketing.memory.write().await.locations.insert(
            id,
            LocationRecord {
                user_id: u.id,
                upstream_type: t,
                upstream_key: key,
                payload: item.clone(),
            },
        );
        persist_location(&state.ticketing, u.id, id, t, key, &item).await?;
        let mut public = item;
        remove_keys(&mut public, &["key"]);
        if let Value::Object(map) = &mut public {
            map.insert("id".into(), json!(id));
        }
        out.push(public);
    }
    Ok(Json(json!({"locations":out})))
}

async fn passengers(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let u = current_user(&state, &headers).await?;
    state.ticketing.limit(u.id, "search", SEARCH_LIMIT).await?;
    let cached = state
        .ticketing
        .reference_cache
        .read()
        .await
        .get("passengers")
        .filter(|(at, _)| at.elapsed() < Duration::from_secs(86400))
        .map(|(_, value)| value.clone());
    let raw = if let Some(value) = cached {
        value
    } else {
        let (_, value) = state
            .ticketing
            .client()?
            .passenger_types()
            .await
            .map_err(cd_error)?;
        state
            .ticketing
            .reference_cache
            .write()
            .await
            .insert("passengers".into(), (Instant::now(), value.clone()));
        value
    };
    Ok(Json(
        json!({"passengers":raw.get("passengers").or_else(||raw.get("data")).cloned().unwrap_or(raw)}),
    ))
}

async fn create_intent(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<TicketingIntentRequest>,
) -> Result<Response, ApiError> {
    let user = current_user(&state, &headers).await?;
    state
        .ticketing
        .limit(user.id, "mutation", MUTATION_LIMIT)
        .await?;
    let body_value = json!(&body);
    idempotent(&state.ticketing,user.id,"create_intent",&headers,&body_value,||async {
        let reference=state.ticketing.journey_reference(body.journey_reference).await?;
        let legs=reference.snapshot.get("legs").and_then(Value::as_array).ok_or_else(||api_error("journey_reference_invalid","Stored journey reference is invalid"))?;
        let candidates=reference.snapshot.get("candidateSegmentIndexes").and_then(Value::as_array).into_iter().flatten().filter_map(|value|value.as_u64().map(|value|value as usize)).collect::<Vec<_>>();
        let mut selected=if body.segment_indexes.is_empty(){candidates.clone()}else{body.segment_indexes.clone()};
        selected.sort_unstable();selected.dedup();
        if selected.is_empty()||selected.iter().any(|index|!candidates.contains(index)||*index>=legs.len()){return Err(validation("segmentIndexes must select ticketing candidate segments"));}
        let first=*selected.first().expect("non-empty");let last=*selected.last().expect("non-empty");
        if legs[first..=last].iter().any(|leg|!matches!(leg.get("mode").and_then(Value::as_str),Some("train"|"walk"))){return Err(api_error("mixed_journey_not_supported","Selected ticketing segments must be contiguous train legs"));}
        let from_name=legs[first].get("fromName").and_then(Value::as_str).ok_or_else(||api_error("journey_not_correlatable","Journey origin has no station name"))?;
        let to_name=legs[last].get("toName").and_then(Value::as_str).ok_or_else(||api_error("journey_not_correlatable","Journey destination has no station name"))?;
        let from=resolve_cd_station(state.ticketing.client()?,from_name).await?;let to=resolve_cd_station(state.ticketing.client()?,to_name).await?;
        let from_key=from.key.ok_or_else(||api_error("upstream_malformed_response","ČD station has no key"))?;let to_key=to.key.ok_or_else(||api_error("upstream_malformed_response","ČD station has no key"))?;
        let service_date=reference.snapshot.get("serviceDate").and_then(Value::as_str).and_then(|value|NaiveDate::parse_from_str(value,"%Y-%m-%d").ok()).ok_or_else(||api_error("journey_reference_invalid","Journey service date is invalid"))?;
        let departure=legs[first].get("departureSeconds").and_then(Value::as_u64).ok_or_else(||api_error("journey_reference_invalid","Journey departure is invalid"))? as u32;
        let arrival=legs[last].get("arrivalSeconds").and_then(Value::as_u64).ok_or_else(||api_error("journey_reference_invalid","Journey arrival is invalid"))? as u32;
        let booking=booking_request(&body.passenger_groups,body.class,4097)?;
        let upstream_request=SearchConnectionsRequest{from_type:3,from:from_key,to_type:3,to:to_key,via_type:None,via:None,change_type:None,change:None,date_time:Some(cd_date_time(service_date,departure)?),previous:false,max_count:10,parameters:ConnectionParameters{max_change:Some(8),..Default::default()},booking:booking.clone()};
        let(typed,raw)=state.ticketing.client()?.search_connections(&upstream_request).await.map_err(cd_error)?;
        let handle=typed.handle.ok_or_else(||api_error("upstream_malformed_response","ČD response did not contain a search handle"))?;
        let tolerance=std::env::var("CD_TICKET_CORRELATION_TOLERANCE_MINUTES").ok().and_then(|value|value.parse::<i64>().ok()).unwrap_or(10).clamp(1,30);
        let matched=correlate_connection(&raw,legs,&selected,service_date,departure,arrival,tolerance)?;
        let from_id=store_resolved_location(&state.ticketing,user.id,&from).await?;let to_id=store_resolved_location(&state.ticketing,user.id,&to).await?;
        let search_id=Uuid::new_v4();let(all_connections,public)=public_connections(typed.conn_info.as_ref().map(|value|&value.connections),raw.get("connInfo").unwrap_or(&raw));
        let connection_id=all_connections.iter().find_map(|(opaque,upstream)|(*upstream==matched).then_some(*opaque)).ok_or_else(||api_error("journey_not_correlatable","Matched ČD connection is missing from the result map"))?;
        let request=SearchRequest{from_location_id:from_id,to_location_id:to_id,via_location_id:None,change_location_id:None,date_time:Some(cd_date_time(service_date,departure)?),previous:false,max_count:10,max_changes:Some(8),passenger_groups:body.passenger_groups.clone(),class:body.class};
        let search=SearchRecord{id:search_id,user_id:user.id,handle,request,raw:raw.clone(),connections:all_connections,expires_at:Utc::now()+chrono::Duration::minutes(30)};
        state.ticketing.memory.write().await.searches.insert(search_id,search.clone());persist_search(&state.ticketing,&search).await?;
        let quote_booking=booking_request(&body.passenger_groups,body.class,QUOTE_CREATE_FLAGS)?;
        let(_,quote)=state.ticketing.client()?.create_price_offer(handle,matched,Some(QUOTE_CREATE_FLAGS),&quote_booking).await.map_err(cd_error)?;
        let booking_id=quote.get("bookingId").and_then(Value::as_str).ok_or_else(||api_error("upstream_malformed_response","ČD quote has no booking identifier"))?.to_string();
        let order=OrderRecord{id:Uuid::new_v4(),user_id:user.id,search_id,connection_id,conn_id:matched,booking_id,status:"draft".into(),selected_offer_type:None,amount_hellers:offer_amount(&quote),customer:None,quote,checkout_session_id:None,version:0};
        state.ticketing.memory.write().await.orders.insert(order.id,order.clone());persist_order(&state.ticketing,&order).await?;
        Ok((StatusCode::CREATED,json!({"journeyReference":body.journey_reference,"searchId":search_id,"connectionId":connection_id,"order":public_order(&order),"connection":public.into_iter().find(|value|value.get("id")==Some(&json!(connection_id))),"correlation":{"method":"server_side_schedule_and_train_evidence","toleranceMinutes":tolerance}})))
    }).await
}

async fn create_search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SearchRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let u = current_user(&state, &headers).await?;
    state.ticketing.limit(u.id, "search", SEARCH_LIMIT).await?;
    validate_search(&body)?;
    let from = owned_location(&state.ticketing, u.id, body.from_location_id).await?;
    let to = owned_location(&state.ticketing, u.id, body.to_location_id).await?;
    let via = optional_location(&state.ticketing, u.id, body.via_location_id).await?;
    let change = optional_location(&state.ticketing, u.id, body.change_location_id).await?;
    let booking = booking_request(&body.passenger_groups, body.class, 4097)?;
    let request = SearchConnectionsRequest {
        from_type: from.upstream_type,
        from: from.upstream_key,
        to_type: to.upstream_type,
        to: to.upstream_key,
        via_type: via.as_ref().map(|x| x.upstream_type),
        via: via.map(|x| x.upstream_key),
        change_type: change.as_ref().map(|x| x.upstream_type),
        change: change.map(|x| x.upstream_key),
        date_time: body.date_time.clone(),
        previous: body.previous,
        max_count: body.max_count,
        parameters: ConnectionParameters {
            max_change: body.max_changes,
            ..Default::default()
        },
        booking,
    };
    let (typed, raw) = state
        .ticketing
        .client()?
        .search_connections(&request)
        .await
        .map_err(cd_error)?;
    let handle = typed.handle.ok_or_else(|| {
        api_error(
            "upstream_malformed_response",
            "ČD response did not contain a search handle",
        )
    })?;
    let id = Uuid::new_v4();
    let (connections, public) = public_connections(
        typed.conn_info.as_ref().map(|x| &x.connections),
        raw.get("connInfo").unwrap_or(&raw),
    );
    let record = SearchRecord {
        id,
        user_id: u.id,
        handle,
        request: body,
        raw: raw.clone(),
        connections: connections.clone(),
        expires_at: Utc::now() + chrono::Duration::minutes(30),
    };
    state
        .ticketing
        .memory
        .write()
        .await
        .searches
        .insert(id, record.clone());
    persist_search(&state.ticketing, &record).await?;
    Ok((
        StatusCode::CREATED,
        Json(
            json!({"searchId":id,"allowPrevious":typed.conn_info.as_ref().and_then(|x|x.allow_prev),"allowNext":typed.conn_info.as_ref().and_then(|x|x.allow_next),"connections":public}),
        ),
    ))
}

async fn search_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    let u = current_user(&state, &headers).await?;
    state.ticketing.limit(u.id, "search", SEARCH_LIMIT).await?;
    let mut s = owned_search(&state.ticketing, u.id, id).await?;
    let anchor = q
        .cursor
        .map(|c| s.connections.get(&c).copied().ok_or_else(not_found_owned))
        .transpose()?;
    let req = PageRequest {
        conn_id: anchor,
        previous: false,
        listed_count: 0,
        max_count: 5,
        booking: booking_request(&s.request.passenger_groups, s.request.class, 4097)?,
    };
    let (typed, raw) = state
        .ticketing
        .client()?
        .connections_page(s.handle, &req)
        .await
        .map_err(cd_error)?;
    let (map, public) = public_connections(Some(&typed.connections), &raw);
    s.connections.extend(map);
    s.raw = raw;
    state
        .ticketing
        .memory
        .write()
        .await
        .searches
        .insert(id, s.clone());
    persist_search(&state.ticketing, &s).await?;
    Ok(Json(
        json!({"searchId":id,"allowPrevious":typed.allow_prev,"allowNext":typed.allow_next,"connections":public}),
    ))
}

async fn connection_detail(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((sid, cid)): Path<(Uuid, Uuid)>,
) -> Result<Json<Value>, ApiError> {
    let u = current_user(&state, &headers).await?;
    let s = owned_search(&state.ticketing, u.id, sid).await?;
    let upstream = *s.connections.get(&cid).ok_or_else(not_found_owned)?;
    let booking = booking_request(&s.request.passenger_groups, s.request.class, 4097)?;
    let (_, mut raw) = state
        .ticketing
        .client()?
        .connection_detail(s.handle, upstream, Some(4097), &booking)
        .await
        .map_err(cd_error)?;
    remove_keys(&mut raw, &["id", "handle", "connId", "bookingId"]);
    if let Value::Object(map) = &mut raw {
        map.insert("id".into(), json!(cid));
    }
    Ok(Json(raw))
}

async fn list_orders(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<OrdersQuery>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let allowed = [
        "draft",
        "checkout_pending",
        "paid",
        "issued",
        "cancelled",
        "issuance_failed",
        "refunded",
        "refund_pending",
    ];
    if query
        .status
        .as_ref()
        .is_some_and(|status| !allowed.contains(&status.as_str()))
    {
        return Err(validation("unsupported order status"));
    }
    let limit = query.limit.unwrap_or(20);
    if !(1..=50).contains(&limit) {
        return Err(validation("limit must be between 1 and 50"));
    }
    if let Some(db) = &state.ticketing.db {
        let cursor_created = if let Some(cursor) = query.cursor {
            Some(
                sqlx::query_scalar::<_, DateTime<Utc>>(
                    "SELECT created_at FROM cd_ticketing_orders WHERE id=$1 AND user_id=$2",
                )
                .bind(cursor)
                .bind(user.id)
                .fetch_optional(db)
                .await
                .map_err(db_error)?
                .ok_or_else(not_found_owned)?,
            )
        } else {
            None
        };
        let rows=sqlx::query("SELECT * FROM cd_ticketing_orders WHERE user_id=$1 AND ($2::text IS NULL OR status=$2) AND ($3::timestamptz IS NULL OR (created_at,id)<($3,$4)) ORDER BY created_at DESC,id DESC LIMIT $5").bind(user.id).bind(query.status.as_deref()).bind(cursor_created).bind(query.cursor.unwrap_or(Uuid::nil())).bind(limit+1).fetch_all(db).await.map_err(db_error)?;
        let has_more = rows.len() as i64 > limit;
        let mut orders = Vec::new();
        for row in rows.into_iter().take(limit as usize) {
            orders.push(order_collection_item(&state.ticketing, order_from_row(row)?).await?);
        }
        let next_cursor = has_more
            .then(|| orders.last().and_then(|value| value.get("id")).cloned())
            .flatten();
        return Ok(Json(json!({"orders":orders,"nextCursor":next_cursor})));
    }
    let memory = state.ticketing.memory.read().await;
    let mut records = memory
        .orders
        .values()
        .filter(|order| {
            order.user_id == user.id
                && query
                    .status
                    .as_ref()
                    .is_none_or(|status| &order.status == status)
        })
        .cloned()
        .collect::<Vec<_>>();
    records.sort_by_key(|order| std::cmp::Reverse(order.id));
    if let Some(cursor) = query.cursor {
        records.retain(|order| order.id < cursor);
    }
    let has_more = records.len() as i64 > limit;
    let selected = records.into_iter().take(limit as usize).collect::<Vec<_>>();
    drop(memory);
    let mut orders = Vec::new();
    for order in selected {
        orders.push(order_collection_item(&state.ticketing, order).await?);
    }
    let next_cursor = has_more
        .then(|| orders.last().and_then(|value| value.get("id")).cloned())
        .flatten();
    Ok(Json(json!({"orders":orders,"nextCursor":next_cursor})))
}

async fn order_collection_item(
    service: &TicketingService,
    order: OrderRecord,
) -> Result<Value, ApiError> {
    let mut value = public_order(&order);
    let (documents, tickets) = if let Some(db) = &service.db {
        let documents=sqlx::query("SELECT id,document_type FROM cd_ticketing_documents WHERE user_id=$1 AND order_id=$2 ORDER BY created_at").bind(order.user_id).bind(order.id).fetch_all(db).await.map_err(db_error)?.into_iter().map(|row|{let document_type:Option<i32>=row.get("document_type");json!({"id":row.get::<Uuid,_>("id"),"documentType":document_type,"contentType":content_type_for_document_type(document_type)})}).collect::<Vec<_>>();
        let rows=sqlx::query("SELECT id,user_id,order_id,upstream_ticket_id,payload,returned FROM cd_ticketing_tickets WHERE user_id=$1 AND order_id=$2 ORDER BY created_at").bind(order.user_id).bind(order.id).fetch_all(db).await.map_err(db_error)?;
        let mut tickets = Vec::new();
        for row in rows {
            let ticket = TicketRecord {
                id: row.get("id"),
                user_id: row.get("user_id"),
                order_id: row.get("order_id"),
                upstream_id: row.get("upstream_ticket_id"),
                payload: row.get("payload"),
                returned: row.get("returned"),
            };
            let mut item = public_ticket(&ticket);
            let refund=sqlx::query("SELECT id,user_id,ticket_id,status,amount_hellers,payload FROM cd_ticketing_refunds WHERE user_id=$1 AND ticket_id=$2 ORDER BY updated_at DESC LIMIT 1").bind(order.user_id).bind(ticket.id).fetch_optional(db).await.map_err(db_error)?.map(|row|public_refund(&RefundRecord{id:row.get("id"),user_id:row.get("user_id"),ticket_id:row.get("ticket_id"),status:row.get("status"),amount_hellers:row.get::<Option<i32>,_>("amount_hellers").map(i64::from),payload:row.get("payload")}));
            item["latestRefund"] = json!(refund);
            tickets.push(item);
        }
        (documents, tickets)
    } else {
        let memory = service.memory.read().await;
        let documents=memory.documents.values().filter(|document|document.user_id==order.user_id&&document.order_id==order.id).map(|document|json!({"id":document.id,"documentType":document.document_type,"contentType":content_type_for_document_type(document.document_type)})).collect();
        let tickets = memory
            .tickets
            .values()
            .filter(|ticket| ticket.user_id == order.user_id && ticket.order_id == order.id)
            .map(|ticket| {
                let mut item = public_ticket(ticket);
                let refund = memory
                    .refunds
                    .values()
                    .filter(|refund| {
                        refund.user_id == order.user_id && refund.ticket_id == ticket.id
                    })
                    .max_by_key(|refund| refund.id)
                    .map(public_refund);
                item["latestRefund"] = json!(refund);
                item
            })
            .collect();
        (documents, tickets)
    };
    value["documents"] = json!(documents);
    value["tickets"] = json!(tickets);
    Ok(value)
}

async fn create_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<OrderCreateRequest>,
) -> Result<Response, ApiError> {
    let u = current_user(&state, &headers).await?;
    state
        .ticketing
        .limit(u.id, "mutation", MUTATION_LIMIT)
        .await?;
    idempotent(
        &state.ticketing,
        u.id,
        "create_order",
        &headers,
        &body,
        || async {
            let s = owned_search(&state.ticketing, u.id, body.search_id).await?;
            let conn = *s
                .connections
                .get(&body.connection_id)
                .ok_or_else(not_found_owned)?;
            let booking = booking_request(&body.passenger_groups, body.class, QUOTE_CREATE_FLAGS)?;
            let (_, raw) = state
                .ticketing
                .client()?
                .create_price_offer(s.handle, conn, Some(QUOTE_CREATE_FLAGS), &booking)
                .await
                .map_err(cd_error)?;
            let booking_id = raw
                .get("bookingId")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    api_error(
                        "upstream_malformed_response",
                        "ČD quote has no booking identifier",
                    )
                })?
                .to_string();
            let id = Uuid::new_v4();
            let amount = offer_amount(&raw);
            let record = OrderRecord {
                id,
                user_id: u.id,
                search_id: s.id,
                connection_id: body.connection_id,
                conn_id: conn,
                booking_id,
                status: "draft".into(),
                selected_offer_type: None,
                amount_hellers: amount,
                customer: None,
                quote: raw,
                checkout_session_id: None,
                version: 0,
            };
            state
                .ticketing
                .memory
                .write()
                .await
                .orders
                .insert(id, record.clone());
            persist_order(&state.ticketing, &record).await?;
            Ok((StatusCode::CREATED, public_order(&record)))
        },
    )
    .await
}

async fn get_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let u = current_user(&state, &headers).await?;
    Ok(Json(public_order(
        &owned_order(&state.ticketing, u.id, id).await?,
    )))
}

async fn refresh_quote(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let user = current_user(&state, &headers).await?;
    idempotent(
        &state.ticketing,
        user.id,
        "refresh_quote",
        &headers,
        &id,
        || async {
            let _guard = state.ticketing.mutation_guard(id).await?;
            let mut order = owned_order(&state.ticketing, user.id, id).await?;
            require_draft(&order)?;
            let (_, raw) = state
                .ticketing
                .client()?
                .refresh_price_offer(&order.booking_id, Some(QUOTE_CREATE_FLAGS))
                .await
                .map_err(cd_error)?;
            order.amount_hellers = offer_amount(&raw);
            order.quote = raw;
            order.version += 1;
            save_order(&state.ticketing, &order).await?;
            Ok((StatusCode::OK, public_order(&order)))
        },
    )
    .await
}

async fn select_offer(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(body): Json<OfferRequest>,
) -> Result<Response, ApiError> {
    let u = current_user(&state, &headers).await?;
    if ![
        1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 20, 21, 22, 23, 25,
    ]
    .contains(&body.offer_type)
    {
        return Err(validation("unsupported offerType"));
    }
    idempotent(
        &state.ticketing,
        u.id,
        "select_offer",
        &headers,
        &body.offer_type,
        || async {
            let _guard = state.ticketing.mutation_guard(id).await?;
            let mut o = owned_order(&state.ticketing, u.id, id).await?;
            require_draft(&o)?;
            let (_, raw) = state
                .ticketing
                .client()?
                .select_price_offer(&o.booking_id, Some(body.offer_type))
                .await
                .map_err(cd_error)?;
            o.selected_offer_type = Some(body.offer_type);
            o.amount_hellers = offer_amount(&raw).or(o.amount_hellers);
            o.quote = raw;
            o.version += 1;
            save_order(&state.ticketing, &o).await?;
            Ok((StatusCode::OK, public_order(&o)))
        },
    )
    .await
}

async fn cancel_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let u = current_user(&state, &headers).await?;
    idempotent(
        &state.ticketing,
        u.id,
        "cancel_order",
        &headers,
        &id,
        || async {
            let _guard = state.ticketing.mutation_guard(id).await?;
            let mut o = owned_order(&state.ticketing, u.id, id).await?;
            if o.status == "cancelled" {
                return Ok((StatusCode::OK, public_order(&o)));
            }
            require_draft(&o)?;
            state
                .ticketing
                .client()?
                .release_price_offer(&o.booking_id)
                .await
                .map_err(cd_error)?;
            o.status = "cancelled".into();
            o.version += 1;
            save_order(&state.ticketing, &o).await?;
            audit(
                &state.ticketing,
                Some(u.id),
                Some(o.id),
                "order_cancelled",
                "success",
                json!({}),
            )
            .await?;
            Ok((StatusCode::OK, public_order(&o)))
        },
    )
    .await
}

async fn add_ons(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let u = current_user(&state, &headers).await?;
    let o = owned_order(&state.ticketing, u.id, id).await?;
    let (_, raw) = state
        .ticketing
        .client()?
        .additional_services(&o.booking_id)
        .await
        .map_err(cd_error)?;
    Ok(Json(sanitize_public(raw)))
}

async fn set_reservations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(body): Json<ReservationRequest>,
) -> Result<Response, ApiError> {
    validate_reservations(&body)?;
    let request = body.clone();
    mutate_addon(&state, headers, id, "reservations", &body, |client, bid| {
        Box::pin(async move {
            client
                .set_reservations(&bid, &json!({"trains":request.trains}))
                .await
                .map(|(_, v)| v)
        })
    })
    .await
}
async fn set_bicycles(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(body): Json<BicycleRequest>,
) -> Result<Response, ApiError> {
    if !(0..=6).contains(&body.count) {
        return Err(validation("count must be between 0 and 6"));
    }
    if body.trains.len() > 20
        || body.trains.iter().any(|item| {
            item.train_id < 0 || ![1, 2, 4, 8, 16, 32, 64, 256].contains(&item.bike_type)
        })
    {
        return Err(validation("invalid bicycle train selection"));
    }
    let request = body.clone();
    mutate_addon(&state, headers, id, "bicycles", &body, |client, bid| {
        Box::pin(async move {
            let data = json!({"trains":request.trains});
            if request.price_only {
                client
                    .bike_price(&bid, request.count, &data)
                    .await
                    .map(|(_, v)| v)
            } else {
                client
                    .set_bikes(&bid, request.count, &data)
                    .await
                    .map(|(_, v)| v)
            }
        })
    })
    .await
}
async fn set_dogs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(body): Json<DogRequest>,
) -> Result<Response, ApiError> {
    if !(0..=6).contains(&body.count) || ![1, 2, 4].contains(&body.direction) {
        return Err(validation("invalid dog count or direction"));
    }
    mutate_addon(&state, headers, id, "dogs", &body, |client, bid| {
        Box::pin(async move {
            if body.price_only {
                client
                    .dog_price(&bid, body.count, body.direction)
                    .await
                    .map(|(_, v)| v)
            } else {
                client
                    .set_dogs(&bid, body.count, body.direction)
                    .await
                    .map(|(_, v)| v)
            }
        })
    })
    .await
}

type BoxCdFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, CdError>> + Send + 'a>>;
async fn mutate_addon<T: Serialize>(
    state: &AppState,
    headers: HeaderMap,
    id: Uuid,
    op: &'static str,
    body: &T,
    call: impl for<'a> FnOnce(&'a Arc<dyn CdApi>, String) -> BoxCdFuture<'a>,
) -> Result<Response, ApiError> {
    let u = current_user(state, &headers).await?;
    idempotent(&state.ticketing, u.id, op, &headers, body, || async {
        let _guard = state.ticketing.mutation_guard(id).await?;
        let o = owned_order(&state.ticketing, u.id, id).await?;
        require_draft(&o)?;
        let raw = call(state.ticketing.client()?, o.booking_id.clone())
            .await
            .map_err(cd_error)?;
        Ok((StatusCode::OK, sanitize_public(raw)))
    })
    .await
}

async fn coach_schema(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(q): Query<CoachQuery>,
) -> Result<Json<Value>, ApiError> {
    let u = current_user(&state, &headers).await?;
    let o = owned_order(&state.ticketing, u.id, id).await?;
    if !(200..=2000).contains(&q.width) || q.max_height.is_some_and(|v| !(200..=3000).contains(&v))
    {
        return Err(validation("invalid schema dimensions"));
    }
    let (_, info) = state
        .ticketing
        .client()?
        .reservations(&o.booking_id)
        .await
        .map_err(cd_error)?;
    let schema_info = find_schema_info(&info, q.train_id)
        .ok_or_else(|| api_error("not_found", "No coach schema is available for this train"))?;
    let req = SchemaRequest {
        flags: 1 | (if q.coach.is_some() { 4 } else { 0 }),
        schema_info,
        coach_number: q.coach,
        self_reserved_seats: vec![],
        schema_width: q.width,
        schema_max_height: q.max_height,
        vertical_schema: false,
        class: None,
    };
    let (_, raw) = state
        .ticketing
        .client()?
        .coach_schema(&req)
        .await
        .map_err(cd_error)?;
    Ok(Json(sanitize_public(raw)))
}

async fn checkout(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(body): Json<CheckoutRequest>,
) -> Result<Response, ApiError> {
    let u = current_user(&state, &headers).await?;
    validate_customer(&body.customer)?;
    idempotent(
        &state.ticketing,
        u.id,
        "checkout",
        &headers,
        &body.customer,
        || async {
            let _guard = state.ticketing.mutation_guard(id).await?;
            let mut o = owned_order(&state.ticketing, u.id, id).await?;
            require_draft(&o)?;
            let amount = o.amount_hellers.ok_or_else(|| {
                api_error(
                    "quote_incomplete",
                    "The selected offer has no payable amount",
                )
            })?;
            let p = PaymentOrder {
                order_id: o.id,
                amount_hellers: amount,
                currency: "CZK",
            };
            let session = state
                .ticketing
                .payment
                .create_checkout(&p)
                .await
                .map_err(payment_error)?;
            o.status = "checkout_pending".into();
            o.customer = Some(body.customer.clone());
            o.checkout_session_id = Some(session.id.clone());
            o.version += 1;
            save_order(&state.ticketing, &o).await?;
            audit(
                &state.ticketing,
                Some(u.id),
                Some(o.id),
                "checkout_created",
                "success",
                json!({"amountHellers":amount,"currency":"CZK"}),
            )
            .await?;
            Ok((
                StatusCode::CREATED,
                json!({"order":public_order(&o),"checkoutSession":session}),
            ))
        },
    )
    .await
}

async fn complete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let u = current_user(&state, &headers).await?;
    idempotent(
        &state.ticketing,
        u.id,
        "complete",
        &headers,
        &id,
        || async {
            let _guard = state.ticketing.mutation_guard(id).await?;
            let mut o = owned_order(&state.ticketing, u.id, id).await?;
            if o.status == "issued" {
                return Ok((StatusCode::OK, public_order(&o)));
            }
            if o.status == "paid" || o.status == "issuance_failed" {
                let (_, sold) = state
                    .ticketing
                    .client()?
                    .sold_tickets(&o.booking_id)
                    .await
                    .map_err(cd_error)?;
                if sold
                    .get("tickets")
                    .and_then(Value::as_array)
                    .is_some_and(|tickets| !tickets.is_empty())
                {
                    persist_issued(&state.ticketing, &mut o, &sold).await?;
                    return Ok((StatusCode::OK, public_order(&o)));
                }
                return Err(api_error(
                    "issuance_pending",
                    "Payment is verified but ČD issuance could not yet be reconciled",
                ));
            }
            if o.status != "checkout_pending" {
                return Err(api_error(
                    "invalid_order_state",
                    "Order is not awaiting payment",
                ));
            }
            let amount = o
                .amount_hellers
                .ok_or_else(|| api_error("quote_incomplete", "Order amount is unavailable"))?;
            let session = o
                .checkout_session_id
                .clone()
                .ok_or_else(|| api_error("invalid_order_state", "Checkout session is missing"))?;
            state
                .ticketing
                .payment
                .verify_settled(
                    &session,
                    &PaymentOrder {
                        order_id: o.id,
                        amount_hellers: amount,
                        currency: "CZK",
                    },
                )
                .await
                .map_err(payment_error)?;
            audit(
                &state.ticketing,
                Some(u.id),
                Some(o.id),
                "payment_verified",
                "success",
                json!({"amountHellers":amount,"currency":"CZK"}),
            )
            .await?;
            let customer = o
                .customer
                .clone()
                .ok_or_else(|| api_error("invalid_order_state", "Customer data is missing"))?;
            let c = CustomerInfo {
                email: customer.email,
                name: customer.name,
                in_card_number: customer.in_card_number,
                birth_date: customer.birth_date,
                company_name: customer.company_name,
            };
            state
                .ticketing
                .client()?
                .fix_price_offer(&o.booking_id, &c)
                .await
                .map_err(cd_error)?;
            o.status = "paid".into();
            save_order(&state.ticketing, &o).await?;
            match state
                .ticketing
                .client()?
                .sell_tickets(&o.booking_id, Some(&o.id.to_string()), None)
                .await
            {
                Ok((_, raw)) => {
                    persist_issued(&state.ticketing, &mut o, &raw).await?;
                    audit(
                        &state.ticketing,
                        Some(u.id),
                        Some(o.id),
                        "documents_issued",
                        "success",
                        json!({}),
                    )
                    .await?;
                    Ok((StatusCode::OK, public_order(&o)))
                }
                Err(error) => {
                    o.status = "issuance_failed".into();
                    save_order(&state.ticketing, &o).await?;
                    audit(
                        &state.ticketing,
                        Some(u.id),
                        Some(o.id),
                        "documents_issued",
                        "failure",
                        json!({"errorCode":error.stable_code()}),
                    )
                    .await?;
                    Err(cd_error(error))
                }
            }
        },
    )
    .await
}

async fn order_documents(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let u = current_user(&state, &headers).await?;
    let _ = owned_order(&state.ticketing, u.id, id).await?;
    if let Some(db) = &state.ticketing.db {
        let docs = sqlx::query("SELECT id,document_type FROM cd_ticketing_documents WHERE user_id=$1 AND order_id=$2 ORDER BY created_at")
            .bind(u.id).bind(id).fetch_all(db).await.map_err(db_error)?.into_iter().map(|row| {
                let document_type:Option<i32>=row.get("document_type");
                json!({"id":row.get::<Uuid,_>("id"),"documentType":document_type,"contentType":content_type_for_document_type(document_type)})
            }).collect::<Vec<_>>();
        let tickets = sqlx::query("SELECT id,user_id,order_id,upstream_ticket_id,payload,returned FROM cd_ticketing_tickets WHERE user_id=$1 AND order_id=$2 ORDER BY created_at")
            .bind(u.id).bind(id).fetch_all(db).await.map_err(db_error)?.into_iter().map(|row| public_ticket(&TicketRecord{id:row.get("id"),user_id:row.get("user_id"),order_id:row.get("order_id"),upstream_id:row.get("upstream_ticket_id"),payload:row.get("payload"),returned:row.get("returned")})).collect::<Vec<_>>();
        return Ok(Json(json!({"documents":docs,"tickets":tickets})));
    }
    let memory = state.ticketing.memory.read().await;
    let docs:Vec<_>=memory.documents.values().filter(|d|d.user_id==u.id&&d.order_id==id).map(|d|json!({"id":d.id,"documentType":d.document_type,"contentType":content_type_for_document_type(d.document_type)})).collect();
    let tickets: Vec<_> = memory
        .tickets
        .values()
        .filter(|t| t.user_id == u.id && t.order_id == id)
        .map(public_ticket)
        .collect();
    Ok(Json(json!({"documents":docs,"tickets":tickets})))
}

async fn document(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let u = current_user(&state, &headers).await?;
    let d = owned_document(&state.ticketing, u.id, id).await?;
    let data = state
        .ticketing
        .client()?
        .document(&d.upstream_id)
        .await
        .map_err(cd_error)?;
    let filename = format!(
        "cesta-ticket-{}.{}",
        d.id,
        if data.content_type == "application/pdf" {
            "pdf"
        } else {
            "png"
        }
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, data.content_type)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .header(header::CACHE_CONTROL, "private, no-store")
        .body(Body::from(data.bytes))
        .map_err(|_| api_error("internal_error", "Could not stream the document"))
}

#[derive(Deserialize)]
struct RefundQuoteQuery {
    email: String,
}
async fn refund_quote(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(q): Query<RefundQuoteQuery>,
) -> Result<Json<Value>, ApiError> {
    let u = current_user(&state, &headers).await?;
    validate_email(&q.email)?;
    let t = owned_ticket(&state.ticketing, u.id, id).await?;
    let (_, raw) = state
        .ticketing
        .client()?
        .refund_quote(&t.upstream_id, &q.email)
        .await
        .map_err(cd_error)?;
    Ok(Json(sanitize_public(raw)))
}
async fn refund(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(body): Json<RefundRequest>,
) -> Result<Response, ApiError> {
    let u = current_user(&state, &headers).await?;
    validate_email(&body.email)?;
    idempotent(
        &state.ticketing,
        u.id,
        "refund",
        &headers,
        &body.email,
        || async {
            let t = owned_ticket(&state.ticketing, u.id, id).await?;
            if t.returned {
                return Err(api_error(
                    "invalid_ticket_state",
                    "Ticket has already been returned",
                ));
            }
            let quote = state
                .ticketing
                .client()?
                .refund_quote(&t.upstream_id, &body.email)
                .await
                .map_err(cd_error)?
                .1;
            if quote
                .get("errors")
                .and_then(Value::as_array)
                .is_some_and(|v| !v.is_empty())
            {
                return Err(api_error(
                    "refund_not_available",
                    "ČD reports that this ticket cannot be refunded",
                ));
            }
            let result = state
                .ticketing
                .client()?
                .refund_ticket(&t.upstream_id, &body.email)
                .await
                .map_err(cd_error)?
                .map(|(_, v)| v)
                .unwrap_or_else(|| json!({}));
            let r = RefundRecord {
                id: Uuid::new_v4(),
                user_id: u.id,
                ticket_id: t.id,
                status: "requested".into(),
                amount_hellers: quote.get("price2Refund").and_then(Value::as_i64),
                payload: result,
            };
            state
                .ticketing
                .memory
                .write()
                .await
                .refunds
                .insert(r.id, r.clone());
            persist_refund(&state.ticketing, &r).await?;
            audit(
                &state.ticketing,
                Some(u.id),
                Some(t.order_id),
                "refund_submitted",
                "success",
                json!({"amountHellers":r.amount_hellers}),
            )
            .await?;
            Ok((StatusCode::ACCEPTED, public_refund(&r)))
        },
    )
    .await
}
async fn refund_latest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let u = current_user(&state, &headers).await?;
    let t = owned_ticket(&state.ticketing, u.id, id).await?;
    let (_, status) = state
        .ticketing
        .client()?
        .refund_status(&t.upstream_id)
        .await
        .map_err(cd_error)?;
    let latest = if let Some(db) = &state.ticketing.db {
        sqlx::query("SELECT id,user_id,ticket_id,status,amount_hellers,payload FROM cd_ticketing_refunds WHERE user_id=$1 AND ticket_id=$2 ORDER BY updated_at DESC LIMIT 1").bind(u.id).bind(id).fetch_optional(db).await.map_err(db_error)?.map(|row|public_refund(&RefundRecord{id:row.get("id"),user_id:row.get("user_id"),ticket_id:row.get("ticket_id"),status:row.get("status"),amount_hellers:row.get::<Option<i32>,_>("amount_hellers").map(i64::from),payload:row.get("payload")}))
    } else {
        state
            .ticketing
            .memory
            .read()
            .await
            .refunds
            .values()
            .filter(|r| r.user_id == u.id && r.ticket_id == id)
            .max_by_key(|r| r.id)
            .map(public_refund)
    };
    Ok(Json(
        json!({"refund":latest,"upstreamStatus":sanitize_public(status)}),
    ))
}

async fn resolve_cd_station(client: &Arc<dyn CdApi>, name: &str) -> Result<cd::Location, ApiError> {
    let (locations, _) = client
        .search_locations(name, Some(3), 10)
        .await
        .map_err(cd_error)?;
    let expected = transit_model::normalize_czech_name(name);
    let mut exact = locations
        .data
        .iter()
        .filter(|location| {
            location.key.is_some()
                && location
                    .name
                    .as_deref()
                    .is_some_and(|value| transit_model::normalize_czech_name(value) == expected)
        })
        .cloned()
        .collect::<Vec<_>>();
    if exact.len() == 1 {
        return Ok(exact.remove(0));
    }
    let mut valid = locations
        .data
        .into_iter()
        .filter(|location| location.key.is_some())
        .collect::<Vec<_>>();
    if exact.is_empty() && valid.len() == 1 {
        return Ok(valid.remove(0));
    }
    Err(api_error(
        "station_correlation_ambiguous",
        "Cesta station name does not resolve to one unique ČD station",
    ))
}

async fn store_resolved_location(
    service: &TicketingService,
    user_id: Uuid,
    location: &cd::Location,
) -> Result<Uuid, ApiError> {
    let upstream_type = location.r#type.unwrap_or(3);
    let upstream_key = location
        .key
        .ok_or_else(|| api_error("upstream_malformed_response", "ČD station has no key"))?;
    let id = Uuid::new_v4();
    let payload = serde_json::to_value(location)
        .map_err(|_| api_error("internal_error", "Could not store resolved station"))?;
    service.memory.write().await.locations.insert(
        id,
        LocationRecord {
            user_id,
            upstream_type,
            upstream_key,
            payload: payload.clone(),
        },
    );
    persist_location(service, user_id, id, upstream_type, upstream_key, &payload).await?;
    Ok(id)
}

fn journey_naive(date: NaiveDate, seconds: u32) -> Result<NaiveDateTime, ApiError> {
    let day = date
        .checked_add_days(chrono::Days::new((seconds / 86400) as u64))
        .ok_or_else(|| validation("journey date is out of range"))?;
    let time = NaiveTime::from_num_seconds_from_midnight_opt(seconds % 86400, 0)
        .ok_or_else(|| validation("journey time is invalid"))?;
    Ok(day.and_time(time))
}

fn cd_date_time(date: NaiveDate, seconds: u32) -> Result<String, ApiError> {
    Ok(journey_naive(date, seconds)?
        .format("%Y-%m-%d %H:%M")
        .to_string())
}

fn parse_cd_date_time(value: &str) -> Option<NaiveDateTime> {
    [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%-d.%-m.%Y %H:%M",
    ]
    .into_iter()
    .find_map(|format| NaiveDateTime::parse_from_str(value, format).ok())
}

fn connection_bounds(connection: &Value) -> Option<(NaiveDateTime, NaiveDateTime)> {
    let trains = connection.get("trains")?.as_array()?;
    let first = trains.first()?;
    let last = trains.last()?;
    let first_route = first.pointer("/trainData/route")?.as_array()?;
    let last_route = last.pointer("/trainData/route")?.as_array()?;
    let departure = first_route.iter().find_map(|item| {
        item.get("dep")
            .and_then(Value::as_str)
            .and_then(parse_cd_date_time)
    })?;
    let arrival = last_route.iter().rev().find_map(|item| {
        item.get("arr")
            .and_then(Value::as_str)
            .and_then(parse_cd_date_time)
    })?;
    Some((departure, arrival))
}

fn train_number(value: &str) -> Option<String> {
    let digits = value
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>();
    (!digits.is_empty()).then_some(digits)
}

fn correlate_connection(
    raw: &Value,
    legs: &[Value],
    selected: &[usize],
    service_date: NaiveDate,
    departure: u32,
    arrival: u32,
    tolerance_minutes: i64,
) -> Result<i32, ApiError> {
    let connections = raw
        .pointer("/connInfo/connections")
        .or_else(|| raw.get("connections"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            api_error(
                "upstream_malformed_response",
                "ČD response has no connection list",
            )
        })?;
    let target_departure = journey_naive(service_date, departure)?;
    let target_arrival = journey_naive(service_date, arrival)?;
    let expected_numbers = selected
        .iter()
        .filter_map(|index| legs.get(*index))
        .filter_map(|leg| leg.get("routeName").and_then(Value::as_str))
        .filter_map(train_number)
        .collect::<Vec<_>>();
    let mut matches = connections
        .iter()
        .filter_map(|connection| {
            let id = connection
                .get("id")
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())?;
            let (bounds_departure, bounds_arrival) = connection_bounds(connection)?;
            let dep_delta = (bounds_departure - target_departure).num_minutes().abs();
            let arr_delta = (bounds_arrival - target_arrival).num_minutes().abs();
            if dep_delta > tolerance_minutes || arr_delta > tolerance_minutes {
                return None;
            }
            let upstream_numbers = connection
                .get("trains")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|train| {
                    train
                        .pointer("/trainData/train/trainNum")
                        .and_then(Value::as_str)
                })
                .filter_map(train_number)
                .collect::<Vec<_>>();
            let unmatched = expected_numbers
                .iter()
                .filter(|expected| !upstream_numbers.contains(expected))
                .count() as i64;
            Some((dep_delta + arr_delta + unmatched * 5, id, unmatched))
        })
        .collect::<Vec<_>>();
    matches.sort_unstable();
    let Some(best) = matches.first().copied() else {
        return Err(api_error(
            "journey_not_correlatable",
            "No ČD connection matches the selected Cesta journey",
        ));
    };
    if matches.get(1).is_some_and(|next| next.0 == best.0) {
        return Err(api_error(
            "journey_correlation_ambiguous",
            "Multiple ČD connections match the selected Cesta journey",
        ));
    }
    Ok(best.1)
}

fn validate_search(b: &SearchRequest) -> Result<(), ApiError> {
    if b.from_location_id == b.to_location_id {
        return Err(validation("fromLocationId and toLocationId must differ"));
    }
    if !(1..=10).contains(&b.max_count) || !(1..=2).contains(&b.class) {
        return Err(validation("invalid maxCount or class"));
    }
    if b.max_changes.is_some_and(|v| !(0..=8).contains(&v)) {
        return Err(validation("maxChanges must be between 0 and 8"));
    }
    booking_request(&b.passenger_groups, b.class, 0).map(|_| ())
}
fn booking_request(
    groups: &[PassengerRequest],
    class: i32,
    flags: i32,
) -> Result<BookingRequest, ApiError> {
    if groups.len() > 8 {
        return Err(validation("at most 8 passenger groups are allowed"));
    }
    let mut total = 0;
    let mut passengers = Vec::new();
    for g in groups {
        if g.passenger_type_id <= 0
            || !(1..=10).contains(&g.count)
            || g.age.is_some_and(|a| !(1..=200).contains(&a))
        {
            return Err(validation("invalid passenger group"));
        }
        total += g.count;
        passengers.push(Passenger {
            count: Some(g.count),
            id: Some(g.passenger_type_id),
            age: g.age,
            extra: [("cards".into(), json!(g.reduction_ids))]
                .into_iter()
                .collect(),
            ..Default::default()
        });
    }
    if total > 10 {
        return Err(validation("at most 10 passengers are allowed"));
    }
    Ok(BookingRequest {
        flags: Some(flags),
        class: Some(class),
        passengers,
        extra: Default::default(),
    })
}
fn validate_customer(c: &CustomerRequest) -> Result<(), ApiError> {
    validate_email(&c.email)?;
    if c.name
        .as_ref()
        .is_some_and(|v| v.trim().is_empty() || v.len() > 120)
        || c.company_name.as_ref().is_some_and(|v| v.len() > 160)
    {
        return Err(validation("invalid customer name"));
    }
    Ok(())
}
fn validate_reservations(request: &ReservationRequest) -> Result<(), ApiError> {
    const ALLOWED: i32 = 1 | 2 | 4 | 8 | 16 | 256 | 512 | 32768 | 65536;
    if request.trains.len() > 20 {
        return Err(validation("at most 20 train reservations are allowed"));
    }
    for item in &request.trains {
        if item.train_id < 0
            || item.res_type <= 0
            || item.res_type & !ALLOWED != 0
            || item.count.is_some_and(|v| !(0..=10).contains(&v))
            || item.coach.is_some_and(|v| v <= 0)
            || item.places.len() > 10
            || item.places.iter().any(|v| *v <= 0)
        {
            return Err(validation("invalid reservation selection"));
        }
        for positions in [
            &item.couchette_berth_male_positions,
            &item.berth_female_positions,
        ]
        .into_iter()
        .flatten()
        {
            if [positions.top, positions.middle, positions.bottom]
                .into_iter()
                .flatten()
                .any(|v| !(0..=2).contains(&v))
            {
                return Err(validation("invalid couchette or berth positions"));
            }
        }
    }
    Ok(())
}
fn validate_email(v: &str) -> Result<(), ApiError> {
    if v.len() > 254 || !v.contains('@') || v.chars().any(char::is_control) {
        Err(validation("invalid email"))
    } else {
        Ok(())
    }
}
fn require_draft(o: &OrderRecord) -> Result<(), ApiError> {
    if o.status != "draft" {
        Err(api_error("invalid_order_state", "Order is not editable"))
    } else {
        Ok(())
    }
}
fn i32_field(v: &Value, k: &str) -> Result<i32, ApiError> {
    v.get(k)
        .and_then(Value::as_i64)
        .and_then(|n| i32::try_from(n).ok())
        .ok_or_else(|| api_error("upstream_malformed_response", &format!("Missing {k}")))
}
fn offer_amount(v: &Value) -> Option<i64> {
    v.get("offers")
        .and_then(Value::as_array)
        .and_then(|a| {
            a.iter()
                .find(|x| {
                    x.get("basicPriceFlags")
                        .and_then(Value::as_i64)
                        .is_some_and(|f| f & 2 != 0)
                })
                .or_else(|| a.first())
        })
        .and_then(|x| x.get("price"))
        .and_then(Value::as_i64)
        .or_else(|| v.get("price").and_then(Value::as_i64))
}
fn remove_keys(v: &mut Value, keys: &[&str]) {
    match v {
        Value::Object(m) => {
            for k in keys {
                m.remove(*k);
            }
            for item in m.values_mut() {
                remove_keys(item, keys)
            }
        }
        Value::Array(a) => {
            for item in a {
                remove_keys(item, keys)
            }
        }
        _ => {}
    }
}
fn sanitize_public(mut value: Value) -> Value {
    remove_keys(
        &mut value,
        &[
            "handle",
            "connId",
            "bookingId",
            "documentId",
            "ticketId",
            "sjtTicketId",
            "orderNumber",
        ],
    );
    value
}
fn public_connections(
    typed: Option<&Vec<cd::ConnectionInfo>>,
    raw: &Value,
) -> (HashMap<Uuid, i32>, Vec<Value>) {
    let raw_items = raw
        .get("connections")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut map = HashMap::new();
    let mut out = Vec::new();
    for (index, item) in raw_items.into_iter().enumerate() {
        let upstream = typed
            .and_then(|v| v.get(index))
            .and_then(|v| v.id)
            .or_else(|| {
                item.get("id")
                    .and_then(Value::as_i64)
                    .and_then(|v| i32::try_from(v).ok())
            });
        if let Some(upstream) = upstream {
            let id = Uuid::new_v4();
            map.insert(id, upstream);
            let mut p = item;
            remove_keys(
                &mut p,
                &[
                    "id",
                    "handle",
                    "connId",
                    "bookingId",
                    "documentId",
                    "ticketId",
                ],
            );
            if let Value::Object(m) = &mut p {
                m.insert("id".into(), json!(id));
            }
            out.push(p)
        }
    }
    (map, out)
}
fn public_order(o: &OrderRecord) -> Value {
    let mut q = o.quote.clone();
    remove_keys(
        &mut q,
        &[
            "handle",
            "connId",
            "bookingId",
            "documentId",
            "ticketId",
            "orderNumber",
        ],
    );
    json!({"id":o.id,"searchId":o.search_id,"connectionId":o.connection_id,"status":o.status,"selectedOfferType":o.selected_offer_type,"amountHellers":o.amount_hellers,"currency":"CZK","quote":q,"version":o.version})
}
fn public_ticket(t: &TicketRecord) -> Value {
    let mut p = t.payload.clone();
    remove_keys(&mut p, &["ticketId", "documentId", "orderNumber"]);
    json!({"id":t.id,"orderId":t.order_id,"returned":t.returned,"details":p})
}
fn public_refund(r: &RefundRecord) -> Value {
    json!({"id":r.id,"ticketId":r.ticket_id,"status":r.status,"amountHellers":r.amount_hellers,"details":sanitize_public(r.payload.clone())})
}
fn content_type_for_document_type(t: Option<i32>) -> Option<&'static str> {
    match t {
        Some(1 | 3) => Some("application/pdf"),
        Some(2) => Some("image/png"),
        _ => None,
    }
}
fn find_schema_info(v: &Value, train: i32) -> Option<Value> {
    v.get("trains")?
        .as_array()?
        .iter()
        .find(|x| x.get("trainId").and_then(Value::as_i64) == Some(train.into()))?
        .get("reservationSchemaInfo")
        .or_else(|| v.get("schemaInfo"))
        .cloned()
}

async fn idempotent<T: Serialize, F, Fut>(
    service: &TicketingService,
    user: Uuid,
    op: &str,
    headers: &HeaderMap,
    body: &T,
    run: F,
) -> Result<Response, ApiError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(StatusCode, Value), ApiError>>,
{
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            api_error(
                "idempotency_key_required",
                "Idempotency-Key header is required",
            )
        })?;
    if key.len() < 8
        || key.len() > 128
        || !key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.:".contains(c))
    {
        return Err(validation("invalid Idempotency-Key"));
    }
    let hash = hex::encode(Sha256::digest(
        serde_json::to_vec(body).map_err(|_| validation("invalid request"))?,
    ));
    let cache_key = (user, op.to_string(), key.to_string());
    let request_lock = {
        let mut locks = service.idempotency_locks.write().await;
        locks
            .entry(cache_key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let _request_guard = request_lock.lock().await;
    if let Some(old) = service
        .memory
        .read()
        .await
        .idempotency
        .get(&cache_key)
        .cloned()
    {
        if old.request_hash != hash {
            return Err(api_error(
                "idempotency_conflict",
                "Idempotency-Key was used with a different request",
            ));
        }
        return Ok((old.status, Json(old.body)).into_response());
    }
    if let Some(db) = &service.db {
        let acquired=sqlx::query("INSERT INTO cd_ticketing_idempotency(user_id,operation,idempotency_key,request_hash,locked_until) VALUES($1,$2,$3,$4,now()+interval '2 minutes') ON CONFLICT(user_id,operation,idempotency_key) DO UPDATE SET request_hash=EXCLUDED.request_hash,locked_until=EXCLUDED.locked_until,status_code=NULL,response=NULL WHERE cd_ticketing_idempotency.locked_until<now() AND cd_ticketing_idempotency.status_code IS NULL RETURNING 1").bind(user).bind(op).bind(key).bind(&hash).fetch_optional(db).await.map_err(db_error)?.is_some();
        if !acquired {
            let row=sqlx::query("SELECT request_hash,status_code,response FROM cd_ticketing_idempotency WHERE user_id=$1 AND operation=$2 AND idempotency_key=$3").bind(user).bind(op).bind(key).fetch_one(db).await.map_err(db_error)?;
            if row.get::<String, _>("request_hash") != hash {
                return Err(api_error(
                    "idempotency_conflict",
                    "Idempotency-Key was used with a different request",
                ));
            }
            if let (Some(status), Some(response)) = (
                row.try_get::<Option<i32>, _>("status_code").ok().flatten(),
                row.try_get::<Option<Value>, _>("response").ok().flatten(),
            ) {
                return Ok((
                    StatusCode::from_u16(status as u16).unwrap_or(StatusCode::OK),
                    Json(response),
                )
                    .into_response());
            }
            return Err(api_error(
                "operation_in_progress",
                "An identical operation is already in progress",
            ));
        }
    }
    let result = run().await;
    if result.is_err()
        && let Some(db) = &service.db
    {
        sqlx::query("DELETE FROM cd_ticketing_idempotency WHERE user_id=$1 AND operation=$2 AND idempotency_key=$3 AND status_code IS NULL").bind(user).bind(op).bind(key).execute(db).await.map_err(db_error)?;
    }
    let (status, response) = result?;
    service.memory.write().await.idempotency.insert(
        cache_key,
        IdempotentResult {
            request_hash: hash.clone(),
            status,
            body: response.clone(),
        },
    );
    if let Some(db) = &service.db {
        sqlx::query("UPDATE cd_ticketing_idempotency SET status_code=$4,response=$5,locked_until=now() WHERE user_id=$1 AND operation=$2 AND idempotency_key=$3").bind(user).bind(op).bind(key).bind(status.as_u16() as i32).bind(&response).execute(db).await.map_err(db_error)?;
    }
    Ok((status, Json(response)).into_response())
}

async fn optional_location(
    s: &TicketingService,
    u: Uuid,
    id: Option<Uuid>,
) -> Result<Option<LocationRecord>, ApiError> {
    match id {
        Some(v) => owned_location(s, u, v).await.map(Some),
        None => Ok(None),
    }
}
#[allow(clippy::collapsible_if)]
async fn owned_location(
    s: &TicketingService,
    u: Uuid,
    id: Uuid,
) -> Result<LocationRecord, ApiError> {
    if let Some(v) = s.memory.read().await.locations.get(&id).cloned() {
        return if v.user_id == u {
            Ok(v)
        } else {
            Err(not_found_owned())
        };
    }
    if let Some(db) = &s.db {
        if let Some(r)=sqlx::query("SELECT user_id,upstream_type,upstream_key,payload FROM cd_ticketing_locations WHERE id=$1 AND expires_at>now()").bind(id).fetch_optional(db).await.map_err(db_error)?{if r.get::<Uuid,_>("user_id")==u{return Ok(LocationRecord{user_id:u,upstream_type:r.get("upstream_type"),upstream_key:r.get("upstream_key"),payload:r.get("payload")})}}
    }
    Err(not_found_owned())
}
#[allow(clippy::collapsible_if)]
async fn owned_search(s: &TicketingService, u: Uuid, id: Uuid) -> Result<SearchRecord, ApiError> {
    if let Some(v) = s.memory.read().await.searches.get(&id).cloned() {
        return if v.user_id == u && v.expires_at > Utc::now() {
            Ok(v)
        } else {
            Err(not_found_owned())
        };
    }
    if let Some(db) = &s.db {
        if let Some(r)=sqlx::query("SELECT user_id,upstream_handle,request,payload,connection_map,expires_at FROM cd_ticketing_searches WHERE id=$1").bind(id).fetch_optional(db).await.map_err(db_error)?{if r.get::<Uuid,_>("user_id")==u&&r.get::<DateTime<Utc>,_>("expires_at")>Utc::now(){let cm:Value=r.get("connection_map");let connections=cm.as_object().into_iter().flat_map(|m|m.iter()).filter_map(|(k,v)|Some((Uuid::parse_str(k).ok()?,i32::try_from(v.as_i64()?).ok()?))).collect();return Ok(SearchRecord{id,user_id:u,handle:r.get("upstream_handle"),request:serde_json::from_value(r.get("request")).map_err(|_|api_error("internal_error","Stored search is invalid"))?,raw:r.get("payload"),connections,expires_at:r.get("expires_at")})}}
    }
    Err(not_found_owned())
}
#[allow(clippy::collapsible_if)]
async fn owned_order(s: &TicketingService, u: Uuid, id: Uuid) -> Result<OrderRecord, ApiError> {
    if let Some(v) = s.memory.read().await.orders.get(&id).cloned() {
        return if v.user_id == u {
            Ok(v)
        } else {
            Err(not_found_owned())
        };
    }
    if let Some(db) = &s.db {
        if let Some(r) = sqlx::query("SELECT * FROM cd_ticketing_orders WHERE id=$1")
            .bind(id)
            .fetch_optional(db)
            .await
            .map_err(db_error)?
        {
            if r.get::<Uuid, _>("user_id") == u {
                return order_from_row(r);
            }
        }
    }
    Err(not_found_owned())
}
#[allow(clippy::collapsible_if)]
async fn owned_document(
    s: &TicketingService,
    u: Uuid,
    id: Uuid,
) -> Result<DocumentRecord, ApiError> {
    if let Some(v) = s.memory.read().await.documents.get(&id).cloned() {
        return if v.user_id == u {
            Ok(v)
        } else {
            Err(not_found_owned())
        };
    }
    if let Some(db) = &s.db {
        if let Some(r)=sqlx::query("SELECT user_id,order_id,upstream_document_id,document_type FROM cd_ticketing_documents WHERE id=$1").bind(id).fetch_optional(db).await.map_err(db_error)?{if r.get::<Uuid,_>("user_id")==u{return Ok(DocumentRecord{id,user_id:u,order_id:r.get("order_id"),upstream_id:r.get("upstream_document_id"),document_type:r.get("document_type")})}}
    }
    Err(not_found_owned())
}
#[allow(clippy::collapsible_if)]
async fn owned_ticket(s: &TicketingService, u: Uuid, id: Uuid) -> Result<TicketRecord, ApiError> {
    if let Some(v) = s.memory.read().await.tickets.get(&id).cloned() {
        return if v.user_id == u {
            Ok(v)
        } else {
            Err(not_found_owned())
        };
    }
    if let Some(db) = &s.db {
        if let Some(r)=sqlx::query("SELECT user_id,order_id,upstream_ticket_id,payload,returned FROM cd_ticketing_tickets WHERE id=$1").bind(id).fetch_optional(db).await.map_err(db_error)?{if r.get::<Uuid,_>("user_id")==u{return Ok(TicketRecord{id,user_id:u,order_id:r.get("order_id"),upstream_id:r.get("upstream_ticket_id"),payload:r.get("payload"),returned:r.get("returned")})}}
    }
    Err(not_found_owned())
}
fn order_from_row(r: sqlx::postgres::PgRow) -> Result<OrderRecord, ApiError> {
    Ok(OrderRecord {
        id: r.get("id"),
        user_id: r.get("user_id"),
        search_id: r.get("search_id"),
        connection_id: r.get("connection_id"),
        conn_id: r.get("upstream_conn_id"),
        booking_id: r.get("upstream_booking_id"),
        status: r.get("status"),
        selected_offer_type: r.get("selected_offer_type"),
        amount_hellers: r.get::<Option<i32>, _>("amount_hellers").map(i64::from),
        customer: r
            .get::<Option<Value>, _>("customer")
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| api_error("internal_error", "Stored customer is invalid"))?,
        quote: r.get("quote"),
        checkout_session_id: r.get("checkout_session_id"),
        version: r.get("version"),
    })
}

async fn persist_location(
    s: &TicketingService,
    u: Uuid,
    id: Uuid,
    t: i32,
    k: i32,
    p: &Value,
) -> Result<(), ApiError> {
    if let Some(db) = &s.db {
        sqlx::query("INSERT INTO cd_ticketing_locations(id,user_id,upstream_type,upstream_key,payload,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '24 hours') ON CONFLICT(user_id,upstream_type,upstream_key) DO UPDATE SET id=EXCLUDED.id,payload=EXCLUDED.payload,expires_at=EXCLUDED.expires_at").bind(id).bind(u).bind(t).bind(k).bind(p).execute(db).await.map_err(db_error)?;
    }
    Ok(())
}
async fn persist_search(s: &TicketingService, r: &SearchRecord) -> Result<(), ApiError> {
    if let Some(db) = &s.db {
        let map: Map<String, Value> = r
            .connections
            .iter()
            .map(|(k, v)| (k.to_string(), json!(v)))
            .collect();
        sqlx::query("INSERT INTO cd_ticketing_searches(id,user_id,upstream_handle,request,payload,connection_map,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO UPDATE SET payload=EXCLUDED.payload,connection_map=EXCLUDED.connection_map,updated_at=now()").bind(r.id).bind(r.user_id).bind(r.handle).bind(json!(r.request)).bind(&r.raw).bind(Value::Object(map)).bind(r.expires_at).execute(db).await.map_err(db_error)?;
    }
    Ok(())
}
async fn persist_order(s: &TicketingService, o: &OrderRecord) -> Result<(), ApiError> {
    if let Some(db) = &s.db {
        sqlx::query("INSERT INTO cd_ticketing_orders(id,user_id,search_id,connection_id,upstream_conn_id,upstream_booking_id,status,selected_offer_type,amount_hellers,customer,quote,checkout_session_id,version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)").bind(o.id).bind(o.user_id).bind(o.search_id).bind(o.connection_id).bind(o.conn_id).bind(&o.booking_id).bind(&o.status).bind(o.selected_offer_type).bind(o.amount_hellers.map(|v|v as i32)).bind(o.customer.as_ref().map(|v|json!(v))).bind(&o.quote).bind(&o.checkout_session_id).bind(o.version).execute(db).await.map_err(db_error)?;
    }
    Ok(())
}
async fn save_order(s: &TicketingService, o: &OrderRecord) -> Result<(), ApiError> {
    s.memory.write().await.orders.insert(o.id, o.clone());
    if let Some(db) = &s.db {
        sqlx::query("UPDATE cd_ticketing_orders SET status=$3,selected_offer_type=$4,amount_hellers=$5,customer=$6,quote=$7,checkout_session_id=$8,version=$9,updated_at=now() WHERE id=$1 AND user_id=$2").bind(o.id).bind(o.user_id).bind(&o.status).bind(o.selected_offer_type).bind(o.amount_hellers.map(|v|v as i32)).bind(o.customer.as_ref().map(|v|json!(v))).bind(&o.quote).bind(&o.checkout_session_id).bind(o.version).execute(db).await.map_err(db_error)?;
    }
    Ok(())
}
async fn persist_issued(
    s: &TicketingService,
    o: &mut OrderRecord,
    raw: &Value,
) -> Result<(), ApiError> {
    for doc in raw
        .get("tickets")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        if let Some(upstream) = doc.get("documentId").and_then(Value::as_str) {
            let d = DocumentRecord {
                id: Uuid::new_v4(),
                user_id: o.user_id,
                order_id: o.id,
                upstream_id: upstream.into(),
                document_type: doc
                    .get("documentType")
                    .and_then(Value::as_i64)
                    .and_then(|v| i32::try_from(v).ok()),
            };
            s.memory.write().await.documents.insert(d.id, d.clone());
            if let Some(db) = &s.db {
                sqlx::query("INSERT INTO cd_ticketing_documents(id,user_id,order_id,upstream_document_id,document_type) VALUES($1,$2,$3,$4,$5) ON CONFLICT(order_id,upstream_document_id) DO NOTHING").bind(d.id).bind(d.user_id).bind(d.order_id).bind(&d.upstream_id).bind(d.document_type).execute(db).await.map_err(db_error)?;
            }
        }
        for ticket in doc
            .get("ticketsIds")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            if let Some(upstream) = ticket.get("ticketId").and_then(Value::as_str) {
                let t = TicketRecord {
                    id: Uuid::new_v4(),
                    user_id: o.user_id,
                    order_id: o.id,
                    upstream_id: upstream.into(),
                    payload: ticket.clone(),
                    returned: ticket
                        .get("isReturned")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                };
                s.memory.write().await.tickets.insert(t.id, t.clone());
                if let Some(db) = &s.db {
                    sqlx::query("INSERT INTO cd_ticketing_tickets(id,user_id,order_id,upstream_ticket_id,payload,returned) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(order_id,upstream_ticket_id) DO UPDATE SET payload=EXCLUDED.payload,returned=EXCLUDED.returned,updated_at=now()").bind(t.id).bind(t.user_id).bind(t.order_id).bind(&t.upstream_id).bind(&t.payload).bind(t.returned).execute(db).await.map_err(db_error)?;
                }
            }
        }
    }
    o.status = "issued".into();
    o.quote = raw.clone();
    o.version += 1;
    save_order(s, o).await
}
async fn persist_refund(s: &TicketingService, r: &RefundRecord) -> Result<(), ApiError> {
    if let Some(db) = &s.db {
        sqlx::query("INSERT INTO cd_ticketing_refunds(id,user_id,ticket_id,status,amount_hellers,payload) VALUES($1,$2,$3,$4,$5,$6)").bind(r.id).bind(r.user_id).bind(r.ticket_id).bind(&r.status).bind(r.amount_hellers.map(|v|v as i32)).bind(&r.payload).execute(db).await.map_err(db_error)?;
    }
    Ok(())
}

async fn audit(
    service: &TicketingService,
    user_id: Option<Uuid>,
    order_id: Option<Uuid>,
    event_type: &str,
    outcome: &str,
    mut context: Value,
) -> Result<(), ApiError> {
    cd::redact_json(&mut context);
    if let Some(db) = &service.db {
        sqlx::query("INSERT INTO cd_ticketing_audit_events(user_id,order_id,event_type,outcome,sanitized_context) VALUES($1,$2,$3,$4,$5)")
            .bind(user_id).bind(order_id).bind(event_type).bind(outcome).bind(context)
            .execute(db).await.map_err(db_error)?;
    }
    Ok(())
}

fn api_error(code: &str, message: &str) -> ApiError {
    ApiError {
        code: code.into(),
        message: message.into(),
    }
}
fn validation(m: &str) -> ApiError {
    api_error("validation_error", m)
}
fn not_found_owned() -> ApiError {
    api_error("not_found", "Ticketing resource was not found")
}
fn db_error(_: sqlx::Error) -> ApiError {
    api_error("internal_error", "Ticketing storage operation failed")
}
fn cd_error(e: CdError) -> ApiError {
    api_error(
        e.stable_code(),
        match e {
            CdError::Timeout => "ČD did not respond in time",
            CdError::Unavailable => "ČD is temporarily unavailable",
            CdError::NotConfigured => "ČD ticketing is not configured",
            CdError::Rejected { .. } => "ČD rejected the operation",
            CdError::MalformedResponse => "ČD returned an invalid response",
            CdError::InvalidSigningKey => "ČD signing is not configured correctly",
        },
    )
}
fn payment_error(e: PaymentError) -> ApiError {
    match e {
        PaymentError::NotConfigured => api_error(
            "payment_provider_unavailable",
            "Payment provider is not configured",
        ),
        PaymentError::Unavailable => api_error(
            "payment_provider_unavailable",
            "Payment provider is unavailable",
        ),
        PaymentError::NotSettled => api_error(
            "payment_not_settled",
            "Payment has not been verified as settled",
        ),
        PaymentError::Mismatch => api_error(
            "payment_verification_failed",
            "Payment details do not match the order",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;
    #[test]
    fn money_stays_integer_hellers() {
        assert_eq!(
            offer_amount(&json!({"offers":[{"basicPriceFlags":2,"price":12345}]})),
            Some(12345)
        );
    }
    #[test]
    fn public_order_removes_upstream_ids() {
        let o = OrderRecord {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            search_id: Uuid::new_v4(),
            connection_id: Uuid::new_v4(),
            conn_id: 7,
            booking_id: "secret".into(),
            status: "draft".into(),
            selected_offer_type: None,
            amount_hellers: Some(100),
            customer: None,
            quote: json!({"bookingId":"secret","offers":[]}),
            checkout_session_id: None,
            version: 0,
        };
        let p = public_order(&o);
        assert!(p["quote"].get("bookingId").is_none());
        assert!(p.to_string().find("secret").is_none());
    }
    #[test]
    fn passenger_validation_rejects_excess() {
        let g = PassengerRequest {
            passenger_type_id: 5,
            count: 11,
            age: None,
            reduction_ids: vec![],
        };
        assert!(booking_request(&[g], 2, 0).is_err());
    }

    #[tokio::test]
    async fn ticketing_api_requires_authentication() {
        let app = crate::build_router(crate::app_state().await.unwrap());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ticketing/locations?q=Praha")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ticketing_api_hides_another_users_order() {
        let state = crate::app_state().await.unwrap();
        let owner = crate::create_user_record(
            "owner@example.cz",
            "correct horse battery staple",
            None,
            vec![],
        )
        .unwrap();
        let other = crate::create_user_record(
            "other@example.cz",
            "correct horse battery staple",
            None,
            vec![],
        )
        .unwrap();
        state.users.write().await.insert(owner.id, owner.clone());
        state.users.write().await.insert(other.id, other.clone());
        let order = OrderRecord {
            id: Uuid::new_v4(),
            user_id: owner.id,
            search_id: Uuid::new_v4(),
            connection_id: Uuid::new_v4(),
            conn_id: 1,
            booking_id: "private-booking".into(),
            status: "draft".into(),
            selected_offer_type: None,
            amount_hellers: Some(100),
            customer: None,
            quote: json!({}),
            checkout_session_id: None,
            version: 0,
        };
        state
            .ticketing
            .memory
            .write()
            .await
            .orders
            .insert(order.id, order.clone());
        let token = crate::auth_response(&state, &other)
            .await
            .unwrap()
            .access_token;
        let app = crate::build_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/ticketing/orders/{}", order.id))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn concurrent_identical_mutations_execute_once() {
        let service = TicketingService::new(None, Arc::new(DisabledPaymentProvider), None);
        let user = Uuid::new_v4();
        let count = Arc::new(AtomicUsize::new(0));
        let call = |service: TicketingService, count: Arc<AtomicUsize>| async move {
            let mut headers = HeaderMap::new();
            headers.insert("idempotency-key", "same-key-123".parse().unwrap());
            idempotent(
                &service,
                user,
                "test",
                &headers,
                &json!({"a":1}),
                || async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Ok((StatusCode::OK, json!({"ok":true})))
                },
            )
            .await
            .unwrap()
            .status()
        };
        let (left, right) = tokio::join!(
            call(service.clone(), count.clone()),
            call(service, count.clone())
        );
        assert_eq!(left, StatusCode::OK);
        assert_eq!(right, StatusCode::OK);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn payment_verification_rejects_amount_mismatch() {
        async fn provider() -> Json<Value> {
            Json(
                json!({"id":"session","status":"settled","amountHellers":100,"currency":"CZK","redirectUrl":"https://pay.invalid/session"}),
            )
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(axum::routing::any(provider)),
            )
            .await
            .unwrap();
        });
        let provider = HttpPaymentProvider::new(
            format!("http://{address}"),
            "secret".into(),
            "jedes://ticketing/checkout/return".into(),
            "jedes://ticketing/checkout/cancel".into(),
            Duration::from_secs(2),
        )
        .unwrap();
        let order = PaymentOrder {
            order_id: Uuid::new_v4(),
            amount_hellers: 200,
            currency: "CZK",
        };
        assert!(matches!(
            provider.verify_settled("session", &order).await,
            Err(PaymentError::Mismatch)
        ));
    }

    #[tokio::test]
    async fn openapi_documents_ticketing_security_and_request_schemas() {
        let Json(specification) = crate::openapi().await;
        assert_eq!(
            specification["paths"]["/ticketing/orders"]["post"]["security"][0]["bearerAuth"],
            json!([])
        );
        assert_eq!(
            specification["paths"]["/ticketing/orders"]["post"]["requestBody"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/TicketingOrderCreateRequest"
        );
        assert_eq!(
            specification["components"]["securitySchemes"]["bearerAuth"]["scheme"],
            "bearer"
        );
        assert_eq!(
            specification["paths"]["/ticketing/intents"]["post"]["responses"]["201"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/TicketingIntentResult"
        );
        assert_eq!(
            specification["paths"]["/ticketing/orders"]["get"]["responses"]["200"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/TicketingOrderCollection"
        );
        assert_eq!(
            specification["paths"]["/auth/login"]["post"]["responses"]["200"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/AuthResponse"
        );
    }

    #[tokio::test]
    async fn journey_results_receive_opaque_ticketing_metadata() {
        let service = TicketingService::new(None, Arc::new(DisabledPaymentProvider), None);
        let mut journeys = vec![
            json!({"id":"journey-a","departure_time":3600,"arrival_time":7200,"legs":[{"mode":"train","from_stop_id":"a","to_stop_id":"b","route_id":"r","departure_time":3600,"arrival_time":7200}]}),
        ];
        let related = json!({"stops":[{"id":"a","name":"Praha hl. n."},{"id":"b","name":"Brno hl. n."}],"routes":[{"id":"r","short_name":"EC 275"}]});
        service
            .annotate_journeys(
                &mut journeys,
                &related,
                NaiveDate::from_ymd_opt(2026, 7, 6).unwrap(),
            )
            .await
            .unwrap();
        let reference = journeys[0]["ticketing"]["journeyReference"]
            .as_str()
            .unwrap();
        assert!(Uuid::parse_str(reference).is_ok());
        assert_eq!(
            journeys[0]["ticketing"]["availability"],
            "authentication_required"
        );
        assert!(journeys[0]["ticketing"]["indicativePriceHellers"].is_null());
    }

    #[test]
    fn server_side_connection_correlation_uses_schedule_and_train_number() {
        let raw = json!({"connInfo":{"connections":[{"id":7,"trains":[{"trainData":{"train":{"trainNum":"275"},"route":[{"dep":"2026-07-06 08:00"},{"arr":"2026-07-06 10:00"}]}}]},{"id":8,"trains":[{"trainData":{"train":{"trainNum":"999"},"route":[{"dep":"2026-07-06 09:00"},{"arr":"2026-07-06 11:00"}]}}]}]}});
        let legs = vec![json!({"routeName":"EC 275"})];
        assert_eq!(
            correlate_connection(
                &raw,
                &legs,
                &[0],
                NaiveDate::from_ymd_opt(2026, 7, 6).unwrap(),
                8 * 3600,
                10 * 3600,
                10
            )
            .unwrap(),
            7
        );
    }
}
