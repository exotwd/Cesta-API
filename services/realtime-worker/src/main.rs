use std::{collections::HashMap, env, time::Duration};

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use prost::Message;
use reqwest::{Client, header::HeaderValue};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};

const PID_SOURCE: &str = "pid_gtfs_rt";
const PID_FEED_ID: &str = "pid_realtime";
const PID_STATIC_FEED_ID: &str = "pid_gtfs";
const IDS_JMK_SOURCE: &str = "ids_jmk_positions";
const IDS_JMK_FEED_ID: &str = "ids_jmk_realtime";
const DUK_SOURCE: &str = "duk_positions";
const DUK_FEED_ID: &str = "duk_realtime";

#[derive(Clone, PartialEq, Message)]
struct FeedMessage {
    #[prost(message, optional, tag = "1")]
    header: Option<FeedHeader>,
    #[prost(message, repeated, tag = "2")]
    entity: Vec<FeedEntity>,
}

#[derive(Clone, PartialEq, Message)]
struct FeedHeader {
    #[prost(uint64, optional, tag = "3")]
    timestamp: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
struct FeedEntity {
    #[prost(string, required, tag = "1")]
    id: String,
    #[prost(message, optional, tag = "3")]
    trip_update: Option<TripUpdate>,
    #[prost(message, optional, tag = "4")]
    vehicle: Option<VehiclePosition>,
}

#[derive(Clone, PartialEq, Message)]
struct TripUpdate {
    #[prost(message, optional, tag = "1")]
    trip: Option<TripDescriptor>,
    #[prost(message, repeated, tag = "2")]
    stop_time_update: Vec<StopTimeUpdate>,
    #[prost(message, optional, tag = "3")]
    vehicle: Option<VehicleDescriptor>,
}

#[derive(Clone, PartialEq, Message)]
struct TripDescriptor {
    #[prost(string, optional, tag = "1")]
    trip_id: Option<String>,
    #[prost(string, optional, tag = "2")]
    start_time: Option<String>,
    #[prost(string, optional, tag = "3")]
    start_date: Option<String>,
    #[prost(int32, optional, tag = "4")]
    schedule_relationship: Option<i32>,
    #[prost(string, optional, tag = "5")]
    route_id: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct StopTimeUpdate {
    #[prost(uint32, optional, tag = "1")]
    stop_sequence: Option<u32>,
    #[prost(message, optional, tag = "2")]
    arrival: Option<StopTimeEvent>,
    #[prost(message, optional, tag = "3")]
    departure: Option<StopTimeEvent>,
    #[prost(string, optional, tag = "4")]
    stop_id: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct StopTimeEvent {
    #[prost(int32, optional, tag = "1")]
    delay: Option<i32>,
    #[prost(int64, optional, tag = "2")]
    time: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct VehiclePosition {
    #[prost(message, optional, tag = "1")]
    trip: Option<TripDescriptor>,
    #[prost(message, optional, tag = "2")]
    position: Option<Position>,
    #[prost(uint32, optional, tag = "3")]
    current_stop_sequence: Option<u32>,
    #[prost(int32, optional, tag = "4")]
    current_status: Option<i32>,
    #[prost(uint64, optional, tag = "5")]
    timestamp: Option<u64>,
    #[prost(string, optional, tag = "7")]
    stop_id: Option<String>,
    #[prost(message, optional, tag = "8")]
    vehicle: Option<VehicleDescriptor>,
    #[prost(int32, optional, tag = "9")]
    occupancy_status: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
struct Position {
    #[prost(float, required, tag = "1")]
    latitude: f32,
    #[prost(float, required, tag = "2")]
    longitude: f32,
    #[prost(float, optional, tag = "3")]
    bearing: Option<f32>,
    #[prost(float, optional, tag = "5")]
    speed: Option<f32>,
}

#[derive(Clone, PartialEq, Message)]
struct VehicleDescriptor {
    #[prost(string, optional, tag = "1")]
    id: Option<String>,
    #[prost(string, optional, tag = "2")]
    label: Option<String>,
}

#[derive(Debug, Clone)]
struct RealtimeRecord {
    source: &'static str,
    source_feed_id: &'static str,
    source_entity_id: String,
    trip_id: Option<String>,
    route_id: Option<String>,
    stop_id: Option<String>,
    delay_seconds: Option<i32>,
    estimated_arrival: Option<DateTime<Utc>>,
    estimated_departure: Option<DateTime<Utc>>,
    cancellation_status: Option<String>,
    vehicle_id: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    bearing: Option<f64>,
    details: VehicleDetails,
    fetched_at: DateTime<Utc>,
    valid_until: DateTime<Utc>,
    service_date: Option<NaiveDate>,
    raw_payload: Value,
}

#[derive(Debug, Clone, Default)]
struct VehicleDetails {
    route_short_name: Option<String>,
    destination: Option<String>,
    vehicle_type: Option<String>,
    speed_kmh: Option<f64>,
    wheelchair_accessible: Option<bool>,
    air_conditioned: Option<bool>,
    usb_chargers: Option<bool>,
    occupancy_status: Option<String>,
    registration_number: Option<String>,
    operator_name: Option<String>,
    tracking: Option<bool>,
    state: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    if env::args().any(|argument| argument == "--check-feeds") {
        check_external_feeds().await?;
        return Ok(());
    }

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let pool = connect_database_with_retry(&database_url).await?;
    apply_migrations_with_retry(&pool).await;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .user_agent("Cesta-API realtime-worker")
        .build()?;

    if env_bool("USE_MOCK_REALTIME", false) {
        tracing::warn!(mock = true, "mock realtime mode is enabled");
        run_mock_loop(&pool).await;
        return Ok(());
    }

    tracing::info!("starting public transport realtime worker");
    let duk_enabled = env_bool("DUK_ENABLED", false);
    tokio::join!(
        run_pid_loop(pool.clone(), client.clone()),
        run_ids_jmk_loop(pool.clone(), client.clone()),
        async move {
            if duk_enabled {
                run_duk_loop(pool, client).await;
            } else {
                tracing::info!(
                    "DÚK realtime connector is disabled until redistribution terms are confirmed"
                );
            }
        }
    );
    Ok(())
}

async fn check_external_feeds() -> Result<()> {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .user_agent("Cesta-API realtime-worker feed check")
        .build()?;
    let token = non_empty_env("PID_API_TOKEN");
    let trip_updates_url = env::var("PID_TRIP_UPDATES_URL").unwrap_or_else(|_| {
        "https://api.golemio.cz/v2/vehiclepositions/gtfsrt/trip_updates.pb".to_string()
    });
    let vehicle_positions_url = env::var("PID_VEHICLE_POSITIONS_URL").unwrap_or_else(|_| {
        "https://api.golemio.cz/v2/vehiclepositions/gtfsrt/vehicle_positions.pb".to_string()
    });
    let jmk_url = env::var("IDS_JMK_VEHICLES_URL")
        .unwrap_or_else(|_| "https://kordis-jmk.cz/gtfs/gtfsReal.dat".to_string());
    let duk_url = env::var("DUK_VEHICLES_URL")
        .unwrap_or_else(|_| "https://tabule.portabo.cz/api/v1-tabule/cis/GetTraffic/0".to_string());
    let (trip_bytes, vehicle_bytes) = tokio::try_join!(
        fetch_bytes(&client, &trip_updates_url, token.as_deref()),
        fetch_bytes(&client, &vehicle_positions_url, token.as_deref())
    )?;
    let trip_feed = FeedMessage::decode(trip_bytes.as_slice())?;
    let vehicle_feed = FeedMessage::decode(vehicle_bytes.as_slice())?;
    let trip_records = pid_trip_records(&trip_feed)?;
    let vehicle_records = pid_vehicle_records(&vehicle_feed)?;
    let jmk_bytes = fetch_bytes(&client, &jmk_url, None).await?;
    let jmk_feed = FeedMessage::decode(jmk_bytes.as_slice())?;
    let jmk_records = ids_jmk_vehicle_records(&jmk_feed);
    let duk_vehicles = if env_bool("DUK_ENABLED", false) {
        let duk: Value = client
            .get(&duk_url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Some(
            duk.get("VehicleList")
                .and_then(Value::as_array)
                .map_or(0, Vec::len),
        )
    } else {
        None
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "pid": {
                "trip_entities": trip_feed.entity.len(),
                "trip_records": trip_records.len(),
                "vehicle_entities": vehicle_feed.entity.len(),
                "vehicle_records": vehicle_records.len(),
                "source_timestamp": feed_timestamp(&trip_feed)
            },
            "ids_jmk": {
                "entities": jmk_feed.entity.len(),
                "vehicle_records": jmk_records.len(),
                "source_timestamp": feed_timestamp(&jmk_feed)
            },
            "duk": {
                "enabled": env_bool("DUK_ENABLED", false),
                "vehicles": duk_vehicles
            }
        }))?
    );
    Ok(())
}

async fn connect_database_with_retry(database_url: &str) -> Result<PgPool> {
    loop {
        match PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(error) => {
                tracing::warn!(error = %error, "database unavailable; retrying");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn apply_migrations(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(include_str!(
        "../../../infra/postgres/migrations/0006_public_transport_feeds.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../infra/postgres/migrations/0015_vehicle_map_contract.sql"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn apply_migrations_with_retry(pool: &PgPool) {
    loop {
        match apply_migrations(pool).await {
            Ok(()) => return,
            Err(error) => {
                tracing::warn!(error = %error, "database schema is not ready; retrying migrations");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn run_pid_loop(pool: PgPool, client: Client) {
    let trip_updates_url = env::var("PID_TRIP_UPDATES_URL").unwrap_or_else(|_| {
        "https://api.golemio.cz/v2/vehiclepositions/gtfsrt/trip_updates.pb".to_string()
    });
    let vehicle_positions_url = env::var("PID_VEHICLE_POSITIONS_URL").unwrap_or_else(|_| {
        "https://api.golemio.cz/v2/vehiclepositions/gtfsrt/vehicle_positions.pb".to_string()
    });
    let vehicle_positions_json_url = env::var("PID_VEHICLE_POSITIONS_JSON_URL")
        .unwrap_or_else(|_| "https://api.golemio.cz/v2/vehiclepositions".to_string());
    let token = non_empty_env("PID_API_TOKEN");
    let interval = env_u64("PID_POLL_INTERVAL_SECONDS", 20).max(10);
    loop {
        let attempted_at = Utc::now();
        let result = sync_pid_realtime(
            &pool,
            &client,
            &trip_updates_url,
            &vehicle_positions_url,
            &vehicle_positions_json_url,
            token.as_deref(),
        )
        .await;
        finish_sync(
            &pool,
            PID_SOURCE,
            &trip_updates_url,
            "gtfs_realtime",
            attempted_at,
            result,
        )
        .await;
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

async fn sync_pid_realtime(
    pool: &PgPool,
    client: &Client,
    trip_updates_url: &str,
    vehicle_positions_url: &str,
    vehicle_positions_json_url: &str,
    token: Option<&str>,
) -> Result<(usize, usize, Option<DateTime<Utc>>, Value)> {
    let trip_bytes = fetch_bytes(client, trip_updates_url, token).await?;
    let trip_feed = FeedMessage::decode(trip_bytes.as_ref())?;
    let mut records = pid_trip_records(&trip_feed)?;
    let (vehicle_records, vehicle_entities, position_format) = if let Some(token) = token {
        match fetch_pid_vehicle_json(client, vehicle_positions_json_url, token).await {
            Ok(payload) => {
                let records = pid_json_vehicle_records(&payload)?;
                let count = payload
                    .get("features")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len);
                (records, count, "golemio_geojson")
            }
            Err(error) => {
                tracing::warn!(%error, "Golemio GeoJSON positions failed; using GTFS-RT fallback");
                let vehicle_bytes = fetch_bytes(client, vehicle_positions_url, Some(token)).await?;
                let vehicle_feed = FeedMessage::decode(vehicle_bytes.as_ref())?;
                let count = vehicle_feed.entity.len();
                (
                    pid_vehicle_records(&vehicle_feed)?,
                    count,
                    "gtfs_realtime_fallback",
                )
            }
        }
    } else {
        let vehicle_bytes = fetch_bytes(client, vehicle_positions_url, None).await?;
        let vehicle_feed = FeedMessage::decode(vehicle_bytes.as_ref())?;
        let count = vehicle_feed.entity.len();
        (pid_vehicle_records(&vehicle_feed)?, count, "gtfs_realtime")
    };
    records.extend(vehicle_records);
    let source_timestamp = records
        .iter()
        .map(|record| record.fetched_at)
        .max()
        .or_else(|| feed_timestamp(&trip_feed));
    let received = records.len();
    let written = persist_records(pool, &records).await?;
    cleanup_expired(pool).await?;
    Ok((
        received,
        written,
        source_timestamp,
        json!({
            "trip_update_entities": trip_feed.entity.len(),
            "vehicle_position_entities": vehicle_entities,
            "position_format": position_format
        }),
    ))
}

async fn fetch_pid_vehicle_json(client: &Client, url: &str, token: &str) -> Result<Value> {
    Ok(client
        .get(url)
        .header(
            "X-Access-Token",
            HeaderValue::from_str(token).context("invalid PID_API_TOKEN")?,
        )
        .query(&[
            ("limit", "10000"),
            ("includeNotTracking", "false"),
            ("includeNotPublic", "false"),
            ("preferredTimezone", "Europe_Prague"),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn fetch_bytes(client: &Client, url: &str, token: Option<&str>) -> Result<Vec<u8>> {
    let mut request = client.get(url);
    if let Some(token) = token {
        request = request.header(
            "X-Access-Token",
            HeaderValue::from_str(token).context("invalid PID_API_TOKEN")?,
        );
    }
    Ok(request
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec())
}

fn pid_trip_records(feed: &FeedMessage) -> Result<Vec<RealtimeRecord>> {
    let fetched_at = feed_timestamp(feed).unwrap_or_else(Utc::now);
    let valid_until = fetched_at + chrono::Duration::seconds(90);
    let mut records = Vec::new();
    for entity in &feed.entity {
        let Some(update) = &entity.trip_update else {
            continue;
        };
        let Some(trip) = update.trip.as_ref() else {
            continue;
        };
        let trip_id = trip
            .trip_id
            .as_deref()
            .map(|id| scoped_pid_id(PID_STATIC_FEED_ID, id));
        let route_id = trip
            .route_id
            .as_deref()
            .map(|id| scoped_pid_id(PID_STATIC_FEED_ID, id));
        let service_date = trip
            .start_date
            .as_deref()
            .and_then(|date| NaiveDate::parse_from_str(date, "%Y%m%d").ok());
        let cancellation_status = match trip.schedule_relationship {
            Some(3) => Some("cancelled".to_string()),
            Some(7) => Some("deleted".to_string()),
            _ => None,
        };
        let vehicle_id = update
            .vehicle
            .as_ref()
            .and_then(|vehicle| vehicle.id.clone());
        if update.stop_time_update.is_empty() {
            records.push(RealtimeRecord {
                source: PID_SOURCE,
                source_feed_id: PID_FEED_ID,
                source_entity_id: format!("trip:{}", entity.id),
                trip_id,
                route_id,
                stop_id: None,
                delay_seconds: None,
                estimated_arrival: None,
                estimated_departure: None,
                cancellation_status,
                vehicle_id,
                lat: None,
                lon: None,
                bearing: None,
                details: VehicleDetails::default(),
                fetched_at,
                valid_until,
                service_date,
                raw_payload: json!({
                    "entity_id": entity.id.as_str(),
                    "trip_id": trip.trip_id.as_deref(),
                    "route_id": trip.route_id.as_deref(),
                    "start_date": trip.start_date.as_deref(),
                    "schedule_relationship": trip.schedule_relationship
                }),
            });
            continue;
        }
        for stop_update in &update.stop_time_update {
            let stop_key = format!(
                "sequence-{}:{}",
                stop_update.stop_sequence.unwrap_or(0),
                stop_update.stop_id.as_deref().unwrap_or("unknown")
            );
            let stop_id = stop_update
                .stop_id
                .as_deref()
                .map(|id| scoped_pid_id(PID_STATIC_FEED_ID, id));
            let arrival = stop_update.arrival.as_ref();
            let departure = stop_update.departure.as_ref();
            let delay_seconds = departure
                .and_then(|event| event.delay)
                .or_else(|| arrival.and_then(|event| event.delay));
            records.push(RealtimeRecord {
                source: PID_SOURCE,
                source_feed_id: PID_FEED_ID,
                source_entity_id: format!("trip:{}:{stop_key}", entity.id),
                trip_id: trip_id.clone(),
                route_id: route_id.clone(),
                stop_id,
                delay_seconds,
                estimated_arrival: arrival.and_then(|event| unix_time(event.time)),
                estimated_departure: departure.and_then(|event| unix_time(event.time)),
                cancellation_status: cancellation_status.clone(),
                vehicle_id: vehicle_id.clone(),
                lat: None,
                lon: None,
                bearing: None,
                details: VehicleDetails::default(),
                fetched_at,
                valid_until,
                service_date,
                raw_payload: json!({
                    "entity_id": entity.id.as_str(),
                    "stop_id": stop_update.stop_id.as_deref(),
                    "stop_sequence": stop_update.stop_sequence,
                    "arrival_delay": arrival.and_then(|event| event.delay),
                    "arrival_time": arrival.and_then(|event| event.time),
                    "departure_delay": departure.and_then(|event| event.delay),
                    "departure_time": departure.and_then(|event| event.time)
                }),
            });
        }
    }
    Ok(records)
}

fn pid_vehicle_records(feed: &FeedMessage) -> Result<Vec<RealtimeRecord>> {
    let feed_fetched_at = feed_timestamp(feed).unwrap_or_else(Utc::now);
    let mut records = Vec::new();
    for entity in &feed.entity {
        let Some(vehicle) = &entity.vehicle else {
            continue;
        };
        let trip = vehicle.trip.as_ref();
        let position = vehicle.position.as_ref();
        let fetched_at = vehicle
            .timestamp
            .and_then(|timestamp| unix_time(Some(timestamp as i64)))
            .unwrap_or(feed_fetched_at);
        records.push(RealtimeRecord {
            source: PID_SOURCE,
            source_feed_id: PID_FEED_ID,
            source_entity_id: format!("vehicle:{}", entity.id),
            trip_id: trip
                .and_then(|trip| trip.trip_id.as_deref())
                .map(|id| scoped_pid_id(PID_STATIC_FEED_ID, id)),
            route_id: trip
                .and_then(|trip| trip.route_id.as_deref())
                .map(|id| scoped_pid_id(PID_STATIC_FEED_ID, id)),
            stop_id: vehicle
                .stop_id
                .as_deref()
                .map(|id| scoped_pid_id(PID_STATIC_FEED_ID, id)),
            delay_seconds: None,
            estimated_arrival: None,
            estimated_departure: None,
            cancellation_status: None,
            vehicle_id: vehicle.vehicle.as_ref().and_then(|value| value.id.clone()),
            lat: position.map(|position| position.latitude as f64),
            lon: position.map(|position| position.longitude as f64),
            bearing: position.and_then(|position| position.bearing.map(f64::from)),
            details: VehicleDetails {
                speed_kmh: position
                    .and_then(|position| position.speed.map(|speed| f64::from(speed) * 3.6)),
                occupancy_status: occupancy_status_name(vehicle.occupancy_status)
                    .map(str::to_string),
                state: vehicle_status_name(vehicle.current_status).map(str::to_string),
                ..VehicleDetails::default()
            },
            fetched_at,
            valid_until: fetched_at + chrono::Duration::seconds(90),
            service_date: trip
                .and_then(|trip| trip.start_date.as_deref())
                .and_then(|date| NaiveDate::parse_from_str(date, "%Y%m%d").ok()),
            raw_payload: json!({
                "entity_id": entity.id.as_str(),
                "trip_id": trip.and_then(|trip| trip.trip_id.as_deref()),
                "route_id": trip.and_then(|trip| trip.route_id.as_deref()),
                "stop_id": vehicle.stop_id.as_deref(),
                "vehicle_id": vehicle.vehicle.as_ref().and_then(|value| value.id.as_deref()),
                "timestamp": vehicle.timestamp
            }),
        });
    }
    Ok(records)
}

fn pid_json_vehicle_records(payload: &Value) -> Result<Vec<RealtimeRecord>> {
    let features = payload
        .get("features")
        .and_then(Value::as_array)
        .context("Golemio vehicle response is missing features")?;
    let mut records = Vec::with_capacity(features.len());
    for feature in features {
        let coordinates = feature
            .pointer("/geometry/coordinates")
            .and_then(Value::as_array);
        let (Some(lon), Some(lat)) = (
            coordinates
                .and_then(|values| values.first())
                .and_then(Value::as_f64),
            coordinates
                .and_then(|values| values.get(1))
                .and_then(Value::as_f64),
        ) else {
            continue;
        };
        let properties = feature.get("properties").unwrap_or(feature);
        let trip = properties.get("trip").unwrap_or(&Value::Null);
        let position = properties.get("last_position").unwrap_or(&Value::Null);
        let gtfs = trip.get("gtfs").unwrap_or(&Value::Null);
        let registration_number = value_string(trip.get("vehicle_registration_number"));
        let source_trip_id = value_string(gtfs.get("trip_id"));
        let vehicle_id = registration_number
            .clone()
            .map(|value| format!("registration:{value}"))
            .or_else(|| source_trip_id.clone().map(|value| format!("trip:{value}")));
        let Some(vehicle_id) = vehicle_id else {
            continue;
        };
        let fetched_at = position
            .get("origin_timestamp")
            .and_then(Value::as_str)
            .and_then(parse_timestamp)
            .unwrap_or_else(Utc::now);
        let vehicle_type_description = trip
            .pointer("/vehicle_type/description_en")
            .and_then(Value::as_str)
            .or_else(|| {
                trip.pointer("/vehicle_type/description_cs")
                    .and_then(Value::as_str)
            });
        let route_type = gtfs.get("route_type").and_then(Value::as_i64);
        records.push(RealtimeRecord {
            source: PID_SOURCE,
            source_feed_id: PID_FEED_ID,
            source_entity_id: format!("vehicle:{vehicle_id}"),
            trip_id: source_trip_id.map(|id| scoped_pid_id(PID_STATIC_FEED_ID, &id)),
            route_id: value_string(gtfs.get("route_id"))
                .map(|id| scoped_pid_id(PID_STATIC_FEED_ID, &id)),
            stop_id: value_string(position.pointer("/next_stop/id"))
                .map(|id| scoped_pid_id(PID_STATIC_FEED_ID, &id)),
            delay_seconds: value_i32(position.pointer("/delay/actual")),
            estimated_arrival: None,
            estimated_departure: None,
            cancellation_status: value_bool(position.get("is_canceled"))
                .filter(|value| *value)
                .map(|_| "cancelled".to_string()),
            vehicle_id: Some(vehicle_id),
            lat: Some(lat),
            lon: Some(lon),
            bearing: value_f64(position.get("bearing")),
            details: VehicleDetails {
                route_short_name: value_string(gtfs.get("route_short_name"))
                    .or_else(|| value_string(trip.get("origin_route_name"))),
                destination: value_string(gtfs.get("trip_headsign")),
                vehicle_type: canonical_vehicle_type(vehicle_type_description, route_type),
                speed_kmh: value_f64(position.get("speed")),
                wheelchair_accessible: value_bool(trip.get("wheelchair_accessible")),
                air_conditioned: value_bool(trip.get("air_conditioned")),
                usb_chargers: value_bool(trip.get("usb_chargers")),
                occupancy_status: None,
                registration_number,
                operator_name: value_string(trip.pointer("/agency_name/real")),
                tracking: value_bool(position.get("tracking")),
                state: value_string(position.get("state_position")),
            },
            fetched_at,
            valid_until: fetched_at + chrono::Duration::seconds(90),
            service_date: None,
            raw_payload: feature.clone(),
        });
    }
    Ok(records)
}

async fn run_ids_jmk_loop(pool: PgPool, client: Client) {
    let url = env::var("IDS_JMK_VEHICLES_URL")
        .unwrap_or_else(|_| "https://kordis-jmk.cz/gtfs/gtfsReal.dat".to_string());
    let interval = env_u64("IDS_JMK_POLL_INTERVAL_SECONDS", 30).max(15);
    loop {
        let attempted_at = Utc::now();
        let result = sync_ids_jmk(&pool, &client, &url).await;
        finish_sync(
            &pool,
            IDS_JMK_SOURCE,
            &url,
            "gtfs_realtime",
            attempted_at,
            result,
        )
        .await;
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

async fn sync_ids_jmk(
    pool: &PgPool,
    client: &Client,
    url: &str,
) -> Result<(usize, usize, Option<DateTime<Utc>>, Value)> {
    let bytes = fetch_bytes(client, url, None).await?;
    let feed = FeedMessage::decode(bytes.as_slice())?;
    let records = ids_jmk_vehicle_records(&feed);
    let received = records.len();
    let written = persist_records(pool, &records).await?;
    Ok((
        received,
        written,
        feed_timestamp(&feed),
        json!({"entities": feed.entity.len(), "format": "gtfs_realtime"}),
    ))
}

fn ids_jmk_vehicle_records(feed: &FeedMessage) -> Vec<RealtimeRecord> {
    let feed_fetched_at = feed_timestamp(feed).unwrap_or_else(Utc::now);
    feed.entity
        .iter()
        .filter_map(|entity| {
            let vehicle = entity.vehicle.as_ref()?;
            let position = vehicle.position.as_ref()?;
            let trip = vehicle.trip.as_ref();
            let descriptor = vehicle.vehicle.as_ref();
            let vehicle_id = descriptor
                .and_then(|value| value.id.clone().or_else(|| value.label.clone()))
                .unwrap_or_else(|| entity.id.clone());
            let fetched_at = vehicle
                .timestamp
                .and_then(|timestamp| unix_time(Some(timestamp as i64)))
                .unwrap_or(feed_fetched_at);
            let route_short_name = trip.and_then(|trip| trip.route_id.clone());
            Some(RealtimeRecord {
                source: IDS_JMK_SOURCE,
                source_feed_id: IDS_JMK_FEED_ID,
                source_entity_id: format!("vehicle:{}", entity.id),
                trip_id: trip
                    .and_then(|trip| trip.trip_id.as_deref())
                    .map(|id| format!("ids_jmk:trip:{id}")),
                route_id: trip
                    .and_then(|trip| trip.route_id.as_deref())
                    .map(|id| format!("ids_jmk:route:{id}")),
                stop_id: vehicle
                    .stop_id
                    .as_deref()
                    .map(|id| format!("ids_jmk:stop:{id}")),
                delay_seconds: None,
                estimated_arrival: None,
                estimated_departure: None,
                cancellation_status: None,
                vehicle_id: Some(vehicle_id),
                lat: Some(position.latitude as f64),
                lon: Some(position.longitude as f64),
                bearing: position.bearing.map(f64::from),
                details: VehicleDetails {
                    route_short_name,
                    speed_kmh: position.speed.map(|speed| f64::from(speed) * 3.6),
                    occupancy_status: occupancy_status_name(vehicle.occupancy_status)
                        .map(str::to_string),
                    state: vehicle_status_name(vehicle.current_status).map(str::to_string),
                    ..VehicleDetails::default()
                },
                fetched_at,
                valid_until: fetched_at + chrono::Duration::seconds(60),
                service_date: trip
                    .and_then(|trip| trip.start_date.as_deref())
                    .and_then(|date| NaiveDate::parse_from_str(date, "%Y%m%d").ok()),
                raw_payload: json!({
                    "entity_id": entity.id,
                    "trip_id": trip.and_then(|trip| trip.trip_id.as_deref()),
                    "route_id": trip.and_then(|trip| trip.route_id.as_deref()),
                    "stop_id": vehicle.stop_id,
                    "vehicle_id": descriptor.and_then(|value| value.id.as_deref()),
                    "vehicle_label": descriptor.and_then(|value| value.label.as_deref()),
                    "current_stop_sequence": vehicle.current_stop_sequence,
                    "current_status": vehicle.current_status,
                    "occupancy_status": vehicle.occupancy_status,
                    "timestamp": vehicle.timestamp
                }),
            })
        })
        .collect()
}

async fn run_duk_loop(pool: PgPool, client: Client) {
    let url = env::var("DUK_VEHICLES_URL")
        .unwrap_or_else(|_| "https://tabule.portabo.cz/api/v1-tabule/cis/GetTraffic/0".to_string());
    let interval = env_u64("DUK_POLL_INTERVAL_SECONDS", 30).max(15);
    loop {
        let attempted_at = Utc::now();
        let result = sync_duk(&pool, &client, &url).await;
        finish_sync(
            &pool,
            DUK_SOURCE,
            &url,
            "vehicle_positions",
            attempted_at,
            result,
        )
        .await;
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

async fn sync_duk(
    pool: &PgPool,
    client: &Client,
    url: &str,
) -> Result<(usize, usize, Option<DateTime<Utc>>, Value)> {
    let payload: Value = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let vehicles = payload
        .get("VehicleList")
        .and_then(Value::as_array)
        .context("DUK response is missing VehicleList")?;
    let mut records = Vec::with_capacity(vehicles.len());
    let mut latest_source_time: Option<DateTime<Utc>> = None;
    for vehicle in vehicles {
        let Some(vehicle_id) = value_string(vehicle.get("ID")) else {
            continue;
        };
        let fetched_at = vehicle
            .get("GPSPositionDT")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);
        latest_source_time =
            Some(latest_source_time.map_or(fetched_at, |time| time.max(fetched_at)));
        records.push(RealtimeRecord {
            source: DUK_SOURCE,
            source_feed_id: DUK_FEED_ID,
            source_entity_id: format!("vehicle:{vehicle_id}"),
            trip_id: vehicle
                .get("qride_tripID")
                .and_then(Value::as_str)
                .map(|id| format!("duk:trip:{id}")),
            route_id: value_string(vehicle.get("CISLineID")).map(|id| format!("duk:route:{id}")),
            stop_id: match (
                value_string(vehicle.get("StationNode")),
                value_string(vehicle.get("StationPost")),
            ) {
                (Some(node), Some(post)) => Some(format!("duk:stop:{node}:{post}")),
                (Some(node), None) => Some(format!("duk:stop:{node}")),
                _ => None,
            },
            delay_seconds: value_i32(vehicle.get("Delay")).map(|minutes| minutes * 60),
            estimated_arrival: vehicle
                .get("ArrivalDT")
                .and_then(Value::as_str)
                .and_then(parse_timestamp),
            estimated_departure: vehicle
                .get("TODepartureDT")
                .and_then(Value::as_str)
                .and_then(parse_timestamp),
            cancellation_status: None,
            vehicle_id: Some(vehicle_id),
            lat: value_f64(vehicle.get("Latitude")),
            lon: value_f64(vehicle.get("Longitude")),
            bearing: value_f64(vehicle.get("Azimut")),
            details: VehicleDetails::default(),
            fetched_at,
            valid_until: fetched_at + chrono::Duration::seconds(90),
            service_date: None,
            raw_payload: vehicle.clone(),
        });
    }
    let received = records.len();
    let written = persist_records(pool, &records).await?;
    Ok((received, written, latest_source_time, json!({})))
}

async fn persist_records(pool: &PgPool, records: &[RealtimeRecord]) -> Result<usize> {
    let records = deduplicate_records(records);
    let mut written = 0usize;
    for chunk in records.chunks(2_000) {
        let sources = chunk.iter().map(|record| record.source).collect::<Vec<_>>();
        let source_feed_ids = chunk
            .iter()
            .map(|record| record.source_feed_id)
            .collect::<Vec<_>>();
        let source_entity_ids = chunk
            .iter()
            .map(|record| record.source_entity_id.as_str())
            .collect::<Vec<_>>();
        let trip_ids = chunk
            .iter()
            .map(|record| record.trip_id.clone())
            .collect::<Vec<_>>();
        let route_ids = chunk
            .iter()
            .map(|record| record.route_id.clone())
            .collect::<Vec<_>>();
        let stop_ids = chunk
            .iter()
            .map(|record| record.stop_id.clone())
            .collect::<Vec<_>>();
        let delays = chunk
            .iter()
            .map(|record| record.delay_seconds)
            .collect::<Vec<_>>();
        let arrivals = chunk
            .iter()
            .map(|record| record.estimated_arrival)
            .collect::<Vec<_>>();
        let departures = chunk
            .iter()
            .map(|record| record.estimated_departure)
            .collect::<Vec<_>>();
        let cancellations = chunk
            .iter()
            .map(|record| record.cancellation_status.clone())
            .collect::<Vec<_>>();
        let vehicle_ids = chunk
            .iter()
            .map(|record| record.vehicle_id.clone())
            .collect::<Vec<_>>();
        let lats = chunk.iter().map(|record| record.lat).collect::<Vec<_>>();
        let lons = chunk.iter().map(|record| record.lon).collect::<Vec<_>>();
        let bearings = chunk
            .iter()
            .map(|record| record.bearing)
            .collect::<Vec<_>>();
        let fetched = chunk
            .iter()
            .map(|record| record.fetched_at)
            .collect::<Vec<_>>();
        let valid = chunk
            .iter()
            .map(|record| record.valid_until)
            .collect::<Vec<_>>();
        let service_dates = chunk
            .iter()
            .map(|record| record.service_date)
            .collect::<Vec<_>>();
        let payloads = chunk
            .iter()
            .map(|record| record.raw_payload.clone())
            .collect::<Vec<_>>();
        let route_short_names = chunk
            .iter()
            .map(|record| record.details.route_short_name.clone())
            .collect::<Vec<_>>();
        let destinations = chunk
            .iter()
            .map(|record| record.details.destination.clone())
            .collect::<Vec<_>>();
        let vehicle_types = chunk
            .iter()
            .map(|record| record.details.vehicle_type.clone())
            .collect::<Vec<_>>();
        let speeds = chunk
            .iter()
            .map(|record| record.details.speed_kmh)
            .collect::<Vec<_>>();
        let wheelchair_accessible = chunk
            .iter()
            .map(|record| record.details.wheelchair_accessible)
            .collect::<Vec<_>>();
        let air_conditioned = chunk
            .iter()
            .map(|record| record.details.air_conditioned)
            .collect::<Vec<_>>();
        let usb_chargers = chunk
            .iter()
            .map(|record| record.details.usb_chargers)
            .collect::<Vec<_>>();
        let occupancy_statuses = chunk
            .iter()
            .map(|record| record.details.occupancy_status.clone())
            .collect::<Vec<_>>();
        let registration_numbers = chunk
            .iter()
            .map(|record| record.details.registration_number.clone())
            .collect::<Vec<_>>();
        let operator_names = chunk
            .iter()
            .map(|record| record.details.operator_name.clone())
            .collect::<Vec<_>>();
        let tracking = chunk
            .iter()
            .map(|record| record.details.tracking)
            .collect::<Vec<_>>();
        let states = chunk
            .iter()
            .map(|record| record.details.state.clone())
            .collect::<Vec<_>>();
        written += sqlx::query(
            r#"
            INSERT INTO realtime_updates (
              source, source_feed_id, source_entity_id, trip_id, route_id, stop_id,
              delay_seconds, estimated_arrival, estimated_departure, cancellation_status,
              vehicle_id, vehicle_position, bearing, fetched_at, valid_until,
              service_date, confidence, raw_payload, route_short_name, destination,
              vehicle_type, speed_kmh, wheelchair_accessible, air_conditioned,
              usb_chargers, occupancy_status, vehicle_registration_number,
              operator_name, tracking, state
            )
            SELECT
              item.source, item.source_feed_id, item.source_entity_id, item.trip_id,
              item.route_id, item.stop_id, item.delay_seconds, item.estimated_arrival,
              item.estimated_departure, item.cancellation_status, item.vehicle_id,
              CASE WHEN item.lat IS NULL OR item.lon IS NULL THEN NULL
                ELSE ST_SetSRID(ST_MakePoint(item.lon, item.lat), 4326)::geography END,
              item.bearing, item.fetched_at, item.valid_until, item.service_date,
              'estimated', item.raw_payload, item.route_short_name, item.destination,
              item.vehicle_type, item.speed_kmh, item.wheelchair_accessible,
              item.air_conditioned, item.usb_chargers, item.occupancy_status,
              item.registration_number, item.operator_name, item.tracking, item.state
            FROM UNNEST(
              $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
              $7::integer[], $8::timestamptz[], $9::timestamptz[], $10::text[],
              $11::text[], $12::double precision[], $13::double precision[],
              $14::double precision[], $15::timestamptz[], $16::timestamptz[],
              $17::date[], $18::jsonb[], $19::text[], $20::text[], $21::text[],
              $22::double precision[], $23::boolean[], $24::boolean[], $25::boolean[],
              $26::text[], $27::text[], $28::text[], $29::boolean[], $30::text[]
            ) AS item(
              source, source_feed_id, source_entity_id, trip_id, route_id, stop_id,
              delay_seconds, estimated_arrival, estimated_departure, cancellation_status,
              vehicle_id, lat, lon, bearing, fetched_at, valid_until, service_date, raw_payload,
              route_short_name, destination, vehicle_type, speed_kmh, wheelchair_accessible,
              air_conditioned, usb_chargers, occupancy_status, registration_number,
              operator_name, tracking, state
            )
            ON CONFLICT (source, source_entity_id) WHERE source_entity_id IS NOT NULL
            DO UPDATE SET
              source_feed_id = EXCLUDED.source_feed_id,
              trip_id = EXCLUDED.trip_id,
              route_id = EXCLUDED.route_id,
              stop_id = EXCLUDED.stop_id,
              delay_seconds = EXCLUDED.delay_seconds,
              estimated_arrival = EXCLUDED.estimated_arrival,
              estimated_departure = EXCLUDED.estimated_departure,
              cancellation_status = EXCLUDED.cancellation_status,
              vehicle_id = EXCLUDED.vehicle_id,
              vehicle_position = EXCLUDED.vehicle_position,
              bearing = EXCLUDED.bearing,
              fetched_at = EXCLUDED.fetched_at,
              valid_until = EXCLUDED.valid_until,
              service_date = EXCLUDED.service_date,
              route_short_name = EXCLUDED.route_short_name,
              destination = EXCLUDED.destination,
              vehicle_type = EXCLUDED.vehicle_type,
              speed_kmh = EXCLUDED.speed_kmh,
              wheelchair_accessible = EXCLUDED.wheelchair_accessible,
              air_conditioned = EXCLUDED.air_conditioned,
              usb_chargers = EXCLUDED.usb_chargers,
              occupancy_status = EXCLUDED.occupancy_status,
              vehicle_registration_number = EXCLUDED.vehicle_registration_number,
              operator_name = EXCLUDED.operator_name,
              tracking = EXCLUDED.tracking,
              state = EXCLUDED.state,
              confidence = EXCLUDED.confidence,
              raw_payload = EXCLUDED.raw_payload
            "#,
        )
        .bind(sources)
        .bind(source_feed_ids)
        .bind(source_entity_ids)
        .bind(trip_ids)
        .bind(route_ids)
        .bind(stop_ids)
        .bind(delays)
        .bind(arrivals)
        .bind(departures)
        .bind(cancellations)
        .bind(vehicle_ids)
        .bind(lats)
        .bind(lons)
        .bind(bearings)
        .bind(fetched)
        .bind(valid)
        .bind(service_dates)
        .bind(payloads)
        .bind(route_short_names)
        .bind(destinations)
        .bind(vehicle_types)
        .bind(speeds)
        .bind(wheelchair_accessible)
        .bind(air_conditioned)
        .bind(usb_chargers)
        .bind(occupancy_statuses)
        .bind(registration_numbers)
        .bind(operator_names)
        .bind(tracking)
        .bind(states)
        .execute(pool)
        .await?
        .rows_affected() as usize;
    }
    Ok(written)
}

fn deduplicate_records(records: &[RealtimeRecord]) -> Vec<&RealtimeRecord> {
    let mut records_by_identity = HashMap::new();
    for record in records {
        records_by_identity
            .entry((record.source, record.source_entity_id.as_str()))
            .and_modify(|existing: &mut &RealtimeRecord| {
                if record.fetched_at >= existing.fetched_at {
                    *existing = record;
                }
            })
            .or_insert(record);
    }
    let mut records = records_by_identity.into_values().collect::<Vec<_>>();
    records.sort_by(|left, right| {
        left.source
            .cmp(right.source)
            .then_with(|| left.source_entity_id.cmp(&right.source_entity_id))
    });
    records
}

async fn cleanup_expired(pool: &PgPool) -> Result<()> {
    sqlx::query("DELETE FROM realtime_updates WHERE valid_until < now() - interval '48 hours'")
        .execute(pool)
        .await?;
    Ok(())
}

async fn finish_sync(
    pool: &PgPool,
    source_id: &str,
    source_url: &str,
    data_kind: &str,
    attempted_at: DateTime<Utc>,
    result: Result<(usize, usize, Option<DateTime<Utc>>, Value)>,
) {
    let (status, received, written, source_timestamp, error_message, metadata) = match result {
        Ok((received, written, source_timestamp, metadata)) => {
            tracing::info!(source_id, received, written, "realtime source synchronized");
            (
                "success",
                received,
                written,
                source_timestamp,
                None,
                metadata,
            )
        }
        Err(error) => {
            tracing::warn!(source_id, error = %error, "realtime source synchronization failed");
            ("error", 0, 0, None, Some(error.to_string()), json!({}))
        }
    };
    if let Err(error) = sqlx::query(
        r#"
        INSERT INTO data_source_syncs (
          source_id, source_url, data_kind, status, last_attempt_at, last_success_at,
          source_timestamp, records_received, records_written, error_message, metadata
        )
        VALUES ($1, $2, $3, $4, $5,
          CASE WHEN $4 = 'success' THEN $5 ELSE NULL END,
          $6, $7, $8, $9, $10)
        ON CONFLICT (source_id) DO UPDATE SET
          source_url = EXCLUDED.source_url,
          data_kind = EXCLUDED.data_kind,
          status = EXCLUDED.status,
          last_attempt_at = EXCLUDED.last_attempt_at,
          last_success_at = COALESCE(EXCLUDED.last_success_at, data_source_syncs.last_success_at),
          source_timestamp = COALESCE(EXCLUDED.source_timestamp, data_source_syncs.source_timestamp),
          records_received = EXCLUDED.records_received,
          records_written = EXCLUDED.records_written,
          error_message = EXCLUDED.error_message,
          metadata = EXCLUDED.metadata
        "#,
    )
    .bind(source_id)
    .bind(source_url)
    .bind(data_kind)
    .bind(status)
    .bind(attempted_at)
    .bind(source_timestamp)
    .bind(received as i32)
    .bind(written as i32)
    .bind(error_message)
    .bind(metadata)
    .execute(pool)
    .await
    {
        tracing::error!(source_id, error = %error, "failed to record synchronization status");
    }
}

async fn run_mock_loop(pool: &PgPool) {
    loop {
        let now = Utc::now();
        let record = RealtimeRecord {
            source: "mock",
            source_feed_id: "mock_realtime",
            source_entity_id: "mock-update-1".to_string(),
            trip_id: Some("mock:trip-rail-1".to_string()),
            route_id: Some("mock:route-r9".to_string()),
            stop_id: Some("mock:stop-praha-hl-n".to_string()),
            delay_seconds: Some(120),
            estimated_arrival: None,
            estimated_departure: Some(now + chrono::Duration::minutes(2)),
            cancellation_status: None,
            vehicle_id: None,
            lat: None,
            lon: None,
            bearing: None,
            details: VehicleDetails::default(),
            fetched_at: now,
            valid_until: now + chrono::Duration::minutes(5),
            service_date: None,
            raw_payload: json!({"mock": true}),
        };
        if let Err(error) = persist_records(pool, &[record]).await {
            tracing::error!(error = %error, "failed to persist mock realtime update");
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

fn scoped_pid_id(feed_id: &str, source_id: &str) -> String {
    format!("{feed_id}:{source_id}")
}

fn feed_timestamp(feed: &FeedMessage) -> Option<DateTime<Utc>> {
    feed.header
        .as_ref()
        .and_then(|header| header.timestamp)
        .and_then(|timestamp| unix_time(Some(timestamp as i64)))
}

fn unix_time(timestamp: Option<i64>) -> Option<DateTime<Utc>> {
    timestamp.and_then(|value| Utc.timestamp_opt(value, 0).single())
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
        .filter(|value| value.timestamp() > 0)
}

fn value_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_i32(value: Option<&Value>) -> Option<i32> {
    value?.as_i64().and_then(|value| i32::try_from(value).ok())
}

fn value_f64(value: Option<&Value>) -> Option<f64> {
    value?.as_f64()
}

fn value_bool(value: Option<&Value>) -> Option<bool> {
    match value? {
        Value::Bool(value) => Some(*value),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn canonical_vehicle_type(description: Option<&str>, route_type: Option<i64>) -> Option<String> {
    let description = description.unwrap_or_default().to_lowercase();
    let mapped = if description.contains("trolley") || description.contains("trolej") {
        Some("trolleybus")
    } else if description.contains("tram") {
        Some("tram")
    } else if description.contains("metro") || description.contains("subway") {
        Some("metro")
    } else if description.contains("train")
        || description.contains("rail")
        || description.contains("vlak")
    {
        Some("train")
    } else if description.contains("ferry") || description.contains("boat") {
        Some("ferry")
    } else if description.contains("cable")
        || description.contains("funicular")
        || description.contains("lanov")
    {
        Some("cable_car")
    } else if description.contains("bus") || description.contains("autobus") {
        Some("bus")
    } else {
        match route_type {
            Some(0 | 900..=999) => Some("tram"),
            Some(1) => Some("metro"),
            Some(2 | 100..=199 | 400..=499) => Some("train"),
            Some(3 | 200..=299 | 700..=799) => Some("bus"),
            Some(4 | 1000..=1099) => Some("ferry"),
            Some(5 | 1300..=1399) => Some("cable_car"),
            Some(11 | 800..=899) => Some("trolleybus"),
            _ => None,
        }
    };
    mapped.map(str::to_string)
}

fn occupancy_status_name(status: Option<i32>) -> Option<&'static str> {
    match status? {
        0 => Some("empty"),
        1 => Some("many_seats_available"),
        2 => Some("few_seats_available"),
        3 => Some("standing_room_only"),
        4 => Some("crushed_standing_room_only"),
        5 => Some("full"),
        6 => Some("not_accepting_passengers"),
        7 => Some("no_data_available"),
        8 => Some("not_boardable"),
        _ => None,
    }
}

fn vehicle_status_name(status: Option<i32>) -> Option<&'static str> {
    match status? {
        0 => Some("incoming_at"),
        1 => Some("stopped_at"),
        2 => Some("in_transit_to"),
        _ => None,
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_duk_timestamps_and_ignores_epoch_placeholders() {
        assert!(parse_timestamp("2026-07-04T13:48:09+02:00").is_some());
        assert!(parse_timestamp("1970-01-01T02:00:00+02:00").is_none());
    }

    #[test]
    fn converts_string_and_numeric_source_values() {
        assert_eq!(value_string(Some(&json!(42))).as_deref(), Some("42"));
        assert_eq!(value_bool(Some(&json!("false"))), Some(false));
    }

    #[test]
    fn realtime_batch_keeps_latest_duplicate_entity() {
        let timestamp = Utc::now();
        let record = |fetched_at, delay_seconds| RealtimeRecord {
            source: PID_SOURCE,
            source_feed_id: PID_FEED_ID,
            source_entity_id: "vehicle:duplicate".to_string(),
            trip_id: None,
            route_id: None,
            stop_id: None,
            delay_seconds: Some(delay_seconds),
            estimated_arrival: None,
            estimated_departure: None,
            cancellation_status: None,
            vehicle_id: Some("duplicate".to_string()),
            lat: None,
            lon: None,
            bearing: None,
            details: VehicleDetails::default(),
            fetched_at,
            valid_until: fetched_at + chrono::Duration::seconds(90),
            service_date: None,
            raw_payload: json!({}),
        };
        let older = record(timestamp, 30);
        let newer = record(timestamp + chrono::Duration::seconds(1), 60);

        let input = [older, newer];
        let records = deduplicate_records(&input);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].delay_seconds, Some(60));
    }

    #[test]
    fn decodes_pid_trip_delay_with_static_gtfs_ids() {
        let feed = FeedMessage {
            header: Some(FeedHeader {
                timestamp: Some(1_788_000_000),
            }),
            entity: vec![FeedEntity {
                id: "update-1".to_string(),
                trip_update: Some(TripUpdate {
                    trip: Some(TripDescriptor {
                        trip_id: Some("991_11915_260207".to_string()),
                        route_id: Some("L991".to_string()),
                        start_date: Some("20260704".to_string()),
                        ..Default::default()
                    }),
                    stop_time_update: vec![StopTimeUpdate {
                        stop_id: Some("U1Z1P".to_string()),
                        departure: Some(StopTimeEvent {
                            delay: Some(120),
                            time: Some(1_788_000_120),
                        }),
                        ..Default::default()
                    }],
                    vehicle: None,
                }),
                vehicle: None,
            }],
        };
        let decoded = FeedMessage::decode(feed.encode_to_vec().as_slice()).unwrap();
        let records = pid_trip_records(&decoded).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].trip_id.as_deref(),
            Some("pid_gtfs:991_11915_260207")
        );
        assert_eq!(records[0].route_id.as_deref(), Some("pid_gtfs:L991"));
        assert_eq!(records[0].stop_id.as_deref(), Some("pid_gtfs:U1Z1P"));
        assert_eq!(records[0].delay_seconds, Some(120));
    }

    #[test]
    fn normalizes_golemio_vehicle_equipment() {
        let payload = json!({
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [14.4378, 50.0755]},
                "properties": {
                    "trip": {
                        "agency_name": {"real": "DPP"},
                        "gtfs": {
                            "route_id": "L119",
                            "route_short_name": "119",
                            "route_type": 3,
                            "trip_id": "119_1_260720",
                            "trip_headsign": "Letiště"
                        },
                        "vehicle_registration_number": 8826,
                        "vehicle_type": {"description_en": "regional bus"},
                        "wheelchair_accessible": true,
                        "air_conditioned": true,
                        "usb_chargers": false
                    },
                    "last_position": {
                        "bearing": 125,
                        "speed": 42,
                        "delay": {"actual": 30},
                        "next_stop": {"id": "U1Z1P"},
                        "origin_timestamp": "2026-07-20T12:34:56Z",
                        "tracking": true,
                        "state_position": "on_track",
                        "is_canceled": false
                    }
                }
            }]
        });

        let records = pid_json_vehicle_records(&payload).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].vehicle_id.as_deref(), Some("registration:8826"));
        assert_eq!(records[0].details.vehicle_type.as_deref(), Some("bus"));
        assert_eq!(records[0].details.speed_kmh, Some(42.0));
        assert_eq!(records[0].details.wheelchair_accessible, Some(true));
        assert_eq!(records[0].details.air_conditioned, Some(true));
        assert_eq!(records[0].details.usb_chargers, Some(false));
        assert_eq!(records[0].details.destination.as_deref(), Some("Letiště"));
    }
}
