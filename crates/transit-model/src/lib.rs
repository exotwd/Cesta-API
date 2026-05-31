use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use geo_types::Point;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TransportMode {
    Train,
    Tram,
    Bus,
    Metro,
    Trolleybus,
    Ferry,
    CableCar,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoordinateConfidence {
    Exact,
    High,
    Medium,
    Low,
    Unresolved,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRef {
    pub feed_id: String,
    pub original_id: String,
    pub import_run_id: Option<Uuid>,
    pub priority: i32,
    pub confidence: Option<CoordinateConfidence>,
    pub suppressed_as_duplicate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stop {
    pub id: String,
    pub source_ids: Vec<SourceRef>,
    pub name: String,
    pub normalized_name: String,
    pub municipality: Option<String>,
    pub district: Option<String>,
    pub region: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub geom: Option<Point<f64>>,
    pub coordinate_confidence: CoordinateConfidence,
    pub coordinate_source: Option<String>,
    pub stop_area_id: Option<String>,
    pub platform_code: Option<String>,
    pub modes: Vec<TransportMode>,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopArea {
    pub id: String,
    pub name: String,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agency {
    pub id: String,
    pub source_id: String,
    pub name: String,
    pub url: Option<String>,
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operator {
    pub id: String,
    pub name: String,
    pub source_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Route {
    pub id: String,
    pub source_id: String,
    pub agency_id: Option<String>,
    pub operator_id: Option<String>,
    pub short_name: Option<String>,
    pub long_name: Option<String>,
    pub mode: TransportMode,
    pub gtfs_route_type: Option<i32>,
    pub color: Option<String>,
    pub text_color: Option<String>,
    pub source_priority: i32,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trip {
    pub id: String,
    pub source_id: String,
    pub route_id: String,
    pub service_id: String,
    pub headsign: Option<String>,
    pub direction_id: Option<i16>,
    pub shape_id: Option<String>,
    pub restrictions: Value,
    pub raw_source_metadata: Value,
    pub source_priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopTime {
    pub trip_id: String,
    pub stop_id: String,
    pub stop_sequence: u32,
    pub arrival_time: u32,
    pub departure_time: u32,
    pub pickup_type: Option<i16>,
    pub drop_off_type: Option<i16>,
    pub timepoint: Option<bool>,
    pub platform: Option<String>,
    pub raw_notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Calendar {
    pub service_id: String,
    pub monday: bool,
    pub tuesday: bool,
    pub wednesday: bool,
    pub thursday: bool,
    pub friday: bool,
    pub saturday: bool,
    pub sunday: bool,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalendarDate {
    pub service_id: String,
    pub date: NaiveDate,
    pub exception_type: i16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transfer {
    pub from_stop_id: String,
    pub to_stop_id: String,
    pub min_transfer_seconds: u32,
    pub distance_meters: Option<u32>,
    pub walking_geometry: Option<Value>,
    pub confidence: CoordinateConfidence,
    pub accessibility_level: Option<String>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealtimeUpdate {
    pub id: String,
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub stop_id: Option<String>,
    pub delay_seconds: Option<i32>,
    pub estimated_arrival: Option<DateTime<Utc>>,
    pub estimated_departure: Option<DateTime<Utc>>,
    pub cancellation_status: Option<String>,
    pub platform_change: Option<String>,
    pub vehicle_position: Option<Point<f64>>,
    pub source: String,
    pub fetched_at: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub confidence: RealtimeConfidence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RealtimeConfidence {
    Exact,
    Estimated,
    Stale,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RealtimeStatus {
    Full,
    Partial,
    Unavailable,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JourneyLeg {
    pub from_stop_id: String,
    pub to_stop_id: String,
    pub route_id: Option<String>,
    pub trip_id: Option<String>,
    pub departure_time: u32,
    pub arrival_time: u32,
    pub mode: TransportMode,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Journey {
    pub id: String,
    pub legs: Vec<JourneyLeg>,
    pub departure_time: u32,
    pub arrival_time: u32,
    pub duration_seconds: u32,
    pub transfer_count: u32,
    pub walking_distance_meters: u32,
    pub realtime_status: RealtimeStatus,
    pub risk_score: f32,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflinePackage {
    pub id: String,
    pub name_cs: String,
    pub version: String,
    pub checksum: Option<String>,
    pub valid_from: Option<NaiveDate>,
    pub valid_until: Option<NaiveDate>,
    pub size_bytes: Option<u64>,
    pub mock: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TicketOption {
    pub id: String,
    pub name_cs: String,
    pub provider: String,
    pub price_czk: Option<f32>,
    pub mock: bool,
}

pub fn normalize_czech_name(value: &str) -> String {
    value
        .trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn parse_gtfs_time(value: &str) -> Option<u32> {
    let mut parts = value.split(':');
    let hours = parts.next()?.parse::<u32>().ok()?;
    let minutes = parts.next()?.parse::<u32>().ok()?;
    let seconds = parts.next()?.parse::<u32>().ok()?;
    Some(hours * 3600 + minutes * 60 + seconds)
}

pub fn seconds_to_time(value: u32) -> String {
    let hours = value / 3600;
    let minutes = (value % 3600) / 60;
    let seconds = value % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

pub fn naive_time_to_seconds(value: NaiveTime) -> u32 {
    value.signed_duration_since(NaiveTime::MIN).num_seconds() as u32
}
