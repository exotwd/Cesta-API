use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Europe::Prague;
use quick_xml::events::Event;
use quick_xml::Reader;
use reqwest::Client;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::time::Duration;

use crate::{
    RealtimeRecord, VehicleDetails, finish_sync, persist_records, DUK_FEED_ID, DUK_SOURCE,
};

pub async fn run_duk_loop(pool: PgPool, client: Client) {
    let url = std::env::var("DUK_OPEN_API_URL")
        .unwrap_or_else(|_| "http://provoz.kr-ustecky.cz:7500/Open".to_string());
    let interval = crate::env_u64("DUK_POLL_INTERVAL_SECONDS", 30).max(15);

    tokio::join!(
        async {
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
        },
        async {
            loop {
                let attempted_at = Utc::now();
                let result = sync_duk_departures_all_nodes(&pool, &client, &url).await;
                finish_sync(
                    &pool,
                    "duk_departures",
                    &url,
                    "trip_summaries",
                    attempted_at,
                    result,
                )
                .await;
                tokio::time::sleep(Duration::from_secs(interval * 2)).await;
            }
        }
    );
}

async fn sync_duk(
    pool: &PgPool,
    client: &Client,
    url: &str,
) -> Result<(usize, usize, Option<DateTime<Utc>>, Value)> {
    let body = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
    <s:Body>
        <GetActualVehicles xmlns="http://tempuri.org/"/>
    </s:Body>
</s:Envelope>"#;

    let response = client
        .post(url)
        .header("Content-Type", "text/xml; charset=utf-8")
        .header("SOAPAction", "http://tempuri.org/IOpen/GetActualVehicles")
        .body(body)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let mut reader = Reader::from_str(&response);
    reader.config_mut().trim_text_end = true;
    let mut buf = Vec::new();

    let mut vehicles = Vec::new();
    let mut current_vehicle = serde_json::Map::new();
    let mut current_tag = String::new();
    let mut in_vehicle = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let name = name.split(':').last().unwrap_or(&name).to_string(); // strip namespace
                if name == "Vehicle" || name == "GetActualVehiclesResult" { // Adjust depending on actual tag name, using Vehicle as generic guess
                    in_vehicle = true;
                    current_vehicle = serde_json::Map::new();
                } else if in_vehicle {
                    current_tag = name;
                }
            }
            Ok(Event::Text(e)) => {
                if in_vehicle && !current_tag.is_empty() {
                    let text = e.unescape().unwrap_or_default().to_string();
                    current_vehicle.insert(current_tag.clone(), Value::String(text));
                }
            }
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let name = name.split(':').last().unwrap_or(&name).to_string();
                if name == "Vehicle" || name == "GetActualVehiclesResult" {
                    in_vehicle = false;
                    vehicles.push(Value::Object(current_vehicle.clone()));
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => anyhow::bail!("XML parse error: {}", e),
            _ => (),
        }
        buf.clear();
    }

    let mut records = Vec::with_capacity(vehicles.len());
    let mut latest_source_time: Option<DateTime<Utc>> = None;

    for vehicle in vehicles {
        let Some(vehicle_id) = vehicle.get("ID").and_then(Value::as_str) else {
            continue;
        };

        // DUK API returns time in Europe/Prague. We parse it and convert to UTC.
        let fetched_at = vehicle
            .get("GPSPositionDT")
            .and_then(Value::as_str)
            .and_then(|val| {
                // Try parse naive date time
                chrono::NaiveDateTime::parse_from_str(val, "%Y-%m-%dT%H:%M:%S")
                    .ok()
                    .or_else(|| chrono::NaiveDateTime::parse_from_str(val, "%Y-%m-%dT%H:%M:%S%.f").ok())
            })
            .and_then(|ndt| Prague.from_local_datetime(&ndt).single())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        // Ignore stale data (older than 10 minutes)
        if Utc::now().signed_duration_since(fetched_at).num_minutes() > 10 {
            continue;
        }

        latest_source_time =
            Some(latest_source_time.map_or(fetched_at, |time| time.max(fetched_at)));

        let delay_seconds = vehicle
            .get("Delay")
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<i32>().ok())
            .map(|minutes| minutes * 60);

        let lat = vehicle
            .get("Latitude")
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<f64>().ok());

        let lon = vehicle
            .get("Longitude")
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<f64>().ok());
            
        let bearing = vehicle
            .get("Azimut")
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<f64>().ok());

        records.push(RealtimeRecord {
            source: DUK_SOURCE,
            source_feed_id: DUK_FEED_ID,
            source_entity_id: format!("vehicle:{vehicle_id}"),
            trip_id: vehicle
                .get("qride_tripID") // Just in case they still return it
                .and_then(Value::as_str)
                .map(|id| format!("duk:trip:{id}")),
            route_id: vehicle
                .get("CISLineID")
                .and_then(Value::as_str)
                .map(|id| format!("duk:route:{id}")),
            stop_id: match (
                vehicle.get("StationNode").and_then(Value::as_str),
                vehicle.get("StationPost").and_then(Value::as_str),
            ) {
                (Some(node), Some(post)) => Some(format!("duk:stop:{node}:{post}")),
                (Some(node), None) => Some(format!("duk:stop:{node}")),
                _ => None,
            },
            delay_seconds,
            estimated_arrival: None, // Need departures endpoint for this
            estimated_departure: None,
            cancellation_status: None,
            vehicle_id: Some(vehicle_id.to_string()),
            lat,
            lon,
            bearing,
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


async fn sync_duk_departures_all_nodes(
    pool: &PgPool,
    client: &Client,
    url: &str,
) -> Result<(usize, usize, Option<DateTime<Utc>>, Value)> {
    // 1. Fetch active DUK nodes from database
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT split_part(id, ':', 3) as node
        FROM stops
        WHERE id LIKE 'duk:stop:%' AND is_active = true
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut nodes: Vec<String> = Vec::new();
    for row in rows {
        if let Ok(node) = row.try_get::<String, _>("node") {
            nodes.push(node);
        }
    }

    let mut total_received = 0;
    let mut total_written = 0;
    let mut latest_time: Option<DateTime<Utc>> = None;

    // 2. Query each node (we could do this concurrently or in chunks)
    for chunk in nodes.chunks(10) {
        let mut futures = Vec::new();
        for node in chunk {
            futures.push(sync_duk_departures_node(pool, client, url, node));
        }

        let results = futures_util::future::join_all(futures).await;
        for res in results {
            if let Ok((rec, writ, time, _)) = res {
                total_received += rec;
                total_written += writ;
                if let Some(t) = time {
                    latest_time = Some(latest_time.map_or(t, |max_t| max_t.max(t)));
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await; // Rate limit protection
    }

    Ok((total_received, total_written, latest_time, json!({})))
}

async fn sync_duk_departures_node(
    pool: &PgPool,
    client: &Client,
    url: &str,
    node_id: &str,
) -> Result<(usize, usize, Option<DateTime<Utc>>, Value)> {
    let body = format!(
        r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
    <s:Body>
        <GetStationDeparturesThroughAllPostsOfNode xmlns="http://tempuri.org/">
            <nodeId>{}</nodeId>
        </GetStationDeparturesThroughAllPostsOfNode>
    </s:Body>
</s:Envelope>"#,
        node_id
    );

    let response = client
        .post(url)
        .header("Content-Type", "text/xml; charset=utf-8")
        .header("SOAPAction", "http://tempuri.org/IOpen/GetStationDeparturesThroughAllPostsOfNode")
        .body(body)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let mut reader = Reader::from_str(&response);
    reader.config_mut().trim_text_end = true;
    let mut buf = Vec::new();

    let mut departures = Vec::new();
    let mut current_departure = serde_json::Map::new();
    let mut current_tag = String::new();
    let mut in_departure = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let name = name.split(':').last().unwrap_or(&name).to_string();
                if name == "Departure" || name == "GetStationDeparturesThroughAllPostsOfNodeResult" { 
                    in_departure = true;
                    current_departure = serde_json::Map::new();
                } else if in_departure {
                    current_tag = name;
                }
            }
            Ok(Event::Text(e)) => {
                if in_departure && !current_tag.is_empty() {
                    let text = e.unescape().unwrap_or_default().to_string();
                    current_departure.insert(current_tag.clone(), Value::String(text));
                }
            }
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let name = name.split(':').last().unwrap_or(&name).to_string();
                if name == "Departure" || name == "GetStationDeparturesThroughAllPostsOfNodeResult" {
                    in_departure = false;
                    departures.push(Value::Object(current_departure.clone()));
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => anyhow::bail!("XML parse error: {}", e),
            _ => (),
        }
        buf.clear();
    }

    let mut records = Vec::with_capacity(departures.len());
    let mut latest_source_time: Option<DateTime<Utc>> = None;
    let fetched_at = Utc::now();

    for departure in departures {
        let cis_line = departure.get("CISLineID").and_then(Value::as_str);
        
        let estimated_departure = departure
            .get("DepartureDT") // Adjust based on real API XML tags
            .and_then(Value::as_str)
            .and_then(|val| {
                chrono::NaiveDateTime::parse_from_str(val, "%Y-%m-%dT%H:%M:%S")
                    .ok()
                    .or_else(|| chrono::NaiveDateTime::parse_from_str(val, "%Y-%m-%dT%H:%M:%S%.f").ok())
            })
            .and_then(|ndt| Prague.from_local_datetime(&ndt).single())
            .map(|dt| dt.with_timezone(&Utc));

        let delay_seconds = departure
            .get("Delay")
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<i32>().ok())
            .map(|minutes| minutes * 60);

        latest_source_time = Some(latest_source_time.map_or(fetched_at, |time| time.max(fetched_at)));

        records.push(RealtimeRecord {
            source: "duk_departures",
            source_feed_id: DUK_FEED_ID,
            source_entity_id: format!("departure:{}:{}", node_id, departure.get("ID").and_then(Value::as_str).unwrap_or("unknown")),
            trip_id: departure
                .get("qride_tripID")
                .and_then(Value::as_str)
                .map(|id| format!("duk:trip:{id}")),
            route_id: cis_line.map(|id| format!("duk:route:{id}")),
            stop_id: Some(format!("duk:stop:{}", node_id)),
            delay_seconds,
            estimated_arrival: None,
            estimated_departure,
            cancellation_status: None,
            vehicle_id: departure.get("VehicleID").and_then(Value::as_str).map(String::from),
            lat: None,
            lon: None,
            bearing: None,
            details: VehicleDetails::default(),
            fetched_at,
            valid_until: fetched_at + chrono::Duration::minutes(5),
            service_date: None,
            raw_payload: departure.clone(),
        });
    }

    let received = records.len();
    let written = if received > 0 { persist_records(pool, &records).await? } else { 0 };
    Ok((received, written, latest_source_time, json!({})))
}
