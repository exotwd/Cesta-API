use std::{
    fs::File,
    io::{Read, Seek},
    path::Path,
};

use anyhow::{Context, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use transit_model::{
    AccessibilityStatus, Agency, Calendar, CalendarDate, Route, Stop, StopLocationType, StopTime,
    TransportMode, normalize_czech_name, parse_gtfs_time,
};
use zip::ZipArchive;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportOptions {
    pub source_feed_id: String,
    pub source_priority: i32,
    pub limit_rows: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GtfsDataset {
    pub agencies: Vec<Agency>,
    pub stops: Vec<Stop>,
    pub routes: Vec<Route>,
    pub trips: Vec<GtfsTrip>,
    pub stop_times: Vec<StopTime>,
    pub calendars: Vec<Calendar>,
    pub calendar_dates: Vec<CalendarDate>,
    pub validation_issues: Vec<ValidationIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GtfsTrip {
    pub route_id: String,
    pub service_id: String,
    pub trip_id: String,
    pub trip_headsign: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub severity: ValidationSeverity,
    pub code: String,
    pub message: String,
    pub source_file: Option<String>,
    pub affected_entity: Option<String>,
    pub raw_payload: Option<serde_json::Value>,
}

pub fn sha256_file(path: impl AsRef<Path>) -> Result<String> {
    let mut file = File::open(path.as_ref())
        .with_context(|| format!("failed to open {}", path.as_ref().display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

pub fn parse_gtfs_zip(path: impl AsRef<Path>, options: ImportOptions) -> Result<GtfsDataset> {
    let file = File::open(path.as_ref())
        .with_context(|| format!("failed to open GTFS zip {}", path.as_ref().display()))?;
    let mut archive = ZipArchive::new(file)?;
    parse_archive(&mut archive, options)
}

fn parse_archive<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    options: ImportOptions,
) -> Result<GtfsDataset> {
    let mut dataset = GtfsDataset::default();
    let names = archive.file_names().map(str::to_string).collect::<Vec<_>>();

    for required in [
        "agency.txt",
        "stops.txt",
        "routes.txt",
        "trips.txt",
        "stop_times.txt",
    ] {
        if !names.iter().any(|name| name == required) {
            dataset.validation_issues.push(ValidationIssue {
                severity: ValidationSeverity::Error,
                code: "missing_required_file".to_string(),
                message: format!("Required GTFS file {required} is missing"),
                source_file: Some(required.to_string()),
                affected_entity: None,
                raw_payload: None,
            });
        }
    }

    if names.iter().any(|name| name == "agency.txt") {
        dataset.agencies = parse_agencies(archive, options.limit_rows)?;
    }
    if names.iter().any(|name| name == "stops.txt") {
        dataset.stops = parse_stops(
            archive,
            &options,
            options.limit_rows,
            &mut dataset.validation_issues,
        )?;
    }
    if names.iter().any(|name| name == "routes.txt") {
        dataset.routes = parse_routes(archive, &options, options.limit_rows)?;
    }
    if names.iter().any(|name| name == "trips.txt") {
        dataset.trips = parse_trips(archive, options.limit_rows)?;
    }
    if names.iter().any(|name| name == "stop_times.txt") {
        dataset.stop_times =
            parse_stop_times(archive, options.limit_rows, &mut dataset.validation_issues)?;
    }
    if names.iter().any(|name| name == "calendar.txt") {
        dataset.calendars = parse_calendars(archive, options.limit_rows)?;
    }
    if names.iter().any(|name| name == "calendar_dates.txt") {
        dataset.calendar_dates = parse_calendar_dates(archive, options.limit_rows)?;
    }
    if dataset.calendars.is_empty() && dataset.calendar_dates.is_empty() {
        dataset.validation_issues.push(ValidationIssue {
            severity: ValidationSeverity::Warning,
            code: "missing_service_calendar".to_string(),
            message: "GTFS feed has neither calendar.txt nor calendar_dates.txt".to_string(),
            source_file: None,
            affected_entity: None,
            raw_payload: None,
        });
    }
    populate_stop_modes(&mut dataset);

    Ok(dataset)
}

fn csv_reader<R: Read>(reader: R) -> csv::Reader<R> {
    csv::ReaderBuilder::new().flexible(true).from_reader(reader)
}

#[derive(Debug, Deserialize)]
struct AgencyRow {
    agency_id: Option<String>,
    agency_name: String,
    agency_url: Option<String>,
    agency_timezone: Option<String>,
}

fn parse_agencies<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    limit: Option<usize>,
) -> Result<Vec<Agency>> {
    let file = archive.by_name("agency.txt")?;
    csv_reader(file)
        .deserialize::<AgencyRow>()
        .take(limit.unwrap_or(usize::MAX))
        .map(|row| {
            let row = row?;
            Ok(Agency {
                id: row
                    .agency_id
                    .clone()
                    .unwrap_or_else(|| row.agency_name.clone()),
                source_id: row.agency_id.unwrap_or_else(|| row.agency_name.clone()),
                name: row.agency_name,
                url: row.agency_url,
                timezone: row.agency_timezone,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct StopRow {
    stop_id: String,
    stop_name: String,
    stop_lat: Option<f64>,
    stop_lon: Option<f64>,
    platform_code: Option<String>,
    location_type: Option<i16>,
    parent_station: Option<String>,
    wheelchair_boarding: Option<i16>,
}

fn parse_stops<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    options: &ImportOptions,
    limit: Option<usize>,
    issues: &mut Vec<ValidationIssue>,
) -> Result<Vec<Stop>> {
    let file = archive.by_name("stops.txt")?;
    csv_reader(file)
        .deserialize::<StopRow>()
        .take(limit.unwrap_or(usize::MAX))
        .map(|row| {
            let row = row?;
            if row.stop_lat.is_none() || row.stop_lon.is_none() {
                issues.push(ValidationIssue {
                    severity: ValidationSeverity::Warning,
                    code: "stop_without_coordinates".to_string(),
                    message: "Stop has no usable coordinates".to_string(),
                    source_file: Some("stops.txt".to_string()),
                    affected_entity: Some(row.stop_id.clone()),
                    raw_payload: None,
                });
            }
            let location_type = match row.location_type.unwrap_or(0) {
                0 => StopLocationType::Stop,
                1 => StopLocationType::Station,
                2 => StopLocationType::EntranceExit,
                3 => StopLocationType::GenericNode,
                4 => StopLocationType::BoardingArea,
                value => {
                    issues.push(ValidationIssue {
                        severity: ValidationSeverity::Warning,
                        code: "invalid_stop_location_type".to_string(),
                        message: format!("Unsupported GTFS location_type {value}; using stop"),
                        source_file: Some("stops.txt".to_string()),
                        affected_entity: Some(row.stop_id.clone()),
                        raw_payload: Some(serde_json::json!({"location_type": value})),
                    });
                    StopLocationType::Stop
                }
            };
            let wheelchair_boarding = match row.wheelchair_boarding.unwrap_or(0) {
                0 => AccessibilityStatus::Unknown,
                1 => AccessibilityStatus::Accessible,
                2 => AccessibilityStatus::Inaccessible,
                value => {
                    issues.push(ValidationIssue {
                        severity: ValidationSeverity::Warning,
                        code: "invalid_wheelchair_boarding".to_string(),
                        message: format!(
                            "Unsupported GTFS wheelchair_boarding {value}; using unknown"
                        ),
                        source_file: Some("stops.txt".to_string()),
                        affected_entity: Some(row.stop_id.clone()),
                        raw_payload: Some(serde_json::json!({"wheelchair_boarding": value})),
                    });
                    AccessibilityStatus::Unknown
                }
            };
            Ok(Stop {
                id: row.stop_id.clone(),
                source_ids: vec![transit_model::SourceRef {
                    feed_id: options.source_feed_id.clone(),
                    original_id: row.stop_id,
                    import_run_id: None,
                    priority: options.source_priority,
                    confidence: None,
                    suppressed_as_duplicate: false,
                }],
                name: row.stop_name.clone(),
                normalized_name: normalize_czech_name(&row.stop_name),
                municipality: None,
                district: None,
                region: None,
                lat: row.stop_lat,
                lon: row.stop_lon,
                geom: row
                    .stop_lat
                    .zip(row.stop_lon)
                    .map(|(lat, lon)| geo_types::Point::new(lon, lat)),
                coordinate_confidence: if row.stop_lat.is_some() && row.stop_lon.is_some() {
                    transit_model::CoordinateConfidence::Exact
                } else {
                    transit_model::CoordinateConfidence::Unresolved
                },
                coordinate_source: Some(options.source_feed_id.clone()),
                stop_area_id: None,
                platform_code: row.platform_code,
                location_type,
                parent_station_id: row.parent_station.filter(|value| !value.trim().is_empty()),
                wheelchair_boarding,
                modes: Vec::new(),
                is_active: true,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct RouteRow {
    route_id: String,
    agency_id: Option<String>,
    route_short_name: Option<String>,
    route_long_name: Option<String>,
    route_type: Option<i32>,
    route_color: Option<String>,
    route_text_color: Option<String>,
}

fn parse_routes<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    options: &ImportOptions,
    limit: Option<usize>,
) -> Result<Vec<Route>> {
    let file = archive.by_name("routes.txt")?;
    csv_reader(file)
        .deserialize::<RouteRow>()
        .take(limit.unwrap_or(usize::MAX))
        .map(|row| {
            let row = row?;
            Ok(Route {
                id: row.route_id.clone(),
                source_id: row.route_id,
                agency_id: row.agency_id,
                operator_id: None,
                short_name: row.route_short_name,
                long_name: row.route_long_name,
                mode: map_gtfs_route_type(row.route_type),
                gtfs_route_type: row.route_type,
                color: row.route_color,
                text_color: row.route_text_color,
                source_priority: options.source_priority,
                is_active: true,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct TripRow {
    route_id: String,
    service_id: String,
    trip_id: String,
    trip_headsign: Option<String>,
}

fn parse_trips<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    limit: Option<usize>,
) -> Result<Vec<GtfsTrip>> {
    let file = archive.by_name("trips.txt")?;
    csv_reader(file)
        .deserialize::<TripRow>()
        .take(limit.unwrap_or(usize::MAX))
        .map(|row| {
            let row = row?;
            Ok(GtfsTrip {
                route_id: row.route_id,
                service_id: row.service_id,
                trip_id: row.trip_id,
                trip_headsign: row.trip_headsign,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct StopTimeRow {
    trip_id: String,
    arrival_time: String,
    departure_time: String,
    stop_id: String,
    stop_sequence: u32,
    pickup_type: Option<i16>,
    drop_off_type: Option<i16>,
    timepoint: Option<i16>,
}

fn parse_stop_times<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    limit: Option<usize>,
    issues: &mut Vec<ValidationIssue>,
) -> Result<Vec<StopTime>> {
    let file = archive.by_name("stop_times.txt")?;
    let mut stop_times = Vec::new();
    for row in csv_reader(file)
        .deserialize::<StopTimeRow>()
        .take(limit.unwrap_or(usize::MAX))
    {
        match row {
            Ok(row) => {
                let arrival_time = parse_gtfs_time(&row.arrival_time);
                let departure_time = parse_gtfs_time(&row.departure_time);
                match (arrival_time, departure_time) {
                    (Some(arrival_time), Some(departure_time)) => stop_times.push(StopTime {
                        trip_id: row.trip_id,
                        stop_id: row.stop_id,
                        stop_sequence: row.stop_sequence,
                        arrival_time,
                        departure_time,
                        pickup_type: row.pickup_type,
                        drop_off_type: row.drop_off_type,
                        timepoint: row.timepoint.map(|value| value == 1),
                        platform: None,
                        raw_notes: None,
                    }),
                    _ => issues.push(ValidationIssue {
                        severity: ValidationSeverity::Warning,
                        code: "malformed_stop_time".to_string(),
                        message: "Stop time has invalid arrival or departure time".to_string(),
                        source_file: Some("stop_times.txt".to_string()),
                        affected_entity: Some(row.trip_id),
                        raw_payload: None,
                    }),
                }
            }
            Err(error) => issues.push(ValidationIssue {
                severity: ValidationSeverity::Warning,
                code: "malformed_row".to_string(),
                message: error.to_string(),
                source_file: Some("stop_times.txt".to_string()),
                affected_entity: None,
                raw_payload: None,
            }),
        }
    }
    Ok(stop_times)
}

pub fn map_gtfs_route_type(route_type: Option<i32>) -> TransportMode {
    match route_type {
        Some(0) | Some(900..=999) => TransportMode::Tram,
        Some(1) => TransportMode::Metro,
        Some(2) | Some(100..=199) | Some(400..=499) => TransportMode::Train,
        Some(3) | Some(200..=299) | Some(700..=799) => TransportMode::Bus,
        Some(4) | Some(1000..=1099) => TransportMode::Ferry,
        Some(5) | Some(1300..=1399) => TransportMode::CableCar,
        Some(11) | Some(800..=899) => TransportMode::Trolleybus,
        _ => TransportMode::Unknown,
    }
}

fn populate_stop_modes(dataset: &mut GtfsDataset) {
    use std::collections::{HashMap, HashSet};

    let route_modes = dataset
        .routes
        .iter()
        .map(|route| (route.id.as_str(), route.mode.clone()))
        .collect::<HashMap<_, _>>();
    let trip_modes = dataset
        .trips
        .iter()
        .filter_map(|trip| {
            route_modes
                .get(trip.route_id.as_str())
                .cloned()
                .map(|mode| (trip.trip_id.as_str(), mode))
        })
        .collect::<HashMap<_, _>>();
    let mut stop_modes: HashMap<&str, HashSet<TransportMode>> = HashMap::new();
    for stop_time in &dataset.stop_times {
        if let Some(mode) = trip_modes.get(stop_time.trip_id.as_str()) {
            stop_modes
                .entry(stop_time.stop_id.as_str())
                .or_default()
                .insert(mode.clone());
        }
    }
    for stop in &mut dataset.stops {
        stop.modes = stop_modes
            .remove(stop.id.as_str())
            .unwrap_or_default()
            .into_iter()
            .collect();
        stop.modes.sort_by_key(transport_mode_rank);
    }
}

fn transport_mode_rank(mode: &TransportMode) -> u8 {
    match mode {
        TransportMode::Train => 0,
        TransportMode::Metro => 1,
        TransportMode::Tram => 2,
        TransportMode::Trolleybus => 3,
        TransportMode::Bus => 4,
        TransportMode::Ferry => 5,
        TransportMode::CableCar => 6,
        TransportMode::Unknown => 7,
    }
}

#[derive(Debug, Deserialize)]
struct CalendarRow {
    service_id: String,
    monday: i16,
    tuesday: i16,
    wednesday: i16,
    thursday: i16,
    friday: i16,
    saturday: i16,
    sunday: i16,
    start_date: String,
    end_date: String,
}

fn parse_calendars<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    limit: Option<usize>,
) -> Result<Vec<Calendar>> {
    let file = archive.by_name("calendar.txt")?;
    csv_reader(file)
        .deserialize::<CalendarRow>()
        .take(limit.unwrap_or(usize::MAX))
        .map(|row| {
            let row = row?;
            Ok(Calendar {
                service_id: row.service_id,
                monday: row.monday == 1,
                tuesday: row.tuesday == 1,
                wednesday: row.wednesday == 1,
                thursday: row.thursday == 1,
                friday: row.friday == 1,
                saturday: row.saturday == 1,
                sunday: row.sunday == 1,
                start_date: parse_gtfs_date(&row.start_date)?,
                end_date: parse_gtfs_date(&row.end_date)?,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct CalendarDateRow {
    service_id: String,
    date: String,
    exception_type: i16,
}

fn parse_calendar_dates<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    limit: Option<usize>,
) -> Result<Vec<CalendarDate>> {
    let file = archive.by_name("calendar_dates.txt")?;
    csv_reader(file)
        .deserialize::<CalendarDateRow>()
        .take(limit.unwrap_or(usize::MAX))
        .map(|row| {
            let row = row?;
            Ok(CalendarDate {
                service_id: row.service_id,
                date: parse_gtfs_date(&row.date)?,
                exception_type: row.exception_type,
            })
        })
        .collect()
}

fn parse_gtfs_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value.trim(), "%Y%m%d")
        .with_context(|| format!("invalid GTFS date {value}"))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;
    use zip::write::SimpleFileOptions;

    use super::*;

    #[test]
    fn parses_small_fixture() {
        let mut file = NamedTempFile::new().unwrap();
        {
            let mut zip = zip::ZipWriter::new(&mut file);
            let options = SimpleFileOptions::default();
            zip.start_file("agency.txt", options).unwrap();
            zip.write_all(b"agency_id,agency_name,agency_url,agency_timezone\npid,PID,https://pid.cz,Europe/Prague\n").unwrap();
            zip.start_file("stops.txt", options).unwrap();
            zip.write_all(b"stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station,wheelchair_boarding\ns1,Praha hl.n.,50.083,14.435,1,,1\ns2,Brno hl.n.,49.191,16.612,0,s1,2\n").unwrap();
            zip.start_file("routes.txt", options).unwrap();
            zip.write_all(b"route_id,agency_id,route_short_name,route_long_name,route_type\nr1,pid,R9,Praha - Brno,2\n").unwrap();
            zip.start_file("trips.txt", options).unwrap();
            zip.write_all(b"route_id,service_id,trip_id,trip_headsign\nr1,wd,t1,Brno\n")
                .unwrap();
            zip.start_file("stop_times.txt", options).unwrap();
            zip.write_all(b"trip_id,arrival_time,departure_time,stop_id,stop_sequence\nt1,08:00:00,08:00:00,s1,1\nt1,10:35:00,10:35:00,s2,2\n").unwrap();
            zip.start_file("calendar.txt", options).unwrap();
            zip.write_all(b"service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nwd,1,1,1,1,1,0,0,20260701,20260731\n").unwrap();
            zip.start_file("calendar_dates.txt", options).unwrap();
            zip.write_all(b"service_id,date,exception_type\nwd,20260706,2\n")
                .unwrap();
            zip.finish().unwrap();
        }

        let dataset = parse_gtfs_zip(
            file.path(),
            ImportOptions {
                source_feed_id: "fixture".to_string(),
                source_priority: 10,
                limit_rows: None,
            },
        )
        .unwrap();

        assert_eq!(dataset.agencies.len(), 1);
        assert_eq!(dataset.stops.len(), 2);
        assert_eq!(dataset.routes[0].mode, TransportMode::Train);
        assert_eq!(dataset.stops[0].modes, vec![TransportMode::Train]);
        assert_eq!(dataset.stops[0].location_type, StopLocationType::Station);
        assert_eq!(
            dataset.stops[0].wheelchair_boarding,
            AccessibilityStatus::Accessible
        );
        assert_eq!(dataset.stops[1].parent_station_id.as_deref(), Some("s1"));
        assert_eq!(dataset.stop_times.len(), 2);
        assert_eq!(dataset.calendars.len(), 1);
        assert!(dataset.calendars[0].monday);
        assert_eq!(dataset.calendar_dates.len(), 1);
        assert_eq!(dataset.calendar_dates[0].exception_type, 2);
    }

    #[test]
    fn reports_missing_optional_coordinates() {
        let mut file = NamedTempFile::new().unwrap();
        {
            let mut zip = zip::ZipWriter::new(&mut file);
            let options = SimpleFileOptions::default();
            zip.start_file("agency.txt", options).unwrap();
            zip.write_all(b"agency_name\nAgency\n").unwrap();
            zip.start_file("stops.txt", options).unwrap();
            zip.write_all(b"stop_id,stop_name\ns1,Stop\n").unwrap();
            zip.start_file("routes.txt", options).unwrap();
            zip.write_all(b"route_id,route_type\nr1,3\n").unwrap();
            zip.start_file("trips.txt", options).unwrap();
            zip.write_all(b"route_id,service_id,trip_id\nr1,wd,t1\n")
                .unwrap();
            zip.start_file("stop_times.txt", options).unwrap();
            zip.write_all(b"trip_id,arrival_time,departure_time,stop_id,stop_sequence\nt1,bad,08:00:00,s1,1\n").unwrap();
            zip.finish().unwrap();
        }

        let dataset = parse_gtfs_zip(
            file.path(),
            ImportOptions {
                source_feed_id: "fixture".to_string(),
                source_priority: 10,
                limit_rows: None,
            },
        )
        .unwrap();

        assert!(
            dataset
                .validation_issues
                .iter()
                .any(|issue| issue.code == "stop_without_coordinates")
        );
        assert!(
            dataset
                .validation_issues
                .iter()
                .any(|issue| issue.code == "malformed_stop_time")
        );
    }

    #[test]
    fn checksum_generation() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"cesta").unwrap();
        let checksum = sha256_file(file.path()).unwrap();
        assert_eq!(checksum.len(), 64);
    }

    #[test]
    fn maps_extended_gtfs_route_types() {
        assert_eq!(map_gtfs_route_type(Some(101)), TransportMode::Train);
        assert_eq!(map_gtfs_route_type(Some(401)), TransportMode::Train);
        assert_eq!(map_gtfs_route_type(Some(201)), TransportMode::Bus);
        assert_eq!(map_gtfs_route_type(Some(701)), TransportMode::Bus);
        assert_eq!(map_gtfs_route_type(Some(800)), TransportMode::Trolleybus);
        assert_eq!(map_gtfs_route_type(Some(900)), TransportMode::Tram);
        assert_eq!(map_gtfs_route_type(Some(1000)), TransportMode::Ferry);
        assert_eq!(map_gtfs_route_type(Some(1300)), TransportMode::CableCar);
    }
}
