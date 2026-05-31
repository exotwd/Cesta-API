use chrono::{NaiveDate, Utc};
use transit_model::OfflinePackage;

pub fn development_packages() -> Vec<OfflinePackage> {
    vec![
        OfflinePackage {
            id: "cz-all".to_string(),
            name_cs: "Cela Ceska republika".to_string(),
            version: Utc::now().format("%Y%m%d-dev").to_string(),
            checksum: None,
            valid_from: NaiveDate::from_ymd_opt(2026, 1, 1),
            valid_until: NaiveDate::from_ymd_opt(2026, 12, 31),
            size_bytes: None,
            mock: true,
        },
        OfflinePackage {
            id: "pid".to_string(),
            name_cs: "Praha a Stredni Cechy".to_string(),
            version: Utc::now().format("%Y%m%d-dev").to_string(),
            checksum: None,
            valid_from: NaiveDate::from_ymd_opt(2026, 1, 1),
            valid_until: NaiveDate::from_ymd_opt(2026, 12, 31),
            size_bytes: None,
            mock: true,
        },
    ]
}

pub fn package_manifest(package: &OfflinePackage) -> serde_json::Value {
    serde_json::json!({
        "id": package.id,
        "name_cs": package.name_cs,
        "version": package.version,
        "checksum": package.checksum,
        "valid_from": package.valid_from,
        "valid_until": package.valid_until,
        "mock": package.mock,
        "contents": [
            "metadata.json",
            "stops",
            "stop_areas",
            "routes",
            "trips",
            "stop_times",
            "calendars",
            "transfers",
            "search_index",
            "validation_summary"
        ]
    })
}

