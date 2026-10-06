use crate::{GtfsDataset, GtfsTransfer};
use std::collections::HashMap;
use tracing::debug;

use super::models::StopAreaNode;

pub struct GraphPatcher;

impl GraphPatcher {
    pub fn apply_hierarchy(dataset: &mut GtfsDataset, stop_areas: &[StopAreaNode]) {
        let mut cis_to_node: HashMap<String, String> = HashMap::new();

        for area in stop_areas {
            for child in &area.child_platforms {
                if let Some(cis) = &child.cis_id {
                    // Cis ID represents the stop in our graph
                    cis_to_node.insert(cis.clone(), area.id.clone());
                }
            }
        }

        let mut mutated_stops = 0;
        let mut active_nodes: HashMap<String, Vec<String>> = HashMap::new();

        // 1. Mutate stops
        for stop in &mut dataset.stops {
            if let Some(node_id) = cis_to_node.get(&stop.id) {
                stop.parent_station_id = Some(node_id.clone());
                stop.stop_area_id = Some(node_id.clone());
                active_nodes
                    .entry(node_id.clone())
                    .or_default()
                    .push(stop.id.clone());
                mutated_stops += 1;
            } else {
                debug!("Stop {} has no matched CIS ID, skipping parent mutation", stop.id);
            }
        }

        debug!("Mutated {} stops with parent node IDs", mutated_stops);

        // 2. Generate transfers
        let mut generated_transfers = 0;
        for (_, child_ids) in active_nodes {
            let count = child_ids.len();
            if count > 1 {
                for from_id in &child_ids {
                    for to_id in &child_ids {
                        if from_id != to_id {
                            // Check if transfer already exists to maintain idempotency
                            let exists = dataset.transfers.iter().any(|t| {
                                t.from_stop_id == *from_id && t.to_stop_id == *to_id
                            });

                            if !exists {
                                dataset.transfers.push(GtfsTransfer {
                                    from_stop_id: from_id.clone(),
                                    to_stop_id: to_id.clone(),
                                    transfer_type: 2, // minimum time transfer
                                    min_transfer_time: Some(120),
                                    from_trip_id: None,
                                    to_trip_id: None,
                                    max_waiting_time: None,
                                });
                                generated_transfers += 1;
                            }
                        }
                    }
                }
            }
        }

        debug!("Generated {} missing internal transfers", generated_transfers);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GtfsDataset, GtfsTransfer};
    use geo_types::Point;
    use transit_model::{AccessibilityStatus, CoordinateConfidence, Stop, StopLocationType};

    fn make_dummy_stop(id: &str) -> Stop {
        Stop {
            id: id.to_string(),
            source_ids: vec![],
            name: "Test".to_string(),
            normalized_name: "test".to_string(),
            municipality: None,
            district: None,
            region: None,
            lat: Some(0.0),
            lon: Some(0.0),
            geom: None,
            coordinate_confidence: CoordinateConfidence::Exact,
            coordinate_source: None,
            stop_area_id: None,
            platform_code: None,
            location_type: StopLocationType::Stop,
            parent_station_id: None,
            station_id: None,
            complex_id: None,
            has_station_layout: false,
            station_layout_version: None,
            wheelchair_boarding: AccessibilityStatus::Unknown,
            modes: vec![],
            is_active: true,
        }
    }

    #[test]
    fn test_apply_hierarchy_safe_orphans() {
        let mut dataset = GtfsDataset {
            agencies: vec![],
            stops: vec![make_dummy_stop("orphan_stop"), make_dummy_stop("cis_stop")],
            routes: vec![],
            trips: vec![],
            stop_times: vec![],
            shapes: vec![],
            calendars: vec![],
            calendar_dates: vec![],
            transfers: vec![],
            validation_issues: vec![],
        };

        let areas = vec![StopAreaNode {
            id: "node_1".to_string(),
            name: "Node 1".to_string(),
            centroid: Point::new(0.0, 0.0),
            child_platforms: vec![super::super::models::ParsedStopRecord {
                node_id: "node_1".to_string(),
                stop_id: "1".to_string(),
                name: "Node 1".to_string(),
                platform_code: None,
                cis_id: Some("cis_stop".to_string()),
                lat: 0.0,
                lon: 0.0,
            }],
        }];

        GraphPatcher::apply_hierarchy(&mut dataset, &areas);

        assert_eq!(dataset.stops[0].parent_station_id, None);
        assert_eq!(dataset.stops[1].parent_station_id, Some("node_1".to_string()));
    }

    #[test]
    fn test_apply_hierarchy_generates_transfers() {
        let mut dataset = GtfsDataset {
            agencies: vec![],
            stops: vec![
                make_dummy_stop("stop_1"),
                make_dummy_stop("stop_2"),
                make_dummy_stop("stop_3"),
            ],
            routes: vec![],
            trips: vec![],
            stop_times: vec![],
            shapes: vec![],
            calendars: vec![],
            calendar_dates: vec![],
            transfers: vec![],
            validation_issues: vec![],
        };

        let areas = vec![StopAreaNode {
            id: "node_group".to_string(),
            name: "Group".to_string(),
            centroid: Point::new(0.0, 0.0),
            child_platforms: vec![
                super::super::models::ParsedStopRecord {
                    node_id: "node_group".to_string(),
                    stop_id: "1".to_string(),
                    name: "1".to_string(),
                    platform_code: None,
                    cis_id: Some("stop_1".to_string()),
                    lat: 0.0,
                    lon: 0.0,
                },
                super::super::models::ParsedStopRecord {
                    node_id: "node_group".to_string(),
                    stop_id: "2".to_string(),
                    name: "2".to_string(),
                    platform_code: None,
                    cis_id: Some("stop_2".to_string()),
                    lat: 0.0,
                    lon: 0.0,
                },
                super::super::models::ParsedStopRecord {
                    node_id: "node_group".to_string(),
                    stop_id: "3".to_string(),
                    name: "3".to_string(),
                    platform_code: None,
                    cis_id: Some("stop_3".to_string()),
                    lat: 0.0,
                    lon: 0.0,
                },
            ],
        }];

        GraphPatcher::apply_hierarchy(&mut dataset, &areas);

        // 3 stops -> N * (N - 1) = 6 transfers
        assert_eq!(dataset.transfers.len(), 6);
        assert!(dataset
            .transfers
            .iter()
            .all(|t| t.min_transfer_time == Some(120)));
    }
}
