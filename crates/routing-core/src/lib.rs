use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use transit_model::{Journey, JourneyLeg, RealtimeStatus, Transfer, TransportMode};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    pub trip_id: String,
    pub route_id: String,
    pub from_stop_id: String,
    pub to_stop_id: String,
    pub departure_time: u32,
    pub arrival_time: u32,
    pub mode: TransportMode,
    pub delay_seconds: Option<i32>,
}

#[derive(Debug, Clone, Default)]
pub struct RoutingSnapshot {
    pub connections: Vec<Connection>,
    pub transfers: Vec<Transfer>,
}

#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub from_stop_id: String,
    pub to_stop_id: String,
    pub departure_time: u32,
    pub max_transfers: u32,
    pub modes: Vec<TransportMode>,
}

#[derive(Debug, Clone)]
struct Label {
    arrival_time: u32,
    transfers: u32,
    legs: Vec<JourneyLeg>,
}

pub fn earliest_arrivals(snapshot: &RoutingSnapshot, request: SearchRequest) -> Vec<Journey> {
    let allowed_modes: HashSet<TransportMode> = request.modes.into_iter().collect();
    let mut connections = snapshot.connections.clone();
    connections.sort_by_key(|connection| connection.departure_time);

    let mut labels: HashMap<String, Label> = HashMap::new();
    labels.insert(
        request.from_stop_id.clone(),
        Label {
            arrival_time: request.departure_time,
            transfers: 0,
            legs: Vec::new(),
        },
    );

    relax_walking_transfers(&mut labels, &snapshot.transfers, request.max_transfers);

    for connection in connections {
        if !allowed_modes.is_empty() && !allowed_modes.contains(&connection.mode) {
            continue;
        }

        let Some(current) = labels.get(&connection.from_stop_id).cloned() else {
            continue;
        };

        if current.arrival_time > connection.departure_time {
            continue;
        }

        let transfers = if current.legs.is_empty() {
            0
        } else {
            current.transfers + 1
        };

        if transfers > request.max_transfers {
            continue;
        }

        let risk_warning = connection.delay_seconds.and_then(|delay| {
            (delay > 0).then(|| format!("delay_may_affect_connection:{delay}"))
        });
        let mut warnings = Vec::new();
        if let Some(warning) = risk_warning {
            warnings.push(warning);
        }

        let mut legs = current.legs.clone();
        legs.push(JourneyLeg {
            from_stop_id: connection.from_stop_id.clone(),
            to_stop_id: connection.to_stop_id.clone(),
            route_id: Some(connection.route_id.clone()),
            trip_id: Some(connection.trip_id.clone()),
            departure_time: connection.departure_time,
            arrival_time: connection.arrival_time,
            mode: connection.mode.clone(),
            warnings,
        });

        let better = labels
            .get(&connection.to_stop_id)
            .is_none_or(|known| connection.arrival_time < known.arrival_time);

        if better {
            labels.insert(
                connection.to_stop_id.clone(),
                Label {
                    arrival_time: connection.arrival_time,
                    transfers,
                    legs,
                },
            );
            relax_walking_transfers(&mut labels, &snapshot.transfers, request.max_transfers);
        }
    }

    let Some(best) = labels.get(&request.to_stop_id) else {
        return Vec::new();
    };

    let warnings = best
        .legs
        .iter()
        .flat_map(|leg| leg.warnings.iter())
        .count() as f32;

    vec![Journey {
        id: "journey-1".to_string(),
        legs: best.legs.clone(),
        departure_time: request.departure_time,
        arrival_time: best.arrival_time,
        duration_seconds: best.arrival_time.saturating_sub(request.departure_time),
        transfer_count: best.transfers,
        walking_distance_meters: 0,
        realtime_status: RealtimeStatus::Unavailable,
        risk_score: warnings.min(10.0),
        labels: vec!["nejrychlejsi".to_string()],
    }]
}

fn relax_walking_transfers(
    labels: &mut HashMap<String, Label>,
    transfers: &[Transfer],
    max_transfers: u32,
) {
    loop {
        let mut changed = false;
        for transfer in transfers {
            let Some(current) = labels.get(&transfer.from_stop_id).cloned() else {
                continue;
            };
            let arrival_time = current.arrival_time + transfer.min_transfer_seconds;
            let transfers_count = current.transfers + 1;
            if transfers_count > max_transfers {
                continue;
            }

            let mut legs = current.legs.clone();
            legs.push(JourneyLeg {
                from_stop_id: transfer.from_stop_id.clone(),
                to_stop_id: transfer.to_stop_id.clone(),
                route_id: None,
                trip_id: None,
                departure_time: current.arrival_time,
                arrival_time,
                mode: TransportMode::Unknown,
                warnings: vec!["walking_transfer".to_string()],
            });

            let better = labels
                .get(&transfer.to_stop_id)
                .is_none_or(|known| arrival_time < known.arrival_time);

            if better {
                labels.insert(
                    transfer.to_stop_id.clone(),
                    Label {
                        arrival_time,
                        transfers: transfers_count,
                        legs,
                    },
                );
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
}

pub fn fixture_snapshot() -> RoutingSnapshot {
    RoutingSnapshot {
        connections: vec![
            Connection {
                trip_id: "trip-rail-1".to_string(),
                route_id: "route-r9".to_string(),
                from_stop_id: "stop-praha-hl-n".to_string(),
                to_stop_id: "stop-brno-hl-n".to_string(),
                departure_time: 8 * 3600,
                arrival_time: 10 * 3600 + 35 * 60,
                mode: TransportMode::Train,
                delay_seconds: None,
            },
            Connection {
                trip_id: "trip-bus-1".to_string(),
                route_id: "route-300".to_string(),
                from_stop_id: "stop-praha-hl-n".to_string(),
                to_stop_id: "stop-jihlava".to_string(),
                departure_time: 9 * 3600,
                arrival_time: 10 * 3600 + 50 * 60,
                mode: TransportMode::Bus,
                delay_seconds: Some(180),
            },
            Connection {
                trip_id: "trip-bus-2".to_string(),
                route_id: "route-301".to_string(),
                from_stop_id: "stop-jihlava".to_string(),
                to_stop_id: "stop-brno-hl-n".to_string(),
                departure_time: 11 * 3600,
                arrival_time: 12 * 3600 + 15 * 60,
                mode: TransportMode::Bus,
                delay_seconds: None,
            },
        ],
        transfers: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use transit_model::CoordinateConfidence;

    use super::*;

    #[test]
    fn direct_trip() {
        let journeys = earliest_arrivals(
            &fixture_snapshot(),
            SearchRequest {
                from_stop_id: "stop-praha-hl-n".to_string(),
                to_stop_id: "stop-brno-hl-n".to_string(),
                departure_time: 7 * 3600,
                max_transfers: 4,
                modes: vec![TransportMode::Train],
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].transfer_count, 0);
    }

    #[test]
    fn one_transfer() {
        let journeys = earliest_arrivals(
            &fixture_snapshot(),
            SearchRequest {
                from_stop_id: "stop-praha-hl-n".to_string(),
                to_stop_id: "stop-brno-hl-n".to_string(),
                departure_time: 8 * 3600 + 45 * 60,
                max_transfers: 2,
                modes: vec![TransportMode::Bus],
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].transfer_count, 1);
    }

    #[test]
    fn no_connection() {
        let journeys = earliest_arrivals(
            &fixture_snapshot(),
            SearchRequest {
                from_stop_id: "stop-brno-hl-n".to_string(),
                to_stop_id: "stop-praha-hl-n".to_string(),
                departure_time: 7 * 3600,
                max_transfers: 2,
                modes: vec![TransportMode::Train],
            },
        );

        assert!(journeys.is_empty());
    }

    #[test]
    fn walking_transfer() {
        let snapshot = RoutingSnapshot {
            connections: vec![Connection {
                trip_id: "trip-1".to_string(),
                route_id: "route-1".to_string(),
                from_stop_id: "b".to_string(),
                to_stop_id: "c".to_string(),
                departure_time: 8 * 3600 + 15 * 60,
                arrival_time: 9 * 3600,
                mode: TransportMode::Bus,
                delay_seconds: None,
            }],
            transfers: vec![Transfer {
                from_stop_id: "a".to_string(),
                to_stop_id: "b".to_string(),
                min_transfer_seconds: 10 * 60,
                distance_meters: Some(600),
                walking_geometry: None,
                confidence: CoordinateConfidence::High,
                accessibility_level: None,
                source: "fixture".to_string(),
            }],
        };
        let journeys = earliest_arrivals(
            &snapshot,
            SearchRequest {
                from_stop_id: "a".to_string(),
                to_stop_id: "c".to_string(),
                departure_time: 8 * 3600,
                max_transfers: 2,
                modes: vec![TransportMode::Bus],
            },
        );

        assert_eq!(journeys.len(), 1);
    }

    #[test]
    fn max_transfers_exceeded() {
        let journeys = earliest_arrivals(
            &fixture_snapshot(),
            SearchRequest {
                from_stop_id: "stop-praha-hl-n".to_string(),
                to_stop_id: "stop-brno-hl-n".to_string(),
                departure_time: 8 * 3600 + 45 * 60,
                max_transfers: 0,
                modes: vec![TransportMode::Bus],
            },
        );

        assert!(journeys.is_empty());
    }

    #[test]
    fn delayed_connection_marked_risky() {
        let journeys = earliest_arrivals(
            &fixture_snapshot(),
            SearchRequest {
                from_stop_id: "stop-praha-hl-n".to_string(),
                to_stop_id: "stop-brno-hl-n".to_string(),
                departure_time: 8 * 3600 + 45 * 60,
                max_transfers: 2,
                modes: vec![TransportMode::Bus],
            },
        );

        assert!(journeys[0].risk_score > 0.0);
    }
}
