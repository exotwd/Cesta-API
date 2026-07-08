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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaptorStopTime {
    pub stop_id: String,
    pub arrival_time: u32,
    pub departure_time: u32,
    pub pickup_allowed: bool,
    pub drop_off_allowed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaptorTrip {
    pub trip_id: String,
    pub route_id: String,
    pub mode: TransportMode,
    pub stop_times: Vec<RaptorStopTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RaptorRoute {
    trips: Vec<RaptorTrip>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RaptorTimetable {
    routes: Vec<RaptorRoute>,
    stop_routes: HashMap<String, Vec<(usize, usize)>>,
    transfers_by_stop: HashMap<String, Vec<Transfer>>,
    trip_count: usize,
}

impl RaptorTimetable {
    pub fn new(trips: Vec<RaptorTrip>, transfers: Vec<Transfer>) -> Self {
        let trip_count = trips.len();
        let mut grouped = HashMap::<(TransportMode, Vec<String>), Vec<RaptorTrip>>::new();
        for trip in trips {
            let key = (
                trip.mode.clone(),
                trip.stop_times
                    .iter()
                    .map(|stop| stop.stop_id.clone())
                    .collect(),
            );
            grouped.entry(key).or_default().push(trip);
        }
        let mut routes = grouped
            .into_values()
            .map(|mut trips| {
                trips.sort_by_key(|trip| {
                    trip.stop_times
                        .first()
                        .map_or(u32::MAX, |stop| stop.departure_time)
                });
                RaptorRoute { trips }
            })
            .collect::<Vec<_>>();
        routes.sort_by(|left, right| {
            left.trips[0]
                .route_id
                .cmp(&right.trips[0].route_id)
                .then_with(|| left.trips[0].trip_id.cmp(&right.trips[0].trip_id))
        });

        let mut stop_routes = HashMap::<String, Vec<(usize, usize)>>::new();
        for (route_index, route) in routes.iter().enumerate() {
            for (stop_index, stop) in route.trips[0].stop_times.iter().enumerate() {
                stop_routes
                    .entry(stop.stop_id.clone())
                    .or_default()
                    .push((route_index, stop_index));
            }
        }
        let mut transfers_by_stop = HashMap::<String, Vec<Transfer>>::new();
        for transfer in transfers {
            transfers_by_stop
                .entry(transfer.from_stop_id.clone())
                .or_default()
                .push(transfer);
        }
        Self {
            routes,
            stop_routes,
            transfers_by_stop,
            trip_count,
        }
    }

    pub fn trip_count(&self) -> usize {
        self.trip_count
    }
}

#[derive(Debug, Clone)]
pub struct RaptorRequest {
    pub from_stop_ids: Vec<String>,
    pub to_stop_ids: Vec<String>,
    pub extra_transfers: Vec<Transfer>,
    pub departure_time: u32,
    pub max_transfers: u32,
    pub min_transfer_seconds: u32,
    pub modes: Vec<TransportMode>,
}

#[derive(Debug, Clone)]
enum RaptorParent {
    Ride {
        previous_stop: String,
        previous_round: usize,
        trip_id: String,
        route_id: String,
        mode: TransportMode,
        departure_time: u32,
        arrival_time: u32,
    },
    Walk {
        previous_stop: String,
        departure_time: u32,
        arrival_time: u32,
        distance_meters: Option<u32>,
    },
}

/// Round-based public-transit routing following Algorithm 1 in Delling et al.
/// Each round boards one additional trip; walking transfers stay in the same round.
pub fn raptor(timetable: &RaptorTimetable, request: RaptorRequest) -> Vec<Journey> {
    let allowed_modes = request.modes.into_iter().collect::<HashSet<_>>();
    let target_stops = request.to_stop_ids.iter().cloned().collect::<HashSet<_>>();
    let mut extra_transfers_by_stop = HashMap::<String, Vec<Transfer>>::new();
    for transfer in request.extra_transfers {
        extra_transfers_by_stop
            .entry(transfer.from_stop_id.clone())
            .or_default()
            .push(transfer);
    }
    let max_rounds = request.max_transfers as usize + 1;
    let mut best = HashMap::<String, u32>::new();
    let mut rounds = vec![HashMap::<String, u32>::new(); max_rounds + 1];
    let mut parents = HashMap::<(usize, String), RaptorParent>::new();
    let mut marked = HashSet::<String>::new();

    for stop in request.from_stop_ids {
        rounds[0].insert(stop.clone(), request.departure_time);
        best.insert(stop.clone(), request.departure_time);
        marked.insert(stop);
    }
    relax_raptor_transfers(
        0,
        &mut rounds,
        &mut best,
        &mut marked,
        &mut parents,
        &timetable.transfers_by_stop,
        &extra_transfers_by_stop,
        u32::MAX,
    );

    for round in 1..=max_rounds {
        if marked.is_empty() {
            break;
        }
        let previous_marked = std::mem::take(&mut marked);
        let mut routes_to_scan = HashMap::<usize, usize>::new();
        for stop in &previous_marked {
            for &(route_index, stop_index) in timetable.stop_routes.get(stop).into_iter().flatten()
            {
                routes_to_scan
                    .entry(route_index)
                    .and_modify(|earliest| *earliest = (*earliest).min(stop_index))
                    .or_insert(stop_index);
            }
        }
        let best_target = target_stops
            .iter()
            .filter_map(|stop| best.get(stop))
            .copied()
            .min()
            .unwrap_or(u32::MAX);

        for (route_index, start_index) in routes_to_scan {
            let trips = &timetable.routes[route_index].trips;
            if !allowed_modes.is_empty() && !allowed_modes.contains(&trips[0].mode) {
                continue;
            }
            let stops = &trips[0].stop_times;
            let mut current_trip: Option<(&RaptorTrip, usize)> = None;
            for index in start_index..stops.len() {
                let stop_id = &stops[index].stop_id;
                if let Some((trip, board_index)) = current_trip
                    && trip.stop_times[index].drop_off_allowed
                    && trip.stop_times[index].arrival_time < best_target
                    && trip.stop_times[index].arrival_time
                        < best.get(stop_id).copied().unwrap_or(u32::MAX)
                {
                    let stop_time = &trip.stop_times[index];
                    let boarded_at = &trip.stop_times[board_index];
                    rounds[round].insert(stop_id.clone(), stop_time.arrival_time);
                    best.insert(stop_id.clone(), stop_time.arrival_time);
                    marked.insert(stop_id.clone());
                    parents.insert(
                        (round, stop_id.clone()),
                        RaptorParent::Ride {
                            previous_stop: boarded_at.stop_id.clone(),
                            previous_round: round - 1,
                            trip_id: trip.trip_id.clone(),
                            route_id: trip.route_id.clone(),
                            mode: trip.mode.clone(),
                            departure_time: boarded_at.departure_time,
                            arrival_time: stop_time.arrival_time,
                        },
                    );
                }

                let Some(previous_arrival) = rounds[round - 1].get(stop_id).copied() else {
                    continue;
                };
                let transfer_slack = if round == 1
                    || matches!(
                        parents.get(&(round - 1, stop_id.clone())),
                        Some(RaptorParent::Walk { .. })
                    ) {
                    0
                } else {
                    request.min_transfer_seconds
                };
                let ready_time = previous_arrival.saturating_add(transfer_slack);
                let catchable = trips
                    .iter()
                    .filter(|trip| {
                        let stop = &trip.stop_times[index];
                        stop.pickup_allowed && ready_time <= stop.departure_time
                    })
                    .min_by_key(|trip| trip.stop_times[index].departure_time);
                if let Some(candidate) = catchable
                    && current_trip.is_none_or(|(current, _)| {
                        candidate.stop_times[index].departure_time
                            < current.stop_times[index].departure_time
                    })
                {
                    current_trip = Some((candidate, index));
                }
            }
        }

        let best_target = target_stops
            .iter()
            .filter_map(|stop| best.get(stop))
            .copied()
            .min()
            .unwrap_or(u32::MAX);
        relax_raptor_transfers(
            round,
            &mut rounds,
            &mut best,
            &mut marked,
            &mut parents,
            &timetable.transfers_by_stop,
            &extra_transfers_by_stop,
            best_target,
        );
    }

    let mut journeys = Vec::new();
    for round in 1..=max_rounds {
        let Some((target, arrival_time)) = target_stops
            .iter()
            .filter_map(|stop| rounds[round].get(stop).map(|time| (stop, *time)))
            .min_by_key(|(_, time)| *time)
        else {
            continue;
        };
        let Some(legs) = reconstruct_raptor_journey(round, target, &parents) else {
            continue;
        };
        let departure_time = legs
            .first()
            .map_or(request.departure_time, |leg| leg.departure_time);
        let walking_distance_meters = journey_walking_distance_meters(&legs);
        journeys.push(Journey {
            id: String::new(),
            legs,
            departure_time,
            arrival_time,
            duration_seconds: arrival_time.saturating_sub(departure_time),
            transfer_count: round.saturating_sub(1) as u32,
            walking_distance_meters,
            realtime_status: RealtimeStatus::Unavailable,
            risk_score: 0.0,
            labels: Vec::new(),
        });
    }
    journeys.sort_by_key(|journey| (journey.arrival_time, journey.transfer_count));
    journeys
}

#[allow(clippy::too_many_arguments)]
fn relax_raptor_transfers(
    round: usize,
    rounds: &mut [HashMap<String, u32>],
    best: &mut HashMap<String, u32>,
    marked: &mut HashSet<String>,
    parents: &mut HashMap<(usize, String), RaptorParent>,
    transfers_by_stop: &HashMap<String, Vec<Transfer>>,
    extra_transfers_by_stop: &HashMap<String, Vec<Transfer>>,
    best_target: u32,
) {
    let mut queue = marked.iter().cloned().collect::<Vec<_>>();
    while let Some(from) = queue.pop() {
        let Some(departure_time) = rounds[round].get(&from).copied() else {
            continue;
        };
        for transfer in transfers_by_stop
            .get(&from)
            .into_iter()
            .flatten()
            .chain(extra_transfers_by_stop.get(&from).into_iter().flatten())
        {
            let arrival_time = departure_time.saturating_add(transfer.min_transfer_seconds);
            if arrival_time >= best_target
                || arrival_time >= best.get(&transfer.to_stop_id).copied().unwrap_or(u32::MAX)
            {
                continue;
            }
            rounds[round].insert(transfer.to_stop_id.clone(), arrival_time);
            best.insert(transfer.to_stop_id.clone(), arrival_time);
            marked.insert(transfer.to_stop_id.clone());
            queue.push(transfer.to_stop_id.clone());
            parents.insert(
                (round, transfer.to_stop_id.clone()),
                RaptorParent::Walk {
                    previous_stop: from.clone(),
                    departure_time,
                    arrival_time,
                    distance_meters: transfer.distance_meters,
                },
            );
        }
    }
}

fn reconstruct_raptor_journey(
    mut round: usize,
    target: &str,
    parents: &HashMap<(usize, String), RaptorParent>,
) -> Option<Vec<JourneyLeg>> {
    let mut stop = target.to_string();
    let mut legs = Vec::new();
    while let Some(parent) = parents.get(&(round, stop.clone())) {
        match parent {
            RaptorParent::Ride {
                previous_stop,
                previous_round,
                trip_id,
                route_id,
                mode,
                departure_time,
                arrival_time,
            } => {
                legs.push(JourneyLeg {
                    from_stop_id: previous_stop.clone(),
                    to_stop_id: stop,
                    route_id: Some(route_id.clone()),
                    trip_id: Some(trip_id.clone()),
                    departure_time: *departure_time,
                    arrival_time: *arrival_time,
                    mode: mode.clone(),
                    warnings: Vec::new(),
                });
                stop = previous_stop.clone();
                round = *previous_round;
            }
            RaptorParent::Walk {
                previous_stop,
                departure_time,
                arrival_time,
                distance_meters,
            } => {
                legs.push(JourneyLeg {
                    from_stop_id: previous_stop.clone(),
                    to_stop_id: stop,
                    route_id: None,
                    trip_id: None,
                    departure_time: *departure_time,
                    arrival_time: *arrival_time,
                    mode: TransportMode::Unknown,
                    warnings: vec![format!("walking_transfer:{}", distance_meters.unwrap_or(0))],
                });
                stop = previous_stop.clone();
            }
        }
    }
    (!legs.is_empty()).then(|| {
        legs.reverse();
        legs
    })
}

fn journey_walking_distance_meters(legs: &[JourneyLeg]) -> u32 {
    legs.iter()
        .filter(|leg| leg.route_id.is_none() && leg.trip_id.is_none())
        .flat_map(|leg| leg.warnings.iter())
        .filter_map(|warning| warning.strip_prefix("walking_transfer:"))
        .filter_map(|distance| distance.parse::<u32>().ok())
        .sum()
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

        let risk_warning = connection
            .delay_seconds
            .and_then(|delay| (delay > 0).then(|| format!("delay_may_affect_connection:{delay}")));
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

    let warnings = best.legs.iter().flat_map(|leg| leg.warnings.iter()).count() as f32;

    let departure_time = best
        .legs
        .first()
        .map(|leg| leg.departure_time)
        .unwrap_or(request.departure_time);

    vec![Journey {
        id: "journey-1".to_string(),
        legs: best.legs.clone(),
        departure_time,
        arrival_time: best.arrival_time,
        duration_seconds: best.arrival_time.saturating_sub(departure_time),
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
    fn uses_next_viable_departure_after_requested_time() {
        let journeys = earliest_arrivals(
            &fixture_snapshot(),
            SearchRequest {
                from_stop_id: "stop-praha-hl-n".to_string(),
                to_stop_id: "stop-brno-hl-n".to_string(),
                departure_time: 8 * 3600 + 1,
                max_transfers: 2,
                modes: vec![TransportMode::Bus],
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].departure_time, 9 * 3600);
        assert_eq!(journeys[0].legs[0].departure_time, 9 * 3600);
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

    #[test]
    fn raptor_returns_pareto_journeys_by_transfer_round() {
        let stop_time = |stop: &str, arrival, departure| RaptorStopTime {
            stop_id: stop.to_string(),
            arrival_time: arrival,
            departure_time: departure,
            pickup_allowed: true,
            drop_off_allowed: true,
        };
        let timetable = RaptorTimetable::new(
            vec![
                RaptorTrip {
                    trip_id: "direct".into(),
                    route_id: "r-direct".into(),
                    mode: TransportMode::Train,
                    stop_times: vec![
                        stop_time("a", 8 * 3600, 8 * 3600),
                        stop_time("c", 10 * 3600, 10 * 3600),
                    ],
                },
                RaptorTrip {
                    trip_id: "first".into(),
                    route_id: "r-first".into(),
                    mode: TransportMode::Train,
                    stop_times: vec![
                        stop_time("a", 8 * 3600 + 60, 8 * 3600 + 60),
                        stop_time("b", 9 * 3600, 9 * 3600),
                    ],
                },
                RaptorTrip {
                    trip_id: "second".into(),
                    route_id: "r-second".into(),
                    mode: TransportMode::Train,
                    stop_times: vec![
                        stop_time("b", 9 * 3600 + 300, 9 * 3600 + 300),
                        stop_time("c", 9 * 3600 + 1800, 9 * 3600 + 1800),
                    ],
                },
            ],
            Vec::new(),
        );
        let journeys = raptor(
            &timetable,
            RaptorRequest {
                from_stop_ids: vec!["a".into()],
                to_stop_ids: vec!["c".into()],
                extra_transfers: Vec::new(),
                departure_time: 8 * 3600,
                max_transfers: 2,
                min_transfer_seconds: 5 * 60,
                modes: vec![TransportMode::Train],
            },
        );

        assert_eq!(journeys.len(), 2);
        assert_eq!(journeys[0].transfer_count, 1);
        assert_eq!(journeys[0].arrival_time, 9 * 3600 + 1800);
        assert_eq!(journeys[1].transfer_count, 0);
    }

    #[test]
    fn raptor_can_start_from_nearby_stop_with_request_transfer() {
        let stop_time = |stop: &str, arrival, departure| RaptorStopTime {
            stop_id: stop.to_string(),
            arrival_time: arrival,
            departure_time: departure,
            pickup_allowed: true,
            drop_off_allowed: true,
        };
        let timetable = RaptorTimetable::new(
            vec![RaptorTrip {
                trip_id: "nearby-trip".into(),
                route_id: "nearby-route".into(),
                mode: TransportMode::Bus,
                stop_times: vec![
                    stop_time("nearby", 8 * 3600 + 180, 8 * 3600 + 180),
                    stop_time("target", 8 * 3600 + 1800, 8 * 3600 + 1800),
                ],
            }],
            Vec::new(),
        );

        let journeys = raptor(
            &timetable,
            RaptorRequest {
                from_stop_ids: vec!["selected".into()],
                to_stop_ids: vec!["target".into()],
                extra_transfers: vec![Transfer {
                    from_stop_id: "selected".into(),
                    to_stop_id: "nearby".into(),
                    min_transfer_seconds: 120,
                    distance_meters: Some(150),
                    walking_geometry: None,
                    confidence: CoordinateConfidence::Medium,
                    accessibility_level: None,
                    source: "test".into(),
                }],
                departure_time: 8 * 3600,
                max_transfers: 1,
                min_transfer_seconds: 5 * 60,
                modes: vec![TransportMode::Bus],
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs[0].from_stop_id, "selected");
        assert_eq!(journeys[0].legs[0].to_stop_id, "nearby");
        assert_eq!(journeys[0].walking_distance_meters, 150);
    }
}
