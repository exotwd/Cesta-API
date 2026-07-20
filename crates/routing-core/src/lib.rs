use std::collections::{BTreeSet, HashMap, HashSet};

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
    pub service_verified: bool,
    pub stop_times: Vec<RaptorStopTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RaptorRoute {
    trips: Vec<RaptorTrip>,
    stop_indices: Vec<usize>,
    departures_by_stop_index: Vec<Vec<usize>>,
    verified_departures_by_stop_index: Vec<Vec<usize>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RaptorTimetable {
    stops: Vec<String>,
    stop_indices: HashMap<String, usize>,
    routes: Vec<RaptorRoute>,
    stop_routes: Vec<Vec<(usize, usize)>>,
    transfers_by_stop: Vec<Vec<RaptorTransfer>>,
    trip_count: usize,
    has_unverified_services: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RaptorTransfer {
    from_stop_index: usize,
    to_stop_index: usize,
    min_transfer_seconds: u32,
    distance_meters: Option<u32>,
}

impl RaptorTimetable {
    pub fn new(trips: Vec<RaptorTrip>, transfers: Vec<Transfer>) -> Self {
        let trip_count = trips.len();
        let has_unverified_services = trips.iter().any(|trip| !trip.service_verified);
        let mut stop_indices = HashMap::<String, usize>::new();
        let mut stops = Vec::<String>::new();
        for stop_id in
            trips
                .iter()
                .flat_map(|trip| trip.stop_times.iter().map(|stop_time| &stop_time.stop_id))
                .chain(transfers.iter().flat_map(|transfer| {
                    [&transfer.from_stop_id, &transfer.to_stop_id].into_iter()
                }))
        {
            if !stop_indices.contains_key(stop_id) {
                stop_indices.insert(stop_id.clone(), stops.len());
                stops.push(stop_id.clone());
            }
        }

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
                let departures_by_stop_index = route_departure_indices_by_stop_index(&trips, false);
                let verified_departures_by_stop_index =
                    route_departure_indices_by_stop_index(&trips, true);
                let stop_indices_for_route = trips
                    .first()
                    .into_iter()
                    .flat_map(|trip| trip.stop_times.iter())
                    .map(|stop_time| stop_indices[&stop_time.stop_id])
                    .collect();
                RaptorRoute {
                    trips,
                    stop_indices: stop_indices_for_route,
                    departures_by_stop_index,
                    verified_departures_by_stop_index,
                }
            })
            .collect::<Vec<_>>();
        routes.sort_by(|left, right| {
            left.trips[0]
                .route_id
                .cmp(&right.trips[0].route_id)
                .then_with(|| left.trips[0].trip_id.cmp(&right.trips[0].trip_id))
        });

        let mut stop_routes = vec![Vec::<(usize, usize)>::new(); stops.len()];
        for (route_index, route) in routes.iter().enumerate() {
            for (stop_index, stop) in route.stop_indices.iter().copied().enumerate() {
                stop_routes[stop].push((route_index, stop_index));
            }
        }
        let mut transfers_by_stop = vec![Vec::<RaptorTransfer>::new(); stops.len()];
        for transfer in transfers {
            let Some(&from_stop_index) = stop_indices.get(&transfer.from_stop_id) else {
                continue;
            };
            let Some(&to_stop_index) = stop_indices.get(&transfer.to_stop_id) else {
                continue;
            };
            transfers_by_stop[from_stop_index].push(RaptorTransfer {
                from_stop_index,
                to_stop_index,
                min_transfer_seconds: transfer.min_transfer_seconds,
                distance_meters: transfer.distance_meters,
            });
        }
        Self {
            stops,
            stop_indices,
            routes,
            stop_routes,
            transfers_by_stop,
            trip_count,
            has_unverified_services,
        }
    }

    pub fn trip_count(&self) -> usize {
        self.trip_count
    }

    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    pub fn max_route_trip_count(&self) -> usize {
        self.routes
            .iter()
            .map(|route| route.trips.len())
            .max()
            .unwrap_or(0)
    }

    pub fn has_unverified_services(&self) -> bool {
        self.has_unverified_services
    }

    pub fn departure_times_from_stops(
        &self,
        stop_ids: &[String],
        departure_time: u32,
        window_seconds: u32,
        max_departures: usize,
        modes: &[TransportMode],
        allow_unverified_services: bool,
    ) -> Vec<u32> {
        let max_departures = max_departures.max(1);
        let mut selected = BTreeSet::from([departure_time]);
        if window_seconds == 0 || max_departures == 1 {
            return selected.into_iter().collect();
        }

        let allowed_modes = modes.iter().cloned().collect::<HashSet<_>>();
        let latest_departure = departure_time.saturating_add(window_seconds);
        let coverage_probe_count = max_departures.saturating_sub(1).min(11);
        for probe_index in 1..=coverage_probe_count {
            let offset = ((window_seconds as u64 * probe_index as u64)
                / (coverage_probe_count as u64 + 1)) as u32;
            selected.insert(departure_time.saturating_add(offset));
        }

        let mut route_departures = Vec::<Vec<u32>>::new();
        for stop_index in stop_ids
            .iter()
            .filter_map(|stop_id| self.stop_indices.get(stop_id).copied())
        {
            for &(route_index, route_stop_index) in
                self.stop_routes.get(stop_index).into_iter().flatten()
            {
                let route = &self.routes[route_index];
                let trips = &route.trips;
                if !allowed_modes.is_empty() && !allowed_modes.contains(&trips[0].mode) {
                    continue;
                }
                let departure_indices = if allow_unverified_services {
                    &route.departures_by_stop_index
                } else {
                    &route.verified_departures_by_stop_index
                };
                let Some(indices) = departure_indices.get(route_stop_index) else {
                    continue;
                };
                let first_candidate = indices.partition_point(|trip_index| {
                    trips[*trip_index].stop_times[route_stop_index].departure_time < departure_time
                });
                for trip_index in &indices[first_candidate..] {
                    let stop_time = &trips[*trip_index].stop_times[route_stop_index];
                    if stop_time.departure_time > latest_departure {
                        break;
                    }
                    if stop_time.pickup_allowed {
                        route_departures.push(vec![stop_time.departure_time]);
                        break;
                    }
                }
            }
        }

        route_departures.sort_by_key(|departures| departures[0]);
        for departures in route_departures {
            if selected.len() >= max_departures {
                break;
            }
            selected.extend(departures);
        }

        selected.into_iter().take(max_departures).collect()
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
    pub allow_unverified_services: bool,
}

#[derive(Debug, Clone, Default)]
pub struct RaptorSearchStats {
    pub rounds: usize,
    pub routes_scanned: usize,
    pub marked_stops: usize,
}

#[derive(Debug, Clone)]
pub struct RaptorSearchOutput {
    pub journeys: Vec<Journey>,
    pub stats: RaptorSearchStats,
}

#[derive(Debug, Clone)]
enum RaptorParent {
    Ride {
        previous_stop: usize,
        previous_round: usize,
        trip_id: String,
        route_id: String,
        mode: TransportMode,
        departure_time: u32,
        arrival_time: u32,
    },
    Walk {
        previous_stop: usize,
        departure_time: u32,
        arrival_time: u32,
        distance_meters: Option<u32>,
    },
}

/// Round-based public-transit routing following Algorithm 1 in Delling et al.
/// Each round boards one additional trip; walking transfers stay in the same round.
pub fn raptor(timetable: &RaptorTimetable, request: RaptorRequest) -> Vec<Journey> {
    raptor_with_stats(timetable, request).journeys
}

/// Round-based public-transit routing with lightweight scan counters for diagnostics.
pub fn raptor_with_stats(
    timetable: &RaptorTimetable,
    request: RaptorRequest,
) -> RaptorSearchOutput {
    let allow_unverified_services = request.allow_unverified_services;
    let allowed_modes = request.modes.into_iter().collect::<HashSet<_>>();
    let mut stats = RaptorSearchStats::default();
    let mut request_stop_ids = Vec::<String>::new();
    let mut request_stop_indices = HashMap::<String, usize>::new();
    let resolve_stop_index =
        |stop_id: &str,
         request_stop_ids: &mut Vec<String>,
         request_stop_indices: &mut HashMap<String, usize>| {
            if let Some(index) = timetable.stop_indices.get(stop_id).copied() {
                return index;
            }
            if let Some(index) = request_stop_indices.get(stop_id).copied() {
                return index;
            }
            let index = timetable.stops.len() + request_stop_ids.len();
            request_stop_indices.insert(stop_id.to_string(), index);
            request_stop_ids.push(stop_id.to_string());
            index
        };
    let from_stop_indices = request
        .from_stop_ids
        .iter()
        .map(|stop_id| {
            resolve_stop_index(stop_id, &mut request_stop_ids, &mut request_stop_indices)
        })
        .collect::<Vec<_>>();
    let mut target_stops = request
        .to_stop_ids
        .iter()
        .map(|stop_id| {
            resolve_stop_index(stop_id, &mut request_stop_ids, &mut request_stop_indices)
        })
        .collect::<Vec<_>>();
    target_stops.sort_unstable();
    target_stops.dedup();
    let mut extra_transfer_pairs = Vec::<RaptorTransfer>::new();
    for transfer in request.extra_transfers {
        let from_stop_index = resolve_stop_index(
            &transfer.from_stop_id,
            &mut request_stop_ids,
            &mut request_stop_indices,
        );
        let to_stop_index = resolve_stop_index(
            &transfer.to_stop_id,
            &mut request_stop_ids,
            &mut request_stop_indices,
        );
        extra_transfer_pairs.push(RaptorTransfer {
            from_stop_index,
            to_stop_index,
            min_transfer_seconds: transfer.min_transfer_seconds,
            distance_meters: transfer.distance_meters,
        });
    }
    let stop_count = timetable.stops.len() + request_stop_ids.len();
    // Endpoint walking links are sparse. Avoid a country-sized allocation for every
    // range probe by indexing only stops that have request-specific transfers.
    let mut extra_transfers_by_stop = HashMap::<usize, Vec<RaptorTransfer>>::new();
    for transfer in extra_transfer_pairs {
        extra_transfers_by_stop
            .entry(transfer.from_stop_index)
            .or_default()
            .push(transfer);
    }
    let max_rounds = request.max_transfers as usize + 1;
    let mut best = vec![u32::MAX; stop_count];
    let mut rounds = vec![vec![u32::MAX; stop_count]; max_rounds + 1];
    let mut parents = (0..=max_rounds)
        .map(|_| HashMap::<usize, RaptorParent>::new())
        .collect::<Vec<_>>();
    let mut marked = Vec::<usize>::new();
    let mut marked_flags = vec![false; stop_count];
    let mut queued_route_positions = vec![usize::MAX; timetable.routes.len()];

    for stop in from_stop_indices {
        rounds[0][stop] = request.departure_time;
        best[stop] = request.departure_time;
        mark_raptor_stop(stop, &mut marked, &mut marked_flags);
    }
    relax_raptor_transfers(
        0,
        &mut rounds,
        &mut best,
        &mut marked,
        &mut marked_flags,
        &mut parents,
        &timetable.transfers_by_stop,
        &extra_transfers_by_stop,
        u32::MAX,
    );

    for round in 1..=max_rounds {
        if marked.is_empty() {
            break;
        }
        stats.rounds += 1;
        stats.marked_stops += marked.len();
        let previous_marked = std::mem::take(&mut marked);
        for stop in &previous_marked {
            marked_flags[*stop] = false;
        }
        let mut routes_to_scan = Vec::<(usize, usize)>::new();
        for stop in previous_marked {
            for &(route_index, stop_index) in timetable.stop_routes.get(stop).into_iter().flatten()
            {
                let position = queued_route_positions[route_index];
                if position == usize::MAX {
                    queued_route_positions[route_index] = routes_to_scan.len();
                    routes_to_scan.push((route_index, stop_index));
                } else if stop_index < routes_to_scan[position].1 {
                    routes_to_scan[position].1 = stop_index;
                }
            }
        }
        for &(route_index, _) in &routes_to_scan {
            queued_route_positions[route_index] = usize::MAX;
        }
        let best_target = target_stops
            .iter()
            .map(|stop| best[*stop])
            .min()
            .unwrap_or(u32::MAX);

        for (route_index, start_index) in routes_to_scan {
            stats.routes_scanned += 1;
            let trips = &timetable.routes[route_index].trips;
            if !allowed_modes.is_empty() && !allowed_modes.contains(&trips[0].mode) {
                continue;
            }
            let departure_indices = if allow_unverified_services {
                &timetable.routes[route_index].departures_by_stop_index
            } else {
                &timetable.routes[route_index].verified_departures_by_stop_index
            };
            let stops = &trips[0].stop_times;
            let mut current_trip: Option<(&RaptorTrip, usize)> = None;
            for index in start_index..stops.len() {
                let stop_index = timetable.routes[route_index].stop_indices[index];
                if let Some((trip, board_index)) = current_trip
                    && trip.stop_times[index].drop_off_allowed
                    && trip.stop_times[index].arrival_time < best_target
                    && trip.stop_times[index].arrival_time < best[stop_index]
                {
                    let stop_time = &trip.stop_times[index];
                    let boarded_at = &trip.stop_times[board_index];
                    let boarded_stop = timetable.routes[route_index].stop_indices[board_index];
                    rounds[round][stop_index] = stop_time.arrival_time;
                    best[stop_index] = stop_time.arrival_time;
                    mark_raptor_stop(stop_index, &mut marked, &mut marked_flags);
                    parents[round].insert(
                        stop_index,
                        RaptorParent::Ride {
                            previous_stop: boarded_stop,
                            previous_round: round - 1,
                            trip_id: trip.trip_id.clone(),
                            route_id: trip.route_id.clone(),
                            mode: trip.mode.clone(),
                            departure_time: boarded_at.departure_time,
                            arrival_time: stop_time.arrival_time,
                        },
                    );
                }

                let previous_arrival = rounds[round - 1][stop_index];
                if previous_arrival == u32::MAX {
                    continue;
                }
                let transfer_slack = if round == 1
                    || matches!(
                        parents[round - 1].get(&stop_index),
                        Some(RaptorParent::Walk { .. })
                    ) {
                    0
                } else {
                    request.min_transfer_seconds
                };
                let ready_time = previous_arrival.saturating_add(transfer_slack);
                let catchable =
                    earliest_catchable_trip(trips, departure_indices, index, ready_time);
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
            .map(|stop| best[*stop])
            .min()
            .unwrap_or(u32::MAX);
        relax_raptor_transfers(
            round,
            &mut rounds,
            &mut best,
            &mut marked,
            &mut marked_flags,
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
            .filter_map(|stop| {
                let arrival_time = rounds[round][*stop];
                (arrival_time != u32::MAX).then_some((*stop, arrival_time))
            })
            .min_by_key(|(_, time)| *time)
        else {
            continue;
        };
        let Some(legs) =
            reconstruct_raptor_journey(timetable, &request_stop_ids, round, target, &parents)
        else {
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
    RaptorSearchOutput { journeys, stats }
}

fn route_departure_indices_by_stop_index(
    trips: &[RaptorTrip],
    verified_only: bool,
) -> Vec<Vec<usize>> {
    let stop_count = trips.first().map_or(0, |trip| trip.stop_times.len());
    (0..stop_count)
        .map(|stop_index| {
            let mut indices = trips
                .iter()
                .enumerate()
                .filter_map(|(trip_index, trip)| {
                    (!verified_only || trip.service_verified).then_some(trip_index)
                })
                .collect::<Vec<_>>();
            indices
                .sort_by_key(|trip_index| trips[*trip_index].stop_times[stop_index].departure_time);
            indices
        })
        .collect()
}

fn earliest_catchable_trip<'a>(
    trips: &'a [RaptorTrip],
    departure_indices_by_stop_index: &[Vec<usize>],
    stop_index: usize,
    ready_time: u32,
) -> Option<&'a RaptorTrip> {
    let departure_indices = departure_indices_by_stop_index.get(stop_index)?;
    let first_candidate = departure_indices.partition_point(|trip_index| {
        trips[*trip_index].stop_times[stop_index].departure_time < ready_time
    });
    departure_indices[first_candidate..]
        .iter()
        .map(|trip_index| &trips[*trip_index])
        .find(|trip| trip.stop_times[stop_index].pickup_allowed)
}

fn mark_raptor_stop(stop: usize, marked: &mut Vec<usize>, marked_flags: &mut [bool]) {
    if !marked_flags[stop] {
        marked_flags[stop] = true;
        marked.push(stop);
    }
}

#[allow(clippy::too_many_arguments)]
fn relax_raptor_transfers(
    round: usize,
    rounds: &mut [Vec<u32>],
    best: &mut [u32],
    marked: &mut Vec<usize>,
    marked_flags: &mut [bool],
    parents: &mut [HashMap<usize, RaptorParent>],
    transfers_by_stop: &[Vec<RaptorTransfer>],
    extra_transfers_by_stop: &HashMap<usize, Vec<RaptorTransfer>>,
    best_target: u32,
) {
    let mut queue = marked.clone();
    while let Some(from) = queue.pop() {
        let departure_time = rounds[round][from];
        if departure_time == u32::MAX {
            continue;
        }
        for transfer in transfers_by_stop
            .get(from)
            .into_iter()
            .flatten()
            .chain(extra_transfers_by_stop.get(&from).into_iter().flatten())
        {
            let arrival_time = departure_time.saturating_add(transfer.min_transfer_seconds);
            if arrival_time >= best_target || arrival_time >= best[transfer.to_stop_index] {
                continue;
            }
            rounds[round][transfer.to_stop_index] = arrival_time;
            best[transfer.to_stop_index] = arrival_time;
            mark_raptor_stop(transfer.to_stop_index, marked, marked_flags);
            queue.push(transfer.to_stop_index);
            parents[round].insert(
                transfer.to_stop_index,
                RaptorParent::Walk {
                    previous_stop: from,
                    departure_time,
                    arrival_time,
                    distance_meters: transfer.distance_meters,
                },
            );
        }
    }
}

fn reconstruct_raptor_journey(
    timetable: &RaptorTimetable,
    request_stop_ids: &[String],
    mut round: usize,
    target: usize,
    parents: &[HashMap<usize, RaptorParent>],
) -> Option<Vec<JourneyLeg>> {
    let mut stop = target;
    let mut legs = Vec::new();
    while let Some(parent) = parents.get(round).and_then(|round| round.get(&stop)) {
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
                    from_stop_id: raptor_stop_id(timetable, request_stop_ids, *previous_stop),
                    to_stop_id: raptor_stop_id(timetable, request_stop_ids, stop),
                    route_id: Some(route_id.clone()),
                    trip_id: Some(trip_id.clone()),
                    departure_time: *departure_time,
                    arrival_time: *arrival_time,
                    mode: mode.clone(),
                    warnings: Vec::new(),
                });
                stop = *previous_stop;
                round = *previous_round;
            }
            RaptorParent::Walk {
                previous_stop,
                departure_time,
                arrival_time,
                distance_meters,
            } => {
                legs.push(JourneyLeg {
                    from_stop_id: raptor_stop_id(timetable, request_stop_ids, *previous_stop),
                    to_stop_id: raptor_stop_id(timetable, request_stop_ids, stop),
                    route_id: None,
                    trip_id: None,
                    departure_time: *departure_time,
                    arrival_time: *arrival_time,
                    mode: TransportMode::Unknown,
                    warnings: vec![format!("walking_transfer:{}", distance_meters.unwrap_or(0))],
                });
                stop = *previous_stop;
            }
        }
    }
    (!legs.is_empty()).then(|| {
        legs.reverse();
        legs
    })
}

fn raptor_stop_id(timetable: &RaptorTimetable, request_stop_ids: &[String], stop: usize) -> String {
    timetable.stops.get(stop).cloned().unwrap_or_else(|| {
        request_stop_ids
            .get(stop.saturating_sub(timetable.stops.len()))
            .cloned()
            .unwrap_or_default()
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
                    service_verified: true,
                    stop_times: vec![
                        stop_time("a", 8 * 3600, 8 * 3600),
                        stop_time("c", 10 * 3600, 10 * 3600),
                    ],
                },
                RaptorTrip {
                    trip_id: "first".into(),
                    route_id: "r-first".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("a", 8 * 3600 + 60, 8 * 3600 + 60),
                        stop_time("b", 9 * 3600, 9 * 3600),
                    ],
                },
                RaptorTrip {
                    trip_id: "second".into(),
                    route_id: "r-second".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
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
                allow_unverified_services: false,
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
                service_verified: true,
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
                allow_unverified_services: false,
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs[0].from_stop_id, "selected");
        assert_eq!(journeys[0].legs[0].to_stop_id, "nearby");
        assert_eq!(journeys[0].walking_distance_meters, 150);
    }

    #[test]
    fn raptor_uses_earliest_catchable_trip_at_boarding_stop() {
        let stop_time = |stop: &str, time| RaptorStopTime {
            stop_id: stop.to_string(),
            arrival_time: time,
            departure_time: time,
            pickup_allowed: true,
            drop_off_allowed: true,
        };
        let timetable = RaptorTimetable::new(
            vec![
                RaptorTrip {
                    trip_id: "first-at-route-origin".into(),
                    route_id: "route".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("a", 8 * 3600),
                        stop_time("b", 9 * 3600),
                        stop_time("c", 10 * 3600),
                    ],
                },
                RaptorTrip {
                    trip_id: "first-at-transfer-stop".into(),
                    route_id: "route".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("a", 8 * 3600 + 600),
                        stop_time("b", 8 * 3600 + 1800),
                        stop_time("c", 9 * 3600),
                    ],
                },
            ],
            Vec::new(),
        );

        let journeys = raptor(
            &timetable,
            RaptorRequest {
                from_stop_ids: vec!["origin".into()],
                to_stop_ids: vec!["c".into()],
                extra_transfers: vec![Transfer {
                    from_stop_id: "origin".into(),
                    to_stop_id: "b".into(),
                    min_transfer_seconds: 8 * 3600 + 20 * 60,
                    distance_meters: Some(100),
                    walking_geometry: None,
                    confidence: CoordinateConfidence::Medium,
                    accessibility_level: None,
                    source: "test".into(),
                }],
                departure_time: 0,
                max_transfers: 1,
                min_transfer_seconds: 0,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(
            journeys[0].legs[1].trip_id.as_deref(),
            Some("first-at-transfer-stop")
        );
        assert_eq!(journeys[0].arrival_time, 9 * 3600);
    }

    #[test]
    fn raptor_excludes_faster_unverified_trip_from_verified_search() {
        let stop_time = |stop: &str, time| RaptorStopTime {
            stop_id: stop.to_string(),
            arrival_time: time,
            departure_time: time,
            pickup_allowed: true,
            drop_off_allowed: true,
        };
        let timetable = RaptorTimetable::new(
            vec![
                RaptorTrip {
                    trip_id: "ghost".into(),
                    route_id: "route".into(),
                    mode: TransportMode::Train,
                    service_verified: false,
                    stop_times: vec![stop_time("a", 5 * 3600), stop_time("b", 8 * 3600)],
                },
                RaptorTrip {
                    trip_id: "real".into(),
                    route_id: "route".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![stop_time("a", 6 * 3600), stop_time("b", 9 * 3600)],
                },
            ],
            Vec::new(),
        );

        let journeys = raptor(
            &timetable,
            RaptorRequest {
                from_stop_ids: vec!["a".into()],
                to_stop_ids: vec!["b".into()],
                extra_transfers: Vec::new(),
                departure_time: 4 * 3600,
                max_transfers: 0,
                min_transfer_seconds: 300,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs[0].trip_id.as_deref(), Some("real"));
    }

    #[test]
    fn timetable_samples_coverage_probes_for_range_search() {
        let stop_time = |stop: &str, time| RaptorStopTime {
            stop_id: stop.to_string(),
            arrival_time: time,
            departure_time: time,
            pickup_allowed: true,
            drop_off_allowed: true,
        };
        let timetable = RaptorTimetable::new(
            vec![
                RaptorTrip {
                    trip_id: "first".into(),
                    route_id: "route".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![stop_time("a", 8 * 3600), stop_time("b", 9 * 3600)],
                },
                RaptorTrip {
                    trip_id: "second".into(),
                    route_id: "route".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("a", 8 * 3600 + 900),
                        stop_time("b", 9 * 3600 + 900),
                    ],
                },
                RaptorTrip {
                    trip_id: "outside-window".into(),
                    route_id: "route".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![stop_time("a", 10 * 3600), stop_time("b", 11 * 3600)],
                },
            ],
            Vec::new(),
        );

        let departures = timetable.departure_times_from_stops(
            &["a".to_string()],
            8 * 3600,
            1800,
            3,
            &[TransportMode::Train],
            false,
        );

        assert_eq!(departures, vec![8 * 3600, 8 * 3600 + 600, 8 * 3600 + 1200]);
    }

    #[test]
    fn timetable_keeps_later_coverage_when_origin_departures_are_crowded() {
        let stop_time = |stop: &str, time| RaptorStopTime {
            stop_id: stop.to_string(),
            arrival_time: time,
            departure_time: time,
            pickup_allowed: true,
            drop_off_allowed: true,
        };
        let mut trips = (1..=20)
            .map(|minute| RaptorTrip {
                trip_id: format!("crowding-{minute}"),
                route_id: format!("local-{minute}"),
                mode: TransportMode::Train,
                service_verified: true,
                stop_times: vec![
                    stop_time("a", 8 * 3600 + minute * 60),
                    stop_time("local", 8 * 3600 + (minute + 10) * 60),
                ],
            })
            .collect::<Vec<_>>();
        trips.push(RaptorTrip {
            trip_id: "useful-later".into(),
            route_id: "intercity".into(),
            mode: TransportMode::Train,
            service_verified: true,
            stop_times: vec![
                stop_time("a", 8 * 3600 + 55 * 60),
                stop_time("b", 9 * 3600 + 30 * 60),
            ],
        });
        let timetable = RaptorTimetable::new(trips, Vec::new());

        let departures = timetable.departure_times_from_stops(
            &["a".to_string()],
            8 * 3600,
            3600,
            6,
            &[TransportMode::Train],
            false,
        );

        assert!(departures.contains(&(8 * 3600 + 50 * 60)));
    }

    #[test]
    #[ignore = "explicit large-network performance regression"]
    fn large_timetable_route_search_stays_below_latency_budget() {
        let route_count = 5_000;
        let trips_per_route = 20;
        let mut trips = Vec::with_capacity(route_count * trips_per_route);
        for route in 0..route_count {
            for trip in 0..trips_per_route {
                let departure = 6 * 3600 + trip as u32 * 300 + (route % 60) as u32;
                trips.push(RaptorTrip {
                    trip_id: format!("trip-{route}-{trip}"),
                    route_id: format!("route-{route}"),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        RaptorStopTime {
                            stop_id: "origin".to_string(),
                            arrival_time: departure,
                            departure_time: departure,
                            pickup_allowed: true,
                            drop_off_allowed: true,
                        },
                        RaptorStopTime {
                            stop_id: format!("middle-{route}"),
                            arrival_time: departure + 600,
                            departure_time: departure + 620,
                            pickup_allowed: true,
                            drop_off_allowed: true,
                        },
                        RaptorStopTime {
                            stop_id: "destination".to_string(),
                            arrival_time: departure + 1_200,
                            departure_time: departure + 1_200,
                            pickup_allowed: true,
                            drop_off_allowed: true,
                        },
                    ],
                });
            }
        }
        let timetable = RaptorTimetable::new(trips, Vec::new());
        let started = std::time::Instant::now();
        let journeys = raptor(
            &timetable,
            RaptorRequest {
                from_stop_ids: vec!["origin".to_string()],
                to_stop_ids: vec!["destination".to_string()],
                extra_transfers: Vec::new(),
                departure_time: 7 * 3600,
                max_transfers: 3,
                min_transfer_seconds: 300,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
            },
        );
        let elapsed = started.elapsed();
        eprintln!(
            "large timetable: {} trips, {} route patterns, search {:?}",
            route_count * trips_per_route,
            route_count,
            elapsed
        );

        assert!(!journeys.is_empty());
        assert!(
            elapsed < std::time::Duration::from_millis(1_500),
            "large timetable search took {elapsed:?}"
        );
    }
}
