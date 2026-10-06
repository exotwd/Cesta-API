use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use transit_model::{Journey, JourneyLeg, RealtimeStatus, Transfer, TransportMode};

const MAX_REALTIME_ROUTING_DELAY_SECONDS: i32 = 6 * 3600;

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

#[derive(Debug, Clone, Copy, Default)]
struct RealtimeDelayBounds {
    max_positive_seconds: u32,
    max_early_seconds: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RaptorTimetable {
    stops: Vec<String>,
    stop_indices: HashMap<String, usize>,
    routes: Vec<RaptorRoute>,
    stop_routes: Vec<Vec<(usize, usize)>>,
    transfers_by_stop: Vec<Vec<RaptorTransfer>>,
    #[serde(default)]
    minimum_change_seconds_by_stop: Vec<Option<u32>>,
    #[serde(default)]
    trip_route_indices: HashMap<String, usize>,
    trip_count: usize,
    has_unverified_services: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RaptorTransfer {
    from_stop_index: usize,
    to_stop_index: usize,
    min_transfer_seconds: u32,
    distance_meters: Option<u32>,
    walking_geometry: Option<Value>,
    source: String,
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

        let mut grouped = HashMap::<(String, TransportMode, Vec<String>), Vec<RaptorTrip>>::new();
        for trip in trips {
            let key = (
                trip.route_id.clone(),
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
            .flat_map(partition_non_overtaking_trips)
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

        let trip_route_indices = routes
            .iter()
            .enumerate()
            .flat_map(|(route_index, route)| {
                route
                    .trips
                    .iter()
                    .map(move |trip| (trip.trip_id.clone(), route_index))
            })
            .collect::<HashMap<_, _>>();

        let mut stop_routes = vec![Vec::<(usize, usize)>::new(); stops.len()];
        for (route_index, route) in routes.iter().enumerate() {
            for (stop_index, stop) in route.stop_indices.iter().copied().enumerate() {
                stop_routes[stop].push((route_index, stop_index));
            }
        }
        let mut transfers_by_stop = vec![Vec::<RaptorTransfer>::new(); stops.len()];
        let mut minimum_change_seconds_by_stop = vec![None::<u32>; stops.len()];
        for transfer in transfers {
            let Some(&from_stop_index) = stop_indices.get(&transfer.from_stop_id) else {
                continue;
            };
            let Some(&to_stop_index) = stop_indices.get(&transfer.to_stop_id) else {
                continue;
            };
            if from_stop_index == to_stop_index {
                minimum_change_seconds_by_stop[from_stop_index] = Some(
                    minimum_change_seconds_by_stop[from_stop_index]
                        .map_or(transfer.min_transfer_seconds, |current| {
                            current.min(transfer.min_transfer_seconds)
                        }),
                );
                continue;
            }
            transfers_by_stop[from_stop_index].push(RaptorTransfer {
                from_stop_index,
                to_stop_index,
                min_transfer_seconds: transfer.min_transfer_seconds,
                distance_meters: transfer.distance_meters,
                walking_geometry: transfer.walking_geometry,
                source: transfer.source,
            });
        }
        Self {
            stops,
            stop_indices,
            routes,
            stop_routes,
            transfers_by_stop,
            minimum_change_seconds_by_stop,
            trip_route_indices,
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

    #[allow(clippy::too_many_arguments)]
    pub fn departure_times_from_stops(
        &self,
        stop_ids: &[String],
        access_transfers: &[Transfer],
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
        let requested_stop_ids = stop_ids.iter().map(String::as_str).collect::<HashSet<_>>();
        let mut origin_access_seconds = HashMap::<usize, u32>::new();
        for stop_id in stop_ids {
            if let Some(stop_index) = self.stop_indices.get(stop_id).copied() {
                origin_access_seconds.insert(stop_index, 0);
            }
        }
        for transfer in access_transfers {
            if !requested_stop_ids.contains(transfer.from_stop_id.as_str()) {
                continue;
            }
            let Some(stop_index) = self.stop_indices.get(&transfer.to_stop_id).copied() else {
                continue;
            };
            origin_access_seconds
                .entry(stop_index)
                .and_modify(|known| *known = (*known).min(transfer.min_transfer_seconds))
                .or_insert(transfer.min_transfer_seconds);
        }

        for (stop_index, access_seconds) in origin_access_seconds {
            let earliest_vehicle_departure = departure_time.saturating_add(access_seconds);
            let latest_vehicle_departure = latest_departure.saturating_add(access_seconds);
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
                    trips[*trip_index].stop_times[route_stop_index].departure_time
                        < earliest_vehicle_departure
                });
                for trip_index in &indices[first_candidate..] {
                    let stop_time = &trips[*trip_index].stop_times[route_stop_index];
                    if stop_time.departure_time > latest_vehicle_departure {
                        break;
                    }
                    if stop_time.pickup_allowed {
                        selected.insert(stop_time.departure_time.saturating_sub(access_seconds));
                    }
                }
            }
        }

        let departures = selected.into_iter().collect::<Vec<_>>();
        if departures.len() <= max_departures {
            return departures;
        }
        (0..max_departures)
            .map(|sample_index| {
                let candidate_index = sample_index * (departures.len() - 1) / (max_departures - 1);
                departures[candidate_index]
            })
            .collect()
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
    /// Extra user-requested margin added after every interchange movement.
    pub transfer_buffer_seconds: u32,
    pub modes: Vec<TransportMode>,
    pub allow_unverified_services: bool,
    pub realtime: Arc<RaptorRealtimeData>,
}

#[derive(Debug, Clone)]
pub struct RaptorRealtimeUpdate {
    pub trip_id: String,
    pub stop_id: Option<String>,
    pub delay_seconds: i32,
}

#[derive(Debug, Clone, Default)]
pub struct RaptorRealtimeData {
    delays_by_trip: HashMap<String, i32>,
    delays_by_trip_stop: HashMap<String, HashMap<String, i32>>,
    max_positive_delay_seconds: u32,
    max_early_departure_seconds: u32,
    delay_bounds_by_trip: HashMap<String, RealtimeDelayBounds>,
}

impl RaptorRealtimeData {
    pub fn from_updates(updates: impl IntoIterator<Item = RaptorRealtimeUpdate>) -> Self {
        let mut data = Self::default();
        for update in updates {
            let delay_seconds = update.delay_seconds.clamp(
                -MAX_REALTIME_ROUTING_DELAY_SECONDS,
                MAX_REALTIME_ROUTING_DELAY_SECONDS,
            );
            data.max_positive_delay_seconds = data
                .max_positive_delay_seconds
                .max(delay_seconds.max(0) as u32);
            data.max_early_departure_seconds = data
                .max_early_departure_seconds
                .max(delay_seconds.saturating_neg().max(0) as u32);
            let bounds = data
                .delay_bounds_by_trip
                .entry(update.trip_id.clone())
                .or_default();
            bounds.max_positive_seconds =
                bounds.max_positive_seconds.max(delay_seconds.max(0) as u32);
            bounds.max_early_seconds = bounds
                .max_early_seconds
                .max(delay_seconds.saturating_neg().max(0) as u32);
            if let Some(stop_id) = update.stop_id {
                data.delays_by_trip_stop
                    .entry(update.trip_id.clone())
                    .or_default()
                    .entry(stop_id)
                    .or_insert(delay_seconds);
            }
            data.delays_by_trip
                .entry(update.trip_id)
                .or_insert(delay_seconds);
        }
        data
    }

    pub fn is_empty(&self) -> bool {
        self.delays_by_trip.is_empty()
    }

    pub fn trip_count(&self) -> usize {
        self.delays_by_trip.len()
    }

    fn delay_seconds(&self, trip_id: &str, stop_id: &str) -> i32 {
        self.delays_by_trip_stop
            .get(trip_id)
            .and_then(|stops| stops.get(stop_id))
            .or_else(|| self.delays_by_trip.get(trip_id))
            .copied()
            .unwrap_or(0)
    }

    fn adjusted_time(&self, trip_id: &str, stop_id: &str, scheduled_time: u32) -> u32 {
        apply_delay(scheduled_time, self.delay_seconds(trip_id, stop_id))
    }

    fn delay_bounds_by_route(&self, timetable: &RaptorTimetable) -> Vec<RealtimeDelayBounds> {
        if timetable.trip_route_indices.is_empty() {
            return vec![
                RealtimeDelayBounds {
                    max_positive_seconds: self.max_positive_delay_seconds,
                    max_early_seconds: self.max_early_departure_seconds,
                };
                timetable.routes.len()
            ];
        }

        let mut bounds_by_route = vec![RealtimeDelayBounds::default(); timetable.routes.len()];
        for (trip_id, trip_bounds) in &self.delay_bounds_by_trip {
            let Some(&route_index) = timetable.trip_route_indices.get(trip_id) else {
                continue;
            };
            let route_bounds = &mut bounds_by_route[route_index];
            route_bounds.max_positive_seconds = route_bounds
                .max_positive_seconds
                .max(trip_bounds.max_positive_seconds);
            route_bounds.max_early_seconds = route_bounds
                .max_early_seconds
                .max(trip_bounds.max_early_seconds);
        }
        bounds_by_route
    }
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
        used_official_change_time: bool,
        realtime_applied: bool,
    },
    Walk {
        previous_stop: usize,
        departure_time: u32,
        arrival_time: u32,
        distance_meters: Option<u32>,
        geometry: Option<Value>,
        source: String,
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
    raptor_with_stats_excluding_routes(timetable, request, &HashSet::new())
}

/// Enumerate bounded direct services between the explicitly expanded endpoints.
///
/// RAPTOR keeps earliest-arrival labels, so a quicker service can suppress a
/// useful direct line. This complements those labels with actual direct trips,
/// without following walking links or depending on sampled range departures.
/// The departure window and aggregate journey times include realtime; leg times
/// remain scheduled, matching the regular RAPTOR response convention.
pub fn direct_journeys(
    timetable: &RaptorTimetable,
    request: &RaptorRequest,
    window_seconds: u32,
    max_candidates: usize,
) -> Vec<Journey> {
    if max_candidates == 0 {
        return Vec::new();
    }
    let allowed_modes = request.modes.iter().collect::<HashSet<_>>();
    let latest_departure = request.departure_time.saturating_add(window_seconds);
    let bounds_by_route = request.realtime.delay_bounds_by_route(timetable);
    let mut destination_positions = HashMap::<usize, BTreeSet<usize>>::new();
    for stop_id in &request.to_stop_ids {
        let Some(&stop_index) = timetable.stop_indices.get(stop_id) else {
            continue;
        };
        for &(route_index, position) in &timetable.stop_routes[stop_index] {
            destination_positions
                .entry(route_index)
                .or_default()
                .insert(position);
        }
    }
    let mut endpoint_pairs = BTreeSet::new();
    for stop_id in &request.from_stop_ids {
        let Some(&stop_index) = timetable.stop_indices.get(stop_id) else {
            continue;
        };
        for &(route_index, board) in &timetable.stop_routes[stop_index] {
            if let Some(destinations) = destination_positions.get(&route_index) {
                for &alight in destinations.range((board + 1)..) {
                    endpoint_pairs.insert((route_index, board, alight));
                }
            }
        }
    }

    let mut candidates = Vec::<DirectJourneyCandidate>::new();
    for (route_index, board, alight) in endpoint_pairs {
        let route = &timetable.routes[route_index];
        if !allowed_modes.is_empty() && !allowed_modes.contains(&route.trips[0].mode) {
            continue;
        }
        let departures = if request.allow_unverified_services {
            &route.departures_by_stop_index[board]
        } else {
            &route.verified_departures_by_stop_index[board]
        };
        let bounds = bounds_by_route[route_index];
        let earliest_scheduled = request
            .departure_time
            .saturating_sub(bounds.max_positive_seconds);
        let latest_scheduled = latest_departure.saturating_add(bounds.max_early_seconds);
        let first = departures.partition_point(|&trip_index| {
            route.trips[trip_index].stop_times[board].departure_time < earliest_scheduled
        });
        let mut pair_candidates = Vec::new();
        for &trip_index in &departures[first..] {
            let trip = &route.trips[trip_index];
            let boarded_at = &trip.stop_times[board];
            if boarded_at.departure_time > latest_scheduled {
                break;
            }
            let alighted_at = &trip.stop_times[alight];
            if !boarded_at.pickup_allowed || !alighted_at.drop_off_allowed {
                continue;
            }
            let departure_time = request.realtime.adjusted_time(
                &trip.trip_id,
                &boarded_at.stop_id,
                boarded_at.departure_time,
            );
            if departure_time < request.departure_time || departure_time > latest_departure {
                continue;
            }
            let arrival_time = request.realtime.adjusted_time(
                &trip.trip_id,
                &alighted_at.stop_id,
                alighted_at.arrival_time,
            );
            if arrival_time < departure_time {
                continue;
            }
            let mut warnings = Vec::new();
            if departure_time != boarded_at.departure_time
                || arrival_time != alighted_at.arrival_time
            {
                warnings.push("realtime_routing_applied".to_string());
            }
            pair_candidates.push(DirectJourneyCandidate {
                trip_id: trip.trip_id.clone(),
                route_id: trip.route_id.clone(),
                from_stop_id: boarded_at.stop_id.clone(),
                to_stop_id: alighted_at.stop_id.clone(),
                journey: Journey {
                    id: String::new(),
                    legs: vec![JourneyLeg {
                        from_stop_id: boarded_at.stop_id.clone(),
                        to_stop_id: alighted_at.stop_id.clone(),
                        route_id: Some(trip.route_id.clone()),
                        trip_id: Some(trip.trip_id.clone()),
                        departure_time: boarded_at.departure_time,
                        arrival_time: alighted_at.arrival_time,
                        mode: trip.mode.clone(),
                        warnings,
                        geometry: None,
                    }],
                    departure_time,
                    arrival_time,
                    duration_seconds: arrival_time - departure_time,
                    transfer_count: 0,
                    walking_distance_meters: 0,
                    realtime_status: RealtimeStatus::Unavailable,
                    risk_score: 0.0,
                    labels: Vec::new(),
                },
            });
            // Keep memory bounded even if a busy route has many departures.
            // Realtime can reorder them, so retain the best rather than stopping
            // after the first scheduled departures.
            pair_candidates.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
            pair_candidates.truncate(max_candidates);
        }
        candidates.extend(pair_candidates);
    }
    candidates.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
    let mut identities = HashSet::new();
    candidates.retain(|candidate| identities.insert(candidate.identity()));

    // Reserve one earliest candidate per public line before filling the limit.
    // Pattern partitions and later departures of a frequent line must not crowd
    // out another direct line serving the same selected stops.
    let mut routes = HashSet::new();
    let mut selected = BTreeSet::new();
    for (index, candidate) in candidates.iter().enumerate() {
        if routes.insert(&candidate.route_id) {
            selected.insert(index);
            if selected.len() == max_candidates {
                break;
            }
        }
    }
    for index in 0..candidates.len() {
        if selected.len() == max_candidates {
            break;
        }
        selected.insert(index);
    }
    candidates
        .into_iter()
        .enumerate()
        .filter_map(|(index, candidate)| selected.contains(&index).then_some(candidate.journey))
        .collect()
}

struct DirectJourneyCandidate {
    trip_id: String,
    route_id: String,
    from_stop_id: String,
    to_stop_id: String,
    journey: Journey,
}

impl DirectJourneyCandidate {
    fn sort_key(&self) -> (u32, u32, &str, &str, &str, &str) {
        (
            self.journey.departure_time,
            self.journey.arrival_time,
            &self.route_id,
            &self.trip_id,
            &self.from_stop_id,
            &self.to_stop_id,
        )
    }

    fn identity(&self) -> (String, String, String) {
        (
            self.trip_id.clone(),
            self.from_stop_id.clone(),
            self.to_stop_id.clone(),
        )
    }
}

pub fn raptor_with_stats_excluding_routes(
    timetable: &RaptorTimetable,
    request: RaptorRequest,
    excluded_route_ids: &HashSet<String>,
) -> RaptorSearchOutput {
    let allow_unverified_services = request.allow_unverified_services;
    let realtime = request.realtime.clone();
    let realtime_delay_bounds_by_route = realtime.delay_bounds_by_route(timetable);
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
            walking_geometry: transfer.walking_geometry,
            source: transfer.source,
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
            if excluded_route_ids.contains(&trips[0].route_id) {
                continue;
            }
            if !allowed_modes.is_empty() && !allowed_modes.contains(&trips[0].mode) {
                continue;
            }
            let departure_indices = if allow_unverified_services {
                &timetable.routes[route_index].departures_by_stop_index
            } else {
                &timetable.routes[route_index].verified_departures_by_stop_index
            };
            let stops = &trips[0].stop_times;
            let mut current_trip: Option<(&RaptorTrip, usize, bool)> = None;
            for index in start_index..stops.len() {
                let stop_index = timetable.routes[route_index].stop_indices[index];
                if let Some((trip, board_index, used_official_change_time)) = current_trip
                    && trip.stop_times[index].drop_off_allowed
                {
                    let stop_time = &trip.stop_times[index];
                    let effective_arrival_time = realtime.adjusted_time(
                        &trip.trip_id,
                        &stop_time.stop_id,
                        stop_time.arrival_time,
                    );
                    if effective_arrival_time < best_target
                        && effective_arrival_time < best[stop_index]
                    {
                        let boarded_at = &trip.stop_times[board_index];
                        let effective_departure_time = realtime.adjusted_time(
                            &trip.trip_id,
                            &boarded_at.stop_id,
                            boarded_at.departure_time,
                        );
                        let boarded_stop = timetable.routes[route_index].stop_indices[board_index];
                        rounds[round][stop_index] = effective_arrival_time;
                        best[stop_index] = effective_arrival_time;
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
                                used_official_change_time,
                                realtime_applied: effective_departure_time
                                    != boarded_at.departure_time
                                    || effective_arrival_time != stop_time.arrival_time,
                            },
                        );
                    }
                }

                let previous_arrival = rounds[round - 1][stop_index];
                if previous_arrival == u32::MAX {
                    continue;
                }
                let (transfer_slack, used_official_change_time) =
                    match parents[round - 1].get(&stop_index) {
                        None if round == 1 => (0, false),
                        Some(RaptorParent::Walk { .. }) => (request.transfer_buffer_seconds, false),
                        Some(RaptorParent::Ride { .. }) => match timetable
                            .minimum_change_seconds_by_stop
                            .get(stop_index)
                            .copied()
                            .flatten()
                        {
                            Some(seconds) => (
                                seconds.saturating_add(request.transfer_buffer_seconds),
                                true,
                            ),
                            None => (
                                request
                                    .min_transfer_seconds
                                    .saturating_add(request.transfer_buffer_seconds),
                                false,
                            ),
                        },
                        _ => (
                            request
                                .min_transfer_seconds
                                .saturating_add(request.transfer_buffer_seconds),
                            false,
                        ),
                    };
                let ready_time = previous_arrival.saturating_add(transfer_slack);
                let catchable = earliest_catchable_trip(
                    trips,
                    departure_indices,
                    index,
                    ready_time,
                    realtime.as_ref(),
                    realtime_delay_bounds_by_route[route_index],
                );
                if let Some((candidate, effective_departure_time)) = catchable
                    && current_trip.is_none_or(|(current, _, _)| {
                        let current_stop_time = &current.stop_times[index];
                        effective_departure_time
                            < realtime.adjusted_time(
                                &current.trip_id,
                                &current_stop_time.stop_id,
                                current_stop_time.departure_time,
                            )
                    })
                {
                    current_trip = Some((candidate, index, used_official_change_time));
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
    for (round, round_arrivals) in rounds.iter().enumerate().take(max_rounds + 1).skip(1) {
        let Some((target, arrival_time)) = target_stops
            .iter()
            .filter_map(|stop| {
                let arrival_time = round_arrivals[*stop];
                (arrival_time != u32::MAX).then_some((*stop, arrival_time))
            })
            .min_by_key(|(_, time)| *time)
        else {
            continue;
        };
        let Some(mut legs) =
            reconstruct_raptor_journey(timetable, &request_stop_ids, round, target, &parents)
        else {
            continue;
        };
        align_leading_walk_to_first_transit_departure(&mut legs);
        let departure_time = legs.first().map_or(request.departure_time, |leg| {
            leg.trip_id
                .as_deref()
                .map_or(leg.departure_time, |trip_id| {
                    realtime.adjusted_time(trip_id, &leg.from_stop_id, leg.departure_time)
                })
        });
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

fn partition_non_overtaking_trips(mut trips: Vec<RaptorTrip>) -> Vec<Vec<RaptorTrip>> {
    trips.sort_by(|left, right| {
        let left_first = &left.stop_times[0];
        let right_first = &right.stop_times[0];
        left_first
            .departure_time
            .cmp(&right_first.departure_time)
            .then_with(|| left_first.arrival_time.cmp(&right_first.arrival_time))
            .then_with(|| left.trip_id.cmp(&right.trip_id))
    });

    if trips
        .windows(2)
        .all(|pair| raptor_trip_precedes(&pair[0], &pair[1]))
    {
        return vec![trips];
    }

    let mut partitions = Vec::<Vec<RaptorTrip>>::new();
    for trip in trips {
        let compatible_partition = partitions
            .iter()
            .enumerate()
            .filter(|(_, partition)| {
                partition
                    .last()
                    .is_some_and(|previous| raptor_trip_precedes(previous, &trip))
            })
            .max_by_key(|(_, partition)| {
                partition
                    .last()
                    .and_then(|previous| previous.stop_times.first())
                    .map_or(0, |stop_time| stop_time.departure_time)
            })
            .map(|(index, _)| index);

        if let Some(index) = compatible_partition {
            partitions[index].push(trip);
        } else {
            partitions.push(vec![trip]);
        }
    }
    partitions
}

fn raptor_trip_precedes(left: &RaptorTrip, right: &RaptorTrip) -> bool {
    left.stop_times
        .iter()
        .zip(&right.stop_times)
        .all(|(left, right)| {
            left.arrival_time <= right.arrival_time && left.departure_time <= right.departure_time
        })
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
    realtime: &RaptorRealtimeData,
    delay_bounds: RealtimeDelayBounds,
) -> Option<(&'a RaptorTrip, u32)> {
    let departure_indices = departure_indices_by_stop_index.get(stop_index)?;
    if delay_bounds.max_positive_seconds == 0 && delay_bounds.max_early_seconds == 0 {
        let first_candidate = departure_indices.partition_point(|trip_index| {
            trips[*trip_index].stop_times[stop_index].departure_time < ready_time
        });
        return departure_indices[first_candidate..]
            .iter()
            .map(|trip_index| &trips[*trip_index])
            .find(|trip| trip.stop_times[stop_index].pickup_allowed)
            .map(|trip| (trip, trip.stop_times[stop_index].departure_time));
    }

    let earliest_scheduled_time = ready_time.saturating_sub(delay_bounds.max_positive_seconds);
    let first_candidate = departure_indices.partition_point(|trip_index| {
        trips[*trip_index].stop_times[stop_index].departure_time < earliest_scheduled_time
    });
    let mut best = None::<(&RaptorTrip, u32)>;
    for trip_index in &departure_indices[first_candidate..] {
        let trip = &trips[*trip_index];
        let stop_time = &trip.stop_times[stop_index];
        if best.is_some_and(|(_, best_departure)| {
            stop_time
                .departure_time
                .saturating_sub(delay_bounds.max_early_seconds)
                >= best_departure
        }) {
            break;
        }
        if !stop_time.pickup_allowed {
            continue;
        }
        let effective_departure =
            realtime.adjusted_time(&trip.trip_id, &stop_time.stop_id, stop_time.departure_time);
        if effective_departure >= ready_time
            && best.is_none_or(|(_, best_departure)| effective_departure < best_departure)
        {
            best = Some((trip, effective_departure));
        }
    }
    best
}

fn apply_delay(scheduled_time: u32, delay_seconds: i32) -> u32 {
    if delay_seconds >= 0 {
        scheduled_time.saturating_add(delay_seconds as u32)
    } else {
        scheduled_time.saturating_sub(delay_seconds.unsigned_abs())
    }
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
                    geometry: transfer.walking_geometry.clone(),
                    source: transfer.source.clone(),
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
                used_official_change_time,
                realtime_applied,
            } => {
                let mut warnings = Vec::new();
                if *used_official_change_time {
                    warnings.push("official_minimum_change_time".to_string());
                }
                if *realtime_applied {
                    warnings.push("realtime_routing_applied".to_string());
                }
                legs.push(JourneyLeg {
                    from_stop_id: raptor_stop_id(timetable, request_stop_ids, *previous_stop),
                    to_stop_id: raptor_stop_id(timetable, request_stop_ids, stop),
                    route_id: Some(route_id.clone()),
                    trip_id: Some(trip_id.clone()),
                    departure_time: *departure_time,
                    arrival_time: *arrival_time,
                    mode: mode.clone(),
                    warnings,
                    geometry: None,
                });
                stop = *previous_stop;
                round = *previous_round;
            }
            RaptorParent::Walk {
                previous_stop,
                departure_time,
                arrival_time,
                distance_meters,
                geometry,
                source,
            } => {
                legs.push(JourneyLeg {
                    from_stop_id: raptor_stop_id(timetable, request_stop_ids, *previous_stop),
                    to_stop_id: raptor_stop_id(timetable, request_stop_ids, stop),
                    route_id: None,
                    trip_id: None,
                    departure_time: *departure_time,
                    arrival_time: *arrival_time,
                    mode: TransportMode::Unknown,
                    warnings: vec![
                        format!("walking_transfer:{}", distance_meters.unwrap_or(0)),
                        format!("walking_source:{source}"),
                    ],
                    geometry: geometry.clone(),
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

fn align_leading_walk_to_first_transit_departure(legs: &mut [JourneyLeg]) {
    let leading_walk_count = legs
        .iter()
        .take_while(|leg| leg.route_id.is_none() && leg.trip_id.is_none())
        .count();
    let Some(first_transit) = legs.get(leading_walk_count) else {
        return;
    };
    if leading_walk_count == 0 {
        return;
    }

    let durations = legs[..leading_walk_count]
        .iter()
        .map(|leg| leg.arrival_time.checked_sub(leg.departure_time))
        .collect::<Option<Vec<_>>>();
    let Some(durations) = durations else {
        return;
    };
    let total_duration = durations
        .iter()
        .try_fold(0_u32, |total, duration| total.checked_add(*duration));
    let Some(total_duration) = total_duration else {
        return;
    };
    if first_transit.departure_time < total_duration {
        return;
    }

    let mut next_departure = first_transit.departure_time;
    for (leg, duration) in legs[..leading_walk_count].iter_mut().zip(durations).rev() {
        leg.arrival_time = next_departure;
        leg.departure_time = next_departure - duration;
        next_departure = leg.departure_time;
    }
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
            geometry: None,
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
                geometry: transfer.walking_geometry.clone(),
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

    fn direct_scan_trip(trip_id: &str, route_id: &str, departure: u32, arrival: u32) -> RaptorTrip {
        RaptorTrip {
            trip_id: trip_id.into(),
            route_id: route_id.into(),
            mode: TransportMode::Tram,
            service_verified: true,
            stop_times: vec![
                RaptorStopTime {
                    stop_id: "karlovo".into(),
                    departure_time: departure,
                    arrival_time: departure,
                    pickup_allowed: true,
                    drop_off_allowed: true,
                },
                RaptorStopTime {
                    stop_id: "strossmayerovo".into(),
                    departure_time: arrival,
                    arrival_time: arrival,
                    pickup_allowed: true,
                    drop_off_allowed: true,
                },
            ],
        }
    }

    fn direct_scan_request() -> RaptorRequest {
        RaptorRequest {
            from_stop_ids: vec!["karlovo".into()],
            to_stop_ids: vec!["strossmayerovo".into()],
            extra_transfers: Vec::new(),
            departure_time: 8 * 3600,
            max_transfers: 0,
            min_transfer_seconds: 180,
            transfer_buffer_seconds: 0,
            modes: Vec::new(),
            allow_unverified_services: false,
            realtime: Arc::new(RaptorRealtimeData::default()),
        }
    }

    #[test]
    fn direct_scan_preserves_a_line_suppressed_by_earliest_arrival_labels() {
        let request = direct_scan_request();
        let start = request.departure_time;
        let timetable = RaptorTimetable::new(
            vec![
                direct_scan_trip("17-fast", "17", start, start + 600),
                direct_scan_trip("6-direct", "6", start + 60, start + 780),
            ],
            Vec::new(),
        );
        let regular = raptor(&timetable, request.clone());
        assert_eq!(regular.len(), 1);
        assert_eq!(regular[0].legs[0].route_id.as_deref(), Some("17"));

        let direct = direct_journeys(&timetable, &request, 1800, 10);
        assert_eq!(direct.len(), 2);
        assert_eq!(direct[1].legs[0].route_id.as_deref(), Some("6"));
        assert_eq!(direct[1].duration_seconds, 720);
        assert_eq!(direct[1].walking_distance_meters, 0);
        assert_eq!(direct[1].transfer_count, 0);
    }

    #[test]
    fn direct_scan_honors_modes_verified_services_and_departure_window() {
        let mut request = direct_scan_request();
        let start = request.departure_time;
        let mut unverified = direct_scan_trip("unverified", "6", start + 60, start + 600);
        unverified.service_verified = false;
        let mut bus = direct_scan_trip("bus", "bus", start + 120, start + 720);
        bus.mode = TransportMode::Bus;
        let timetable = RaptorTimetable::new(
            vec![
                unverified,
                bus,
                direct_scan_trip("too-early", "6", start - 1, start + 600),
                direct_scan_trip("boundary", "6", start + 1800, start + 2400),
                direct_scan_trip("too-late", "6", start + 1801, start + 2500),
            ],
            Vec::new(),
        );
        request.modes = vec![TransportMode::Tram];
        let verified = direct_journeys(&timetable, &request, 1800, 10);
        assert_eq!(verified.len(), 1);
        assert_eq!(verified[0].legs[0].trip_id.as_deref(), Some("boundary"));
        request.allow_unverified_services = true;
        let all = direct_journeys(&timetable, &request, 1800, 10);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].legs[0].trip_id.as_deref(), Some("unverified"));
    }

    #[test]
    fn direct_scan_handles_delayed_held_trips_and_early_departures() {
        let mut request = direct_scan_request();
        let start = request.departure_time;
        let timetable = RaptorTimetable::new(
            vec![
                direct_scan_trip("held", "6", start - 120, start + 600),
                direct_scan_trip("already-left", "6", start + 30, start + 750),
                direct_scan_trip("early-boundary", "6", start + 1810, start + 2530),
            ],
            Vec::new(),
        );
        request.realtime = Arc::new(RaptorRealtimeData::from_updates([
            RaptorRealtimeUpdate {
                trip_id: "held".into(),
                stop_id: None,
                delay_seconds: 180,
            },
            RaptorRealtimeUpdate {
                trip_id: "already-left".into(),
                stop_id: None,
                delay_seconds: -60,
            },
            RaptorRealtimeUpdate {
                trip_id: "early-boundary".into(),
                stop_id: None,
                delay_seconds: -10,
            },
        ]));
        let direct = direct_journeys(&timetable, &request, 1800, 10);
        assert_eq!(direct.len(), 2);
        assert_eq!(direct[0].legs[0].trip_id.as_deref(), Some("held"));
        assert_eq!(direct[0].departure_time, start + 60);
        assert_eq!(direct[0].arrival_time, start + 780);
        assert_eq!(direct[0].duration_seconds, 720);
        assert_eq!(direct[0].legs[0].departure_time, start - 120);
        assert_eq!(direct[0].legs[0].arrival_time, start + 600);
        assert!(
            direct[0].legs[0]
                .warnings
                .contains(&"realtime_routing_applied".to_string())
        );
        assert_eq!(direct[1].departure_time, start + 1800);
    }

    #[test]
    fn direct_scan_requires_correct_direction_and_allowed_boarding() {
        let request = direct_scan_request();
        let start = request.departure_time;
        let mut reverse = direct_scan_trip("reverse", "6", start, start + 600);
        reverse.stop_times[0].stop_id = "strossmayerovo".into();
        reverse.stop_times[1].stop_id = "karlovo".into();
        let mut no_pickup = direct_scan_trip("no-pickup", "6", start, start + 600);
        no_pickup.stop_times[0].pickup_allowed = false;
        let mut no_dropoff = direct_scan_trip("no-dropoff", "6", start, start + 600);
        no_dropoff.stop_times[1].drop_off_allowed = false;
        let mut wrong_platform = direct_scan_trip("wrong-platform", "6", start, start + 600);
        wrong_platform.stop_times[0].stop_id = "other-platform".into();
        let timetable = RaptorTimetable::new(
            vec![reverse, no_pickup, no_dropoff, wrong_platform],
            Vec::new(),
        );
        assert!(direct_journeys(&timetable, &request, 1800, 10).is_empty());
        let mut same_stop = request;
        same_stop.to_stop_ids = same_stop.from_stop_ids.clone();
        assert!(direct_journeys(&timetable, &same_stop, 1800, 10).is_empty());
    }

    #[test]
    fn direct_scan_does_not_follow_extra_walking_links() {
        let mut request = direct_scan_request();
        let start = request.departure_time;
        let mut trip = direct_scan_trip("nearby", "17", start + 300, start + 900);
        trip.stop_times[0].stop_id = "nearby-platform".into();
        let access = Transfer {
            from_stop_id: "karlovo".into(),
            to_stop_id: "nearby-platform".into(),
            min_transfer_seconds: 60,
            distance_meters: Some(100),
            walking_geometry: None,
            confidence: CoordinateConfidence::High,
            accessibility_level: None,
            source: "fixture".into(),
        };
        let timetable = RaptorTimetable::new(vec![trip], vec![access.clone()]);
        request.extra_transfers.push(access);
        assert!(direct_journeys(&timetable, &request, 1800, 10).is_empty());
        assert!(!raptor(&timetable, request).is_empty());
    }

    #[test]
    fn direct_scan_caps_realtime_reordered_candidates_by_effective_departure() {
        let mut request = direct_scan_request();
        let start = request.departure_time;
        let timetable = RaptorTimetable::new(
            vec![
                direct_scan_trip("delayed", "6", start - 60, start + 660),
                direct_scan_trip("on-time", "6", start + 60, start + 780),
            ],
            Vec::new(),
        );
        request.realtime = Arc::new(RaptorRealtimeData::from_updates([RaptorRealtimeUpdate {
            trip_id: "delayed".into(),
            stop_id: None,
            delay_seconds: 600,
        }]));
        let direct = direct_journeys(&timetable, &request, 1800, 1);
        assert_eq!(direct.len(), 1);
        assert_eq!(direct[0].legs[0].trip_id.as_deref(), Some("on-time"));
    }

    #[test]
    fn direct_scan_reserves_each_line_and_is_deterministic_and_bounded() {
        let mut request = direct_scan_request();
        let start = request.departure_time;
        request.from_stop_ids.push("karlovo".into());
        request.to_stop_ids.push("strossmayerovo".into());
        let mut trips = (0..10)
            .map(|index| {
                direct_scan_trip(
                    &format!("17-{index}"),
                    "17",
                    start + index * 60,
                    start + index * 60 + 600,
                )
            })
            .collect::<Vec<_>>();
        trips.push(direct_scan_trip("6-later", "6", start + 1200, start + 1920));
        let timetable = RaptorTimetable::new(trips.clone(), Vec::new());
        trips.reverse();
        let reversed = RaptorTimetable::new(trips, Vec::new());
        assert!(direct_journeys(&timetable, &request, 1800, 0).is_empty());
        let direct = direct_journeys(&timetable, &request, 1800, 2);
        assert_eq!(direct.len(), 2);
        assert_eq!(direct[0].legs[0].trip_id.as_deref(), Some("17-0"));
        assert_eq!(direct[1].legs[0].trip_id.as_deref(), Some("6-later"));
        let reversed_direct = direct_journeys(&reversed, &request, 1800, 2);
        assert_eq!(
            direct
                .iter()
                .map(|journey| &journey.legs[0].trip_id)
                .collect::<Vec<_>>(),
            reversed_direct
                .iter()
                .map(|journey| &journey.legs[0].trip_id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn direct_scan_finds_departures_omitted_by_range_sampling() {
        let request = direct_scan_request();
        let start = request.departure_time;
        let mut trips = (1..=30)
            .map(|minute| {
                let mut trip = direct_scan_trip(
                    &format!("crowding-{minute}"),
                    "local",
                    start + minute * 60,
                    start + (minute + 10) * 60,
                );
                trip.stop_times[1].stop_id = "local-destination".into();
                trip
            })
            .collect::<Vec<_>>();
        trips.push(direct_scan_trip(
            "direct-unsampled",
            "6",
            start + 840,
            start + 1560,
        ));
        let timetable = RaptorTimetable::new(trips, Vec::new());
        let sampled = timetable.departure_times_from_stops(
            &request.from_stop_ids,
            &[],
            start,
            1800,
            4,
            &[],
            false,
        );
        assert!(!sampled.contains(&(start + 840)));
        let direct = direct_journeys(&timetable, &request, 1800, 10);
        assert_eq!(direct.len(), 1);
        assert_eq!(
            direct[0].legs[0].trip_id.as_deref(),
            Some("direct-unsampled")
        );
    }

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
                transfer_buffer_seconds: 0,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::default()),
            },
        );

        assert_eq!(journeys.len(), 2);
        assert_eq!(journeys[0].transfer_count, 1);
        assert_eq!(journeys[0].arrival_time, 9 * 3600 + 1800);
        assert_eq!(journeys[1].transfer_count, 0);
    }

    #[test]
    fn raptor_adds_requested_buffer_after_interchange() {
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
                    trip_id: "feeder".into(),
                    route_id: "feeder-route".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![stop_time("a", 8 * 3600), stop_time("b", 9 * 3600)],
                },
                RaptorTrip {
                    trip_id: "tight".into(),
                    route_id: "connection-route".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("b", 9 * 3600 + 10 * 60),
                        stop_time("c", 10 * 3600),
                    ],
                },
                RaptorTrip {
                    trip_id: "buffered".into(),
                    route_id: "connection-route".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("b", 9 * 3600 + 20 * 60),
                        stop_time("c", 10 * 3600 + 10 * 60),
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
                max_transfers: 1,
                min_transfer_seconds: 5 * 60,
                transfer_buffer_seconds: 10 * 60,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::default()),
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs[1].trip_id.as_deref(), Some("buffered"));
    }

    #[test]
    fn raptor_route_exclusion_finds_a_distinct_alternative() {
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
                    trip_id: "fast-trip".into(),
                    route_id: "fast-route".into(),
                    mode: TransportMode::Tram,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("a", 8 * 3600, 8 * 3600),
                        stop_time("b", 8 * 3600 + 600, 8 * 3600 + 600),
                    ],
                },
                RaptorTrip {
                    trip_id: "alternative-trip".into(),
                    route_id: "alternative-route".into(),
                    mode: TransportMode::Metro,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("a", 8 * 3600 + 60, 8 * 3600 + 60),
                        stop_time("b", 8 * 3600 + 720, 8 * 3600 + 720),
                    ],
                },
            ],
            Vec::new(),
        );
        let request = RaptorRequest {
            from_stop_ids: vec!["a".into()],
            to_stop_ids: vec!["b".into()],
            extra_transfers: Vec::new(),
            departure_time: 8 * 3600,
            max_transfers: 0,
            min_transfer_seconds: 180,
            transfer_buffer_seconds: 0,
            modes: vec![TransportMode::Tram, TransportMode::Metro],
            allow_unverified_services: false,
            realtime: Arc::new(RaptorRealtimeData::default()),
        };

        let result = raptor_with_stats_excluding_routes(
            &timetable,
            request,
            &HashSet::from(["fast-route".to_string()]),
        );

        assert_eq!(result.journeys.len(), 1);
        assert_eq!(
            result.journeys[0].legs[0].route_id.as_deref(),
            Some("alternative-route")
        );
    }

    #[test]
    fn raptor_uses_official_same_stop_change_time() {
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
                    trip_id: "feeder".into(),
                    route_id: "r1".into(),
                    mode: TransportMode::Bus,
                    service_verified: true,
                    stop_times: vec![stop_time("a", 8 * 3600), stop_time("b", 9 * 3600)],
                },
                RaptorTrip {
                    trip_id: "connection".into(),
                    route_id: "r2".into(),
                    mode: TransportMode::Bus,
                    service_verified: true,
                    stop_times: vec![stop_time("b", 9 * 3600 + 180), stop_time("c", 10 * 3600)],
                },
            ],
            vec![Transfer {
                from_stop_id: "b".into(),
                to_stop_id: "b".into(),
                min_transfer_seconds: 120,
                distance_meters: None,
                walking_geometry: None,
                confidence: CoordinateConfidence::Exact,
                accessibility_level: None,
                source: "pid_gtfs_transfer".into(),
            }],
        );

        let journeys = raptor(
            &timetable,
            RaptorRequest {
                from_stop_ids: vec!["a".into()],
                to_stop_ids: vec!["c".into()],
                extra_transfers: Vec::new(),
                departure_time: 8 * 3600,
                max_transfers: 1,
                min_transfer_seconds: 5 * 60,
                transfer_buffer_seconds: 0,
                modes: vec![TransportMode::Bus],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::default()),
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs.len(), 2);
        assert_eq!(journeys[0].legs[1].trip_id.as_deref(), Some("connection"));
        assert_eq!(
            journeys[0].legs[1].warnings,
            vec!["official_minimum_change_time"]
        );
    }

    #[test]
    fn raptor_uses_realtime_delays_to_reject_missed_connections() {
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
                    trip_id: "delayed-feeder".into(),
                    route_id: "r1".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![stop_time("a", 8 * 3600), stop_time("b", 9 * 3600)],
                },
                RaptorTrip {
                    trip_id: "missed".into(),
                    route_id: "r2".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![stop_time("b", 9 * 3600 + 5 * 60), stop_time("c", 10 * 3600)],
                },
                RaptorTrip {
                    trip_id: "catchable".into(),
                    route_id: "r2".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("b", 9 * 3600 + 20 * 60),
                        stop_time("c", 10 * 3600 + 20 * 60),
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
                max_transfers: 1,
                min_transfer_seconds: 5 * 60,
                transfer_buffer_seconds: 0,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::from_updates([RaptorRealtimeUpdate {
                    trip_id: "delayed-feeder".into(),
                    stop_id: None,
                    delay_seconds: 10 * 60,
                }])),
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs[1].trip_id.as_deref(), Some("catchable"));
        assert_eq!(journeys[0].arrival_time, 10 * 3600 + 20 * 60);
        assert!(
            journeys[0].legs[0]
                .warnings
                .iter()
                .any(|warning| warning == "realtime_routing_applied")
        );
    }

    #[test]
    fn raptor_can_board_a_connection_held_by_realtime_delay() {
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
                    trip_id: "feeder".into(),
                    route_id: "r1".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![stop_time("a", 8 * 3600), stop_time("b", 9 * 3600)],
                },
                RaptorTrip {
                    trip_id: "held".into(),
                    route_id: "r2".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![stop_time("b", 9 * 3600 + 2 * 60), stop_time("c", 10 * 3600)],
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
                max_transfers: 1,
                min_transfer_seconds: 5 * 60,
                transfer_buffer_seconds: 0,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::from_updates([RaptorRealtimeUpdate {
                    trip_id: "held".into(),
                    stop_id: None,
                    delay_seconds: 5 * 60,
                }])),
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs[1].trip_id.as_deref(), Some("held"));
        assert_eq!(journeys[0].arrival_time, 10 * 3600 + 5 * 60);
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
                transfer_buffer_seconds: 0,
                modes: vec![TransportMode::Bus],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::default()),
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs[0].from_stop_id, "selected");
        assert_eq!(journeys[0].legs[0].to_stop_id, "nearby");
        assert_eq!(journeys[0].legs[0].departure_time, 8 * 3600 + 60);
        assert_eq!(journeys[0].legs[0].arrival_time, 8 * 3600 + 180);
        assert_eq!(journeys[0].departure_time, 8 * 3600 + 60);
        assert_eq!(journeys[0].walking_distance_meters, 150);
    }

    #[test]
    fn nearby_access_does_not_hide_better_trip_at_selected_origin() {
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
                    trip_id: "direct-from-selected-origin".into(),
                    route_id: "route".into(),
                    mode: TransportMode::Tram,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("nearby-earlier-stop", 7 * 3600 + 58 * 60),
                        stop_time("selected-origin-platform", 8 * 3600 + 4 * 60),
                        stop_time("target", 8 * 3600 + 20 * 60),
                    ],
                },
                RaptorTrip {
                    trip_id: "later-from-nearby-stop".into(),
                    route_id: "route".into(),
                    mode: TransportMode::Tram,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("nearby-earlier-stop", 8 * 3600 + 10 * 60),
                        stop_time("selected-origin-platform", 8 * 3600 + 12 * 60),
                        stop_time("target", 8 * 3600 + 28 * 60),
                    ],
                },
            ],
            Vec::new(),
        );

        let journeys = raptor(
            &timetable,
            RaptorRequest {
                from_stop_ids: vec!["selected-origin".into(), "selected-origin-platform".into()],
                to_stop_ids: vec!["target".into()],
                extra_transfers: vec![Transfer {
                    from_stop_id: "selected-origin".into(),
                    to_stop_id: "nearby-earlier-stop".into(),
                    min_transfer_seconds: 388,
                    distance_meters: Some(485),
                    walking_geometry: None,
                    confidence: CoordinateConfidence::High,
                    accessibility_level: None,
                    source: "endpoint_access".into(),
                }],
                departure_time: 8 * 3600,
                max_transfers: 0,
                min_transfer_seconds: 5 * 60,
                transfer_buffer_seconds: 0,
                modes: vec![TransportMode::Tram],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::default()),
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs.len(), 1);
        assert_eq!(
            journeys[0].legs[0].trip_id.as_deref(),
            Some("direct-from-selected-origin")
        );
        assert_eq!(journeys[0].departure_time, 8 * 3600 + 4 * 60);
        assert_eq!(journeys[0].arrival_time, 8 * 3600 + 20 * 60);
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
                transfer_buffer_seconds: 0,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::default()),
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
    fn raptor_handles_overtaking_trips_on_the_same_route_pattern() {
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
                    trip_id: "slow".into(),
                    route_id: "express-pattern".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("a", 8 * 3600),
                        stop_time("b", 8 * 3600 + 30 * 60),
                        stop_time("c", 10 * 3600),
                    ],
                },
                RaptorTrip {
                    trip_id: "fast".into(),
                    route_id: "express-pattern".into(),
                    mode: TransportMode::Train,
                    service_verified: true,
                    stop_times: vec![
                        stop_time("a", 8 * 3600 + 5 * 60),
                        stop_time("b", 8 * 3600 + 35 * 60),
                        stop_time("c", 9 * 3600),
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
                departure_time: 7 * 3600,
                max_transfers: 0,
                min_transfer_seconds: 0,
                transfer_buffer_seconds: 0,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::default()),
            },
        );

        assert_eq!(timetable.route_count(), 2);
        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs[0].trip_id.as_deref(), Some("fast"));
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
                transfer_buffer_seconds: 0,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::default()),
            },
        );

        assert_eq!(journeys.len(), 1);
        assert_eq!(journeys[0].legs[0].trip_id.as_deref(), Some("real"));
    }

    #[test]
    fn timetable_profiles_real_departure_events() {
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
            &[],
            8 * 3600,
            1800,
            3,
            &[TransportMode::Train],
            false,
        );

        assert_eq!(departures, vec![8 * 3600, 8 * 3600 + 900]);
    }

    #[test]
    fn timetable_samples_the_full_window_when_origin_departures_are_crowded() {
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
            &[],
            8 * 3600,
            3600,
            6,
            &[TransportMode::Train],
            false,
        );

        assert!(departures.contains(&(8 * 3600 + 55 * 60)));
    }

    #[test]
    fn timetable_profiles_vehicle_departures_from_coordinate_access_time() {
        let stop_time = |stop: &str, time| RaptorStopTime {
            stop_id: stop.to_string(),
            arrival_time: time,
            departure_time: time,
            pickup_allowed: true,
            drop_off_allowed: true,
        };
        let timetable = RaptorTimetable::new(
            vec![RaptorTrip {
                trip_id: "tram".into(),
                route_id: "tram-1".into(),
                mode: TransportMode::Tram,
                service_verified: true,
                stop_times: vec![
                    stop_time("boarding", 8 * 3600 + 10 * 60),
                    stop_time("destination", 8 * 3600 + 30 * 60),
                ],
            }],
            Vec::new(),
        );
        let coordinate = "coordinate:50.000000,14.000000".to_string();
        let access = Transfer {
            from_stop_id: coordinate.clone(),
            to_stop_id: "boarding".to_string(),
            min_transfer_seconds: 4 * 60,
            distance_meters: Some(300),
            walking_geometry: None,
            confidence: transit_model::CoordinateConfidence::High,
            accessibility_level: None,
            source: "test".to_string(),
        };

        let departures = timetable.departure_times_from_stops(
            &[coordinate],
            &[access],
            8 * 3600,
            30 * 60,
            10,
            &[TransportMode::Tram],
            false,
        );

        assert_eq!(departures, vec![8 * 3600, 8 * 3600 + 6 * 60]);
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
                transfer_buffer_seconds: 0,
                modes: vec![TransportMode::Train],
                allow_unverified_services: false,
                realtime: Arc::new(RaptorRealtimeData::default()),
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
