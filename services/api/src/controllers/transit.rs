use crate::*;

pub(crate) async fn search_stops(
    State(state): State<AppState>,
    Query(query): Query<StopSearchQuery>,
) -> Json<Value> {
    let q = query.q.unwrap_or_default();
    let normalized = normalize_search_text(&q);
    let limit = query.limit.unwrap_or(10).clamp(1, 50);
    if let Some(pool) = &state.db {
        let stop_limit = if query.include_cities {
            limit.saturating_mul(2).min(50)
        } else {
            limit
        };
        let (stops_result, cities) = if query.include_cities {
            let (stops, cities) = tokio::join!(
                search_stops_db(pool, &q, &normalized, stop_limit),
                search_cities_db(pool, &q, &normalized, limit)
            );
            (stops, cities.unwrap_or_default())
        } else {
            (
                search_stops_db(pool, &q, &normalized, stop_limit).await,
                Vec::new(),
            )
        };
        return match stops_result {
            Ok(stops) => {
                let (results, visible_cities, visible_stops) =
                    ranked_place_suggestions(&cities, &stops, &normalized, limit);
                let related = if query.include_related {
                    Some(
                        stop_search_related_data_db(pool, &visible_stops)
                            .await
                            .unwrap_or_else(|error| {
                                json!({"warnings": [format!("database stop related data failed: {error}")]})
                            }),
                    )
                } else {
                    None
                };
                if query.include_cities {
                    let mut response = json!({
                        "results": results,
                        "cities": visible_cities.into_iter().map(|city| city_search_json(&city)).collect::<Vec<_>>(),
                        "stops": visible_stops.iter().map(stop_search_json).collect::<Vec<_>>(),
                        "data_status": database_data_status()
                    });
                    if let Some(related) = related {
                        response["related"] = related;
                    }
                    Json(response)
                } else {
                    let mut response = json!({
                        "stops": visible_stops.iter().map(stop_search_json).collect::<Vec<_>>(),
                        "data_status": database_data_status()
                    });
                    if let Some(related) = related {
                        response["related"] = related;
                    }
                    Json(response)
                }
            }
            Err(error) => {
                let data_status = json!({
                    "source": "database",
                    "schedule": "unknown",
                    "realtime": "unavailable",
                    "warnings": [format!("database stop search failed: {error}")]
                });
                if query.include_cities {
                    Json(json!({
                        "results": [],
                        "cities": [],
                        "stops": [],
                        "data_status": data_status
                    }))
                } else {
                    Json(json!({"stops": [], "data_status": data_status}))
                }
            }
        };
    }

    let stops = ranked_stop_suggestions(state.stops.iter(), &normalized, limit);
    if query.include_cities {
        let cities = ranked_city_suggestions(state.cities.iter(), &normalized, 50);
        let (results, visible_cities, visible_stops) =
            ranked_place_suggestions(&cities, &stops, &normalized, limit);
        Json(json!({
            "results": results,
            "cities": visible_cities.into_iter().map(|city| city_search_json(&city)).collect::<Vec<_>>(),
            "stops": visible_stops.iter().map(stop_search_json).collect::<Vec<_>>(),
            "data_status": mock_status(state.use_mock_data)
        }))
    } else {
        Json(json!({
            "stops": stops.iter().map(stop_search_json).collect::<Vec<_>>(),
            "data_status": mock_status(state.use_mock_data)
        }))
    }
}

pub(crate) fn ranked_place_suggestions(
    cities: &[City],
    stops: &[Stop],
    normalized_query: &str,
    limit: usize,
) -> (Vec<Value>, Vec<City>, Vec<Stop>) {
    enum Candidate<'a> {
        City(&'a City),
        Stop(&'a Stop),
    }

    let mut candidates = cities
        .iter()
        .filter_map(|city| {
            city_search_score(city, normalized_query)
                .map(|score| (score, city.name.as_str(), Candidate::City(city)))
        })
        .chain(stops.iter().filter_map(|stop| {
            stop_search_score(stop, normalized_query)
                .map(|score| (score, stop.name.as_str(), Candidate::Stop(stop)))
        }))
        .collect::<Vec<_>>();
    candidates.sort_by(|(left_score, left_name, _), (right_score, right_name, _)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_name.cmp(right_name))
    });

    let mut results = Vec::new();
    let mut visible_cities = Vec::new();
    let mut visible_stops = Vec::new();
    for (_, _, candidate) in candidates.into_iter().take(limit) {
        match candidate {
            Candidate::City(city) => {
                results.push(city_search_json(city));
                visible_cities.push(city.clone());
            }
            Candidate::Stop(stop) => {
                results.push(stop_search_json(stop));
                visible_stops.push(stop.clone());
            }
        }
    }
    (results, visible_cities, visible_stops)
}

pub(crate) fn ranked_stop_suggestions<'a>(
    stops: impl Iterator<Item = &'a Stop>,
    normalized_query: &str,
    limit: usize,
) -> Vec<Stop> {
    let mut scored_stops = stops
        .enumerate()
        .filter_map(|(index, stop)| {
            stop_search_score(stop, normalized_query).map(|score| (score, index, stop.clone()))
        })
        .collect::<Vec<_>>();
    scored_stops.sort_by(
        |(left_score, left_index, left_stop), (right_score, right_index, right_stop)| {
            right_score
                .cmp(left_score)
                .then_with(|| left_stop.name.cmp(&right_stop.name))
                .then_with(|| left_index.cmp(right_index))
        },
    );

    let mut suggestions: Vec<Stop> = Vec::new();
    for stop in scored_stops.into_iter().map(|(_, _, stop)| stop) {
        if suggestions
            .iter()
            .any(|existing| stops_are_same_suggestion(existing, &stop))
        {
            continue;
        }
        suggestions.push(stop);
        if suggestions.len() == limit {
            break;
        }
    }
    suggestions
}

pub(crate) async fn nearby_stops(
    State(state): State<AppState>,
    Query(query): Query<NearbyQuery>,
) -> Json<Value> {
    let radius = query.radius.unwrap_or(1000.0);
    if let Some(pool) = &state.db {
        return match nearby_stops_db(pool, query.lat, query.lon, radius).await {
            Ok(stops) => Json(
                json!({"stops": stops, "radius": radius, "data_status": database_data_status()}),
            ),
            Err(error) => Json(json!({
                "stops": [],
                "radius": radius,
                "data_status": {
                    "source": "database",
                    "schedule": "unknown",
                    "realtime": "unavailable",
                    "warnings": [format!("database nearby stop query failed: {error}")]
                }
            })),
        };
    }

    let stops = state
        .stops
        .iter()
        .filter(|stop| {
            stop.lat.zip(stop.lon).is_some_and(|(lat, lon)| {
                let distance_m = haversine_m(query.lat, query.lon, lat, lon);
                distance_m <= radius
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    Json(json!({"stops": stops, "radius": radius, "data_status": mock_status(state.use_mock_data)}))
}

pub(crate) async fn stops_in_bounds(
    State(state): State<AppState>,
    Query(query): Query<StopsInBoundsQuery>,
) -> Result<Json<Value>, ApiError> {
    query.validate()?;
    let limit = query.limit.unwrap_or(500);

    if let Some(pool) = &state.db {
        return match stops_in_bounds_db(pool, &query, limit).await {
            Ok(mut stops) => Ok(Json(stops_in_bounds_response(
                &mut stops,
                limit,
                database_data_status(),
            ))),
            Err(error) => Ok(Json(json!({
                "stops": [],
                "nextCursor": null,
                "data_status": {
                    "source": "database",
                    "schedule": "unknown",
                    "realtime": "unavailable",
                    "warnings": [format!("database in-bounds stop query failed: {error}")]
                }
            }))),
        };
    }

    let mut stops = state
        .stops
        .iter()
        .filter(|stop| stop.is_active)
        .filter(|stop| {
            stop.lat.zip(stop.lon).is_some_and(|(lat, lon)| {
                lat >= query.south && lat <= query.north && lon >= query.west && lon <= query.east
            })
        })
        .filter(|stop| {
            query
                .cursor
                .as_ref()
                .is_none_or(|cursor| stop.id.as_str() > cursor.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    stops.sort_by(|left, right| left.id.cmp(&right.id));
    stops.truncate(limit + 1);

    Ok(Json(stops_in_bounds_response(
        &mut stops,
        limit,
        mock_status(state.use_mock_data),
    )))
}

fn stops_in_bounds_response(stops: &mut Vec<Stop>, limit: usize, data_status: Value) -> Value {
    let has_more = stops.len() > limit;
    stops.truncate(limit);
    let next_cursor = has_more
        .then(|| stops.last().map(|stop| stop.id.clone()))
        .flatten();
    json!({
        "stops": stops,
        "nextCursor": next_cursor,
        "data_status": data_status
    })
}

pub(crate) async fn stop_detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if let Some(pool) = &state.db {
        let stop = get_stop_db(pool, &id)
            .await
            .map_err(internal_error)?
            .ok_or_else(not_found)?;
        return Ok(Json(
            json!({"stop": stop, "data_status": database_data_status()}),
        ));
    }

    let stop = state
        .stops
        .iter()
        .find(|stop| stop.id == id)
        .ok_or_else(not_found)?;
    Ok(Json(
        json!({"stop": stop, "data_status": mock_status(state.use_mock_data)}),
    ))
}

pub(crate) async fn stop_area(Path(id): Path<String>) -> Json<Value> {
    Json(json!({"id": id, "warning": "stop area detail is pending imported stop-area data"}))
}

pub(crate) async fn departures(
    State(state): State<AppState>,
    Query(query): Query<DeparturesQuery>,
) -> Json<Value> {
    let limit = query.limit.unwrap_or(10);
    if let Some(pool) = &state.db {
        let earliest = query
            .time
            .as_deref()
            .and_then(parse_query_time_seconds)
            .unwrap_or(0);
        return match departures_db(pool, &query.stop_id, earliest, limit).await {
            Ok(departures) => Json(json!({
                "stop_id": query.stop_id,
                "departures": departures,
                "data_status": database_data_status()
            })),
            Err(error) => Json(json!({
                "stop_id": query.stop_id,
                "departures": [],
                "data_status": {
                    "source": "database",
                    "schedule": "unknown",
                    "realtime": "unavailable",
                    "warnings": [format!("database departures query failed: {error}")]
                }
            })),
        };
    }

    Json(json!({
        "stop_id": query.stop_id,
        "departures": fixture_departures().into_iter().take(limit).collect::<Vec<_>>(),
        "data_status": {
            "schedule": if state.use_mock_data { "mock" } else { "current" },
            "realtime": "unavailable",
            "warnings": if state.use_mock_data { vec!["fixture departures are in use"] } else { Vec::<&str>::new() }
        }
    }))
}

pub(crate) async fn board_departures(Path(stop_id): Path<String>) -> Json<Value> {
    Json(public_board_payload(&stop_id))
}

pub(crate) async fn board_qr(Path(stop_id): Path<String>) -> Json<Value> {
    Json(
        json!({"stop_id": stop_id, "qr_url": format!("https://cesta.local/boards/{stop_id}"), "mock": true}),
    )
}

pub(crate) async fn realtime_vehicles(
    State(state): State<AppState>,
    Query(query): Query<RealtimeVehiclesQuery>,
) -> Json<Value> {
    let Some(pool) = &state.db else {
        return Json(json!({"vehicles": [], "data_status": mock_status(state.use_mock_data)}));
    };
    let limit = query.limit.unwrap_or(2_000).clamp(1, 10_000) as i64;
    match sqlx::query(
        r#"
        SELECT DISTINCT ON (source, vehicle_id)
          source, source_feed_id, vehicle_id, trip_id, route_id, stop_id,
          delay_seconds, estimated_arrival, estimated_departure,
          ST_Y(vehicle_position::geometry) AS lat,
          ST_X(vehicle_position::geometry) AS lon,
          bearing, fetched_at, valid_until, confidence
        FROM realtime_updates
        WHERE vehicle_id IS NOT NULL
          AND vehicle_position IS NOT NULL
          AND (valid_until IS NULL OR valid_until >= now())
          AND ($1::text IS NULL OR source = $1)
        ORDER BY source, vehicle_id, fetched_at DESC
        LIMIT $2
        "#,
    )
    .bind(query.source)
    .bind(limit)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => Json(json!({
            "vehicles": rows.into_iter().map(|row| json!({
                "source": row.get::<String, _>("source"),
                "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
                "vehicle_id": row.get::<String, _>("vehicle_id"),
                "trip_id": row.get::<Option<String>, _>("trip_id"),
                "route_id": row.get::<Option<String>, _>("route_id"),
                "stop_id": row.get::<Option<String>, _>("stop_id"),
                "delay_seconds": row.get::<Option<i32>, _>("delay_seconds"),
                "estimated_arrival": row.get::<Option<DateTime<Utc>>, _>("estimated_arrival"),
                "estimated_departure": row.get::<Option<DateTime<Utc>>, _>("estimated_departure"),
                "position": {
                    "lat": row.get::<f64, _>("lat"),
                    "lon": row.get::<f64, _>("lon")
                },
                "bearing": row.get::<Option<f64>, _>("bearing"),
                "fetched_at": row.get::<DateTime<Utc>, _>("fetched_at"),
                "valid_until": row.get::<Option<DateTime<Utc>>, _>("valid_until"),
                "confidence": row.get::<String, _>("confidence")
            })).collect::<Vec<_>>()
        })),
        Err(error) => Json(json!({"vehicles": [], "warnings": [error.to_string()]})),
    }
}

pub(crate) async fn data_sources_status(State(state): State<AppState>) -> Json<Value> {
    let Some(pool) = &state.db else {
        return Json(json!({"sources": [], "data_status": mock_status(state.use_mock_data)}));
    };
    match sqlx::query(
        r#"
        SELECT source_id, source_url, data_kind, status, last_attempt_at,
               last_success_at, source_timestamp, records_received,
               records_written, error_message, metadata
        FROM data_source_syncs
        ORDER BY source_id ASC
        "#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => Json(json!({
            "sources": rows.into_iter().map(|row| json!({
                "source_id": row.get::<String, _>("source_id"),
                "source_url": row.get::<String, _>("source_url"),
                "data_kind": row.get::<String, _>("data_kind"),
                "status": row.get::<String, _>("status"),
                "last_attempt_at": row.get::<DateTime<Utc>, _>("last_attempt_at"),
                "last_success_at": row.get::<Option<DateTime<Utc>>, _>("last_success_at"),
                "source_timestamp": row.get::<Option<DateTime<Utc>>, _>("source_timestamp"),
                "records_received": row.get::<i32, _>("records_received"),
                "records_written": row.get::<i32, _>("records_written"),
                "error_message": row.get::<Option<String>, _>("error_message"),
                "metadata": row.get::<Value, _>("metadata")
            })).collect::<Vec<_>>()
        })),
        Err(error) => Json(json!({"sources": [], "warnings": [error.to_string()]})),
    }
}

pub(crate) async fn journey_search(
    State(state): State<AppState>,
    Json(body): Json<JourneySearchBody>,
) -> Result<Json<Value>, ApiError> {
    let departure_time = parse_journey_departure_seconds(&body.datetime)?;
    let service_date = parse_journey_service_date(&body.datetime)?;
    let include_intermediate_stops = body.include_intermediate_stops;
    let _request_metadata = (
        &body.mode,
        &body.walking_speed,
        body.prefer_reliable_transfers,
        body.offline_compatible,
        body.from.lat,
        body.from.lon,
        body.to.lat,
        body.to.lon,
    );

    if let Some(pool) = &state.db {
        let (from_validation, to_validation) = tokio::join!(
            validate_journey_point_db(pool, &body.from),
            validate_journey_point_db(pool, &body.to)
        );
        from_validation?;
        to_validation?;
        return match query_journeys_db(
            pool,
            &state.raptor_cache,
            &state.config.routing_snapshot_dir,
            &state.route_search_diagnostics,
            &body,
            departure_time,
            service_date,
        )
        .await
        {
            Ok((mut journeys, warnings, related, search_started_at)) => {
                let realtime_status = related["realtime_status"].as_str().unwrap_or("unavailable");
                let ticketing_started = tokio::time::Instant::now();
                let ticketing_result = state
                    .ticketing
                    .annotate_journeys(&mut journeys, &related, service_date)
                    .await;
                append_route_search_timing(
                    &state.route_search_diagnostics,
                    search_started_at,
                    "ticketing_annotation",
                    elapsed_millis(ticketing_started),
                    Some(format!("{} journeys", journeys.len())),
                    ticketing_result.is_ok(),
                )
                .await;
                ticketing_result?;
                Ok(Json(json!({
                    "journeys": journeys,
                    "related": related,
                    "data_status": database_data_status_with_realtime(realtime_status),
                    "warnings": warnings
                })))
            }
            Err(error) => Ok(Json(json!({
                "journeys": [],
                "data_status": {
                    "source": "database",
                    "schedule": "unknown",
                    "realtime": "unavailable",
                    "warnings": [format!("database journey search failed: {error}")]
                },
                "warnings": [format!("database journey search failed: {error}")]
            }))),
        };
    }

    validate_journey_point_fixture(&state.cities, &body.from)?;
    validate_journey_point_fixture(&state.cities, &body.to)?;
    let from_stop_id = resolve_journey_point_fixture(&state.stops, &state.cities, &body.from)
        .unwrap_or_else(|| {
            body.from
                .id
                .clone()
                .unwrap_or_else(|| body.from.point_type.clone())
        });
    let to_stop_id = resolve_journey_point_fixture(&state.stops, &state.cities, &body.to)
        .unwrap_or_else(|| {
            body.to
                .id
                .clone()
                .unwrap_or_else(|| body.to.point_type.clone())
        });
    let mut journeys = earliest_arrivals(
        &fixture_snapshot(),
        RoutingSearchRequest {
            from_stop_id: from_stop_id.clone(),
            to_stop_id: to_stop_id.clone(),
            departure_time,
            max_transfers: body.max_transfers,
            modes: body.transport_modes.clone(),
        },
    );
    let mut warnings = if state.use_mock_data {
        vec!["routing uses fixture snapshot until imported snapshots are wired".to_string()]
    } else {
        Vec::new()
    };
    if journeys.is_empty() && departure_time > 0 {
        journeys = earliest_arrivals(
            &fixture_snapshot(),
            RoutingSearchRequest {
                from_stop_id,
                to_stop_id,
                departure_time: 0,
                max_transfers: body.max_transfers,
                modes: body.transport_modes,
            },
        );
        if !journeys.is_empty() {
            warnings.push(
                "no departures were found after the requested time; returned earliest service-day journeys"
                    .to_string(),
            );
        }
    }
    let mut journey_values = if include_intermediate_stops {
        fixture_journeys_with_stop_calls(&journeys, &state.stops)
    } else {
        journeys
            .iter()
            .map(|journey| serde_json::to_value(journey).unwrap_or_else(|_| json!({})))
            .collect::<Vec<_>>()
    };
    let fixture_related = json!({"stops":state.stops.iter().map(|stop|json!({"id":stop.id,"name":stop.name})).collect::<Vec<_>>(),"routes":[]});
    state
        .ticketing
        .annotate_journeys(&mut journey_values, &fixture_related, service_date)
        .await?;
    Ok(Json(json!({
        "journeys": journey_values,
        "data_status": {
            "schedule": if state.use_mock_data { "mock" } else { "current" },
            "realtime": "unavailable",
            "offline_compatible": true,
            "valid_until": "2026-12-31"
        },
        "warnings": warnings
    })))
}

pub(crate) fn fixture_journeys_with_stop_calls(journeys: &[Journey], stops: &[Stop]) -> Vec<Value> {
    journeys
        .iter()
        .map(|journey| {
            let mut value = serde_json::to_value(journey).unwrap_or_else(|_| json!({}));
            for (index, leg) in journey.legs.iter().enumerate() {
                let endpoint = |stop_id: &str, arrival: u32, departure: u32, origin: bool| {
                    let stop = stops.iter().find(|stop| stop.id == stop_id);
                    json!({
                        "trip_id": leg.trip_id,
                        "stop_id": stop_id,
                        "stop_sequence": null,
                        "name": stop.map(|stop| stop.name.as_str()).unwrap_or(stop_id),
                        "municipality": stop.and_then(|stop| stop.municipality.as_deref()),
                        "lat": stop.and_then(|stop| stop.lat),
                        "lon": stop.and_then(|stop| stop.lon),
                        "platform": stop.and_then(|stop| stop.platform_code.as_deref()),
                        "scheduled_arrival_seconds": arrival,
                        "scheduled_departure_seconds": departure,
                        "scheduled_arrival": transit_model::seconds_to_time(arrival),
                        "scheduled_departure": transit_model::seconds_to_time(departure),
                        "is_origin": origin,
                        "is_destination": !origin,
                        "is_intermediate": false,
                        "realtime": {"status": "unavailable"}
                    })
                };
                value["legs"][index]["intermediate_stop_count"] = json!(0);
                value["legs"][index]["stop_calls"] = json!([
                    endpoint(
                        &leg.from_stop_id,
                        leg.departure_time,
                        leg.departure_time,
                        true
                    ),
                    endpoint(&leg.to_stop_id, leg.arrival_time, leg.arrival_time, false)
                ]);
            }
            value
        })
        .collect()
}

pub(crate) fn parse_journey_departure_seconds(datetime: &str) -> Result<u32, ApiError> {
    if let Ok(value) = chrono::DateTime::parse_from_rfc3339(datetime) {
        return Ok(seconds_since_midnight(value.time()));
    }

    if let Ok(value) = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%dT%H:%M:%S") {
        return Ok(seconds_since_midnight(value.time()));
    }

    if let Ok(value) = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S") {
        return Ok(seconds_since_midnight(value.time()));
    }

    if let Ok(value) = NaiveTime::parse_from_str(datetime, "%H:%M:%S") {
        return Ok(seconds_since_midnight(value));
    }

    Err(ApiError {
        code: "invalid_datetime".to_string(),
        message: "datetime must be RFC3339, YYYY-MM-DDTHH:MM:SS, YYYY-MM-DD HH:MM:SS, or HH:MM:SS"
            .to_string(),
    })
}

pub(crate) fn parse_journey_service_date(datetime: &str) -> Result<chrono::NaiveDate, ApiError> {
    if let Ok(value) = chrono::DateTime::parse_from_rfc3339(datetime) {
        return Ok(value.date_naive());
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%dT%H:%M:%S") {
        return Ok(value.date());
    }
    if let Ok(value) = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S") {
        return Ok(value.date());
    }
    if NaiveTime::parse_from_str(datetime, "%H:%M:%S").is_ok() {
        return Ok(Utc::now().date_naive());
    }
    Err(ApiError {
        code: "invalid_datetime".to_string(),
        message: "datetime must include a valid service date and time".to_string(),
    })
}

pub(crate) fn seconds_since_midnight(time: NaiveTime) -> u32 {
    time.num_seconds_from_midnight()
}

pub(crate) async fn realtime_trip(Path(trip_id): Path<String>) -> Json<Value> {
    Json(
        json!({"trip_id": trip_id, "updates": [], "realtime_status": "unavailable", "mock": false}),
    )
}

pub(crate) async fn realtime_status() -> Json<Value> {
    Json(
        json!({"status":"unavailable","sources":[],"mock_worker_available":true,"warning":"real realtime feeds are not connected yet"}),
    )
}

pub(crate) async fn offline_packages() -> Json<Value> {
    Json(json!({"packages": offline_pack::development_packages()}))
}

pub(crate) async fn offline_package_metadata(
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let package = package_by_id(&id)?;
    Ok(Json(offline_pack::package_manifest(&package)))
}

pub(crate) async fn offline_package_download(Path(id): Path<String>) -> Json<Value> {
    Json(
        json!({"id": id, "status":"not_available", "warning":"offline package binary generation is pending"}),
    )
}

pub(crate) async fn offline_package_delta(Path(id): Path<String>) -> Json<Value> {
    Json(
        json!({"id": id, "status":"not_available", "warning":"delta packages are planned for a later phase"}),
    )
}

pub(crate) async fn ticket_recommendation() -> Json<Value> {
    Json(
        json!({"options": [mock_ticket()], "mock": true, "warning": "ticket purchase and payment are out of scope"}),
    )
}

pub(crate) async fn ticket_quote() -> Json<Value> {
    Json(json!({"quote": mock_ticket(), "mock": true, "payment_enabled": false}))
}
