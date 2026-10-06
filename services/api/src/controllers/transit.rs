use crate::*;

pub(crate) struct CachedStopCatalog {
    loaded_at: std::time::Instant,
    etag: String,
    body: axum::body::Bytes,
}

pub(crate) async fn stop_catalog(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    // Single-flight rebuild, one cached immutable payload, at most 30 seconds of server caching.
    let mut cache = state.stop_catalog_cache.lock().await;
    if state.db.is_none()
        || cache
            .as_ref()
            .is_none_or(|entry| entry.loaded_at.elapsed() >= std::time::Duration::from_secs(30))
    {
        let (mut stops, data_status) = if let Some(pool) = &state.db {
            (
                stop_catalog_db(pool).await.map_err(internal_error)?,
                database_data_status(),
            )
        } else {
            (
                state.stops.as_ref().clone(),
                mock_status(state.use_mock_data),
            )
        };
        stops.retain(|stop| {
            stop.is_active
                && !stop.name.trim().is_empty()
                && !stop.normalized_name.trim().is_empty()
                && matches!(
                    stop.location_type,
                    StopLocationType::Stop | StopLocationType::Station
                )
        });
        stops.sort_by(|left, right| left.id.cmp(&right.id));
        let cpu_permit = routing_cpu_permits()
            .acquire_owned()
            .await
            .map_err(internal_error)?;
        let body = tokio::task::spawn_blocking(move || {
            let _cpu_permit = cpu_permit;
            serde_json::to_vec(&json!({
                "schema_version": 1,
                "count": stops.len(),
                "stops": stops.iter().map(stop_catalog_json).collect::<Vec<_>>(),
                "data_status": data_status
            }))
        })
        .await
        .map_err(internal_error)?
        .map_err(internal_error)?;
        let etag = format!("W/\"{}\"", hex::encode(Sha256::digest(&body)));
        *cache = Some(CachedStopCatalog {
            loaded_at: std::time::Instant::now(),
            etag,
            body: body.into(),
        });
    }
    let cached = cache.as_ref().expect("catalog initialized");
    let mut response = if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(',').any(|candidate| {
                let candidate = candidate.trim();
                candidate == "*"
                    || candidate.strip_prefix("W/").unwrap_or(candidate)
                        == cached.etag.strip_prefix("W/").unwrap_or(&cached.etag)
            })
        }) {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        (
            [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
            axum::body::Body::from(cached.body.clone()),
        )
            .into_response()
    };
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&cached.etag).expect("SHA-256 ETag is a valid header value"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    Ok(response)
}

pub(crate) async fn search_stops(
    State(state): State<AppState>,
    Query(query): Query<StopSearchQuery>,
) -> Result<Json<Value>, ApiError> {
    if query
        .q
        .as_ref()
        .is_some_and(|q| q.len() > 200 || q.chars().any(char::is_control))
    {
        return Err(ApiError {
            code: "validation_error".into(),
            message: "q must contain at most 200 bytes and no control characters".into(),
        });
    }
    if query.limit.is_some_and(|limit| !(1..=50).contains(&limit)) {
        return Err(ApiError {
            code: "validation_error".into(),
            message: "limit must be between 1 and 50".into(),
        });
    }
    let result = search_stops_inner(State(state), Query(query)).await;
    if result.0["data_status"]["schedule"] == "unknown" {
        return Err(service_unavailable("stop search unavailable"));
    }
    Ok(result)
}

async fn search_stops_inner(
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
        let database_search = time::timeout(
            std::time::Duration::from_secs(STOP_SEARCH_TIMEOUT_SECONDS),
            async {
                if query.include_cities {
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
                }
            },
        )
        .await;
        let (stops_result, cities) = match database_search {
            Ok(result) => result,
            Err(_) => {
                tracing::warn!(
                    timeout_seconds = STOP_SEARCH_TIMEOUT_SECONDS,
                    "database stop search timed out"
                );
                return stop_search_failure_response(
                    query.include_cities,
                    format!(
                        "database stop search timed out after {STOP_SEARCH_TIMEOUT_SECONDS} seconds"
                    ),
                );
            }
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
                                json!({"warnings": [safe_data_warning(error, "database stop related data unavailable")]})
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
            Err(error) => stop_search_failure_response(
                query.include_cities,
                safe_data_warning(error, "database stop search unavailable"),
            ),
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

fn stop_search_failure_response(include_cities: bool, warning: String) -> Json<Value> {
    let data_status = json!({
        "source": "database",
        "schedule": "unknown",
        "realtime": "unavailable",
        "warnings": [warning]
    });
    if include_cities {
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
    if limit == 0 {
        return Vec::new();
    }

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
                .then_with(|| {
                    stop_suggestion_mode_rank(left_stop).cmp(&stop_suggestion_mode_rank(right_stop))
                })
                .then_with(|| left_index.cmp(right_index))
        },
    );
    let score_floor = scored_stops
        .first()
        .and_then(|(score, _, _)| (*score >= 10_000).then_some(9_000));

    let mut suggestions: Vec<Stop> = Vec::new();
    for stop in scored_stops
        .into_iter()
        .filter(|(score, _, _)| score_floor.is_none_or(|floor| *score >= floor))
        .map(|(_, _, stop)| stop)
    {
        if let Some(existing) = suggestions
            .iter_mut()
            .find(|existing| stops_are_same_suggestion(existing, &stop))
        {
            merge_stop_suggestion(existing, &stop);
        } else {
            suggestions.push(stop);
        }
    }
    suggestions.truncate(limit);
    suggestions
}

pub(crate) async fn nearby_stops(
    State(state): State<AppState>,
    Query(query): Query<NearbyQuery>,
) -> Result<Json<Value>, ApiError> {
    query.validate()?;
    let radius = query.radius.unwrap_or(1000.0);
    if let Some(pool) = &state.db {
        return match nearby_stops_db(pool, query.lat, query.lon, radius).await {
            Ok(stops) => Ok(Json(
                json!({"stops": stops, "radius": radius, "data_status": database_data_status()}),
            )),
            Err(error) => Err(service_unavailable(error)),
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
    Ok(Json(
        json!({"stops": stops, "radius": radius, "data_status": mock_status(state.use_mock_data)}),
    ))
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
            Err(error) => Err(service_unavailable(error)),
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
    let stops = stops.iter().map(stop_search_json).collect::<Vec<_>>();
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

pub(crate) async fn station_layout(
    State(state): State<AppState>,
    Path(station_id): Path<String>,
    Query(query): Query<StationLayoutQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Some(level) = query.level.as_deref()
        && level.trim().is_empty()
    {
        return Err(ApiError {
            code: "validation_error".to_string(),
            message: "level must not be empty".to_string(),
        });
    }

    let payload = if let Some(pool) = &state.db {
        station_layout_db(pool, &station_id, query.level.as_deref())
            .await
            .map_err(internal_error)?
            .ok_or_else(not_found)?
    } else if state.use_mock_data {
        mock_station_layout(&station_id, query.level.as_deref())?
    } else {
        return Err(not_found());
    };
    json_etag_response(payload, &headers, "public, max-age=60, must-revalidate")
}

async fn station_layout_db(
    pool: &PgPool,
    station_id: &str,
    level: Option<&str>,
) -> Result<Option<Value>, sqlx::Error> {
    let Some(layout) = sqlx::query(
        r#"
        SELECT station_id, complex_id, name, version, updated_at, source, attribution
        FROM station_layouts
        WHERE station_id = $1 AND active = true
        LIMIT 1
        "#,
    )
    .bind(station_id)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    let version = layout.get::<String, _>("version");
    if let Some(level) = level {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM station_layout_levels WHERE station_id = $1 AND layout_version = $2 AND level_id = $3)",
        )
        .bind(station_id)
        .bind(&version)
        .bind(level)
        .fetch_one(pool)
        .await?;
        if !exists {
            return Ok(None);
        }
    }
    let levels = sqlx::query(
        r#"
        SELECT level_id, name, level_index
        FROM station_layout_levels
        WHERE station_id = $1 AND layout_version = $2
          AND ($3::text IS NULL OR level_id = $3)
        ORDER BY level_index ASC, level_id ASC
        "#,
    )
    .bind(station_id)
    .bind(&version)
    .bind(level)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "id": row.get::<String, _>("level_id"),
            "name": row.get::<String, _>("name"),
            "index": row.get::<i32, _>("level_index")
        })
    })
    .collect::<Vec<_>>();
    let elements = sqlx::query(
        r#"
        SELECT element.element_id, element.kind, element.level_id, element.label,
               element.platform, element.track, element.wheelchair_accessible,
               COALESCE(status.available, element.default_available) AS available,
               element.geometry, element.properties
        FROM station_layout_elements element
        LEFT JOIN LATERAL (
          SELECT facility.available
          FROM station_facility_status facility
          WHERE facility.station_id = element.station_id
            AND facility.element_id = element.element_id
            AND facility.observed_at <= now()
            AND facility.valid_until >= now()
          ORDER BY facility.observed_at DESC
          LIMIT 1
        ) status ON true
        WHERE element.station_id = $1 AND element.layout_version = $2
          AND ($3::text IS NULL OR element.level_id IS NULL OR element.level_id = $3)
        ORDER BY element.kind ASC, element.element_id ASC
        "#,
    )
    .bind(station_id)
    .bind(&version)
    .bind(level)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "id": row.get::<String, _>("element_id"),
            "kind": row.get::<String, _>("kind"),
            "level_id": row.get::<Option<String>, _>("level_id"),
            "label": row.get::<Option<String>, _>("label"),
            "platform": row.get::<Option<String>, _>("platform"),
            "track": row.get::<Option<String>, _>("track"),
            "wheelchair_accessible": row.get::<Option<bool>, _>("wheelchair_accessible"),
            "available": row.get::<Option<bool>, _>("available"),
            "geometry": row.get::<Value, _>("geometry"),
            "properties": row.get::<Value, _>("properties")
        })
    })
    .collect::<Vec<_>>();

    Ok(Some(json!({
        "stationId": layout.get::<String, _>("station_id"),
        "complexId": layout.get::<Option<String>, _>("complex_id"),
        "name": layout.get::<String, _>("name"),
        "version": version,
        "updatedAt": layout.get::<DateTime<Utc>, _>("updated_at"),
        "source": layout.get::<String, _>("source"),
        "attribution": layout.get::<String, _>("attribution"),
        "levels": levels,
        "elements": elements
    })))
}

pub(crate) fn mock_station_layout(
    station_id: &str,
    level: Option<&str>,
) -> Result<Value, ApiError> {
    if level.is_some_and(|level| level != "mock-platform") {
        return Err(not_found());
    }
    Ok(json!({
        "stationId": station_id,
        "complexId": format!("complex:{station_id}"),
        "name": "Ukázková stanice",
        "version": "mock-v1",
        "updatedAt": "2026-10-01T08:00:00Z",
        "source": "Cesta development fixture",
        "attribution": "UKÁZKOVÁ GEOMETRIE – NENÍ URČENA K NAVIGACI",
        "mock": true,
        "levels": [{"id": "mock-platform", "name": "Ukázkové nástupiště", "index": 0}],
        "elements": [{
            "id": "mock-lift-1", "kind": "elevator", "level_id": "mock-platform",
            "label": "Ukázkový výtah", "platform": "1", "track": null,
            "wheelchair_accessible": true, "available": true,
            "geometry": {"type": "Point", "coordinates": [14.43, 50.08]},
            "properties": {"mock": true}
        }]
    }))
}

fn json_etag_response(
    payload: Value,
    headers: &HeaderMap,
    cache_control: &'static str,
) -> Result<Response, ApiError> {
    let body = serde_json::to_vec(&payload).map_err(internal_error)?;
    let etag = format!("W/\"{}\"", hex::encode(Sha256::digest(&body)));
    let not_modified = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|candidate| candidate.trim() == etag));
    let mut response = if not_modified {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        (
            [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
            body,
        )
            .into_response()
    };
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&etag).expect("SHA-256 ETag is valid"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(cache_control),
    );
    Ok(response)
}

pub(crate) async fn train_formation(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    Query(query): Query<FormationQuery>,
) -> Result<Json<Value>, ApiError> {
    if let Some(call_id) = query.at_call_id.as_deref()
        && call_id.trim().is_empty()
    {
        return Err(ApiError {
            code: "validation_error".to_string(),
            message: "atCallId must not be empty".to_string(),
        });
    }
    if let Some(pool) = &state.db {
        return train_formation_db(pool, &run_id, query.at_call_id.as_deref())
            .await
            .map_err(internal_error)?
            .map(Json)
            .ok_or_else(not_found);
    }
    if !state.use_mock_data {
        return Err(not_found());
    }
    Ok(Json(json!({
        "runId": run_id,
        "atCallId": query.at_call_id,
        "status": "planned",
        "orientationKnown": true,
        "directionLabel": "Ukázkový směr",
        "updatedAt": "2026-10-01T08:00:00Z",
        "validUntil": "2099-01-01T00:00:00Z",
        "source": "Cesta development fixture – ukázka",
        "mock": true,
        "vehicles": [
            {"position": 1, "type": "locomotive", "is_locomotive": true},
            {"position": 2, "number": "21", "class": "2", "is_locomotive": false,
             "wheelchair_accessible": true, "features": ["wheelchair", "bicycle", "wifi"]}
        ]
    })))
}

async fn train_formation_db(
    pool: &PgPool,
    run_id: &str,
    at_call_id: Option<&str>,
) -> Result<Option<Value>, sqlx::Error> {
    let call_sequence = if let Some(call_id) = at_call_id {
        let Some(sequence) = sqlx::query_scalar::<_, i32>(
            "SELECT stop_sequence FROM run_calls WHERE call_id = $1 AND run_id = $2",
        )
        .bind(call_id)
        .bind(run_id)
        .fetch_optional(pool)
        .await?
        else {
            return Ok(None);
        };
        Some(sequence)
    } else {
        None
    };
    let formations = sqlx::query(
        r#"
        SELECT id, status, orientation_known, direction_label, updated_at, valid_until, source,
               valid_from_call_sequence, valid_to_call_sequence
        FROM train_formations
        WHERE run_id = $1
          AND ($2::integer IS NULL OR valid_from_call_sequence IS NULL OR valid_from_call_sequence <= $2)
          AND ($2::integer IS NULL OR valid_to_call_sequence IS NULL OR valid_to_call_sequence >= $2)
        ORDER BY (status = 'confirmed') DESC, updated_at DESC
        "#,
    )
    .bind(run_id)
    .bind(call_sequence)
    .fetch_all(pool)
    .await?;
    let Some(formation) = formations.first() else {
        return Ok(None);
    };
    let formation_id = formation.get::<Uuid, _>("id");
    let vehicles = sqlx::query(
        r#"
        SELECT position, number, class, vehicle_type, is_locomotive,
               wheelchair_accessible, features, destination
        FROM formation_vehicles
        WHERE formation_id = $1
        ORDER BY array_index ASC
        "#,
    )
    .bind(formation_id)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| {
        json!({
            "position": row.get::<Option<i32>, _>("position"),
            "number": row.get::<Option<String>, _>("number"),
            "class": row.get::<Option<String>, _>("class"),
            "type": row.get::<Option<String>, _>("vehicle_type"),
            "is_locomotive": row.get::<bool, _>("is_locomotive"),
            "wheelchair_accessible": row.get::<Option<bool>, _>("wheelchair_accessible"),
            "features": row.get::<Vec<String>, _>("features"),
            "destination": row.get::<Option<String>, _>("destination")
        })
    })
    .collect::<Vec<_>>();
    let valid_until = formation.get::<DateTime<Utc>, _>("valid_until");
    let covers_whole_run = formation
        .get::<Option<i32>, _>("valid_from_call_sequence")
        .is_none()
        && formation
            .get::<Option<i32>, _>("valid_to_call_sequence")
            .is_none();
    let orientation_known = formation.get::<bool, _>("orientation_known")
        && valid_until >= Utc::now()
        && (at_call_id.is_some() || (formations.len() == 1 && covers_whole_run));
    Ok(Some(json!({
        "runId": run_id,
        "atCallId": at_call_id,
        "status": formation.get::<String, _>("status"),
        "orientationKnown": orientation_known,
        "directionLabel": formation.get::<Option<String>, _>("direction_label"),
        "updatedAt": formation.get::<DateTime<Utc>, _>("updated_at"),
        "validUntil": valid_until,
        "source": formation.get::<String, _>("source"),
        "vehicles": vehicles
    })))
}

pub(crate) async fn boarding_guidance(
    State(state): State<AppState>,
    Path((journey_id, leg_index)): Path<(String, usize)>,
    Query(query): Query<BoardingGuidanceQuery>,
) -> Result<Json<Value>, ApiError> {
    if !matches!(query.profile.as_str(), "fastest" | "wheelchair") {
        return Err(ApiError {
            code: "unsupported_boarding_profile".to_string(),
            message: "profile must be fastest or wheelchair".to_string(),
        });
    }
    if let Some(pool) = &state.db {
        return boarding_guidance_db(pool, &journey_id, leg_index, &query.profile)
            .await
            .map_err(internal_error)?
            .map(Json)
            .ok_or_else(not_found);
    }
    if !state.use_mock_data {
        return Err(not_found());
    }
    Ok(Json(json!({
        "status": "available", "trainZone": "middle",
        "reasonCode": if query.profile == "wheelchair" { "closest_to_elevator" } else { "closest_to_transfer" },
        "precision": "zone", "basis": "mock_verified_station_rule",
        "targetElementId": "mock-lift-1", "layoutVersion": "mock-v1",
        "validUntil": "2099-01-01T00:00:00Z", "mock": true,
        "journeyId": journey_id, "legIndex": leg_index, "profile": query.profile
    })))
}

async fn boarding_guidance_db(
    pool: &PgPool,
    journey_id: &str,
    leg_index: usize,
    profile: &str,
) -> Result<Option<Value>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT rule.train_zone, rule.reason_code, rule.precision,
               rule.coach_position_from_front, rule.door_side_relative_to_travel,
               rule.target_element_id, rule.layout_version, rule.valid_until,
               element.wheelchair_accessible,
               COALESCE(status.available, element.default_available) AS target_available
        FROM journey_boarding_guidance guidance
        JOIN metro_boarding_rules rule ON rule.rule_id = guidance.rule_id
        JOIN station_layouts layout
          ON layout.station_id = rule.arrival_station_id
         AND layout.version = rule.layout_version AND layout.active = true
        JOIN station_layout_elements element
          ON element.station_id = rule.arrival_station_id
         AND element.layout_version = rule.layout_version
         AND element.element_id = rule.target_element_id
        LEFT JOIN LATERAL (
          SELECT facility.available
          FROM station_facility_status facility
          WHERE facility.station_id = rule.arrival_station_id
            AND facility.element_id = rule.target_element_id
            AND facility.observed_at <= now() AND facility.valid_until >= now()
          ORDER BY facility.observed_at DESC LIMIT 1
        ) status ON true
        WHERE guidance.journey_id = $1 AND guidance.leg_index = $2 AND guidance.profile = $3
        LIMIT 1
        "#,
    )
    .bind(journey_id)
    .bind(leg_index as i32)
    .bind(profile)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let valid_until = row.get::<DateTime<Utc>, _>("valid_until");
    let target_available = row.get::<Option<bool>, _>("target_available");
    let wheelchair_verified = row.get::<Option<bool>, _>("wheelchair_accessible") == Some(true);
    if valid_until < Utc::now()
        || target_available != Some(true)
        || (profile == "wheelchair" && !wheelchair_verified)
    {
        return Ok(Some(json!({
            "status": "unavailable",
            "reason": if valid_until < Utc::now() { "expired" } else { "path_or_facility_unavailable" }
        })));
    }
    Ok(Some(json!({
        "status": "available",
        "trainZone": row.get::<String, _>("train_zone"),
        "reasonCode": row.get::<String, _>("reason_code"),
        "precision": row.get::<String, _>("precision"),
        "basis": "verified_station_rule",
        "targetElementId": row.get::<String, _>("target_element_id"),
        "layoutVersion": row.get::<String, _>("layout_version"),
        "validUntil": valid_until,
        "coachPositionFromFront": row.get::<Option<i32>, _>("coach_position_from_front"),
        "doorSideRelativeToTravel": row.get::<Option<String>, _>("door_side_relative_to_travel")
    })))
}

pub(crate) async fn stop_area(Path(id): Path<String>) -> Json<Value> {
    Json(json!({"id": id, "warning": "stop area detail is pending imported stop-area data"}))
}

pub(crate) async fn departures(
    State(state): State<AppState>,
    Query(query): Query<DeparturesQuery>,
) -> Result<Json<Value>, ApiError> {
    query.validate()?;
    let limit = query.limit.unwrap_or(10);
    let earliest = query
        .time
        .as_deref()
        .and_then(parse_query_time_seconds)
        .unwrap_or_else(current_prague_time_seconds);
    let service_date = Utc::now()
        .with_timezone(&chrono_tz::Europe::Prague)
        .date_naive();
    if let Some(pool) = &state.db {
        return match departures_db(pool, &query.stop_id, earliest, limit, service_date).await {
            Ok(departures) => Ok(Json(json!({
                "stop_id": query.stop_id,
                "departures": departures,
                "data_status": database_data_status()
            }))),
            Err(error) => Err(service_unavailable(error)),
        };
    }

    Ok(Json(json!({
        "stop_id": query.stop_id,
        "departures": fixture_departures()
            .into_iter()
            .filter(|departure| {
                departure["scheduled_departure"]
                    .as_str()
                    .and_then(parse_query_time_seconds)
                    .is_some_and(|departure| departure >= earliest)
            })
            .take(limit)
            .collect::<Vec<_>>(),
        "data_status": {
            "schedule": if state.use_mock_data { "mock" } else { "current" },
            "realtime": "unavailable",
            "warnings": if state.use_mock_data { vec!["fixture departures are in use"] } else { Vec::<&str>::new() }
        }
    })))
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
) -> Result<Json<Value>, ApiError> {
    let bbox = query.parsed_bbox()?;
    let Some(pool) = &state.db else {
        return Ok(Json(
            json!({"vehicles": [], "dataStatus": mock_status(state.use_mock_data)}),
        ));
    };
    let limit = query.limit.unwrap_or(2_000).clamp(1, 10_000) as i64;
    let requested_filter = query.source.or_else(|| {
        query.provider.as_deref().map(|provider| match provider {
            "pid" => "pid_gtfs_rt".to_string(),
            "ids_jmk" => "ids_jmk_positions".to_string(),
            "duk" => "duk_positions".to_string(),
            value => value.to_string(),
        })
    });
    let (source_filter, source_feed_filter) = match requested_filter {
        Some(value)
            if matches!(
                value.as_str(),
                "pid_realtime" | "ids_jmk_realtime" | "duk_realtime"
            ) =>
        {
            (None, Some(value))
        }
        value => (value, None),
    };
    let (west, south, east, north) = bbox.map_or((None, None, None, None), |values| {
        (
            Some(values[0]),
            Some(values[1]),
            Some(values[2]),
            Some(values[3]),
        )
    });
    match sqlx::query(
        r#"
        SELECT DISTINCT ON (realtime.source_feed_id, realtime.vehicle_id)
          realtime.source, realtime.source_feed_id, realtime.vehicle_id,
          realtime.trip_id, realtime.route_id, realtime.stop_id,
          realtime.service_date, call.stop_sequence,
          delay_seconds, estimated_arrival, estimated_departure,
          ST_Y(vehicle_position::geometry) AS lat,
          ST_X(vehicle_position::geometry) AS lon,
          bearing, speed_kmh, route_short_name, destination, vehicle_type,
          wheelchair_accessible, air_conditioned, usb_chargers, occupancy_status,
          vehicle_registration_number, operator_name, tracking, realtime.state,
          fetched_at, valid_until, confidence,
          feed.url AS source_url, feed.license_id, feed.attribution,
          feed.terms_url, feed.redistribution_allowed
        FROM realtime_updates realtime
        JOIN source_feeds feed
          ON feed.id = realtime.source_feed_id
         AND feed.enabled = true
        LEFT JOIN LATERAL (
          SELECT stop_time.stop_sequence
          FROM stop_times stop_time
          WHERE stop_time.trip_id = realtime.trip_id
            AND stop_time.stop_id = realtime.stop_id
          ORDER BY stop_time.stop_sequence ASC
          LIMIT 1
        ) call ON true
        WHERE realtime.vehicle_id IS NOT NULL
          AND vehicle_position IS NOT NULL
          AND (valid_until IS NULL OR valid_until >= now())
          AND ($1::text IS NULL OR realtime.source = $1)
          AND ($2::text IS NULL OR realtime.source_feed_id = $2)
          AND (
            $3::double precision IS NULL
            OR ST_Covers(
              ST_MakeEnvelope($3, $4, $5, $6, 4326),
              vehicle_position::geometry
            )
          )
        ORDER BY realtime.source_feed_id, realtime.vehicle_id, fetched_at DESC
        LIMIT $7
        "#,
    )
    .bind(source_filter)
    .bind(source_feed_filter)
    .bind(west)
    .bind(south)
    .bind(east)
    .bind(north)
    .bind(limit)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => Ok(Json(json!({
            "vehicles": rows.into_iter().map(|row| {
              let trip_id = row.get::<Option<String>, _>("trip_id");
              let stop_id = row.get::<Option<String>, _>("stop_id");
              let service_date = row.get::<Option<chrono::NaiveDate>, _>("service_date")
                  .unwrap_or_else(|| Utc::now().with_timezone(&chrono_tz::Europe::Prague).date_naive());
              let run_id = trip_id.as_deref().map(|trip_id| operational_run_id(trip_id, service_date));
              let call_id = run_id.as_deref()
                  .zip(stop_id.as_deref())
                  .zip(row.get::<Option<i32>, _>("stop_sequence"))
                  .map(|((run_id, stop_id), sequence)| operational_call_id(run_id, stop_id, sequence as i64));
              json!({
                "id": format!("{}:{}", vehicle_provider(&row.get::<String, _>("source")), row.get::<String, _>("vehicle_id")),
                "provider": vehicle_provider(&row.get::<String, _>("source")),
                "source": {
                    "feedId": row.get::<Option<String>, _>("source_feed_id"),
                    "url": row.get::<Option<String>, _>("source_url"),
                    "license": row.get::<Option<String>, _>("license_id"),
                    "attribution": row.get::<Option<String>, _>("attribution"),
                    "termsUrl": row.get::<Option<String>, _>("terms_url"),
                    "redistributionAllowed": row.get::<Option<bool>, _>("redistribution_allowed")
                },
                "vehicleId": row.get::<String, _>("vehicle_id"),
                "registrationNumber": row.get::<Option<String>, _>("vehicle_registration_number"),
                "latitude": row.get::<f64, _>("lat"),
                "longitude": row.get::<f64, _>("lon"),
                "heading": row.get::<Option<f64>, _>("bearing"),
                "speedKmh": row.get::<Option<f64>, _>("speed_kmh"),
                "route": {
                    "id": row.get::<Option<String>, _>("route_id"),
                    "shortName": row.get::<Option<String>, _>("route_short_name"),
                    "tripId": row.get::<Option<String>, _>("trip_id"),
                    "runId": run_id,
                    "callId": call_id,
                    "destination": row.get::<Option<String>, _>("destination"),
                    "nextStopId": row.get::<Option<String>, _>("stop_id")
                },
                "vehicleType": row.get::<Option<String>, _>("vehicle_type"),
                "accessibility": {
                    "wheelchairAccessible": row.get::<Option<bool>, _>("wheelchair_accessible")
                },
                "amenities": {
                    "airConditioned": row.get::<Option<bool>, _>("air_conditioned"),
                    "usbChargers": row.get::<Option<bool>, _>("usb_chargers")
                },
                "occupancyStatus": row.get::<Option<String>, _>("occupancy_status"),
                "operatorName": row.get::<Option<String>, _>("operator_name"),
                "tracking": row.get::<Option<bool>, _>("tracking"),
                "state": row.get::<Option<String>, _>("state"),
                "delaySeconds": row.get::<Option<i32>, _>("delay_seconds"),
                "estimatedArrival": row.get::<Option<DateTime<Utc>>, _>("estimated_arrival"),
                "estimatedDeparture": row.get::<Option<DateTime<Utc>>, _>("estimated_departure"),
                "updatedAt": row.get::<DateTime<Utc>, _>("fetched_at"),
                "validUntil": row.get::<Option<DateTime<Utc>>, _>("valid_until"),
                "confidence": row.get::<String, _>("confidence")
              })
            }).collect::<Vec<_>>()
        }))),
        Err(error) => Err(service_unavailable(error)),
    }
}

fn vehicle_provider(source: &str) -> &'static str {
    match source {
        "pid_gtfs_rt" => "pid",
        "ids_jmk_positions" => "ids_jmk",
        "duk_positions" => "duk",
        _ => "unknown",
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
        WHERE source_id IN (
          'pid_gtfs', 'pid_lines_geodata', 'pid_gtfs_rt',
          'ids_jmk_gtfs', 'ids_jmk_positions'
        )
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
        Err(error) => Json(
            json!({"sources": [], "warnings": [safe_data_warning(error, "Transport data query unavailable")]}),
        ),
    }
}

pub(crate) async fn journey_search(
    State(state): State<AppState>,
    Json(body): Json<JourneySearchBody>,
) -> Result<Json<Value>, ApiError> {
    if body.mode != "depart_at" {
        return Err(ApiError {
            code: "unsupported_search_mode".to_string(),
            message: "Only depart_at is supported; arrive_by search is not available yet"
                .to_string(),
        });
    }
    body.validate_limits()?;
    validate_journey_preferences(&body)?;
    let departure_time = parse_journey_departure_seconds(&body.datetime)?;
    let service_date = parse_journey_service_date(&body.datetime)?;
    let today = Utc::now()
        .with_timezone(&chrono_tz::Europe::Prague)
        .date_naive();
    if state.db.is_some()
        && !(-1..=90).contains(&service_date.signed_duration_since(today).num_days())
    {
        return Err(ApiError {
            code: "validation_error".into(),
            message: "Search date must be between yesterday and 90 days ahead".into(),
        });
    }
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
            &state.endpoint_access_cache,
            &state.pedestrian_router,
            &state.routing_realtime_cache,
            &state.config.routing_snapshot_dir,
            &state.route_search_diagnostics,
            &state.telemetry,
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
                state.telemetry.journey(
                    journeys.len(),
                    !warnings.is_empty(),
                    matches!(realtime_status, "unavailable" | "scheduled"),
                );
                Ok(Json(json!({
                    "journeys": journeys,
                    "related": related,
                    "data_status": database_data_status_with_realtime(realtime_status),
                    "warnings": warnings
                })))
            }
            Err(error) => Err(service_unavailable(error)),
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
    let fixture_related = json!({"stops":state.stops.iter().map(stop_search_json).collect::<Vec<_>>(),"routes":[],"trips":[],"stop_times":[]});
    attach_journey_display_metadata(&mut journey_values, &fixture_related);
    attach_journey_assistance_identity(&mut journey_values, &fixture_related, service_date);
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
                        "stop_sequence": if origin { 0 } else { 1 },
                        "name": stop.map(|stop| stop.name.as_str()).unwrap_or(stop_id),
                        "municipality": stop.and_then(|stop| stop.municipality.as_deref()),
                        "lat": stop.and_then(|stop| stop.lat),
                        "lon": stop.and_then(|stop| stop.lon),
                        "platform": stop.and_then(|stop| stop.platform_code.as_deref()),
                        "station_id": stop.and_then(|stop| stop.station_id.as_deref()),
                        "complex_id": stop.and_then(|stop| stop.complex_id.as_deref()),
                        "has_station_layout": stop.is_some_and(|stop| stop.has_station_layout),
                        "station_layout_version": stop.and_then(|stop| stop.station_layout_version.as_deref()),
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
        return Ok(seconds_since_midnight(
            value.with_timezone(&chrono_tz::Europe::Prague).time(),
        ));
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
        return Ok(value.with_timezone(&chrono_tz::Europe::Prague).date_naive());
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

pub(crate) async fn realtime_trip(
    State(state): State<AppState>,
    Path(trip_id): Path<String>,
) -> Json<Value> {
    let Some(pool) = &state.db else {
        return Json(json!({
            "trip_id": trip_id,
            "updates": [],
            "realtime_status": "unavailable",
            "mock": state.use_mock_data
        }));
    };

    let query = sqlx::query(
        r#"
        SELECT realtime.source, realtime.source_feed_id, realtime.source_entity_id,
               realtime.trip_id, realtime.route_id, realtime.stop_id,
               realtime.delay_seconds, realtime.estimated_arrival,
               realtime.estimated_departure, realtime.cancellation_status,
               realtime.vehicle_id, realtime.fetched_at, realtime.valid_until,
               realtime.confidence
        FROM realtime_updates realtime
        JOIN source_feeds feed
          ON feed.id = realtime.source_feed_id
         AND feed.enabled = true
        WHERE realtime.trip_id = $1
          AND (realtime.valid_until IS NULL OR realtime.valid_until >= now())
        ORDER BY
          (realtime.raw_payload->>'stop_sequence')::integer ASC NULLS LAST,
          realtime.fetched_at DESC
        LIMIT 500
        "#,
    )
    .bind(&trip_id)
    .fetch_all(pool)
    .await;

    match query {
        Ok(rows) => {
            let updates = rows
                .into_iter()
                .map(|row| {
                    json!({
                        "source": row.get::<String, _>("source"),
                        "source_feed_id": row.get::<Option<String>, _>("source_feed_id"),
                        "source_entity_id": row.get::<Option<String>, _>("source_entity_id"),
                        "trip_id": row.get::<Option<String>, _>("trip_id"),
                        "route_id": row.get::<Option<String>, _>("route_id"),
                        "stop_id": row.get::<Option<String>, _>("stop_id"),
                        "delay_seconds": row.get::<Option<i32>, _>("delay_seconds"),
                        "estimated_arrival": row.get::<Option<DateTime<Utc>>, _>("estimated_arrival"),
                        "estimated_departure": row.get::<Option<DateTime<Utc>>, _>("estimated_departure"),
                        "cancellation_status": row.get::<Option<String>, _>("cancellation_status"),
                        "vehicle_id": row.get::<Option<String>, _>("vehicle_id"),
                        "fetched_at": row.get::<DateTime<Utc>, _>("fetched_at"),
                        "valid_until": row.get::<Option<DateTime<Utc>>, _>("valid_until"),
                        "confidence": row.get::<String, _>("confidence")
                    })
                })
                .collect::<Vec<_>>();
            let realtime_status = if updates.is_empty() {
                "unavailable"
            } else {
                "realtime"
            };
            Json(json!({
                "trip_id": trip_id,
                "updates": updates,
                "realtime_status": realtime_status,
                "mock": false
            }))
        }
        Err(error) => Json(json!({
            "trip_id": trip_id,
            "updates": [],
            "realtime_status": "unavailable",
            "mock": false,
            "warnings": [safe_data_warning(error, "database realtime trip query unavailable")]
        })),
    }
}

pub(crate) async fn realtime_status(State(state): State<AppState>) -> Json<Value> {
    let Some(pool) = &state.db else {
        return Json(json!({
            "status": "unavailable",
            "sources": [],
            "mock": state.use_mock_data,
            "warnings": ["database is unavailable"]
        }));
    };

    let query = sqlx::query(
        r#"
        SELECT sync.source_id, sync.source_url, sync.status AS sync_status,
               sync.last_attempt_at, sync.last_success_at, sync.source_timestamp,
               sync.records_received, sync.records_written, sync.error_message
        FROM data_source_syncs sync
        JOIN source_feeds feed
          ON feed.id = CASE sync.source_id
            WHEN 'pid_gtfs_rt' THEN 'pid_realtime'
            WHEN 'pid_trip_summaries' THEN 'pid_realtime'
            WHEN 'pid_vehicle_positions' THEN 'pid_realtime'
            WHEN 'ids_jmk_positions' THEN 'ids_jmk_realtime'
            WHEN 'duk_positions' THEN 'duk_realtime'
            ELSE sync.source_id
          END
         AND feed.enabled = true
        WHERE sync.data_kind IN ('gtfs_realtime', 'json_realtime')
        ORDER BY sync.source_id
        "#,
    )
    .fetch_all(pool)
    .await;

    match query {
        Ok(rows) => {
            let mut any_fresh = false;
            let mut any_known = false;
            let mut any_syncing = false;
            let sources = rows
                .into_iter()
                .map(|row| {
                    any_known = true;
                    let source_timestamp = row.get::<Option<DateTime<Utc>>, _>("source_timestamp");
                    let sync_status = row.get::<String, _>("sync_status");
                    let current = source_timestamp
                        .is_some_and(|timestamp| timestamp > Utc::now() - Duration::minutes(5));
                    any_fresh |= current;
                    any_syncing |= sync_status == "syncing";
                    json!({
                        "source_id": row.get::<String, _>("source_id"),
                        "source_url": row.get::<String, _>("source_url"),
                        "status": if sync_status == "syncing" { "syncing" } else if current { "current" } else { "stale" },
                        "sync_status": sync_status,
                        "last_attempt_at": row.get::<DateTime<Utc>, _>("last_attempt_at"),
                        "last_success_at": row.get::<Option<DateTime<Utc>>, _>("last_success_at"),
                        "source_timestamp": source_timestamp,
                        "records_received": row.get::<i32, _>("records_received"),
                        "records_written": row.get::<i32, _>("records_written"),
                        "error_message": row.get::<Option<String>, _>("error_message")
                    })
                })
                .collect::<Vec<_>>();
            Json(json!({
                "status": if any_fresh { "current" } else if any_syncing { "syncing" } else if any_known { "stale" } else { "unavailable" },
                "sources": sources,
                "mock": false
            }))
        }
        Err(error) => Json(json!({
            "status": "unavailable",
            "sources": [],
            "mock": false,
            "warnings": [safe_data_warning(error, "database realtime status query unavailable")]
        })),
    }
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
