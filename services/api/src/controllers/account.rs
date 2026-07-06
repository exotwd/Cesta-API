use crate::*;

pub(crate) async fn register(
    State(state): State<AppState>,
    Json(body): Json<RegisterRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let email = body.email.trim();
    if email.len() > 254 || !email.contains('@') || body.password.len() < 8 {
        return Err(ApiError {
            code: "validation_error".to_string(),
            message: "A valid email and a password of at least 8 characters are required"
                .to_string(),
        });
    }
    let exists = if let Some(db) = &state.db {
        sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM users WHERE lower(email)=lower($1) AND deleted_at IS NULL)").bind(email).fetch_one(db).await.map_err(internal_error)?
    } else {
        state
            .users
            .read()
            .await
            .values()
            .any(|user| user.email.eq_ignore_ascii_case(email) && user.deleted_at.is_none())
    };
    if exists {
        return Err(ApiError {
            code: "conflict".to_string(),
            message: "Email is already registered".to_string(),
        });
    }
    let user = create_user_record(
        email,
        &body.password,
        body.display_name,
        vec!["user".to_string()],
    )
    .map_err(internal_error)?;
    if let Some(db) = &state.db {
        let mut transaction = db.begin().await.map_err(internal_error)?;
        sqlx::query("INSERT INTO users(id,email,password_hash,display_name,created_at) VALUES($1,$2,$3,$4,$5)").bind(user.id).bind(&user.email).bind(&user.password_hash).bind(&user.display_name).bind(user.created_at).execute(&mut *transaction).await.map_err(internal_error)?;
        for role in &user.roles {
            sqlx::query("INSERT INTO user_roles(user_id,role) VALUES($1,$2)")
                .bind(user.id)
                .bind(role)
                .execute(&mut *transaction)
                .await
                .map_err(internal_error)?;
        }
        sqlx::query("INSERT INTO user_profiles(user_id) VALUES($1) ON CONFLICT DO NOTHING")
            .bind(user.id)
            .execute(&mut *transaction)
            .await
            .map_err(internal_error)?;
        transaction.commit().await.map_err(internal_error)?;
    }
    let response = auth_response(&state, &user).await?;
    state.users.write().await.insert(user.id, user);
    Ok(Json(response))
}

pub(crate) async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let _device_name = body.device_name;
    let user = if let Some(db) = &state.db {
        user_by_email_db(db, &body.email)
            .await
            .map_err(internal_error)?
            .ok_or_else(unauthorized)?
    } else {
        state
            .users
            .read()
            .await
            .values()
            .find(|user| user.email.eq_ignore_ascii_case(&body.email) && user.deleted_at.is_none())
            .cloned()
            .ok_or_else(unauthorized)?
    };
    verify_password(&body.password, &user.password_hash)?;
    state.users.write().await.insert(user.id, user.clone());
    Ok(Json(auth_response(&state, &user).await?))
}

pub(crate) async fn refresh(
    State(state): State<AppState>,
    Json(body): Json<RefreshRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let token_hash = hash_token(&body.refresh_token);
    let user_id = if let Some(db) = &state.db {
        sqlx::query_scalar::<_, Uuid>(
            "UPDATE user_sessions SET revoked_at=now() WHERE id=(SELECT id FROM user_sessions WHERE refresh_token_hash=$1 AND revoked_at IS NULL AND expires_at>now() ORDER BY created_at DESC LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING user_id",
        )
        .bind(&token_hash)
        .fetch_optional(db)
        .await
        .map_err(internal_error)?
        .ok_or_else(unauthorized)?
    } else {
        state
            .refresh_tokens
            .write()
            .await
            .remove(&token_hash)
            .ok_or_else(unauthorized)?
    };
    let user = if let Some(db) = &state.db {
        user_by_id_db(db, user_id)
            .await
            .map_err(internal_error)?
            .ok_or_else(unauthorized)?
    } else {
        state
            .users
            .read()
            .await
            .get(&user_id)
            .cloned()
            .ok_or_else(unauthorized)?
    };
    state.users.write().await.insert(user.id, user.clone());
    Ok(Json(auth_response(&state, &user).await?))
}

pub(crate) async fn logout(
    State(state): State<AppState>,
    Json(body): Json<RefreshRequest>,
) -> Result<Json<Value>, ApiError> {
    state
        .refresh_tokens
        .write()
        .await
        .remove(&hash_token(&body.refresh_token));
    if let Some(db) = &state.db {
        sqlx::query("UPDATE user_sessions SET revoked_at=now() WHERE refresh_token_hash=$1 AND revoked_at IS NULL").bind(hash_token(&body.refresh_token)).execute(db).await.map_err(internal_error)?;
    }
    Ok(Json(json!({"status":"logged_out"})))
}

pub(crate) async fn auth_me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<PublicUser>, ApiError> {
    let user = current_user(&state, &headers).await?;
    Ok(Json(public_user(&user)))
}

pub(crate) async fn update_me(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<PublicUser>, ApiError> {
    let current = current_user(&state, &headers).await?;
    let mut users = state.users.write().await;
    let user = users.get_mut(&current.id).ok_or_else(unauthorized)?;
    if let Some(display_name) = body.get("display_name").and_then(Value::as_str) {
        user.display_name = Some(display_name.to_string());
    }
    Ok(Json(public_user(user)))
}

pub(crate) async fn delete_me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let current = current_user(&state, &headers).await?;
    if let Some(user) = state.users.write().await.get_mut(&current.id) {
        user.deleted_at = Some(Utc::now());
    }
    Ok(Json(json!({"status":"deleted"})))
}

pub(crate) async fn change_password() -> Json<Value> {
    Json(
        json!({"status":"not_implemented","warning":"password change endpoint is reserved for the database-backed auth flow"}),
    )
}

pub(crate) async fn profile(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    Ok(Json(json!({
        "user_id": user.id,
        "preferred_walking_speed": "normal",
        "prefer_fewer_transfers": false,
        "prefer_reliable_transfers": true,
        "default_departure_mode": "depart_at",
        "language": "cs",
        "accessibility_preferences": {}
    })))
}

pub(crate) async fn list_saved_places(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let places = state
        .saved_places
        .read()
        .await
        .get(&user.id)
        .cloned()
        .unwrap_or_default();
    Ok(Json(json!({"saved_places": places})))
}

pub(crate) async fn create_saved_place(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SavedPlaceRequest>,
) -> Result<Json<SavedPlace>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let now = Utc::now();
    let place = SavedPlace {
        id: Uuid::new_v4(),
        user_id: user.id,
        name: body.name,
        place_type: body.place_type,
        stop_id: body.stop_id,
        lat: body.lat,
        lon: body.lon,
        address: body.address,
        created_at: now,
        updated_at: now,
    };
    state
        .saved_places
        .write()
        .await
        .entry(user.id)
        .or_default()
        .push(place.clone());
    Ok(Json(place))
}

pub(crate) async fn update_saved_place() -> Json<Value> {
    Json(
        json!({"status":"not_implemented","warning":"PATCH saved place is reserved for repository-backed update"}),
    )
}

pub(crate) async fn delete_saved_place(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    state
        .saved_places
        .write()
        .await
        .entry(user.id)
        .or_default()
        .retain(|place| place.id != id);
    Ok(Json(json!({"status":"deleted"})))
}

pub(crate) async fn list_favorite_stops(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let favorites = state
        .favorite_stops
        .read()
        .await
        .get(&user.id)
        .cloned()
        .unwrap_or_default();
    Ok(Json(json!({"favorite_stops": favorites})))
}

pub(crate) async fn add_favorite_stop(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<FavoriteStopRequest>,
) -> Result<Json<FavoriteStop>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let favorite = FavoriteStop {
        id: Uuid::new_v4(),
        user_id: user.id,
        stop_id: body.stop_id,
        created_at: Utc::now(),
    };
    state
        .favorite_stops
        .write()
        .await
        .entry(user.id)
        .or_default()
        .push(favorite.clone());
    Ok(Json(favorite))
}

pub(crate) async fn delete_favorite_stop(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    state
        .favorite_stops
        .write()
        .await
        .entry(user.id)
        .or_default()
        .retain(|favorite| favorite.id != id);
    Ok(Json(json!({"status":"deleted"})))
}

pub(crate) async fn empty_user_collection() -> Json<Value> {
    Json(json!({"items":[],"warning":"endpoint shape is implemented; persistence is pending"}))
}

pub(crate) async fn notification_preferences() -> Json<Value> {
    Json(json!({"notification_preferences":[],"warning":"notification persistence is pending"}))
}
