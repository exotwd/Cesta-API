use crate::*;

pub(crate) async fn register(
    State(state): State<AppState>,
    Json(body): Json<RegisterRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    let email = body.email.trim();
    if email.len() > 254
        || !email.contains('@')
        || body.password.len() < 8
        || body.password.len() > 1024
        || body
            .display_name
            .as_ref()
            .is_some_and(|name| name.len() > 200)
    {
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
    let user = services::auth::create_user_record_async(
        email,
        &body.password,
        body.display_name,
        vec!["user".to_string()],
    )
    .await?;
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
    http::validation::bounded_text(&body.email, "email", 254)?;
    if body.password.len() > 1024 {
        return Err(unauthorized());
    }
    let identifier = hash_token(&body.email.trim().to_ascii_lowercase());
    let attempt_id = if let Some(db) = &state.db {
        Some(enforce_auth_rate_limit(db, &identifier, "login", 10, "15 minutes").await?)
    } else {
        None
    };
    let _device_name = body.device_name;
    let user = if let Some(db) = &state.db {
        let user = user_by_email_db(db, &body.email)
            .await
            .map_err(internal_error)?;
        let Some(user) = user else {
            let _ = services::auth::verify_password_async(
                &body.password,
                services::auth::dummy_password_hash(),
            )
            .await;
            return Err(unauthorized());
        };
        user
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
    if let Err(error) =
        services::auth::verify_password_async(&body.password, &user.password_hash).await
    {
        return Err(error);
    }
    if let Some(db) = &state.db {
        record_auth_attempt(db, attempt_id.expect("database attempt reserved"), true).await?;
    }
    state.users.write().await.insert(user.id, user.clone());
    Ok(Json(auth_response(&state, &user).await?))
}

pub(crate) async fn refresh(
    State(state): State<AppState>,
    Json(body): Json<RefreshRequest>,
) -> Result<Json<AuthResponse>, ApiError> {
    http::validation::bounded_text(&body.refresh_token, "refresh_token", 256)?;
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
        let mut transaction = db.begin().await.map_err(internal_error)?;
        let user_ids = sqlx::query_scalar::<_, Uuid>("UPDATE user_sessions SET revoked_at=now() WHERE refresh_token_hash=$1 AND revoked_at IS NULL RETURNING user_id")
            .bind(hash_token(&body.refresh_token)).fetch_all(&mut *transaction).await.map_err(internal_error)?;
        for user_id in user_ids {
            sqlx::query("UPDATE journey_subscriptions SET ended_at=COALESCE(ended_at,now()) WHERE user_id=$1 AND ended_at IS NULL")
                .bind(user_id).execute(&mut *transaction).await.map_err(internal_error)?;
        }
        transaction.commit().await.map_err(internal_error)?;
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
    let display_name = body
        .get("display_name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if display_name.as_ref().is_some_and(|value| value.len() > 120) {
        return Err(validation_error("display_name is too long"));
    }
    if let Some(db) = &state.db {
        sqlx::query(
            "UPDATE users SET display_name=$2,updated_at=now() WHERE id=$1 AND deleted_at IS NULL",
        )
        .bind(current.id)
        .bind(&display_name)
        .execute(db)
        .await
        .map_err(internal_error)?;
    }
    let mut users = state.users.write().await;
    let user = users.entry(current.id).or_insert(current);
    user.display_name = display_name;
    Ok(Json(public_user(user)))
}

pub(crate) async fn delete_me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let current = current_user(&state, &headers).await?;
    let replacement_password =
        services::auth::hash_password_async(&Uuid::new_v4().to_string()).await?;
    if let Some(db) = &state.db {
        let mut transaction = db.begin().await.map_err(internal_error)?;
        sqlx::query("UPDATE users SET email=$2,password_hash=$3,display_name=NULL,deleted_at=now(),updated_at=now(),auth_version=auth_version+1 WHERE id=$1 AND deleted_at IS NULL")
            .bind(current.id)
            .bind(format!("deleted-{}@deleted.invalid", current.id))
            .bind(replacement_password)
            .execute(&mut *transaction)
            .await
            .map_err(internal_error)?;
        sqlx::query(
            "UPDATE user_sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE user_id=$1",
        )
        .bind(current.id)
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;
        sqlx::query("DELETE FROM password_reset_tokens WHERE user_id=$1")
            .bind(current.id)
            .execute(&mut *transaction)
            .await
            .map_err(internal_error)?;
        for statement in [
            "DELETE FROM saved_places WHERE user_id=$1",
            "DELETE FROM favorite_stops WHERE user_id=$1",
            "DELETE FROM favorite_routes WHERE user_id=$1",
            "DELETE FROM notification_preferences WHERE user_id=$1",
            "DELETE FROM saved_routes WHERE user_id=$1",
            "DELETE FROM mobile_devices WHERE user_id=$1",
            "DELETE FROM user_profiles WHERE user_id=$1",
        ] {
            sqlx::query(statement)
                .bind(current.id)
                .execute(&mut *transaction)
                .await
                .map_err(internal_error)?;
        }
        transaction.commit().await.map_err(internal_error)?;
    }
    if let Some(user) = state.users.write().await.get_mut(&current.id) {
        user.deleted_at = Some(Utc::now());
        user.auth_version += 1;
    }
    state
        .refresh_tokens
        .write()
        .await
        .retain(|_, user_id| *user_id != current.id);
    state.saved_places.write().await.remove(&current.id);
    state.favorite_stops.write().await.remove(&current.id);
    Ok(Json(json!({
        "status":"deleted",
        "purchase_records":"retained_and_pseudonymized_for_contractual_and_legal_obligations"
    })))
}

pub(crate) async fn change_password(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ChangePasswordRequest>,
) -> Result<Json<Value>, ApiError> {
    validate_new_password(&body.new_password)?;
    let current = current_user(&state, &headers).await?;
    services::auth::verify_password_async(&body.current_password, &current.password_hash).await?;
    let password_hash = services::auth::hash_password_async(&body.new_password).await?;
    if let Some(db) = &state.db {
        let mut transaction = db.begin().await.map_err(internal_error)?;
        sqlx::query("UPDATE users SET password_hash=$2,auth_version=auth_version+1,updated_at=now() WHERE id=$1 AND deleted_at IS NULL")
            .bind(current.id).bind(&password_hash).execute(&mut *transaction).await.map_err(internal_error)?;
        sqlx::query(
            "UPDATE user_sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE user_id=$1",
        )
        .bind(current.id)
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;
        transaction.commit().await.map_err(internal_error)?;
    }
    if let Some(user) = state.users.write().await.get_mut(&current.id) {
        user.password_hash = password_hash;
        user.auth_version += 1;
    }
    state
        .refresh_tokens
        .write()
        .await
        .retain(|_, user_id| *user_id != current.id);
    Ok(Json(
        json!({"status":"password_changed","sessions_revoked":true}),
    ))
}

pub(crate) async fn profile(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    if let Some(db) = &state.db {
        let profile = sqlx::query_scalar::<_, Value>(
            "SELECT jsonb_build_object('user_id',user_id,'home_stop_id',home_stop_id,'work_stop_id',work_stop_id,'preferred_walking_speed',preferred_walking_speed,'prefer_fewer_transfers',prefer_fewer_transfers,'prefer_reliable_transfers',prefer_reliable_transfers,'default_departure_mode',default_departure_mode,'language',language,'accessibility_preferences',accessibility_preferences,'version',version,'updated_at',updated_at) FROM user_profiles WHERE user_id=$1",
        ).bind(user.id).fetch_optional(db).await.map_err(internal_error)?;
        return Ok(Json(profile.unwrap_or_else(|| json!({"user_id":user.id}))));
    }
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

pub(crate) async fn update_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ProfileUpdateRequest>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    if body
        .preferred_walking_speed
        .as_deref()
        .is_some_and(|value| !matches!(value, "slow" | "normal" | "fast"))
        || body
            .default_departure_mode
            .as_deref()
            .is_some_and(|value| !matches!(value, "depart_at" | "arrive_by"))
        || body
            .language
            .as_deref()
            .is_some_and(|value| value.len() < 2 || value.len() > 10)
        || body
            .accessibility_preferences
            .as_ref()
            .is_some_and(|value| !value.is_object())
    {
        return Err(validation_error("invalid profile preference"));
    }
    let Some(db) = &state.db else {
        return Err(ApiError {
            code: "database_required".into(),
            message: "Profile persistence requires the database".into(),
        });
    };
    let profile = sqlx::query_scalar::<_, Value>(
        r#"UPDATE user_profiles SET
          preferred_walking_speed=COALESCE($2,preferred_walking_speed),
          prefer_fewer_transfers=COALESCE($3,prefer_fewer_transfers),
          prefer_reliable_transfers=COALESCE($4,prefer_reliable_transfers),
          default_departure_mode=COALESCE($5,default_departure_mode),
          language=COALESCE($6,language),
          accessibility_preferences=COALESCE($7,accessibility_preferences),
          updated_at=now(),version=version+1 WHERE user_id=$1
          RETURNING jsonb_build_object('user_id',user_id,'home_stop_id',home_stop_id,'work_stop_id',work_stop_id,'preferred_walking_speed',preferred_walking_speed,'prefer_fewer_transfers',prefer_fewer_transfers,'prefer_reliable_transfers',prefer_reliable_transfers,'default_departure_mode',default_departure_mode,'language',language,'accessibility_preferences',accessibility_preferences,'version',version,'updated_at',updated_at)"#,
    ).bind(user.id).bind(body.preferred_walking_speed).bind(body.prefer_fewer_transfers)
      .bind(body.prefer_reliable_transfers).bind(body.default_departure_mode).bind(body.language)
      .bind(body.accessibility_preferences).fetch_one(db).await.map_err(internal_error)?;
    Ok(Json(profile))
}

pub(crate) async fn request_password_reset(
    State(state): State<AppState>,
    Json(body): Json<PasswordResetRequest>,
) -> Result<Json<Value>, ApiError> {
    let generic = json!({"status":"accepted","message":"If the account exists, reset instructions will be sent."});
    let Some(db) = &state.db else {
        return Ok(Json(generic));
    };
    let identifier = hash_token(&body.email.trim().to_ascii_lowercase());
    let attempt_id =
        enforce_auth_rate_limit(db, &identifier, "password_reset_request", 3, "1 hour").await?;
    record_auth_attempt(db, attempt_id, true).await?;
    let Some(user) = user_by_email_db(db, body.email.trim())
        .await
        .map_err(internal_error)?
    else {
        return Ok(Json(generic));
    };
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let token_hash = hash_token(&token);
    let mut transaction = db.begin().await.map_err(internal_error)?;
    sqlx::query("UPDATE password_reset_tokens SET used_at=COALESCE(used_at,now()) WHERE user_id=$1 AND used_at IS NULL")
        .bind(user.id).execute(&mut *transaction).await.map_err(internal_error)?;
    sqlx::query("INSERT INTO password_reset_tokens(user_id,token_hash,expires_at) VALUES($1,$2,now()+interval '30 minutes')")
        .bind(user.id).bind(&token_hash).execute(&mut *transaction).await.map_err(internal_error)?;
    transaction.commit().await.map_err(internal_error)?;
    if let Err(error) = deliver_password_reset(&user.email, &token).await {
        tracing::warn!(error_code = %error.code, "password reset delivery failed");
        sqlx::query("DELETE FROM password_reset_tokens WHERE token_hash=$1")
            .bind(token_hash)
            .execute(db)
            .await
            .map_err(internal_error)?;
    }
    Ok(Json(generic))
}

pub(crate) async fn complete_password_reset(
    State(state): State<AppState>,
    Json(body): Json<PasswordResetCompleteRequest>,
) -> Result<Json<Value>, ApiError> {
    validate_new_password(&body.new_password)?;
    let Some(db) = &state.db else {
        return Err(unauthorized());
    };
    http::validation::bounded_text(&body.token, "token", 256)?;
    let token_hash = hash_token(&body.token);
    let attempt_id =
        enforce_auth_rate_limit(db, &token_hash, "password_reset_complete", 8, "15 minutes")
            .await?;
    let password_hash = services::auth::hash_password_async(&body.new_password).await?;
    let mut transaction = db.begin().await.map_err(internal_error)?;
    let user_id = sqlx::query_scalar::<_, Uuid>("UPDATE password_reset_tokens SET used_at=now() WHERE id=(SELECT id FROM password_reset_tokens WHERE token_hash=$1 AND used_at IS NULL AND expires_at>now() AND failed_attempts<8 FOR UPDATE) RETURNING user_id")
        .bind(&token_hash).fetch_optional(&mut *transaction).await.map_err(internal_error)?;
    let Some(user_id) = user_id else {
        transaction.rollback().await.map_err(internal_error)?;

        return Err(ApiError {
            code: "invalid_reset_token".into(),
            message: "The reset token is invalid or expired".into(),
        });
    };
    sqlx::query("UPDATE users SET password_hash=$2,auth_version=auth_version+1,updated_at=now() WHERE id=$1 AND deleted_at IS NULL")
        .bind(user_id).bind(password_hash).execute(&mut *transaction).await.map_err(internal_error)?;
    sqlx::query("UPDATE user_sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE user_id=$1")
        .bind(user_id)
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;
    transaction.commit().await.map_err(internal_error)?;
    record_auth_attempt(db, attempt_id, true).await?;
    state.users.write().await.remove(&user_id);
    state
        .refresh_tokens
        .write()
        .await
        .retain(|_, id| *id != user_id);
    Ok(Json(
        json!({"status":"password_reset","sessions_revoked":true}),
    ))
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

pub(crate) async fn update_saved_place(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    current_user(&state, &headers).await?;
    Ok(Json(
        json!({"status":"not_implemented","warning":"PATCH saved place is reserved for repository-backed update"}),
    ))
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

pub(crate) async fn empty_user_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    current_user(&state, &headers).await?;
    Ok(Json(
        json!({"items":[],"warning":"endpoint shape is implemented; persistence is pending"}),
    ))
}

pub(crate) async fn notification_preferences(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    current_user(&state, &headers).await?;
    Ok(Json(
        json!({"notification_preferences":[],"warning":"notification persistence is pending"}),
    ))
}

pub(crate) async fn register_device(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<DeviceRegistrationRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let user = current_user(&state, &headers).await?;
    if !matches!(body.platform.as_str(), "ios" | "android")
        || body.push_token.len() < 16
        || body.push_token.len() > 4096
        || body.push_token.chars().any(char::is_whitespace)
        || !valid_timezone(&body.timezone)
        || body
            .app_version
            .as_ref()
            .is_some_and(|value| value.len() > 50)
        || body.locale.as_ref().is_some_and(|value| value.len() > 20)
    {
        return Err(validation_error("invalid device registration"));
    }
    let Some(db) = &state.db else {
        return Err(database_required());
    };
    let token_hash = hash_token(&body.push_token);
    let device = sqlx::query_scalar::<_, Value>(
        r#"INSERT INTO mobile_devices(user_id,platform,push_token,token_hash,app_version,locale,timezone)
        VALUES($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT(token_hash) DO UPDATE SET platform=EXCLUDED.platform,push_token=EXCLUDED.push_token,
          app_version=EXCLUDED.app_version,locale=EXCLUDED.locale,timezone=EXCLUDED.timezone,
          enabled=true,updated_at=now(),last_seen_at=now()
        WHERE mobile_devices.user_id=EXCLUDED.user_id
        RETURNING jsonb_build_object('id',id,'platform',platform,'app_version',app_version,
          'locale',locale,'timezone',timezone,'enabled',enabled,'created_at',created_at,
          'updated_at',updated_at,'last_seen_at',last_seen_at)"#,
    ).bind(user.id).bind(&body.platform).bind(&body.push_token).bind(token_hash)
      .bind(body.app_version).bind(body.locale).bind(body.timezone)
      .fetch_optional(db).await.map_err(internal_error)?;
    device
        .map(|value| (StatusCode::CREATED, Json(value)))
        .ok_or_else(|| ApiError {
            code: "conflict".into(),
            message: "Push token is already registered to another account".into(),
        })
}

pub(crate) async fn list_devices(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let Some(db) = &state.db else {
        return Err(database_required());
    };
    let devices = sqlx::query_scalar::<_, Value>(
        "SELECT jsonb_build_object('id',id,'platform',platform,'app_version',app_version,'locale',locale,'timezone',timezone,'enabled',enabled,'created_at',created_at,'updated_at',updated_at,'last_seen_at',last_seen_at) FROM mobile_devices WHERE user_id=$1 ORDER BY updated_at DESC,id",
    ).bind(user.id).fetch_all(db).await.map_err(internal_error)?;
    Ok(Json(json!({"devices":devices})))
}

pub(crate) async fn delete_device(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let Some(db) = &state.db else {
        return Err(database_required());
    };
    let mut transaction = db.begin().await.map_err(internal_error)?;
    let disabled = sqlx::query("UPDATE mobile_devices SET enabled=false,push_token='',updated_at=now() WHERE id=$1 AND user_id=$2")
        .bind(id).bind(user.id).execute(&mut *transaction).await.map_err(internal_error)?.rows_affected();
    if disabled == 0 {
        return Err(ApiError {
            code: "not_found".into(),
            message: "Device was not found".into(),
        });
    }
    sqlx::query("UPDATE journey_subscriptions SET ended_at=COALESCE(ended_at,now()) WHERE device_id=$1 AND user_id=$2")
        .bind(id).bind(user.id).execute(&mut *transaction).await.map_err(internal_error)?;
    transaction.commit().await.map_err(internal_error)?;
    Ok(Json(
        json!({"status":"disabled","subscriptions_ended":true}),
    ))
}

pub(crate) async fn create_journey_subscription(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<JourneySubscriptionRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let user = current_user(&state, &headers).await?;
    validate_journey_subscription(&body)?;
    let Some(db) = &state.db else {
        return Err(database_required());
    };
    let device_owned = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM mobile_devices WHERE id=$1 AND user_id=$2 AND enabled)",
    )
    .bind(body.device_id)
    .bind(user.id)
    .fetch_one(db)
    .await
    .map_err(internal_error)?;
    if !device_owned {
        return Err(ApiError {
            code: "not_found".into(),
            message: "Device was not found".into(),
        });
    }
    verify_trip_call(
        db,
        &body.trip_id,
        &body.boarding_stop_id,
        body.boarding_stop_sequence,
        &body.run_id,
        body.service_date,
        &body.boarding_call_id,
    )
    .await?;
    if let (Some(trip_id), Some(run_id), Some(service_date), Some(call_id)) = (
        body.connection_trip_id.as_deref(),
        body.connection_run_id.as_deref(),
        body.connection_service_date,
        body.connection_call_id.as_deref(),
    ) {
        let connection = sqlx::query("SELECT stop_id,stop_sequence FROM stop_times WHERE trip_id=$1 ORDER BY stop_sequence LIMIT 1")
            .bind(trip_id).fetch_optional(db).await.map_err(internal_error)?
            .ok_or_else(|| validation_error("connection trip does not exist"))?;
        let stop_id = connection.get::<String, _>("stop_id");
        let sequence = connection.get::<i32, _>("stop_sequence");
        verify_trip_call(
            db,
            trip_id,
            &stop_id,
            sequence,
            run_id,
            service_date,
            call_id,
        )
        .await?;
    }
    let subscription = sqlx::query_scalar::<_, Value>(
        r#"INSERT INTO journey_subscriptions(user_id,device_id,run_id,service_date,trip_id,
          boarding_call_id,boarding_stop_id,boarding_stop_sequence,alighting_call_id,
          alighting_stop_id,alighting_stop_sequence,connection_run_id,connection_service_date,
          connection_trip_id,connection_call_id,minimum_transfer_seconds,significant_delay_seconds,expires_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        ON CONFLICT(device_id,run_id,boarding_call_id) DO UPDATE SET
          alighting_call_id=EXCLUDED.alighting_call_id,alighting_stop_id=EXCLUDED.alighting_stop_id,
          alighting_stop_sequence=EXCLUDED.alighting_stop_sequence,connection_run_id=EXCLUDED.connection_run_id,
          connection_service_date=EXCLUDED.connection_service_date,connection_trip_id=EXCLUDED.connection_trip_id,
          connection_call_id=EXCLUDED.connection_call_id,minimum_transfer_seconds=EXCLUDED.minimum_transfer_seconds,
          significant_delay_seconds=EXCLUDED.significant_delay_seconds,expires_at=EXCLUDED.expires_at,ended_at=NULL
        WHERE journey_subscriptions.user_id=EXCLUDED.user_id
        RETURNING to_jsonb(journey_subscriptions)-'user_id'"#,
    ).bind(user.id).bind(body.device_id).bind(body.run_id).bind(body.service_date).bind(body.trip_id)
      .bind(body.boarding_call_id).bind(body.boarding_stop_id).bind(body.boarding_stop_sequence)
      .bind(body.alighting_call_id).bind(body.alighting_stop_id).bind(body.alighting_stop_sequence)
      .bind(body.connection_run_id).bind(body.connection_service_date).bind(body.connection_trip_id)
      .bind(body.connection_call_id).bind(body.minimum_transfer_seconds).bind(body.significant_delay_seconds)
      .bind(body.expires_at).fetch_one(db).await.map_err(internal_error)?;
    Ok((StatusCode::CREATED, Json(subscription)))
}

pub(crate) async fn list_journey_subscriptions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let Some(db) = &state.db else {
        return Err(database_required());
    };
    let subscriptions = sqlx::query_scalar::<_, Value>("SELECT to_jsonb(subscription)-'user_id' FROM journey_subscriptions AS subscription WHERE user_id=$1 ORDER BY created_at DESC,id")
        .bind(user.id).fetch_all(db).await.map_err(internal_error)?;
    Ok(Json(json!({"subscriptions":subscriptions})))
}

pub(crate) async fn delete_journey_subscription(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let Some(db) = &state.db else {
        return Err(database_required());
    };
    let ended = sqlx::query("UPDATE journey_subscriptions SET ended_at=COALESCE(ended_at,now()) WHERE id=$1 AND user_id=$2")
        .bind(id).bind(user.id).execute(db).await.map_err(internal_error)?.rows_affected();
    if ended == 0 {
        return Err(ApiError {
            code: "not_found".into(),
            message: "Subscription was not found".into(),
        });
    }
    Ok(Json(json!({"status":"ended"})))
}

fn validate_journey_subscription(body: &JourneySubscriptionRequest) -> Result<(), ApiError> {
    let now = Utc::now();
    if body.expires_at <= now
        || body.expires_at > now + Duration::days(14)
        || !(60..=7200).contains(&body.significant_delay_seconds)
        || body
            .minimum_transfer_seconds
            .is_some_and(|value| !(0..=7200).contains(&value))
        || body.boarding_stop_sequence < 0
        || body
            .alighting_stop_sequence
            .is_some_and(|value| value < body.boarding_stop_sequence)
    {
        return Err(validation_error("invalid journey subscription"));
    }
    let connection_fields = [
        body.connection_run_id.is_some(),
        body.connection_service_date.is_some(),
        body.connection_trip_id.is_some(),
        body.connection_call_id.is_some(),
    ];
    if connection_fields.iter().any(|value| *value) && !connection_fields.iter().all(|value| *value)
    {
        return Err(validation_error("connection identity must be complete"));
    }
    Ok(())
}

async fn verify_trip_call(
    db: &PgPool,
    trip_id: &str,
    stop_id: &str,
    stop_sequence: i32,
    run_id: &str,
    service_date: chrono::NaiveDate,
    call_id: &str,
) -> Result<(), ApiError> {
    if operational_run_id(trip_id, service_date) != run_id
        || operational_call_id(run_id, stop_id, i64::from(stop_sequence)) != call_id
    {
        return Err(validation_error(
            "run_id or call_id does not match the dated GTFS trip call",
        ));
    }
    let exists = sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM stop_times WHERE trip_id=$1 AND stop_id=$2 AND stop_sequence=$3)")
        .bind(trip_id).bind(stop_id).bind(stop_sequence).fetch_one(db).await.map_err(internal_error)?;
    if !exists {
        return Err(validation_error("GTFS trip call does not exist"));
    }
    Ok(())
}

pub(crate) async fn list_saved_routes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SavedRouteListQuery>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let Some(db) = &state.db else {
        return Err(ApiError {
            code: "database_required".into(),
            message: "Route synchronization requires the database".into(),
        });
    };
    let rows = sqlx::query_scalar::<_, Value>(
        r#"SELECT jsonb_build_object(
          'id',id,'name',name,'origin',origin,'destination',destination,'via',via,
          'via_dwell_seconds',via_dwell_seconds,'transport_modes',transport_modes,
          'preferences',preferences,'position',position,'pinned',pinned,'commute',commute,
          'version',version,'created_at',created_at,'updated_at',updated_at,'deleted_at',deleted_at)
        FROM saved_routes WHERE user_id=$1 AND ($2::timestamptz IS NULL OR updated_at>$2)
          AND ($3 OR deleted_at IS NULL)
        ORDER BY pinned DESC, position ASC, updated_at ASC, id ASC LIMIT 500"#,
    )
    .bind(user.id)
    .bind(query.since)
    .bind(query.include_deleted || query.since.is_some())
    .fetch_all(db)
    .await
    .map_err(internal_error)?;
    Ok(Json(json!({"routes":rows,"server_time":Utc::now()})))
}

pub(crate) async fn create_saved_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SavedRouteRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let user = current_user(&state, &headers).await?;
    validate_saved_route(&body)?;
    let Some(db) = &state.db else {
        return Err(database_required());
    };
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM saved_routes WHERE user_id=$1 AND deleted_at IS NULL",
    )
    .bind(user.id)
    .fetch_one(db)
    .await
    .map_err(internal_error)?;
    if count >= 100 {
        return Err(validation_error("saved route limit reached"));
    }
    let value = sqlx::query_scalar::<_, Value>(
        r#"INSERT INTO saved_routes(user_id,name,origin,destination,via,via_dwell_seconds,transport_modes,preferences,position,pinned,commute)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        RETURNING jsonb_build_object('id',id,'name',name,'origin',origin,'destination',destination,'via',via,'via_dwell_seconds',via_dwell_seconds,'transport_modes',transport_modes,'preferences',preferences,'position',position,'pinned',pinned,'commute',commute,'version',version,'created_at',created_at,'updated_at',updated_at,'deleted_at',deleted_at)"#,
    ).bind(user.id).bind(body.name.trim()).bind(body.origin).bind(body.destination).bind(body.via)
      .bind(body.via_dwell_seconds).bind(body.transport_modes).bind(body.preferences).bind(body.position)
      .bind(body.pinned).bind(body.commute).fetch_one(db).await.map_err(map_database_conflict)?;
    Ok((StatusCode::CREATED, Json(value)))
}

pub(crate) async fn update_saved_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(body): Json<SavedRouteRequest>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    validate_saved_route(&body)?;
    let expected = body
        .expected_version
        .ok_or_else(|| validation_error("expected_version is required"))?;
    let Some(db) = &state.db else {
        return Err(database_required());
    };
    let value = sqlx::query_scalar::<_, Value>(
        r#"UPDATE saved_routes SET name=$4,origin=$5,destination=$6,via=$7,via_dwell_seconds=$8,
        transport_modes=$9,preferences=$10,position=$11,pinned=$12,commute=$13
        WHERE id=$1 AND user_id=$2 AND version=$3 AND deleted_at IS NULL
        RETURNING jsonb_build_object('id',id,'name',name,'origin',origin,'destination',destination,'via',via,'via_dwell_seconds',via_dwell_seconds,'transport_modes',transport_modes,'preferences',preferences,'position',position,'pinned',pinned,'commute',commute,'version',version,'created_at',created_at,'updated_at',updated_at,'deleted_at',deleted_at)"#,
    ).bind(id).bind(user.id).bind(expected).bind(body.name.trim()).bind(body.origin).bind(body.destination)
      .bind(body.via).bind(body.via_dwell_seconds).bind(body.transport_modes).bind(body.preferences)
      .bind(body.position).bind(body.pinned).bind(body.commute).fetch_optional(db).await.map_err(map_database_conflict)?;
    value.map(Json).ok_or_else(|| ApiError {
        code: "version_conflict".into(),
        message: "Route was deleted, belongs to another account, or has a newer version".into(),
    })
}

pub(crate) async fn delete_saved_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(query): Query<SavedRouteDeleteQuery>,
) -> Result<Json<Value>, ApiError> {
    let user = current_user(&state, &headers).await?;
    let Some(db) = &state.db else {
        return Err(database_required());
    };
    let value = sqlx::query_scalar::<_, Value>(
        "UPDATE saved_routes SET deleted_at=now() WHERE id=$1 AND user_id=$2 AND version=$3 AND deleted_at IS NULL RETURNING jsonb_build_object('id',id,'version',version,'updated_at',updated_at,'deleted_at',deleted_at)",
    ).bind(id).bind(user.id).bind(query.expected_version).fetch_optional(db).await.map_err(internal_error)?;
    value.map(Json).ok_or_else(|| ApiError {
        code: "version_conflict".into(),
        message: "Route was deleted, belongs to another account, or has a newer version".into(),
    })
}

fn validate_new_password(password: &str) -> Result<(), ApiError> {
    if password.len() < 12 || password.len() > 1024 {
        return Err(validation_error(
            "new password must contain 12 to 1024 characters",
        ));
    }
    Ok(())
}

fn validate_saved_route(route: &SavedRouteRequest) -> Result<(), ApiError> {
    if route.name.trim().is_empty()
        || route.name.len() > 120
        || !route.origin.is_object()
        || !route.destination.is_object()
        || route.via.as_ref().is_some_and(|value| !value.is_object())
        || !route.preferences.is_object()
        || route.via_dwell_seconds < 0
        || route.via_dwell_seconds > 86_400
        || route.position < 0
        || route.position > 999
        || route.transport_modes.len() > 12
    {
        return Err(validation_error("invalid saved route"));
    }
    if let Some(commute) = &route.commute {
        let days_valid = commute
            .get("daysOfWeek")
            .and_then(Value::as_array)
            .is_some_and(|days| {
                !days.is_empty()
                    && days.len() <= 7
                    && days
                        .iter()
                        .all(|day| day.as_u64().is_some_and(|day| (1..=7).contains(&day)))
            });
        let arrival_valid = commute
            .get("localArrivalTime")
            .and_then(Value::as_str)
            .is_some_and(valid_local_time);
        let reminder_valid = commute
            .get("reminderLeadMinutes")
            .and_then(Value::as_u64)
            .is_some_and(|minutes| minutes <= 1440);
        let timezone_valid = commute
            .get("timezone")
            .and_then(Value::as_str)
            .is_some_and(valid_timezone);
        if !commute.is_object()
            || !days_valid
            || !arrival_valid
            || !reminder_valid
            || !timezone_valid
        {
            return Err(validation_error("invalid commute schedule"));
        }
    }
    Ok(())
}

fn valid_local_time(value: &str) -> bool {
    NaiveTime::parse_from_str(value, "%H:%M").is_ok()
}

fn valid_timezone(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.split('/').all(|part| {
            !part.is_empty()
                && part.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '+')
                })
        })
}

async fn enforce_auth_rate_limit(
    db: &PgPool,
    identifier_hash: &str,
    action: &str,
    limit: i64,
    window: &str,
) -> Result<i64, ApiError> {
    let interval = if window == "1 hour" {
        "1 hour"
    } else {
        "15 minutes"
    };
    let mut transaction = db.begin().await.map_err(internal_error)?;
    sqlx::query("SET LOCAL statement_timeout='4000ms'")
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;
    // Count and reserve under a per-account/action lock: parallel login attempts cannot all
    // pass the limit before any of them finishes its expensive password verification.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("auth:{action}:{identifier_hash}"))
        .execute(&mut *transaction)
        .await
        .map_err(internal_error)?;
    let query = format!(
        "SELECT count(*) FROM auth_attempts WHERE identifier_hash=$1 AND action=$2 AND attempted_at>now()-interval '{interval}'"
    );
    let attempts = sqlx::query_scalar::<_, i64>(&query)
        .bind(identifier_hash)
        .bind(action)
        .fetch_one(&mut *transaction)
        .await
        .map_err(internal_error)?;
    if attempts >= limit {
        return Err(ApiError {
            code: "rate_limited".into(),
            message: "Too many attempts; try again later".into(),
        });
    }
    let id = sqlx::query_scalar::<_, i64>(
        "INSERT INTO auth_attempts(identifier_hash,action) VALUES($1,$2) RETURNING id",
    )
    .bind(identifier_hash)
    .bind(action)
    .fetch_one(&mut *transaction)
    .await
    .map_err(internal_error)?;
    transaction.commit().await.map_err(internal_error)?;
    Ok(id)
}
async fn record_auth_attempt(db: &PgPool, id: i64, succeeded: bool) -> Result<(), ApiError> {
    sqlx::query("UPDATE auth_attempts SET succeeded=$2 WHERE id=$1")
        .bind(id)
        .bind(succeeded)
        .execute(db)
        .await
        .map_err(internal_error)?;
    Ok(())
}

async fn deliver_password_reset(email: &str, token: &str) -> Result<(), ApiError> {
    let reset_base = env::var("PASSWORD_RESET_URL").map_err(|_| ApiError {
        code: "delivery_not_configured".into(),
        message: "Password reset delivery is unavailable".into(),
    })?;
    let delivery_url = env::var("PASSWORD_RESET_DELIVERY_URL").map_err(|_| ApiError {
        code: "delivery_not_configured".into(),
        message: "Password reset delivery is unavailable".into(),
    })?;
    let api_key = env::var("PASSWORD_RESET_DELIVERY_API_KEY").map_err(|_| ApiError {
        code: "delivery_not_configured".into(),
        message: "Password reset delivery is unavailable".into(),
    })?;
    let mut reset_url = reqwest::Url::parse(&reset_base)
        .map_err(|_| validation_error("PASSWORD_RESET_URL is invalid"))?;
    reset_url.query_pairs_mut().append_pair("token", token);
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .map_err(internal_error)?
        .post(delivery_url)
        .bearer_auth(api_key)
        .json(&json!({"recipient":email,"template":"password_reset","resetUrl":reset_url.as_str()}))
        .send()
        .await
        .map_err(|_| ApiError {
            code: "delivery_unavailable".into(),
            message: "Password reset delivery failed".into(),
        })?;
    if !response.status().is_success() {
        return Err(ApiError {
            code: "delivery_unavailable".into(),
            message: "Password reset delivery failed".into(),
        });
    }
    Ok(())
}

fn validation_error(message: &str) -> ApiError {
    ApiError {
        code: "validation_error".into(),
        message: message.into(),
    }
}

fn database_required() -> ApiError {
    ApiError {
        code: "database_required".into(),
        message: "This operation requires database persistence".into(),
    }
}

fn map_database_conflict(error: sqlx::Error) -> ApiError {
    if error
        .as_database_error()
        .and_then(|error| error.code())
        .as_deref()
        == Some("23505")
    {
        ApiError {
            code: "conflict".into(),
            message: "A saved route with that name already exists".into(),
        }
    } else {
        internal_error(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(commute: Option<Value>) -> SavedRouteRequest {
        SavedRouteRequest {
            name: "Práce".into(),
            origin: json!({"type":"stop","id":"a"}),
            destination: json!({"type":"stop","id":"b"}),
            via: Some(json!({"type":"stop","id":"c"})),
            via_dwell_seconds: 600,
            transport_modes: vec!["train".into(), "metro".into()],
            preferences: json!({"profile":"standard"}),
            position: 0,
            pinned: true,
            commute,
            expected_version: Some(1),
        }
    }

    #[test]
    fn saved_route_accepts_complete_commute_contract() {
        let request = route(Some(json!({
            "daysOfWeek":[1,2,3,4,5],
            "localArrivalTime":"08:30",
            "reminderLeadMinutes":20,
            "timezone":"Europe/Prague"
        })));
        assert!(validate_saved_route(&request).is_ok());
    }

    #[test]
    fn saved_route_rejects_invalid_local_time_and_timezone() {
        let request = route(Some(json!({
            "daysOfWeek":[1,8],
            "localArrivalTime":"25:00",
            "reminderLeadMinutes":20,
            "timezone":""
        })));
        assert_eq!(
            validate_saved_route(&request).unwrap_err().code,
            "validation_error"
        );
    }

    #[test]
    fn password_policy_applies_to_changes_and_resets() {
        assert!(validate_new_password("short").is_err());
        assert!(validate_new_password("long-enough-password").is_ok());
    }

    #[test]
    fn journey_subscription_requires_complete_dated_connection_identity() {
        let service_date = Utc::now().date_naive();
        let trip_id = "pid_gtfs:trip-1".to_string();
        let run_id = operational_run_id(&trip_id, service_date);
        let mut request = JourneySubscriptionRequest {
            device_id: Uuid::new_v4(),
            run_id: run_id.clone(),
            service_date,
            trip_id,
            boarding_call_id: operational_call_id(&run_id, "pid_gtfs:stop-1", 1),
            boarding_stop_id: "pid_gtfs:stop-1".into(),
            boarding_stop_sequence: 1,
            alighting_call_id: None,
            alighting_stop_id: None,
            alighting_stop_sequence: Some(4),
            connection_run_id: None,
            connection_service_date: None,
            connection_trip_id: None,
            connection_call_id: None,
            minimum_transfer_seconds: None,
            significant_delay_seconds: 300,
            expires_at: Utc::now() + Duration::hours(3),
        };
        assert!(validate_journey_subscription(&request).is_ok());
        request.connection_trip_id = Some("pid_gtfs:trip-2".into());
        assert_eq!(
            validate_journey_subscription(&request).unwrap_err().code,
            "validation_error"
        );
    }
}
