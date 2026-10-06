use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use axum::http::HeaderMap;
use chrono::{Duration, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use std::sync::{Arc, OnceLock};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    ApiError, AppState, AuthResponse, Claims, PublicUser, UserRecord, internal_error,
    repositories::users,
};

pub(crate) fn create_user_record(
    email: &str,
    password: &str,
    display_name: Option<String>,
    roles: Vec<String>,
) -> anyhow::Result<UserRecord> {
    let password_hash = hash_password(password)?;
    Ok(UserRecord {
        id: Uuid::new_v4(),
        email: email.to_string(),
        password_hash,
        display_name,
        roles,
        created_at: Utc::now(),
        deleted_at: None,
        auth_version: 1,
    })
}

pub(crate) fn hash_password(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|error| anyhow::anyhow!(error.to_string()))
}

pub(crate) fn verify_password(password: &str, password_hash: &str) -> Result<(), ApiError> {
    let parsed = PasswordHash::new(password_hash).map_err(|_| unauthorized())?;
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .map_err(|_| unauthorized())
}

async fn password_job<T: Send + 'static>(
    job: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    static PERMITS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    let permit = PERMITS
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(2)))
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError {
            code: "service_overloaded".into(),
            message: "Authentication capacity is temporarily exhausted".into(),
        })?;
    // Cancellation must not release capacity while Argon2 is still running.
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        job()
    })
    .await
    .map_err(internal_error)?
}
pub(crate) async fn hash_password_async(password: &str) -> Result<String, ApiError> {
    let password = Zeroizing::new(password.to_owned());
    password_job(move || hash_password(&password).map_err(internal_error)).await
}
pub(crate) async fn verify_password_async(
    password: &str,
    password_hash: &str,
) -> Result<(), ApiError> {
    if password.len() > 1024 {
        return Err(unauthorized());
    }
    let password = Zeroizing::new(password.to_owned());
    let password_hash = password_hash.to_owned();
    password_job(move || verify_password(&password, &password_hash)).await
}
pub(crate) async fn create_user_record_async(
    email: &str,
    password: &str,
    display_name: Option<String>,
    roles: Vec<String>,
) -> Result<UserRecord, ApiError> {
    Ok(UserRecord {
        id: Uuid::new_v4(),
        email: email.to_owned(),
        password_hash: hash_password_async(password).await?,
        display_name,
        roles,
        created_at: Utc::now(),
        deleted_at: None,
        auth_version: 1,
    })
}
pub(crate) fn dummy_password_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        hash_password("unavailable-account-dummy-password").expect("Argon2 configuration")
    })
}

pub(crate) async fn auth_response(
    state: &AppState,
    user: &UserRecord,
) -> Result<AuthResponse, ApiError> {
    let expires_at = Utc::now() + Duration::minutes(15);
    let claims = Claims {
        sub: user.id.to_string(),
        email: user.email.clone(),
        roles: user.roles.clone(),
        exp: expires_at.timestamp() as usize,
        auth_version: user.auth_version,
    };
    let access_token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(state.jwt_secret.as_bytes()),
    )
    .map_err(internal_error)?;
    let refresh_token = Uuid::new_v4().to_string();
    let refresh_token_hash = hash_token(&refresh_token);
    state
        .refresh_tokens
        .write()
        .await
        .insert(refresh_token_hash.clone(), user.id);
    if let Some(db) = &state.db {
        sqlx::query("INSERT INTO user_sessions(user_id,refresh_token_hash,created_at,expires_at) VALUES($1,$2,now(),now()+interval '30 days')")
            .bind(user.id).bind(refresh_token_hash).execute(db).await.map_err(internal_error)?;
    }
    Ok(AuthResponse {
        access_token,
        refresh_token,
        token_type: "Bearer".to_string(),
        expires_in_seconds: 900,
        user: public_user(user),
    })
}

pub(crate) async fn current_user(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<UserRecord, ApiError> {
    let token = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(unauthorized)?;
    let claims = decode::<Claims>(
        token,
        &DecodingKey::from_secret(state.jwt_secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| unauthorized())?
    .claims;
    let id = Uuid::parse_str(&claims.sub).map_err(|_| unauthorized())?;
    if let Some(db) = &state.db {
        let user = users::find_by_id(db, id)
            .await
            .map_err(internal_error)?
            .ok_or_else(unauthorized)?;
        if user.deleted_at.is_some() {
            return Err(unauthorized());
        }
        if claims.auth_version != user.auth_version {
            return Err(unauthorized());
        }
        state.users.write().await.insert(user.id, user.clone());
        return Ok(user);
    }
    let user = state
        .users
        .read()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(unauthorized)?;
    if user.deleted_at.is_some() || claims.auth_version != user.auth_version {
        return Err(unauthorized());
    }
    Ok(user)
}

pub(crate) async fn require_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<UserRecord, ApiError> {
    let user = current_user(state, headers).await?;
    if user
        .roles
        .iter()
        .any(|role| role == "admin" || role == "data_admin")
    {
        Ok(user)
    } else {
        Err(ApiError {
            code: "forbidden".to_string(),
            message: "Admin role is required".to_string(),
        })
    }
}

pub(crate) fn public_user(user: &UserRecord) -> PublicUser {
    let _created_at = user.created_at;
    PublicUser {
        id: user.id,
        email: user.email.clone(),
        display_name: user.display_name.clone(),
        roles: user.roles.clone(),
    }
}

pub(crate) fn hash_token(value: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn unauthorized() -> ApiError {
    ApiError {
        code: "unauthorized".to_string(),
        message: "Authentication required".to_string(),
    }
}
