use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::UserRecord;

pub(crate) async fn find_by_email(
    pool: &PgPool,
    email: &str,
) -> Result<Option<UserRecord>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT id,email,password_hash,display_name,created_at,deleted_at FROM users WHERE lower(email)=lower($1) AND deleted_at IS NULL",
    )
    .bind(email)
    .fetch_optional(pool)
    .await?;
    user_from_row(pool, row).await
}

pub(crate) async fn find_by_id(pool: &PgPool, id: Uuid) -> Result<Option<UserRecord>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT id,email,password_hash,display_name,created_at,deleted_at FROM users WHERE id=$1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    user_from_row(pool, row).await
}

async fn user_from_row(
    pool: &PgPool,
    row: Option<sqlx::postgres::PgRow>,
) -> Result<Option<UserRecord>, sqlx::Error> {
    let Some(row) = row else { return Ok(None) };
    let id: Uuid = row.get("id");
    let roles = sqlx::query_scalar::<_, String>(
        "SELECT role FROM user_roles WHERE user_id=$1 ORDER BY role",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    Ok(Some(UserRecord {
        id,
        email: row.get("email"),
        password_hash: row.get("password_hash"),
        display_name: row.get("display_name"),
        roles,
        created_at: row.get("created_at"),
        deleted_at: row.get("deleted_at"),
    }))
}
