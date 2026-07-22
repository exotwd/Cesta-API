use std::time::Duration;

use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::time;

use crate::config::DatabasePoolConfig;

pub(crate) async fn connect_with_retry(
    database_url: &str,
    pool_config: &DatabasePoolConfig,
) -> anyhow::Result<PgPool> {
    let mut last_error = None;
    for attempt in 1..=30 {
        match PgPoolOptions::new()
            .min_connections(pool_config.min_connections)
            .max_connections(pool_config.max_connections)
            .acquire_timeout(pool_config.acquire_timeout)
            .connect(database_url)
            .await
        {
            Ok(pool) => match apply_startup_migrations(&pool).await {
                Ok(()) => return Ok(pool),
                Err(error) => {
                    tracing::warn!(attempt, %error, "database migration failed; retrying");
                    last_error = Some(error);
                    time::sleep(Duration::from_secs(1)).await;
                }
            },
            Err(error) => {
                tracing::warn!(attempt, %error, "database is not ready yet");
                last_error = Some(error);
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    Err(anyhow::anyhow!(
        "database connection failed after retries: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    ))
}

async fn apply_startup_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS cesta_schema_migrations (
          version text PRIMARY KEY,
          applied_at timestamptz NOT NULL DEFAULT now()
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Existing installations predate migration tracking. Terminal schema artifacts prove that
    // these idempotent migrations already completed and prevent their full-table work repeating.
    sqlx::query(
        r#"
        INSERT INTO cesta_schema_migrations (version)
        SELECT '0005_cities'
        WHERE to_regclass('public.cities') IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'stops' AND column_name = 'city_id'
          )
          AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'cities'
              AND column_name = 'source_reference_date'
          )
        ON CONFLICT (version) DO NOTHING
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO cesta_schema_migrations (version)
        SELECT '0006_public_transport_feeds'
        WHERE to_regclass('public.route_geometries') IS NOT NULL
          AND to_regclass('public.data_source_syncs') IS NOT NULL
          AND EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'realtime_updates'
              AND column_name = 'source_entity_id'
          )
        ON CONFLICT (version) DO NOTHING
        "#,
    )
    .execute(pool)
    .await?;

    apply_startup_migration(
        pool,
        "0005_cities",
        include_str!("../../../../infra/postgres/migrations/0005_cities.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0006_public_transport_feeds",
        include_str!("../../../../infra/postgres/migrations/0006_public_transport_feeds.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0007_routing_algorithm_config",
        include_str!("../../../../infra/postgres/migrations/0007_routing_algorithm_config.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0008_cd_ticketing",
        include_str!("../../../../infra/postgres/migrations/0008_cd_ticketing.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0009_ticketing_journey_intents",
        include_str!("../../../../infra/postgres/migrations/0009_ticketing_journey_intents.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0010_journey_search_indexes",
        include_str!("../../../../infra/postgres/migrations/0010_journey_search_indexes.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0011_transfer_search_indexes",
        include_str!("../../../../infra/postgres/migrations/0011_transfer_search_indexes.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0014_routing_range_and_endpoint_cache",
        include_str!(
            "../../../../infra/postgres/migrations/0014_routing_range_and_endpoint_cache.sql"
        ),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0015_vehicle_map_contract",
        include_str!("../../../../infra/postgres/migrations/0015_vehicle_map_contract.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0016_data_repairs",
        include_str!("../../../../infra/postgres/migrations/0016_data_repairs.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0017_stop_deduplication",
        include_str!("../../../../infra/postgres/migrations/0017_stop_deduplication.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0018_automatic_directional_stop_merges",
        include_str!(
            "../../../../infra/postgres/migrations/0018_automatic_directional_stop_merges.sql"
        ),
    )
    .await
}

async fn apply_startup_migration(
    pool: &PgPool,
    version: &str,
    statements: &str,
) -> Result<(), sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('cesta-api-startup-migrations'))")
        .execute(&mut *transaction)
        .await?;
    let already_applied: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM cesta_schema_migrations WHERE version = $1)",
    )
    .bind(version)
    .fetch_one(&mut *transaction)
    .await?;
    if already_applied {
        transaction.commit().await?;
        return Ok(());
    }

    sqlx::raw_sql(statements).execute(&mut *transaction).await?;
    sqlx::query("INSERT INTO cesta_schema_migrations (version) VALUES ($1)")
        .bind(version)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await
}
