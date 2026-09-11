use std::time::Duration;

use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::time;

use crate::config::DatabasePoolConfig;

const PID_PUBLIC_QUERY_PERFORMANCE_MIGRATION: &str =
    include_str!("../../../../infra/postgres/migrations/0021_pid_public_query_performance.sql");

#[cfg(test)]
const PID_REALTIME_VEHICLE_INDEX_MIGRATION: &str =
    include_str!("../../../../infra/postgres/migrations/0028_pid_realtime_vehicle_index.sql");

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

    // The production schema is strictly ordered. Avoid re-running legacy artifact probes and
    // acquiring one advisory lock per historical migration on every API restart once the latest
    // migration is recorded. This keeps service availability independent of catalog latency while
    // the realtime worker is writing heavily.
    let schema_is_current: bool = sqlx::query_scalar(
        r#"
        SELECT
          EXISTS(
            SELECT 1 FROM cesta_schema_migrations
            WHERE version = '0021_pid_public_query_performance'
          )
          AND EXISTS(
            SELECT 1 FROM cesta_schema_migrations
            WHERE version = '0027_routing_geometries_and_walking_cache'
          )
        "#,
    )
    .fetch_one(pool)
    .await?;
    if schema_is_current {
        return Ok(());
    }

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
        "0012_stop_search_indexes",
        include_str!("../../../../infra/postgres/migrations/0012_stop_search_indexes.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0013_stop_suggester_fast_path_indexes",
        include_str!(
            "../../../../infra/postgres/migrations/0013_stop_suggester_fast_path_indexes.sql"
        ),
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
    .await?;
    apply_nontransactional_startup_migration(
        pool,
        "0019_storage_optimization",
        include_str!("../../../../infra/postgres/migrations/0019_storage_optimization.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0020_pid_only_sources",
        include_str!("../../../../infra/postgres/migrations/0020_pid_only_sources.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0021_pid_public_query_performance",
        PID_PUBLIC_QUERY_PERFORMANCE_MIGRATION,
    )
    .await?;
    apply_startup_migration(
        pool,
        "0021_pid_gtfs_transfers",
        include_str!("../../../../infra/postgres/migrations/0021_pid_gtfs_transfers.sql"),
    )
    .await?;
    apply_nontransactional_startup_migration(
        pool,
        "0022_realtime_routing_index",
        include_str!("../../../../infra/postgres/migrations/0022_realtime_routing_index.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0023_reactivate_referenced_stops",
        include_str!("../../../../infra/postgres/migrations/0023_reactivate_referenced_stops.sql"),
    )
    .await?;
    apply_nontransactional_startup_migration(
        pool,
        "0024_journey_stop_expansion_index",
        include_str!("../../../../infra/postgres/migrations/0024_journey_stop_expansion_index.sql"),
    )
    .await?;
    apply_nontransactional_startup_migration(
        pool,
        "0025_realtime_trip_summary_index",
        include_str!("../../../../infra/postgres/migrations/0025_realtime_trip_summary_index.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0026_realtime_expiry_index",
        include_str!("../../../../infra/postgres/migrations/0026_realtime_expiry_index.sql"),
    )
    .await?;
    apply_startup_migration(
        pool,
        "0027_routing_geometries_and_walking_cache",
        include_str!(
            "../../../../infra/postgres/migrations/0027_routing_geometries_and_walking_cache.sql"
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

async fn apply_nontransactional_startup_migration(
    pool: &PgPool,
    version: &str,
    statements: &str,
) -> Result<(), sqlx::Error> {
    let mut connection = pool.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock(hashtext('cesta-api-startup-migrations'))")
        .execute(&mut *connection)
        .await?;

    let migration_result = async {
        let already_applied: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM cesta_schema_migrations WHERE version = $1)",
        )
        .bind(version)
        .fetch_one(&mut *connection)
        .await?;
        if already_applied {
            return Ok(());
        }

        // PostgreSQL operations such as DROP INDEX CONCURRENTLY cannot run inside
        // a transaction. These migrations must therefore be idempotent so a crash
        // between the operation and version recording remains safe to retry.
        sqlx::raw_sql(statements).execute(&mut *connection).await?;
        sqlx::query("INSERT INTO cesta_schema_migrations (version) VALUES ($1)")
            .bind(version)
            .execute(&mut *connection)
            .await?;
        Ok::<(), sqlx::Error>(())
    }
    .await;

    let unlock_result =
        sqlx::query("SELECT pg_advisory_unlock(hashtext('cesta-api-startup-migrations'))")
            .execute(&mut *connection)
            .await;
    migration_result?;
    unlock_result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{PID_PUBLIC_QUERY_PERFORMANCE_MIGRATION, PID_REALTIME_VEHICLE_INDEX_MIGRATION};

    #[test]
    fn pid_public_stop_projection_excludes_indirect_source_mappings() {
        let migration = PID_PUBLIC_QUERY_PERFORMANCE_MIGRATION.to_ascii_lowercase();

        assert!(migration.contains("where stop.source_feed_id = 'pid_gtfs'"));
        assert!(!migration.contains("lateral"));
        assert!(!migration.contains("stop_source_ids"));
    }

    #[test]
    fn pid_public_stop_projection_has_a_matching_spatial_index() {
        let migration = PID_PUBLIC_QUERY_PERFORMANCE_MIGRATION.to_ascii_lowercase();

        assert!(migration.contains("on stops using gist (geom)"));
        assert!(migration.contains("where source_feed_id = 'pid_gtfs'"));
        assert!(migration.contains("and is_active = true"));
    }

    #[test]
    fn pid_vehicle_map_has_a_latest_position_index() {
        let migration = PID_REALTIME_VEHICLE_INDEX_MIGRATION.to_ascii_lowercase();

        assert!(migration.contains("realtime_updates_pid_vehicle_latest_idx"));
        assert!(migration.contains("(source_feed_id, vehicle_id, fetched_at desc)"));
        assert!(migration.contains("where source_feed_id = 'pid_realtime'"));
        assert!(migration.contains("create index concurrently"));
    }
}
