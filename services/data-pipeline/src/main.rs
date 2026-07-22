use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use gtfs_importer::{GtfsDataset, ImportOptions, ValidationSeverity, parse_gtfs_zip, sha256_file};
use reqwest::{
    StatusCode,
    header::{CONTENT_LENGTH, ETAG, HeaderMap, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED},
};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tokio::{fs, io::AsyncWriteExt};
use transit_model::{AccessibilityStatus, StopLocationType, TransportMode, normalize_czech_name};
use uuid::Uuid;

const GGU_FILES: &[(&str, &str, i32)] = &[
    ("JDF_merged_GTFS.zip", "ggu_jdf_gtfs_latest", 30),
    ("CZPTT_GTFS.zip", "ggu_czptt_gtfs_latest", 20),
    ("JDF_merged.zip", "ggu_jdf_raw_latest", 40),
    ("jdf-to-gtfs.log.json", "ggu_log", 30),
    ("czptt-to-gtfs.log.json", "ggu_log", 20),
    ("fixing.log.json", "ggu_log", 30),
    ("merging.log.json", "ggu_log", 30),
    ("main.log", "ggu_log", 30),
];

const TRIP_BATCH_SIZE: usize = 10_000;
const STOP_TIME_BATCH_SIZE: usize = 10_000;
const DEFAULT_CZ_CITIES_URL: &str =
    "https://raw.githubusercontent.com/33bcdd/souradnice-mest/master/souradnice.csv";
const DEFAULT_PID_GTFS_URL: &str = "https://data.pid.cz/PID_GTFS.zip";
const DEFAULT_PID_LINES_URL: &str = "https://data.pid.cz/geodata/Linky_7d_WGS84.json";
const PID_FEED_ID: &str = "pid_gtfs";
const PID_LINES_FEED_ID: &str = "pid_lines_geodata";
const PID_SOURCE_PRIORITY: i32 = 10;
const DEFAULT_RAW_RUNS_TO_KEEP: usize = 3;
const DEFAULT_DB_IMPORT_RUNS_TO_KEEP: usize = 3;

#[derive(Debug, Clone, serde::Deserialize)]
struct ManifestEntry {
    file: String,
    downloaded: Option<bool>,
    size_bytes: Option<u64>,
    sha256: Option<String>,
    etag: Option<String>,
    last_modified: Option<String>,
    content_length: Option<u64>,
}

#[derive(Debug, Clone)]
struct RemoteFileMetadata {
    http_status: u16,
    etag: Option<String>,
    last_modified: Option<String>,
    content_length: Option<u64>,
}

#[derive(Debug, Clone)]
struct ReusableRun {
    path: PathBuf,
    manifest: HashMap<String, ManifestEntry>,
}

#[derive(Debug, Default)]
struct TripBatch {
    ids: Vec<String>,
    import_run_ids: Vec<Uuid>,
    source_feed_ids: Vec<String>,
    source_ids: Vec<String>,
    route_ids: Vec<String>,
    service_ids: Vec<String>,
    headsigns: Vec<Option<String>>,
    source_priorities: Vec<i32>,
}

impl TripBatch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            ids: Vec::with_capacity(capacity),
            import_run_ids: Vec::with_capacity(capacity),
            source_feed_ids: Vec::with_capacity(capacity),
            source_ids: Vec::with_capacity(capacity),
            route_ids: Vec::with_capacity(capacity),
            service_ids: Vec::with_capacity(capacity),
            headsigns: Vec::with_capacity(capacity),
            source_priorities: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.ids.len()
    }

    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    fn clear(&mut self) {
        self.ids.clear();
        self.import_run_ids.clear();
        self.source_feed_ids.clear();
        self.source_ids.clear();
        self.route_ids.clear();
        self.service_ids.clear();
        self.headsigns.clear();
        self.source_priorities.clear();
    }
}

#[derive(Debug, Default)]
struct StopTimeBatch {
    trip_ids: Vec<String>,
    stop_ids: Vec<String>,
    stop_sequences: Vec<i32>,
    arrival_times: Vec<i32>,
    departure_times: Vec<i32>,
    pickup_types: Vec<Option<i16>>,
    drop_off_types: Vec<Option<i16>>,
    timepoints: Vec<Option<bool>>,
    platforms: Vec<Option<String>>,
    raw_notes: Vec<Option<String>>,
    import_run_ids: Vec<Uuid>,
    source_feed_ids: Vec<String>,
    source_priorities: Vec<i32>,
}

impl StopTimeBatch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            trip_ids: Vec::with_capacity(capacity),
            stop_ids: Vec::with_capacity(capacity),
            stop_sequences: Vec::with_capacity(capacity),
            arrival_times: Vec::with_capacity(capacity),
            departure_times: Vec::with_capacity(capacity),
            pickup_types: Vec::with_capacity(capacity),
            drop_off_types: Vec::with_capacity(capacity),
            timepoints: Vec::with_capacity(capacity),
            platforms: Vec::with_capacity(capacity),
            raw_notes: Vec::with_capacity(capacity),
            import_run_ids: Vec::with_capacity(capacity),
            source_feed_ids: Vec::with_capacity(capacity),
            source_priorities: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.trip_ids.len()
    }

    fn is_empty(&self) -> bool {
        self.trip_ids.is_empty()
    }

    fn clear(&mut self) {
        self.trip_ids.clear();
        self.stop_ids.clear();
        self.stop_sequences.clear();
        self.arrival_times.clear();
        self.departure_times.clear();
        self.pickup_types.clear();
        self.drop_off_types.clear();
        self.timepoints.clear();
        self.platforms.clear();
        self.raw_notes.clear();
        self.import_run_ids.clear();
        self.source_feed_ids.clear();
        self.source_priorities.clear();
    }
}

#[derive(Debug, Parser)]
#[command(name = "data-pipeline")]
struct Cli {
    #[arg(long, env = "STORAGE_DIR", default_value = "storage")]
    storage_dir: PathBuf,
    #[arg(
        long,
        env = "GGU_LATEST_BASE_URL",
        default_value = "https://data.jr.ggu.cz/results/latest/"
    )]
    ggu_latest_base_url: String,
    #[arg(long, env = "PID_GTFS_URL", default_value = DEFAULT_PID_GTFS_URL)]
    pid_gtfs_url: String,
    #[arg(long, env = "PID_LINES_URL", default_value = DEFAULT_PID_LINES_URL)]
    pid_lines_url: String,
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Download {
        source: Source,
    },
    Import {
        source: Source,
        #[arg(long)]
        limit_rows: Option<usize>,
        #[arg(long)]
        force_db_export: bool,
    },
    Validate {
        target: Target,
    },
    ImportAndValidate {
        source: Source,
        #[arg(long)]
        limit_rows: Option<usize>,
        #[arg(long)]
        force_db_export: bool,
    },
    Summarize {
        target: Target,
    },
    ImportCities {
        #[arg(long, default_value = DEFAULT_CZ_CITIES_URL)]
        source_url: String,
    },
    SyncPid {
        #[arg(long)]
        force_db_export: bool,
    },
    RunScheduler {
        #[arg(
            long,
            env = "SCHEDULE_UPDATE_INTERVAL_SECONDS",
            default_value_t = 21_600
        )]
        interval_seconds: u64,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Source {
    GguLatest,
    Pid,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Target {
    Latest,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Download {
            source: Source::GguLatest,
        } => {
            let run_dir = download_ggu_latest(&cli.storage_dir, &cli.ggu_latest_base_url).await?;
            println!("{}", run_dir.display());
        }
        Command::Download {
            source: Source::Pid,
        } => {
            let run_dir = download_pid_gtfs(&cli.storage_dir, &cli.pid_gtfs_url).await?;
            println!("{}", run_dir.display());
        }
        Command::Import {
            source: Source::GguLatest,
            limit_rows,
            force_db_export,
        } => {
            let run_dir = latest_run_dir(&cli.storage_dir)?;
            import_ggu_latest(
                &run_dir,
                limit_rows,
                cli.database_url.as_deref(),
                force_db_export,
            )
            .await?;
        }
        Command::Import {
            source: Source::Pid,
            limit_rows,
            force_db_export,
        } => {
            let run_dir = latest_pid_run_dir(&cli.storage_dir)?;
            import_pid_gtfs(
                &run_dir,
                limit_rows,
                cli.database_url.as_deref(),
                force_db_export,
            )
            .await?;
        }
        Command::Validate {
            target: Target::Latest,
        } => {
            let run_dir = latest_run_dir(&cli.storage_dir)?;
            validate_latest(&run_dir)?;
        }
        Command::ImportAndValidate {
            source: Source::GguLatest,
            limit_rows,
            force_db_export,
        } => {
            let run_dir = download_ggu_latest(&cli.storage_dir, &cli.ggu_latest_base_url).await?;
            import_ggu_latest(
                &run_dir,
                limit_rows,
                cli.database_url.as_deref(),
                force_db_export,
            )
            .await?;
            validate_latest(&run_dir)?;
        }
        Command::ImportAndValidate {
            source: Source::Pid,
            limit_rows,
            force_db_export,
        } => {
            let run_dir = download_pid_gtfs(&cli.storage_dir, &cli.pid_gtfs_url).await?;
            import_pid_gtfs(
                &run_dir,
                limit_rows,
                cli.database_url.as_deref(),
                force_db_export,
            )
            .await?;
        }
        Command::Summarize {
            target: Target::Latest,
        } => {
            let run_dir = latest_run_dir(&cli.storage_dir)?;
            summarize(&run_dir)?;
        }
        Command::ImportCities { source_url } => {
            let database_url = cli
                .database_url
                .as_deref()
                .context("DATABASE_URL is required for import-cities")?;
            import_czech_cities(database_url, &source_url).await?;
        }
        Command::SyncPid { force_db_export } => {
            let database_url = cli
                .database_url
                .as_deref()
                .context("DATABASE_URL is required for sync-pid")?;
            sync_pid(
                &cli.storage_dir,
                database_url,
                &cli.pid_gtfs_url,
                &cli.pid_lines_url,
                force_db_export,
            )
            .await?;
        }
        Command::RunScheduler { interval_seconds } => {
            let database_url = cli
                .database_url
                .as_deref()
                .context("DATABASE_URL is required for run-scheduler")?;
            run_pid_scheduler(
                &cli.storage_dir,
                database_url,
                &cli.pid_gtfs_url,
                &cli.pid_lines_url,
                interval_seconds,
            )
            .await?;
        }
    }
    Ok(())
}

async fn import_czech_cities(database_url: &str, source_url: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(database_url)
        .await?;
    sqlx::raw_sql(include_str!(
        "../../../infra/postgres/migrations/0005_cities.sql"
    ))
    .execute(&pool)
    .await?;

    let csv_bytes = reqwest::get(source_url)
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_bytes.as_ref());
    let mut ids = Vec::new();
    let mut official_ids = Vec::new();
    let mut names = Vec::new();
    let mut normalized_names = Vec::new();
    let mut regions = Vec::new();
    let mut lats = Vec::new();
    let mut lons = Vec::new();
    let mut importances = Vec::new();

    for row in reader.records() {
        let row = row?;
        let name = row
            .get(0)
            .context("city row is missing municipality name")?;
        let official_id = row
            .get(1)
            .context("city row is missing official municipality ID")?;
        let region = row.get(4).context("city row is missing region")?;
        let lat = row
            .get(7)
            .context("city row is missing latitude")?
            .parse::<f64>()?;
        let lon = row
            .get(8)
            .context("city row is missing longitude")?
            .parse::<f64>()?;
        ids.push(format!("city:CZ:{official_id}"));
        official_ids.push(official_id.to_string());
        normalized_names.push(normalize_czech_name(name));
        importances.push(czech_city_importance(name));
        names.push(name.to_string());
        regions.push(region.to_string());
        lats.push(lat);
        lons.push(lon);
    }

    let imported: i64 = sqlx::query_scalar(
        r#"
        WITH imported AS (
          INSERT INTO cities (
            id, official_municipality_id, name, normalized_name, region,
            country_code, lat, lon, importance, source_url, source_reference_date
          )
          SELECT
            city.id, city.official_id, city.name, city.normalized_name,
            city.region, 'CZ', city.lat, city.lon, city.importance, $9, DATE '2018-01-01'
          FROM UNNEST(
            $1::text[], $2::text[], $3::text[], $4::text[],
            $5::text[], $6::double precision[], $7::double precision[], $8::integer[]
          ) AS city(
            id, official_id, name, normalized_name, region, lat, lon, importance
          )
          ON CONFLICT (id) DO UPDATE SET
            official_municipality_id = EXCLUDED.official_municipality_id,
            name = EXCLUDED.name,
            normalized_name = EXCLUDED.normalized_name,
            region = EXCLUDED.region,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            importance = EXCLUDED.importance,
            source_url = EXCLUDED.source_url,
            source_reference_date = EXCLUDED.source_reference_date
          RETURNING 1
        )
        SELECT COUNT(*) FROM imported
        "#,
    )
    .bind(&ids)
    .bind(&official_ids)
    .bind(&names)
    .bind(&normalized_names)
    .bind(&regions)
    .bind(&lats)
    .bind(&lons)
    .bind(&importances)
    .bind(source_url)
    .fetch_one(&pool)
    .await?;

    let assigned_stops = sqlx::query(
        r#"
        UPDATE stops AS stop
        SET
          city_id = (
            SELECT city.id
            FROM cities AS city
            WHERE city.country_code = 'CZ'
              AND (
                trim(regexp_replace(lower(unaccent(stop.name)), '[^a-z0-9]+', ' ', 'g')) = city.normalized_name
                OR trim(regexp_replace(lower(unaccent(stop.name)), '[^a-z0-9]+', ' ', 'g')) LIKE city.normalized_name || ' %'
              )
            ORDER BY
              CASE
                WHEN stop.lat IS NOT NULL AND stop.lon IS NOT NULL
                  THEN power(stop.lat - city.lat, 2) + power(stop.lon - city.lon, 2)
                ELSE 1
              END,
              length(city.normalized_name) DESC,
              city.importance DESC
            LIMIT 1
          ),
          city_assignment_source = 'name_fallback'
        WHERE city_assignment_source IS DISTINCT FROM 'official'
          AND EXISTS (
            SELECT 1
            FROM cities AS city
            WHERE city.country_code = 'CZ'
              AND (
                trim(regexp_replace(lower(unaccent(stop.name)), '[^a-z0-9]+', ' ', 'g')) = city.normalized_name
                OR trim(regexp_replace(lower(unaccent(stop.name)), '[^a-z0-9]+', ' ', 'g')) LIKE city.normalized_name || ' %'
              )
          )
        "#,
    )
    .execute(&pool)
    .await?
    .rows_affected();

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "source_url": source_url,
            "cities_imported": imported,
            "stops_assigned": assigned_stops
        }))?
    );
    Ok(())
}

fn czech_city_importance(name: &str) -> i32 {
    match normalize_czech_name(name).as_str() {
        "praha" => 100,
        "brno" => 90,
        "ostrava" => 85,
        "plzen" => 80,
        "liberec" | "olomouc" => 75,
        "ceske budejovice" | "hradec kralove" | "pardubice" => 70,
        _ => 0,
    }
}

async fn run_pid_scheduler(
    storage_dir: &Path,
    database_url: &str,
    gtfs_url: &str,
    lines_url: &str,
    interval_seconds: u64,
) -> Result<()> {
    let interval_seconds = interval_seconds.max(300);
    loop {
        let retry_after = if let Err(error) =
            sync_pid(storage_dir, database_url, gtfs_url, lines_url, false).await
        {
            tracing::error!(error = %error, "PID schedule synchronization failed");
            60
        } else {
            interval_seconds
        };
        tokio::time::sleep(Duration::from_secs(retry_after)).await;
    }
}

async fn sync_pid(
    storage_dir: &Path,
    database_url: &str,
    gtfs_url: &str,
    lines_url: &str,
    force_db_export: bool,
) -> Result<()> {
    let attempted_at = Utc::now();
    let result = async {
        let run_dir = download_pid_gtfs(storage_dir, gtfs_url).await?;
        import_pid_gtfs(&run_dir, None, Some(database_url), force_db_export).await?;
        sync_pid_line_geodata(database_url, lines_url).await?;
        Ok::<PathBuf, anyhow::Error>(run_dir)
    }
    .await;

    if let Ok(pool) = connect_import_database(database_url).await
        && apply_feed_migrations(&pool).await.is_ok()
    {
        let (status, succeeded_at, error_message, metadata) = match &result {
            Ok(run_dir) => (
                "success",
                Some(Utc::now()),
                None,
                serde_json::json!({"run_dir": run_dir.display().to_string()}),
            ),
            Err(error) => (
                "error",
                None,
                Some(error.to_string()),
                serde_json::json!({}),
            ),
        };
        let counts = sqlx::query(
            r#"
                SELECT
                  (SELECT COUNT(*) FROM routes WHERE source_feed_id = $1) AS routes,
                  (SELECT COUNT(*) FROM trips WHERE source_feed_id = $1) AS trips,
                  (SELECT COUNT(*) FROM stop_times WHERE source_feed_id = $1) AS stop_times
                "#,
        )
        .bind(PID_FEED_ID)
        .fetch_one(&pool)
        .await
        .ok();
        let records = counts
            .as_ref()
            .map(|row| {
                row.get::<i64, _>("routes")
                    + row.get::<i64, _>("trips")
                    + row.get::<i64, _>("stop_times")
            })
            .unwrap_or(0) as usize;
        record_data_sync(
            &pool,
            PID_FEED_ID,
            gtfs_url,
            "schedule",
            attempted_at,
            succeeded_at,
            records,
            records,
            error_message.as_deref(),
            serde_json::json!({"status": status, "details": metadata}),
        )
        .await?;
    }
    result.map(|_| ())
}

async fn download_pid_gtfs(storage_dir: &Path, url: &str) -> Result<PathBuf> {
    const FILE_NAME: &str = "PID_GTFS.zip";
    let reusable_run = latest_reusable_pid_run(storage_dir)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .connect_timeout(Duration::from_secs(30))
        .build()?;
    let previous = reusable_run
        .as_ref()
        .and_then(|run| run.manifest.get(FILE_NAME));
    let remote = fetch_remote_file_metadata(&client, url, previous).await?;

    if let Some(run) = &reusable_run
        && can_reuse_file(run, FILE_NAME, remote.as_ref())
    {
        tracing::info!(path = %run.path.display(), "PID GTFS has not changed");
        prune_raw_run_directories(storage_dir, "pid", &run.path).await?;
        return Ok(run.path.clone());
    }

    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let run_dir = storage_dir
        .join("raw")
        .join("pid")
        .join("latest")
        .join(timestamp);
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("download {url}"))?
        .error_for_status()?;
    fs::create_dir_all(&run_dir).await?;
    let status = response.status().as_u16();
    let etag = header_to_string(response.headers(), ETAG);
    let last_modified = header_to_string(response.headers(), LAST_MODIFIED);
    let content_length = header_to_u64(response.headers(), CONTENT_LENGTH);
    let output = run_dir.join(FILE_NAME);
    let mut file = fs::File::create(&output).await?;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk?).await?;
    }
    file.flush().await?;
    let size_bytes = fs::metadata(&output).await?.len();
    let sha256 = sha256_file(&output)?;
    let manifest = vec![serde_json::json!({
        "file": FILE_NAME,
        "feed_id": PID_FEED_ID,
        "priority": PID_SOURCE_PRIORITY,
        "url": url,
        "http_status": status,
        "downloaded": true,
        "size_bytes": size_bytes,
        "sha256": sha256,
        "etag": etag,
        "last_modified": last_modified,
        "content_length": content_length
    })];
    fs::write(
        run_dir.join("download-manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )
    .await?;
    prune_raw_run_directories(storage_dir, "pid", &run_dir).await?;
    Ok(run_dir)
}

fn latest_reusable_pid_run(storage_dir: &Path) -> Result<Option<ReusableRun>> {
    let root = storage_dir.join("raw").join("pid").join("latest");
    if !root.exists() {
        return Ok(None);
    }
    let mut entries = std::fs::read_dir(&root)?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries.into_iter().rev() {
        let path = entry.path();
        let manifest_path = path.join("download-manifest.json");
        if !manifest_path.exists() || !path.join("PID_GTFS.zip").exists() {
            continue;
        }
        let entries: Vec<ManifestEntry> = serde_json::from_slice(&std::fs::read(manifest_path)?)?;
        return Ok(Some(ReusableRun {
            path,
            manifest: entries
                .into_iter()
                .map(|entry| (entry.file.clone(), entry))
                .collect(),
        }));
    }
    Ok(None)
}

fn latest_pid_run_dir(storage_dir: &Path) -> Result<PathBuf> {
    latest_reusable_pid_run(storage_dir)?
        .map(|run| run.path)
        .context("no PID GTFS downloads found; run download pid first")
}

async fn import_pid_gtfs(
    run_dir: &Path,
    limit_rows: Option<usize>,
    database_url: Option<&str>,
    force_db_export: bool,
) -> Result<()> {
    const FILE_NAME: &str = "PID_GTFS.zip";
    let path = run_dir.join(FILE_NAME);
    let manifest = download_manifest_by_file(run_dir)?;
    let manifest_entry = manifest.get(FILE_NAME);
    let checksum = file_checksum(&path, manifest_entry)?;
    let pool = match database_url {
        Some(url) if !url.is_empty() => Some(connect_import_database(url).await?),
        _ => None,
    };
    if let Some(pool) = &pool {
        apply_feed_migrations(pool).await?;
        if !force_db_export
            && let Some(skip) = database_import_skip_reason(
                pool,
                &format!("{PID_FEED_ID}:{FILE_NAME}"),
                checksum.as_deref(),
            )
            .await?
        {
            println!("{}", serde_json::to_string_pretty(&skip)?);
            return Ok(());
        }
    }

    let dataset = parse_gtfs_zip(
        &path,
        ImportOptions {
            source_feed_id: PID_FEED_ID.to_string(),
            source_priority: PID_SOURCE_PRIORITY,
            limit_rows,
        },
    )?;
    let database = if let Some(pool) = &pool {
        export_dataset_to_postgres(
            pool,
            run_dir,
            FILE_NAME,
            PID_FEED_ID,
            PID_SOURCE_PRIORITY,
            &dataset,
            limit_rows.is_none(),
            manifest_entry,
            checksum.as_deref(),
        )
        .await?
    } else {
        serde_json::json!({"exported": false})
    };
    let summary = serde_json::json!({
        "file": FILE_NAME,
        "feed_id": PID_FEED_ID,
        "agencies": dataset.agencies.len(),
        "stops": dataset.stops.len(),
        "routes": dataset.routes.len(),
        "trips": dataset.trips.len(),
        "stop_times": dataset.stop_times.len(),
        "calendars": dataset.calendars.len(),
        "calendar_dates": dataset.calendar_dates.len(),
        "validation_issues": dataset.validation_issues,
        "database": database
    });
    std::fs::write(
        run_dir.join("import-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

async fn sync_pid_line_geodata(database_url: &str, url: &str) -> Result<()> {
    let pool = connect_import_database(database_url).await?;
    apply_feed_migrations(&pool).await?;
    let attempted_at = Utc::now();
    let result = sync_pid_line_geodata_inner(&pool, url, attempted_at).await;
    match &result {
        Ok((received, written)) => {
            record_data_sync(
                &pool,
                PID_LINES_FEED_ID,
                url,
                "route_geometry",
                attempted_at,
                Some(attempted_at),
                *received,
                *written,
                None,
                serde_json::json!({"format": "GeoJSON", "crs": "WGS84"}),
            )
            .await?;
        }
        Err(error) => {
            record_data_sync(
                &pool,
                PID_LINES_FEED_ID,
                url,
                "route_geometry",
                attempted_at,
                None,
                0,
                0,
                Some(&error.to_string()),
                serde_json::json!({}),
            )
            .await?;
        }
    }
    let (received, written) = result?;
    tracing::info!(received, written, "PID route geometries synchronized");
    Ok(())
}

async fn sync_pid_line_geodata_inner(
    pool: &PgPool,
    url: &str,
    fetched_at: chrono::DateTime<Utc>,
) -> Result<(usize, usize)> {
    let payload: Value = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()?
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let features = payload
        .get("features")
        .and_then(Value::as_array)
        .context("PID line GeoJSON does not contain a features array")?;
    let mut transaction = pool.begin().await?;
    let mut written = 0usize;
    for (index, feature) in features.iter().enumerate() {
        let properties = feature
            .get("properties")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let geometry = feature
            .get("geometry")
            .cloned()
            .context("PID line feature is missing geometry")?;
        let source_route_id = properties
            .get("route_id")
            .and_then(Value::as_str)
            .context("PID line feature is missing route_id")?
            .to_string();
        let source_feature_id = properties
            .get("OBJECTID")
            .map(Value::to_string)
            .unwrap_or_else(|| format!("{source_route_id}:{index}"));
        let validity = properties
            .get("validity")
            .and_then(Value::as_str)
            .map(parse_pid_validity)
            .transpose()?
            .unwrap_or_default();
        let affected = sqlx::query(
            r#"
            INSERT INTO route_geometries (
              source_feed_id, source_feature_id, route_id, source_route_id,
              validity, geometry, geom, properties, fetched_at
            )
            VALUES (
              $1, $2, $3, $4, $5, $6,
              ST_SetSRID(ST_GeomFromGeoJSON($6::text), 4326), $7, $8
            )
            ON CONFLICT (source_feed_id, source_feature_id) DO UPDATE SET
              route_id = EXCLUDED.route_id,
              source_route_id = EXCLUDED.source_route_id,
              validity = EXCLUDED.validity,
              geometry = EXCLUDED.geometry,
              geom = EXCLUDED.geom,
              properties = EXCLUDED.properties,
              fetched_at = EXCLUDED.fetched_at
            "#,
        )
        .bind(PID_LINES_FEED_ID)
        .bind(source_feature_id)
        .bind(format!("{PID_FEED_ID}:{source_route_id}"))
        .bind(&source_route_id)
        .bind(validity)
        .bind(geometry)
        .bind(properties)
        .bind(fetched_at)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        written += affected as usize;
    }
    sqlx::query("DELETE FROM route_geometries WHERE source_feed_id = $1 AND fetched_at < $2")
        .bind(PID_LINES_FEED_ID)
        .bind(fetched_at)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok((features.len(), written))
}

fn parse_pid_validity(value: &str) -> Result<Vec<chrono::NaiveDate>> {
    value
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            chrono::NaiveDate::parse_from_str(value.trim(), "%Y%m%d")
                .with_context(|| format!("invalid PID route validity date {value}"))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn record_data_sync(
    pool: &PgPool,
    source_id: &str,
    source_url: &str,
    data_kind: &str,
    attempted_at: chrono::DateTime<Utc>,
    succeeded_at: Option<chrono::DateTime<Utc>>,
    records_received: usize,
    records_written: usize,
    error_message: Option<&str>,
    metadata: Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO data_source_syncs (
          source_id, source_url, data_kind, status, last_attempt_at, last_success_at,
          source_timestamp, records_received, records_written, error_message, metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8, $9, $10)
        ON CONFLICT (source_id) DO UPDATE SET
          source_url = EXCLUDED.source_url,
          data_kind = EXCLUDED.data_kind,
          status = EXCLUDED.status,
          last_attempt_at = EXCLUDED.last_attempt_at,
          last_success_at = COALESCE(EXCLUDED.last_success_at, data_source_syncs.last_success_at),
          source_timestamp = COALESCE(EXCLUDED.source_timestamp, data_source_syncs.source_timestamp),
          records_received = EXCLUDED.records_received,
          records_written = EXCLUDED.records_written,
          error_message = EXCLUDED.error_message,
          metadata = EXCLUDED.metadata
        "#,
    )
    .bind(source_id)
    .bind(source_url)
    .bind(data_kind)
    .bind(if succeeded_at.is_some() { "success" } else { "error" })
    .bind(attempted_at)
    .bind(succeeded_at)
    .bind(records_received as i32)
    .bind(records_written as i32)
    .bind(error_message)
    .bind(metadata)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::collapsible_if)]
async fn download_ggu_latest(storage_dir: &Path, base_url: &str) -> Result<PathBuf> {
    let reusable_run = latest_reusable_ggu_run(storage_dir)?;
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .connect_timeout(Duration::from_secs(30))
        .build()?;

    let mut remote_metadata = HashMap::new();
    let mut all_files_reusable = reusable_run.is_some();
    for (file_name, _, _) in GGU_FILES {
        let url = format!("{}/{}", base_url.trim_end_matches('/'), file_name);
        let previous = reusable_run
            .as_ref()
            .and_then(|run| run.manifest.get(*file_name));
        let remote = fetch_remote_file_metadata(&client, &url, previous).await?;
        if let Some(run) = &reusable_run {
            all_files_reusable =
                all_files_reusable && can_reuse_file(run, file_name, remote.as_ref());
        }
        remote_metadata.insert((*file_name).to_string(), remote);
    }

    if all_files_reusable {
        let run = reusable_run.expect("checked above");
        write_reuse_checked_manifest(&run, base_url, &remote_metadata, &timestamp).await?;
        eprintln!("GGU latest has not changed; reusing {}", run.path.display());
        prune_raw_run_directories(storage_dir, "ggu", &run.path).await?;
        return Ok(run.path);
    }

    let run_dir = storage_dir
        .join("raw")
        .join("ggu")
        .join("latest")
        .join(timestamp);
    fs::create_dir_all(&run_dir).await?;

    let mut manifest = Vec::new();
    for (file_name, feed_id, priority) in GGU_FILES {
        let url = format!("{}/{}", base_url.trim_end_matches('/'), file_name);
        let output = run_dir.join(file_name);
        let remote = remote_metadata.get(*file_name).and_then(Clone::clone);
        if let Some(run) = &reusable_run {
            if can_reuse_file(run, file_name, remote.as_ref()) {
                let previous = run
                    .manifest
                    .get(*file_name)
                    .context("missing reusable manifest entry")?;
                let reuse_method = reuse_local_file(&run.path.join(file_name), &output).await?;
                manifest.push(serde_json::json!({
                    "file": file_name,
                    "feed_id": feed_id,
                    "priority": priority,
                    "url": url,
                    "http_status": remote.as_ref().map(|metadata| metadata.http_status),
                    "downloaded": false,
                    "reused_from": run.path.display().to_string(),
                    "reuse_method": reuse_method,
                    "size_bytes": previous.size_bytes,
                    "sha256": previous.sha256,
                    "etag": remote.as_ref().and_then(|metadata| metadata.etag.clone()).or_else(|| previous.etag.clone()),
                    "last_modified": remote.as_ref().and_then(|metadata| metadata.last_modified.clone()).or_else(|| previous.last_modified.clone()),
                    "content_length": remote.as_ref().and_then(|metadata| metadata.content_length).or(previous.content_length),
                }));
                continue;
            }
        }

        let response = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("download {url}"))?;
        let status = response.status().as_u16();
        if !response.status().is_success() {
            if let Some(run) = &reusable_run
                && let Some(previous) = run.manifest.get(*file_name)
                && run.path.join(file_name).exists()
            {
                let reuse_method = reuse_local_file(&run.path.join(file_name), &output).await?;
                manifest.push(serde_json::json!({
                    "file": file_name,
                    "feed_id": feed_id,
                    "priority": priority,
                    "url": url,
                    "http_status": status,
                    "downloaded": false,
                    "reused_after_http_error": true,
                    "reused_from": run.path.display().to_string(),
                    "reuse_method": reuse_method,
                    "size_bytes": previous.size_bytes,
                    "sha256": previous.sha256,
                    "etag": previous.etag,
                    "last_modified": previous.last_modified,
                    "content_length": previous.content_length
                }));
                continue;
            }
            manifest.push(serde_json::json!({
                "file": file_name,
                "url": url,
                "http_status": status,
                "downloaded": false
            }));
            continue;
        }

        let etag = header_to_string(response.headers(), ETAG);
        let last_modified = header_to_string(response.headers(), LAST_MODIFIED);
        let content_length = header_to_u64(response.headers(), CONTENT_LENGTH);
        let mut file = fs::File::create(&output).await?;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            file.write_all(&chunk?).await?;
        }
        file.flush().await?;
        let checksum = sha256_file(&output)?;
        let metadata = fs::metadata(&output).await?;
        manifest.push(serde_json::json!({
            "file": file_name,
            "feed_id": feed_id,
            "priority": priority,
            "url": url,
            "http_status": status,
            "downloaded": true,
            "size_bytes": metadata.len(),
            "sha256": checksum,
            "etag": etag,
            "last_modified": last_modified,
            "content_length": content_length
        }));
    }

    fs::write(
        run_dir.join("download-manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )
    .await?;
    let missing_files = GGU_FILES
        .iter()
        .filter(|(file_name, _, _)| !run_dir.join(file_name).exists())
        .map(|(file_name, _, _)| *file_name)
        .collect::<Vec<_>>();
    if !missing_files.is_empty() {
        fs::remove_dir_all(&run_dir).await?;
        anyhow::bail!(
            "GGU download did not produce required files: {}",
            missing_files.join(", ")
        );
    }
    prune_raw_run_directories(storage_dir, "ggu", &run_dir).await?;
    Ok(run_dir)
}

async fn fetch_remote_file_metadata(
    client: &reqwest::Client,
    url: &str,
    previous: Option<&ManifestEntry>,
) -> Result<Option<RemoteFileMetadata>> {
    let mut request = client.head(url);
    if let Some(etag) = previous.and_then(|entry| entry.etag.as_deref()) {
        request = request.header(IF_NONE_MATCH, etag);
    }
    if let Some(last_modified) = previous.and_then(|entry| entry.last_modified.as_deref()) {
        request = request.header(IF_MODIFIED_SINCE, last_modified);
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("check remote metadata {url}"))?;
    let status = response.status().as_u16();
    if !response.status().is_success() {
        return Ok(Some(RemoteFileMetadata {
            http_status: status,
            etag: header_to_string(response.headers(), ETAG),
            last_modified: header_to_string(response.headers(), LAST_MODIFIED),
            content_length: header_to_u64(response.headers(), CONTENT_LENGTH),
        }));
    }
    Ok(Some(RemoteFileMetadata {
        http_status: status,
        etag: header_to_string(response.headers(), ETAG),
        last_modified: header_to_string(response.headers(), LAST_MODIFIED),
        content_length: header_to_u64(response.headers(), CONTENT_LENGTH),
    }))
}

fn latest_reusable_ggu_run(storage_dir: &Path) -> Result<Option<ReusableRun>> {
    let root = storage_dir.join("raw").join("ggu").join("latest");
    if !root.exists() {
        return Ok(None);
    }
    let mut entries = std::fs::read_dir(&root)?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries.into_iter().rev() {
        let run_dir = entry.path();
        let manifest_path = run_dir.join("download-manifest.json");
        if !manifest_path.exists() {
            continue;
        }
        let manifest_entries: Vec<ManifestEntry> =
            serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        let manifest = manifest_entries
            .into_iter()
            .map(|entry| (entry.file.clone(), entry))
            .collect::<HashMap<_, _>>();
        if GGU_FILES
            .iter()
            .all(|(file_name, _, _)| run_dir.join(file_name).exists())
        {
            return Ok(Some(ReusableRun {
                path: run_dir,
                manifest,
            }));
        }
    }

    Ok(None)
}

fn can_reuse_file(run: &ReusableRun, file_name: &str, remote: Option<&RemoteFileMetadata>) -> bool {
    let Some(remote) = remote else {
        return false;
    };
    if remote.http_status != StatusCode::NOT_MODIFIED.as_u16()
        && !(200..300).contains(&remote.http_status)
    {
        return false;
    }
    if !run.path.join(file_name).exists() {
        return false;
    }
    let Some(previous) = run.manifest.get(file_name) else {
        return false;
    };
    if matches!(previous.downloaded, Some(false)) && previous.sha256.is_none() {
        return false;
    }
    if remote.http_status == StatusCode::NOT_MODIFIED.as_u16() {
        return true;
    }

    let previous_size = previous.size_bytes.or(previous.content_length);
    let size_matches = match (previous_size, remote.content_length) {
        (Some(previous), Some(remote)) => previous == remote,
        (_, None) => true,
        _ => false,
    };

    match (&previous.etag, &remote.etag) {
        (Some(previous), Some(remote)) => return previous == remote && size_matches,
        (Some(_), None) => return false,
        _ => {}
    }

    match (&previous.last_modified, &remote.last_modified) {
        (Some(previous), Some(remote)) => return previous == remote && size_matches,
        (Some(_), None) => return false,
        _ => {}
    }

    false
}

async fn reuse_local_file(source: &Path, destination: &Path) -> Result<&'static str> {
    match fs::hard_link(source, destination).await {
        Ok(()) => Ok("hard_link"),
        Err(hard_link_error) => {
            tracing::debug!(
                %hard_link_error,
                source = %source.display(),
                destination = %destination.display(),
                "hard-link reuse unavailable; copying unchanged source file"
            );
            fs::copy(source, destination).await?;
            Ok("copy")
        }
    }
}

fn raw_runs_to_keep() -> usize {
    std::env::var("RAW_IMPORT_RUNS_TO_KEEP")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_RAW_RUNS_TO_KEEP)
        .max(1)
}

fn db_import_runs_to_keep() -> usize {
    std::env::var("DB_IMPORT_RUNS_TO_KEEP")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_DB_IMPORT_RUNS_TO_KEEP)
        .max(1)
}

async fn prune_raw_run_directories(
    storage_dir: &Path,
    source: &str,
    protected_run: &Path,
) -> Result<()> {
    prune_raw_run_directories_with_limit(storage_dir, source, protected_run, raw_runs_to_keep())
        .await
}

async fn prune_raw_run_directories_with_limit(
    storage_dir: &Path,
    source: &str,
    protected_run: &Path,
    runs_to_keep: usize,
) -> Result<()> {
    let root = storage_dir.join("raw").join(source).join("latest");
    let mut entries = match fs::read_dir(&root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut runs = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if chrono::NaiveDateTime::parse_from_str(&name, "%Y%m%dT%H%M%SZ").is_ok() {
            runs.push(entry.path());
        }
    }
    runs.sort();
    let mut remove_count = runs.len().saturating_sub(runs_to_keep.max(1));
    for run in runs {
        if remove_count == 0 {
            break;
        }
        if run == protected_run {
            continue;
        }
        fs::remove_dir_all(&run)
            .await
            .with_context(|| format!("remove obsolete raw import run {}", run.display()))?;
        remove_count -= 1;
        tracing::info!(source, path = %run.display(), "deleted obsolete raw import run");
    }
    Ok(())
}

async fn write_reuse_checked_manifest(
    run: &ReusableRun,
    base_url: &str,
    remote_metadata: &HashMap<String, Option<RemoteFileMetadata>>,
    checked_at: &str,
) -> Result<()> {
    let mut manifest = Vec::new();
    for (file_name, feed_id, priority) in GGU_FILES {
        let previous = run
            .manifest
            .get(*file_name)
            .context("missing reusable manifest entry")?;
        let remote = remote_metadata.get(*file_name).and_then(Clone::clone);
        manifest.push(serde_json::json!({
            "file": file_name,
            "feed_id": feed_id,
            "priority": priority,
            "url": format!("{}/{}", base_url.trim_end_matches('/'), file_name),
            "http_status": remote.as_ref().map(|metadata| metadata.http_status),
            "downloaded": false,
            "reused_existing_run": true,
            "checked_at": checked_at,
            "size_bytes": previous.size_bytes,
            "sha256": previous.sha256,
            "etag": remote.as_ref().and_then(|metadata| metadata.etag.clone()).or_else(|| previous.etag.clone()),
            "last_modified": remote.as_ref().and_then(|metadata| metadata.last_modified.clone()).or_else(|| previous.last_modified.clone()),
            "content_length": remote.as_ref().and_then(|metadata| metadata.content_length).or(previous.content_length),
        }));
    }
    fs::write(
        run.path.join("download-manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )
    .await?;
    Ok(())
}

fn header_to_string(headers: &HeaderMap, name: reqwest::header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn header_to_u64(headers: &HeaderMap, name: reqwest::header::HeaderName) -> Option<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn latest_run_dir(storage_dir: &Path) -> Result<PathBuf> {
    let root = storage_dir.join("raw").join("ggu").join("latest");
    let mut entries = std::fs::read_dir(&root)
        .with_context(|| format!("no GGU latest downloads found under {}", root.display()))?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    entries
        .last()
        .map(|entry| entry.path())
        .context("no GGU latest run directories found")
}

#[allow(clippy::collapsible_if)]
async fn import_ggu_latest(
    run_dir: &Path,
    limit_rows: Option<usize>,
    database_url: Option<&str>,
    force_db_export: bool,
) -> Result<()> {
    let mut summary = Vec::new();
    let pool = match database_url {
        Some(url) if !url.is_empty() => Some(connect_import_database(url).await?),
        _ => None,
    };
    let manifest = download_manifest_by_file(run_dir)?;

    for (file_name, feed_id, priority) in GGU_FILES
        .iter()
        .filter(|(name, _, _)| name.ends_with("_GTFS.zip"))
    {
        let path = run_dir.join(file_name);
        if !path.exists() {
            continue;
        }
        let source = format!("{feed_id}:{file_name}");
        let manifest_entry = manifest.get(*file_name);
        let checksum = file_checksum(&path, manifest_entry)?;
        if let Some(pool) = &pool {
            if !force_db_export {
                if let Some(skip_summary) =
                    database_import_skip_reason(pool, &source, checksum.as_deref()).await?
                {
                    summary.push(serde_json::json!({
                        "file": file_name,
                        "feed_id": feed_id,
                        "database": skip_summary
                    }));
                    continue;
                }
            }
        }

        let dataset = parse_gtfs_zip(
            &path,
            ImportOptions {
                source_feed_id: (*feed_id).to_string(),
                source_priority: *priority,
                limit_rows,
            },
        )?;

        let mut db_summary = serde_json::json!({"exported": false});
        if let Some(pool) = &pool {
            db_summary = export_dataset_to_postgres(
                pool,
                run_dir,
                file_name,
                feed_id,
                *priority,
                &dataset,
                limit_rows.is_none(),
                manifest_entry,
                checksum.as_deref(),
            )
            .await?;
        }

        summary.push(serde_json::json!({
            "file": file_name,
            "feed_id": feed_id,
            "agencies": dataset.agencies.len(),
            "stops": dataset.stops.len(),
            "routes": dataset.routes.len(),
            "trips": dataset.trips.len(),
            "stop_times": dataset.stop_times.len(),
            "calendars": dataset.calendars.len(),
            "calendar_dates": dataset.calendar_dates.len(),
            "validation_issues": dataset.validation_issues,
            "database": db_summary
        }));
    }
    std::fs::write(
        run_dir.join("import-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

async fn connect_import_database(database_url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    sqlx::query("SET synchronous_commit TO off")
        .execute(&pool)
        .await?;
    Ok(pool)
}

async fn apply_feed_migrations(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(include_str!(
        "../../../infra/postgres/migrations/0005_cities.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../infra/postgres/migrations/0006_public_transport_feeds.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../infra/postgres/migrations/0015_vehicle_map_contract.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../infra/postgres/migrations/0016_data_repairs.sql"
    ))
    .execute(pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../infra/postgres/migrations/0017_stop_deduplication.sql"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

fn download_manifest_by_file(run_dir: &Path) -> Result<HashMap<String, ManifestEntry>> {
    let path = run_dir.join("download-manifest.json");
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let entries: Vec<ManifestEntry> = serde_json::from_slice(&std::fs::read(path)?)?;
    Ok(entries
        .into_iter()
        .map(|entry| (entry.file.clone(), entry))
        .collect())
}

fn file_checksum(path: &Path, manifest_entry: Option<&ManifestEntry>) -> Result<Option<String>> {
    if let Some(checksum) = manifest_entry.and_then(|entry| entry.sha256.clone()) {
        return Ok(Some(checksum));
    }
    if path.exists() {
        return Ok(Some(sha256_file(path)?));
    }
    Ok(None)
}

async fn database_import_skip_reason(
    pool: &PgPool,
    source: &str,
    checksum: Option<&str>,
) -> Result<Option<serde_json::Value>, sqlx::Error> {
    if let Some(row) = sqlx::query(
        r#"
        SELECT id, started_at, summary
        FROM import_runs
        WHERE source = $1 AND status = 'running'
        ORDER BY started_at DESC
        LIMIT 1
        "#,
    )
    .bind(source)
    .fetch_optional(pool)
    .await?
    {
        return Ok(Some(serde_json::json!({
            "exported": false,
            "skipped": true,
            "skip_reason": "import_already_running",
            "source": source,
            "import_run_id": row.get::<Uuid, _>("id"),
            "started_at": row.get::<chrono::DateTime<Utc>, _>("started_at"),
            "summary": row.get::<Value, _>("summary")
        })));
    }

    let Some(checksum) = checksum else {
        return Ok(None);
    };

    if let Some(row) = sqlx::query(
        r#"
        SELECT id, finished_at, summary
        FROM import_runs
        WHERE source = $1
          AND status = 'success'
          AND summary->>'sha256' = $2
        ORDER BY finished_at DESC NULLS LAST, started_at DESC
        LIMIT 1
        "#,
    )
    .bind(source)
    .bind(checksum)
    .fetch_optional(pool)
    .await?
    {
        let previous_summary = row.get::<Value, _>("summary");
        if previous_summary
            .get("complete_dataset")
            .and_then(Value::as_bool)
            == Some(false)
        {
            return Ok(None);
        }
        let feed_id = source.split(':').next().unwrap_or(source);
        let has_service_calendar: bool = sqlx::query_scalar(
            r#"
            SELECT
              EXISTS (SELECT 1 FROM calendars WHERE source_feed_id = $1)
              OR EXISTS (SELECT 1 FROM calendar_dates WHERE source_feed_id = $1)
            "#,
        )
        .bind(feed_id)
        .fetch_one(pool)
        .await?;
        if previous_summary.get("calendars").is_none() || !has_service_calendar {
            return Ok(None);
        }
        return Ok(Some(serde_json::json!({
            "exported": false,
            "skipped": true,
            "skip_reason": "source_checksum_unchanged",
            "source": source,
            "sha256": checksum,
            "previous_import_run_id": row.get::<Uuid, _>("id"),
            "previous_finished_at": row.get::<Option<chrono::DateTime<Utc>>, _>("finished_at"),
            "previous_summary": previous_summary
        })));
    }

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
async fn export_dataset_to_postgres(
    pool: &PgPool,
    run_dir: &Path,
    file_name: &str,
    feed_id: &str,
    priority: i32,
    dataset: &GtfsDataset,
    complete_dataset: bool,
    manifest_entry: Option<&ManifestEntry>,
    checksum: Option<&str>,
) -> Result<serde_json::Value> {
    apply_feed_migrations(pool).await?;

    let source = format!("{feed_id}:{file_name}");
    let import_run_id: Uuid = sqlx::query_scalar(
        "INSERT INTO import_runs (source, status, summary) VALUES ($1, 'running', $2) RETURNING id",
    )
    .bind(&source)
    .bind(serde_json::json!({
        "run_dir": run_dir.display().to_string(),
        "file": file_name,
        "feed_id": feed_id,
        "complete_dataset": complete_dataset,
        "sha256": checksum,
        "size_bytes": manifest_entry.and_then(|entry| entry.size_bytes),
        "etag": manifest_entry.and_then(|entry| entry.etag.clone()),
        "last_modified": manifest_entry.and_then(|entry| entry.last_modified.clone())
    }))
    .fetch_one(pool)
    .await?;

    let mut agencies = HashSet::new();
    let mut stops = HashSet::new();
    let mut routes = HashSet::new();
    let mut trips = HashSet::new();
    let mut inserted_stop_times = 0_u64;
    let mut skipped_stop_times = 0_u64;

    for agency in &dataset.agencies {
        let id = scoped_id(feed_id, &agency.source_id);
        sqlx::query(
            r#"
            INSERT INTO agencies (id, import_run_id, source_feed_id, source_id, name, url, timezone)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE SET
              import_run_id = EXCLUDED.import_run_id,
              source_feed_id = EXCLUDED.source_feed_id,
              source_id = EXCLUDED.source_id,
              name = EXCLUDED.name,
              url = EXCLUDED.url,
              timezone = EXCLUDED.timezone
            WHERE (
              agencies.import_run_id,
              agencies.source_feed_id,
              agencies.source_id,
              agencies.name,
              agencies.url,
              agencies.timezone
            ) IS DISTINCT FROM (
              EXCLUDED.import_run_id,
              EXCLUDED.source_feed_id,
              EXCLUDED.source_id,
              EXCLUDED.name,
              EXCLUDED.url,
              EXCLUDED.timezone
            )
            "#,
        )
        .bind(&id)
        .bind(import_run_id)
        .bind(feed_id)
        .bind(&agency.source_id)
        .bind(&agency.name)
        .bind(&agency.url)
        .bind(&agency.timezone)
        .execute(pool)
        .await?;
        agencies.insert(agency.source_id.clone());
    }

    for stop in &dataset.stops {
        let id = scoped_id(feed_id, &stop.id);
        let modes = stop.modes.iter().map(mode_to_db).collect::<Vec<_>>();
        let parent_station_id = stop
            .parent_station_id
            .as_deref()
            .map(|source_id| scoped_id(feed_id, source_id));
        sqlx::query(
            r#"
            WITH resolved_city AS (
              SELECT COALESCE(
                (
                  SELECT city.id
                  FROM cities AS city
                  WHERE city.country_code = 'CZ'
                    AND city.normalized_name = $21
                  ORDER BY
                    CASE
                      WHEN $9::double precision IS NOT NULL AND $10::double precision IS NOT NULL
                        THEN power($9 - city.lat, 2) + power($10 - city.lon, 2)
                      ELSE 1
                    END,
                    city.importance DESC
                  LIMIT 1
                ),
                (
                  SELECT city.id
                  FROM cities AS city
                  WHERE city.country_code = 'CZ'
                    AND (
                      $5 = city.normalized_name
                      OR $5 LIKE city.normalized_name || ' %'
                    )
                  ORDER BY
                    CASE
                      WHEN $9::double precision IS NOT NULL AND $10::double precision IS NOT NULL
                        THEN power($9 - city.lat, 2) + power($10 - city.lon, 2)
                      ELSE 1
                    END,
                    length(city.normalized_name) DESC,
                    city.importance DESC
                  LIMIT 1
                )
              ) AS id
            )
            INSERT INTO stops (
              id, import_run_id, source_feed_id, name, normalized_name, municipality, district, region,
              lat, lon, geom, coordinate_confidence, coordinate_source, stop_area_id, platform_code,
              location_type, parent_station_id, wheelchair_boarding, modes,
              source_priority, is_active, city_id, city_assignment_source
            )
            SELECT
              $1, $2, $3, $4, $5, $6, $7, $8,
              $9, $10,
              CASE WHEN $9::double precision IS NULL OR $10::double precision IS NULL
                THEN NULL
                ELSE ST_SetSRID(ST_MakePoint($10, $9), 4326)::geography
              END,
              $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
              resolved_city.id,
              CASE WHEN resolved_city.id IS NULL THEN NULL ELSE 'name_fallback' END
            FROM resolved_city
            ON CONFLICT (id) DO UPDATE SET
              import_run_id = EXCLUDED.import_run_id,
              source_feed_id = EXCLUDED.source_feed_id,
              name = EXCLUDED.name,
              normalized_name = EXCLUDED.normalized_name,
              lat = EXCLUDED.lat,
              lon = EXCLUDED.lon,
              geom = EXCLUDED.geom,
              coordinate_confidence = EXCLUDED.coordinate_confidence,
              coordinate_source = EXCLUDED.coordinate_source,
              platform_code = EXCLUDED.platform_code,
              location_type = EXCLUDED.location_type,
              parent_station_id = EXCLUDED.parent_station_id,
              wheelchair_boarding = EXCLUDED.wheelchair_boarding,
              modes = EXCLUDED.modes,
              source_priority = EXCLUDED.source_priority,
              is_active = EXCLUDED.is_active,
              city_id = EXCLUDED.city_id,
              city_assignment_source = EXCLUDED.city_assignment_source
            WHERE (
              stops.import_run_id,
              stops.source_feed_id,
              stops.name,
              stops.normalized_name,
              stops.lat,
              stops.lon,
              stops.coordinate_confidence,
              stops.coordinate_source,
              stops.platform_code,
              stops.location_type,
              stops.parent_station_id,
              stops.wheelchair_boarding,
              stops.modes,
              stops.source_priority,
              stops.is_active,
              stops.city_id,
              stops.city_assignment_source
            ) IS DISTINCT FROM (
              EXCLUDED.import_run_id,
              EXCLUDED.source_feed_id,
              EXCLUDED.name,
              EXCLUDED.normalized_name,
              EXCLUDED.lat,
              EXCLUDED.lon,
              EXCLUDED.coordinate_confidence,
              EXCLUDED.coordinate_source,
              EXCLUDED.platform_code,
              EXCLUDED.location_type,
              EXCLUDED.parent_station_id,
              EXCLUDED.wheelchair_boarding,
              EXCLUDED.modes,
              EXCLUDED.source_priority,
              EXCLUDED.is_active,
              EXCLUDED.city_id,
              EXCLUDED.city_assignment_source
            )
            "#,
        )
        .bind(&id)
        .bind(import_run_id)
        .bind(feed_id)
        .bind(&stop.name)
        .bind(&stop.normalized_name)
        .bind(&stop.municipality)
        .bind(&stop.district)
        .bind(&stop.region)
        .bind(stop.lat)
        .bind(stop.lon)
        .bind(confidence_to_db(&stop.coordinate_confidence))
        .bind(&stop.coordinate_source)
        .bind(&stop.stop_area_id)
        .bind(&stop.platform_code)
        .bind(stop_location_type_to_db(stop.location_type))
        .bind(parent_station_id)
        .bind(accessibility_to_db(stop.wheelchair_boarding))
        .bind(&modes)
        .bind(priority)
        .bind(stop.is_active)
        .bind(
            stop.municipality
                .as_deref()
                .map(normalize_czech_name)
                .unwrap_or_default(),
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO stop_source_ids (
              stop_id, source_feed_id, original_source_id, import_run_id, priority, confidence, suppressed_as_duplicate
            )
            VALUES ($1, $2, $3, $4, $5, $6, false)
            ON CONFLICT (source_feed_id, original_source_id) DO UPDATE SET
              stop_id = EXCLUDED.stop_id,
              import_run_id = EXCLUDED.import_run_id,
              priority = EXCLUDED.priority,
              confidence = EXCLUDED.confidence,
              suppressed_as_duplicate = false
            WHERE (
              stop_source_ids.stop_id,
              stop_source_ids.import_run_id,
              stop_source_ids.priority,
              stop_source_ids.confidence,
              stop_source_ids.suppressed_as_duplicate
            ) IS DISTINCT FROM (
              EXCLUDED.stop_id,
              EXCLUDED.import_run_id,
              EXCLUDED.priority,
              EXCLUDED.confidence,
              false
            )
            "#,
        )
        .bind(&id)
        .bind(feed_id)
        .bind(&stop.id)
        .bind(import_run_id)
        .bind(priority)
        .bind(confidence_to_db(&stop.coordinate_confidence))
        .execute(pool)
        .await?;
        stops.insert(stop.id.clone());
    }

    for route in &dataset.routes {
        let id = scoped_id(feed_id, &route.id);
        let agency_id = route
            .agency_id
            .as_ref()
            .filter(|agency_id| agencies.contains(*agency_id))
            .map(|agency_id| scoped_id(feed_id, agency_id));
        sqlx::query(
            r#"
            INSERT INTO routes (
              id, import_run_id, source_feed_id, source_id, agency_id, operator_id, short_name,
              long_name, mode, gtfs_route_type, color, text_color, source_priority, is_active
            )
            VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $8, $9, $10, $11, $12, true)
            ON CONFLICT (id) DO UPDATE SET
              import_run_id = EXCLUDED.import_run_id,
              source_feed_id = EXCLUDED.source_feed_id,
              agency_id = EXCLUDED.agency_id,
              short_name = EXCLUDED.short_name,
              long_name = EXCLUDED.long_name,
              mode = EXCLUDED.mode,
              gtfs_route_type = EXCLUDED.gtfs_route_type,
              color = EXCLUDED.color,
              text_color = EXCLUDED.text_color,
              source_priority = EXCLUDED.source_priority,
              is_active = true
            WHERE (
              routes.import_run_id,
              routes.source_feed_id,
              routes.agency_id,
              routes.short_name,
              routes.long_name,
              routes.mode,
              routes.gtfs_route_type,
              routes.color,
              routes.text_color,
              routes.source_priority,
              routes.is_active
            ) IS DISTINCT FROM (
              EXCLUDED.import_run_id,
              EXCLUDED.source_feed_id,
              EXCLUDED.agency_id,
              EXCLUDED.short_name,
              EXCLUDED.long_name,
              EXCLUDED.mode,
              EXCLUDED.gtfs_route_type,
              EXCLUDED.color,
              EXCLUDED.text_color,
              EXCLUDED.source_priority,
              true
            )
            "#,
        )
        .bind(&id)
        .bind(import_run_id)
        .bind(feed_id)
        .bind(&route.source_id)
        .bind(&agency_id)
        .bind(&route.short_name)
        .bind(&route.long_name)
        .bind(mode_to_db(&route.mode))
        .bind(route.gtfs_route_type)
        .bind(&route.color)
        .bind(&route.text_color)
        .bind(route.source_priority)
        .execute(pool)
        .await?;
        routes.insert(route.id.clone());
    }

    let mut inserted_trips = 0_u64;
    let mut trip_batch = TripBatch::with_capacity(TRIP_BATCH_SIZE);
    for trip in &dataset.trips {
        if !routes.contains(&trip.route_id) {
            continue;
        }
        trip_batch.ids.push(scoped_id(feed_id, &trip.trip_id));
        trip_batch.import_run_ids.push(import_run_id);
        trip_batch.source_feed_ids.push(feed_id.to_string());
        trip_batch.source_ids.push(trip.trip_id.clone());
        trip_batch
            .route_ids
            .push(scoped_id(feed_id, &trip.route_id));
        trip_batch
            .service_ids
            .push(scoped_id(feed_id, &trip.service_id));
        trip_batch.headsigns.push(trip.trip_headsign.clone());
        trip_batch.source_priorities.push(priority);
        trips.insert(trip.trip_id.clone());

        if trip_batch.len() >= TRIP_BATCH_SIZE {
            inserted_trips += flush_trip_batch(pool, &mut trip_batch).await?;
        }
    }
    inserted_trips += flush_trip_batch(pool, &mut trip_batch).await?;

    if inserted_trips > 0 {
        tracing::info!(
            feed_id,
            inserted_trips,
            "exported trips with batched inserts"
        );
    }

    let mut stop_time_batch = StopTimeBatch::with_capacity(STOP_TIME_BATCH_SIZE);
    for stop_time in &dataset.stop_times {
        if !trips.contains(&stop_time.trip_id) || !stops.contains(&stop_time.stop_id) {
            skipped_stop_times += 1;
            continue;
        }

        stop_time_batch
            .trip_ids
            .push(scoped_id(feed_id, &stop_time.trip_id));
        stop_time_batch
            .stop_ids
            .push(scoped_id(feed_id, &stop_time.stop_id));
        stop_time_batch
            .stop_sequences
            .push(stop_time.stop_sequence as i32);
        stop_time_batch
            .arrival_times
            .push(stop_time.arrival_time as i32);
        stop_time_batch
            .departure_times
            .push(stop_time.departure_time as i32);
        stop_time_batch.pickup_types.push(stop_time.pickup_type);
        stop_time_batch.drop_off_types.push(stop_time.drop_off_type);
        stop_time_batch.timepoints.push(stop_time.timepoint);
        stop_time_batch.platforms.push(stop_time.platform.clone());
        stop_time_batch.raw_notes.push(stop_time.raw_notes.clone());
        stop_time_batch.import_run_ids.push(import_run_id);
        stop_time_batch.source_feed_ids.push(feed_id.to_string());
        stop_time_batch.source_priorities.push(priority);

        if stop_time_batch.len() >= STOP_TIME_BATCH_SIZE {
            inserted_stop_times += flush_stop_time_batch(pool, &mut stop_time_batch).await?;
        }
    }
    inserted_stop_times += flush_stop_time_batch(pool, &mut stop_time_batch).await?;

    if inserted_stop_times > 0 {
        tracing::info!(
            feed_id,
            inserted_stop_times,
            skipped_stop_times,
            "exported stop_times with batched inserts"
        );
    }

    let (inserted_calendars, inserted_calendar_dates) =
        export_service_calendars(pool, dataset, feed_id, import_run_id).await?;

    for issue in &dataset.validation_issues {
        sqlx::query(
            r#"
            INSERT INTO validation_issues (
              import_run_id, source_feed_id, severity, code, message, source_file, affected_entity, raw_payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(import_run_id)
        .bind(feed_id)
        .bind(severity_to_db(&issue.severity))
        .bind(&issue.code)
        .bind(&issue.message)
        .bind(&issue.source_file)
        .bind(&issue.affected_entity)
        .bind(&issue.raw_payload)
        .execute(pool)
        .await?;
    }

    let stale_cleanup = if complete_dataset {
        prune_stale_feed_schedule_rows(pool, feed_id, import_run_id).await?
    } else {
        serde_json::json!({
            "skipped": true,
            "reason": "partial import created with --limit-rows"
        })
    };

    // These database-side repairs are deliberately conservative. Exact cross-feed aliases may be
    // confirmed automatically; reviewed nearby mappings are retained and re-applied after imports.
    let safe_repairs =
        sqlx::query_scalar::<_, serde_json::Value>("SELECT cesta_apply_safe_data_repairs()")
            .fetch_one(pool)
            .await?;
    let confirmed_stop_merges =
        sqlx::query_scalar::<_, serde_json::Value>("SELECT cesta_apply_confirmed_stop_merges()")
            .fetch_one(pool)
            .await?;
    let automatic_repair_summary = serde_json::json!({
        "safe_repairs": safe_repairs,
        "confirmed_stop_merges": confirmed_stop_merges,
        "import_run_id": import_run_id,
        "source_feed_id": feed_id
    });
    sqlx::query(
        r#"
        INSERT INTO data_repair_runs (repair_type, status, summary, finished_at)
        VALUES ('automatic_after_import', 'completed', $1, now())
        "#,
    )
    .bind(&automatic_repair_summary)
    .execute(pool)
    .await?;

    let mut summary = serde_json::json!({
        "exported": true,
        "import_run_id": import_run_id,
        "source": source,
        "run_dir": run_dir.display().to_string(),
        "file": file_name,
        "feed_id": feed_id,
        "sha256": checksum,
        "size_bytes": manifest_entry.and_then(|entry| entry.size_bytes),
        "etag": manifest_entry.and_then(|entry| entry.etag.clone()),
        "last_modified": manifest_entry.and_then(|entry| entry.last_modified.clone()),
        "agencies": agencies.len(),
        "stops": stops.len(),
        "routes": routes.len(),
        "trips": trips.len(),
        "stop_times": inserted_stop_times,
        "calendars": inserted_calendars,
        "calendar_dates": inserted_calendar_dates,
        "skipped_stop_times": skipped_stop_times,
        "stale_cleanup": stale_cleanup,
        "automatic_repairs": automatic_repair_summary,
        "validation_issues": dataset.validation_issues.len()
    });

    sqlx::query(
        "UPDATE import_runs SET status = 'success', finished_at = now(), summary = $2 WHERE id = $1",
    )
    .bind(import_run_id)
    .bind(&summary)
    .execute(pool)
    .await?;

    if complete_dataset {
        let retention_cleanup =
            prune_obsolete_feed_import_data(pool, feed_id, import_run_id, db_import_runs_to_keep())
                .await?;
        summary["retention_cleanup"] = retention_cleanup;
        sqlx::query("UPDATE import_runs SET summary = $2 WHERE id = $1")
            .bind(import_run_id)
            .bind(&summary)
            .execute(pool)
            .await?;
    }

    let row = sqlx::query("SELECT COUNT(*) AS count FROM stops WHERE source_feed_id = $1")
        .bind(feed_id)
        .fetch_one(pool)
        .await?;
    let visible_stops: i64 = row.try_get("count")?;
    Ok(serde_json::json!({
        "exported": true,
        "import_run_id": import_run_id,
        "summary": summary,
        "visible_stops_for_feed": visible_stops
    }))
}

async fn export_service_calendars(
    pool: &PgPool,
    dataset: &GtfsDataset,
    feed_id: &str,
    import_run_id: Uuid,
) -> Result<(u64, u64), sqlx::Error> {
    let mut calendars = 0_u64;
    for calendar in &dataset.calendars {
        calendars += sqlx::query(
            r#"
            INSERT INTO calendars (
              service_id, monday, tuesday, wednesday, thursday, friday,
              saturday, sunday, start_date, end_date, import_run_id, source_feed_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (service_id) DO UPDATE SET
              monday = EXCLUDED.monday,
              tuesday = EXCLUDED.tuesday,
              wednesday = EXCLUDED.wednesday,
              thursday = EXCLUDED.thursday,
              friday = EXCLUDED.friday,
              saturday = EXCLUDED.saturday,
              sunday = EXCLUDED.sunday,
              start_date = EXCLUDED.start_date,
              end_date = EXCLUDED.end_date,
              import_run_id = EXCLUDED.import_run_id,
              source_feed_id = EXCLUDED.source_feed_id
            "#,
        )
        .bind(scoped_id(feed_id, &calendar.service_id))
        .bind(calendar.monday)
        .bind(calendar.tuesday)
        .bind(calendar.wednesday)
        .bind(calendar.thursday)
        .bind(calendar.friday)
        .bind(calendar.saturday)
        .bind(calendar.sunday)
        .bind(calendar.start_date)
        .bind(calendar.end_date)
        .bind(import_run_id)
        .bind(feed_id)
        .execute(pool)
        .await?
        .rows_affected();
    }

    let mut calendar_dates = 0_u64;
    for exception in &dataset.calendar_dates {
        calendar_dates += sqlx::query(
            r#"
            INSERT INTO calendar_dates (
              service_id, date, exception_type, import_run_id, source_feed_id
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (service_id, date) DO UPDATE SET
              exception_type = EXCLUDED.exception_type,
              import_run_id = EXCLUDED.import_run_id,
              source_feed_id = EXCLUDED.source_feed_id
            "#,
        )
        .bind(scoped_id(feed_id, &exception.service_id))
        .bind(exception.date)
        .bind(exception.exception_type)
        .bind(import_run_id)
        .bind(feed_id)
        .execute(pool)
        .await?
        .rows_affected();
    }
    Ok((calendars, calendar_dates))
}

async fn prune_stale_feed_schedule_rows(
    pool: &PgPool,
    feed_id: &str,
    import_run_id: Uuid,
) -> Result<serde_json::Value> {
    let deleted_stop_times = sqlx::query(
        r#"
        DELETE FROM stop_times
        WHERE source_feed_id = $1
          AND import_run_id IS DISTINCT FROM $2
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    let deleted_trips = sqlx::query(
        r#"
        DELETE FROM trips
        WHERE source_feed_id = $1
          AND import_run_id IS DISTINCT FROM $2
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    let deleted_routes = sqlx::query(
        r#"
        DELETE FROM routes
        WHERE source_feed_id = $1
          AND import_run_id IS DISTINCT FROM $2
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    let deleted_calendar_dates = sqlx::query(
        r#"
        DELETE FROM calendar_dates
        WHERE source_feed_id = $1
          AND import_run_id IS DISTINCT FROM $2
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    let deleted_calendars = sqlx::query(
        r#"
        DELETE FROM calendars
        WHERE source_feed_id = $1
          AND import_run_id IS DISTINCT FROM $2
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    let deleted_shapes = sqlx::query(
        r#"
        DELETE FROM shapes
        WHERE source_feed_id = $1
          AND import_run_id IS DISTINCT FROM $2
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    let deleted_stop_source_ids = sqlx::query(
        r#"
        DELETE FROM stop_source_ids
        WHERE source_feed_id = $1
          AND import_run_id IS DISTINCT FROM $2
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    let deactivated_stops = sqlx::query(
        r#"
        UPDATE stops
        SET is_active = false
        WHERE source_feed_id = $1
          AND import_run_id IS DISTINCT FROM $2
          AND is_active = true
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    let deleted_obsolete_transfers = sqlx::query(
        r#"
        DELETE FROM transfers transfer
        WHERE EXISTS (
            SELECT 1
            FROM stops stop
            WHERE stop.id IN (transfer.from_stop_id, transfer.to_stop_id)
              AND stop.source_feed_id = $1
              AND stop.import_run_id IS DISTINCT FROM $2
              AND stop.is_active = false
        )
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    let deleted_stops = sqlx::query(
        r#"
        DELETE FROM stops stop
        WHERE stop.source_feed_id = $1
          AND stop.import_run_id IS DISTINCT FROM $2
          AND stop.is_active = false
          AND NOT EXISTS (
            SELECT 1 FROM stop_times stop_time WHERE stop_time.stop_id = stop.id
          )
          AND NOT EXISTS (
            SELECT 1 FROM stop_source_ids source_id WHERE source_id.stop_id = stop.id
          )
          AND NOT EXISTS (
            SELECT 1
            FROM transfers transfer
            WHERE transfer.from_stop_id = stop.id OR transfer.to_stop_id = stop.id
          )
          AND NOT EXISTS (
            SELECT 1
            FROM manual_stop_matches manual_match
            WHERE manual_match.stop_id = stop.id OR manual_match.target_stop_id = stop.id
          )
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    let deleted_agencies = sqlx::query(
        r#"
        DELETE FROM agencies
        WHERE source_feed_id = $1
          AND import_run_id IS DISTINCT FROM $2
          AND NOT EXISTS (
            SELECT 1 FROM routes WHERE routes.agency_id = agencies.id
          )
        "#,
    )
    .bind(feed_id)
    .bind(import_run_id)
    .execute(pool)
    .await?
    .rows_affected();

    Ok(serde_json::json!({
        "deleted_stop_times": deleted_stop_times,
        "deleted_trips": deleted_trips,
        "deleted_routes": deleted_routes,
        "deleted_calendars": deleted_calendars,
        "deleted_calendar_dates": deleted_calendar_dates,
        "deleted_shapes": deleted_shapes,
        "deleted_stop_source_ids": deleted_stop_source_ids,
        "deactivated_stops": deactivated_stops,
        "deleted_obsolete_transfers": deleted_obsolete_transfers,
        "deleted_stops": deleted_stops,
        "deleted_agencies": deleted_agencies
    }))
}

async fn prune_obsolete_feed_import_data(
    pool: &PgPool,
    feed_id: &str,
    current_import_run_id: Uuid,
    runs_to_keep: usize,
) -> Result<serde_json::Value> {
    let retained_import_run_ids = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT id
        FROM (
          SELECT id
          FROM import_runs
          WHERE (summary->>'feed_id' = $1 OR source LIKE $1 || ':%')
            AND status = 'success'
          ORDER BY finished_at DESC NULLS LAST, started_at DESC
          LIMIT $2
        ) retained
        UNION
        SELECT $3::uuid
        UNION
        SELECT id
        FROM import_runs
        WHERE (summary->>'feed_id' = $1 OR source LIKE $1 || ':%')
          AND status = 'running'
        "#,
    )
    .bind(feed_id)
    .bind(runs_to_keep.max(1) as i64)
    .bind(current_import_run_id)
    .fetch_all(pool)
    .await?;

    let deleted_validation_issues = sqlx::query(
        r#"
        DELETE FROM validation_issues
        WHERE source_feed_id = $1
          AND import_run_id IS NOT NULL
          AND NOT (import_run_id = ANY($2::uuid[]))
        "#,
    )
    .bind(feed_id)
    .bind(&retained_import_run_ids)
    .execute(pool)
    .await?
    .rows_affected();

    let deleted_import_runs = sqlx::query(
        r#"
        DELETE FROM import_runs run
        WHERE (run.summary->>'feed_id' = $1 OR run.source LIKE $1 || ':%')
          AND run.status <> 'running'
          AND NOT (run.id = ANY($2::uuid[]))
          AND NOT EXISTS (SELECT 1 FROM agencies item WHERE item.import_run_id = run.id)
          AND NOT EXISTS (SELECT 1 FROM stops item WHERE item.import_run_id = run.id)
          AND NOT EXISTS (SELECT 1 FROM stop_source_ids item WHERE item.import_run_id = run.id)
          AND NOT EXISTS (SELECT 1 FROM routes item WHERE item.import_run_id = run.id)
          AND NOT EXISTS (SELECT 1 FROM trips item WHERE item.import_run_id = run.id)
          AND NOT EXISTS (SELECT 1 FROM stop_times item WHERE item.import_run_id = run.id)
          AND NOT EXISTS (SELECT 1 FROM calendars item WHERE item.import_run_id = run.id)
          AND NOT EXISTS (SELECT 1 FROM calendar_dates item WHERE item.import_run_id = run.id)
          AND NOT EXISTS (SELECT 1 FROM shapes item WHERE item.import_run_id = run.id)
          AND NOT EXISTS (SELECT 1 FROM validation_issues item WHERE item.import_run_id = run.id)
        "#,
    )
    .bind(feed_id)
    .bind(&retained_import_run_ids)
    .execute(pool)
    .await?
    .rows_affected();

    Ok(serde_json::json!({
        "db_import_runs_to_keep": runs_to_keep.max(1),
        "retained_import_run_ids": retained_import_run_ids,
        "deleted_validation_issues": deleted_validation_issues,
        "deleted_import_runs": deleted_import_runs
    }))
}

async fn flush_trip_batch(pool: &PgPool, batch: &mut TripBatch) -> Result<u64, sqlx::Error> {
    if batch.is_empty() {
        return Ok(0);
    }

    let row_count = batch.len() as u64;
    sqlx::query(
        r#"
        WITH rows AS (
          SELECT *
          FROM UNNEST(
            $1::text[],
            $2::uuid[],
            $3::text[],
            $4::text[],
            $5::text[],
            $6::text[],
            $7::text[],
            $8::integer[]
          ) AS t(
            id, import_run_id, source_feed_id, source_id, route_id, service_id, headsign, source_priority
          )
        )
        INSERT INTO trips (
          id, import_run_id, source_feed_id, source_id, route_id, service_id, headsign,
          direction_id, shape_id, restrictions, raw_source_metadata, source_priority
        )
        SELECT
          id, import_run_id, source_feed_id, source_id, route_id, service_id, headsign,
          NULL::smallint, NULL::text, '{}'::jsonb, '{}'::jsonb, source_priority
        FROM rows
        ON CONFLICT (id) DO UPDATE SET
          import_run_id = EXCLUDED.import_run_id,
          source_feed_id = EXCLUDED.source_feed_id,
          route_id = EXCLUDED.route_id,
          service_id = EXCLUDED.service_id,
          headsign = EXCLUDED.headsign,
          source_priority = EXCLUDED.source_priority
        WHERE (
          trips.import_run_id,
          trips.source_feed_id,
          trips.route_id,
          trips.service_id,
          trips.headsign,
          trips.source_priority
        ) IS DISTINCT FROM (
          EXCLUDED.import_run_id,
          EXCLUDED.source_feed_id,
          EXCLUDED.route_id,
          EXCLUDED.service_id,
          EXCLUDED.headsign,
          EXCLUDED.source_priority
        )
        "#,
    )
    .bind(&batch.ids)
    .bind(&batch.import_run_ids)
    .bind(&batch.source_feed_ids)
    .bind(&batch.source_ids)
    .bind(&batch.route_ids)
    .bind(&batch.service_ids)
    .bind(&batch.headsigns)
    .bind(&batch.source_priorities)
    .execute(pool)
    .await?;
    batch.clear();
    Ok(row_count)
}

async fn flush_stop_time_batch(
    pool: &PgPool,
    batch: &mut StopTimeBatch,
) -> Result<u64, sqlx::Error> {
    if batch.is_empty() {
        return Ok(0);
    }

    let row_count = batch.len() as u64;
    sqlx::query(
        r#"
        INSERT INTO stop_times (
          trip_id, stop_id, stop_sequence, arrival_time, departure_time, pickup_type,
          drop_off_type, timepoint, platform, raw_notes, import_run_id, source_feed_id, source_priority
        )
        SELECT *
        FROM UNNEST(
          $1::text[],
          $2::text[],
          $3::integer[],
          $4::integer[],
          $5::integer[],
          $6::smallint[],
          $7::smallint[],
          $8::boolean[],
          $9::text[],
          $10::text[],
          $11::uuid[],
          $12::text[],
          $13::integer[]
        )
        ON CONFLICT (trip_id, stop_sequence) DO UPDATE SET
          stop_id = EXCLUDED.stop_id,
          arrival_time = EXCLUDED.arrival_time,
          departure_time = EXCLUDED.departure_time,
          pickup_type = EXCLUDED.pickup_type,
          drop_off_type = EXCLUDED.drop_off_type,
          timepoint = EXCLUDED.timepoint,
          platform = EXCLUDED.platform,
          raw_notes = EXCLUDED.raw_notes,
          import_run_id = EXCLUDED.import_run_id,
          source_feed_id = EXCLUDED.source_feed_id,
          source_priority = EXCLUDED.source_priority
        WHERE (
          stop_times.import_run_id,
          stop_times.stop_id,
          stop_times.arrival_time,
          stop_times.departure_time,
          stop_times.pickup_type,
          stop_times.drop_off_type,
          stop_times.timepoint,
          stop_times.platform,
          stop_times.raw_notes,
          stop_times.source_feed_id,
          stop_times.source_priority
        ) IS DISTINCT FROM (
          EXCLUDED.import_run_id,
          EXCLUDED.stop_id,
          EXCLUDED.arrival_time,
          EXCLUDED.departure_time,
          EXCLUDED.pickup_type,
          EXCLUDED.drop_off_type,
          EXCLUDED.timepoint,
          EXCLUDED.platform,
          EXCLUDED.raw_notes,
          EXCLUDED.source_feed_id,
          EXCLUDED.source_priority
        )
        "#,
    )
    .bind(&batch.trip_ids)
    .bind(&batch.stop_ids)
    .bind(&batch.stop_sequences)
    .bind(&batch.arrival_times)
    .bind(&batch.departure_times)
    .bind(&batch.pickup_types)
    .bind(&batch.drop_off_types)
    .bind(&batch.timepoints)
    .bind(&batch.platforms)
    .bind(&batch.raw_notes)
    .bind(&batch.import_run_ids)
    .bind(&batch.source_feed_ids)
    .bind(&batch.source_priorities)
    .execute(pool)
    .await?;
    batch.clear();
    Ok(row_count)
}

fn scoped_id(feed_id: &str, source_id: &str) -> String {
    format!("{feed_id}:{source_id}")
}

fn mode_to_db(mode: &TransportMode) -> &'static str {
    match mode {
        TransportMode::Train => "train",
        TransportMode::Tram => "tram",
        TransportMode::Bus => "bus",
        TransportMode::Metro => "metro",
        TransportMode::Trolleybus => "trolleybus",
        TransportMode::Ferry => "ferry",
        TransportMode::CableCar => "cable_car",
        TransportMode::Unknown => "unknown",
    }
}

fn stop_location_type_to_db(location_type: StopLocationType) -> &'static str {
    match location_type {
        StopLocationType::Stop => "stop",
        StopLocationType::Station => "station",
        StopLocationType::EntranceExit => "entrance_exit",
        StopLocationType::GenericNode => "generic_node",
        StopLocationType::BoardingArea => "boarding_area",
    }
}

fn accessibility_to_db(status: AccessibilityStatus) -> &'static str {
    match status {
        AccessibilityStatus::Unknown => "unknown",
        AccessibilityStatus::Accessible => "accessible",
        AccessibilityStatus::Inaccessible => "inaccessible",
    }
}

fn confidence_to_db(confidence: &transit_model::CoordinateConfidence) -> &'static str {
    match confidence {
        transit_model::CoordinateConfidence::Exact => "exact",
        transit_model::CoordinateConfidence::High => "high",
        transit_model::CoordinateConfidence::Medium => "medium",
        transit_model::CoordinateConfidence::Low => "low",
        transit_model::CoordinateConfidence::Unresolved => "unresolved",
    }
}

fn severity_to_db(severity: &ValidationSeverity) -> &'static str {
    match severity {
        ValidationSeverity::Info => "info",
        ValidationSeverity::Warning => "warning",
        ValidationSeverity::Error => "error",
    }
}

fn validate_latest(run_dir: &Path) -> Result<()> {
    let summary_path = run_dir.join("import-summary.json");
    let summary: serde_json::Value = serde_json::from_slice(&std::fs::read(&summary_path)?)?;
    let report = serde_json::json!({
        "run_dir": run_dir.display().to_string(),
        "validated_at": Utc::now(),
        "summary": summary,
        "checks": [
            "missing_required_gtfs_files",
            "stop_without_coordinates",
            "malformed_stop_time",
            "unsupported_route_type",
            "conversion_log_summary"
        ]
    });
    std::fs::write(
        run_dir.join("validation-report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn summarize(run_dir: &Path) -> Result<()> {
    for file in [
        "download-manifest.json",
        "import-summary.json",
        "validation-report.json",
    ] {
        let path = run_dir.join(file);
        if path.exists() {
            println!("{file}:");
            println!("{}", std::fs::read_to_string(path)?);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reusable_run(root: &Path, entry: ManifestEntry) -> ReusableRun {
        std::fs::create_dir_all(root).unwrap();
        std::fs::write(root.join(&entry.file), b"same-size").unwrap();
        ReusableRun {
            path: root.to_path_buf(),
            manifest: HashMap::from([(entry.file.clone(), entry)]),
        }
    }

    #[test]
    fn parses_pid_line_validity_dates() {
        let dates = parse_pid_validity("20260704,20260705").unwrap();
        assert_eq!(dates.len(), 2);
        assert_eq!(dates[0].to_string(), "2026-07-04");
    }

    #[test]
    fn conditional_not_modified_response_reuses_local_file() {
        let root = std::env::temp_dir().join(format!("cesta-reuse-{}", Uuid::new_v4()));
        let run = reusable_run(
            &root,
            ManifestEntry {
                file: "feed.zip".to_string(),
                downloaded: Some(true),
                size_bytes: Some(9),
                sha256: Some("checksum".to_string()),
                etag: Some("etag".to_string()),
                last_modified: None,
                content_length: Some(9),
            },
        );
        let remote = RemoteFileMetadata {
            http_status: StatusCode::NOT_MODIFIED.as_u16(),
            etag: None,
            last_modified: None,
            content_length: None,
        };

        assert!(can_reuse_file(&run, "feed.zip", Some(&remote)));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn equal_size_without_http_validators_does_not_reuse_file() {
        let root = std::env::temp_dir().join(format!("cesta-reuse-{}", Uuid::new_v4()));
        let run = reusable_run(
            &root,
            ManifestEntry {
                file: "feed.zip".to_string(),
                downloaded: Some(true),
                size_bytes: Some(9),
                sha256: Some("checksum".to_string()),
                etag: None,
                last_modified: None,
                content_length: Some(9),
            },
        );
        let remote = RemoteFileMetadata {
            http_status: StatusCode::OK.as_u16(),
            etag: None,
            last_modified: None,
            content_length: Some(9),
        };

        assert!(!can_reuse_file(&run, "feed.zip", Some(&remote)));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn raw_run_retention_removes_only_old_timestamped_directories() {
        let storage = std::env::temp_dir().join(format!("cesta-retention-{}", Uuid::new_v4()));
        let root = storage.join("raw").join("pid").join("latest");
        fs::create_dir_all(&root).await.unwrap();
        let names = [
            "20260701T000000Z",
            "20260702T000000Z",
            "20260703T000000Z",
            "20260704T000000Z",
            "20260705T000000Z",
        ];
        for name in names {
            fs::create_dir_all(root.join(name)).await.unwrap();
        }
        fs::create_dir_all(root.join("manual-backup"))
            .await
            .unwrap();
        let protected = root.join("20260705T000000Z");

        prune_raw_run_directories_with_limit(&storage, "pid", &protected, 3)
            .await
            .unwrap();

        assert!(!root.join("20260701T000000Z").exists());
        assert!(!root.join("20260702T000000Z").exists());
        assert!(root.join("20260703T000000Z").exists());
        assert!(root.join("20260704T000000Z").exists());
        assert!(protected.exists());
        assert!(root.join("manual-backup").exists());
        fs::remove_dir_all(storage).await.unwrap();
    }
}
