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
use reqwest::header::{CONTENT_LENGTH, ETAG, HeaderMap, LAST_MODIFIED};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tokio::{fs, io::AsyncWriteExt};
use transit_model::TransportMode;
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
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Source {
    GguLatest,
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
        Command::Summarize {
            target: Target::Latest,
        } => {
            let run_dir = latest_run_dir(&cli.storage_dir)?;
            summarize(&run_dir)?;
        }
    }
    Ok(())
}

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
        let remote = fetch_remote_file_metadata(&client, &url).await?;
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
                fs::copy(run.path.join(file_name), &output).await?;
                manifest.push(serde_json::json!({
                    "file": file_name,
                    "feed_id": feed_id,
                    "priority": priority,
                    "url": url,
                    "http_status": remote.as_ref().map(|metadata| metadata.http_status),
                    "downloaded": false,
                    "reused_from": run.path.display().to_string(),
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
    Ok(run_dir)
}

async fn fetch_remote_file_metadata(
    client: &reqwest::Client,
    url: &str,
) -> Result<Option<RemoteFileMetadata>> {
    let response = client
        .head(url)
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
    if !(200..300).contains(&remote.http_status) {
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

    let previous_size = previous.size_bytes.or(previous.content_length);
    let size_matches = match (previous_size, remote.content_length) {
        (Some(previous), Some(remote)) => previous == remote,
        (_, None) => true,
        _ => false,
    };

    match (&previous.etag, &remote.etag) {
        (Some(previous), Some(remote)) => return previous == remote && size_matches,
        (Some(_), None) => return size_matches,
        _ => {}
    }

    match (&previous.last_modified, &remote.last_modified) {
        (Some(previous), Some(remote)) => return previous == remote && size_matches,
        (Some(_), None) => return size_matches,
        _ => {}
    }

    size_matches
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
            "downloaded": true,
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
        return Ok(Some(serde_json::json!({
            "exported": false,
            "skipped": true,
            "skip_reason": "source_checksum_unchanged",
            "source": source,
            "sha256": checksum,
            "previous_import_run_id": row.get::<Uuid, _>("id"),
            "previous_finished_at": row.get::<Option<chrono::DateTime<Utc>>, _>("finished_at"),
            "previous_summary": row.get::<Value, _>("summary")
        })));
    }

    Ok(None)
}

async fn export_dataset_to_postgres(
    pool: &PgPool,
    run_dir: &Path,
    file_name: &str,
    feed_id: &str,
    priority: i32,
    dataset: &GtfsDataset,
    manifest_entry: Option<&ManifestEntry>,
    checksum: Option<&str>,
) -> Result<serde_json::Value> {
    let source = format!("{feed_id}:{file_name}");
    let import_run_id: Uuid = sqlx::query_scalar(
        "INSERT INTO import_runs (source, status, summary) VALUES ($1, 'running', $2) RETURNING id",
    )
    .bind(&source)
    .bind(serde_json::json!({
        "run_dir": run_dir.display().to_string(),
        "file": file_name,
        "feed_id": feed_id,
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
              agencies.source_feed_id,
              agencies.source_id,
              agencies.name,
              agencies.url,
              agencies.timezone
            ) IS DISTINCT FROM (
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
        sqlx::query(
            r#"
            INSERT INTO stops (
              id, import_run_id, source_feed_id, name, normalized_name, municipality, district, region,
              lat, lon, geom, coordinate_confidence, coordinate_source, stop_area_id, platform_code,
              modes, source_priority, is_active
            )
            VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8,
              $9, $10,
              CASE WHEN $9::double precision IS NULL OR $10::double precision IS NULL
                THEN NULL
                ELSE ST_SetSRID(ST_MakePoint($10, $9), 4326)::geography
              END,
              $11, $12, $13, $14, $15, $16, $17
            )
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
              modes = EXCLUDED.modes,
              source_priority = EXCLUDED.source_priority,
              is_active = EXCLUDED.is_active
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
              stops.modes,
              stops.source_priority,
              stops.is_active
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
              EXCLUDED.modes,
              EXCLUDED.source_priority,
              EXCLUDED.is_active
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
        .bind(&modes)
        .bind(priority)
        .bind(stop.is_active)
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
              stop_source_ids.priority,
              stop_source_ids.confidence,
              stop_source_ids.suppressed_as_duplicate
            ) IS DISTINCT FROM (
              EXCLUDED.stop_id,
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
        trip_batch.service_ids.push(trip.service_id.clone());
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

    let stale_cleanup = prune_stale_feed_schedule_rows(pool, feed_id, import_run_id).await?;

    let summary = serde_json::json!({
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
        "skipped_stop_times": skipped_stop_times,
        "stale_cleanup": stale_cleanup,
        "validation_issues": dataset.validation_issues.len()
    });

    sqlx::query(
        "UPDATE import_runs SET status = 'success', finished_at = now(), summary = $2 WHERE id = $1",
    )
    .bind(import_run_id)
    .bind(&summary)
    .execute(pool)
    .await?;

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

    Ok(serde_json::json!({
        "deleted_stop_times": deleted_stop_times,
        "deleted_trips": deleted_trips,
        "deleted_routes": deleted_routes
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
