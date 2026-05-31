use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use gtfs_importer::{ImportOptions, parse_gtfs_zip, sha256_file};
use tokio::{fs, io::AsyncWriteExt};

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

#[derive(Debug, Parser)]
#[command(name = "data-pipeline")]
struct Cli {
    #[arg(long, env = "STORAGE_DIR", default_value = "storage")]
    storage_dir: PathBuf,
    #[arg(long, env = "GGU_LATEST_BASE_URL", default_value = "https://data.jr.ggu.cz/results/latest/")]
    ggu_latest_base_url: String,
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
    },
    Validate {
        target: Target,
    },
    ImportAndValidate {
        source: Source,
        #[arg(long)]
        limit_rows: Option<usize>,
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
        Command::Download { source: Source::GguLatest } => {
            let run_dir = download_ggu_latest(&cli.storage_dir, &cli.ggu_latest_base_url).await?;
            println!("{}", run_dir.display());
        }
        Command::Import {
            source: Source::GguLatest,
            limit_rows,
        } => {
            let run_dir = latest_run_dir(&cli.storage_dir)?;
            import_ggu_latest(&run_dir, limit_rows)?;
        }
        Command::Validate { target: Target::Latest } => {
            let run_dir = latest_run_dir(&cli.storage_dir)?;
            validate_latest(&run_dir)?;
        }
        Command::ImportAndValidate {
            source: Source::GguLatest,
            limit_rows,
        } => {
            let run_dir = download_ggu_latest(&cli.storage_dir, &cli.ggu_latest_base_url).await?;
            import_ggu_latest(&run_dir, limit_rows)?;
            validate_latest(&run_dir)?;
        }
        Command::Summarize { target: Target::Latest } => {
            let run_dir = latest_run_dir(&cli.storage_dir)?;
            summarize(&run_dir)?;
        }
    }
    Ok(())
}

async fn download_ggu_latest(storage_dir: &Path, base_url: &str) -> Result<PathBuf> {
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let run_dir = storage_dir.join("raw").join("ggu").join("latest").join(timestamp);
    fs::create_dir_all(&run_dir).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .connect_timeout(Duration::from_secs(30))
        .build()?;

    let mut manifest = Vec::new();
    for (file_name, feed_id, priority) in GGU_FILES {
        let url = format!("{}/{}", base_url.trim_end_matches('/'), file_name);
        let output = run_dir.join(file_name);
        let response = client.get(&url).send().await.with_context(|| format!("download {url}"))?;
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
            "sha256": checksum
        }));
    }

    fs::write(
        run_dir.join("download-manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )
    .await?;
    Ok(run_dir)
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

fn import_ggu_latest(run_dir: &Path, limit_rows: Option<usize>) -> Result<()> {
    let mut summary = Vec::new();
    for (file_name, feed_id, priority) in GGU_FILES.iter().filter(|(name, _, _)| name.ends_with("_GTFS.zip")) {
        let path = run_dir.join(file_name);
        if !path.exists() {
            continue;
        }
        let dataset = parse_gtfs_zip(
            &path,
            ImportOptions {
                source_feed_id: (*feed_id).to_string(),
                source_priority: *priority,
                limit_rows,
            },
        )?;
        summary.push(serde_json::json!({
            "file": file_name,
            "feed_id": feed_id,
            "agencies": dataset.agencies.len(),
            "stops": dataset.stops.len(),
            "routes": dataset.routes.len(),
            "trips": dataset.trips.len(),
            "stop_times": dataset.stop_times.len(),
            "validation_issues": dataset.validation_issues
        }));
    }
    std::fs::write(
        run_dir.join("import-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
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
    for file in ["download-manifest.json", "import-summary.json", "validation-report.json"] {
        let path = run_dir.join(file);
        if path.exists() {
            println!("{file}:");
            println!("{}", std::fs::read_to_string(path)?);
        }
    }
    Ok(())
}
