use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, bail};

const DEVELOPMENT_JWT_SECRET: &str = "dev-only-change-me";

#[derive(Clone, Debug)]
pub(crate) struct AppConfig {
    pub(crate) bind_address: SocketAddr,
    pub(crate) cors_allowed_origins: Vec<String>,
    pub(crate) database_url: Option<String>,
    pub(crate) database_pool: DatabasePoolConfig,
    pub(crate) jwt_secret: String,
    pub(crate) request_body_limit_bytes: usize,
    pub(crate) routing_snapshot_dir: PathBuf,
    pub(crate) use_mock_data: bool,
    pub(crate) production: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct DatabasePoolConfig {
    pub(crate) min_connections: u32,
    pub(crate) max_connections: u32,
    pub(crate) acquire_timeout: Duration,
}

impl AppConfig {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        let environment = env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());
        let production = environment.eq_ignore_ascii_case("production");
        let use_mock_data = parse_bool("USE_MOCK_DATA", true)?;
        let port = parse_number("API_PORT", 8070_u16)?;
        let host = env::var("API_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let bind_address = format!("{host}:{port}").parse().with_context(|| {
            format!("API_HOST and API_PORT do not form a valid address: {host}:{port}")
        })?;

        let database_url = env::var("DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty());
        if !use_mock_data && database_url.is_none() {
            bail!("DATABASE_URL is required when USE_MOCK_DATA=false");
        }
        let min_connections = parse_number("DB_POOL_MIN", 2_u32)?;
        let max_connections = parse_number("DB_POOL_MAX", 10_u32)?;
        if max_connections == 0 || min_connections > max_connections {
            bail!("DB_POOL_MAX must be greater than zero and at least DB_POOL_MIN");
        }

        let jwt_secret = env::var("JWT_SECRET")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEVELOPMENT_JWT_SECRET.to_string());
        if production && (jwt_secret == DEVELOPMENT_JWT_SECRET || jwt_secret.len() < 32) {
            bail!("JWT_SECRET must be set to at least 32 characters in production");
        }

        let cors_allowed_origins = env::var("CORS_ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "*".to_string())
            .split(',')
            .map(str::trim)
            .filter(|origin| !origin.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if cors_allowed_origins.is_empty() {
            bail!("CORS_ALLOWED_ORIGINS must contain '*' or at least one origin");
        }
        for origin in cors_allowed_origins
            .iter()
            .filter(|origin| origin.as_str() != "*")
        {
            origin.parse::<axum::http::HeaderValue>().with_context(|| {
                format!("CORS_ALLOWED_ORIGINS contains an invalid origin: {origin}")
            })?;
        }
        if production && cors_allowed_origins.iter().any(|origin| origin == "*") {
            bail!("CORS_ALLOWED_ORIGINS cannot contain '*' in production");
        }

        Ok(Self {
            bind_address,
            cors_allowed_origins,
            database_url,
            database_pool: DatabasePoolConfig {
                min_connections,
                max_connections,
                acquire_timeout: Duration::from_secs(parse_number(
                    "DB_ACQUIRE_TIMEOUT_SECONDS",
                    10_u64,
                )?),
            },
            jwt_secret,
            request_body_limit_bytes: parse_number("REQUEST_BODY_LIMIT_BYTES", 1024_usize * 1024)?,
            routing_snapshot_dir: env::var("ROUTING_SNAPSHOT_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("storage").join("processed").join("routing")),
            use_mock_data,
            production,
        })
    }
}

fn parse_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) => value
            .parse::<bool>()
            .with_context(|| format!("{name} must be 'true' or 'false'")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("could not read {name}")),
    }
}

fn parse_number<T>(name: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(name) {
        Ok(value) => value
            .parse::<T>()
            .with_context(|| format!("{name} has an invalid numeric value")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("could not read {name}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_suitable_for_local_fixture_development() {
        let config = AppConfig {
            bind_address: "0.0.0.0:8070".parse().unwrap(),
            cors_allowed_origins: vec!["*".to_string()],
            database_url: None,
            database_pool: DatabasePoolConfig {
                min_connections: 2,
                max_connections: 10,
                acquire_timeout: Duration::from_secs(10),
            },
            jwt_secret: DEVELOPMENT_JWT_SECRET.to_string(),
            request_body_limit_bytes: 1024 * 1024,
            routing_snapshot_dir: PathBuf::from("storage").join("processed").join("routing"),
            use_mock_data: true,
            production: false,
        };

        assert!(config.use_mock_data);
        assert!(config.database_url.is_none());
    }
}
