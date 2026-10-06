//! Admission control is independent of authentication and never trusts public proxy headers.
use crate::{ApiError, AppState};
use axum::{
    body::Body,
    extract::{ConnectInfo, MatchedPath, State},
    http::{Method, Request, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const MAX_CLIENTS: usize = 10_000;
const WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(crate) struct Security {
    clients: Arc<Mutex<HashMap<(IpAddr, &'static str), Window>>>,
    requests: Arc<Semaphore>,
    searches: Arc<Semaphore>,
}
struct Window {
    started: Instant,
    count: u32,
}

impl Security {
    pub(crate) fn new(requests: usize, searches: usize) -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            requests: Arc::new(Semaphore::new(requests)),
            searches: Arc::new(Semaphore::new(searches)),
        }
    }
    fn allow(&self, ip: IpAddr, group: &'static str, limit: u32, now: Instant) -> Result<(), u64> {
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if clients.len() >= MAX_CLIENTS {
            clients.retain(|_, window| now.duration_since(window.started) < WINDOW);
            if clients.len() >= MAX_CLIENTS && !clients.contains_key(&(ip, group)) {
                return Err(60);
            }
        }
        let window = clients.entry((ip, group)).or_insert(Window {
            started: now,
            count: 0,
        });
        if now.duration_since(window.started) >= WINDOW {
            *window = Window {
                started: now,
                count: 0,
            };
        }
        if window.count >= limit {
            return Err(60_u64
                .saturating_sub(now.duration_since(window.started).as_secs())
                .max(1));
        }
        window.count += 1;
        Ok(())
    }
}

fn client_ip(request: &Request<Body>, trust_loopback: bool) -> IpAddr {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip());
    // Caddy appends its verified client IP. Only the rightmost address is trusted, and
    // only when the actual TCP peer is a configured local proxy. Never trust X-Real-IP.
    if trust_loopback
        && peer.is_some_and(|ip| ip.is_loopback())
        && let Some(ip) = request
            .headers()
            .get("x-forwarded-for")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.rsplit(',').next())
            .and_then(|s| s.trim().parse::<IpAddr>().ok())
    {
        return normalize_ip(ip);
    }
    normalize_ip(peer.unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)))
}
fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        _ => ip,
    }
}
fn unavailable(code: &str, message: &str, retry: u64) -> Response {
    let mut response = ApiError {
        code: code.into(),
        message: message.into(),
    }
    .into_response();
    response.headers_mut().insert(
        header::RETRY_AFTER,
        retry.to_string().parse().expect("numeric Retry-After"),
    );
    response
}
pub(crate) async fn protect(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let endpoint = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str())
        .unwrap_or("unmatched")
        .to_string();
    let search = endpoint == "/journeys/search";
    let health = matches!(endpoint.as_str(), "/health" | "/ready");
    let auth = endpoint.starts_with("/auth/") && request.method() != Method::GET;
    let group = if auth {
        "auth"
    } else if search {
        "routing"
    } else {
        "public"
    };
    let limit = if auth {
        state.config.auth_requests_per_minute
    } else if search {
        state.config.routing_requests_per_minute
    } else {
        state.config.public_requests_per_minute
    };
    if !health
        && let Err(retry) = state.security.allow(
            client_ip(&request, state.config.trust_loopback_proxy),
            group,
            limit,
            Instant::now(),
        )
    {
        return unavailable("rate_limited", "Too many requests; retry later", retry);
    }
    let _request_permit = if health {
        None
    } else {
        match state.security.requests.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                return unavailable(
                    "service_overloaded",
                    "API capacity is temporarily exhausted",
                    1,
                );
            }
        }
    };
    let _search_permit = if search {
        match state.security.searches.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                return unavailable(
                    "service_overloaded",
                    "Journey search capacity is temporarily exhausted",
                    1,
                );
            }
        }
    } else {
        None
    };
    // Do not cancel account changes or ticket issuance after a timeout: their side effects
    // have independent transactional rules. Only read-only operations receive this deadline.
    if search || (request.method() == Method::GET && !endpoint.starts_with("/ticketing/")) {
        match tokio::time::timeout(state.config.read_request_timeout, next.run(request)).await {
            Ok(response) => response,
            Err(_) => unavailable("request_timeout", "The request exceeded its time budget", 1),
        }
    } else {
        next.run(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forwarded_headers_require_a_real_trusted_peer_and_use_last_hop() {
        let mut request = Request::builder()
            .header("x-forwarded-for", "1.2.3.4, 5.6.7.8")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo("9.8.7.6:5000".parse::<SocketAddr>().unwrap()));
        assert_eq!(client_ip(&request, true).to_string(), "9.8.7.6");
        request
            .extensions_mut()
            .insert(ConnectInfo("127.0.0.1:5000".parse::<SocketAddr>().unwrap()));
        assert_eq!(client_ip(&request, true).to_string(), "5.6.7.8");
        assert_eq!(client_ip(&request, false).to_string(), "127.0.0.1");
    }
    #[test]
    fn rate_limit_resets_and_keeps_endpoint_groups_independent() {
        let security = Security::new(1, 1);
        let ip = "127.0.0.1".parse().unwrap();
        let now = Instant::now();
        assert!(security.allow(ip, "routing", 1, now).is_ok());
        assert_eq!(security.allow(ip, "routing", 1, now), Err(60));
        assert!(security.allow(ip, "auth", 1, now).is_ok());
        assert!(security.allow(ip, "routing", 1, now + WINDOW).is_ok());
    }
}

pub(crate) async fn canonical_request_id(mut request: Request<Body>, next: Next) -> Response {
    if request.headers().get("x-request-id").is_some_and(|value| {
        value
            .to_str()
            .ok()
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .is_none()
    }) {
        request.headers_mut().remove("x-request-id");
    }
    next.run(request).await
}
