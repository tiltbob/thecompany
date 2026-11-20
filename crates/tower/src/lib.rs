//! An embeddable, loopback-only development relay for the numbers network.
//!
//! The tower serves plain HTTP, limits traffic received from each relay client, and sees
//! only ciphertext from traffic between endpoints. It can admit only a static list of endpoint
//! ids, and it counts what it refused.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use iroh_base::EndpointId;
use iroh_relay::server::{
    Access, AccessControl, ClientRateLimit, ClientRequest, RelayConfig, Server, ServerConfig,
};

/// The default dev tower bind address.
pub const DEFAULT_BIND: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 3340));

/// The default for [`TowerConfig::client_rate`]: 2 MiB/s per client.
pub const DEFAULT_CLIENT_RATE: u32 = 2 * 1024 * 1024;

/// The lowest accepted [`TowerConfig::client_rate`].
///
/// iroh-relay refills its token bucket every 100 ms and refuses every connection when a refill
/// would be less than one byte, so a lower rate would make the tower unusable rather than slow.
pub const MIN_CLIENT_RATE: u32 = 10;

/// The reason a denied client receives from the tower.
const DENY_REASON: &str = "not on this tower's list";

/// Configuration for a plain-HTTP development tower.
///
/// Build it with [`TowerConfig::dev`] or [`Default`], then adjust it with the builder-style
/// setters. The struct is `#[non_exhaustive]` so that later fields do not break callers outside
/// this crate. [`Tower::spawn`] checks the whole configuration.
///
/// Its `Debug` output shows how many endpoint ids are listed, never the ids themselves.
#[derive(Clone)]
#[non_exhaustive]
pub struct TowerConfig {
    /// A loopback socket address. Port zero lets the operating system choose a port.
    pub bind: SocketAddr,
    /// The endpoint ids that may use the relay, as text (hex or base32, as iroh prints them).
    ///
    /// `None` admits every client. `Some` admits only the listed ids, so an empty list admits
    /// nobody. [`Tower::spawn`] parses every entry and fails on the first one that is not a
    /// valid endpoint id. Entries are not trimmed.
    ///
    /// The list gates the relay service only. Any client can still use the tower's other HTTP
    /// paths (the latency probe and the captive-portal check), so an unlisted endpoint can tell
    /// that the tower exists. A denied client is told why, never comes online, and keeps
    /// retrying: about ten times in its first ten seconds, then every 10 to 32 seconds. The list
    /// is fixed for the tower's lifetime.
    pub allow: Option<Vec<String>>,
    /// The most each client may send to the tower, in bytes per second.
    ///
    /// The tower reads more slowly from a client that sends faster; it does not drop or
    /// disconnect it. [`Tower::spawn`] refuses a value below [`MIN_CLIENT_RATE`].
    pub client_rate: u32,
    /// The most a client may send at once before the rate applies, in bytes.
    ///
    /// `None` uses iroh-relay's default, a tenth of [`Self::client_rate`]. [`Tower::spawn`]
    /// refuses zero.
    pub client_burst: Option<u32>,
}

impl TowerConfig {
    /// The development tower that listens on `bind`.
    ///
    /// A dev tower speaks plain HTTP, without TLS. It accepts only a loopback bind address, and
    /// [`Tower::spawn`] refuses any other. It has no allowlist: any client that can reach the
    /// port may use the relay. Each client may send [`DEFAULT_CLIENT_RATE`] bytes per second.
    pub fn dev(bind: SocketAddr) -> Self {
        Self {
            bind,
            allow: None,
            client_rate: DEFAULT_CLIENT_RATE,
            client_burst: None,
        }
    }

    /// Admits only the listed endpoint ids, given as text; an empty list admits nobody.
    ///
    /// This means no relay service for others, not that the tower is invisible to them: see the
    /// `allow` field.
    pub fn allow<I, S>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allow = Some(ids.into_iter().map(Into::into).collect());
        self
    }

    /// Admits every client, as a dev tower does by default.
    pub fn allow_everyone(mut self) -> Self {
        self.allow = None;
        self
    }

    /// Sets the most each client may send to the tower, in bytes per second.
    pub fn client_rate(mut self, bytes_per_second: u32) -> Self {
        self.client_rate = bytes_per_second;
        self
    }

    /// Sets the most a client may send at once before the rate applies, in bytes.
    pub fn client_burst(mut self, bytes: u32) -> Self {
        self.client_burst = Some(bytes);
        self
    }
}

impl Default for TowerConfig {
    /// The dev tower on [`DEFAULT_BIND`].
    fn default() -> Self {
        Self::dev(DEFAULT_BIND)
    }
}

impl fmt::Debug for TowerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TowerConfig")
            .field("bind", &self.bind)
            .field("allow", &self.allow.as_ref().map(|ids| Listed(ids.len())))
            .field("client_rate", &self.client_rate)
            .field("client_burst", &self.client_burst)
            .finish()
    }
}

/// Shows the size of an id list in `Debug` output instead of its entries.
struct Listed(usize);

impl fmt::Debug for Listed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} endpoint ids", self.0)
    }
}

/// An error starting or stopping a development tower.
#[derive(Debug, thiserror::Error)]
pub enum TowerError {
    /// The bind address is not a loopback address.
    #[error("the dev tower requires a loopback bind address, got {0}")]
    NonLoopbackBind(SocketAddr),
    /// An entry of [`TowerConfig::allow`] is not a valid endpoint id.
    ///
    /// Neither the message nor the source contains the entry's text.
    #[error(
        "entry {position} of the tower's allowlist (counting from 1) is not a valid endpoint id"
    )]
    InvalidAllowlistEntry {
        /// The entry's position in the list, counting from 1.
        position: usize,
        /// Why the entry did not parse.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// [`TowerConfig::client_rate`] is below [`MIN_CLIENT_RATE`].
    #[error("the tower's client rate must be at least {MIN_CLIENT_RATE} bytes per second, got {0}")]
    InvalidClientRate(u32),
    /// [`TowerConfig::client_burst`] is zero.
    #[error("the tower's client burst must be at least one byte")]
    InvalidClientBurst,
    /// The relay server failed to start.
    #[error("failed to start the dev tower: {0}")]
    Start(#[source] Box<dyn Error + Send + Sync>),
    /// The relay server started without the HTTP listener a dev tower needs.
    #[error("the dev tower started without an HTTP listener")]
    MissingHttpAddress,
    /// The relay server did not shut down cleanly.
    #[error("failed to shut down the dev tower")]
    Shutdown(#[source] Box<dyn Error + Send + Sync>),
}

/// Counters of what a running tower refused or limited since it started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TowerStats {
    /// Relay connection attempts the allowlist denied.
    ///
    /// A denied client keeps retrying with a backoff, so one client counts once per attempt.
    pub denied: u64,
    /// Client connections the rate limit slowed down at least once.
    ///
    /// This is iroh-relay's `conns_rx_ratelimited_total`. It counts a connection once, when a
    /// read first had to wait, however often it waited afterwards.
    pub rate_limited: u64,
    /// QUIC address discovery connections the tower accepted.
    ///
    /// This is iroh-relay's `qad_connections`. A dev tower has no QUIC listener, so it stays 0.
    pub qad_connections: u64,
}

/// The allowlist hook: admits only the listed endpoint ids and counts the rest.
///
/// It holds public ids only, no key, and logs nothing.
struct Allowlist {
    members: HashSet<EndpointId>,
    denied: Arc<AtomicU64>,
}

impl fmt::Debug for Allowlist {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Allowlist")
            .field("members", &Listed(self.members.len()))
            .field("denied", &self.denied.load(Ordering::Relaxed))
            .finish()
    }
}

impl AccessControl for Allowlist {
    async fn on_connect(&self, request: &ClientRequest) -> Access {
        if self.members.contains(&request.endpoint_id()) {
            Access::Allow
        } else {
            self.denied.fetch_add(1, Ordering::Relaxed);
            Access::Deny {
                reason: Some(DENY_REASON.to_owned()),
            }
        }
    }
}

/// Parses the configured ids; the error names the position of the first bad entry, not its text.
fn parse_allowlist(ids: &[String]) -> Result<HashSet<EndpointId>, TowerError> {
    ids.iter()
        .enumerate()
        .map(|(index, id)| {
            id.parse::<EndpointId>()
                .map_err(|error| TowerError::InvalidAllowlistEntry {
                    position: index + 1,
                    source: Box::new(error),
                })
        })
        .collect()
}

/// Checks the configured rate and burst and converts them for iroh-relay.
fn client_rate_limit(config: &TowerConfig) -> Result<ClientRateLimit, TowerError> {
    let bytes_per_second = NonZeroU32::new(config.client_rate)
        .filter(|rate| rate.get() >= MIN_CLIENT_RATE)
        .ok_or(TowerError::InvalidClientRate(config.client_rate))?;
    let mut limit = ClientRateLimit::new(bytes_per_second);
    if let Some(burst) = config.client_burst {
        limit.max_burst_bytes = Some(NonZeroU32::new(burst).ok_or(TowerError::InvalidClientBurst)?);
    }
    Ok(limit)
}

/// A running development tower.
pub struct Tower {
    server: Server,
    url: String,
    denied: Arc<AtomicU64>,
}

impl Tower {
    /// Starts a rate-limited HTTP relay on the configured loopback address.
    ///
    /// Fails before binding anything if the bind address is not loopback, an allowlist entry is
    /// not a valid endpoint id, or the rate or burst is out of range.
    pub async fn spawn(config: TowerConfig) -> Result<Self, TowerError> {
        if !config.bind.ip().is_loopback() {
            return Err(TowerError::NonLoopbackBind(config.bind));
        }
        let members = config.allow.as_deref().map(parse_allowlist).transpose()?;
        let rate_limit = client_rate_limit(&config)?;

        let denied = Arc::new(AtomicU64::new(0));
        let mut relay = RelayConfig::new(config.bind);
        relay.limits.client_rx = Some(rate_limit);
        if let Some(members) = members {
            relay.access = Arc::new(Allowlist {
                members,
                denied: Arc::clone(&denied),
            });
        }
        let mut server_config = ServerConfig::default();
        server_config.relay = Some(relay);

        let server = Server::spawn(server_config)
            .await
            .map_err(|error| TowerError::Start(Box::new(error)))?;
        let addr = server.http_addr().ok_or(TowerError::MissingHttpAddress)?;
        Ok(Self {
            server,
            url: format!("http://{addr}"),
            denied,
        })
    }

    /// Returns the URL of the bound listener, including the port selected by the OS.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns what the tower has refused or limited since it started.
    pub fn stats(&self) -> TowerStats {
        let metrics = &self.server.metrics().server;
        TowerStats {
            denied: self.denied.load(Ordering::Relaxed),
            rate_limited: metrics.conns_rx_ratelimited_total.get(),
            qad_connections: metrics.qad_connections.get(),
        }
    }

    /// Requests graceful shutdown and waits for the relay to stop.
    ///
    /// The tower stops accepting connections and shuts the relay server down. It does not
    /// promise that every client sees the disconnect at once: with iroh-relay 1.5, a client
    /// that connected within about the last second sometimes still reported itself connected
    /// for some seconds afterwards (8 of 180 runs), but never when shutdown came at least one
    /// second later. A test that restarts a tower should therefore check that its clients
    /// noticed.
    pub async fn shutdown(self) -> Result<(), TowerError> {
        self.server
            .shutdown()
            .await
            .map_err(|error| TowerError::Shutdown(Box::new(error)))
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    use super::{MIN_CLIENT_RATE, Tower, TowerConfig, TowerError, TowerStats};

    fn loopback() -> TowerConfig {
        TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into())
    }

    #[tokio::test]
    async fn serves_http_on_the_bound_port_and_closes_on_shutdown() {
        timeout(Duration::from_secs(5), async {
            let tower = Tower::spawn(loopback()).await.unwrap();
            let authority = tower.url().strip_prefix("http://").unwrap();
            let addr: SocketAddr = authority.parse().unwrap();
            assert_eq!(addr.ip(), Ipv4Addr::LOCALHOST);
            assert_ne!(addr.port(), 0);

            let mut stream = TcpStream::connect(addr).await.unwrap();
            let request =
                format!("GET /healthz HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));

            tower.shutdown().await.unwrap();
            let error = TcpStream::connect(addr).await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ConnectionRefused);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rejects_non_loopback_addresses() {
        let bind = (Ipv4Addr::UNSPECIFIED, 0).into();
        let result = Tower::spawn(TowerConfig::dev(bind)).await;
        assert!(matches!(result, Err(TowerError::NonLoopbackBind(addr)) if addr == bind));
    }

    #[tokio::test]
    async fn rejects_a_rate_below_the_minimum_and_a_zero_burst() {
        for rate in [0, MIN_CLIENT_RATE - 1] {
            let result = Tower::spawn(loopback().client_rate(rate)).await;
            assert!(matches!(result, Err(TowerError::InvalidClientRate(r)) if r == rate));
        }
        let result = Tower::spawn(loopback().client_burst(0)).await;
        assert!(matches!(result, Err(TowerError::InvalidClientBurst)));
    }

    #[tokio::test]
    async fn starts_with_the_minimum_rate_a_one_byte_burst_and_zero_stats() {
        timeout(Duration::from_secs(5), async {
            let config = loopback().client_rate(MIN_CLIENT_RATE).client_burst(1);
            let tower = Tower::spawn(config).await.unwrap();
            assert_eq!(tower.stats(), TowerStats::default());
            tower.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[test]
    fn debug_output_counts_listed_ids_without_showing_them() {
        let config = loopback().allow(["first-entry", "second-entry"]);
        let debug = format!("{config:?}");
        assert!(debug.contains("2 endpoint ids"));
        assert!(!debug.contains("first-entry") && !debug.contains("second-entry"));
    }
}
