//! The tower: an embeddable relay for the numbers network, used by the `tower` binary and tests.
//!
//! A tower comes in two kinds. A development tower serves plain HTTP on a loopback address. A
//! TLS tower serves HTTPS on any address, with its certificate read from PEM files, and can run
//! QUIC address discovery so that endpoints learn their public address. Either kind limits the
//! traffic received from each relay client, can admit only a static list of endpoint ids, counts
//! what it refused, and sees only ciphertext from traffic between endpoints.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use iroh_base::EndpointId;
use iroh_relay::server::{
    Access, AccessControl, ClientRateLimit, ClientRequest, QuicConfig, RelayConfig, Server,
    ServerConfig, TlsConfig,
};

mod tls;

pub use tls::{DEFAULT_HTTPS_BIND, DEFAULT_QAD_BIND, DEFAULT_TLS_HTTP_BIND, TlsTower};

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

/// Configuration for a tower: a plain-HTTP development tower or a TLS tower.
///
/// Build it with [`TowerConfig::dev`], [`TowerConfig::tls`] or [`Default`], then adjust it with
/// the builder-style setters. The struct is `#[non_exhaustive]` so that later fields do not break
/// callers outside this crate. [`Tower::spawn`] checks the whole configuration.
///
/// Its `Debug` output shows how many endpoint ids are listed, never the ids themselves.
#[derive(Clone)]
#[non_exhaustive]
pub struct TowerConfig {
    /// The plain-HTTP listener. Port zero lets the operating system choose a port.
    ///
    /// A dev tower serves the relay here and requires a loopback address. A TLS tower serves the
    /// relay over HTTPS instead, but iroh-relay still binds this listener, and it cannot be
    /// turned off: it answers only the captive-portal check (`/generate_204`), which clients send
    /// over plain HTTP. A TLS tower may bind any address.
    pub bind: SocketAddr,
    /// The TLS configuration, or `None` for a plain-HTTP dev tower.
    pub tls: Option<TlsTower>,
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
            tls: None,
            allow: None,
            client_rate: DEFAULT_CLIENT_RATE,
            client_burst: None,
        }
    }

    /// A TLS tower, with its plain-HTTP listener on [`DEFAULT_TLS_HTTP_BIND`].
    ///
    /// Like a dev tower, it has no allowlist and lets each client send [`DEFAULT_CLIENT_RATE`]
    /// bytes per second until the setters say otherwise.
    pub fn tls(tls: TlsTower) -> Self {
        Self {
            tls: Some(tls),
            ..Self::dev(DEFAULT_TLS_HTTP_BIND)
        }
    }

    /// Sets the plain-HTTP listener; see the `bind` field for what it serves.
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.bind = addr;
        self
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
            .field("tls", &self.tls)
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

/// An error starting or stopping a tower.
#[derive(Debug, thiserror::Error)]
pub enum TowerError {
    /// A dev tower's bind address is not a loopback address.
    #[error("the dev tower requires a loopback bind address, got {0}")]
    NonLoopbackBind(SocketAddr),
    /// [`TlsTower::hostname`] is not a DNS name or IP literal that fits in a URL.
    #[error("the TLS tower's host name {0:?} is not a valid URL host")]
    InvalidHostname(String),
    /// The certificate file is missing, unreadable, or holds no valid PEM certificate.
    #[error("cannot use the tower's certificate file {}: {reason}", path.display())]
    CertificateFile {
        /// The configured path.
        path: PathBuf,
        /// What is wrong with the file. It never quotes the file's contents.
        reason: String,
    },
    /// The private key file is missing, unreadable, or holds no usable PEM private key.
    #[error("cannot use the tower's private key file {}: {reason}", path.display())]
    KeyFile {
        /// The configured path.
        path: PathBuf,
        /// What is wrong with the file, as iroh-relay's certificate loader reports it. It never
        /// contains key material.
        reason: String,
    },
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
    /// The relay server failed to start, for example because an address is in use.
    #[error("failed to start the tower: {0}")]
    Start(#[source] Box<dyn Error + Send + Sync>),
    /// The relay server started without the listener the tower needs.
    #[error("the tower started without an HTTP listener")]
    MissingHttpAddress,
    /// The relay server did not shut down cleanly.
    #[error("failed to shut down the tower")]
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
    /// This is iroh-relay's `qad_connections`. It stays 0 on a dev tower and on a TLS tower
    /// without address discovery. An endpoint probes only when it has an IP socket and the host
    /// has an address that is not loopback.
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

/// A running tower.
pub struct Tower {
    server: Server,
    url: String,
    http_addr: SocketAddr,
    denied: Arc<AtomicU64>,
}

impl Tower {
    /// Starts a rate-limited relay: plain HTTP on loopback, or HTTPS from the TLS configuration.
    ///
    /// Fails before binding anything if a dev tower's bind address is not loopback, an allowlist
    /// entry is not a valid endpoint id, the rate or burst is out of range, or a TLS tower's host
    /// name, certificate file or key file is unusable.
    pub async fn spawn(config: TowerConfig) -> Result<Self, TowerError> {
        if config.tls.is_none() && !config.bind.ip().is_loopback() {
            return Err(TowerError::NonLoopbackBind(config.bind));
        }
        let members = config.allow.as_deref().map(parse_allowlist).transpose()?;
        let rate_limit = client_rate_limit(&config)?;
        let tls = match &config.tls {
            Some(tls) => {
                tls::tower_url(&tls.hostname, tls.https_bind.port())?;
                Some((tls, tls::cert_config(tls).await?))
            }
            None => None,
        };

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
        if let Some((tls, cert)) = tls {
            relay.tls = Some(TlsConfig::new(tls.https_bind, cert));
            server_config.quic = tls.qad_bind.map(QuicConfig::new);
        }
        server_config.relay = Some(relay);

        let server = Server::spawn(server_config)
            .await
            .map_err(|error| TowerError::Start(Box::new(error)))?;
        let http_addr = server.http_addr().ok_or(TowerError::MissingHttpAddress)?;
        let url = match &config.tls {
            Some(tls) => {
                let https = server.https_addr().ok_or(TowerError::MissingHttpAddress)?;
                tls::tower_url(&tls.hostname, https.port())?
            }
            None => format!("http://{http_addr}"),
        };
        Ok(Self {
            server,
            url,
            http_addr,
            denied,
        })
    }

    /// Returns the tower's URL, with the port the operating system chose.
    ///
    /// A dev tower returns `http://` and its bound socket address, with no trailing slash, for
    /// example `http://127.0.0.1:3340`. A TLS tower returns its canonical URL: `https`, the host
    /// name in lower case without a trailing dot, the port only when it is not 443, and the
    /// path `/`, for example `https://tower1.example.com/` or `https://127.0.0.1:45123/`.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the bound plain-HTTP address: the relay of a dev tower, and the captive-portal
    /// listener of a TLS tower.
    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    /// Returns the bound UDP port of QUIC address discovery, or `None` when it is off.
    pub fn qad_port(&self) -> Option<u16> {
        self.server.quic_addr().map(|addr| addr.port())
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

    use super::{MIN_CLIENT_RATE, TlsTower, Tower, TowerConfig, TowerError, TowerStats};

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

    #[tokio::test]
    async fn a_tls_tower_is_not_held_to_loopback() {
        // The certificate check fails before anything binds, so this binds nothing either way.
        let tls = TlsTower::new(
            "tower.example",
            "/nonexistent/cert.pem",
            "/nonexistent/key.pem",
        );
        let config = TowerConfig::tls(tls).bind((Ipv4Addr::UNSPECIFIED, 0).into());
        let result = Tower::spawn(config).await;
        assert!(matches!(result, Err(TowerError::CertificateFile { .. })));
    }

    #[test]
    fn debug_output_counts_listed_ids_without_showing_them() {
        let config = loopback().allow(["first-entry", "second-entry"]);
        let debug = format!("{config:?}");
        assert!(debug.contains("2 endpoint ids"));
        assert!(!debug.contains("first-entry") && !debug.contains("second-entry"));
    }
}
