//! The tower: an embeddable relay for the numbers network, used by the `tower` binary and tests.
//!
//! A tower comes in two kinds. A development tower serves plain HTTP on a loopback address. A
//! TLS tower serves HTTPS on any address, with its certificate read from PEM files or obtained
//! from Let's Encrypt with ACME, and can run QUIC address discovery so that endpoints learn
//! their public address. Either kind limits the
//! traffic received from each relay client, can admit only a static list of endpoint ids, counts
//! what it refused, and sees only ciphertext from traffic between endpoints.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use iroh_base::EndpointId;
use iroh_relay::server::{
    Access, AccessControl, ClientRateLimit, ClientRequest, QuicConfig, RelayConfig, Server,
    ServerConfig, SpawnError, TlsConfig,
};

mod certs;
pub mod config;
mod tls;

pub use certs::TlsFileProblem;
pub use tls::{
    CertSource, DEFAULT_HTTPS_BIND, DEFAULT_QAD_BIND, DEFAULT_TLS_HTTP_BIND, MAX_RELOAD_INTERVAL,
    TlsTower,
};

/// The tracing target of the TLS tower's certificate reload warnings.
///
/// The library logs nothing else. A binary that filters log events by target can let this one
/// through; the `tower` binary does.
pub const TLS_LOG_TARGET: &str = "numbers_tower::tls";

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
    /// The size of each client's token bucket, in bytes: what a client that has been quiet may
    /// send before the rate applies.
    ///
    /// It is a budget, not a cap on one read. The tower takes each read whole, whatever its size,
    /// and then makes the client wait before the next read until the bucket has refilled at
    /// [`Self::client_rate`]. `None` uses iroh-relay's default, a tenth of [`Self::client_rate`].
    /// [`Tower::spawn`] refuses zero.
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

    /// Sets the size of each client's token bucket, in bytes; see [`Self::client_burst`].
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

/// One of a tower's listeners, as an error names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Listener {
    /// The plain-HTTP listener: the relay of a dev tower, the captive-portal check of a TLS
    /// tower.
    Http,
    /// The HTTPS listener of a TLS tower: the relay.
    Https,
    /// The UDP socket of QUIC address discovery.
    AddressDiscovery,
}

impl fmt::Display for Listener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Http => "plain-HTTP listener",
            Self::Https => "HTTPS listener",
            Self::AddressDiscovery => "address discovery socket",
        })
    }
}

/// An error starting or stopping a tower.
///
/// The enum is `#[non_exhaustive]`: later versions add variants.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TowerError {
    /// A dev tower's bind address is not a loopback address.
    #[error("the dev tower requires a loopback bind address, got {0}")]
    NonLoopbackBind(SocketAddr),
    /// [`TlsTower::hostname`] is neither a DNS host name nor an IP literal.
    ///
    /// The error does not repeat the name; see [`TlsTower::hostname`] for the rule.
    #[error("the TLS tower's host name is not a valid DNS host name or IP literal")]
    InvalidHostname,
    /// The certificate file cannot be used.
    ///
    /// The error names which of the two files and one fixed [`TlsFileProblem`], and
    /// deliberately not the path: the caller configured it and knows it, and paths often
    /// contain the tower's host name. It carries no content of the file and no upstream error,
    /// and the variant has no source.
    #[error("cannot use the tower's certificate file: {problem}")]
    #[non_exhaustive]
    CertificateFile {
        /// What is wrong with the file.
        problem: TlsFileProblem,
    },
    /// The private key file cannot be used, including when its key does not belong to the
    /// certificate ([`TlsFileProblem::KeyMismatch`]).
    ///
    /// The error names which of the two files and one fixed [`TlsFileProblem`], and
    /// deliberately not the path: the caller configured it and knows it, and paths often
    /// contain the tower's host name. It carries no byte of the key and no upstream error, and
    /// the variant has no source.
    #[error("cannot use the tower's private key file: {problem}")]
    #[non_exhaustive]
    KeyFile {
        /// What is wrong with the file.
        problem: TlsFileProblem,
    },
    /// [`TlsTower::reload_interval`] is zero or too long to schedule (over
    /// [`MAX_RELOAD_INTERVAL`]).
    #[error("the TLS tower's certificate reload interval is zero or too long to schedule")]
    InvalidReloadInterval,
    /// An ACME certificate was asked for a host name that is an IP literal or a single label.
    ///
    /// Let's Encrypt issues certificates for DNS names under a public suffix only, so the name
    /// needs at least two labels. The error does not repeat the name.
    #[error("an ACME certificate needs a DNS host name of at least two labels, not an IP literal")]
    AcmeNeedsDnsName,
    /// The ACME contact is not `mailto:` (in lower case) followed by one address: exactly one
    /// `@` with something on both sides, and no white space, comma or `?`.
    ///
    /// The error does not repeat the contact.
    #[error("the ACME contact must be a `mailto:` address")]
    InvalidAcmeContact,
    /// The ACME cache directory can be read or entered by the group or by others.
    ///
    /// It holds the ACME account key and the certificate's private key; the tower refuses to
    /// start the ACME client with it. The error names no path.
    #[error("the ACME cache directory must be accessible to the tower's user only (mode 0700)")]
    AcmeCacheNotPrivate,
    /// The ACME cache path exists and is not a directory. The error names no path.
    #[error("the ACME cache path is not a directory")]
    AcmeCacheNotADirectory,
    /// The ACME cache directory could not be checked or created; the kind of I/O error says
    /// why. The error names no path.
    #[error("cannot use the ACME cache directory ({0})")]
    AcmeCacheUnusable(std::io::ErrorKind),
    /// The relay stopped by itself, without being asked to: one of its listeners or its ACME
    /// client ended. See [`Tower::stopped`].
    #[error("the tower's relay stopped by itself")]
    Stopped,
    /// A listener could not be bound; the kind of I/O error says why, for example
    /// `AddrInUse` (the port is taken) or `PermissionDenied` (a privileged port without the
    /// right to bind it).
    ///
    /// The message names the listener, never its address. A listener the relay had already
    /// bound can stay bound until the runtime runs again: a caller that retries at once on the
    /// same ports can see "address in use". `Tower::spawn` yields once before returning the
    /// error, which is usually enough; there is no stronger promise.
    #[error("cannot bind the {listener} ({kind})")]
    #[non_exhaustive]
    Bind {
        /// Which listener.
        listener: Listener,
        /// The kind of I/O error.
        kind: std::io::ErrorKind,
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
    /// The relay server failed to start for a reason other than a listener that could not be
    /// bound (that is [`TowerError::Bind`]).
    ///
    /// For a failure of iroh-relay's own start, the message and the source are a fixed text of
    /// this crate: iroh-relay's text is dropped, since one of its messages names the socket
    /// address it could not bind. The other causes (the TLS configuration, a load task that did
    /// not finish) pass on their own text, which names no address, host name or path.
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

/// Maps iroh-relay's start error to this crate's own, with no upstream text.
///
/// The listeners, from iroh-relay 1.5.0's source: `BindTlsListener` is the plain-HTTP listener
/// that a TLS tower always binds (`server.rs:807`), despite its name; `BindTcpListener` is the
/// relay's own listener (`server/http_server.rs:462`), HTTPS for a TLS tower and plain HTTP for
/// a dev tower; `QuicSpawn` carries, for a socket that cannot be bound, the `io::Error` as the
/// source of its `EndpointServer` variant (`quic.rs:114`). The module of that variant's type is
/// private, so its source is read through `Error::source`; a failure to read the socket's local
/// address right after binding it would show the same way.
fn spawn_error(error: &SpawnError, tls: bool) -> TowerError {
    let start = |reason: &'static str| TowerError::Start(Box::new(std::io::Error::other(reason)));
    match error {
        SpawnError::BindTlsListener { source, .. } => TowerError::Bind {
            listener: Listener::Http,
            kind: source.kind(),
        },
        SpawnError::BindTcpListener { source, .. } => TowerError::Bind {
            listener: if tls { Listener::Https } else { Listener::Http },
            kind: source.kind(),
        },
        SpawnError::QuicSpawn { source, .. } => {
            match Error::source(source).and_then(|cause| cause.downcast_ref::<std::io::Error>()) {
                Some(cause) => TowerError::Bind {
                    listener: Listener::AddressDiscovery,
                    kind: cause.kind(),
                },
                None => start("address discovery could not start"),
            }
        }
        SpawnError::LocalAddr { .. } | SpawnError::NoLocalAddr { .. } => {
            start("a listener's local address could not be read")
        }
        SpawnError::AcmeClientTlsConfig { .. } => {
            start("the ACME client's TLS configuration could not be built")
        }
        _ => start("the relay server could not start"),
    }
}

/// A running tower.
pub struct Tower {
    server: Server,
    url: String,
    http_addr: SocketAddr,
    denied: Arc<AtomicU64>,
    /// The certificate reload task of a TLS tower; dropping it stops the task.
    reloader: Option<certs::Reloader>,
    /// How many distinct endpoint ids the allowlist admits; `None` admits everybody.
    listed: Option<usize>,
    /// Set once [`Tower::stopped`] saw the relay end: its supervisor must not be awaited again.
    relay_ended: bool,
}

impl Tower {
    /// Starts a rate-limited relay: plain HTTP on loopback, or HTTPS from the TLS configuration.
    ///
    /// Fails before binding anything if a dev tower's bind address is not loopback, an allowlist
    /// entry is not a valid endpoint id, the rate or burst is out of range, or a TLS tower's host
    /// name, reload interval, certificate file or key file is unusable. The files are read on
    /// tokio's blocking pool; a regular file on a mount that stops answering blocks that read,
    /// and then this call does not return.
    ///
    /// If iroh-relay fails to bind a port (it is taken, for example), a listener it had already
    /// bound is released only when the runtime next runs its aborted tasks. This call yields once
    /// before returning that error, as a best effort; see [`TowerError::Start`].
    pub async fn spawn(config: TowerConfig) -> Result<Self, TowerError> {
        if config.tls.is_none() && !config.bind.ip().is_loopback() {
            return Err(TowerError::NonLoopbackBind(config.bind));
        }
        let members = config.allow.as_deref().map(parse_allowlist).transpose()?;
        let listed = members.as_ref().map(HashSet::len);
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
        let mut reloader = None;
        if let Some((tls, (cert, reload))) = tls {
            relay.tls = Some(TlsConfig::new(tls.https_bind, cert));
            server_config.quic = tls.qad_bind.map(QuicConfig::new);
            reloader = reload;
        }
        server_config.relay = Some(relay);

        // The last check, after every other one and right before the relay (and with it the ACME
        // client) starts, so that building the configuration has no side effects.
        if let Some(CertSource::Acme { cache_dir, .. }) = config.tls.as_ref().map(|tls| &tls.source)
        {
            let cache_dir = cache_dir.clone();
            tokio::task::spawn_blocking(move || tls::prepare_acme_cache(&cache_dir))
                .await
                .map_err(|error| TowerError::Start(Box::new(error)))??;
        }
        let server = match Server::spawn(server_config).await {
            Ok(server) => server,
            Err(error) => {
                // Lets the runtime run the aborted tasks that still hold a listener.
                tokio::task::yield_now().await;
                return Err(spawn_error(&error, config.tls.is_some()));
            }
        };
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
            reloader,
            listed,
            relay_ended: false,
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
        self.qad_addr().map(|addr| addr.port())
    }

    /// Returns the bound HTTPS address of a TLS tower, or `None` for a dev tower.
    pub fn https_addr(&self) -> Option<SocketAddr> {
        self.server.https_addr()
    }

    /// Returns the bound UDP address of QUIC address discovery, or `None` when it is off.
    pub fn qad_addr(&self) -> Option<SocketAddr> {
        self.server.quic_addr()
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
        drop(self.reloader);
        if self.relay_ended {
            // The relay's tasks have all ended; there is nothing left to stop.
            return Ok(());
        }
        self.server
            .shutdown()
            .await
            .map_err(|error| TowerError::Shutdown(Box::new(error)))
    }

    /// Waits until the relay stops by itself, and returns [`TowerError::Stopped`].
    ///
    /// iroh-relay's supervisor ends as soon as any of the relay's tasks ends: a listener's
    /// accept loop (address discovery ends on a UDP receive error), the ACME client, or a task
    /// that panicked. Nothing listens after that. This future stays pending while the tower
    /// runs; it never asks the relay to stop. It is cancel-safe: dropping it leaves the tower
    /// running, and [`Tower::shutdown`] works afterwards, also after it returned.
    pub async fn stopped(&mut self) -> TowerError {
        if !self.relay_ended {
            // The supervisor's own result names no more than which task ended; it is dropped.
            let _ = self.server.join().await;
            self.relay_ended = true;
        }
        TowerError::Stopped
    }

    /// Returns how many distinct endpoint ids the allowlist admits, or `None` when it admits
    /// everybody.
    ///
    /// Ids are counted as the tower parsed them, so the same id written twice, or once in hex
    /// and once in base32, counts once.
    pub fn listed(&self) -> Option<usize> {
        self.listed
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

    use super::{
        MAX_RELOAD_INTERVAL, MIN_CLIENT_RATE, TlsTower, Tower, TowerConfig, TowerError, TowerStats,
    };

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

    /// After a drop or a shutdown, nothing holds the certificate pair or the reload task's
    /// provider: the reload task is gone and the key is released.
    #[tokio::test]
    async fn a_dropped_or_stopped_tls_tower_releases_its_key_and_stops_reloading() {
        timeout(Duration::from_secs(20), async {
            let dir = tempfile::tempdir().unwrap();
            let generated = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
            let (cert, key) = (dir.path().join("cert.pem"), dir.path().join("key.pem"));
            std::fs::write(&cert, generated.cert.pem()).unwrap();
            std::fs::write(&key, generated.signing_key.serialize_pem()).unwrap();
            let loopback: SocketAddr = (Ipv4Addr::LOCALHOST, 0).into();

            for graceful in [false, true] {
                let tls = TlsTower::new("127.0.0.1", &cert, &key)
                    .https_bind(loopback)
                    .qad_bind(Some(loopback))
                    // Long enough that only the abort can end the task within the bound.
                    .reload_interval(MAX_RELOAD_INTERVAL);
                let tower = Tower::spawn(TowerConfig::tls(tls).bind(loopback))
                    .await
                    .unwrap();
                let (pair, provider) = tower.reloader.as_ref().unwrap().watch.clone();
                assert!(pair.strong_count() > 0 && provider.strong_count() > 0);
                if graceful {
                    tower.shutdown().await.unwrap();
                } else {
                    drop(tower);
                }
                // The relay's own tasks end asynchronously after a drop; no event marks it.
                let mut tick = tokio::time::interval(Duration::from_millis(10));
                while pair.strong_count() > 0 || provider.strong_count() > 0 {
                    tick.tick().await;
                }
            }
        })
        .await
        .unwrap();
    }

    /// `stopped` stays pending while the tower runs, and `shutdown` still works after it was
    /// dropped.
    #[tokio::test]
    async fn stopped_stays_pending_on_a_running_tower() {
        timeout(Duration::from_secs(10), async {
            let mut tower = Tower::spawn(loopback()).await.unwrap();
            assert!(
                timeout(Duration::from_millis(500), tower.stopped())
                    .await
                    .is_err()
            );
            tower.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }

    /// The same id twice, once in hex and once in base32, counts once.
    #[tokio::test]
    async fn listed_counts_distinct_ids() {
        timeout(Duration::from_secs(10), async {
            let key = iroh_base::SecretKey::generate().public();
            let hex = key.to_string();
            let base32 = data_encoding_base32(key.as_bytes());
            let other = iroh_base::SecretKey::generate().public().to_string();
            let tower = Tower::spawn(loopback().allow([hex.clone(), hex, base32, other]))
                .await
                .unwrap();
            assert_eq!(tower.listed(), Some(2));
            tower.shutdown().await.unwrap();
            let open = Tower::spawn(loopback()).await.unwrap();
            assert_eq!(open.listed(), None);
            open.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }

    /// Lower-case RFC 4648 base32 without padding, as iroh accepts an id.
    fn data_encoding_base32(bytes: &[u8]) -> String {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
        let mut out = String::new();
        let (mut buffer, mut bits) = (0u32, 0);
        for &byte in bytes {
            buffer = (buffer << 8) | u32::from(byte);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                out.push(char::from(ALPHABET[((buffer >> bits) & 31) as usize]));
            }
        }
        if bits > 0 {
            out.push(char::from(ALPHABET[((buffer << (5 - bits)) & 31) as usize]));
        }
        out
    }

    #[test]
    fn debug_output_counts_listed_ids_without_showing_them() {
        let config = loopback().allow(["first-entry", "second-entry"]);
        let debug = format!("{config:?}");
        assert!(debug.contains("2 endpoint ids"));
        assert!(!debug.contains("first-entry") && !debug.contains("second-entry"));
    }
}
