//! Finding a domain's towers through DNS SRV.
//!
//! A station that is given only a domain asks DNS for the SRV record `_numbers._tcp.<domain>.`.
//! Each record names one tower by target host and port, and becomes the tower URL
//! `https://<target>:<port>/`. Priority and weight are not used: the station wants every tower the
//! domain lists, not a choice among them.
//!
//! The URLs are canonical: the host is in lower case and has no trailing dot, the port is left out
//! when it is 443, and the list is sorted and has no duplicates. The relay code compares tower URLs
//! as text, so two spellings of one tower would be two towers to it.
//!
//! [`DnsLocator`] never falls back to public DNS resolvers. It asks either the resolvers the
//! system is configured with or one nameserver the caller names. The towers a station trusts must
//! not come from a resolver it did not choose.
//!
//! Two bounds make a lookup fail with [`LocateError::Timeout`]. n0-dns-resolver gives each query to
//! a UDP nameserver 6 seconds, a constant that cannot be changed from here; that is a budget per
//! query, not per lookup, which may ask several nameservers, follow a CNAME chain or retry over TCP.
//! The locator's own deadline bounds the whole lookup: [`DEFAULT_LOCATE_TIMEOUT`], 10 seconds,
//! unless [`DnsLocator::with_timeout`] sets another.
//!
//! This module logs nothing, and its errors carry no name. n0-dns-resolver itself logs the names it
//! looks up and the records it gets back through `tracing`, at debug and trace level: a binary that
//! lets those levels through for `n0_dns_resolver` writes the domain and the tower host names to
//! its log.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use n0_dns_resolver::{DnsProtocol, DnsResolver, Nameserver};
use url::Url;

use crate::TowerSet;

/// The SRV service and protocol labels put in front of a domain: `_numbers._tcp`.
pub const SRV_SERVICE: &str = "_numbers._tcp";

/// How long [`DnsLocator::locate`](Locator::locate) may take in all, unless
/// [`DnsLocator::with_timeout`] sets another deadline: 10 seconds.
pub const DEFAULT_LOCATE_TIMEOUT: Duration = Duration::from_secs(10);

/// The longest domain name, in characters, without the trailing dot (RFC 1035).
const MAX_NAME_LEN: usize = 253;
/// The longest label of a domain name, in characters (RFC 1035).
const MAX_LABEL_LEN: usize = 63;
/// The port an `https` URL leaves out.
const HTTPS_PORT: u16 = 443;

/// An error finding the towers of a domain.
///
/// No variant carries the domain or a target: the caller knows which domain it asked for, and
/// neither belongs in a log.
#[derive(Debug, thiserror::Error)]
pub enum LocateError {
    /// The domain is not a plain domain name.
    #[error("invalid domain name")]
    InvalidDomain,
    /// The domain names no tower: it does not exist, it has no SRV record for the service, or every
    /// record is unusable.
    #[error("the domain names no tower")]
    NoTower,
    /// The DNS resolver did not answer in time.
    #[error("the DNS lookup timed out")]
    Timeout,
    /// The DNS lookup failed for another reason, such as an error response or no usable
    /// nameserver.
    #[error("the DNS lookup failed")]
    Resolver,
}

/// Returns the fully qualified SRV name to look up for `domain`: `_numbers._tcp.<domain>.`.
///
/// The domain may end in one dot and is put in lower case. It must be a plain host name: labels of
/// ASCII letters, digits and hyphens, separated by single dots. A scheme, a path, a port,
/// whitespace, an underscore or an empty label make it [`LocateError::InvalidDomain`].
pub fn srv_name(domain: &str) -> Result<String, LocateError> {
    let domain = domain.strip_suffix('.').unwrap_or(domain);
    if !is_host_name(domain) {
        return Err(LocateError::InvalidDomain);
    }
    Ok(format!("{SRV_SERVICE}.{}.", domain.to_ascii_lowercase()))
}

/// Returns the canonical tower URLs for SRV answers given as (target, port) pairs.
///
/// Each usable pair becomes `https://<target>:<port>/` with the target in lower case and without
/// its trailing dot, and the port left out when it is 443. The result is sorted and has no
/// duplicates. A pair is skipped when its target is empty or `.` (RFC 2782: the service is not
/// available), when the target is not a plain host name, when the port is 0, or when the URL would
/// not come out in this canonical form or would not be accepted by [`TowerSet::from_url`].
pub fn towers_from_srv(records: impl IntoIterator<Item = (String, u16)>) -> Vec<String> {
    records
        .into_iter()
        .filter_map(|(target, port)| tower_url(&target, port))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The canonical tower URL for one SRV answer, or `None` if the answer is unusable.
fn tower_url(target: &str, port: u16) -> Option<String> {
    let host = target.strip_suffix('.').unwrap_or(target);
    if port == 0 || !is_host_name(host) {
        return None;
    }
    let host = host.to_ascii_lowercase();
    let url = if port == HTTPS_PORT {
        format!("https://{host}/")
    } else {
        format!("https://{host}:{port}/")
    };
    // The URL parser rewrites some hosts, such as a short numeric one into an IPv4 address. Keep
    // only URLs that survive parsing unchanged, so the text is the one the relay code will see.
    let parsed = Url::parse(&url).ok()?;
    if parsed.as_str() != url || TowerSet::from_url(&url).is_err() {
        return None;
    }
    Some(url)
}

/// Whether `name`, without a trailing dot, is a plain host name: labels of 1 to 63 ASCII letters,
/// digits and hyphens, separated by single dots, at most 253 characters in all.
fn is_host_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= MAX_LABEL_LEN
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

/// Finds the tower URLs a domain names.
///
/// The station holds a `Locator`. [`DnsLocator`] is the real one; tests can supply their own.
pub trait Locator: Send + Sync {
    /// Returns the canonical tower URLs for `domain`, as [`towers_from_srv`] makes them. The list
    /// is never empty: a domain that names no tower is [`LocateError::NoTower`].
    fn locate(&self, domain: &str)
    -> impl Future<Output = Result<Vec<String>, LocateError>> + Send;
}

/// A [`Locator`] that asks DNS for the domain's SRV record, through n0-dns-resolver.
///
/// It never adds public resolvers: it asks the system's resolvers or one named nameserver only.
pub struct DnsLocator {
    resolver: DnsResolver,
    timeout: Duration,
}

impl DnsLocator {
    /// A locator over the system's resolver configuration, with no public fallback.
    ///
    /// Construction does no IO; the configuration is read on the first lookup. The deadline is
    /// [`DEFAULT_LOCATE_TIMEOUT`].
    pub fn system() -> Self {
        Self {
            resolver: DnsResolver::builder().use_system_config().build(),
            timeout: DEFAULT_LOCATE_TIMEOUT,
        }
    }

    /// A locator that asks only the nameserver at `addr`, over UDP (and TCP when an answer is
    /// truncated). For tests, and for people who must name their resolver. The deadline is
    /// [`DEFAULT_LOCATE_TIMEOUT`].
    pub fn with_nameserver(addr: SocketAddr) -> Self {
        Self {
            resolver: DnsResolver::builder()
                .nameserver(Nameserver::new(addr, DnsProtocol::Udp))
                .build(),
            timeout: DEFAULT_LOCATE_TIMEOUT,
        }
    }

    /// Sets the deadline for a whole lookup, after which `locate` fails with
    /// [`LocateError::Timeout`].
    ///
    /// A zero deadline fails every lookup with `Timeout` before anything is sent. (The timer under
    /// the deadline counts in whole milliseconds, so a zero deadline left to it would let a fast
    /// answer through now and then.) A deadline above 6 seconds lets the resolver's own per-query
    /// timeout fire first against a single silent nameserver.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl Locator for DnsLocator {
    /// Looks up `_numbers._tcp.<domain>.` and maps the answer through [`towers_from_srv`].
    ///
    /// An invalid domain, or a zero deadline, fails before anything is sent. The whole lookup runs
    /// under the locator's deadline (see [`DnsLocator::with_timeout`]) and needs a tokio runtime
    /// with the time driver enabled.
    ///
    /// The resolver's cache is cleared first, because the station asks DNS rarely and then wants
    /// the current answer. Without that, n0-dns-resolver as configured here would answer from its
    /// cache: it keeps a positive answer for the smallest TTL among its records, capped at one day
    /// (a TTL of 0 is not cached), with no minimum TTL; it serves no stale answers; and it caches no
    /// negative answers (NXDOMAIN, an empty answer), since negative caching is off unless the
    /// builder asks for it.
    async fn locate(&self, domain: &str) -> Result<Vec<String>, LocateError> {
        let name = srv_name(domain)?;
        if self.timeout.is_zero() {
            return Err(LocateError::Timeout);
        }
        self.resolver.clear_cache();
        let lookup = n0_future::time::timeout(self.timeout, self.resolver.lookup_srv(name));
        let Ok(result) = lookup.await else {
            return Err(LocateError::Timeout);
        };
        let records = match result {
            Ok(records) => records,
            Err(n0_dns_resolver::Error::NxDomain { .. }) => return Err(LocateError::NoTower),
            Err(n0_dns_resolver::Error::Timeout { .. }) => return Err(LocateError::Timeout),
            Err(n0_dns_resolver::Error::InvalidName { .. }) => {
                return Err(LocateError::InvalidDomain);
            }
            Err(_) => return Err(LocateError::Resolver),
        };
        let towers = towers_from_srv(records.into_iter().map(|r| (r.target, r.port)));
        if towers.is_empty() {
            return Err(LocateError::NoTower);
        }
        Ok(towers)
    }
}

impl fmt::Debug for DnsLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DnsLocator").finish_non_exhaustive()
    }
}
