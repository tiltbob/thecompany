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
//! A domain may name at most [`MAX_LOCATED_TOWERS`] towers. A larger answer is an error of its
//! own, [`LocateError::TooManyTowers`], not a silent cut: which towers a station gets must not
//! depend on an arbitrary choice.
//!
//! Two bounds make a lookup fail with [`LocateError::Timeout`]. n0-dns-resolver gives each query to
//! a UDP nameserver 6 seconds, a constant that cannot be changed from here; that is a budget per
//! query, not per lookup, which may ask several nameservers, follow a CNAME chain or retry over TCP.
//! The locator's own deadline bounds the whole lookup's network waits: [`DEFAULT_LOCATE_TIMEOUT`],
//! 10 seconds, unless [`DnsLocator::with_timeout`] sets another. The deadline cannot interrupt
//! synchronous work: the system-configured locator reads the system's resolver configuration at
//! the start of each lookup, on the calling task, and the deadline cannot fire while that read
//! runs. On Linux and the BSDs the read is `/etc/resolv.conf` and the hosts file, two small local
//! files; on Apple platforms the SystemConfiguration dynamic store, on Windows the list of network
//! adapters, on Android a call into Java. It normally takes well under a millisecond, but nothing
//! bounds it if the file system or the platform service hangs.
//!
//! This module logs nothing, and its errors carry no name. n0-dns-resolver itself logs the names it
//! looks up and the records it gets back through `tracing`, at debug and trace level: a binary that
//! lets those levels through for `n0_dns_resolver` writes the domain and the tower host names to
//! its log.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use n0_dns_resolver::{DnsProtocol, DnsResolver, Nameserver};
use url::Url;

use crate::TowerSet;

/// The SRV service and protocol labels put in front of a domain: `_numbers._tcp`.
pub const SRV_SERVICE: &str = "_numbers._tcp";

/// How long [`DnsLocator::locate`](Locator::locate) may take in all, unless
/// [`DnsLocator::with_timeout`] sets another deadline: 10 seconds.
pub const DEFAULT_LOCATE_TIMEOUT: Duration = Duration::from_secs(10);

/// The most towers one domain may name: equal to the bound of the tower set the station installs
/// them in. A larger answer is [`LocateError::TooManyTowers`].
pub const MAX_LOCATED_TOWERS: usize = 8;

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
    /// The domain names more than [`MAX_LOCATED_TOWERS`] usable, distinct towers.
    #[error("the domain names more towers than a station can use")]
    TooManyTowers,
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
/// 1 to 63 ASCII letters, digits and hyphens, separated by single dots, no label beginning or
/// ending with a hyphen. A scheme, a path, a port, whitespace, an underscore or an empty label make
/// it [`LocateError::InvalidDomain`].
///
/// The whole name must fit a DNS name, 253 characters without the final dot. `_numbers._tcp.`
/// takes 14 of them, so the domain may have at most 239 characters (without its trailing dot).
pub fn srv_name(domain: &str) -> Result<String, LocateError> {
    let domain = domain.strip_suffix('.').unwrap_or(domain);
    if !is_host_name(domain) || SRV_SERVICE.len() + 1 + domain.len() > MAX_NAME_LEN {
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
/// digits and hyphens that neither begin nor end with a hyphen, separated by single dots, at most
/// 253 characters in all. The tower applies the same rule to its own host name.
fn is_host_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_LEN
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= MAX_LABEL_LEN
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

/// The future [`Locator::locate`] returns: boxed, `Send`, and borrowing the locator and the domain.
pub type LocateFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<String>, LocateError>> + Send + 'a>>;

/// Finds the tower URLs a domain names.
///
/// The station holds a `Locator` as `Arc<dyn Locator>`. [`DnsLocator`] is the real one; tests can
/// supply their own. The trait is object-safe, so `locate` returns a boxed future
/// ([`LocateFuture`]) rather than an `impl Future`. A lookup happens rarely, so the allocation does
/// not matter.
pub trait Locator: Send + Sync {
    /// Returns the canonical tower URLs for `domain`, as [`towers_from_srv`] makes them. The list
    /// is never empty: a domain that names no tower is [`LocateError::NoTower`].
    fn locate<'a>(&'a self, domain: &'a str) -> LocateFuture<'a>;
}

/// A shared locator is a locator: an `Arc<DnsLocator>` or an `Arc<dyn Locator>` can be passed
/// wherever a `Locator` is expected. It forwards to the locator inside.
impl<L: Locator + ?Sized> Locator for Arc<L> {
    fn locate<'a>(&'a self, domain: &'a str) -> LocateFuture<'a> {
        (**self).locate(domain)
    }
}

/// A [`Locator`] that asks DNS for the domain's SRV record, through n0-dns-resolver.
///
/// It never adds public resolvers: it asks the system's resolvers or one named nameserver only.
pub struct DnsLocator {
    source: Source,
    timeout: Duration,
}

/// Where a [`DnsLocator`] gets its resolver.
enum Source {
    /// A new system-configured resolver for every lookup.
    System,
    /// One resolver for one named nameserver, kept for the locator's life. Boxed: the resolver is
    /// large and the other variant is empty.
    Nameserver(Box<DnsResolver>),
}

/// The resolver one lookup uses: a fresh one (boxed, as it is large), or the locator's own.
enum LookupResolver<'a> {
    Fresh(Box<DnsResolver>),
    Kept(&'a DnsResolver),
}

impl std::ops::Deref for LookupResolver<'_> {
    type Target = DnsResolver;

    fn deref(&self) -> &DnsResolver {
        match self {
            Self::Fresh(resolver) => resolver,
            Self::Kept(resolver) => resolver,
        }
    }
}

impl DnsLocator {
    /// A locator over the system's resolver configuration, with no public fallback.
    ///
    /// Every lookup builds a new resolver, which reads the system configuration afresh, so a
    /// lookup after a network change asks the nameservers configured now, not those of the first
    /// lookup. (A resolver reads the configuration once, on its first lookup, and keeps it; its
    /// `clear_cache` clears answers only.) Lookups are rare and building a resolver does no IO, so
    /// this costs one read of the configuration per lookup. Nothing is carried from one lookup to
    /// the next: no cached answer, no measured round trip, no pooled TCP connection.
    ///
    /// Construction does no IO. The deadline is [`DEFAULT_LOCATE_TIMEOUT`].
    pub fn system() -> Self {
        Self {
            source: Source::System,
            timeout: DEFAULT_LOCATE_TIMEOUT,
        }
    }

    /// A locator that asks only the nameserver at `addr`. For tests, and for people who must name
    /// their resolver.
    ///
    /// Each query goes out over UDP, with up to six datagrams paced 300 ms to 1 s apart and then
    /// further. If no answer has come 900 ms after the first datagram, the same query also goes to
    /// `addr` over TCP and races the datagrams; a truncated UDP answer brings TCP in at once. The
    /// first answer on either wins, within the resolver's 6 seconds per query.
    ///
    /// The locator keeps one resolver for its life, with its cache, its measured round trip and
    /// its pooled TCP connection; `locate` clears the cache before each lookup. The deadline is
    /// [`DEFAULT_LOCATE_TIMEOUT`].
    pub fn with_nameserver(addr: SocketAddr) -> Self {
        let resolver = DnsResolver::builder()
            .nameserver(Nameserver::new(addr, DnsProtocol::Udp))
            .build();
        Self {
            source: Source::Nameserver(Box::new(resolver)),
            timeout: DEFAULT_LOCATE_TIMEOUT,
        }
    }

    /// Sets the deadline for a whole lookup, after which `locate` fails with
    /// [`LocateError::Timeout`].
    ///
    /// The deadline bounds the lookup's network waits; it cannot interrupt the synchronous read of
    /// the system configuration (see the [module docs](self)). A zero deadline fails every lookup
    /// with `Timeout` before anything is sent. (The timer under the deadline counts in whole
    /// milliseconds, so a zero deadline left to it would let a fast answer through now and then.)
    /// A deadline above 6 seconds lets the resolver's own per-query timeout fire first against a
    /// single silent nameserver.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl Locator for DnsLocator {
    /// Looks up `_numbers._tcp.<domain>.` and maps the answer through [`towers_from_srv`].
    ///
    /// An invalid domain, or a zero deadline, fails before anything is sent. The lookup runs under
    /// the locator's deadline (see [`DnsLocator::with_timeout`]) and needs a tokio runtime with the
    /// time driver enabled. More than [`MAX_LOCATED_TOWERS`] towers, counted after
    /// [`towers_from_srv`] has dropped unusable and duplicate targets, is
    /// [`LocateError::TooManyTowers`].
    ///
    /// No lookup is answered from an earlier one's cache, because the station asks DNS rarely and
    /// then wants the current answer. The system-configured locator gets this from its fresh
    /// resolver per lookup. The locator for one nameserver clears its resolver's cache first;
    /// without that, n0-dns-resolver as configured here would answer from its cache: it keeps a
    /// positive answer for the smallest TTL among its records, capped at one day (a TTL of 0 is not
    /// cached), with no minimum TTL; it serves no stale answers; and it caches no negative answers
    /// (NXDOMAIN, an empty answer), since negative caching is off unless the builder asks for it.
    fn locate<'a>(&'a self, domain: &'a str) -> LocateFuture<'a> {
        Box::pin(self.lookup(domain))
    }
}

impl DnsLocator {
    /// The resolver for one lookup: a new system-configured one, or the kept one with its cache
    /// cleared.
    fn lookup_resolver(&self) -> LookupResolver<'_> {
        match &self.source {
            Source::System => {
                LookupResolver::Fresh(Box::new(DnsResolver::builder().use_system_config().build()))
            }
            Source::Nameserver(resolver) => {
                resolver.clear_cache();
                LookupResolver::Kept(resolver)
            }
        }
    }

    /// The body of [`Locator::locate`] for `DnsLocator`, before it is boxed.
    async fn lookup(&self, domain: &str) -> Result<Vec<String>, LocateError> {
        let name = srv_name(domain)?;
        if self.timeout.is_zero() {
            return Err(LocateError::Timeout);
        }
        let resolver = self.lookup_resolver();
        let lookup = n0_future::time::timeout(self.timeout, resolver.lookup_srv(name));
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
        if towers.len() > MAX_LOCATED_TOWERS {
            return Err(LocateError::TooManyTowers);
        }
        Ok(towers)
    }
}

impl fmt::Debug for DnsLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DnsLocator").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use n0_dns_resolver::DnsResolver;

    use super::{DnsLocator, LookupResolver};

    // The system-configured locator builds a new resolver for each lookup, so that a changed
    // system configuration is read again. Building a resolver does no IO, so this reads nothing
    // from the host; it shows only that two lookups do not share a resolver.
    #[test]
    fn system_locator_uses_a_fresh_resolver_per_lookup() {
        let locator = DnsLocator::system();
        let first = locator.lookup_resolver();
        let second = locator.lookup_resolver();
        let (LookupResolver::Fresh(a), LookupResolver::Fresh(b)) = (&first, &second) else {
            panic!("the system locator reused a resolver");
        };
        // Both are alive, so distinct resolvers sit at distinct addresses.
        assert!(!std::ptr::eq::<DnsResolver>(&**a, &**b));
    }

    #[test]
    fn nameserver_locator_keeps_its_resolver() {
        let locator = DnsLocator::with_nameserver(SocketAddr::from((Ipv4Addr::LOCALHOST, 53)));
        let first = locator.lookup_resolver();
        let second = locator.lookup_resolver();
        let (LookupResolver::Kept(a), LookupResolver::Kept(b)) = (first, second) else {
            panic!("the nameserver locator built a new resolver");
        };
        assert!(std::ptr::eq(a, b));
    }
}
