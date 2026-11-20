//! Finding a domain's towers through DNS SRV.
//!
//! A station that is given only a domain asks DNS for the SRV record `_numbers._tcp.<domain>.`.
//! Each record names one tower by target host and port, and becomes a [`TowerEntry`] built by
//! [`TowerEntry::from_url`] from `https://<target>:<port>`. Priority and weight are not used: the
//! station wants every tower the domain lists, not a choice among them.
//!
//! The URLs are those of [`canonical_tower_url`], the one URL rule of the workspace: the host is in
//! lower case and has no trailing dot, the port is left out when it is 443. A tower found here and
//! the same host and port typed by a user are the same string. The list is sorted by URL and has no
//! duplicates: the relay code compares tower URLs as text, so two spellings of one tower would be
//! two towers to it. SRV carries no QAD port, so every located tower has the default of
//! [`TowerEntry::from_url`] for HTTPS: QAD on UDP 7842.
//!
//! [`DnsLocator`] never falls back to public DNS resolvers. It asks either the resolvers the
//! system is configured with or one nameserver the caller names. The towers a station trusts must
//! not come from a resolver it did not choose.
//!
//! A domain may name at most [`MAX_TOWERS`] towers, the bound of a [`TowerSet`]. A larger answer
//! is an error of its own, [`LocateError::TooManyTowers`], not a silent cut: which towers a station
//! gets must not depend on an arbitrary choice.
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

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use n0_dns_resolver::{DnsProtocol, DnsResolver, Nameserver};
use url::{Host, Url};

use crate::{DnsConfig, MAX_TOWERS, TowerEntry};
#[cfg(doc)]
use crate::{TowerSet, canonical_tower_url};

/// The SRV service and protocol labels put in front of a domain: `_numbers._tcp`.
pub const SRV_SERVICE: &str = "_numbers._tcp";

/// How long [`DnsLocator::locate`](Locator::locate) may take in all, unless
/// [`DnsLocator::with_timeout`] sets another deadline: 10 seconds.
pub const DEFAULT_LOCATE_TIMEOUT: Duration = Duration::from_secs(10);

/// The longest domain name, in characters, without the trailing dot (RFC 1035).
const MAX_NAME_LEN: usize = 253;
/// The longest label of a domain name, in characters (RFC 1035).
const MAX_LABEL_LEN: usize = 63;

/// An error finding the towers of a domain.
///
/// No variant carries the domain, a target, a tower URL or a nameserver address: the caller knows
/// which domain it asked for, and none of them belongs in a log.
#[derive(Debug, thiserror::Error)]
pub enum LocateError {
    /// The domain is not a plain domain name.
    #[error("invalid domain name")]
    InvalidDomain,
    /// The domain names no tower: it does not exist, it has no SRV record for the service, or every
    /// record is unusable.
    #[error("the domain names no tower")]
    NoTower,
    /// The domain names more than [`MAX_TOWERS`] usable towers with distinct canonical URLs.
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

/// Returns the towers for SRV answers given as (target, port) pairs.
///
/// Each usable pair becomes [`TowerEntry::from_url`] of `https://<target>:<port>`, so its URL is
/// the one [`canonical_tower_url`] gives and its QAD setting is the default for HTTPS: UDP 7842
/// (SRV carries no QAD port). The result is sorted by URL; two pairs that come to the same URL are
/// one tower, the first kept. This function does not apply the bound of [`MAX_TOWERS`]; the
/// locator does.
///
/// A pair is skipped when:
/// - its target is empty or `.` (RFC 2782: the service is not available), or its port is 0;
/// - its target holds anything but ASCII letters, digits, hyphens and dots, so that the URL parser
///   cannot rewrite it (no percent-decoding, no international name, no URL syntax);
/// - [`TowerEntry::from_url`] refuses the URL (an empty label, a label of more than 63 characters
///   or one that begins or ends with a hyphen, a name of more than 253, more than one final dot);
/// - the host is an IP address, also one the URL parser reads from a numeric name such as
///   `1.2.3.4`: an SRV target is a DNS name.
pub fn towers_from_srv(records: impl IntoIterator<Item = (String, u16)>) -> Vec<TowerEntry> {
    let mut towers = BTreeMap::new();
    for (target, port) in records {
        if let Some(entry) = tower_entry(&target, port) {
            towers.entry(entry.url().to_owned()).or_insert(entry);
        }
    }
    towers.into_values().collect()
}

/// The tower for one SRV answer, or `None` if the answer is unusable.
fn tower_entry(target: &str, port: u16) -> Option<TowerEntry> {
    if port == 0
        || target.is_empty()
        || target == "."
        || !target
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
    {
        return None;
    }
    let entry = TowerEntry::from_url(&format!("https://{target}:{port}")).ok()?;
    // `canonical_tower_url` accepts IP literals for explicit tower URLs; an SRV target is a name.
    let url = Url::parse(entry.url()).ok()?;
    matches!(url.host(), Some(Host::Domain(_))).then_some(entry)
}

/// Whether `name`, without a trailing dot, is a plain host name: labels of 1 to 63 ASCII letters,
/// digits and hyphens that neither begin nor end with a hyphen, separated by single dots, at most
/// 253 characters in all. The rule of [`srv_name`] for the bootstrap domain, which is not a URL;
/// tower hosts follow [`canonical_tower_url`], which applies the same rule to DNS names.
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
    Pin<Box<dyn Future<Output = Result<Vec<TowerEntry>, LocateError>> + Send + 'a>>;

/// Finds the towers a domain names.
///
/// The station holds a `Locator` as `Arc<dyn Locator>`. [`DnsLocator`] is the real one; tests can
/// supply their own. The trait is object-safe, so `locate` returns a boxed future
/// ([`LocateFuture`]) rather than an `impl Future`. A lookup happens rarely, so the allocation does
/// not matter.
pub trait Locator: Send + Sync {
    /// Returns the towers for `domain`, as [`towers_from_srv`] makes them: canonical URLs, sorted,
    /// no two alike. The list is never empty and has at most [`MAX_TOWERS`] entries: a domain that
    /// names no tower is [`LocateError::NoTower`], one that names more is
    /// [`LocateError::TooManyTowers`].
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
    /// A locator for the resolver `dns` describes: [`DnsConfig::System`] is [`Self::system`],
    /// [`DnsConfig::Nameserver`] is [`Self::with_nameserver`]. Construction does no IO. The
    /// deadline is [`DEFAULT_LOCATE_TIMEOUT`].
    pub fn new(dns: DnsConfig) -> Self {
        let source = match dns {
            DnsConfig::System => Source::System,
            DnsConfig::Nameserver(addr) => {
                let resolver = DnsResolver::builder()
                    .nameserver(Nameserver::new(addr, DnsProtocol::Udp))
                    .build();
                Source::Nameserver(Box::new(resolver))
            }
        };
        Self {
            source,
            timeout: DEFAULT_LOCATE_TIMEOUT,
        }
    }

    /// A locator over the system's resolver configuration, with no public fallback:
    /// `DnsLocator::new(DnsConfig::System)`.
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
        Self::new(DnsConfig::System)
    }

    /// A locator that asks only the nameserver at `addr`:
    /// `DnsLocator::new(DnsConfig::Nameserver(addr))`. For tests, and for people who must name
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
        Self::new(DnsConfig::Nameserver(addr))
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
    /// time driver enabled. More than [`MAX_TOWERS`] towers, counted after
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
    async fn lookup(&self, domain: &str) -> Result<Vec<TowerEntry>, LocateError> {
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
        if towers.len() > MAX_TOWERS {
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

    use super::{DnsLocator, LookupResolver, Source};
    use crate::DnsConfig;

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

    // Constructed only: neither locator is asked anything, so no test reaches a real resolver.
    #[test]
    fn new_follows_the_dns_config() {
        let system = DnsLocator::new(DnsConfig::System);
        assert!(matches!(system.source, Source::System));
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 53));
        let named = DnsLocator::new(DnsConfig::Nameserver(addr));
        assert!(matches!(named.source, Source::Nameserver(_)));
        assert!(matches!(DnsLocator::system().source, Source::System));
        assert!(matches!(
            DnsLocator::with_nameserver(addr).source,
            Source::Nameserver(_)
        ));
        for locator in [system, named] {
            let debug = format!("{locator:?}");
            assert_eq!(debug, "DnsLocator { .. }");
            assert!(!debug.contains("127.0.0.1") && !debug.contains("53"));
        }
    }
}
