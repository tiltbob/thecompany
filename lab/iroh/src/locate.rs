//! M2 (section 11): SRV bootstrap. Resolvers pinned to one explicit nameserver with no public
//! fallback, the SRV lookup, and the tower URL built from an SRV answer.
use std::net::SocketAddr;

use iroh::RelayUrl;
use n0_dns_resolver::{DnsProtocol, DnsResolver, Nameserver, SrvRecordData};

pub const SERVICE: &str = "_numbers._tcp";

/// n0-dns-resolver with exactly one UDP nameserver. A fresh `Builder` reads nothing from the
/// host and has an empty fallback tier, so nothing else is ever queried.
pub fn srv_resolver(ns: SocketAddr) -> DnsResolver {
    DnsResolver::builder()
        .nameserver(Nameserver::new(ns, DnsProtocol::Udp))
        .build()
}

/// iroh's resolver (relay dial, HTTPS probe, QAD) with exactly one UDP nameserver.
/// NOT `iroh::dns::DnsResolver::with_nameserver(addr)`: that keeps the default fallback mode
/// (Deferred) and so adds Cloudflare, Google and Quad9 behind it.
pub fn iroh_resolver(ns: SocketAddr) -> iroh::dns::DnsResolver {
    iroh::dns::DnsResolver::builder()
        .add_nameserver_config(iroh::dns::NameserverConfig::udp(ns.ip()).with_port(ns.port()))
        .disable_fallback()
        .build()
}

/// `_numbers._tcp.<domain>.` as an FQDN (no search list).
pub fn srv_name(domain: &str) -> String {
    format!("{SERVICE}.{}.", domain.trim_end_matches('.'))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlForm {
    /// `https://host.example:443/`, as typed by people and as certificates name the host.
    NoDot,
    /// `https://host.example.:443/`, fully qualified.
    TrailingDot,
}

/// The tower URL for one SRV answer, or None for the "service not available" target ".".
pub fn tower_url(rec: &SrvRecordData, form: UrlForm) -> Option<RelayUrl> {
    let host = rec.target.trim_end_matches('.');
    if host.is_empty() {
        return None;
    }
    let dot = match form {
        UrlForm::NoDot => "",
        UrlForm::TrailingDot => ".",
    };
    format!("https://{host}{dot}:{}/", rec.port).parse().ok()
}

/// SRV answer -> sorted, deduplicated tower URLs. NXDOMAIN and NODATA both give an empty list.
pub async fn lookup(
    resolver: &DnsResolver,
    domain: &str,
    form: UrlForm,
) -> Result<Vec<RelayUrl>, n0_dns_resolver::Error> {
    let recs = match resolver.lookup_srv(srv_name(domain)).await {
        Ok(recs) => recs,
        Err(n0_dns_resolver::Error::NxDomain { .. }) => Vec::new(),
        Err(e) => return Err(e),
    };
    let mut urls: Vec<RelayUrl> = recs.iter().filter_map(|r| tower_url(r, form)).collect();
    urls.sort();
    urls.dedup();
    Ok(urls)
}
