//! Pure tower URL validation and the shared tower set limit.

use url::{Host, Url};

/// The maximum number of towers in a configured set.
pub const MAX_TOWERS: usize = 8;

/// An error parsing or validating a tower configuration.
#[derive(Debug, thiserror::Error)]
pub enum TowerSetError {
    #[error("invalid tower URL")]
    InvalidUrl,
    #[error("a tower URL must use HTTP or HTTPS")]
    UnsupportedScheme,
    #[error("a tower URL must have a root path and no credentials, query, or fragment")]
    UnsupportedUrlParts,
    #[error("plain HTTP tower URLs require a loopback IP address")]
    NonLoopbackHttp,
    #[error("QUIC address discovery requires an HTTPS tower")]
    QadRequiresHttps,
    #[error("the QUIC address discovery port must be nonzero")]
    InvalidQadPort,
    #[error("a tower set supports at most {MAX_TOWERS} towers")]
    TooManyTowers,
    #[error("a tower set contains a duplicate URL")]
    DuplicateUrl,
}

/// Returns a canonical root URL for an HTTPS tower or an HTTP tower on a loopback IP literal.
///
/// The canonical form uses lowercase DNS names without a trailing dot, omits the scheme's
/// default port, and ends in a root slash. Internationalized DNS names become ASCII.
/// IPv4 addresses must be written in dotted decimal.
pub fn canonical_tower_url(input: &str) -> Result<String, TowerSetError> {
    if input.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(TowerSetError::InvalidUrl);
    }
    let mut url = Url::parse(input).map_err(|_| TowerSetError::InvalidUrl)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(TowerSetError::UnsupportedScheme);
    }
    if url.port() == Some(0) {
        return Err(TowerSetError::InvalidUrl);
    }
    if url.path() != "/"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(TowerSetError::UnsupportedUrlParts);
    }

    let host = url.host().ok_or(TowerSetError::InvalidUrl)?;
    if let Host::Ipv4(ip) = host {
        // Compare with the original host to reject the URL parser's numeric aliases.
        let (_, authority_and_suffix) = input.split_once("://").ok_or(TowerSetError::InvalidUrl)?;
        let written = authority_and_suffix
            .split([':', '/', '?', '#'])
            .next()
            .unwrap_or_default();
        if written.strip_suffix('.').unwrap_or(written) != ip.to_string() {
            return Err(TowerSetError::InvalidUrl);
        }
    }
    if url.scheme() == "http" {
        let is_loopback = match host {
            Host::Ipv4(ip) => ip.is_loopback(),
            Host::Ipv6(ip) => ip.is_loopback(),
            Host::Domain(_) => false,
        };
        if !is_loopback {
            return Err(TowerSetError::NonLoopbackHttp);
        }
    }

    // The URL parser already lowercases DNS names and strips default ports.
    if let Host::Domain(domain) = host {
        let name = domain.strip_suffix('.').unwrap_or(domain);
        if name.len() > 253
            || name.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
        {
            return Err(TowerSetError::InvalidUrl);
        }
        if name != domain {
            let name = name.to_owned();
            url.set_host(Some(&name))
                .map_err(|_| TowerSetError::InvalidUrl)?;
        }
    }
    Ok(url.into())
}

#[cfg(test)]
mod tests {
    use super::{TowerSetError, canonical_tower_url};

    #[test]
    fn canonicalizes_explicit_and_srv_urls() {
        let explicit = canonical_tower_url("https://example.invalid").unwrap();
        let srv = canonical_tower_url("HTTPS://ExAmPlE.InVaLiD.:443/").unwrap();
        assert_eq!(explicit, "https://example.invalid/");
        assert_eq!(srv, explicit);
        assert_eq!(
            canonical_tower_url("https://ExAmPlE.InVaLiD.:8443").unwrap(),
            "https://example.invalid:8443/"
        );
        assert_eq!(
            canonical_tower_url("http://[0:0:0:0:0:0:0:1]:80").unwrap(),
            "http://[::1]/"
        );
        assert_eq!(
            canonical_tower_url("https://[2001:db8::1]").unwrap(),
            "https://[2001:db8::1]/"
        );
        assert_eq!(
            canonical_tower_url("https://bücher.example.invalid").unwrap(),
            "https://xn--bcher-kva.example.invalid/"
        );
    }

    #[test]
    fn accepts_only_dotted_decimal_ipv4() {
        for url in [
            "https://017.0.0.1",
            "https://1.2",
            "https://123",
            "http://127.1",
            "http://0x7f.1",
            "http://2130706433",
        ] {
            assert!(matches!(
                canonical_tower_url(url),
                Err(TowerSetError::InvalidUrl)
            ));
        }
        assert_eq!(
            canonical_tower_url("http://127.0.0.1.:3340").unwrap(),
            "http://127.0.0.1:3340/"
        );
        assert_eq!(
            canonical_tower_url("https://127.0.0.1").unwrap(),
            "https://127.0.0.1/"
        );
        assert_eq!(
            canonical_tower_url("https://192.0.2.1").unwrap(),
            "https://192.0.2.1/"
        );
    }

    #[test]
    fn rejects_zero_relay_ports_and_literal_whitespace() {
        for url in [
            "https://tower.example.invalid:0/",
            "http://127.0.0.1:0/",
            " https://tower.example.invalid",
            "https://tower.ex\tample.invalid",
            "https://tower.example.invalid/\n",
        ] {
            assert!(matches!(
                canonical_tower_url(url),
                Err(TowerSetError::InvalidUrl)
            ));
        }
    }

    #[test]
    fn rejects_invalid_dns_hosts() {
        for url in [
            "https://",
            "https://./",
            "https://example.invalid../",
            "https://-example.invalid/",
            "https://example-.invalid/",
            "https://example_.invalid/",
            "https://example..invalid/",
        ] {
            assert!(matches!(
                canonical_tower_url(url),
                Err(TowerSetError::InvalidUrl)
            ));
        }
    }

    #[test]
    fn validates_dns_label_and_name_length_boundaries() {
        let label = "a".repeat(63);
        let name = format!("{label}.{label}.{label}.{}", "a".repeat(61));
        for dot in ["", "."] {
            for host in [format!("{label}.example.invalid"), name.clone()] {
                assert_eq!(
                    canonical_tower_url(&format!("https://{host}{dot}/")).unwrap(),
                    format!("https://{host}/")
                );
            }
            for host in [format!("{label}a.example.invalid"), format!("{name}a")] {
                assert!(matches!(
                    canonical_tower_url(&format!("https://{host}{dot}/")),
                    Err(TowerSetError::InvalidUrl)
                ));
            }
        }
    }
}
