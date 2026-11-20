//! Tower configuration for endpoint binding.

use std::fmt;

use iroh::{RelayConfig, RelayMap, RelayUrl};
use iroh_relay::RelayQuicConfig;
use url::{Host, Url};

/// The configured tower set. Use the default for an empty set.
#[derive(Clone, Default)]
pub struct TowerSet {
    tower: Option<Tower>,
}

#[derive(Clone)]
struct Tower {
    url: RelayUrl,
    qad: Option<RelayQuicConfig>,
}

/// An error parsing or validating an explicit tower URL.
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
}

impl TowerSet {
    /// Configures one tower from an explicit URL.
    ///
    /// Use a root URL with no credentials, query, or fragment. HTTP requires a
    /// literal loopback IP address and disables QUIC address discovery. HTTPS
    /// uses the default QAD port.
    pub fn from_url(url: &str) -> Result<Self, TowerSetError> {
        let url = Url::parse(url).map_err(|_| TowerSetError::InvalidUrl)?;
        let qad = match url.scheme() {
            "http" => None,
            "https" => Some(RelayQuicConfig::default()),
            _ => return Err(TowerSetError::UnsupportedScheme),
        };
        let host = url.host().ok_or(TowerSetError::InvalidUrl)?;
        if url.path() != "/"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(TowerSetError::UnsupportedUrlParts);
        }

        let is_loopback = match host {
            Host::Ipv4(ip) => ip.is_loopback(),
            Host::Ipv6(ip) => ip.is_loopback(),
            Host::Domain(_) => false,
        };
        if url.scheme() == "http" && !is_loopback {
            return Err(TowerSetError::NonLoopbackHttp);
        }
        Ok(Self {
            tower: Some(Tower {
                url: url.into(),
                qad,
            }),
        })
    }

    pub(crate) fn relay_map(&self) -> RelayMap {
        self.tower
            .iter()
            .map(|tower| RelayConfig::new(tower.url.clone(), tower.qad.clone()))
            .collect()
    }
}

impl fmt::Debug for TowerSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TowerSet")
            .field("len", &usize::from(self.tower.is_some()))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use iroh::RelayUrl;

    use super::{TowerSet, TowerSetError};

    #[test]
    fn http_and_https_have_explicit_qad_settings() {
        let http = TowerSet::from_url("http://127.0.0.1:3340").unwrap();
        let http_url: RelayUrl = "http://127.0.0.1:3340/".parse().unwrap();
        let http_config = http.relay_map().get(&http_url).unwrap();
        assert!(http_config.quic.is_none());

        let https = TowerSet::from_url("https://tower.example.invalid:443").unwrap();
        let https_url: RelayUrl = "https://tower.example.invalid/".parse().unwrap();
        let https_config = https.relay_map().get(&https_url).unwrap();
        assert_eq!(https_config.quic.as_ref().map(|qad| qad.port), Some(7842));

        assert!(TowerSet::default().relay_map().is_empty());
    }

    #[test]
    fn plain_http_requires_a_literal_loopback_ip() {
        assert!(TowerSet::from_url("http://127.2.3.4:3340").is_ok());
        assert!(TowerSet::from_url("http://[::1]:3340").is_ok());
        for url in [
            "http://192.0.2.10:3340",
            "http://[2001:db8::1]:3340",
            "http://localhost:3340",
            "http://tower.example.invalid:3340",
        ] {
            assert!(matches!(
                TowerSet::from_url(url),
                Err(TowerSetError::NonLoopbackHttp)
            ));
        }
    }

    #[test]
    fn rejects_unsupported_tower_urls() {
        assert!(matches!(
            TowerSet::from_url("file:///relay"),
            Err(TowerSetError::UnsupportedScheme)
        ));
        assert!(matches!(
            TowerSet::from_url("http://dev@127.0.0.1"),
            Err(TowerSetError::UnsupportedUrlParts)
        ));
        assert!(matches!(
            TowerSet::from_url("http://127.0.0.1?relay"),
            Err(TowerSetError::UnsupportedUrlParts)
        ));
        assert!(matches!(
            TowerSet::from_url("http://127.0.0.1/#relay"),
            Err(TowerSetError::UnsupportedUrlParts)
        ));
        assert!(matches!(
            TowerSet::from_url("http://127.0.0.1:3340/relay"),
            Err(TowerSetError::UnsupportedUrlParts)
        ));
        assert!(matches!(
            TowerSet::from_url("https://tower.example.invalid/relay-prefix/"),
            Err(TowerSetError::UnsupportedUrlParts)
        ));
    }
}
