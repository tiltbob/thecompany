//! Tower configuration for endpoint binding.

use std::fmt;

use iroh::{RelayConfig, RelayMap, RelayUrl};
use iroh_relay::RelayQuicConfig;
use url::Url;

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
    #[error("a tower URL cannot contain credentials, a query, or a fragment")]
    UnsupportedUrlParts,
}

impl TowerSet {
    /// Configures one tower from an explicit URL.
    ///
    /// HTTP has QUIC address discovery disabled. HTTPS uses the default QAD port.
    pub fn from_url(url: &str) -> Result<Self, TowerSetError> {
        let mut url = Url::parse(url).map_err(|_| TowerSetError::InvalidUrl)?;
        let qad = match url.scheme() {
            "http" => None,
            "https" => Some(RelayQuicConfig::default()),
            _ => return Err(TowerSetError::UnsupportedScheme),
        };
        if url.host_str().is_none() {
            return Err(TowerSetError::InvalidUrl);
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(TowerSetError::UnsupportedUrlParts);
        }

        // Canonicalize the base path before converting. RelayUrl otherwise logs the URL
        // when appending the slash needed to join relative relay paths.
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
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
        let http = TowerSet::from_url("http://127.0.0.1:3340/relay-prefix").unwrap();
        let http_url: RelayUrl = "http://127.0.0.1:3340/relay-prefix/".parse().unwrap();
        let http_config = http.relay_map().get(&http_url).unwrap();
        assert!(http_config.quic.is_none());

        let https = TowerSet::from_url("https://127.0.0.1:443").unwrap();
        let https_url: RelayUrl = "https://127.0.0.1/".parse().unwrap();
        let https_config = https.relay_map().get(&https_url).unwrap();
        assert_eq!(https_config.quic.as_ref().map(|qad| qad.port), Some(7842));

        assert!(TowerSet::default().relay_map().is_empty());
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
    }
}
