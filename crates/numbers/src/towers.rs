//! Tower configuration for endpoint binding.

use std::fmt;

use iroh::{RelayConfig, RelayMap, RelayUrl};
use iroh_relay::{RelayQuicConfig, defaults::DEFAULT_RELAY_QUIC_PORT};
use serde::{Deserialize, Serialize};

pub use numbers_base::{MAX_TOWERS, TowerSetError, canonical_tower_url};

/// The configured tower set. Use the default for an empty set.
///
/// The serde representation is a list of records with `url` and `qad_port` fields.
/// An absent or null `qad_port` disables QAD. Unknown fields are rejected.
/// The list is validated when deserialized.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(try_from = "Vec<PersistedTower>", into = "Vec<PersistedTower>")]
pub struct TowerSet {
    entries: Vec<TowerEntry>,
}

/// A tower's canonical relay URL and QUIC address discovery (QAD) setting.
#[derive(Clone, PartialEq, Eq)]
pub struct TowerEntry {
    url: RelayUrl,
    qad: Qad,
}

/// Whether to use QUIC address discovery (QAD) for a tower.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Qad {
    /// Use no QUIC address discovery for this tower.
    Off,
    /// Probe this UDP port on the tower host.
    Port(u16),
}

impl Qad {
    fn port(self) -> Option<u16> {
        match self {
            Self::Off => None,
            Self::Port(port) => Some(port),
        }
    }
}

impl TowerEntry {
    /// Validates a tower URL and uses UDP 7842 for HTTPS or disables QAD for HTTP.
    pub fn from_url(url: &str) -> Result<Self, TowerSetError> {
        let url: RelayUrl = canonical_tower_url(url)?
            .parse()
            .map_err(|_| TowerSetError::InvalidUrl)?;
        let qad = if url.scheme() == "https" {
            Qad::Port(DEFAULT_RELAY_QUIC_PORT)
        } else {
            Qad::Off
        };
        Ok(Self { url, qad })
    }

    /// Sets a nonzero QAD port on an HTTPS tower.
    pub fn with_qad_port(mut self, port: u16) -> Result<Self, TowerSetError> {
        if port == 0 {
            return Err(TowerSetError::InvalidQadPort);
        }
        if self.url.scheme() != "https" {
            return Err(TowerSetError::QadRequiresHttps);
        }
        self.qad = Qad::Port(port);
        Ok(self)
    }

    /// Disables QAD for this tower.
    pub fn without_qad(mut self) -> Self {
        self.qad = Qad::Off;
        self
    }

    /// Returns the canonical root URL.
    pub fn url(&self) -> &str {
        self.url.as_str()
    }

    /// Returns this tower's QAD setting.
    pub fn qad(&self) -> Qad {
        self.qad
    }
}

impl fmt::Debug for TowerEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TowerEntry")
            .field("qad", &self.qad)
            .finish_non_exhaustive()
    }
}

impl TowerSet {
    /// Configures one tower from an explicit URL with its default QAD setting.
    pub fn from_url(url: &str) -> Result<Self, TowerSetError> {
        Ok(Self {
            entries: vec![TowerEntry::from_url(url)?],
        })
    }

    /// Configures up to eight towers with unique canonical URLs, in input order.
    pub fn from_entries(
        entries: impl IntoIterator<Item = TowerEntry>,
    ) -> Result<Self, TowerSetError> {
        let mut set = Self::default();
        for entry in entries {
            if set.len() == MAX_TOWERS {
                return Err(TowerSetError::TooManyTowers);
            }
            if set.entries.iter().any(|existing| existing.url == entry.url) {
                return Err(TowerSetError::DuplicateUrl);
            }
            set.entries.push(entry);
        }
        Ok(set)
    }

    /// Returns the configured tower entries.
    pub fn entries(&self) -> &[TowerEntry] {
        &self.entries
    }

    /// Iterates over the configured towers' canonical URLs.
    pub fn urls(&self) -> impl ExactSizeIterator<Item = &str> {
        self.entries.iter().map(TowerEntry::url)
    }

    /// Returns how many towers are configured.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no towers are configured.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns `(added, removed)` to go from this set to `other`.
    ///
    /// A QAD setting change removes the old entry and adds the new one at the same URL.
    /// When applying this diff to a map keyed by URL, apply removals first.
    pub fn diff(&self, other: &Self) -> (Vec<TowerEntry>, Vec<TowerEntry>) {
        let added = other.entries.iter().filter(|e| !self.entries.contains(e));
        let removed = self.entries.iter().filter(|e| !other.entries.contains(e));
        (added.cloned().collect(), removed.cloned().collect())
    }

    pub(crate) fn relay_map(&self) -> RelayMap {
        self.entries
            .iter()
            .map(|tower| {
                RelayConfig::new(
                    tower.url.clone(),
                    tower.qad.port().map(RelayQuicConfig::new),
                )
            })
            .collect()
    }
}

impl fmt::Debug for TowerSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TowerSet")
            .field("len", &self.len())
            .finish()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedTower {
    url: String,
    qad_port: Option<u16>,
}

impl TryFrom<Vec<PersistedTower>> for TowerSet {
    type Error = TowerSetError;

    fn try_from(towers: Vec<PersistedTower>) -> Result<Self, Self::Error> {
        if towers.len() > MAX_TOWERS {
            return Err(TowerSetError::TooManyTowers);
        }
        let entries = towers
            .into_iter()
            .map(|tower| {
                let entry = TowerEntry::from_url(&tower.url)?;
                match tower.qad_port {
                    Some(port) => entry.with_qad_port(port),
                    None => Ok(entry.without_qad()),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_entries(entries)
    }
}

impl From<TowerSet> for Vec<PersistedTower> {
    fn from(towers: TowerSet) -> Self {
        towers
            .entries
            .into_iter()
            .map(|tower| PersistedTower {
                url: tower.url().to_owned(),
                qad_port: tower.qad.port(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use iroh::RelayUrl;
    use serde_json::json;

    use super::{Qad, TowerEntry, TowerSet, TowerSetError};

    #[test]
    fn tower_entry_and_public_reexports_share_the_base_validator() {
        let input = "HTTPS://ToWeR.Example.Invalid.:443/";
        let canonical = numbers_base::canonical_tower_url(input).unwrap();
        let via_numbers: Result<String, numbers_base::TowerSetError> =
            crate::canonical_tower_url(input);
        assert_eq!(via_numbers.unwrap(), canonical);
        assert_eq!(TowerEntry::from_url(input).unwrap().url(), canonical);
        assert_eq!(crate::MAX_TOWERS, numbers_base::MAX_TOWERS);
        assert!(matches!(
            TowerEntry::from_url("http://tower.example.invalid"),
            Err(numbers_base::TowerSetError::NonLoopbackHttp)
        ));
    }

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

    #[test]
    fn rejects_duplicates_and_caps_the_set_at_eight() {
        let entries = (3340..3349)
            .map(|port| TowerEntry::from_url(&format!("http://127.0.0.1:{port}")).unwrap());
        let set = TowerSet::from_entries(entries.clone().take(8)).unwrap();
        assert_eq!(set.len(), 8);
        assert_eq!(set.urls().len(), 8);
        assert!(!set.is_empty());
        assert!(TowerSet::default().is_empty());
        assert!(matches!(
            TowerSet::from_entries(entries),
            Err(TowerSetError::TooManyTowers)
        ));
        assert!(matches!(
            TowerSet::from_entries([
                TowerEntry::from_url("https://example.com").unwrap(),
                TowerEntry::from_url("https://EXAMPLE.COM.:443/")
                    .unwrap()
                    .without_qad(),
            ]),
            Err(TowerSetError::DuplicateUrl)
        ));
    }

    #[test]
    fn diff_ignores_order_and_reports_qad_changes_as_replacements() {
        let keep = TowerEntry::from_url("https://example.com:3340").unwrap();
        let remove = TowerEntry::from_url("http://127.0.0.1:3341").unwrap();
        let add = TowerEntry::from_url("http://127.0.0.1:3342").unwrap();
        let before = TowerEntry::from_url("https://example.com").unwrap();
        let after = before.clone().with_qad_port(5340).unwrap();
        let old = TowerSet::from_entries([keep.clone(), remove.clone(), before.clone()]).unwrap();
        let new = TowerSet::from_entries([after.clone(), add.clone(), keep]).unwrap();
        assert_eq!(old.diff(&new), (vec![after, add], vec![remove, before]));

        let reordered = TowerSet::from_entries(new.entries().iter().rev().cloned()).unwrap();
        assert_eq!(new.diff(&reordered), (vec![], vec![]));
    }

    #[test]
    fn an_explicit_qad_port_is_used_in_the_relay_config() {
        let entry = TowerEntry::from_url("https://example.com")
            .unwrap()
            .with_qad_port(5340)
            .unwrap();
        assert_eq!(entry.qad(), Qad::Port(5340));
        let url: RelayUrl = entry.url().parse().unwrap();
        let set = TowerSet::from_entries([entry]).unwrap();
        let config = set.relay_map().get(&url).unwrap();
        assert_eq!(config.quic.as_ref().map(|qad| qad.port), Some(5340));
    }

    #[test]
    fn serde_round_trip_uses_canonical_urls_and_explicit_qad_settings() {
        let input = json!([
            {"url": "https://EXAMPLE.COM.:443", "qad_port": 5340},
            {"url": "https://example.com:3340", "qad_port": null},
            {"url": "https://example.com:3341"}
        ]);
        let set: TowerSet = serde_json::from_value(input).unwrap();
        assert_eq!(
            set.urls().collect::<Vec<_>>(),
            [
                "https://example.com/",
                "https://example.com:3340/",
                "https://example.com:3341/"
            ]
        );
        assert_eq!(set.entries()[0].qad(), Qad::Port(5340));
        assert_eq!(set.entries()[1].qad(), Qad::Off);
        assert_eq!(set.entries()[2].qad(), Qad::Off);
        let persisted = serde_json::to_value(&set).unwrap();
        assert_eq!(
            persisted,
            json!([
                {"url": "https://example.com/", "qad_port": 5340},
                {"url": "https://example.com:3340/", "qad_port": null},
                {"url": "https://example.com:3341/", "qad_port": null}
            ])
        );
        let restored: TowerSet = serde_json::from_value(persisted).unwrap();
        assert_eq!(restored.entries(), set.entries());
    }

    #[test]
    fn serde_rejects_invalid_tower_sets() {
        let oversized: Vec<_> = (3340..3349)
            .map(|port| json!({"url": format!("http://127.0.0.1:{port}")}))
            .collect();
        for input in [
            json!(oversized),
            json!([{"url": "http://example.com", "qad_port": null}]),
            json!([{"url": "https://example.com", "qad_port": 0}]),
            json!([{"url": "https://tower.example.invalid", "qadport": 7842}]),
            json!([{"url": "http://127.0.0.1:3340", "qad_port": 7842}]),
            json!([
                {"url": "https://EXAMPLE.COM.:443", "qad_port": 5340},
                {"url": "https://example.com/", "qad_port": null}
            ]),
        ] {
            assert!(serde_json::from_value::<TowerSet>(input).is_err());
        }
    }
}
