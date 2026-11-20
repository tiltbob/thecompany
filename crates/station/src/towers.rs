//! Tower startup, the configured set, and the original provenance kept on disk.

use std::collections::HashSet;
use std::future::pending;
use std::sync::Arc;

use chief::api::{TowerSource, TowerState, TowerStatus, clean_tower_display};
use numbers::bootstrap::{LocateError, LocateFuture, srv_name};
use numbers::{
    DnsConfig, DnsLocator, Locator, Qad, TowerControl, TowerControlError, TowerEntry, TowerSet,
    TowerSetError,
};
use serde::{Deserialize, Serialize};

use crate::storage::{SaveOutcome, Storage};
use crate::{StartError, TowerInput};

const VERSION: u32 = 1;
pub(super) const LOG_TARGET: &str = "numbers_station::towers";
pub(super) const SYNC_ERROR: &str =
    "the tower record was written, but the station could not sync its data directory";

pub(super) type LocateResult = Result<Vec<TowerEntry>, LocateError>;

pub(super) struct RuntimeTowers {
    pub mode: TowerMode,
    pub control: TowerControl,
}

impl RuntimeTowers {
    pub(super) fn statuses(&self) -> Result<Vec<TowerStatus>, TowerControlError> {
        Ok(self.mode.statuses(&self.control.tower_states()?))
    }
}

pub(super) enum TowerMode {
    Pinned(TowerSet),
    Domain(DomainTowers),
}

pub(super) struct DomainTowers {
    pub record: RememberedTowers,
    pub lookup: Option<LocateFuture<'static>>,
    pub uncertain: bool,
    remembered: HashSet<String>,
}

impl DomainTowers {
    pub(super) fn apply(&mut self, record: RememberedTowers, saved: SaveOutcome) {
        self.remembered
            .retain(|url| record.set().urls().any(|entry| entry == url));
        self.record = record;
        self.uncertain = saved == SaveOutcome::DurabilityUncertain;
    }
}

impl TowerMode {
    /// Loads the tower configuration while the startup lock is held. `input` has been validated.
    pub(super) fn prepare(
        input: TowerInput,
        storage: &Storage,
        locator: Option<Arc<dyn Locator>>,
        dns: DnsConfig,
    ) -> Result<Self, StartError> {
        let requested = match input {
            TowerInput::Pinned(set) => return Ok(Self::Pinned(set)),
            TowerInput::Domain(domain) => Some(domain),
            TowerInput::Remembered => None,
        };
        let stored = match storage.load_towers() {
            Ok(record) => record,
            Err(_) => {
                tracing::warn!(target: LOG_TARGET, "ignoring an unusable remembered tower record");
                None
            }
        };
        let (record, reloaded) = match (requested, stored) {
            (None, None) => return Err(StartError::MissingTowerConfig),
            (None, Some(record)) => (record, true),
            (Some(domain), Some(record)) if domain == record.domain() => (record, true),
            (Some(domain), previous) => {
                let record = RememberedTowers::new(&domain, [])
                    .map_err(|_| StartError::InvalidBootstrapDomain)?;
                save_bootstrap_record(storage, &record)?;
                if previous.is_some() {
                    tracing::warn!(target: LOG_TARGET, "bootstrap domain changed; cleared the remembered towers");
                }
                (record, false)
            }
        };
        let lookup: Option<LocateFuture<'static>> = if record.set().is_empty() {
            if reloaded {
                // A failed startup can leave a visible empty record. Confirm it before discovery.
                save_bootstrap_record(storage, &record)?;
            }
            let domain = record.domain().to_owned();
            let locator = locator.unwrap_or_else(|| Arc::new(DnsLocator::new(dns)));
            Some(Box::pin(async move { locator.locate(&domain).await }))
        } else {
            None
        };
        let remembered = if reloaded {
            record.set().urls().map(str::to_owned).collect()
        } else {
            HashSet::new()
        };
        Ok(Self::Domain(DomainTowers {
            record,
            lookup,
            uncertain: false,
            remembered,
        }))
    }

    pub(super) fn set(&self) -> &TowerSet {
        match self {
            Self::Pinned(set) => set,
            Self::Domain(domain) => domain.record.set(),
        }
    }

    pub(super) fn domain(&self) -> Option<&str> {
        match self {
            Self::Pinned(_) => None,
            Self::Domain(domain) => Some(domain.record.domain()),
        }
    }

    /// Poll the saved lookup by reference so canceling a turn leaves the lookup in place.
    pub(super) async fn locate(&mut self) -> LocateResult {
        if let Self::Domain(domain) = self
            && let Some(lookup) = &mut domain.lookup
        {
            return lookup.await;
        }
        pending().await
    }

    pub(super) fn statuses(&self, observed: &[numbers::TowerState]) -> Vec<TowerStatus> {
        let row = |entry: &TowerEntry, source| TowerStatus {
            url: entry.url().to_owned(),
            state: display_state(observed.iter().find(|tower| tower.url() == entry.url())),
            source: Some(source),
        };
        match self {
            Self::Pinned(set) => set
                .entries()
                .iter()
                .map(|entry| row(entry, TowerSource::Configured))
                .collect(),
            Self::Domain(domain) => domain
                .record
                .entries()
                .map(|(entry, origin)| {
                    let source = if domain.remembered.contains(entry.url()) {
                        TowerSource::Remembered
                    } else {
                        origin.into()
                    };
                    row(entry, source)
                })
                .collect(),
        }
    }
}

fn save_bootstrap_record(storage: &Storage, record: &RememberedTowers) -> Result<(), StartError> {
    match storage.save_towers(record)? {
        SaveOutcome::Durable => Ok(()),
        SaveOutcome::DurabilityUncertain => Err(StartError::Storage {
            detail: SYNC_ERROR.to_owned(),
        }),
    }
}

fn display_state(observed: Option<&numbers::TowerState>) -> TowerState {
    let Some(observed) = observed else {
        return TowerState::Standby;
    };
    match &observed.status {
        numbers::TowerStatus::Connected => TowerState::Connected,
        numbers::TowerStatus::Denied { reason } => TowerState::Denied {
            reason: clean_tower_display(reason),
        },
        numbers::TowerStatus::Failing => TowerState::Failing {
            error: "could not connect to the tower".to_owned(),
        },
        numbers::TowerStatus::Unknown if observed.is_home => TowerState::Connecting,
        numbers::TowerStatus::Unknown => TowerState::Standby,
    }
}

/// The source at the time the station learned a tower. Reloading does not change it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum TowerOrigin {
    Configured,
    Srv,
    Peer,
}

impl From<TowerOrigin> for TowerSource {
    fn from(origin: TowerOrigin) -> Self {
        match origin {
            TowerOrigin::Configured => Self::Configured,
            TowerOrigin::Srv => Self::Srv,
            TowerOrigin::Peer => Self::Peer,
        }
    }
}

#[derive(Clone)]
pub(super) struct RememberedTowers {
    domain: String,
    set: TowerSet,
    // Each source belongs to the entry at the same position in the ordered set.
    origins: Vec<TowerOrigin>,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum TowerRecordError {
    #[error("expected a versioned tower record")]
    Malformed,
    #[error("unsupported tower record version")]
    Version,
    #[error("invalid bootstrap domain")]
    Domain,
    #[error("{0}")]
    Set(#[from] TowerSetError),
}

/// The domain form used for storage, equality, and the locator call.
pub(super) fn canonical_domain(domain: &str) -> Result<String, LocateError> {
    srv_name(domain)?;
    Ok(domain
        .strip_suffix('.')
        .unwrap_or(domain)
        .to_ascii_lowercase())
}

impl RememberedTowers {
    pub(super) fn new(
        domain: &str,
        entries: impl IntoIterator<Item = (TowerEntry, TowerOrigin)>,
    ) -> Result<Self, TowerRecordError> {
        let domain = canonical_domain(domain).map_err(|_| TowerRecordError::Domain)?;
        let mut origins = Vec::new();
        let set = TowerSet::from_entries(entries.into_iter().map(|(entry, origin)| {
            origins.push(origin);
            entry
        }))?;
        Ok(Self {
            domain,
            set,
            origins,
        })
    }

    pub(super) fn domain(&self) -> &str {
        &self.domain
    }

    pub(super) fn set(&self) -> &TowerSet {
        &self.set
    }

    pub(super) fn entries(&self) -> impl ExactSizeIterator<Item = (&TowerEntry, TowerOrigin)> {
        self.set()
            .entries()
            .iter()
            .zip(self.origins.iter().copied())
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, TowerRecordError> {
        let record: DiskTowers =
            serde_json::from_slice(bytes).map_err(|_| TowerRecordError::Malformed)?;
        if record.version != VERSION {
            return Err(TowerRecordError::Version);
        }
        let entries = record
            .towers
            .into_iter()
            .map(|tower| {
                let entry = TowerEntry::from_url(&tower.url)?;
                let entry = match tower.qad_port {
                    Some(port) => entry.with_qad_port(port)?,
                    None => entry.without_qad(),
                };
                Ok((entry, tower.source))
            })
            .collect::<Result<Vec<_>, TowerSetError>>()?;
        Self::new(&record.bootstrap_domain, entries)
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&DiskTowers {
            version: VERSION,
            bootstrap_domain: self.domain().to_owned(),
            towers: self
                .entries()
                .map(|(entry, source)| DiskTower {
                    url: entry.url().to_owned(),
                    qad_port: match entry.qad() {
                        Qad::Off => None,
                        Qad::Port(port) => Some(port),
                    },
                    source,
                })
                .collect(),
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskTowers {
    version: u32,
    bootstrap_domain: String,
    towers: Vec<DiskTower>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskTower {
    url: String,
    qad_port: Option<u16>,
    source: TowerOrigin,
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File, Permissions};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    use numbers::MAX_TOWERS;
    use serde_json::{Value, json};

    use super::*;

    fn decode(record: &Value) -> Result<RememberedTowers, TowerRecordError> {
        let bytes = serde_json::to_vec(record).expect("encode tower fixture");
        RememberedTowers::decode(&bytes)
    }

    #[test]
    fn absent_home_observation_is_standby() {
        assert_eq!(display_state(None), TowerState::Standby);
    }

    #[test]
    fn domain_startup_retries_confirm_an_empty_record_before_discovery() {
        struct StartupLocator;

        impl Locator for StartupLocator {
            fn locate<'a>(&'a self, _domain: &'a str) -> LocateFuture<'a> {
                panic!("startup must leave discovery to the actor");
            }
        }

        let temp = tempfile::tempdir().expect("make startup fixture directory");
        let data_dir = temp.path().join("station");
        let record_path = data_dir.join("towers.json");
        let storage = Storage::lock(&data_dir).expect("lock fixture");
        let entry = TowerEntry::from_url("http://127.0.0.1:3340/").expect("make fixture entry");
        let old = RememberedTowers::new("old.example.invalid", [(entry, TowerOrigin::Srv)])
            .expect("make old fixture record");
        storage.save_towers(&old).expect("store the old record");
        let old_bytes = fs::read(&record_path).expect("read previous domain record");
        let prepare = |input| {
            TowerMode::prepare(
                input,
                &storage,
                Some(Arc::new(StartupLocator)),
                DnsConfig::default(),
            )
        };
        let requested = || TowerInput::Domain("next.example.invalid".to_owned());

        storage.fail_next_tower_sync();
        fs::set_permissions(&record_path, Permissions::from_mode(0o644))
            .expect("make the old record unsafe to replace");
        assert!(
            matches!(prepare(requested()), Err(StartError::Storage { detail })
            if detail.contains("0600"))
        );
        assert!(fs::read(&record_path).expect("read unchanged old record") == old_bytes);
        fs::set_permissions(&record_path, Permissions::from_mode(0o600))
            .expect("restore record permissions");

        assert!(
            matches!(prepare(requested()), Err(StartError::Storage { detail })
            if detail == SYNC_ERROR)
        );
        let new = storage
            .load_towers()
            .expect("load the visible record")
            .expect("the replacement record exists");
        assert_eq!(new.domain(), "next.example.invalid");
        assert!(new.set().is_empty());
        let new_bytes = fs::read(&record_path).expect("read visible empty record");

        for input in [requested(), TowerInput::Remembered] {
            storage.fail_next_tower_sync();
            assert!(matches!(prepare(input), Err(StartError::Storage { detail })
                if detail == SYNC_ERROR));
            assert!(fs::read(&record_path).expect("read retried empty record") == new_bytes);
        }

        let uncertain = File::open(&record_path).expect("hold the uncertain record open");
        let uncertain_inode = uncertain
            .metadata()
            .expect("read the uncertain inode")
            .ino();
        let result = prepare(TowerInput::Remembered);
        assert!(matches!(result, Ok(TowerMode::Domain(domain))
            if domain.record.domain() == "next.example.invalid"
                && domain.record.set().is_empty() && domain.lookup.is_some()));
        let durable_inode = fs::metadata(&record_path)
            .expect("read the confirmed inode")
            .ino();
        assert_ne!(durable_inode, uncertain_inode);
        assert!(fs::read(record_path).expect("read confirmed empty record") == new_bytes);
    }

    #[test]
    fn remembered_record_round_trips_order_settings_and_original_sources() {
        let fixture = json!({
            "version": 1,
            "bootstrap_domain": "Boot.Example.Invalid.",
            "towers": [
                {"url": "HTTPS://First.Example.Invalid:443", "qad_port": 8610, "source": "srv"},
                {"url": "http://127.0.0.1:3341", "qad_port": null, "source": "configured"},
                {"url": "https://second.example.invalid/", "qad_port": null, "source": "peer"}
            ]
        });
        let record = decode(&fixture).expect("parse v1 tower record");
        assert_eq!(record.domain(), "boot.example.invalid");
        assert!(record.set().urls().eq([
            "https://first.example.invalid/",
            "http://127.0.0.1:3341/",
            "https://second.example.invalid/"
        ]));
        assert!(
            record
                .entries()
                .map(|(entry, source)| (entry.qad(), source))
                .eq([
                    (Qad::Port(8610), TowerOrigin::Srv),
                    (Qad::Off, TowerOrigin::Configured),
                    (Qad::Off, TowerOrigin::Peer),
                ])
        );

        let bytes = record.encode().expect("encode validated tower record");
        let mut expected = fixture;
        expected["bootstrap_domain"] = json!("boot.example.invalid");
        expected["towers"][0]["url"] = json!("https://first.example.invalid/");
        expected["towers"][1]["url"] = json!("http://127.0.0.1:3341/");
        assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), expected);
        let reloaded = RememberedTowers::decode(&bytes).expect("reload tower record");
        assert_eq!(reloaded.domain(), record.domain());
        assert!(reloaded.entries().eq(record.entries()));
    }

    #[test]
    fn rejects_unknown_version_domain_fields_and_invalid_entries() {
        let record = json!({
            "version": 1,
            "bootstrap_domain": "example.invalid",
            "towers": [{"url": "https://first.example.invalid/", "qad_port": 7842, "source": "srv"}]
        });
        let mut bad = record.clone();
        bad["version"] = json!(2);
        assert!(matches!(decode(&bad), Err(TowerRecordError::Version)));

        let mut bad = record.clone();
        bad["bootstrap_domain"] = json!("example.invalid..");
        assert!(matches!(decode(&bad), Err(TowerRecordError::Domain)));

        let mut bad = record.clone();
        bad["unknown"] = json!(true);
        assert!(matches!(decode(&bad), Err(TowerRecordError::Malformed)));

        for invalid in [
            json!({"url": "https://other.example.invalid/", "source": "remembered"}),
            json!({"url": "https://other.example.invalid/", "source": "future"}),
            json!({"url": "https://other.example.invalid/", "source": "peer", "unknown": true}),
            json!({"url": "http://other.example.invalid/", "source": "configured"}),
            json!({"url": "http://127.0.0.1:3341/", "qad_port": 7842, "source": "srv"}),
            json!({"url": "https://other.example.invalid/", "qad_port": 0, "source": "srv"}),
        ] {
            let mut bad = record.clone();
            bad["towers"].as_array_mut().unwrap().push(invalid);
            assert!(decode(&bad).is_err(), "a partial tower record was accepted");
        }
    }

    #[test]
    fn accepts_an_empty_or_full_set_and_rejects_canonical_duplicates_and_overflow() {
        let mut record = json!({
            "version": 1, "bootstrap_domain": "example.invalid", "towers": []
        });
        assert!(
            decode(&record)
                .expect("accept an empty set")
                .set()
                .is_empty()
        );
        let entry = |n| json!({"url": format!("http://127.0.0.1:{n}/"), "source": "configured"});
        for port in 3340..3340 + MAX_TOWERS {
            record["towers"].as_array_mut().unwrap().push(entry(port));
        }
        assert_eq!(
            decode(&record).expect("accept a full set").set().len(),
            MAX_TOWERS
        );
        record["towers"]
            .as_array_mut()
            .unwrap()
            .push(entry(3340 + MAX_TOWERS));
        assert!(matches!(
            decode(&record),
            Err(TowerRecordError::Set(TowerSetError::TooManyTowers))
        ));

        record["towers"] = json!([
            {"url": "https://other.example.invalid/", "qad_port": 7842, "source": "srv"},
            {"url": "HTTPS://OTHER.EXAMPLE.INVALID.:443", "qad_port": null, "source": "peer"}
        ]);
        assert!(matches!(
            decode(&record),
            Err(TowerRecordError::Set(TowerSetError::DuplicateUrl))
        ));
    }
}
