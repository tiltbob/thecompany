//! The whole-set announcement carried by a `towers` stream.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{MAX_TOWERS, Qad, TowerEntry, TowerSet};

/// An ordered snapshot of at most eight unique canonical HTTPS towers.
///
/// Its URLs and QAD settings are announcement data. Receiving a snapshot does not install it.
#[derive(Clone, PartialEq, Eq)]
pub struct TowerAnnouncement {
    entries: Vec<TowerEntry>,
}

impl TowerAnnouncement {
    /// Projects a local working set to its HTTPS entries, preserving order and QAD settings.
    pub fn from_working_set(set: &TowerSet) -> Self {
        Self {
            entries: set
                .entries()
                .iter()
                .filter(|entry| entry.url().starts_with("https://"))
                .cloned()
                .collect(),
        }
    }

    /// The towers in the announcement's order.
    pub fn entries(&self) -> &[TowerEntry] {
        &self.entries
    }
}

impl fmt::Debug for TowerAnnouncement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TowerAnnouncement")
            .field("len", &self.entries.len())
            .finish()
    }
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum WireTowerAnnouncement {
    Snapshot { towers: Vec<WireTower> },
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum WireTower {
    Entry { url: String, qad_port: Option<u16> },
}

impl From<&TowerAnnouncement> for WireTowerAnnouncement {
    fn from(announcement: &TowerAnnouncement) -> Self {
        Self::Snapshot {
            towers: announcement
                .entries()
                .iter()
                .map(|entry| WireTower::Entry {
                    url: entry.url().to_owned(),
                    qad_port: match entry.qad() {
                        Qad::Off => None,
                        Qad::Port(port) => Some(port),
                    },
                })
                .collect(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct InvalidAnnouncement;

impl TryFrom<WireTowerAnnouncement> for TowerAnnouncement {
    type Error = InvalidAnnouncement;

    fn try_from(wire: WireTowerAnnouncement) -> Result<Self, Self::Error> {
        let WireTowerAnnouncement::Snapshot { towers } = wire;
        if towers.len() > MAX_TOWERS {
            return Err(InvalidAnnouncement);
        }
        let entries = towers
            .into_iter()
            .map(|WireTower::Entry { url, qad_port }| {
                let entry = TowerEntry::from_url(&url).map_err(|_| InvalidAnnouncement)?;
                if !entry.url().starts_with("https://") || entry.url() != url {
                    return Err(InvalidAnnouncement);
                }
                match qad_port {
                    Some(port) => entry.with_qad_port(port).map_err(|_| InvalidAnnouncement),
                    None => Ok(entry.without_qad()),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        TowerSet::from_entries(entries.iter().cloned()).map_err(|_| InvalidAnnouncement)?;
        Ok(Self { entries })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum TowerReply {
    Ack,
    Nack { reason: TowerNackReason },
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TowerNackReason {
    UnknownProtocol,
    UnsupportedVersion,
    InvalidTowers,
    Unavailable,
}

#[cfg(test)]
mod tests;
