//! Section 4: TowerLookup, answering every id with the tower URLs.
use std::sync::Arc;

use iroh::address_lookup::{AddressLookup, EndpointData, EndpointInfo, Error, Item};
use iroh::{EndpointId, RelayUrl, TransportAddr};
use n0_future::{StreamExt, boxed::BoxStream, stream};

#[derive(Debug, Clone)]
pub struct TowerLookup {
    relays: Arc<[RelayUrl]>,
}

impl TowerLookup {
    pub fn new(relays: impl IntoIterator<Item = RelayUrl>) -> Self {
        Self {
            relays: relays.into_iter().collect(),
        }
    }
}

impl AddressLookup for TowerLookup {
    fn resolve(&self, id: EndpointId) -> Option<BoxStream<Result<Item, Error>>> {
        if self.relays.is_empty() {
            return None;
        }
        let data = EndpointData::from_iter(self.relays.iter().cloned().map(TransportAddr::Relay));
        let item = Item::new(EndpointInfo::from_parts(id, data), "tower", None);
        Some(stream::once(Ok(item)).boxed())
    }
}
