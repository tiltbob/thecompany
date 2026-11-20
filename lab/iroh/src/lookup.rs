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

/// M2: answers every id with the URLs of a shared `RelayMap`, so `insert_relay` / `remove_relay`
/// on the endpoint that was built from the same map show up here (T23 checks it).
#[derive(Debug, Clone)]
pub struct MapLookup {
    map: iroh::RelayMap,
}

impl MapLookup {
    pub fn new(map: iroh::RelayMap) -> Self {
        Self { map }
    }
}

impl AddressLookup for MapLookup {
    fn resolve(&self, id: EndpointId) -> Option<BoxStream<Result<Item, Error>>> {
        let urls: Vec<RelayUrl> = self.map.urls();
        if urls.is_empty() {
            return None;
        }
        let data = EndpointData::from_iter(urls.into_iter().map(TransportAddr::Relay));
        let item = Item::new(EndpointInfo::from_parts(id, data), "map", None);
        Some(stream::once(Ok(item)).boxed())
    }
}
