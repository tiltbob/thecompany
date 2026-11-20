//! `numbers`: the network library.
//!
//! Node identity, tower lookup, and authenticated endpoints. The library takes iroh
//! without default features, as described in `docs/ARCHITECTURE.md`.
//!
//! - [`bootstrap`]: finding a domain's towers through DNS SRV.
//! - `testing` (cargo feature `test-support`): `FakeDns`, a DNS server on 127.0.0.1 for tests.

pub mod bootstrap;
pub mod endpoint;
pub mod gate;
pub mod identity;
#[cfg(feature = "test-support")]
pub mod testing;
pub mod towers;

pub use bootstrap::{DnsLocator, LocateError, LocateFuture, Locator};
pub use endpoint::{
    DnsConfig, Endpoint, EndpointBindError, EndpointConfig, EndpointRouterBuilder, IpTransport,
    TlsRoots,
};
pub use gate::{Authorizer, CLOSE_NOT_ALLOWED, Protocol};
pub use identity::{NodeId, generate_node_key, node_id};
pub use towers::{MAX_TOWERS, Qad, TowerEntry, TowerSet, TowerSetError, canonical_tower_url};
