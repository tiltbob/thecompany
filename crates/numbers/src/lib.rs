//! `numbers`: the network library.
//!
//! Node identity, tower lookup, and authenticated endpoints. The library takes iroh
//! without default features, as described in `docs/ARCHITECTURE.md`.

pub mod endpoint;
pub mod gate;
pub mod identity;
pub mod towers;

pub use endpoint::{
    Endpoint, EndpointBindError, EndpointConfig, EndpointRouterBuilder, IpTransport,
};
pub use gate::{Authorizer, CLOSE_NOT_ALLOWED, Protocol};
pub use identity::{NodeId, generate_node_key, node_id};
pub use towers::{TowerSet, TowerSetError};
