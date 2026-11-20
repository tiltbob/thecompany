//! `numbers`: the network library.
//!
//! Node identity, tower lookup, authenticated links, and chat streams. The library
//! takes iroh without default features, as described in `docs/ARCHITECTURE.md`.

pub mod endpoint;
pub mod gate;
pub mod identity;
pub mod link;
pub mod proto;
pub mod towers;

pub use endpoint::{
    Endpoint, EndpointBindError, EndpointConfig, EndpointRouterBuilder, IpTransport,
};
pub use gate::{Authorizer, CLOSE_NOT_ALLOWED, Protocol};
pub use identity::{NodeId, generate_node_key, node_id};
pub use link::{
    IncomingChat, LINK_ALPN, Link, LinkConfig, LinkHandle, LinkPath, PathUpdate, SendError,
    ShutdownError,
};
pub use towers::{TowerSet, TowerSetError};
