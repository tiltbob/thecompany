//! Endpoint binding and bare-id lookup via configured towers.

use std::fmt;
use std::net::Ipv4Addr;
use std::sync::Arc;

use iroh::address_lookup::{AddressLookup, EndpointData, EndpointInfo, Error, Item};
use iroh::dns::DnsResolver;
use iroh::endpoint::{
    BindError, InvalidSocketAddr, NetReportConfig, PortmapperConfig, QuicTransportConfig, presets,
};
use iroh::protocol::{Router, RouterBuilder};
use iroh::{RelayMap, RelayMode, SecretKey, TransportAddr};
use n0_future::{StreamExt, boxed::BoxStream, stream};

use crate::gate::Gate;
use crate::{Authorizer, NodeId, TowerSet};

/// Endpoint settings supplied by the station.
#[derive(Clone, Debug, Default)]
pub struct EndpointConfig {
    /// The explicit tower configuration.
    pub towers: TowerSet,
    /// How to bind IP sockets.
    pub ip_transport: IpTransport,
}

/// Which IP sockets an endpoint opens.
#[derive(Clone, Copy, Debug, Default)]
pub enum IpTransport {
    /// Bind iroh's default IP sockets.
    #[default]
    Default,
    /// Bind a single IPv4 loopback socket.
    LoopbackOnly,
    /// Open no IP sockets for peer connections, so all paths go through a relay.
    RelayOnly,
}

/// An error opening the endpoint.
#[derive(thiserror::Error)]
pub enum EndpointBindError {
    #[error("could not configure the loopback socket for the numbers endpoint")]
    Loopback(#[source] InvalidSocketAddr),
    #[error("could not bind the numbers endpoint")]
    Bind(#[source] BindError),
}

impl fmt::Debug for EndpointBindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// A bound iroh endpoint and the policy used by its incoming router.
pub struct Endpoint {
    inner: iroh::Endpoint,
    gate: Gate,
}

impl Endpoint {
    /// Binds the endpoint without waiting for a tower to be reachable.
    pub async fn bind(
        key: &[u8; 32],
        config: EndpointConfig,
        authorizer: Arc<Authorizer>,
    ) -> Result<Self, EndpointBindError> {
        let relays = config.towers.relay_map();
        let gate = Gate { authorizer };
        let mut net_report = NetReportConfig::default();
        net_report.captive_portal_check = false;
        let transport_config = QuicTransportConfig::builder()
            .max_concurrent_bidi_streams(16u32.into())
            .max_concurrent_uni_streams(0u32.into())
            .build();
        let dns_resolver = DnsResolver::builder()
            .with_system_defaults()
            .disable_fallback()
            .build();
        let builder = iroh::Endpoint::builder(presets::Minimal)
            .secret_key(SecretKey::from_bytes(key))
            .relay_mode(RelayMode::Custom(relays.clone()))
            .address_lookup(TowerLookup { relays })
            .hooks(gate.clone())
            .net_report_config(net_report)
            .portmapper_config(PortmapperConfig::Disabled)
            .transport_config(transport_config)
            .dns_resolver(dns_resolver);
        let builder = match config.ip_transport {
            IpTransport::Default => builder,
            IpTransport::LoopbackOnly => builder
                .clear_ip_transports()
                .bind_addr((Ipv4Addr::LOCALHOST, 0))
                .map_err(EndpointBindError::Loopback)?,
            IpTransport::RelayOnly => builder.clear_ip_transports(),
        };
        let inner = builder.bind().await.map_err(EndpointBindError::Bind)?;
        Ok(Self { inner, gate })
    }

    /// Accesses iroh for outgoing dials, status, and shutdown.
    ///
    /// Inbound protocols must use [Self::router_builder] to apply the pre-handshake filter.
    /// Use fully handshaken connections so the authorization hook has run.
    pub fn iroh(&self) -> &iroh::Endpoint {
        &self.inner
    }

    /// Starts configuring the inbound router with the pre-handshake filter installed.
    ///
    /// Register the protocol handlers on the returned builder before spawning it.
    pub fn router_builder(&self) -> RouterBuilder {
        let gate = self.gate.clone();
        Router::builder(self.inner.clone())
            .incoming_filter(Arc::new(move |incoming| gate.filter(incoming)))
    }

    /// Returns the endpoint's policy for authorizing streams.
    pub fn authorizer(&self) -> &Authorizer {
        &self.gate.authorizer
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Endpoint").finish_non_exhaustive()
    }
}

struct TowerLookup {
    relays: RelayMap,
}

impl fmt::Debug for TowerLookup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TowerLookup").finish_non_exhaustive()
    }
}

impl AddressLookup for TowerLookup {
    fn resolve(&self, id: NodeId) -> Option<BoxStream<Result<Item, Error>>> {
        let urls: Vec<_> = self.relays.urls();
        if urls.is_empty() {
            return None;
        }
        let data: EndpointData = urls.into_iter().map(TransportAddr::Relay).collect();
        let item = Item::new(EndpointInfo::from_parts(id, data), "tower", None);
        Some(stream::once(Ok(item)).boxed())
    }
}
