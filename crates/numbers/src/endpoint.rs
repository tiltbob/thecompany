//! Endpoint binding and bare-id lookup via configured towers.

use std::fmt;
use std::future::poll_fn;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex as SnapshotMutex, MutexGuard, PoisonError, Weak};
use std::task::Poll;

use iroh::address_lookup::{AddressLookup, EndpointData, EndpointInfo, Error, Item};
use iroh::dns::DnsResolver;
use iroh::endpoint::{
    BindError, Incoming, InvalidSocketAddr, NetReportConfig, PortmapperConfig, QuicTransportConfig,
    RelayStatus, presets,
};
use iroh::protocol::{DynProtocolHandler, Router, RouterBuilder as IrohRouterBuilder};
use iroh::{RelayConfig, RelayMap, RelayMode, RelayUrl, SecretKey, TransportAddr, Watcher};
use n0_future::task::AbortOnDropHandle;
use n0_future::{StreamExt, boxed::BoxStream, stream};
use tokio::sync::Mutex;

use crate::gate::Gate;
use crate::{Authorizer, MAX_TOWERS, NodeId, TowerSet};

/// Endpoint settings supplied by the station.
#[derive(Clone, Debug, Default)]
pub struct EndpointConfig {
    /// The explicit tower configuration.
    pub towers: TowerSet,
    /// How to bind IP sockets.
    pub ip_transport: IpTransport,
    /// Which certificate roots to trust for TLS connections to towers.
    pub tls_roots: TlsRoots,
    /// Which DNS resolver to use for tower hosts.
    pub dns: DnsConfig,
}

/// Certificate roots for TLS connections to towers.
///
/// [Endpoint::bind] currently supports only [Self::WebPki].
#[derive(Clone, Default)]
pub enum TlsRoots {
    /// Trust the bundled WebPKI roots.
    #[default]
    WebPki,
    /// Trust the bundled WebPKI roots and the supplied DER-encoded certificates.
    WebPkiPlus(Vec<Vec<u8>>),
    /// Trust only the supplied DER-encoded certificates.
    CustomOnly(Vec<Vec<u8>>),
}

impl fmt::Debug for TlsRoots {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WebPki => f.write_str("WebPki"),
            Self::WebPkiPlus(_) => f.debug_tuple("WebPkiPlus").finish_non_exhaustive(),
            Self::CustomOnly(_) => f.debug_tuple("CustomOnly").finish_non_exhaustive(),
        }
    }
}

/// DNS resolver settings for tower hosts.
///
/// [Endpoint::bind] currently supports only [Self::System].
#[derive(Clone, Copy, Default)]
pub enum DnsConfig {
    /// Use system DNS with public fallback resolvers disabled.
    #[default]
    System,
    /// Use the specified nameserver.
    Nameserver(SocketAddr),
}

impl fmt::Debug for DnsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::System => f.write_str("System"),
            Self::Nameserver(_) => f.debug_tuple("Nameserver").finish_non_exhaustive(),
        }
    }
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
    #[error("custom TLS roots are not supported by the numbers endpoint yet")]
    UnsupportedTlsRoots,
    #[error("an explicit DNS nameserver is not supported by the numbers endpoint yet")]
    UnsupportedDns,
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
    runtime: Arc<EndpointRuntime>,
}

struct EndpointRuntime {
    _tower_observer: AbortOnDropHandle<()>,
    endpoint: iroh::Endpoint,
    gate: Gate,
    relays: RelayMap,
    tower_snapshot: Arc<SnapshotMutex<TowerSnapshot>>,
    /// Serializes map edits and keeps the URL whose notification may have been interrupted.
    tower_updates: Mutex<Option<RelayUrl>>,
}

struct TowerSnapshot {
    wanted: TowerSet,
    // The endpoint binds one relay transport, which observes one home at a time.
    home_failure: Option<(String, TowerStatus)>,
}

impl TowerSnapshot {
    fn lock(snapshot: &SnapshotMutex<Self>) -> MutexGuard<'_, Self> {
        snapshot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn configure(&mut self, wanted: &TowerSet) {
        if let Some((url, _)) = &self.home_failure
            && !wanted.urls().any(|configured| configured == url)
        {
            self.home_failure = None;
        }
        self.wanted = wanted.clone();
    }

    fn observe(&mut self, home: Option<&str>, status: TowerStatus) -> TowerStatus {
        if let Some(url) = home {
            match status {
                TowerStatus::Failing | TowerStatus::Denied { .. } => {
                    self.home_failure = Some((url.to_owned(), status.clone()));
                    return status;
                }
                TowerStatus::Unknown => {
                    if let Some((previous, failure)) = &self.home_failure
                        && previous == url
                    {
                        return failure.clone();
                    }
                }
                TowerStatus::Connected => {}
            }
        }
        self.home_failure = None;
        status
    }

    fn observe_relays<'a>(&mut self, home: &'a [RelayStatus]) -> (Option<&'a str>, TowerStatus) {
        let observed = home
            .iter()
            .find(|relay| self.wanted.urls().any(|url| url == relay.url().as_str()));
        let status = match observed {
            Some(relay) if relay.is_connected() => TowerStatus::Connected,
            Some(relay) => match relay.auth_denied_reason() {
                Some(reason) => TowerStatus::denied(reason),
                None if relay.last_error().is_some() => TowerStatus::Failing,
                None => TowerStatus::Unknown,
            },
            None => TowerStatus::Unknown,
        };
        let url = observed.map(|relay| relay.url().as_str());
        (url, self.observe(url, status))
    }
}

fn watch_tower_home(
    endpoint: &iroh::Endpoint,
    snapshot: &Arc<SnapshotMutex<TowerSnapshot>>,
) -> AbortOnDropHandle<()> {
    let mut home = endpoint.home_relay_status();
    TowerSnapshot::lock(snapshot).observe_relays(&home.get());
    let snapshot = Arc::downgrade(snapshot);
    let closed = endpoint.closed();
    AbortOnDropHandle::new(tokio::spawn(async move {
        let observe = async {
            while poll_fn(|cx| {
                let Some(snapshot) = snapshot.upgrade() else {
                    return Poll::Ready(false);
                };
                // Sample and apply under the same lock used by tower_states. The task must
                // not overwrite a newer synchronous observation with an earlier sample.
                let mut snapshot = TowerSnapshot::lock(&snapshot);
                match home.poll_updated(cx) {
                    Poll::Ready(Ok(())) => {
                        snapshot.observe_relays(home.peek());
                        Poll::Ready(true)
                    }
                    Poll::Ready(Err(_)) => Poll::Ready(false),
                    Poll::Pending => Poll::Pending,
                }
            })
            .await
            {}
        };
        let _ = closed.run_until(observe).await;
    }))
}

/// A station's handle for observing and updating its endpoint's towers.
///
/// The bound [Endpoint] and its gated router own the runtime. A station can
/// retain this handle while the router is running. Keeping or dropping an
/// idle handle has no effect on the endpoint's lifetime.
#[derive(Clone)]
pub struct TowerControl {
    runtime: Weak<EndpointRuntime>,
}

impl fmt::Debug for TowerControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TowerControl").finish_non_exhaustive()
    }
}

/// An error observing or updating the runtime tower set.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TowerControlError {
    /// The bound endpoint is closed or its runtime was dropped.
    #[error("the numbers endpoint is closed")]
    Closed,
}

/// The endpoint's observation of one currently configured tower.
#[derive(Clone, PartialEq, Eq)]
pub struct TowerState {
    url: String,
    /// Whether iroh has selected this configured tower as home.
    pub is_home: bool,
    /// The last observed connection status.
    pub status: TowerStatus,
}

impl TowerState {
    /// Returns the configured canonical URL.
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl fmt::Debug for TowerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TowerState")
            .field("is_home", &self.is_home)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

/// The connection information available for a configured tower.
#[derive(Clone, PartialEq, Eq)]
pub enum TowerStatus {
    /// This tower is not the observed home, or no connection outcome has been observed yet.
    Unknown,
    /// The endpoint is connected to this tower.
    Connected,
    /// The last observed connection attempt failed. The station supplies any display text.
    Failing,
    /// The tower refused the endpoint.
    Denied {
        /// Untrusted tower text of unrestricted length, with `char::is_control` characters
        /// replaced by spaces. The station applies its API's cleaning and display-size limit
        /// before showing this to a frontend.
        reason: String,
    },
}

impl TowerStatus {
    fn denied(reason: &str) -> Self {
        Self::Denied {
            reason: reason.replace(char::is_control, " "),
        }
    }
}

impl fmt::Debug for TowerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "Unknown",
            Self::Connected => "Connected",
            Self::Failing => "Failing",
            Self::Denied { .. } => "Denied",
        })
    }
}

impl Endpoint {
    /// Binds the endpoint without waiting for a tower to be reachable.
    pub async fn bind(
        key: &[u8; 32],
        config: EndpointConfig,
        authorizer: Arc<Authorizer>,
    ) -> Result<Self, EndpointBindError> {
        if !matches!(config.tls_roots, TlsRoots::WebPki) {
            return Err(EndpointBindError::UnsupportedTlsRoots);
        }
        if !matches!(config.dns, DnsConfig::System) {
            return Err(EndpointBindError::UnsupportedDns);
        }
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
            .address_lookup(TowerLookup {
                relays: relays.clone(),
            })
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
        let endpoint = builder.bind().await.map_err(EndpointBindError::Bind)?;
        let tower_snapshot = Arc::new(SnapshotMutex::new(TowerSnapshot {
            wanted: config.towers,
            home_failure: None,
        }));
        let tower_observer = watch_tower_home(&endpoint, &tower_snapshot);
        Ok(Self {
            runtime: Arc::new(EndpointRuntime {
                _tower_observer: tower_observer,
                endpoint,
                gate,
                relays,
                tower_snapshot,
                tower_updates: Mutex::new(None),
            }),
        })
    }

    /// Accesses iroh only for outgoing dials, status, and shutdown.
    ///
    /// The authorization hook checks the authenticated remote id and negotiated ALPN
    /// on outgoing connections too. Use fully handshaken connections so the hook has run.
    ///
    /// Serve all inbound protocols through [Self::router_builder]. Never call
    /// [`iroh::Endpoint::accept`], [`iroh::Endpoint::set_alpns`], or [`Router::builder`]
    /// with this handle. Doing so can bypass the pre-handshake filter.
    /// Use [Self::set_towers] and [Self::nudge_towers] for relay-map changes.
    pub fn iroh(&self) -> &iroh::Endpoint {
        &self.runtime.endpoint
    }

    /// Returns the station's tower control handle.
    pub fn tower_control(&self) -> TowerControl {
        TowerControl {
            runtime: Arc::downgrade(&self.runtime),
        }
    }

    /// Applies changes to the configured towers without rebinding the endpoint.
    ///
    /// See [TowerControl::set_towers] for cancellation and existing relay connections.
    pub async fn set_towers(&self, towers: &TowerSet) -> Result<(), TowerControlError> {
        self.runtime.set_towers(towers).await
    }

    /// Reads the configured-tower snapshot. See [TowerControl::tower_states].
    pub fn tower_states(&self) -> Result<Vec<TowerState>, TowerControlError> {
        self.runtime.tower_states()
    }

    /// Reconciles the relay map and requests a full net report if the configured set is nonempty.
    ///
    /// Call this only when the home is missing or disconnected. A full report can move the home
    /// off a working tower. See [TowerControl::nudge_towers] for cancellation behavior.
    pub async fn nudge_towers(&self) -> Result<(), TowerControlError> {
        self.runtime.nudge_towers().await
    }

    /// Starts configuring the inbound router, which applies the pre-handshake filter.
    ///
    /// Register the protocol handlers on the returned builder before spawning it.
    pub fn router_builder(&self) -> EndpointRouterBuilder {
        EndpointRouterBuilder {
            inner: Router::builder(self.runtime.endpoint.clone()),
            runtime: Arc::clone(&self.runtime),
            #[cfg(test)]
            filter_decisions: None,
        }
    }

    /// Returns the endpoint's policy for authorizing streams.
    pub fn authorizer(&self) -> &Authorizer {
        &self.runtime.gate.authorizer
    }

    pub(crate) fn authorizer_handle(&self) -> Arc<Authorizer> {
        Arc::clone(&self.runtime.gate.authorizer)
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Endpoint").finish_non_exhaustive()
    }
}

impl TowerControl {
    /// Applies changes to the configured towers without rebinding the endpoint.
    ///
    /// The requested set becomes the configured snapshot before relay-map edits begin. If the
    /// update is interrupted, the live relay map may be partially updated. A later update
    /// reconciles the map with its target and delivers any interrupted change notification.
    /// Lookup can see both old and new URLs during an update, up to [`MAX_TOWERS`]. Reapplying
    /// a fully applied set sends no notification.
    ///
    /// Removing a tower from the relay map does not disconnect an existing relay connection.
    /// Iroh may keep the removed home if the new set is empty or no replacement responds.
    pub async fn set_towers(&self, towers: &TowerSet) -> Result<(), TowerControlError> {
        self.runtime()?.set_towers(towers).await
    }

    /// Reads the configured-tower snapshot, including during a relay-map update. A configured
    /// home retains its last observed failure or denial while retrying. A connected observation
    /// or a change of observed home clears the old failure.
    ///
    /// The runtime observes the home even between calls. Iroh's latest-value watcher can
    /// coalesce brief transitions.
    ///
    /// A home removed from the configured set is omitted even if it is still connected.
    /// To observe the actual home, use [`iroh::Endpoint::home_relay_status`].
    pub fn tower_states(&self) -> Result<Vec<TowerState>, TowerControlError> {
        self.runtime()?.tower_states()
    }

    /// Reconciles the relay map with the configured set and requests a full net report if the
    /// set is nonempty. Call this when the home tower is missing or disconnected. A full report
    /// can change the home even when its connection works. While a report is running, a
    /// periodic tick can replace a pending full-report request with an incremental one.
    ///
    /// If interrupted, a later [Self::set_towers] or nudge reconciles the map and delivers
    /// any interrupted map-edit notification. A cancelled request for an unchanged map
    /// takes another explicit nudge to request again.
    pub async fn nudge_towers(&self) -> Result<(), TowerControlError> {
        self.runtime()?.nudge_towers().await
    }

    fn runtime(&self) -> Result<Arc<EndpointRuntime>, TowerControlError> {
        self.runtime.upgrade().ok_or(TowerControlError::Closed)
    }
}

impl EndpointRuntime {
    fn ensure_open(&self) -> Result<(), TowerControlError> {
        if self.endpoint.is_closed() {
            return Err(TowerControlError::Closed);
        }
        Ok(())
    }

    async fn set_towers(&self, towers: &TowerSet) -> Result<(), TowerControlError> {
        let mut pending = self.tower_updates.lock().await;
        self.apply_tower_update(Some(towers), &mut pending).await
    }

    async fn nudge_towers(&self) -> Result<(), TowerControlError> {
        let mut pending = self.tower_updates.lock().await;
        self.apply_tower_update(None, &mut pending).await
    }

    fn tower_snapshot(&self) -> MutexGuard<'_, TowerSnapshot> {
        TowerSnapshot::lock(&self.tower_snapshot)
    }

    /// Called with `tower_updates` held through all map edits and notifications. `None` nudges
    /// the current configuration.
    async fn apply_tower_update(
        &self,
        towers: Option<&TowerSet>,
        pending: &mut Option<RelayUrl>,
    ) -> Result<(), TowerControlError> {
        self.ensure_open()?;
        let desired = {
            let mut snapshot = self.tower_snapshot();
            if let Some(towers) = towers {
                snapshot.configure(towers);
            }
            snapshot.wanted.relay_map()
        };
        let mut notify = towers.is_none();
        let mut obsolete = self
            .relays
            .urls::<Vec<_>>()
            .into_iter()
            .filter(|url| !desired.contains(url));
        for relay in desired.relays::<Vec<_>>() {
            let current = self.relays.get(&relay.url);
            if current.as_ref() == Some(&relay) {
                continue;
            }
            // Replace changed settings at the same URL in place. When adding at capacity,
            // one URL must be obsolete since the desired set has room for the new URL.
            if current.is_none()
                && self.relays.len() == MAX_TOWERS
                && let Some(url) = obsolete.next()
            {
                self.change_relay(url, None, pending).await;
            }
            self.change_relay(relay.url.clone(), Some(relay), pending)
                .await;
            notify = false;
        }
        for url in obsolete {
            self.change_relay(url, None, pending).await;
            notify = false;
        }

        // The report sees the whole map. One completed edit covers any interrupted notice.
        // If the map already matched, replay a pending notice or send one for an explicit nudge.
        if let Some(url) = pending.clone() {
            let relay = self.relays.get(&url);
            self.change_relay(url, relay, pending).await;
        } else if notify && let Some(relay) = self.relays.relays::<Vec<_>>().into_iter().next() {
            // An explicit nudge of a matching map leaves no edit to replay.
            self.endpoint.insert_relay(relay.url.clone(), relay).await;
        }
        self.ensure_open()
    }

    async fn change_relay(
        &self,
        url: RelayUrl,
        relay: Option<Arc<RelayConfig>>,
        pending: &mut Option<RelayUrl>,
    ) {
        // Iroh mutates the map before awaiting delivery of RelayMapChange. Keep the URL even
        // if the future is dropped after the mutation and before the notification is sent.
        *pending = Some(url.clone());
        if let Some(relay) = relay {
            self.endpoint.insert_relay(url, relay).await;
        } else {
            self.endpoint.remove_relay(&url).await;
        }
        *pending = None;
    }

    fn tower_states(&self) -> Result<Vec<TowerState>, TowerControlError> {
        self.ensure_open()?;
        let mut snapshot = self.tower_snapshot();
        let home = self.endpoint.home_relay_status().get();
        let (home_url, status) = snapshot.observe_relays(&home);
        Ok(snapshot
            .wanted
            .urls()
            .map(|url| {
                let is_home = Some(url) == home_url;
                TowerState {
                    url: url.to_owned(),
                    is_home,
                    status: if is_home {
                        status.clone()
                    } else {
                        TowerStatus::Unknown
                    },
                }
            })
            .collect())
    }
}

/// Registers protocol handlers for an endpoint's gated inbound router.
pub struct EndpointRouterBuilder {
    inner: IrohRouterBuilder,
    runtime: Arc<EndpointRuntime>,
    #[cfg(test)]
    filter_decisions:
        Option<tokio::sync::mpsc::UnboundedSender<iroh::protocol::IncomingFilterOutcome>>,
}

impl EndpointRouterBuilder {
    /// Registers a handler for an ALPN in negotiation order.
    pub fn accept(
        mut self,
        alpn: impl AsRef<[u8]>,
        handler: impl Into<Box<dyn DynProtocolHandler>>,
    ) -> Self {
        self.inner = self.inner.accept(alpn, handler);
        self
    }

    /// Installs the endpoint's gate and starts accepting connections.
    #[must_use = "Router aborts when dropped, use Router::shutdown to shut the router down cleanly"]
    pub fn spawn(self) -> Router {
        let Self {
            inner,
            runtime,
            #[cfg(test)]
            filter_decisions,
        } = self;
        // The router's filter keeps the runtime available to the station's
        // weak tower control handle after the Endpoint wrapper is dropped.
        let filter = move |incoming: &Incoming| runtime.gate.filter(incoming);
        #[cfg(test)]
        let filter = move |incoming: &Incoming| {
            let decision = filter(incoming);
            if let Some(observer) = &filter_decisions {
                let _ = observer.send(decision);
            }
            decision
        };
        inner.incoming_filter(Arc::new(filter)).spawn()
    }
}

impl fmt::Debug for EndpointRouterBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EndpointRouterBuilder")
            .finish_non_exhaustive()
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

#[cfg(test)]
mod tests;
