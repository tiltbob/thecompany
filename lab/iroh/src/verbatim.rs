//! The snippets of docs/notes/iroh-snippets.md, sections 2 to 10, copied as written (only wrapped
//! in functions and given the free variables they use). Built only with `--features verbatim`, to
//! record which ones compile as written. The working, corrected code is in the other modules.
#![allow(dead_code, unused_imports, unused_variables, unused_mut)]

type Res<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

// ---- Section 2 ----
mod s2 {
    use super::Res;
    use iroh::RelayUrl; // FIX1
    use iroh_relay::server::{ClientRateLimit, RelayConfig, Server, ServerConfig};
    use std::{net::Ipv4Addr, num::NonZeroU32};
    pub async fn run() -> Res {
        let mut relay = RelayConfig::new((Ipv4Addr::LOCALHOST, 0)); // tls: None => plain HTTP
        relay.limits.client_rx =                                    // bytes/s per client (plus burst)
            Some(ClientRateLimit::new(NonZeroU32::new(2 * 1024 * 1024).expect("non-zero")));
        let mut config = ServerConfig::default();
        config.relay = Some(relay);
        let server = Server::spawn(config).await?;
        let addr = server.http_addr().expect("relay enabled"); // https_addr() is None in this mode
        let url: RelayUrl = format!("http://{addr}").parse()?; // Tower::url(); http_url() is test-utils only
        server.shutdown().await?; // graceful; dropping the Server also stops it
        Ok(())
    }
}

// ---- Section 3 ----
mod s3 {
    use iroh::NetReportConfig;
    use iroh::{RelayConfig, RelayMap, RelayMode, RelayUrl};
    use iroh_relay::RelayQuicConfig; // FIX3

    /// One entry per tower: canonical URL and QAD port. None = no QAD (every http:// tower);
    /// a bare https:// URL defaults to 7842 (RelayQuicConfig::default()).
    pub fn relay_mode(towers: &[(RelayUrl, Option<u16>)]) -> RelayMode {
        let map: RelayMap = towers
            .iter()
            .map(|(url, qad)| RelayConfig::new(url.clone(), qad.map(RelayQuicConfig::new)))
            .collect();
        RelayMode::Custom(map)
    }

    pub fn net_report() {
        // NetReportConfig: iroh-1.5.0 src/net_report.rs; the Builder method that takes it was not recorded.
        let mut net_report = NetReportConfig::default();
        net_report.captive_portal_check = false; // no GET http://<tower-host>/generate_204 on port 80
    }
}

// ---- Section 4 ----
mod s4 {
    use iroh::address_lookup::{AddressLookup, EndpointData, EndpointInfo, Error, Item};
    use iroh::{EndpointId, RelayUrl, TransportAddr};
    use n0_future::{StreamExt, boxed::BoxStream, stream};
    use std::sync::Arc;
    #[derive(Debug, Clone)]
    pub struct TowerLookup {
        relays: Arc<[RelayUrl]>, // the same canonical list the RelayMap is built from
    }

    impl AddressLookup for TowerLookup {
        // publish(): default no-op; the tower routes by id.
        fn resolve(&self, id: EndpointId) -> Option<BoxStream<Result<Item, Error>>> {
            if self.relays.is_empty() {
                return None;
            }
            let data =
                EndpointData::from_iter(self.relays.iter().cloned().map(TransportAddr::Relay));
            let item = Item::new(EndpointInfo::from_parts(id, data), "tower", None);
            Some(stream::once(Ok(item)).boxed())
        }
    }
}

// ---- Section 5 ----
mod s5 {
    use super::Res;
    use iroh::{
        Endpoint, RelayMap, RelayMode,
        endpoint::{PortmapperConfig, presets},
    };
    use std::net::Ipv4Addr;
    pub struct Cfg {
        pub loopback_only: bool,
        pub relay_only: bool,
    }
    pub async fn run(
        secret: iroh::SecretKey,
        towers: Vec<(iroh::RelayUrl, Option<u16>)>,
        lookup: super::s4::TowerLookup,
        auth: std::sync::Arc<super::s6::Authorizer>,
        cfg: Cfg,
    ) -> Res {
        use super::s3::relay_mode;
        let mut builder = Endpoint::builder(presets::Minimal)
            .secret_key(secret) // iroh SecretKey from the 32 key bytes
            .relay_mode(relay_mode(&towers)) // or RelayMode::Custom(RelayMap::empty())
            .address_lookup(lookup.clone()) // section 4
            .hooks(super::s6::Gate { auth: auth.clone() }) // section 6
            .portmapper_config(PortmapperConfig::Disabled); // no UPnP/PCP/NAT-PMP; exported without the feature
        if cfg.loopback_only {
            builder = builder.bind_addr((Ipv4Addr::LOCALHOST, 0))?; // FIX2
        }
        if cfg.relay_only {
            builder = builder.clear_ip_transports(); // no UDP sockets, no QAD, no direct; online() still works
        }
        let ep = builder.bind().await?;
        Ok(())
    }
}

// ---- Section 6 ----
mod s6 {
    use iroh::endpoint::{
        AfterHandshakeOutcome, Connection, EndpointHooks, Incoming, IncomingAddr,
    };
    use iroh::protocol::{IncomingFilterOutcome, Router};
    use std::sync::Arc;

    #[derive(Debug, Default)]
    pub struct Authorizer(std::collections::HashSet<iroh::EndpointId>);
    impl Authorizer {
        pub fn contains(&self, id: &iroh::EndpointId) -> bool {
            self.0.contains(id)
        }
    }

    pub const LINK_ALPN: &[u8] = b"numbers/0";
    pub const CLOSE_NOT_ALLOWED: u32 = 403; // reviews used 1 and 403; define all close codes in `numbers`
    #[derive(Debug)]
    pub struct Gate {
        pub auth: Arc<Authorizer>,
    } // our type; must NOT hold an Endpoint (reference cycle)
    impl EndpointHooks for Gate {
        async fn after_handshake<'a>(&'a self, conn: &'a Connection) -> AfterHandshakeOutcome {
            if conn.alpn() != LINK_ALPN || self.auth.contains(&conn.remote_id()) {
                AfterHandshakeOutcome::Accept
            } else {
                AfterHandshakeOutcome::Reject {
                    error_code: CLOSE_NOT_ALLOWED.into(),
                    reason: Vec::new(),
                }
            }
        }
    }

    pub fn router(ep: iroh::Endpoint, auth: Arc<Authorizer>, link: super::s7::Link) -> Router {
        let auth_f = auth.clone();
        let router = Router::builder(ep)
            .incoming_filter(Arc::new(move |inc: &Incoming| match inc.remote_addr() {
                IncomingAddr::Relay { endpoint_id, .. } if !auth_f.contains(&endpoint_id) => {
                    IncomingFilterOutcome::Ignore // no answer at all
                }
                IncomingAddr::Ip(_) if !inc.remote_addr_validated() => IncomingFilterOutcome::Retry,
                _ => IncomingFilterOutcome::Accept,
            }))
            .accept(LINK_ALPN, link)
            .spawn();
        router
    }
}

// ---- Section 7 ----
mod s7 {
    use iroh::endpoint::Connection;
    use iroh::protocol::{AcceptError, ProtocolHandler};
    #[derive(Debug)]
    pub struct Link;
    impl ProtocolHandler for Link {
        async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
            let peer = conn.remote_id(); // infallible after the handshake
            while let Ok((send, recv)) = conn.accept_bi().await { /* StreamHeader, check, dispatch */
            }
            Ok(()) // returning drops and closes the connection
        }
    }
    pub fn refuse() -> AcceptError {
        n0_error::e!(AcceptError::NotAllowed)
    }
    pub fn refuse2() -> AcceptError {
        AcceptError::from_err(std::io::Error::other("x"))
    }
}

// ---- Section 8 ----
mod s8 {
    use super::Res;
    use super::s6::LINK_ALPN;
    use std::time::Duration;
    pub async fn run(ep: iroh::Endpoint, peer_id: iroh::EndpointId) -> Res {
        let conn = ep.connect(peer_id, LINK_ALPN).await?;
        let mut paths = conn.paths_stream();
        let selected_relay = paths.wait_any(|p| p.is_selected() && p.is_relay());
        let on_relay = tokio::time::timeout(Duration::from_secs(10), selected_relay)
            .await?
            .is_some();
        assert!(on_relay);
        assert!(conn.paths().iter().all(|p| !p.is_ip()));
        Ok(())
    }
}

// ---- Section 10 ----
mod s10 {
    use std::fs::{File, OpenOptions, TryLockError};
    use std::{io, path::Path};

    /// Held for the lifetime of the node; dropping the file releases the lock.
    pub struct InstanceLock(#[allow(dead_code)] File);

    pub fn lock_data_dir(dir: &Path) -> io::Result<InstanceLock> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("lock"))?; // reviews used "lock" and "node.lock"; any fixed name
        match file.try_lock() {
            Ok(()) => Ok(InstanceLock(file)),
            Err(TryLockError::WouldBlock) => Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "another station is already running with this data dir (one station per identity)",
            )),
            Err(TryLockError::Error(e)) => Err(e),
        }
    }
}
