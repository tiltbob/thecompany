//! Helpers shared by the tower's integration tests: iroh endpoints acting as stations.
//!
//! Every id is generated at run time and nothing here prints one. The home relay status that
//! failure messages print carries the tower URL and the error, not an id.

// Each test file uses a different subset of these helpers.
#![allow(dead_code)]

use std::future::Future;
use std::net::Ipv4Addr;
use std::time::Duration;

use iroh::endpoint::{Connection, PortmapperConfig, RelayStatus, presets};
use iroh::tls::CaTlsConfig;
use iroh::{
    Endpoint, EndpointAddr, NetReportConfig, RelayConfig, RelayMap, RelayMode, RelayUrl, SecretKey,
    Watcher,
};
use numbers_tower::{Tower, TowerConfig};
use tokio::time::timeout;

pub const ALPN: &[u8] = b"test/tower/0";
pub const DENY_REASON: &str = "not on this tower's list";
/// Bounds every test, so that a regression fails instead of hanging.
pub const TEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Under load a home relay may be selected only on the next net report (20-26 s later).
pub const ONLINE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long an unlisted endpoint must stay offline.
pub const DENIED_WITHIN: Duration = Duration::from_secs(10);

/// Starts a tower and parses its URL.
pub async fn spawn(config: TowerConfig) -> (Tower, RelayUrl) {
    let tower = Tower::spawn(config)
        .await
        .unwrap_or_else(|error| panic!("start tower: {error}"));
    let url = tower
        .url()
        .parse()
        .unwrap_or_else(|_| panic!("parse tower URL"));
    (tower, url)
}

/// How a test station reaches its tower.
pub struct Station {
    /// The tower, and its QAD port if the station should probe it.
    pub relay: RelayConfig,
    /// The trusted roots for the tower's certificate; `None` keeps iroh's default roots.
    pub roots: Option<CaTlsConfig>,
    /// One UDP socket on 127.0.0.1, instead of none (relay-only).
    pub ip_socket: bool,
}

impl Station {
    /// A relay-only station of a tower without address discovery.
    pub fn relay_only(tower: &RelayUrl) -> Self {
        Self {
            relay: RelayConfig::new(tower.clone(), None),
            roots: None,
            ip_socket: false,
        }
    }

    /// Binds an endpoint with the settings a station uses and the given key.
    pub async fn bind(self, key: SecretKey) -> Endpoint {
        let mut net_report = NetReportConfig::default();
        net_report.captive_portal_check = false;
        let mut builder = Endpoint::builder(presets::Minimal)
            .secret_key(key)
            .relay_mode(RelayMode::Custom(RelayMap::from_iter([self.relay])))
            .clear_ip_transports()
            .portmapper_config(PortmapperConfig::Disabled)
            .net_report_config(net_report)
            .alpns(vec![ALPN.to_vec()]);
        if let Some(roots) = self.roots {
            builder = builder.ca_tls_config(roots);
        }
        if self.ip_socket {
            builder = builder
                .bind_addr((Ipv4Addr::LOCALHOST, 0))
                .unwrap_or_else(|_| panic!("set the bind address"));
        }
        builder
            .bind()
            .await
            .unwrap_or_else(|_| panic!("bind endpoint"))
    }
}

/// A relay-only endpoint homed on `tower`.
pub async fn station(key: SecretKey, tower: &RelayUrl) -> Endpoint {
    Station::relay_only(tower).bind(key).await
}

/// The home relay status as text, for failure messages. It carries no id.
pub fn status(endpoint: &Endpoint) -> String {
    format!("{:?}", endpoint.home_relay_status().get())
}

/// Whether the endpoint reports a connection to its home relay.
pub fn connected(endpoint: &Endpoint) -> bool {
    endpoint
        .home_relay_status()
        .get()
        .iter()
        .any(RelayStatus::is_connected)
}

pub async fn online(endpoint: &Endpoint) {
    if timeout(ONLINE_TIMEOUT, endpoint.online()).await.is_err() {
        panic!(
            "endpoint not online after 30 s; home relay status: {}",
            status(endpoint)
        );
    }
}

/// Waits until the home relay reports a denial, and returns its reason.
///
/// The endpoint first has to choose the tower as its home relay, which under load may happen
/// only with the next net report, so the wait has the same bound as coming online.
pub async fn denial(endpoint: &Endpoint) -> String {
    let mut watcher = endpoint.home_relay_status();
    let wait = async {
        loop {
            let reason = watcher
                .get()
                .iter()
                .find_map(|relay: &RelayStatus| relay.auth_denied_reason().map(str::to_owned));
            if let Some(reason) = reason {
                return reason;
            }
            if watcher.updated().await.is_err() {
                panic!("home relay status watcher closed");
            }
        }
    };
    match timeout(ONLINE_TIMEOUT, wait).await {
        Ok(reason) => reason,
        Err(_) => panic!(
            "no denial within 30 s; home relay status: {}",
            status(endpoint)
        ),
    }
}

/// Fails unless `future` is still pending after `within`.
pub async fn stays_pending<F: Future>(within: Duration, future: F, what: &str) {
    assert!(timeout(within, future).await.is_err(), "{what}");
}

pub fn dial_addr(endpoint: &Endpoint, tower: &RelayUrl) -> EndpointAddr {
    EndpointAddr::new(endpoint.id()).with_relay_url(tower.clone())
}

pub async fn accept(endpoint: &Endpoint) -> Connection {
    let incoming = endpoint
        .accept()
        .await
        .unwrap_or_else(|| panic!("endpoint closed"));
    let accepting = incoming
        .accept()
        .unwrap_or_else(|_| panic!("accept incoming connection"));
    accepting
        .await
        .unwrap_or_else(|_| panic!("complete incoming connection"))
}

pub async fn bounded(test: impl Future<Output = ()>) {
    if timeout(TEST_TIMEOUT, test).await.is_err() {
        panic!("test timed out after {TEST_TIMEOUT:?}");
    }
}
