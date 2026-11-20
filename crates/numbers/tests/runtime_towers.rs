#![cfg(test)]

use std::net::{Ipv4Addr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use iroh::TransportAddr;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use numbers::{
    Authorizer, Endpoint, EndpointConfig, IpTransport, TowerControl, TowerControlError, TowerEntry,
    TowerSet, TowerState, TowerStatus, canonical_tower_url, generate_node_key,
};
use numbers_tower::{Tower, TowerConfig};
use tokio::time::{sleep, timeout};

const ALPN: &[u8] = b"test/runtime-towers/0";
const ONLINE_TIMEOUT: Duration = Duration::from_secs(45);
const FAILOVER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct HoldConnection;

impl ProtocolHandler for HoldConnection {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        connection.closed().await;
        Ok(())
    }
}

fn checked<T, E>(result: Result<T, E>, message: &str) -> T {
    result.unwrap_or_else(|_| panic!("{message}"))
}

async fn start_tower() -> Tower {
    checked(
        Tower::spawn(TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into())).await,
        "start loopback tower",
    )
}

fn tower_url(tower: &Tower) -> String {
    canonical_tower_url(tower.url()).unwrap()
}

async fn bind(towers: TowerSet, ip_transport: IpTransport, policy: Arc<Authorizer>) -> Endpoint {
    checked(
        Endpoint::bind(
            &generate_node_key(),
            EndpointConfig {
                towers,
                ip_transport,
                ..EndpointConfig::default()
            },
            policy,
        )
        .await,
        "bind endpoint",
    )
}

fn states(control: &TowerControl) -> Vec<TowerState> {
    checked(control.tower_states(), "read tower states")
}

async fn wait_for_home(control: &TowerControl, url: &str, status: TowerStatus) {
    while !states(control)
        .iter()
        .any(|state| state.url() == url && state.is_home && state.status == status)
    {
        sleep(Duration::from_millis(20)).await;
    }
}

async fn connect_and_check_relay(dialer: &Endpoint, callee: &Endpoint, url: &str) {
    let connection = checked(
        dialer.iroh().connect(callee.iroh().id(), ALPN).await,
        "connect by bare id",
    );
    let mut paths = connection.paths_stream();
    let selected = paths.wait_any(|path| path.is_selected()).await;
    assert!(selected.is_some_and(|path| {
        matches!(path.remote_addr(), TransportAddr::Relay(relay) if relay.as_str() == url)
    }));
    assert!(connection.paths().iter().all(|path| !path.is_ip()));
    connection.close(0u32.into(), b"finished");
}

#[tokio::test]
async fn new_relay_only_connection_uses_second_tower_after_nudge() {
    let tower_one = start_tower().await;
    let first_url = tower_url(&tower_one);
    // Reserve the second address without serving relay traffic until both endpoints
    // have established their home and first connection through tower one.
    let reservation = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let second_address = reservation.local_addr().unwrap();
    let second_url = format!("http://{second_address}/");
    let towers = TowerSet::from_entries([
        TowerEntry::from_url(&first_url).unwrap(),
        TowerEntry::from_url(&second_url).unwrap(),
    ])
    .unwrap();

    let first_policy = Arc::new(Authorizer::new(&[ALPN], &[]));
    let second_policy = Arc::new(Authorizer::new(&[ALPN], &[]));
    let (first, second) = tokio::join!(
        bind(towers.clone(), IpTransport::RelayOnly, first_policy.clone()),
        bind(towers, IpTransport::RelayOnly, second_policy.clone()),
    );
    first_policy.replace_peers([(second.iroh().id(), "second".into())]);
    second_policy.replace_peers([(first.iroh().id(), "first".into())]);
    let first_router = first.router_builder().accept(ALPN, HoldConnection).spawn();
    let second_router = second.router_builder().accept(ALPN, HoldConnection).spawn();
    assert!(first.iroh().bound_sockets().is_empty());
    assert!(second.iroh().bound_sockets().is_empty());
    let first_control = first.tower_control();
    let second_control = second.tower_control();
    timeout(ONLINE_TIMEOUT, async {
        tokio::join!(
            wait_for_home(&first_control, &first_url, TowerStatus::Connected),
            wait_for_home(&second_control, &first_url, TowerStatus::Connected),
        );
        for control in [&first_control, &second_control] {
            assert!(states(control).iter().any(|state| {
                state.url() == second_url && !state.is_home && state.status == TowerStatus::Unknown
            }));
        }
        connect_and_check_relay(&first, &second, &first_url).await;
    })
    .await
    .expect("initial relay connection timed out");

    // Upstream may leave a detached upgrade alive if the tower shuts down immediately.
    sleep(Duration::from_secs(1)).await;
    drop(reservation);
    let tower_two = checked(
        Tower::spawn(TowerConfig::dev(second_address)).await,
        "start second loopback tower",
    );
    assert_eq!(tower_url(&tower_two), second_url);
    for control in [&first_control, &second_control] {
        assert!(states(control).iter().any(|state| {
            state.url() == first_url && state.is_home && state.status == TowerStatus::Connected
        }));
    }

    timeout(FAILOVER_TIMEOUT, async {
        checked(tower_one.shutdown().await, "stop first tower");
        tokio::join!(
            wait_for_home(&first_control, &first_url, TowerStatus::Failing),
            wait_for_home(&second_control, &first_url, TowerStatus::Failing),
        );
        // The measured plain-HTTP failover without intervention exceeds this budget.
        // Exercise the manual nudge, with both towers still configured.
        let (first_nudge, second_nudge) =
            tokio::join!(first_control.nudge_towers(), second_control.nudge_towers());
        checked(first_nudge, "nudge first endpoint");
        checked(second_nudge, "nudge second endpoint");
        tokio::join!(
            wait_for_home(&first_control, &second_url, TowerStatus::Connected),
            wait_for_home(&second_control, &second_url, TowerStatus::Connected),
        );
        connect_and_check_relay(&first, &second, &second_url).await;
    })
    .await
    .expect("fresh relay connection through tower two took over 30 seconds");
    let (first_shutdown, second_shutdown) =
        tokio::join!(first_router.shutdown(), second_router.shutdown());
    checked(first_shutdown, "stop first router");
    checked(second_shutdown, "stop second router");
    checked(tower_two.shutdown().await, "stop second tower");
}

#[tokio::test]
async fn replacing_towers_moves_home_without_rebinding() {
    let first = start_tower().await;
    let second = start_tower().await;
    let first_set = TowerSet::from_url(first.url()).unwrap();
    let second_set = TowerSet::from_url(second.url()).unwrap();
    let endpoint = bind(
        first_set,
        IpTransport::LoopbackOnly,
        Arc::new(Authorizer::new(&[ALPN], &[])),
    )
    .await;
    let sockets = endpoint.iroh().bound_sockets();
    assert_eq!(sockets.len(), 1);
    let control = endpoint.tower_control();

    timeout(ONLINE_TIMEOUT, async {
        wait_for_home(&control, &tower_url(&first), TowerStatus::Connected).await;
        checked(endpoint.set_towers(&second_set).await, "replace towers");
        assert_eq!(states(&control).len(), 1);
        wait_for_home(&control, &tower_url(&second), TowerStatus::Connected).await;
    })
    .await
    .expect("tower replacement timed out");
    assert_eq!(endpoint.iroh().bound_sockets(), sockets);
    checked(
        endpoint.set_towers(&TowerSet::default()).await,
        "remove all towers",
    );
    assert!(checked(endpoint.tower_states(), "read empty tower set").is_empty());
    endpoint.iroh().close().await;
    checked(first.shutdown().await, "stop first tower");
    checked(second.shutdown().await, "stop second tower");
}

#[tokio::test]
async fn refusing_home_tower_is_denied() {
    let tower = checked(
        Tower::spawn(
            TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into()).allow(std::iter::empty::<String>()),
        )
        .await,
        "start denying tower",
    );
    let endpoint = bind(
        TowerSet::from_url(tower.url()).unwrap(),
        IpTransport::RelayOnly,
        Arc::new(Authorizer::new(&[ALPN], &[])),
    )
    .await;
    let control = endpoint.tower_control();
    let reason = "not on this tower's list";
    timeout(
        ONLINE_TIMEOUT,
        wait_for_home(
            &control,
            &tower_url(&tower),
            TowerStatus::Denied {
                reason: reason.into(),
            },
        ),
    )
    .await
    .expect("tower denial was not observed");
    let state = &states(&control)[0];
    let debug = format!("{state:?}");
    assert!(!debug.contains(state.url()));
    assert!(!debug.contains(reason));
    endpoint.iroh().close().await;
    checked(tower.shutdown().await, "stop denying tower");
}

#[tokio::test]
async fn station_control_survives_endpoint_handoff_and_releases_with_router() {
    let tower = start_tower().await;
    let towers = TowerSet::from_url(tower.url()).unwrap();
    let endpoint = bind(
        TowerSet::default(),
        IpTransport::RelayOnly,
        Arc::new(Authorizer::new(&[ALPN], &[])),
    )
    .await;
    drop(endpoint.tower_control());
    assert!(!endpoint.iroh().is_closed());
    let control = endpoint.tower_control();
    let closed = endpoint.iroh().closed();
    let router = endpoint
        .router_builder()
        .accept(ALPN, HoldConnection)
        .spawn();
    drop(endpoint);
    checked(
        control.set_towers(&towers).await,
        "set towers after endpoint handoff",
    );
    timeout(
        ONLINE_TIMEOUT,
        wait_for_home(&control, &tower_url(&tower), TowerStatus::Connected),
    )
    .await
    .expect("endpoint did not come online after router handoff");
    drop(router);
    timeout(ONLINE_TIMEOUT, closed)
        .await
        .expect("tower control kept the endpoint alive");
    assert_eq!(control.tower_states(), Err(TowerControlError::Closed));
    assert_eq!(
        control.set_towers(&towers).await,
        Err(TowerControlError::Closed)
    );
    assert_eq!(control.nudge_towers().await, Err(TowerControlError::Closed));
    checked(tower.shutdown().await, "stop tower");
}
