#![cfg(test)]

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::{ConnectError, ConnectingError, Connection, ConnectionError};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{EndpointAddr, RelayUrl, TransportAddr};
use numbers::{
    Authorizer, Endpoint, EndpointConfig, IpTransport, NodeId, TowerSet, generate_node_key, node_id,
};
use numbers_tower::{Tower, TowerConfig};
use tokio::sync::mpsc::{
    UnboundedReceiver, UnboundedSender, error::TryRecvError, unbounded_channel,
};
use tokio::time::timeout;

const ALPN: &[u8] = b"test/endpoint/0";
const OTHER_ALPN: &[u8] = b"test/endpoint/1";
const TEST_TIMEOUT: Duration = Duration::from_secs(45);
// Under load a home relay may be selected on the next net report.
const ONLINE_TIMEOUT: Duration = Duration::from_secs(30);
const IGNORE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
struct ObserveAccept(UnboundedSender<NodeId>);

impl ProtocolHandler for ObserveAccept {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let _ = self.0.send(connection.remote_id());
        connection.closed().await;
        Ok(())
    }
}

async fn bind(config: EndpointConfig, authorizer: &Arc<Authorizer>) -> Endpoint {
    let key = generate_node_key();
    let endpoint = Endpoint::bind(&key, config, Arc::clone(authorizer))
        .await
        .unwrap_or_else(|_| panic!("bind endpoint"));
    assert!(endpoint.iroh().id() == node_id(&key));
    endpoint
}

async fn start_tower() -> (Tower, EndpointConfig) {
    let tower = Tower::spawn(TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into()))
        .await
        .unwrap_or_else(|_| panic!("start loopback tower"));
    let config = EndpointConfig {
        towers: TowerSet::from_url(tower.url()).unwrap_or_else(|_| panic!("parse tower URL")),
        ip_transport: IpTransport::RelayOnly,
    };
    (tower, config)
}

async fn bind_direct(authorizer: &Arc<Authorizer>) -> Endpoint {
    bind(
        EndpointConfig {
            towers: TowerSet::default(),
            ip_transport: IpTransport::LoopbackOnly,
        },
        authorizer,
    )
    .await
}

async fn wait_for_tower(first: &Endpoint, second: &Endpoint) {
    timeout(ONLINE_TIMEOUT, async {
        tokio::join!(first.iroh().online(), second.iroh().online());
    })
    .await
    .expect("endpoints did not come online through the tower");
}

fn direct_addr(endpoint: &Endpoint) -> EndpointAddr {
    let sockets = endpoint.iroh().bound_sockets();
    assert_eq!(sockets.len(), 1);
    assert_eq!(sockets[0].ip(), Ipv4Addr::LOCALHOST);
    assert_ne!(sockets[0].port(), 0);
    EndpointAddr::new(endpoint.iroh().id()).with_ip_addr(sockets[0])
}

fn observe(endpoint: &Endpoint, alpns: &[&[u8]]) -> (Router, UnboundedReceiver<NodeId>) {
    let (send, receive) = unbounded_channel();
    let mut router = endpoint.router_builder();
    for alpn in alpns {
        router = router.accept(*alpn, ObserveAccept(send.clone()));
    }
    (router.spawn(), receive)
}

async fn require_relay_path(connection: &Connection, expected: &RelayUrl) {
    let mut paths = connection.paths_stream();
    let selected = paths.wait_any(|path| path.is_selected()).await;
    assert!(selected.is_some_and(|path| {
        matches!(path.remote_addr(), TransportAddr::Relay(url) if url == expected)
    }));
    assert!(connection.paths().iter().all(|path| !path.is_ip()));
}

fn require_not_allowed(error: ConnectionError) {
    let ConnectionError::ApplicationClosed(close) = error else {
        panic!("expected application close");
    };
    assert_eq!(close.error_code, 403u32.into());
}

async fn require_remote_refusal(result: Result<Connection, ConnectError>) {
    match result {
        Ok(connection) => require_not_allowed(connection.closed().await),
        Err(ConnectError::Connecting {
            source: ConnectingError::ConnectionError { source, .. },
            ..
        })
        | Err(ConnectError::Connection { source, .. }) => require_not_allowed(source),
        Err(_) => panic!("expected a refusal from the remote endpoint"),
    }
}

#[tokio::test]
async fn authorized_endpoints_dial_by_id_through_the_tower() {
    timeout(TEST_TIMEOUT, async {
        let (tower, config) = start_tower().await;
        let relay = tower
            .url()
            .parse()
            .unwrap_or_else(|_| panic!("parse relay URL"));
        let first_auth = Arc::new(Authorizer::new(&[ALPN], &[]));
        let second_auth = Arc::new(Authorizer::new(&[ALPN], &[]));
        let first = bind(config.clone(), &first_auth).await;
        let second = bind(config, &second_auth).await;
        let (first_id, second_id) = (first.iroh().id(), second.iroh().id());
        first_auth.replace_peers([(second_id, "second".into())]);
        second_auth.replace_peers([(first_id, "first".into())]);
        let (first_router, mut first_receipts) = observe(&first, &[ALPN]);
        let (second_router, mut second_receipts) = observe(&second, &[ALPN]);

        assert!(first.iroh().bound_sockets().is_empty());
        assert!(second.iroh().bound_sockets().is_empty());
        wait_for_tower(&first, &second).await;
        let (first_dial, second_dial) = tokio::join!(
            first.iroh().connect(second_id, ALPN),
            second.iroh().connect(first_id, ALPN),
        );
        let first_dial = first_dial.unwrap_or_else(|_| panic!("first bare-id dial succeeds"));
        let second_dial = second_dial.unwrap_or_else(|_| panic!("second bare-id dial succeeds"));
        assert!(first_receipts.recv().await == Some(second_id));
        assert!(second_receipts.recv().await == Some(first_id));
        require_relay_path(&first_dial, &relay).await;
        require_relay_path(&second_dial, &relay).await;

        let (first_result, second_result) =
            tokio::join!(first_router.shutdown(), second_router.shutdown());
        assert!(first_result.is_ok());
        assert!(second_result.is_ok());
        assert!(tower.shutdown().await.is_ok());
    })
    .await
    .expect("authorized relay test timed out");
}

#[tokio::test]
async fn direct_stranger_and_unknown_alpn_are_refused_before_dispatch() {
    timeout(TEST_TIMEOUT, async {
        let caller_auth = Arc::new(Authorizer::new(&[ALPN, OTHER_ALPN], &[]));
        let callee_auth = Arc::new(Authorizer::new(&[ALPN], &[]));
        let caller = bind_direct(&caller_auth).await;
        let callee = bind_direct(&callee_auth).await;
        caller_auth.replace_peers([(callee.iroh().id(), "callee".into())]);
        // Register both with QUIC to exercise the gate after ALPN negotiation.
        let (router, mut receipts) = observe(&callee, &[ALPN, OTHER_ALPN]);
        let address = direct_addr(&callee);

        require_remote_refusal(caller.iroh().connect(address.clone(), ALPN).await).await;
        assert!(matches!(receipts.try_recv(), Err(TryRecvError::Empty)));

        callee_auth.replace_peers([(caller.iroh().id(), "caller".into())]);
        require_remote_refusal(caller.iroh().connect(address, OTHER_ALPN).await).await;
        assert!(matches!(receipts.try_recv(), Err(TryRecvError::Empty)));

        assert!(router.shutdown().await.is_ok());
        caller.iroh().close().await;
    })
    .await
    .expect("direct refusal test timed out");
}

#[tokio::test]
async fn relayed_stranger_is_ignored_before_dispatch() {
    timeout(TEST_TIMEOUT, async {
        let (tower, config) = start_tower().await;
        let caller_auth = Arc::new(Authorizer::new(&[ALPN], &[]));
        let callee_auth = Arc::new(Authorizer::new(&[ALPN], &[]));
        let caller = bind(config.clone(), &caller_auth).await;
        let callee = bind(config, &callee_auth).await;
        let (caller_id, callee_id) = (caller.iroh().id(), callee.iroh().id());
        caller_auth.replace_peers([(callee_id, "callee".into())]);
        let (router, mut receipts) = observe(&callee, &[ALPN]);
        wait_for_tower(&caller, &callee).await;

        let refused = timeout(IGNORE_TIMEOUT, caller.iroh().connect(callee_id, ALPN)).await;
        assert!(
            refused.is_err(),
            "expected the relayed dial to go unanswered"
        );
        assert!(matches!(receipts.try_recv(), Err(TryRecvError::Empty)));

        // The same caller can reach the same router once added to the peer book.
        callee_auth.replace_peers([(caller_id, "caller".into())]);
        let connection = caller.iroh().connect(callee_id, ALPN).await;
        assert!(connection.is_ok());
        assert!(receipts.recv().await == Some(caller_id));

        assert!(router.shutdown().await.is_ok());
        caller.iroh().close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await
    .expect("relayed stranger test timed out");
}

#[tokio::test]
async fn outgoing_dial_to_unknown_peer_closes_with_not_allowed() {
    timeout(TEST_TIMEOUT, async {
        let caller_auth = Arc::new(Authorizer::new(&[ALPN], &[]));
        let callee_auth = Arc::new(Authorizer::new(&[ALPN], &[]));
        let caller = bind_direct(&caller_auth).await;
        let callee = bind_direct(&callee_auth).await;
        callee_auth.replace_peers([(caller.iroh().id(), "caller".into())]);
        callee.iroh().set_alpns(vec![ALPN.to_vec()]);

        // Bypass the incoming filter for this probe so we can inspect the outgoing
        // hook's close even if it overtakes the callee's handshake.
        let accept = async {
            let incoming = callee.iroh().accept().await.expect("incoming connection");
            let accepting = incoming
                .accept()
                .unwrap_or_else(|_| panic!("accept direct connection"));
            match accepting.await {
                Ok(connection) => require_not_allowed(connection.closed().await),
                Err(ConnectingError::ConnectionError { source, .. }) => require_not_allowed(source),
                Err(_) => panic!("expected the caller to close its outgoing connection"),
            }
        };
        let (dial, ()) = tokio::join!(caller.iroh().connect(direct_addr(&callee), ALPN), accept);
        assert!(matches!(
            dial,
            Err(ConnectError::Connecting {
                source: ConnectingError::LocallyRejected { .. },
                ..
            })
        ));

        tokio::join!(caller.iroh().close(), callee.iroh().close());
    })
    .await
    .expect("outgoing refusal test timed out");
}
