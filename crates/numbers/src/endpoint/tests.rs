use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, IncomingFilterOutcome, ProtocolHandler};
use numbers_tower::{Tower, TowerConfig};
use tokio::sync::mpsc::{UnboundedSender, error::TryRecvError, unbounded_channel};
use tokio::time::timeout;

use super::{DnsConfig, Endpoint, EndpointBindError, EndpointConfig, IpTransport, TlsRoots};
use crate::{Authorizer, TowerSet, generate_node_key};

const ALPN: &[u8] = b"test/endpoint/0";

#[derive(Debug)]
struct ObserveAccept(UnboundedSender<()>);

impl ProtocolHandler for ObserveAccept {
    async fn accept(&self, _connection: Connection) -> Result<(), AcceptError> {
        let _ = self.0.send(());
        Ok(())
    }
}

#[tokio::test]
async fn bind_rejects_unsupported_tls_and_dns_settings() {
    let key = generate_node_key();
    let policy = Arc::new(Authorizer::new(&[ALPN], &[]));
    for tls_roots in [TlsRoots::WebPkiPlus(vec![]), TlsRoots::CustomOnly(vec![])] {
        let config = EndpointConfig {
            tls_roots,
            ..EndpointConfig::default()
        };
        assert!(matches!(
            Endpoint::bind(&key, config, policy.clone()).await,
            Err(EndpointBindError::UnsupportedTlsRoots)
        ));
    }
    let config = EndpointConfig {
        dns: DnsConfig::Nameserver((Ipv4Addr::LOCALHOST, 53).into()),
        ..EndpointConfig::default()
    };
    assert!(matches!(
        Endpoint::bind(&key, config, policy).await,
        Err(EndpointBindError::UnsupportedDns)
    ));
}

#[tokio::test]
async fn router_ignores_unknown_relayed_peer_before_handshake() {
    timeout(Duration::from_secs(45), async {
        let tower = Tower::spawn(TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into()))
            .await
            .unwrap_or_else(|_| panic!("start loopback tower"));
        let config = EndpointConfig {
            towers: TowerSet::from_url(tower.url()).expect("parse loopback tower URL"),
            ip_transport: IpTransport::RelayOnly,
            ..EndpointConfig::default()
        };
        let callee = Endpoint::bind(
            &generate_node_key(),
            config.clone(),
            Arc::new(Authorizer::new(&[ALPN], &[])),
        )
        .await
        .unwrap_or_else(|_| panic!("bind callee"));
        let caller_policy = Arc::new(Authorizer::new(&[ALPN], &[]));
        caller_policy.replace_peers([(callee.iroh().id(), "callee".into())]);
        let caller = Endpoint::bind(&generate_node_key(), config, caller_policy)
            .await
            .unwrap_or_else(|_| panic!("bind caller"));

        let (send_decisions, mut decisions) = unbounded_channel();
        let (send_receipts, mut receipts) = unbounded_channel();
        let mut builder = callee.router_builder();
        builder.filter_decisions = Some(send_decisions);
        let router = builder.accept(ALPN, ObserveAccept(send_receipts)).spawn();
        timeout(Duration::from_secs(30), async {
            tokio::join!(caller.iroh().online(), callee.iroh().online());
        })
        .await
        .expect("endpoints did not come online through the tower");

        {
            let dial = caller.iroh().connect(callee.iroh().id(), ALPN);
            tokio::pin!(dial);
            tokio::select! {
                decision = decisions.recv() => {
                    assert_eq!(decision, Some(IncomingFilterOutcome::Ignore));
                }
                _ = &mut dial => panic!("the dial finished without an Ignore decision"),
            }

            // Check that the observed decision leaves the dial unanswered and no handler runs.
            assert!(
                timeout(Duration::from_secs(1), &mut dial).await.is_err(),
                "the ignored dial completed"
            );
            while let Ok(decision) = decisions.try_recv() {
                assert_eq!(decision, IncomingFilterOutcome::Ignore);
            }
            assert!(matches!(receipts.try_recv(), Err(TryRecvError::Empty)));
        }

        assert!(router.shutdown().await.is_ok());
        caller.iroh().close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await
    .expect("relayed stranger test timed out");
}
