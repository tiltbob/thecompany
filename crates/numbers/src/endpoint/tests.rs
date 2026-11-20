use std::future::{Future, poll_fn};
use std::net::{Ipv4Addr, TcpListener};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, IncomingFilterOutcome, ProtocolHandler};
use iroh::{RelayUrl, Watcher};
use iroh_relay::server::{Access, AccessControl, ClientRequest, RelayConfig, Server, ServerConfig};
use numbers_tower::{Tower, TowerConfig};
use tokio::sync::Barrier;
use tokio::sync::mpsc::{UnboundedSender, error::TryRecvError, unbounded_channel};
use tokio::task::coop::{consume_budget, has_budget_remaining};
use tokio::time::{sleep, timeout};

use super::{
    DnsConfig, Endpoint, EndpointBindError, EndpointConfig, IpTransport, TlsRoots, TowerSnapshot,
    TowerStatus,
};
use crate::{Authorizer, MAX_TOWERS, TowerEntry, TowerSet, generate_node_key};

const ALPN: &[u8] = b"test/endpoint/0";

#[derive(Debug)]
struct ObserveAccept(UnboundedSender<()>);

impl ProtocolHandler for ObserveAccept {
    async fn accept(&self, _connection: Connection) -> Result<(), AcceptError> {
        let _ = self.0.send(());
        Ok(())
    }
}

#[test]
fn dns_config_debug_hides_the_nameserver() {
    let dns = DnsConfig::Nameserver((Ipv4Addr::LOCALHOST, 53).into());
    assert_eq!(format!("{dns:?}"), "Nameserver(..)");
}

#[test]
fn denied_status_replaces_controls_with_spaces() {
    let TowerStatus::Denied { reason } = TowerStatus::denied("hello\nworld\tfrom\u{0085}tower")
    else {
        panic!("expected tower denial");
    };
    assert_eq!(reason, "hello world from tower");
}

#[tokio::test]
async fn replacing_qad_updates_the_config_at_the_same_url() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let entry =
        TowerEntry::from_url(&format!("https://{}", listener.local_addr().unwrap())).unwrap();
    let url: RelayUrl = entry.url().parse().unwrap();
    let initial = TowerSet::from_entries([entry.clone()]).unwrap();
    let updated = TowerSet::from_entries([entry.with_qad_port(7342).unwrap()]).unwrap();
    let endpoint = Endpoint::bind(
        &generate_node_key(),
        EndpointConfig {
            towers: initial,
            ip_transport: IpTransport::RelayOnly,
            ..EndpointConfig::default()
        },
        Arc::new(Authorizer::new(&[ALPN], &[])),
    )
    .await
    .unwrap();
    let relays = &endpoint.runtime.relays;
    assert_eq!(relays.get(&url).unwrap().quic.as_ref().unwrap().port, 7842);
    {
        let mut pending = endpoint.runtime.tower_updates.lock().await;
        assert!(
            poll_without_budget(
                endpoint
                    .runtime
                    .apply_tower_update(Some(&updated), &mut pending)
            )
            .await
            .is_pending()
        );
        assert_eq!(relays.len(), 1);
        assert_eq!(relays.get(&url).unwrap().quic.as_ref().unwrap().port, 7342);
    }
    endpoint.set_towers(&updated).await.unwrap();
    assert_eq!(relays.len(), 1);
    assert_eq!(relays.get(&url).unwrap().quic.as_ref().unwrap().port, 7342);
    endpoint.nudge_towers().await.unwrap();
    assert_eq!(relays.get(&url).unwrap().quic.as_ref().unwrap().port, 7342);
    endpoint.iroh().close().await;
}

fn reserve_towers(count: usize) -> (Vec<TcpListener>, TowerSet) {
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap())
        .collect();
    let towers = TowerSet::from_entries(listeners.iter().map(|listener| {
        TowerEntry::from_url(&format!("http://{}", listener.local_addr().unwrap())).unwrap()
    }))
    .unwrap();
    (listeners, towers)
}

async fn relay_only(towers: TowerSet) -> Endpoint {
    Endpoint::bind(
        &generate_node_key(),
        EndpointConfig {
            towers,
            ip_transport: IpTransport::RelayOnly,
            ..EndpointConfig::default()
        },
        Arc::new(Authorizer::new(&[ALPN], &[])),
    )
    .await
    .unwrap_or_else(|_| panic!("bind loopback test endpoint"))
}

/// Poll once with no budget for iroh's channel send, then drop the future. Its map mutation
/// happened synchronously before the send.
async fn poll_without_budget<F: Future>(future: F) -> Poll<F::Output> {
    for _ in 0..256 {
        if !has_budget_remaining() {
            break;
        }
        consume_budget().await;
    }
    assert!(!has_budget_remaining(), "test needs a budgeted Tokio task");
    let mut future = pin!(future);
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

#[tokio::test]
async fn an_unchanged_set_keeps_its_config_and_sends_no_notification() {
    let (_listeners, towers) = reserve_towers(1);
    let endpoint = relay_only(towers.clone()).await;
    let runtime = &endpoint.runtime;
    let url: RelayUrl = towers.urls().next().unwrap().parse().unwrap();
    let before = runtime.relays.get(&url).unwrap();
    {
        let mut pending = runtime.tower_updates.lock().await;
        assert_eq!(
            poll_without_budget(runtime.apply_tower_update(Some(&towers), &mut pending)).await,
            Poll::Ready(Ok(()))
        );
        assert!(pending.is_none());
    }
    let after = runtime.relays.get(&url).unwrap();
    assert!(Arc::ptr_eq(&before, &after));
    endpoint.iroh().close().await;
}

#[tokio::test]
async fn cancelling_a_nudge_does_not_make_an_unchanged_set_notify() {
    let (_listeners, towers) = reserve_towers(1);
    let endpoint = relay_only(towers.clone()).await;
    let runtime = &endpoint.runtime;
    {
        let mut pending = runtime.tower_updates.lock().await;
        assert!(
            poll_without_budget(runtime.apply_tower_update(None, &mut pending))
                .await
                .is_pending()
        );
        assert!(runtime.relays == towers.relay_map());
        assert_eq!(
            poll_without_budget(runtime.apply_tower_update(Some(&towers), &mut pending)).await,
            Poll::Ready(Ok(())),
        );
        assert!(pending.is_none());
    }
    endpoint.set_towers(&TowerSet::default()).await.unwrap();
    {
        let mut pending = runtime.tower_updates.lock().await;
        assert_eq!(
            poll_without_budget(runtime.apply_tower_update(None, &mut pending)).await,
            Poll::Ready(Ok(())),
            "an empty map has nothing to nudge",
        );
        assert!(pending.is_none());
    }
    endpoint.iroh().close().await;
}

#[derive(Debug)]
struct RelayAdmits(AtomicBool);

impl AccessControl for RelayAdmits {
    async fn on_connect(&self, _request: &ClientRequest) -> Access {
        if self.0.load(Ordering::Relaxed) {
            Access::Allow
        } else {
            Access::Deny {
                reason: Some("fixture denial".to_owned()),
            }
        }
    }
}

#[tokio::test]
async fn the_home_observer_clears_denial_without_a_status_request() {
    let access = Arc::new(RelayAdmits(AtomicBool::new(false)));
    let mut relay = RelayConfig::new((Ipv4Addr::LOCALHOST, 0));
    relay.access = access.clone();
    let mut config = ServerConfig::default();
    config.relay = Some(relay);
    let server = Server::spawn(config)
        .await
        .expect("start the loopback relay");
    let towers = TowerSet::from_url(&format!("http://{}", server.http_addr().unwrap())).unwrap();
    let endpoint = relay_only(towers).await;
    timeout(Duration::from_secs(45), async {
        let mut home = endpoint.iroh().home_relay_status();
        loop {
            if endpoint.tower_states().unwrap()[0].status == TowerStatus::denied("fixture denial") {
                break;
            }
            home.updated().await.expect("home watcher stays connected");
        }
    })
    .await
    .expect("observe the initial relay denial");

    // Do not ask for a numbers status until the observer has cleared the remembered denial.
    access.0.store(true, Ordering::Relaxed);
    timeout(Duration::from_secs(10), endpoint.iroh().online())
        .await
        .expect("the relay admits the endpoint");
    let cleared = timeout(Duration::from_secs(2), async {
        while endpoint.runtime.tower_snapshot().home_failure.is_some() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    endpoint.iroh().close().await;
    server.shutdown().await.expect("stop the loopback relay");
    assert!(
        cleared.is_ok(),
        "a stable connection must clear the remembered denial without a status call"
    );
}

#[tokio::test]
async fn the_home_observer_stops_on_close_and_on_final_runtime_drop() {
    for explicit_close in [true, false] {
        let endpoint = relay_only(TowerSet::default()).await;
        // Let the observer reach its watcher before testing runtime drop.
        tokio::task::yield_now().await;
        let raw = endpoint.iroh().clone();
        let observer = endpoint.runtime._tower_observer.abort_handle();
        let control = endpoint.tower_control();
        let still_owned = if explicit_close {
            raw.close().await;
            Some(endpoint)
        } else {
            drop(endpoint);
            assert!(!raw.is_closed(), "the separate raw clone remains open");
            None
        };
        timeout(Duration::from_secs(2), async {
            while !observer.is_finished() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the runtime-owned observer stops");
        assert!(control.tower_states().is_err());
        drop(still_owned);
        raw.close().await;
    }
}

#[tokio::test]
async fn interrupted_notifications_are_retried_for_matching_nonempty_and_empty_maps() {
    let (_listeners, full) = reserve_towers(1);
    let empty = TowerSet::default();
    let endpoint = relay_only(empty.clone()).await;
    let runtime = &endpoint.runtime;
    let url: RelayUrl = full.urls().next().unwrap().parse().unwrap();

    for wanted in [&full, &empty] {
        {
            let mut pending = runtime.tower_updates.lock().await;
            assert!(
                poll_without_budget(runtime.apply_tower_update(Some(wanted), &mut pending))
                    .await
                    .is_pending()
            );
            assert!(
                runtime.relays == wanted.relay_map(),
                "the map edit took effect"
            );
            assert!(pending.as_ref() == Some(&url), "only the notice is pending");

            // A retry must reach iroh's channel send despite the lack of a map diff. Drop this
            // retry too and check that a later retry still knows to send the notice.
            assert!(
                poll_without_budget(runtime.apply_tower_update(Some(wanted), &mut pending))
                    .await
                    .is_pending()
            );
            assert!(pending.as_ref() == Some(&url));
            if wanted.is_empty() {
                assert!(
                    poll_without_budget(runtime.apply_tower_update(None, &mut pending))
                        .await
                        .is_pending(),
                    "an empty map still replays an interrupted removal notification",
                );
                assert!(pending.as_ref() == Some(&url));
            }
        }
        if wanted.is_empty() {
            endpoint.nudge_towers().await.unwrap();
        } else {
            endpoint.set_towers(wanted).await.unwrap();
        }
        assert!(runtime.tower_updates.lock().await.is_none());
        assert!(runtime.relays == wanted.relay_map());
    }
    endpoint.iroh().close().await;
}

#[tokio::test]
async fn a_nudge_repairs_the_latest_interrupted_full_replacement() {
    let (_first, first) = reserve_towers(MAX_TOWERS);
    let (_second, second) = reserve_towers(MAX_TOWERS);
    let (_third, third) = reserve_towers(MAX_TOWERS);
    let endpoint = relay_only(first.clone()).await;
    let runtime = &endpoint.runtime;

    for wanted in [&second, &third, &first]
        .into_iter()
        .cycle()
        .take(MAX_TOWERS * 3)
    {
        let mut pending = runtime.tower_updates.lock().await;
        assert!(
            poll_without_budget(runtime.apply_tower_update(Some(wanted), &mut pending))
                .await
                .is_pending()
        );
        assert!(!runtime.relays.is_empty(), "lookup lost its only relay");
        assert!(
            runtime.relays.len() <= MAX_TOWERS,
            "lookup exceeded its relay cap"
        );
        assert!(pending.is_some());
        let states = endpoint.tower_states().unwrap();
        assert!(states.iter().map(|state| state.url()).eq(wanted.urls()));
    }
    endpoint.set_towers(&second).await.unwrap();
    {
        let mut pending = runtime.tower_updates.lock().await;
        assert!(
            poll_without_budget(runtime.apply_tower_update(Some(&first), &mut pending))
                .await
                .is_pending()
        );
        assert!(
            runtime.relays != first.relay_map(),
            "the nudge has map edits to repair"
        );
    }
    endpoint.nudge_towers().await.unwrap();
    assert!(runtime.relays == first.relay_map());
    assert!(runtime.tower_updates.lock().await.is_none());

    // A dropped nudge of a sole relay must leave it available to bare-ID lookup.
    let single = TowerSet::from_entries(first.entries().iter().take(1).cloned()).unwrap();
    endpoint.set_towers(&single).await.unwrap();
    {
        let mut pending = runtime.tower_updates.lock().await;
        assert!(
            poll_without_budget(runtime.apply_tower_update(None, &mut pending))
                .await
                .is_pending()
        );
        assert!(runtime.relays == single.relay_map());
    }
    endpoint.nudge_towers().await.unwrap();
    assert!(runtime.relays == single.relay_map());
    endpoint.iroh().close().await;
}

#[tokio::test]
async fn a_tower_update_keeps_the_lock_while_its_notification_is_pending() {
    let (_first, first) = reserve_towers(MAX_TOWERS);
    let (_second, second) = reserve_towers(2);
    let endpoint = relay_only(TowerSet::default()).await;
    let runtime = &endpoint.runtime;
    {
        let mut pending = runtime.tower_updates.lock().await;
        assert!(
            poll_without_budget(runtime.apply_tower_update(Some(&first), &mut pending))
                .await
                .is_pending()
        );
    }

    let mut nudge = pin!(endpoint.nudge_towers());
    tokio::task::yield_now().await;
    // Leave enough cooperative budget to acquire the lock and begin reconciling, but
    // force the nudge to pause before delivering all its notifications.
    for _ in 0..126 {
        consume_budget().await;
    }
    assert!(
        poll_fn(|cx| Poll::Ready(nudge.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert!(
        runtime.relays != first.relay_map(),
        "the nudge still has edits to make"
    );
    tokio::task::yield_now().await;
    let mut set = pin!(endpoint.set_towers(&second));
    assert!(
        poll_fn(|cx| Poll::Ready(set.as_mut().poll(cx)))
            .await
            .is_pending(),
        "the set waits for the nudge's suspended map edit",
    );
    nudge.as_mut().await.unwrap();
    set.as_mut().await.unwrap();
    assert!(runtime.relays == second.relay_map());
    endpoint.iroh().close().await;
}

#[tokio::test]
async fn a_set_keeps_the_lock_while_its_map_edits_are_suspended() {
    let (_first, first) = reserve_towers(MAX_TOWERS);
    let (_second, second) = reserve_towers(2);
    let endpoint = relay_only(TowerSet::default()).await;
    let runtime = &endpoint.runtime;

    let mut set_first = pin!(endpoint.set_towers(&first));
    tokio::task::yield_now().await;
    // Leave budget to acquire the lock and start applying the set. A notification send
    // exhausts the budget before the remaining relay-map edits can run.
    for _ in 0..126 {
        consume_budget().await;
    }
    assert!(
        poll_fn(|cx| Poll::Ready(set_first.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert!(
        !runtime.relays.is_empty(),
        "the first set has edited the map"
    );
    assert!(
        runtime.relays != first.relay_map(),
        "the first set still has edits to make"
    );

    tokio::task::yield_now().await;
    let mut set_second = pin!(endpoint.set_towers(&second));
    assert!(
        poll_fn(|cx| Poll::Ready(set_second.as_mut().poll(cx)))
            .await
            .is_pending(),
        "the second set waits for the first set's suspended map edit",
    );
    set_first.as_mut().await.unwrap();
    set_second.as_mut().await.unwrap();
    assert!(runtime.relays == second.relay_map());
    endpoint.iroh().close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_and_nudge_serialize_and_converge_on_the_requested_map() {
    let (_first, first) = reserve_towers(2);
    let (_second, second) = reserve_towers(2);
    let endpoint = relay_only(first.clone()).await;
    let runtime = &endpoint.runtime;
    {
        let _pending = runtime.tower_updates.lock().await;
        let mut set = pin!(endpoint.set_towers(&second));
        let mut nudge = pin!(endpoint.nudge_towers());
        poll_fn(|cx| {
            assert!(set.as_mut().poll(cx).is_pending());
            assert!(nudge.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(runtime.relays == first.relay_map());
        assert!(runtime.tower_snapshot().wanted.urls().eq(first.urls()));
    }

    let empty = TowerSet::default();
    for towers in [&second, &empty, &first].into_iter().cycle().take(24) {
        let start = Arc::new(Barrier::new(2));
        let setter = endpoint.tower_control();
        let nudger = setter.clone();
        let set_start = Arc::clone(&start);
        let wanted = towers.clone();
        let set = tokio::spawn(async move {
            set_start.wait().await;
            setter.set_towers(&wanted).await
        });
        let nudge = tokio::spawn(async move {
            start.wait().await;
            nudger.nudge_towers().await
        });
        let (set, nudge) = timeout(Duration::from_secs(5), async { tokio::join!(set, nudge) })
            .await
            .expect("concurrent tower updates made no progress");
        set.unwrap().unwrap();
        nudge.unwrap().unwrap();
        assert!(
            runtime.relays == towers.relay_map(),
            "a nudge restored an obsolete relay"
        );
    }
    endpoint.iroh().close().await;
}

#[test]
fn a_home_keeps_its_last_failure_only_until_a_new_observation_supersedes_it() {
    let (_listeners, wanted) = reserve_towers(2);
    let first = wanted.urls().next();
    let second = wanted.urls().nth(1);
    let mut snapshot = TowerSnapshot {
        wanted: wanted.clone(),
        home_failure: None,
    };
    let denied = TowerStatus::denied("fixture denial");
    assert_eq!(snapshot.observe(first, denied.clone()), denied);
    assert_eq!(snapshot.observe(first, TowerStatus::Unknown), denied);
    assert_eq!(
        snapshot.observe(first, TowerStatus::Failing),
        TowerStatus::Failing
    );
    assert_eq!(
        snapshot.observe(first, TowerStatus::Unknown),
        TowerStatus::Failing
    );
    assert_eq!(
        snapshot.observe(first, TowerStatus::Connected),
        TowerStatus::Connected
    );
    assert_eq!(
        snapshot.observe(first, TowerStatus::Unknown),
        TowerStatus::Unknown
    );

    snapshot.observe(first, denied.clone());
    assert_eq!(
        snapshot.observe(second, TowerStatus::Unknown),
        TowerStatus::Unknown
    );
    assert_eq!(
        snapshot.observe(first, TowerStatus::Unknown),
        TowerStatus::Unknown
    );
    snapshot.observe(first, denied.clone());
    snapshot.observe(None, TowerStatus::Unknown);
    assert_eq!(
        snapshot.observe(first, TowerStatus::Unknown),
        TowerStatus::Unknown
    );
    snapshot.observe(first, denied.clone());
    snapshot.configure(&wanted);
    assert_eq!(snapshot.observe(first, TowerStatus::Unknown), denied);
    snapshot.configure(&TowerSet::default());
    snapshot.configure(&wanted);
    assert_eq!(
        snapshot.observe(first, TowerStatus::Unknown),
        TowerStatus::Unknown
    );
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
