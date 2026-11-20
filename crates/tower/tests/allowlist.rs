#![cfg(test)]
//! A tower with an allowlist and a rate limit, exercised by iroh endpoints acting as stations.
//!
//! Every endpoint is relay-only and every id is generated at run time. Assertions compare ids
//! with `assert!(a == b)` so that a failure prints no id.

use std::future::Future;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use iroh::endpoint::{Connection, PortmapperConfig, RelayStatus, presets};
use iroh::{
    Endpoint, EndpointAddr, NetReportConfig, RelayConfig, RelayMap, RelayMode, RelayUrl, SecretKey,
    Watcher,
};
use numbers_tower::{Tower, TowerConfig, TowerError};
use tokio::time::timeout;

const ALPN: &[u8] = b"test/tower/0";
const DENY_REASON: &str = "not on this tower's list";
/// Bounds every test, so that a regression fails instead of hanging.
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Under load a home relay may be selected only on the next net report (20-26 s later).
const ONLINE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long an unlisted endpoint must stay offline, and how soon it must see the denial.
const DENIED_WITHIN: Duration = Duration::from_secs(10);

fn loopback() -> TowerConfig {
    TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into())
}

async fn spawn(config: TowerConfig) -> (Tower, RelayUrl) {
    let tower = Tower::spawn(config)
        .await
        .unwrap_or_else(|_| panic!("start loopback tower"));
    let url = tower
        .url()
        .parse()
        .unwrap_or_else(|_| panic!("parse tower URL"));
    (tower, url)
}

/// A relay-only endpoint homed on `tower`, with the same settings a station uses.
async fn station(key: SecretKey, tower: &RelayUrl) -> Endpoint {
    let relays = RelayMap::from_iter([RelayConfig::new(tower.clone(), None)]);
    let mut net_report = NetReportConfig::default();
    net_report.captive_portal_check = false;
    Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .relay_mode(RelayMode::Custom(relays))
        .clear_ip_transports()
        .portmapper_config(PortmapperConfig::Disabled)
        .net_report_config(net_report)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .unwrap_or_else(|_| panic!("bind endpoint"))
}

/// The home relay status as text, for failure messages. It carries no id.
fn status(endpoint: &Endpoint) -> String {
    format!("{:?}", endpoint.home_relay_status().get())
}

/// Whether the endpoint reports a connection to its home relay.
fn connected(endpoint: &Endpoint) -> bool {
    endpoint
        .home_relay_status()
        .get()
        .iter()
        .any(RelayStatus::is_connected)
}

async fn online(endpoint: &Endpoint) {
    if timeout(ONLINE_TIMEOUT, endpoint.online()).await.is_err() {
        panic!(
            "endpoint not online after 30 s; home relay status: {}",
            status(endpoint)
        );
    }
}

/// Waits until the home relay reports a denial, and returns its reason.
async fn denial(endpoint: &Endpoint) -> String {
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
    match timeout(DENIED_WITHIN, wait).await {
        Ok(reason) => reason,
        Err(_) => panic!(
            "no denial within 10 s; home relay status: {}",
            status(endpoint)
        ),
    }
}

/// Fails unless `future` is still pending after `within`.
async fn stays_pending<F: Future>(within: Duration, future: F, what: &str) {
    assert!(timeout(within, future).await.is_err(), "{what}");
}

fn dial_addr(endpoint: &Endpoint, tower: &RelayUrl) -> EndpointAddr {
    EndpointAddr::new(endpoint.id()).with_relay_url(tower.clone())
}

async fn accept(endpoint: &Endpoint) -> Connection {
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

async fn bounded(test: impl Future<Output = ()>) {
    if timeout(TEST_TIMEOUT, test).await.is_err() {
        panic!("test timed out after {TEST_TIMEOUT:?}");
    }
}

#[tokio::test]
async fn listed_endpoint_comes_online_and_unlisted_one_is_denied() {
    bounded(async {
        let (listed_key, unlisted_key) = (SecretKey::generate(), SecretKey::generate());
        let (tower, url) = spawn(loopback().allow([listed_key.public().to_string()])).await;
        let listed = station(listed_key, &url).await;
        let unlisted = station(unlisted_key, &url).await;

        let (reason, ()) = tokio::join!(denial(&unlisted), online(&listed));
        assert_eq!(reason, DENY_REASON);
        stays_pending(
            DENIED_WITHIN,
            unlisted.online(),
            "an unlisted endpoint came online",
        )
        .await;
        assert!(!connected(&unlisted));
        assert!(connected(&listed));
        let stats = tower.stats();
        assert!(stats.denied >= 1, "{stats:?}");
        assert_eq!(stats.qad_connections, 0);

        listed.close().await;
        unlisted.close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn an_empty_list_admits_nobody() {
    bounded(async {
        let (tower, url) = spawn(loopback().allow(Vec::<String>::new())).await;
        let endpoint = station(SecretKey::generate(), &url).await;

        assert_eq!(denial(&endpoint).await, DENY_REASON);
        assert!(tower.stats().denied >= 1);

        endpoint.close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn no_list_admits_everybody() {
    bounded(async {
        let (tower, url) = spawn(loopback()).await;
        let first = station(SecretKey::generate(), &url).await;
        let second = station(SecretKey::generate(), &url).await;

        tokio::join!(online(&first), online(&second));
        assert_eq!(tower.stats().denied, 0);

        first.close().await;
        second.close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn an_unparsable_entry_fails_spawn_without_echoing_it() {
    bounded(async {
        let valid = SecretKey::generate().public().to_string();
        let bad = "this-is-not-an-endpoint-id";
        // Not an id at all; behind a valid id; a valid id cut short (decodes, wrong length).
        for (list, position) in [
            (vec![bad.to_owned()], 1),
            (vec![valid.clone(), bad.to_owned()], 2),
            (vec![valid.clone(), valid[..valid.len() - 2].to_owned()], 2),
        ] {
            let entries = list.clone();
            let Err(error) = Tower::spawn(loopback().allow(list)).await else {
                panic!("spawn accepted an unparsable allowlist entry");
            };
            let reported = match &error {
                TowerError::InvalidAllowlistEntry { position, .. } => Some(*position),
                _ => None,
            };
            assert_eq!(reported, Some(position));
            let mut texts = vec![error.to_string(), format!("{error:?}")];
            let mut source = std::error::Error::source(&error);
            while let Some(cause) = source {
                texts.push(cause.to_string());
                texts.push(format!("{cause:?}"));
                source = cause.source();
            }
            assert!(texts[0].contains(&format!("entry {position} ")));
            for entry in &entries {
                assert!(texts.iter().all(|text| !text.contains(entry.as_str())));
            }
        }
    })
    .await;
}

#[tokio::test]
async fn a_low_rate_slows_a_relayed_transfer_and_is_counted() {
    const RATE: u32 = 32 * 1024;
    const SIZE: usize = 192 * 1024;
    bounded(async {
        let (sender_key, receiver_key) = (SecretKey::generate(), SecretKey::generate());
        let ids = [
            sender_key.public().to_string(),
            receiver_key.public().to_string(),
        ];
        let (tower, url) = spawn(loopback().allow(ids).client_rate(RATE)).await;
        let sender = station(sender_key, &url).await;
        let receiver = station(receiver_key, &url).await;
        tokio::join!(online(&sender), online(&receiver));

        let started = Instant::now();
        let receive = async {
            let connection = accept(&receiver).await;
            let mut stream = connection
                .accept_uni()
                .await
                .unwrap_or_else(|_| panic!("accept stream"));
            let data = stream
                .read_to_end(SIZE)
                .await
                .unwrap_or_else(|_| panic!("read stream"));
            (connection, data.len())
        };
        let send = async {
            let connection = sender
                .connect(dial_addr(&receiver, &url), ALPN)
                .await
                .unwrap_or_else(|_| panic!("dial through the tower"));
            assert!(connection.paths().iter().all(|path| !path.is_ip()));
            let mut stream = connection
                .open_uni()
                .await
                .unwrap_or_else(|_| panic!("open stream"));
            stream.write_all(&[7; SIZE]).await.unwrap();
            stream.finish().unwrap();
            connection
        };
        let ((_received_on, received), _sent_on) = tokio::join!(receive, send);
        let elapsed = started.elapsed();

        assert_eq!(received, SIZE);
        // The tower reads the sender's upload through a token bucket that refills at the rate. It
        // takes each read whole and makes the client wait for that read's debt before the next
        // one, so the last read is never waited for, and the exact time depends on how the bytes
        // fall into reads. At the rate the payload takes six seconds; a quarter of that holds
        // however the reads fall, and is far above an unlimited transfer (about a tenth of a
        // second on loopback).
        let floor = Duration::from_secs_f64(SIZE as f64 / f64::from(RATE) / 4.0);
        assert!(elapsed >= floor, "{elapsed:?} < {floor:?}");
        assert!(tower.stats().rate_limited >= 1, "{:?}", tower.stats());
        // Slowed down, not dropped: both stay connected to the tower.
        assert!(connected(&sender));
        assert!(connected(&receiver));

        sender.close().await;
        receiver.close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn a_listed_endpoint_cannot_reach_an_unlisted_one() {
    /// How long both dials must stay unanswered. Undelivered dials fail only by the 30 s QUIC
    /// idle timeout, which this test does not wait for.
    const UNANSWERED: Duration = Duration::from_secs(3);
    bounded(async {
        let listed_key = SecretKey::generate();
        let (tower, url) = spawn(loopback().allow([listed_key.public().to_string()])).await;
        let listed = station(listed_key, &url).await;
        let unlisted = station(SecretKey::generate(), &url).await;
        let (reason, ()) = tokio::join!(denial(&unlisted), online(&listed));
        assert_eq!(reason, DENY_REASON);

        // The tower drops what the listed endpoint sends to an id it has no connection for, and
        // the unlisted endpoint cannot reach the tower at all, so neither dial gets an answer
        // and neither side accepts anything.
        let listed_dial = listed.connect(dial_addr(&unlisted, &url), ALPN);
        let unlisted_dial = unlisted.connect(dial_addr(&listed, &url), ALPN);
        tokio::join!(
            stays_pending(UNANSWERED, listed_dial, "the listed dial got an answer"),
            stays_pending(UNANSWERED, unlisted_dial, "the unlisted dial got an answer"),
            stays_pending(UNANSWERED, listed.accept(), "the listed side accepted"),
            stays_pending(UNANSWERED, unlisted.accept(), "the unlisted side accepted"),
        );
        assert!(connected(&listed));
        assert!(tower.stats().denied >= 1);

        listed.close().await;
        unlisted.close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}
