#![allow(dead_code)]
use std::fmt::Display;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use iroh::endpoint::RelayStatus;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointId, SecretKey, Watcher};
use iroh_lab::Res;
use iroh_lab::endpoint::{self, EpConfig};
use iroh_lab::gate::{self, FilterLog, LINK_ALPN, OTHER_ALPN};
use iroh_lab::link::Link;

pub mod dns;

pub fn lo0() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

pub fn secret() -> [u8; 32] {
    SecretKey::generate().to_bytes()
}

pub fn id_of(secret: &[u8; 32]) -> EndpointId {
    SecretKey::from_bytes(secret).public()
}

/// One measurement line; the report is built from these.
pub fn m(test: &str, what: &str, v: impl Display) {
    println!("MEASURE {test} | {what} | {v}");
}

pub fn ms(d: Duration) -> String {
    format!("{:.0} ms", d.as_secs_f64() * 1000.0)
}

pub fn stats(v: &[Duration]) -> String {
    let mut v = v.to_vec();
    v.sort();
    if v.is_empty() {
        return "n/a".into();
    }
    format!(
        "min {} / median {} / max {} (n={})",
        ms(v[0]),
        ms(v[v.len() / 2]),
        ms(v[v.len() - 1]),
        v.len()
    )
}

#[derive(Debug)]
pub struct Node {
    pub ep: Endpoint,
    pub router: Router,
    pub link: Link,
    pub filter: Arc<FilterLog>,
    pub cfg: EpConfig,
}

impl Node {
    pub fn id(&self) -> EndpointId {
        self.ep.id()
    }
}

pub async fn node(cfg: EpConfig, enforce_filter: bool) -> Res<Node> {
    node_with(cfg, enforce_filter, Link::default()).await
}

pub async fn node_with(cfg: EpConfig, enforce_filter: bool, link: Link) -> Res<Node> {
    let ep = endpoint::bind(&cfg).await?;
    let filter = Arc::new(FilterLog::default());
    let router = Router::builder(ep.clone())
        .incoming_filter(gate::incoming_filter(
            cfg.auth.clone(),
            filter.clone(),
            enforce_filter,
        ))
        .accept(LINK_ALPN, link.clone())
        .accept(OTHER_ALPN, link.clone())
        .spawn();
    Ok(Node {
        ep,
        router,
        link,
        filter,
        cfg,
    })
}

/// Time until online(), or None after `limit`.
pub async fn online_within(ep: &Endpoint, start: Instant, limit: Duration) -> Option<Duration> {
    match tokio::time::timeout(limit, ep.online()).await {
        Ok(()) => Some(start.elapsed()),
        Err(_) => None,
    }
}

pub fn fmt_status(v: &[RelayStatus]) -> String {
    if v.is_empty() {
        return "[] (empty)".into();
    }
    v.iter()
        .map(|s| {
            let err = s
                .last_error()
                .map(|e| {
                    let e = format!("{e}");
                    e.chars().take(90).collect::<String>()
                })
                .unwrap_or_else(|| "-".into());
            format!(
                "{{url={} connected={} last_error={} auth_denied={:?}}}",
                s.url(),
                s.is_connected(),
                err,
                s.auth_denied_reason()
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn status(ep: &Endpoint) -> String {
    fmt_status(&ep.home_relay_status().get())
}

pub fn is_connected(ep: &Endpoint) -> bool {
    ep.home_relay_status()
        .get()
        .iter()
        .any(|s| s.is_connected())
}

pub type StatusLog = Arc<Mutex<Vec<(Duration, String)>>>;

/// Logs every change of home_relay_status(), timestamped from `start`.
pub fn watch_status(ep: &Endpoint, start: Instant) -> (tokio::task::JoinHandle<()>, StatusLog) {
    let log: StatusLog = Arc::default();
    let mut w = ep.home_relay_status();
    let l = log.clone();
    if let Ok(mut g) = l.lock() {
        g.push((
            start.elapsed(),
            format!("initial: {}", fmt_status(&w.get())),
        ));
    }
    let h = tokio::spawn(async move {
        while let Ok(v) = w.updated().await {
            if let Ok(mut g) = l.lock() {
                g.push((start.elapsed(), fmt_status(&v)));
            }
        }
    });
    (h, log)
}

pub fn print_log(test: &str, log: &StatusLog) {
    if let Ok(g) = log.lock() {
        for (t, s) in g.iter() {
            m(test, &format!("status@{}", ms(*t)), s);
        }
    }
}

/// A free UDP port on 127.0.0.1 (bound and released).
pub fn free_udp_port() -> Res<u16> {
    let s = std::net::UdpSocket::bind(lo0())?;
    Ok(s.local_addr()?.port())
}

/// A free TCP port on 127.0.0.1 (bound and released): a "tower that is down".
pub fn free_tcp_port() -> Res<u16> {
    let s = std::net::TcpListener::bind(lo0())?;
    Ok(s.local_addr()?.port())
}

pub fn short_err(e: &dyn Display) -> String {
    format!("{e}").chars().take(200).collect()
}
