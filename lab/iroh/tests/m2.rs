//! M2 experiments T20-T27: TLS tower, QAD, several towers, runtime relay-map changes, a denying
//! tower, the rate limit, SRV bootstrap against a fake DNS server, and first-home-tower timing.
//! Everything runs on 127.0.0.1; every wait is bounded. Endpoint ids and keys are never printed:
//! endpoints are "A", "B", "X", "Y", "stranger"; towers are labelled the same way in status lines.
//! Run with `--nocapture` to see the MEASURE lines.
mod common;

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::dns::{FakeDns, Owner};
use common::*;
use iroh::endpoint::Connection;
use iroh::tls::CaTlsConfig;
use iroh::{Endpoint, RelayMap, RelayUrl, Watcher};
use iroh_lab::Res;
use iroh_lab::allowlist::Members;
use iroh_lab::endpoint::{self, EpConfig, Relays};
use iroh_lab::gate::{Authorizer, LINK_ALPN};
use iroh_lab::link::request;
use iroh_lab::locate::{self, UrlForm};
use iroh_lab::paths;
use iroh_lab::tls_tower::{CertFiles, M2Tower, TowerSpec, client_config};
use iroh_relay::server::{ClientRateLimit, DynAccessControl};
use rustls::pki_types::CertificateDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

const S: fn(u64) -> Duration = Duration::from_secs;
const MS: fn(u64) -> Duration = Duration::from_millis;

// ---------------------------------------------------------------------------------------------
// Helpers

/// A self-signed certificate written to PEM files, as a production tower would read them.
struct Cert {
    files: CertFiles,
    der: CertificateDer<'static>,
    _dir: tempfile::TempDir,
}

fn cert(sans: &[&str]) -> Res<Cert> {
    let dir = tempfile::tempdir()?;
    let sans: Vec<String> = sans.iter().map(|s| s.to_string()).collect();
    let ck = rcgen::generate_simple_self_signed(sans)?;
    let files = CertFiles {
        cert: dir.path().join("cert.pem"),
        key: dir.path().join("key.pem"),
    };
    std::fs::write(&files.cert, ck.cert.pem())?;
    std::fs::write(&files.key, ck.signing_key.serialize_pem())?;
    Ok(Cert {
        files,
        der: ck.cert.der().clone(),
        _dir: dir,
    })
}

fn trust(certs: &[&Cert]) -> CaTlsConfig {
    CaTlsConfig::custom_roots(certs.iter().map(|c| c.der.clone()))
}

fn lo(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

async fn tls_tower(
    c: &Cert,
    quic: bool,
    access: Option<Arc<dyn DynAccessControl>>,
) -> Res<M2Tower> {
    let mut spec = TowerSpec::tls(lo0(), lo0(), c.files.clone());
    if quic {
        spec.quic = Some(lo0());
    }
    spec.access = access;
    M2Tower::spawn(spec).await
}

async fn plain_tower(addr: SocketAddr, access: Option<Arc<dyn DynAccessControl>>) -> Res<M2Tower> {
    let mut spec = TowerSpec::plain(addr);
    spec.access = access;
    M2Tower::spawn(spec).await
}

fn dead_url() -> Res<RelayUrl> {
    Ok(format!("http://127.0.0.1:{}/", free_tcp_port()?).parse()?)
}

fn cfg(relays: Relays) -> EpConfig {
    EpConfig::new(secret(), relays, Arc::new(Authorizer::default()))
}

fn configs(t: &[(&RelayUrl, Option<u16>)]) -> Relays {
    Relays::Configs(t.iter().map(|(u, q)| ((*u).clone(), *q)).collect())
}

fn shared(urls: &[&RelayUrl]) -> RelayMap {
    let map = RelayMap::empty();
    for u in urls {
        map.insert((*u).clone(), client_config(u, None));
    }
    map
}

/// Two configs that authorize each other.
fn pair(rx: Relays, ry: Relays) -> (EpConfig, EpConfig) {
    let (kx, ky) = (secret(), secret());
    let x = EpConfig::new(kx, rx, Arc::new(Authorizer::new([id_of(&ky)])));
    let y = EpConfig::new(ky, ry, Arc::new(Authorizer::new([id_of(&kx)])));
    (x, y)
}

/// Replaces tower URLs by labels in printed text.
#[derive(Debug, Clone, Default)]
struct Labels(Vec<(String, String)>);

impl Labels {
    fn of(pairs: &[(&RelayUrl, &str)]) -> Self {
        Self(
            pairs
                .iter()
                .map(|(u, l)| (u.to_string(), l.to_string()))
                .collect(),
        )
    }
    fn apply(&self, s: &str) -> String {
        let mut s = s.to_string();
        for (u, l) in &self.0 {
            s = s.replace(u, l);
        }
        s
    }
    fn name(&self, u: &RelayUrl) -> String {
        self.apply(&u.to_string())
    }
}

fn st(ep: &Endpoint, l: &Labels) -> String {
    l.apply(&status(ep))
}

/// Label of the connected home tower, if any.
fn home(ep: &Endpoint, l: &Labels) -> Option<String> {
    ep.home_relay_status()
        .get()
        .iter()
        .find(|s| s.is_connected())
        .map(|s| l.name(s.url()))
}

fn print_log_l(test: &str, what: &str, log: &StatusLog, l: &Labels) {
    if let Ok(g) = log.lock() {
        for (t, s) in g.iter() {
            m(test, &format!("{what} status@{}", ms(*t)), l.apply(s));
        }
    }
}

/// Polls `f` every 20 ms; the elapsed time when it first held, or None after `limit`.
async fn wait_for(limit: Duration, mut f: impl FnMut() -> bool) -> Option<Duration> {
    let t = Instant::now();
    while t.elapsed() < limit {
        if f() {
            return Some(t.elapsed());
        }
        tokio::time::sleep(MS(20)).await;
    }
    None
}

/// bind -> online(), timed from before bind.
async fn bind_online(cfg: &EpConfig, limit: Duration) -> Res<(Endpoint, Option<Duration>)> {
    let t = Instant::now();
    let ep = endpoint::bind(cfg).await?;
    let on = online_within(&ep, t, limit).await;
    Ok((ep, on))
}

/// One raw HTTP/1.1 GET; returns the status line.
async fn http_get(addr: SocketAddr, path: &str) -> String {
    let r = timeout(S(3), async {
        let mut s = tokio::net::TcpStream::connect(addr).await?;
        let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await?;
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await?;
        Res::Ok(
            String::from_utf8_lossy(&buf)
                .lines()
                .next()
                .unwrap_or("")
                .to_string(),
        )
    })
    .await;
    match r {
        Ok(Ok(l)) if l.is_empty() => "(closed without a response)".into(),
        Ok(Ok(l)) if l.starts_with("HTTP/") => l,
        Ok(Ok(l)) => format!("not HTTP: {:?}", l.chars().take(12).collect::<String>()),
        Ok(Err(e)) => format!("error: {}", short_err(&*e)),
        Err(_) => "no answer within 3 s".into(),
    }
}

/// Request/echo with a timeout: Ok(elapsed) or the error text.
async fn ping(conn: &Connection, limit: Duration) -> Result<Duration, String> {
    let t = Instant::now();
    match timeout(limit, request(conn, b"ping")).await {
        Ok(Ok(_)) => Ok(t.elapsed()),
        Ok(Err(e)) => Err(short_err(&*e)),
        Err(_) => Err(format!("no echo within {}", ms(limit))),
    }
}

/// Retries `ping` every second until it works, the connection closes, or `limit` passes.
async fn ping_until_ok(conn: &Connection, limit: Duration) -> (Option<Duration>, String) {
    let t = Instant::now();
    let mut last = String::new();
    while t.elapsed() < limit {
        if let Some(reason) = conn.close_reason() {
            return (None, format!("connection closed: {}", short_err(&reason)));
        }
        match ping(conn, S(2)).await {
            Ok(_) => return (Some(t.elapsed()), "echo ok".into()),
            Err(e) => last = e,
        }
        tokio::time::sleep(MS(500)).await;
    }
    (None, format!("still failing after {}: {last}", ms(limit)))
}

/// An error with its whole source chain.
fn chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut s = e.to_string();
    let mut cur = e.source();
    while let Some(c) = cur {
        s.push_str(" <- ");
        s.push_str(&c.to_string());
        cur = c.source();
    }
    s.chars().take(400).collect()
}

fn provider_check(test: &str) {
    m(
        test,
        "rustls process-default CryptoProvider installed (by anything in this test binary so far)",
        rustls::crypto::CryptoProvider::get_default().is_some(),
    );
}

// ---------------------------------------------------------------------------------------------
// T20: a TLS tower with a certificate from files.
#[tokio::test(flavor = "multi_thread")]
async fn t20_tls_tower_from_files() -> Res {
    timeout(S(90), async {
        provider_check("T20");
        let c = cert(&["127.0.0.1"])?;
        let t = Instant::now();
        let tower = tls_tower(&c, false, None).await?;
        m("T20", "Server::spawn with TLS from files", ms(t.elapsed()));
        let (http, https) = (
            tower.http_addr().ok_or("no http addr")?,
            tower.https_addr().ok_or("no https addr")?,
        );
        m(
            "T20",
            "listeners: http_addr / https_addr / quic_addr",
            format!(
                "{} / {} / {:?} (http asked 127.0.0.1:0, https asked 127.0.0.1:0)",
                http.ip(),
                https.ip(),
                tower.quic_addr()
            ),
        );
        m(
            "T20",
            "http and https ports differ",
            http.port() != https.port(),
        );
        for path in ["/generate_204", "/ping", "/relay", "/healthz", "/"] {
            m(
                "T20",
                &format!("plain GET {path} on http_addr"),
                http_get(http, path).await,
            );
        }
        m(
            "T20",
            "plain-HTTP GET /ping on https_addr",
            http_get(https, "/ping").await,
        );
        let url = tower.url_for("127.0.0.1")?;
        let l = Labels::of(&[(&url, "T")]);

        // trusted: custom_roots([our cert])
        let mut times = vec![];
        for i in 0..5 {
            let mut c1 = cfg(configs(&[(&url, None)]));
            c1.ca_tls = Some(trust(&[&c]));
            let (ep, on) = bind_online(&c1, S(20)).await?;
            if i == 0 {
                m("T20", "trusted endpoint status after online", st(&ep, &l));
            }
            times.push(on.ok_or("trusted endpoint not online in 20 s")?);
            ep.close().await;
        }
        m(
            "T20",
            "trusted (custom_roots): bind -> online",
            stats(&times),
        );
        m(
            "T20",
            "relay accepts after 5 trusted endpoints (metrics.accepts)",
            tower.metrics().accepts.get(),
        );

        // untrusted: the default CaTlsConfig (embedded WebPKI roots) and custom_roots([])
        for (name, ca) in [
            ("default roots", None),
            ("custom_roots([])", Some(CaTlsConfig::custom_roots([]))),
        ] {
            let before = (
                tower.metrics().accepts.get(),
                tower.metrics().http_connections.get(),
            );
            let mut c2 = cfg(configs(&[(&url, None)]));
            c2.ca_tls = ca;
            let t0 = Instant::now();
            let ep = match endpoint::bind(&c2).await {
                Ok(ep) => ep,
                Err(e) => {
                    m("T20", &format!("untrusted ({name}): bind()"), chain(&*e));
                    continue;
                }
            };
            let (h, log) = watch_status(&ep, t0);
            let on = online_within(&ep, t0, S(12)).await;
            m(
                "T20",
                &format!("untrusted ({name}): online within 12 s"),
                format!("{:?}", on.map(ms)),
            );
            m(
                "T20",
                &format!("untrusted ({name}): status at 12 s"),
                st(&ep, &l),
            );
            h.abort();
            print_log_l("T20", &format!("untrusted ({name})"), &log, &l);
            m(
                "T20",
                &format!("untrusted ({name}): tower relay accepts / http(s) connections in 12 s"),
                format!(
                    "{} / {}",
                    tower.metrics().accepts.get() - before.0,
                    tower.metrics().http_connections.get() - before.1
                ),
            );
            ep.close().await;
        }

        tower.shutdown().await?;
        provider_check("T20");
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T21: QUIC address discovery (QAD) on loopback.
#[tokio::test(flavor = "multi_thread")]
async fn t21_qad() -> Res {
    timeout(S(120), async {
        // QAD without TLS
        let mut spec = TowerSpec::plain(lo0());
        spec.quic = Some(lo0());
        match M2Tower::spawn(spec).await {
            Ok(_) => m("T21", "plain tower with QuicConfig", "spawned (unexpected)"),
            Err(e) => m(
                "T21",
                "plain tower with QuicConfig: Server::spawn error",
                chain(&*e),
            ),
        }

        let c = cert(&["127.0.0.1"])?;
        let tower = tls_tower(&c, true, None).await?;
        let qad = tower.quic_addr().ok_or("no quic addr")?;
        m(
            "T21",
            "quic_addr ip / differs from https port",
            format!(
                "{} / {}",
                qad.ip(),
                Some(qad.port()) != tower.https_addr().map(|a| a.port())
            ),
        );
        let url = tower.url_for("127.0.0.1")?;
        let closed = free_udp_port()?;
        let mut res: HashMap<&str, Vec<Duration>> = HashMap::new();
        for run in 0..5 {
            for (mode, port) in [
                ("QAD on", Some(qad.port())),
                ("QAD port closed", Some(closed)),
                ("QAD off", None),
            ] {
                let mx = tower.metrics();
                let before = (mx.qad_incoming.get(), mx.qad_connections.get());
                let mut c1 = cfg(configs(&[(&url, port)]));
                c1.ca_tls = Some(trust(&[&c]));
                let (ep, on) = bind_online(&c1, S(20)).await?;
                let on = on.ok_or(format!("{mode}: not online in 20 s"))?;
                res.entry(mode).or_default().push(on);
                // QAD runs in parallel with the HTTPS probe; give it a moment to be counted.
                tokio::time::sleep(MS(300)).await;
                let mx = tower.metrics();
                if run == 0 {
                    let bound = ep.bound_sockets();
                    let addrs: Vec<String> = ep
                        .addr()
                        .ip_addrs()
                        .map(|a| {
                            if bound.contains(a) {
                                "own socket".to_string()
                            } else {
                                format!("other {}", a.ip())
                            }
                        })
                        .collect();
                    m(
                        "T21",
                        &format!("{mode}: tower qad_incoming / qad_connections delta"),
                        format!(
                            "{} / {}",
                            mx.qad_incoming.get() - before.0,
                            mx.qad_connections.get() - before.1
                        ),
                    );
                    m(
                        "T21",
                        &format!("{mode}: ep.addr() ip addrs"),
                        format!("{addrs:?}"),
                    );
                    #[cfg(feature = "net-report")]
                    if let Some(r) = ep.net_report().get() {
                        let probes: Vec<String> = r
                            .relay_latency
                            .iter()
                            .map(|(p, _, d)| format!("{p:?} {}", ms(d)))
                            .collect();
                        let g4 = r.global_v4.map(|a| {
                            if bound.contains(&SocketAddr::V4(a)) {
                                "own socket".to_string()
                            } else {
                                format!("{}", a.ip())
                            }
                        });
                        m(
                            "T21",
                            &format!("{mode}: net_report() [unstable-net-report]"),
                            format!(
                                "udp_v4={} global_v4={g4:?} mapping_varies={:?} probes={probes:?}",
                                r.udp_v4,
                                r.mapping_varies_by_dest()
                            ),
                        );
                    }
                }
                ep.close().await;
            }
        }
        for mode in ["QAD on", "QAD port closed", "QAD off"] {
            m(
                "T21",
                &format!("{mode}: bind -> online"),
                stats(res.get(mode).map(|v| v.as_slice()).unwrap_or(&[])),
            );
        }
        let mx = tower.metrics();
        m(
            "T21",
            "tower totals after 15 endpoints: qad_incoming / qad_connections / qad_incoming_error / qad_connections_closed",
            format!(
                "{} / {} / {} / {}",
                mx.qad_incoming.get(),
                mx.qad_connections.get(),
                mx.qad_incoming_error.get(),
                mx.qad_connections_closed.get()
            ),
        );
        // relay-only endpoint (no UDP socket) with QAD configured
        let before = tower.metrics().qad_incoming.get();
        let mut c1 = cfg(configs(&[(&url, Some(qad.port()))]));
        c1.ca_tls = Some(trust(&[&c]));
        c1.relay_only = true;
        let (ep, on) = bind_online(&c1, S(20)).await?;
        tokio::time::sleep(MS(300)).await;
        m(
            "T21",
            "relay-only endpoint, QAD configured: online / qad_incoming delta",
            format!(
                "{:?} / {}",
                on.map(ms),
                tower.metrics().qad_incoming.get() - before
            ),
        );
        ep.close().await;
        tower.shutdown().await?;
        provider_check("T21");
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T22: two towers. (a) home choice, failover and hysteresis, parallel instances: plain towers
// waiting / nudged with network_change() / nudged with remove_relay + insert_relay of the dead
// home, and TLS towers with QAD waiting; (b) X and Y with one tower each in their maps but both in
// their lookups; (c) an open connection when the tower carrying its selected path stops.
#[tokio::test(flavor = "multi_thread")]
async fn t22_two_towers() -> Res {
    timeout(S(170), async {
        let c = cert(&["127.0.0.1"])?;
        let (a, b, cc) = tokio::join!(t22a(&c), t22b(), t22c());
        a?;
        b?;
        cc?;
        Res::Ok(())
    })
    .await?
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Plain,
    TlsQad,
}

/// A tower of either kind, with what is needed to restart it on the same ports.
struct Tw {
    tower: M2Tower,
    url: RelayUrl,
    qad: Option<u16>,
    addrs: (SocketAddr, Option<SocketAddr>, Option<SocketAddr>),
}

async fn tw(
    kind: Kind,
    c: &Cert,
    addrs: (SocketAddr, Option<SocketAddr>, Option<SocketAddr>),
) -> Res<Tw> {
    let spec = match kind {
        Kind::Plain => TowerSpec::plain(addrs.0),
        Kind::TlsQad => {
            let mut s = TowerSpec::tls(addrs.0, addrs.1.unwrap_or(lo0()), c.files.clone());
            s.quic = Some(addrs.2.unwrap_or(lo0()));
            s
        }
    };
    let tower = M2Tower::spawn(spec).await?;
    let url = tower.url_for("127.0.0.1")?;
    let qad = tower.quic_addr().map(|a| a.port());
    let addrs = (
        tower.http_addr().ok_or("no http")?,
        tower.https_addr(),
        tower.quic_addr(),
    );
    Ok(Tw {
        tower,
        url,
        qad,
        addrs,
    })
}

async fn tw_new(kind: Kind, c: &Cert) -> Res<Tw> {
    tw(kind, c, (lo0(), None, None)).await
}

async fn tw_restart(
    kind: Kind,
    c: &Cert,
    addrs: (SocketAddr, Option<SocketAddr>, Option<SocketAddr>),
) -> Res<Tw> {
    let t = Instant::now();
    loop {
        match tw(kind, c, addrs).await {
            Ok(t) => return Ok(t),
            Err(_) if t.elapsed() < S(5) => tokio::time::sleep(MS(100)).await,
            Err(e) => return Err(e),
        }
    }
}

fn tw_cfg(towers: &[&Tw], c: &Cert, kind: Kind) -> EpConfig {
    let mut e = cfg(configs(
        &towers.iter().map(|t| (&t.url, t.qad)).collect::<Vec<_>>(),
    ));
    if kind == Kind::TlsQad {
        e.ca_tls = Some(trust(&[c]));
    }
    e
}

fn selected_relay(conn: &Connection) -> Option<RelayUrl> {
    conn.paths()
        .iter()
        .find(|p| p.is_selected() && p.is_relay())
        .and_then(|p| match p.remote_addr() {
            iroh::TransportAddr::Relay(u) => Some(u.clone()),
            _ => None,
        })
}

type T22aOut = (
    Duration,
    String,
    usize,
    Option<Duration>,
    Option<Duration>,
    String,
);

async fn t22a_run(i: usize, kind: Kind, nudge: &str, c: &Cert) -> Res<T22aOut> {
    let ta = tw_new(kind, c).await?;
    let tb = tw_new(kind, c).await?;
    let l = Labels::of(&[(&ta.url, "A"), (&tb.url, "B")]);
    let e = tw_cfg(&[&ta, &tb], c, kind);
    let (ep, on) = bind_online(&e, S(20)).await?;
    let on = on.ok_or("not online with two towers in 20 s")?;
    let first = home(&ep, &l).ok_or("no home")?;
    let entries = ep.home_relay_status().get().len();
    if i == 0 && kind == Kind::Plain && nudge == "wait" {
        m("T22", "two towers up: status after online", st(&ep, &l));
    }
    // stop the home tower (>= 1 s after the last connect, see notes section 2)
    tokio::time::sleep(S(1)).await;
    let (stopped, other) = if first == "A" { (ta, tb) } else { (tb, ta) };
    let (addrs, dead_url) = (stopped.addrs, stopped.url.clone());
    let want = if first == "A" { "B" } else { "A" };
    let t0 = Instant::now();
    let (h, log) = watch_status(&ep, t0);
    stopped.tower.shutdown().await?;
    match nudge {
        "network_change()" => {
            tokio::time::sleep(S(1)).await;
            ep.network_change().await;
        }
        "remove + insert dead home" => {
            tokio::time::sleep(S(1)).await;
            if let Some(cfg) = ep.remove_relay(&dead_url).await {
                ep.insert_relay(dead_url.clone(), cfg).await;
            }
        }
        _ => {}
    }
    let failover = wait_for(S(70), || home(&ep, &l).as_deref() == Some(want))
        .await
        .map(|_| t0.elapsed());
    // restart the stopped tower on the same ports, then watch for 50 s
    let restarted = tw_restart(kind, c, addrs).await?;
    let tr = Instant::now();
    let back = wait_for(S(50), || home(&ep, &l).as_deref() == Some(first.as_str()))
        .await
        .map(|_| tr.elapsed());
    h.abort();
    if i == 0 && nudge == "wait" {
        print_log_l("T22", &format!("{kind:?}: home stopped (run 0)"), &log, &l);
    }
    let end = st(&ep, &l);
    ep.close().await;
    restarted.tower.shutdown().await?;
    other.tower.shutdown().await?;
    Ok((on, first, entries, failover, back, end))
}

async fn t22a(c: &Cert) -> Res {
    let variants: Vec<(Kind, &str, usize)> = vec![
        (Kind::Plain, "wait", 5),
        (Kind::Plain, "network_change()", 3),
        (Kind::Plain, "remove + insert dead home", 3),
        (Kind::TlsQad, "wait", 3),
    ];
    let mut futs = vec![];
    for (kind, nudge, n) in &variants {
        for i in 0..*n {
            futs.push(async move { (*kind, *nudge, i, t22a_run(i, *kind, nudge, c).await) });
        }
    }
    let mut online = vec![];
    let mut fail: HashMap<String, Vec<Duration>> = HashMap::new();
    let mut back_n: HashMap<String, (usize, usize)> = HashMap::new();
    for (kind, nudge, i, r) in n0_future::join_all(futs).await {
        let (on, first, entries, failover, back, end) = r?;
        let key = format!("{kind:?}, {nudge}");
        online.push(on);
        if let Some(f) = failover {
            fail.entry(key.clone()).or_default().push(f);
        }
        let e = back_n.entry(key.clone()).or_default();
        e.1 += 1;
        if back.is_some() {
            e.0 += 1;
        }
        m(
            "T22",
            &format!(
                "{key}, run {i}: first home / status entries / home stopped -> on the other / moved back within 50 s of restart / status at end"
            ),
            format!(
                "{first} / {entries} / {:?} / {:?} / {end}",
                failover.map(ms),
                back.map(ms)
            ),
        );
    }
    m(
        "T22",
        "two towers: bind -> online (all variants)",
        stats(&online),
    );
    for (kind, nudge, _) in &variants {
        let key = format!("{kind:?}, {nudge}");
        m(
            "T22",
            &format!("{key}: home tower stopped -> connected to the other"),
            stats(fail.get(&key).map(|v| v.as_slice()).unwrap_or(&[])),
        );
        let (b, n) = back_n.get(&key).copied().unwrap_or_default();
        m(
            "T22",
            &format!("{key}: home moved back to the restarted tower within 50 s"),
            format!("{b} of {n}"),
        );
    }
    Ok(())
}

async fn t22b() -> Res {
    let ta = plain_tower(lo0(), None).await?;
    let tb = plain_tower(lo0(), None).await?;
    let (ua, ub) = (ta.url()?, tb.url()?);
    let l = Labels::of(&[(&ua, "A"), (&ub, "B")]);
    let mut times = vec![];
    for i in 0..5 {
        let (mut cx, mut cy) = pair(configs(&[(&ua, None)]), configs(&[(&ub, None)]));
        cx.lookup = Some(vec![ua.clone(), ub.clone()]);
        cy.lookup = Some(vec![ua.clone(), ub.clone()]);
        cx.relay_only = true;
        cy.relay_only = true;
        let x = node(cx, true).await?;
        let y = node(cy, true).await?;
        let t = Instant::now();
        let (ox, oy) = tokio::join!(
            online_within(&x.ep, t, S(20)),
            online_within(&y.ep, t, S(20))
        );
        ox.ok_or("X not online")?;
        oy.ok_or("Y not online")?;
        let t = Instant::now();
        let r = timeout(S(15), async {
            let conn = x.ep.connect(y.id(), LINK_ALPN).await?;
            request(&conn, b"x").await?;
            Res::Ok(conn)
        })
        .await;
        match r {
            Ok(Ok(conn)) => {
                times.push(t.elapsed());
                if i == 0 {
                    m(
                        "T22",
                        "X (map A) -> Y (map B), lookups [A, B]: paths",
                        l.apply(&paths::describe(&conn)),
                    );
                }
            }
            Ok(Err(e)) => m(
                "T22",
                "X (map A) -> Y (map B)",
                format!("Err {}", chain(&*e)),
            ),
            Err(_) => m("T22", "X (map A) -> Y (map B)", "no echo within 15 s"),
        }
        x.router.shutdown().await?;
        y.router.shutdown().await?;
    }
    m(
        "T22",
        "X (map A only) dials Y (map B only), both lookups [A, B], relay-only: connect + echo",
        stats(&times),
    );
    // X can reach only A: its lookup for Y lists A and a dead URL instead of B.
    let dead = dead_url()?;
    let (mut cx, mut cy) = pair(configs(&[(&ua, None)]), configs(&[(&ub, None)]));
    cx.lookup = Some(vec![ua.clone(), dead]);
    cx.relay_only = true;
    cy.relay_only = true;
    let x = node(cx, true).await?;
    let y = node(cy, true).await?;
    let t = Instant::now();
    online_within(&x.ep, t, S(20)).await.ok_or("X not online")?;
    online_within(&y.ep, t, S(20)).await.ok_or("Y not online")?;
    let t = Instant::now();
    let r = timeout(S(10), x.ep.connect(y.id(), LINK_ALPN)).await;
    m(
        "T22",
        "X reaches only A (lookup [A, dead]), Y only on B: connect",
        match r {
            Ok(Ok(_)) => format!("connected after {} (unexpected)", ms(t.elapsed())),
            Ok(Err(e)) => format!("Err after {}: {}", ms(t.elapsed()), chain(&e)),
            Err(_) => "nothing within 10 s (would end at the 30 s idle timeout)".into(),
        },
    );
    x.router.shutdown().await?;
    y.router.shutdown().await?;
    ta.shutdown().await?;
    tb.shutdown().await?;
    Ok(())
}

type T22cOut = (
    String,
    String,
    String,
    Option<Duration>,
    String,
    String,
    String,
);

/// Relay-only X and Y (so no QAD: no socket), both homed on A, then B added; the tower carrying
/// the selected path stops. `nudge`: remove_relay + insert_relay of the stopped tower on both,
/// 1 s later.
async fn t22c_run(i: usize, nudge: bool) -> Res<T22cOut> {
    let ta = plain_tower(lo0(), None).await?;
    let tb = plain_tower(lo0(), None).await?;
    let (ua, ub) = (ta.url()?, tb.url()?);
    let l = Labels::of(&[(&ua, "A"), (&ub, "B")]);
    let (mut cx, mut cy) = pair(
        Relays::Shared(shared(&[&ua])),
        Relays::Shared(shared(&[&ua])),
    );
    cx.relay_only = true;
    cy.relay_only = true;
    let x = node(cx, true).await?;
    let y = node(cy, true).await?;
    let t = Instant::now();
    let (ox, oy) = tokio::join!(
        online_within(&x.ep, t, S(20)),
        online_within(&y.ep, t, S(20))
    );
    ox.ok_or("X not online")?;
    oy.ok_or("Y not online")?;
    x.ep.insert_relay(ub.clone(), client_config(&ub, None))
        .await;
    y.ep.insert_relay(ub.clone(), client_config(&ub, None))
        .await;
    let conn = timeout(S(10), x.ep.connect(y.id(), LINK_ALPN)).await??;
    request(&conn, b"x").await?;
    tokio::time::sleep(S(2)).await;
    let homes = format!("{:?}/{:?}", home(&x.ep, &l), home(&y.ep, &l));
    let before = l.apply(&paths::describe(&conn));
    let carrier = selected_relay(&conn)
        .map(|u| l.name(&u))
        .unwrap_or_default();
    let (stop, keep) = if carrier == "B" { (tb, ta) } else { (ta, tb) };
    let t0 = Instant::now();
    let (h, log) = watch_status(&y.ep, t0);
    stop.shutdown().await?;
    if nudge {
        // what a station would do when its home tower stays disconnected
        tokio::time::sleep(S(1)).await;
        let dead = if carrier == "B" { &ub } else { &ua };
        for ep in [&x.ep, &y.ep] {
            if let Some(c) = ep.remove_relay(dead).await {
                ep.insert_relay(dead.clone(), c).await;
            }
        }
    }
    let (back, how) = ping_until_ok(&conn, S(55)).await;
    let rehome = format!("{:?}/{:?}", home(&x.ep, &l), home(&y.ep, &l));
    h.abort();
    if i == 0 {
        print_log_l(
            "T22",
            &format!("open conn, nudge={nudge}: Y after the carrier stopped (run 0)"),
            &log,
            &l,
        );
    }
    let after = l.apply(&paths::describe(&conn));
    let t = Instant::now();
    let redial = match timeout(S(10), async {
        let c2 = x.ep.connect(y.id(), LINK_ALPN).await?;
        request(&c2, b"again").await?;
        Res::Ok(l.apply(&paths::describe(&c2)))
    })
    .await
    {
        Ok(Ok(p)) => format!("ok after {}; paths {p}", ms(t.elapsed())),
        Ok(Err(e)) => format!("Err {}", chain(&*e)),
        Err(_) => "nothing within 10 s".into(),
    };
    x.router.shutdown().await?;
    y.router.shutdown().await?;
    keep.shutdown().await?;
    Ok((
        homes,
        before,
        carrier,
        back,
        how,
        rehome,
        format!("{after}; fresh dial X -> Y: {redial}"),
    ))
}

async fn t22c() -> Res {
    let futs = [(0, false), (1, false), (0, true), (1, true)]
        .map(|(i, nudge)| async move { (i, nudge, t22c_run(i, nudge).await) });
    for (i, nudge, r) in n0_future::join_all(futs).await {
        let (homes, before, carrier, back, how, rehome, after) = r?;
        let k = format!("open relayed conn, remove+insert nudge={nudge}, run {i}");
        m(
            "T22",
            &format!("{k}: homes X/Y; paths; carrier stopped"),
            format!("{homes}; {before}; {carrier}"),
        );
        m(
            "T22",
            &format!("{k}: carrier stopped -> echo works again"),
            format!("{:?} ({how})", back.map(ms)),
        );
        m(
            "T22",
            &format!("{k}: homes X/Y after; old conn paths; fresh dial"),
            format!("{rehome}; {after}"),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// T23: changing the relay map at runtime. (a) 5 parallel instances: X and Y relay-only on A with
// an open connection; X gets B (insert_relay), then loses A (remove_relay). (b) Removing the only
// (home) tower, inserting it again, and inserting a URL that is already there.
#[tokio::test(flavor = "multi_thread")]
async fn t23_runtime_relay_map() -> Res {
    timeout(S(150), async {
        let (a, b) = tokio::join!(t23a(), t23b());
        a?;
        b?;
        Res::Ok(())
    })
    .await?
}

type T23Out = (
    Duration,
    usize,
    usize,
    String,
    Option<Duration>,
    String,
    String,
    String,
    String,
);

async fn t23a_run(i: usize) -> Res<T23Out> {
    let ta = plain_tower(lo0(), None).await?;
    let tb = plain_tower(lo0(), None).await?;
    let (ua, ub) = (ta.url()?, tb.url()?);
    let l = Labels::of(&[(&ua, "A"), (&ub, "B")]);
    let mx = shared(&[&ua]);
    let (mut cx, mut cy) = pair(Relays::Shared(mx.clone()), Relays::Shared(shared(&[&ua])));
    cx.relay_only = true;
    cy.relay_only = true;
    let x = node(cx, true).await?;
    let y = node(cy, true).await?;
    let t = Instant::now();
    let (ox, oy) = tokio::join!(
        online_within(&x.ep, t, S(20)),
        online_within(&y.ep, t, S(20))
    );
    ox.ok_or("X not online")?;
    oy.ok_or("Y not online")?;
    let conn = timeout(S(10), x.ep.connect(y.id(), LINK_ALPN)).await??;
    request(&conn, b"x").await?;
    let t0 = Instant::now();
    let (h, log) = watch_status(&x.ep, t0);
    // add B
    let ti = Instant::now();
    x.ep.insert_relay(ub.clone(), client_config(&ub, None))
        .await;
    let insert_took = ti.elapsed();
    let map_len_after_insert = mx.len(); // shared with the endpoint?
    tokio::time::sleep(S(3)).await;
    let after_insert = format!(
        "{} | echo {:?}",
        st(&x.ep, &l),
        ping(&conn, S(2)).await.map(ms)
    );
    // remove A (X's home)
    let tr = Instant::now();
    let removed = x.ep.remove_relay(&ua).await.is_some();
    let map_len_after_remove = mx.len();
    let rehome = wait_for(S(60), || home(&x.ep, &l).as_deref() == Some("B")).await;
    let _ = removed;
    let status_after_remove = st(&x.ep, &l);
    let (echo, how) = ping_until_ok(&conn, S(5)).await;
    let conn_state = format!(
        "{:?} ({how}); paths {}",
        echo.map(ms),
        l.apply(&paths::describe(&conn))
    );
    // a fresh dial from X to Y (Y only on A, X's lookup now only [B])
    let fresh = match timeout(S(8), x.ep.connect(y.id(), LINK_ALPN)).await {
        Ok(Ok(c2)) => format!("{:?}", ping(&c2, S(3)).await.map(ms)),
        Ok(Err(e)) => format!("Err {}", chain(&e)),
        Err(_) => "no connection within 8 s".into(),
    };
    h.abort();
    if i == 0 {
        print_log_l(
            "T23",
            "X: insert B at 0 ms, remove A 3 s later (run 0)",
            &log,
            &l,
        );
    }
    let _ = tr;
    x.router.shutdown().await?;
    y.router.shutdown().await?;
    ta.shutdown().await?;
    tb.shutdown().await?;
    Ok((
        insert_took,
        map_len_after_insert,
        map_len_after_remove,
        after_insert,
        rehome,
        status_after_remove,
        conn_state,
        fresh,
        String::new(),
    ))
}

async fn t23a() -> Res {
    let futs = (0..5).map(|i| async move { (i, t23a_run(i).await) });
    let mut rehomes = vec![];
    for (i, r) in n0_future::join_all(futs).await {
        let (ins, len_i, len_r, after_insert, rehome, after_remove, conn, fresh, _) = r?;
        if let Some(d) = rehome {
            rehomes.push(d);
        }
        m(
            "T23",
            &format!(
                "run {i}: insert_relay(B) took / shared RelayMap len after insert, after remove"
            ),
            format!("{} / {len_i}, {len_r}", ms(ins)),
        );
        m(
            "T23",
            &format!("run {i}: 3 s after insert_relay(B): X status | open conn"),
            after_insert,
        );
        m(
            "T23",
            &format!("run {i}: remove_relay(A = home) -> X home B; status then"),
            format!("{:?}; {after_remove}", rehome.map(ms)),
        );
        m(
            "T23",
            &format!("run {i}: open conn (Y on A only) after remove_relay(A)"),
            conn,
        );
        m(
            "T23",
            &format!("run {i}: fresh dial X -> Y after remove_relay(A)"),
            fresh,
        );
    }
    m(
        "T23",
        "remove_relay(home A) -> connected to B",
        stats(&rehomes),
    );
    Ok(())
}

async fn t23b() -> Res {
    let ta = plain_tower(lo0(), None).await?;
    let ua = ta.url()?;
    let l = Labels::of(&[(&ua, "A")]);
    let mut back = vec![];
    let mut same = vec![];
    for i in 0..5 {
        let map = shared(&[&ua]);
        let (ep, on) = bind_online(&cfg(Relays::Shared(map.clone())), S(20)).await?;
        on.ok_or("not online")?;
        let t0 = Instant::now();
        let (h, log) = watch_status(&ep, t0);
        let removed = ep.remove_relay(&ua).await;
        tokio::time::sleep(S(1)).await;
        let s1 = st(&ep, &l);
        let still_online = timeout(MS(100), ep.online()).await.is_ok();
        let ti = Instant::now();
        ep.insert_relay(ua.clone(), client_config(&ua, None)).await;
        let on2 = online_within(&ep, ti, S(40)).await;
        if let Some(d) = on2 {
            back.push(d);
        }
        // the same URL again while it is present (and home)
        tokio::time::sleep(S(1)).await;
        let n_before = log.lock().map(|g| g.len()).unwrap_or(0);
        let ts = Instant::now();
        let prev = ep.insert_relay(ua.clone(), client_config(&ua, None)).await;
        let took = ts.elapsed();
        tokio::time::sleep(S(3)).await;
        let n_after = log.lock().map(|g| g.len()).unwrap_or(0);
        same.push(took);
        h.abort();
        if i == 0 {
            m(
                "T23",
                "only tower removed: remove_relay returned / status 1 s later / online() resolves",
                format!("{:?} / {s1} / {still_online}", removed.map(|_| "Some")),
            );
            m(
                "T23",
                "same URL inserted while present: returned / status changes within 3 s / status",
                format!(
                    "{:?} / {} / {}",
                    prev.map(|_| "Some(previous)"),
                    n_after - n_before,
                    st(&ep, &l)
                ),
            );
            print_log_l(
                "T23",
                "remove only tower, re-insert, insert again (run 0)",
                &log,
                &l,
            );
        }
        ep.close().await;
    }
    m(
        "T23",
        "only (home) tower removed, then inserted again -> online",
        stats(&back),
    );
    m(
        "T23",
        "insert_relay of a URL already present: call took",
        stats(&same),
    );
    ta.shutdown().await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// T24: a tower that admits only listed endpoint ids. (a) a stranger against a TLS+QAD tower that
// denies it, 60 s; (b) a denying and an admitting tower together; (c) a listed endpoint dialing a
// stranger through the denying tower, and the reverse; (d) changing the list while the tower runs.
#[tokio::test(flavor = "multi_thread")]
async fn t24_denying_tower() -> Res {
    timeout(S(150), async {
        let c = cert(&["127.0.0.1"])?;
        let (a, rest) = tokio::join!(t24a(&c), async {
            // D denies everyone except L; P admits everyone.
            let kl = secret();
            let members = Arc::new(Members::new(
                [id_of(&kl)],
                Some("not a member of this network"),
            ));
            let acc: Arc<dyn DynAccessControl> = members.clone();
            let d = plain_tower(lo0(), Some(acc)).await?;
            let p = plain_tower(lo0(), None).await?;
            let (b, cd) = tokio::join!(t24b(&d, &p), t24cd(&d, &members, kl));
            b?;
            cd?;
            d.shutdown().await?;
            p.shutdown().await?;
            Res::Ok(())
        });
        a?;
        rest?;
        Res::Ok(())
    })
    .await?
}

async fn t24a(c: &Cert) -> Res {
    let members = Arc::new(Members::new([], Some("not a member of this network")));
    let acc: Arc<dyn DynAccessControl> = members.clone();
    let tower = tls_tower(c, true, Some(acc)).await?;
    let url = tower.url_for("127.0.0.1")?;
    let qad = tower.quic_addr().map(|a| a.port());
    let l = Labels::of(&[(&url, "D")]);
    let mut e = cfg(configs(&[(&url, qad)]));
    e.ca_tls = Some(trust(&[c]));
    let stranger_id = id_of(&e.secret);
    let t0 = Instant::now();
    let ep = endpoint::bind(&e).await?;
    let (h, log) = watch_status(&ep, t0);
    let on = online_within(&ep, t0, S(60)).await;
    h.abort();
    m(
        "T24",
        "stranger vs denying TLS+QAD tower: online within 60 s",
        format!("{:?}", on.map(ms)),
    );
    m("T24", "stranger: status at 60 s", st(&ep, &l));
    if let Some(s) = ep.home_relay_status().get().first() {
        m(
            "T24",
            "stranger: RelayStatus::auth_denied_reason() / last_error()",
            format!(
                "{:?} / {:?}",
                s.auth_denied_reason(),
                s.last_error().map(|e| format!("{e}"))
            ),
        );
    }
    let attempts = members.attempts(&stranger_id);
    let secs: Vec<String> = attempts
        .iter()
        .map(|(t, _)| format!("{:.2}", t.duration_since(t0).as_secs_f64()))
        .collect();
    m(
        "T24",
        "stranger: on_connect calls at the tower in 60 s (count; seconds after bind)",
        format!("{}; {}", attempts.len(), secs.join(" ")),
    );
    let mx = tower.metrics();
    m(
        "T24",
        "denying tower metrics after 60 s: accepts / http(s) connections / qad_incoming / qad_connections",
        format!(
            "{} / {} / {} / {}",
            mx.accepts.get(),
            mx.http_connections.get(),
            mx.qad_incoming.get(),
            mx.qad_connections.get()
        ),
    );
    if let Ok(g) = log.lock() {
        let changes: Vec<String> = g
            .iter()
            .take(6)
            .map(|(t, s)| format!("@{} {}", ms(*t), l.apply(s)))
            .collect();
        m("T24", "stranger: first status changes", changes.join(" ; "));
        m("T24", "stranger: status changes in 60 s", g.len());
    }
    let https = tower.https_addr().ok_or("no https")?;
    let http = tower.http_addr().ok_or("no http")?;
    m(
        "T24",
        "denying tower: plain GET /generate_204 on its http listener (no id involved)",
        http_get(http, "/generate_204").await,
    );
    let _ = https;
    ep.close().await;
    tower.shutdown().await?;
    Ok(())
}

async fn t24b(d: &M2Tower, p: &M2Tower) -> Res {
    let (ud, up) = (d.url()?, p.url()?);
    let l = Labels::of(&[(&ud, "D"), (&up, "P")]);
    // 6 strangers with both towers in the map
    let futs = (0..6).map(|_| {
        let (ud, up, l) = (ud.clone(), up.clone(), l.clone());
        async move {
            let t0 = Instant::now();
            let ep = endpoint::bind(&cfg(configs(&[(&ud, None), (&up, None)]))).await?;
            let (h, log) = watch_status(&ep, t0);
            let on = online_within(&ep, t0, S(45)).await;
            h.abort();
            let was_home_d = log
                .lock()
                .map(|g| g.iter().any(|(_, s)| l.apply(s).contains("url=D")))
                .unwrap_or(false);
            let mut end = st(&ep, &l);
            if on.is_none() {
                // what a station would do on auth_denied: drop that tower from its map
                let tr = Instant::now();
                ep.remove_relay(&ud).await;
                let on2 = online_within(&ep, tr, S(20)).await;
                end = format!(
                    "{end}; after remove_relay(D): online after {:?}, {}",
                    on2.map(ms),
                    st(&ep, &l)
                );
            }
            ep.close().await;
            Res::Ok((on, was_home_d, end))
        }
    });
    let mut ons = vec![];
    let mut d_home = 0;
    let mut stuck = 0;
    for r in n0_future::join_all(futs).await {
        let (on, was_d, end) = r?;
        if let Some(d) = on {
            ons.push(d);
        } else {
            stuck += 1;
        }
        if was_d {
            d_home += 1;
        }
        m(
            "T24",
            "stranger with [D denies, P admits]: online / D was home at some point / status at end",
            format!("{:?} / {was_d} / {end}", on.map(ms)),
        );
    }
    m("T24", "strangers with [D, P]: bind -> online", stats(&ons));
    m(
        "T24",
        "strangers with [D, P]: D chosen as home at some point / not online in 45 s",
        format!("{d_home} of 6 / {stuck} of 6"),
    );
    // forced: start with only D (home, denied), then insert P 3 s later
    let futs = (0..3).map(|_| {
        let (ud, up, l) = (ud.clone(), up.clone(), l.clone());
        async move {
            let ep = endpoint::bind(&cfg(Relays::Shared(shared(&[&ud])))).await?;
            tokio::time::sleep(S(3)).await;
            let s3 = st(&ep, &l);
            let ti = Instant::now();
            ep.insert_relay(up.clone(), client_config(&up, None)).await;
            let on = online_within(&ep, ti, S(60)).await;
            let end = st(&ep, &l);
            ep.close().await;
            Res::Ok((s3, on, end))
        }
    });
    for r in n0_future::join_all(futs).await {
        let (s3, on, end) = r?;
        m(
            "T24",
            "stranger homed on D (denied), then insert_relay(P): status before / insert -> online / status at end",
            format!("{s3} / {:?} / {end}", on.map(ms)),
        );
    }
    Ok(())
}

async fn t24cd(d: &M2Tower, members: &Arc<Members>, kl: [u8; 32]) -> Res {
    let ud = d.url()?;
    let l = Labels::of(&[(&ud, "D")]);
    let ks = secret();
    // L (listed) and S (stranger) know each other; both relay-only on D
    let mut cl = EpConfig::new(
        kl,
        configs(&[(&ud, None)]),
        Arc::new(Authorizer::new([id_of(&ks)])),
    );
    let mut cs = EpConfig::new(
        ks,
        configs(&[(&ud, None)]),
        Arc::new(Authorizer::new([id_of(&kl)])),
    );
    cl.relay_only = true;
    cs.relay_only = true;
    let lnode = node(cl, true).await?;
    let snode = node(cs, true).await?;
    let t = Instant::now();
    let on_l = online_within(&lnode.ep, t, S(20)).await;
    m(
        "T24",
        "listed L on D: bind -> online",
        format!("{:?}", on_l.map(ms)),
    );
    on_l.ok_or("listed endpoint not online")?;
    for (who, from, to) in [
        ("listed L -> stranger S", &lnode, &snode),
        ("stranger S -> listed L", &snode, &lnode),
    ] {
        let t = Instant::now();
        let r = timeout(S(10), from.ep.connect(to.id(), LINK_ALPN)).await;
        m(
            "T24",
            &format!("{who} through D: connect"),
            match r {
                Ok(Ok(_)) => format!("connected after {}", ms(t.elapsed())),
                Ok(Err(e)) => format!("Err after {}: {}", ms(t.elapsed()), chain(&e)),
                Err(_) => "nothing within 10 s".into(),
            },
        );
    }
    m(
        "T24",
        "callee side: accepted connections at L / at S",
        format!(
            "{} / {}",
            lnode
                .link
                .stats
                .accepted
                .load(std::sync::atomic::Ordering::SeqCst),
            snode
                .link
                .stats
                .accepted
                .load(std::sync::atomic::Ordering::SeqCst)
        ),
    );
    snode.router.shutdown().await?;
    // (d) change the list while the tower runs
    tokio::time::sleep(S(1)).await;
    let lid = id_of(&kl);
    members.remove(&lid);
    tokio::time::sleep(S(2)).await;
    m(
        "T24",
        "L removed from the list: L's status 2 s later (no disconnect yet)",
        st(&lnode.ep, &l),
    );
    let svc = d.service().ok_or("no relay service")?;
    let t0 = Instant::now();
    let (h, log) = watch_status(&lnode.ep, t0);
    let found = svc.clients().disconnect(lid, None);
    let gone = wait_for(S(10), || !is_connected(&lnode.ep)).await;
    let denied = wait_for(S(30), || {
        lnode
            .ep
            .home_relay_status()
            .get()
            .iter()
            .any(|s| s.auth_denied_reason().is_some())
    })
    .await;
    m(
        "T24",
        "clients().disconnect(L, None): returned / L not connected after / auth_denied shown after",
        format!(
            "{found} / {:?} / {:?}",
            gone.map(ms),
            denied.map(|_| ms(t0.elapsed()))
        ),
    );
    let ti = Instant::now();
    members.insert(lid);
    let back = wait_for(S(40), || is_connected(&lnode.ep)).await;
    m(
        "T24",
        "L listed again -> connected",
        format!("{:?}", back.map(ms)),
    );
    h.abort();
    print_log_l("T24", "L: disconnect, denied, re-listed", &log, &l);
    let _ = ti;
    lnode.router.shutdown().await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// T25: per-client receive rate limit. 8 echoes of 60 000 bytes over a relayed connection through
// a tower limited to 64 KiB/s per client, through an unlimited tower, and through the limited
// tower after set_client_rate_limit(None).
#[tokio::test(flavor = "multi_thread")]
async fn t25_rate_limit() -> Res {
    timeout(S(120), async {
        const N: usize = 8;
        const SIZE: usize = 60_000;
        let mut lim = ClientRateLimit::new(NonZeroU32::new(64 * 1024).ok_or("zero")?);
        lim.max_burst_bytes = None;
        let mut spec = TowerSpec::plain(lo0());
        spec.client_rx = Some(lim);
        let limited = M2Tower::spawn(spec).await?;
        let open = plain_tower(lo0(), None).await?;
        let msg = vec![7u8; SIZE];
        let run = |tower: &M2Tower, name: &'static str| {
            let url = tower.url();
            let msg = msg.clone();
            async move {
                let url = url?;
                let l = Labels::of(&[(&url, "T")]);
                let (mut cx, mut cy) = pair(configs(&[(&url, None)]), configs(&[(&url, None)]));
                cx.relay_only = true;
                cy.relay_only = true;
                let x = node(cx, true).await?;
                let y = node(cy, true).await?;
                let t = Instant::now();
                online_within(&x.ep, t, S(20)).await.ok_or("X offline")?;
                online_within(&y.ep, t, S(20)).await.ok_or("Y offline")?;
                let conn = timeout(S(10), x.ep.connect(y.id(), LINK_ALPN)).await??;
                let (h, log) = watch_status(&x.ep, Instant::now());
                let t = Instant::now();
                let mut each = vec![];
                let mut err = None;
                for _ in 0..N {
                    let ti = Instant::now();
                    match timeout(S(30), request(&conn, &msg)).await {
                        Ok(Ok(echo)) if echo.len() == SIZE => each.push(ti.elapsed()),
                        Ok(Ok(_)) => err = Some("short echo".to_string()),
                        Ok(Err(e)) => err = Some(short_err(&*e)),
                        Err(_) => err = Some("no echo within 30 s".into()),
                    }
                    if err.is_some() {
                        break;
                    }
                }
                let total = t.elapsed();
                h.abort();
                let changes = log.lock().map(|g| g.len().saturating_sub(1)).unwrap_or(0);
                let rate = (each.len() * SIZE) as f64 / total.as_secs_f64() / 1024.0;
                m(
                    "T25",
                    &format!("{name}: {N} x {SIZE} B echoed: total / per echo / error"),
                    format!(
                        "{} ({rate:.0} KiB/s each way) / {} / {err:?}",
                        ms(total),
                        stats(&each)
                    ),
                );
                m(
                    "T25",
                    &format!(
                        "{name}: X status changes during the transfer / status at end / conn closed"
                    ),
                    format!(
                        "{changes} / {} / {:?}",
                        st(&x.ep, &l),
                        conn.close_reason().map(|r| r.to_string())
                    ),
                );
                x.router.shutdown().await?;
                y.router.shutdown().await?;
                Res::Ok(())
            }
        };
        let (a, b) = tokio::join!(
            run(&limited, "64 KiB/s tower"),
            run(&open, "unlimited tower")
        );
        a?;
        b?;
        let mx = limited.metrics();
        m(
            "T25",
            "limited tower: bytes_rx_ratelimited_total / conns_rx_ratelimited_total / bytes_recv",
            format!(
                "{} / {} / {}",
                mx.bytes_rx_ratelimited_total.get(),
                mx.conns_rx_ratelimited_total.get(),
                mx.bytes_recv.get()
            ),
        );
        m(
            "T25",
            "unlimited tower: bytes_rx_ratelimited_total / conns_rx_ratelimited_total",
            format!(
                "{} / {}",
                open.metrics().bytes_rx_ratelimited_total.get(),
                open.metrics().conns_rx_ratelimited_total.get()
            ),
        );
        limited
            .service()
            .ok_or("no service")?
            .set_client_rate_limit(None);
        run(&limited, "limited tower after set_client_rate_limit(None)").await?;
        limited.shutdown().await?;
        open.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T26: SRV lookup against a fake DNS server on 127.0.0.1, then the whole chain: SRV -> tower URLs
// -> an endpoint whose own resolver uses the same server comes online through TLS towers whose
// certificates name the SRV targets.
#[tokio::test(flavor = "multi_thread")]
async fn t26_srv_bootstrap() -> Res {
    timeout(S(120), async {
        let ca = cert(&["a.example.test"])?;
        let cb = cert(&["b.example.test"])?;
        let ta = tls_tower(&ca, false, None).await?;
        let tb = tls_tower(&cb, false, None).await?;
        let pa = ta.https_addr().ok_or("no https")?.port();
        let pb = tb.https_addr().ok_or("no https")?.port();
        let port_name = |p: u16| if p == pa { "port(A)" } else if p == pb { "port(B)" } else { "other" };
        let mut zone = HashMap::new();
        zone.insert(
            "_numbers._tcp.example.test".to_string(),
            Owner::Srv(vec![
                (10, 60, pa, "a.example.test".into()),
                (20, 40, pb, "b.example.test".into()),
            ]),
        );
        zone.insert("a.example.test".to_string(), Owner::A(Ipv4Addr::LOCALHOST));
        zone.insert("b.example.test".to_string(), Owner::A(Ipv4Addr::LOCALHOST));
        zone.insert("_numbers._tcp.empty.test".to_string(), Owner::Empty);
        let dns = FakeDns::spawn(zone).await?;
        let r = locate::srv_resolver(dns.addr);
        let ns: Vec<String> = r
            .configured_nameservers()
            .iter()
            .map(|n| format!("{:?} {}", n.protocol(), if n.addr() == dns.addr { "fake" } else { "OTHER" }))
            .collect();
        m("T26", "n0_dns_resolver::DnsResolver::builder().nameserver(..): configured_nameservers()", format!("{ns:?}"));

        // SRV answer shape
        let t = Instant::now();
        let recs = r.lookup_srv(locate::srv_name("example.test")).await?;
        let took = t.elapsed();
        let shape: Vec<String> = recs
            .iter()
            .map(|x| format!("{{priority {}, weight {}, port {}, target {:?}}}", x.priority, x.weight, port_name(x.port), x.target))
            .collect();
        m("T26", "lookup_srv(\"_numbers._tcp.example.test.\")", format!("{} in {}", shape.join(", "), ms(took)));
        let q1 = dns.count();
        let _ = r.lookup_srv(locate::srv_name("example.test")).await?;
        let q2 = dns.count();
        r.clear_cache();
        let _ = r.lookup_srv(locate::srv_name("example.test")).await?;
        let q3 = dns.count();
        m("T26", "queries at the server: first lookup / second (cached?) / after clear_cache()", format!("{q1} / {} / {}", q2 - q1, q3 - q2));
        let without_dot = r.lookup_srv("_numbers._tcp.example.test").await.map(|v| v.len());
        m("T26", "lookup_srv without trailing dot (no search list configured)", format!("{without_dot:?}"));

        // NXDOMAIN, NODATA
        let nx = r.lookup_srv(locate::srv_name("missing.test")).await;
        m("T26", "NXDOMAIN: lookup_srv result", match nx {
            Ok(v) => format!("Ok({} records)", v.len()),
            Err(e) => format!("Err({e}) is NxDomain: {}", matches!(e, n0_dns_resolver::Error::NxDomain { .. })),
        });
        let nd = r.lookup_srv(locate::srv_name("empty.test")).await;
        m("T26", "NODATA (empty answer): lookup_srv result", match nd {
            Ok(v) => format!("Ok({} records)", v.len()),
            Err(e) => format!("Err({e})"),
        });
        let c0 = dns.count();
        let _ = r.lookup_srv(locate::srv_name("missing.test")).await;
        m("T26", "NXDOMAIN again: queries sent (negative caching is off by default)", dns.count() - c0);

        // no answer at all
        dns.silent.store(true, std::sync::atomic::Ordering::SeqCst);
        let c0 = dns.count();
        let t = Instant::now();
        let silent = r.lookup_srv(locate::srv_name("silent.test")).await;
        m(
            "T26",
            "server silent: lookup_srv error / after / queries sent",
            format!("{:?} / {} / {}", silent.map(|v| v.len()).map_err(|e| e.to_string()), ms(t.elapsed()), dns.count() - c0),
        );
        dns.silent.store(false, std::sync::atomic::Ordering::SeqCst);

        // the whole chain
        let urls = locate::lookup(&r, "example.test", UrlForm::NoDot).await?;
        let l = Labels::of(&[(&ta.url_for("a.example.test")?, "A"), (&tb.url_for("b.example.test")?, "B")]);
        m("T26", "tower URLs from SRV (no-dot form)", format!("{:?}", urls.iter().map(|u| l.name(u)).collect::<Vec<_>>()));
        let mut times = vec![];
        for i in 0..5 {
            let c0 = dns.count();
            let mut e = cfg(Relays::Configs(urls.iter().map(|u| (u.clone(), None)).collect()));
            e.dns = Some(locate::iroh_resolver(dns.addr));
            e.ca_tls = Some(trust(&[&ca, &cb]));
            let (ep, on) = bind_online(&e, S(20)).await?;
            if i == 0 {
                let log = dns.log();
                let qs: Vec<String> = log[c0..].iter().map(|(n, t)| format!("{n} {t}")).collect();
                m("T26", "endpoint via SRV URLs: status / DNS queries it sent", format!("{} / {qs:?}", st(&ep, &l)));
            }
            times.push(on.ok_or("endpoint not online through the SRV towers")?);
            ep.close().await;
        }
        m("T26", "SRV URLs, fake DNS, TLS by name: bind -> online", stats(&times));

        // trailing dot
        let dotted = locate::lookup(&r, "example.test", UrlForm::TrailingDot).await?;
        let plain_a: RelayUrl = format!("https://a.example.test:{pa}/").parse()?;
        let dotted_a: RelayUrl = format!("https://a.example.test.:{pa}/").parse()?;
        m(
            "T26",
            "RelayUrl(\"https://a.example.test:P/\") == RelayUrl(\"https://a.example.test.:P/\") / host_str of the dotted one",
            format!("{} / {:?}", plain_a == dotted_a, dotted_a.host_str()),
        );
        let ld = Labels::of(&[(&dotted_a, "A.")]);
        let c0 = dns.count();
        let mut e = cfg(Relays::Configs(vec![(dotted_a.clone(), None)]));
        e.dns = Some(locate::iroh_resolver(dns.addr));
        e.ca_tls = Some(trust(&[&ca]));
        let (ep, on) = bind_online(&e, S(15)).await?;
        let qs: Vec<String> = dns.log()[c0..].iter().map(|(n, t)| format!("{n} {t}")).collect();
        m(
            "T26",
            "endpoint with only https://a.example.test.:P/ (cert SAN a.example.test): online / status / DNS queries",
            format!("{:?} / {} / {qs:?}", on.map(ms), ld.apply(&status(&ep)), ),
        );
        ep.close().await;
        m("T26", "tower URLs from SRV (dotted form)", format!("{} URLs, first host {:?}", dotted.len(), dotted.first().and_then(|u| u.host_str().map(str::to_owned))));
        m("T26", "total queries at the fake server", dns.count());
        ta.shutdown().await?;
        tb.shutdown().await?;
        provider_check("T26");
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T27: first home tower with several towers. (a) one up, one down at start; (b) both down, one
// comes up 3 s later, waiting or nudged with remove_relay + insert_relay; (c) remove + insert on an
// endpoint that is already online through another tower, with an open connection.
#[tokio::test(flavor = "multi_thread")]
async fn t27_first_home_tower() -> Res {
    timeout(S(120), async {
        let (a, b, c) = tokio::join!(t27a(), t27b(), t27c());
        a?;
        b?;
        c?;
        Res::Ok(())
    })
    .await?
}

async fn t27a() -> Res {
    let futs = (0..5).map(|_| async move {
        let ta = plain_tower(lo0(), None).await?;
        let ua = ta.url()?;
        let dead = dead_url()?;
        let l = Labels::of(&[(&ua, "A"), (&dead, "B(down)")]);
        let (ep, on) = bind_online(&cfg(configs(&[(&ua, None), (&dead, None)])), S(30)).await?;
        let s = st(&ep, &l);
        ep.close().await;
        ta.shutdown().await?;
        Res::Ok((on, s))
    });
    let mut v = vec![];
    for r in n0_future::join_all(futs).await {
        let (on, s) = r?;
        if let Some(d) = on {
            v.push(d);
        }
        m(
            "T27",
            "A up, B down at start: online / status",
            format!("{:?} / {s}", on.map(ms)),
        );
    }
    m("T27", "A up, B down at start: bind -> online", stats(&v));
    Ok(())
}

async fn t27b() -> Res {
    let futs = (0..8).map(|i| async move {
        let nudge = i % 2 == 1;
        let pa = free_tcp_port()?;
        let ua: RelayUrl = format!("http://127.0.0.1:{pa}/").parse()?;
        let ub = dead_url()?;
        let ep = endpoint::bind(&cfg(configs(&[(&ua, None), (&ub, None)]))).await?;
        tokio::time::sleep(S(3)).await;
        let s3 = status(&ep);
        let ta = plain_tower(lo(pa), None).await?;
        let t = Instant::now();
        if nudge && let Some(c) = ep.remove_relay(&ua).await {
            ep.insert_relay(ua.clone(), c).await;
        }
        let on = online_within(&ep, t, S(60)).await;
        ep.close().await;
        ta.shutdown().await?;
        Res::Ok((nudge, on, s3))
    });
    let mut w = vec![];
    let mut n = vec![];
    for r in n0_future::join_all(futs).await {
        let (nudge, on, s3) = r?;
        if let Some(d) = on {
            if nudge { n.push(d) } else { w.push(d) }
        }
        m(
            "T27",
            "both down at start, A up 3 s later: nudge / status at 3 s / A up -> online",
            format!("{nudge} / {s3} / {:?}", on.map(ms)),
        );
    }
    m(
        "T27",
        "both down, A up after 3 s, waiting: A up -> online",
        stats(&w),
    );
    m(
        "T27",
        "both down, A up after 3 s, remove+insert A: A up -> online",
        stats(&n),
    );
    Ok(())
}

async fn t27c() -> Res {
    let ta = plain_tower(lo0(), None).await?;
    let tb = plain_tower(lo0(), None).await?;
    let (ua, ub) = (ta.url()?, tb.url()?);
    let l = Labels::of(&[(&ua, "A"), (&ub, "B")]);
    let mut gaps = 0;
    for i in 0..3 {
        // X and Y on A only first, B inserted afterwards (so A stays home)
        let (mut cx, mut cy) = pair(
            Relays::Shared(shared(&[&ua])),
            Relays::Shared(shared(&[&ua])),
        );
        cx.relay_only = true;
        cy.relay_only = true;
        let x = node(cx, true).await?;
        let y = node(cy, true).await?;
        let t = Instant::now();
        online_within(&x.ep, t, S(20)).await.ok_or("X offline")?;
        online_within(&y.ep, t, S(20)).await.ok_or("Y offline")?;
        x.ep.insert_relay(ub.clone(), client_config(&ub, None))
            .await;
        let conn = timeout(S(10), x.ep.connect(y.id(), LINK_ALPN)).await??;
        request(&conn, b"x").await?;
        tokio::time::sleep(S(1)).await;
        for (what, url) in [
            ("remove+insert B (not home)", &ub),
            ("remove+insert A (home)", &ua),
        ] {
            let t0 = Instant::now();
            let (h, log) = watch_status(&x.ep, t0);
            if let Some(c) = x.ep.remove_relay(url).await {
                x.ep.insert_relay(url.clone(), c).await;
            }
            let back = wait_for(S(30), || is_connected(&x.ep)).await;
            let echo = ping(&conn, S(3)).await.map(ms);
            tokio::time::sleep(S(2)).await;
            h.abort();
            let changes = log.lock().map(|g| g.len().saturating_sub(1)).unwrap_or(0);
            let disc = log
                .lock()
                .map(|g| g.iter().skip(1).any(|(_, s)| s.contains("connected=false")))
                .unwrap_or(false);
            if disc {
                gaps += 1;
            }
            m(
                "T27",
                &format!(
                    "run {i}: X online via A, open conn, {what}: status changes in 2 s / connected after / echo / status"
                ),
                format!(
                    "{changes} / {:?} / {echo:?} / {}",
                    back.map(ms),
                    st(&x.ep, &l)
                ),
            );
            if i == 0 {
                print_log_l("T27", &format!("{what} (run 0)"), &log, &l);
            }
        }
        x.router.shutdown().await?;
        y.router.shutdown().await?;
    }
    m(
        "T27",
        "remove+insert (6 times): X's status showed connected=false at some point",
        format!("{gaps} of 6"),
    );
    ta.shutdown().await?;
    tb.shutdown().await?;
    Ok(())
}
