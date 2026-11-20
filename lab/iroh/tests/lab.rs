//! T0-T12 plus extras T1b, T7b-T7f, T8b, T14. T13 is this suite run inside a loopback-only network
//! namespace (run.sh). Every test is bounded by a timeout. Run with `--nocapture` to see MEASURE lines.
mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::*;
use iroh::endpoint::{ConnectionError, Side};
use iroh::{NetReportConfig, RelayConfig};
use iroh_lab::Res;
use iroh_lab::endpoint::{self, EpConfig, Relays};
use iroh_lab::gate::{Authorizer, CLOSE_NOT_ALLOWED, LINK_ALPN, OTHER_ALPN};
use iroh_lab::link::{Link, Refuse, request};
use iroh_lab::paths;
use iroh_lab::tower::Tower;
use tokio::time::timeout;

const S: fn(u64) -> Duration = Duration::from_secs;

fn pair_cfgs(towers: &[(iroh::RelayUrl, Option<u16>)]) -> (EpConfig, EpConfig) {
    let (ka, kb) = (secret(), secret());
    let a = EpConfig::new(
        ka,
        Relays::Configs(towers.to_vec()),
        Arc::new(Authorizer::new([id_of(&kb)])),
    );
    let b = EpConfig::new(
        kb,
        Relays::Configs(towers.to_vec()),
        Arc::new(Authorizer::new([id_of(&ka)])),
    );
    (a, b)
}

/// Binds callee and dialer, returns them with their bind->online times.
async fn online_pair(a: EpConfig, b: EpConfig) -> Res<(Node, Node, Duration, Duration)> {
    let ta = Instant::now();
    let na = node(a, true).await?;
    let tb = Instant::now();
    let nb = node(b, true).await?;
    let (oa, ob) = tokio::join!(
        online_within(&na.ep, ta, S(20)),
        online_within(&nb.ep, tb, S(20))
    );
    Ok((
        na,
        nb,
        oa.ok_or("callee not online in 20 s")?,
        ob.ok_or("dialer not online in 20 s")?,
    ))
}

// ---------------------------------------------------------------------------------------------
// T0: what the bind forms actually bind.
#[tokio::test(flavor = "multi_thread")]
async fn t0_bind_forms() -> Res {
    timeout(S(30), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        let auth = Arc::new(Authorizer::default());
        // (a) the snippet's form: bind_addr((LOCALHOST, 0)) without clearing.
        let cfg = EpConfig::new(secret(), Relays::Configs(towers.clone()), auth.clone());
        let mut b = endpoint::builder(&EpConfig {
            loopback_only: false,
            ..cfg.clone()
        })?;
        b = b.bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?;
        let ep = b.bind().await?;
        let socks = ep.bound_sockets();
        m(
            "T0",
            "bind_addr(127.0.0.1:0) only: bound sockets",
            format!("{socks:?}"),
        );
        ep.close().await;
        // (b) clear_ip_transports().bind_addr(..)
        let ep = endpoint::bind(&cfg).await?;
        m(
            "T0",
            "clear_ip_transports + bind_addr: bound sockets",
            format!("{:?}", ep.bound_sockets()),
        );
        let t = Instant::now();
        ep.online().await;
        m(
            "T0",
            "clear+bind_addr: addr() ip addrs after online",
            format!("{:?}", ep.addr().ip_addrs().collect::<Vec<_>>()),
        );
        m("T0", "clear+bind_addr: online", ms(t.elapsed()));
        ep.close().await;
        // (c) relay only
        let ep = endpoint::bind(&EpConfig {
            relay_only: true,
            ..cfg.clone()
        })
        .await?;
        m(
            "T0",
            "relay_only: bound sockets",
            format!("{:?}", ep.bound_sockets()),
        );
        ep.online().await;
        m(
            "T0",
            "relay_only: addr() ip addrs / relay urls",
            format!(
                "{} / {}",
                ep.addr().ip_addrs().count(),
                ep.addr().relay_urls().count()
            ),
        );
        ep.close().await;
        // (d) the default: no bind_addr at all
        let ep = endpoint::bind(&EpConfig {
            loopback_only: false,
            ..cfg.clone()
        })
        .await?;
        m(
            "T0",
            "default (no bind_addr): bound sockets",
            format!("{:?}", ep.bound_sockets()),
        );
        ep.close().await;
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T1: dial by id alone through TowerLookup, 5 runs.
#[tokio::test(flavor = "multi_thread")]
async fn t1_dial_by_id() -> Res {
    timeout(S(120), async {
        let tower = Tower::spawn(lo0()).await?;
        m("T1", "tower url", tower.url());
        m(
            "T1",
            "tower https_addr()",
            format!("{:?}", tower.https_addr()),
        );
        let towers = vec![(tower.url().clone(), None)];
        let (mut on_a, mut on_b, mut conn_t, mut reply_t, mut shut_t) =
            (vec![], vec![], vec![], vec![], vec![]);
        for run in 0..5 {
            let (ca, cb) = pair_cfgs(&towers);
            let (a, b, oa, ob) = online_pair(ca, cb).await?;
            on_a.push(oa);
            on_b.push(ob);
            assert!(
                b.ep.remote_info(a.id()).await.is_none(),
                "dialer knew nothing about the callee"
            );
            let t = Instant::now();
            let conn = timeout(S(10), b.ep.connect(a.id(), LINK_ALPN)).await??;
            let tc = t.elapsed();
            let reply = timeout(S(10), request(&conn, b"hello tower")).await??;
            let tr = t.elapsed();
            assert_eq!(reply, b"hello tower");
            conn_t.push(tc);
            reply_t.push(tr);
            m(
                "T1",
                &format!("run {run}"),
                format!(
                    "callee online {} dialer online {} connect {} first reply {} paths {}",
                    ms(oa),
                    ms(ob),
                    ms(tc),
                    ms(tr),
                    paths::describe(&conn)
                ),
            );
            // the hook ran on both sides
            let ac = a.cfg.hook_log.calls();
            let bc = b.cfg.hook_log.calls();
            assert!(ac.iter().any(|c| c.side == Side::Server && !c.rejected));
            assert!(bc.iter().any(|c| c.side == Side::Client && !c.rejected));
            conn.close(0u32.into(), b"bye");
            let t = Instant::now();
            a.router.shutdown().await?;
            b.router.shutdown().await?;
            shut_t.push(t.elapsed());
        }
        m("T1", "bind->online callee", stats(&on_a));
        m("T1", "bind->online dialer", stats(&on_b));
        m("T1", "connect()", stats(&conn_t));
        m("T1", "connect()->first reply", stats(&reply_t));
        m(
            "T1",
            "two router.shutdown() after conn.close",
            stats(&shut_t),
        );
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T2: relay only on both: the selected path is the relay, no path is IP (section 8 as written).
#[tokio::test(flavor = "multi_thread")]
async fn t2_relay_only() -> Res {
    timeout(S(90), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        let (mut on, mut conn_t, mut reply_t) = (vec![], vec![], vec![]);
        for run in 0..3 {
            let (mut ca, mut cb) = pair_cfgs(&towers);
            ca.relay_only = true;
            cb.relay_only = true;
            let (a, b, oa, ob) = online_pair(ca, cb).await?;
            on.push(oa);
            on.push(ob);
            assert!(a.ep.bound_sockets().is_empty());
            let t = Instant::now();
            // section 8, as written
            let conn = b.ep.connect(a.id(), LINK_ALPN).await?;
            conn_t.push(t.elapsed());
            let mut ps = conn.paths_stream();
            let selected_relay = ps.wait_any(|p| p.is_selected() && p.is_relay());
            let on_relay = timeout(Duration::from_secs(10), selected_relay)
                .await?
                .is_some();
            assert!(on_relay);
            assert!(conn.paths().iter().all(|p| !p.is_ip()));
            drop(ps);
            let reply = request(&conn, b"x").await?;
            reply_t.push(t.elapsed());
            assert_eq!(reply, b"x");
            tokio::time::sleep(S(3)).await;
            assert!(
                conn.paths().iter().all(|p| !p.is_ip()),
                "still no IP path after 3 s"
            );
            m(
                "T2",
                &format!("run {run}"),
                format!("paths after 3 s: {}", paths::describe(&conn)),
            );
            // the callee side sees the same
            conn.close(0u32.into(), b"bye");
            a.router.shutdown().await?;
            b.router.shutdown().await?;
        }
        m("T2", "relay-only bind->online", stats(&on));
        m("T2", "relay-only connect()", stats(&conn_t));
        m("T2", "relay-only connect()->first reply", stats(&reply_t));
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T3: with IP transports (loopback): does the connection move to a direct path, and how fast?
#[tokio::test(flavor = "multi_thread")]
async fn t3_direct_upgrade() -> Res {
    timeout(S(120), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        let mut direct_t = vec![];
        for run in 0..5 {
            let (ca, cb) = pair_cfgs(&towers);
            let (a, b, _, _) = online_pair(ca, cb).await?;
            let t = Instant::now();
            let conn = timeout(S(10), b.ep.connect(a.id(), LINK_ALPN)).await??;
            let at_connect = paths::describe(&conn);
            let tc = t.elapsed();
            let direct = paths::wait_selected_ip(&conn, S(20)).await;
            let td = t.elapsed();
            m(
                "T3",
                &format!("run {run} both with IP"),
                format!(
                    "at connect ({}): {} | direct selected={} after {} | {}",
                    ms(tc),
                    at_connect,
                    direct,
                    ms(td),
                    paths::describe(&conn)
                ),
            );
            if direct {
                direct_t.push(td);
            }
            conn.close(0u32.into(), b"bye");
            a.router.shutdown().await?;
            b.router.shutdown().await?;
        }
        m(
            "T3",
            "connect()->direct path selected (both IP)",
            stats(&direct_t),
        );
        // one side relay-only: never direct
        let (mut ca, cb) = pair_cfgs(&towers);
        ca.relay_only = true;
        let (a, b, _, _) = online_pair(ca, cb).await?;
        let conn = timeout(S(10), b.ep.connect(a.id(), LINK_ALPN)).await??;
        let direct = paths::wait_selected_ip(&conn, S(8)).await;
        m(
            "T3",
            "callee relay-only, dialer IP: direct within 8 s",
            format!("{direct} | {}", paths::describe(&conn)),
        );
        assert!(!direct);
        conn.close(0u32.into(), b"bye");
        a.router.shutdown().await?;
        b.router.shutdown().await?;
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T4a: unknown id over the relay is ignored by the incoming filter.
#[tokio::test(flavor = "multi_thread")]
async fn t4a_filter_ignores_stranger() -> Res {
    timeout(S(150), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        let (mut ca, cb) = pair_cfgs(&towers);
        ca.auth = Arc::new(Authorizer::default()); // the callee allows nobody
        let (a, b, _, _) = online_pair(ca, cb).await?;
        // without a timeout around connect (bounded at 120 s by the test)
        let t = Instant::now();
        let r = timeout(S(120), b.ep.connect(a.id(), LINK_ALPN)).await;
        let took = t.elapsed();
        match &r {
            Err(_) => m(
                "T4a",
                "connect() without timeout",
                format!("still pending after {}", ms(took)),
            ),
            Ok(Ok(_)) => m(
                "T4a",
                "connect() without timeout",
                format!("UNEXPECTED success after {}", ms(took)),
            ),
            Ok(Err(e)) => m(
                "T4a",
                "connect() without timeout",
                format!("Err after {}: {} | {:?}", ms(took), short_err(e), e),
            ),
        }
        m(
            "T4a",
            "callee filter seen/ignored",
            format!(
                "{}/{}",
                a.filter.seen.load(Ordering::SeqCst),
                a.filter.ignored.load(Ordering::SeqCst)
            ),
        );
        m(
            "T4a",
            "callee hook calls / link accepts",
            format!(
                "{} / {}",
                a.cfg.hook_log.calls().len(),
                a.link.stats.accepted.load(Ordering::SeqCst)
            ),
        );
        if let Some(info) = b.ep.remote_info(a.id()).await {
            let addrs: Vec<String> = info.addrs().map(|x| format!("{}", x.addr())).collect();
            m(
                "T4a",
                "dialer remote_info addrs of callee",
                format!("{addrs:?}"),
            );
        }
        assert_eq!(a.link.stats.accepted.load(Ordering::SeqCst), 0);
        assert!(a.cfg.hook_log.calls().is_empty());
        assert!(a.filter.ignored.load(Ordering::SeqCst) >= 1);
        // with a 5 s connect timeout
        let t = Instant::now();
        let r = timeout(S(5), b.ep.connect(a.id(), LINK_ALPN)).await;
        m(
            "T4a",
            "connect() inside timeout(5 s)",
            format!(
                "{} after {}",
                if r.is_err() { "Elapsed" } else { "returned" },
                ms(t.elapsed())
            ),
        );
        a.router.shutdown().await?;
        b.router.shutdown().await?;
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T4b: the filter lets the stranger through; the after_handshake hook rejects it.
#[tokio::test(flavor = "multi_thread")]
async fn t4b_hook_rejects_stranger() -> Res {
    timeout(S(60), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        let (mut ca, cb) = pair_cfgs(&towers);
        ca.auth = Arc::new(Authorizer::default());
        let ta = Instant::now();
        let a = node(ca, false).await?; // filter does not enforce
        let tb = Instant::now();
        let b = node(cb, true).await?;
        online_within(&a.ep, ta, S(20)).await.ok_or("a offline")?;
        online_within(&b.ep, tb, S(20)).await.ok_or("b offline")?;
        let t = Instant::now();
        let r = timeout(S(30), b.ep.connect(a.id(), LINK_ALPN)).await?;
        match r {
            Err(e) => m(
                "T4b",
                "connect()",
                format!("Err after {}: {} | {:?}", ms(t.elapsed()), short_err(&e), e),
            ),
            Ok(conn) => {
                m("T4b", "connect()", format!("Ok after {}", ms(t.elapsed())));
                let req = timeout(S(10), request(&conn, b"x")).await;
                m(
                    "T4b",
                    "request on it",
                    format!(
                        "{:?} after {}",
                        req.map(|r| r.map_err(|e| short_err(&e))),
                        ms(t.elapsed())
                    ),
                );
                let reason = timeout(S(10), conn.closed()).await?;
                m(
                    "T4b",
                    "conn.closed()",
                    format!("after {}: {} | {:?}", ms(t.elapsed()), reason, reason),
                );
                if let ConnectionError::ApplicationClosed(ac) = &reason {
                    assert_eq!(ac.error_code, CLOSE_NOT_ALLOWED.into());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        let calls = a.cfg.hook_log.calls();
        m(
            "T4b",
            "callee hook calls",
            format!(
                "{:?}",
                calls
                    .iter()
                    .map(|c| (
                        c.side,
                        String::from_utf8_lossy(&c.alpn).to_string(),
                        c.rejected
                    ))
                    .collect::<Vec<_>>()
            ),
        );
        m(
            "T4b",
            "callee ProtocolHandler::accept calls",
            a.link.stats.accepted.load(Ordering::SeqCst),
        );
        assert_eq!(a.link.stats.accepted.load(Ordering::SeqCst), 0);

        // refusal inside ProtocolHandler::accept, both forms
        for refuse in [Refuse::ErrOnly, Refuse::CloseThenErr] {
            let (cc, cd) = pair_cfgs(&towers);
            let link = Link {
                refuse,
                ..Link::default()
            };
            let tc = Instant::now();
            let c = node_with(cc, true, link).await?;
            let d = node(cd, true).await?;
            online_within(&c.ep, tc, S(20)).await.ok_or("c offline")?;
            online_within(&d.ep, tc, S(20)).await.ok_or("d offline")?;
            let t = Instant::now();
            let conn = timeout(S(10), d.ep.connect(c.id(), LINK_ALPN)).await??;
            let tconn = t.elapsed();
            let req = timeout(S(10), request(&conn, b"x")).await;
            let reason = timeout(S(10), conn.closed()).await?;
            m(
                "T4b",
                &format!("refuse in accept {refuse:?}"),
                format!(
                    "connect Ok after {}; request -> {:?}; closed after {}: {}",
                    ms(tconn),
                    req.map(|r| r.map_err(|e| short_err(&e))).map(|r| r.is_ok()),
                    ms(t.elapsed()),
                    reason
                ),
            );
            c.router.shutdown().await?;
            d.router.shutdown().await?;
        }
        a.router.shutdown().await?;
        b.router.shutdown().await?;
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T4c: the hook runs for outgoing connections; an ALPN-unaware allow-list breaks dials.
#[tokio::test(flavor = "multi_thread")]
async fn t4c_hook_outgoing() -> Res {
    timeout(S(60), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        for alpn_aware in [true, false] {
            // callee C allows D; dialer D allows nobody; D dials C on numbers/join/0
            let (cc, mut cd) = pair_cfgs(&towers);
            cd.auth = Arc::new(Authorizer::default());
            cd.alpn_aware_gate = alpn_aware;
            let (c, d, _, _) = online_pair(cc, cd).await?;
            let t = Instant::now();
            let r = timeout(S(15), d.ep.connect(c.id(), OTHER_ALPN)).await?;
            let outcome = match r {
                Err(e) => format!(
                    "connect Err after {}: {} | {:?}",
                    ms(t.elapsed()),
                    short_err(&e),
                    e
                ),
                Ok(conn) => {
                    let req = timeout(S(5), request(&conn, b"join")).await;
                    let ok = matches!(req, Ok(Ok(_)));
                    let closed = if ok {
                        "-".to_string()
                    } else {
                        format!("{:?}", conn.close_reason())
                    };
                    format!(
                        "connect Ok after {}; request ok={} close_reason={}",
                        ms(t.elapsed()),
                        ok,
                        closed
                    )
                }
            };
            m(
                "T4c",
                &format!("dial on numbers/join/0, dialer gate alpn_aware={alpn_aware}"),
                outcome,
            );
            let calls = d.cfg.hook_log.calls();
            m(
                "T4c",
                &format!("dialer hook calls (alpn_aware={alpn_aware})"),
                format!(
                    "{:?}",
                    calls
                        .iter()
                        .map(|c| (
                            c.side,
                            String::from_utf8_lossy(&c.alpn).to_string(),
                            c.rejected
                        ))
                        .collect::<Vec<_>>()
                ),
            );
            assert!(
                calls.iter().any(|c| c.side == Side::Client),
                "hook ran for the outgoing connection"
            );
            m(
                "T4c",
                &format!("callee accepts (alpn_aware={alpn_aware})"),
                c.link.stats.accepted.load(Ordering::SeqCst),
            );
            c.router.shutdown().await?;
            d.router.shutdown().await?;
        }
        // and on numbers/0 the dialer's own gate refuses an id that is not in its list
        let (cc, mut cd) = pair_cfgs(&towers);
        cd.auth = Arc::new(Authorizer::default());
        let (c, d, _, _) = online_pair(cc, cd).await?;
        let t = Instant::now();
        let r = timeout(S(15), d.ep.connect(c.id(), LINK_ALPN)).await?;
        let s = match r {
            Err(e) => format!("Err after {}: {}", ms(t.elapsed()), short_err(&e)),
            Ok(conn) => format!(
                "Ok after {}; request ok={}",
                ms(t.elapsed()),
                matches!(timeout(S(5), request(&conn, b"x")).await, Ok(Ok(_)))
            ),
        };
        m(
            "T4c",
            "dial on numbers/0 to an id not in the dialer's list (ALPN-aware gate)",
            s,
        );
        c.router.shutdown().await?;
        d.router.shutdown().await?;
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T5: bare-URL relay constructors versus RelayConfig::new(url, None).
#[tokio::test(flavor = "multi_thread")]
async fn t5_bare_url_constructors() -> Res {
    timeout(S(120), async {
        let tower = Tower::spawn(lo0()).await?;
        let url = tower.url().clone();
        let variants = [
            (
                "RelayConfig::new(url, None)",
                Relays::Configs(vec![(url.clone(), None)]),
            ),
            (
                "RelayMode::custom([url])",
                Relays::BareCustom(vec![url.clone()]),
            ),
            ("RelayMap::from(url)", Relays::BareFrom(url.clone())),
        ];
        for (name, relays) in variants {
            for relay_only in [false, true] {
                let mut v = vec![];
                for _ in 0..3 {
                    let mut cfg =
                        EpConfig::new(secret(), relays.clone(), Arc::new(Authorizer::default()));
                    cfg.relay_only = relay_only;
                    let t = Instant::now();
                    let ep = endpoint::bind(&cfg).await?;
                    if let Some(d) = online_within(&ep, t, S(20)).await {
                        v.push(d);
                    } else {
                        m("T5", name, "NOT online in 20 s");
                    }
                    ep.close().await;
                }
                m(
                    "T5",
                    &format!("{name} relay_only={relay_only} bind->online"),
                    stats(&v),
                );
            }
        }
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T6: NetReportConfig variants against the plain-HTTP tower (30 s each, concurrently).
#[tokio::test(flavor = "multi_thread")]
async fn t6_net_report_configs() -> Res {
    timeout(S(60), async {
        let tower = Tower::spawn(lo0()).await?;
        let url = tower.url().clone();
        let minimal = NetReportConfig::minimal();
        let default = NetReportConfig::default();
        let mut no_portal = NetReportConfig::default();
        no_portal.captive_portal_check = false;
        let variants: Vec<(&str, NetReportConfig, Relays, bool)> = vec![
            (
                "minimal(), no QAD",
                minimal.clone(),
                Relays::Configs(vec![(url.clone(), None)]),
                false,
            ),
            (
                "minimal(), no QAD, relay_only",
                minimal.clone(),
                Relays::Configs(vec![(url.clone(), None)]),
                true,
            ),
            (
                "minimal(), bare url (QAD 7842)",
                minimal.clone(),
                Relays::BareCustom(vec![url.clone()]),
                false,
            ),
            (
                "default() (portal check on), no QAD",
                default.clone(),
                Relays::Configs(vec![(url.clone(), None)]),
                false,
            ),
            (
                "default() + captive_portal_check=false, no QAD",
                no_portal.clone(),
                Relays::Configs(vec![(url.clone(), None)]),
                false,
            ),
        ];
        let futs = variants
            .into_iter()
            .map(|(name, nr, relays, relay_only)| async move {
                let mut cfg = EpConfig::new(secret(), relays, Arc::new(Authorizer::default()));
                cfg.net_report = nr;
                cfg.relay_only = relay_only;
                let t = Instant::now();
                let ep = endpoint::bind(&cfg).await?;
                let r = online_within(&ep, t, S(30)).await;
                m(
                    "T6",
                    name,
                    match r {
                        Some(d) => format!("online after {}", ms(d)),
                        None => format!("NOT online after 30 s; status {}", status(&ep)),
                    },
                );
                ep.close().await;
                Res::Ok(())
            });
        for r in n0_future::join_all(futs).await {
            r?;
        }
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T7: empty relay map, insert_relay, tower stop, tower restart on the same port.
#[tokio::test(flavor = "multi_thread")]
async fn t7_insert_relay_and_tower_restart() -> Res {
    timeout(S(150), async {
        let tower = Tower::spawn(lo0()).await?;
        let addr = tower.addr().ok_or("no addr")?;
        let url = tower.url().clone();
        let mut cfg = EpConfig::new(secret(), Relays::Empty, Arc::new(Authorizer::default()));
        cfg.lookup = Some(vec![url.clone()]);
        let t0 = Instant::now();
        let ep = endpoint::bind(&cfg).await?;
        m("T7", "bind() with empty map", ms(t0.elapsed()));
        let (h, log) = watch_status(&ep, t0);
        let early = online_within(&ep, Instant::now(), Duration::from_millis(1000)).await;
        m(
            "T7",
            "online within 1 s with empty map",
            format!("{early:?}"),
        );
        m("T7", "status before insert", status(&ep));
        let ti = Instant::now();
        let prev = ep
            .insert_relay(url.clone(), Arc::new(RelayConfig::new(url.clone(), None)))
            .await;
        m(
            "T7",
            "insert_relay returned",
            format!("{:?} after {}", prev.map(|_| "Some"), ms(ti.elapsed())),
        );
        m("T7", "status right after insert", status(&ep));
        let on = online_within(&ep, ti, S(40)).await;
        m("T7", "insert_relay -> online", format!("{:?}", on.map(ms)));
        m("T7", "status after online", status(&ep));
        on.ok_or("not online after insert_relay")?;

        // stop the tower
        let ts = Instant::now();
        tower.shutdown().await?;
        m("T7", "tower.shutdown()", ms(ts.elapsed()));
        let mut first_change = None;
        while ts.elapsed() < S(40) {
            if !is_connected(&ep) {
                first_change = Some(ts.elapsed());
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        m(
            "T7",
            "tower stopped -> status not connected",
            format!("{:?}", first_change.map(ms)),
        );
        tokio::time::sleep(S(2)).await;
        m("T7", "status 2 s after it noticed", status(&ep));

        // new tower on the same port
        let tr = Instant::now();
        let mut tower2 = None;
        while tr.elapsed() < S(5) {
            match Tower::spawn(addr).await {
                Ok(t) => {
                    tower2 = Some(t);
                    break;
                }
                Err(e) => {
                    m("T7", "restart bind error", short_err(&e));
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        let tower2 = tower2.ok_or("could not rebind the tower port")?;
        m(
            "T7",
            "new tower bound on the same port after",
            ms(tr.elapsed()),
        );
        let tr = Instant::now();
        let mut back = None;
        while tr.elapsed() < S(60) {
            if is_connected(&ep) {
                back = Some(tr.elapsed());
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        m(
            "T7",
            "tower restarted -> status connected again",
            format!("{:?}", back.map(ms)),
        );
        m("T7", "status at the end", status(&ep));
        h.abort();
        print_log("T7", &log);
        ep.close().await;
        tower2.shutdown().await?;
        back.ok_or("did not come back in 60 s")?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T8: started while the tower is down.
#[tokio::test(flavor = "multi_thread")]
async fn t8_tower_down_at_start() -> Res {
    timeout(S(120), async {
        let port = free_tcp_port()?;
        let addr = std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port));
        let url: iroh::RelayUrl = format!("http://{addr}").parse()?;
        let cfg = EpConfig::new(
            secret(),
            Relays::Configs(vec![(url.clone(), None)]),
            Arc::new(Authorizer::default()),
        );
        let t0 = Instant::now();
        let ep = endpoint::bind(&cfg).await?;
        m("T8", "bind() with the tower down", ms(t0.elapsed()));
        let (h, log) = watch_status(&ep, t0);
        let ep2 = ep.clone();
        let online_task = tokio::spawn(async move {
            ep2.online().await;
            Instant::now()
        });
        for at in [100u64, 1000, 3000, 5000, 10000, 20000, 30000] {
            let target = t0 + Duration::from_millis(at);
            tokio::time::sleep_until(target.into()).await;
            m("T8", &format!("status at {at} ms"), status(&ep));
        }
        assert!(!online_task.is_finished());
        let ts = Instant::now();
        let tower = Tower::spawn(addr).await?;
        let done = timeout(S(60), online_task).await;
        match done {
            Ok(Ok(when)) => m(
                "T8",
                "tower started -> online()",
                ms(when.duration_since(ts)),
            ),
            _ => m("T8", "tower started -> online()", "NOT within 60 s"),
        }
        m("T8", "status at the end", status(&ep));
        h.abort();
        print_log("T8", &log);
        ep.close().await;
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T9: shutdown and restart in one process on the same key and UDP port (5 runs).
#[tokio::test(flavor = "multi_thread")]
async fn t9_shutdown_restart_same_port() -> Res {
    timeout(S(120), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        let (mut shut_t, mut peer_t, mut rebind_t, mut redial_t) = (vec![], vec![], vec![], vec![]);
        let mut first_attempt_failed = 0;
        for run in 0..5 {
            let (mut ca, cb) = pair_cfgs(&towers);
            ca.port = free_udp_port()?;
            let (a, b, _, _) = online_pair(ca.clone(), cb).await?;
            let conn = timeout(S(10), b.ep.connect(a.id(), LINK_ALPN)).await??;
            request(&conn, b"x").await?;
            paths::wait_selected_ip(&conn, S(10)).await;
            if run == 0 {
                m("T9", "bound sockets", format!("{:?}", a.ep.bound_sockets()));
                m(
                    "T9",
                    "peer connection paths before shutdown",
                    paths::describe(&conn),
                );
            }
            let conn2 = conn.clone();
            let peer_seen = tokio::spawn(async move { conn2.closed().await });
            let ts = Instant::now();
            a.router.shutdown().await?;
            shut_t.push(ts.elapsed());
            assert!(a.ep.is_closed(), "router.shutdown() closes the endpoint");
            let reason = timeout(S(40), peer_seen).await??;
            peer_t.push(ts.elapsed());
            if run == 0 {
                m("T9", "peer saw", format!("{reason}"));
            }
            // rebind while a clone of the old Endpoint is still alive
            let held = a.ep.clone();
            let Node { ep, router, .. } = a;
            drop(router);
            drop(ep);
            let r = endpoint::bind(&ca).await;
            if run == 0 {
                m(
                    "T9",
                    "rebind same key+port while an Endpoint clone is alive",
                    match &r {
                        Ok(_) => "Ok".to_string(),
                        Err(e) => format!("Err: {}", short_err(e)),
                    },
                );
            }
            assert!(r.is_err());
            drop(held);
            // rebind after every clone is dropped: retry every 5 ms
            let tr = Instant::now();
            let mut failures = 0;
            let ep = loop {
                match endpoint::bind(&ca).await {
                    Ok(ep) => break ep,
                    Err(e) => {
                        failures += 1;
                        if failures == 1 && run == 0 {
                            m(
                                "T9",
                                "first rebind after the last drop failed with",
                                short_err(&e),
                            );
                        }
                        if tr.elapsed() > S(5) {
                            return Err("could not rebind in 5 s".into());
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            };
            if failures > 0 {
                first_attempt_failed += 1;
            }
            rebind_t.push(tr.elapsed());
            m(
                "T9",
                &format!("run {run}"),
                format!(
                    "rebind ok after {} ({failures} failed attempts)",
                    ms(tr.elapsed())
                ),
            );
            let a2 = iroh::protocol::Router::builder(ep.clone())
                .accept(LINK_ALPN, Link::default())
                .spawn();
            online_within(&ep, tr, S(20))
                .await
                .ok_or("rebound endpoint offline")?;
            let t = Instant::now();
            let conn = timeout(S(15), b.ep.connect(ep.id(), LINK_ALPN)).await??;
            request(&conn, b"again").await?;
            redial_t.push(t.elapsed());
            a2.shutdown().await?;
            b.router.shutdown().await?;
        }
        m(
            "T9",
            "router.shutdown() with one open connection",
            stats(&shut_t),
        );
        m("T9", "peer sees the close after", stats(&peer_t));
        m("T9", "last drop -> rebind succeeds", stats(&rebind_t));
        m(
            "T9",
            "runs where the first rebind attempt failed",
            format!("{first_attempt_failed}/5"),
        );
        m("T9", "peer re-dial to the restarted node", stats(&redial_t));
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

/// Data-dir lock (section 10).
#[test]
fn t9b_data_dir_lock() -> Res {
    let dir = tempfile::tempdir()?;
    let l1 = iroh_lab::lock::lock_data_dir(dir.path())?;
    let e = iroh_lab::lock::lock_data_dir(dir.path())
        .err()
        .ok_or("second lock succeeded")?;
    m(
        "T9b",
        "second lock in the same process",
        format!("{:?}: {}", e.kind(), e),
    );
    assert_eq!(e.kind(), std::io::ErrorKind::AddrInUse);
    drop(l1);
    let _l2 = iroh_lab::lock::lock_data_dir(dir.path())?;
    m("T9b", "lock after drop", "Ok");
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// T10: two endpoints with the same secret key on the tower at once.
#[tokio::test(flavor = "multi_thread")]
async fn t10_same_key_twice() -> Res {
    timeout(S(120), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        let (ca, cb) = pair_cfgs(&towers);
        let mut c1 = ca.clone();
        let mut c2 = ca.clone();
        c1.relay_only = true; // relayed only, so the tower decides who gets the dial
        c2.relay_only = true;
        let mut cb = cb;
        cb.relay_only = true;
        let t1 = Instant::now();
        let e1 = node(c1, true).await?;
        online_within(&e1.ep, t1, S(20)).await.ok_or("e1 offline")?;
        let t2 = Instant::now();
        let e2 = node(c2, true).await?;
        online_within(&e2.ep, t2, S(20)).await.ok_or("e2 offline")?;
        let tb = Instant::now();
        let b = node(cb, true).await?;
        online_within(&b.ep, tb, S(20)).await.ok_or("b offline")?;
        tokio::time::sleep(S(1)).await;
        m(
            "T10",
            "status of e1 (older) with e2 (newer) online",
            status(&e1.ep),
        );
        m("T10", "status of e2 (newer)", status(&e2.ep));
        let conn = timeout(S(15), b.ep.connect(e1.id(), LINK_ALPN)).await??;
        let ok = timeout(S(10), request(&conn, b"x")).await;
        m(
            "T10",
            "dial while both online",
            format!(
                "request ok={} accepted e1(older)={} e2(newer)={}",
                matches!(ok, Ok(Ok(_))),
                e1.link.stats.accepted.load(Ordering::SeqCst),
                e2.link.stats.accepted.load(Ordering::SeqCst)
            ),
        );
        conn.close(0u32.into(), b"bye");
        // newer leaves
        let ts = Instant::now();
        e2.router.shutdown().await?;
        m("T10", "e2 router.shutdown()", ms(ts.elapsed()));
        tokio::time::sleep(S(1)).await;
        m("T10", "status of e1 after e2 left", status(&e1.ep));
        let t = Instant::now();
        let r = timeout(S(40), async {
            let conn = b.ep.connect(e1.id(), LINK_ALPN).await?;
            request(&conn, b"y").await?;
            Res::Ok(())
        })
        .await;
        m(
            "T10",
            "re-dial after the newer left",
            format!(
                "{:?} after {}; accepted e1={} e2={}",
                r.map(|x| x.map_err(|e| short_err(&e))),
                ms(t.elapsed()),
                e1.link.stats.accepted.load(Ordering::SeqCst),
                e2.link.stats.accepted.load(Ordering::SeqCst)
            ),
        );
        e1.router.shutdown().await?;
        b.router.shutdown().await?;
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T11: dial an id that is simply offline.
#[tokio::test(flavor = "multi_thread")]
async fn t11_dial_offline() -> Res {
    timeout(S(150), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        for relay_only in [false, true] {
            let offline = id_of(&secret());
            let mut cfg = EpConfig::new(
                secret(),
                Relays::Configs(towers.clone()),
                Arc::new(Authorizer::new([offline])),
            );
            cfg.relay_only = relay_only;
            let t = Instant::now();
            let b = node(cfg, true).await?;
            online_within(&b.ep, t, S(20)).await.ok_or("offline")?;
            let t = Instant::now();
            let r = timeout(S(120), b.ep.connect(offline, LINK_ALPN)).await;
            m(
                "T11",
                &format!("connect() to an offline id, relay_only={relay_only}"),
                match r {
                    Err(_) => format!("still pending after {}", ms(t.elapsed())),
                    Ok(Ok(_)) => "UNEXPECTED Ok".to_string(),
                    Ok(Err(e)) => {
                        format!("Err after {}: {} | {:?}", ms(t.elapsed()), short_err(&e), e)
                    }
                },
            );
            b.router.shutdown().await?;
        }
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T12: a peer killed without closing; and a healthy idle connection for 45 s.
#[tokio::test(flavor = "multi_thread")]
async fn t12_peer_killed() -> Res {
    timeout(S(120), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        let url = tower.url().to_string();
        // (a)-(b): callee in a child process, SIGKILLed; relay-only and direct
        // (c)-(d): callee on its own runtime, dropped with shutdown_background(); relay-only and direct
        // (e): healthy idle connection, no traffic for 45 s
        let mut futs: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = Res> + Send>>> =
            vec![];
        for relay_only in [true, false] {
            let towers1 = towers.clone();
            let url = url.clone();
            futs.push(Box::pin(async move {
                let towers = towers1;
                let tag = format!("child SIGKILL, relay_only={relay_only}");
                let (kc, kb) = (secret(), secret());
                let mut cb = EpConfig::new(
                    kb,
                    Relays::Configs(towers),
                    Arc::new(Authorizer::new([id_of(&kc)])),
                );
                cb.relay_only = relay_only;
                let b = node(cb, true).await?;
                let exe = std::env::current_exe()?;
                let mut child = std::process::Command::new(exe)
                    .args([
                        "--exact",
                        "child_callee",
                        "--ignored",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env("LAB_CHILD_TOWER", &url)
                    .env("LAB_CHILD_KEY", hex(&kc))
                    .env("LAB_CHILD_ALLOW", hex(&kb))
                    .env("LAB_CHILD_RELAY_ONLY", if relay_only { "1" } else { "0" })
                    .stdout(std::process::Stdio::null())
                    .spawn()?;
                b.ep.online().await;
                // wait until the child is reachable
                let t = Instant::now();
                let conn = loop {
                    match timeout(S(3), b.ep.connect(id_of(&kc), LINK_ALPN)).await {
                        Ok(Ok(c)) => break c,
                        _ if t.elapsed() < S(30) => continue,
                        _ => return Err("child not reachable".into()),
                    }
                };
                request(&conn, b"x").await?;
                if !relay_only {
                    paths::wait_selected_ip(&conn, S(10)).await;
                }
                let before = paths::describe(&conn);
                let tk = Instant::now();
                child.kill()?;
                child.wait()?;
                let reason = timeout(S(90), conn.closed()).await?;
                m(
                    "T12",
                    &tag,
                    format!(
                        "closed after {}: {} (paths before kill: {})",
                        ms(tk.elapsed()),
                        reason,
                        before
                    ),
                );
                b.router.shutdown().await?;
                Res::Ok(())
            }));
            let towers = towers.clone();
            futs.push(Box::pin(async move {
                let tag = format!("runtime shutdown_background, relay_only={relay_only}");
                let (mut ca, mut cb) = pair_cfgs(&towers);
                ca.relay_only = relay_only;
                cb.relay_only = relay_only;
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()?;
                let (tx, rx) = tokio::sync::oneshot::channel();
                let ca2 = ca.clone();
                rt.spawn(async move {
                    let n = node(ca2, true).await;
                    let ok = n.is_ok();
                    let _ = tx.send(ok);
                    let _keep = n;
                    std::future::pending::<()>().await;
                });
                if !rx.await? {
                    return Err("callee failed".into());
                }
                let b = node(cb, true).await?;
                b.ep.online().await;
                let t = Instant::now();
                let conn = loop {
                    match timeout(S(3), b.ep.connect(id_of(&ca.secret), LINK_ALPN)).await {
                        Ok(Ok(c)) => break c,
                        _ if t.elapsed() < S(30) => continue,
                        _ => return Err("callee not reachable".into()),
                    }
                };
                request(&conn, b"x").await?;
                if !relay_only {
                    paths::wait_selected_ip(&conn, S(10)).await;
                }
                let tk = Instant::now();
                rt.shutdown_background();
                let reason = timeout(S(90), conn.closed()).await?;
                m(
                    "T12",
                    &tag,
                    format!("closed after {}: {}", ms(tk.elapsed()), reason),
                );
                b.router.shutdown().await?;
                Res::Ok(())
            }));
        }
        let towers2 = towers.clone();
        futs.push(Box::pin(async move {
            let (ca, cb) = pair_cfgs(&towers2);
            let (a, b, _, _) = online_pair(ca, cb).await?;
            let conn = timeout(S(10), b.ep.connect(a.id(), LINK_ALPN)).await??;
            request(&conn, b"x").await?;
            tokio::time::sleep(S(45)).await;
            let alive = conn.close_reason().is_none();
            let ok = matches!(timeout(S(5), request(&conn, b"y")).await, Ok(Ok(_)));
            m(
                "T12",
                "healthy connection idle 45 s",
                format!("still open={alive} request ok={ok}"),
            );
            a.router.shutdown().await?;
            b.router.shutdown().await?;
            Res::Ok(())
        }));
        for r in n0_future::join_all(futs).await {
            r?;
        }
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

fn hex(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Res<[u8; 32]> {
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(2 * i..2 * i + 2).ok_or("short")?, 16)?;
    }
    Ok(out)
}

/// The callee process for T12; runs only when started by it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "child process for t12"]
async fn child_callee() -> Res {
    let Ok(url) = std::env::var("LAB_CHILD_TOWER") else {
        return Ok(());
    };
    let key = unhex(&std::env::var("LAB_CHILD_KEY")?)?;
    let allow = unhex(&std::env::var("LAB_CHILD_ALLOW")?)?;
    let mut cfg = EpConfig::new(
        key,
        Relays::Configs(vec![(url.parse()?, None)]),
        Arc::new(Authorizer::new([id_of(&allow)])),
    );
    cfg.relay_only = std::env::var("LAB_CHILD_RELAY_ONLY").as_deref() == Ok("1");
    let _n = node(cfg, true).await?;
    tokio::time::sleep(S(100)).await; // killed long before
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// T14 (extra): towers in the lookup that are not in the dialer's own map.
#[tokio::test(flavor = "multi_thread")]
async fn t14_tower_not_in_dialer_map() -> Res {
    timeout(S(60), async {
        let ta = Tower::spawn(lo0()).await?;
        let tb = Tower::spawn(lo0()).await?;
        let dead: iroh::RelayUrl = format!("http://127.0.0.1:{}", free_tcp_port()?).parse()?;
        let (mut ca, mut cb) = pair_cfgs(&[(ta.url().clone(), None)]);
        ca.relay_only = true;
        cb.relay_only = true;
        cb.relays = Relays::Configs(vec![(tb.url().clone(), None)]);
        // the dialer's lookup lists a dead tower, its own tower and the callee's tower
        cb.lookup = Some(vec![dead.clone(), tb.url().clone(), ta.url().clone()]);
        let (a, b, _, _) = online_pair(ca, cb).await?;
        let t = Instant::now();
        let r = timeout(S(20), async {
            let conn = b.ep.connect(a.id(), LINK_ALPN).await?;
            request(&conn, b"x").await?;
            Res::Ok(conn)
        })
        .await;
        match r {
            Ok(Ok(conn)) => m(
                "T14",
                "callee on tower A, dialer homed on tower B, lookup [dead, B, A]",
                format!(
                    "Ok after {}; paths {}",
                    ms(t.elapsed()),
                    paths::describe(&conn)
                ),
            ),
            Ok(Err(e)) => m(
                "T14",
                "callee on A, dialer on B",
                format!("Err after {}: {}", ms(t.elapsed()), short_err(&e)),
            ),
            Err(_) => m("T14", "callee on A, dialer on B", "timeout 20 s"),
        }
        m("T14", "dialer status", status(&b.ep));
        a.router.shutdown().await?;
        b.router.shutdown().await?;
        ta.shutdown().await?;
        tb.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T7b: a long tower outage (35 s): how long after the restart until the endpoint is back?
// One endpoint waits; the other is nudged with remove_relay + insert_relay right after the restart.
#[tokio::test(flavor = "multi_thread")]
async fn t7b_long_tower_outage() -> Res {
    timeout(S(120), async {
        let tower = Tower::spawn(lo0()).await?;
        let addr = tower.addr().ok_or("no addr")?;
        let url = tower.url().clone();
        let t0 = Instant::now();
        let mut eps = vec![];
        for _ in 0..2 {
            let cfg = EpConfig::new(secret(), Relays::Configs(vec![(url.clone(), None)]), Arc::new(Authorizer::default()));
            let ep = endpoint::bind(&cfg).await?;
            online_within(&ep, t0, S(20)).await.ok_or("offline")?;
            eps.push(ep);
        }
        let (h, log) = watch_status(&eps[0], t0);
        tower.shutdown().await?;
        tokio::time::sleep(S(35)).await;
        m("T7b", "status after 35 s outage", status(&eps[0]));
        let tr = Instant::now();
        let tower2 = Tower::spawn(addr).await?;
        let nudged = eps[1].clone();
        let u = url.clone();
        let nudge = async move {
            if let Some(c) = nudged.remove_relay(&u).await {
                nudged.insert_relay(u, c).await;
            }
            nudged.online().await;
            tr.elapsed()
        };
        let passive = async {
            while tr.elapsed() < S(60) {
                if is_connected(&eps[0]) {
                    return Some(tr.elapsed());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            None
        };
        let (back, nudged_back) = tokio::join!(passive, timeout(S(60), nudge));
        m("T7b", "35 s outage: tower restarted -> connected again (waiting)", format!("{:?}", back.map(ms)));
        m("T7b", "35 s outage: tower restarted -> online (remove_relay + insert_relay)", format!("{:?}", nudged_back.ok().map(ms)));
        h.abort();
        if let Ok(g) = log.lock() {
            let times: Vec<String> = g.iter().filter(|(_, s)| s.contains("last_error=-")).map(|(t, _)| format!("{:.1}", t.as_secs_f64())).collect();
            m("T7b", "connection attempts of the waiting endpoint (s since bind; outage from ~0.2 s, restart at ~35.2 s)", times.join(" "));
        }
        for ep in eps {
            ep.close().await;
        }
        tower2.shutdown().await?;
        back.ok_or("not back in 60 s")?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T8b: tower down at start, started 3 s later; can the station speed up its first home tower?
#[tokio::test(flavor = "multi_thread")]
async fn t8b_tower_down_at_start_nudges() -> Res {
    timeout(S(90), async {
        let futs = ["nothing", "network_change()", "remove_relay + insert_relay"].map(
            |nudge| async move {
                let port = free_tcp_port()?;
                let addr = std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port));
                let url: iroh::RelayUrl = format!("http://{addr}").parse()?;
                let cfg = EpConfig::new(
                    secret(),
                    Relays::Configs(vec![(url.clone(), None)]),
                    Arc::new(Authorizer::default()),
                );
                let ep = endpoint::bind(&cfg).await?;
                tokio::time::sleep(S(3)).await;
                let tower = Tower::spawn(addr).await?;
                let t = Instant::now();
                match nudge {
                    "network_change()" => ep.network_change().await,
                    "remove_relay + insert_relay" => {
                        let c = ep.remove_relay(&url).await.ok_or("not in map")?;
                        ep.insert_relay(url.clone(), c).await;
                    }
                    _ => {}
                }
                let on = online_within(&ep, t, S(60)).await;
                m(
                    "T8b",
                    &format!("tower up 3 s after bind, nudge: {nudge}"),
                    format!("online after {:?}", on.map(ms)),
                );
                ep.close().await;
                tower.shutdown().await?;
                Res::Ok(())
            },
        );
        for r in n0_future::join_all(futs).await {
            r?;
        }
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T1b: 20 fresh pairs per mode, to see the tail of connect() and the first reply.
#[tokio::test(flavor = "multi_thread")]
async fn t1b_connect_tail() -> Res {
    timeout(S(120), async {
        let tower = Tower::spawn(lo0()).await?;
        let towers = vec![(tower.url().clone(), None)];
        for relay_only in [true, false] {
            let (mut conn_t, mut reply_t) = (vec![], vec![]);
            for _ in 0..20 {
                let (mut ca, mut cb) = pair_cfgs(&towers);
                ca.relay_only = relay_only;
                cb.relay_only = relay_only;
                let (a, b, _, _) = online_pair(ca, cb).await?;
                let t = Instant::now();
                let conn = timeout(S(10), b.ep.connect(a.id(), LINK_ALPN)).await??;
                conn_t.push(t.elapsed());
                timeout(S(10), request(&conn, b"x")).await??;
                reply_t.push(t.elapsed());
                conn.close(0u32.into(), b"bye");
                a.router.shutdown().await?;
                b.router.shutdown().await?;
            }
            let slow = conn_t
                .iter()
                .filter(|d| **d > Duration::from_millis(500))
                .count();
            m(
                "T1b",
                &format!("relay_only={relay_only} connect()"),
                format!("{} ; >500 ms: {slow}/20", stats(&conn_t)),
            );
            m(
                "T1b",
                &format!("relay_only={relay_only} connect()->first reply"),
                stats(&reply_t),
            );
        }
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T7c: does Server::shutdown() always drop a client that has just come online?
// 10 runs per variant: relay map given at bind, or empty map + insert_relay (as in T7);
// the tower is shut down right after online(), or 500 ms after.
#[tokio::test(flavor = "multi_thread")]
async fn t7c_tower_shutdown_race() -> Res {
    timeout(S(120), async {
        for inserted in [false, true] {
            for delay_ms in [0u64, 500] {
                let futs = (0..10).map(|_| async move {
                    let tower = Tower::spawn(lo0()).await?;
                    let url = tower.url().clone();
                    let relays = if inserted { Relays::Empty } else { Relays::Configs(vec![(url.clone(), None)]) };
                    let cfg = EpConfig::new(secret(), relays, Arc::new(Authorizer::default()));
                    let ep = endpoint::bind(&cfg).await?;
                    if inserted {
                        ep.insert_relay(url.clone(), Arc::new(RelayConfig::new(url, None))).await;
                    }
                    let t = Instant::now();
                    let on = online_within(&ep, t, S(40)).await.ok_or("offline after 40 s")?;
                    if on > S(5) {
                        m("T7c", "SLOW online() under load", ms(on));
                    }
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    tower.shutdown().await?;
                    tokio::time::sleep(S(3)).await;
                    let zombie = is_connected(&ep);
                    ep.close().await;
                    Res::Ok(zombie)
                });
                let mut zombies = 0;
                for r in n0_future::join_all(futs).await {
                    if r? {
                        zombies += 1;
                    }
                }
                m("T7c", &format!("insert_relay={inserted}, tower.shutdown() {delay_ms} ms after online(): still 'connected' 3 s later"), format!("{zombies}/10"));
            }
        }
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T7d: empty map, insert_relay right after bind() (as Station::start would): time to online,
// 20 runs per delay, all concurrently.
#[tokio::test(flavor = "multi_thread")]
async fn t7d_insert_right_after_bind() -> Res {
    timeout(S(60), async {
        let tower = Tower::spawn(lo0()).await?;
        let url = tower.url().clone();
        for delay_ms in [0u64, 50, 300] {
            let futs = (0..20).map(|_| {
                let url = url.clone();
                async move {
                    let cfg =
                        EpConfig::new(secret(), Relays::Empty, Arc::new(Authorizer::default()));
                    let ep = endpoint::bind(&cfg).await?;
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    let t = Instant::now();
                    ep.insert_relay(url.clone(), Arc::new(RelayConfig::new(url, None)))
                        .await;
                    let r = online_within(&ep, t, S(40)).await;
                    ep.close().await;
                    Res::Ok(r)
                }
            });
            let (mut v, mut missed) = (vec![], 0);
            for r in n0_future::join_all(futs).await {
                match r? {
                    Some(d) => v.push(d),
                    None => missed += 1,
                }
            }
            m(
                "T7d",
                &format!("insert_relay {delay_ms} ms after bind -> online"),
                format!("{} ; not online in 40 s: {missed}", stats(&v)),
            );
        }
        tower.shutdown().await?;
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T7e: T7's exact sequence 10 times concurrently: tower, endpoint with an empty map, 1 s wait,
// insert_relay, online, tower.shutdown() at once; is the endpoint still "connected" 3 s later?
#[tokio::test(flavor = "multi_thread")]
async fn t7e_t7_sequence_race() -> Res {
    timeout(S(90), async {
        for (wait_ms, watch) in [(1000u64, true), (1000, false), (0, true)] {
            let futs = (0..10).map(|_| async move {
                let tower = Tower::spawn(lo0()).await?;
                let url = tower.url().clone();
                let cfg = EpConfig::new(secret(), Relays::Empty, Arc::new(Authorizer::default()));
                let t0 = Instant::now();
                let ep = endpoint::bind(&cfg).await?;
                let w = watch.then(|| watch_status(&ep, t0));
                tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                ep.insert_relay(url.clone(), Arc::new(RelayConfig::new(url, None))).await;
                online_within(&ep, Instant::now(), S(40)).await.ok_or("offline")?;
                tower.shutdown().await?;
                tokio::time::sleep(S(3)).await;
                let zombie = is_connected(&ep);
                if let Some((h, _)) = w {
                    h.abort();
                }
                ep.close().await;
                Res::Ok(zombie)
            });
            let mut zombies = 0;
            for r in n0_future::join_all(futs).await {
                if r? {
                    zombies += 1;
                }
            }
            m("T7e", &format!("wait {wait_ms} ms before insert_relay, status watcher={watch}: still 'connected' 3 s after tower.shutdown()"), format!("{zombies}/10"));
        }
        Res::Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------------------------
// T7f: ways to stop a test tower without leaving a zombie client connection:
// (a) Server::shutdown() 1 s after the client came online; (b) the tower on its own runtime,
// stopped with Runtime::shutdown_background() (kills the detached connection tasks too).
#[tokio::test(flavor = "multi_thread")]
async fn t7f_stopping_a_test_tower() -> Res {
    timeout(S(120), async {
        for own_runtime in [false, true] {
            let mut zombies = 0;
            for _ in 0..3 {
                let futs = (0..10).map(|_| async move {
                    let (tower, rt) = if own_runtime {
                        let rt = tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(1)
                            .enable_all()
                            .build()?;
                        let t = rt.spawn(Tower::spawn(lo0())).await??;
                        (t, Some(rt))
                    } else {
                        (Tower::spawn(lo0()).await?, None)
                    };
                    let url = tower.url().clone();
                    let cfg =
                        EpConfig::new(secret(), Relays::Empty, Arc::new(Authorizer::default()));
                    let ep = endpoint::bind(&cfg).await?;
                    ep.insert_relay(url.clone(), Arc::new(RelayConfig::new(url, None)))
                        .await;
                    online_within(&ep, Instant::now(), S(40))
                        .await
                        .ok_or("offline")?;
                    match rt {
                        Some(rt) => {
                            rt.shutdown_background();
                            drop(tower);
                        }
                        None => {
                            tokio::time::sleep(S(1)).await;
                            tower.shutdown().await?;
                        }
                    }
                    tokio::time::sleep(S(3)).await;
                    let zombie = is_connected(&ep);
                    ep.close().await;
                    Res::Ok(zombie)
                });
                for r in n0_future::join_all(futs).await {
                    if r? {
                        zombies += 1;
                    }
                }
            }
            let how = if own_runtime {
                "own runtime + shutdown_background()"
            } else {
                "Server::shutdown() 1 s after online"
            };
            m(
                "T7f",
                how,
                format!("still 'connected' 3 s later: {zombies}/30"),
            );
        }
        Res::Ok(())
    })
    .await?
}
