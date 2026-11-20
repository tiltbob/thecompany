#![cfg(test)]
//! `numbers::bootstrap`: the SRV name, the canonical tower URLs, and `DnsLocator` against the fake
//! DNS server of `numbers::testing`. Everything runs on 127.0.0.1; only names under the reserved
//! `example.test` are used.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use numbers::bootstrap::{DEFAULT_LOCATE_TIMEOUT, SRV_SERVICE, srv_name, towers_from_srv};
use numbers::testing::FakeDns;
use numbers::{
    DnsConfig, DnsLocator, LocateError, LocateFuture, Locator, MAX_TOWERS, Qad, TowerEntry,
    TowerSet, canonical_tower_url,
};
use tokio::time::timeout;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The resolver gives up on a silent nameserver after 6 s, a constant in n0-dns-resolver.
const SILENT_TEST_TIMEOUT: Duration = Duration::from_secs(15);

const DOMAIN: &str = "example.test";
const SRV: &str = "_numbers._tcp.example.test";

async fn bounded<T>(limit: Duration, fut: impl Future<Output = T>) -> T {
    timeout(limit, fut).await.expect("test timed out")
}

fn pairs(list: &[(&str, u16)]) -> Vec<(String, u16)> {
    list.iter().map(|(t, p)| ((*t).to_owned(), *p)).collect()
}

/// The URLs of `towers`, in order.
fn urls(towers: &[TowerEntry]) -> Vec<&str> {
    towers.iter().map(TowerEntry::url).collect()
}

async fn server() -> (FakeDns, DnsLocator) {
    let dns = FakeDns::start().await.expect("bind the fake DNS server");
    let locator = DnsLocator::with_nameserver(dns.addr());
    (dns, locator)
}

/// Asserts that the server was asked only SRV queries for the SRV name of `example.test`.
fn assert_only_srv_queries(dns: &FakeDns) {
    let queries = dns.queries();
    assert!(!queries.is_empty());
    for q in &queries {
        assert_eq!((q.name.as_str(), q.qtype.as_str()), (SRV, "SRV"));
    }
}

// Pure functions.

#[test]
fn srv_name_with_and_without_trailing_dot() {
    assert_eq!(SRV_SERVICE, "_numbers._tcp");
    assert_eq!(
        srv_name("example.test").unwrap(),
        "_numbers._tcp.example.test."
    );
    assert_eq!(
        srv_name("example.test.").unwrap(),
        "_numbers._tcp.example.test."
    );
    assert_eq!(
        srv_name("Team-1.Example.TEST").unwrap(),
        "_numbers._tcp.team-1.example.test."
    );
    assert_eq!(srv_name("test").unwrap(), "_numbers._tcp.test.");
}

#[test]
fn srv_name_refuses_what_is_not_a_domain() {
    let long_label = format!("{}.test", "a".repeat(64));
    let long_name = format!("{}test", "abcdefghi.".repeat(25));
    for bad in [
        "",
        ".",
        "..",
        "example.test..",
        ".example.test",
        "example..test",
        "https://example.test",
        "example.test/path",
        "example.test:443",
        "example .test",
        " example.test",
        "example.test\n",
        "\texample.test",
        "_numbers._tcp.example.test",
        "user@example.test",
        "exa*mple.test",
        "bücher.example.test",
        "-team.example.test",
        "team-.example.test",
        "example.-test",
        "exa;mple.test",
        &long_label,
        &long_name,
    ] {
        assert!(
            matches!(srv_name(bad), Err(LocateError::InvalidDomain)),
            "accepted {bad:?}"
        );
    }
}

/// A domain of exactly `len` characters under `example.test`, in labels of at most 63.
fn domain_of_len(len: usize) -> String {
    let suffix = ".example.test";
    let mut rest = len - suffix.len();
    let mut labels = Vec::new();
    while rest > 0 {
        // A label of n characters and its dot take n + 1; the last one has no dot.
        let take = rest.min(63);
        let take = if rest - take == 1 { take - 1 } else { take };
        labels.push("a".repeat(take));
        rest -= take;
        rest = rest.saturating_sub(1); // the dot before the next label
    }
    let domain = format!("{}{suffix}", labels.join("."));
    assert_eq!(domain.len(), len);
    domain
}

#[test]
fn srv_name_length_boundary() {
    // 253 characters without the final dot, less the 14 of `_numbers._tcp.`, leaves 239.
    assert_eq!(SRV_SERVICE.len() + 1, 14);
    let longest = domain_of_len(239);
    let name = srv_name(&longest).unwrap();
    assert_eq!(name.len(), 254); // 253 and the final dot
    assert_eq!(srv_name(&format!("{longest}.")).unwrap(), name);
    let too_long = domain_of_len(240);
    assert!(matches!(
        srv_name(&too_long),
        Err(LocateError::InvalidDomain)
    ));
    assert!(matches!(
        srv_name(&format!("{too_long}.")),
        Err(LocateError::InvalidDomain)
    ));
    // The 63-character label rule still holds inside a short name.
    assert!(srv_name(&format!("{}.test", "a".repeat(63))).is_ok());
    assert!(srv_name(&format!("{}.test", "a".repeat(64))).is_err());
}

#[tokio::test]
async fn locate_longest_domain_through_the_resolver() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        let longest = domain_of_len(239);
        let srv = format!("_numbers._tcp.{longest}");
        dns.set_srv(&srv, &[("a.example.test", 443)]);
        assert_eq!(
            urls(&locator.locate(&longest).await.unwrap()),
            ["https://a.example.test/"]
        );
        assert_eq!(dns.query_count_for(&srv, "SRV"), 1);
        assert_eq!(dns.query_count(), 1);
    })
    .await;
}

/// `n` distinct targets `t0.example.test` and on, all on port 443.
fn targets(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("t{i}.example.test")).collect()
}

#[tokio::test]
async fn locate_accepts_eight_towers() {
    bounded(TEST_TIMEOUT, async {
        assert_eq!(MAX_TOWERS, 8);
        let (dns, locator) = server().await;
        let names = targets(8);
        let records: Vec<(&str, u16)> = names.iter().map(|t| (t.as_str(), 443)).collect();
        dns.set_srv(SRV, &records);
        assert_eq!(locator.locate(DOMAIN).await.unwrap().len(), 8);
    })
    .await;
}

#[tokio::test]
async fn locate_refuses_nine_towers() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        let names = targets(9);
        let records: Vec<(&str, u16)> = names.iter().map(|t| (t.as_str(), 443)).collect();
        dns.set_srv(SRV, &records);
        let err = locator.locate(DOMAIN).await.unwrap_err();
        assert!(matches!(err, LocateError::TooManyTowers), "{err:?}");
    })
    .await;
}

#[tokio::test]
async fn locate_counts_towers_after_removing_duplicates_and_unusable() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        let names = targets(8);
        // 13 records: the 8 towers, then a duplicate, two spellings of a duplicate, the root
        // target and a target that is not a host name. They come down to the 8.
        let mut records: Vec<(&str, u16)> = names.iter().map(|t| (t.as_str(), 443)).collect();
        records.extend([
            ("t0.example.test", 443),
            ("T1.EXAMPLE.TEST.", 443),
            ("t2.example.test.", 443),
            (".", 443),
            ("-bad.example.test", 443),
        ]);
        dns.set_srv(SRV, &records);
        let towers = locator.locate(DOMAIN).await.unwrap();
        assert_eq!(towers.len(), 8);
        assert_eq!(towers[0].url(), "https://t0.example.test/");
        // What the station installs: `TowerSet` takes the answer as it is.
        assert_eq!(TowerSet::from_entries(towers).unwrap().len(), 8);
    })
    .await;
}

#[test]
fn towers_from_srv_two_targets() {
    let towers = towers_from_srv(pairs(&[("b.example.test", 3340), ("a.example.test", 443)]));
    assert_eq!(
        urls(&towers),
        ["https://a.example.test/", "https://b.example.test:3340/"]
    );
}

#[test]
fn towers_from_srv_canonical_form() {
    let towers = towers_from_srv(pairs(&[
        ("c.example.test", 8443),
        ("A.Example.TEST.", 443),
        ("a.example.test", 443),
        ("a.example.test.", 443),
        ("a.example.test", 8443),
        ("C.EXAMPLE.TEST", 8443),
        (".", 443),
        ("", 443),
        ("b.example.test", 0),
        ("b.example.test/x", 443),
        ("b.example.test..", 443),
        ("-b.example.test", 443),
        ("b-.example.test", 443),
        ("b_b.example.test", 443),
        ("b;b.example.test", 443),
    ]));
    let found = urls(&towers);
    assert_eq!(
        found,
        [
            "https://a.example.test/",
            "https://a.example.test:8443/",
            "https://c.example.test:8443/",
        ]
    );
    let mut sorted = found.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(found, sorted);
    for url in &found {
        assert_eq!(canonical_tower_url(url).unwrap(), *url);
        assert!(!url.contains(".test."), "trailing dot in {url}");
    }
    assert!(TowerSet::from_entries(towers).is_ok());
}

// The URL rule is `canonical_tower_url`'s: a tower from SRV and the same host and port typed by a
// user are the same string (issue 67).
#[test]
fn towers_from_srv_matches_what_a_user_types() {
    for (target, port, typed) in [
        ("h.example.test.", 443, "https://h.example.test"),
        ("H.Example.Test", 443, "https://h.example.test:443/"),
        ("h.example.test.", 8443, "HTTPS://H.EXAMPLE.TEST.:8443"),
    ] {
        let towers = towers_from_srv(pairs(&[(target, port)]));
        assert_eq!(urls(&towers), [canonical_tower_url(typed).unwrap()]);
        assert_eq!(towers[0], TowerEntry::from_url(typed).unwrap());
    }
}

// SRV carries no QAD port: a located tower gets the HTTPS default of `TowerEntry::from_url`.
#[test]
fn towers_from_srv_uses_the_default_qad_port() {
    let towers = towers_from_srv(pairs(&[("a.example.test", 443), ("b.example.test", 3340)]));
    assert_eq!(towers.len(), 2);
    for tower in &towers {
        assert_eq!(tower.qad(), Qad::Port(7842));
    }
}

#[test]
fn towers_from_srv_one_tower_per_canonical_url() {
    let towers = towers_from_srv(pairs(&[
        ("a.example.test", 443),
        ("A.EXAMPLE.TEST.", 443),
        ("a.example.test.", 443),
    ]));
    assert_eq!(urls(&towers), ["https://a.example.test/"]);
}

// The bound is the locator's, not this function's: nine distinct targets stay nine here.
#[test]
fn towers_from_srv_does_not_cut() {
    let names = targets(9);
    let records: Vec<(&str, u16)> = names.iter().map(|t| (t.as_str(), 443)).collect();
    assert_eq!(towers_from_srv(pairs(&records)).len(), 9);
}

// What is not a DNS name is skipped, also where `canonical_tower_url` would accept it for a typed
// URL: an IP address, an international name, a percent-encoded name.
#[test]
fn towers_from_srv_skips_what_is_not_a_dns_name() {
    let long_label = format!("{}.example.test", "a".repeat(64));
    let towers = towers_from_srv(pairs(&[
        ("192.0.2.1", 443),
        ("192.0.2.1.", 443),
        ("127.0.0.1", 3340),
        ("[::1]", 443),
        ("123", 443),
        ("0x7f.1", 443),
        ("1.2.3.04", 443),
        ("a.1", 443),
        ("bücher.example.test", 443),
        ("xn--bcher-kva.example.test", 443),
        ("a%62.example.test", 443),
        ("a@b.example.test", 443),
        ("a:1.example.test", 443),
        ("a?.example.test", 443),
        ("a#.example.test", 443),
        ("a\\b.example.test", 443),
        ("a b.example.test", 443),
        ("a\tb.example.test", 443),
        ("example..test", 443),
        (&long_label, 443),
    ]));
    // Only the name that is already ASCII remains.
    assert_eq!(urls(&towers), ["https://xn--bcher-kva.example.test/"]);
}

#[test]
fn towers_from_srv_skips_unusable_targets() {
    assert!(towers_from_srv(pairs(&[(".", 443), ("", 3340)])).is_empty());
    assert!(towers_from_srv(Vec::new()).is_empty());
}

// DnsLocator against the fake server.

#[tokio::test]
async fn locate_two_targets() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        dns.set_srv(SRV, &[("b.example.test.", 3340), ("A.example.test", 443)]);
        let towers = locator.locate(DOMAIN).await.unwrap();
        assert_eq!(
            urls(&towers),
            ["https://a.example.test/", "https://b.example.test:3340/"]
        );
        assert_only_srv_queries(&dns);
        assert_eq!(dns.query_count_for("_numbers._tcp.example.test.", "SRV"), 1);
        assert_eq!(dns.query_count(), 1);
    })
    .await;
}

#[tokio::test]
async fn locate_nxdomain_is_no_tower() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        let err = locator.locate(DOMAIN).await.unwrap_err();
        assert!(matches!(err, LocateError::NoTower), "{err:?}");
        assert_only_srv_queries(&dns);
    })
    .await;
}

#[tokio::test]
async fn locate_empty_answer_is_no_tower() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        dns.set_empty(SRV);
        let err = locator.locate(DOMAIN).await.unwrap_err();
        assert!(matches!(err, LocateError::NoTower), "{err:?}");
        assert_only_srv_queries(&dns);
    })
    .await;
}

#[tokio::test]
async fn locate_root_target_is_no_tower() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        dns.set_srv(SRV, &[(".", 443)]);
        let err = locator.locate(DOMAIN).await.unwrap_err();
        assert!(matches!(err, LocateError::NoTower), "{err:?}");
        assert_only_srv_queries(&dns);
    })
    .await;
}

#[tokio::test]
async fn locate_asks_again_after_the_answer_changed() {
    // The fake's records have a TTL of 300 s, so without `clear_cache()` in `locate` the second
    // and third lookups would be answered from the resolver's cache and this test would fail.
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        dns.set_srv(SRV, &[("a.example.test", 443)]);
        assert_eq!(
            urls(&locator.locate(DOMAIN).await.unwrap()),
            ["https://a.example.test/"]
        );
        dns.set_srv(SRV, &[("b.example.test", 443), ("c.example.test", 443)]);
        assert_eq!(
            urls(&locator.locate(DOMAIN).await.unwrap()),
            ["https://b.example.test/", "https://c.example.test/"]
        );
        dns.remove(SRV);
        let err = locator.locate(DOMAIN).await.unwrap_err();
        assert!(matches!(err, LocateError::NoTower), "{err:?}");
        assert_eq!(dns.query_count(), 3);
        assert_only_srv_queries(&dns);
    })
    .await;
}

#[tokio::test]
async fn locate_asks_nothing_but_the_srv_name() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        dns.set_srv(SRV, &[("a.example.test", 3340)]);
        dns.set_a("a.example.test");
        dns.set_srv(
            "_numbers._tcp.other.example.test",
            &[("x.example.test", 443)],
        );
        locator.locate(DOMAIN).await.unwrap();
        // Also with the trailing dot and in another case: the same single name is asked.
        locator.locate("EXAMPLE.test.").await.unwrap();
        assert_eq!(dns.query_count(), 2);
        assert_eq!(dns.query_count_for("a.example.test", "A"), 0);
        assert_eq!(
            dns.query_count_for("_numbers._tcp.other.example.test", "SRV"),
            0
        );
        assert_only_srv_queries(&dns);
    })
    .await;
}

#[test]
fn default_deadline_is_ten_seconds() {
    assert_eq!(DEFAULT_LOCATE_TIMEOUT, Duration::from_secs(10));
}

#[tokio::test]
async fn locate_deadline_bounds_a_silent_server() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        let locator = locator.with_timeout(Duration::from_millis(500));
        dns.set_srv(SRV, &[("a.example.test", 443)]);
        dns.set_silent(true);
        let start = Instant::now();
        let err = locator.locate(DOMAIN).await.unwrap_err();
        let elapsed = start.elapsed();
        assert!(matches!(err, LocateError::Timeout), "{err:?}");
        assert!(elapsed < Duration::from_secs(3), "took {elapsed:?}");
        // The resolver retransmits, so there may be several queries; all are for the SRV name.
        assert_only_srv_queries(&dns);
    })
    .await;
}

#[tokio::test]
async fn locate_zero_deadline_times_out_without_a_query() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        let locator = locator.with_timeout(Duration::ZERO);
        dns.set_srv(SRV, &[("a.example.test", 443)]);
        let err = locator.locate(DOMAIN).await.unwrap_err();
        assert!(matches!(err, LocateError::Timeout), "{err:?}");
        // Refused before the resolver ran: nothing was sent.
        assert_eq!(dns.query_count(), 0);
    })
    .await;
}

// Slow on purpose: this is the one test of the resolver's own `Error::Timeout`, which n0-dns-resolver
// raises after 6 s (a constant it does not let us change) for a silent UDP nameserver. The locator's
// deadline is set above that, so the resolver's timeout is the one that fires.
#[tokio::test]
async fn locate_resolver_timeout_is_timeout() {
    bounded(SILENT_TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        let locator = locator.with_timeout(Duration::from_secs(12));
        dns.set_srv(SRV, &[("a.example.test", 443)]);
        dns.set_silent(true);
        let start = Instant::now();
        let err = locator.locate(DOMAIN).await.unwrap_err();
        let elapsed = start.elapsed();
        assert!(matches!(err, LocateError::Timeout), "{err:?}");
        // The resolver gave up, not the 12 s deadline.
        assert!(elapsed < Duration::from_secs(10), "took {elapsed:?}");
        assert_only_srv_queries(&dns);
    })
    .await;
}

#[tokio::test]
async fn locate_invalid_domain_sends_nothing() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        for bad in ["", "https://example.test", "example.test:443", "a b.test"] {
            let err = locator.locate(bad).await.unwrap_err();
            assert!(matches!(err, LocateError::InvalidDomain), "{err:?}");
        }
        assert_eq!(dns.query_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn errors_and_debug_do_not_echo_names() {
    bounded(TEST_TIMEOUT, async {
        const MARKER: &str = "zq7marker";
        let domain = format!("{MARKER}.example.test");
        let srv = format!("_numbers._tcp.{domain}");
        let (dns, locator) = server().await;

        let mut errors = Vec::new();
        // NoTower from NXDOMAIN.
        errors.push(locator.locate(&domain).await.unwrap_err());
        // NoTower from a target that is not a host name, which contains the marker.
        dns.set_srv(&srv, &[(&format!("{MARKER}_x.example.test"), 443)]);
        errors.push(locator.locate(&domain).await.unwrap_err());
        // InvalidDomain.
        errors.push(
            locator
                .locate(&format!("https://{domain}"))
                .await
                .unwrap_err(),
        );
        errors.push(srv_name(&format!("{MARKER} .test")).unwrap_err());
        // The variants that a fake server cannot easily produce here.
        errors.push(LocateError::TooManyTowers);
        errors.push(LocateError::Timeout);
        errors.push(LocateError::Resolver);

        for err in &errors {
            for text in [err.to_string(), format!("{err:?}"), format!("{err:#?}")] {
                assert!(!text.contains(MARKER), "{text}");
                assert!(!text.contains("example"), "{text}");
            }
        }
        let debug = format!("{locator:?}");
        assert_eq!(debug, "DnsLocator { .. }");
        assert!(!format!("{dns:?}").contains(MARKER));
    })
    .await;
}

#[tokio::test]
async fn system_locator_is_built_without_io() {
    bounded(TEST_TIMEOUT, async {
        // Constructed only: never queried, so no test reaches a real resolver.
        let locator = DnsLocator::system();
        assert_eq!(format!("{locator:?}"), "DnsLocator { .. }");
        let locator = DnsLocator::new(DnsConfig::System);
        assert_eq!(format!("{locator:?}"), "DnsLocator { .. }");
    })
    .await;
}

#[tokio::test]
async fn new_with_a_nameserver_asks_that_nameserver() {
    bounded(TEST_TIMEOUT, async {
        let dns = FakeDns::start().await.expect("bind the fake DNS server");
        let locator = DnsLocator::new(DnsConfig::Nameserver(dns.addr()));
        // The address is not in the locator's Debug, as it is not in `DnsConfig`'s.
        let debug = format!("{locator:?}");
        assert_eq!(debug, "DnsLocator { .. }");
        assert!(!debug.contains(&dns.addr().port().to_string()));
        dns.set_srv(SRV, &[("a.example.test", 443)]);
        let towers = locator.locate(DOMAIN).await.unwrap();
        assert_eq!(urls(&towers), ["https://a.example.test/"]);
        assert_eq!(towers[0].qad(), Qad::Port(7842));
        assert_eq!(dns.query_count(), 1);
        assert_only_srv_queries(&dns);
    })
    .await;
}

// The trait as the station holds it: `Arc<dyn Locator>`.

// Compile-time: `dyn Locator` and `Arc<dyn Locator>` are `Send + Sync`, and so is what holds them;
// the future `locate` returns is `Send`.
const _: () = {
    const fn send_sync<T: Send + Sync + ?Sized>() {}
    const fn send<T: Send + ?Sized>() {}
    send_sync::<dyn Locator>();
    send_sync::<Arc<dyn Locator>>();
    send::<LocateFuture<'static>>();
};

/// A locator that answers every domain with a fixed result, as the station's tests will use.
struct FixedLocator(Result<Vec<TowerEntry>, ()>);

impl Locator for FixedLocator {
    fn locate<'a>(&'a self, _domain: &'a str) -> LocateFuture<'a> {
        let result = self.0.clone().map_err(|()| LocateError::NoTower);
        Box::pin(async move { result })
    }
}

/// Generic use, the other way a caller may hold a locator.
async fn via_generic<L: Locator>(
    locator: &L,
    domain: &str,
) -> Result<Vec<TowerEntry>, LocateError> {
    locator.locate(domain).await
}

#[tokio::test]
async fn dns_locator_through_arc_dyn() {
    bounded(TEST_TIMEOUT, async {
        let (dns, locator) = server().await;
        dns.set_srv(SRV, &[("b.example.test", 3340), ("a.example.test", 443)]);
        let shared: Arc<dyn Locator> = Arc::new(locator);
        let expected = ["https://a.example.test/", "https://b.example.test:3340/"];
        assert_eq!(urls(&shared.locate(DOMAIN).await.unwrap()), expected);
        // `Arc<dyn Locator>` is itself a `Locator`, so it also goes where one is expected.
        assert_eq!(urls(&via_generic(&shared, DOMAIN).await.unwrap()), expected);
        // The future can move to another task.
        let task = tokio::spawn({
            let shared = Arc::clone(&shared);
            async move { shared.locate(DOMAIN).await }
        });
        assert_eq!(urls(&task.await.unwrap().unwrap()), expected);
        assert_eq!(dns.query_count(), 3);
        assert_only_srv_queries(&dns);
    })
    .await;
}

#[tokio::test]
async fn hand_written_locator_as_arc_dyn() {
    bounded(TEST_TIMEOUT, async {
        let towers = vec![TowerEntry::from_url("https://a.example.test/").unwrap()];
        let found: Arc<dyn Locator> = Arc::new(FixedLocator(Ok(towers.clone())));
        let none: Arc<dyn Locator> = Arc::new(FixedLocator(Err(())));
        assert_eq!(found.locate(DOMAIN).await.unwrap(), towers);
        let err = none.locate(DOMAIN).await.unwrap_err();
        assert!(matches!(err, LocateError::NoTower), "{err:?}");
        let locators: Vec<Arc<dyn Locator>> = vec![found, none];
        assert_eq!(locators.len(), 2);
    })
    .await;
}
