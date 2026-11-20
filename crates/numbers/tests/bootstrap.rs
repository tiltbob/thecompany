#![cfg(test)]
//! `numbers::bootstrap`: the SRV name, the canonical tower URLs, and `DnsLocator` against the fake
//! DNS server of `numbers::testing`. Everything runs on 127.0.0.1; only names under the reserved
//! `example.test` are used.

use std::future::Future;
use std::time::{Duration, Instant};

use numbers::bootstrap::{DEFAULT_LOCATE_TIMEOUT, SRV_SERVICE, srv_name, towers_from_srv};
use numbers::testing::FakeDns;
use numbers::{DnsLocator, LocateError, Locator, TowerSet};
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
        &long_label,
        &long_name,
    ] {
        assert!(
            matches!(srv_name(bad), Err(LocateError::InvalidDomain)),
            "accepted {bad:?}"
        );
    }
}

#[test]
fn towers_from_srv_two_targets() {
    let towers = towers_from_srv(pairs(&[("b.example.test", 3340), ("a.example.test", 443)]));
    assert_eq!(
        towers,
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
    ]));
    assert_eq!(
        towers,
        [
            "https://a.example.test/",
            "https://a.example.test:8443/",
            "https://c.example.test:8443/",
        ]
    );
    let mut sorted = towers.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(towers, sorted);
    for url in &towers {
        assert!(TowerSet::from_url(url).is_ok(), "TowerSet refused {url}");
        assert!(!url.contains(".test."), "trailing dot in {url}");
    }
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
            towers,
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
            locator.locate(DOMAIN).await.unwrap(),
            ["https://a.example.test/"]
        );
        dns.set_srv(SRV, &[("b.example.test", 443), ("c.example.test", 443)]);
        assert_eq!(
            locator.locate(DOMAIN).await.unwrap(),
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
    })
    .await;
}
