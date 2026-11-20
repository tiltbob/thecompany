#![cfg(test)]
//! A TLS tower with a certificate from PEM files, exercised by iroh endpoints acting as stations.
//!
//! The certificate is self-signed for 127.0.0.1 and made at run time; nothing here prints an id
//! or key material.

mod common;

use std::fmt;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{
    DENY_REASON, Station, bounded, connected, denial, online, spawn, status, stays_pending,
};
use iroh::tls::CaTlsConfig;
use iroh::{RelayConfig, RelayUrl, SecretKey, Watcher};
use iroh_relay::RelayQuicConfig;
use numbers_tower::{
    MAX_RELOAD_INTERVAL, TlsFileProblem, TlsTower, Tower, TowerConfig, TowerError,
};
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, SubscriberExt};

/// How long an endpoint that does not trust the certificate must stay offline.
const UNTRUSTED_FOR: Duration = Duration::from_secs(5);
/// How long the QAD counter may take to catch up with an endpoint that came online.
const QAD_COUNTED_WITHIN: Duration = Duration::from_secs(5);

/// A self-signed certificate for 127.0.0.1 and its key, written as PEM files.
struct Cert {
    der: CertificateDer<'static>,
    cert: PathBuf,
    key: PathBuf,
    cert_pem: String,
    key_pem: String,
    key_der: Vec<u8>,
    dir: tempfile::TempDir,
}

/// Generates a pair and writes it as `<name>.cert.pem` and `<name>.key.pem` into `dir`.
fn generate(dir: tempfile::TempDir, name: &str) -> Cert {
    let generated = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])
        .unwrap_or_else(|_| panic!("generate a certificate"));
    let cert = dir.path().join(format!("{name}.cert.pem"));
    let key = dir.path().join(format!("{name}.key.pem"));
    let cert_pem = generated.cert.pem();
    let key_pem = generated.signing_key.serialize_pem();
    std::fs::write(&cert, &cert_pem).unwrap_or_else(|_| panic!("write cert"));
    std::fs::write(&key, &key_pem).unwrap_or_else(|_| panic!("write key"));
    Cert {
        der: generated.cert.der().clone(),
        cert,
        key,
        cert_pem,
        key_pem,
        key_der: generated.signing_key.serialize_der(),
        dir,
    }
}

/// A pair in a new temporary directory.
fn cert() -> Cert {
    generate(
        tempfile::tempdir().unwrap_or_else(|_| panic!("make a temporary directory")),
        "tower",
    )
}

/// Another pair, written next to `dir`'s files; its own directory handle is only a stand-in.
fn cert_in(dir: &tempfile::TempDir, name: &str) -> Cert {
    let mut pair = generate(
        tempfile::tempdir().unwrap_or_else(|_| panic!("make a temporary directory")),
        name,
    );
    let (cert, key) = (
        dir.path().join(format!("{name}.cert.pem")),
        dir.path().join(format!("{name}.key.pem")),
    );
    std::fs::copy(&pair.cert, &cert).unwrap_or_else(|_| panic!("copy cert"));
    std::fs::copy(&pair.key, &key).unwrap_or_else(|_| panic!("copy key"));
    pair.cert = cert;
    pair.key = key;
    pair
}

fn loopback() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

/// A TLS tower on 127.0.0.1 with OS-chosen ports, reached as 127.0.0.1, without QAD.
fn tls_tower(cert: &Path, key: &Path) -> TlsTower {
    TlsTower::new("127.0.0.1", cert, key)
        .https_bind(loopback())
        .qad_bind(None)
}

fn config(tls: TlsTower) -> TowerConfig {
    TowerConfig::tls(tls).bind(loopback())
}

/// A relay-only station that trusts `cert`.
fn trusting(cert: &Cert, tower: &RelayUrl) -> Station {
    Station {
        roots: Some(CaTlsConfig::custom_roots([cert.der.clone()])),
        ..Station::relay_only(tower)
    }
}

/// Whether this host has an IPv4 address that is not loopback, which iroh needs before it plans
/// a QAD probe. Connecting a UDP socket only picks a route and a source address; it sends
/// nothing. 192.0.2.0/24 is reserved for documentation.
fn has_non_loopback_ipv4() -> bool {
    let Ok(socket) = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) else {
        return false;
    };
    if socket.connect((Ipv4Addr::new(192, 0, 2, 1), 9)).is_err() {
        return false;
    }
    socket
        .local_addr()
        .is_ok_and(|addr| !addr.ip().is_loopback() && !addr.ip().is_unspecified())
}

#[tokio::test]
async fn a_trusting_endpoint_comes_online_through_the_tls_tower() {
    bounded(async {
        let cert = cert();
        let (tower, url) = spawn(config(tls_tower(&cert.cert, &cert.key))).await;
        let https_port = url.port().unwrap_or_else(|| panic!("URL without a port"));
        assert_eq!(tower.url(), format!("https://127.0.0.1:{https_port}/"));
        assert_ne!(tower.http_addr().port(), https_port);
        assert_eq!(tower.qad_port(), None);

        let endpoint = trusting(&cert, &url).bind(SecretKey::generate()).await;
        online(&endpoint).await;
        assert!(connected(&endpoint));

        endpoint.close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn an_endpoint_with_default_roots_does_not_come_online() {
    bounded(async {
        let cert = cert();
        let (tower, url) = spawn(config(tls_tower(&cert.cert, &cert.key))).await;
        let endpoint = Station::relay_only(&url).bind(SecretKey::generate()).await;

        stays_pending(
            UNTRUSTED_FOR,
            endpoint.online(),
            "an endpoint that does not trust the certificate came online",
        )
        .await;
        // The HTTPS latency probe fails on the certificate too, so no home tower is ever chosen.
        let relays = endpoint.home_relay_status().get();
        assert!(relays.is_empty(), "{}", status(&endpoint));

        endpoint.close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn an_endpoint_with_an_ip_socket_runs_address_discovery() {
    bounded(async {
        let cert = cert();
        let tls = tls_tower(&cert.cert, &cert.key).qad_bind(Some(loopback()));
        let (tower, url) = spawn(config(tls)).await;
        let qad_port = tower
            .qad_port()
            .unwrap_or_else(|| panic!("QAD is on but has no port"));
        assert_ne!(qad_port, 0);

        let station = Station {
            relay: RelayConfig::new(url.clone(), Some(RelayQuicConfig::new(qad_port))),
            ip_socket: true,
            ..trusting(&cert, &url)
        };
        let endpoint = station.bind(SecretKey::generate()).await;
        online(&endpoint).await;

        if has_non_loopback_ipv4() {
            // iroh-relay counts a QAD connection once its handshake completes on the tower,
            // which may be just after the endpoint came online. No event marks that, so poll
            // the counter under a deadline.
            let counted = timeout(QAD_COUNTED_WITHIN, async {
                let mut tick = tokio::time::interval(Duration::from_millis(10));
                while tower.stats().qad_connections == 0 {
                    tick.tick().await;
                }
            })
            .await;
            assert!(counted.is_ok(), "{:?}", tower.stats());
        } else {
            println!(
                "skipped the QAD counter check: this host has no non-loopback IPv4 address, so \
                 iroh plans no QAD probe"
            );
        }

        endpoint.close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn the_allowlist_applies_to_a_tls_tower() {
    bounded(async {
        let cert = cert();
        let (listed_key, unlisted_key) = (SecretKey::generate(), SecretKey::generate());
        let config =
            config(tls_tower(&cert.cert, &cert.key)).allow([listed_key.public().to_string()]);
        let (tower, url) = spawn(config).await;
        let listed = trusting(&cert, &url).bind(listed_key).await;
        let unlisted = trusting(&cert, &url).bind(unlisted_key).await;

        let (reason, ()) = tokio::join!(denial(&unlisted), online(&listed));
        assert_eq!(reason, DENY_REASON);
        assert!(!connected(&unlisted));
        assert!(tower.stats().denied >= 1);

        listed.close().await;
        unlisted.close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn the_plain_http_listener_answers_the_captive_portal_check() {
    bounded(async {
        let cert = cert();
        let (tower, _url) = spawn(config(tls_tower(&cert.cert, &cert.key))).await;
        let addr = tower.http_addr();
        assert!(addr.ip().is_loopback());

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = "GET /generate_204 HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 204 "), "{response}");

        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

/// A loopback TCP port that was free a moment ago.
fn free_tcp_port() -> u16 {
    let listener = TcpListener::bind(loopback()).unwrap();
    listener.local_addr().unwrap().port()
}

/// A loopback UDP port that was free a moment ago.
fn free_udp_port() -> u16 {
    let socket = UdpSocket::bind(loopback()).unwrap();
    socket.local_addr().unwrap().port()
}

/// Spawns a tower reached as 127.0.0.1 on fixed ports with the given files, expects it to fail,
/// checks that none of the ports stayed bound, and returns the error.
async fn spawn_fails(cert: &Path, key: &Path) -> TowerError {
    spawn_fails_as("127.0.0.1", cert, key).await
}

/// [`spawn_fails`] for a tower reached as `hostname`.
async fn spawn_fails_as(hostname: &str, cert: &Path, key: &Path) -> TowerError {
    let (http, https, qad) = (free_tcp_port(), free_tcp_port(), free_udp_port());
    let tls = TlsTower::new(hostname, cert, key)
        .https_bind((Ipv4Addr::LOCALHOST, https).into())
        .qad_bind(Some((Ipv4Addr::LOCALHOST, qad).into()));
    let config = TowerConfig::tls(tls).bind((Ipv4Addr::LOCALHOST, http).into());
    let Err(error) = Tower::spawn(config).await else {
        panic!("spawn accepted an unusable configuration");
    };
    for port in [http, https] {
        assert!(
            TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_ok(),
            "TCP {port} is bound"
        );
    }
    assert!(
        UdpSocket::bind((Ipv4Addr::LOCALHOST, qad)).is_ok(),
        "UDP {qad} is bound"
    );
    error
}

/// The error's Display, Debug (plain and pretty) and every source's Display and Debug.
fn texts(error: &TowerError) -> Vec<String> {
    let mut texts = vec![
        error.to_string(),
        format!("{error:?}"),
        format!("{error:#?}"),
    ];
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        texts.push(cause.to_string());
        texts.push(format!("{cause:?}"));
        source = cause.source();
    }
    texts
}

/// Fails if any text holds `needle` as text, or as the numbers of a byte vector in decimal or
/// hex (with or without `0x`), however the vector was spaced or wrapped.
fn assert_not_in(texts: &[String], needle: &[u8], what: &str) {
    let join = |format: fn(&u8) -> String| needle.iter().map(format).collect::<Vec<_>>().join(",");
    let forms = [
        String::from_utf8_lossy(needle).into_owned(),
        join(|b| b.to_string()),
        join(|b| format!("{b:x}")),
        join(|b| format!("{b:#x}")),
    ];
    for text in texts {
        let squeezed: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        for form in &forms {
            assert!(
                !text.contains(form.as_str()) && !squeezed.contains(form.as_str()),
                "an error shows {what}"
            );
        }
    }
}

/// Fails if any text holds a base64 line of the key or a run of its DER bytes.
fn assert_no_key_material(texts: &[String], cert: &Cert) {
    for line in cert
        .key_pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
    {
        assert_not_in(texts, line.as_bytes(), "a line of the key");
    }
    assert_not_in(
        texts,
        &cert.key_der[cert.key_der.len() - 16..],
        "bytes of the key",
    );
}

/// Expects a certificate file error with `problem`, no source, and no path in its text.
fn expect_certificate_problem(error: &TowerError, path: &Path, problem: TlsFileProblem) {
    let TowerError::CertificateFile { problem: found, .. } = error else {
        panic!("expected a certificate file error, got: {error}");
    };
    assert!(*found == problem, "{error}");
    assert!(std::error::Error::source(error).is_none());
    assert_not_in(
        &texts(error),
        path.as_os_str().as_encoded_bytes(),
        "the path",
    );
}

/// Expects a key file error with `problem`, no source, and no path in its text.
fn expect_key_problem(error: &TowerError, path: &Path, problem: TlsFileProblem) {
    let TowerError::KeyFile { problem: found, .. } = error else {
        panic!("expected a key file error, got: {error}");
    };
    assert!(*found == problem, "{error}");
    assert!(std::error::Error::source(error).is_none());
    assert_not_in(
        &texts(error),
        path.as_os_str().as_encoded_bytes(),
        "the path",
    );
}

/// A marker that no real file holds, so that finding it in an error means a leak.
const MARKER: &str = "QZXMARKERQZX";

/// PEM with a malformed header line (four dashes at the end instead of five), and PEM whose
/// end marker is malformed. The PEM parser reports both with the bytes of the bad line or
/// label in its error.
fn malformed_pem() -> [(&'static str, String); 2] {
    [
        (
            "a malformed header",
            format!(
                "-----BEGIN {MARKER} PRIVATE KEY----\nAAAA\n-----END {MARKER} PRIVATE KEY-----\n"
            ),
        ),
        (
            "a malformed end marker",
            format!("-----BEGIN {MARKER}-----\nAAAA\n-----END {MARKER}---\n"),
        ),
    ]
}

/// DER that is valid (a SEQUENCE holding INTEGER 0) but neither a certificate nor a key.
const NOT_A_CERTIFICATE_DER: &str = "MAMCAQA=";

/// A CERTIFICATE section whose DER is not a certificate.
fn unparsable_certificate_section() -> String {
    format!("-----BEGIN CERTIFICATE-----\n{NOT_A_CERTIFICATE_DER}\n-----END CERTIFICATE-----\n")
}

#[tokio::test]
async fn unusable_certificate_files_fail_spawn_before_binding() {
    bounded(async {
        let cert = cert();
        let other = cert_in(&cert.dir, "other");
        let dir = cert.dir.path();
        let write = |name: &str, text: &str| {
            let path = dir.join(name);
            std::fs::write(&path, text).unwrap();
            path
        };
        let missing = dir.join("missing.pem");
        let not_pem = write("not-pem.txt", "this is not a PEM file\n");
        let invalid_der = write(
            "invalid-der.pem",
            &format!(
                "-----BEGIN CERTIFICATE-----\n{NOT_A_CERTIFICATE_DER}\n-----END CERTIFICATE-----\n"
            ),
        );
        let unsupported_key = write(
            "unsupported-key.pem",
            &format!(
                "-----BEGIN PRIVATE KEY-----\n{NOT_A_CERTIFICATE_DER}\n-----END PRIVATE KEY-----\n"
            ),
        );
        // A good leaf that matches the key, followed by a second certificate that does not parse.
        let bad_second = write(
            "bad-second.pem",
            &format!("{}{}", cert.cert_pem, unparsable_certificate_section()),
        );
        let two_keys = write(
            "two-keys.pem",
            &format!("{}{}", cert.key_pem, other.key_pem),
        );
        // An encrypted key is skipped by its label, whatever its body holds.
        let encrypted_key = write(
            "encrypted-key.pem",
            &format!(
                "-----BEGIN ENCRYPTED PRIVATE KEY-----\n{NOT_A_CERTIFICATE_DER}\n\
                 -----END ENCRYPTED PRIVATE KEY-----\n"
            ),
        );
        // One byte over the 64 KiB cap.
        let too_large = write("too-large.pem", &"x".repeat(64 * 1024 + 1));

        for (path, problem) in [
            (
                &missing,
                TlsFileProblem::Unreadable(std::io::ErrorKind::NotFound),
            ),
            (&not_pem, TlsFileProblem::NoCertificate),
            (&too_large, TlsFileProblem::TooLarge),
            (&invalid_der, TlsFileProblem::UnparsableCertificate),
            (&bad_second, TlsFileProblem::UnparsableCertificate),
            // Paths swapped by mistake: the key file holds no certificate.
            (&cert.key, TlsFileProblem::NoCertificate),
        ] {
            let error = spawn_fails(path, &cert.key).await;
            expect_certificate_problem(&error, path, problem);
            assert_no_key_material(&texts(&error), &cert);
        }

        for (path, problem) in [
            (
                &missing,
                TlsFileProblem::Unreadable(std::io::ErrorKind::NotFound),
            ),
            (&not_pem, TlsFileProblem::NoPrivateKey),
            (&too_large, TlsFileProblem::TooLarge),
            (&encrypted_key, TlsFileProblem::NoPrivateKey),
            // Paths swapped by mistake: the certificate file holds no key.
            (&cert.cert, TlsFileProblem::NoPrivateKey),
            (&unsupported_key, TlsFileProblem::UnsupportedKey),
            (&two_keys, TlsFileProblem::SeveralPrivateKeys),
            // A valid certificate with a valid key of another pair.
            (&other.key, TlsFileProblem::KeyMismatch),
        ] {
            let error = spawn_fails(&cert.cert, path).await;
            expect_key_problem(&error, path, problem);
            let texts = texts(&error);
            assert_no_key_material(&texts, &cert);
            assert_no_key_material(&texts, &other);
        }
    })
    .await;
}

#[tokio::test]
async fn malformed_pem_fails_spawn_without_showing_the_file() {
    bounded(async {
        let cert = cert();
        for (index, (what, pem)) in malformed_pem().into_iter().enumerate() {
            let path = cert.dir.path().join(format!("malformed-{index}.pem"));
            std::fs::write(&path, &pem).unwrap();

            let error = spawn_fails(&path, &cert.key).await;
            expect_certificate_problem(&error, &path, TlsFileProblem::MalformedPem);
            assert_not_in(&texts(&error), MARKER.as_bytes(), what);

            let error = spawn_fails(&cert.cert, &path).await;
            expect_key_problem(&error, &path, TlsFileProblem::MalformedPem);
            assert_not_in(&texts(&error), MARKER.as_bytes(), what);
        }
    })
    .await;
}

/// Host names that are neither a DNS host name nor an IP literal.
const INVALID_HOST_NAMES: [&str; 5] = [
    "",
    "tower/path",
    "user@tower",
    "tower_qzx.example",
    "-qzx.example",
];

#[tokio::test]
async fn an_invalid_host_name_fails_spawn_without_repeating_it() {
    let cert = cert();
    let tls = tls_tower(&cert.cert, &cert.key);
    for name in INVALID_HOST_NAMES {
        let mut tls = tls.clone();
        name.clone_into(&mut tls.hostname);
        let Err(error) = Tower::spawn(config(tls)).await else {
            panic!("spawn accepted an invalid host name");
        };
        assert!(matches!(error, TowerError::InvalidHostname));
        if !name.is_empty() {
            assert_not_in(&texts(&error), name.as_bytes(), "the host name");
        }
    }
}

/// Spawns `tls` with its HTTPS port held by another listener, expects it to fail, and returns
/// the error.
async fn spawn_fails_with_https_taken(tls: TlsTower) -> TowerError {
    let taken = TcpListener::bind(loopback()).unwrap();
    let tls = tls.https_bind(taken.local_addr().unwrap());
    let Err(error) = Tower::spawn(config(tls)).await else {
        panic!("spawn bound an HTTPS port that is taken");
    };
    error
}

/// The host name is checked before anything is bound: an invalid name leaves every port free,
/// and it is the error even when the HTTPS port is taken, where binding would fail first.
#[tokio::test]
async fn an_invalid_host_name_fails_spawn_before_binding() {
    bounded(async {
        let cert = cert();
        for name in INVALID_HOST_NAMES {
            let error = spawn_fails_as(name, &cert.cert, &cert.key).await;
            assert!(matches!(error, TowerError::InvalidHostname), "{error}");

            let mut tls = tls_tower(&cert.cert, &cert.key);
            name.clone_into(&mut tls.hostname);
            let error = spawn_fails_with_https_taken(tls).await;
            assert!(matches!(error, TowerError::InvalidHostname), "{error}");
        }
    })
    .await;
}

/// A TLS tower whose HTTPS port is taken fails to start.
#[tokio::test]
async fn a_taken_https_port_fails_spawn_with_a_start_error() {
    bounded(async {
        let cert = cert();
        let error = spawn_fails_with_https_taken(tls_tower(&cert.cert, &cert.key)).await;
        assert!(matches!(error, TowerError::Start(_)), "{error}");
    })
    .await;
}

/// Sends `GET path` over HTTPS to `addr` as 127.0.0.1, trusting only `roots`, and returns the
/// status line or the error. Blocking; run it with `spawn_blocking`.
fn https_status(
    addr: SocketAddr,
    roots: Vec<CertificateDer<'static>>,
    path: &str,
) -> Result<String, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut store = rustls::RootCertStore::empty();
    for root in roots {
        store.add(root).map_err(|error| error.to_string())?;
    }
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| error.to_string())?
        .with_root_certificates(store)
        .with_no_client_auth();
    let name = ServerName::try_from("127.0.0.1").map_err(|error| error.to_string())?;
    let connection =
        rustls::ClientConnection::new(Arc::new(config), name).map_err(|error| error.to_string())?;
    let tcp = std::net::TcpStream::connect(addr).map_err(|error| error.to_string())?;
    tcp.set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| error.to_string())?;
    tcp.set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| error.to_string())?;
    let mut stream = rustls::StreamOwned::new(connection, tcp);
    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|error| error.to_string())?;
    let mut response = Vec::new();
    let mut buffer = [0; 1024];
    while !response.windows(2).any(|pair| pair == b"\r\n") {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(&buffer[..read]),
            Err(error) => return Err(error.to_string()),
        }
    }
    let response = String::from_utf8_lossy(&response);
    response
        .lines()
        .next()
        .map(str::to_owned)
        .ok_or_else(|| "empty response".to_owned())
}

/// [`https_status`] on the blocking pool.
async fn https(
    addr: SocketAddr,
    roots: &[&CertificateDer<'static>],
    path: &'static str,
) -> Result<String, String> {
    let roots = roots.iter().map(|root| (*root).clone()).collect();
    tokio::task::spawn_blocking(move || https_status(addr, roots, path))
        .await
        .unwrap_or_else(|_| Err("the request task failed".to_owned()))
}

fn https_addr(url: &RelayUrl) -> SocketAddr {
    let port = url.port().unwrap_or_else(|| panic!("URL without a port"));
    (Ipv4Addr::LOCALHOST, port).into()
}

#[tokio::test]
async fn https_answers_a_client_that_trusts_the_certificate_and_fails_one_that_does_not() {
    bounded(async {
        let cert = cert();
        let unrelated = cert_in(&cert.dir, "unrelated");
        let (tower, url) = spawn(config(tls_tower(&cert.cert, &cert.key))).await;
        let addr = https_addr(&url);

        let healthz = https(addr, &[&cert.der], "/healthz").await;
        // Hard rule 6: serving HTTPS installed no process-wide crypto provider. Nothing in this
        // test binary installs one; every rustls configuration gets its provider explicitly.
        assert!(rustls::crypto::CryptoProvider::get_default().is_none());
        assert_eq!(healthz.as_deref(), Ok("HTTP/1.1 200 OK"));
        let ping = https(addr, &[&cert.der], "/ping").await;
        assert_eq!(ping.as_deref(), Ok("HTTP/1.1 200 OK"));

        for roots in [&[][..], &[&unrelated.der][..]] {
            let Err(error) = https(addr, roots, "/healthz").await else {
                panic!("a client that does not trust the certificate got an answer");
            };
            assert!(error.contains("invalid peer certificate"), "{error}");
        }

        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

/// The reload interval of the reload test.
const RELOAD: Duration = Duration::from_millis(100);

/// Replaces a file's content by renaming a complete new file over it.
fn replace(path: &Path, text: &str) {
    let staged = path.with_extension("staged");
    std::fs::write(&staged, text).unwrap();
    std::fs::rename(&staged, path).unwrap();
}

/// One event from the tower's own targets: its level, its target and every field as text, the
/// message included.
#[derive(Debug)]
struct Captured {
    level: tracing::Level,
    target: String,
    fields: Vec<(String, String)>,
}

impl Captured {
    /// The text of field `name`, if the event has it.
    fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }
}

/// A layer that records every event whose target starts with `numbers_tower`.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Captured>>>);

impl Capture {
    /// How many events it has recorded.
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    /// Whether an event recorded since the first `from` is the warning of a failed reload of
    /// `file` with `problem`.
    fn has_reload_warning(&self, from: usize, file: &str, problem: TlsFileProblem) -> bool {
        self.0.lock().unwrap()[from..].iter().any(|event| {
            event.level == tracing::Level::WARN
                && event.target == "numbers_tower::tls"
                && event.field("message")
                    == Some(
                        "TLS certificate reload failed; the tower keeps serving the pair in use",
                    )
                && event.field("file") == Some(file)
                && event.field("problem") == Some(problem.to_string().as_str())
        })
    }

    /// Fails if the level, target, a field name or a field value of any recorded event holds
    /// `needle`.
    fn assert_never_shows(&self, needle: &str, what: &str) {
        for event in self.0.lock().unwrap().iter() {
            let texts = [event.level.to_string(), event.target.clone()];
            let fields = event.fields.iter().flat_map(|(name, value)| [name, value]);
            for text in texts.iter().chain(fields) {
                assert!(
                    !text.contains(needle),
                    "a log event shows {what}: {event:?}"
                );
            }
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _context: Context<'_, S>) {
        let metadata = event.metadata();
        if !metadata.target().starts_with("numbers_tower") {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0.lock().unwrap().push(Captured {
            level: *metadata.level(),
            target: metadata.target().to_owned(),
            fields: fields.0,
        });
    }
}

/// The fields of one event, each as text.
#[derive(Default)]
struct Fields(Vec<(String, String)>);

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_owned(), value.to_owned()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }
}

/// Waits, polling every reload interval, until a client that trusts only `cert` gets an answer
/// from `addr`; fails after 10 s.
async fn until_served(addr: SocketAddr, cert: &Cert, what: &str) {
    let served = timeout(Duration::from_secs(10), async {
        let mut tick = tokio::time::interval(RELOAD);
        while https(addr, &[&cert.der], "/healthz").await.as_deref() != Ok("HTTP/1.1 200 OK") {
            tick.tick().await;
        }
    })
    .await;
    assert!(served.is_ok(), "{what} was not served within 10 s");
}

/// Reloading serves a renewed pair, keeps it while the files on disk are broken, warns about
/// each broken pair with the file and the kind of problem only, and goes on to serve the next
/// good pair.
#[tokio::test]
async fn a_reload_serves_a_renewed_pair_and_keeps_the_last_good_one() {
    // `#[tokio::test]` runs every task on this thread, the reload task's warnings included, so
    // a default subscriber for this thread sees them. The guard need not be `Send`: neither
    // `bounded` nor the test's runtime asks the future to be.
    let capture = Capture::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    bounded(async {
        let first = cert();
        let tls = tls_tower(&first.cert, &first.key).reload_interval(RELOAD);
        let (tower, url) = spawn(config(tls)).await;
        let addr = https_addr(&url);
        assert_eq!(
            https(addr, &[&first.der], "/healthz").await.as_deref(),
            Ok("HTTP/1.1 200 OK")
        );

        // A renewed pair for the same name: served within a few intervals.
        let second = cert_in(&first.dir, "second");
        replace(&first.cert, &second.cert_pem);
        replace(&first.key, &second.key_pem);
        until_served(addr, &second, "the renewed pair").await;
        assert!(https(addr, &[&first.der], "/healthz").await.is_err());

        // Broken pairs: a certificate with another key; malformed PEM in both files; a new leaf
        // with its own key but a second certificate that does not parse. Each stays on disk for
        // several intervals, and the renewed pair stays in use throughout. The certificate file
        // is checked first, so a reload between the two renames reports the same file.
        let third = cert_in(&first.dir, "third");
        let [(_, malformed_header), (_, malformed_end)] = malformed_pem();
        let bad_chain = format!("{}{}", third.cert_pem, unparsable_certificate_section());
        for (cert_pem, key_pem, file, problem) in [
            (
                third.cert_pem.as_str(),
                second.key_pem.as_str(),
                "private key file",
                TlsFileProblem::KeyMismatch,
            ),
            (
                malformed_header.as_str(),
                malformed_end.as_str(),
                "certificate file",
                TlsFileProblem::MalformedPem,
            ),
            (
                bad_chain.as_str(),
                third.key_pem.as_str(),
                "certificate file",
                TlsFileProblem::UnparsableCertificate,
            ),
        ] {
            let from = capture.len();
            replace(&first.cert, cert_pem);
            replace(&first.key, key_pem);
            // Asserting that nothing changes: there is no event to wait for.
            tokio::time::sleep(RELOAD * 5).await;
            // The warning is an event, but a slow host may not have reloaded yet.
            let warned = timeout(Duration::from_secs(10), async {
                let mut tick = tokio::time::interval(RELOAD);
                while !capture.has_reload_warning(from, file, problem) {
                    tick.tick().await;
                }
            })
            .await;
            assert!(warned.is_ok(), "no warning about the {file}: {problem}");
            assert_eq!(
                https(addr, &[&second.der], "/healthz").await.as_deref(),
                Ok("HTTP/1.1 200 OK")
            );
            assert!(https(addr, &[&third.der], "/healthz").await.is_err());
        }

        // A good pair again: reloading went on after the failures.
        let fourth = cert_in(&first.dir, "fourth");
        replace(&first.cert, &fourth.cert_pem);
        replace(&first.key, &fourth.key_pem);
        until_served(addr, &fourth, "the pair after the broken ones").await;

        // No warning names where the files are, or quotes them.
        let directory = first.dir.path().to_string_lossy().into_owned();
        capture.assert_never_shows(&directory, "the directory");
        for path in [&first.cert, &first.key] {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            capture.assert_never_shows(&name, "a file name");
        }
        capture.assert_never_shows(MARKER, "the content of a malformed file");

        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

/// The parameters of a CA certificate named `name`.
fn ca_params(name: &str) -> rcgen::CertificateParams {
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    params
}

/// A leaf for 127.0.0.1 signed by an intermediate CA that a root CA signed, written as
/// `leaf, intermediate` PEM, with the leaf's key; returns the files and the root's certificate.
fn chain_in(dir: &Path) -> (PathBuf, PathBuf, CertificateDer<'static>) {
    let root_key = rcgen::KeyPair::generate().unwrap();
    let root_params = ca_params("numbers test root CA");
    let root = root_params.self_signed(&root_key).unwrap();
    let root_issuer = rcgen::Issuer::from_params(&root_params, &root_key);
    let intermediate_key = rcgen::KeyPair::generate().unwrap();
    let intermediate_params = ca_params("numbers test intermediate CA");
    let intermediate = intermediate_params
        .signed_by(&intermediate_key, &root_issuer)
        .unwrap();
    let intermediate_issuer = rcgen::Issuer::from_params(&intermediate_params, &intermediate_key);
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf = rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()])
        .unwrap()
        .signed_by(&leaf_key, &intermediate_issuer)
        .unwrap();
    let (cert, key) = (dir.join("chain.pem"), dir.join("chain-key.pem"));
    std::fs::write(&cert, format!("{}{}", leaf.pem(), intermediate.pem())).unwrap();
    std::fs::write(&key, leaf_key.serialize_pem()).unwrap();
    (cert, key, root.der().clone())
}

/// The tower serves every certificate of its file, not only the first. The file holds a leaf
/// and the intermediate CA that signed it; the client trusts only the root CA, which is not in
/// the file, so it can build a path to the leaf only if the tower sends the intermediate. It
/// also shows that an intermediate passes the check of every certificate.
#[tokio::test]
async fn the_whole_chain_is_served_so_a_client_that_trusts_only_the_root_accepts_it() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key, root) = chain_in(dir.path());
        let (tower, url) = spawn(config(tls_tower(&cert, &key))).await;
        let addr = https_addr(&url);
        assert_eq!(
            https(addr, &[&root], "/healthz").await.as_deref(),
            Ok("HTTP/1.1 200 OK")
        );
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

/// Standard base64 with padding, as a PEM body uses it.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut text = String::new();
    for chunk in bytes.chunks(3) {
        let byte = |index: usize| u32::from(chunk.get(index).copied().unwrap_or(0));
        let triple = (byte(0) << 16) | (byte(1) << 8) | byte(2);
        for position in 0..4 {
            if position <= chunk.len() {
                let sextet = (triple >> (18 - 6 * position)) & 0x3f;
                text.push(char::from(ALPHABET[sextet as usize]));
            } else {
                text.push('=');
            }
        }
    }
    text
}

/// `der` as a PEM section labelled `label`, 64 characters to a line.
fn pem(label: &str, der: &[u8]) -> String {
    let body = base64(der);
    let mut text = format!("-----BEGIN {label}-----\n");
    for line in body.as_bytes().chunks(64) {
        text.push_str(std::str::from_utf8(line).unwrap());
        text.push('\n');
    }
    text.push_str(&format!("-----END {label}-----\n"));
    text
}

/// The first DER element of `input` (definite length, short or long form): its tag, its
/// content and what follows it.
fn der_element(input: &[u8]) -> (u8, &[u8], &[u8]) {
    let (&tag, rest) = input.split_first().unwrap();
    let (&first, rest) = rest.split_first().unwrap();
    let (length, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let (octets, rest) = rest.split_at(usize::from(first & 0x7f));
        let length = octets
            .iter()
            .fold(0, |length, &octet| (length << 8) | usize::from(octet));
        (length, rest)
    };
    let (content, rest) = rest.split_at(length);
    (tag, content, rest)
}

/// The SEC1 `ECPrivateKey` inside a PKCS#8 EC key: the content of the OCTET STRING that follows
/// the version and the algorithm in `SEQUENCE { INTEGER, SEQUENCE, OCTET STRING, ... }`.
fn sec1_from_pkcs8(pkcs8: &[u8]) -> Vec<u8> {
    let (tag, body, _) = der_element(pkcs8);
    assert_eq!(tag, 0x30, "PKCS#8 is a SEQUENCE");
    let (tag, _version, rest) = der_element(body);
    assert_eq!(tag, 0x02, "the version is an INTEGER");
    let (tag, _algorithm, rest) = der_element(rest);
    assert_eq!(tag, 0x30, "the algorithm is a SEQUENCE");
    let (tag, sec1, _) = der_element(rest);
    assert_eq!(tag, 0x04, "the key is an OCTET STRING");
    sec1.to_vec()
}

/// The tower serves with each kind of key `TlsTower::key_path` promises beyond the ECDSA P-256
/// PKCS#8 key the other tests use: ECDSA P-384 and Ed25519 in PKCS#8, ECDSA P-256 and P-384 in
/// SEC1 (`EC PRIVATE KEY`). RSA keys, PKCS#1 or PKCS#8, have no test: nothing in the workspace
/// can make an RSA key at run time, and the repository tracks no key files.
#[tokio::test]
async fn ecdsa_and_ed25519_keys_in_pkcs8_and_sec1_are_served() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        for (index, (what, algorithm, sec1)) in [
            ("ECDSA P-384, PKCS#8", &rcgen::PKCS_ECDSA_P384_SHA384, false),
            ("Ed25519, PKCS#8", &rcgen::PKCS_ED25519, false),
            ("ECDSA P-256, SEC1", &rcgen::PKCS_ECDSA_P256_SHA256, true),
            ("ECDSA P-384, SEC1", &rcgen::PKCS_ECDSA_P384_SHA384, true),
        ]
        .into_iter()
        .enumerate()
        {
            let key = rcgen::KeyPair::generate_for(algorithm).unwrap();
            let certificate = rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()])
                .unwrap()
                .self_signed(&key)
                .unwrap();
            let key_pem = if sec1 {
                pem("EC PRIVATE KEY", &sec1_from_pkcs8(&key.serialize_der()))
            } else {
                key.serialize_pem()
            };
            let (cert_path, key_path) = (
                dir.path().join(format!("{index}.cert.pem")),
                dir.path().join(format!("{index}.key.pem")),
            );
            std::fs::write(&cert_path, certificate.pem()).unwrap();
            std::fs::write(&key_path, key_pem).unwrap();

            let (tower, url) = spawn(config(tls_tower(&cert_path, &key_path))).await;
            assert_eq!(
                https(https_addr(&url), &[certificate.der()], "/healthz")
                    .await
                    .as_deref(),
                Ok("HTTP/1.1 200 OK"),
                "{what}"
            );
            assert!(tower.shutdown().await.is_ok());
        }
    })
    .await;
}

/// A reload interval that the clock cannot schedule fails spawn before anything is bound.
#[tokio::test]
async fn a_zero_or_unschedulable_reload_interval_fails_spawn_before_binding() {
    bounded(async {
        let cert = cert();
        let year = MAX_RELOAD_INTERVAL;
        for every in [
            Duration::ZERO,
            Duration::MAX,
            year + Duration::from_nanos(1),
        ] {
            let (http, https) = (free_tcp_port(), free_tcp_port());
            let tls = TlsTower::new("127.0.0.1", &cert.cert, &cert.key)
                .https_bind((Ipv4Addr::LOCALHOST, https).into())
                .qad_bind(None)
                .reload_interval(every);
            let config = TowerConfig::tls(tls).bind((Ipv4Addr::LOCALHOST, http).into());
            let result = Tower::spawn(config).await;
            assert!(
                matches!(result, Err(TowerError::InvalidReloadInterval)),
                "{every:?} was accepted"
            );
            for port in [http, https] {
                assert!(TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_ok());
            }
        }
        // The longest accepted interval starts a tower.
        let tls = tls_tower(&cert.cert, &cert.key).reload_interval(year);
        let (tower, _url) = spawn(config(tls)).await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

/// A path that names a directory or a device is refused before it is opened, as the
/// certificate file and as the key file, and nothing is bound.
#[tokio::test]
async fn a_path_that_is_not_a_regular_file_fails_spawn_before_binding() {
    bounded(async {
        let cert = cert();
        let directory = cert.dir.path().to_owned();
        let mut paths = vec![directory];
        if cfg!(unix) {
            paths.push(PathBuf::from("/dev/null"));
        }
        for path in &paths {
            let error = spawn_fails(path, &cert.key).await;
            expect_certificate_problem(&error, path, TlsFileProblem::NotAFile);
            let error = spawn_fails(&cert.cert, path).await;
            expect_key_problem(&error, path, TlsFileProblem::NotAFile);
        }
    })
    .await;
}

/// A symbolic link to a good file is followed, as a certbot `live/` directory needs.
#[cfg(unix)]
#[tokio::test]
async fn symbolic_links_to_good_files_are_followed() {
    bounded(async {
        let cert = cert();
        let (cert_link, key_link) = (
            cert.dir.path().join("live-cert.pem"),
            cert.dir.path().join("live-key.pem"),
        );
        std::os::unix::fs::symlink(&cert.cert, &cert_link).unwrap();
        std::os::unix::fs::symlink(&cert.key, &key_link).unwrap();
        let (tower, url) = spawn(config(tls_tower(&cert_link, &key_link))).await;
        assert_eq!(
            https(https_addr(&url), &[&cert.der], "/healthz")
                .await
                .as_deref(),
            Ok("HTTP/1.1 200 OK")
        );
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

/// One file holding the chain and the key serves as both, and the swapped-path cases still
/// give what they gave.
#[tokio::test]
async fn a_combined_file_serves_as_certificate_and_key() {
    bounded(async {
        let cert = cert();
        let combined = cert.dir.path().join("combined.pem");
        std::fs::write(&combined, format!("{}{}", cert.cert_pem, cert.key_pem)).unwrap();
        let (tower, url) = spawn(config(tls_tower(&combined, &combined))).await;
        assert_eq!(
            https(https_addr(&url), &[&cert.der], "/healthz")
                .await
                .as_deref(),
            Ok("HTTP/1.1 200 OK")
        );
        assert!(tower.shutdown().await.is_ok());

        let error = spawn_fails(&cert.key, &cert.key).await;
        expect_certificate_problem(&error, &cert.key, TlsFileProblem::NoCertificate);
        let error = spawn_fails(&cert.cert, &cert.cert).await;
        expect_key_problem(&error, &cert.cert, TlsFileProblem::NoPrivateKey);
    })
    .await;
}
