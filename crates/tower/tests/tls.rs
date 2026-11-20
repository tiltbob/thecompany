#![cfg(test)]
//! A TLS tower with a certificate from PEM files, exercised by iroh endpoints acting as stations.
//!
//! The certificate is self-signed for 127.0.0.1 and made at run time; nothing here prints an id
//! or key material.

mod common;

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

/// Spawns a tower on fixed ports with the given files, expects it to fail, checks that none of
/// the ports stayed bound, and returns the error.
async fn spawn_fails(cert: &Path, key: &Path) -> TowerError {
    let (http, https, qad) = (free_tcp_port(), free_tcp_port(), free_udp_port());
    let tls = TlsTower::new("127.0.0.1", cert, key)
        .https_bind((Ipv4Addr::LOCALHOST, https).into())
        .qad_bind(Some((Ipv4Addr::LOCALHOST, qad).into()));
    let config = TowerConfig::tls(tls).bind((Ipv4Addr::LOCALHOST, http).into());
    let Err(error) = Tower::spawn(config).await else {
        panic!("spawn accepted unusable certificate files");
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
    let TowerError::CertificateFile { problem: found } = error else {
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
    let TowerError::KeyFile { problem: found } = error else {
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

        for (path, problem) in [
            (
                &missing,
                TlsFileProblem::Unreadable(std::io::ErrorKind::NotFound),
            ),
            (&not_pem, TlsFileProblem::NoCertificate),
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

#[tokio::test]
async fn an_invalid_host_name_fails_spawn_without_repeating_it() {
    let cert = cert();
    let tls = tls_tower(&cert.cert, &cert.key);
    for name in [
        "",
        "tower/path",
        "user@tower",
        "tower_qzx.example",
        "-qzx.example",
    ] {
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

#[tokio::test]
async fn a_reload_serves_a_renewed_pair_and_keeps_the_last_good_one() {
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
        let renewed = timeout(Duration::from_secs(10), async {
            let mut tick = tokio::time::interval(RELOAD);
            while https(addr, &[&second.der], "/healthz").await.is_err() {
                tick.tick().await;
            }
        })
        .await;
        assert!(
            renewed.is_ok(),
            "the renewed certificate was not served within 10 s"
        );
        assert!(https(addr, &[&first.der], "/healthz").await.is_err());

        // Broken pairs: a certificate with another key; malformed PEM in both files; a new leaf
        // with its own key but a second certificate that does not parse. Each stays on disk for
        // several intervals, and the renewed pair stays in use throughout.
        let third = cert_in(&first.dir, "third");
        let [(_, malformed_header), (_, malformed_end)] = malformed_pem();
        let bad_chain = format!("{}{}", third.cert_pem, unparsable_certificate_section());
        for (cert_pem, key_pem) in [
            (third.cert_pem.as_str(), second.key_pem.as_str()),
            (malformed_header.as_str(), malformed_end.as_str()),
            (bad_chain.as_str(), third.key_pem.as_str()),
        ] {
            replace(&first.cert, cert_pem);
            replace(&first.key, key_pem);
            // Asserting that nothing changes: there is no event to wait for.
            tokio::time::sleep(RELOAD * 5).await;
            assert_eq!(
                https(addr, &[&second.der], "/healthz").await.as_deref(),
                Ok("HTTP/1.1 200 OK")
            );
            assert!(https(addr, &[&third.der], "/healthz").await.is_err());
        }

        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

/// A leaf for 127.0.0.1 signed by a CA, as `leaf, CA` PEM, with the leaf's key and the CA's
/// certificate.
fn chain_in(dir: &Path) -> (PathBuf, PathBuf, CertificateDer<'static>) {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "numbers test CA");
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf = rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()])
        .unwrap()
        .signed_by(&leaf_key, &issuer)
        .unwrap();
    let (cert, key) = (dir.join("chain.pem"), dir.join("chain-key.pem"));
    std::fs::write(&cert, format!("{}{}", leaf.pem(), ca.pem())).unwrap();
    std::fs::write(&key, leaf_key.serialize_pem()).unwrap();
    (cert, key, ca.der().clone())
}

/// webpki parses an intermediate or CA certificate like a leaf, so a real chain of leaf and CA
/// passes the check of every certificate, and the tower serves with it: a client that trusts the
/// CA accepts the leaf. This does not show that the CA certificate itself is sent, since a
/// client that trusts the CA needs only the leaf.
#[tokio::test]
async fn a_two_certificate_chain_is_accepted_and_served() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key, ca) = chain_in(dir.path());
        let (tower, url) = spawn(config(tls_tower(&cert, &key))).await;
        let addr = https_addr(&url);
        assert_eq!(
            https(addr, &[&ca], "/healthz").await.as_deref(),
            Ok("HTTP/1.1 200 OK")
        );
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}

/// A reload interval that the clock cannot schedule fails spawn before anything is bound.
#[tokio::test]
async fn an_unschedulable_reload_interval_fails_spawn_before_binding() {
    bounded(async {
        let cert = cert();
        let year = MAX_RELOAD_INTERVAL;
        for every in [Duration::MAX, year + Duration::from_nanos(1)] {
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
