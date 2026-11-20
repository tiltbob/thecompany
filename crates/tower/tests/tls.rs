#![cfg(test)]
//! A TLS tower with a certificate from PEM files, exercised by iroh endpoints acting as stations.
//!
//! The certificate is self-signed for 127.0.0.1 and made at run time; nothing here prints an id
//! or key material.

mod common;

use std::net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::time::Duration;

use common::{
    DENY_REASON, Station, bounded, connected, denial, online, spawn, status, stays_pending,
};
use iroh::tls::CaTlsConfig;
use iroh::{RelayConfig, RelayUrl, SecretKey, Watcher};
use iroh_relay::RelayQuicConfig;
use numbers_tower::{TlsTower, Tower, TowerConfig, TowerError};
use rustls::pki_types::CertificateDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// How long an endpoint that does not trust the certificate must stay offline.
const UNTRUSTED_FOR: Duration = Duration::from_secs(5);
/// How long the QAD counter may take to catch up with an endpoint that came online.
const QAD_COUNTED_WITHIN: Duration = Duration::from_secs(5);

/// A self-signed certificate for 127.0.0.1, written as PEM files into a temporary directory.
struct Cert {
    der: CertificateDer<'static>,
    cert: PathBuf,
    key: PathBuf,
    key_pem: String,
    dir: tempfile::TempDir,
}

fn cert() -> Cert {
    let generated = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])
        .unwrap_or_else(|_| panic!("generate a certificate"));
    let dir = tempfile::tempdir().unwrap_or_else(|_| panic!("make a temporary directory"));
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    let key_pem = generated.signing_key.serialize_pem();
    std::fs::write(&cert, generated.cert.pem()).unwrap_or_else(|_| panic!("write cert"));
    std::fs::write(&key, &key_pem).unwrap_or_else(|_| panic!("write key"));
    Cert {
        der: generated.cert.der().clone(),
        cert,
        key,
        key_pem,
        dir,
    }
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

/// The error's Display, Debug and every source, as text.
fn texts(error: &TowerError) -> Vec<String> {
    let mut texts = vec![error.to_string(), format!("{error:?}")];
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        texts.push(cause.to_string());
        source = cause.source();
    }
    texts
}

/// Fails if any text contains a line of the key's base64 body.
fn assert_no_key_material(texts: &[String], cert: &Cert) {
    for line in cert
        .key_pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
    {
        assert!(
            texts.iter().all(|text| !text.contains(line)),
            "an error quotes the key"
        );
    }
}

#[tokio::test]
async fn unusable_certificate_files_fail_spawn_before_binding() {
    bounded(async {
        let cert = cert();
        let dir = cert.dir.path();
        let missing = dir.join("missing.pem");
        let not_pem = dir.join("not-pem.txt");
        std::fs::write(&not_pem, "this is not a PEM file\n").unwrap();
        let malformed = dir.join("malformed.pem");
        std::fs::write(
            &malformed,
            "-----BEGIN CERTIFICATE-----\n!!not base64!!\n-----END CERTIFICATE-----\n",
        )
        .unwrap();

        // Certificate file problems name the certificate file.
        for (path, expected) in [
            (&missing, "No such file"),
            (&not_pem, "holds no PEM certificate"),
            (&malformed, "malformed PEM section"),
            // Paths swapped by mistake: the key file holds no certificate.
            (&cert.key, "holds no PEM certificate"),
        ] {
            let error = spawn_fails(path, &cert.key).await;
            let TowerError::CertificateFile { path: reported, .. } = &error else {
                panic!("expected a certificate file error, got: {error}");
            };
            assert!(reported == path);
            let texts = texts(&error);
            assert!(
                texts[0].contains(&path.display().to_string()),
                "{}",
                texts[0]
            );
            assert!(texts[0].contains(expected), "{}", texts[0]);
            assert_no_key_material(&texts, &cert);
        }

        // Key file problems name the key file.
        for (path, expected) in [
            (&missing, "No such file"),
            (&not_pem, "no key found"),
            (&cert.cert, "no key found"),
        ] {
            let error = spawn_fails(&cert.cert, path).await;
            let TowerError::KeyFile { path: reported, .. } = &error else {
                panic!("expected a key file error, got: {error}");
            };
            assert!(reported == path);
            let texts = texts(&error);
            assert!(
                texts[0].contains(&path.display().to_string()),
                "{}",
                texts[0]
            );
            assert!(texts[0].contains(expected), "{}", texts[0]);
            assert_no_key_material(&texts, &cert);
        }
    })
    .await;
}

#[tokio::test]
async fn an_invalid_host_name_fails_spawn() {
    let cert = cert();
    let tls = tls_tower(&cert.cert, &cert.key);
    for name in ["", "tower/path", "user@tower"] {
        let mut tls = tls.clone();
        name.clone_into(&mut tls.hostname);
        let result = Tower::spawn(config(tls)).await;
        assert!(matches!(result, Err(TowerError::InvalidHostname(h)) if h == name));
    }
}
