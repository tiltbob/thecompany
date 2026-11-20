use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{TransportAddr, Watcher};
use numbers_tower::{TlsTower, Tower, TowerConfig};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};
use tokio::time::{sleep, timeout};

use crate::testing::FakeDns;
use crate::{
    Authorizer, DnsConfig, Endpoint, EndpointBindError, EndpointConfig, IpTransport, TlsRoots,
    TowerEntry, TowerSet, TowerStatus, generate_node_key,
};

const ALPN: &[u8] = b"test/tls-endpoint/0";
const IP_HOST: &str = "127.0.0.1";
const DNS_HOST: &str = "tower1.example.test";
const TEST_TIMEOUT: Duration = Duration::from_secs(45);

fn checked<T, E>(result: Result<T, E>, message: &str) -> T {
    result.unwrap_or_else(|_| panic!("{message}"))
}

fn loopback() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

fn private_ca() -> (Issuer<'static, KeyPair>, Vec<u8>) {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "numbers test CA");
    let key = checked(KeyPair::generate(), "generate a test CA key");
    let root = checked(params.self_signed(&key), "generate a test CA certificate");
    (Issuer::new(params, key), root.der().to_vec())
}

struct TlsFixture {
    tower: Tower,
    root: Vec<u8>,
    _cert_dir: tempfile::TempDir,
}

impl TlsFixture {
    async fn start(host: &str, qad: bool) -> Self {
        let (issuer, root) = private_ca();
        let mut params = checked(
            CertificateParams::new(vec![host.to_owned()]),
            "set the test tower certificate name",
        );
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = checked(KeyPair::generate(), "generate a test tower key");
        let cert = checked(
            params.signed_by(&key, &issuer),
            "issue a test tower certificate",
        );
        let cert_dir = checked(tempfile::tempdir(), "make a test certificate directory");
        let cert_path = cert_dir.path().join("cert.pem");
        let key_path = cert_dir.path().join("key.pem");
        checked(
            std::fs::write(&cert_path, cert.pem()),
            "write test certificate",
        );
        checked(
            std::fs::write(&key_path, key.serialize_pem()),
            "write test key",
        );
        let tls = TlsTower::new(host, cert_path, key_path)
            .https_bind(loopback())
            .qad_bind(qad.then(loopback));
        let tower = checked(
            Tower::spawn(TowerConfig::tls(tls).bind(loopback())).await,
            "start loopback TLS tower",
        );
        Self {
            tower,
            root,
            _cert_dir: cert_dir,
        }
    }

    fn towers(&self) -> TowerSet {
        let entry = checked(
            TowerEntry::from_url(self.tower.url()),
            "parse TLS relay URL",
        );
        let entry = match self.tower.qad_port() {
            Some(port) => checked(entry.with_qad_port(port), "set random QAD port"),
            None => entry.without_qad(),
        };
        checked(TowerSet::from_entries([entry]), "configure TLS tower")
    }
}

async fn bind(
    towers: TowerSet,
    tls_roots: TlsRoots,
    dns: DnsConfig,
    authorizer: Arc<Authorizer>,
) -> Endpoint {
    checked(
        Endpoint::bind(
            &generate_node_key(),
            EndpointConfig {
                towers,
                ip_transport: IpTransport::RelayOnly,
                tls_roots,
                dns,
            },
            authorizer,
        )
        .await,
        "bind relay-only TLS endpoint",
    )
}

#[derive(Debug)]
struct EchoChat;

impl ProtocolHandler for EchoChat {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut receive) = connection
            .accept_bi()
            .await
            .map_err(AcceptError::from_err)?;
        let chat = receive
            .read_to_end(1024)
            .await
            .map_err(AcceptError::from_err)?;
        send.write_all(&chat).await.map_err(AcceptError::from_err)?;
        send.finish().map_err(AcceptError::from_err)?;
        connection.closed().await;
        Ok(())
    }
}

fn has_non_loopback_ipv4() -> bool {
    let Ok(socket) = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) else {
        return false;
    };
    // Like the tower's QAD test, use a UDP connect to a documentation address to
    // select a source address without sending any packets.
    if socket.connect((Ipv4Addr::new(192, 0, 2, 1), 9)).is_err() {
        return false;
    }
    socket
        .local_addr()
        .is_ok_and(|addr| !addr.ip().is_loopback() && !addr.ip().is_unspecified())
}

#[tokio::test]
async fn custom_roots_enable_bare_id_chat_through_a_tls_tower() {
    timeout(TEST_TIMEOUT, async {
        let fixture = TlsFixture::start(IP_HOST, false).await;
        let first_policy = Arc::new(Authorizer::new(&[ALPN], &[]));
        let second_policy = Arc::new(Authorizer::new(&[ALPN], &[]));
        let (first, second) = tokio::join!(
            bind(fixture.towers(), TlsRoots::CustomOnly(vec![fixture.root.clone()]), DnsConfig::System, first_policy.clone()),
            bind(fixture.towers(), TlsRoots::WebPkiPlus(vec![fixture.root.clone()]), DnsConfig::System, second_policy.clone()),
        );
        first_policy.replace_peers([(second.iroh().id(), "second".into())]);
        second_policy.replace_peers([(first.iroh().id(), "first".into())]);
        let first_router = first.router_builder().accept(ALPN, EchoChat).spawn();
        let second_router = second.router_builder().accept(ALPN, EchoChat).spawn();
        assert!(first.iroh().bound_sockets().is_empty());
        assert!(second.iroh().bound_sockets().is_empty());
        tokio::join!(first.iroh().online(), second.iroh().online());

        let connection = checked(first.iroh().connect(second.iroh().id(), ALPN).await, "dial peer by id through TLS");
        let (mut send, mut receive) = checked(connection.open_bi().await, "open chat stream");
        let message = b"hello through the TLS tower";
        checked(send.write_all(message).await, "send chat");
        checked(send.finish(), "finish chat send");
        assert_eq!(checked(receive.read_to_end(1024).await, "receive chat echo"), message);
        let mut paths = connection.paths_stream();
        let selected = paths.wait_any(|path| path.is_selected()).await;
        assert!(selected.is_some_and(|path| {
            matches!(path.remote_addr(), TransportAddr::Relay(url) if url.as_str() == fixture.tower.url())
        }));
        assert!(connection.paths().iter().all(|path| !path.is_ip()));
        connection.close(0u32.into(), b"finished");

        let (first_stop, second_stop) = tokio::join!(first_router.shutdown(), second_router.shutdown());
        checked(first_stop, "stop first endpoint");
        checked(second_stop, "stop second endpoint");
        checked(fixture.tower.shutdown().await, "stop TLS tower");
    })
    .await
    .expect("TLS chat timed out");
}

#[tokio::test]
async fn endpoint_net_report_uses_the_configured_qad_port() {
    if !has_non_loopback_ipv4() {
        println!("skipped QAD: iroh needs a usable non-loopback IPv4 address to plan this probe");
        return;
    }

    timeout(TEST_TIMEOUT, async {
        let fixture = TlsFixture::start(IP_HOST, true).await;
        assert!(fixture.tower.qad_port().is_some());
        let config = EndpointConfig {
            towers: fixture.towers(),
            ip_transport: IpTransport::LoopbackOnly,
            tls_roots: TlsRoots::CustomOnly(vec![fixture.root]),
            dns: DnsConfig::System,
        };
        let endpoint = checked(
            Endpoint::bind(
                &generate_node_key(),
                config,
                Arc::new(Authorizer::new(&[ALPN], &[])),
            )
            .await,
            "bind TLS endpoint with a loopback IP socket",
        );
        endpoint.iroh().online().await;
        timeout(Duration::from_secs(5), async {
            while fixture.tower.stats().qad_connections == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the endpoint's net report never reached the configured QAD port");
        endpoint.iroh().close().await;
        checked(fixture.tower.shutdown().await, "stop QAD-enabled TLS tower");
    })
    .await
    .expect("TLS endpoint QAD test timed out");
}

#[tokio::test]
async fn an_untrusted_tower_stays_offline_and_unknown() {
    timeout(TEST_TIMEOUT, async {
        let fixture = TlsFixture::start(IP_HOST, false).await;
        let endpoint = bind(
            fixture.towers(),
            TlsRoots::WebPki,
            DnsConfig::System,
            Arc::new(Authorizer::new(&[ALPN], &[])),
        )
        .await;
        assert!(
            timeout(Duration::from_secs(5), endpoint.iroh().online())
                .await
                .is_err(),
            "untrusted tower came online"
        );
        assert!(endpoint.iroh().home_relay_status().get().is_empty());
        let states = checked(endpoint.tower_states(), "read untrusted tower state");
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].url(), fixture.tower.url());
        assert!(!states[0].is_home);
        assert_eq!(states[0].status, TowerStatus::Unknown);
        endpoint.iroh().close().await;
        checked(fixture.tower.shutdown().await, "stop untrusted TLS tower");
    })
    .await
    .expect("untrusted tower test timed out");
}

#[tokio::test]
async fn the_configured_dns_server_resolves_the_tls_tower() {
    timeout(TEST_TIMEOUT, async {
        let dns = checked(FakeDns::start().await, "start loopback DNS server");
        dns.set_a(DNS_HOST);
        let fixture = TlsFixture::start(DNS_HOST, false).await;
        let endpoint = bind(
            fixture.towers(),
            TlsRoots::CustomOnly(vec![fixture.root]),
            DnsConfig::Nameserver(dns.addr()),
            Arc::new(Authorizer::new(&[ALPN], &[])),
        )
        .await;
        endpoint.iroh().online().await;
        assert!(
            endpoint
                .iroh()
                .home_relay_status()
                .get()
                .iter()
                .any(|relay| {
                    relay.url().as_str() == fixture.tower.url() && relay.is_connected()
                })
        );
        assert!(dns.query_count_for(DNS_HOST, "A") > 0);
        assert!(dns.queries().iter().all(|query| {
            query.name == DNS_HOST && matches!(query.qtype.as_str(), "A" | "AAAA")
        }));
        endpoint.iroh().close().await;
        checked(fixture.tower.shutdown().await, "stop DNS-named TLS tower");
    })
    .await
    .expect("TLS tower through configured DNS did not come online");
}

#[test]
fn custom_only_excludes_the_bundled_webpki_roots() {
    let (_, root) = private_ca();
    for (tls_roots, expected_mode) in [
        (TlsRoots::CustomOnly(vec![root.clone()]), "ExtraRootsOnly"),
        (TlsRoots::WebPkiPlus(vec![root]), "EmbeddedWebPki"),
        (TlsRoots::WebPki, "EmbeddedWebPki"),
    ] {
        let config = checked(tls_roots.ca_tls_config(), "build the TLS config");
        // iroh-relay 1.5's `tls.rs` exposes the trust mode through CaTlsConfig's Debug.
        // Check the mode offline without presenting a certificate from a public CA.
        assert!(
            format!("{config:?}").contains(&format!("mode: {expected_mode}")),
            "expected the TLS trust mode {expected_mode}"
        );
    }
}

#[tokio::test]
async fn binding_accepts_webpki_with_no_extra_roots() {
    let endpoint = bind(
        TowerSet::default(),
        TlsRoots::WebPkiPlus(vec![]),
        DnsConfig::System,
        Arc::new(Authorizer::new(&[ALPN], &[])),
    )
    .await;
    endpoint.iroh().close().await;
}

#[tokio::test]
async fn binding_rejects_empty_custom_roots_and_any_malformed_root() {
    let (_, root) = private_ca();
    let malformed = b"malformed certificate fixture".to_vec();
    for tls_roots in [
        TlsRoots::CustomOnly(vec![]),
        TlsRoots::CustomOnly(vec![root.clone(), malformed.clone()]),
        TlsRoots::WebPkiPlus(vec![root, malformed]),
    ] {
        let config = EndpointConfig {
            tls_roots,
            ..EndpointConfig::default()
        };
        let result = Endpoint::bind(
            &generate_node_key(),
            config,
            Arc::new(Authorizer::new(&[ALPN], &[])),
        )
        .await;
        let Err(error @ EndpointBindError::InvalidTlsRoots) = result else {
            panic!("binding must reject invalid TLS roots");
        };
        assert_eq!(
            error.to_string(),
            "invalid TLS roots for the numbers endpoint"
        );
        assert_eq!(format!("{error:?}"), error.to_string());
    }
}
