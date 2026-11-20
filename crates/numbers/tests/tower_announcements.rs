#![cfg(test)]

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{Connection, ConnectionError, ReadError, RecvStream, SendStream};
use numbers::proto::chat::{ChatMessage, ChatReply};
use numbers::proto::towers::TowerAnnouncement;
use numbers::proto::{CHAT_PROTOCOL, Hello, HelloEcho, StreamHeader, TOWERS_PROTOCOL};
use numbers::{
    Authorizer, Endpoint, EndpointConfig, IncomingChat, IncomingTowers, IpTransport, LINK_ALPN,
    Link, LinkConfig, LinkPath, NodeId, Qad, TowerExchange, TowerSet, generate_node_key, node_id,
};
use numbers_base::{WIRE_FRAME_MAX, read_frame, write_frame};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::time::timeout;

const CASE: Duration = Duration::from_secs(18);
const CONNECT: Duration = Duration::from_secs(2);
const SEND: Duration = Duration::from_secs(3);
const QUIET: Duration = Duration::from_millis(120);
const GENERATION: [u8; 16] = [1; 16];
const CHALLENGE: [u8; 16] = [2; 16];

fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    result.unwrap_or_else(|_| panic!("{operation}"))
}

fn config() -> LinkConfig {
    LinkConfig {
        connect_timeout: CONNECT,
        send_deadline: SEND,
    }
}

fn snapshot(url: &str) -> TowerSet {
    checked(TowerSet::from_url(url), "make a known working set fixture")
}

struct Fixture {
    endpoint: Endpoint,
    auth: Arc<Authorizer>,
    address: EndpointAddr,
}

impl Fixture {
    async fn bind(key: &[u8; 32], allow_towers: bool) -> Self {
        let protocols: &'static [&'static str] = if allow_towers {
            &[CHAT_PROTOCOL, TOWERS_PROTOCOL]
        } else {
            &[CHAT_PROTOCOL]
        };
        let auth = Arc::new(Authorizer::new(&[LINK_ALPN], protocols));
        let endpoint = checked(
            Endpoint::bind(
                key,
                EndpointConfig {
                    ip_transport: IpTransport::LoopbackOnly,
                    ..EndpointConfig::default()
                },
                Arc::clone(&auth),
            )
            .await,
            "bind an isolated loopback endpoint",
        );
        let sockets = endpoint.iroh().bound_sockets();
        assert_eq!(sockets.len(), 1);
        assert!(sockets[0].ip() == Ipv4Addr::LOCALHOST);
        let address = EndpointAddr::new(endpoint.iroh().id()).with_ip_addr(sockets[0]);
        Self {
            endpoint,
            auth,
            address,
        }
    }

    fn id(&self) -> NodeId {
        self.endpoint.iroh().id()
    }

    fn allow(&self, peer: &Self, name: &str) {
        self.auth.replace_peers([(peer.id(), name.to_owned())]);
        checked(self.endpoint.iroh().address_lookup(), "read test lookup")
            .add(MemoryLookup::from_endpoint_info([peer.address.clone()]));
    }
}

async fn pair() -> (Fixture, Fixture) {
    let first = Fixture::bind(&generate_node_key(), true).await;
    let second = Fixture::bind(&generate_node_key(), true).await;
    first.allow(&second, "second");
    second.allow(&first, "first");
    (first, second)
}

async fn frame<T: DeserializeOwned>(recv: &mut RecvStream) -> T {
    checked(
        read_frame(recv, WIRE_FRAME_MAX, Some(CONNECT)).await,
        "read complete test frame",
    )
}

async fn write<T: Serialize>(send: &mut SendStream, value: &T) {
    checked(
        write_frame(send, value, WIRE_FRAME_MAX).await,
        "write complete test frame",
    );
}

async fn dial(peer: &Fixture, address: &EndpointAddr) -> Connection {
    checked(
        peer.endpoint
            .iroh()
            .connect(address.clone(), LINK_ALPN)
            .await,
        "dial the test endpoint over loopback",
    )
}

async fn raw_client_hello(connection: &Connection) {
    let (mut send, mut recv) = checked(connection.open_bi().await, "open test hello");
    let hello = Hello::local(GENERATION, CHALLENGE);
    write(&mut send, &hello).await;
    let response: Hello = frame(&mut recv).await;
    assert!(response.validate().is_ok());
    assert!(response.echo == Some(CHALLENGE));
    write(
        &mut send,
        &HelloEcho {
            echo: response.challenge,
        },
    )
    .await;
    checked(send.finish(), "finish test hello");
}

async fn raw_server_hello(peer: &Fixture) -> Connection {
    let incoming = peer
        .endpoint
        .iroh()
        .accept()
        .await
        .expect("raw endpoint is listening");
    let accepting = checked(incoming.accept(), "accept outgoing loopback handshake");
    let connection = checked(accepting.await, "complete outgoing loopback handshake");
    let (mut send, mut recv) = checked(connection.accept_bi().await, "accept outgoing hello");
    let hello: Hello = frame(&mut recv).await;
    assert!(hello.validate().is_ok() && hello.echo.is_none());
    let mut response = Hello::local(GENERATION, CHALLENGE);
    response.echo = Some(hello.challenge);
    write(&mut send, &response).await;
    checked(send.finish(), "finish raw server hello");
    let echo: HelloEcho = frame(&mut recv).await;
    assert!(echo.echo == CHALLENGE);
    connection
}

async fn read_request(connection: &Connection) -> (StreamHeader, Value, SendStream) {
    let (send, mut recv) = checked(connection.accept_bi().await, "accept test stream");
    let header = frame(&mut recv).await;
    let body = frame(&mut recv).await;
    (header, body, send)
}

async fn reply(mut send: SendStream, value: &Value) {
    write(&mut send, value).await;
    let _ = send.finish();
}

async fn read_towers(connection: &Connection, url: &str, response: Value) {
    let (header, body, send) = read_request(connection).await;
    assert_eq!(header, StreamHeader::towers());
    assert_eq!(body, json!({"towers":[{"url":url,"qad_port":7842}]}));
    reply(send, &response).await;
}

async fn open_request<T: Serialize>(
    connection: &Connection,
    header: &StreamHeader,
    body: &T,
) -> RecvStream {
    // Pack both frames so a header-level refusal can race the reply on its own.
    let mut request = Vec::new();
    checked(
        write_frame(&mut request, header, WIRE_FRAME_MAX).await,
        "encode test header",
    );
    checked(
        write_frame(&mut request, body, WIRE_FRAME_MAX).await,
        "encode test body",
    );
    let (mut send, recv) = checked(connection.open_bi().await, "open test request");
    checked(send.write_all(&request).await, "write both test frames");
    let _ = send.finish();
    recv
}

async fn tower_request(connection: &Connection, value: &Value) -> RecvStream {
    open_request(connection, &StreamHeader::towers(), value).await
}

async fn next(exchange: &mut TowerExchange) -> IncomingTowers {
    checked(
        timeout(CONNECT, exchange.recv()).await,
        "receive an announcement",
    )
    .expect("tower inbox is open")
}

fn commit(incoming: IncomingTowers, peer: NodeId, name: &str) -> TowerAnnouncement {
    let mut result = None;
    assert!(incoming.accept_if_current(|station, caller, announcement| {
        assert!(station == peer, "authenticated peer matches");
        assert!(caller == name, "authorized name matches");
        result = Some(announcement);
        true
    }));
    result.expect("callback committed the snapshot")
}

fn chat() -> ChatMessage {
    ChatMessage {
        id: "probe".into(),
        sent_at_ms: 100,
        body: "keep chat available".into(),
    }
}

async fn expect_chat(inbox: &mut mpsc::Receiver<IncomingChat>, station: NodeId) {
    let message = inbox.recv().await.expect("chat inbox is open");
    assert!(message.station_id == station && message.message == chat());
    message.respond(ChatReply::Ack);
}

async fn raw_chat(
    connection: &Connection,
    inbox: &mut mpsc::Receiver<IncomingChat>,
    station: NodeId,
) {
    let mut reply = open_request(connection, &StreamHeader::chat(), &chat()).await;
    expect_chat(inbox, station).await;
    let reply: ChatReply = frame(&mut reply).await;
    assert_eq!(reply, ChatReply::Ack);
}

fn nack(reason: &str) -> Value {
    json!({"type":"nack","reason":reason})
}

fn ack() -> Value {
    json!({"type":"ack"})
}

async fn assert_no_stream(connection: &Connection) {
    assert!(timeout(QUIET, connection.accept_bi()).await.is_err());
}

#[tokio::test]
async fn both_directions_announce_and_chat_over_the_same_loopback_link() {
    checked(
        timeout(CASE, async {
            let (left, right) = pair().await;
            let left_id = left.id();
            let right_id = right.id();
            let (left_link, _, mut left_exchange) = Link::start_with_towers(
                left.endpoint,
                config(),
                &snapshot("https://left.invalid/"),
            );
            let (right_link, mut inbox, mut right_exchange) =
                Link::start_with_towers(right.endpoint, config(), &TowerSet::default());
            let handle = left_link.handle();
            let (delivered, (), left_snapshot, right_snapshot) = tokio::join!(
                handle.send(right_id, chat()),
                expect_chat(&mut inbox, left_id),
                next(&mut left_exchange),
                next(&mut right_exchange),
            );
            assert_eq!(delivered, Ok(ChatReply::Ack));
            let received = commit(left_snapshot, right_id, "second");
            assert!(received.entries().is_empty());
            let received = commit(right_snapshot, left_id, "first");
            assert_eq!(received.entries().len(), 1);
            assert_eq!(received.entries()[0].url(), "https://left.invalid/");
            assert_eq!(received.entries()[0].qad(), Qad::Port(7842));
            assert_eq!(handle.path(right_id), LinkPath::Direct);
            checked(left_link.shutdown().await, "stop the announcing link");
            checked(right_link.shutdown().await, "stop its peer");
            assert!(left_exchange.recv().await.is_none() && right_exchange.recv().await.is_none());
            assert_eq!(handle.path(right_id), LinkPath::Down);
        })
        .await,
        "bidirectional announcement test completed",
    );
}

#[tokio::test]
async fn a_same_path_replacement_announces_the_new_snapshot_and_invalidates_old_items() {
    checked(
        timeout(CASE, async {
            let (local, remote) = pair().await;
            let address = local.address.clone();
            let peer = remote.id();
            let (link, _chat, mut exchange) = Link::start_with_towers(
                local.endpoint,
                config(),
                &snapshot("https://first.invalid/"),
            );
            let handle = link.handle();
            let first = dial(&remote, &address).await;
            raw_client_hello(&first).await;
            read_towers(&first, "https://first.invalid/", ack()).await;
            let _pending = tower_request(&first, &json!({"towers":[]})).await;
            let obsolete = next(&mut exchange).await;
            assert_eq!(handle.path(peer), LinkPath::Direct);
            let mut paths = handle.subscribe_paths();
            exchange.set_working_towers(&snapshot("https://second.invalid/"));
            assert_no_stream(&first).await;

            let second = dial(&remote, &address).await;
            raw_client_hello(&second).await;
            read_towers(&second, "https://second.invalid/", ack()).await;
            assert!(
                matches!(first.closed().await, ConnectionError::ApplicationClosed(close)
            if close.error_code.into_inner() == 409)
            );
            assert_eq!(handle.path(peer), LinkPath::Direct);
            assert!(matches!(
                paths.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ));
            assert!(!obsolete.accept_if_current(|_, _, _| panic!("replaced item must not commit")));

            let mut response = tower_request(
                &second,
                &json!({"towers":[
                    {"url":"https://candidate.invalid/","qad_port":8610}
                ]}),
            )
            .await;
            let committed = commit(next(&mut exchange).await, peer, "second");
            assert_eq!(frame::<Value>(&mut response).await, ack());
            second.close(0u32.into(), b"");
            checked(link.shutdown().await, "stop the local link");
            assert_eq!(committed.entries()[0].url(), "https://candidate.invalid/");
            assert_eq!(committed.entries()[0].qad(), Qad::Port(8610));
            checked(
                timeout(CONNECT, remote.endpoint.iroh().close()).await,
                "close raw endpoint",
            );
        })
        .await,
        "replacement announcement test completed",
    );
}

#[tokio::test]
async fn revoke_and_readd_never_revalidates_a_queued_announcement() {
    checked(
        timeout(CASE, async {
            let (local, remote) = pair().await;
            let address = local.address.clone();
            let policy = Arc::clone(&local.auth);
            let peer = remote.id();
            let (link, _chat, mut exchange) = Link::start_with_towers(
                local.endpoint,
                config(),
                &snapshot("https://local.invalid/"),
            );
            let handle = link.handle();
            let first = dial(&remote, &address).await;
            raw_client_hello(&first).await;
            read_towers(&first, "https://local.invalid/", ack()).await;
            let _reply = tower_request(&first, &json!({"towers":[]})).await;
            let revoked = next(&mut exchange).await;

            policy.replace_peers([]);
            handle.close_peer(peer);
            policy.replace_peers([(peer, "second".to_owned())]);
            let second = dial(&remote, &address).await;
            raw_client_hello(&second).await;
            read_towers(&second, "https://local.invalid/", ack()).await;
            assert!(!revoked.accept_if_current(|_, _, _| panic!("revoked item must not commit")));

            let mut response = tower_request(&second, &json!({"towers":[]})).await;
            assert!(
                commit(next(&mut exchange).await, peer, "second")
                    .entries()
                    .is_empty()
            );
            assert_eq!(frame::<Value>(&mut response).await, ack());
            checked(link.shutdown().await, "stop the local link");
            remote.endpoint.iroh().close().await;
        })
        .await,
        "peer revocation announcement test completed",
    );
}

async fn closed_without_a_stream(connection: &Connection, code: u64) {
    assert!(matches!(connection.accept_bi().await,
        Err(ConnectionError::ApplicationClosed(close)) if close.error_code.into_inner() == code));
}

#[tokio::test]
async fn a_losing_or_timed_out_hello_never_announces() {
    checked(
        timeout(CASE, async {
            let local_key = generate_node_key();
            let remote_key = generate_node_key();
            let (lower, higher) = if node_id(&local_key) < node_id(&remote_key) {
                (local_key, remote_key)
            } else {
                (remote_key, local_key)
            };
            let local = Fixture::bind(&lower, true).await;
            let remote = Fixture::bind(&higher, true).await;
            local.allow(&remote, "second");
            remote.allow(&local, "first");
            let address = local.address.clone();
            // The raw side controls the cross-dial hello and reads the first wire attempts.
            remote.endpoint.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
            let peer = remote.id();
            let (link, _chat, mut exchange) = Link::start_with_towers(
                local.endpoint,
                config(),
                &snapshot("https://local.invalid/"),
            );
            let handle = link.handle();

            let raw = async {
                let winner = raw_server_hello(&remote).await;
                let mut saw_chat = false;
                let mut saw_towers = false;
                for _ in 0..2 {
                    let (header, body, send) = read_request(&winner).await;
                    if header == StreamHeader::chat() {
                        assert!(!saw_chat);
                        assert!(
                            body == checked(serde_json::to_value(chat()), "encode expected chat")
                        );
                        saw_chat = true;
                        reply(send, &ack()).await;
                    } else {
                        assert_eq!(header, StreamHeader::towers());
                        assert!(!saw_towers);
                        assert_eq!(
                            body,
                            json!({"towers":[{"url":"https://local.invalid/","qad_port":7842}]})
                        );
                        saw_towers = true;
                        // Model a chat-only peer. The old reply must end this single attempt.
                        reply(send, &nack("unknown_protocol")).await;
                    }
                }
                assert!(saw_chat && saw_towers);
                winner
            };
            let (delivered, winner) = tokio::join!(handle.send(peer, chat()), raw);
            assert_eq!(delivered, Ok(ChatReply::Ack));
            assert_no_stream(&winner).await;

            // The existing outgoing link was initiated by the lower ID in this generation.
            let loser = dial(&remote, &address).await;
            raw_client_hello(&loser).await;
            closed_without_a_stream(&loser, 409).await;
            let silent = dial(&remote, &address).await;
            closed_without_a_stream(&silent, 400).await;
            assert!(winner.close_reason().is_none());
            assert_eq!(handle.path(peer), LinkPath::Direct);
            assert!(timeout(QUIET, exchange.recv()).await.is_err());
            checked(link.shutdown().await, "stop the local link");
            remote.endpoint.iroh().close().await;
        })
        .await,
        "losing and timed-out hello test completed",
    );
}

async fn expect_reset(recv: &mut RecvStream, code: u64) {
    assert!(matches!(recv.read(&mut [0]).await,
        Err(ReadError::Reset(reset)) if reset.into_inner() == code));
}

#[tokio::test]
async fn legacy_version_and_stream_gate_refusals_leave_chat_available() {
    checked(
        timeout(CASE, async {
            for (opt_in, permit_towers) in [(false, true), (true, false), (true, true)] {
                let local = Fixture::bind(&generate_node_key(), permit_towers).await;
                let remote = Fixture::bind(&generate_node_key(), true).await;
                local.allow(&remote, "second");
                remote.allow(&local, "first");
                let address = local.address.clone();
                let peer = remote.id();
                let (link, mut chats, mut exchange) = if opt_in {
                    let (link, chats, exchange) = Link::start_with_towers(
                        local.endpoint,
                        config(),
                        &snapshot("https://local.invalid/"),
                    );
                    (link, chats, Some(exchange))
                } else {
                    let (link, chats) = Link::start(local.endpoint, config());
                    (link, chats, None)
                };
                let connection = dial(&remote, &address).await;
                raw_client_hello(&connection).await;
                let empty = json!({"towers":[]});
                if !opt_in {
                    let mut response = tower_request(&connection, &empty).await;
                    assert_eq!(
                        frame::<Value>(&mut response).await,
                        nack("unknown_protocol")
                    );
                } else if !permit_towers {
                    let mut response = tower_request(&connection, &empty).await;
                    expect_reset(&mut response, 403).await;
                    assert!(
                        timeout(QUIET, exchange.as_mut().unwrap().recv())
                            .await
                            .is_err()
                    );
                } else {
                    read_towers(&connection, "https://local.invalid/", ack()).await;
                    let mut header = StreamHeader::towers();
                    header.v += 1;
                    let mut response = open_request(&connection, &header, &empty).await;
                    assert_eq!(
                        frame::<Value>(&mut response).await,
                        nack("unsupported_version")
                    );
                    let mut response = tower_request(&connection, &json!({"towers":[
                    {"url":"https://valid.invalid/"}, {"url":"https://NONCANONICAL.invalid/"}
                ]})).await;
                    assert_eq!(frame::<Value>(&mut response).await, nack("invalid_towers"));
                    assert!(
                        timeout(QUIET, exchange.as_mut().unwrap().recv())
                            .await
                            .is_err()
                    );
                    drop(exchange.take());
                    let mut response = tower_request(&connection, &empty).await;
                    assert_eq!(frame::<Value>(&mut response).await, nack("unavailable"));
                }
                raw_chat(&connection, &mut chats, peer).await;
                assert_no_stream(&connection).await;
                checked(link.shutdown().await, "stop the local link");
                remote.endpoint.iroh().close().await;
            }
        })
        .await,
        "tower version and gate tests completed",
    );
}

#[tokio::test]
async fn extra_recognized_streams_cannot_fill_the_shared_receive_slots() {
    checked(
        timeout(CASE, async {
            let (local, remote) = pair().await;
            let address = local.address.clone();
            let peer = remote.id();
            let (link, mut chats, mut exchange) = Link::start_with_towers(
                local.endpoint,
                config(),
                &snapshot("https://local.invalid/"),
            );
            let connection = dial(&remote, &address).await;
            raw_client_hello(&connection).await;
            read_towers(&connection, "https://local.invalid/", ack()).await;

            let empty = json!({"towers":[]});
            let mut pending = tower_request(&connection, &empty).await;
            let held = next(&mut exchange).await;
            let mut extras = Vec::new();
            for _ in 0..18 {
                extras.push(tower_request(&connection, &empty).await);
            }
            raw_chat(&connection, &mut chats, peer).await;
            for mut extra in extras {
                assert_eq!(frame::<Value>(&mut extra).await, nack("unavailable"));
            }
            assert!(commit(held, peer, "second").entries().is_empty());
            assert_eq!(frame::<Value>(&mut pending).await, ack());
            assert!(timeout(QUIET, exchange.recv()).await.is_err());

            checked(link.shutdown().await, "stop the local link");
            remote.endpoint.iroh().close().await;
        })
        .await,
        "concurrent tower receive test completed",
    );
}

#[tokio::test]
async fn a_full_mailbox_waits_for_capacity_without_blocking_chat_or_shutdown() {
    checked(
        timeout(CASE, async {
            let (local, remote) = pair().await;
            let address = local.address.clone();
            let peer = remote.id();
            let (link, mut chats, mut exchange) = Link::start_with_towers(
                local.endpoint,
                LinkConfig {
                    send_deadline: Duration::from_millis(180),
                    ..config()
                },
                &snapshot("https://local.invalid/"),
            );
            let handle = link.handle();
            let connection = dial(&remote, &address).await;
            raw_client_hello(&connection).await;
            read_towers(&connection, "https://local.invalid/", ack()).await;

            let empty = json!({"towers":[]});
            // A timed-out sender loses confirmation, but the valid queued item remains.
            for _ in 0..32 {
                let mut timed_out = tower_request(&connection, &empty).await;
                expect_reset(&mut timed_out, 0).await;
            }
            let mut overflow = tower_request(&connection, &empty).await;
            expect_reset(&mut overflow, 0).await;

            let mut waiting = tower_request(
                &connection,
                &json!({"towers":[
                    {"url":"https://waiting.invalid/"}
                ]}),
            )
            .await;
            raw_chat(&connection, &mut chats, peer).await;
            assert!(
                commit(next(&mut exchange).await, peer, "second")
                    .entries()
                    .is_empty()
            );
            for _ in 1..32 {
                drop(next(&mut exchange).await);
            }
            let available = commit(next(&mut exchange).await, peer, "second");
            assert_eq!(available.entries().len(), 1);
            assert_eq!(available.entries()[0].url(), "https://waiting.invalid/");
            assert_eq!(frame::<Value>(&mut waiting).await, ack());

            let mut held = tower_request(&connection, &empty).await;
            expect_reset(&mut held, 0).await;
            checked(
                link.shutdown().await,
                "stop while an announcement remains queued",
            );
            let stale = next(&mut exchange).await;
            assert!(!stale.accept_if_current(|_, _, _| panic!("stopped link cannot commit")));
            assert!(exchange.recv().await.is_none());
            assert_eq!(handle.path(peer), LinkPath::Down);
            remote.endpoint.iroh().close().await;
        })
        .await,
        "bounded tower mailbox test completed",
    );
}
