#![cfg(test)]

use std::future::Future;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::time::Duration;

use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{
    ConnectError, ConnectingError, Connection, ConnectionError, ReadError, RecvStream, SendStream,
    Side,
};
use numbers::link::{IncomingChat, LINK_ALPN, Link, LinkConfig, LinkHandle, LinkPath, SendError};
use numbers::proto::chat::{CHAT_BODY_MAX, ChatMessage, ChatReply, NackReason, ReceiptWindow};
use numbers::proto::{CHAT_PROTOCOL, CHAT_VERSION, Hello, HelloEcho, StreamHeader};
use numbers::{
    Authorizer, Endpoint, EndpointConfig, IpTransport, NodeId, TowerSet, generate_node_key, node_id,
};
use numbers_base::{FrameError, WIRE_FRAME_MAX, read_frame, write_frame};
use numbers_tower::{Tower, TowerConfig};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use tokio::sync::mpsc::{Receiver, error::TryRecvError};
use tokio::sync::{broadcast, oneshot};
use tokio::time::{Instant, sleep, timeout, timeout_at};

const CASE_TIMEOUT: Duration = Duration::from_secs(45);
// A loaded test runner can miss the first net report that selects the home relay.
const ONLINE_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const SEND_DEADLINE: Duration = Duration::from_secs(4);
const PROBE_TIMEOUT: Duration = Duration::from_secs(6);
const RAW_GENERATION: [u8; 16] = [1; 16];

fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    result.unwrap_or_else(|_| panic!("{operation}"))
}

fn config() -> LinkConfig {
    LinkConfig {
        connect_timeout: CONNECT_TIMEOUT,
        send_deadline: SEND_DEADLINE,
    }
}

fn message(id: &str, body: &str) -> ChatMessage {
    ChatMessage {
        id: id.into(),
        sent_at_ms: 123,
        body: body.into(),
    }
}

async fn bind(config: EndpointConfig) -> (Endpoint, Arc<Authorizer>) {
    bind_with_key(&generate_node_key(), config).await
}

async fn bind_with_key(key: &[u8; 32], config: EndpointConfig) -> (Endpoint, Arc<Authorizer>) {
    let auth = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
    let endpoint = checked(
        Endpoint::bind(key, config, Arc::clone(&auth)).await,
        "bind test endpoint",
    );
    (endpoint, auth)
}

async fn bind_direct() -> (Endpoint, Arc<Authorizer>) {
    bind_direct_with_key(&generate_node_key()).await
}

async fn bind_direct_with_key(key: &[u8; 32]) -> (Endpoint, Arc<Authorizer>) {
    bind_with_key(
        key,
        EndpointConfig {
            ip_transport: IpTransport::LoopbackOnly,
            ..EndpointConfig::default()
        },
    )
    .await
}

fn ordered_keys() -> ([u8; 32], [u8; 32]) {
    let first = generate_node_key();
    let second = generate_node_key();
    assert!(node_id(&first) != node_id(&second));
    if node_id(&first) < node_id(&second) {
        (first, second)
    } else {
        (second, first)
    }
}

fn loopback_socket(endpoint: &Endpoint) -> SocketAddr {
    let sockets = endpoint.iroh().bound_sockets();
    assert_eq!(sockets.len(), 1);
    assert_eq!(sockets[0].ip(), Ipv4Addr::LOCALHOST);
    sockets[0]
}

fn loopback_address(endpoint: &Endpoint) -> EndpointAddr {
    EndpointAddr::new(endpoint.iroh().id()).with_ip_addr(loopback_socket(endpoint))
}

fn add_lookup(endpoint: &Endpoint, lookup: MemoryLookup) {
    checked(endpoint.iroh().address_lookup(), "read address lookup").add(lookup);
}

fn add_address(endpoint: &Endpoint, addr: EndpointAddr) {
    add_lookup(endpoint, MemoryLookup::from_endpoint_info([addr]));
}

fn allow_peers(
    first: (&Endpoint, &Authorizer),
    second: (&Endpoint, &Authorizer),
) -> (NodeId, NodeId) {
    let first_id = first.0.iroh().id();
    let second_id = second.0.iroh().id();
    first.1.replace_peers([(second_id, "second".into())]);
    second.1.replace_peers([(first_id, "first".into())]);
    (first_id, second_id)
}

async fn acknowledge(
    inbox: &mut Receiver<IncomingChat>,
    peer: NodeId,
    name: &str,
    expected: &ChatMessage,
) {
    let incoming = checked(
        timeout(PROBE_TIMEOUT, inbox.recv()).await,
        "wait for chat delivery",
    )
    .expect("link inbox remains open");
    assert_delivery(&incoming, peer, name, expected);
    incoming.respond(ChatReply::Ack);
}

fn assert_delivery(incoming: &IncomingChat, peer: NodeId, name: &str, expected: &ChatMessage) {
    assert!(incoming.station_id == peer);
    assert_eq!(incoming.peer.as_ref(), name);
    assert!(incoming.message.id == expected.id);
    assert_eq!(incoming.message.sent_at_ms, expected.sent_at_ms);
    assert!(incoming.message.body == expected.body);
}

async fn receive_with_receipts(
    inbox: &mut Receiver<IncomingChat>,
    peer: NodeId,
    name: &str,
    expected: &[&ChatMessage],
    deliveries: &mut Vec<String>,
) {
    let mut receipts = ReceiptWindow::new(3);
    while let Some(incoming) = inbox.recv().await {
        let message = expected
            .iter()
            .find(|message| message.id == incoming.message.id)
            .expect("receive an expected chat message");
        assert_delivery(&incoming, peer, name, message);
        // Model the station's receipt recording at the delivery boundary, including
        // acknowledging a retry when a duplicate link closed before the first ACK arrived.
        if receipts.record(&incoming.peer, &incoming.message.id) {
            deliveries.push(incoming.message.id.clone());
        }
        incoming.respond(ChatReply::Ack);
    }
}

async fn wait_for_path(handle: &LinkHandle, peer: NodeId, expected: LinkPath) {
    let mut changes = handle.subscribe_paths();
    checked(
        timeout(PROBE_TIMEOUT, async {
            while handle.path(peer) != expected {
                checked(changes.recv().await, "receive link path change");
            }
        })
        .await,
        "observe selected link path",
    );
}

async fn write<T: Serialize>(send: &mut SendStream, value: &T) {
    checked(
        write_frame(send, value, WIRE_FRAME_MAX).await,
        "write test frame",
    );
}

async fn read<T: DeserializeOwned>(recv: &mut RecvStream) -> T {
    checked(
        read_frame(recv, WIRE_FRAME_MAX, Some(PROBE_TIMEOUT)).await,
        "read test frame",
    )
}

async fn dial(endpoint: &Endpoint, addr: &EndpointAddr) -> Connection {
    checked(
        endpoint.iroh().connect(addr.clone(), LINK_ALPN).await,
        "dial link over loopback",
    )
}

async fn reply_to_chat(conn: &Connection, expected: &ChatMessage) {
    let (mut send, _) = accept_chat(conn, expected).await;
    write(&mut send, &ChatReply::Ack).await;
    checked(send.finish(), "finish chat reply");
}

async fn accept_chat(conn: &Connection, expected: &ChatMessage) -> (SendStream, serde_json::Value) {
    let (send, mut recv) = checked(conn.accept_bi().await, "accept chat stream");
    let header: StreamHeader = read(&mut recv).await;
    assert_eq!(header.proto, CHAT_PROTOCOL);
    assert_eq!(header.v, CHAT_VERSION);
    let message: serde_json::Value = read(&mut recv).await;
    assert!(message == checked(serde_json::to_value(expected), "serialize expected chat"));
    (send, message)
}

async fn request<T: Serialize>(
    conn: &Connection,
    header: &StreamHeader,
    body: &T,
) -> Result<ChatReply, FrameError> {
    // Write both frames together so an immediate refusal of the header can race the reply
    // without also racing a separate write of the body.
    let mut request = Vec::new();
    checked(
        write_frame(&mut request, header, WIRE_FRAME_MAX).await,
        "encode test header",
    );
    checked(
        write_frame(&mut request, body, WIRE_FRAME_MAX).await,
        "encode test body",
    );
    let (mut send, mut recv) = checked(conn.open_bi().await, "open chat stream");
    checked(send.write_all(&request).await, "write chat request");
    // The receiver may already have stopped reading a rejected request.
    let _ = send.finish();
    read_frame(&mut recv, WIRE_FRAME_MAX, Some(PROBE_TIMEOUT)).await
}

async fn wait_for_close(conn: &Connection) {
    checked(
        timeout(PROBE_TIMEOUT, conn.closed()).await,
        "close test link",
    );
}

async fn rebind_after_shutdown(socket: SocketAddr) {
    checked(
        timeout(Duration::from_secs(1), async {
            loop {
                match UdpSocket::bind(socket) {
                    Ok(_) => break,
                    Err(error) if error.kind() == ErrorKind::AddrInUse => {
                        sleep(Duration::from_millis(10)).await;
                    }
                    Err(_) => panic!("rebind link socket after shutdown"),
                }
            }
        })
        .await,
        "link socket remained bound after shutdown",
    );
}

struct HeldHello {
    send: SendStream,
    recv: RecvStream,
    hello: Hello,
}

impl HeldHello {
    fn response(&self, generation: [u8; 16], challenge: [u8; 16]) -> Hello {
        let mut reply = Hello::local(generation, challenge);
        reply.echo = Some(self.hello.challenge);
        reply
    }

    async fn reply(&mut self, generation: [u8; 16], challenge: [u8; 16]) {
        let reply = self.response(generation, challenge);
        write(&mut self.send, &reply).await;
        checked(self.send.finish(), "finish delayed hello");
    }

    async fn confirm(&mut self, challenge: [u8; 16]) {
        let confirmation: HelloEcho = read(&mut self.recv).await;
        assert!(confirmation.echo == challenge);
    }
}

async fn accept_hello(endpoint: &Endpoint) -> (Connection, HeldHello) {
    // Accept directly to control when the peer replies to the outgoing hello.
    let incoming = checked(
        timeout(PROBE_TIMEOUT, endpoint.iroh().accept()).await,
        "wait for outgoing link",
    )
    .expect("raw peer is still listening");
    let accepting = checked(incoming.accept(), "accept outgoing handshake");
    let conn = checked(accepting.await, "complete outgoing handshake");
    let (send, mut recv) = checked(conn.accept_bi().await, "accept hello stream");
    let hello: Hello = read(&mut recv).await;
    assert_eq!(hello.validate(), Ok(()));
    assert!(hello.echo.is_none());
    (conn, HeldHello { send, recv, hello })
}

async fn begin_hello(conn: &Connection, local: &Hello) -> (SendStream, Hello) {
    let (mut send, mut recv) = checked(conn.open_bi().await, "open hello stream");
    write(&mut send, local).await;
    let remote: Hello = read(&mut recv).await;
    assert_eq!(remote.validate(), Ok(()));
    assert!(remote.echo == Some(local.challenge));
    (send, remote)
}

async fn finish_hello(send: &mut SendStream, remote: &Hello) {
    write(
        send,
        &HelloEcho {
            echo: remote.challenge,
        },
    )
    .await;
    checked(send.finish(), "finish client hello");
}

async fn restart_without_close(original_dials: bool, restarted_sends_first: bool) {
    let (survivor_key, restarting_key) = ordered_keys();
    let (survivor_endpoint, survivor_auth) = bind_direct_with_key(&survivor_key).await;
    let survivor_id = survivor_endpoint.iroh().id();
    let restarting_id = node_id(&restarting_key);
    survivor_auth.replace_peers([(restarting_id, "restarting".into())]);
    let survivor_addr = loopback_address(&survivor_endpoint);
    let original_peer_addr = survivor_addr.clone();
    let initial = message("initial", "before restarting");
    let original_initial = initial.clone();
    let (ready, original_addr) = oneshot::channel();
    let (crash, stop) = oneshot::channel();

    let original = tokio::task::spawn_blocking(move || {
        let runtime = checked(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build(),
            "create the original station's separate runtime",
        );
        let link = runtime.block_on(async move {
            let (endpoint, auth) = bind_direct_with_key(&restarting_key).await;
            auth.replace_peers([(survivor_id, "survivor".into())]);
            add_address(&endpoint, original_peer_addr);
            let address = loopback_address(&endpoint);
            let (link, mut inbox) = Link::start(endpoint, config());
            checked(ready.send(address), "share the original station's address");
            if original_dials {
                assert_eq!(
                    link.handle().send(survivor_id, original_initial).await,
                    Ok(ChatReply::Ack),
                );
            } else {
                acknowledge(&mut inbox, survivor_id, "survivor", &original_initial).await;
            }
            let _ = stop.await;
            link
        });
        // Stop the QUIC drivers before the link can send CONNECTION_CLOSE from Drop.
        runtime.shutdown_background();
        drop(link);
    });

    let original_addr = checked(
        original_addr.await,
        "receive the original station's address",
    );
    let lookup = MemoryLookup::from_endpoint_info([original_addr]);
    add_lookup(&survivor_endpoint, lookup.clone());
    let (survivor, mut survivor_inbox) = Link::start(survivor_endpoint, config());
    let survivor_handle = survivor.handle();
    if original_dials {
        acknowledge(&mut survivor_inbox, restarting_id, "restarting", &initial).await;
    } else {
        assert_eq!(
            survivor_handle.send(restarting_id, initial).await,
            Ok(ChatReply::Ack),
        );
    }
    wait_for_path(&survivor_handle, restarting_id, LinkPath::Direct).await;
    checked(crash.send(()), "stop the original station's runtime");
    checked(original.await, "join the original station's runtime");
    assert_eq!(survivor_handle.path(restarting_id), LinkPath::Direct);

    let (restarted, mut restarted_inbox) = checked(
        timeout(PROBE_TIMEOUT, async {
            let (endpoint, auth) = bind_direct_with_key(&restarting_key).await;
            assert!(endpoint.iroh().id() == restarting_id);
            auth.replace_peers([(survivor_id, "survivor".into())]);
            add_address(&endpoint, survivor_addr);
            lookup.set_endpoint_info(loopback_address(&endpoint));
            Link::start(endpoint, config())
        })
        .await,
        "restart the station with the original key",
    );
    let restarted_handle = restarted.handle();

    let after = message("after-restart", "sent by the restarted station");
    let reverse = message("reverse", "sent by the surviving station");
    let from_restarted = async {
        let (reply, ()) = tokio::join!(
            restarted_handle.send(survivor_id, after.clone()),
            acknowledge(&mut survivor_inbox, restarting_id, "restarting", &after),
        );
        assert_eq!(reply, Ok(ChatReply::Ack));
    };
    let from_survivor = async {
        let (reply, ()) = tokio::join!(
            survivor_handle.send(restarting_id, reverse.clone()),
            acknowledge(&mut restarted_inbox, survivor_id, "survivor", &reverse),
        );
        assert_eq!(reply, Ok(ChatReply::Ack));
    };
    checked(
        timeout(PROBE_TIMEOUT, async {
            if restarted_sends_first {
                from_restarted.await;
                from_survivor.await;
            } else {
                from_survivor.await;
                from_restarted.await;
            }
        })
        .await,
        "exchange chat before the stale connection's idle timeout",
    );

    let (survivor_result, restarted_result) =
        tokio::join!(survivor.shutdown(), restarted.shutdown());
    checked(survivor_result, "stop surviving link");
    checked(restarted_result, "stop restarted link");
}

#[tokio::test]
async fn restart_recovers_when_the_original_link_was_dialed_by_the_restarted_station() {
    checked(
        timeout(CASE_TIMEOUT, restart_without_close(true, true)).await,
        "restart test timed out",
    );
}

#[tokio::test]
async fn restart_recovers_when_the_original_link_was_dialed_by_the_lower_id_survivor() {
    checked(
        timeout(CASE_TIMEOUT, restart_without_close(false, true)).await,
        "restart test timed out",
    );
}

#[tokio::test]
async fn survivor_can_send_first_after_the_other_station_restarts() {
    checked(
        timeout(CASE_TIMEOUT, restart_without_close(false, false)).await,
        "restart test timed out",
    );
}

async fn crossed_hellos(
    local_is_lower: bool,
    first_side: Side,
    outgoing_generation: [u8; 16],
    winner_side: Side,
) {
    let (lower, higher) = ordered_keys();
    let (local_key, raw_key) = if local_is_lower {
        (lower, higher)
    } else {
        (higher, lower)
    };
    let (endpoint, auth) = bind_direct_with_key(&local_key).await;
    let (peer, peer_auth) = bind_direct_with_key(&raw_key).await;
    let (_, peer_id) = allow_peers((&endpoint, &auth), (&peer, &peer_auth));
    let address = loopback_address(&endpoint);
    add_address(&endpoint, loopback_address(&peer));
    peer.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
    let (link, mut inbox) = Link::start(
        endpoint,
        LinkConfig {
            connect_timeout: PROBE_TIMEOUT,
            send_deadline: PROBE_TIMEOUT * 2,
        },
    );
    let handle = link.handle();
    let probe = message("probe", "send during crossed hellos");
    let outgoing_handle = handle.clone();
    let sent = probe.clone();
    let send = tokio::spawn(async move { outgoing_handle.send(peer_id, sent).await });
    let (outgoing, mut held) = accept_hello(&peer).await;
    let incoming = dial(&peer, &address).await;
    let (mut incoming_hello, remote) =
        begin_hello(&incoming, &Hello::local(RAW_GENERATION, [2; 16])).await;
    assert!(remote.generation == held.hello.generation);

    // Both hellos are pending. A chat stream proves the first connection was admitted
    // before the second handshake completes. Hold the chat ACK across the tie-break.
    let first = match first_side {
        Side::Client => {
            held.reply(outgoing_generation, [1; 16]).await;
            held.confirm([1; 16]).await;
            &outgoing
        }
        Side::Server => {
            finish_hello(&mut incoming_hello, &remote).await;
            &incoming
        }
    };
    let (mut first_reply, _) = accept_chat(first, &probe).await;
    match first_side {
        Side::Client => finish_hello(&mut incoming_hello, &remote).await,
        Side::Server => held.reply(outgoing_generation, [1; 16]).await,
    }
    let (winner, loser) = match winner_side {
        Side::Client => (&outgoing, &incoming),
        Side::Server => (&incoming, &outgoing),
    };
    let reason = checked(
        timeout(PROBE_TIMEOUT, loser.closed()).await,
        "close losing link",
    );
    assert!(
        matches!(reason, ConnectionError::ApplicationClosed(close) if close.error_code.into_inner() == 409)
    );
    assert!(winner.close_reason().is_none());

    if winner.stable_id() == first.stable_id() {
        write(&mut first_reply, &ChatReply::Ack).await;
        checked(
            first_reply.finish(),
            "finish chat reply on the winning link",
        );
    } else {
        reply_to_chat(winner, &probe).await;
    }
    assert_eq!(
        checked(send.await, "join send during crossed hellos"),
        Ok(ChatReply::Ack)
    );

    let reverse = message("reverse", "use the surviving connection in both directions");
    let header = StreamHeader::chat();
    let (reply, ()) = tokio::join!(
        request(winner, &header, &reverse),
        acknowledge(&mut inbox, peer_id, "second", &reverse),
    );
    assert_eq!(
        checked(reply, "read reply on the winning link"),
        ChatReply::Ack
    );

    checked(link.shutdown().await, "stop crossed link");
    peer.iroh().close().await;
}

#[tokio::test]
async fn crossed_hellos_keep_the_lower_id_dialer_in_either_completion_order() {
    for (local_is_lower, winner) in [(true, Side::Client), (false, Side::Server)] {
        for first in [Side::Client, Side::Server] {
            checked(
                timeout(
                    CASE_TIMEOUT,
                    crossed_hellos(local_is_lower, first, RAW_GENERATION, winner),
                )
                .await,
                "crossed hello test timed out",
            );
        }
    }
}

#[tokio::test]
async fn delayed_old_generation_hello_cannot_replace_the_new_generation() {
    checked(
        timeout(
            CASE_TIMEOUT,
            crossed_hellos(true, Side::Server, [2; 16], Side::Server),
        )
        .await,
        "delayed hello test timed out",
    );
}

#[tokio::test]
async fn simultaneous_sends_finish_in_both_directions_over_the_relay() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let tower = checked(
                Tower::spawn(TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into())).await,
                "start loopback tower",
            );
            let relay_config = EndpointConfig {
                towers: checked(TowerSet::from_url(tower.url()), "parse tower URL"),
                ip_transport: IpTransport::RelayOnly,
                ..EndpointConfig::default()
            };
            let (first, first_auth) = bind(relay_config.clone()).await;
            let (second, second_auth) = bind(relay_config).await;
            let (first_id, second_id) = allow_peers((&first, &first_auth), (&second, &second_auth));
            checked(
                timeout(ONLINE_TIMEOUT, async {
                    tokio::join!(first.iroh().online(), second.iroh().online());
                })
                .await,
                "both endpoints come online through the tower",
            );
            let (first, mut first_inbox) = Link::start(first, config());
            let (second, mut second_inbox) = Link::start(second, config());
            let first_handle = first.handle();
            let second_handle = second.handle();

            // The body at the exact UTF-8 byte limit contains characters that need escaping in JSON.
            let first_message = message("first", &"\u{1}".repeat(CHAT_BODY_MAX));
            let second_message = message("second", &"é".repeat(CHAT_BODY_MAX / 2));
            let followup = message("followup", "after simultaneous dial");
            let first_expected = [&second_message, &followup];
            let second_expected = [&first_message];
            let mut first_deliveries = Vec::new();
            let mut second_deliveries = Vec::new();
            let send_messages = async {
                let (first_reply, second_reply) = tokio::join!(
                    first_handle.send(second_id, first_message.clone()),
                    second_handle.send(first_id, second_message.clone()),
                );
                assert!(matches!(first_reply, Ok(ChatReply::Ack)));
                assert!(matches!(second_reply, Ok(ChatReply::Ack)));
                tokio::join!(
                    wait_for_path(&first_handle, second_id, LinkPath::Relayed),
                    wait_for_path(&second_handle, first_id, LinkPath::Relayed),
                );
                assert!(matches!(
                    second_handle.send(first_id, followup.clone()).await,
                    Ok(ChatReply::Ack)
                ));
                assert!(matches!(
                    second_handle.send(first_id, second_message.clone()).await,
                    Ok(ChatReply::Ack)
                ));
            };
            tokio::select! {
                () = send_messages => {},
                () = receive_with_receipts(
                    &mut first_inbox, second_id, "second", &first_expected,
                    &mut first_deliveries,
                ) => panic!("first inbox closed during relay exchange"),
                () = receive_with_receipts(
                    &mut second_inbox, first_id, "first", &second_expected,
                    &mut second_deliveries,
                ) => panic!("second inbox closed during relay exchange"),
            }
            assert!(first_deliveries == ["second", "followup"]);
            assert!(second_deliveries == ["first"]);

            let (first_result, second_result) = tokio::join!(first.shutdown(), second.shutdown());
            checked(first_result, "stop first link");
            checked(second_result, "stop second link");
            checked(tower.shutdown().await, "stop loopback tower");
        })
        .await,
        "relay chat test timed out",
    );
}

#[tokio::test]
async fn malformed_hello_and_invalid_streams_are_rejected_without_blocking_chat() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (endpoint, auth) = bind_direct().await;
            let (peer, peer_auth) = bind_direct().await;
            let (id, peer_id) = allow_peers((&endpoint, &auth), (&peer, &peer_auth));
            let address = EndpointAddr::new(id).with_ip_addr(loopback_socket(&endpoint));
            let (link, mut inbox) = Link::start(endpoint, config());
            let handle = link.handle();
            let oversize = message("oversize", &"a".repeat(CHAT_BODY_MAX + 1));
            assert!(matches!(
                handle.send(peer_id, oversize).await,
                Err(SendError::InvalidMessage(NackReason::BodyTooLarge))
            ));

            let partial = dial(&peer, &address).await;
            let (mut send, _recv) = checked(partial.open_bi().await, "open incomplete hello");
            checked(send.write_all(&[0, 0]).await, "write partial length prefix");
            wait_for_close(&partial).await;
            assert_eq!(handle.path(peer_id), LinkPath::Down);

            let malformed = dial(&peer, &address).await;
            let (mut send, _recv) = checked(malformed.open_bi().await, "open malformed hello");
            write(&mut send, &json!({"v":"invalid","network":"","head":""})).await;
            checked(send.finish(), "finish malformed hello");
            expect_close(&malformed, 400).await;
            assert_eq!(handle.path(peer_id), LinkPath::Down);

            let premature_echo = dial(&peer, &address).await;
            let (mut send, mut recv) =
                checked(premature_echo.open_bi().await, "open initial hello");
            let mut initial = Hello::local(RAW_GENERATION, [2; 16]);
            initial.echo = Some([3; 16]);
            write(&mut send, &initial).await;
            let response = checked(
                timeout(PROBE_TIMEOUT, recv.read(&mut [0])).await,
                "reject the first hello before replying",
            );
            assert!(matches!(
                response,
                Ok(None) | Err(ReadError::ConnectionLost(_))
            ));
            expect_close(&premature_echo, 400).await;
            assert_eq!(handle.path(peer_id), LinkPath::Down);

            let wrong_echo = dial(&peer, &address).await;
            let (mut send, remote) =
                begin_hello(&wrong_echo, &Hello::local(RAW_GENERATION, [2; 16])).await;
            let mut echo = remote.challenge;
            echo[0] ^= 1;
            write(&mut send, &HelloEcho { echo }).await;
            checked(send.finish(), "finish wrong hello echo");
            expect_close(&wrong_echo, 400).await;
            assert_eq!(handle.path(peer_id), LinkPath::Down);

            let conn = dial(&peer, &address).await;
            let (mut send, remote) =
                begin_hello(&conn, &Hello::local(RAW_GENERATION, [3; 16])).await;
            finish_hello(&mut send, &remote).await;
            wait_for_path(&handle, peer_id, LinkPath::Direct).await;

            let probe = message("probe", "must not be delivered");
            let unknown = StreamHeader {
                proto: "other".into(),
                v: CHAT_VERSION,
            };
            let unsupported = StreamHeader {
                proto: CHAT_PROTOCOL.into(),
                v: CHAT_VERSION + 1,
            };
            for (header, reason) in [
                (unknown, NackReason::UnknownProtocol),
                (unsupported, NackReason::UnsupportedVersion),
            ] {
                assert_eq!(
                    checked(request(&conn, &header, &probe).await, "read typed NACK"),
                    ChatReply::Nack(reason),
                );
            }

            // Each base64 group decodes to three bytes, so this exceeds the chat limit
            // while remaining smaller than a single wire frame.
            let large_body = json!({
                "id":"oversize",
                "sent_at_ms":0,
                "body":"YWFh".repeat(CHAT_BODY_MAX / 3 + 1),
            });
            assert!(matches!(
                request(&conn, &StreamHeader::chat(), &large_body).await,
                Ok(ChatReply::Nack(NackReason::BodyTooLarge))
            ));

            let oversize_length = u32::try_from(WIRE_FRAME_MAX + 1).expect("wire cap fits u32");
            let oversized = oversize_length.to_be_bytes();
            let truncated = [0, 0, 0, 4, b'{'];
            let malformed = [0, 0, 0, 1, b'{'];
            let mut header = Vec::new();
            checked(
                write_frame(&mut header, &StreamHeader::chat(), WIRE_FRAME_MAX).await,
                "encode chat header",
            );
            for frame in [oversized.as_slice(), &truncated, &malformed] {
                let mut request = header.clone();
                request.extend_from_slice(frame);
                let (mut send, mut recv) =
                    checked(conn.open_bi().await, "open invalid chat stream");
                checked(send.write_all(&request).await, "write invalid chat frame");
                let _ = send.finish();
                assert!(matches!(
                    read_frame::<_, ChatReply>(&mut recv, WIRE_FRAME_MAX, Some(PROBE_TIMEOUT))
                        .await,
                    Err(FrameError::Closed | FrameError::Io(_)),
                ));
            }
            assert!(matches!(inbox.try_recv(), Err(TryRecvError::Empty)));

            let valid = message("valid", "same connection");
            let chat_header = StreamHeader::chat();
            let (mut stalled_send, mut stalled_recv) =
                checked(conn.open_bi().await, "open stalled chat");
            write(&mut stalled_send, &chat_header).await;
            let (reply, ()) = tokio::join!(
                request(&conn, &chat_header, &valid),
                acknowledge(&mut inbox, peer_id, "second", &valid),
            );
            assert!(matches!(reply, Ok(ChatReply::Ack)));
            let stalled_reply = checked(
                timeout(PROBE_TIMEOUT, stalled_recv.read(&mut [0])).await,
                "receive reset after the stream deadline",
            );
            assert!(matches!(stalled_reply, Err(ReadError::Reset(code)) if code.into_inner() == 0));

            // The raw peer has neither a router nor a tower address for a new dial.
            let reverse = message("reverse", "reuse accepted link");
            let (reply, ()) = tokio::join!(
                handle.send(peer_id, reverse.clone()),
                reply_to_chat(&conn, &reverse),
            );
            assert!(matches!(reply, Ok(ChatReply::Ack)));

            // An established connection still checks authorization on each chat stream.
            auth.replace_peers([]);
            assert!(matches!(
                request(&conn, &chat_header, &probe).await,
                Err(FrameError::Io(error)) if matches!(
                    error.get_ref().and_then(|source| source.downcast_ref::<ReadError>()),
                    Some(ReadError::Reset(code)) if code.into_inner() == 403
                )
            ));
            assert!(conn.close_reason().is_none());
            assert!(matches!(inbox.try_recv(), Err(TryRecvError::Empty)));

            checked(link.shutdown().await, "stop direct link");
            peer.iroh().close().await;
        })
        .await,
        "stream validation test timed out",
    );
}

#[tokio::test]
async fn receiver_deadline_resets_a_chat_held_by_the_application() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (endpoint, auth) = bind_direct().await;
            let (peer, peer_auth) = bind_direct().await;
            let (_, peer_id) = allow_peers((&endpoint, &auth), (&peer, &peer_auth));
            let address = loopback_address(&endpoint);
            let (link, mut inbox) = Link::start(endpoint, config());
            let conn = dial(&peer, &address).await;
            let (mut send, remote) =
                begin_hello(&conn, &Hello::local(RAW_GENERATION, [1; 16])).await;
            finish_hello(&mut send, &remote).await;
            wait_for_path(&link.handle(), peer_id, LinkPath::Direct).await;

            let chat = message(
                "held-reply",
                "the application receives this chat without answering",
            );
            let header = StreamHeader::chat();
            // Hold the application's reply sender while the raw peer waits for the reset.
            let (response, held) = checked(
                timeout(SEND_DEADLINE + Duration::from_secs(1), async {
                    tokio::join!(request(&conn, &header, &chat), inbox.recv())
                })
                .await,
                "reset the unanswered chat by the receiver deadline",
            );
            let held = held.expect("receive the complete chat before it times out");
            assert_delivery(&held, peer_id, "second", &chat);
            assert!(matches!(
                response,
                Err(FrameError::Io(error)) if matches!(
                    error.get_ref().and_then(|source| source.downcast_ref::<ReadError>()),
                    Some(ReadError::Reset(code)) if code.into_inner() == 0
                )
            ));
            assert!(conn.close_reason().is_none());
            drop(held);

            let next = message("after-deadline", "another stream can still use this link");
            let (reply, ()) = tokio::join!(
                request(&conn, &header, &next),
                acknowledge(&mut inbox, peer_id, "second", &next),
            );
            assert_eq!(
                checked(reply, "read ACK after a receiver deadline"),
                ChatReply::Ack
            );
            checked(link.shutdown().await, "stop link after receiver deadline");
            peer.iroh().close().await;
        })
        .await,
        "receiver reply deadline test timed out",
    );
}

#[tokio::test]
async fn outgoing_hello_rejects_missing_and_wrong_server_echoes() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (endpoint, auth) = bind_direct().await;
            let (peer, peer_auth) = bind_direct().await;
            let (_, peer_id) = allow_peers((&endpoint, &auth), (&peer, &peer_auth));
            add_address(&endpoint, loopback_address(&peer));
            peer.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
            let (link, mut inbox) = Link::start(endpoint, config());
            let handle = link.handle();
            let chat = message("bad-echo", "hello must complete before chat");

            for missing in [true, false] {
                let (result, ()) = tokio::join!(handle.send(peer_id, chat.clone()), async {
                    let (conn, mut held) = accept_hello(&peer).await;
                    let mut response = held.response(RAW_GENERATION, [1; 16]);
                    let mut wrong = held.hello.challenge;
                    wrong[0] ^= 1;
                    response.echo = (!missing).then_some(wrong);
                    write(&mut held.send, &response).await;
                    checked(held.send.finish(), "finish a server hello with a bad echo");
                    expect_close(&conn, 400).await;
                });
                assert_eq!(result, Err(SendError::BadHello));
                assert_eq!(handle.path(peer_id), LinkPath::Down);
                assert!(matches!(inbox.try_recv(), Err(TryRecvError::Empty)));
            }

            checked(link.shutdown().await, "stop link after hello rejection");
            peer.iroh().close().await;
        })
        .await,
        "server echo rejection test timed out",
    );
}

#[tokio::test]
async fn dial_recovers_after_cancellation_and_timeout_then_stops_on_revocation() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (endpoint, auth) = bind_direct().await;
            let (peer, peer_auth) = bind_direct().await;
            let (_, peer_id) = allow_peers((&endpoint, &auth), (&peer, &peer_auth));
            let local_socket = loopback_socket(&endpoint);
            checked(endpoint.iroh().address_lookup(), "read address lookup").add(
                MemoryLookup::from_endpoint_info([
                    EndpointAddr::new(peer_id).with_ip_addr(loopback_socket(&peer))
                ]),
            );
            peer.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
            let (link, _inbox) = Link::start(endpoint, config());
            let handle = link.handle();

            let cancel_handle = handle.clone();
            let canceled = tokio::spawn(async move {
                cancel_handle
                    .send(peer_id, message("canceled", "cancel before hello"))
                    .await
            });
            let (conn, mut hello) = accept_hello(&peer).await;
            canceled.abort();
            assert!(matches!(canceled.await, Err(error) if error.is_cancelled()));

            let valid = message("continued", "reuse ongoing dial");
            let (reply, ()) = tokio::join!(handle.send(peer_id, valid.clone()), async {
                hello.reply(RAW_GENERATION, [1; 16]).await;
                hello.confirm([1; 16]).await;
                reply_to_chat(&conn, &valid).await;
            });
            assert!(matches!(reply, Ok(ChatReply::Ack)));
            wait_for_path(&handle, peer_id, LinkPath::Direct).await;

            handle.close_peer(peer_id);
            wait_for_close(&conn).await;
            assert_eq!(handle.path(peer_id), LinkPath::Down);

            let expiring_handle = handle.clone();
            let expiring = tokio::spawn(async move {
                expiring_handle
                    .send(peer_id, message("expires", "peer never replies to hello"))
                    .await
            });
            let (expired, _held_hello) = accept_hello(&peer).await;
            assert!(matches!(
                checked(expiring.await, "join timed-out send"),
                Err(SendError::Timeout)
            ));
            wait_for_close(&expired).await;
            assert_eq!(handle.path(peer_id), LinkPath::Down);

            // A new send must reach the peer instead of reusing the expired attempt.
            let pending_handle = handle.clone();
            let pending = tokio::spawn(async move {
                pending_handle
                    .send(peer_id, message("revoked", "revoke before hello"))
                    .await
            });
            let (retired, mut hello) = accept_hello(&peer).await;
            let mut changes = handle.subscribe_paths();
            auth.replace_peers([]);
            handle.close_peer(peer_id);
            assert!(matches!(
                handle
                    .send(peer_id, message("removed", "cannot reconnect"))
                    .await,
                Err(SendError::NotAllowed)
            ));
            auth.replace_peers([(peer_id, "re-added".into())]);
            // The remote may already have seen the close before this delayed reply.
            let delayed = hello.response(RAW_GENERATION, [2; 16]);
            let _ = write_frame(&mut hello.send, &delayed, WIRE_FRAME_MAX).await;
            let _ = hello.send.finish();
            let send_result = checked(pending.await, "join revoked send");
            assert!(matches!(
                send_result,
                Err(SendError::Closed | SendError::NotAllowed)
            ));
            wait_for_close(&retired).await;
            assert_eq!(handle.path(peer_id), LinkPath::Down);
            loop {
                match changes.try_recv() {
                    Ok(update) => assert_eq!(update.path, LinkPath::Down),
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(_) => panic!("unexpected interruption to path tracking"),
                }
            }

            let restored = message("restored", "use a new connection after re-adding");
            let (reply, fresh) = tokio::join!(handle.send(peer_id, restored.clone()), async {
                let (fresh, mut hello) = accept_hello(&peer).await;
                hello.reply(RAW_GENERATION, [3; 16]).await;
                hello.confirm([3; 16]).await;
                reply_to_chat(&fresh, &restored).await;
                fresh
            });
            assert_eq!(reply, Ok(ChatReply::Ack));
            assert!(fresh.close_reason().is_none());

            checked(link.shutdown().await, "stop link with a surviving handle");
            assert_eq!(handle.path(peer_id), LinkPath::Down);
            rebind_after_shutdown(local_socket).await;
            peer.iroh().close().await;
        })
        .await,
        "cancellation and revocation test timed out",
    );
}

#[tokio::test]
async fn slow_application_ack_keeps_new_and_established_links_usable() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (endpoint, auth) = bind_direct().await;
            let (peer, peer_auth) = bind_direct().await;
            let (_, peer_id) = allow_peers((&endpoint, &auth), (&peer, &peer_auth));
            add_address(&endpoint, loopback_address(&peer));
            peer.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
            let (link, mut inbox) = Link::start(endpoint, config());
            let handle = link.handle();
            let fresh = message("fresh", "slow ACK on a fresh link");
            let (reply, conn) = tokio::join!(handle.send(peer_id, fresh.clone()), async {
                let (conn, mut hello) = accept_hello(&peer).await;
                hello.reply(RAW_GENERATION, [1; 16]).await;
                hello.confirm([1; 16]).await;
                let (mut held_ack, _) = accept_chat(&conn, &fresh).await;
                sleep(CONNECT_TIMEOUT + Duration::from_millis(100)).await;
                assert!(conn.close_reason().is_none());
                write(&mut held_ack, &ChatReply::Ack).await;
                checked(held_ack.finish(), "finish a delayed ACK on the new link");
                conn
            });
            assert_eq!(reply, Ok(ChatReply::Ack));

            let established = message("established", "slow ACK on an established link");
            let reverse = message("reverse", "other chat streams still make progress");
            let header = StreamHeader::chat();
            let (reply, ()) = tokio::join!(handle.send(peer_id, established.clone()), async {
                let (mut held_ack, _) = accept_chat(&conn, &established).await;
                // Only transport traffic has returned while the progress checkpoint passes.
                sleep(CONNECT_TIMEOUT + Duration::from_millis(100)).await;
                assert!(conn.close_reason().is_none());
                let (other_reply, ()) = tokio::join!(
                    request(&conn, &header, &reverse),
                    acknowledge(&mut inbox, peer_id, "second", &reverse),
                );
                assert_eq!(
                    checked(other_reply, "receive ACK on the other stream"),
                    ChatReply::Ack
                );
                write(&mut held_ack, &ChatReply::Ack).await;
                checked(
                    held_ack.finish(),
                    "finish a delayed ACK on the established link",
                );
            });
            assert_eq!(reply, Ok(ChatReply::Ack));

            checked(
                link.shutdown().await,
                "stop link after delayed acknowledgments",
            );
            peer.iroh().close().await;
        })
        .await,
        "slow application ACK test timed out",
    );
}

#[tokio::test]
async fn waiting_for_stream_credit_keeps_a_healthy_link_usable() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (endpoint, auth) = bind_direct().await;
            let (peer, peer_auth) = bind_direct().await;
            let (_, peer_id) = allow_peers((&endpoint, &auth), (&peer, &peer_auth));
            let address = loopback_address(&endpoint);
            let (link, mut inbox) = Link::start(endpoint, config());
            let handle = link.handle();
            // The raw peer initiates the hello and warmup so the station starts
            // with its full allowance of 16 locally initiated streams.
            let conn = dial(&peer, &address).await;
            let (mut hello, remote) =
                begin_hello(&conn, &Hello::local(RAW_GENERATION, [1; 16])).await;
            finish_hello(&mut hello, &remote).await;
            let warmup = message("warmup", "establish the link");
            let header = StreamHeader::chat();
            let (reply, ()) = tokio::join!(
                request(&conn, &header, &warmup),
                acknowledge(&mut inbox, peer_id, "second", &warmup),
            );
            assert_eq!(checked(reply, "establish the link with a chat"), ChatReply::Ack);

            let mut outstanding = Vec::new();
            for index in 0..16 {
                let chat = message(&format!("hold-{index}"), "hold one stream credit");
                let task_handle = handle.clone();
                let sent = chat.clone();
                let task = tokio::spawn(async move { task_handle.send(peer_id, sent).await });
                let (held_ack, _) = checked(
                    timeout(PROBE_TIMEOUT, accept_chat(&conn, &chat)).await,
                    "occupy a stream credit",
                );
                outstanding.push((task, held_ack));
            }

            let next = message("credit", "wait for one of the older streams to finish");
            let mut send = pin!(handle.send(peer_id, next.clone()));
            tokio::select! {
                reply = send.as_mut() => panic!("send ended while stream credit was held: {reply:?}"),
                _ = conn.accept_bi() => panic!("a new stream arrived while all credits were held"),
                () = sleep(CONNECT_TIMEOUT + Duration::from_millis(100)) => {}
            }
            assert!(conn.close_reason().is_none());
            // noq advertises new stream credits in batches when the window is full.
            for _ in 0..3 {
                let (task, mut ack) = outstanding.pop().expect("one held stream");
                write(&mut ack, &ChatReply::Ack).await;
                checked(ack.finish(), "release a stream credit");
                assert_eq!(checked(task.await, "join a released send"), Ok(ChatReply::Ack));
            }
            let (reply, ()) = checked(
                timeout(PROBE_TIMEOUT, async { tokio::join!(send, reply_to_chat(&conn, &next)) })
                    .await,
                "send after the peer advertises more credits",
            );
            assert_eq!(reply, Ok(ChatReply::Ack));
            for (task, mut ack) in outstanding {
                write(&mut ack, &ChatReply::Ack).await;
                checked(ack.finish(), "release another stream credit");
                assert_eq!(checked(task.await, "join an outstanding send"), Ok(ChatReply::Ack));
            }

            checked(link.shutdown().await, "stop link after a full stream window");
            peer.iroh().close().await;
        })
        .await,
        "stream credit test timed out",
    );
}

enum WinnerAction {
    Acknowledge,
    Replace,
    Stall,
    Unavailable,
}

struct CrossedRedial {
    temporary: Connection,
    winner: Connection,
    winner_hello: SendStream,
    remote: Hello,
}

async fn stage_crossed_redial(peer: &Endpoint, address: &EndpointAddr) -> CrossedRedial {
    let (temporary, mut hello) = accept_hello(peer).await;
    let winner = dial(peer, address).await;
    let (winner_hello, remote) = begin_hello(&winner, &Hello::local(RAW_GENERATION, [2; 16])).await;
    hello.reply(RAW_GENERATION, [3; 16]).await;
    hello.confirm([3; 16]).await;
    CrossedRedial {
        temporary,
        winner,
        winner_hello,
        remote,
    }
}

async fn while_sending<T>(
    send: Pin<&mut impl Future<Output = Result<ChatReply, SendError>>>,
    step: impl Future<Output = T>,
) -> T {
    tokio::select! {
        reply = send => panic!("send ended before the next test step: {reply:?}"),
        output = step => output,
    }
}

async fn expect_close(conn: &Connection, code: u64) {
    assert!(
        matches!(conn.closed().await, ConnectionError::ApplicationClosed(close) if close.error_code.into_inner() == code)
    );
}

async fn start_link_for_close_wait() -> (Link, Endpoint, Arc<Authorizer>) {
    let (endpoint, auth) = bind_direct().await;
    let (peer, peer_auth) = bind_direct().await;
    allow_peers((&endpoint, &auth), (&peer, &peer_auth));
    add_address(&endpoint, loopback_address(&peer));
    peer.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
    let (link, _) = Link::start(
        endpoint,
        LinkConfig {
            send_deadline: PROBE_TIMEOUT,
            ..config()
        },
    );
    (link, peer, auth)
}

async fn reset_first_chat(peer: &Endpoint, chat: &ChatMessage) -> Connection {
    let (conn, mut hello) = accept_hello(peer).await;
    hello.reply(RAW_GENERATION, [1; 16]).await;
    hello.confirm([1; 16]).await;
    let (mut ack, _) = accept_chat(&conn, chat).await;
    checked(ack.reset(0u32.into()), "reset the chat reply stream");
    conn
}

#[tokio::test]
async fn chat_retry_waits_for_a_later_replacement_close() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (link, peer, _) = start_link_for_close_wait().await;
            let handle = link.handle();
            let chat = message("replacement-close", "retry after the replacement close");
            let mut send = pin!(handle.send(peer.iroh().id(), chat.clone()));
            let first = while_sending(send.as_mut(), reset_first_chat(&peer, &chat)).await;
            // Leave the connection open while the sender processes the stream reset.
            while_sending(send.as_mut(), sleep(Duration::from_millis(300))).await;
            first.close(409u32.into(), b"");

            let (reply, second) = tokio::join!(send, async {
                let (second, mut hello) = accept_hello(&peer).await;
                hello.reply(RAW_GENERATION, [2; 16]).await;
                hello.confirm([2; 16]).await;
                reply_to_chat(&second, &chat).await;
                second
            });
            assert_eq!(reply, Ok(ChatReply::Ack));
            assert!(second.close_reason().is_none());
            checked(
                link.shutdown().await,
                "stop link after the replacement retry",
            );
            peer.iroh().close().await;
        })
        .await,
        "late replacement close test timed out",
    );
}

#[tokio::test]
async fn chat_reset_on_an_open_connection_waits_only_its_connect_budget() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (link, peer, _) = start_link_for_close_wait().await;
            let handle = link.handle();
            let chat = message("reset", "the connection stays open after the reset");
            let mut send = pin!(handle.send(peer.iroh().id(), chat.clone()));
            let first = while_sending(send.as_mut(), reset_first_chat(&peer, &chat)).await;
            let reset_at = Instant::now();
            // Leave plenty of send budget beyond this outer test bound so a longer
            // wait cannot pass merely because the send's own overall deadline fires.
            let reply = checked(
                timeout(CONNECT_TIMEOUT + Duration::from_secs(1), send).await,
                "send remained pending past the close wait",
            );
            assert_eq!(reply, Err(SendError::Disconnected));
            assert!(reset_at.elapsed() >= CONNECT_TIMEOUT - Duration::from_millis(250));
            assert!(first.close_reason().is_none());
            checked(link.shutdown().await, "stop link after the reset wait");
            peer.iroh().close().await;
        })
        .await,
        "open connection reset test timed out",
    );
}

#[tokio::test]
async fn revoking_a_peer_ends_a_send_waiting_for_a_close() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (link, peer, auth) = start_link_for_close_wait().await;
            let handle = link.handle();
            let peer_id = peer.iroh().id();
            let chat = message("revoked", "revoke while waiting for close");
            let mut send = pin!(handle.send(peer_id, chat.clone()));
            let _first = while_sending(send.as_mut(), reset_first_chat(&peer, &chat)).await;
            while_sending(send.as_mut(), sleep(Duration::from_millis(300))).await;

            auth.replace_peers([]);
            handle.close_peer(peer_id);
            let reply = checked(
                timeout(Duration::from_secs(1), send).await,
                "revocation did not wake the send",
            );
            assert_eq!(reply, Err(SendError::Closed));
            checked(link.shutdown().await, "stop link after revocation");
            peer.iroh().close().await;
        })
        .await,
        "revocation during close wait test timed out",
    );
}

async fn crossed_redial(install_before_polling: bool, action: WinnerAction) {
    let (lower, higher) = ordered_keys();
    let (endpoint, auth) = bind_direct_with_key(&higher).await;
    let (peer, peer_auth) = bind_direct_with_key(&lower).await;
    let (_, peer_id) = allow_peers((&endpoint, &auth), (&peer, &peer_auth));
    let address = loopback_address(&endpoint);
    add_address(&endpoint, loopback_address(&peer));
    peer.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
    let budget = LinkConfig {
        connect_timeout: CONNECT_TIMEOUT,
        send_deadline: CONNECT_TIMEOUT * 3,
    };
    let (link, _inbox) = Link::start(endpoint, budget);
    let handle = link.handle();
    let chat = message("crossed-redial", "send while both peers reconnect");
    let started = Instant::now();
    let mut send = pin!(handle.send(peer_id, chat.clone()));
    let (initial, initial_ack) = while_sending(send.as_mut(), async {
        let (initial, mut hello) = accept_hello(&peer).await;
        hello.reply(RAW_GENERATION, [1; 16]).await;
        hello.confirm([1; 16]).await;
        let (initial_ack, _) = accept_chat(&initial, &chat).await;
        (initial, initial_ack)
    })
    .await;

    if matches!(action, WinnerAction::Stall) {
        // Spend enough of the budget on the first attempt to detect a reset on retry.
        sleep(CONNECT_TIMEOUT / 2).await;
    }
    initial.close(0u32.into(), b"");
    wait_for_path(&handle, peer_id, LinkPath::Down).await;
    let CrossedRedial {
        temporary,
        winner,
        mut winner_hello,
        remote,
    } = if install_before_polling {
        // Leave the send parked while another send installs a new connection. The
        // earlier closure is ordinary even though another connection is already open.
        let probe = message(
            "reconnect",
            "install a link before the original send resumes",
        );
        let (ack, crossed) = tokio::join!(handle.send(peer_id, probe.clone()), async {
            let crossed = stage_crossed_redial(&peer, &address).await;
            reply_to_chat(&crossed.temporary, &probe).await;
            crossed
        });
        assert_eq!(ack, Ok(ChatReply::Ack));
        crossed
    } else {
        while_sending(send.as_mut(), stage_crossed_redial(&peer, &address)).await
    };
    drop(initial_ack);

    // Admit the higher-ID dial first. The retry must reach it before the lower-ID
    // dial finishes its hello and supersedes it.
    let (retry_ack, _) = while_sending(send.as_mut(), accept_chat(&temporary, &chat)).await;
    finish_hello(&mut winner_hello, &remote).await;
    expect_close(&temporary, 409).await;
    drop(retry_ack);

    // accept_chat checks the whole message, including the original ID, on each attempt.
    let (mut winner_ack, _) = while_sending(send.as_mut(), accept_chat(&winner, &chat)).await;
    match action {
        WinnerAction::Acknowledge => {
            write(&mut winner_ack, &ChatReply::Ack).await;
            checked(winner_ack.finish(), "finish ACK on the winning connection");
            assert_eq!(send.await, Ok(ChatReply::Ack));
        }
        WinnerAction::Replace => {
            // A fresh connection from the same initiator can replace the winner again.
            let replacement = dial(&peer, &address).await;
            let (mut hello, remote) =
                begin_hello(&replacement, &Hello::local(RAW_GENERATION, [4; 16])).await;
            finish_hello(&mut hello, &remote).await;
            expect_close(&winner, 409).await;
            assert_eq!(send.await, Err(SendError::Disconnected));

            let fresh = message("fresh", "the same-initiator replacement is active");
            let (reply, ()) = tokio::join!(
                handle.send(peer_id, fresh.clone()),
                reply_to_chat(&replacement, &fresh),
            );
            assert_eq!(reply, Ok(ChatReply::Ack));
        }
        WinnerAction::Stall => {
            // The send must still expire at one deadline after its first poll.
            let latest = started + budget.send_deadline + Duration::from_millis(500);
            let result = checked(timeout_at(latest, send).await, "original send deadline");
            assert_eq!(result, Err(SendError::Timeout));
            assert!(winner.close_reason().is_none());
        }
        WinnerAction::Unavailable => {
            let unavailable = ChatReply::Nack(NackReason::Unavailable);
            write(&mut winner_ack, &unavailable).await;
            checked(
                winner_ack.finish(),
                "finish unavailable reply on the winning connection",
            );
            assert_eq!(send.await, Err(SendError::Timeout));

            let first_try = message("unavailable-first", "try the link with a different message");
            let (result, ()) = tokio::join!(handle.send(peer_id, first_try.clone()), async {
                let (mut ack, _) = accept_chat(&winner, &first_try).await;
                write(&mut ack, &unavailable).await;
                checked(
                    ack.finish(),
                    "finish unavailable reply to the first attempt",
                );
            });
            assert_eq!(result, Ok(unavailable));
        }
    }

    checked(link.shutdown().await, "stop link after crossed redial");
    peer.iroh().close().await;
}

#[tokio::test]
async fn crossed_redial_retries_after_closure_and_replacement_with_the_same_message() {
    checked(
        timeout(
            CASE_TIMEOUT,
            crossed_redial(false, WinnerAction::Acknowledge),
        )
        .await,
        "crossed redial send timed out",
    );
}

#[tokio::test]
async fn crossed_redial_does_not_retry_a_second_replacement() {
    checked(
        timeout(CASE_TIMEOUT, crossed_redial(false, WinnerAction::Replace)).await,
        "repeated replacement test timed out",
    );
}

#[tokio::test]
async fn crossed_redial_keeps_the_original_deadline() {
    checked(
        timeout(CASE_TIMEOUT, crossed_redial(false, WinnerAction::Stall)).await,
        "replacement send deadline test timed out",
    );
}

#[tokio::test]
async fn late_observation_of_an_ordinary_close_preserves_the_replacement_retry() {
    checked(
        timeout(
            CASE_TIMEOUT,
            crossed_redial(true, WinnerAction::Acknowledge),
        )
        .await,
        "late ordinary close test timed out",
    );
}

#[tokio::test]
async fn unavailable_after_retry_reports_uncertain_delivery() {
    checked(
        timeout(
            CASE_TIMEOUT,
            crossed_redial(false, WinnerAction::Unavailable),
        )
        .await,
        "unavailable after retry test timed out",
    );
}

async fn lose_outgoing_dial(peer: &Endpoint, address: &EndpointAddr, outgoing: &Connection) {
    // Reading the server hello proves the winning incoming dial is pending locally.
    let winner = dial(peer, address).await;
    let (mut hello, _) = begin_hello(&winner, &Hello::local(RAW_GENERATION, [2; 16])).await;
    outgoing.close(409u32.into(), b"");
    // The winning peer ends the hello stream before confirming the challenge.
    checked(hello.finish(), "end the winning hello before confirmation");
    expect_close(&winner, 400).await;
}

async fn redial_after_lost_hello(lose_second_dial: bool) {
    let (lower, higher) = ordered_keys();
    let (endpoint, auth) = bind_direct_with_key(&higher).await;
    let (peer, peer_auth) = bind_direct_with_key(&lower).await;
    let (_, peer_id) = allow_peers((&endpoint, &auth), (&peer, &peer_auth));
    let address = loopback_address(&endpoint);
    add_address(&endpoint, loopback_address(&peer));
    peer.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
    let (link, _inbox) = Link::start(
        endpoint,
        LinkConfig {
            connect_timeout: PROBE_TIMEOUT,
            send_deadline: PROBE_TIMEOUT * 3,
        },
    );
    let handle = link.handle();
    let chat = message("redial", "keep the queued chat across a lost dial");
    let mut send = pin!(handle.send(peer_id, chat.clone()));
    let (first, first_hello) = while_sending(send.as_mut(), accept_hello(&peer)).await;
    while_sending(send.as_mut(), lose_outgoing_dial(&peer, &address, &first)).await;

    let (second, mut second_hello) = while_sending(send.as_mut(), accept_hello(&peer)).await;
    assert!(
        first_hello.hello.generation == second_hello.hello.generation,
        "the running link keeps its generation"
    );
    assert!(
        first_hello.hello.challenge != second_hello.hello.challenge,
        "each outgoing hello uses a fresh challenge"
    );
    if lose_second_dial {
        // Let the test observe the winner's failure before polling the send again.
        lose_outgoing_dial(&peer, &address, &second).await;
        tokio::select! {
            result = send => assert_eq!(result, Err(SendError::Disconnected)),
            _ = peer.iroh().accept() => panic!("a third dial followed the second loss"),
        }
        assert_eq!(handle.path(peer_id), LinkPath::Down);
    } else {
        let (reply, ()) = tokio::join!(send, async {
            second_hello.reply(RAW_GENERATION, [3; 16]).await;
            second_hello.confirm([3; 16]).await;
            reply_to_chat(&second, &chat).await;
        });
        assert_eq!(reply, Ok(ChatReply::Ack));
    }

    checked(link.shutdown().await, "stop link after lost dial");
    peer.iroh().close().await;
}

#[tokio::test]
async fn lost_dial_retries_after_the_pending_winner_fails_its_hello() {
    checked(
        timeout(CASE_TIMEOUT, redial_after_lost_hello(false)).await,
        "retry after lost dial timed out",
    );
}

#[tokio::test]
async fn lost_dial_stops_after_a_second_loss() {
    checked(
        timeout(CASE_TIMEOUT, redial_after_lost_hello(true)).await,
        "second dial loss test timed out",
    );
}

#[tokio::test]
async fn duplicate_pending_hello_returns_without_waiting_for_a_winner() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (caller, caller_auth) = bind_direct().await;
            let (callee, callee_auth) = bind_direct().await;
            let (caller_id, callee_id) =
                allow_peers((&caller, &caller_auth), (&callee, &callee_auth));
            let address = EndpointAddr::new(callee_id).with_ip_addr(loopback_socket(&callee));
            checked(caller.iroh().address_lookup(), "read address lookup")
                .add(MemoryLookup::from_endpoint_info([address.clone()]));
            let (callee, mut inbox) = Link::start(
                callee,
                LinkConfig {
                    connect_timeout: Duration::from_secs(12),
                    ..config()
                },
            );
            let callee_handle = callee.handle();

            // Neither connection sends a hello. Seeing one refused tells us the other has
            // occupied the pending slot, independent of which accept task ran first.
            let first = dial(&caller, &address).await;
            let (held, refusal) = match caller.iroh().connect(address, LINK_ALPN).await {
                Ok(second) => tokio::select! {
                    reason = first.closed() => (second, reason),
                    reason = second.closed() => (first, reason),
                },
                Err(ConnectError::Connecting {
                    source: ConnectingError::ConnectionError { source, .. },
                    ..
                })
                | Err(ConnectError::Connection { source, .. }) => (first, source),
                Err(_) => panic!("expected the receiver to refuse a duplicate connection"),
            };
            assert!(matches!(
                refusal,
                ConnectionError::ApplicationClosed(close) if close.error_code.into_inner() == 0
            ));
            assert!(held.close_reason().is_none());

            let (caller, _inbox) = Link::start(caller, config());
            let handle = caller.handle();
            let result = checked(
                timeout(
                    Duration::from_secs(3),
                    handle.send(callee_id, message("busy", "pending hello")),
                )
                .await,
                "pending hello refusal should return before the send deadline",
            );
            assert!(matches!(result, Err(SendError::Disconnected)));
            assert!(held.close_reason().is_none());
            assert_eq!(handle.path(callee_id), LinkPath::Down);
            assert!(matches!(inbox.try_recv(), Err(TryRecvError::Empty)));

            callee_handle.close_peer(caller_id);
            wait_for_close(&held).await;
            let fresh = message("fresh", "after the held connection closes");
            let (reply, ()) = tokio::join!(
                handle.send(callee_id, fresh.clone()),
                acknowledge(&mut inbox, caller_id, "first", &fresh),
            );
            assert!(matches!(reply, Ok(ChatReply::Ack)));

            let (caller_result, callee_result) = tokio::join!(caller.shutdown(), callee.shutdown());
            checked(caller_result, "stop caller link");
            checked(callee_result, "stop callee link");
        })
        .await,
        "pending hello test timed out",
    );
}
