#![cfg(test)]

use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{
    ConnectError, ConnectingError, Connection, ConnectionError, RecvStream, SendStream,
};
use numbers::link::{IncomingChat, LINK_ALPN, Link, LinkConfig, LinkHandle, LinkPath, SendError};
use numbers::proto::chat::{CHAT_BODY_MAX, ChatMessage, ChatReply, NackReason, ReceiptWindow};
use numbers::proto::{CHAT_PROTOCOL, CHAT_VERSION, Hello, StreamHeader};
use numbers::{
    Authorizer, Endpoint, EndpointConfig, IpTransport, NodeId, TowerSet, generate_node_key,
};
use numbers_base::{FrameError, WIRE_FRAME_MAX, read_frame, write_frame};
use numbers_tower::{Tower, TowerConfig};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Receiver, error::TryRecvError};
use tokio::time::{sleep, timeout};

const CASE_TIMEOUT: Duration = Duration::from_secs(45);
// A loaded test runner can miss the first net report that selects the home relay.
const ONLINE_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const SEND_DEADLINE: Duration = Duration::from_secs(4);
const PROBE_TIMEOUT: Duration = Duration::from_secs(6);

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
    let auth = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
    let endpoint = checked(
        Endpoint::bind(&generate_node_key(), config, Arc::clone(&auth)).await,
        "bind test endpoint",
    );
    (endpoint, auth)
}

async fn bind_direct() -> (Endpoint, Arc<Authorizer>) {
    bind(EndpointConfig {
        towers: TowerSet::default(),
        ip_transport: IpTransport::LoopbackOnly,
    })
    .await
}

fn loopback_socket(endpoint: &Endpoint) -> SocketAddr {
    let sockets = endpoint.iroh().bound_sockets();
    assert_eq!(sockets.len(), 1);
    assert_eq!(sockets[0].ip(), Ipv4Addr::LOCALHOST);
    sockets[0]
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
    let (mut send, mut recv) = checked(conn.accept_bi().await, "accept chat stream");
    let header: StreamHeader = read(&mut recv).await;
    assert_eq!(header.proto, CHAT_PROTOCOL);
    assert_eq!(header.v, CHAT_VERSION);
    let message: serde_json::Value = read(&mut recv).await;
    assert!(message == checked(serde_json::to_value(expected), "serialize expected chat"));
    write(&mut send, &ChatReply::Ack).await;
    checked(send.finish(), "finish chat reply");
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

async fn accept_hello(endpoint: &Endpoint) -> (Connection, SendStream) {
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
    assert_eq!(hello.v, Hello::local().v);
    assert!(hello.network.is_empty() && hello.head.is_empty());
    (conn, send)
}

#[tokio::test]
async fn simultaneous_sends_finish_in_both_directions_over_the_relay() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let tower = checked(
                Tower::spawn(TowerConfig {
                    bind: (Ipv4Addr::LOCALHOST, 0).into(),
                })
                .await,
                "start loopback tower",
            );
            let relay_config = EndpointConfig {
                towers: checked(TowerSet::from_url(tower.url()), "parse tower URL"),
                ip_transport: IpTransport::RelayOnly,
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
            let second_message = message("second", "return message");
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
            wait_for_close(&malformed).await;
            assert_eq!(handle.path(peer_id), LinkPath::Down);

            let conn = dial(&peer, &address).await;
            let (mut send, mut recv) = checked(conn.open_bi().await, "open valid hello");
            write(&mut send, &Hello::local()).await;
            checked(send.finish(), "finish valid hello");
            let hello: Hello = read(&mut recv).await;
            assert_eq!(hello.v, Hello::local().v);
            assert!(hello.network.is_empty() && hello.head.is_empty());
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
                let result = request(&conn, &header, &probe).await;
                assert!(match result {
                    Ok(ChatReply::Nack(actual)) => actual == reason,
                    Err(FrameError::Closed | FrameError::Io(_)) => true,
                    _ => false,
                });
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

            let (mut send, mut recv) = checked(conn.open_bi().await, "open oversize frame stream");
            let oversize_length = u32::try_from(WIRE_FRAME_MAX + 1).expect("wire cap fits u32");
            let mut prefix = Vec::new();
            checked(
                write_frame(&mut prefix, &StreamHeader::chat(), WIRE_FRAME_MAX).await,
                "encode chat header",
            );
            prefix.extend_from_slice(&oversize_length.to_be_bytes());
            checked(send.write_all(&prefix).await, "announce oversize frame");
            assert!(matches!(
                read_frame::<_, ChatReply>(&mut recv, WIRE_FRAME_MAX, Some(PROBE_TIMEOUT)).await,
                Err(FrameError::Closed | FrameError::Io(_)),
            ));
            assert!(matches!(inbox.try_recv(), Err(TryRecvError::Empty)));

            let valid = message("valid", "same connection");
            let chat_header = StreamHeader::chat();
            let (reply, ()) = tokio::join!(
                request(&conn, &chat_header, &valid),
                acknowledge(&mut inbox, peer_id, "second", &valid),
            );
            assert!(matches!(reply, Ok(ChatReply::Ack)));

            // The raw peer has neither a router nor a tower address for a new dial.
            let reverse = message("reverse", "reuse accepted link");
            let (reply, ()) = tokio::join!(
                handle.send(peer_id, reverse.clone()),
                reply_to_chat(&conn, &reverse),
            );
            assert!(matches!(reply, Ok(ChatReply::Ack)));

            checked(link.shutdown().await, "stop direct link");
            peer.iroh().close().await;
        })
        .await,
        "stream validation test timed out",
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
                write(&mut hello, &Hello::local()).await;
                checked(hello.finish(), "finish delayed hello");
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
            // The remote may already have seen the close before this delayed reply.
            let _ = write_frame(&mut hello, &Hello::local(), WIRE_FRAME_MAX).await;
            let _ = hello.finish();
            let send_result = checked(pending.await, "join revoked send");
            assert!(matches!(
                send_result,
                Err(SendError::Closed | SendError::NotAllowed)
            ));
            wait_for_close(&retired).await;
            assert_eq!(handle.path(peer_id), LinkPath::Down);
            assert!(matches!(
                handle
                    .send(peer_id, message("removed", "cannot reconnect"))
                    .await,
                Err(SendError::NotAllowed)
            ));
            loop {
                match changes.try_recv() {
                    Ok(update) => assert_eq!(update.path, LinkPath::Down),
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(_) => panic!("unexpected interruption to path tracking"),
                }
            }

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
            assert!(matches!(refusal, ConnectionError::ApplicationClosed(_)));
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
