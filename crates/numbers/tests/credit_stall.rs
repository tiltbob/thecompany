#![cfg(test)]

use std::net::{Ipv4Addr, UdpSocket};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use numbers::link::{IncomingChat, LINK_ALPN, Link, LinkConfig, LinkHandle, LinkPath, SendError};
use numbers::proto::chat::{ChatMessage, ChatReply};
use numbers::proto::{CHAT_PROTOCOL, Hello, HelloEcho, StreamHeader};
use numbers::{
    Authorizer, Endpoint, EndpointConfig, IpTransport, NodeId, generate_node_key, node_id,
};
use numbers_base::{WIRE_FRAME_MAX, read_frame, write_frame};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, sleep_until, timeout};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const SHORT_SEND_DEADLINE: Duration = Duration::from_secs(4);
const LONG_SEND_DEADLINE: Duration = Duration::from_secs(12);
const CASE_TIMEOUT: Duration = Duration::from_secs(20);

fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    result.unwrap_or_else(|_| panic!("{operation}"))
}

fn config(send_deadline: Duration) -> LinkConfig {
    LinkConfig {
        connect_timeout: CONNECT_TIMEOUT,
        send_deadline,
    }
}

fn message(id: &str) -> ChatMessage {
    ChatMessage {
        id: id.into(),
        sent_at_ms: 123,
        body: "test chat".into(),
    }
}

async fn bind(key: &[u8; 32], peer: NodeId) -> (Endpoint, EndpointAddr) {
    let authorizer = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
    authorizer.replace_peers([(peer, "peer".into())]);
    let endpoint = checked(
        Endpoint::bind(
            key,
            EndpointConfig {
                ip_transport: IpTransport::LoopbackOnly,
                ..EndpointConfig::default()
            },
            authorizer,
        )
        .await,
        "bind loopback endpoint",
    );
    let sockets = endpoint.iroh().bound_sockets();
    assert_eq!(sockets.len(), 1);
    assert_eq!(sockets[0].ip(), Ipv4Addr::LOCALHOST);
    let address = EndpointAddr::new(endpoint.iroh().id()).with_ip_addr(sockets[0]);
    (endpoint, address)
}

async fn write(send: &mut SendStream, value: &impl Serialize) {
    checked(
        write_frame(send, value, WIRE_FRAME_MAX).await,
        "write frame",
    );
}

async fn read<T: DeserializeOwned>(recv: &mut RecvStream) -> T {
    checked(read_frame(recv, WIRE_FRAME_MAX, None).await, "read frame")
}

async fn raw_peer(key: &[u8; 32], address: EndpointAddr) -> (Endpoint, Connection) {
    let (endpoint, _) = bind(key, address.id).await;
    let connection = checked(
        endpoint.iroh().connect(address, LINK_ALPN).await,
        "dial survivor",
    );
    // Initiate the hello from the raw peer so it uses none of the survivor's credits.
    let (mut send, mut recv) = checked(connection.open_bi().await, "open hello stream");
    let hello = Hello::local([1; 16], [2; 16]);
    write(&mut send, &hello).await;
    let response: Hello = read(&mut recv).await;
    assert_eq!(response.validate(), Ok(()));
    assert!(response.echo == Some(hello.challenge));
    write(
        &mut send,
        &HelloEcho {
            echo: response.challenge,
        },
    )
    .await;
    checked(send.finish(), "finish hello");
    (endpoint, connection)
}

async fn wait_for_path(handle: &LinkHandle, peer: NodeId) {
    let mut paths = handle.subscribe_paths();
    while handle.path(peer) != LinkPath::Direct {
        checked(paths.recv().await, "observe direct link");
    }
}

async fn accept_chat(connection: &Connection, expected: &ChatMessage) -> SendStream {
    let (send, mut recv) = checked(connection.accept_bi().await, "accept chat stream");
    let header: StreamHeader = read(&mut recv).await;
    assert_eq!(header.proto, CHAT_PROTOCOL);
    let request: serde_json::Value = read(&mut recv).await;
    assert!(request == checked(serde_json::to_value(expected), "encode expected chat"));
    send
}

async fn ack(mut send: SendStream) {
    write(&mut send, &ChatReply::Ack).await;
    checked(send.finish(), "finish chat reply");
}

async fn reply_from_inbox(inbox: &mut mpsc::Receiver<IncomingChat>, expected: &ChatMessage) {
    let chat = inbox.recv().await.expect("receive chat from the link");
    assert!(chat.message.id == expected.id);
    assert!(chat.message.body == expected.body);
    chat.respond(ChatReply::Ack);
}

async fn fill_window(
    handle: &LinkHandle,
    peer: NodeId,
    connection: &Connection,
) -> (JoinSet<Result<ChatReply, SendError>>, Vec<SendStream>) {
    let mut sends = JoinSet::new();
    let mut replies = Vec::new();
    for index in 0..16 {
        let chat = message(&format!("held-{index}"));
        let task_chat = chat.clone();
        let handle = handle.clone();
        sends.spawn(async move { handle.send(peer, task_chat).await });
        replies.push(accept_chat(connection, &chat).await);
    }
    (sends, replies)
}

async fn verify_blocked(
    connection: &Connection,
    handle: &LinkHandle,
    peer: NodeId,
    inbox: &mut mpsc::Receiver<IncomingChat>,
) {
    let before = connection.stats().frame_rx.streams_blocked_bidi;
    tokio::select! {
        biased;
        _ = handle.send(peer, message("blocked")) => panic!("the credit probe finished"),
        _ = connection.accept_bi() => panic!("the credit probe opened a stream"),
        () = async {
            // A reverse chat wakes the transport driver before the held sends reach their
            // deadlines, so the peer sees the queued STREAMS_BLOCKED frame.
            let probe = message("reverse-probe");
            let ((), ()) = tokio::join!(reply_from_inbox(inbox, &probe), async {
                let (mut send, mut recv) = checked(connection.open_bi().await, "open reverse chat");
                write(&mut send, &StreamHeader::chat()).await;
                write(&mut send, &probe).await;
                checked(send.finish(), "finish reverse request");
                assert_eq!(read::<ChatReply>(&mut recv).await, ChatReply::Ack);
            });
            while connection.stats().frame_rx.streams_blocked_bidi == before {
                sleep(Duration::from_millis(10)).await;
            }
        } => {}
    }
}

async fn recover_after_crash() {
    let survivor_key = generate_node_key();
    let peer_key = generate_node_key();
    let peer_id = node_id(&peer_key);
    let (endpoint, survivor_addr) = bind(&survivor_key, peer_id).await;
    let lookup = MemoryLookup::new();
    checked(endpoint.iroh().address_lookup(), "get survivor lookup").add(lookup.clone());
    let (survivor, mut inbox) = Link::start(endpoint, config(SHORT_SEND_DEADLINE));
    let handle = survivor.handle();
    let (ready, connection) = oneshot::channel();
    let (crash, stop) = oneshot::channel::<()>();
    let original = tokio::task::spawn_blocking(move || {
        let runtime = checked(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build(),
            "start separate runtime for the original peer",
        );
        let endpoint = runtime.block_on(async move {
            let (endpoint, connection) = raw_peer(&peer_key, survivor_addr).await;
            let socket = endpoint.iroh().bound_sockets()[0];
            checked(
                ready.send((connection, socket)),
                "share raw peer connection",
            );
            let _ = stop.await;
            endpoint
        });
        // End its drivers before dropping endpoint or reply streams so no close is sent.
        runtime.shutdown_background();
        drop(endpoint);
    });
    let (connection, old_socket) = checked(connection.await, "receive original peer connection");
    wait_for_path(&handle, peer_id).await;
    let (mut sends, replies) = fill_window(&handle, peer_id, &connection).await;
    verify_blocked(&connection, &handle, peer_id, &mut inbox).await;
    // Let the reverse probe's remaining acknowledgements reach the survivor before the crash.
    sleep(Duration::from_millis(100)).await;
    checked(crash.send(()), "crash original peer runtime");
    checked(original.await, "join original peer runtime");
    drop(replies);
    drop(connection);
    assert_eq!(handle.path(peer_id), LinkPath::Direct);

    // Keep the restart on a fresh port even if the system would reuse the old one.
    let _old_port = UdpSocket::bind(old_socket).ok();
    let (endpoint, address) = bind(&peer_key, node_id(&survivor_key)).await;
    assert!(address.ip_addrs().all(|socket| *socket != old_socket));
    lookup.set_endpoint_info(address);
    let (restarted, mut inbox) = Link::start(endpoint, config(SHORT_SEND_DEADLINE));

    // A single caller has less time than one keepalive window. The following call must
    // use the stalled link's existing observation, without the restarted peer dialing first.
    assert_eq!(
        handle.send(peer_id, message("first-after-crash")).await,
        Err(SendError::Timeout),
    );
    while let Some(result) = sends.join_next().await {
        assert_eq!(checked(result, "join held send"), Err(SendError::Timeout));
    }
    assert_eq!(handle.path(peer_id), LinkPath::Direct);
    let after = message("recovery");
    let mut send = pin!(handle.send(peer_id, after.clone()));
    let reply = tokio::select! {
        reply = send.as_mut() => reply,
        () = reply_from_inbox(&mut inbox, &after) => send.await,
    };
    assert_eq!(reply, Ok(ChatReply::Ack));
    let (first, second) = tokio::join!(survivor.shutdown(), restarted.shutdown());
    checked(first, "shut down survivor link");
    checked(second, "shut down restarted link");
}

#[tokio::test]
async fn full_window_recovers_across_short_sends_after_peer_crash() {
    checked(
        timeout(CASE_TIMEOUT, recover_after_crash()).await,
        "recover the full credit window before QUIC's idle timeout",
    );
}

async fn keep_quiet_link() {
    let local_key = generate_node_key();
    let peer_key = generate_node_key();
    let peer_id = node_id(&peer_key);
    let (endpoint, address) = bind(&local_key, peer_id).await;
    let (link, _inbox) = Link::start(endpoint, config(LONG_SEND_DEADLINE));
    let handle = link.handle();
    let (peer, connection) = raw_peer(&peer_key, address).await;
    wait_for_path(&handle, peer_id).await;
    let (mut sends, mut replies) = fill_window(&handle, peer_id, &connection).await;

    // Let the transport finish acknowledging the held requests before starting the wait.
    sleep(Duration::from_millis(300)).await;
    let before = connection.stats();
    let start = Instant::now();
    let next = message("quiet-waiter");
    let mut send = pin!(handle.send(peer_id, next.clone()));
    tokio::select! {
        biased;
        _ = send.as_mut() => panic!("the quiet credit wait ended early"),
        _ = connection.accept_bi() => panic!("the credit wait opened a stream or lost its link"),
        () = async {
            sleep(CONNECT_TIMEOUT + Duration::from_millis(100)).await;
            assert_eq!(connection.stats().frame_rx.streams_blocked_bidi, before.frame_rx.streams_blocked_bidi);
            // Wait past the keepalive interval and the stream-open progress checkpoint.
            sleep_until(start + Duration::from_millis(6500)).await;
            let after = connection.stats();
            assert!(after.frame_rx.streams_blocked_bidi > before.frame_rx.streams_blocked_bidi);
            assert!(after.frame_tx.acks + after.frame_tx.path_acks > before.frame_tx.acks + before.frame_tx.path_acks);
        } => {}
    }
    assert!(connection.close_reason().is_none());
    // Credit is replenished in batches once more than two held streams finish.
    for reply in replies.drain(..3) {
        ack(reply).await;
    }
    let (reply, ()) = tokio::join!(send, async {
        ack(accept_chat(&connection, &next).await).await
    });
    assert_eq!(reply, Ok(ChatReply::Ack));
    for reply in replies {
        ack(reply).await;
    }
    while let Some(result) = sends.join_next().await {
        assert_eq!(checked(result, "join released send"), Ok(ChatReply::Ack));
    }
    checked(link.shutdown().await, "shut down quiet link");
    peer.iroh().close().await;
}

#[tokio::test]
async fn healthy_window_waits_for_keepalive_and_reuses_the_link() {
    checked(
        timeout(CASE_TIMEOUT, keep_quiet_link()).await,
        "wait for credit on the quiet link",
    );
}
