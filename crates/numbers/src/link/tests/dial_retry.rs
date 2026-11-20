use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;

use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;
use numbers_base::{WIRE_FRAME_MAX, read_frame, write_frame};

use crate::proto::StreamHeader;
use crate::proto::chat::WireChatMessage;

use super::*;

const CONNECT_BUDGET: Duration = Duration::from_secs(5);
const SEND_BUDGET: Duration = Duration::from_secs(15);
const CASE_BUDGET: Duration = Duration::from_secs(20);

fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    result.unwrap_or_else(|_| panic!("{operation}"))
}

async fn linked_endpoints() -> (Link, Endpoint, EndpointAddr, Arc<Peer>) {
    let local = loopback_endpoint().await;
    let remote = loopback_endpoint().await;
    let local_addr =
        EndpointAddr::new(local.iroh().id()).with_ip_addr(local.iroh().bound_sockets()[0]);
    let remote_id = remote.iroh().id();
    let remote_addr = EndpointAddr::new(remote_id).with_ip_addr(remote.iroh().bound_sockets()[0]);
    local
        .authorizer()
        .replace_peers([(remote_id, "peer".into())]);
    remote
        .authorizer()
        .replace_peers([(local_addr.id, "peer".into())]);
    checked(local.iroh().address_lookup(), "get loopback address lookup")
        .add(MemoryLookup::from_endpoint_info([remote_addr]));
    let (link, _inbox) = Link::start(
        local,
        LinkConfig {
            connect_timeout: CONNECT_BUDGET,
            send_deadline: SEND_BUDGET,
        },
    );
    let peer = checked(link.shared.peer(remote_id), "track the allowed peer");
    (link, remote, local_addr, peer)
}

fn chat(id: &str) -> ChatMessage {
    ChatMessage {
        id: id.into(),
        sent_at_ms: 1,
        body: "test dial retry".into(),
    }
}

async fn poll_once<T>(mut future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

async fn wait_for_change(peer: &Peer, condition: impl Fn() -> bool) {
    let mut changes = peer.changed.subscribe();
    while !condition() {
        checked(changes.changed().await, "observe the peer state change");
    }
}

async fn wait_for_failed_dial(peer: &Peer, dial: &Dial) {
    wait_for_change(peer, || lock(&dial.result).is_some()).await;
    assert!(matches!(
        *lock(&dial.result),
        Some(DialResult::Failed(SendError::Disconnected))
    ));
}

async fn accept_outgoing(remote: &Endpoint) -> Connection {
    checked(
        remote
            .iroh()
            .accept()
            .await
            .expect("raw peer is listening")
            .await,
        "accept station's outgoing connection",
    )
}

async fn hold_outgoing_hello(remote: &Endpoint) -> (Connection, SendStream, RecvStream) {
    let connection = accept_outgoing(remote).await;
    let (send, mut recv) = checked(connection.accept_bi().await, "accept outgoing hello stream");
    let hello: Hello = checked(
        read_frame(&mut recv, WIRE_FRAME_MAX, None).await,
        "read outgoing hello",
    );
    assert!(hello.validate().is_ok() && hello.echo.is_none());
    (connection, send, recv)
}

async fn hold_incoming_hello(
    remote: &Endpoint,
    local: EndpointAddr,
) -> (Connection, SendStream, RecvStream) {
    let connection = checked(
        remote.iroh().connect(local, LINK_ALPN).await,
        "dial the station",
    );
    let (mut send, mut recv) = checked(connection.open_bi().await, "open incoming hello stream");
    let hello = Hello::local([1; 16], [2; 16]);
    checked(
        write_frame(&mut send, &hello, WIRE_FRAME_MAX).await,
        "send incoming hello",
    );
    let reply: Hello = checked(
        read_frame(&mut recv, WIRE_FRAME_MAX, None).await,
        "read station's hello",
    );
    // Reading the server hello proves the station installed the pending incoming connection.
    // Leave the stream open with the echo still unsent.
    assert!(reply.validate().is_ok() && reply.echo == Some(hello.challenge));
    (connection, send, recv)
}

async fn acknowledge_retry(connection: &Connection, expected: &ChatMessage) {
    checked(
        exchange_hello(connection, Hello::local([1; 16], [3; 16]), CONNECT_BUDGET).await,
        "complete the retry hello",
    );
    let (mut send, mut recv) = checked(connection.accept_bi().await, "accept retried chat stream");
    let header: StreamHeader = checked(
        read_frame(&mut recv, WIRE_FRAME_MAX, None).await,
        "read chat header",
    );
    assert!(header == StreamHeader::chat());
    let wire: WireChatMessage = checked(
        read_frame(&mut recv, WIRE_FRAME_MAX, None).await,
        "read retried chat",
    );
    let chat = checked(ChatMessage::try_from(wire), "validate retried chat");
    assert!(chat == *expected);
    checked(
        write_frame(&mut send, &ChatReply::Ack, WIRE_FRAME_MAX).await,
        "acknowledge retried chat",
    );
    checked(send.finish(), "finish the chat acknowledgement");
}

#[tokio::test]
async fn failed_dial_after_the_original_deadline_does_not_reserve_a_retry() {
    checked(
        timeout(CASE_BUDGET, async {
            let (link, remote, _, peer) = linked_endpoints().await;
            let handle = link.handle();
            let mut retry_available = true;
            // Exercise the retry guard directly without the outer send timer.
            let start = Instant::now() - SEND_BUDGET;
            let mut connection = pin!(handle.connection(&peer, start, &mut retry_available));
            assert!(poll_once(connection.as_mut()).await.is_pending());
            let first_dial = peer.state().dial.clone().expect("first dial is reserved");
            let (outgoing, _send, _recv) = hold_outgoing_hello(&remote).await;
            outgoing.close(CLOSE_LINK.into(), b"");
            wait_for_failed_dial(&peer, &first_dial).await;

            let result = poll_once(connection.as_mut()).await;
            assert!(
                peer.state().dial.is_none(),
                "an expired send must not reserve another dial"
            );
            assert!(matches!(result, Poll::Ready(Err(SendError::Timeout))));
            checked(link.shutdown().await, "stop link after expired dial");
            remote.iroh().close().await;
        })
        .await,
        "expired dial retry test timed out",
    );
}

#[tokio::test]
async fn failed_dial_is_not_counted_until_the_pending_incoming_hello_fails() {
    checked(
        timeout(CASE_BUDGET, async {
            let (link, remote, local_addr, peer) = linked_endpoints().await;
            let handle = link.handle();
            let chat = chat("deferred-retry");
            let mut send = pin!(handle.send(peer.id, chat.clone()));
            assert!(poll_once(send.as_mut()).await.is_pending());
            let first_dial = peer.state().dial.clone().expect("first dial is reserved");
            let (outgoing, _out_send, _out_recv) = hold_outgoing_hello(&remote).await;
            let (incoming, _in_send, _in_recv) = hold_incoming_hello(&remote, local_addr).await;

            outgoing.close(CLOSE_LINK.into(), b"");
            wait_for_failed_dial(&peer, &first_dial).await;
            assert!(peer.state().incoming.is_some());
            assert!(poll_once(send.as_mut()).await.is_pending());
            assert!(
                peer.state().dial.is_none(),
                "wait for the incoming hello before dialing again"
            );

            incoming.close(CLOSE_LINK.into(), b"");
            wait_for_change(&peer, || peer.state().incoming.is_none()).await;
            assert!(
                poll_once(send.as_mut()).await.is_pending(),
                "the send can retry after the incoming hello fails"
            );
            assert!(
                peer.state().dial.is_some(),
                "reserve one fresh outgoing dial"
            );
            let retry = accept_outgoing(&remote).await;
            let (result, ()) = tokio::join!(send, acknowledge_retry(&retry, &chat));
            assert_eq!(result, Ok(ChatReply::Ack));
            assert!(peer.state().dial.is_none());
            checked(link.shutdown().await, "stop link after deferred retry");
            remote.iroh().close().await;
        })
        .await,
        "retry after pending incoming hello test timed out",
    );
}

#[tokio::test]
async fn joining_a_pending_retry_counts_the_failure_even_with_an_incoming_hello() {
    checked(
        timeout(CASE_BUDGET, async {
            let (link, remote, local_addr, peer) = linked_endpoints().await;
            let handle = link.handle();
            let mut first = pin!(handle.send(peer.id, chat("first-waiter")));
            let mut second = pin!(handle.send(peer.id, chat("second-waiter")));
            assert!(poll_once(first.as_mut()).await.is_pending());
            let first_dial = peer.state().dial.clone().expect("first dial is reserved");
            assert!(poll_once(second.as_mut()).await.is_pending());
            assert!(
                peer.state()
                    .dial
                    .as_ref()
                    .is_some_and(|dial| Arc::ptr_eq(dial, &first_dial))
            );
            let (outgoing, _first_send, _first_recv) = hold_outgoing_hello(&remote).await;
            outgoing.close(CLOSE_LINK.into(), b"");
            wait_for_failed_dial(&peer, &first_dial).await;

            // Only the second caller sees the failure before the new incoming hello.
            assert!(poll_once(second.as_mut()).await.is_pending());
            let retry = peer
                .state()
                .dial
                .clone()
                .expect("second caller reserved a retry");
            assert!(!Arc::ptr_eq(&first_dial, &retry));
            let (outgoing, _retry_send, _retry_recv) = hold_outgoing_hello(&remote).await;
            let (incoming, _in_send, _in_recv) = hold_incoming_hello(&remote, local_addr).await;
            assert!(peer.state().incoming.is_some());
            assert!(poll_once(first.as_mut()).await.is_pending());
            assert!(
                peer.state()
                    .dial
                    .as_ref()
                    .is_some_and(|dial| Arc::ptr_eq(dial, &retry))
            );

            outgoing.close(CLOSE_LINK.into(), b"");
            wait_for_failed_dial(&peer, &retry).await;
            assert!(peer.state().incoming.is_some());
            assert!(poll_once(first.as_mut()).await.is_pending());
            assert!(poll_once(second.as_mut()).await.is_pending());
            assert!(peer.state().dial.is_none());

            incoming.close(CLOSE_LINK.into(), b"");
            wait_for_change(&peer, || peer.state().incoming.is_none()).await;
            let first = poll_once(first.as_mut()).await;
            let second = poll_once(second.as_mut()).await;
            assert!(
                peer.state().dial.is_none(),
                "a send got a third dial after two failed dials"
            );
            assert_eq!(first, Poll::Ready(Err(SendError::Disconnected)));
            assert_eq!(second, Poll::Ready(Err(SendError::Disconnected)));
            checked(
                link.shutdown().await,
                "stop link after consecutive failed dials",
            );
            remote.iroh().close().await;
        })
        .await,
        "shared dial retry with pending incoming hello test timed out",
    );
}
