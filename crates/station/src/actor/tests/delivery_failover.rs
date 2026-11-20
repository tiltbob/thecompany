use std::future::Future;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::pin::{Pin, pin};
use std::time::Duration;

use chief::api::MessageStatus;
use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{Connection, ConnectionError, RecvStream, SendStream};
use numbers::link::{IncomingChat, LINK_ALPN, Link, LinkConfig, LinkPath};
use numbers::proto::chat::{ChatMessage, ChatReply, ReceiptWindow};
use numbers::proto::{Hello, HelloEcho, StreamHeader};
use numbers::{Endpoint, NodeId, generate_node_key, node_id};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep, sleep_until, timeout};

use super::super::deliver;
use super::delivery_deadline::{bind, checked, dial_admitted, read, write};

// Scale the default 5 s / 15 s station budgets down by a factor of five.
const CONNECT: Duration = Duration::from_secs(1);
const BUDGET: Duration = Duration::from_secs(3);
const CASE_BUDGET: Duration = Duration::from_secs(20);
const CONFIG: LinkConfig = LinkConfig {
    connect_timeout: CONNECT,
    send_deadline: BUDGET,
};

async fn within(task: impl Future<Output = ()>, operation: &str) {
    checked(timeout(CASE_BUDGET, task).await, operation);
}

async fn while_delivering<T>(
    delivery: Pin<&mut impl Future<Output = MessageStatus>>,
    step: impl Future<Output = T>,
) -> T {
    tokio::select! {
        biased;
        output = step => output,
        status = delivery => panic!("delivery finished before the peer could reply: {status:?}"),
    }
}

fn chat(id: &str) -> ChatMessage {
    ChatMessage {
        id: id.into(),
        sent_at_ms: 123,
        body: "test station deliver".into(),
    }
}

fn address(endpoint: &Endpoint) -> EndpointAddr {
    EndpointAddr::new(endpoint.iroh().id()).with_ip_addr(endpoint.iroh().bound_sockets()[0])
}

fn lookup(endpoint: &Endpoint, addresses: impl IntoIterator<Item = EndpointAddr>) -> MemoryLookup {
    let lookup = MemoryLookup::from_endpoint_info(addresses);
    checked(endpoint.iroh().address_lookup(), "get sender lookup").add(lookup.clone());
    lookup
}

async fn start_sender(key: &[u8; 32], addresses: &[EndpointAddr]) -> Link {
    let endpoint = bind(key, addresses.iter().map(|address| address.id)).await;
    lookup(&endpoint, addresses.iter().cloned());
    Link::start(endpoint, CONFIG).0
}

async fn peer(sender: NodeId) -> Endpoint {
    bind(&generate_node_key(), [sender]).await
}

async fn receive(inbox: &mut mpsc::Receiver<IncomingChat>, message: &ChatMessage) -> IncomingChat {
    let incoming = inbox.recv().await.expect("peer receives the chat");
    assert!(
        incoming.message == *message,
        "peer receives the original message"
    );
    incoming
}

async fn ack(inbox: &mut mpsc::Receiver<IncomingChat>, message: &ChatMessage) {
    receive(inbox, message).await.respond(ChatReply::Ack);
}

fn assert_dial_budgets(elapsed: Duration, count: u32) {
    let expected = CONNECT * count;
    assert!(
        elapsed >= expected && elapsed < expected + CONNECT * 3 / 4,
        "failover took {elapsed:?}; expected about {expected:?}"
    );
}

async fn silent_then_live(silent_count: u32) {
    let key = generate_node_key();
    let live = peer(node_id(&key)).await;
    let silent: Vec<_> = (0..silent_count)
        .map(|_| {
            let socket = checked(
                UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)),
                "bind silent port",
            );
            checked(socket.set_nonblocking(true), "make silent port nonblocking");
            let port = checked(socket.local_addr(), "read silent address");
            (
                EndpointAddr::new(node_id(&generate_node_key())).with_ip_addr(port),
                socket,
            )
        })
        .collect();
    let addresses: Vec<_> = silent
        .iter()
        .map(|(address, _)| address.clone())
        .chain([address(&live)])
        .collect();
    let local = start_sender(&key, &addresses).await;
    let candidates = addresses.iter().map(|address| address.id).collect();
    let (live, mut inbox) = Link::start(live, CONFIG);
    let message = chat("silent-failover");
    let started = Instant::now();
    let mut delivery = pin!(deliver(
        local.handle(),
        candidates,
        message.clone(),
        started + BUDGET
    ));
    let reached = while_delivering(delivery.as_mut(), async {
        let incoming = receive(&mut inbox, &message).await;
        let reached = started.elapsed();
        incoming.respond(ChatReply::Ack);
        reached
    })
    .await;
    let status = delivery.await;
    assert_eq!(status, MessageStatus::Delivered);
    assert_dial_budgets(reached, silent_count);
    for (_, socket) in silent {
        checked(
            socket.recv(&mut [0; 1]),
            "each silent station receives a dial",
        );
    }
    let (local, live) = tokio::join!(local.shutdown(), live.shutdown());
    checked(local, "shut down sender");
    checked(live, "shut down answering station");
}

#[tokio::test]
async fn a_live_second_station_gets_the_chat_after_one_dial_budget() {
    within(silent_then_live(1), "one silent station failover timed out").await;
}

#[tokio::test]
async fn a_live_third_station_gets_the_chat_after_two_dial_budgets() {
    within(
        silent_then_live(2),
        "two silent stations failover timed out",
    )
    .await;
}

#[tokio::test]
async fn a_healthy_slow_station_delivers_without_copying_to_later_candidates() {
    within(
        async {
            let key = generate_node_key();
            let local_id = node_id(&key);
            let (first, second, third) =
                tokio::join!(peer(local_id), peer(local_id), peer(local_id));
            let addresses = [address(&first), address(&second), address(&third)];
            let [first_id, second_id, third_id] = addresses.clone().map(|address| address.id);
            let local = start_sender(&key, &addresses).await;
            let (first, mut first_inbox) = Link::start(first, CONFIG);
            let (second, mut second_inbox) = Link::start(second, CONFIG);
            let (third, mut third_inbox) = Link::start(third, CONFIG);
            let warmup = chat("warmup");
            let handle = local.handle();
            let (reply, ()) = tokio::join!(
                handle.send(first_id, warmup.clone()),
                ack(&mut first_inbox, &warmup)
            );
            assert_eq!(reply, Ok(ChatReply::Ack));
            let message = chat("slow-reply");
            let started = Instant::now();
            let slow_reply = started + CONNECT * 6 / 5;
            let (status, ()) = tokio::select! {
                biased;
                _ = second_inbox.recv() => panic!("the slow chat was copied to the second station"),
                _ = third_inbox.recv() => panic!("the slow chat was copied to the third station"),
                result = async {
                    tokio::join!(
                        deliver(
                            local.handle(),
                            vec![first_id, second_id, third_id],
                            message.clone(),
                            started + BUDGET,
                        ),
                        async {
                            let incoming = receive(&mut first_inbox, &message).await;
                            sleep_until(slow_reply).await;
                            incoming.respond(ChatReply::Ack);
                        },
                    )
                } => result,
            };
            assert_eq!(status, MessageStatus::Delivered);
            assert!(Instant::now() >= slow_reply && started.elapsed() < BUDGET);
            let (local, first, second, third) = tokio::join!(
                local.shutdown(),
                first.shutdown(),
                second.shutdown(),
                third.shutdown()
            );
            for result in [local, first, second, third] {
                checked(result, "shut down slow-reply fixture");
            }
        },
        "slow-reply test timed out",
    )
    .await;
}

async fn accept_hello(peer: &Endpoint) -> (Connection, SendStream, RecvStream, Hello) {
    let connection = checked(
        peer.iroh()
            .accept()
            .await
            .expect("raw station accepts the dial")
            .await,
        "accept raw connection",
    );
    let (send, mut recv) = checked(connection.accept_bi().await, "accept hello stream");
    let hello: Hello = read(&mut recv).await;
    assert!(hello.validate().is_ok() && hello.echo.is_none());
    (connection, send, recv, hello)
}

async fn finish_hello(mut send: SendStream, mut recv: RecvStream, hello: Hello) {
    let mut reply = Hello::local([1; 16], [2; 16]);
    reply.echo = Some(hello.challenge);
    write(&mut send, &reply).await;
    let echo: HelloEcho = read(&mut recv).await;
    assert!(
        echo.echo == reply.challenge,
        "dialer confirms the raw hello"
    );
    checked(send.finish(), "finish hello reply");
}

async fn accept_chat(connection: &Connection, message: &ChatMessage) -> SendStream {
    let (send, mut recv) = checked(connection.accept_bi().await, "accept chat stream");
    assert!(read::<StreamHeader>(&mut recv).await == StreamHeader::chat());
    let wire: serde_json::Value = read(&mut recv).await;
    assert!(wire == checked(serde_json::to_value(message), "encode expected chat"));
    assert!(checked(recv.read_to_end(0).await, "read end of the chat").is_empty());
    send
}

async fn reply_ack(send: &mut SendStream) {
    write(send, &ChatReply::Ack).await;
    checked(send.finish(), "finish chat reply");
}

#[tokio::test]
async fn a_lost_reply_can_be_recovered_on_the_same_station_after_one_dial_budget() {
    within(async {
        let key = generate_node_key();
        let local_id = node_id(&key);
        let (first, second) = tokio::join!(peer(local_id), peer(local_id));
        first.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
        let addresses = [address(&first), address(&second)];
        let [first_id, second_id] = addresses.clone().map(|address| address.id);
        let local = start_sender(&key, &addresses).await;
        let (second, mut second_inbox) = Link::start(second, CONFIG);
        let message = chat("lost-reply");
        let started = Instant::now();
        let reconnected = started + CONNECT * 7 / 5;
        let (status, _connection) = tokio::select! {
            biased;
            _ = second_inbox.recv() => panic!("the redialing chat was copied to the second station"),
            result = async {
                tokio::join!(
                    deliver(local.handle(), vec![first_id, second_id], message.clone(), started + BUDGET),
                    async {
                        let mut receipts = ReceiptWindow::new(16);
                        let (old, send, recv, hello) = accept_hello(&first).await;
                        finish_hello(send, recv, hello).await;
                        let _lost_reply = accept_chat(&old, &message).await;
                        // Model the receiver committing before its ACK is lost. Its receipt
                        // remains while the old connection closes and a new one is admitted.
                        assert!(receipts.record("sender", &message.id));
                        sleep_until(started + CONNECT * 3 / 5).await;
                        old.close(0u32.into(), b"");
                        let (new, send, recv, hello) = accept_hello(&first).await;
                        sleep_until(reconnected).await;
                        finish_hello(send, recv, hello).await;
                        let mut reply = accept_chat(&new, &message).await;
                        assert!(
                            !receipts.record("sender", &message.id),
                            "the same station deduplicates the retry"
                        );
                        reply_ack(&mut reply).await;
                        // The raw peer must keep the transport alive until delivery reads the ACK.
                        new
                    },
                )
            } => result,
        };
        assert_eq!(status, MessageStatus::Delivered);
        assert!(Instant::now() >= reconnected && started.elapsed() < BUDGET);
        let (local, second, ()) = tokio::join!(local.shutdown(), second.shutdown(), first.iroh().close());
        checked(local, "shut down sender after reconnect");
        checked(second, "shut down unused candidate");
    }, "same-station reconnect test timed out").await;
}

async fn bind_silent_port(socket: SocketAddr) -> tokio::net::UdpSocket {
    checked(
        timeout(CONNECT, async {
            loop {
                match tokio::net::UdpSocket::bind(socket).await {
                    Ok(socket) => return socket,
                    Err(error) if error.kind() == ErrorKind::AddrInUse => {
                        sleep(Duration::from_millis(10)).await
                    }
                    Err(_) => panic!("bind the frozen station's old port"),
                }
            }
        })
        .await,
        "the frozen station did not release its UDP port",
    )
}

#[tokio::test]
async fn a_frozen_established_link_costs_one_stall_check_and_one_redial() {
    within(async {
        let key = generate_node_key();
        let local_id = node_id(&key);
        let frozen_key = generate_node_key();
        let frozen_id = node_id(&frozen_key);
        let live = peer(local_id).await;
        let live_addr = address(&live);
        let endpoint = bind(&key, [frozen_id, live_addr.id]).await;
        let local_addr = address(&endpoint);
        let lookup = lookup(&endpoint, [live_addr.clone()]);
        let (local, _inbox) = Link::start(endpoint, CONFIG);
        let (live, mut inbox) = Link::start(live, CONFIG);
        let handle = local.handle();
        let frozen_handle = handle.clone();
        let (ready, ready_rx) = oneshot::channel();
        let (warmed, warmed_rx) = oneshot::channel();
        let (freeze, freeze_rx) = oneshot::channel();
        let lifecycle = tokio::task::spawn_blocking(move || {
            let runtime = checked(
                tokio::runtime::Builder::new_current_thread().enable_all().build(),
                "start a test peer runtime",
            );
            let retained = runtime.block_on(async move {
                let (frozen, connection) = checked(timeout(CASE_BUDGET, async {
                    let frozen = bind(&frozen_key, [local_id]).await;
                    let connection = dial_admitted(&frozen, local_addr, &frozen_handle).await;
                    checked(ready.send(frozen.iroh().bound_sockets()[0]), "publish the frozen station's port");
                    let mut reply = accept_chat(&connection, &chat("before-freeze")).await;
                    reply_ack(&mut reply).await;
                    // Wait for the reply's transport to finish before halting the runtime.
                    // The sender checks the ACK separately and can stop reading before FIN.
                    let _ = checked(reply.stopped().await, "observe the warmup reply's transport");
                    checked(warmed.send(()), "confirm the pre-freeze connection is quiet");
                    (frozen, connection)
                }).await, "warm up the pre-freeze link");
                let _ = freeze_rx.await;
                (frozen, connection)
            });
            // Stop the network tasks before dropping the endpoint so no close is sent.
            runtime.shutdown_background();
            drop(retained);
        });
        let old_port = checked(ready_rx.await, "read the frozen station's port");
        lookup.set_endpoint_info(EndpointAddr::new(frozen_id).with_ip_addr(old_port));
        assert_eq!(handle.send(frozen_id, chat("before-freeze")).await, Ok(ChatReply::Ack));
        checked(warmed_rx.await, "wait for the warmup transport confirmation");
        checked(freeze.send(()), "stop the frozen station without a close");
        checked(lifecycle.await, "join the frozen station runtime");
        let silent = bind_silent_port(old_port).await;
        assert!(handle.path(frozen_id) == LinkPath::Direct, "the sender still has the established link");

        let message = chat("frozen-failover");
        let started = Instant::now();
        let mut delivery = pin!(deliver(
            handle, vec![frozen_id, live_addr.id], message.clone(), started + BUDGET
        ));
        let (reached, stalled) = while_delivering(delivery.as_mut(), async {
            tokio::join!(
                async {
                    let incoming = receive(&mut inbox, &message).await;
                    let reached = started.elapsed();
                    incoming.respond(ChatReply::Ack);
                    reached
                },
                async {
                    let mut packet = [0; 1500];
                    loop {
                        let len = checked(silent.recv(&mut packet).await, "observe the frozen port");
                        // A QUIC v1 Initial proves an actual redial, independently of packets
                        // still being sent on the old link. The fixed bit may be greased.
                        if matches!(&packet[..len], [header, 0, 0, 0, 1, ..] if header & 0xb0 == 0x80) {
                            break started.elapsed();
                        }
                    }
                },
            )
        }).await;
        let status = delivery.await;
        assert_eq!(status, MessageStatus::Delivered);
        assert_dial_budgets(stalled, 1);
        assert_dial_budgets(reached, 2);
        let (local, live) = tokio::join!(local.shutdown(), live.shutdown());
        checked(local, "shut down after frozen-link failover");
        checked(live, "shut down reachable candidate");
    }, "frozen-link failover test timed out").await;
}

#[tokio::test]
async fn a_lone_station_can_retry_an_actual_failed_hello() {
    within(
        async {
            let key = generate_node_key();
            let remote = peer(node_id(&key)).await;
            remote.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
            let local = start_sender(&key, &[address(&remote)]).await;
            let message = chat("lone-station-retry");
            let started = Instant::now();
            let mut delivery = pin!(deliver(
                local.handle(),
                vec![remote.iroh().id()],
                message.clone(),
                started + BUDGET
            ));
            let _connection = while_delivering(delivery.as_mut(), async {
                let (stalled, _send, _recv, _hello) = accept_hello(&remote).await;
                assert!(matches!(
                    stalled.closed().await,
                    ConnectionError::ApplicationClosed(close) if close.error_code.into_inner() == 0
                ));
                assert_dial_budgets(started.elapsed(), 1);
                let (retried, send, recv, hello) = accept_hello(&remote).await;
                finish_hello(send, recv, hello).await;
                let mut reply = accept_chat(&retried, &message).await;
                reply_ack(&mut reply).await;
                retried
            })
            .await;
            let status = delivery.await;
            assert_eq!(status, MessageStatus::Delivered);
            assert!(started.elapsed() < BUDGET);
            let (local, ()) = tokio::join!(local.shutdown(), remote.iroh().close());
            checked(local, "shut down after a lone-station retry");
        },
        "lone-station retry test timed out",
    )
    .await;
}
