use std::pin::pin;
use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use numbers::Endpoint;
use numbers::link::{LINK_ALPN, Link, LinkConfig, LinkPath, SendError};
use numbers::node_id;
use numbers::proto::chat::{ChatMessage, ChatReply};
use numbers::proto::{CHAT_PROTOCOL, StreamHeader};
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep, timeout};

use super::{
    CASE_TIMEOUT, CONNECT_TIMEOUT, PROBE_TIMEOUT, RAW_GENERATION, accept_chat, accept_hello,
    acknowledge, add_address, allow_peers, assert_delivery, bind_direct, bind_direct_with_key,
    checked, config, expect_close, loopback_address, loopback_socket, lose_outgoing_dial, message,
    ordered_keys, read, rebind_after_shutdown, reply_to_chat, wait_for_path, while_sending, write,
};

async fn raw_server(config: LinkConfig) -> (Link, Endpoint) {
    let (endpoint, auth) = bind_direct().await;
    let (peer, peer_auth) = bind_direct().await;
    allow_peers((&endpoint, &auth), (&peer, &peer_auth));
    add_address(&endpoint, loopback_address(&peer));
    peer.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
    (Link::start(endpoint, config).0, peer)
}

#[tokio::test]
async fn timed_out_shared_dial_retries_for_both_callers() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (link, peer) = raw_server(config()).await;
            let peer_id = peer.iroh().id();
            let handle = link.handle();
            let first_chat = message("first-waiter", "the caller that starts the shared dial");
            let second_chat = message("second-waiter", "the caller that joins the shared dial");
            let mut first = pin!(handle.send(peer_id, first_chat.clone()));
            let (stalled, _hello) = while_sending(first.as_mut(), accept_hello(&peer)).await;
            while_sending(first.as_mut(), sleep(CONNECT_TIMEOUT / 3)).await;

            let callers = async {
                tokio::join!(first, handle.send(peer_id, second_chat.clone()))
            };
            let mut callers = pin!(callers);
            let replies = tokio::select! {
                replies = callers.as_mut() => replies,
                _fresh = async {
                    expect_close(&stalled, 0).await;
                    let (fresh, mut hello) = accept_hello(&peer).await;
                    hello.reply(RAW_GENERATION, [3; 16]).await;
                    hello.confirm([3; 16]).await;

                    // Both callers must reach the same fresh connection, in either order.
                    let expected = [
                        checked(serde_json::to_value(&first_chat), "encode the first chat"),
                        checked(serde_json::to_value(&second_chat), "encode the second chat"),
                    ];
                    let mut seen = [false; 2];
                    for _ in 0..2 {
                        let (mut send, mut recv) = checked(fresh.accept_bi().await, "accept retried chat");
                        let header: StreamHeader = read(&mut recv).await;
                        assert_eq!(header.proto, CHAT_PROTOCOL);
                        let chat: serde_json::Value = read(&mut recv).await;
                        let index = expected.iter().position(|expected| *expected == chat)
                            .expect("receive a waiting caller's message");
                        assert!(!seen[index]);
                        seen[index] = true;
                        write(&mut send, &ChatReply::Ack).await;
                        checked(send.finish(), "finish retried chat reply");
                    }
                    fresh
                } => callers.await,
            };
            assert_eq!(replies, (Ok(ChatReply::Ack), Ok(ChatReply::Ack)));
            checked(link.shutdown().await, "stop link after shared dial retry");
            peer.iroh().close().await;
        })
        .await,
        "shared dial timeout and recovery test timed out",
    );
}

async fn fail_shared_dials(codes: &[u32], expected: SendError) {
    let (link, peer) = raw_server(config()).await;
    let peer_id = peer.iroh().id();
    let handle = link.handle();
    let callers = async {
        tokio::join!(
            handle.send(peer_id, message("first", "start a shared dial")),
            handle.send(peer_id, message("second", "join the shared dial")),
        )
    };
    let mut accepted = 0;
    let replies = tokio::select! {
        replies = callers => replies,
        () = async {
            for &code in codes {
                let (conn, _hello) = accept_hello(&peer).await;
                accepted += 1;
                conn.close(code.into(), b"");
            }
            let _ = peer.iroh().accept().await;
        } => panic!("the callers made an extra outgoing dial"),
    };
    assert_eq!(accepted, codes.len());
    assert_eq!(replies, (Err(expected), Err(expected)));
    checked(link.shutdown().await, "stop link after rejected dials");
    peer.iroh().close().await;
}

#[tokio::test]
async fn two_shared_disconnections_exhaust_each_callers_retry() {
    checked(
        timeout(
            CASE_TIMEOUT,
            fail_shared_dials(&[0, 0], SendError::Disconnected),
        )
        .await,
        "bounded shared dial recovery test timed out",
    );
}

#[tokio::test]
async fn two_shared_timeouts_exhaust_each_callers_retry() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let budget = LinkConfig {
                send_deadline: CONNECT_TIMEOUT * 2 + CONNECT_TIMEOUT * 3 / 4,
                ..config()
            };
            let (link, peer) = raw_server(budget).await;
            let peer_id = peer.iroh().id();
            let handle = link.handle();
            let callers = async {
                tokio::join!(
                    handle.send(peer_id, message("first", "start a shared dial")),
                    handle.send(peer_id, message("second", "join the shared dial")),
                )
            };
            let mut accepted = 0;
            let deadline = Instant::now() + budget.send_deadline;
            let replies = tokio::select! {
                biased;
                () = async {
                    // Hold both hello streams open without answering so each dial times out.
                    let _first = accept_hello(&peer).await;
                    accepted += 1;
                    let _second = accept_hello(&peer).await;
                    accepted += 1;
                    let _ = peer.iroh().accept().await;
                } => panic!("the callers made an extra outgoing dial after two timeouts"),
                replies = callers => replies,
            };
            assert_eq!(accepted, 2);
            assert_eq!(replies, (Err(SendError::Timeout), Err(SendError::Timeout)));
            assert!(
                Instant::now() < deadline,
                "the second dial ended both sends"
            );
            checked(link.shutdown().await, "stop link after timed-out dials");
            peer.iroh().close().await;
        })
        .await,
        "bounded shared dial timeout recovery test timed out",
    );
}

#[tokio::test]
async fn shared_authorization_refusal_ends_without_a_retry() {
    checked(
        timeout(
            CASE_TIMEOUT,
            fail_shared_dials(&[403], SendError::NotAllowed),
        )
        .await,
        "authorization refusal test timed out",
    );
}

#[tokio::test]
async fn failed_fallback_after_a_lost_dial_still_gets_a_fresh_retry() {
    checked(
        timeout(CASE_TIMEOUT, async {
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
            let chat = message(
                "lost-then-failed",
                "retry after a lost dial and failed fallback",
            );
            let mut send = pin!(handle.send(peer_id, chat.clone()));

            let (first, _hello) = while_sending(send.as_mut(), accept_hello(&peer)).await;
            while_sending(send.as_mut(), lose_outgoing_dial(&peer, &address, &first)).await;
            // The pending winner failed its hello. Fail the fallback with an ordinary close
            // so the sender needs its separate shared dial retry.
            let (fallback, _hello) = while_sending(send.as_mut(), accept_hello(&peer)).await;
            fallback.close(0u32.into(), b"");

            let (retry, mut hello) = while_sending(send.as_mut(), accept_hello(&peer)).await;
            let (result, ()) = tokio::join!(send, async {
                hello.reply(RAW_GENERATION, [3; 16]).await;
                hello.confirm([3; 16]).await;
                reply_to_chat(&retry, &chat).await;
            });
            assert_eq!(result, Ok(ChatReply::Ack));
            checked(
                link.shutdown().await,
                "stop link after failed fallback recovery",
            );
            peer.iroh().close().await;
        })
        .await,
        "failed fallback dial recovery test timed out",
    );
}

#[tokio::test]
async fn chat_recovery_keeps_the_same_dial_retry_allowance() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (link, peer) = raw_server(config()).await;
            let peer_id = peer.iroh().id();
            let handle = link.handle();
            let chat = message(
                "chat-retry",
                "lose the first connection after sending a chat",
            );
            let mut send = pin!(handle.send(peer_id, chat.clone()));

            let (initial, _hello) = while_sending(send.as_mut(), accept_hello(&peer)).await;
            initial.close(0u32.into(), b"");
            let (connected, _reply) = while_sending(send.as_mut(), async {
                let (conn, mut hello) = accept_hello(&peer).await;
                hello.reply(RAW_GENERATION, [1; 16]).await;
                hello.confirm([1; 16]).await;
                let (reply, _) = accept_chat(&conn, &chat).await;
                (conn, reply)
            })
            .await;
            connected.close(0u32.into(), b"");
            let (recovery, _hello) = while_sending(send.as_mut(), accept_hello(&peer)).await;
            recovery.close(0u32.into(), b"");
            tokio::select! {
                result = send => assert_eq!(result, Err(SendError::Disconnected)),
                _ = peer.iroh().accept() => panic!("the chat retry got another dial retry"),
            }
            checked(link.shutdown().await, "shut down link after chat recovery");
            peer.iroh().close().await;
        })
        .await,
        "shared dial recovery across chat attempts timed out",
    );
}

#[tokio::test]
async fn stale_connection_then_timed_out_dial_still_gets_a_dial_retry() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let budget = config();
            let (link, peer) = raw_server(budget).await;
            let handle = link.handle();
            let chat = message(
                "stale-then-dial",
                "lose the connection, then time out a redial",
            );
            let started = Instant::now();
            let mut send = pin!(handle.send(peer.iroh().id(), chat.clone()));

            let (initial, _reply) = while_sending(send.as_mut(), async {
                let (connection, mut hello) = accept_hello(&peer).await;
                hello.reply(RAW_GENERATION, [1; 16]).await;
                hello.confirm([1; 16]).await;
                let (reply, _) = accept_chat(&connection, &chat).await;
                (connection, reply)
            })
            .await;
            initial.close(0u32.into(), b"");

            // Keep the recovery hello open without answering until the dial times out.
            let (stalled, _hello) = while_sending(send.as_mut(), accept_hello(&peer)).await;
            while_sending(send.as_mut(), expect_close(&stalled, 0)).await;
            let (fresh, mut hello) = while_sending(send.as_mut(), accept_hello(&peer)).await;
            let (result, ()) = tokio::join!(send, async {
                hello.reply(RAW_GENERATION, [2; 16]).await;
                hello.confirm([2; 16]).await;
                reply_to_chat(&fresh, &chat).await;
            });
            assert_eq!(result, Ok(ChatReply::Ack));
            assert!(started.elapsed() < budget.send_deadline);
            checked(
                link.shutdown().await,
                "shut down link after stale dial recovery",
            );
            peer.iroh().close().await;
        })
        .await,
        "stale connection then timed-out dial test timed out",
    );
}

fn is_quic_v1_initial(packet: &[u8]) -> bool {
    // Match the long-header and Initial type bits. A resumed peer can grease the fixed bit.
    matches!(packet, [header, 0, 0, 0, 1, ..] if header & 0xb0 == 0x80)
}

#[test]
fn the_stale_port_observer_matches_either_initial_fixed_bit() {
    assert!(is_quic_v1_initial(&[0xc0, 0, 0, 0, 1]));
    assert!(is_quic_v1_initial(&[0x80, 0, 0, 0, 1]));
    assert!(!is_quic_v1_initial(&[0x40, 0, 0, 0, 1]));
    assert!(!is_quic_v1_initial(&[0xd0, 0, 0, 0, 1]));
    assert!(!is_quic_v1_initial(&[0xc0, 0, 0, 0, 0]));
    assert!(!is_quic_v1_initial(&[0xc0, 0, 0, 0]));
}

async fn restart_with_staggered_sends() {
    let (survivor_key, peer_key) = ordered_keys();
    let survivor_id = node_id(&survivor_key);
    let peer_id = node_id(&peer_key);
    let (endpoint, auth) = bind_direct_with_key(&survivor_key).await;
    auth.replace_peers([(peer_id, "peer".into())]);
    let lookup = MemoryLookup::new();
    checked(endpoint.iroh().address_lookup(), "get survivor lookup").add(lookup.clone());

    let (ready, old_address) = oneshot::channel();
    let (crash, stop) = oneshot::channel::<()>();
    let warmup = message("warmup", "establish the original direct link");
    let original_warmup = warmup.clone();
    let old_runtime = tokio::task::spawn_blocking(move || {
        let runtime = checked(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build(),
            "start original peer runtime",
        );
        let link = runtime.block_on(async move {
            let (endpoint, auth) = bind_direct_with_key(&peer_key).await;
            auth.replace_peers([(survivor_id, "survivor".into())]);
            let address = loopback_address(&endpoint);
            let socket = loopback_socket(&endpoint);
            let (link, mut inbox) = Link::start(endpoint, config());
            checked(ready.send((address, socket)), "share original peer address");
            acknowledge(&mut inbox, survivor_id, "survivor", &original_warmup).await;
            let _ = stop.await;
            link
        });
        runtime.shutdown_background();
        drop(link);
    });

    let (old_address, old_socket) = checked(old_address.await, "get original address");
    lookup.set_endpoint_info(old_address);
    let (survivor, _inbox) = Link::start(endpoint, config());
    let handle = survivor.handle();
    assert_eq!(handle.send(peer_id, warmup).await, Ok(ChatReply::Ack));
    wait_for_path(&handle, peer_id, LinkPath::Direct).await;
    // Let the warmup's transport acknowledgements arrive before starting the stall.
    sleep(Duration::from_millis(100)).await;
    checked(
        crash.send(()),
        "stop original peer without closing the link",
    );
    checked(old_runtime.await, "join crashed peer runtime");
    assert_eq!(handle.path(peer_id), LinkPath::Direct);

    let old_port = rebind_after_shutdown(old_socket).await;
    checked(old_port.set_nonblocking(true), "set stale port nonblocking");
    let old_port = checked(
        tokio::net::UdpSocket::from_std(old_port),
        "observe stale port",
    );
    let (endpoint, auth) = bind_direct_with_key(&peer_key).await;
    auth.replace_peers([(survivor_id, "survivor".into())]);
    assert!(
        loopback_socket(&endpoint) != old_socket,
        "restarted peer uses a new loopback port"
    );
    let new_address = loopback_address(&endpoint);
    let (restarted, mut inbox) = Link::start(endpoint, config());

    let first = message("first-after-restart", "start on the old path");
    let second = message("second-after-restart", "join with time for another dial");
    let third = message(
        "third-after-restart",
        "join with more time for another dial",
    );
    let chats = [&first, &second, &third];
    let send_after = |delay, chat: &ChatMessage| {
        let handle = handle.clone();
        let chat = chat.clone();
        async move {
            if delay != Duration::ZERO {
                sleep(delay).await;
            }
            handle.send(peer_id, chat).await
        }
    };
    let (first, second, third, ()) = tokio::select! {
        replies = async {
            tokio::join!(
                send_after(Duration::ZERO, &first),
                send_after(Duration::from_millis(700), &second),
                send_after(Duration::from_millis(1200), &third),
                async {
                    // Wait for the QUIC v1 Initial at the stale address. Old chat packets
                    // cannot trigger the update. The first redial has already resolved and
                    // will time out against the silent socket before the next lookup.
                    let mut packet = [0; 1_500];
                    loop {
                        let len = checked(old_port.recv(&mut packet).await, "read stale port");
                        if is_quic_v1_initial(&packet[..len]) {
                            break;
                        }
                    }
                    lookup.set_endpoint_info(new_address);
                },
            )
        } => replies,
        () = async {
            while let Some(chat) = inbox.recv().await {
                let expected = chats.iter().find(|expected| chat.message.id == expected.id)
                    .expect("receive a chat from a waiting survivor send");
                assert_delivery(&chat, survivor_id, "survivor", expected);
                chat.respond(ChatReply::Ack);
            }
        } => panic!("the restarted link inbox closed"),
    };
    assert_eq!(
        (first, second, third),
        (
            Err(SendError::Timeout),
            Ok(ChatReply::Ack),
            Ok(ChatReply::Ack)
        )
    );
    let (local, remote) = tokio::join!(survivor.shutdown(), restarted.shutdown());
    checked(local, "shut down surviving link");
    checked(remote, "shut down restarted link");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn later_sends_recover_after_a_direct_peer_restarts() {
    // A stale in-memory lookup forces the first redial to fail regardless of when iroh
    // processes the old path's close.
    checked(
        timeout(CASE_TIMEOUT, restart_with_staggered_sends()).await,
        "restart recovery with concurrent sends timed out",
    );
}
