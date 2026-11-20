use std::pin::pin;
use std::time::Duration;

use iroh::address_lookup::memory::MemoryLookup;
use numbers::Endpoint;
use numbers::link::{LINK_ALPN, Link, LinkPath, SendError};
use numbers::node_id;
use numbers::proto::chat::{ChatMessage, ChatReply};
use numbers::proto::{CHAT_PROTOCOL, StreamHeader};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

use super::{
    CASE_TIMEOUT, CONNECT_TIMEOUT, RAW_GENERATION, accept_chat, accept_hello, acknowledge,
    add_address, allow_peers, assert_delivery, bind_direct, bind_direct_with_key, checked, config,
    expect_close, loopback_address, loopback_socket, message, ordered_keys, read, wait_for_path,
    while_sending, write,
};

async fn raw_server() -> (Link, Endpoint) {
    let (endpoint, auth) = bind_direct().await;
    let (peer, peer_auth) = bind_direct().await;
    allow_peers((&endpoint, &auth), (&peer, &peer_auth));
    add_address(&endpoint, loopback_address(&peer));
    peer.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
    (Link::start(endpoint, config()).0, peer)
}

#[tokio::test]
async fn timed_out_shared_dial_retries_for_both_callers() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (link, peer) = raw_server().await;
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
    let (link, peer) = raw_server().await;
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
async fn chat_recovery_keeps_the_same_dial_retry_allowance() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (link, peer) = raw_server().await;
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

    let _old_port = std::net::UdpSocket::bind(old_socket).ok();
    let (endpoint, auth) = bind_direct_with_key(&peer_key).await;
    auth.replace_peers([(survivor_id, "survivor".into())]);
    assert_ne!(loopback_socket(&endpoint), old_socket);
    lookup.set_endpoint_info(loopback_address(&endpoint));
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
    let replies = tokio::select! {
        replies = async {
            tokio::join!(
                send_after(Duration::ZERO, &first),
                send_after(Duration::from_millis(700), &second),
                send_after(Duration::from_millis(1200), &third),
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
    assert!(matches!(
        replies.0,
        Ok(ChatReply::Ack) | Err(SendError::Timeout)
    ));
    assert_eq!(
        (replies.1, replies.2),
        (Ok(ChatReply::Ack), Ok(ChatReply::Ack))
    );
    let (local, remote) = tokio::join!(survivor.shutdown(), restarted.shutdown());
    checked(local, "shut down surviving link");
    checked(remote, "shut down restarted link");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn later_sends_recover_after_a_direct_peer_restarts() {
    checked(
        timeout(CASE_TIMEOUT, restart_with_staggered_sends()).await,
        "restart recovery with concurrent sends timed out",
    );
}
