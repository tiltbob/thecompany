use iroh::EndpointAddr;
use iroh::address_lookup::memory::MemoryLookup;

use super::tests::{connect, loopback_endpoint};
use super::*;

const CASE_TIMEOUT: Duration = Duration::from_secs(18);
const SHORT_SETUP: Duration = Duration::from_secs(1);
const LONG_SETUP: Duration = Duration::from_secs(4);
const SEND_DEADLINE: Duration = Duration::from_secs(6);

fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    result.unwrap_or_else(|_| panic!("{operation}"))
}

async fn ordered_endpoints() -> (Endpoint, Endpoint) {
    let first = loopback_endpoint().await;
    let second = loopback_endpoint().await;
    let (lower, higher) = if first.iroh().id() < second.iroh().id() {
        (first, second)
    } else {
        (second, first)
    };
    lower
        .authorizer()
        .replace_peers([(higher.iroh().id(), "higher".into())]);
    higher
        .authorizer()
        .replace_peers([(lower.iroh().id(), "lower".into())]);
    (lower, higher)
}

fn config(connect_timeout: Duration) -> LinkConfig {
    LinkConfig {
        connect_timeout,
        send_deadline: SEND_DEADLINE,
    }
}

fn shared_peer(endpoint: &Endpoint, id: NodeId, budget: Duration) -> (Arc<Shared>, Arc<Peer>) {
    let (shared, _) = Shared::new(endpoint, config(budget));
    let peer = checked(shared.peer(id), "get the authorized peer");
    (shared, peer)
}

fn install(shared: &Shared, peer: &Peer, connection: &Connection, generation: [u8; 16]) {
    let mut state = peer.state();
    let observed_generation = state.remote_generation;
    assert!(
        shared.admit(
            peer,
            &mut state,
            connection,
            observed_generation,
            generation,
            None
        ) == DialResult::Ready
    );
}

fn reserve(shared: &Shared, peer: &Peer, connection: &Connection) -> IncomingHello {
    peer.state()
        .reserve_incoming(connection, shared.local_id, peer.id)
        .expect("reserve the incoming hello")
}

fn set_simulated_age(peer: &Peer, age: Duration) {
    let mut state = peer.state();
    let active = state
        .active
        .as_mut()
        .expect("find the controlled active link");
    let lease = Arc::get_mut(&mut active.connection).expect("only the fixture holds the lease");
    lease.admitted_at = Instant::now() - age;
}

#[tokio::test]
async fn asymmetric_crossed_hellos_keep_the_lower_link_while_it_is_pending_or_active() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (lower, higher) = ordered_endpoints().await;
            for keep_incoming_pending in [true, false] {
                let (a, peer_at_a) = shared_peer(&lower, higher.iroh().id(), SHORT_SETUP);
                let (b, peer_at_b) = shared_peer(&higher, lower.iroh().id(), LONG_SETUP);

                // Reserve higher-initiated D early, but defer its application hello until after C.
                let (d_at_b, d_at_a) = connect(&higher, &lower).await;
                let d_hello = b.hello();
                let observed_at_b = {
                    let mut state = peer_at_b.state();
                    state.outgoing = Some(d_at_b.clone());
                    assert!(state.active.is_none());
                    state.remote_generation
                };
                let (c_at_a, c_at_b) = connect(&lower, &higher).await;
                let incoming_c = reserve(&b, &peer_at_b, &c_at_b);
                install(&a, &peer_at_a, &c_at_a, b.generation);
                let age = Duration::from_secs(3);
                set_simulated_age(&peer_at_a, age);
                if !keep_incoming_pending {
                    {
                        let mut state = peer_at_b.state();
                        state.incoming.take();
                        assert!(
                            b.admit(
                                &peer_at_b,
                                &mut state,
                                &c_at_b,
                                incoming_c.observed_generation,
                                a.generation,
                                None
                            ) == DialResult::Ready
                        );
                    }
                    set_simulated_age(&peer_at_b, age);
                }
                // C is old under A's former proposed age rule and young under B's. Moving its
                // timestamp models delayed acceptance without relying on scheduler or sleeps.
                assert!(age >= a.config.connect_timeout * 2 && age < b.config.connect_timeout * 2);

                let incoming_d = reserve(&a, &peer_at_a, &d_at_a);
                assert!(incoming_d.lower_link.is_some());
                let (at_b, at_a) = tokio::join!(
                    exchange_hello(&d_at_b, d_hello, b.config.connect_timeout, || peer_at_b
                        .state()
                        .can_claim_no_open_lower_link(b.local_id, peer_at_b.id)),
                    exchange_hello(&d_at_a, a.hello(), a.config.connect_timeout, || panic!(
                        "only the hello client may make the claim"
                    )),
                );
                let at_b = checked(at_b, "finish D's client hello");
                let at_a = checked(at_a, "read D's authenticated frame 3");
                assert!(!at_a.no_open_lower_initiated_link);

                // Decide D at A after reading frame 3 and before B has made its local admission
                // decision or closed D. An age-based veto bypass can lose C in this order.
                assert!(d_at_a.close_reason().is_none() && d_at_b.close_reason().is_none());
                {
                    let mut state = peer_at_a.state();
                    state.incoming.take();
                    assert!(
                        a.admit(
                            &peer_at_a,
                            &mut state,
                            &d_at_a,
                            incoming_d.observed_generation,
                            at_a.generation,
                            None
                        ) == DialResult::Replaced
                    );
                    assert!(state.is_current(&c_at_a));
                }
                {
                    let mut state = peer_at_b.state();
                    if keep_incoming_pending {
                        state.incoming.take();
                        assert!(
                            b.admit(
                                &peer_at_b,
                                &mut state,
                                &c_at_b,
                                incoming_c.observed_generation,
                                a.generation,
                                None
                            ) == DialResult::Ready
                        );
                    }
                    state.outgoing.take();
                    assert!(
                        b.admit(
                            &peer_at_b,
                            &mut state,
                            &d_at_b,
                            observed_at_b,
                            at_b.generation,
                            None
                        ) == DialResult::Replaced
                    );
                    assert!(state.is_current(&c_at_b));
                }
                assert!(c_at_a.close_reason().is_none() && c_at_b.close_reason().is_none());
                a.stop();
                b.stop();
            }
            tokio::join!(lower.iroh().close(), higher.iroh().close());
        })
        .await,
        "crossed hellos resolve before the test deadline",
    );
}

#[tokio::test]
async fn an_authenticated_claim_cannot_replace_a_newer_lower_initiated_link() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (lower, higher) = ordered_endpoints().await;
            let (a, peer_at_a) = shared_peer(&lower, higher.iroh().id(), SHORT_SETUP);
            let (b, peer_at_b) = shared_peer(&higher, lower.iroh().id(), LONG_SETUP);
            let (c1_at_a, c1_at_b) = connect(&lower, &higher).await;
            install(&a, &peer_at_a, &c1_at_a, b.generation);
            install(&b, &peer_at_b, &c1_at_b, a.generation);
            assert!(
                !peer_at_b
                    .state()
                    .can_claim_no_open_lower_link(b.local_id, peer_at_b.id)
            );
            peer_at_b.state().active.take();

            let (d_at_b, d_at_a) = connect(&higher, &lower).await;
            let incoming_d = reserve(&a, &peer_at_a, &d_at_a);
            let captured_c1 = incoming_d
                .lower_link
                .as_ref()
                .expect("capture the original lease");
            let (client, server) = tokio::join!(
                exchange_hello(&d_at_b, b.hello(), b.config.connect_timeout, || peer_at_b
                    .state()
                    .can_claim_no_open_lower_link(b.local_id, peer_at_b.id)),
                exchange_hello(&d_at_a, a.hello(), a.config.connect_timeout, || false),
            );
            let client = checked(client, "write the retired-link claim");
            let server = checked(server, "read the retired-link claim");
            assert!(server.no_open_lower_initiated_link);
            assert!(!client.no_open_lower_initiated_link);

            // Keep D's incoming reservation while C2 is installed. The delayed claim was made
            // for C1 even though C2 has the same initiator and runs in the same generation.
            let (c2_at_a, c2_at_b) = connect(&lower, &higher).await;
            install(&a, &peer_at_a, &c2_at_a, b.generation);
            install(&b, &peer_at_b, &c2_at_b, a.generation);
            assert!(
                captured_c1.upgrade().is_none(),
                "the capture kept no strong lease"
            );
            assert!(d_at_a.close_reason().is_none());
            {
                let mut state = peer_at_a.state();
                state.incoming.take();
                assert!(
                    a.admit(
                        &peer_at_a,
                        &mut state,
                        &d_at_a,
                        incoming_d.observed_generation,
                        server.generation,
                        Some(captured_c1)
                    ) == DialResult::Replaced
                );
                assert!(state.is_current(&c2_at_a));
            }
            {
                let mut state = peer_at_b.state();
                assert!(
                    b.admit(
                        &peer_at_b,
                        &mut state,
                        &d_at_b,
                        Some(client.generation),
                        client.generation,
                        None
                    ) == DialResult::Replaced
                );
                assert!(state.is_current(&c2_at_b));
            }
            a.stop();
            b.stop();
            tokio::join!(lower.iroh().close(), higher.iroh().close());
        })
        .await,
        "the exact captured lease is checked before replacement",
    );
}

#[tokio::test]
async fn a_matching_capture_does_not_override_outgoing_rank_or_revocation() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (lower, higher) = ordered_endpoints().await;
            let (b, peer) = shared_peer(&higher, lower.iroh().id(), LONG_SETUP);
            let (_c_at_a, c_at_b) = connect(&lower, &higher).await;
            let (d_at_b, _d_at_a) = connect(&higher, &lower).await;
            let generation = [1; 16];
            install(&b, &peer, &c_at_b, generation);
            let captured = {
                let mut state = peer.state();
                let captured = Arc::downgrade(&state.active.as_ref().unwrap().connection);
                assert!(
                    b.admit(
                        &peer,
                        &mut state,
                        &d_at_b,
                        Some(generation),
                        generation,
                        Some(&captured)
                    ) == DialResult::Replaced
                );
                assert!(state.is_current(&c_at_b));
                captured
            };

            let (_new_at_a, new_at_b) = connect(&lower, &higher).await;
            peer.revoke(&b.paths);
            {
                let mut state = peer.state();
                assert!(!state.can_claim_no_open_lower_link(b.local_id, peer.id));
                assert!(
                    b.admit(
                        &peer,
                        &mut state,
                        &new_at_b,
                        Some(generation),
                        generation,
                        Some(&captured)
                    ) == DialResult::Failed(SendError::Closed)
                );
                assert!(state.active.is_none());
            }
            b.stop();
            tokio::join!(lower.iroh().close(), higher.iroh().close());
        })
        .await,
        "a recovery allowance leaves the other admission guards in place",
    );
}

fn add_loopback_lookup(endpoint: &Endpoint, peer: &Endpoint) {
    let address = EndpointAddr::new(peer.iroh().id()).with_ip_addr(peer.iroh().bound_sockets()[0]);
    checked(endpoint.iroh().address_lookup(), "read the test lookup")
        .add(MemoryLookup::from_endpoint_info([address]));
}

fn message(tag: &str) -> ChatMessage {
    ChatMessage {
        id: tag.to_owned(),
        sent_at_ms: 0,
        body: "loopback chat".to_owned(),
    }
}

async fn deliver(
    sender: &LinkHandle,
    to: NodeId,
    from: NodeId,
    incoming: &mut mpsc::Receiver<IncomingChat>,
    tag: &str,
) {
    let chat = message(tag);
    let (result, ()) = checked(
        timeout(SEND_DEADLINE, async {
            tokio::join!(sender.send(to, chat.clone()), async {
                let delivery = incoming.recv().await.expect("receive the test chat");
                assert!(delivery.station_id == from && delivery.message == chat);
                delivery.respond(ChatReply::Ack);
            })
        })
        .await,
        "deliver chat within its original deadline",
    );
    assert_eq!(result, Ok(ChatReply::Ack));
}

#[tokio::test]
async fn synthetic_retirement_reconnects_two_links_with_the_same_generation() {
    checked(
        timeout(CASE_TIMEOUT, async {
            let (lower, higher) = ordered_endpoints().await;
            add_loopback_lookup(&lower, &higher);
            add_loopback_lookup(&higher, &lower);
            let lower_id = lower.iroh().id();
            let higher_id = higher.iroh().id();
            let (a, mut inbox_at_a) = Link::start(lower, config(SHORT_SETUP));
            let (b, mut inbox_at_b) = Link::start(higher, config(SHORT_SETUP));
            let a_handle = a.handle();
            let b_handle = b.handle();
            deliver(&a_handle, higher_id, lower_id, &mut inbox_at_b, "original").await;
            let peer_at_a = checked(
                a.shared.peer(higher_id),
                "find the original lower-side peer",
            );
            let peer_at_b = checked(
                b.shared.peer(lower_id),
                "find the original higher-side peer",
            );
            let c_at_a = {
                let state = peer_at_a.state();
                let active = state.active.as_ref().expect("lower side still has C");
                assert!(active.is_open() && active.connection.transport.side() == Side::Client);
                active.connection.transport.clone()
            };

            // Synthetic retirement at the higher endpoint. Keep its exact old transport and
            // running driver alive while removing only its active lease, without sending a close.
            let retired = {
                let mut state = peer_at_b.state();
                let active = state
                    .active
                    .take()
                    .expect("retire the higher side's exact lease");
                assert!(active.is_open() && active.connection.transport.side() == Side::Server);
                assert!(state.can_claim_no_open_lower_link(b.shared.local_id, peer_at_b.id));
                active.connection
            };
            assert!(peer_at_a.state().is_current(&c_at_a));
            assert!(retired.transport.close_reason().is_none());

            let (send, mut recv) =
                checked(c_at_a.open_bi().await, "open a chat on original transport");
            checked(
                write_chat_request(send, &message("retired")).await,
                "write a chat on old C",
            );
            assert_eq!(
                read_chat_reply(&mut recv, SHORT_SETUP).await,
                Err(SendError::Disconnected)
            );
            assert!(matches!(
                inbox_at_b.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert!(peer_at_a.state().is_current(&c_at_a));
            assert!(retired.transport.close_reason().is_none());

            deliver(&b_handle, lower_id, higher_id, &mut inbox_at_a, "recover").await;
            {
                let state = peer_at_a.state();
                let current = state.active.as_ref().expect("the recovered link is active");
                assert!(current.is_open() && current.connection.transport.side() == Side::Server);
                assert!(!current.matches(&c_at_a));
                assert!(state.remote_generation == Some(b.shared.generation));
            }
            assert!(c_at_a.close_reason().is_some());
            deliver(&a_handle, higher_id, lower_id, &mut inbox_at_b, "reverse").await;
            let (a_closed, b_closed) = tokio::join!(a.shutdown(), b.shutdown());
            checked(a_closed, "stop lower link");
            checked(b_closed, "stop higher link");
        })
        .await,
        "recover and reverse chat on actual loopback Links",
    );
}
