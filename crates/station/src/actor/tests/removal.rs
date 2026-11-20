//! Peer removal against delivery jobs that have already finished but are not joined yet.
//!
//! The test drives the actor itself on a current-thread runtime, so nothing joins `Actor::jobs`
//! behind its back. A worker task finishes only while the test awaits, and in the same poll it
//! moves its entry to the JoinSet's ready list. A job is therefore "finished and not yet joined"
//! exactly when `Pending::worker.is_finished()` is true.

use std::fs::{self, Permissions};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chief::api::{ApiError, Event, FailReason, MessageStatus, Response, Status};
use chief::client::{SUBSCRIPTION_CAPACITY, Subscription, subscription};
use iroh::EndpointAddr;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use numbers::link::{LINK_ALPN, Link, LinkConfig, LinkPath};
use numbers::proto::chat::{ChatReply, NackReason};
use numbers::proto::{CHAT_PROTOCOL, Hello, HelloEcho, StreamHeader};
use numbers::{
    Authorizer, Endpoint, EndpointConfig, IpTransport, NodeId, Protocol, generate_node_key, node_id,
};
use numbers_base::{WIRE_FRAME_MAX, read_frame, write_frame};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::{sleep, timeout};

use super::super::Actor;
use super::queued_event;
use crate::storage::Storage;

/// Longer than any case may run, so an unanswered send cannot end by its deadline in a case.
const SEND_BUDGET: Duration = Duration::from_secs(600);
/// Bounds a failing case only.
const CASE_BUDGET: Duration = Duration::from_secs(60);
/// Interval for re-checking an observable condition; it never decides an outcome.
const POLL: Duration = Duration::from_millis(2);

fn checked<T, E>(result: Result<T, E>, operation: &str) -> T {
    result.unwrap_or_else(|_| panic!("{operation}"))
}

struct Fixture {
    actor: Actor,
    link: Link,
    address: EndpointAddr,
    events: Subscription,
}

struct RawPeer {
    id: NodeId,
    endpoint: Endpoint,
    connection: Connection,
}

async fn bind(key: &[u8; 32], peer: NodeId) -> Endpoint {
    let authorizer = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
    authorizer.replace_peers([(peer, "station".into())]);
    checked(
        Endpoint::bind(
            key,
            EndpointConfig {
                ip_transport: IpTransport::LoopbackOnly,
                ..EndpointConfig::default()
            },
            authorizer,
        )
        .await,
        "bind removal test endpoint",
    )
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

/// An actor on a loopback endpoint, with an event subscription attached after setup.
async fn station(data_dir: &Path) -> Fixture {
    let storage = checked(Storage::lock(data_dir), "lock removal test directory");
    let key = checked(storage.load_node_key(), "load removal test key");
    let own_id = node_id(&key);
    let authorizer = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
    let endpoint = checked(
        Endpoint::bind(
            &key,
            EndpointConfig {
                ip_transport: IpTransport::LoopbackOnly,
                ..EndpointConfig::default()
            },
            Arc::clone(&authorizer),
        )
        .await,
        "bind removal test station",
    );
    let address = EndpointAddr::new(own_id).with_ip_addr(endpoint.iroh().bound_sockets()[0]);
    let (link, _incoming) = Link::start(
        endpoint,
        LinkConfig {
            connect_timeout: CASE_BUDGET,
            send_deadline: SEND_BUDGET,
        },
    );
    let status = Status {
        station_id: own_id.to_string(),
        instance_id: "removal".to_owned(),
        towers: Vec::new(),
        bootstrap_domain: None,
    };
    let actor = Actor::new(
        storage,
        own_id,
        None,
        authorizer,
        status,
        SEND_BUDGET,
        link.handle(),
    );
    let (_, events) = subscription(SUBSCRIPTION_CAPACITY);
    Fixture {
        actor,
        link,
        address,
        events,
    }
}

impl Fixture {
    /// Adds a peer `name` with one raw station that dials in and performs the hello by hand.
    async fn peer(&mut self, name: &str) -> RawPeer {
        let key = generate_node_key();
        let id = node_id(&key);
        checked(
            self.actor.add_peer(name, &id.to_string()),
            "add the raw peer",
        );
        let endpoint = bind(&key, self.actor.own_id).await;
        let mut paths = self.actor.links.subscribe_paths();
        let connection = checked(
            endpoint
                .iroh()
                .connect(self.address.clone(), LINK_ALPN)
                .await,
            "dial station",
        );
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
        while self.actor.links.path(id) != LinkPath::Direct {
            checked(paths.recv().await, "observe admitted direct link");
        }
        RawPeer {
            id,
            endpoint,
            connection,
        }
    }

    /// Starts watching events from now on, after setup events.
    fn watch(&mut self) {
        let (sink, events) = subscription(SUBSCRIPTION_CAPACITY);
        self.actor.sinks.push(sink);
        self.events = events;
    }

    fn send(&mut self, peer: &str) -> u64 {
        let Ok(Response::Message { message }) = self.actor.send(peer, "removal test") else {
            panic!("the station accepts the send");
        };
        assert_eq!(message.status, MessageStatus::Sending);
        message.seq
    }

    /// Takes the chat stream of the send `seq` on the peer's side, unanswered.
    async fn accept(&self, peer: &RawPeer, seq: u64) -> SendStream {
        let (send, mut recv) = checked(peer.connection.accept_bi().await, "accept chat stream");
        assert!(read::<StreamHeader>(&mut recv).await == StreamHeader::chat());
        let chat: serde_json::Value = read(&mut recv).await;
        let expected = &self
            .actor
            .pending
            .get(&seq)
            .expect("the accepted send is pending")
            .message
            .id;
        assert!(
            chat["id"].as_str() == Some(expected.as_str()),
            "the chat stream belongs to the expected send"
        );
        assert!(checked(recv.read_to_end(0).await, "read chat stream end").is_empty());
        send
    }

    fn is_finished(&self, seq: u64) -> bool {
        self.actor
            .pending
            .get(&seq)
            .expect("the send is still pending")
            .worker
            .is_finished()
    }

    /// Waits until the worker of `seq` has returned. Nothing joins it meanwhile.
    async fn finished(&self, seq: u64) {
        while !self.is_finished(seq) {
            sleep(POLL).await;
        }
    }

    /// Answers the send `seq` and waits until its worker has returned.
    async fn answer(&self, mut stream: SendStream, seq: u64, reply: ChatReply) {
        write(&mut stream, &reply).await;
        checked(stream.finish(), "finish chat reply");
        self.finished(seq).await;
    }

    fn status_of(&self, seq: u64) -> MessageStatus {
        checked(self.actor.history.message(seq), "the log holds the message").status
    }

    /// The next queued event, which must be a final message event.
    async fn message_event(&mut self) -> (u64, MessageStatus) {
        let Some(Event::Message { message }) = queued_event(&mut self.events).await else {
            panic!("expected a message event");
        };
        assert_eq!(message.rev, 1);
        (message.seq, message.status)
    }

    async fn peer_removed_event(&mut self, name: &str) {
        let Some(Event::PeerRemoved { name: removed }) = queued_event(&mut self.events).await
        else {
            panic!("expected a peer removed event");
        };
        assert_eq!(removed, name);
    }

    async fn no_event(&mut self) {
        assert!(
            queued_event(&mut self.events).await.is_none(),
            "no further event"
        );
    }

    /// What one `turn` does when it selects the JoinSet branch.
    async fn join_turn(&mut self) {
        let result = self
            .actor
            .jobs
            .join_next()
            .await
            .expect("a job is left to join");
        self.actor.completed(result);
    }

    async fn shut_down(self, peers: Vec<RawPeer>) {
        checked(self.link.shutdown().await, "shut down removal test link");
        drop(self.actor);
        for peer in peers {
            peer.endpoint.iroh().close().await;
        }
    }
}

fn removed() -> MessageStatus {
    MessageStatus::Failed {
        reason: FailReason::PeerRemoved,
    }
}

fn refused() -> MessageStatus {
    MessageStatus::Failed {
        reason: FailReason::Refused,
    }
}

async fn finished_result_survives_removal(reply: ChatReply, expected: MessageStatus) {
    let dir = checked(tempfile::tempdir(), "create removal test directory");
    let mut station = station(&dir.path().join("station")).await;
    let alice = station.peer("alice").await;
    let seq = station.send("alice");
    let stream = station.accept(&alice, seq).await;
    station.answer(stream, seq, reply).await;
    station.watch();

    assert!(matches!(
        station.actor.remove_peer("alice"),
        Ok(Response::Done)
    ));

    assert_eq!(station.message_event().await, (seq, expected.clone()));
    station.peer_removed_event("alice").await;
    station.no_event().await;
    assert_eq!(station.status_of(seq), expected);
    assert!(station.actor.pending.is_empty());
    assert!(station.actor.jobs.is_empty(), "the finished job was joined");
    station.shut_down(vec![alice]).await;
}

#[tokio::test]
async fn a_delivered_send_not_yet_joined_stays_delivered_after_peer_removal() {
    checked(
        timeout(
            CASE_BUDGET,
            finished_result_survives_removal(ChatReply::Ack, MessageStatus::Delivered),
        )
        .await,
        "delivered send before removal timed out",
    );
}

#[tokio::test]
async fn a_refused_send_not_yet_joined_stays_refused_after_peer_removal() {
    checked(
        timeout(
            CASE_BUDGET,
            finished_result_survives_removal(ChatReply::Nack(NackReason::InvalidBody), refused()),
        )
        .await,
        "refused send before removal timed out",
    );
}

#[tokio::test]
async fn an_unfinished_send_becomes_peer_removed_and_stays_so() {
    checked(
        timeout(CASE_BUDGET, async {
            let dir = checked(tempfile::tempdir(), "create removal test directory");
            let mut station = station(&dir.path().join("station")).await;
            let alice = station.peer("alice").await;
            let done = station.send("alice");
            let stream = station.accept(&alice, done).await;
            station.answer(stream, done, ChatReply::Ack).await;
            let open = station.send("alice");
            let unanswered = station.accept(&alice, open).await;
            assert!(!station.is_finished(open));
            station.watch();

            assert!(matches!(
                station.actor.remove_peer("alice"),
                Ok(Response::Done)
            ));

            assert_eq!(
                station.message_event().await,
                (done, MessageStatus::Delivered)
            );
            assert_eq!(station.message_event().await, (open, removed()));
            station.peer_removed_event("alice").await;
            station.no_event().await;
            assert_eq!(station.status_of(open), removed());

            // The aborted worker is joined later through the turn's path: nothing changes.
            let joined = station
                .actor
                .jobs
                .join_next()
                .await
                .expect("the aborted worker is left to join");
            assert!(matches!(&joined, Err(error) if error.is_cancelled()));
            station.actor.completed(joined);
            assert!(station.actor.jobs.is_empty());
            // A completion that raced the removal on another worker thread cannot overwrite it.
            station
                .actor
                .completed(Ok((open, MessageStatus::Delivered)));
            station.no_event().await;
            assert_eq!(station.status_of(open), removed());
            assert_eq!(station.status_of(done), MessageStatus::Delivered);
            drop(unanswered);
            station.shut_down(vec![alice]).await;
        })
        .await,
        "unfinished send at removal timed out",
    );
}

#[tokio::test]
async fn removal_completes_finished_jobs_of_other_peers_in_join_order_and_leaves_the_rest() {
    checked(
        timeout(CASE_BUDGET, async {
            let dir = checked(tempfile::tempdir(), "create removal test directory");
            let mut station = station(&dir.path().join("station")).await;
            let alice = station.peer("alice").await;
            let bob = station.peer("bob").await;

            // Alice's send is older, but bob's job finishes first. JoinSet hands out finished
            // jobs in the order they finished (tokio 1.54 queues woken entries first in, first
            // out), so the join order is bob's, then alice's, unlike the order of the seqs.
            let alice_done = station.send("alice");
            let alice_stream = station.accept(&alice, alice_done).await;
            let bob_done = station.send("bob");
            let bob_stream = station.accept(&bob, bob_done).await;
            station
                .answer(
                    bob_stream,
                    bob_done,
                    ChatReply::Nack(NackReason::InvalidBody),
                )
                .await;
            assert!(!station.is_finished(alice_done));
            station
                .answer(alice_stream, alice_done, ChatReply::Ack)
                .await;
            let bob_open = station.send("bob");
            let bob_unanswered = station.accept(&bob, bob_open).await;
            assert!(!station.is_finished(bob_open));
            station.watch();

            assert!(matches!(
                station.actor.remove_peer("alice"),
                Ok(Response::Done)
            ));

            assert_eq!(station.message_event().await, (bob_done, refused()));
            assert_eq!(
                station.message_event().await,
                (alice_done, MessageStatus::Delivered)
            );
            station.peer_removed_event("alice").await;
            station.no_event().await;
            assert_eq!(station.status_of(bob_done), refused());
            assert_eq!(station.status_of(alice_done), MessageStatus::Delivered);

            // Nothing of bob is failed or removed, and his unfinished send stays queued.
            assert!(station.actor.book.contains_key("bob"));
            assert!(!station.actor.book.contains_key("alice"));
            assert!(
                station
                    .actor
                    .authorizer
                    .authorize(&bob.id, Protocol::Stream(CHAT_PROTOCOL))
                    .is_some()
            );
            assert!(station.actor.links.path(bob.id) == LinkPath::Direct);
            assert_eq!(station.status_of(bob_open), MessageStatus::Sending);
            assert!(station.actor.pending.keys().eq([&bob_open]));
            assert_eq!(station.actor.jobs.len(), 1);

            station
                .answer(bob_unanswered, bob_open, ChatReply::Ack)
                .await;
            station.join_turn().await;
            assert_eq!(
                station.message_event().await,
                (bob_open, MessageStatus::Delivered)
            );
            station.no_event().await;
            assert!(station.actor.jobs.is_empty());
            station.shut_down(vec![alice, bob]).await;
        })
        .await,
        "removal with two peers timed out",
    );
}

#[tokio::test]
async fn a_removal_refused_before_rename_leaves_finished_jobs_to_the_next_turn() {
    checked(
        timeout(CASE_BUDGET, async {
            let dir = checked(tempfile::tempdir(), "create removal test directory");
            let data_dir = dir.path().join("station");
            let book_path = data_dir.join("peers.json");
            let mut station = station(&data_dir).await;
            let alice = station.peer("alice").await;
            let first = station.send("alice");
            let stream = station.accept(&alice, first).await;
            station.answer(stream, first, ChatReply::Ack).await;
            let second = station.send("alice");
            let stream = station.accept(&alice, second).await;
            station
                .answer(stream, second, ChatReply::Nack(NackReason::InvalidBody))
                .await;
            let open = station.send("alice");
            let unanswered = station.accept(&alice, open).await;
            assert!(!station.is_finished(open));
            let book = station.actor.book.clone();
            station.watch();

            // An unsafe destination makes the save fail before rename: no removal happens.
            checked(
                fs::set_permissions(&book_path, Permissions::from_mode(0o644)),
                "make destination unsafe",
            );
            assert!(matches!(
                station.actor.remove_peer("alice"),
                Err(ApiError::Internal { .. })
            ));
            checked(
                fs::set_permissions(&book_path, Permissions::from_mode(0o600)),
                "restore destination mode",
            );

            // Not now: the refused command changes nothing and publishes nothing.
            station.no_event().await;
            assert!(station.actor.book == book);
            assert!(
                station
                    .actor
                    .authorizer
                    .authorize(&alice.id, Protocol::Stream(CHAT_PROTOCOL))
                    .is_some()
            );
            assert!(station.actor.links.path(alice.id) == LinkPath::Direct);
            for seq in [first, second, open] {
                assert_eq!(station.status_of(seq), MessageStatus::Sending);
            }
            assert_eq!(station.actor.pending.len(), 3);
            assert_eq!(station.actor.jobs.len(), 3);

            // At the next turn: the turn's JoinSet branch completes the first finished job.
            station.join_turn().await;
            assert_eq!(
                station.message_event().await,
                (first, MessageStatus::Delivered)
            );
            station.no_event().await;

            // A removal that now succeeds keeps the other finished result and fails the rest.
            assert!(matches!(
                station.actor.remove_peer("alice"),
                Ok(Response::Done)
            ));
            assert_eq!(station.message_event().await, (second, refused()));
            assert_eq!(station.message_event().await, (open, removed()));
            station.peer_removed_event("alice").await;
            station.no_event().await;
            assert_eq!(station.status_of(first), MessageStatus::Delivered);
            assert_eq!(station.status_of(second), refused());
            assert_eq!(station.status_of(open), removed());
            drop(unanswered);
            station.shut_down(vec![alice]).await;
        })
        .await,
        "refused removal timed out",
    );
}

#[tokio::test]
async fn removal_completes_a_failed_worker_of_another_peer_that_is_not_yet_joined() {
    checked(
        timeout(CASE_BUDGET, async {
            let dir = checked(tempfile::tempdir(), "create removal test directory");
            let mut station = station(&dir.path().join("station")).await;
            let alice = station.peer("alice").await;
            let bob = station.peer("bob").await;
            let bob_open = station.send("bob");
            let bob_unanswered = station.accept(&bob, bob_open).await;

            // Aborting through the handle stands in for a worker that panicked: it ends with a
            // JoinError while its send is still pending, and nothing has joined it yet.
            station
                .actor
                .pending
                .get(&bob_open)
                .expect("bob's send is pending")
                .worker
                .abort();
            station.finished(bob_open).await;
            station.watch();

            assert!(matches!(
                station.actor.remove_peer("alice"),
                Ok(Response::Done)
            ));

            let no_answer = MessageStatus::Failed {
                reason: FailReason::NoAnswer,
            };
            assert_eq!(station.message_event().await, (bob_open, no_answer.clone()));
            station.peer_removed_event("alice").await;
            station.no_event().await;
            assert_eq!(station.status_of(bob_open), no_answer);
            assert!(station.actor.pending.is_empty());
            assert!(
                station.actor.jobs.is_empty(),
                "the failed worker was joined"
            );
            drop(bob_unanswered);
            station.shut_down(vec![alice, bob]).await;
        })
        .await,
        "failed worker of another peer at removal timed out",
    );
}
