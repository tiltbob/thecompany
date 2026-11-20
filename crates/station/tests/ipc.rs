#![cfg(test)]
#![cfg(unix)]
//! The local-socket server (`numbers_station::ipc`) with `chief`'s `SocketTransport` and `Client`,
//! over a real Unix socket in a temporary directory, against `chief::fake::FakeStation`.
//!
//! Every test is bounded by a timeout, waits on conditions instead of sleeping, and stays inside
//! its temporary directory. Ids are placeholders (AGENTS.md, hard rule 9).

use chief::API_VERSION;
use chief::api::*;
use chief::client::{Client, SUBSCRIPTION_CAPACITY, SocketTransport, Subscription, Transport};
use chief::fake::{FakeStation, SendOutcome};
use chief::follow::LogUpdate;
use chief::socket::wire::{Hello, HelloReply, Open, Reply};
use numbers_base::{FrameError, LOCAL_FRAME_MAX, read_frame, write_frame};
use numbers_station::ipc::{IpcConfig, IpcError, IpcServer};
use std::future::Future;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, watch};

/// The bound on every test.
const TEST_BOUND: Duration = Duration::from_secs(60);
/// The bound on one read of a raw client.
const READ_BOUND: Duration = Duration::from_secs(10);

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(TEST_BOUND, test)
        .await
        .expect("the test did not finish in time")
}

struct Rig {
    dir: TempDir,
    fake: FakeStation,
    server: IpcServer,
    client: Client<SocketTransport>,
}

impl Rig {
    fn path(&self) -> &Path {
        self.server.path()
    }
}

fn socket_in(dir: &TempDir) -> PathBuf {
    dir.path().join("run/thecompany/station.sock")
}

fn fake(station_id: &str) -> FakeStation {
    FakeStation::new(station_id, "INSTANCE-1")
}

/// A server for a fake with the peers `alice` and `bob`, and a client.
async fn rig_with(config: IpcConfig) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let fake = fake("STATION-A");
    let c = Client::new(fake.clone());
    c.peer_add("alice", "STATION-AL").await.unwrap();
    c.peer_add("bob", "STATION-B").await.unwrap();
    let server = IpcServer::bind(fake.clone(), &socket_in(&dir), config)
        .await
        .unwrap();
    let client = Client::new(SocketTransport::new(server.path()));
    Rig {
        dir,
        fake,
        server,
        client,
    }
}

async fn rig() -> Rig {
    rig_with(IpcConfig::default()).await
}

fn receive_n(fake: &FakeStation, n: usize, body: &str) {
    for _ in 0..n {
        fake.receive("bob", body).unwrap();
    }
}

// A client that speaks the wire by hand.

async fn raw_connect(path: &Path) -> UnixStream {
    UnixStream::connect(path).await.unwrap()
}

async fn raw_read<T: serde::de::DeserializeOwned>(
    stream: &mut UnixStream,
) -> Result<T, FrameError> {
    read_frame(stream, LOCAL_FRAME_MAX, Some(READ_BOUND)).await
}

async fn raw_hello(stream: &mut UnixStream, api: u32) -> HelloReply {
    write_frame(stream, &Hello { api }, LOCAL_FRAME_MAX)
        .await
        .unwrap();
    raw_read(stream).await.unwrap()
}

/// A frame holding `body` as it is.
fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(body);
    out
}

/// Waits until the server closes `stream` (end of stream or a reset), within the read bound.
async fn wait_closed(stream: &mut UnixStream) {
    let mut buf = [0u8; 4096];
    tokio::time::timeout(READ_BOUND, async {
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await
    .expect("the server did not close the connection");
}

async fn next_event(events: &mut Subscription) -> Event {
    tokio::time::timeout(READ_BOUND, events.next())
        .await
        .expect("no event arrived")
        .expect("the subscription ended")
}

fn mode(path: &Path) -> u32 {
    std::fs::symlink_metadata(path)
        .unwrap()
        .permissions()
        .mode()
        & 0o7777
}

/// Binds a server for `fake` on `path`, retrying while the socket is still in use (a dropped
/// server on its way down), within the read bound.
async fn bind_when_free(fake: &FakeStation, path: &Path) -> IpcServer {
    tokio::time::timeout(READ_BOUND, async {
        loop {
            match IpcServer::bind(fake.clone(), path, IpcConfig::default()).await {
                Ok(server) => return server,
                Err(IpcError::InUse { .. }) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(e) => panic!("{e}"),
            }
        }
    })
    .await
    .expect("the socket was not freed in time")
}

// A transport whose calls can stay pending.

/// The call that [`Pending`] holds: [`Request::Message`] for this seq.
const PENDING_SEQ: u64 = 4_242;

fn pending_call() -> Open {
    Open::Call {
        request: Request::Message { seq: PENDING_SEQ },
    }
}

/// What happened to the calls [`Pending`] held.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Counts {
    /// Held calls that reached the transport.
    started: usize,
    /// Held calls whose future is being dropped unfinished (counted before a held drop blocks).
    dropping: usize,
    /// Held calls whose future was dropped unfinished.
    cancelled: usize,
    /// Held calls that were released and answered.
    answered: usize,
}

/// Shared between a [`Pending`] transport and its test.
struct Probe {
    counts: Mutex<Counts>,
    changed: Notify,
    /// The gate of held calls: they answer once it is `true`.
    gate: watch::Sender<bool>,
    /// If set, the next held call that is dropped unfinished blocks in its drop until this
    /// receives a message (or the read bound passes), so that its connection task cannot finish.
    hold_drop: Mutex<Option<mpsc::Receiver<()>>>,
}

impl Probe {
    fn new() -> Arc<Probe> {
        Arc::new(Probe {
            counts: Mutex::new(Counts::default()),
            changed: Notify::new(),
            gate: watch::Sender::new(false),
            hold_drop: Mutex::new(None),
        })
    }

    fn counts(&self) -> Counts {
        *self.counts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn update(&self, change: impl FnOnce(&mut Counts)) {
        change(&mut self.counts.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.notify_waiters();
    }

    /// Waits, within the read bound, until `done` holds for the counts.
    async fn wait(&self, what: &str, done: impl Fn(Counts) -> bool) {
        tokio::time::timeout(READ_BOUND, async {
            loop {
                let notified = self.changed.notified();
                let mut notified = pin!(notified);
                notified.as_mut().enable();
                if done(self.counts()) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}: not within the bound; {:?}", self.counts()));
    }

    /// Lets every held call answer.
    fn release(&self) {
        self.gate.send_replace(true);
    }
}

/// Counts one held call from its start until it is answered or dropped.
struct Held {
    probe: Arc<Probe>,
    answered: bool,
}

impl Drop for Held {
    fn drop(&mut self) {
        if self.answered {
            self.probe.update(|c| c.answered += 1);
            return;
        }
        let hold = self
            .probe
            .hold_drop
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        self.probe.update(|c| c.dropping += 1);
        if let Some(hold) = hold {
            // Blocks this runtime worker on purpose: the connection task cannot finish meanwhile.
            let _ = hold.recv_timeout(READ_BOUND);
        }
        self.probe.update(|c| c.cancelled += 1);
    }
}

/// The fake station, except that [`Request::Message`] for [`PENDING_SEQ`] waits for
/// [`Probe::release`] before the fake answers it.
#[derive(Clone)]
struct Pending {
    fake: FakeStation,
    probe: Arc<Probe>,
}

impl Transport for Pending {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        if !matches!(request, Request::Message { seq: PENDING_SEQ }) {
            return self.fake.call(request).await;
        }
        let mut held = Held {
            probe: Arc::clone(&self.probe),
            answered: false,
        };
        self.probe.update(|c| c.started += 1);
        let mut gate = self.probe.gate.subscribe();
        let _ = gate.wait_for(|open| *open).await;
        let answer = self.fake.call(request).await;
        held.answered = true;
        answer
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        self.fake.subscribe().await
    }
}

/// A server on a [`Pending`] transport with `config`, and a client.
async fn pending_rig(
    config: IpcConfig,
) -> (
    TempDir,
    FakeStation,
    Arc<Probe>,
    IpcServer,
    Client<SocketTransport>,
) {
    let dir = tempfile::tempdir().unwrap();
    let fake = fake("STATION-A");
    let probe = Probe::new();
    let transport = Pending {
        fake: fake.clone(),
        probe: Arc::clone(&probe),
    };
    let server = IpcServer::bind(transport, &socket_in(&dir), config)
        .await
        .unwrap();
    let client = Client::new(SocketTransport::new(server.path()));
    (dir, fake, probe, server, client)
}

/// One slot, and frames that never time out within a test: only the client leaving can free the
/// slot.
fn one_slot() -> IpcConfig {
    IpcConfig {
        frame_timeout: Duration::from_secs(600),
        max_connections: 1,
    }
}

/// A raw client that has sent the hello and `open`, and keeps the connection open.
async fn raw_open(path: &Path, open: &Open) -> UnixStream {
    let mut stream = raw_connect(path).await;
    raw_hello(&mut stream, API_VERSION).await;
    write_frame(&mut stream, open, LOCAL_FRAME_MAX)
        .await
        .unwrap();
    stream
}

// 1. Calls.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_and_the_peer_book_through_the_socket() {
    bounded(async {
        let rig = rig().await;
        let c = &rig.client;
        assert_eq!(c.status().await.unwrap(), rig.fake.status());
        let names = |peers: Vec<Peer>| peers.into_iter().map(|p| p.name).collect::<Vec<_>>();
        assert_eq!(names(c.peers().await.unwrap()), vec!["alice", "bob"]);
        let carol = c.peer_add("carol", "STATION-C").await.unwrap();
        assert_eq!(carol.stations[0].station_id, "STATION-C");
        assert_eq!(
            names(c.peers().await.unwrap()),
            vec!["alice", "bob", "carol"]
        );
        c.peer_remove("carol").await.unwrap();
        assert_eq!(names(c.peers().await.unwrap()), vec!["alice", "bob"]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_with_each_outcome_and_message_through_the_socket() {
    bounded(async {
        let rig = rig().await;
        let c = &rig.client;
        let sent = c.send("bob", "on my way").await.unwrap();
        assert_eq!((sent.rev, sent.status.clone()), (0, MessageStatus::Sending));
        let now = c.message(sent.seq).await.unwrap();
        assert_eq!((now.rev, now.status), (1, MessageStatus::Delivered));

        for reason in [FailReason::NoAnswer, FailReason::Refused] {
            rig.fake.set_outcome(SendOutcome::Fail(reason));
            let sent = c.send("bob", "x").await.unwrap();
            assert_eq!(
                c.message(sent.seq).await.unwrap().status,
                MessageStatus::Failed { reason }
            );
        }

        rig.fake.set_outcome(SendOutcome::Hold);
        let held = c.send("bob", "held").await.unwrap();
        assert_eq!(c.message(held.seq).await.unwrap(), held);
        rig.fake
            .resolve(held.seq, MessageStatus::Delivered)
            .unwrap();
        assert_eq!(
            c.message(held.seq).await.unwrap().status,
            MessageStatus::Delivered
        );

        // send_and_wait over two connections: the subscription and the call.
        rig.fake.set_outcome(SendOutcome::Deliver);
        let done = c.send_and_wait("bob", "wait for it").await.unwrap();
        assert_eq!(done.status, MessageStatus::Delivered);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_log_pages_through_next_before() {
    bounded(async {
        let rig = rig().await;
        receive_n(&rig.fake, 40, &"x".repeat(BODY_MAX_BYTES));
        let mut all = Vec::new();
        let mut before = None;
        let mut pages = 0;
        loop {
            let page = rig.client.log(None, before, 100).await.unwrap();
            pages += 1;
            let mut seqs: Vec<u64> = page.messages.iter().map(|m| m.seq).collect();
            seqs.append(&mut all);
            all = seqs;
            match page.next_before {
                Some(b) => before = Some(b),
                None => break,
            }
        }
        assert!(pages >= 2, "{pages}");
        assert_eq!(all, (1..=40).collect::<Vec<_>>());
        let bob = rig.client.log(Some("bob"), None, 3).await.unwrap();
        assert_eq!(bob.messages.len(), 3);
    })
    .await;
}

// 2. Errors.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn station_errors_arrive_unchanged() {
    bounded(async {
        let rig = rig().await;
        let c = &rig.client;
        let direct = Client::new(rig.fake.clone());
        assert_eq!(
            c.peer_add("-x", "STATION-X").await.unwrap_err(),
            direct.peer_add("-x", "STATION-X").await.unwrap_err()
        );
        assert_eq!(
            c.peer_add("carol", "STATION-B").await.unwrap_err(),
            ApiError::StationTaken {
                station_id: "STATION-B".into(),
                peer: "bob".into()
            }
        );
        assert_eq!(
            c.send("carol", "x").await.unwrap_err(),
            ApiError::PeerNotFound {
                name: "carol".into()
            }
        );
        assert_eq!(
            c.message(999).await.unwrap_err(),
            ApiError::MessageNotFound { seq: 999 }
        );
        rig.fake.stop();
        assert_eq!(c.status().await.unwrap_err(), ApiError::Stopping);
        assert_eq!(c.subscribe().await.unwrap_err(), ApiError::Stopping);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_undecodable_request_gets_bad_request() {
    bounded(async {
        let rig = rig().await;
        for body in [
            &br#"{"type":"call","request":{"type":"nope"}}"#[..],
            br#"{"type":"stream"}"#,
            b"not json",
            br#"{"type":"call","request":{"type":"message","seq":"MARKER-SECRET"}}"#,
        ] {
            let mut stream = raw_connect(rig.path()).await;
            let hello = raw_hello(&mut stream, API_VERSION).await;
            assert_eq!(
                (
                    hello.api,
                    hello.station_id.as_str(),
                    hello.instance_id.as_str()
                ),
                (API_VERSION, "STATION-A", "INSTANCE-1")
            );
            stream.write_all(&frame(body)).await.unwrap();
            let reply: Reply = raw_read(&mut stream).await.unwrap();
            let Reply::Err {
                error: ApiError::BadRequest { detail },
            } = reply
            else {
                panic!("expected BadRequest, got {reply:?}");
            };
            assert!(!detail.is_empty());
            // The detail never quotes the request (hard rule 9).
            assert!(!detail.contains("MARKER"), "{detail}");
            assert!(
                !detail.contains("nope") && !detail.contains("stream"),
                "{detail}"
            );
            wait_closed(&mut stream).await;
        }
        // An undecodable request reached nobody.
        assert!(
            rig.fake
                .requests()
                .iter()
                .all(|r| matches!(r, Request::PeerAdd { .. } | Request::Status))
        );
        assert!(rig.client.status().await.is_ok());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_of_another_version_gets_the_hello_reply_and_is_closed() {
    bounded(async {
        // A frame timeout far beyond this test's bound: only the version check can close the
        // connection in time.
        let rig = rig_with(IpcConfig {
            frame_timeout: Duration::from_secs(600),
            ..IpcConfig::default()
        })
        .await;
        let mut stream = raw_connect(rig.path()).await;
        let hello = raw_hello(&mut stream, API_VERSION + 1).await;
        assert_eq!(hello.api, API_VERSION);
        assert_eq!(hello.station_id, "STATION-A");
        // A call after the mismatched hello is never answered: the stream ends.
        let open = Open::Call {
            request: Request::Status,
        };
        let _ = write_frame(&mut stream, &open, LOCAL_FRAME_MAX).await;
        match raw_read::<Reply>(&mut stream).await {
            Err(FrameError::Closed | FrameError::Io(_)) => {}
            other => panic!("expected the end of the stream, got {other:?}"),
        }
        let status_calls = rig
            .fake
            .requests()
            .iter()
            .filter(|r| matches!(r, Request::Status))
            .count();
        assert_eq!(status_calls, 1, "only the server's own status call");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socket_transport_reports_a_station_of_another_version() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("station.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let hello: Hello = raw_read(&mut stream).await.unwrap();
            assert_eq!(hello.api, API_VERSION);
            let reply = HelloReply {
                api: API_VERSION + 1,
                station_id: "STATION-A".into(),
                instance_id: "INSTANCE-1".into(),
            };
            write_frame(&mut stream, &reply, LOCAL_FRAME_MAX)
                .await
                .unwrap();
        });
        let c = Client::new(SocketTransport::new(&path));
        assert_eq!(
            c.status().await.unwrap_err(),
            ApiError::IncompatibleApi {
                station: API_VERSION + 1,
                client: API_VERSION
            }
        );
        server.await.unwrap();
    })
    .await;
}

// 3. Subscriptions.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_then_events_arrive_in_order() {
    bounded(async {
        let rig = rig().await;
        let mut events = rig.client.subscribe().await.unwrap();
        // Registered before `subscribe` resolved (rule 3): nothing can slip past.
        assert_eq!(rig.fake.subscriber_count(), 1);
        let a = rig.fake.receive("bob", "1").unwrap();
        rig.fake.set_tower_state(TowerState::Connected);
        let carol = rig.client.peer_add("carol", "STATION-C").await.unwrap();
        let b = rig.fake.receive("alice", "2").unwrap();
        assert_eq!(next_event(&mut events).await, Event::Message { message: a });
        assert!(matches!(
            next_event(&mut events).await,
            Event::Status { status } if status.towers[0].state == TowerState::Connected
        ));
        assert_eq!(next_event(&mut events).await, Event::Peer { peer: carol });
        assert_eq!(next_event(&mut events).await, Event::Message { message: b });
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_then_page_yields_no_duplicate() {
    bounded(async {
        let rig = rig().await;
        receive_n(&rig.fake, 5, "before");
        rig.fake.on_call(|fake, request| {
            if matches!(request, Request::Log { .. }) {
                // Between the subscribe and the page.
                fake.receive("bob", "between").unwrap();
                fake.receive("alice", "between").unwrap();
                fake.clear_on_call();
            }
        });
        let mut follower = rig.client.follow_log(None, 50).await.unwrap();
        // Meanwhile the station keeps receiving.
        let fake = rig.fake.clone();
        let feeder = tokio::spawn(async move {
            for _ in 0..200 {
                fake.receive("bob", "live").unwrap();
                tokio::task::yield_now().await;
            }
        });
        let mut seqs = Vec::new();
        while seqs.len() < 207 {
            let update = tokio::time::timeout(READ_BOUND, follower.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let LogUpdate::Added(m) = update else {
                panic!("an incoming message never changes: {update:?}");
            };
            seqs.push(m.seq);
        }
        feeder.await.unwrap();
        assert_eq!(seqs, (1..=207).collect::<Vec<_>>());
    })
    .await;
}

// 4. Slow subscribers.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_that_does_not_read_gets_resync_then_live_events() {
    bounded(async {
        let rig = rig().await;
        let mut events = rig.client.subscribe().await.unwrap();
        receive_n(&rig.fake, 4 * SUBSCRIPTION_CAPACITY + 1_000, "x");
        loop {
            if next_event(&mut events).await == Event::Resync {
                break;
            }
        }
        // Events still in flight may overflow the queue again; each round pushes a fresh event
        // and reads until it arrives or another Resync says it may have been dropped.
        'rounds: loop {
            let live = rig.fake.receive("bob", "live").unwrap();
            loop {
                match next_event(&mut events).await {
                    Event::Message { message } if message.seq == live.seq => break 'rounds,
                    Event::Resync => continue 'rounds,
                    _ => {}
                }
            }
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_subscriber_stalls_only_its_own_connection() {
    bounded(async {
        let rig = rig().await;
        let mut stream = raw_connect(rig.path()).await;
        raw_hello(&mut stream, API_VERSION).await;
        write_frame(&mut stream, &Open::Subscribe, LOCAL_FRAME_MAX)
            .await
            .unwrap();
        assert_eq!(
            raw_read::<Reply>(&mut stream).await.unwrap(),
            Reply::Subscribed
        );
        // 1,000 events of 16 KiB: far more than the socket buffers and the queue of 256 hold, so
        // the server's write blocks and its queue overflows. The station never waits.
        receive_n(&rig.fake, 1_000, &"x".repeat(BODY_MAX_BYTES));
        assert_eq!(rig.client.status().await.unwrap(), rig.fake.status());
        let mut before_resync = 0;
        loop {
            match raw_read::<Event>(&mut stream).await.unwrap() {
                Event::Resync => break,
                Event::Message { .. } => before_resync += 1,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert!(before_resync < 1_000, "{before_resync}");
        // The server's queue was emptied when it sent Resync: the next event is the next change.
        let live = rig.fake.receive("bob", "live").unwrap();
        assert_eq!(
            raw_read::<Event>(&mut stream).await.unwrap(),
            Event::Message { message: live }
        );
    })
    .await;
}

// 5. Cancellation.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_pending_next_loses_nothing() {
    bounded(async {
        let rig = rig().await;
        let mut events = rig.client.subscribe().await.unwrap();
        for i in 0..20 {
            // A `next()` that is pending, then dropped.
            let pending = tokio::time::timeout(Duration::from_millis(1), events.next()).await;
            assert!(
                pending.is_err(),
                "round {i}: an event appeared from nowhere"
            );
            let got = rig.fake.receive("bob", "x").unwrap();
            assert_eq!(
                next_event(&mut events).await,
                Event::Message { message: got }
            );
        }
        let mut follower = rig.client.follow_log(None, 0).await.unwrap();
        for _ in 0..5 {
            let pending = tokio::time::timeout(Duration::from_millis(1), follower.next()).await;
            assert!(pending.is_err());
            let got = rig.fake.receive("bob", "x").unwrap();
            let update = tokio::time::timeout(READ_BOUND, follower.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(update, LogUpdate::Added(got));
        }
    })
    .await;
}

// 6. Clients that go away or misbehave.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_server_notices_a_subscriber_that_went_away() {
    bounded(async {
        let rig = rig().await;
        let events = rig.client.subscribe().await.unwrap();
        let mut raw = raw_connect(rig.path()).await;
        raw_hello(&mut raw, API_VERSION).await;
        write_frame(&mut raw, &Open::Subscribe, LOCAL_FRAME_MAX)
            .await
            .unwrap();
        assert_eq!(
            raw_read::<Reply>(&mut raw).await.unwrap(),
            Reply::Subscribed
        );
        rig.fake.wait_for_subscribers(2).await;
        drop(events);
        rig.fake.wait_for_subscribers(1).await;
        drop(raw);
        rig.fake.wait_for_subscribers(0).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_byte_from_a_subscriber_ends_its_subscription() {
    bounded(async {
        let rig = rig().await;
        let mut raw = raw_connect(rig.path()).await;
        raw_hello(&mut raw, API_VERSION).await;
        write_frame(&mut raw, &Open::Subscribe, LOCAL_FRAME_MAX)
            .await
            .unwrap();
        assert_eq!(
            raw_read::<Reply>(&mut raw).await.unwrap(),
            Reply::Subscribed
        );
        raw.write_all(b"?").await.unwrap();
        wait_closed(&mut raw).await;
        rig.fake.wait_for_subscribers(0).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_server_survives_garbage_silence_and_oversize_frames() {
    bounded(async {
        let rig = rig_with(IpcConfig {
            frame_timeout: Duration::from_millis(200),
            ..IpcConfig::default()
        })
        .await;
        // Garbage that is not a frame: "GET " reads as a length far over the cap.
        let mut raw = raw_connect(rig.path()).await;
        raw.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        wait_closed(&mut raw).await;
        // An oversize length.
        let mut raw = raw_connect(rig.path()).await;
        raw.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
        wait_closed(&mut raw).await;
        let mut raw = raw_connect(rig.path()).await;
        let over = u32::try_from(LOCAL_FRAME_MAX + 1).unwrap();
        raw.write_all(&over.to_be_bytes()).await.unwrap();
        wait_closed(&mut raw).await;
        // A hello that is a frame but not JSON: the server says which version it is, then closes.
        let mut raw = raw_connect(rig.path()).await;
        raw.write_all(&frame(b"\x00\x01garbage")).await.unwrap();
        let hello: HelloReply = raw_read(&mut raw).await.unwrap();
        assert_eq!(hello.api, API_VERSION);
        wait_closed(&mut raw).await;
        // Silence before the hello, and silence after it: closed at the read timeout.
        let mut raw = raw_connect(rig.path()).await;
        wait_closed(&mut raw).await;
        let mut raw = raw_connect(rig.path()).await;
        raw_hello(&mut raw, API_VERSION).await;
        wait_closed(&mut raw).await;
        // Half a frame, then silence.
        let mut raw = raw_connect(rig.path()).await;
        raw.write_all(&[0, 0, 0, 9, b'{']).await.unwrap();
        wait_closed(&mut raw).await;
        // Still serving.
        assert_eq!(rig.client.status().await.unwrap(), rig.fake.status());
        assert!(
            rig.fake
                .requests()
                .iter()
                .all(|r| matches!(r, Request::PeerAdd { .. } | Request::Status))
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn further_connections_wait_for_a_free_slot() {
    bounded(async {
        let rig = rig_with(IpcConfig {
            frame_timeout: Duration::from_secs(2),
            max_connections: 1,
        })
        .await;
        // A silent client holds the only slot until its frame timeout.
        let mut silent = raw_connect(rig.path()).await;
        let client = rig.client.clone();
        let mut call = tokio::spawn(async move { client.status().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(500), &mut call)
                .await
                .is_err(),
            "a second connection was served while the only slot was taken"
        );
        assert_eq!(call.await.unwrap().unwrap(), rig.fake.status());
        wait_closed(&mut silent).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_subscription_holds_its_slot_until_it_is_dropped() {
    bounded(async {
        let rig = rig_with(IpcConfig {
            frame_timeout: Duration::from_secs(600),
            max_connections: 1,
        })
        .await;
        let events = rig.client.subscribe().await.unwrap();
        let client = rig.client.clone();
        let mut call = tokio::spawn(async move { client.status().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(500), &mut call)
                .await
                .is_err(),
            "a call was served while a subscription held the only slot"
        );
        drop(events);
        assert_eq!(call.await.unwrap().unwrap(), rig.fake.status());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_leaves_while_its_subscription_is_opened_frees_the_station_and_its_slot() {
    bounded(async {
        let rig = rig_with(one_slot()).await;
        rig.fake.hold_subscribes(true);

        // A raw client that closes its connection while the station holds its subscribe.
        let raw = raw_open(rig.path(), &Open::Subscribe).await;
        rig.fake.wait_for_held_subscribes(1).await;
        drop(raw);
        tokio::time::timeout(READ_BOUND, rig.fake.wait_for_held_subscribes(0))
            .await
            .expect("the server kept waiting for a subscribe whose client is gone");
        assert_eq!(rig.client.status().await.unwrap(), rig.fake.status());

        // A `SocketTransport` whose call timeout fires while the station holds its subscribe.
        let t = SocketTransport::new(rig.path()).with_call_timeout(Duration::from_millis(200));
        assert_eq!(
            t.subscribe().await.unwrap_err(),
            ApiError::Timeout { after_ms: 200 }
        );
        tokio::time::timeout(READ_BOUND, rig.fake.wait_for_held_subscribes(0))
            .await
            .expect("the server kept waiting for a subscribe whose client timed out");
        // The only slot is free: the next client is served, by the same server.
        assert_eq!(rig.client.status().await.unwrap(), rig.fake.status());
        assert_eq!(rig.fake.subscriber_count(), 0);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_leaves_while_its_call_is_pending_frees_its_slot() {
    bounded(async {
        let (_dir, fake, probe, server, client) = pending_rig(one_slot()).await;

        // A raw client that closes its connection while the station works on its call.
        let raw = raw_open(server.path(), &pending_call()).await;
        probe
            .wait("the call reached the station", |c| c.started == 1)
            .await;
        drop(raw);
        probe
            .wait("the pending call was dropped", |c| c.cancelled == 1)
            .await;
        assert_eq!(client.status().await.unwrap(), fake.status());

        // A `SocketTransport` whose call timeout fires while the station works on its call.
        let t = SocketTransport::new(server.path()).with_call_timeout(Duration::from_millis(200));
        assert_eq!(
            t.call(Request::Message { seq: PENDING_SEQ })
                .await
                .unwrap_err(),
            ApiError::Timeout { after_ms: 200 }
        );
        probe
            .wait("the pending call was dropped", |c| c.cancelled == 2)
            .await;
        // The only slot is free: the next client is served, by the same server.
        assert_eq!(client.status().await.unwrap(), fake.status());
        assert_eq!(probe.counts().answered, 0);
        server.shutdown().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_half_closes_after_open_is_treated_as_gone() {
    bounded(async {
        let (_dir, fake, probe, server, client) = pending_rig(one_slot()).await;

        // A call: the server stops waiting and closes without an answer.
        let mut raw = raw_open(server.path(), &pending_call()).await;
        probe
            .wait("the call reached the station", |c| c.started == 1)
            .await;
        raw.shutdown().await.unwrap();
        probe
            .wait("the pending call was dropped", |c| c.cancelled == 1)
            .await;
        match raw_read::<Reply>(&mut raw).await {
            Err(FrameError::Closed | FrameError::Io(_)) => {}
            other => panic!("expected the end of the stream, got {other:?}"),
        }

        // A subscription that is being opened: the same.
        fake.hold_subscribes(true);
        let mut raw = raw_open(server.path(), &Open::Subscribe).await;
        fake.wait_for_held_subscribes(1).await;
        raw.shutdown().await.unwrap();
        tokio::time::timeout(READ_BOUND, fake.wait_for_held_subscribes(0))
            .await
            .expect("the server kept waiting for a subscribe whose client half-closed");
        wait_closed(&mut raw).await;
        assert_eq!(fake.subscriber_count(), 0);

        assert_eq!(client.status().await.unwrap(), fake.status());
        server.shutdown().await;
    })
    .await;
}

/// Waits until the server's write to `stream` is stuck on a full socket buffer: the bytes queued
/// for the client are above zero and stop growing (two equal readings 100 ms apart). Returns how
/// many are queued.
async fn wait_until_the_server_write_is_stuck(stream: &UnixStream) -> u64 {
    tokio::time::timeout(READ_BOUND, async {
        let mut last = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let queued = rustix::io::ioctl_fionread(stream).unwrap();
            if queued > 0 && queued == last {
                return queued;
            }
            last = queued;
        }
    })
    .await
    .expect("the server's write never got stuck")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_that_stops_reading_then_half_closes_is_treated_as_gone() {
    bounded(async {
        let rig = rig_with(one_slot()).await;
        // About 4 MiB of events of 16 KiB each: far more than the socket buffers hold (about
        // 208 KiB by default on Linux), and fewer than the subscription's queue, so no Resync.
        const EVENTS: usize = 250;
        let body = "x".repeat(BODY_MAX_BYTES);
        // First a half-close, then a full close as the positive control.
        for full_close in [false, true] {
            let mut raw = raw_open(rig.path(), &Open::Subscribe).await;
            assert_eq!(
                raw_read::<Reply>(&mut raw).await.unwrap(),
                Reply::Subscribed
            );
            receive_n(&rig.fake, EVENTS, &body);
            // One byte of the first event: forwarding has started.
            let mut byte = [0u8; 1];
            tokio::time::timeout(READ_BOUND, raw.read_exact(&mut byte))
                .await
                .unwrap()
                .unwrap();
            // The client reads no more; the server's write blocks with most events still to go.
            let queued = wait_until_the_server_write_is_stuck(&raw).await;
            assert!(
                usize::try_from(queued).unwrap() < EVENTS * BODY_MAX_BYTES / 2,
                "the socket buffered {queued} bytes: the write did not block"
            );
            let kept = if full_close {
                drop(raw);
                None
            } else {
                // Only the write half: the read half stays open and unread.
                raw.shutdown().await.unwrap();
                Some(raw)
            };
            tokio::time::timeout(READ_BOUND, rig.fake.wait_for_subscribers(0))
                .await
                .expect("the server kept the subscription of a client that left");
            // The only slot is free: the next client is served, by the same server.
            assert_eq!(rig.client.status().await.unwrap(), rig.fake.status());
            drop(kept);
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_stops_reading_its_reply_then_half_closes_is_treated_as_gone() {
    bounded(async {
        // The frame timeout is far beyond this test's bound: only noticing the half-close can
        // free the slot in time.
        let rig = rig_with(one_slot()).await;
        // A page of about 512 KiB: more than the socket buffers hold.
        receive_n(&rig.fake, 40, &"x".repeat(BODY_MAX_BYTES));
        let open = Open::Call {
            request: Request::Log {
                peer: None,
                before: None,
                limit: 100,
            },
        };
        let mut raw = raw_open(rig.path(), &open).await;
        let mut byte = [0u8; 1];
        tokio::time::timeout(READ_BOUND, raw.read_exact(&mut byte))
            .await
            .unwrap()
            .unwrap();
        let queued = wait_until_the_server_write_is_stuck(&raw).await;
        assert!(
            usize::try_from(queued).unwrap() < 40 * BODY_MAX_BYTES / 2,
            "the socket buffered {queued} bytes: the write did not block"
        );
        raw.shutdown().await.unwrap();
        // The only slot is free: the next client is served, by the same server.
        let client = Client::new(SocketTransport::new(rig.path()).with_call_timeout(READ_BOUND));
        assert_eq!(client.status().await.unwrap(), rig.fake.status());
        drop(raw);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_answer_reaches_a_client_that_waits() {
    bounded(async {
        let (_dir, _fake, probe, server, _client) = pending_rig(one_slot()).await;
        let t = SocketTransport::new(server.path()).with_call_timeout(READ_BOUND);
        let mut call =
            tokio::spawn(async move { t.call(Request::Message { seq: PENDING_SEQ }).await });
        probe
            .wait("the call reached the station", |c| c.started == 1)
            .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut call)
                .await
                .is_err(),
            "the call was answered before the station answered it"
        );
        probe.release();
        assert_eq!(
            call.await.unwrap().unwrap_err(),
            ApiError::MessageNotFound { seq: PENDING_SEQ }
        );
        assert_eq!(probe.counts().answered, 1);
        assert_eq!(probe.counts().cancelled, 0);
        server.shutdown().await;
    })
    .await;
}

// 7. Setup.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setup_makes_a_private_directory_and_a_0600_socket() {
    bounded(async {
        let rig = rig().await;
        let sock_dir = rig.dir.path().join("run/thecompany");
        assert_eq!(mode(&sock_dir), 0o700);
        assert_eq!(mode(&rig.dir.path().join("run")), 0o700);
        assert!(
            std::fs::symlink_metadata(rig.path())
                .unwrap()
                .file_type()
                .is_socket()
        );
        assert_eq!(mode(rig.path()), 0o600);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leftover_socket_is_replaced() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("station.sock");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
        assert!(
            std::fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_socket()
        );
        let fake = fake("STATION-A");
        let server = IpcServer::bind(fake.clone(), &path, IpcConfig::default())
            .await
            .unwrap();
        let c = Client::new(SocketTransport::new(&path));
        assert_eq!(c.status().await.unwrap(), fake.status());
        assert_eq!(mode(&path), 0o600);
        server.shutdown().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn something_else_at_the_path_is_refused_and_left_alone() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = dir.path().join("station.sock");
        std::fs::write(&path, b"keep me").unwrap();
        let err = IpcServer::bind(fake("STATION-A"), &path, IpcConfig::default())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, IpcError::NotASocket { path: p } if *p == path),
            "{err:?}"
        );
        assert!(err.to_string().contains("is not a socket"), "{err}");
        assert_eq!(std::fs::read(&path).unwrap(), b"keep me");

        // A symlink, even to a socket, is not a socket.
        let target = dir.path().join("elsewhere.sock");
        drop(std::os::unix::net::UnixListener::bind(&target).unwrap());
        let link = dir.path().join("link.sock");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let err = IpcServer::bind(fake("STATION-A"), &link, IpcConfig::default())
            .await
            .unwrap_err();
        assert!(matches!(err, IpcError::NotASocket { .. }), "{err:?}");
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_0755_socket_directory_is_refused() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let sock_dir = dir.path().join("thecompany");
        std::fs::create_dir(&sock_dir).unwrap();
        std::fs::set_permissions(&sock_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = sock_dir.join("station.sock");
        let err = IpcServer::bind(fake("STATION-A"), &path, IpcConfig::default())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, IpcError::Directory { path: p, .. } if *p == sock_dir),
            "{err:?}"
        );
        assert!(err.to_string().contains("0755"), "{err}");
        assert!(std::fs::symlink_metadata(&path).is_err());
        assert_eq!(mode(&sock_dir), 0o755);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_path_over_the_limit_is_refused_naming_data_dir() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let long = dir.path().join("d".repeat(120)).join("station.sock");
        let err = IpcServer::bind(fake("STATION-A"), &long, IpcConfig::default())
            .await
            .unwrap_err();
        assert!(matches!(err, IpcError::PathTooLong { .. }), "{err:?}");
        assert!(err.to_string().contains("--data-dir"), "{err}");
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_station_that_does_not_answer_its_status_is_not_served() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let station = fake("STATION-A");
        station.stop();
        let path = socket_in(&dir);
        let err = IpcServer::bind(station, &path, IpcConfig::default())
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                IpcError::Status {
                    error: ApiError::Stopping,
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_shutdown_the_socket_is_gone_and_clients_get_not_running() {
    bounded(async {
        let rig = rig().await;
        let path = rig.path().to_path_buf();
        let mut events = rig.client.subscribe().await.unwrap();
        rig.fake.wait_for_subscribers(1).await;
        rig.server.shutdown().await;
        assert!(std::fs::symlink_metadata(&path).is_err());
        // The open subscription ended (rule 11).
        assert_eq!(
            tokio::time::timeout(READ_BOUND, events.next())
                .await
                .unwrap(),
            None
        );
        rig.fake.wait_for_subscribers(0).await;
        assert_eq!(
            rig.client.status().await.unwrap_err(),
            ApiError::NotRunning {
                socket: path.display().to_string()
            }
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_server_stops_serving_removes_its_socket_and_frees_it() {
    bounded(async {
        let rig = rig().await;
        let path = rig.path().to_path_buf();
        // Idle connections: an open subscription, and a client that has not said hello.
        let mut events = rig.client.subscribe().await.unwrap();
        let mut silent = raw_connect(&path).await;
        drop(rig.server);
        // The teardown runs on the runtime after the drop; the subscription ending shows it did,
        // and the listener was closed before that.
        assert_eq!(
            tokio::time::timeout(READ_BOUND, events.next())
                .await
                .unwrap(),
            None
        );
        wait_closed(&mut silent).await;
        assert!(matches!(
            rig.client.status().await.unwrap_err(),
            ApiError::NotRunning { .. }
        ));
        // The dropped server removes its socket file itself (no new `bind` has run yet that could
        // have removed it as a leftover).
        tokio::time::timeout(READ_BOUND, async {
            while std::fs::symlink_metadata(&path).is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the dropped server left its socket file behind");
        // The lock is released right after; the task may still be on its way out: retry.
        let again = bind_when_free(&rig.fake, &path).await;
        assert!(rig.client.status().await.is_ok());
        again.shutdown().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_server_frees_its_socket_only_after_every_connection_has_finished() {
    bounded(async {
        let (_dir, fake, probe, server, client) = pending_rig(IpcConfig {
            frame_timeout: Duration::from_secs(600),
            ..IpcConfig::default()
        })
        .await;
        let path = server.path().to_path_buf();
        // The connection task of the pending call cannot finish until `release` is sent.
        let (release, held) = mpsc::channel();
        *probe
            .hold_drop
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(held);
        let mut raw = raw_open(&path, &pending_call()).await;
        probe
            .wait("the call reached the station", |c| c.started == 1)
            .await;

        drop(server);
        probe
            .wait("the server began to drop the pending call", |c| {
                c.dropping == 1
            })
            .await;
        // While that connection is still going down, the socket stays taken. The old server's
        // listener is closed and nothing else holds the lock up, so a released lock would show
        // within these attempts.
        for _ in 0..20 {
            match IpcServer::bind(fake.clone(), &path, IpcConfig::default()).await {
                Err(IpcError::InUse { .. }) => {}
                Ok(_) => panic!(
                    "a new server took the socket while a connection of the dropped one was still \
                     going down"
                ),
                Err(e) => panic!("{e}"),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(probe.counts().cancelled, 0);

        release.send(()).unwrap();
        probe
            .wait("the pending call was dropped", |c| c.cancelled == 1)
            .await;
        let again = bind_when_free(&fake, &path).await;
        assert_eq!(client.status().await.unwrap(), fake.status());
        wait_closed(&mut raw).await;
        assert_eq!(probe.counts().answered, 0);
        again.shutdown().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_servers_on_two_paths_serve_independently() {
    bounded(async {
        let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (fake_a, fake_b) = (fake("STATION-A"), fake("STATION-B"));
        let a = IpcServer::bind(fake_a.clone(), &socket_in(&dir_a), IpcConfig::default())
            .await
            .unwrap();
        let b = IpcServer::bind(fake_b.clone(), &socket_in(&dir_b), IpcConfig::default())
            .await
            .unwrap();
        let ca = Client::new(SocketTransport::new(a.path()));
        let cb = Client::new(SocketTransport::new(b.path()));
        assert_eq!(ca.status().await.unwrap().station_id, "STATION-A");
        assert_eq!(cb.status().await.unwrap().station_id, "STATION-B");
        ca.peer_add("bob", "STATION-B").await.unwrap();
        assert!(cb.peers().await.unwrap().is_empty());
        a.shutdown().await;
        assert!(matches!(
            ca.status().await.unwrap_err(),
            ApiError::NotRunning { .. }
        ));
        assert_eq!(cb.status().await.unwrap().station_id, "STATION-B");
        b.shutdown().await;
    })
    .await;
}

// 8. The transport on its own.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_server_is_not_running() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        for path in [
            dir.path().join("station.sock"),
            dir.path().join("missing/dir/station.sock"),
        ] {
            let t = SocketTransport::new(&path);
            let expected = ApiError::NotRunning {
                socket: path.display().to_string(),
            };
            assert_eq!(t.call(Request::Status).await.unwrap_err(), expected);
            assert_eq!(t.subscribe().await.unwrap_err(), expected);
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_that_never_answers_times_out() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("station.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (release, held) = tokio::sync::oneshot::channel::<()>();
        let silent = tokio::spawn(async move {
            let mut kept = Vec::new();
            let mut held = held;
            loop {
                tokio::select! {
                    _ = &mut held => return,
                    accepted = listener.accept() => kept.push(accepted.unwrap().0),
                }
            }
        });
        let t = SocketTransport::new(&path).with_call_timeout(Duration::from_millis(200));
        assert_eq!(t.call_timeout(), Duration::from_millis(200));
        assert_eq!(
            t.call(Request::Status).await.unwrap_err(),
            ApiError::Timeout { after_ms: 200 }
        );
        assert_eq!(
            t.subscribe().await.unwrap_err(),
            ApiError::Timeout { after_ms: 200 }
        );
        release.send(()).unwrap();
        silent.await.unwrap();
    })
    .await;
}

// The socket lock (one server per socket).

fn lock_path(socket: &Path) -> PathBuf {
    let mut name = socket.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

fn identity(path: &Path) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::symlink_metadata(path).unwrap();
    (m.dev(), m.ino())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_server_on_the_same_socket_is_refused_and_takes_nothing() {
    bounded(async {
        let rig = rig().await;
        let before = identity(rig.path());
        let err = IpcServer::bind(fake("STATION-B"), rig.path(), IpcConfig::default())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, IpcError::InUse { path } if path == rig.path()),
            "{err:?}"
        );
        let text = err.to_string();
        assert!(
            text.contains("already serving") && text.contains("--data-dir"),
            "{text}"
        );
        // The live socket is untouched and still served by the first server.
        assert_eq!(identity(rig.path()), before);
        assert_eq!(rig.client.status().await.unwrap().station_id, "STATION-A");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_shutdown_a_new_server_takes_the_socket_and_the_lock_file_stays() {
    bounded(async {
        let rig = rig().await;
        let path = rig.path().to_path_buf();
        rig.server.shutdown().await;
        assert!(std::fs::symlink_metadata(&path).is_err());
        assert!(std::fs::symlink_metadata(lock_path(&path)).is_ok());
        let second = fake("STATION-B");
        let again = IpcServer::bind(second, &path, IpcConfig::default())
            .await
            .unwrap();
        assert_eq!(rig.client.status().await.unwrap().station_id, "STATION-B");
        again.shutdown().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_lock_file_is_0600_and_a_symlink_there_is_refused() {
    bounded(async {
        let rig = rig().await;
        let lock = lock_path(rig.path());
        assert!(
            std::fs::symlink_metadata(&lock)
                .unwrap()
                .file_type()
                .is_file()
        );
        assert_eq!(mode(&lock), 0o600);

        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = dir.path().join("station.sock");
        let target = dir.path().join("target");
        std::fs::write(&target, b"keep me").unwrap();
        std::os::unix::fs::symlink(&target, lock_path(&path)).unwrap();
        let err = IpcServer::bind(fake("STATION-A"), &path, IpcConfig::default())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, IpcError::Lock { path: p, .. } if *p == lock_path(&path)),
            "{err:?}"
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"keep me");
        assert!(std::fs::symlink_metadata(&path).is_err());
    })
    .await;
}

// Deadlines on every frame of a call.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_does_not_read_its_reply_loses_its_slot() {
    bounded(async {
        let rig = rig_with(IpcConfig {
            frame_timeout: Duration::from_millis(300),
            max_connections: 1,
        })
        .await;
        // A page of about 512 KiB: more than the socket buffers hold.
        receive_n(&rig.fake, 40, &"x".repeat(BODY_MAX_BYTES));
        let mut stuck = raw_connect(rig.path()).await;
        raw_hello(&mut stuck, API_VERSION).await;
        let open = Open::Call {
            request: Request::Log {
                peer: None,
                before: None,
                limit: 100,
            },
        };
        write_frame(&mut stuck, &open, LOCAL_FRAME_MAX)
            .await
            .unwrap();
        // The stuck client never reads; the server gives up on it and serves the next one.
        let client = Client::new(SocketTransport::new(rig.path()).with_call_timeout(READ_BOUND));
        assert_eq!(client.status().await.unwrap(), rig.fake.status());
        wait_closed(&mut stuck).await;
    })
    .await;
}

// Rules 3 and 11 over the socket.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_resolves_only_after_the_station_registered_it() {
    bounded(async {
        let rig = rig().await;
        rig.fake.hold_subscribes(true);
        let client = rig.client.clone();
        let mut subscribe = tokio::spawn(async move { client.subscribe().await });
        rig.fake.wait_for_held_subscribes(1).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut subscribe)
                .await
                .is_err(),
            "subscribe resolved before the station registered the subscription"
        );
        assert_eq!(rig.fake.subscriber_count(), 0);
        rig.fake.hold_subscribes(false);
        let mut events = subscribe.await.unwrap().unwrap();
        assert_eq!(rig.fake.subscriber_count(), 1);
        let got = rig.fake.receive("bob", "x").unwrap();
        assert_eq!(
            next_event(&mut events).await,
            Event::Message { message: got }
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_station_delivers_the_queued_events_then_ends_the_subscription() {
    bounded(async {
        let rig = rig().await;
        let mut events = rig.client.subscribe().await.unwrap();
        let sent: Vec<Event> = (0..3)
            .map(|_| Event::Message {
                message: rig.fake.receive("bob", "x").unwrap(),
            })
            .collect();
        rig.fake.stop();
        for event in sent {
            assert_eq!(next_event(&mut events).await, event);
        }
        for _ in 0..2 {
            assert_eq!(
                tokio::time::timeout(READ_BOUND, events.next())
                    .await
                    .unwrap(),
                None
            );
        }
    })
    .await;
}

// The transport's own errors, against servers that misbehave on purpose.

/// Accepts one connection, answers the hello with this build's version, reads the `Open` frame,
/// and hands the stream to `then`.
fn serve_once<F, Fut>(listener: UnixListener, then: F) -> tokio::task::JoinHandle<()>
where
    F: FnOnce(UnixStream, Open) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _: Hello = raw_read(&mut stream).await.unwrap();
        let reply = HelloReply {
            api: API_VERSION,
            station_id: "STATION-A".into(),
            instance_id: "INSTANCE-1".into(),
        };
        write_frame(&mut stream, &reply, LOCAL_FRAME_MAX)
            .await
            .unwrap();
        let open: Open = raw_read(&mut stream).await.unwrap();
        then(stream, open).await;
    })
}

fn raw_listener() -> (TempDir, PathBuf, UnixListener) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("station.sock");
    let listener = UnixListener::bind(&path).unwrap();
    (dir, path, listener)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_that_closes_in_the_middle_of_a_call_is_disconnected() {
    bounded(async {
        let (_dir, path, listener) = raw_listener();
        let server = serve_once(listener, |stream, _| async move { drop(stream) });
        let err = SocketTransport::new(&path)
            .call(Request::Status)
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::Disconnected { .. }), "{err:?}");
        server.await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replies_that_do_not_fit_are_bad_replies_and_never_quoted() {
    bounded(async {
        // An undecodable reply holding a marker.
        let (_dir, path, listener) = raw_listener();
        let server = serve_once(listener, |mut stream, _| async move {
            let body = br#"{"type":"ok","response":{"type":"message","message":"MARKER-SECRET"}}"#;
            stream.write_all(&frame(body)).await.unwrap();
        });
        let err = SocketTransport::new(&path)
            .call(Request::Status)
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::BadReply { .. }), "{err:?}");
        assert!(!format!("{err:?}").contains("MARKER"), "{err:?}");
        assert!(!err.to_string().contains("MARKER"), "{err}");
        server.await.unwrap();

        // `Subscribed` to a call.
        let (_dir, path, listener) = raw_listener();
        let server = serve_once(listener, |mut stream, _| async move {
            write_frame(&mut stream, &Reply::Subscribed, LOCAL_FRAME_MAX)
                .await
                .unwrap();
        });
        let err = SocketTransport::new(&path)
            .call(Request::Status)
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::BadReply { .. }), "{err:?}");
        server.await.unwrap();

        // `Ok` to a subscribe: refused without printing the response.
        let (_dir, path, listener) = raw_listener();
        let server = serve_once(listener, |mut stream, open| async move {
            assert_eq!(open, Open::Subscribe);
            let reply = Reply::Ok {
                response: Response::Peers {
                    peers: vec![Peer {
                        name: "MARKER".into(),
                        stations: vec![],
                    }],
                },
            };
            write_frame(&mut stream, &reply, LOCAL_FRAME_MAX)
                .await
                .unwrap();
        });
        let err = SocketTransport::new(&path).subscribe().await.unwrap_err();
        let ApiError::BadReply { detail } = &err else {
            panic!("expected BadReply, got {err:?}");
        };
        assert!(detail.contains("as if it were a call"), "{detail}");
        assert!(!format!("{err:?}").contains("MARKER"), "{err:?}");
        server.await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bad_event_frame_ends_the_subscription() {
    bounded(async {
        let (_dir, path, listener) = raw_listener();
        let (done, finished) = tokio::sync::oneshot::channel::<()>();
        let server = serve_once(listener, |mut stream, _| async move {
            write_frame(&mut stream, &Reply::Subscribed, LOCAL_FRAME_MAX)
                .await
                .unwrap();
            write_frame(&mut stream, &Event::Resync, LOCAL_FRAME_MAX)
                .await
                .unwrap();
            stream
                .write_all(&frame(b"{\"type\":\"nope\"}"))
                .await
                .unwrap();
            // Keep the connection open: only the bad frame can end the subscription.
            let _ = finished.await;
        });
        let mut events = SocketTransport::new(&path).subscribe().await.unwrap();
        assert_eq!(next_event(&mut events).await, Event::Resync);
        assert_eq!(
            tokio::time::timeout(READ_BOUND, events.next())
                .await
                .unwrap(),
            None
        );
        done.send(()).unwrap();
        server.await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_too_large_for_a_frame_is_refused_before_connecting() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        // Nothing listens here: an attempt to connect would give NotRunning.
        let t = SocketTransport::new(dir.path().join("station.sock"));
        let body = "MARKER".repeat(LOCAL_FRAME_MAX / 6 + 1);
        let err = t
            .call(Request::Send {
                peer: "bob".into(),
                body,
            })
            .await
            .unwrap_err();
        let ApiError::BadRequest { detail } = &err else {
            panic!("expected BadRequest, got {err:?}");
        };
        assert!(detail.contains("limit"), "{detail}");
        assert!(!detail.contains("MARKER"), "{detail}");
    })
    .await;
}

// Not covered here: the uid checks on both ends (the server closing a connection from another
// uid, the client refusing a socket served by another uid). Exercising them needs a second user
// on the machine; they are not faked.
