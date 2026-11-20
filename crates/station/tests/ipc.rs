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
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

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
        let rig = rig().await;
        let mut stream = raw_connect(rig.path()).await;
        let hello = raw_hello(&mut stream, API_VERSION + 1).await;
        assert_eq!(hello.api, API_VERSION);
        assert_eq!(hello.station_id, "STATION-A");
        wait_closed(&mut stream).await;
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
            read_timeout: Duration::from_millis(200),
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
            read_timeout: Duration::from_millis(300),
            max_connections: 1,
        })
        .await;
        // A silent client holds the only slot until its read timeout.
        let mut silent = raw_connect(rig.path()).await;
        let status = rig.client.status().await.unwrap();
        assert_eq!(status, rig.fake.status());
        wait_closed(&mut silent).await;
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
async fn a_dropped_server_stops_serving_and_its_socket_is_replaced_later() {
    bounded(async {
        let rig = rig().await;
        let path = rig.path().to_path_buf();
        let mut events = rig.client.subscribe().await.unwrap();
        drop(rig.server);
        // Aborting happens on the runtime after the drop; the subscription ending shows it did,
        // and the listener was closed before that.
        assert_eq!(
            tokio::time::timeout(READ_BOUND, events.next())
                .await
                .unwrap(),
            None
        );
        assert!(std::fs::symlink_metadata(&path).is_ok());
        assert!(matches!(
            rig.client.status().await.unwrap_err(),
            ApiError::NotRunning { .. }
        ));
        let again = IpcServer::bind(rig.fake.clone(), &path, IpcConfig::default())
            .await
            .unwrap();
        assert!(rig.client.status().await.is_ok());
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
