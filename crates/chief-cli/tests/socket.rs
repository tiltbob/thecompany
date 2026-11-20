#![cfg(test)]
#![cfg(unix)]
//! `chief` commands through the real local socket, as the binary runs them: the socket server
//! (`numbers_station::ipc::IpcServer`) serves a `chief::fake::FakeStation` in a temporary
//! directory, and the commands use `SocketTransport`. Also every command with no station behind
//! the socket.
//!
//! Nothing sleeps, every test is bounded, ids are placeholders (AGENTS.md, hard rule 9).

use chief::api::{
    ApiError, FailReason, Link, Request, Response, TowerSource, TowerState, TowerStatus,
};
use chief::client::{
    BindInstance, Client, SUBSCRIPTION_CAPACITY, SocketTransport, Subscription, Transport,
    subscription,
};
use chief::fake::{FAKE_TOWER_URL, FakeStation, SendOutcome};
use chief_cli::{Cli, Command, Exit, run};
use clap::Parser;
use numbers_station::ipc::{IpcConfig, IpcServer};
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::sync::{Notify, watch};

const BOUND: Duration = Duration::from_secs(30);

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(BOUND, test)
        .await
        .expect("the test did not finish in time")
}

fn parse(args: &[&str]) -> Command {
    Cli::try_parse_from(std::iter::once("chief").chain(args.iter().copied()))
        .unwrap()
        .command
}

async fn exec<T: BindInstance>(client: &Client<T>, args: &[&str]) -> (Exit, String, String) {
    let command = parse(args);
    let mut out = Vec::new();
    let mut err = Vec::new();
    let exit = run(&command, client, tokio::io::empty(), &mut out, &mut err).await;
    (
        exit,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

struct Rig {
    _dir: TempDir,
    fake: FakeStation,
    server: IpcServer,
    client: Client<SocketTransport>,
}

/// A socket server in a temporary directory for a fake station with the peer `bob`, and a client.
async fn rig() -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let fake = FakeStation::new("STATION-A", "INSTANCE-1");
    Client::new(fake.clone())
        .peer_add("bob", "STATION-B")
        .await
        .unwrap();
    let socket = dir.path().join("run/station.sock");
    let server = IpcServer::bind(fake.clone(), &socket, IpcConfig::default())
        .await
        .unwrap();
    let client = Client::new(SocketTransport::new(server.path()));
    Rig {
        _dir: dir,
        fake,
        server,
        client,
    }
}

#[tokio::test]
async fn id_through_the_socket() {
    bounded(async {
        let rig = rig().await;
        assert_eq!(
            exec(&rig.client, &["id"]).await,
            (Exit::Done, "STATION-A\n".to_owned(), String::new())
        );
        rig.server.shutdown().await;
    })
    .await;
}

#[tokio::test]
async fn send_and_log_through_the_socket() {
    bounded(async {
        let rig = rig().await;
        // 2023-11-14T22:13:20Z for every message.
        rig.fake.set_now_ms(1_700_000_000_000);
        assert_eq!(
            exec(&rig.client, &["send", "bob", "on my way"]).await,
            (Exit::Done, "delivered to bob\n".to_owned(), String::new())
        );
        rig.fake
            .set_outcome(SendOutcome::Fail(FailReason::NoAnswer));
        assert_eq!(
            exec(&rig.client, &["send", "bob", "are you there"]).await,
            (
                Exit::Failed,
                String::new(),
                "bob appears offline, or has not added you\n".to_owned()
            )
        );
        assert_eq!(
            exec(&rig.client, &["send", "carol", "hi"]).await,
            (
                Exit::Failed,
                String::new(),
                "no peer named carol\n".to_owned()
            )
        );
        rig.fake.receive("bob", "see you").unwrap();
        assert_eq!(
            exec(&rig.client, &["log"]).await,
            (
                Exit::Done,
                "1 2023-11-14T22:13:20Z -> bob (delivered) on my way\n\
                 2 2023-11-14T22:13:20Z -> bob (failed: no answer) are you there\n\
                 3 2023-11-14T22:13:20Z <- bob see you\n"
                    .to_owned(),
                String::new()
            )
        );
        rig.server.shutdown().await;
    })
    .await;
}

/// A writer the test can read while `run` holds it, and wait on.
#[derive(Clone, Default)]
struct Shared {
    buf: Arc<Mutex<Vec<u8>>>,
    written: Arc<Notify>,
}

impl Write for Shared {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.lock().unwrap().extend_from_slice(data);
        self.written.notify_waiters();
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Shared {
    fn text(&self) -> String {
        String::from_utf8(self.buf.lock().unwrap().clone()).unwrap()
    }

    async fn wait_for(&self, done: impl Fn(&str) -> bool) {
        loop {
            let mut written = pin!(self.written.notified());
            written.as_mut().enable();
            if done(&self.text()) {
                return;
            }
            written.await;
        }
    }
}

#[tokio::test]
async fn chat_through_the_socket() {
    bounded(async {
        let rig = rig().await;
        rig.fake.set_now_ms(1_700_000_000_000);
        rig.fake.receive("bob", "earlier").unwrap();
        let command = parse(&["chat", "bob"]);
        let (input, reader) = tokio::io::duplex(1024);
        let out = Shared::default();
        let err = Shared::default();
        let (mut o, mut e) = (out.clone(), err.clone());
        let session = run(
            &command,
            &rig.client,
            BufReader::new(reader),
            &mut o,
            &mut e,
        );
        let drive = async {
            out.wait_for(|s| s.ends_with("<- bob earlier\n")).await;
            rig.fake.set_now_ms(1_700_000_001_000);
            rig.fake.receive("bob", "live").unwrap();
            out.wait_for(|s| s.ends_with("<- bob live\n")).await;
            drop(input);
        };
        let (exit, ()) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::Done);
        assert_eq!(
            out.text(),
            "1 2023-11-14T22:13:20Z <- bob earlier\n2 2023-11-14T22:13:21Z <- bob live\n"
        );
        assert_eq!(err.text(), "");

        // The server going away ends a chat with exit 3.
        let (input, reader) = tokio::io::duplex(1024);
        let out = Shared::default();
        let err = Shared::default();
        let (mut o, mut e) = (out.clone(), err.clone());
        let session = run(
            &command,
            &rig.client,
            BufReader::new(reader),
            &mut o,
            &mut e,
        );
        let drive = async {
            out.wait_for(|s| s.lines().count() == 2).await;
            rig.server.shutdown().await;
            input
        };
        let (exit, _input) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::NoStation);
        assert_eq!(err.text(), "the station stopped; the chat has ended\n");
    })
    .await;
}

#[tokio::test]
async fn a_chat_skips_a_line_that_is_not_utf8_through_the_socket() {
    bounded(async {
        let rig = rig().await;
        let command = parse(&["chat", "bob"]);
        let mut out = Vec::new();
        let mut err = Vec::new();
        let input: &[u8] = b"one\nnot \xff utf-8\ntwo\n";
        let exit = run(&command, &rig.client, input, &mut out, &mut err).await;
        assert_eq!(exit, Exit::Failed);
        assert_eq!(
            String::from_utf8(err).unwrap(),
            "a line is not valid UTF-8 and was not sent\n"
        );
        let bodies: Vec<String> = rig
            .fake
            .requests()
            .into_iter()
            .filter_map(|r| match r {
                Request::Send { body, .. } => Some(body),
                _ => None,
            })
            .collect();
        assert_eq!(bodies, ["one", "two"]);
        rig.server.shutdown().await;
    })
    .await;
}

#[tokio::test]
async fn status_and_the_peer_book_through_the_socket() {
    bounded(async {
        let rig = rig().await;
        rig.fake.set_tower_state(TowerState::Connected);
        rig.fake
            .set_link("bob", "STATION-B", Link::Relayed)
            .unwrap();
        assert_eq!(
            exec(&rig.client, &["status"]).await,
            (
                Exit::Done,
                format!("tower {FAKE_TOWER_URL} connected\npeer bob STATION-B relayed\n"),
                String::new()
            )
        );
        assert_eq!(
            exec(&rig.client, &["peer", "add", "carol", "STATION-C"]).await,
            (
                Exit::Done,
                "carol STATION-C down\n".to_owned(),
                String::new()
            )
        );
        assert_eq!(
            exec(&rig.client, &["peer", "ls"]).await,
            (
                Exit::Done,
                "bob STATION-B relayed\ncarol STATION-C down\n".to_owned(),
                String::new()
            )
        );
        assert_eq!(
            exec(&rig.client, &["peer", "rm", "carol"]).await,
            (Exit::Done, "removed carol\n".to_owned(), String::new())
        );
        assert_eq!(
            exec(&rig.client, &["peer", "ls"]).await,
            (
                Exit::Done,
                "bob STATION-B relayed\n".to_owned(),
                String::new()
            )
        );
        rig.server.shutdown().await;
    })
    .await;
}

#[tokio::test]
async fn status_shows_every_tower_state_and_source_through_the_socket() {
    bounded(async {
        let rig = rig().await;
        let tower = |url: &str, state, source| TowerStatus {
            url: url.to_owned(),
            state,
            source,
        };
        rig.fake.set_towers(vec![
            tower(
                "https://tower-a.example.invalid/",
                TowerState::Connected,
                Some(TowerSource::Srv),
            ),
            tower(
                "https://tower-b.example.invalid/",
                TowerState::Standby,
                Some(TowerSource::Remembered),
            ),
            tower(
                "https://tower-c.example.invalid/",
                TowerState::Failing {
                    error: "connection refused".to_owned(),
                },
                Some(TowerSource::Configured),
            ),
            tower(
                "https://tower-d.example.invalid/",
                TowerState::Denied {
                    reason: "not allowed".to_owned(),
                },
                Some(TowerSource::Peer),
            ),
            tower(FAKE_TOWER_URL, TowerState::Connecting, None),
        ]);
        rig.fake
            .set_bootstrap_domain(Some("team.example.invalid".to_owned()));
        assert_eq!(
            exec(&rig.client, &["status"]).await,
            (
                Exit::Done,
                "domain team.example.invalid\n\
                 tower https://tower-a.example.invalid/ connected (srv)\n\
                 tower https://tower-b.example.invalid/ standby (remembered)\n\
                 tower https://tower-c.example.invalid/ failing (configured): connection refused\n\
                 tower https://tower-d.example.invalid/ denied (peer): this station is not on \
                 this tower's list (not allowed)\n\
                 tower http://127.0.0.1:3340/ connecting\n\
                 peer bob STATION-B down\n"
                    .to_owned(),
                String::new()
            )
        );
        rig.server.shutdown().await;
    })
    .await;
}

// A chat never sends to another instance that took the socket (finding 1 of the review of #51).

/// Another start of the station: instance `INSTANCE-2`, with a peer `bob` of its own.
async fn other_instance() -> FakeStation {
    let fake = FakeStation::new("STATION-A", "INSTANCE-2");
    Client::new(fake.clone())
        .peer_add("bob", "STATION-OTHER-B")
        .await
        .unwrap();
    fake
}

fn sends(fake: &FakeStation) -> usize {
    fake.requests()
        .iter()
        .filter(|r| matches!(r, Request::Send { .. }))
        .count()
}

/// A socket transport whose subscriptions end late: after the station's end of a subscription,
/// the subscriber sees it only once the test sets `release`. So a test can let a typed line race
/// a subscription that has already ended underneath.
#[derive(Clone)]
struct LateEnd {
    inner: SocketTransport,
    release: watch::Receiver<bool>,
}

impl Transport for LateEnd {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        self.inner.call(request).await
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        let mut inner = self.inner.subscribe().await?;
        let (sink, events) = subscription(SUBSCRIPTION_CAPACITY);
        let mut release = self.release.clone();
        tokio::spawn(async move {
            while let Some(event) = inner.next().await {
                if sink.push(event).is_err() {
                    return;
                }
            }
            let _ = release.wait_for(|r| *r).await;
            drop(sink);
        });
        Ok(events)
    }
}

impl BindInstance for LateEnd {
    fn bound_to(&self, instance_id: &str) -> Self {
        Self {
            inner: self.inner.bound_to(instance_id),
            release: self.release.clone(),
        }
    }
}

#[tokio::test]
async fn a_chat_never_sends_to_another_instance_that_took_the_socket() {
    bounded(async {
        let rig = rig().await;
        let path = rig.server.path().to_path_buf();
        rig.fake.receive("bob", "from one").unwrap();
        let (release, released) = watch::channel(false);
        let client = Client::new(LateEnd {
            inner: SocketTransport::new(&path),
            release: released,
        });
        let command = parse(&["chat", "bob"]);
        let (mut input, reader) = tokio::io::duplex(1024);
        let out = Shared::default();
        let err = Shared::default();
        let (mut o, mut e) = (out.clone(), err.clone());
        let session = run(&command, &client, BufReader::new(reader), &mut o, &mut e);
        let second = other_instance().await;
        let drive = async {
            out.wait_for(|s| s.ends_with("<- bob from one\n")).await;
            // The first instance goes away; the chat does not see its subscription end yet.
            rig.server.shutdown().await;
            let server = IpcServer::bind(second.clone(), &path, IpcConfig::default())
                .await
                .unwrap();
            input.write_all(b"must not arrive\n").await.unwrap();
            // With the fix the chat ends here. Without it, the line goes to the second instance
            // and the chat goes on until its subscription's end is let through.
            let _ =
                tokio::time::timeout(Duration::from_secs(5), err.wait_for(|s| !s.is_empty())).await;
            let _ = release.send(true);
            (input, server)
        };
        let (exit, (_input, server)) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::NoStation);
        // The binding refuses the send with `Disconnected`, which the chat cannot tell from a
        // lost answer.
        assert_eq!(
            err.text(),
            "the station stopped; the chat has ended\n\
             the last line may have been sent; check `chief log bob`\n"
        );
        assert_eq!(sends(&second), 0);
        assert!(second.messages().is_empty());
        assert_eq!(out.text().lines().count(), 1);
        server.shutdown().await;
    })
    .await;
}

/// The end wins: a line typed as the instance ends is not sent, and the chat ends once.
///
/// The line and the end of the subscription are both ready when the chat is polled again. The
/// biased `select!` takes the end of the live view first, and the chat drops the line's send
/// before it has reached any station (in every run observed; the test accepts either way the
/// chat may then describe the line). It does not prove that sends are bound: the line never
/// reaches the next instance whether or not they are. That is
/// `a_chat_never_sends_to_another_instance_that_took_the_socket`, which holds the end of the
/// subscription back until the send has run.
#[tokio::test]
async fn the_end_of_the_instance_wins_over_a_line_typed_as_it_ends() {
    bounded(async {
        let rig = rig().await;
        let path = rig.server.path().to_path_buf();
        rig.fake.receive("bob", "from one").unwrap();
        let client = Client::new(SocketTransport::new(&path));
        let command = parse(&["chat", "bob"]);
        let (mut input, reader) = tokio::io::duplex(1024);
        let out = Shared::default();
        let err = Shared::default();
        let (mut o, mut e) = (out.clone(), err.clone());
        let session = run(&command, &client, BufReader::new(reader), &mut o, &mut e);
        let mut session = pin!(session);
        tokio::select! {
            exit = &mut session => panic!("the chat ended early: {exit:?}"),
            () = out.wait_for(|s| s.ends_with("<- bob from one\n")) => {}
        }
        // The chat is idle and not polled now: the line waits to be read while the first
        // instance goes away and another takes the socket.
        input.write_all(b"must not arrive\n").await.unwrap();
        rig.server.shutdown().await;
        let second = other_instance().await;
        let server = IpcServer::bind(second.clone(), &path, IpcConfig::default())
            .await
            .unwrap();
        // Both the end of the subscription and the line are ready; whichever the chat takes
        // first, nothing reaches the second instance.
        let exit = session.await;
        assert_eq!(exit, Exit::NoStation);
        // The line was either sent off (the chat cannot know how far it got) or not yet.
        let stopped = "the station stopped; the chat has ended\n";
        assert!(
            [
                format!("{stopped}the last line may have been sent; check `chief log bob`\n"),
                format!("{stopped}1 typed line was not sent\n"),
            ]
            .contains(&err.text()),
            "{}",
            err.text()
        );
        assert_eq!(sends(&second), 0);
        assert!(second.messages().is_empty());
        drop(input);
        server.shutdown().await;
    })
    .await;
}

const EVERY_COMMAND: [&[&str]; 9] = [
    &["id"],
    &["status"],
    &["peer", "add", "bob", "STATION-B"],
    &["peer", "ls"],
    &["peer", "rm", "bob"],
    &["send", "bob", "hi"],
    &["log"],
    &["log", "bob"],
    &["chat", "bob"],
];

async fn assert_no_station(path: &Path) {
    let client = Client::new(SocketTransport::new(path));
    let message = format!(
        "no station is running (socket: {})\nstart one with: station run --tower <URL>\n",
        path.display()
    );
    for args in EVERY_COMMAND {
        assert_eq!(
            exec(&client, args).await,
            (Exit::NoStation, String::new(), message.clone()),
            "{args:?} on {}",
            path.display()
        );
    }
}

#[tokio::test]
async fn every_command_without_a_station() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        // A directory with no socket in it.
        assert_no_station(&dir.path().join("station.sock")).await;
        // A directory that does not exist.
        assert_no_station(&dir.path().join("missing/station.sock")).await;
        // A socket file left behind, with nothing listening on it.
        let stale: PathBuf = dir.path().join("stale.sock");
        drop(std::os::unix::net::UnixListener::bind(&stale).unwrap());
        assert!(stale.exists());
        assert_no_station(&stale).await;
    })
    .await;
}
