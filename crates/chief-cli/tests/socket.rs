#![cfg(test)]
#![cfg(unix)]
//! `chief` commands through the real local socket, as the binary runs them: the socket server
//! (`numbers_station::ipc::IpcServer`) serves a `chief::fake::FakeStation` in a temporary
//! directory, and the commands use `SocketTransport`. Also every command with no station behind
//! the socket.
//!
//! Nothing sleeps, every test is bounded, ids are placeholders (AGENTS.md, hard rule 9).

use chief::api::FailReason;
use chief::client::{Client, SocketTransport};
use chief::fake::{FakeStation, SendOutcome};
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
use tokio::io::BufReader;
use tokio::sync::Notify;

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

async fn exec(client: &Client<SocketTransport>, args: &[&str]) -> (Exit, String, String) {
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
