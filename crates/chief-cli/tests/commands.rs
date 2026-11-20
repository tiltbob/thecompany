#![cfg(test)]
//! Every `chief` command run in-process against `chief::fake::FakeStation` (and, for transport
//! errors, a transport that only fails): golden output on stdout and stderr, exit codes, argument
//! parsing, the time format and the control-character filter.
//!
//! Nothing sleeps: tests wait on the fake's subscribers and on output appearing. Every async test
//! is bounded. Ids are placeholders (AGENTS.md, hard rule 9).

use chief::api::*;
use chief::client::{BindInstance, Client, SUBSCRIPTION_CAPACITY, Subscription, Transport};
use chief::fake::{FAKE_LOG_CAPACITY, FAKE_TOWER_URL, FakeStation, SendOutcome};
use chief::paths::PathEnv;
use chief_cli::{
    AFTER_HELP, Cli, Command, Exit, LOG_MAX_MESSAGES, PeerCommand, format_change, format_message,
    format_time, run, say, socket_path,
};
use clap::Parser;
use clap::error::ErrorKind;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWriteExt, BufReader, DuplexStream, ReadBuf};
use tokio::sync::Notify;

const BOUND: Duration = Duration::from_secs(30);

/// 2023-11-14T22:13:20Z, the fixed clock of the tests that print times.
const T0: u64 = 1_700_000_000_000;
/// 2023-11-14T22:13:00Z.
const MINUTE: u64 = T0 - 20_000;

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(BOUND, test)
        .await
        .expect("the test did not finish in time")
}

/// A fake station `STATION-A` with the peers `alice` and `bob`.
async fn fake_with_peers() -> FakeStation {
    let fake = FakeStation::new("STATION-A", "INSTANCE-1");
    let c = Client::new(fake.clone());
    c.peer_add("alice", "STATION-AL").await.unwrap();
    c.peer_add("bob", "STATION-B").await.unwrap();
    fake
}

fn parse(args: &[&str]) -> Command {
    Cli::try_parse_from(std::iter::once("chief").chain(args.iter().copied()))
        .unwrap()
        .command
}

/// Runs `chief <args>` against `client` with no input: (exit, stdout, stderr).
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

    /// Waits until the text written so far satisfies `done`.
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

/// Starts `chief chat <peer>` with a pipe for its input: the future, its stdout, its stderr and the
/// pipe's writing end (drop it for end of input).
fn chat<'a, T: BindInstance>(
    client: &'a Client<T>,
    command: &'a Command,
) -> (
    impl Future<Output = Exit> + 'a,
    Shared,
    Shared,
    DuplexStream,
) {
    let (writer, reader) = tokio::io::duplex(4096);
    let out = Shared::default();
    let err = Shared::default();
    let (mut o, mut e) = (out.clone(), err.clone());
    let fut = async move { run(command, client, BufReader::new(reader), &mut o, &mut e).await };
    (fut, out, err, writer)
}

fn line(m: &Message) -> String {
    format_message(m)
}

fn message(seq: u64, direction: Direction, body: &str, status: MessageStatus) -> Message {
    Message {
        seq,
        rev: 0,
        id: "FAKE-MSG-1".to_owned(),
        peer: "bob".to_owned(),
        direction,
        body: body.to_owned(),
        at_ms: 1_700_000_000_000,
        status,
    }
}

// Formatting.

#[test]
fn time_format() {
    assert_eq!(format_time(0), "1970-01-01T00:00:00Z");
    assert_eq!(format_time(999), "1970-01-01T00:00:00Z");
    // The last millisecond of a day.
    assert_eq!(format_time(86_399_999), "1970-01-01T23:59:59Z");
    assert_eq!(format_time(86_400_000), "1970-01-02T00:00:00Z");
    // Leap days, one of them in a year divisible by 400.
    assert_eq!(format_time(1_709_164_800_000), "2024-02-29T00:00:00Z");
    assert_eq!(format_time(1_709_251_199_999), "2024-02-29T23:59:59Z");
    assert_eq!(format_time(1_709_251_200_000), "2024-03-01T00:00:00Z");
    assert_eq!(format_time(951_782_400_000), "2000-02-29T00:00:00Z");
    // Past 2038, where 32-bit seconds end.
    assert_eq!(format_time(2_147_483_648_000), "2038-01-19T03:14:08Z");
    assert_eq!(format_time(2_208_988_800_000), "2040-01-01T00:00:00Z");
    assert_eq!(format_time(4_102_444_799_000), "2099-12-31T23:59:59Z");
    assert_eq!(format_time(1_700_000_000_000), "2023-11-14T22:13:20Z");
    // The largest time does not overflow.
    assert!(format_time(u64::MAX).ends_with('Z'));
}

#[test]
fn message_lines_golden() {
    let out = |status| message(12, Direction::Outgoing, "on my way", status);
    assert_eq!(
        line(&out(MessageStatus::Delivered)),
        "12 2023-11-14T22:13:20Z -> bob (delivered) on my way"
    );
    assert_eq!(
        line(&out(MessageStatus::Sending)),
        "12 2023-11-14T22:13:20Z -> bob (sending) on my way"
    );
    let failed = |reason| MessageStatus::Failed { reason };
    assert_eq!(
        line(&out(failed(FailReason::NoAnswer))),
        "12 2023-11-14T22:13:20Z -> bob (failed: no answer) on my way"
    );
    assert_eq!(
        line(&out(failed(FailReason::Refused))),
        "12 2023-11-14T22:13:20Z -> bob (failed: refused) on my way"
    );
    assert_eq!(
        line(&out(failed(FailReason::PeerRemoved))),
        "12 2023-11-14T22:13:20Z -> bob (failed: peer removed) on my way"
    );
    // An incoming message has no status; a body that looks like one stays the body.
    let incoming = message(
        13,
        Direction::Incoming,
        "(delivered) see you",
        MessageStatus::Received,
    );
    assert_eq!(
        line(&incoming),
        "13 2023-11-14T22:13:20Z <- bob (delivered) see you"
    );
    assert_eq!(
        format_change(&out(MessageStatus::Delivered)),
        "12 -> bob now (delivered)"
    );
    assert_eq!(
        format_change(&out(failed(FailReason::NoAnswer))),
        "12 -> bob now (failed: no answer)"
    );
}

#[test]
fn say_replaces_every_control_character() {
    let mut out = Vec::new();
    say(&mut out, "a\x1b[2J\nb\tc\rd\u{7}e\u{9b}f").unwrap();
    let text = String::from_utf8(out).unwrap();
    assert_eq!(text, "a [2J b c d e f\n");
    // Unicode line and paragraph separators, and the bidirectional controls.
    let mut out = Vec::new();
    say(
        &mut out,
        "a\u{2028}b\u{2029}c\u{202A}d\u{202B}e\u{202C}f\u{202D}g\u{202E}h\u{2066}i\u{2067}j\
         \u{2068}k\u{2069}l",
    )
    .unwrap();
    let text = String::from_utf8(out).unwrap();
    assert_eq!(text, "a b c d e f g h i j k l\n");
    // Their neighbours stay.
    let mut out = Vec::new();
    say(&mut out, "\u{2027}\u{202F}\u{2065}\u{206A}").unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "\u{2027}\u{202F}\u{2065}\u{206A}\n"
    );
}

/// No control character but the newline at the end of each line.
fn assert_clean(text: &str) {
    for l in text.lines() {
        assert!(
            !l.chars().any(|c| c.is_control()
                || matches!(c, '\u{2028}'..='\u{202E}' | '\u{2066}'..='\u{2069}')),
            "control character in {l:?}"
        );
    }
    assert!(!text.contains('\r'));
}

#[tokio::test]
async fn station_text_cannot_reach_the_terminal_raw() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        // An outgoing body keeps its control characters in the station: it is what this chief
        // typed. The CLI still replaces them with spaces, like everything else it prints.
        fake.set_now_ms(T0);
        let evil =
            "\x1b[2J\x1b]0;owned\x07hello\nforged 9 line\u{2028}10 x\u{202E}desrever\u{2066}iso";
        let (exit, _, _) = exec(&c, &["send", "bob", evil]).await;
        assert_eq!(exit, Exit::Done);
        let (exit, out, err) = exec(&c, &["log"]).await;
        assert_eq!(exit, Exit::Done);
        assert_eq!(err, "");
        assert_eq!(out.lines().count(), 1, "{out:?}");
        assert_eq!(
            out,
            "1 2023-11-14T22:13:20Z -> bob (delivered)  [2J ]0;owned hello forged 9 line 10 x \
             desrever iso\n"
        );
        assert_clean(&out);

        fake.set_tower_state(TowerState::Failing {
            error: "bad\x1b[31m\nthing".to_owned(),
        });
        let (_, out, _) = exec(&c, &["status"]).await;
        assert!(out.starts_with(&format!("tower {FAKE_TOWER_URL} failing: bad [31m thing\n")));
        assert_clean(&out);

        // Error text from the station too.
        let broken = Client::new(Failing(ApiError::Internal {
            detail: "disk\x1b[2K\ngone".to_owned(),
        }));
        let (exit, out, err) = exec(&broken, &["id"]).await;
        assert_eq!(exit, Exit::Failed);
        assert_eq!(out, "");
        assert_eq!(err, "station error: disk [2K gone\n");
    })
    .await;
}

// id, status, peers.

#[tokio::test]
async fn id_prints_the_station_id_alone() {
    bounded(async {
        let c = Client::new(fake_with_peers().await);
        assert_eq!(
            exec(&c, &["id"]).await,
            (Exit::Done, "STATION-A\n".to_owned(), String::new())
        );
    })
    .await;
}

#[tokio::test]
async fn status_shows_each_tower_state_and_each_link() {
    bounded(async {
        let fake = FakeStation::new("STATION-A", "INSTANCE-1");
        let c = Client::new(fake.clone());
        assert_eq!(
            exec(&c, &["status"]).await,
            (
                Exit::Done,
                format!("tower {FAKE_TOWER_URL} connecting\nno peers\n"),
                String::new()
            )
        );

        c.peer_add("alice", "STATION-AL").await.unwrap();
        c.peer_add("bob", "STATION-B").await.unwrap();
        c.peer_add("bob", "STATION-B2").await.unwrap();
        fake.set_link("alice", "STATION-AL", Link::Direct).unwrap();
        fake.set_link("bob", "STATION-B", Link::Relayed).unwrap();
        let peers = "peer alice STATION-AL direct\n\
                     peer bob STATION-B relayed\n\
                     peer bob STATION-B2 down\n";
        for (state, text) in [
            (TowerState::Connected, "connected".to_owned()),
            (
                TowerState::Failing {
                    error: "connection refused".to_owned(),
                },
                "failing: connection refused".to_owned(),
            ),
            (
                TowerState::Denied {
                    reason: "not on the list".to_owned(),
                },
                "denied: not on the list".to_owned(),
            ),
            (TowerState::Connecting, "connecting".to_owned()),
        ] {
            fake.set_tower_state(state);
            assert_eq!(
                exec(&c, &["status"]).await,
                (
                    Exit::Done,
                    format!("tower {FAKE_TOWER_URL} {text}\n{peers}"),
                    String::new()
                )
            );
        }
    })
    .await;
}

#[tokio::test]
async fn peer_add_ls_rm() {
    bounded(async {
        let fake = FakeStation::new("STATION-A", "INSTANCE-1");
        let c = Client::new(fake.clone());
        assert_eq!(
            exec(&c, &["peer", "ls"]).await,
            (Exit::Done, "no peers\n".to_owned(), String::new())
        );
        assert_eq!(
            exec(&c, &["peer", "add", "bob", "STATION-B"]).await,
            (Exit::Done, "bob STATION-B down\n".to_owned(), String::new())
        );
        // A second station: the peer as stored, with both.
        assert_eq!(
            exec(&c, &["peer", "add", "bob", "STATION-B2"]).await,
            (
                Exit::Done,
                "bob STATION-B down\nbob STATION-B2 down\n".to_owned(),
                String::new()
            )
        );
        exec(&c, &["peer", "add", "alice", "STATION-AL"]).await;
        fake.set_link("bob", "STATION-B2", Link::Relayed).unwrap();
        assert_eq!(
            exec(&c, &["peer", "ls"]).await,
            (
                Exit::Done,
                "alice STATION-AL down\nbob STATION-B down\nbob STATION-B2 relayed\n".to_owned(),
                String::new()
            )
        );
        assert_eq!(
            exec(&c, &["peer", "rm", "bob"]).await,
            (Exit::Done, "removed bob\n".to_owned(), String::new())
        );
        assert_eq!(
            exec(&c, &["peer", "ls"]).await,
            (
                Exit::Done,
                "alice STATION-AL down\n".to_owned(),
                String::new()
            )
        );
    })
    .await;
}

#[tokio::test]
async fn peer_errors_exit_1_on_stderr() {
    bounded(async {
        let c = Client::new(fake_with_peers().await);
        let cases: [(&[&str], &str); 5] = [
            (&["peer", "rm", "carol"], "no peer named carol\n"),
            (
                &["peer", "add", "b@d", "STATION-C"],
                "invalid name: may contain only letters, digits, '.', '_' and '-'\n",
            ),
            (
                &["peer", "add", "carol", "STATION-B"],
                "station STATION-B already belongs to peer bob\n",
            ),
            (
                &["peer", "add", "carol", "STATION-A"],
                "invalid station_id: is this station's own id\n",
            ),
            (
                &["peer", "add", "carol", "NOT AN ID"],
                "invalid station_id: is not a station id\n",
            ),
        ];
        for (args, message) in cases {
            assert_eq!(
                exec(&c, args).await,
                (Exit::Failed, String::new(), message.to_owned()),
                "{args:?}"
            );
        }
    })
    .await;
}

// send.

#[tokio::test]
async fn send_outcomes_and_exit_codes() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        assert_eq!(
            exec(&c, &["send", "bob", "on my way"]).await,
            (Exit::Done, "delivered to bob\n".to_owned(), String::new())
        );
        for (reason, text) in [
            (
                FailReason::NoAnswer,
                "bob appears offline, or has not added you\n",
            ),
            (FailReason::Refused, "bob refused the message\n"),
            (
                FailReason::PeerRemoved,
                "bob was removed before the message was delivered\n",
            ),
        ] {
            fake.set_outcome(SendOutcome::Fail(reason));
            assert_eq!(
                exec(&c, &["send", "bob", "hi"]).await,
                (Exit::Failed, String::new(), text.to_owned()),
                "{reason:?}"
            );
        }
        assert_eq!(
            exec(&c, &["send", "carol", "hi"]).await,
            (
                Exit::Failed,
                String::new(),
                "no peer named carol\n".to_owned()
            )
        );
        assert_eq!(
            exec(&c, &["send", "bob", ""]).await,
            (
                Exit::Failed,
                String::new(),
                "invalid body: must not be empty\n".to_owned()
            )
        );
        let big = "x".repeat(BODY_MAX_BYTES + 1);
        let (exit, out, err) = exec(&c, &["send", "bob", &big]).await;
        assert_eq!((exit, out.as_str()), (Exit::Failed, ""));
        assert!(err.starts_with("invalid body: "), "{err}");
    })
    .await;
}

/// Waits for the next outgoing message that is still `Sending` and gives it `status`.
async fn resolve_next(fake: &FakeStation, events: &mut Subscription, status: MessageStatus) {
    loop {
        if let Some(Event::Message { message }) = events.next().await
            && message.status == MessageStatus::Sending
        {
            fake.resolve(message.seq, status).unwrap();
            return;
        }
    }
}

#[tokio::test]
async fn send_waits_for_a_slow_outcome() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.set_outcome(SendOutcome::Hold);
        let c = Client::new(fake.clone());
        let mut events = c.subscribe().await.unwrap();

        let (result, ()) = tokio::join!(exec(&c, &["send", "bob", "slow"]), async {
            resolve_next(&fake, &mut events, MessageStatus::Delivered).await;
        });
        assert_eq!(
            result,
            (Exit::Done, "delivered to bob\n".to_owned(), String::new())
        );

        let failed = MessageStatus::Failed {
            reason: FailReason::NoAnswer,
        };
        let (result, ()) = tokio::join!(exec(&c, &["send", "bob", "slower"]), async {
            resolve_next(&fake, &mut events, failed).await;
        });
        assert_eq!(
            result,
            (
                Exit::Failed,
                String::new(),
                "bob appears offline, or has not added you\n".to_owned()
            )
        );
    })
    .await;
}

// send: what happened at which step (findings A of the second review of #51).

/// The second line of `send` when its `Send` call ended without an answer.
const MAY_HAVE_BEEN_SENT: &str =
    "the message may still have been sent; check `chief log bob` before sending it again\n";
/// The last line of `send` when the station accepted the message and its outcome is unknown.
const ACCEPTED_OUTCOME_UNKNOWN: &str = "the station accepted the message, but its outcome is \
     unknown; check `chief log bob` before sending it again\n";

/// A fake whose `Send` reaches the station, but whose answer is lost: the call ends with `error`.
#[derive(Clone)]
struct LostAnswer {
    fake: FakeStation,
    error: ApiError,
}

impl Transport for LostAnswer {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        let send = matches!(request, Request::Send { .. });
        let answer = self.fake.call(request).await;
        if send {
            Err(self.error.clone())
        } else {
            answer
        }
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        self.fake.subscribe().await
    }
}

impl BindInstance for LostAnswer {
    fn bound_to(&self, instance_id: &str) -> Self {
        Self {
            fake: self.fake.bound_to(instance_id),
            error: self.error.clone(),
        }
    }
}

#[tokio::test]
async fn a_send_without_an_answer_says_the_message_may_have_been_sent() {
    bounded(async {
        for (error, first) in [
            (
                ApiError::Timeout { after_ms: 10_000 },
                "the station did not answer within 10000 ms\n",
            ),
            (
                ApiError::Disconnected {
                    detail: "the station closed the connection".to_owned(),
                },
                "lost the connection to the station: the station closed the connection\n",
            ),
        ] {
            let fake = fake_with_peers().await;
            let c = Client::new(LostAnswer {
                fake: fake.clone(),
                error: error.clone(),
            });
            let (exit, out, err) = exec(&c, &["send", "bob", "hello"]).await;
            assert_eq!(exit, Exit::NoStation, "{error:?}");
            assert_eq!(out, "", "{error:?}");
            assert_eq!(err, format!("{first}{MAY_HAVE_BEEN_SENT}"), "{error:?}");
            // And it was: the station logged and delivered it.
            assert_eq!(sends(&fake), ["hello"]);
            let logged = fake.messages();
            assert_eq!(logged.len(), 1);
            assert_eq!(logged[0].status, MessageStatus::Delivered);
        }
    })
    .await;
}

/// Waits until the station holds an outgoing message that is still `Sending`.
async fn wait_for_sending(events: &mut Subscription) {
    loop {
        if let Some(Event::Message { message }) = events.next().await
            && message.status == MessageStatus::Sending
        {
            return;
        }
    }
}

#[tokio::test]
async fn send_whose_message_left_the_log_before_its_outcome_exits_3_not_1() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.set_outcome(SendOutcome::Hold);
        let c = Client::new(fake.clone());
        let mut events = c.subscribe().await.unwrap();
        let (result, ()) = tokio::join!(exec(&c, &["send", "bob", "evicted"]), async {
            wait_for_sending(&mut events).await;
            // Without a pause: more than the CLI's subscription holds, and more than the log
            // keeps, so its re-read after the `Resync` finds the message gone.
            for i in 0..=FAKE_LOG_CAPACITY {
                fake.receive("alice", &format!("flood {i}")).unwrap();
            }
        });
        let (exit, out, err) = result;
        assert_eq!((exit, out.as_str()), (Exit::NoStation, ""));
        assert_eq!(
            err,
            format!("message 1 is not in the station's log\n{ACCEPTED_OUTCOME_UNKNOWN}")
        );
        assert!(fake.messages().iter().all(|m| m.body != "evicted"));
    })
    .await;
}

#[tokio::test]
async fn send_whose_station_stops_while_it_waits_exits_3() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.set_outcome(SendOutcome::Hold);
        let c = Client::new(fake.clone());
        let mut events = c.subscribe().await.unwrap();
        let (result, ()) = tokio::join!(exec(&c, &["send", "bob", "hello"]), async {
            wait_for_sending(&mut events).await;
            fake.stop();
        });
        let (exit, out, err) = result;
        assert_eq!((exit, out.as_str()), (Exit::NoStation, ""));
        assert_eq!(
            err,
            format!(
                "lost the connection to the station: the subscription ended before the outcome \
                 of the send\n{ACCEPTED_OUTCOME_UNKNOWN}"
            )
        );
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn send_waits_at_most_120_s_for_the_outcome() {
    // Virtual time: the bound of the test is far above the bound of `send`.
    let test = async {
        let fake = fake_with_peers().await;
        fake.set_outcome(SendOutcome::Hold);
        let c = Client::new(fake.clone());
        let start = tokio::time::Instant::now();
        let (exit, out, err) = exec(&c, &["send", "bob", "held"]).await;
        let waited = start.elapsed();
        assert!(
            (Duration::from_secs(120)..Duration::from_secs(121)).contains(&waited),
            "{waited:?}"
        );
        assert_eq!((exit, out.as_str()), (Exit::NoStation, ""));
        assert_eq!(
            err,
            format!(
                "the station did not report the outcome within 120 s\n{ACCEPTED_OUTCOME_UNKNOWN}"
            )
        );
        assert_eq!(sends(&fake), ["held"]);
    };
    tokio::time::timeout(Duration::from_secs(600), test)
        .await
        .expect("send did not give up within 600 s of virtual time");
}

/// A writer that fails every write, like a closed stdout.
struct Closed;

impl Write for Closed {
    fn write(&mut self, _data: &[u8]) -> std::io::Result<usize> {
        Err(std::io::ErrorKind::BrokenPipe.into())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Err(std::io::ErrorKind::BrokenPipe.into())
    }
}

#[tokio::test]
async fn a_delivered_send_exits_0_even_when_stdout_is_gone() {
    bounded(async {
        let c = Client::new(fake_with_peers().await);
        let command = parse(&["send", "bob", "hello"]);
        let mut err = Vec::new();
        let exit = run(&command, &c, tokio::io::empty(), &mut Closed, &mut err).await;
        assert_eq!(exit, Exit::Done);
        assert_eq!(String::from_utf8(err).unwrap(), "");
    })
    .await;
}

// log.

#[tokio::test]
async fn log_prints_everything_oldest_first() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        assert_eq!(
            exec(&c, &["log"]).await,
            (Exit::Done, "no messages\n".to_owned(), String::new())
        );
        fake.set_now_ms(T0);
        fake.receive("bob", "hello there").unwrap();
        fake.set_now_ms(T0 + 5_000);
        exec(&c, &["send", "bob", "hi bob"]).await;
        fake.set_outcome(SendOutcome::Fail(FailReason::NoAnswer));
        fake.set_now_ms(T0 + 60_000);
        exec(&c, &["send", "alice", "hi alice"]).await;
        fake.set_now_ms(T0 + 3_600_000);
        fake.receive("alice", "late").unwrap();

        assert_eq!(
            exec(&c, &["log"]).await,
            (
                Exit::Done,
                "1 2023-11-14T22:13:20Z <- bob hello there\n\
                 2 2023-11-14T22:13:25Z -> bob (delivered) hi bob\n\
                 3 2023-11-14T22:14:20Z -> alice (failed: no answer) hi alice\n\
                 4 2023-11-14T23:13:20Z <- alice late\n"
                    .to_owned(),
                String::new()
            )
        );

        // A peer filter.
        let bob = "1 2023-11-14T22:13:20Z <- bob hello there\n\
                   2 2023-11-14T22:13:25Z -> bob (delivered) hi bob\n";
        assert_eq!(
            exec(&c, &["log", "bob"]).await,
            (Exit::Done, bob.to_owned(), String::new())
        );

        // An unknown peer is an error; a removed one still has its messages.
        assert_eq!(
            exec(&c, &["log", "carol"]).await,
            (
                Exit::Failed,
                String::new(),
                "no peer named carol\n".to_owned()
            )
        );
        exec(&c, &["peer", "rm", "bob"]).await;
        assert_eq!(
            exec(&c, &["log", "bob"]).await,
            (Exit::Done, bob.to_owned(), String::new())
        );
        // A peer in the book with no messages.
        exec(&c, &["peer", "add", "carol", "STATION-C"]).await;
        assert_eq!(
            exec(&c, &["log", "carol"]).await,
            (Exit::Done, "no messages\n".to_owned(), String::new())
        );
    })
    .await;
}

fn log_requests(fake: &FakeStation) -> usize {
    fake.requests()
        .iter()
        .filter(|r| matches!(r, Request::Log { .. }))
        .count()
}

#[tokio::test]
async fn log_pages_past_the_message_limit() {
    bounded(async {
        let n = usize::try_from(LOG_PAGE_MAX_MESSAGES).unwrap() + 500;
        let fake = FakeStation::new("STATION-A", "INSTANCE-1").with_log_capacity(n);
        let c = Client::new(fake.clone());
        c.peer_add("bob", "STATION-B").await.unwrap();
        for i in 0..n {
            fake.receive("bob", &format!("m{i}")).unwrap();
        }
        let (exit, out, err) = exec(&c, &["log"]).await;
        assert_eq!((exit, err.as_str()), (Exit::Done, ""));
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), n);
        for (i, l) in lines.iter().enumerate() {
            assert!(l.starts_with(&format!("{} ", i + 1)), "{l}");
            assert!(l.ends_with(&format!("<- bob m{i}")), "{l}");
        }
        assert_eq!(log_requests(&fake), 2);
    })
    .await;
}

#[tokio::test]
async fn log_pages_past_the_byte_limit() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        // 100 messages of 10 KB: about 1 MB, two pages or more under the 512 KiB budget.
        for i in 0..100 {
            fake.receive("bob", &format!("{i:03}{}", "x".repeat(10_000)))
                .unwrap();
        }
        fake.receive("alice", "not bob").unwrap();
        let (exit, out, _) = exec(&c, &["log", "bob"]).await;
        assert_eq!(exit, Exit::Done);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 100);
        for (i, l) in lines.iter().enumerate() {
            assert!(l.contains(&format!("<- bob {i:03}x")), "line {i}");
        }
        assert!(log_requests(&fake) >= 2);
    })
    .await;
}

// chat.

#[tokio::test]
async fn chat_backlog_then_live_then_end_of_input() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        // 60 messages, one a second from 22:13:00.
        for i in 0..60u64 {
            let peer = if i % 2 == 0 { "bob" } else { "alice" };
            fake.set_now_ms(MINUTE + i * 1000);
            fake.receive(peer, &format!("old {i}")).unwrap();
        }
        let command = parse(&["chat", "bob"]);
        let (session, out, err, mut input) = chat(&c, &command);
        let drive = async {
            // The backlog: the last 50 messages of every peer, oldest first.
            out.wait_for(|s| s.lines().count() == 50).await;
            let backlog: String = (10..60u64)
                .map(|i| {
                    let peer = if i % 2 == 0 { "bob" } else { "alice" };
                    format!("{} 2023-11-14T22:13:{i:02}Z <- {peer} old {i}\n", i + 1)
                })
                .collect();
            assert_eq!(out.text(), backlog);

            // Live, from another peer too.
            fake.set_now_ms(MINUTE + 60_000);
            fake.receive("alice", "live one").unwrap();
            out.wait_for(|s| s.ends_with("61 2023-11-14T22:14:00Z <- alice live one\n"))
                .await;

            // A typed line goes to bob; its outcome comes back as a change.
            fake.set_outcome(SendOutcome::Hold);
            fake.set_now_ms(MINUTE + 120_000);
            input.write_all(b"hello bob\r\n\n   \n").await.unwrap();
            out.wait_for(|s| s.ends_with("62 2023-11-14T22:15:00Z -> bob (sending) hello bob\n"))
                .await;
            fake.resolve(
                62,
                MessageStatus::Failed {
                    reason: FailReason::NoAnswer,
                },
            )
            .unwrap();
            out.wait_for(|s| s.ends_with("62 -> bob now (failed: no answer)\n"))
                .await;

            fake.set_outcome(SendOutcome::Deliver);
            fake.set_now_ms(MINUTE + 180_000);
            input.write_all(b"second\n").await.unwrap();
            out.wait_for(|s| s.ends_with("63 -> bob now (delivered)\n"))
                .await;
            drop(input);
        };
        let (exit, ()) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::Done);
        assert_eq!(err.text(), "");
        // Blank lines were not sent.
        let sends: Vec<Request> = fake
            .requests()
            .into_iter()
            .filter(|r| matches!(r, Request::Send { .. }))
            .collect();
        assert_eq!(
            sends,
            ["hello bob", "second"].map(|b| Request::Send {
                peer: "bob".to_owned(),
                body: b.to_owned()
            })
        );
        let text = out.text();
        let tail: Vec<&str> = text.lines().skip(50).collect();
        assert_eq!(
            tail,
            [
                "61 2023-11-14T22:14:00Z <- alice live one",
                "62 2023-11-14T22:15:00Z -> bob (sending) hello bob",
                "62 -> bob now (failed: no answer)",
                "63 2023-11-14T22:16:00Z -> bob (sending) second",
                "63 -> bob now (delivered)",
            ]
        );
    })
    .await;
}

#[tokio::test]
async fn chat_ends_with_exit_3_when_the_station_stops() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.receive("bob", "before").unwrap();
        let c = Client::new(fake.clone());
        let command = parse(&["chat", "bob"]);
        let (session, out, err, input) = chat(&c, &command);
        let drive = async {
            // Once the backlog is shown, the chat is live.
            out.wait_for(|s| s.lines().count() == 1).await;
            fake.stop();
            // Keep the input open: the chat must end because of the station, not the input.
            input
        };
        let (exit, _input) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::NoStation);
        assert!(out.text().ends_with("<- bob before\n"));
        assert_eq!(err.text(), "the station stopped; the chat has ended\n");
    })
    .await;
}

/// How the station goes away under a chat that fell behind.
#[derive(Clone, Copy, Debug)]
enum Gone {
    /// Every call fails with `Stopping`; the subscription stays open.
    CallsFail,
    /// The station stops: calls fail and the subscription ends.
    Stops,
    /// The station restarts as `INSTANCE-2` and logs a message there.
    Restarts,
}

/// Runs a chat until its backlog is shown, stops polling it while more messages arrive than its
/// subscription holds (so its next read is a `Resync` and a re-read of the log), makes the station
/// go away as `gone` says, then lets the chat run to its end.
async fn chat_lags_then(gone: Gone) {
    let fake = fake_with_peers().await;
    fake.set_now_ms(T0);
    fake.receive("bob", "before").unwrap();
    let c = Client::new(fake.clone());
    let command = parse(&["chat", "bob"]);
    let (session, out, err, input) = chat(&c, &command);
    let mut session = pin!(session);
    tokio::select! {
        exit = &mut session => panic!("the chat ended early: {exit:?}"),
        () = out.wait_for(|s| s.lines().count() == 1) => {}
    }
    // The chat is not polled now: these overflow its subscription.
    for i in 0..=SUBSCRIPTION_CAPACITY {
        fake.receive("alice", &format!("missed {i}")).unwrap();
    }
    let calls_before = fake.requests().len();
    match gone {
        Gone::CallsFail => fake.fail_calls(Some(ApiError::Stopping)),
        Gone::Stops => fake.stop(),
        Gone::Restarts => {
            fake.restart("INSTANCE-2");
            fake.receive("bob", "from the new instance").unwrap();
        }
    }
    let exit = session.await;
    drop(input);
    assert_eq!(exit, Exit::NoStation, "{gone:?}");
    if let Gone::CallsFail = gone {
        // The chat did try to re-read the log, and that call is what failed.
        assert!(fake.requests().len() > calls_before);
    }
    // Nothing of the gap, nothing of a new instance: only the backlog.
    assert_eq!(
        out.text(),
        "1 2023-11-14T22:13:20Z <- bob before\n",
        "{gone:?}"
    );
    // One plain line, no error repeated.
    assert_eq!(
        err.text(),
        "the station stopped; the chat has ended\n",
        "{gone:?}"
    );
}

#[tokio::test]
async fn chat_ends_when_a_reread_after_a_resync_fails() {
    bounded(chat_lags_then(Gone::CallsFail)).await;
}

#[tokio::test]
async fn chat_ends_when_the_station_stops_during_a_resync() {
    bounded(chat_lags_then(Gone::Stops)).await;
}

#[tokio::test]
async fn chat_ends_when_the_station_restarts_under_it() {
    bounded(chat_lags_then(Gone::Restarts)).await;
}

#[tokio::test]
async fn chat_reports_a_refused_line_goes_on_and_exits_1() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        let command = parse(&["chat", "bob"]);
        let (session, out, err, mut input) = chat(&c, &command);
        let drive = async {
            fake.wait_for_subscribers(1).await;
            // bob goes away while the chat runs: the send fails, the chat goes on.
            Client::new(fake.clone()).peer_remove("bob").await.unwrap();
            input.write_all(b"anyone?\n").await.unwrap();
            err.wait_for(|s| s == "no peer named bob\n").await;
            fake.receive("alice", "still here").unwrap();
            out.wait_for(|s| s.ends_with("<- alice still here\n")).await;
            // A later line is still sent.
            Client::new(fake.clone())
                .peer_add("bob", "STATION-B")
                .await
                .unwrap();
            input.write_all(b"back again\n").await.unwrap();
            out.wait_for(|s| s.contains("-> bob (sending) back again\n"))
                .await;
            drop(input);
        };
        let (exit, ()) = tokio::join!(session, drive);
        // One line was refused: exit 1 at the end of the input.
        assert_eq!(exit, Exit::Failed);
        assert_eq!(err.text(), "no peer named bob\n");
        assert_eq!(sends(&fake), ["anyone?", "back again"]);
    })
    .await;
}

// chat: bad input lines and a chat that ends early (findings B of the second review of #51).

/// Starts `chief chat <peer>` reading `input`: the future, its stdout and its stderr.
fn chat_on<'a, T: BindInstance, R: AsyncRead + Unpin + 'a>(
    client: &'a Client<T>,
    command: &'a Command,
    input: R,
) -> (impl Future<Output = Exit> + 'a, Shared, Shared) {
    let out = Shared::default();
    let err = Shared::default();
    let (mut o, mut e) = (out.clone(), err.clone());
    let fut = async move { run(command, client, BufReader::new(input), &mut o, &mut e).await };
    (fut, out, err)
}

/// Runs a chat on `input` (all of it, then end of input) to its end: (exit, stdout, stderr).
async fn chat_all<T: BindInstance>(client: &Client<T>, input: &[u8]) -> (Exit, String, String) {
    let command = parse(&["chat", "bob"]);
    let (session, out, err) = chat_on(client, &command, input);
    let exit = session.await;
    (exit, out.text(), err.text())
}

const NOT_UTF8: &str = "a line is not valid UTF-8 and was not sent\n";

fn too_long() -> String {
    format!("a line is longer than {BODY_MAX_BYTES} bytes and was not sent\n")
}

#[tokio::test]
async fn a_line_that_is_not_utf8_is_skipped_and_the_chat_goes_on() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        let (exit, _, err) = chat_all(&c, b"one\nbad \xff\xfe byte\ntwo\n").await;
        assert_eq!(exit, Exit::Failed);
        assert_eq!(err, NOT_UTF8);
        assert_eq!(sends(&fake), ["one", "two"]);
    })
    .await;
}

#[tokio::test]
async fn a_line_of_the_maximum_length_is_sent_and_a_longer_one_is_skipped() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        let longest = "a".repeat(BODY_MAX_BYTES);
        let longer = "b".repeat(BODY_MAX_BYTES + 1);
        // The longest line also with a `\r` before its newline, which is dropped.
        let input = format!("{longest}\n{longer}\n{longest}\r\nafter\n");
        let (exit, _, err) = chat_all(&c, input.as_bytes()).await;
        assert_eq!(exit, Exit::Failed);
        assert_eq!(err, too_long());
        assert_eq!(sends(&fake), [longest.as_str(), &longest, "after"]);
    })
    .await;
}

/// An input of `left` bytes of `x` and no newline, then end of input.
struct Endless {
    left: usize,
}

impl AsyncRead for Endless {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let n = self.left.min(buf.remaining()).min(4096);
        buf.put_slice(&[b'x'; 4096][..n]);
        self.left -= n;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn an_input_that_never_ends_a_line_is_skipped_as_one_line_too_long() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        let command = parse(&["chat", "bob"]);
        let (session, out, err) = chat_on(&c, &command, Endless { left: 8 << 20 });
        assert_eq!(session.await, Exit::Failed);
        assert_eq!(out.text(), "");
        assert_eq!(err.text(), too_long());
        assert!(sends(&fake).is_empty());
    })
    .await;
}

#[tokio::test]
async fn crlf_and_a_last_line_without_a_newline() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        let (exit, _, err) = chat_all(&c, b"one\r\n\r\n  \ntwo\r\nlast").await;
        assert_eq!((exit, err.as_str()), (Exit::Done, ""));
        assert_eq!(sends(&fake), ["one", "two", "last"]);
    })
    .await;
}

#[tokio::test]
async fn a_failing_input_still_sends_the_lines_read_and_exits_1() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        let command = parse(&["chat", "bob"]);
        let input = tokio::io::AsyncReadExt::chain(&b"one\ntwo\npart"[..], Broken);
        let (session, _out, err) = chat_on(&c, &command, input);
        assert_eq!(session.await, Exit::Failed);
        assert_eq!(err.text(), "cannot read the input: the pipe broke\n");
        assert_eq!(sends(&fake), ["one", "two"]);
    })
    .await;
}

/// An input that fails every read.
struct Broken;

impl AsyncRead for Broken {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::Error::other("the pipe broke")))
    }
}

/// How many chunks a [`Chunks`] input has handed out.
#[derive(Clone, Default)]
struct Taken {
    n: Arc<Mutex<usize>>,
    changed: Arc<Notify>,
}

impl Taken {
    async fn wait_for(&self, n: usize) {
        loop {
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            if *self.n.lock().unwrap() >= n {
                return;
            }
            changed.await;
        }
    }
}

/// An input the test feeds one chunk at a time; it is `Pending` between chunks and ends when the
/// sender is dropped. The chat has taken a chunk once [`Taken`] counts it: the chat's poll that
/// took it has read every line in it before the test runs again.
struct Chunks {
    rx: tokio::sync::mpsc::UnboundedReceiver<&'static [u8]>,
    taken: Taken,
}

fn chunks() -> (
    tokio::sync::mpsc::UnboundedSender<&'static [u8]>,
    Chunks,
    Taken,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let taken = Taken::default();
    (
        tx,
        Chunks {
            rx,
            taken: taken.clone(),
        },
        taken,
    )
}

impl AsyncRead for Chunks {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(chunk)) => {
                buf.put_slice(chunk);
                *self.taken.n.lock().unwrap() += 1;
                self.taken.changed.notify_waiters();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[tokio::test]
async fn a_line_split_across_reads_survives_a_live_message_between_its_halves() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.set_now_ms(T0);
        let c = Client::new(fake.clone());
        let command = parse(&["chat", "bob"]);
        let (tx, input, taken) = chunks();
        let (session, out, err) = chat_on(&c, &command, input);
        let drive = async {
            fake.wait_for_subscribers(1).await;
            tx.send(b"hel").unwrap();
            taken.wait_for(1).await;
            // The follower's branch wins the select: the read of the line is dropped between
            // its halves.
            fake.receive("alice", "between").unwrap();
            out.wait_for(|s| s.ends_with("<- alice between\n")).await;
            tx.send(b"lo\n").unwrap();
            out.wait_for(|s| s.contains("-> bob (sending) hello\n"))
                .await;
            drop(tx);
        };
        let (exit, ()) = tokio::join!(session, drive);
        assert_eq!((exit, err.text().as_str()), (Exit::Done, ""));
        assert_eq!(sends(&fake), ["hello"]);
    })
    .await;
}

#[tokio::test]
async fn a_chat_that_ends_early_reports_the_line_in_flight_and_the_lines_queued() {
    bounded(async {
        let fake = fake_with_peers().await;
        let gated = Gated::new(&fake, HoldAt::Request);
        let gate = Arc::clone(&gated.gate);
        let c = Client::new(gated);
        let command = parse(&["chat", "bob"]);
        let (tx, input, taken) = chunks();
        let (session, _out, err) = chat_on(&c, &command, input);
        let drive = async {
            fake.wait_for_subscribers(1).await;
            tx.send(b"one\ntwo\nthree\n").unwrap();
            taken.wait_for(1).await;
            gate.wait_held(1).await;
            fake.stop();
            tx
        };
        let (exit, _tx) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::NoStation);
        assert_eq!(
            err.text(),
            "the station stopped; the chat has ended\n\
             the last line may have been sent; check `chief log bob`\n\
             2 typed lines were not sent\n"
        );
        assert!(sends(&fake).is_empty());
    })
    .await;
}

#[tokio::test]
async fn a_send_without_an_answer_in_a_chat_says_the_line_may_have_been_sent() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(LostAnswer {
            fake: fake.clone(),
            error: ApiError::Timeout { after_ms: 10_000 },
        });
        let command = parse(&["chat", "bob"]);
        let (session, out, err, mut input) = chat(&c, &command);
        let drive = async {
            fake.wait_for_subscribers(1).await;
            input.write_all(b"hello\n").await.unwrap();
            input
        };
        let (exit, _input) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::NoStation);
        assert_eq!(
            err.text(),
            "the station did not answer within 10000 ms\n\
             the last line may have been sent; check `chief log bob`\n"
        );
        // It was sent; the send's error ends the chat in the poll that sent it, before the
        // live view shows it.
        assert_eq!(sends(&fake), ["hello"]);
        assert_eq!(out.text(), "");
    })
    .await;
}

#[tokio::test]
async fn chat_with_an_unknown_peer_fails_before_showing_anything() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.receive("bob", "something").unwrap();
        let c = Client::new(fake.clone());
        let command = parse(&["chat", "carol"]);
        let (session, out, err, _input) = chat(&c, &command);
        assert_eq!(session.await, Exit::Failed);
        assert_eq!(out.text(), "");
        assert_eq!(err.text(), "no peer named carol\n");
        assert_eq!(fake.subscriber_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn chat_with_input_already_closed_shows_the_backlog_and_ends() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.receive("bob", "one").unwrap();
        fake.receive("bob", "two").unwrap();
        let c = Client::new(fake.clone());
        let command = parse(&["chat", "bob"]);
        let (session, out, err, input) = chat(&c, &command);
        drop(input);
        assert_eq!(session.await, Exit::Done);
        assert_eq!(out.text().lines().count(), 2);
        assert_eq!(err.text(), "");
    })
    .await;
}

// One station instance per command (findings 1 and 2 of the review of #51).

fn sends(fake: &FakeStation) -> Vec<String> {
    fake.requests()
        .into_iter()
        .filter_map(|r| match r {
            Request::Send { body, .. } => Some(body),
            _ => None,
        })
        .collect()
}

/// Makes the fake restart as `INSTANCE-2` inside its `n`-th call that `pick` selects, before the
/// fake looks at it, and log a message of `bob` there.
fn restart_on(fake: &FakeStation, n: usize, pick: fn(&Request) -> bool) {
    let mut seen = 0;
    fake.on_call(move |fake, request| {
        if pick(request) {
            seen += 1;
            if seen == n {
                fake.clear_on_call();
                fake.restart("INSTANCE-2");
                fake.receive("bob", "from the new instance").unwrap();
            }
        }
    });
}

#[tokio::test]
async fn log_with_a_restart_between_two_pages_prints_nothing_and_exits_3() {
    bounded(async {
        let n = usize::try_from(LOG_PAGE_MAX_MESSAGES).unwrap() + 200;
        let fake = FakeStation::new("STATION-A", "INSTANCE-1").with_log_capacity(n);
        let c = Client::new(fake.clone());
        c.peer_add("bob", "STATION-B").await.unwrap();
        for i in 0..n {
            fake.receive("bob", &format!("m{i}")).unwrap();
        }
        restart_on(&fake, 2, |r| matches!(r, Request::Log { .. }));
        let (exit, out, err) = exec(&c, &["log"]).await;
        assert_eq!(exit, Exit::NoStation);
        // No message at all, of either instance.
        assert_eq!(out, "");
        assert_eq!(err.lines().count(), 1, "{err}");
        assert!(
            err.starts_with("lost the connection to the station: "),
            "{err}"
        );
        // The second page never reached the new instance.
        assert_eq!(log_requests(&fake), 1);
    })
    .await;
}

#[tokio::test]
async fn status_with_a_restart_between_its_two_calls_prints_nothing_and_exits_3() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.set_tower_state(TowerState::Connected);
        fake.set_link("bob", "STATION-B", Link::Relayed).unwrap();
        let c = Client::new(fake.clone());
        // The new instance's peers have every link down.
        restart_on(&fake, 1, |r| matches!(r, Request::Peers));
        let (exit, out, err) = exec(&c, &["status"]).await;
        assert_eq!(exit, Exit::NoStation);
        assert_eq!(out, "");
        assert_eq!(err.lines().count(), 1, "{err}");
        assert!(
            err.starts_with("lost the connection to the station: "),
            "{err}"
        );
        assert!(!fake.requests().contains(&Request::Peers));
    })
    .await;
}

#[tokio::test]
async fn send_with_a_restart_before_its_send_never_sends() {
    bounded(async {
        let fake = fake_with_peers().await;
        let c = Client::new(fake.clone());
        restart_on(&fake, 1, |r| matches!(r, Request::Send { .. }));
        let (exit, out, err) = exec(&c, &["send", "bob", "hello"]).await;
        assert_eq!((exit, out.as_str()), (Exit::NoStation, ""));
        // The bound transport refuses with `Disconnected`, which the CLI cannot tell from a lost
        // answer: it says the message may have been sent, though it was not.
        assert_eq!(
            err,
            format!(
                "lost the connection to the station: the station restarted: another instance \
                 runs now\n{MAY_HAVE_BEEN_SENT}"
            )
        );
        assert!(sends(&fake).is_empty());
    })
    .await;
}

/// A fake whose `Log` answers `fix` rewrites: `fix(before, page)`, where `before` is the request's.
///
/// It counts its `Log` calls and panics at call `max_logs + 1`, so that a `log` that never stops
/// asking fails the test at once instead of hanging it: an answer that comes without a pause
/// never returns to the runtime, and the test's bound could not fire.
#[derive(Clone)]
struct Cursor {
    fake: FakeStation,
    fix: fn(Option<u64>, LogPage) -> LogPage,
    logs: Arc<Mutex<usize>>,
    max_logs: usize,
}

impl Cursor {
    /// A wrapper for a log of a few pages: at most 10 `Log` calls.
    fn new(fake: &FakeStation, fix: fn(Option<u64>, LogPage) -> LogPage) -> Self {
        Self {
            fake: fake.clone(),
            fix,
            logs: Arc::default(),
            max_logs: 10,
        }
    }
}

impl Transport for Cursor {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        let before = match &request {
            Request::Log { before, .. } => {
                let mut logs = self.logs.lock().unwrap();
                *logs += 1;
                assert!(
                    *logs <= self.max_logs,
                    "log asked for more than {} pages",
                    self.max_logs
                );
                Some(*before)
            }
            _ => None,
        };
        match (before, self.fake.call(request).await?) {
            (Some(before), Response::Log { page }) => Ok(Response::Log {
                page: (self.fix)(before, page),
            }),
            (_, response) => Ok(response),
        }
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        self.fake.subscribe().await
    }
}

impl BindInstance for Cursor {
    fn bound_to(&self, instance_id: &str) -> Self {
        Self {
            fake: self.fake.bound_to(instance_id),
            fix: self.fix,
            logs: Arc::clone(&self.logs),
            max_logs: self.max_logs,
        }
    }
}

/// A fake with the peer `bob` and more messages of his than one page holds.
async fn fake_with_two_pages() -> FakeStation {
    let n = usize::try_from(LOG_PAGE_MAX_MESSAGES).unwrap() + 200;
    let fake = FakeStation::new("STATION-A", "INSTANCE-1").with_log_capacity(n);
    Client::new(fake.clone())
        .peer_add("bob", "STATION-B")
        .await
        .unwrap();
    for i in 0..n {
        fake.receive("bob", &format!("m{i}")).unwrap();
    }
    fake
}

/// Runs `chief log` over each wrapper: nothing on stdout, one `BadReply` line, exit 3.
async fn assert_bad_log(wrappers: impl IntoIterator<Item = Cursor>) {
    for (i, cursor) in wrappers.into_iter().enumerate() {
        let c = Client::new(cursor);
        let (exit, out, err) = exec(&c, &["log"]).await;
        assert_eq!((exit, out.as_str()), (Exit::NoStation, ""), "fix {i}");
        assert_eq!(err.lines().count(), 1, "fix {i}: {err}");
        assert!(
            err.starts_with("unexpected reply from the station: "),
            "fix {i}: {err}"
        );
    }
}

#[tokio::test]
async fn a_log_cursor_that_does_not_move_back_is_a_bad_reply() {
    bounded(async {
        let fake = fake_with_two_pages().await;
        let fixes: [fn(Option<u64>, LogPage) -> LogPage; 3] = [
            // The second page repeats the cursor it was asked with.
            |before, page| LogPage {
                next_before: before.or(page.next_before),
                ..page
            },
            // The second page gives a cursor above the one it was asked with.
            |before, page| LogPage {
                next_before: before.map(|b| b + 1).or(page.next_before),
                ..page
            },
            // The first page is empty and has a cursor.
            |_, page| LogPage {
                messages: Vec::new(),
                ..page
            },
        ];
        assert_bad_log(fixes.map(|fix| Cursor::new(&fake, fix))).await;
    })
    .await;
}

#[tokio::test]
async fn a_log_page_of_the_wrong_shape_is_a_bad_reply() {
    bounded(async {
        let fake = fake_with_two_pages().await;
        let fixes: [fn(Option<u64>, LogPage) -> LogPage; 4] = [
            // The first page has two messages out of order (not the first one).
            |before, mut page| {
                if before.is_none() {
                    page.messages.swap(1, 2);
                }
                page
            },
            // The second page ends with a message at the cursor it was asked with.
            |before, mut page| {
                if let (Some(b), Some(last)) = (before, page.messages.last_mut()) {
                    last.seq = b;
                }
                page
            },
            // The second page has a message above the cursor it was asked with.
            |before, mut page| {
                if let (Some(b), Some(last)) = (before, page.messages.last_mut()) {
                    last.seq = b + 5;
                }
                page
            },
            // The first page's cursor is below its first message: the next page would skip one.
            |before, page| LogPage {
                next_before: page
                    .next_before
                    .map(|n| if before.is_none() { n - 1 } else { n }),
                ..page
            },
        ];
        assert_bad_log(fixes.map(|fix| Cursor::new(&fake, fix))).await;
    })
    .await;
}

#[tokio::test]
async fn a_log_longer_than_any_station_keeps_is_a_bad_reply() {
    bounded(async {
        let fake = fake_with_peers().await;
        // Every page is a full page of made-up messages just below the cursor asked, without end.
        let endless = |before: Option<u64>, _page: LogPage| {
            let top = before.unwrap_or(1 << 40);
            let first = top - u64::from(LOG_PAGE_MAX_MESSAGES);
            LogPage {
                messages: (first..top)
                    .map(|seq| message(seq, Direction::Incoming, "x", MessageStatus::Received))
                    .collect(),
                next_before: Some(first),
            }
        };
        let pages = LOG_MAX_MESSAGES / usize::try_from(LOG_PAGE_MAX_MESSAGES).unwrap();
        let cursor = Cursor {
            max_logs: pages + 2,
            ..Cursor::new(&fake, endless)
        };
        let logs = Arc::clone(&cursor.logs);
        assert_bad_log([cursor]).await;
        // It stopped at the cap: one page past it at most.
        assert_eq!(*logs.lock().unwrap(), pages + 1);
    })
    .await;
}

#[tokio::test]
async fn chat_never_sends_to_an_instance_restarted_inside_the_send() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.receive("bob", "before").unwrap();
        let c = Client::new(fake.clone());
        let command = parse(&["chat", "bob"]);
        let (session, out, err, mut input) = chat(&c, &command);
        let drive = async {
            out.wait_for(|s| s.ends_with("<- bob before\n")).await;
            restart_on(&fake, 1, |r| matches!(r, Request::Send { .. }));
            input.write_all(b"must not arrive\n").await.unwrap();
            input
        };
        let (exit, _input) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::NoStation);
        // The bound transport refuses the send with `Disconnected`, which the chat cannot tell
        // from a lost answer.
        assert_eq!(
            err.text(),
            "the station stopped; the chat has ended\n\
             the last line may have been sent; check `chief log bob`\n"
        );
        assert!(sends(&fake).is_empty());
        // The new instance logged only its own message.
        let bodies: Vec<String> = fake.messages().into_iter().map(|m| m.body).collect();
        assert_eq!(bodies, ["from the new instance"]);
        assert!(!out.text().contains("must not arrive"));
        assert!(!out.text().contains("from the new instance"));
    })
    .await;
}

// chat keeps reading while a send is pending (finding 3 of the review of #51).

/// Where [`Gated`] holds a `Send`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HoldAt {
    /// Before the fake gets the request.
    Request,
    /// After the fake answered, with the answer.
    Reply,
}

/// Counts the held `Send` calls and lets them go one permit at a time.
struct Gate {
    held: Mutex<usize>,
    changed: Notify,
    permits: tokio::sync::Semaphore,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            held: Mutex::new(0),
            changed: Notify::new(),
            permits: tokio::sync::Semaphore::new(0),
        })
    }

    fn held(&self) -> usize {
        *self.held.lock().unwrap()
    }

    fn count(&self, delta: isize) {
        let mut held = self.held.lock().unwrap();
        *held = held.checked_add_signed(delta).unwrap();
        self.changed.notify_waiters();
    }

    /// Waits for one permit, counted as held meanwhile, also when the call is dropped.
    async fn pass(&self) {
        struct Held<'a>(&'a Gate);
        impl Drop for Held<'_> {
            fn drop(&mut self) {
                self.0.count(-1);
            }
        }
        self.count(1);
        let _held = Held(self);
        self.permits.acquire().await.unwrap().forget();
    }

    async fn wait_held(&self, n: usize) {
        loop {
            let mut changed = pin!(self.changed.notified());
            changed.as_mut().enable();
            if self.held() == n {
                return;
            }
            changed.await;
        }
    }

    fn release(&self, n: usize) {
        self.permits.add_permits(n);
    }
}

/// A fake whose `Send` calls wait at a [`Gate`].
#[derive(Clone)]
struct Gated {
    fake: FakeStation,
    gate: Arc<Gate>,
    at: HoldAt,
}

impl Gated {
    fn new(fake: &FakeStation, at: HoldAt) -> Self {
        Self {
            fake: fake.clone(),
            gate: Gate::new(),
            at,
        }
    }
}

impl Transport for Gated {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        let send = matches!(request, Request::Send { .. });
        if send && self.at == HoldAt::Request {
            self.gate.pass().await;
        }
        let answer = self.fake.call(request).await;
        if send && self.at == HoldAt::Reply {
            self.gate.pass().await;
        }
        answer
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        self.fake.subscribe().await
    }
}

impl BindInstance for Gated {
    fn bound_to(&self, instance_id: &str) -> Self {
        Self {
            fake: self.fake.bound_to(instance_id),
            gate: Arc::clone(&self.gate),
            at: self.at,
        }
    }
}

/// How long a test waits for something that must happen while a send is held, before it decides
/// that the chat is stuck in the send.
const WHILE_HELD: Duration = Duration::from_secs(5);

#[tokio::test]
async fn chat_shows_incoming_messages_while_a_send_is_pending() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.set_now_ms(T0);
        let gated = Gated::new(&fake, HoldAt::Reply);
        let gate = Arc::clone(&gated.gate);
        let c = Client::new(gated);
        let command = parse(&["chat", "bob"]);
        let (session, out, err, mut input) = chat(&c, &command);
        let drive = async {
            fake.wait_for_subscribers(1).await;
            input.write_all(b"hello\n").await.unwrap();
            gate.wait_held(1).await;
            fake.receive("bob", "while you wait").unwrap();
            tokio::time::timeout(
                WHILE_HELD,
                out.wait_for(|s| s.ends_with("<- bob while you wait\n")),
            )
            .await
            .expect("an incoming message was not shown while the send was pending");
            assert_eq!(gate.held(), 1);
            gate.release(1);
            drop(input);
        };
        let (exit, ()) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::Done);
        assert_eq!(err.text(), "");
        assert_eq!(
            out.text(),
            "1 2023-11-14T22:13:20Z -> bob (sending) hello\n\
             1 -> bob now (delivered)\n\
             2 2023-11-14T22:13:20Z <- bob while you wait\n"
        );
    })
    .await;
}

#[tokio::test]
async fn the_station_stopping_while_a_send_is_held_ends_the_chat_once() {
    bounded(async {
        // The station stops: the live view and the held send both find out.
        let fake = fake_with_peers().await;
        let gated = Gated::new(&fake, HoldAt::Request);
        let gate = Arc::clone(&gated.gate);
        let c = Client::new(gated);
        let command = parse(&["chat", "bob"]);
        let (session, _out, err, mut input) = chat(&c, &command);
        let drive = async {
            fake.wait_for_subscribers(1).await;
            input.write_all(b"hello\n").await.unwrap();
            gate.wait_held(1).await;
            fake.stop();
            // If the chat still waits for the send, it now gets `Stopping`.
            gate.release(1);
            input
        };
        let (exit, _input) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::NoStation);
        // The live view ends the chat first, with the send in flight.
        assert_eq!(
            err.text(),
            "the station stopped; the chat has ended\n\
             the last line may have been sent; check `chief log bob`\n"
        );

        // Only the send finds out: calls fail with `Stopping`, the subscription stays open.
        let fake = fake_with_peers().await;
        let gated = Gated::new(&fake, HoldAt::Request);
        let gate = Arc::clone(&gated.gate);
        let c = Client::new(gated);
        let (session, _out, err, mut input) = chat(&c, &command);
        let drive = async {
            fake.wait_for_subscribers(1).await;
            input.write_all(b"hello\n").await.unwrap();
            gate.wait_held(1).await;
            fake.fail_calls(Some(ApiError::Stopping));
            gate.release(1);
            input
        };
        let (exit, _input) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::NoStation);
        // `Stopping` is a refusal: the line was not sent.
        assert_eq!(err.text(), "the station stopped; the chat has ended\n");
    })
    .await;
}

#[tokio::test]
async fn typed_lines_are_sent_one_at_a_time_in_order() {
    bounded(async {
        let fake = fake_with_peers().await;
        let gated = Gated::new(&fake, HoldAt::Reply);
        let gate = Arc::clone(&gated.gate);
        let c = Client::new(gated);
        let command = parse(&["chat", "bob"]);
        let (session, out, err, mut input) = chat(&c, &command);
        let drive = async {
            fake.wait_for_subscribers(1).await;
            input.write_all(b"one\ntwo\nthree\n").await.unwrap();
            for (i, body) in ["one", "two", "three"].into_iter().enumerate() {
                out.wait_for(|s| s.contains(&format!("-> bob (sending) {body}\n")))
                    .await;
                gate.wait_held(1).await;
                // One in flight: the next line waits for this one's answer.
                assert_eq!(sends(&fake).len(), i + 1);
                gate.release(1);
            }
            drop(input);
        };
        let (exit, ()) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::Done);
        assert_eq!(err.text(), "");
        assert_eq!(sends(&fake), ["one", "two", "three"]);
    })
    .await;
}

#[tokio::test]
async fn end_of_input_with_a_send_in_flight_still_sends_it() {
    bounded(async {
        let fake = fake_with_peers().await;
        let gated = Gated::new(&fake, HoldAt::Request);
        let gate = Arc::clone(&gated.gate);
        let c = Client::new(gated);
        let command = parse(&["chat", "bob"]);
        let (session, out, err, mut input) = chat(&c, &command);
        let drive = async {
            fake.wait_for_subscribers(1).await;
            input.write_all(b"last\nqueued\n").await.unwrap();
            gate.wait_held(1).await;
            drop(input);
            // The chat still shows what arrives while its sends finish.
            fake.receive("bob", "meanwhile").unwrap();
            tokio::time::timeout(
                WHILE_HELD,
                out.wait_for(|s| s.contains("<- bob meanwhile\n")),
            )
            .await
            .expect("nothing was shown after the end of input");
            gate.release(2);
        };
        let (exit, ()) = tokio::join!(session, drive);
        assert_eq!(exit, Exit::Done);
        assert_eq!(err.text(), "");
        assert_eq!(sends(&fake), ["last", "queued"]);
    })
    .await;
}

// Errors of the transport and of a stopped station.

/// A transport that answers every call and subscription with one error.
struct Failing(ApiError);

impl BindInstance for Failing {
    fn bound_to(&self, _instance_id: &str) -> Self {
        Failing(self.0.clone())
    }
}

impl Transport for Failing {
    async fn call(&self, _request: Request) -> Result<chief::api::Response, ApiError> {
        Err(self.0.clone())
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        Err(self.0.clone())
    }
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

#[tokio::test]
async fn transport_errors_exit_3() {
    bounded(async {
        let cases = [
            (
                ApiError::IncompatibleApi {
                    station: 2,
                    client: 1,
                },
                "the station speaks API version 2 and this chief speaks version 1; use the same \
                 release of both\n",
            ),
            (
                ApiError::Disconnected {
                    detail: "the station closed the connection".to_owned(),
                },
                "lost the connection to the station: the station closed the connection\n",
            ),
            (
                ApiError::Timeout { after_ms: 10_000 },
                "the station did not answer within 10000 ms\n",
            ),
            (
                ApiError::BadReply {
                    detail: "nonsense".to_owned(),
                },
                "unexpected reply from the station: nonsense\n",
            ),
            (
                ApiError::NotRunning {
                    socket: "/run/x.sock".to_owned(),
                },
                "no station is running (socket: /run/x.sock)\n\
                 start one with: station run --tower <URL>\n",
            ),
        ];
        for (error, message) in cases {
            let c = Client::new(Failing(error.clone()));
            for args in EVERY_COMMAND {
                assert_eq!(
                    exec(&c, args).await,
                    (Exit::NoStation, String::new(), message.to_owned()),
                    "{error:?} {args:?}"
                );
            }
        }
    })
    .await;
}

#[tokio::test]
async fn a_stopping_station_exits_3() {
    bounded(async {
        let fake = fake_with_peers().await;
        fake.stop();
        let c = Client::new(fake);
        for args in EVERY_COMMAND {
            assert_eq!(
                exec(&c, args).await,
                (
                    Exit::NoStation,
                    String::new(),
                    "the station is shutting down\n".to_owned()
                ),
                "{args:?}"
            );
        }
    })
    .await;
}

#[test]
fn exit_codes() {
    assert_eq!(
        [Exit::Done, Exit::Failed, Exit::Usage, Exit::NoStation].map(Exit::code),
        [0, 1, 2, 3]
    );
}

// Arguments.

fn cli(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("chief").chain(args.iter().copied()))
}

#[test]
fn every_command_parses() {
    let s = |v: &str| v.to_owned();
    let cases: [(&[&str], Command); 9] = [
        (&["id"], Command::Id),
        (&["status"], Command::Status),
        (
            &["peer", "add", "bob", "STATION-B"],
            Command::Peer {
                command: PeerCommand::Add {
                    name: s("bob"),
                    station_id: s("STATION-B"),
                },
            },
        ),
        (
            &["peer", "ls"],
            Command::Peer {
                command: PeerCommand::Ls,
            },
        ),
        (
            &["peer", "rm", "bob"],
            Command::Peer {
                command: PeerCommand::Rm { name: s("bob") },
            },
        ),
        (
            &["send", "bob", "on my way"],
            Command::Send {
                peer: s("bob"),
                text: s("on my way"),
            },
        ),
        (&["log"], Command::Log { peer: None }),
        (
            &["log", "bob"],
            Command::Log {
                peer: Some(s("bob")),
            },
        ),
        (&["chat", "bob"], Command::Chat { peer: s("bob") }),
    ];
    for (args, command) in cases {
        let parsed = cli(args).unwrap();
        assert_eq!(parsed.command, command, "{args:?}");
        assert_eq!(parsed.station, None);
    }
    // A text that starts with a dash, after `--`.
    assert_eq!(
        cli(&["send", "bob", "--", "-1"]).unwrap().command,
        Command::Send {
            peer: s("bob"),
            text: s("-1")
        }
    );
}

#[test]
fn station_option_before_or_after_the_command() {
    for args in [
        &["--station", "/tmp/s.sock", "id"][..],
        &["id", "--station", "/tmp/s.sock"],
        &["--station=/tmp/s.sock", "peer", "ls"],
        &["peer", "ls", "--station", "/tmp/s.sock"],
    ] {
        assert_eq!(
            cli(args).unwrap().station,
            Some(PathBuf::from("/tmp/s.sock")),
            "{args:?}"
        );
    }
}

#[test]
fn usage_errors() {
    for args in [
        &[][..],
        &["frobnicate"],
        &["peer"],
        &["peer", "add", "bob"],
        &["peer", "rm"],
        &["send", "bob"],
        &["send", "bob", "two", "words"],
        &["log", "bob", "alice"],
        &["chat"],
        &["id", "--station"],
        &["--bogus", "id"],
    ] {
        let error = cli(args).unwrap_err();
        assert_eq!(Exit::for_parse_error(&error), Exit::Usage, "{args:?}");
        assert_eq!(error.exit_code(), 2, "{args:?}");
    }
    for args in [
        &["--help"][..],
        &["help"],
        &["--version"],
        &["peer", "--help"],
    ] {
        let error = cli(args).unwrap_err();
        assert_eq!(Exit::for_parse_error(&error), Exit::Done, "{args:?}");
    }
    let help = cli(&["--help"]).unwrap_err().to_string();
    assert!(help.contains(AFTER_HELP), "{help}");
    assert_eq!(cli(&["--help"]).unwrap_err().kind(), ErrorKind::DisplayHelp);
}

// The socket path.

fn env(data: Option<&str>, runtime: Option<&str>, home: Option<&str>) -> PathEnv {
    PathEnv {
        xdg_data_home: data.map(PathBuf::from),
        xdg_runtime_dir: runtime.map(PathBuf::from),
        home: home.map(PathBuf::from),
    }
}

#[test]
fn socket_path_resolution() {
    let given = Path::new("/srv/s/station.sock");
    assert_eq!(
        socket_path(Some(given), &env(None, None, None)).unwrap(),
        given
    );
    assert_eq!(
        socket_path(Some(given), &env(Some("/d"), Some("/r"), Some("/h"))).unwrap(),
        given
    );
    assert_eq!(
        socket_path(None, &env(Some("/d"), None, Some("/h"))).unwrap(),
        Path::new("/d/thecompany/station/station.sock")
    );
    assert_eq!(
        socket_path(None, &env(None, None, Some("/h"))).unwrap(),
        Path::new("/h/.local/share/thecompany/station/station.sock")
    );
    assert_eq!(
        socket_path(None, &env(Some("/d"), Some("/r"), Some("/h"))).unwrap(),
        Path::new("/r/thecompany/station.sock")
    );
    let no_home = socket_path(None, &env(None, Some("/r"), None)).unwrap_err();
    assert!(no_home.contains("--station"), "{no_home}");
    assert!(no_home.contains("HOME"), "{no_home}");
    let relative = socket_path(None, &env(Some("rel"), None, Some("also-rel"))).unwrap_err();
    assert!(relative.contains("--station"), "{relative}");
    let long = format!("/{}/station.sock", "x".repeat(200));
    let too_long = socket_path(Some(Path::new(&long)), &env(None, None, None)).unwrap_err();
    assert!(
        too_long.starts_with("cannot use the socket path: "),
        "{too_long}"
    );
}

#[test]
fn a_socket_path_error_names_only_what_chief_has() {
    let long_dir = format!("/{}", "x".repeat(200));
    let long = format!("{long_dir}/station.sock");
    let cases = [
        // Given with --station.
        (
            socket_path(Some(Path::new("")), &env(None, None, None)),
            "cannot use the socket path: the socket path is empty; pass another --station PATH"
                .to_owned(),
        ),
        (
            socket_path(Some(Path::new(&long)), &env(None, None, None)),
            format!(
                "cannot use the socket path: {long} is {} bytes long, but a Unix socket path \
                 can be at most {} bytes here; pass another --station PATH",
                long.len(),
                chief::paths::SOCKET_PATH_MAX_BYTES
            ),
        ),
        // The default, from the environment.
        (
            socket_path(None, &env(None, Some(&long_dir), Some("/h"))),
            format!(
                "cannot use the socket path: {long_dir}/thecompany/station.sock is {} bytes \
                 long, but a Unix socket path can be at most {} bytes here; pass --station PATH, \
                 or set XDG_RUNTIME_DIR to a shorter directory",
                long_dir.len() + "/thecompany/station.sock".len(),
                chief::paths::SOCKET_PATH_MAX_BYTES
            ),
        ),
    ];
    for (result, expected) in cases {
        let message = result.unwrap_err();
        assert_eq!(message, expected);
        assert!(!message.contains("--data-dir"), "{message}");
    }
}
