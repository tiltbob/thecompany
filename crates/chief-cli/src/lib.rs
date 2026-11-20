//! `chief_cli`: the `chief` command-line frontend, as a library.
//!
//! The binary (`src/main.rs`) only parses the arguments, works out the socket path with
//! [`socket_path`], builds a [`chief::client::SocketTransport`] and calls [`run`] with the real
//! stdin, stdout and stderr. Everything else is here, generic over the transport
//! ([`BindInstance`]), so that tests run every command in-process against a fake station and
//! capture what it prints.
//!
//! # Commands
//!
//! ```text
//! chief [--station PATH] <command>
//!   id                             this station's id
//!   status                         the bootstrap domain, the state and source of each tower, then
//!                                  the link to each peer station
//!   peer add <name> <station-id>   adds a station to a peer; prints the peer as stored
//!   peer ls                        one line per peer station
//!   peer rm <name>                 removes a peer
//!   tower candidates [--after-peer PEER] [--limit N]  one live page of candidate URLs
//!   tower add <url>                explicitly adds a tower to the installed set
//!   tower remove <url>             explicitly removes a tower from the installed set
//!   send <peer> <text>             sends a message; once the station accepted it, waits at most
//!                                  120 s for its outcome
//!   log [peer]                     the whole message log, oldest first
//!   chat <peer>                    the last 50 messages of all peers, then live; each line read
//!                                  from stdin is sent to <peer>; end of input ends the chat
//! ```
//!
//! # One station instance per command
//!
//! A station gets a new instance id on every start (rule 1 in `chief::api`). A command that makes
//! more than one call reads the status first and binds its client to that instance
//! ([`chief::client::BindInstance`]): `status` reads the peers, `log` every page, `send` its
//! subscription and its send, and `chat` its follower and every send through the bound client. If
//! the station restarts in between, the next call fails before it reaches the new instance, and
//! the command ends with exit 3 and one line, as for a lost connection (`send` and `chat` add the
//! lines below): it never mixes the answers of two instances, and never sends a typed line to a
//! station other than the one whose messages it shows. `id`, the `peer` commands and the `tower`
//! commands each make one call.
//!
//! # log
//!
//! `log` reads the whole log page by page and prints it only once every page has been read. It
//! checks every page against rule 6 in `chief::api`: its messages ascend by `seq`, lie below the
//! cursor it was asked with, and its cursor is the `seq` of its first message; a page with a
//! cursor is not empty. So the cursor moves back with every page. It stops at
//! [`LOG_MAX_MESSAGES`] (100,000) messages, or at [`LOG_MAX_BYTES`] (64 MiB) of text owned by the
//! messages (bodies, peer names and ids), counted before a page is kept; both are far more than a
//! station keeps. A page that breaks a rule, or a log that runs past either bound, ends `log` with
//! exit 3, one line, and nothing printed.
//!
//! # send
//!
//! `send` reads the status, subscribes, sends the message and waits for its outcome. The calls
//! are bounded by the transport's own timeouts; the wait for the outcome, once the station has
//! accepted the message, by [`SEND_OUTCOME_WAIT`] (120 s). It tells three cases apart:
//!
//! - Delivered: `delivered to <peer>` on stdout, exit 0, also when stdout no longer takes the
//!   line.
//! - Not delivered, exit 1: the station refused the message (an unknown peer, an invalid body), so
//!   nothing was sent; or the station reported that it failed (`<peer> appears offline, or has
//!   not added you`, `<peer> refused the message`, `<peer> was removed before the message was
//!   delivered`).
//! - Unknown, exit 3, and stderr ends with one of two sentences. The send got no answer it could
//!   read (it timed out, the connection broke, or the answer was nonsense): its error, then ``the
//!   message may still have been sent; check `chief log <peer>` before sending it again``. Or the
//!   station accepted the message and the wait then failed (the station stopped or restarted, the
//!   message left the log before its outcome was seen, or 120 s passed): the error, or `the
//!   station did not report the outcome within 120 s`, then ``the station accepted the message,
//!   but its outcome is unknown; check `chief log <peer>` before sending it again``.
//!
//! Any other exit 3 (no station is running, it speaks another API version, it is shutting down,
//! or it restarted and the restart was noticed before the send was attempted, by the subscribe)
//! means that nothing was sent; its error may take more than one line. A restart noticed by the
//! send call itself ends with the may-have-been-sent sentence although nothing was written: the
//! bound call refuses the new instance with the same error as a lost connection, and the CLI
//! cannot tell the two apart. Once the station has accepted the message, `send` never exits 1
//! unless the station reported it failed.
//!
//! # chat
//!
//! `chat` keeps showing what the station sends while a typed line is being sent. One line is sent
//! at a time; lines typed meanwhile wait, in order: at most 64, and the input is not read while
//! that many wait. A line ends at a newline or, for the last one, at the end of the input. The
//! newline is not part of it, and one carriage return at its end is dropped, whichever way it
//! ends (`ab\r` sends `ab`, `ab\r\r` sends `ab\r`); a carriage return anywhere else is kept. A
//! line of at most 16384 bytes that is empty or holds only white space is not sent, and counts
//! for nothing.
//!
//! A line that cannot be sent prints one line on stderr, and the chat goes on:
//!
//! - one the station refuses (an unknown peer, an invalid body): the station's error;
//! - one longer than [`BODY_MAX_BYTES`] (16384) bytes, whatever it holds, also only spaces: `a
//!   line is longer than 16384 bytes and was not sent` (the chat skips it to its end and keeps
//!   none of it, so it cannot tell whether it was blank);
//! - one that is not valid UTF-8: `a line is not valid UTF-8 and was not sent`.
//!
//! If the input cannot be read, `cannot read the input: <error>`, and the chat goes on as at the
//! end of the input; a line read only in part is dropped.
//!
//! When the station cannot be talked to any more (it stopped, is shutting down, restarted, or the
//! connection broke), noticed by a send or by the live view, whichever comes first, the chat ends
//! once, with one line on stderr and exit 3. Whenever a chat ends before the end of its input, for
//! this or any other error, that line is followed by what was left undone:
//!
//! - ``the last line may have been sent; check `chief log <peer>` `` when the send of a line had
//!   started and had no answer yet, or ended without one: it timed out, the connection broke, or
//!   the answer was nonsense (also when that send's own error is what ends the chat);
//! - then `1 typed line was not sent` or `<N> typed lines were not sent`, counting the lines
//!   still waiting, a line whose send had not started yet, and a line the station refused because
//!   it is going away (`Stopping`, `NotRunning`, `IncompatibleApi`): none of those went anywhere.
//!
//! End of input ends `chat` once the line being sent and the lines still waiting have been sent
//! (each bounded by the transport's call timeout); what the station shows meanwhile is still
//! printed. The end of the input is read once, also when it ends a last line without a newline:
//! the chat does not read the input again after it. The exit is then 0 if the station accepted
//! every line that was sent, and no line was too long or not UTF-8, and the input did not fail;
//! otherwise 1. Blank lines (of at most 16384 bytes) do not count. The outcome of the last
//! message is not waited for (it is in `log`); `send` is the command that waits for the outcome.
//!
//! # Output (stable: scripts parse it)
//!
//! Every line goes through [`say`], which applies rule 9 of `chief::api` once more, as the last
//! place to find out that a station did not (hard rule 8): every control character, Unicode line
//! or paragraph separator and bidirectional control becomes a space (the set is
//! [`chief::api::is_replaced`]). Nothing a peer or a tower wrote can move the cursor, clear the
//! screen, forge a line, or reorder the fields of a line. Fields are separated by one space; peer
//! names, station ids, URLs, times and status words contain no space, so only the last field of a
//! line may.
//!
//! - `id`: the station id, alone on its line.
//! - A message (`log`, `chat`): `<seq> <time> <arrow> <peer> [(<status>)] <body>`. The time is UTC,
//!   `YYYY-MM-DDTHH:MM:SSZ` ([`format_time`]). The arrow is `->` for an outgoing message and `<-`
//!   for an incoming one. An outgoing message has its status in parentheses before the body:
//!   `sending`, `delivered`, `failed: no answer`, `failed: refused` or `failed: peer removed`; an
//!   incoming one has none. The body is last and runs to the end of the line:
//!
//!   ```text
//!   12 2023-11-14T22:13:20Z -> bob (delivered) on my way
//!   13 2023-11-14T22:13:25Z <- bob see you there
//!   ```
//!
//! - A change of an outgoing message (`chat` only): `<seq> -> <peer> now (<status>)`, without the
//!   body. Its second field is an arrow, where a message line has a time.
//! - `status`: first `domain <domain>`, only when the station knows the DNS domain it bootstraps
//!   its towers from (SRV). Then `tower <url> <state>[ (<source>)][: <detail>]` per tower, in the
//!   station's order. The state is `connecting`, `connected`, `standby` (a tower of the station's
//!   set whose state is not observed), `failing` or `denied`. The source, in parentheses, says how
//!   the station learned of the tower: `configured`, `remembered`, `srv` or `peer`; a station that
//!   does not say has no parentheses. The detail, after a colon, is a failing tower's error or a
//!   denied tower's reason, as the tower gave it (a tower of this project denies with `not on this
//!   tower's list`). From an error, a reason and the domain, leading and trailing white space and
//!   the characters of rule 9 are removed; one with nothing left after that is left out, with its
//!   colon (or, for the domain, its line). The URL is always one field: every white-space
//!   character left in it after rule 9 becomes `%20`, and an empty URL prints as `-`. Then
//!   `peer <name> <station-id> <link>` per peer station, with the link `down`, `relayed` or
//!   `direct`. `no towers` stands for an empty list of towers, `no peers` for an empty peer book:
//!
//!   ```text
//!   domain team.example.invalid
//!   tower https://tower-a.example.invalid/ connected (srv)
//!   tower https://tower-c.example.invalid/ failing (configured): connection refused
//!   tower https://tower-d.example.invalid/ denied (peer): not on this tower's list
//!   peer bob STATION-B relayed
//!   ```
//! - `peer ls` and `peer add`: `<name> <station-id> <link>` per peer station; `peer ls` says
//!   `no peers` when the book is empty.
//! - `peer rm`: `removed <name>`. `send`: `delivered to <peer>`. An empty `log`: `no messages`.
//!
//! - `tower candidates`: `candidate <peer> <url>` per entry. If more groups remain, one
//!   `next-after-peer <peer>` line follows. The empty boundary prints as `""`. Peer names,
//!   URLs and the continuation each occupy one field.
//! - `tower add`: `added <url>` or `<url> is already in the set`.
//! - `tower remove`: `removed <url>` or `<url> is not in the set`.
//!
//! Errors go to stderr as plain sentences, one per line, starting lowercase.
//!
//! # Exit codes
//!
//! See [`Exit`]: 0 done (for `send`: delivered); 1 the station refused or the operation failed
//! (for `send`: refused, so not sent, or failed, so not delivered; for `chat`: at the end of the
//! input, a line was refused or not sent, or the input failed); 2 usage error; 3 cannot talk
//! to a station (no socket path could be worked out, none is running, it speaks another API
//! version, it is shutting down, the connection broke, it stopped answering or answered
//! nonsense), or the station restarted during the command (`log`, `status`, `send`), or stopped
//! during a `chat`; for `send` also an unknown outcome (see "send" above).
//!
//! # Process-global state
//!
//! None here (hard rule 6): no runtime, no environment reads, no signal handlers. The binary reads
//! the environment once through [`PathEnv::read`] and owns the runtime.

use chief::api::{
    ApiError, BODY_MAX_BYTES, Direction, FailReason, LOG_PAGE_MAX_MESSAGES, Link, LogPage, Message,
    MessageStatus, Peer, TOWER_CANDIDATES_PAGE_MAX_GROUPS, TowerAddOutcome, TowerRemoveOutcome,
    TowerSource, TowerState, TowerStatus, clean,
};
use chief::client::{BindInstance, Client};
use chief::follow::LogUpdate;
use chief::paths::{PathEnv, check_socket_path};
use clap::{Parser, Subcommand};
use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

/// How many messages `chat` shows before it goes live.
pub const CHAT_TAIL: u32 = 50;

/// How many typed lines `chat` reads ahead while one is being sent. Further lines stay unread in
/// the input until there is room.
pub const CHAT_QUEUE_MAX: usize = 64;

/// How many messages `log` reads at most, far above what a station keeps (M1 keeps 1,000): a
/// station whose pages add up to more is answering nonsense ([`ApiError::BadReply`]).
pub const LOG_MAX_MESSAGES: usize = 100_000;

/// How many bytes of text the messages of a `log` may own in all (body, peer name and id): 64 MiB,
/// four times the largest log a station keeps (1,000 messages of at most 16 KiB). A station whose
/// pages add up to more is answering nonsense ([`ApiError::BadReply`]).
pub const LOG_MAX_BYTES: usize = 64 << 20;

/// How long `send` waits for the outcome of a message the station accepted, before it gives up
/// with exit 3: a bound of its own, whatever the station's send deadline is.
pub const SEND_OUTCOME_WAIT: Duration = Duration::from_secs(120);

/// The text after the options in `--help`: the exit codes and where the socket is.
pub const AFTER_HELP: &str = "\
Exit status:
  0  done; for send: the message was delivered
  1  the station refused or the operation failed; for send: the station refused the message
     (it was not sent) or reported that it failed (it was not delivered); for chat: at the end
     of the input, a line was refused, too long or not UTF-8, or the input failed
  2  usage error
  3  cannot talk to a station: no socket path could be worked out, none is running, it speaks
     another API version, it is shutting down, the connection broke, it stopped answering, it
     answered nonsense (such as a malformed page of the log, or a log of more than 100,000
     messages or 64 MiB of text), or it restarted during the command; for send: also when the
     outcome is unknown (see chief send --help); for chat: the station stopped or restarted

Without --station, the socket is $XDG_RUNTIME_DIR/thecompany/station.sock when XDG_RUNTIME_DIR is
set, otherwise station.sock in the station's data dir, $XDG_DATA_HOME/thecompany/station (or
~/.local/share/thecompany/station).";

/// The command line of `chief`.
#[derive(Debug, Clone, PartialEq, Eq, Parser)]
#[command(
    name = "chief",
    version,
    about = "Talk to your station: peers, messages, chat.",
    after_help = AFTER_HELP
)]
pub struct Cli {
    /// The station's socket, instead of the default one
    #[arg(long, value_name = "PATH", global = true)]
    pub station: Option<PathBuf>,
    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// A command of `chief`.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum Command {
    /// Print this station's id, to hand to the other chief
    Id,
    /// Show the bootstrap domain, each tower's state and source, then each peer station's link
    ///
    /// The first line is `domain <domain>` when the station finds its towers through a domain.
    /// Then one line per tower: `tower <url> <state>[ (<source>)][: <detail>]`.
    ///
    /// States: connecting; connected; standby (in the station's set, state not observed); failing
    /// (the last attempt failed, the station keeps trying; its error follows); denied (the tower
    /// refused this station, and trying again does not help until the tower's operator changes
    /// that; the tower's reason follows; a tower of this project says "not on this tower's list":
    /// give its operator the id that `chief id` prints).
    ///
    /// Tower sources include configured (given at startup or added by the chief without a matching
    /// peer announcement), remembered (stored by an earlier run), srv (found through the domain's
    /// DNS record), and peer (named by a peer and added by the chief).
    ///
    /// Then one line per peer station: `peer <name> <station-id> <link>`.
    Status,
    /// Manage the peer book
    Peer {
        /// What to do with the peer book.
        #[command(subcommand)]
        command: PeerCommand,
    },
    /// List candidates or change the installed tower set
    Tower {
        /// What to do with the towers.
        #[command(subcommand)]
        command: TowerCommand,
    },
    /// Send a message and wait for the outcome
    ///
    /// Once the station has accepted the message, waits at most 120 s for its outcome (the calls
    /// before that have the transport's own timeouts). Exit 0: delivered. Exit 1: the station
    /// refused the message, so it was not sent, or reported that it failed.
    ///
    /// Exit 3: the outcome is unknown only if stderr ends with "the message may still have been
    /// sent; ..." (the send got no answer, or a restart of the station refused it: then nothing
    /// was written, but the two look alike) or "the station accepted the message, but its outcome
    /// is unknown; ..." (the station accepted it, then stopped, restarted, lost it from its log,
    /// or did not report the outcome within 120 s); check `chief log <peer>` before sending it
    /// again. Any other exit 3 means that no station could be talked to and nothing was sent.
    Send {
        /// The peer's name
        peer: String,
        /// The message (quote it if it has spaces)
        text: String,
    },
    /// Print the whole message log, oldest first, and exit
    Log {
        /// Only the messages exchanged with this peer
        peer: Option<String>,
    },
    /// Show the last 50 messages, then follow live; each line typed is sent to the peer
    ///
    /// Lines are sent one at a time, in order, while the chat keeps showing new messages. One
    /// carriage return at the end of a line is dropped. An empty or blank line of at most 16384
    /// bytes is ignored. A line longer than 16384 bytes (even if it is only spaces), or not valid
    /// UTF-8, is not sent: one line on stderr, and the chat goes on; so does a line the station
    /// refuses.
    ///
    /// End of input (one Ctrl-D) ends the chat once the lines typed so far have been sent,
    /// without waiting for their outcome; `chief send` is the command that waits for it. Nothing
    /// typed after the end of input is read. The exit is then 0 if the station accepted every
    /// line sent and no line was too long or not UTF-8, and 1 if the station refused a line, a
    /// line was not sent, or the input could not be read.
    ///
    /// The chat ends with exit 3 when the station stops or restarts. Then stderr also says if the
    /// last line may have been sent (check `chief log <peer>`) and how many typed lines were not
    /// sent.
    Chat {
        /// The peer that typed lines go to (messages of every peer are shown)
        peer: String,
    },
}

/// A `chief peer` command.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum PeerCommand {
    /// Add a station to a peer, creating the peer if needed; prints the peer as stored
    Add {
        /// The peer's name: letters, digits, '.', '_' and '-'
        name: String,
        /// The id the other chief's `chief id` printed
        station_id: String,
    },
    /// List the peers, one line per peer station
    Ls,
    /// Remove a peer and close its links; its messages stay in the log
    Rm {
        /// The peer's name
        name: String,
    },
}

/// A `chief tower` command.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum TowerCommand {
    /// Read one live page of candidate towers
    ///
    /// Prints `candidate <peer> <url>` per URL. If another group remains, the last line is
    /// `next-after-peer <peer>`. Pass that peer name to `--after-peer` for the next page. New
    /// announcements or a station restart can change later pages. A zero limit probes for another
    /// group without listing any. To continue after a probe, raise the limit and keep your
    /// original boundary, omitting `--after-peer` if it was omitted for the probe. An empty
    /// boundary is displayed as `""`.
    Candidates {
        /// Only peer names strictly after this boundary
        #[arg(long, value_name = "PEER")]
        after_peer: Option<String>,
        /// Maximum peer groups to read, capped at 32 (0 probes for more)
        #[arg(long, value_name = "N", default_value_t = TOWER_CANDIDATES_PAGE_MAX_GROUPS)]
        limit: u32,
    },
    /// Explicitly add a tower and print its canonical URL and outcome
    Add {
        /// Tower URL to add
        url: String,
    },
    /// Explicitly remove a tower and print its canonical URL and outcome
    ///
    /// SRV discovery can bring a removed URL back into the set.
    Remove {
        /// Tower URL to remove
        url: String,
    },
}

/// How `chief` ends. [`Exit::code`] is the process's exit status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// 0: done. For `send`: the message was delivered.
    Done,
    /// 1: the station refused or the operation failed: an unknown peer, an invalid name or body,
    /// any error the station itself answers with. For `send`: the station refused the message (it
    /// was not sent) or reported that it failed (it was not delivered). For `chat`, at the end of
    /// the input: a line was refused, too long or not UTF-8, or the input failed.
    Failed,
    /// 2: a usage error, reported by clap.
    Usage,
    /// 3: cannot talk to a station: none is running, it speaks another API version, it is
    /// shutting down, the connection broke, it did not answer in time or answered nonsense. Also
    /// when the station restarted during a command of several calls (the next call is refused
    /// before it reaches the new instance), when a `chat` ends because the station stopped, when
    /// no socket path can be worked out, and when the outcome of a `send` is unknown (see the
    /// crate docs).
    NoStation,
}

impl Exit {
    /// The exit status: 0, 1, 2 or 3.
    pub fn code(self) -> u8 {
        match self {
            Self::Done => 0,
            Self::Failed => 1,
            Self::Usage => 2,
            Self::NoStation => 3,
        }
    }

    /// The exit for an error of [`Cli::try_parse`]: [`Exit::Done`] for `--help` and `--version`,
    /// which clap reports as errors, [`Exit::Usage`] for everything else.
    pub fn for_parse_error(error: &clap::Error) -> Self {
        if error.use_stderr() {
            Self::Usage
        } else {
            Self::Done
        }
    }

    /// The exit for a station API error: [`Exit::NoStation`] for the errors that mean the station
    /// cannot be talked to, [`ApiError::Stopping`] included (nothing was done), [`Exit::Failed`]
    /// for every other error the station answers with.
    pub fn for_error(error: &ApiError) -> Self {
        match error {
            ApiError::NotRunning { .. }
            | ApiError::IncompatibleApi { .. }
            | ApiError::Disconnected { .. }
            | ApiError::Timeout { .. }
            | ApiError::BadReply { .. }
            | ApiError::Stopping => Self::NoStation,
            ApiError::PeerNotFound { .. }
            | ApiError::StationTaken { .. }
            | ApiError::MessageNotFound { .. }
            | ApiError::Invalid { .. }
            | ApiError::TowersPinned
            | ApiError::TowerSetFull { .. }
            | ApiError::Internal { .. }
            | ApiError::BadRequest { .. } => Self::Failed,
        }
    }
}

impl From<Exit> for std::process::ExitCode {
    fn from(exit: Exit) -> Self {
        Self::from(exit.code())
    }
}

/// The socket `chief` talks to: `station` (from `--station`) if given, otherwise the default
/// socket for `env` ([`PathEnv::default_data_dir`], then [`PathEnv::socket_path`]).
///
/// Fails, with a sentence for the user that names `--station`, when no default data dir can be
/// worked out (neither `XDG_DATA_HOME` nor `HOME` is an absolute path) or the path does not fit a
/// Unix socket address. For a path that does not fit, the sentence keeps the reason of
/// [`check_socket_path`] and replaces its advice, which is for the daemon, with `chief`'s own:
/// another `--station PATH` when one was given, otherwise `--station PATH` or a shorter
/// `XDG_RUNTIME_DIR` (the default socket is under it when it is set). The caller prints it and
/// exits with [`Exit::NoStation`].
pub fn socket_path(station: Option<&Path>, env: &PathEnv) -> Result<PathBuf, String> {
    let path = match station {
        Some(path) => path.to_path_buf(),
        None => {
            let data_dir = env.default_data_dir().ok_or_else(|| {
                "cannot work out where the station's socket is: neither XDG_DATA_HOME nor HOME \
                 is set to an absolute path; pass --station PATH"
                    .to_owned()
            })?;
            env.socket_path(&data_dir)
        }
    };
    check_socket_path(&path).map_err(|e| match e {
        ApiError::Invalid { reason, .. } => {
            // The library's reason ends with advice for the daemon (`--data-dir`), which `chief`
            // does not have: keep the reason, give advice of its own.
            let reason = reason.rsplit_once("; ").map_or(reason.as_str(), |(r, _)| r);
            let advice = if station.is_some() {
                "pass another --station PATH"
            } else {
                "pass --station PATH, or set XDG_RUNTIME_DIR to a shorter directory"
            };
            format!("cannot use the socket path: {reason}; {advice}")
        }
        other => other.to_string(),
    })?;
    Ok(path)
}

/// Runs one command against the station behind `client` and returns how `chief` ends.
///
/// Normal output goes to `out`, errors to `err`, every line through [`say`]. `input` is read only
/// by `chat`, one line per message; the binary passes stdin, tests pass whatever they like.
///
/// The transport must be able to bind itself to one station instance ([`BindInstance`]): a
/// command of several calls (`status`, `log`, `send`, `chat`) reads the status first and makes
/// every later call through a client bound to that instance (see "One station instance per
/// command" in the crate docs). `client` itself stays unbound.
///
/// Exit codes ([`Exit`]): 0 done; 1 the station refused; 3 cannot talk to the station, which
/// includes a station that is shutting down, the bound instance being gone (the station restarted
/// mid-command), a log page of the wrong shape or a log longer than [`LOG_MAX_MESSAGES`]
/// ([`ApiError::BadReply`]), a `send` whose outcome is unknown (it got no answer, or the station
/// accepted it and the wait for the outcome failed or passed [`SEND_OUTCOME_WAIT`]), and a `chat`
/// that ends because the station stopped. At the end of
/// `input`, once the lines read so far have been sent, `chat` returns 0 if the station accepted
/// every non-empty line, and 1 if it refused one, a line was too long or not UTF-8, or `input`
/// failed.
pub async fn run<T, I>(
    command: &Command,
    client: &Client<T>,
    input: I,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Exit
where
    T: BindInstance,
    I: AsyncBufRead + Unpin,
{
    let result = match command {
        Command::Id => id(client, out).await,
        Command::Status => status(client, out).await,
        Command::Peer { command } => match command {
            PeerCommand::Add { name, station_id } => peer_add(client, name, station_id, out).await,
            PeerCommand::Ls => peer_ls(client, out).await,
            PeerCommand::Rm { name } => peer_rm(client, name, out).await,
        },
        Command::Tower { command } => match command {
            TowerCommand::Candidates { after_peer, limit } => {
                tower_candidates(client, after_peer.as_deref(), *limit, out).await
            }
            TowerCommand::Add { url } => tower_add(client, url, out).await,
            TowerCommand::Remove { url } => tower_remove(client, url, out).await,
        },
        Command::Send { peer, text } => send(client, peer, text, out, err).await,
        Command::Log { peer } => log(client, peer.as_deref(), out).await,
        Command::Chat { peer } => chat(client, peer, input, out, err).await,
    };
    match result {
        Ok(exit) => exit,
        Err(Stop::Api(error)) => report(err, &error),
        Err(Stop::Output(error)) => {
            // stdout is gone (a closed pipe, a full disk): say so if stderr still works.
            let _ = say(err, &format!("cannot write the output: {error}"));
            Exit::Failed
        }
    }
}

/// Writes `text` and a newline to `w`, after applying rule 9 of `chief::api` to `text` once
/// more with [`clean`]: every control character, every Unicode line or paragraph separator and
/// every bidirectional control becomes one space ([`chief::api::is_replaced`] defines the set).
///
/// The one way this crate prints anything, so that no text from a station, a peer or a tower
/// reaches the terminal unfiltered (hard rule 8). The station applies rule 9 to peer text
/// already; a terminal is the last place to find out it did not.
pub fn say(w: &mut dyn Write, text: &str) -> io::Result<()> {
    writeln!(w, "{}", clean(text))?;
    w.flush()
}

/// Formats unix milliseconds as UTC `YYYY-MM-DDTHH:MM:SSZ`, dropping the milliseconds. Years past
/// 9999 get more digits.
pub fn format_time(at_ms: u64) -> String {
    let secs = at_ms / 1000;
    let (days, of_day) = (secs / 86_400, secs % 86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        of_day / 3600,
        of_day % 3600 / 60,
        of_day % 60
    )
}

/// The proleptic Gregorian date `days` days after 1970-01-01, as (year, month, day): Howard
/// Hinnant's `civil_from_days`, for non-negative days only.
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    // Shift the epoch to 0000-03-01, so that a leap day is the last day of its year.
    let z = days + 719_468;
    let era = z / 146_097;
    let day_of_era = z % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

/// One message as a line of `log` and `chat` (see the module docs), before [`say`] cleans it.
pub fn format_message(message: &Message) -> String {
    let head = format!(
        "{} {} {} {}",
        message.seq,
        format_time(message.at_ms),
        arrow(message.direction),
        message.peer
    );
    match status_label(&message.status) {
        Some(status) => format!("{head} ({status}) {}", message.body),
        None => format!("{head} {}", message.body),
    }
}

/// The line `chat` prints when an outgoing message changed: `<seq> -> <peer> now (<status>)`.
pub fn format_change(message: &Message) -> String {
    let status = status_label(&message.status).unwrap_or_else(|| "received".to_owned());
    format!(
        "{} {} {} now ({status})",
        message.seq,
        arrow(message.direction),
        message.peer
    )
}

fn arrow(direction: Direction) -> &'static str {
    match direction {
        Direction::Outgoing => "->",
        Direction::Incoming => "<-",
    }
}

/// The status shown in parentheses; `None` for an incoming message.
fn status_label(status: &MessageStatus) -> Option<String> {
    Some(match status {
        MessageStatus::Sending => "sending".to_owned(),
        MessageStatus::Delivered => "delivered".to_owned(),
        MessageStatus::Failed { reason } => format!("failed: {}", reason_label(*reason)),
        MessageStatus::Received => return None,
    })
}

fn reason_label(reason: FailReason) -> &'static str {
    match reason {
        FailReason::NoAnswer => "no answer",
        FailReason::Refused => "refused",
        FailReason::PeerRemoved => "peer removed",
    }
}

fn link_label(link: Link) -> &'static str {
    match link {
        Link::Down => "down",
        Link::Relayed => "relayed",
        Link::Direct => "direct",
    }
}

fn source_label(source: TowerSource) -> &'static str {
    match source {
        TowerSource::Configured => "configured",
        TowerSource::Remembered => "remembered",
        TowerSource::Srv => "srv",
        TowerSource::Peer => "peer",
    }
}

/// `text` with rule 9 applied ([`clean`]) and white space trimmed at both ends; `None` when
/// nothing is left, so that the text is left out with its colon.
fn shown_text(text: &str) -> Option<String> {
    let text = clean(text);
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_owned())
}

/// One display field with rule 9 applied. Each remaining white-space character becomes
/// `%20`, and an empty field becomes `-`. Canonical URLs and valid peer names stay intact.
fn line_token(text: &str) -> String {
    let mut token = String::with_capacity(text.len());
    for c in clean(text).chars() {
        if c.is_whitespace() {
            token.push_str("%20");
        } else {
            token.push(c);
        }
    }
    if token.is_empty() {
        token.push('-');
    }
    token
}

/// One tower as a line of `status`, before [`say`] cleans it:
/// `tower <url> <state>[ (<source>)][: <detail>]` (see the module docs).
fn tower_line(tower: &TowerStatus) -> String {
    let (state, detail) = match &tower.state {
        TowerState::Connecting => ("connecting", None),
        TowerState::Connected => ("connected", None),
        TowerState::Standby => ("standby", None),
        TowerState::Failing { error } => ("failing", shown_text(error)),
        TowerState::Denied { reason } => ("denied", shown_text(reason)),
    };
    let mut line = format!("tower {} {state}", line_token(&tower.url));
    if let Some(source) = tower.source {
        line.push_str(" (");
        line.push_str(source_label(source));
        line.push(')');
    }
    if let Some(detail) = detail {
        line.push_str(": ");
        line.push_str(&detail);
    }
    line
}

/// `<name> <station-id> <link>`, one per station of `peer`.
fn peer_lines(peer: &Peer) -> impl Iterator<Item = String> + '_ {
    peer.stations
        .iter()
        .map(|s| format!("{} {} {}", peer.name, s.station_id, link_label(s.link)))
}

/// Why a command stopped early.
enum Stop {
    /// The station, or the transport to it, failed.
    Api(ApiError),
    /// Writing to `out` failed.
    Output(io::Error),
}

impl From<ApiError> for Stop {
    fn from(error: ApiError) -> Self {
        Self::Api(error)
    }
}

impl From<io::Error> for Stop {
    fn from(error: io::Error) -> Self {
        Self::Output(error)
    }
}

type Outcome = Result<Exit, Stop>;

/// Prints `error` for the user and returns its exit.
fn report(err: &mut dyn Write, error: &ApiError) -> Exit {
    let lines = match error {
        ApiError::NotRunning { socket } => vec![
            format!("no station is running (socket: {socket})"),
            "start one with: station run --tower <URL>".to_owned(),
        ],
        ApiError::IncompatibleApi { station, client } => vec![format!(
            "the station speaks API version {station} and this chief speaks version {client}; \
             use the same release of both"
        )],
        other => vec![other.to_string()],
    };
    for line in lines {
        // Nothing more can be done if stderr is gone too.
        let _ = say(err, &line);
    }
    Exit::for_error(error)
}

async fn id<T: BindInstance>(client: &Client<T>, out: &mut dyn Write) -> Outcome {
    let status = client.status().await?;
    say(out, &status.station_id)?;
    Ok(Exit::Done)
}

async fn status<T: BindInstance>(client: &Client<T>, out: &mut dyn Write) -> Outcome {
    let status = client.status().await?;
    // The peers of the instance whose towers are shown, or none.
    let peers = client.bound_to(&status.instance_id).peers().await?;
    if let Some(domain) = status.bootstrap_domain.as_deref().and_then(shown_text) {
        say(out, &format!("domain {domain}"))?;
    }
    if status.towers.is_empty() {
        say(out, "no towers")?;
    }
    for tower in &status.towers {
        say(out, &tower_line(tower))?;
    }
    if peers.is_empty() {
        say(out, "no peers")?;
    }
    for line in peers.iter().flat_map(peer_lines) {
        say(out, &format!("peer {line}"))?;
    }
    Ok(Exit::Done)
}

async fn peer_add<T: BindInstance>(
    client: &Client<T>,
    name: &str,
    station_id: &str,
    out: &mut dyn Write,
) -> Outcome {
    let peer = client.peer_add(name, station_id).await?;
    for line in peer_lines(&peer) {
        say(out, &line)?;
    }
    Ok(Exit::Done)
}

async fn peer_ls<T: BindInstance>(client: &Client<T>, out: &mut dyn Write) -> Outcome {
    let peers = client.peers().await?;
    if peers.is_empty() {
        say(out, "no peers")?;
    }
    for line in peers.iter().flat_map(peer_lines) {
        say(out, &line)?;
    }
    Ok(Exit::Done)
}

async fn peer_rm<T: BindInstance>(client: &Client<T>, name: &str, out: &mut dyn Write) -> Outcome {
    client.peer_remove(name).await?;
    say(out, &format!("removed {name}"))?;
    Ok(Exit::Done)
}

async fn tower_candidates<T: BindInstance>(
    client: &Client<T>,
    after_peer: Option<&str>,
    limit: u32,
    out: &mut dyn Write,
) -> Outcome {
    let page = client.tower_candidates(after_peer, limit).await?;
    for group in page.groups {
        let peer = line_token(&group.peer);
        for candidate in group.candidates {
            say(
                out,
                &format!("candidate {peer} {}", line_token(&candidate.url)),
            )?;
        }
    }
    if let Some(after_peer) = page.next_after_peer {
        let cursor = if after_peer.is_empty() {
            r#""""#.to_owned()
        } else {
            line_token(&after_peer)
        };
        say(out, &format!("next-after-peer {cursor}"))?;
    }
    Ok(Exit::Done)
}

async fn tower_add<T: BindInstance>(client: &Client<T>, url: &str, out: &mut dyn Write) -> Outcome {
    let result = client.tower_add(url).await?;
    let url = line_token(&result.tower.url);
    let line = match result.outcome {
        TowerAddOutcome::Added => format!("added {url}"),
        TowerAddOutcome::AlreadyInSet => format!("{url} is already in the set"),
    };
    say(out, &line)?;
    Ok(Exit::Done)
}

async fn tower_remove<T: BindInstance>(
    client: &Client<T>,
    url: &str,
    out: &mut dyn Write,
) -> Outcome {
    let result = client.tower_remove(url).await?;
    let url = line_token(&result.url);
    let line = match result.outcome {
        TowerRemoveOutcome::Removed => format!("removed {url}"),
        TowerRemoveOutcome::NotInSet => format!("{url} is not in the set"),
    };
    say(out, &line)?;
    Ok(Exit::Done)
}

async fn send<T: BindInstance>(
    client: &Client<T>,
    peer: &str,
    text: &str,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Outcome {
    // Bound, so that a restart between the subscription and the send cannot send the message to
    // an instance whose outcome this command never sees.
    let instance = client.status().await?.instance_id;
    let client = client.bound_to(&instance);
    // Subscribed before the send, so that the outcome cannot slip past (`Client::send_and_wait`
    // in three steps, so that an error says which step it ended).
    let mut events = client.subscribe().await?;
    let sent = match client.send(peer, text).await {
        Ok(sent) => sent,
        // The call ended without an answer it could read: the station may have taken the message
        // (rule 12 in chief::api).
        Err(error) if unanswered(&error) => {
            let exit = report(err, &error);
            let _ = say(
                err,
                &format!(
                    "the message may still have been sent; check `chief log {peer}` before \
                     sending it again"
                ),
            );
            return Ok(exit);
        }
        // A refusal: nothing was sent.
        Err(error) => return Err(Stop::Api(error)),
    };
    // From here on the station holds the message: no error means "not delivered".
    let waited = tokio::time::timeout(
        SEND_OUTCOME_WAIT,
        client.wait_for_outcome(&mut events, sent),
    )
    .await;
    match waited {
        Ok(Ok(message)) => match message.status {
            MessageStatus::Delivered => {
                // Delivered whether or not stdout still takes the line.
                let _ = say(out, &format!("delivered to {}", message.peer));
                return Ok(Exit::Done);
            }
            MessageStatus::Failed { reason } => {
                // FailReason's Display follows the peer's name: "bob appears offline, ...".
                let _ = say(err, &format!("{} {reason}", message.peer));
                return Ok(Exit::Failed);
            }
            MessageStatus::Sending | MessageStatus::Received => {
                let error = ApiError::BadReply {
                    detail: "the send ended without an outcome".to_owned(),
                };
                report(err, &error);
            }
        },
        // The exit is 3 whatever the error: a `MessageNotFound` here is not a refusal.
        Ok(Err(error)) => {
            report(err, &error);
        }
        Err(_) => {
            let _ = say(
                err,
                &format!(
                    "the station did not report the outcome within {} s",
                    SEND_OUTCOME_WAIT.as_secs()
                ),
            );
        }
    }
    let _ = say(
        err,
        &format!(
            "the station accepted the message, but its outcome is unknown; check `chief log \
             {peer}` before sending it again"
        ),
    );
    Ok(Exit::NoStation)
}

/// Whether `error` ended a call without an answer that could be read, so that the station may
/// have carried the call out (rule 12 in `chief::api`): [`ApiError::Timeout`],
/// [`ApiError::Disconnected`], and [`ApiError::BadReply`], an answer that came but could not be
/// read.
fn unanswered(error: &ApiError) -> bool {
    matches!(
        error,
        ApiError::Timeout { .. } | ApiError::Disconnected { .. } | ApiError::BadReply { .. }
    )
}

async fn log<T: BindInstance>(
    client: &Client<T>,
    peer: Option<&str>,
    out: &mut dyn Write,
) -> Outcome {
    // Every page from one instance: a cursor means something only with the instance it came from
    // (rule 1 in chief::api).
    let instance = client.status().await?.instance_id;
    let client = client.bound_to(&instance);
    // Pages come newest first; each lists its messages oldest first (rule 6 in chief::api). They
    // are buffered and printed only once the whole read has succeeded.
    let mut pages: Vec<Vec<Message>> = Vec::new();
    let mut before: Option<u64> = None;
    let mut read = 0usize;
    let mut bytes = 0usize;
    loop {
        let page = client.log(peer, before, LOG_PAGE_MAX_MESSAGES).await?;
        check_page(&page, before)?;
        read = read.saturating_add(page.messages.len());
        if read > LOG_MAX_MESSAGES {
            return Err(Stop::Api(ApiError::BadReply {
                detail: format!("the log runs past {LOG_MAX_MESSAGES} messages"),
            }));
        }
        // Checked before the page is kept: never more than the budget and the page in hand.
        bytes = page
            .messages
            .iter()
            .map(text_bytes)
            .fold(bytes, usize::saturating_add);
        if bytes > LOG_MAX_BYTES {
            return Err(Stop::Api(ApiError::BadReply {
                detail: format!("the log runs past {LOG_MAX_BYTES} bytes"),
            }));
        }
        let empty = page.messages.is_empty();
        pages.push(page.messages);
        match page.next_before {
            None => break,
            // Only a page of `limit: 0` may be empty with a cursor (rule 6).
            Some(_) if empty => {
                return Err(Stop::Api(ApiError::BadReply {
                    detail: "the station gave an empty page of the log with a cursor".to_owned(),
                }));
            }
            // `check_page` made sure the cursor is the first message's seq, below `before`: it
            // moves back, so the read ends.
            Some(next) => before = Some(next),
        }
    }
    let mut messages = pages.into_iter().rev().flatten().peekable();
    if messages.peek().is_none() {
        // A peer nobody has gives an empty page, not an error (rule 6): a removed peer's messages
        // stay in the log. Only an empty answer for a name that is not in the book is a typo.
        if let Some(name) = peer
            && !client.peers().await?.iter().any(|p| p.name == name)
        {
            return Err(Stop::Api(ApiError::PeerNotFound {
                name: name.to_owned(),
            }));
        }
        say(out, "no messages")?;
    }
    for message in messages {
        say(out, &format_message(&message))?;
    }
    Ok(Exit::Done)
}

/// The bytes of text `message` owns, as [`LOG_MAX_BYTES`] counts them: its body, peer name and
/// id. Its status holds no text.
fn text_bytes(message: &Message) -> usize {
    message.body.len() + message.peer.len() + message.id.len()
}

/// Checks the shape of a page of the log asked with `before` (rule 6 in `chief::api`): its
/// messages ascend by `seq`, are all below `before` if one was asked, and its cursor, if any, is
/// the `seq` of its first message. An empty page with a cursor is left to the caller.
fn check_page(page: &LogPage, before: Option<u64>) -> Result<(), ApiError> {
    let bad = |detail: &str| {
        Err(ApiError::BadReply {
            detail: detail.to_owned(),
        })
    };
    if !page.messages.windows(2).all(|w| w[0].seq < w[1].seq) {
        return bad("the messages of a page of the log do not ascend");
    }
    if let Some(b) = before
        && page.messages.iter().any(|m| m.seq >= b)
    {
        return bad("a page of the log holds a message at or after its cursor");
    }
    match (page.next_before, page.messages.first()) {
        (Some(next), Some(first)) if next != first.seq => {
            bad("the cursor of a page of the log is not its first message")
        }
        _ => Ok(()),
    }
}

/// A send of `chat` in flight.
type PendingSend<'a> = Pin<Box<dyn Future<Output = Result<Message, ApiError>> + 'a>>;

/// Whether `error` means that the chat cannot go on with this station: the errors of "cannot talk
/// to a station" ([`Exit::NoStation`]), a station that is shutting down included.
fn station_gone(error: &ApiError) -> bool {
    Exit::for_error(error) == Exit::NoStation
}

/// Why a chat ended before the end of its input.
enum ChatEnd {
    /// The station is gone: `Some(error)` is what noticed it, `None` that the live view ended.
    Gone(Option<ApiError>),
    /// Another error of the live view.
    Error(ApiError),
    /// Writing to `out` failed.
    Output(io::Error),
}

/// Ends a chat before the end of its input: one line for `end` on `err`, as `run` would print it,
/// then what was left undone: the line in flight, if a send was (`in_flight`), and the number of
/// typed lines still queued (`queued`). Returns the exit for `end`: 3 when the station is gone.
fn end_chat(err: &mut dyn Write, end: ChatEnd, peer: &str, in_flight: bool, queued: usize) -> Exit {
    let exit = match end {
        // Timeout, IncompatibleApi, BadReply: one line each in `report`.
        ChatEnd::Gone(Some(
            error @ (ApiError::Timeout { .. }
            | ApiError::IncompatibleApi { .. }
            | ApiError::BadReply { .. }),
        )) => {
            report(err, &error);
            Exit::NoStation
        }
        // The end of the live view, or a station that stopped, restarted or went away.
        ChatEnd::Gone(_) => {
            let _ = say(err, "the station stopped; the chat has ended");
            Exit::NoStation
        }
        ChatEnd::Error(error) => report(err, &error),
        ChatEnd::Output(error) => {
            let _ = say(err, &format!("cannot write the output: {error}"));
            Exit::Failed
        }
    };
    if in_flight {
        let _ = say(
            err,
            &format!("the last line may have been sent; check `chief log {peer}`"),
        );
    }
    match queued {
        0 => {}
        1 => {
            let _ = say(err, "1 typed line was not sent");
        }
        n => {
            let _ = say(err, &format!("{n} typed lines were not sent"));
        }
    }
    exit
}

/// What [`LineReader::next`] read.
enum Input {
    /// A line, without its `\n` and without one `\r` at its end, whether it ended with a newline
    /// or with the end of the input (`ab\r\r\n` gives `ab\r`): valid UTF-8 and at most
    /// [`BODY_MAX_BYTES`] bytes. It may be empty or blank; the caller decides.
    Line(String),
    /// A line longer than [`BODY_MAX_BYTES`] bytes, whatever it holds (none of it was kept, so a
    /// blank one is reported too); it was skipped to its end.
    TooLong,
    /// A line that is not valid UTF-8.
    NotUtf8,
    /// The end of the input; every call after the first end gives it again without reading.
    End,
    /// The input failed. A line read only in part before is dropped.
    Failed(io::Error),
}

/// How many bytes of a line [`LineReader`] keeps: [`BODY_MAX_BYTES`] and one `\r`.
const LINE_KEEP_MAX: usize = BODY_MAX_BYTES + 1;

/// Reads the lines of `chat` from a buffered input, keeping at most [`LINE_KEEP_MAX`] bytes of a
/// line: a longer line is skipped to its end and reported as [`Input::TooLong`], however long it
/// runs. A line ends at `\n`; the last one may end at the end of the input.
struct LineReader<R> {
    input: R,
    /// The line read so far; empty while a line that is too long is skipped.
    line: Vec<u8>,
    /// The line read so far is too long, and is being skipped.
    too_long: bool,
    /// The input has ended: from now on `next` gives [`Input::End`] without reading it again. A
    /// terminal does not repeat its end of input, and may take more input after it.
    ended: bool,
}

impl<R: AsyncBufRead + Unpin> LineReader<R> {
    fn new(input: R) -> Self {
        Self {
            input,
            line: Vec::new(),
            too_long: false,
            ended: false,
        }
    }

    /// The next line, or what else the input gave.
    ///
    /// Cancel-safe, so it can be used in `select!`. There are two awaits. `fill_buf` takes
    /// nothing from the input; what it returned is moved into `self` and consumed without an
    /// await in between. The other is a yield after each part of a line that has not ended yet,
    /// so that an input that is always ready but never ends a line cannot keep the `select!`
    /// from polling its other branches; when it comes, everything read so far is in `self`. So a
    /// dropped call loses nothing, and the part of a line read so far stays in `self` for the
    /// next call.
    ///
    /// The end of the input is read once. It is remembered in `self` as soon as `fill_buf`
    /// reports it, before the line it finishes is returned (whether that line is good, too long
    /// or not UTF-8); every later call returns [`Input::End`] without reading the input again.
    async fn next(&mut self) -> Input {
        loop {
            if self.ended {
                return Input::End;
            }
            let (used, complete) = {
                let buf = match self.input.fill_buf().await {
                    Ok(buf) => buf,
                    Err(error) => {
                        self.line.clear();
                        self.too_long = false;
                        return Input::Failed(error);
                    }
                };
                if buf.is_empty() {
                    // The end of the input, remembered before anything else; the last line may
                    // lack its newline.
                    self.ended = true;
                    if self.too_long || !self.line.is_empty() {
                        return self.finish();
                    }
                    return Input::End;
                }
                let (part, used, complete) = match buf.iter().position(|&b| b == b'\n') {
                    Some(at) => (&buf[..at], at + 1, true),
                    None => (buf, buf.len(), false),
                };
                if !self.too_long {
                    if self.line.len() + part.len() > LINE_KEEP_MAX {
                        // Too long to send: keep nothing more of it.
                        self.too_long = true;
                        self.line = Vec::new();
                    } else {
                        self.line.extend_from_slice(part);
                    }
                }
                (used, complete)
            };
            self.input.consume(used);
            if complete {
                return self.finish();
            }
            // The line goes on: let the caller's other branches run before reading more.
            tokio::task::yield_now().await;
        }
    }

    /// The line read so far, which has ended; resets for the next one.
    fn finish(&mut self) -> Input {
        let mut line = std::mem::take(&mut self.line);
        if std::mem::take(&mut self.too_long) {
            return Input::TooLong;
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        if line.len() > BODY_MAX_BYTES {
            return Input::TooLong;
        }
        match String::from_utf8(line) {
            Ok(line) => Input::Line(line),
            Err(_) => Input::NotUtf8,
        }
    }
}

async fn chat<T, I>(
    client: &Client<T>,
    peer: &str,
    input: I,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Outcome
where
    T: BindInstance,
    I: AsyncBufRead + Unpin,
{
    // The follower and every send talk to this one instance, or fail (rule 1 in chief::api).
    let instance = client.status().await?.instance_id;
    let client = &client.bound_to(&instance);
    if !client.peers().await?.iter().any(|p| p.name == peer) {
        return Err(Stop::Api(ApiError::PeerNotFound {
            name: peer.to_owned(),
        }));
    }
    let mut follower = client.follow_log(None, CHAT_TAIL).await?;
    let mut lines = LineReader::new(input);
    // Typed lines not sent yet, in order, and the one send in flight.
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut pending: Option<PendingSend<'_>> = None;
    // Whether `pending` has been polled: before that, its line has not gone anywhere.
    let mut started = false;
    let mut input_open = true;
    // Whether every non-empty line read so far was accepted by the station: the exit at the end
    // of the input.
    let mut all_accepted = true;
    loop {
        if pending.is_none()
            && let Some(text) = queue.pop_front()
        {
            pending = Some(Box::pin(async move { client.send(peer, &text).await }));
            started = false;
        }
        if !input_open && pending.is_none() {
            return Ok(if all_accepted {
                Exit::Done
            } else {
                Exit::Failed
            });
        }
        let reading = input_open && queue.len() < CHAT_QUEUE_MAX;
        let end = tokio::select! {
            // Biased: what the station has to show goes first, so the backlog is printed before a
            // line is read, and the end of the station before a send's error.
            biased;
            // Cancel-safe: updates not yet yielded stay queued in the follower.
            update = follower.next() => match update {
                Ok(Some(LogUpdate::Added(message))) => match say(out, &format_message(&message)) {
                    Ok(()) => continue,
                    Err(error) => ChatEnd::Output(error),
                },
                Ok(Some(LogUpdate::Changed(message))) => match say(out, &format_change(&message)) {
                    Ok(()) => continue,
                    Err(error) => ChatEnd::Output(error),
                },
                // The subscription ended, or a re-read after a `Resync` found another instance.
                Ok(None) => ChatEnd::Gone(None),
                Err(error) if station_gone(&error) => ChatEnd::Gone(Some(error)),
                // Any other error ends the chat too, with its own message and exit; it is never
                // retried, so a station that keeps failing cannot spin this loop.
                Err(error) => ChatEnd::Error(error),
            },
            // The send in flight, polled together with the follower and the input. A future is
            // polled only here, and dropped once it is done.
            sent = async {
                match pending.as_mut() {
                    Some(send) => {
                        started = true;
                        send.await
                    }
                    None => std::future::pending().await,
                }
            }, if pending.is_some() => {
                pending = None;
                match sent {
                    // The outcome arrives through the follower, as a change of the message.
                    Ok(_) => continue,
                    // This send's own error ends the chat. Its line may have been sent when the
                    // call ended without an answer (rule 12 in chief::api); a refusal (`Stopping`,
                    // `NotRunning`, `IncompatibleApi`) sent nothing, so its line counts as not
                    // sent.
                    Err(error) if station_gone(&error) => {
                        let in_flight = unanswered(&error);
                        let not_sent = queue.len() + usize::from(!in_flight);
                        let exit =
                            end_chat(err, ChatEnd::Gone(Some(error)), peer, in_flight, not_sent);
                        return Ok(exit);
                    }
                    // A refusal of this one message: say why and go on.
                    Err(error) => {
                        report(err, &error);
                        all_accepted = false;
                        continue;
                    }
                }
            },
            // Cancel-safe: see `LineReader::next`.
            line = lines.next(), if reading => {
                match line {
                    Input::Line(text) => {
                        if !text.trim().is_empty() {
                            queue.push_back(text);
                        }
                    }
                    Input::TooLong => {
                        all_accepted = false;
                        let _ = say(
                            err,
                            &format!("a line is longer than {BODY_MAX_BYTES} bytes and was not sent"),
                        );
                    }
                    Input::NotUtf8 => {
                        all_accepted = false;
                        let _ = say(err, "a line is not valid UTF-8 and was not sent");
                    }
                    // End of input: the lines read so far are still sent.
                    Input::End => input_open = false,
                    // As the end of input, after one line.
                    Input::Failed(error) => {
                        all_accepted = false;
                        input_open = false;
                        let _ = say(err, &format!("cannot read the input: {error}"));
                    }
                }
                continue;
            },
        };
        // The live view ended the chat, or failed, or stdout did. A send never polled has not
        // gone anywhere: its line counts as not sent.
        let in_flight = pending.is_some() && started;
        let not_sent = queue.len() + usize::from(pending.is_some() && !started);
        return Ok(end_chat(err, end, peer, in_flight, not_sent));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, BufReader, ReadBuf};

    /// An input that hands over one block per read, then ends.
    struct Blocks(VecDeque<Vec<u8>>);

    impl AsyncRead for Blocks {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if let Some(block) = self.0.pop_front() {
                assert!(block.len() <= buf.remaining());
                buf.put_slice(&block);
            }
            Poll::Ready(Ok(()))
        }
    }

    /// One call of `next()` raced against a ready branch, as a biased `select!` in `chat` would
    /// race it against the live view: `None` when the call was dropped at its yield.
    async fn turn<R: AsyncBufRead + Unpin>(reader: &mut LineReader<R>) -> Option<Input> {
        tokio::select! {
            biased;
            input = reader.next() => Some(input),
            () = std::future::ready(()) => None,
        }
    }

    fn label(input: &Input) -> String {
        match input {
            Input::Line(line) => format!("line {line}"),
            Input::TooLong => "too long".to_owned(),
            Input::NotUtf8 => "not utf-8".to_owned(),
            Input::End => "end".to_owned(),
            Input::Failed(error) => format!("failed: {error}"),
        }
    }

    #[tokio::test]
    async fn the_reader_keeps_at_most_one_line_and_loses_nothing_when_dropped() {
        // 1 MiB without a newline, in blocks of 4 KiB; then its newline; then a good line; then
        // a good line in two blocks.
        let mut blocks: VecDeque<Vec<u8>> = (0..256).map(|_| vec![b'x'; 4096]).collect();
        blocks.extend([
            b"\n".to_vec(),
            b"good one\n".to_vec(),
            b"good ".to_vec(),
            b"two\n".to_vec(),
        ]);
        let mut reader = LineReader::new(BufReader::with_capacity(4096, Blocks(blocks)));
        let mut got = Vec::new();
        let mut dropped = 0;
        for _ in 0..1000 {
            match turn(&mut reader).await {
                Some(Input::End) => break,
                Some(input) => got.push(label(&input)),
                None => dropped += 1,
            }
            // Never more than one line's worth, and nothing of a line that is too long.
            assert!(reader.line.len() <= LINE_KEEP_MAX, "{}", reader.line.len());
            if reader.too_long {
                assert!(reader.line.is_empty(), "{}", reader.line.len());
            }
        }
        assert_eq!(got, ["too long", "line good one", "line good two"]);
        // Dropped at every block of the long line, and between the two halves of `good two`.
        assert_eq!(dropped, 256 + 1);
    }
}
