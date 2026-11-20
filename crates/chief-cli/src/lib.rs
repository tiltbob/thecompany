//! `chief_cli`: the `chief` command-line frontend, as a library.
//!
//! The binary (`src/main.rs`) only parses the arguments, works out the socket path with
//! [`socket_path`], builds a [`chief::client::SocketTransport`] and calls [`run`] with the real
//! stdin, stdout and stderr. Everything else is here, generic over the [`Transport`], so that tests
//! run every command in-process against a fake station and capture what it prints.
//!
//! # Commands
//!
//! ```text
//! chief [--station PATH] <command>
//!   id                             this station's id
//!   status                         the state of each tower, then the link to each peer station
//!   peer add <name> <station-id>   adds a station to a peer; prints the peer as stored
//!   peer ls                        one line per peer station
//!   peer rm <name>                 removes a peer
//!   send <peer> <text>             sends a message and waits for the outcome
//!   log [peer]                     the whole message log, oldest first
//!   chat <peer>                    the last 50 messages of all peers, then live; each line read
//!                                  from stdin is sent to <peer>; end of input ends the chat
//! ```
//!
//! # Output (stable: scripts parse it)
//!
//! Every line goes through [`say`], which replaces control characters with spaces (hard rule 8):
//! nothing a peer or a tower wrote can move the cursor, clear the screen or forge a line. Fields
//! are separated by one space; peer names, station ids, URLs, times and status words contain no
//! space, so only the last field of a line may.
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
//! - `status`: `tower <url> <state>` per tower, with the state `connecting`, `connected`,
//!   `failing: <error>` or `denied: <reason>`; then `peer <name> <station-id> <link>` per peer
//!   station, with the link `down`, `relayed` or `direct`, or `no peers`.
//! - `peer ls` and `peer add`: `<name> <station-id> <link>` per peer station; `peer ls` says
//!   `no peers` when the book is empty.
//! - `peer rm`: `removed <name>`. `send`: `delivered to <peer>`. An empty `log`: `no messages`.
//!
//! Errors go to stderr as plain sentences, one per line, starting lowercase.
//!
//! # Exit codes
//!
//! See [`Exit`]: 0 done (for `send`: delivered); 1 the station refused or the operation failed
//! (for `send`: not delivered); 2 usage error; 3 cannot talk to a station.
//!
//! # Process-global state
//!
//! None here (hard rule 6): no runtime, no environment reads, no signal handlers. The binary reads
//! the environment once through [`PathEnv::read`] and owns the runtime.

use chief::api::{
    ApiError, Direction, FailReason, LOG_PAGE_MAX_MESSAGES, Link, Message, MessageStatus, Peer,
    TowerState,
};
use chief::client::{Client, Transport};
use chief::follow::LogUpdate;
use chief::paths::{PathEnv, check_socket_path};
use clap::{Parser, Subcommand};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

/// How many messages `chat` shows before it goes live.
pub const CHAT_TAIL: u32 = 50;

/// The text after the options in `--help`: the exit codes and where the socket is.
pub const AFTER_HELP: &str = "\
Exit status:
  0  done; for send: the message was delivered
  1  the station refused or the operation failed; for send: the message was not delivered
  2  usage error
  3  cannot talk to a station: none is running, it speaks another API version, or it stopped
     answering

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
    /// Show the state of each tower, then the link to each peer station
    Status,
    /// Manage the peer book
    Peer {
        /// What to do with the peer book.
        #[command(subcommand)]
        command: PeerCommand,
    },
    /// Send a message and wait for the outcome
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
    /// Show the last 50 messages, then follow live; each line typed is sent to <PEER>
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

/// How `chief` ends. [`Exit::code`] is the process's exit status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// 0: done. For `send`: the message was delivered.
    Done,
    /// 1: the station refused or the operation failed: an unknown peer, an invalid name or body,
    /// any error the station itself answers with. For `send`: the message was not delivered.
    Failed,
    /// 2: a usage error, reported by clap.
    Usage,
    /// 3: cannot talk to a station: none is running, it speaks another API version, the
    /// connection broke, it did not answer in time or answered nonsense. Also when no socket path
    /// can be worked out.
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
    /// cannot be talked to, [`Exit::Failed`] for every error the station answers with.
    pub fn for_error(error: &ApiError) -> Self {
        match error {
            ApiError::NotRunning { .. }
            | ApiError::IncompatibleApi { .. }
            | ApiError::Disconnected { .. }
            | ApiError::Timeout { .. }
            | ApiError::BadReply { .. } => Self::NoStation,
            ApiError::PeerNotFound { .. }
            | ApiError::StationTaken { .. }
            | ApiError::MessageNotFound { .. }
            | ApiError::Invalid { .. }
            | ApiError::Stopping
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
/// Unix socket address. The caller prints it and exits with [`Exit::NoStation`].
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
        ApiError::Invalid { reason, .. } => format!("cannot use the socket path: {reason}"),
        other => other.to_string(),
    })?;
    Ok(path)
}

/// Runs one command against the station behind `client` and returns how `chief` ends.
///
/// Normal output goes to `out`, errors to `err`, every line through [`say`]. `input` is read only
/// by `chat`, one line per message; the binary passes stdin, tests pass whatever they like.
pub async fn run<T, I>(
    command: &Command,
    client: &Client<T>,
    input: I,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Exit
where
    T: Transport,
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

/// Writes `text` and a newline to `w`, with every control character in `text` (as
/// [`char::is_control`] defines them: newline, tab, escape and the rest) replaced by a space, as
/// the station does with peer text (rule 9 in `chief::api`).
///
/// The one way this crate prints anything, so that no text from a station, a peer or a tower
/// reaches the terminal unfiltered (hard rule 8). The station strips peer text already; a
/// terminal is the last place to find out it did not.
pub fn say(w: &mut dyn Write, text: &str) -> io::Result<()> {
    let clean: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    writeln!(w, "{clean}")?;
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

fn tower_label(state: &TowerState) -> String {
    match state {
        TowerState::Connecting => "connecting".to_owned(),
        TowerState::Connected => "connected".to_owned(),
        TowerState::Failing { error } => format!("failing: {error}"),
        TowerState::Denied { reason } => format!("denied: {reason}"),
    }
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
            format!("no station is running (no socket at {socket})"),
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

async fn id<T: Transport>(client: &Client<T>, out: &mut dyn Write) -> Outcome {
    let status = client.status().await?;
    say(out, &status.station_id)?;
    Ok(Exit::Done)
}

async fn status<T: Transport>(client: &Client<T>, out: &mut dyn Write) -> Outcome {
    let status = client.status().await?;
    let peers = client.peers().await?;
    if status.towers.is_empty() {
        say(out, "no towers")?;
    }
    for tower in &status.towers {
        say(
            out,
            &format!("tower {} {}", tower.url, tower_label(&tower.state)),
        )?;
    }
    if peers.is_empty() {
        say(out, "no peers")?;
    }
    for line in peers.iter().flat_map(peer_lines) {
        say(out, &format!("peer {line}"))?;
    }
    Ok(Exit::Done)
}

async fn peer_add<T: Transport>(
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

async fn peer_ls<T: Transport>(client: &Client<T>, out: &mut dyn Write) -> Outcome {
    let peers = client.peers().await?;
    if peers.is_empty() {
        say(out, "no peers")?;
    }
    for line in peers.iter().flat_map(peer_lines) {
        say(out, &line)?;
    }
    Ok(Exit::Done)
}

async fn peer_rm<T: Transport>(client: &Client<T>, name: &str, out: &mut dyn Write) -> Outcome {
    client.peer_remove(name).await?;
    say(out, &format!("removed {name}"))?;
    Ok(Exit::Done)
}

async fn send<T: Transport>(
    client: &Client<T>,
    peer: &str,
    text: &str,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Outcome {
    let message = client.send_and_wait(peer, text).await?;
    match message.status {
        MessageStatus::Delivered => {
            say(out, &format!("delivered to {}", message.peer))?;
            Ok(Exit::Done)
        }
        MessageStatus::Failed { reason } => {
            // FailReason's Display follows the peer's name: "bob appears offline, ...".
            let _ = say(err, &format!("{} {reason}", message.peer));
            Ok(Exit::Failed)
        }
        MessageStatus::Sending | MessageStatus::Received => Err(Stop::Api(ApiError::BadReply {
            detail: "the send ended without an outcome".to_owned(),
        })),
    }
}

async fn log<T: Transport>(client: &Client<T>, peer: Option<&str>, out: &mut dyn Write) -> Outcome {
    // Pages come newest first; each lists its messages oldest first (rule 6 in chief::api).
    let mut pages: Vec<Vec<Message>> = Vec::new();
    let mut before: Option<u64> = None;
    loop {
        let page = client.log(peer, before, LOG_PAGE_MAX_MESSAGES).await?;
        let first = page.messages.first().map(|m| m.seq);
        pages.push(page.messages);
        match (page.next_before, first) {
            // Go on only while the cursor moves back, so a station that repeats itself cannot
            // keep this loop going.
            (Some(next), Some(_)) if before.is_none_or(|b| next < b) => before = Some(next),
            _ => break,
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

async fn chat<T, I>(
    client: &Client<T>,
    peer: &str,
    input: I,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Outcome
where
    T: Transport,
    I: AsyncBufRead + Unpin,
{
    if !client.peers().await?.iter().any(|p| p.name == peer) {
        return Err(Stop::Api(ApiError::PeerNotFound {
            name: peer.to_owned(),
        }));
    }
    let mut follower = client.follow_log(None, CHAT_TAIL).await?;
    let mut lines = input.lines();
    loop {
        tokio::select! {
            // Biased: what the station has to show goes first, so the backlog is printed before a
            // line is read, and an outcome before the end of input.
            biased;
            // Cancel-safe: updates not yet yielded stay queued in the follower.
            update = follower.next() => match update {
                Ok(Some(LogUpdate::Added(message))) => say(out, &format_message(&message))?,
                Ok(Some(LogUpdate::Changed(message))) => say(out, &format_change(&message))?,
                // The station is gone: the subscription ended, or a re-read after a `Resync`
                // found no station. Ends the chat like the end of the subscription.
                Ok(None)
                | Err(
                    ApiError::NotRunning { .. }
                    | ApiError::Disconnected { .. }
                    | ApiError::Stopping,
                ) => {
                    let _ = say(err, "the station stopped; the chat has ended");
                    return Ok(Exit::NoStation);
                }
                // Any other error ends the chat too, with its own message and exit; it is never
                // retried, so a station that keeps failing cannot spin this loop.
                Err(error) => return Err(Stop::Api(error)),
            },
            // Cancel-safe: `Lines::next_line` keeps a partly read line in its own buffer.
            line = lines.next_line() => match line {
                Ok(Some(line)) => {
                    let text = line.strip_suffix('\r').unwrap_or(&line);
                    if text.trim().is_empty() {
                        continue;
                    }
                    // The outcome arrives through the follower, as a change of the message.
                    match client.send(peer, text).await {
                        Ok(_) => {}
                        Err(error) if Exit::for_error(&error) == Exit::NoStation => {
                            return Err(Stop::Api(error));
                        }
                        Err(error) => {
                            report(err, &error);
                        }
                    }
                }
                Ok(None) => return Ok(Exit::Done),
                Err(error) => {
                    let _ = say(err, &format!("cannot read the input: {error}"));
                    return Ok(Exit::Failed);
                }
            },
        }
    }
}
