//! The station API: plain data that crosses the local socket as JSON and, later, FFI.
//!
//! A frontend sends a [`Request`] and gets one [`Response`] or one [`ApiError`]. It can also open a
//! subscription to live [`Event`]s (see [`crate::client`]). This module defines those values and the
//! rules below; it holds no code that talks to a station.
//!
//! # Types
//!
//! Every type here uses only what AGENTS.md hard rule 7 allows: strings, integers, `Option`, `Vec`,
//! and the records and enums of this module (bytes and maps are allowed too, but none is used yet).
//! Nothing a `Request`, `Response`, `Event` or `ApiError` carries holds a `bool`, a tuple, a
//! reference or a `usize`. Ids, paths and URLs are strings. Times are unix milliseconds in a `u64`.
//! Enums have unit variants and struct variants only.
//!
//! # Wire form (JSON, serde)
//!
//! - [`Request`], [`Response`], [`Event`], [`ApiError`], [`TowerState`] and [`MessageStatus`] are
//!   internally tagged: `{"type":"<variant in snake_case>", ...fields}`, for example
//!   `{"type":"peer_add","name":"bob","station_id":"..."}` or `{"type":"status"}`.
//! - The label enums [`Link`], [`Direction`] and [`FailReason`] are bare snake_case strings, for
//!   example `"relayed"`.
//! - Field names are the Rust names. Unknown fields are ignored on input, at every level.
//! - An `Option` field that is `None` is left out on output. A missing field or `null` reads as
//!   `None`.
//! - Every enum is closed: a `type` or label this build does not know fails to decode.
//! - [`API_VERSION`] says when two builds can talk: only when their versions are equal.
//!
//! # Rules
//!
//! Stations, transports and frontends rely on these. "The station" is the station core; a transport
//! passes values through unchanged.
//!
//! 1. **seq and instance.** Every message that enters the station's log, incoming or outgoing, gets
//!    a [`Message::seq`]: 1, 2, 3 and so on, strictly increasing in the order messages enter the
//!    log, never reused within an instance. [`Status::instance_id`] is new on every start of the
//!    station, and `seq` starts again at 1. A `seq` means something only together with the instance
//!    it came from. A client that sees a different `instance_id` drops every `seq`, `rev` and
//!    snapshot it holds. The log may evict its oldest messages (M1 keeps the newest 1,000), so
//!    seqs below the oldest kept message are gone; [`Request::Message`] for one of them gives
//!    [`ApiError::MessageNotFound`].
//! 2. **rev and "already seen".** [`Message::rev`] is 0 when a message enters the log and grows by
//!    1 with every change of it. An [`Event::Message`] is already seen when the client holds a
//!    message with the same `seq` (of the same instance) and a `rev` at least as high; the client
//!    skips it. Otherwise the event's message replaces the held one or is added. [`Event::Peer`],
//!    [`Event::PeerRemoved`] and [`Event::Status`] carry the full new state of what changed, never
//!    a delta, so the client applies them in order over what it holds and needs no check.
//! 3. **Attach order.** A frontend that shows live state (a) subscribes, which resolves only once
//!    the station has registered the subscription; (b) reads its snapshots: [`Request::Log`] pages
//!    (rule 6), and [`Request::Status`] and [`Request::Peers`] as needed; (c) then reads events,
//!    applying rule 2. Nothing is missed: every change the station applies after (a) resolved is
//!    delivered on the subscription or covered by a later [`Event::Resync`], and every change it
//!    applied before it answered a snapshot call is in that snapshot. Events for changes that the
//!    snapshot already holds are harmless: rule 2 skips the messages, and the full-state events
//!    arrive in order, so the state is right again once the client has read the events queued so
//!    far. A frontend that must know that its subscription and its snapshots come from one
//!    instance (rule 1) reads [`Request::Status`] before (a) and again after (b): an instance id
//!    never comes back, so two equal ids mean that one instance answered everything in between.
//!    [`crate::client::Client::follow_log`] does this.
//! 4. **Event order.** One subscription delivers events in the order in which the station applied
//!    the changes. The station pushes the events of a change before it answers the call that caused
//!    it. In-process, the event is therefore queued before the answer arrives. Over the socket, calls
//!    and subscriptions use separate connections and are not ordered against each other; rule 2
//!    makes that harmless.
//! 5. **Resync.** [`Event::Resync`] means that events after the previous one were dropped because
//!    the subscriber fell behind. The subscription stays open and every later event is delivered
//!    normally. Every event read before the `Resync` precedes the gap. The client treats everything
//!    it derived as stale, re-reads [`Request::Status`], [`Request::Peers`] and the log as far back
//!    as it shows (rule 6), and keeps reading the same subscription, applying rule 2. It does not
//!    subscribe again.
//! 6. **Log paging.** [`Request::Log`] selects the messages whose `peer` equals `peer` (all if
//!    `None`) and whose `seq` is below `before` (all if `None`), newest first. It stops at
//!    `min(limit, LOG_PAGE_MAX_MESSAGES)` messages, or before the message that would push the sum
//!    of the messages' JSON lengths (`serde_json::to_vec(&message).len()`) over
//!    [`LOG_PAGE_MAX_BYTES`]. A page holds at least one message if any matches and `limit > 0`. The
//!    page lists its messages in ascending `seq` order. [`LogPage::next_before`] is the cursor:
//!    `None` when the page reaches the oldest matching message, otherwise `Some(seq)`, and the next
//!    page is asked with `before` set to that `seq`. For a page with messages it is the `seq` of the
//!    first one. Only `limit: 0` gives an empty page while matching messages remain; its cursor is
//!    the `before` that was asked, or, if none was, the newest matching message's `seq` plus one,
//!    so that asking again with a higher limit makes progress. A peer name nobody has gives an
//!    empty page with `None`, not an error: a removed peer's messages stay in the log. Clients
//!    accept pages shorter than `limit`.
//! 7. **Outgoing messages.** [`Request::Send`] checks the peer first, then the body: it fails with
//!    [`ApiError::PeerNotFound`] for an unknown peer, then with [`ApiError::Invalid`] (field
//!    `body`) for an empty body or one over [`BODY_MAX_BYTES`]. Otherwise the station logs the
//!    message as [`Direction::Outgoing`] with [`MessageStatus::Sending`] and rev 0, pushes its
//!    event, and answers with it. Exactly one change follows, to a final status, with rev 1:
//!    [`MessageStatus::Delivered`] (a station of the peer acknowledged it), or
//!    [`MessageStatus::Failed`] with [`FailReason::NoAnswer`] (no answer before the send
//!    deadline), [`FailReason::Refused`] (a station of the peer refused it) or
//!    [`FailReason::PeerRemoved`] (the peer was removed first). While the station keeps running, it
//!    always pushes that one event, with the whole message at rev 1 and its final status, also when
//!    the message has left the log meanwhile (rule 1; only when 1,000 newer messages entered the log
//!    first); the log then no longer holds it, and [`Request::Message`] for its `seq` gives
//!    [`ApiError::MessageNotFound`]. If the station stops first, its subscriptions end (rule 11)
//!    and no outcome is promised for a send that was still `Sending`. A final status never changes.
//!    A client accepts a final status already in the answer to `Send`.
//! 8. **Incoming messages.** The station logs a message from a peer as [`Direction::Incoming`] with
//!    [`MessageStatus::Received`] and rev 0, with [`Message::at_ms`] set to the time it arrived, and
//!    pushes its event. It refuses, and does not log, a message whose id does not have the form of
//!    [`Message::id`]. The station remembers the newest [`RECEIPT_WINDOW`] accepted messages by
//!    peer and id, across all peers. A message whose peer and id are among them is acknowledged to
//!    the sender again, is not logged again and produces no event. Older ones are forgotten, so a
//!    message resent after that many newer ones is logged as a new message, and a restart forgets
//!    them all. Incoming messages never change.
//! 9. **Text from outside.** Before a frontend sees a string that came from a peer or a tower, the
//!    station replaces every control character in it with a space: every `char` for which
//!    `char::is_control` is true (C0, DEL and C1, so newline, carriage return, tab and escape too).
//!    M1 messages are therefore single lines (AGENTS.md, hard rule 8). [`TowerState::Failing`] and
//!    [`TowerState::Denied`] carry display text: show it, never match on it.
//! 10. **Peer book.** Peer names follow [`check_peer_name`] and are compared byte for byte.
//!     [`Request::PeerAdd`] creates the peer or appends the station to an existing one and pushes
//!     [`Event::Peer`]; adding a station the peer already has changes nothing and pushes nothing.
//!     Its checks run in this order: the name ([`check_peer_name`], [`ApiError::Invalid`] with
//!     field `name`); the form of the station id ([`ApiError::Invalid`], field `station_id`; what
//!     a valid id looks like is the station's to say); the station's own id
//!     ([`ApiError::Invalid`], field `station_id`); a station that belongs to another peer
//!     ([`ApiError::StationTaken`]). [`Request::PeerRemove`] fails the
//!     peer's outgoing messages that are still `Sending` (rule 7, with their events), closes its
//!     links and pushes [`Event::PeerRemoved`]. Any change of a link pushes [`Event::Peer`] with the
//!     whole peer.
//! 11. **End of a subscription.** A subscription ends when the station stops or the transport
//!     loses its connection: [`crate::client::Subscription::next`] returns the events still queued,
//!     then `None`, and `None` on every later call. It never resumes. To go on, the client
//!     subscribes again, follows rule 3 and compares [`Status::instance_id`] with the one it held
//!     (rule 1). While the station shuts down, calls fail with [`ApiError::Stopping`].
//! 12. **A call without an answer.** A call that ends with [`ApiError::Disconnected`] or
//!     [`ApiError::Timeout`], or whose caller stopped waiting for it, may still have been carried
//!     out. The station finishes a change it has accepted whether or not anyone still waits for
//!     the answer, and pushes the change's events as always. A frontend that needs to know reads
//!     the state again (the log, the peer book) before it repeats a change; a repeated
//!     [`Request::Send`] is a second message.

use serde::{Deserialize, Serialize};
use std::fmt;

/// The version of this module's wire form and rules.
///
/// A station and a frontend talk only when their `API_VERSION` is equal. A transport that connects
/// to a station, such as the socket, compares the two first and reports a mismatch as
/// [`ApiError::IncompatibleApi`].
///
/// `API_VERSION` changes with every change to this module's wire form or to the meaning of its
/// values, except a new field that older code can safely ignore. Such a field is an `Option` or has
/// `#[serde(default)]`, so that code which knows it also reads JSON without it. A new variant of
/// any enum, a renamed, removed or retyped field, and a new required field all change
/// `API_VERSION`.
///
/// Version 1 is not final until the first station that implements it is released with M1. Until
/// then its rules are being completed, and clarifications of them do not change the number; from
/// that release on, the rule above applies as written.
pub const API_VERSION: u32 = 1;

/// The largest body of a chat message, in bytes of UTF-8 (16 KiB). [`Request::Send`] with a larger
/// body fails with [`ApiError::Invalid`]. Equal to the chat protocol's cap.
pub const BODY_MAX_BYTES: usize = 16 * 1024;

/// The longest [`Message::id`], in bytes (64). An id is 1 to this many ASCII letters, digits,
/// underscores or hyphens. Equal to the chat protocol's cap.
pub const MESSAGE_ID_MAX_BYTES: usize = 64;

/// The longest peer name, in bytes. See [`check_peer_name`].
pub const PEER_NAME_MAX_BYTES: usize = 32;

/// The most messages one [`Request::Log`] returns, whatever its `limit` says.
pub const LOG_PAGE_MAX_MESSAGES: u32 = 1000;

/// The most bytes of messages one [`Request::Log`] returns: the sum of
/// `serde_json::to_vec(&message).len()` over the page (rule 6 in the [module docs](self)). It keeps
/// a [`Response::Log`] well under the local socket's 1 MiB frame cap.
pub const LOG_PAGE_MAX_BYTES: usize = 512 * 1024;

/// How many accepted incoming messages the station remembers, by peer and id, to recognise a
/// message that is sent again (rule 8 in the [module docs](self)).
pub const RECEIPT_WINDOW: usize = 1000;

/// A call to the station. The station answers each with exactly one [`Response`], named on each
/// variant, or with an [`ApiError`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    /// This station's id, instance and towers. Answer: [`Response::Status`].
    Status,
    /// The peer book with the state of every link. Answer: [`Response::Peers`].
    Peers,
    /// Adds the station `station_id` to the peer `name`, creating the peer if needed (rule 10).
    /// Answer: [`Response::Peer`] with the peer as it is afterwards. Errors:
    /// [`ApiError::Invalid`] (field `name` or `station_id`), [`ApiError::StationTaken`].
    PeerAdd {
        /// The peer's name in this station's peer book. Must pass [`check_peer_name`].
        name: String,
        /// The peer's station id, in the text form of [`Status::station_id`].
        station_id: String,
    },
    /// Removes the peer with all its stations and closes its links (rule 10). Its messages stay in
    /// the log. Answer: [`Response::Done`]. Error: [`ApiError::PeerNotFound`].
    PeerRemove {
        /// The peer's name.
        name: String,
    },
    /// Sends a chat message to the peer (rule 7). Answer: [`Response::Message`] with the new
    /// outgoing message, normally [`MessageStatus::Sending`]; its outcome arrives as
    /// [`Event::Message`] with the same `seq`. Errors: [`ApiError::PeerNotFound`],
    /// [`ApiError::Invalid`] (field `body`).
    Send {
        /// The peer's name.
        peer: String,
        /// The text, 1 to [`BODY_MAX_BYTES`] bytes.
        body: String,
    },
    /// One message of the log. Answer: [`Response::Message`]. Error:
    /// [`ApiError::MessageNotFound`].
    Message {
        /// The message's [`Message::seq`].
        seq: u64,
    },
    /// A page of the log (rule 6). Answer: [`Response::Log`].
    Log {
        /// Only messages whose [`Message::peer`] equals this name; `None` for all peers.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        peer: Option<String>,
        /// Only messages whose `seq` is below this; `None` starts at the newest message.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before: Option<u64>,
        /// The most messages wanted. The page may hold fewer.
        limit: u32,
    },
}

/// The answer to a [`Request`]. Each variant carries the value one [`crate::client::Client`] method
/// returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    /// The answer to [`Request::Status`].
    Status {
        /// The station's status.
        status: Status,
    },
    /// The answer to [`Request::Peers`].
    Peers {
        /// Every peer in the book, ordered by name (byte order).
        peers: Vec<Peer>,
    },
    /// The answer to [`Request::PeerAdd`].
    Peer {
        /// The peer as it is after the call.
        peer: Peer,
    },
    /// The answer to [`Request::Send`] and [`Request::Message`].
    Message {
        /// The message as it is in the log.
        message: Message,
    },
    /// The answer to [`Request::Log`].
    Log {
        /// The page.
        page: LogPage,
    },
    /// The answer to [`Request::PeerRemove`]: the call took effect and returns nothing.
    Done,
}

/// A live event on a subscription.
///
/// Every event but `Resync` carries the full new state of what changed, never a delta (rule 2 in
/// the [module docs](self)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// Events were dropped because this subscriber fell behind (rule 5): re-read the snapshots and
    /// keep reading this subscription. A station never pushes it: [`crate::client::Subscription`]
    /// makes it after its queue overflowed, and a transport passes on one it received.
    Resync,
    /// A message entered the log (`rev` 0) or changed (`rev` above 0).
    Message {
        /// The message as it is now.
        message: Message,
    },
    /// A peer was added, gained a station, or one of its links changed.
    Peer {
        /// The whole peer as it is now.
        peer: Peer,
    },
    /// A peer was removed. Its messages stay in the log.
    PeerRemoved {
        /// The removed peer's name.
        name: String,
    },
    /// The station's status changed (in M1, the state of a tower).
    Status {
        /// The whole status as it is now.
        status: Status,
    },
}

/// The station's identity and its view of its towers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    /// This station's id, as text: what `chief id` prints and what a peer passes to
    /// [`Request::PeerAdd`].
    pub station_id: String,
    /// Opaque, and new on every start of the station; compare for equality only (rule 1). A
    /// transport that reports an instance id when it connects reports this same value.
    pub instance_id: String,
    /// Every configured tower, in configuration order. M1 has exactly one.
    pub towers: Vec<TowerStatus>,
}

/// One configured tower and the station's connection to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TowerStatus {
    /// The tower's URL as configured.
    pub url: String,
    /// The station's connection to the tower.
    pub state: TowerState,
}

/// The state of the station's connection to one tower.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TowerState {
    /// Not connected, and no failure seen yet.
    Connecting,
    /// Connected: the station can be reached through this tower.
    Connected,
    /// The last attempt failed; the station keeps trying.
    Failing {
        /// Why, as display text (rule 9).
        error: String,
    },
    /// The tower refused this station. Trying again does not help until the tower's policy
    /// changes.
    Denied {
        /// Why, as display text (rule 9).
        reason: String,
    },
}

/// A peer in the station's peer book: a name for one or more stations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    /// The name, unique in the book (see [`check_peer_name`]).
    pub name: String,
    /// The peer's stations, in the order they were added. Never empty.
    pub stations: Vec<PeerStation>,
}

/// One station of a peer and the link to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerStation {
    /// The station's id, in the text form of [`Status::station_id`].
    pub station_id: String,
    /// The state of the link to that station.
    pub link: Link,
}

/// The state of the link to one peer station, by the path it uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Link {
    /// No link: none was opened, it is being opened, or it was closed.
    Down,
    /// Linked, and the path in use goes through a tower.
    Relayed,
    /// Linked, and the path in use is direct.
    Direct,
}

/// One message of the station's log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// The message's position in this instance's log (rule 1).
    pub seq: u64,
    /// 0 when the message enters the log, 1 higher after each change (rule 2).
    pub rev: u32,
    /// The chat message id both stations share, as text. Opaque to frontends. The sending station
    /// makes it; the receiving station uses it to drop duplicates (rule 8).
    ///
    /// Its form: 1 to [`MESSAGE_ID_MAX_BYTES`] ASCII letters, digits, underscores (`_`) or hyphens
    /// (`-`). A station refuses a message from a peer whose id has another form, so a frontend
    /// never sees one.
    pub id: String,
    /// The peer's name when the message entered the log. It stays when the peer is removed.
    pub peer: String,
    /// Whether this station sent or received the message.
    pub direction: Direction,
    /// The text, at most [`BODY_MAX_BYTES`] bytes. In an incoming message, every control
    /// character is replaced by a space (rule 9), and the text may be empty: only
    /// [`Request::Send`] has a lower bound.
    pub body: String,
    /// When this station logged the message, by its own clock, in unix milliseconds. For an
    /// incoming message that is when it arrived; the sender's clock is never used.
    pub at_ms: u64,
    /// Where the message stands. [`MessageStatus::Received`] for every incoming message; one of the
    /// others for every outgoing one.
    pub status: MessageStatus,
}

/// Whether this station sent or received a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// From the peer to this station.
    Incoming,
    /// From this station to the peer.
    Outgoing,
}

/// Where a message stands (rules 7 and 8). Every status but `Sending` is final.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageStatus {
    /// Outgoing, and no outcome yet.
    Sending,
    /// Outgoing, and a station of the peer acknowledged it: that station holds the message in
    /// memory. Final.
    Delivered,
    /// Outgoing, and not delivered. Final.
    Failed {
        /// Why.
        reason: FailReason,
    },
    /// Incoming. Final.
    Received,
}

/// Why an outgoing message failed.
///
/// `Display` gives a phrase that follows the peer's name: `format!("{peer} {reason}")` reads
/// "bob appears offline, or has not added you".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailReason {
    /// No acknowledgement before the send deadline: the peer is offline, or has not added this
    /// station (a station ignores callers it does not know, which looks the same).
    NoAnswer,
    /// The peer's station answered with a refusal.
    Refused,
    /// The peer was removed before the message was delivered.
    PeerRemoved,
}

impl fmt::Display for FailReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoAnswer => "appears offline, or has not added you",
            Self::Refused => "refused the message",
            Self::PeerRemoved => "was removed before the message was delivered",
        })
    }
}

/// A page of the log (rule 6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogPage {
    /// The messages, in ascending `seq` order.
    pub messages: Vec<Message>,
    /// The cursor to the next older page (rule 6). `Some(seq)`: older matching messages remain;
    /// ask again with `before` set to `seq`, which is the `seq` of `messages[0]` when the page is not
    /// empty. `None`: this page reaches the oldest matching message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_before: Option<u64>,
}

/// The one error type of the station API.
///
/// The variants are grouped by the layer that produces them. A transport passes the station's
/// errors through unchanged. `Display` gives a sentence for the user.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiError {
    // Produced by the station core.
    /// No peer has this name.
    PeerNotFound {
        /// The name asked for.
        name: String,
    },
    /// [`Request::PeerAdd`] named a station that already belongs to another peer.
    StationTaken {
        /// The station asked for.
        station_id: String,
        /// The peer it belongs to.
        peer: String,
    },
    /// No message with this `seq` is in the log: it was evicted, or the `seq` came from another
    /// instance (rule 1).
    MessageNotFound {
        /// The `seq` asked for.
        seq: u64,
    },
    /// A request field has a value the station does not accept.
    Invalid {
        /// The request field's name, such as `name`, `station_id` or `body`.
        field: String,
        /// What is wrong, as display text that follows "invalid {field}: ".
        reason: String,
    },
    /// The station is shutting down and takes no more calls.
    Stopping,
    /// The station failed in a way the caller cannot fix, for example writing its peer book.
    Internal {
        /// What failed, as display text.
        detail: String,
    },
    // Produced by the socket: its server, or its client for a request it cannot send.
    /// The request could not be carried to the station: the socket server could not decode it,
    /// or the socket transport refused, before sending anything, a request too large for one
    /// frame.
    BadRequest {
        /// What was wrong, as display text. It never quotes the request.
        detail: String,
    },
    // Produced by the client side of a transport.
    /// No station is running: nothing listens on the socket, or the socket does not exist.
    NotRunning {
        /// The socket's path.
        socket: String,
    },
    /// The station speaks another [`API_VERSION`].
    IncompatibleApi {
        /// The station's `API_VERSION`.
        station: u32,
        /// This build's `API_VERSION`.
        client: u32,
    },
    /// The connection to the station broke during a call, or a subscription ended while a
    /// [`crate::client::Client`] method was waiting on it.
    Disconnected {
        /// What happened, as display text.
        detail: String,
    },
    /// The station did not answer a call in time.
    Timeout {
        /// How long the transport waited, in milliseconds.
        after_ms: u64,
    },
    /// The station's reply could not be used: a frame that does not decode or is too large, or a
    /// response that does not fit the request.
    BadReply {
        /// What was wrong, as display text.
        detail: String,
    },
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerNotFound { name } => write!(f, "no peer named {name}"),
            Self::StationTaken { station_id, peer } => {
                write!(f, "station {station_id} already belongs to peer {peer}")
            }
            Self::MessageNotFound { seq } => write!(f, "message {seq} is not in the station's log"),
            Self::Invalid { field, reason } => write!(f, "invalid {field}: {reason}"),
            Self::Stopping => f.write_str("the station is shutting down"),
            Self::Internal { detail } => write!(f, "station error: {detail}"),
            Self::BadRequest { detail } => {
                write!(f, "the station could not read the request: {detail}")
            }
            Self::NotRunning { socket } => {
                write!(f, "no station is running (nothing listens on {socket})")
            }
            Self::IncompatibleApi { station, client } => write!(
                f,
                "the station speaks API version {station}, this chief speaks {client}"
            ),
            Self::Disconnected { detail } => {
                write!(f, "lost the connection to the station: {detail}")
            }
            Self::Timeout { after_ms } => {
                write!(f, "the station did not answer within {after_ms} ms")
            }
            Self::BadReply { detail } => write!(f, "unexpected reply from the station: {detail}"),
        }
    }
}

impl std::error::Error for ApiError {}

/// Checks a peer name: 1 to [`PEER_NAME_MAX_BYTES`] ASCII letters, digits, `.`, `_` and `-`,
/// starting with a letter or a digit.
///
/// The station enforces it on [`Request::PeerAdd`]; a frontend may check early. Fails with
/// [`ApiError::Invalid`] with field `name`.
pub fn check_peer_name(name: &str) -> Result<(), ApiError> {
    let invalid = |reason: String| {
        Err(ApiError::Invalid {
            field: "name".to_owned(),
            reason,
        })
    };
    if name.is_empty() || name.len() > PEER_NAME_MAX_BYTES {
        return invalid(format!("must be 1 to {PEER_NAME_MAX_BYTES} characters"));
    }
    if !name.starts_with(|c: char| c.is_ascii_alphanumeric()) {
        return invalid("must start with a letter or a digit".to_owned());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return invalid("may contain only letters, digits, '.', '_' and '-'".to_owned());
    }
    Ok(())
}
