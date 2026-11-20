//! [`FakeStation`]: an in-memory station for tests, behind the cargo feature `fake`.
//!
//! It follows the rules of [`crate::api`] closely enough to test transports, the socket server and
//! frontends against it: a peer book, a log with `seq` and `rev`, paging, sends whose outcome the
//! test scripts, incoming messages, subscriptions, stopping and restarting. It has no network,
//! spawns nothing, and reads the clock only for [`Message::at_ms`], and not even then after
//! [`FakeStation::set_now_ms`]. It is the reference that frontend tests trust, so its own tests
//! check it against the rules.
//!
//! Test hooks beyond the rules: [`FakeStation::on_call`] runs code at the start of every call,
//! [`FakeStation::fail_calls`] makes every call fail, and [`FakeStation::hold_subscribes`] keeps
//! `subscribe` from returning until the test lets it.
//!
//! What it leaves out: links never change on their own (a test sets them with
//! [`FakeStation::set_link`]), there is no send deadline (a held send stays `Sending` until
//! [`FakeStation::resolve`]), and nothing is persisted.

use crate::api::{
    ApiError, BODY_MAX_BYTES, Direction, Event, FailReason, LOG_PAGE_MAX_BYTES,
    LOG_PAGE_MAX_MESSAGES, Link, LogPage, Message, MessageStatus, Peer, PeerStation,
    RECEIPT_WINDOW, Request, Response, Status, TowerState, TowerStatus, check_peer_name,
};
use crate::client::{EventSink, SUBSCRIPTION_CAPACITY, Subscription, Transport, subscription};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

/// The URL of the fake's one tower.
pub const FAKE_TOWER_URL: &str = "http://127.0.0.1:3340/";

/// How many messages the fake keeps by default, like the station: the newest 1,000.
pub const FAKE_LOG_CAPACITY: usize = 1_000;

/// What happens to a message right after the fake answered [`Request::Send`] with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    /// It becomes [`MessageStatus::Delivered`] (rev 1). The default.
    Deliver,
    /// It becomes [`MessageStatus::Failed`] with this reason (rev 1).
    Fail(FailReason),
    /// It stays [`MessageStatus::Sending`] until [`FakeStation::resolve`] or the peer's removal.
    Hold,
}

type CallHook = Box<dyn FnMut(&FakeStation, &Request) + Send>;

/// An in-memory station that implements [`Transport`]. Cheap to clone; clones share one station.
///
/// The outcome of a send is applied within the call, after the answer was made: the answer holds
/// the `Sending` message (rev 0), and the event of the outcome (rev 1) is pushed before the call
/// returns. So the outcome's event is queued before the caller sees the answer (rule 4).
#[derive(Clone)]
pub struct FakeStation {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    hook: Mutex<Option<CallHook>>,
    /// Woken when a subscription is added or a closed one is dropped.
    subscribers_changed: Notify,
    /// Woken when held subscribes may go on.
    subscribe_gate: Notify,
}

struct State {
    status: Status,
    peers: BTreeMap<String, Vec<PeerStation>>,
    log: VecDeque<Message>,
    log_capacity: usize,
    next_seq: u64,
    next_id: u64,
    outcome: SendOutcome,
    sinks: Vec<Arc<EventSink>>,
    requests: Vec<Request>,
    stopped: bool,
    /// The newest [`RECEIPT_WINDOW`] accepted incoming messages, by peer and id, oldest first
    /// (rule 8). Independent of the log's capacity.
    receipts: VecDeque<(String, String)>,
    /// The time new messages get, instead of the wall clock.
    now_ms: Option<u64>,
    /// The error every call answers while set.
    fail_calls: Option<ApiError>,
    /// Whether `subscribe` waits for [`FakeStation::hold_subscribes`]`(false)`.
    hold_subscribes: bool,
    /// How many `subscribe` calls are waiting.
    held_subscribes: usize,
}

impl fmt::Debug for FakeStation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state();
        f.debug_struct("FakeStation")
            .field("peers", &state.peers.len())
            .field("messages", &state.log.len())
            .field("subscribers", &state.sinks.len())
            .field("stopped", &state.stopped)
            .finish_non_exhaustive()
    }
}

impl FakeStation {
    /// A running station with this station id and instance id, an empty peer book, an empty log of
    /// [`FAKE_LOG_CAPACITY`] messages, [`SendOutcome::Deliver`], and one tower at
    /// [`FAKE_TOWER_URL`] in [`TowerState::Connecting`].
    pub fn new(station_id: impl Into<String>, instance_id: impl Into<String>) -> Self {
        let state = State {
            status: Status {
                station_id: station_id.into(),
                instance_id: instance_id.into(),
                towers: vec![TowerStatus {
                    url: FAKE_TOWER_URL.to_owned(),
                    state: TowerState::Connecting,
                }],
            },
            peers: BTreeMap::new(),
            log: VecDeque::new(),
            log_capacity: FAKE_LOG_CAPACITY,
            next_seq: 1,
            next_id: 1,
            outcome: SendOutcome::Deliver,
            sinks: Vec::new(),
            requests: Vec::new(),
            stopped: false,
            receipts: VecDeque::new(),
            now_ms: None,
            fail_calls: None,
            hold_subscribes: false,
            held_subscribes: 0,
        };
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(state),
                hook: Mutex::new(None),
                subscribers_changed: Notify::new(),
                subscribe_gate: Notify::new(),
            }),
        }
    }

    /// Keeps only the newest `capacity` messages (at least 1), evicting the oldest.
    pub fn with_log_capacity(self, capacity: usize) -> Self {
        {
            let mut state = self.state();
            state.log_capacity = capacity.max(1);
            state.evict();
        }
        self
    }

    /// Sets what happens to every later send ([`SendOutcome`]).
    pub fn set_outcome(&self, outcome: SendOutcome) {
        self.state().outcome = outcome;
    }

    /// Sets the state of the one tower and pushes [`Event::Status`].
    pub fn set_tower_state(&self, tower: TowerState) {
        let mut state = self.state();
        for t in &mut state.status.towers {
            t.state = tower.clone();
        }
        let status = state.status.clone();
        state.emit(&Event::Status { status });
    }

    /// Sets the link to one of a peer's stations and, if it changed, pushes [`Event::Peer`] (rule
    /// 10). Fails with [`ApiError::PeerNotFound`] for an unknown peer, and with
    /// [`ApiError::Invalid`] (field `station_id`) for a station the peer does not have.
    pub fn set_link(&self, peer: &str, station_id: &str, link: Link) -> Result<Peer, ApiError> {
        let mut state = self.state();
        let stations = state
            .peers
            .get_mut(peer)
            .ok_or_else(|| ApiError::PeerNotFound {
                name: peer.to_owned(),
            })?;
        let station = stations
            .iter_mut()
            .find(|s| s.station_id == station_id)
            .ok_or_else(|| invalid("station_id", "is not a station of this peer"))?;
        let changed = station.link != link;
        station.link = link;
        let whole = Peer {
            name: peer.to_owned(),
            stations: stations.clone(),
        };
        if changed {
            state.emit(&Event::Peer {
                peer: whole.clone(),
            });
        }
        Ok(whole)
    }

    /// Logs a message from `peer` with a new id (rule 8) and pushes its event. Every control
    /// character in `body` is replaced with a space (rule 9). Fails with [`ApiError::PeerNotFound`] for a peer not in
    /// the book, and with [`ApiError::Stopping`] after [`FakeStation::stop`].
    pub fn receive(&self, peer: &str, body: &str) -> Result<Message, ApiError> {
        let id = {
            let mut state = self.state();
            state.new_id()
        };
        self.receive_with_id(peer, &id, body)?
            .ok_or_else(|| ApiError::Internal {
                detail: "a new id was already logged".to_owned(),
            })
    }

    /// Logs a message from `peer` with the chat id `id`, unless the pair is among the newest
    /// [`RECEIPT_WINDOW`] accepted ones, across all peers: then it logs nothing, pushes nothing,
    /// leaves the window as it is and returns `None` (rule 8). After that many newer accepted
    /// messages the pair is forgotten and the id is logged again as a new message. The window does
    /// not depend on the log's capacity. Errors as [`FakeStation::receive`].
    pub fn receive_with_id(
        &self,
        peer: &str,
        id: &str,
        body: &str,
    ) -> Result<Option<Message>, ApiError> {
        let mut state = self.state();
        if state.stopped {
            return Err(ApiError::Stopping);
        }
        if !state.peers.contains_key(peer) {
            return Err(ApiError::PeerNotFound {
                name: peer.to_owned(),
            });
        }
        // Rule 8: a duplicate inside the window is dropped and does not move the window.
        if state.receipts.iter().any(|(p, i)| p == peer && i == id) {
            return Ok(None);
        }
        state.receipts.push_back((peer.to_owned(), id.to_owned()));
        while state.receipts.len() > RECEIPT_WINDOW {
            state.receipts.pop_front();
        }
        let body: String = body
            .chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect();
        Ok(Some(state.append(
            peer,
            id.to_owned(),
            Direction::Incoming,
            body,
            MessageStatus::Received,
        )))
    }

    /// Gives a held outgoing message its final status (`Delivered` or `Failed`): rev 1 and its
    /// event (rule 7). Fails with [`ApiError::MessageNotFound`] when the `seq` is not in the log,
    /// and with [`ApiError::Invalid`] when the message is not an outgoing one still `Sending`
    /// (field `seq`) or `status` is not a final outgoing status (field `status`).
    pub fn resolve(&self, seq: u64, status: MessageStatus) -> Result<Message, ApiError> {
        if !matches!(
            status,
            MessageStatus::Delivered | MessageStatus::Failed { .. }
        ) {
            return Err(invalid("status", "must be delivered or failed"));
        }
        self.state().finish(seq, status)
    }

    /// Stops the station: every later call and subscribe fails with [`ApiError::Stopping`], and
    /// every subscription ends (rule 11).
    pub fn stop(&self) {
        let mut state = self.state();
        state.stopped = true;
        state.sinks.clear();
        self.inner.subscribers_changed.notify_waiters();
    }

    /// Restarts the station as a new instance (rule 1): every subscription ends (rule 11), the log
    /// is emptied and `seq` starts again at 1, the receipt window of rule 8 is forgotten, every
    /// link is down, and calls are answered again
    /// after a [`FakeStation::stop`]. The peer book, the send outcome and the other test settings
    /// stay, as a station's peer book survives a restart.
    pub fn restart(&self, instance_id: impl Into<String>) {
        let mut state = self.state();
        state.status.instance_id = instance_id.into();
        state.sinks.clear();
        state.log.clear();
        state.receipts.clear();
        state.next_seq = 1;
        state.stopped = false;
        for stations in state.peers.values_mut() {
            for station in stations {
                station.link = Link::Down;
            }
        }
        self.inner.subscribers_changed.notify_waiters();
    }

    /// From now on every message that enters the log gets `ms` as its [`Message::at_ms`], until
    /// this is called again. Before the first call the wall clock is used.
    pub fn set_now_ms(&self, ms: u64) {
        self.state().now_ms = Some(ms);
    }

    /// While `Some`, every call answers with this error (after the [`FakeStation::on_call`] hook,
    /// and recorded in [`FakeStation::requests`]); `None` answers normally again. `subscribe` and
    /// open subscriptions are not affected.
    pub fn fail_calls(&self, error: Option<ApiError>) {
        self.state().fail_calls = error;
    }

    /// While `true`, `subscribe` does not register its subscription and does not return: it waits
    /// until this is set to `false` again. A test uses it to see that a transport's `subscribe`
    /// resolves only after the station registered the subscription (rule 3).
    pub fn hold_subscribes(&self, hold: bool) {
        self.state().hold_subscribes = hold;
        if !hold {
            self.inner.subscribe_gate.notify_waiters();
        }
    }

    /// How many `subscribe` calls are waiting because of [`FakeStation::hold_subscribes`].
    pub fn held_subscribes(&self) -> usize {
        self.state().held_subscribes
    }

    /// Waits until exactly `n` `subscribe` calls are held. Bound it with a timeout.
    pub async fn wait_for_held_subscribes(&self, n: usize) {
        loop {
            let notified = self.inner.subscribers_changed.notified();
            let mut notified = pin!(notified);
            notified.as_mut().enable();
            if self.held_subscribes() == n {
                return;
            }
            notified.await;
        }
    }

    /// The number of subscriptions whose [`Subscription`] is still alive.
    pub fn subscriber_count(&self) -> usize {
        let mut state = self.state();
        state.sinks.retain(|s| !s.is_closed());
        state.sinks.len()
    }

    /// Waits until exactly `n` subscriptions are alive. Wakes when one is added or dropped, so a
    /// test can wait for a server to notice a client that went away. Bound it with a timeout.
    pub async fn wait_for_subscribers(&self, n: usize) {
        loop {
            let notified = self.inner.subscribers_changed.notified();
            let mut notified = pin!(notified);
            notified.as_mut().enable();
            let sinks = {
                let mut state = self.state();
                state.sinks.retain(|s| !s.is_closed());
                state.sinks.clone()
            };
            if sinks.len() == n {
                return;
            }
            let mut closes: Vec<Pin<Box<dyn Future<Output = ()> + Send + '_>>> = sinks
                .iter()
                .map(|s| Box::pin(s.closed()) as Pin<Box<dyn Future<Output = ()> + Send>>)
                .collect();
            std::future::poll_fn(|cx| {
                if notified.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(());
                }
                for close in &mut closes {
                    if close.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(());
                    }
                }
                Poll::Pending
            })
            .await;
        }
    }

    /// Every request the fake received, in order, also those it refused.
    pub fn requests(&self) -> Vec<Request> {
        self.state().requests.clone()
    }

    /// The whole log, oldest first.
    pub fn messages(&self) -> Vec<Message> {
        self.state().log.iter().cloned().collect()
    }

    /// The status the fake answers with.
    pub fn status(&self) -> Status {
        self.state().status.clone()
    }

    /// Runs `hook` at the start of every later call, before the fake looks at the request, with
    /// the fake unlocked: the hook may change the station (receive a message, for example), so a
    /// test can make things happen exactly between a client's calls. Replaces an earlier hook.
    pub fn on_call(&self, hook: impl FnMut(&FakeStation, &Request) + Send + 'static) {
        *self.hook() = Some(Box::new(hook));
    }

    /// Removes the hook of [`FakeStation::on_call`].
    pub fn clear_on_call(&self) {
        *self.hook() = None;
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn hook(&self) -> MutexGuard<'_, Option<CallHook>> {
        self.inner
            .hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn run_hook(&self, request: &Request) {
        // Taken out while it runs, so the hook may call into the fake (and even replace itself).
        let Some(mut hook) = self.hook().take() else {
            return;
        };
        hook(self, request);
        let mut slot = self.hook();
        if slot.is_none() {
            *slot = Some(hook);
        }
    }

    fn handle(&self, request: Request) -> Result<Response, ApiError> {
        self.run_hook(&request);
        let mut state = self.state();
        state.requests.push(request.clone());
        if let Some(error) = &state.fail_calls {
            return Err(error.clone());
        }
        if state.stopped {
            return Err(ApiError::Stopping);
        }
        match request {
            Request::Status => Ok(Response::Status {
                status: state.status.clone(),
            }),
            Request::Peers => Ok(Response::Peers {
                peers: state
                    .peers
                    .iter()
                    .map(|(name, stations)| Peer {
                        name: name.clone(),
                        stations: stations.clone(),
                    })
                    .collect(),
            }),
            Request::PeerAdd { name, station_id } => state.peer_add(name, station_id),
            Request::PeerRemove { name } => state.peer_remove(name),
            Request::Send { peer, body } => state.send(peer, body),
            Request::Message { seq } => state
                .log
                .iter()
                .find(|m| m.seq == seq)
                .map(|m| Response::Message { message: m.clone() })
                .ok_or(ApiError::MessageNotFound { seq }),
            Request::Log {
                peer,
                before,
                limit,
            } => Ok(Response::Log {
                page: page(&state.log, peer.as_deref(), before, limit),
            }),
        }
    }
}

impl Transport for FakeStation {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        self.handle(request)
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        let mut held: Option<HeldSubscribe<'_>> = None;
        loop {
            let gate = self.inner.subscribe_gate.notified();
            let mut gate = pin!(gate);
            gate.as_mut().enable();
            if !self.state().hold_subscribes {
                break;
            }
            if held.is_none() {
                held = Some(HeldSubscribe::new(self));
            }
            gate.await;
        }
        drop(held);
        let mut state = self.state();
        if state.stopped {
            return Err(ApiError::Stopping);
        }
        let (sink, events) = subscription(SUBSCRIPTION_CAPACITY);
        state.sinks.push(Arc::new(sink));
        self.inner.subscribers_changed.notify_waiters();
        Ok(events)
    }
}

/// Counts one held `subscribe` while it lives, also when the call is dropped.
struct HeldSubscribe<'a>(&'a FakeStation);

impl<'a> HeldSubscribe<'a> {
    fn new(fake: &'a FakeStation) -> Self {
        fake.state().held_subscribes += 1;
        fake.inner.subscribers_changed.notify_waiters();
        Self(fake)
    }
}

impl Drop for HeldSubscribe<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state();
        state.held_subscribes = state.held_subscribes.saturating_sub(1);
        self.0.inner.subscribers_changed.notify_waiters();
    }
}

impl State {
    fn new_id(&mut self) -> String {
        let id = format!("FAKE-MSG-{}", self.next_id);
        self.next_id += 1;
        id
    }

    /// Pushes `event` to every subscription, dropping those whose [`Subscription`] is gone.
    fn emit(&mut self, event: &Event) {
        self.sinks.retain(|s| s.push(event.clone()).is_ok());
    }

    fn evict(&mut self) {
        while self.log.len() > self.log_capacity {
            self.log.pop_front();
        }
    }

    /// Logs a new message with the next `seq` and rev 0, and pushes its event.
    fn append(
        &mut self,
        peer: &str,
        id: String,
        direction: Direction,
        body: String,
        status: MessageStatus,
    ) -> Message {
        let message = Message {
            seq: self.next_seq,
            rev: 0,
            id,
            peer: peer.to_owned(),
            direction,
            body,
            at_ms: self.now_ms.unwrap_or_else(now_ms),
            status,
        };
        self.next_seq += 1;
        self.log.push_back(message.clone());
        self.evict();
        self.emit(&Event::Message {
            message: message.clone(),
        });
        message
    }

    /// Moves an outgoing message that is still `Sending` to `status`, with the next rev and its
    /// event.
    fn finish(&mut self, seq: u64, status: MessageStatus) -> Result<Message, ApiError> {
        let message = self
            .log
            .iter_mut()
            .find(|m| m.seq == seq)
            .ok_or(ApiError::MessageNotFound { seq })?;
        if message.direction != Direction::Outgoing || message.status != MessageStatus::Sending {
            return Err(invalid(
                "seq",
                "is not an outgoing message that is still sending",
            ));
        }
        message.status = status;
        message.rev += 1;
        let message = message.clone();
        self.emit(&Event::Message {
            message: message.clone(),
        });
        Ok(message)
    }

    fn peer_add(&mut self, name: String, station_id: String) -> Result<Response, ApiError> {
        check_peer_name(&name)?;
        if station_id.is_empty()
            || station_id
                .chars()
                .any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(invalid("station_id", "is not a station id"));
        }
        if station_id == self.status.station_id {
            return Err(invalid("station_id", "is this station's own id"));
        }
        let owner = self.peers.iter().find(|(owner, stations)| {
            **owner != name && stations.iter().any(|s| s.station_id == station_id)
        });
        if let Some((owner, _)) = owner {
            return Err(ApiError::StationTaken {
                station_id,
                peer: owner.clone(),
            });
        }
        let stations = self.peers.entry(name.clone()).or_default();
        let changed = !stations.iter().any(|s| s.station_id == station_id);
        if changed {
            stations.push(PeerStation {
                station_id,
                link: Link::Down,
            });
        }
        let peer = Peer {
            name,
            stations: stations.clone(),
        };
        if changed {
            self.emit(&Event::Peer { peer: peer.clone() });
        }
        Ok(Response::Peer { peer })
    }

    fn peer_remove(&mut self, name: String) -> Result<Response, ApiError> {
        if !self.peers.contains_key(&name) {
            return Err(ApiError::PeerNotFound { name });
        }
        let pending: Vec<u64> = self
            .log
            .iter()
            .filter(|m| {
                m.peer == name
                    && m.direction == Direction::Outgoing
                    && m.status == MessageStatus::Sending
            })
            .map(|m| m.seq)
            .collect();
        for seq in pending {
            self.finish(
                seq,
                MessageStatus::Failed {
                    reason: FailReason::PeerRemoved,
                },
            )?;
        }
        self.peers.remove(&name);
        self.emit(&Event::PeerRemoved { name });
        Ok(Response::Done)
    }

    fn send(&mut self, peer: String, body: String) -> Result<Response, ApiError> {
        if !self.peers.contains_key(&peer) {
            return Err(ApiError::PeerNotFound { name: peer });
        }
        if body.is_empty() {
            return Err(invalid("body", "must not be empty"));
        }
        if body.len() > BODY_MAX_BYTES {
            return Err(invalid(
                "body",
                &format!("must be at most {BODY_MAX_BYTES} bytes"),
            ));
        }
        let id = self.new_id();
        let message = self.append(&peer, id, Direction::Outgoing, body, MessageStatus::Sending);
        let outcome = match self.outcome {
            SendOutcome::Deliver => Some(MessageStatus::Delivered),
            SendOutcome::Fail(reason) => Some(MessageStatus::Failed { reason }),
            SendOutcome::Hold => None,
        };
        if let Some(status) = outcome {
            // A capacity of 1 can evict the message before its outcome; then there is nothing to
            // change, as for a real station.
            if self.log.iter().any(|m| m.seq == message.seq) {
                self.finish(message.seq, status)?;
            }
        }
        Ok(Response::Message { message })
    }
}

/// One page of `log` (oldest first) as rule 6 in [`crate::api`] defines it.
fn page(log: &VecDeque<Message>, peer: Option<&str>, before: Option<u64>, limit: u32) -> LogPage {
    let mut newest_first = log
        .iter()
        .rev()
        .filter(|m| peer.is_none_or(|p| m.peer == p) && before.is_none_or(|b| m.seq < b));
    let cap = usize::try_from(limit.min(LOG_PAGE_MAX_MESSAGES)).unwrap_or(usize::MAX);
    if cap == 0 {
        return LogPage {
            messages: Vec::new(),
            next_before: newest_first
                .next()
                .map(|newest| before.unwrap_or(newest.seq + 1)),
        };
    }
    let mut messages = Vec::new();
    let mut bytes = 0usize;
    let mut more = false;
    for message in newest_first {
        let len = serde_json::to_vec(message).map_or(usize::MAX, |json| json.len());
        if messages.len() == cap
            || (!messages.is_empty() && bytes.saturating_add(len) > LOG_PAGE_MAX_BYTES)
        {
            more = true;
            break;
        }
        bytes = bytes.saturating_add(len);
        messages.push(message.clone());
    }
    messages.reverse();
    let next_before = if more {
        messages.first().map(|m| m.seq)
    } else {
        None
    };
    LogPage {
        messages,
        next_before,
    }
}

fn invalid(field: &str, reason: &str) -> ApiError {
    ApiError::Invalid {
        field: field.to_owned(),
        reason: reason.to_owned(),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}
