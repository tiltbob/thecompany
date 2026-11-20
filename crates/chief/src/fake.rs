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
//! A handle can be bound to one instance ([`BindInstance`]): see "A bound handle" on
//! [`FakeStation`].
//!
//! What it leaves out: links never change on their own (a test sets them with
//! [`FakeStation::set_link`]), there is no send deadline (a held send stays `Sending` until
//! [`FakeStation::resolve`]), and nothing is persisted.

use crate::api::{
    ApiError, BODY_MAX_BYTES, Direction, Event, FailReason, LOG_PAGE_MAX_BYTES,
    LOG_PAGE_MAX_MESSAGES, Link, LogPage, MESSAGE_ID_MAX_BYTES, Message, MessageStatus, Peer,
    PeerStation, RECEIPT_WINDOW, Request, Response, Status, TowerState, TowerStatus,
    check_peer_name,
};
use crate::client::{
    BindInstance, EventSink, SUBSCRIPTION_CAPACITY, Subscription, Transport, subscription,
};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::pin;
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

/// The hook of [`FakeStation::on_call`], and how often it was set or removed. A hook is taken out
/// of the slot while it runs, and is put back afterwards only if the slot was not changed
/// meanwhile: a hook that removes or replaces itself stays removed or replaced.
#[derive(Default)]
struct HookSlot {
    hook: Option<CallHook>,
    changes: u64,
}

/// An in-memory station that implements [`Transport`]. Cheap to clone; clones share one station.
///
/// The outcome of a send is applied within the call, after the answer was made: the answer holds
/// the `Sending` message (rev 0), and the event of the outcome (rev 1) is pushed before the call
/// returns. So the outcome's event is queued before the caller sees the answer (rule 4).
///
/// # A bound handle
///
/// [`BindInstance::bound_to`] gives a handle of the same station that is bound to one instance;
/// a clone of it is bound too, and the station and its other handles are not. A bound handle
/// compares the station's current instance id with its own:
///
/// - For a call, first before the [`FakeStation::on_call`] hook: a handle bound to an instance
///   that is already gone refuses the call there, and the hook does not run. Then again after the
///   hook, under the same lock that then handles the call, so a restart made by the hook itself
///   (or by anything else while it ran) is caught too.
/// - For a subscribe (which runs no hook), under the same lock that registers the subscription,
///   also after a wait at [`FakeStation::hold_subscribes`].
///
/// When they differ, the call or subscribe fails with [`ApiError::Disconnected`] and the request
/// never reached the station: it is not recorded in [`FakeStation::requests`], not handled, logs
/// nothing and pushes no event, and no subscription is registered. The binding concerns the
/// [`Transport`] methods only; the test controls ([`FakeStation::receive`],
/// [`FakeStation::restart`] and the others) act on the shared station from any handle.
#[derive(Clone)]
pub struct FakeStation {
    inner: Arc<Inner>,
    /// The instance this handle is bound to, if any.
    bound: Option<Arc<str>>,
}

struct Inner {
    state: Mutex<State>,
    hook: Mutex<HookSlot>,
    /// Woken when a subscription is added, when the number of held subscribes changes, and when
    /// the station stops or restarts.
    subscribers_changed: Notify,
    /// Woken when held subscribes may go on or must give up: the hold is released, or the station
    /// stops or restarts.
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
    /// Owned here only: nothing else holds a sink, so dropping one ends its subscription.
    sinks: Vec<EventSink>,
    requests: Vec<Request>,
    stopped: bool,
    /// Grows by 1 on every stop and restart. A `subscribe` belongs to the generation it began in,
    /// and gives up with [`ApiError::Stopping`] when that one has ended.
    generation: u64,
    /// Every outgoing message that is still `Sending`, by `seq`, kept apart from the log so that
    /// its outcome is pushed even after the log has evicted it (rule 7).
    pending: BTreeMap<u64, Message>,
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
            .field("bound", &self.bound.is_some())
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
            generation: 0,
            pending: BTreeMap::new(),
            receipts: VecDeque::new(),
            now_ms: None,
            fail_calls: None,
            hold_subscribes: false,
            held_subscribes: 0,
        };
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(state),
                hook: Mutex::new(HookSlot::default()),
                subscribers_changed: Notify::new(),
                subscribe_gate: Notify::new(),
            }),
            bound: None,
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
    ///
    /// The text in the state (the `error` of [`TowerState::Failing`], the `reason` of
    /// [`TowerState::Denied`]) came from outside, so every control character in it is replaced with
    /// a space (rule 9) before it is stored and before the event is pushed.
    pub fn set_tower_state(&self, tower: TowerState) {
        let tower = match tower {
            TowerState::Failing { error } => TowerState::Failing {
                error: scrub(&error),
            },
            TowerState::Denied { reason } => TowerState::Denied {
                reason: scrub(&reason),
            },
            other @ (TowerState::Connecting | TowerState::Connected) => other,
        };
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

    /// Logs a message from `peer` with a new id (rule 8) and pushes its event, as a station does
    /// with a message that arrived from a peer's station. Every control character in `body` is
    /// replaced with a space (rule 9).
    ///
    /// It follows the limits a station applies to what arrives, and a refused message changes
    /// nothing: no receipt, no log entry, no event, no `seq` and no id used. The checks run in this
    /// order: [`ApiError::Stopping`] after [`FakeStation::stop`]; [`ApiError::PeerNotFound`] for a
    /// peer not in the book; [`ApiError::Invalid`] with field `body` for a body over
    /// [`BODY_MAX_BYTES`] (counted before control characters are replaced). An empty body is
    /// accepted: only [`Request::Send`] has a lower bound (rule 7), and a peer may send an empty
    /// body. Only a message that
    /// passes them takes a new id, `FAKE-MSG-<n>`, which has the form of [`Message::id`]. Fails
    /// with [`ApiError::Internal`] in the one case where that new id is already in the receipt
    /// window, which happens only when a test passed an id of the fake's own form to
    /// [`FakeStation::receive_with_id`].
    pub fn receive(&self, peer: &str, body: &str) -> Result<Message, ApiError> {
        self.state()
            .receive(peer, None, body)?
            .ok_or_else(|| ApiError::Internal {
                detail: "a new id was already logged".to_owned(),
            })
    }

    /// Logs a message from `peer` with the chat id `id`, as [`FakeStation::receive`] does, unless
    /// the pair is among the newest [`RECEIPT_WINDOW`] accepted ones, across all peers: then it
    /// logs nothing, pushes nothing, leaves the window as it is and returns `None` (rule 8). After
    /// that many newer accepted messages the pair is forgotten and the id is logged again as a new
    /// message. The window does not depend on the log's capacity.
    ///
    /// A refused message changes nothing: no receipt, no log entry, no event, no `seq` used. The
    /// checks run in this order: [`ApiError::Stopping`] after [`FakeStation::stop`];
    /// [`ApiError::PeerNotFound`] for a peer not in the book; [`ApiError::Invalid`] with field `id`
    /// for an id without the form of [`Message::id`] (1 to [`MESSAGE_ID_MAX_BYTES`] ASCII letters,
    /// digits, `_` or `-`), which a station refuses from a peer; [`ApiError::Invalid`] with field
    /// `body` for a body over [`BODY_MAX_BYTES`] (counted before control characters are replaced;
    /// an empty body is accepted); and only then the duplicate check of rule 8.
    pub fn receive_with_id(
        &self,
        peer: &str,
        id: &str,
        body: &str,
    ) -> Result<Option<Message>, ApiError> {
        self.state().receive(peer, Some(id), body)
    }

    /// Gives a held outgoing message its final status (`Delivered` or `Failed`): rev 1 and its
    /// event (rule 7), also when the log has evicted the message meanwhile (the log then stays as
    /// it is). Fails with [`ApiError::Invalid`] when `status` is not a final outgoing status (field
    /// `status`), or when the `seq` is in the log but is not an outgoing message still `Sending`
    /// (field `seq`); and with [`ApiError::MessageNotFound`] for any other `seq` that is not a
    /// held message.
    pub fn resolve(&self, seq: u64, status: MessageStatus) -> Result<Message, ApiError> {
        if !matches!(
            status,
            MessageStatus::Delivered | MessageStatus::Failed { .. }
        ) {
            return Err(invalid("status", "must be delivered or failed"));
        }
        self.state().finish(seq, status)
    }

    /// Stops the station: every subscription ends (rule 11), and every later call and subscribe
    /// fails with [`ApiError::Stopping`], at once also while [`FakeStation::hold_subscribes`] is
    /// on. A subscribe that is waiting at that hold wakes up and fails with
    /// [`ApiError::Stopping`] too: it belonged to the instance that stopped, and never attaches to
    /// a later one.
    pub fn stop(&self) {
        {
            let mut state = self.state();
            state.stopped = true;
            state.generation += 1;
            state.sinks.clear();
        }
        self.inner.subscribe_gate.notify_waiters();
        self.inner.subscribers_changed.notify_waiters();
    }

    /// Restarts the station as a new instance (rule 1): every subscription ends (rule 11), the log
    /// is emptied and `seq` starts again at 1, the receipt window of rule 8 is forgotten, every
    /// link is down, and calls are answered again after a [`FakeStation::stop`]. A subscribe that
    /// is waiting at [`FakeStation::hold_subscribes`] wakes up and fails with
    /// [`ApiError::Stopping`]: it belonged to the old instance and never attaches to the new one.
    /// The peer book, the send outcome and the other test settings, the hold of subscribes
    /// included, stay, as a station's peer book survives a restart.
    pub fn restart(&self, instance_id: impl Into<String>) {
        {
            let mut state = self.state();
            state.status.instance_id = instance_id.into();
            state.sinks.clear();
            state.log.clear();
            state.pending.clear();
            state.receipts.clear();
            state.next_seq = 1;
            state.stopped = false;
            state.generation += 1;
            for stations in state.peers.values_mut() {
                for station in stations {
                    station.link = Link::Down;
                }
            }
        }
        self.inner.subscribe_gate.notify_waiters();
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
    /// until this is set to `false` again, or until the station stops or restarts, which makes it
    /// fail with [`ApiError::Stopping`]. A test uses it to see that a transport's `subscribe`
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

    /// Waits until exactly `n` `subscribe` calls are held. Bound it with a timeout. It holds nothing
    /// of the station's but a borrow of the fake while it waits.
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

    /// Waits until exactly `n` subscriptions are alive, so a test can wait for a server to notice
    /// a client that went away. Bound it with a timeout.
    ///
    /// It polls: it counts, and while the count is not `n` it yields to the runtime once (waking
    /// itself) and counts again, because a client that drops its [`Subscription`] does not notify
    /// the fake. Between counts it holds no sink and no future borrowed from one, so a waiting or
    /// abandoned call never keeps a subscription open after [`FakeStation::stop`],
    /// [`FakeStation::restart`] or anything else dropped the station's end of it.
    pub async fn wait_for_subscribers(&self, n: usize) {
        while self.subscriber_count() != n {
            yield_once().await;
        }
    }

    /// Every request the fake received, in order, also those it refused. A request that a bound
    /// handle turned away because another instance runs never reached the station and is not here.
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
    ///
    /// A call through a handle bound to an instance that is already gone (see "A bound handle" on
    /// [`FakeStation`]) is refused before the hook, which then does not run for it. `subscribe`
    /// runs no hook.
    ///
    /// A hook may call [`FakeStation::clear_on_call`] or `on_call` itself: it then does not run
    /// again, so a hook that removes itself runs for one call only.
    pub fn on_call(&self, hook: impl FnMut(&FakeStation, &Request) + Send + 'static) {
        let mut slot = self.hook();
        slot.hook = Some(Box::new(hook));
        slot.changes += 1;
    }

    /// Removes the hook of [`FakeStation::on_call`], also when called from inside the hook.
    pub fn clear_on_call(&self) {
        let mut slot = self.hook();
        slot.hook = None;
        slot.changes += 1;
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn hook(&self) -> MutexGuard<'_, HookSlot> {
        self.inner
            .hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn run_hook(&self, request: &Request) {
        // Taken out while it runs, so the hook may call into the fake, and remove or replace
        // itself: it is put back only if nobody changed the slot while it ran.
        let (mut hook, changes) = {
            let mut slot = self.hook();
            let Some(hook) = slot.hook.take() else {
                return;
            };
            (hook, slot.changes)
        };
        hook(self, request);
        let mut slot = self.hook();
        if slot.changes == changes {
            slot.hook = Some(hook);
        }
    }

    /// [`ApiError::Disconnected`] if this handle is bound to an instance other than the one in
    /// `state`. The decisive check is made under the lock the caller then handles the request
    /// under; [`FakeStation::handle`] also checks once before the hook.
    fn other_instance(&self, state: &State) -> Option<ApiError> {
        let bound = self.bound.as_deref()?;
        (state.status.instance_id != bound).then(|| ApiError::Disconnected {
            detail: "the station restarted: another instance runs now".to_owned(),
        })
    }

    fn handle(&self, request: Request) -> Result<Response, ApiError> {
        // Before the hook: a handle bound to an instance that is already gone refuses the call
        // without running the hook, so the instance that runs now sees nothing of it. The guard is
        // a temporary of this statement and is dropped before the hook runs.
        let stale = self.other_instance(&self.state());
        if let Some(error) = stale {
            return Err(error);
        }
        self.run_hook(&request);
        let mut state = self.state();
        // Again after the hook, under the lock that handles the request: a restart made by the hook
        // (or by another thread meanwhile) is caught, and nothing else can come in between.
        if let Some(error) = self.other_instance(&state) {
            return Err(error);
        }
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

impl BindInstance for FakeStation {
    /// A handle of the same station bound to `instance_id` (see "A bound handle" on
    /// [`FakeStation`]). Once another instance runs, its calls are refused before the
    /// [`FakeStation::on_call`] hook runs and without being recorded; a restart made by the hook
    /// itself is caught after it.
    fn bound_to(&self, instance_id: &str) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            bound: Some(Arc::from(instance_id)),
        }
    }
}

impl Transport for FakeStation {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        self.handle(request)
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        // The subscribe belongs to the instance it began in: it fails once that one has stopped
        // or restarted, also while it waits at the hold.
        let generation = {
            let state = self.state();
            if let Some(error) = self.other_instance(&state) {
                return Err(error);
            }
            if state.stopped {
                return Err(ApiError::Stopping);
            }
            state.generation
        };
        let mut held: Option<HeldSubscribe<'_>> = None;
        loop {
            // Registered before the look, so a release, stop or restart after it still wakes us.
            let gate = self.inner.subscribe_gate.notified();
            let mut gate = pin!(gate);
            gate.as_mut().enable();
            let registered = {
                let mut state = self.state();
                // A bound handle reports another instance as such, also after a wait at the hold.
                if let Some(error) = self.other_instance(&state) {
                    Some(Err(error))
                } else if state.stopped || state.generation != generation {
                    Some(Err(ApiError::Stopping))
                } else if state.hold_subscribes {
                    None
                } else {
                    let (sink, events) = subscription(SUBSCRIPTION_CAPACITY);
                    state.sinks.push(sink);
                    Some(Ok(events))
                }
            };
            if let Some(result) = registered {
                // Dropped with the state unlocked: it locks the state to count itself out.
                drop(held);
                self.inner.subscribers_changed.notify_waiters();
                return result;
            }
            if held.is_none() {
                held = Some(HeldSubscribe::new(self));
            }
            gate.await;
        }
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
    /// [`FakeStation::receive`] (`id` is `None`) and [`FakeStation::receive_with_id`]: the checks
    /// in their order, then the receipt window, then the log.
    fn receive(
        &mut self,
        peer: &str,
        id: Option<&str>,
        body: &str,
    ) -> Result<Option<Message>, ApiError> {
        if self.stopped {
            return Err(ApiError::Stopping);
        }
        if !self.peers.contains_key(peer) {
            return Err(ApiError::PeerNotFound {
                name: peer.to_owned(),
            });
        }
        if id.is_some_and(|id| !is_message_id(id)) {
            return Err(invalid(
                "id",
                &format!("must be 1 to {MESSAGE_ID_MAX_BYTES} ASCII letters, digits, '_' or '-'"),
            ));
        }
        // An empty body is accepted: rule 7's lower bound is for `Send`, and the chat protocol
        // lets a peer send an empty one.
        if body.len() > BODY_MAX_BYTES {
            return Err(invalid(
                "body",
                &format!("must be at most {BODY_MAX_BYTES} bytes"),
            ));
        }
        let id = match id {
            Some(id) => id.to_owned(),
            None => self.new_id(),
        };
        // Rule 8: a duplicate inside the window is dropped and does not move the window.
        if self.receipts.iter().any(|(p, i)| p == peer && *i == id) {
            return Ok(None);
        }
        self.receipts.push_back((peer.to_owned(), id.clone()));
        while self.receipts.len() > RECEIPT_WINDOW {
            self.receipts.pop_front();
        }
        Ok(Some(self.append(
            peer,
            id,
            Direction::Incoming,
            scrub(body),
            MessageStatus::Received,
        )))
    }

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

    /// Moves an outgoing message that is still `Sending` to `status`, with the next rev, and pushes
    /// its event, whether or not the log still holds it (rule 7). The log's entry is updated only
    /// if it is still there.
    fn finish(&mut self, seq: u64, status: MessageStatus) -> Result<Message, ApiError> {
        let Some(mut message) = self.pending.remove(&seq) else {
            return Err(if self.log.iter().any(|m| m.seq == seq) {
                invalid("seq", "is not an outgoing message that is still sending")
            } else {
                ApiError::MessageNotFound { seq }
            });
        };
        message.status = status;
        message.rev += 1;
        if let Some(entry) = self.log.iter_mut().find(|m| m.seq == seq) {
            *entry = message.clone();
        }
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
        // In the log or evicted from it: every send to this peer that is still `Sending`.
        let pending: Vec<u64> = self
            .pending
            .values()
            .filter(|m| m.peer == name)
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
        self.pending.insert(message.seq, message.clone());
        let outcome = match self.outcome {
            SendOutcome::Deliver => Some(MessageStatus::Delivered),
            SendOutcome::Fail(reason) => Some(MessageStatus::Failed { reason }),
            SendOutcome::Hold => None,
        };
        if let Some(status) = outcome {
            self.finish(message.seq, status)?;
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

/// Whether `id` has the form of [`Message::id`]: 1 to [`MESSAGE_ID_MAX_BYTES`] ASCII letters,
/// digits, `_` or `-`.
fn is_message_id(id: &str) -> bool {
    (1..=MESSAGE_ID_MAX_BYTES).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Rule 9: every control character replaced with a space.
fn scrub(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

/// Wakes its own task once and returns `Pending`, then is ready: one turn back to the runtime.
fn yield_once() -> impl Future<Output = ()> {
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
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
