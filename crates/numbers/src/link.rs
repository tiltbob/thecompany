//! Authenticated, bidirectional links and chat stream dispatch.

mod streams;

#[cfg(test)]
mod close_wait_tests;

use std::collections::HashMap;
use std::fmt;
use std::future::pending;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use iroh::endpoint::{Connection, ConnectionError, FrameStats, RecvStream, SendStream, Side};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use n0_future::{FuturesUnordered, StreamExt};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, timeout};

use crate::endpoint::QUIC_KEEPALIVE;
use crate::proto::chat::{ChatMessage, ChatReply, NackReason};
use crate::proto::{CHAT_PROTOCOL, Hello};
use crate::{Authorizer, CLOSE_NOT_ALLOWED, Endpoint, NodeId, Protocol, generate_node_key};

use self::streams::{
    close_code, exchange_hello, read_chat_reply, receive_chat, write_chat_request,
};

/// The ALPN of the numbers link.
pub const LINK_ALPN: &[u8] = b"numbers/0";

const CLOSE_BAD_HELLO: u32 = 400;
const CLOSE_REPLACED: u32 = 409;
const CLOSE_LINK: u32 = 0;
const STREAM_LIMIT: usize = 16;
const MAILBOX_CAPACITY: usize = 32;
const STREAM_CREDIT_MARGIN: Duration = Duration::from_secs(1);

/// Time budgets for a link and its chat operations.
#[derive(Debug, Clone, Copy)]
pub struct LinkConfig {
    /// Budget for transport setup and the application hello together.
    ///
    /// This may equal or exceed [`Self::send_deadline`], which still bounds a send.
    /// The progress check after writing a message needs a positive interval shorter
    /// than the remaining send budget. A wait for stream credit also allows time for
    /// a transport keepalive, and its progress observation can span several sends.
    pub connect_timeout: Duration,
    /// Budget for one send, including any dials and retries.
    pub send_deadline: Duration,
}

/// The selected path of a link to a peer station.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum LinkPath {
    /// No usable path is selected.
    #[default]
    Down,
    /// The selected path goes through a tower.
    Relayed,
    /// The selected path is direct.
    Direct,
}

/// A change to the selected path for a station.
#[derive(Clone, Copy)]
pub struct PathUpdate {
    /// The authenticated station whose path changed.
    pub station_id: NodeId,
    /// The new selected path.
    pub path: LinkPath,
}

impl fmt::Debug for PathUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PathUpdate")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

/// A validated chat awaiting the receiver's decision.
pub struct IncomingChat {
    /// The authenticated remote station.
    pub station_id: NodeId,
    /// The local name assigned to that station in the peer book.
    pub peer: Arc<str>,
    /// The received message. Its time and text still come from the peer.
    pub message: ChatMessage,
    reply: oneshot::Sender<ChatReply>,
}

impl IncomingChat {
    /// Responds to the sender. A receiver should ACK only after accepting the message.
    ///
    /// The receiver can commit even if the sender's stream has already closed.
    pub fn respond(self, reply: ChatReply) {
        let _ = self.reply.send(reply);
    }
}

/// Why a send failed before receiving a chat reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SendError {
    /// The outgoing message failed validation.
    #[error("invalid chat message")]
    InvalidMessage(NackReason),
    /// A deadline expired or a retry could not confirm the result of an earlier attempt.
    #[error("no definitive chat reply")]
    Timeout,
    /// Authorization was refused.
    #[error("the station is not authorized for chat")]
    NotAllowed,
    /// The station closed its link or shut down the link runtime.
    #[error("the link was closed")]
    Closed,
    /// The connection could not be established or went away.
    #[error("the peer connection is unavailable")]
    Disconnected,
    /// The peer did not complete a valid hello.
    #[error("invalid link hello")]
    BadHello,
    /// The response frame did not contain a valid reply.
    #[error("invalid chat reply")]
    BadReply,
}

/// A link background task failed during shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a link worker failed")]
pub struct ShutdownError;

/// Owns the router, the endpoint and the outgoing link workers.
pub struct Link {
    shared: Arc<Shared>,
    workers: Arc<Mutex<Option<Workers>>>,
    router: Router,
}

impl Link {
    /// Starts the router and returns its bounded incoming-chat mailbox.
    ///
    /// This performs no network wait. Call from a Tokio runtime with time enabled.
    pub fn start(endpoint: Endpoint, config: LinkConfig) -> (Self, mpsc::Receiver<IncomingChat>) {
        let (shared, receive) = Shared::new(&endpoint, config);
        let workers = Arc::new(Mutex::new(Some(Workers {
            endpoint: endpoint.iroh().clone(),
            tasks: JoinSet::new(),
            failed: false,
        })));
        let router = endpoint
            .router_builder()
            .accept(LINK_ALPN, IncomingLinks(Arc::clone(&shared)))
            .spawn();
        (
            Self {
                shared,
                workers,
                router,
            },
            receive,
        )
    }

    /// Gets a handle for station operations.
    pub fn handle(&self) -> LinkHandle {
        LinkHandle {
            shared: Arc::clone(&self.shared),
            workers: Arc::downgrade(&self.workers),
        }
    }

    /// Closes links and the incoming mailbox, then waits for the workers and router.
    pub async fn shutdown(self) -> Result<(), ShutdownError> {
        self.shared.stop();
        let workers = lock(&self.workers).take();
        let mut outgoing_failed = false;
        if let Some(mut workers) = workers {
            while let Some(result) = workers.tasks.join_next().await {
                outgoing_failed |= result.is_err();
            }
            outgoing_failed |= workers.failed;
        }
        let router_failed = self.router.shutdown().await.is_err();
        if outgoing_failed || router_failed {
            return Err(ShutdownError);
        }
        Ok(())
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.shared.stop();
        // Dropping the join set aborts any task still running after a cancelled shutdown.
        lock(&self.workers).take();
    }
}

/// A cloneable handle for chat sends, link closure and path status.
#[derive(Clone)]
pub struct LinkHandle {
    shared: Arc<Shared>,
    workers: Weak<Mutex<Option<Workers>>>,
}

impl LinkHandle {
    /// Sends one message on a bidirectional chat stream.
    ///
    /// Cancelling this call releases its stream. Other senders can still use the shared dial.
    pub async fn send(
        &self,
        station: NodeId,
        message: ChatMessage,
    ) -> Result<ChatReply, SendError> {
        message.validate().map_err(SendError::InvalidMessage)?;
        let peer = self.shared.peer(station)?;
        if self
            .shared
            .authorizer
            .authorize(&station, Protocol::Stream(CHAT_PROTOCOL))
            .is_none()
        {
            return Err(SendError::NotAllowed);
        }
        timeout(
            self.shared.config.send_deadline,
            self.send_message(&peer, &message),
        )
        .await
        .map_err(|_| SendError::Timeout)?
    }

    async fn send_message(
        &self,
        peer: &Arc<Peer>,
        message: &ChatMessage,
    ) -> Result<ChatReply, SendError> {
        let start = Instant::now();
        let config = self.shared.config;
        let mut first_attempt = true;
        let mut stale_retry_available = true;
        let mut replacement_retry_available = true;
        loop {
            let lease = self.connection(peer).await?;
            let connection = &lease.transport;
            let result = send_chat(
                peer,
                connection,
                message,
                config,
                start,
                first_attempt && lease.admitted_at < start,
            )
            .await;
            let error = match result {
                // The earlier attempt may still be committed after the retry was refused.
                Ok(ChatReply::Nack(NackReason::Unavailable)) if !first_attempt => {
                    return Err(SendError::Timeout);
                }
                Ok(reply) => return Ok(reply),
                Err(error) => error,
            };
            if error == SendError::Disconnected && connection.close_reason().is_none() {
                // A stream can end before this connection's close becomes visible.
                let remaining = config.send_deadline.saturating_sub(start.elapsed());
                wait_for_close(
                    peer,
                    connection.closed(),
                    config.connect_timeout.min(remaining),
                )
                .await?;
            }
            let state = peer.state();
            if state.revoked {
                return Err(SendError::Closed);
            }
            if error != SendError::Disconnected {
                return Err(error);
            }
            let reason = connection.close_reason();
            let code = reason.as_ref().and_then(close_code);
            if code == Some(CLOSE_NOT_ALLOWED) {
                return Err(SendError::NotAllowed);
            }
            // LocallyClosed omits the wire code. Admission records its decision on the
            // exact connection under the peer lock so unrelated redials are not mistaken for it.
            let replaced = code == Some(CLOSE_REPLACED)
                || (matches!(reason, Some(ConnectionError::LocallyClosed))
                    && lease.replaced_locally.load(Ordering::Relaxed));
            // Retrying the same message preserves receipt de-duplication. A redial can
            // lose a crossed-dial election, so allow one observed replacement separately.
            let retry_available = if replaced {
                &mut replacement_retry_available
            } else if reason.is_some() {
                &mut stale_retry_available
            } else {
                return Err(error);
            };
            if !*retry_available {
                return Err(error);
            }
            *retry_available = false;
            first_attempt = false;
        }
    }

    async fn connection(&self, peer: &Arc<Peer>) -> Result<Arc<EstablishedConnection>, SendError> {
        // Subscribe before inspecting state. A later update stays visible across the await.
        let mut changes = peer.changed.subscribe();
        let mut attempt: Option<Arc<Dial>> = None;
        let mut dialed_after_replacement = false;
        loop {
            {
                let mut state = peer.state();
                if state.revoked {
                    return Err(SendError::Closed);
                }
                if !self.shared.authorized(peer.id) {
                    return Err(SendError::NotAllowed);
                }
                if let Some(active) = state.active.as_ref().filter(|active| active.is_open()) {
                    return Ok(Arc::clone(&active.connection));
                }
                if let Some(pending) = &state.dial {
                    attempt = Some(Arc::clone(pending));
                } else if state.incoming.is_none() {
                    let result = attempt.as_ref().and_then(|dial| *lock(&dial.result));
                    match result {
                        Some(DialResult::Failed(error)) => return Err(error),
                        Some(DialResult::Replaced) if dialed_after_replacement => {
                            return Err(SendError::Disconnected);
                        }
                        Some(DialResult::Replaced) => dialed_after_replacement = true,
                        _ => {}
                    }
                    let dial = Arc::new(Dial::default());
                    self.spawn_dial(peer, &dial)?;
                    state.dial = Some(Arc::clone(&dial));
                    attempt = Some(dial);
                }
            }
            if changes.changed().await.is_err() {
                return Err(SendError::Closed);
            }
        }
    }

    // The peer lock is held until the dial is recorded. Its worker sees the reservation before
    // changing any peer state. Workers do not take this worker-set lock.
    fn spawn_dial(&self, peer: &Arc<Peer>, dial: &Arc<Dial>) -> Result<(), SendError> {
        let workers = self.workers.upgrade().ok_or(SendError::Closed)?;
        let mut workers = lock(&workers);
        let workers = workers.as_mut().ok_or(SendError::Closed)?;
        while let Some(result) = workers.tasks.try_join_next() {
            workers.failed |= result.is_err();
        }
        workers.tasks.spawn(run_dial(
            workers.endpoint.clone(),
            Arc::clone(&self.shared),
            Arc::clone(peer),
            Arc::clone(dial),
        ));
        Ok(())
    }

    /// Invalidates pending operations and closes the current connection to a station.
    ///
    /// Update the endpoint's authorizer first when revoking a station's access.
    pub fn close_peer(&self, station: NodeId) {
        let mut registry = lock(&self.shared.registry);
        if let Some(peer) = registry
            .peers
            .remove(&station)
            .and_then(|peer| peer.upgrade())
        {
            peer.revoke(&self.shared.paths);
        }
    }

    /// The station's current selected path.
    pub fn path(&self, station: NodeId) -> LinkPath {
        lock(&self.shared.registry)
            .peers
            .get(&station)
            .and_then(Weak::upgrade)
            .map_or(LinkPath::Down, |peer| peer.state().path())
    }

    /// Observes path changes. If this receiver falls behind, refresh with [Self::path].
    pub fn subscribe_paths(&self) -> broadcast::Receiver<PathUpdate> {
        self.shared.paths.subscribe()
    }
}

struct Shared {
    local_id: NodeId,
    generation: [u8; 16],
    config: LinkConfig,
    authorizer: Arc<Authorizer>,
    incoming: Mutex<Option<mpsc::Sender<IncomingChat>>>,
    paths: broadcast::Sender<PathUpdate>,
    registry: Mutex<Registry>,
}

impl Shared {
    fn new(endpoint: &Endpoint, config: LinkConfig) -> (Arc<Self>, mpsc::Receiver<IncomingChat>) {
        let (incoming, receive) = mpsc::channel(MAILBOX_CAPACITY);
        let (paths, _) = broadcast::channel(MAILBOX_CAPACITY);
        let shared = Arc::new(Self {
            local_id: endpoint.iroh().id(),
            generation: fresh_nonce(),
            config,
            authorizer: endpoint.authorizer_handle(),
            incoming: Mutex::new(Some(incoming)),
            paths,
            registry: Mutex::default(),
        });
        (shared, receive)
    }

    fn peer(&self, id: NodeId) -> Result<Arc<Peer>, SendError> {
        let mut registry = lock(&self.registry);
        if registry.stopped {
            return Err(SendError::Closed);
        }
        if !self.authorized(id) || id == self.local_id {
            return Err(SendError::NotAllowed);
        }
        if let Some(peer) = registry.peers.get(&id).and_then(Weak::upgrade) {
            return Ok(peer);
        }
        registry.peers.retain(|_, peer| peer.strong_count() != 0);
        let peer = Arc::new(Peer {
            id,
            state: Mutex::default(),
            changed: watch::Sender::new(()),
        });
        registry.peers.insert(id, Arc::downgrade(&peer));
        Ok(peer)
    }

    fn authorized(&self, id: NodeId) -> bool {
        self.authorizer
            .authorize(&id, Protocol::Alpn(LINK_ALPN))
            .is_some()
    }

    fn stop(&self) {
        let mut registry = lock(&self.registry);
        registry.stopped = true;
        for peer in registry
            .peers
            .drain()
            .filter_map(|(_, peer)| peer.upgrade())
        {
            peer.revoke(&self.paths);
        }
        lock(&self.incoming).take();
    }

    fn hello(&self) -> Hello {
        Hello::local(self.generation, fresh_nonce())
    }

    fn admit(
        &self,
        peer: &Peer,
        state: &mut PeerState,
        connection: &Connection,
        observed_generation: Option<[u8; 16]>,
        remote_generation: [u8; 16],
    ) -> DialResult {
        if state.revoked {
            connection.close(CLOSE_LINK.into(), b"");
            return DialResult::Failed(SendError::Closed);
        }
        if !self.authorized(peer.id) {
            connection.close(CLOSE_NOT_ALLOWED.into(), b"");
            return DialResult::Failed(SendError::NotAllowed);
        }
        if connection.close_reason().is_some() {
            return dial_failure(connection, SendError::Disconnected);
        }
        let same_generation = state.remote_generation == Some(remote_generation);
        // The snapshot predates the fresh challenge. Reject this competing handshake if
        // another generation was admitted in the meantime.
        if !same_generation && state.remote_generation != observed_generation {
            connection.close(CLOSE_REPLACED.into(), b"");
            return DialResult::Replaced;
        }
        if let Some(active) = state.active.as_ref().filter(|active| active.is_open()) {
            let existing = initiator(self.local_id, peer.id, active.connection.transport.side());
            let candidate = initiator(self.local_id, peer.id, connection.side());
            if same_generation && existing < candidate {
                connection.close(CLOSE_REPLACED.into(), b"");
                return DialResult::Replaced;
            }
            active
                .connection
                .replaced_locally
                .store(true, Ordering::Relaxed);
            active
                .connection
                .transport
                .close(CLOSE_REPLACED.into(), b"");
        }
        let old_path = state.path();
        state.remote_generation = Some(remote_generation);
        let path = selected_path(connection);
        state.active = Some(Active {
            connection: Arc::new(EstablishedConnection {
                transport: connection.clone(),
                admitted_at: Instant::now(),
                replaced_locally: AtomicBool::new(false),
            }),
            path,
            stream_credit_wait: None,
        });
        publish_path(&self.paths, peer.id, old_path, path);
        DialResult::Ready
    }
}

#[derive(Default)]
struct Registry {
    stopped: bool,
    // A live peer is held by its operations and connection drivers. Weak entries leave no
    // connection or state behind after both sides of an operation have gone away.
    peers: HashMap<NodeId, Weak<Peer>>,
}

struct Peer {
    id: NodeId,
    state: Mutex<PeerState>,
    changed: watch::Sender<()>,
}

impl Peer {
    fn state(&self) -> MutexGuard<'_, PeerState> {
        lock(&self.state)
    }

    fn revoke(&self, paths: &broadcast::Sender<PathUpdate>) {
        let mut state = self.state();
        state.revoked = true;
        state.dial.take();
        if let Some(connection) = state.incoming.take() {
            connection.close(CLOSE_LINK.into(), b"");
        }
        if let Some(connection) = state.outgoing.take() {
            connection.close(CLOSE_LINK.into(), b"");
        }
        if let Some(active) = state.active.take() {
            active.connection.transport.close(CLOSE_LINK.into(), b"");
            publish_path(paths, self.id, active.path, LinkPath::Down);
        }
        self.changed.send_replace(());
    }

    async fn revoked(&self) {
        let mut changes = self.changed.subscribe();
        while !self.state().revoked {
            if changes.changed().await.is_err() {
                return;
            }
        }
    }

    fn stream_credit_wait(
        &self,
        connection: &Connection,
        window: Duration,
    ) -> Result<Option<Duration>, SendError> {
        let mut state = self.state();
        let Some(active) = state
            .active
            .as_mut()
            .filter(|active| active.matches(connection) && active.is_open())
        else {
            return Ok(None);
        };
        let now = Instant::now();
        let received = connection.stats().frame_rx;
        let wait = active.stream_credit_wait.get_or_insert(StreamCreditWait {
            since: now,
            received,
        });
        // Keep the observation on the connection so a later caller can finish checking
        // after an earlier send timed out or was cancelled. Authenticated traffic resets it.
        if wait.received != received {
            *wait = StreamCreditWait {
                since: now,
                received,
            };
        }
        let remaining = window.saturating_sub(now.duration_since(wait.since));
        if remaining.is_zero() {
            connection.close(CLOSE_LINK.into(), b"");
            return Err(SendError::Disconnected);
        }
        Ok(Some(remaining))
    }

    fn update_path(&self, paths: &broadcast::Sender<PathUpdate>, connection: &Connection) {
        let mut state = self.state();
        if let Some(active) = state
            .active
            .as_mut()
            .filter(|active| active.matches(connection))
        {
            let path = selected_path(connection);
            publish_path(paths, self.id, active.path, path);
            active.path = path;
        }
    }
}

#[derive(Default)]
struct PeerState {
    revoked: bool,
    incoming: Option<Connection>,
    outgoing: Option<Connection>,
    active: Option<Active>,
    // A connection can close while another generation's handshake is in flight.
    remote_generation: Option<[u8; 16]>,
    dial: Option<Arc<Dial>>,
}

impl PeerState {
    fn path(&self) -> LinkPath {
        self.active
            .as_ref()
            .map_or(LinkPath::Down, |active| active.path)
    }

    fn is_current(&self, connection: &Connection) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| active.matches(connection) && active.is_open())
    }
}

struct Active {
    connection: Arc<EstablishedConnection>,
    path: LinkPath,
    stream_credit_wait: Option<StreamCreditWait>,
}

struct StreamCreditWait {
    since: Instant,
    received: FrameStats,
}

impl Active {
    fn matches(&self, connection: &Connection) -> bool {
        self.connection.transport.stable_id() == connection.stable_id()
    }

    fn is_open(&self) -> bool {
        self.connection.transport.close_reason().is_none()
    }
}

struct EstablishedConnection {
    transport: Connection,
    admitted_at: Instant,
    // Read and written under the peer lock, including after removal from the active slot.
    replaced_locally: AtomicBool,
}

async fn wait_for_close(
    peer: &Peer,
    closed: impl Future<Output = ConnectionError>,
    budget: Duration,
) -> Result<(), SendError> {
    tokio::select! {
        _ = peer.revoked() => Err(SendError::Closed),
        _ = timeout(budget, closed) => Ok(()),
    }
}

async fn open_chat_stream(
    peer: &Peer,
    connection: &Connection,
    config: LinkConfig,
    start: Instant,
    check_progress: bool,
) -> Result<(SendStream, RecvStream), SendError> {
    let open = async {
        connection
            .open_bi()
            .await
            .map_err(|_| SendError::Disconnected)
    };
    if !check_progress {
        return open.await;
    }
    // A blocked noq open queues STREAMS_BLOCKED without waking the driver. A quiet
    // connection may need the next keepalive and time for the peer's ACK to arrive.
    let window = config
        .connect_timeout
        .max(QUIC_KEEPALIVE + STREAM_CREDIT_MARGIN);
    let check = async {
        loop {
            let remaining = config.send_deadline.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                break;
            }
            let Some(wait) = peer.stream_credit_wait(connection, window)? else {
                break;
            };
            if wait >= remaining {
                break;
            }
            sleep(wait).await;
        }
        pending().await
    };
    tokio::select! {
        biased;
        // Only check credit progress after a real pending open. Prefer newly
        // available credit if the open and the check wake up together.
        result = open => result,
        result = check => result,
    }
}

async fn send_chat(
    peer: &Peer,
    connection: &Connection,
    message: &ChatMessage,
    config: LinkConfig,
    start: Instant,
    check_progress: bool,
) -> Result<ChatReply, SendError> {
    let (send, mut recv) =
        open_chat_stream(peer, connection, config, start, check_progress).await?;
    // In noq, frame_rx counts decoded frames after packet authentication, including QUIC
    // ACKs and PINGs. Application handling can lag while the connection keeps making progress.
    let received = check_progress.then(|| connection.stats().frame_rx);
    write_chat_request(send, message).await?;
    let remaining = config.send_deadline.saturating_sub(start.elapsed());
    let reply = read_chat_reply(&mut recv, remaining);
    // Start the reply checkpoint after queuing the request so the peer has a request
    // to acknowledge for the full interval.
    let Some(received) = received
        .filter(|_| !config.connect_timeout.is_zero() && config.connect_timeout < remaining)
    else {
        return reply.await;
    };
    let mut reply = pin!(reply);
    if let Ok(result) = timeout(config.connect_timeout, reply.as_mut()).await {
        return result;
    }
    {
        let state = peer.state();
        if state.is_current(connection) && connection.stats().frame_rx == received {
            connection.close(CLOSE_LINK.into(), b"");
            return Err(SendError::Disconnected);
        }
    }
    reply.await
}

#[derive(Default)]
struct Dial {
    result: Mutex<Option<DialResult>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DialResult {
    Ready,
    Replaced,
    Failed(SendError),
}

struct Workers {
    endpoint: iroh::Endpoint,
    tasks: JoinSet<()>,
    failed: bool,
}

async fn run_dial(endpoint: iroh::Endpoint, shared: Arc<Shared>, peer: Arc<Peer>, dial: Arc<Dial>) {
    let budget = shared.config.connect_timeout;
    let candidate = async {
        let connection = endpoint
            .connect(peer.id, LINK_ALPN)
            .await
            .map_err(connection_failure)?;
        let observed_generation = {
            let mut state = peer.state();
            if state.revoked {
                connection.close(CLOSE_LINK.into(), b"");
                return Err(DialResult::Failed(SendError::Closed));
            }
            state.outgoing = Some(connection.clone());
            state.remote_generation
        };
        let remote_generation = match exchange_hello(&connection, shared.hello(), budget).await {
            Ok(generation) => generation,
            Err(error) => {
                let result = dial_failure(&connection, error);
                let code = if error == SendError::Timeout {
                    CLOSE_LINK
                } else {
                    CLOSE_BAD_HELLO
                };
                connection.close(code.into(), b"");
                return Err(result);
            }
        };
        Ok((connection, observed_generation, remote_generation))
    };
    let result = tokio::select! {
        _ = peer.revoked() => return,
        result = timeout(budget, candidate) => result.unwrap_or(Err(DialResult::Failed(SendError::Timeout))),
    };
    drop(endpoint);
    let connection = {
        let mut state = peer.state();
        // A revoked peer's slot cannot be reused, even after the station is added again.
        if state.revoked {
            return;
        }
        let pending = state.outgoing.take();
        state.dial.take();
        let (outcome, active) = match result {
            Ok((connection, observed_generation, remote_generation)) => {
                let outcome = shared.admit(
                    &peer,
                    &mut state,
                    &connection,
                    observed_generation,
                    remote_generation,
                );
                (
                    outcome,
                    (outcome == DialResult::Ready).then_some(connection),
                )
            }
            Err(outcome) => {
                if let Some(connection) = pending {
                    let code = if outcome == DialResult::Replaced {
                        CLOSE_REPLACED
                    } else {
                        CLOSE_LINK
                    };
                    connection.close(code.into(), b"");
                }
                (outcome, None)
            }
        };
        *lock(&dial.result) = Some(outcome);
        peer.changed.send_replace(());
        active
    };
    if let Some(connection) = connection {
        drive(shared, peer, connection).await;
    }
}

fn connection_failure(error: iroh::endpoint::ConnectError) -> DialResult {
    use iroh::endpoint::{ConnectError, ConnectingError, ConnectionError};

    match error {
        ConnectError::Connecting {
            source: ConnectingError::LocallyRejected { .. },
            ..
        } => DialResult::Failed(SendError::NotAllowed),
        ConnectError::Connection { source, .. }
        | ConnectError::Connecting {
            source: ConnectingError::ConnectionError { source, .. },
            ..
        } => match source {
            ConnectionError::ApplicationClosed(close) => {
                classify_close(close.error_code.into_inner())
            }
            ConnectionError::TimedOut => DialResult::Failed(SendError::Timeout),
            _ => DialResult::Failed(SendError::Disconnected),
        },
        _ => DialResult::Failed(SendError::Disconnected),
    }
}

fn classify_close(code: u64) -> DialResult {
    if code == u64::from(CLOSE_NOT_ALLOWED) {
        DialResult::Failed(SendError::NotAllowed)
    } else if code == u64::from(CLOSE_REPLACED) {
        DialResult::Replaced
    } else if code == u64::from(CLOSE_BAD_HELLO) {
        DialResult::Failed(SendError::BadHello)
    } else {
        DialResult::Failed(SendError::Disconnected)
    }
}

fn dial_failure(connection: &Connection, fallback: SendError) -> DialResult {
    connection
        .close_reason()
        .as_ref()
        .and_then(close_code)
        .map_or(DialResult::Failed(fallback), |code| {
            classify_close(u64::from(code))
        })
}

struct IncomingLinks(Arc<Shared>);

impl fmt::Debug for IncomingLinks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncomingLinks").finish_non_exhaustive()
    }
}

impl ProtocolHandler for IncomingLinks {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let shared = &self.0;
        let peer = match shared.peer(connection.remote_id()) {
            Ok(peer) => peer,
            Err(error) => {
                let code = if error == SendError::NotAllowed {
                    CLOSE_NOT_ALLOWED
                } else {
                    CLOSE_LINK
                };
                connection.close(code.into(), b"");
                return Ok(());
            }
        };
        let observed_generation = {
            let mut state = peer.state();
            if state.revoked {
                connection.close(CLOSE_LINK.into(), b"");
                return Ok(());
            }
            if state.incoming.is_some() {
                // A pending hello may fail, so no winning connection can be promised.
                connection.close(CLOSE_LINK.into(), b"");
                return Ok(());
            }
            state.incoming = Some(connection.clone());
            state.remote_generation
        };
        let budget = shared.config.connect_timeout;
        let hello = timeout(budget, exchange_hello(&connection, shared.hello(), budget)).await;
        let admitted = {
            let mut state = peer.state();
            state.incoming.take();
            let result = match hello {
                Ok(Ok(remote_generation)) => shared.admit(
                    &peer,
                    &mut state,
                    &connection,
                    observed_generation,
                    remote_generation,
                ),
                _ => {
                    connection.close(CLOSE_BAD_HELLO.into(), b"");
                    DialResult::Failed(SendError::BadHello)
                }
            };
            peer.changed.send_replace(());
            result == DialResult::Ready
        };
        if admitted {
            drive(Arc::clone(shared), peer, connection).await;
        }
        Ok(())
    }

    async fn shutdown(&self) {
        self.0.stop();
    }
}

async fn drive(shared: Arc<Shared>, peer: Arc<Peer>, connection: Connection) {
    let mut path_events = connection.path_events();
    let mut streams = FuturesUnordered::new();
    // The installed connection's path can change between admission and subscription.
    peer.update_path(&shared.paths, &connection);
    loop {
        tokio::select! {
            biased;
            _ = connection.closed() => break,
            event = path_events.next() => {
                if event.is_none() {
                    break;
                }
                peer.update_path(&shared.paths, &connection);
            },
            _ = streams.next(), if !streams.is_empty() => {}
            incoming = connection.accept_bi(), if streams.len() < STREAM_LIMIT => {
                let Ok((send, recv)) = incoming else { break };
                streams.push(receive_chat(&shared, &peer, &connection, send, recv));
            },
        }
    }
    // Stream futures belong to the driver and are cancelled with it.
    drop(streams);
    let mut state = peer.state();
    if state
        .active
        .as_ref()
        .is_some_and(|active| active.matches(&connection))
    {
        let path = state.path();
        state.active.take();
        publish_path(&shared.paths, peer.id, path, LinkPath::Down);
        peer.changed.send_replace(());
    }
}

fn initiator(local: NodeId, remote: NodeId, side: Side) -> NodeId {
    match side {
        Side::Client => local,
        Side::Server => remote,
    }
}

fn fresh_nonce() -> [u8; 16] {
    // The node-key generator supplies OS randomness. These bytes are never used as a key.
    loop {
        let random = generate_node_key();
        let mut nonce = [0; 16];
        nonce.copy_from_slice(&random[..16]);
        if nonce != [0; 16] {
            return nonce;
        }
    }
}

fn selected_path(connection: &Connection) -> LinkPath {
    connection
        .paths()
        .iter()
        .find(|path| path.is_selected())
        .map_or(LinkPath::Down, |path| {
            if path.is_relay() {
                LinkPath::Relayed
            } else {
                LinkPath::Direct
            }
        })
}

fn publish_path(
    paths: &broadcast::Sender<PathUpdate>,
    station_id: NodeId,
    before: LinkPath,
    path: LinkPath,
) {
    if before != path {
        let _ = paths.send(PathUpdate { station_id, path });
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EndpointConfig, IpTransport, generate_node_key};

    async fn loopback_endpoint() -> Endpoint {
        let endpoint = Endpoint::bind(
            &generate_node_key(),
            EndpointConfig {
                ip_transport: IpTransport::LoopbackOnly,
                ..EndpointConfig::default()
            },
            Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL])),
        )
        .await
        .unwrap();
        endpoint.iroh().set_alpns(vec![LINK_ALPN.to_vec()]);
        endpoint
    }

    async fn connect(dialer: &Endpoint, listener: &Endpoint) -> (Connection, Connection) {
        let socket = listener.iroh().bound_sockets()[0];
        let address = iroh::EndpointAddr::new(listener.iroh().id()).with_ip_addr(socket);
        let (client, server) = tokio::join!(dialer.iroh().connect(address, LINK_ALPN), async {
            listener.iroh().accept().await.expect("incoming link").await
        });
        (
            client.unwrap_or_else(|_| panic!("dialed loopback connection failed")),
            server.unwrap_or_else(|_| panic!("accepted loopback connection failed")),
        )
    }

    #[tokio::test]
    async fn lower_id_dial_wins_in_either_order_and_loser_cleanup_keeps_it() {
        let budget = Duration::from_secs(10);
        timeout(budget, async {
            let first = loopback_endpoint().await;
            let second = loopback_endpoint().await;
            let (low, high) = if first.iroh().id() < second.iroh().id() {
                (&first, &second)
            } else {
                (&second, &first)
            };
            low.authorizer()
                .replace_peers([(high.iroh().id(), "higher".into())]);
            high.authorizer()
                .replace_peers([(low.iroh().id(), "lower".into())]);
            let config = LinkConfig {
                connect_timeout: budget,
                send_deadline: budget,
            };

            for (local, remote) in [(low, high), (high, low)] {
                for winner_first in [true, false] {
                    let (low_client, high_server) = connect(low, high).await;
                    let (high_client, low_server) = connect(high, low).await;
                    let (winner, loser) = if local.iroh().id() == low.iroh().id() {
                        (&low_client, &low_server)
                    } else {
                        (&high_server, &high_client)
                    };
                    let (early, late) = if winner_first {
                        (winner, loser)
                    } else {
                        (loser, winner)
                    };
                    let (shared, mut inbox) = Shared::new(local, config);
                    let peer = shared.peer(remote.iroh().id()).unwrap();
                    {
                        let mut state = peer.state();
                        assert!(
                            shared.admit(&peer, &mut state, early, None, [1; 16])
                                == DialResult::Ready
                        );
                        let expected = if winner_first {
                            DialResult::Replaced
                        } else {
                            DialResult::Ready
                        };
                        assert!(shared.admit(&peer, &mut state, late, None, [1; 16]) == expected);
                        assert!(state.is_current(winner));
                        assert!(loser.close_reason().is_some());
                    }
                    drive(Arc::clone(&shared), Arc::clone(&peer), loser.clone()).await;
                    assert!(peer.state().is_current(winner));
                    shared.stop();
                    assert!(matches!(
                        inbox.try_recv(),
                        Err(mpsc::error::TryRecvError::Disconnected)
                    ));
                }
            }
            tokio::join!(first.iroh().close(), second.iroh().close());
        })
        .await
        .expect("resolve duplicate links before the test deadline");
    }

    #[tokio::test]
    async fn delayed_hello_cannot_restore_the_previous_generation() {
        let budget = Duration::from_secs(10);
        timeout(budget, async {
            let local = loopback_endpoint().await;
            let remote = loopback_endpoint().await;
            local
                .authorizer()
                .replace_peers([(remote.iroh().id(), "peer".into())]);
            remote
                .authorizer()
                .replace_peers([(local.iroh().id(), "peer".into())]);
            let (shared, _inbox) = Shared::new(
                &local,
                LinkConfig {
                    connect_timeout: budget,
                    send_deadline: budget,
                },
            );
            let peer = shared.peer(remote.iroh().id()).unwrap();
            let (original, _original_remote) = connect(&local, &remote).await;
            let (_delayed_remote, delayed) = connect(&remote, &local).await;
            let (new, _new_remote) = connect(&local, &remote).await;
            {
                let mut state = peer.state();
                let old = [1; 16];
                let restarted = [2; 16];
                assert!(shared.admit(&peer, &mut state, &original, None, old) == DialResult::Ready);
                assert!(
                    shared.admit(&peer, &mut state, &new, Some(old), restarted)
                        == DialResult::Ready
                );
                assert!(state.is_current(&new));
            }

            new.close(CLOSE_LINK.into(), b"");
            drive(Arc::clone(&shared), Arc::clone(&peer), new).await;
            {
                let mut state = peer.state();
                assert!(state.active.is_none());
                assert!(
                    shared.admit(&peer, &mut state, &delayed, Some([1; 16]), [1; 16])
                        == DialResult::Replaced
                );
                assert!(state.active.is_none());
            }
            shared.stop();
            tokio::join!(local.iroh().close(), remote.iroh().close());
        })
        .await
        .expect("reject a delayed generation before the test deadline");
    }

    #[tokio::test]
    async fn full_chat_mailbox_replies_unavailable() {
        let budget = Duration::from_secs(10);
        timeout(budget, async {
            let local = loopback_endpoint().await;
            let remote = loopback_endpoint().await;
            let remote_id = remote.iroh().id();
            local
                .authorizer()
                .replace_peers([(remote_id, "peer".into())]);
            remote
                .authorizer()
                .replace_peers([(local.iroh().id(), "peer".into())]);
            let (shared, inbox) = Shared::new(
                &local,
                LinkConfig {
                    connect_timeout: budget,
                    send_deadline: budget,
                },
            );
            let peer = shared.peer(remote_id).unwrap();
            let (client, server) = connect(&remote, &local).await;
            assert!(
                shared.admit(&peer, &mut peer.state(), &server, None, [1; 16]) == DialResult::Ready
            );

            let chat = ChatMessage {
                id: "probe".into(),
                sent_at_ms: 0,
                body: "chat".into(),
            };
            {
                let mailbox = lock(&shared.incoming);
                let incoming = mailbox.as_ref().expect("chat mailbox is open");
                for _ in 0..inbox.max_capacity() {
                    let (reply, _) = oneshot::channel();
                    assert!(
                        incoming
                            .try_send(IncomingChat {
                                station_id: remote_id,
                                peer: "peer".into(),
                                message: chat.clone(),
                                reply,
                            })
                            .is_ok()
                    );
                }
            }

            let (reply, ()) = tokio::join!(
                async {
                    let (send, mut recv) = client.open_bi().await.unwrap();
                    write_chat_request(send, &chat).await.unwrap();
                    read_chat_reply(&mut recv, budget).await
                },
                async {
                    let (send, recv) = server.accept_bi().await.unwrap();
                    receive_chat(&shared, &peer, &server, send, recv).await;
                }
            );
            assert_eq!(reply, Ok(ChatReply::Nack(NackReason::Unavailable)));
            assert_eq!(inbox.len(), inbox.max_capacity());
            shared.stop();
            tokio::join!(local.iroh().close(), remote.iroh().close());
        })
        .await
        .expect("reply before the test deadline with a full mailbox");
    }
}
