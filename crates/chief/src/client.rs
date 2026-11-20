//! The seam between a frontend and a station: [`Transport`], [`Subscription`] and [`Client`].
//!
//! A [`Transport`] carries calls and subscriptions: the station core in the same process, or the
//! local socket. [`Client`] gives any transport typed methods. A subscription is a pair made by
//! [`subscription`]: the transport keeps the [`EventSink`] and pushes events into it; the frontend
//! pulls them from the [`Subscription`]. The rules both sides keep are in [`crate::api`].
//!
//! [`SocketTransport`] (Unix) is the transport over the station's local socket; it lives in
//! `crate::socket` with the socket's envelope. The attach order for a live view of the log is
//! [`Client::follow_log`] in [`crate::follow`].

#[cfg(unix)]
pub use crate::socket::{DEFAULT_CALL_TIMEOUT, SocketTransport};

use crate::api::{
    ApiError, Event, LogPage, Message, MessageStatus, Peer, Request, Response, Status,
};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Semaphore, mpsc};

/// The queue length a transport gives each subscription: how many events a subscriber may fall
/// behind before it gets [`Event::Resync`].
pub const SUBSCRIPTION_CAPACITY: usize = 256;

/// Carries station API calls and subscriptions. Used through [`Client`], with static dispatch
/// only.
///
/// An implementor writes `async fn call(&self, request: Request) -> Result<Response, ApiError>`
/// and `async fn subscribe(&self) -> Result<Subscription, ApiError>` in its impl; the compiler
/// checks that both futures are `Send`. `'static` lets a server keep a transport in an `Arc`.
pub trait Transport: Send + Sync + 'static {
    /// Makes one call. Answers with the [`Response`] variant the request names, or with an
    /// [`ApiError`]: the station's refusal, passed through unchanged, or the transport's own
    /// failure ([`ApiError::NotRunning`], [`ApiError::IncompatibleApi`],
    /// [`ApiError::Disconnected`], [`ApiError::Timeout`], [`ApiError::BadReply`]).
    fn call(&self, request: Request) -> impl Future<Output = Result<Response, ApiError>> + Send;

    /// Opens a subscription to live events from now on; earlier events are not replayed.
    ///
    /// Resolves only once the station has registered the subscription, so that every change the
    /// station applies afterwards is delivered or covered by a later [`Event::Resync`] (rule 3 in
    /// [`crate::api`]). An implementation makes the pair with
    /// [`subscription`]`(`[`SUBSCRIPTION_CAPACITY`]`)`, keeps the [`EventSink`], pushes every event
    /// in order and unchanged, and drops the sink to end the subscription (rule 11).
    fn subscribe(&self) -> impl Future<Output = Result<Subscription, ApiError>> + Send;
}

impl<T: Transport> Transport for Arc<T> {
    fn call(&self, request: Request) -> impl Future<Output = Result<Response, ApiError>> + Send {
        T::call(self, request)
    }

    fn subscribe(&self) -> impl Future<Output = Result<Subscription, ApiError>> + Send {
        T::subscribe(self)
    }
}

/// Makes a connected pair: an [`EventSink`] for the transport and a [`Subscription`] for the
/// frontend. The queue holds at most `capacity` events. 0 counts as 1, and a capacity above
/// tokio's [`Semaphore::MAX_PERMITS`] counts as that limit.
pub fn subscription(capacity: usize) -> (EventSink, Subscription) {
    let (tx, rx) = mpsc::channel(capacity.clamp(1, Semaphore::MAX_PERMITS));
    let lagged = Arc::new(AtomicBool::new(false));
    (
        EventSink {
            tx,
            lagged: Arc::clone(&lagged),
        },
        Subscription { rx, lagged },
    )
}

/// The transport's end of one subscription. Dropping it ends the subscription. Not `Clone`: a
/// subscription has exactly one producer.
#[derive(Debug)]
pub struct EventSink {
    tx: mpsc::Sender<Event>,
    lagged: Arc<AtomicBool>,
}

/// The error of [`EventSink::push`]: the [`Subscription`] was dropped. The transport drops the
/// sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionClosed;

impl fmt::Display for SubscriptionClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the subscription was dropped")
    }
}

impl std::error::Error for SubscriptionClosed {}

impl EventSink {
    /// Queues `event` without waiting, so a slow subscriber never stalls the producer.
    ///
    /// When the queue is full the event is dropped, and the subscriber's next read returns
    /// [`Event::Resync`] instead of what is queued. Returns `Ok` in both cases. Fails only when the
    /// [`Subscription`] was dropped.
    pub fn push(&self, event: Event) -> Result<(), SubscriptionClosed> {
        match self.tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.lagged.store(true, Ordering::Release);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(SubscriptionClosed),
        }
    }

    /// Whether the [`Subscription`] was dropped.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Resolves once the [`Subscription`] is dropped. Cancel-safe: a server can `select!` on it.
    pub async fn closed(&self) {
        self.tx.closed().await;
    }
}

/// The frontend's end of one subscription: live events from a station, pulled one at a time.
#[derive(Debug)]
pub struct Subscription {
    rx: mpsc::Receiver<Event>,
    lagged: Arc<AtomicBool>,
}

impl Subscription {
    /// The next event, or `None` once the subscription has ended.
    ///
    /// - Events arrive in the order they were pushed.
    /// - If the queue overflowed, the next call returns [`Event::Resync`] and discards the events
    ///   still queued; events pushed later arrive normally (rule 5 in [`crate::api`]).
    /// - When the [`EventSink`] is dropped, the queued events still arrive, then `None`, and `None`
    ///   again on every later call (rule 11).
    /// - Cancel-safe: dropping the future before it resolves loses no event, so it can be used in
    ///   `select!`.
    pub async fn next(&mut self) -> Option<Event> {
        // Only `recv` awaits, and tokio documents it as cancel-safe. The lag check and the drain run
        // within one poll.
        if self.lagged.swap(false, Ordering::AcqRel) {
            while self.rx.try_recv().is_ok() {}
            return Some(Event::Resync);
        }
        self.rx.recv().await
    }
}

/// The station API with typed methods, over any [`Transport`].
#[derive(Debug, Clone)]
pub struct Client<T> {
    transport: T,
}

fn unexpected(response: Response) -> ApiError {
    ApiError::BadReply {
        detail: format!("unexpected response {response:?}"),
    }
}

impl<T: Transport> Client<T> {
    /// A client over `transport`.
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    /// The transport this client uses.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// [`Request::Status`]: this station's id, instance and towers.
    pub async fn status(&self) -> Result<Status, ApiError> {
        match self.transport.call(Request::Status).await? {
            Response::Status { status } => Ok(status),
            other => Err(unexpected(other)),
        }
    }

    /// [`Request::Peers`]: the peer book.
    pub async fn peers(&self) -> Result<Vec<Peer>, ApiError> {
        match self.transport.call(Request::Peers).await? {
            Response::Peers { peers } => Ok(peers),
            other => Err(unexpected(other)),
        }
    }

    /// [`Request::PeerAdd`]: adds `station_id` to the peer `name`, and returns the peer.
    pub async fn peer_add(&self, name: &str, station_id: &str) -> Result<Peer, ApiError> {
        let request = Request::PeerAdd {
            name: name.to_owned(),
            station_id: station_id.to_owned(),
        };
        match self.transport.call(request).await? {
            Response::Peer { peer } => Ok(peer),
            other => Err(unexpected(other)),
        }
    }

    /// [`Request::PeerRemove`]: removes the peer `name`.
    pub async fn peer_remove(&self, name: &str) -> Result<(), ApiError> {
        let request = Request::PeerRemove {
            name: name.to_owned(),
        };
        match self.transport.call(request).await? {
            Response::Done => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// [`Request::Send`]: sends `body` to `peer` and returns the message as logged, normally
    /// [`MessageStatus::Sending`]. Does not wait for the outcome; see [`Client::send_and_wait`].
    pub async fn send(&self, peer: &str, body: &str) -> Result<Message, ApiError> {
        let request = Request::Send {
            peer: peer.to_owned(),
            body: body.to_owned(),
        };
        match self.transport.call(request).await? {
            Response::Message { message } => Ok(message),
            other => Err(unexpected(other)),
        }
    }

    /// [`Request::Message`]: the message with this `seq`.
    pub async fn message(&self, seq: u64) -> Result<Message, ApiError> {
        match self.transport.call(Request::Message { seq }).await? {
            Response::Message { message } => Ok(message),
            other => Err(unexpected(other)),
        }
    }

    /// [`Request::Log`]: one page of the log (rule 6 in [`crate::api`]).
    pub async fn log(
        &self,
        peer: Option<&str>,
        before: Option<u64>,
        limit: u32,
    ) -> Result<LogPage, ApiError> {
        let request = Request::Log {
            peer: peer.map(str::to_owned),
            before,
            limit,
        };
        match self.transport.call(request).await? {
            Response::Log { page } => Ok(page),
            other => Err(unexpected(other)),
        }
    }

    /// Opens a subscription to live events ([`Transport::subscribe`]).
    pub async fn subscribe(&self) -> Result<Subscription, ApiError> {
        self.transport.subscribe().await
    }

    /// Sends `body` to `peer` and waits for the outcome: returns the message once its status is no
    /// longer [`MessageStatus::Sending`].
    ///
    /// It subscribes before it sends, so the outcome cannot slip past, even when it arrives before
    /// the answer to the send. On [`Event::Resync`] it re-reads the message with
    /// [`Client::message`]. If the re-read message has another `id` than the one sent, the station
    /// restarted and the `seq` names another message: that gives [`ApiError::Disconnected`], as does
    /// a subscription that ends first. Other errors of the calls are passed through.
    ///
    /// The station's send deadline bounds the wait; a caller that wants a shorter bound wraps the
    /// call in a timeout.
    pub async fn send_and_wait(&self, peer: &str, body: &str) -> Result<Message, ApiError> {
        let mut events = self.subscribe().await?;
        let sent = self.send(peer, body).await?;
        if sent.status != MessageStatus::Sending {
            return Ok(sent);
        }
        loop {
            let current = match events.next().await {
                Some(Event::Message { message }) if message.seq == sent.seq => message,
                Some(Event::Resync) => self.message(sent.seq).await?,
                Some(_) => continue,
                None => {
                    return Err(ApiError::Disconnected {
                        detail: "the subscription ended before the outcome of the send".to_owned(),
                    });
                }
            };
            if current.id != sent.id {
                return Err(ApiError::Disconnected {
                    detail: format!(
                        "the station restarted: message {} is another message now",
                        sent.seq
                    ),
                });
            }
            if current.status != MessageStatus::Sending {
                return Ok(current);
            }
        }
    }
}
