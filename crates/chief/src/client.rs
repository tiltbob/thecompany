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
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::Notify;

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
/// frontend.
///
/// The queue holds at most `capacity` events; 0 counts as 1. There is no upper bound: the queue
/// allocates as events arrive, not up front.
pub fn subscription(capacity: usize) -> (EventSink, Subscription) {
    let shared = Arc::new(Shared {
        queue: Mutex::new(Queue {
            events: VecDeque::new(),
            capacity: capacity.max(1),
            lagged: false,
            sink_gone: false,
            subscription_gone: false,
        }),
        to_subscriber: Notify::new(),
        to_sink: Notify::new(),
    });
    (
        EventSink {
            shared: Arc::clone(&shared),
        },
        Subscription { shared },
    )
}

/// The state both ends of a subscription share. Every change happens under the one lock, which is
/// never held across an await or across code outside this module, and is followed by a wake-up of
/// the other end.
#[derive(Debug)]
struct Shared {
    queue: Mutex<Queue>,
    /// Woken after every change the subscriber waits for: a push, an overflow, the sink dropped.
    to_subscriber: Notify,
    /// Woken when the subscription is dropped.
    to_sink: Notify,
}

#[derive(Debug)]
struct Queue {
    events: VecDeque<Event>,
    capacity: usize,
    /// The queue overflowed and was cleared; the subscriber's next read is `Event::Resync`.
    lagged: bool,
    sink_gone: bool,
    subscription_gone: bool,
}

impl Shared {
    /// Locks the state. A poisoned lock is used as it is: no code that holds the lock can leave the
    /// state half changed.
    fn lock(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The transport's end of one subscription. Dropping it ends the subscription. Not `Clone`: a
/// subscription has exactly one producer.
#[derive(Debug)]
pub struct EventSink {
    shared: Arc<Shared>,
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
    /// If the queue already holds `capacity` events, it overflows: the queued events and this one
    /// are dropped, and the subscriber's next read returns [`Event::Resync`]. Until it has read
    /// that `Resync`, every further event is dropped too, since the `Resync` covers it. Returns `Ok`
    /// in all these cases. Fails only when the [`Subscription`] was dropped.
    pub fn push(&self, event: Event) -> Result<(), SubscriptionClosed> {
        {
            let mut queue = self.shared.lock();
            if queue.subscription_gone {
                return Err(SubscriptionClosed);
            }
            if queue.lagged {
                return Ok(());
            }
            if queue.events.len() >= queue.capacity {
                queue.events.clear();
                queue.lagged = true;
            } else {
                queue.events.push_back(event);
            }
        }
        self.shared.to_subscriber.notify_one();
        Ok(())
    }

    /// Whether the [`Subscription`] was dropped.
    pub fn is_closed(&self) -> bool {
        self.shared.lock().subscription_gone
    }

    /// Resolves once the [`Subscription`] is dropped, at once if it already was. Cancel-safe: a
    /// server can `select!` on it.
    pub async fn closed(&self) {
        loop {
            // Register for the wake-up before looking, so a drop between the look and the await
            // still wakes this call.
            let mut notified = pin!(self.shared.to_sink.notified());
            notified.as_mut().enable();
            if self.shared.lock().subscription_gone {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for EventSink {
    fn drop(&mut self) {
        self.shared.lock().sink_gone = true;
        self.shared.to_subscriber.notify_one();
    }
}

/// The frontend's end of one subscription: live events from a station, pulled one at a time.
#[derive(Debug)]
pub struct Subscription {
    shared: Arc<Shared>,
}

impl Subscription {
    /// The next event, or `None` once the subscription has ended.
    ///
    /// - Events arrive in the order they were pushed.
    /// - After the queue overflowed, the next call returns [`Event::Resync`]: the events that were
    ///   queued and the ones pushed since are gone, and events pushed after that call arrive
    ///   normally (rule 5 in [`crate::api`]).
    /// - When the [`EventSink`] is dropped, what is still to read arrives first (the queued events,
    ///   or the `Resync` of an overflow), then `None`, and `None` again on every later call
    ///   (rule 11).
    /// - Cancel-safe: dropping the future before it resolves loses no event, so it can be used in
    ///   `select!`.
    pub async fn next(&mut self) -> Option<Event> {
        // Cancel safety: the only await is the wake-up, and the state changes only inside the poll
        // that returns the event, so a dropped future has taken nothing.
        //
        // No lost wake-up: the call registers for the wake-up before it looks at the state. A push
        // or a drop of the sink that changes the state after the look notifies after the change,
        // and that notification reaches the registered waiter (or leaves a permit for the next one).
        loop {
            let mut notified = pin!(self.shared.to_subscriber.notified());
            notified.as_mut().enable();
            {
                let mut queue = self.shared.lock();
                if queue.lagged {
                    queue.lagged = false;
                    return Some(Event::Resync);
                }
                if let Some(event) = queue.events.pop_front() {
                    return Some(event);
                }
                if queue.sink_gone {
                    return None;
                }
            }
            notified.await;
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        {
            let mut queue = self.shared.lock();
            queue.subscription_gone = true;
            queue.events.clear();
        }
        self.shared.to_sink.notify_waiters();
    }
}

/// The station API with typed methods, over any [`Transport`].
#[derive(Debug, Clone)]
pub struct Client<T> {
    transport: T,
}

/// The error for an answer of the wrong kind. It names only the two variants: the response's
/// contents (station ids, URLs, message text) stay out of printable error text (AGENTS.md, hard
/// rule 9).
fn unexpected(expected: &str, response: &Response) -> ApiError {
    let got = match response {
        Response::Status { .. } => "status",
        Response::Peers { .. } => "peers",
        Response::Peer { .. } => "peer",
        Response::Message { .. } => "message",
        Response::Log { .. } => "log",
        Response::Done => "done",
    };
    ApiError::BadReply {
        detail: format!("expected a {expected} response, got a {got} response"),
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
            other => Err(unexpected("status", &other)),
        }
    }

    /// [`Request::Peers`]: the peer book.
    pub async fn peers(&self) -> Result<Vec<Peer>, ApiError> {
        match self.transport.call(Request::Peers).await? {
            Response::Peers { peers } => Ok(peers),
            other => Err(unexpected("peers", &other)),
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
            other => Err(unexpected("peer", &other)),
        }
    }

    /// [`Request::PeerRemove`]: removes the peer `name`.
    pub async fn peer_remove(&self, name: &str) -> Result<(), ApiError> {
        let request = Request::PeerRemove {
            name: name.to_owned(),
        };
        match self.transport.call(request).await? {
            Response::Done => Ok(()),
            other => Err(unexpected("done", &other)),
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
            other => Err(unexpected("message", &other)),
        }
    }

    /// [`Request::Message`]: the message with this `seq`.
    pub async fn message(&self, seq: u64) -> Result<Message, ApiError> {
        match self.transport.call(Request::Message { seq }).await? {
            Response::Message { message } => Ok(message),
            other => Err(unexpected("message", &other)),
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
            other => Err(unexpected("log", &other)),
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
