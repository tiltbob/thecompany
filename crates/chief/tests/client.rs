#![cfg(test)]
//! `chief::client`: the subscription pair, `Client` over a scripted fake transport, and
//! `send_and_wait`. No test sleeps or touches the network or the file system.

use chief::api::*;
use chief::client::*;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;
use tokio::sync::Notify;

// Samples.

fn removed(name: &str) -> Event {
    Event::PeerRemoved { name: name.into() }
}

fn outgoing(seq: u64, rev: u32, status: MessageStatus) -> Message {
    Message {
        seq,
        rev,
        id: format!("MSG-{seq}"),
        peer: "bob".into(),
        direction: Direction::Outgoing,
        body: "on my way".into(),
        at_ms: 1_767_225_600_000,
        status,
    }
}

fn sending() -> Message {
    outgoing(42, 0, MessageStatus::Sending)
}

fn delivered() -> Message {
    outgoing(42, 1, MessageStatus::Delivered)
}

fn event(message: Message) -> Event {
    Event::Message { message }
}

fn send_request() -> Request {
    Request::Send {
        peer: "bob".into(),
        body: "on my way".into(),
    }
}

fn a_status() -> Status {
    Status {
        station_id: "STATION-A".into(),
        instance_id: "INSTANCE-1".into(),
        towers: vec![TowerStatus {
            url: "http://127.0.0.1:3340/".into(),
            state: TowerState::Connected,
            source: None,
        }],
        bootstrap_domain: None,
    }
}

fn a_peer() -> Peer {
    Peer {
        name: "bob".into(),
        stations: vec![PeerStation {
            station_id: "STATION-B".into(),
            link: Link::Relayed,
        }],
    }
}

/// Polls `future` exactly once and says whether it is still pending.
async fn pending_after_one_poll<F: Future>(future: F) -> bool {
    let mut future = pin!(future);
    std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
}

// A scripted fake transport.

/// One scripted call: the request the fake expects, the events it pushes before it answers (as the
/// station does, rule 4), and its answer.
struct Step {
    expect: Request,
    before: Vec<Event>,
    reply: Result<Response, ApiError>,
}

fn step(expect: Request, before: Vec<Event>, reply: Result<Response, ApiError>) -> Step {
    Step {
        expect,
        before,
        reply,
    }
}

#[derive(Default)]
struct State {
    script: VecDeque<Step>,
    sinks: Vec<EventSink>,
    subscribe_error: Option<ApiError>,
}

/// Answers calls from a script and pushes events into every open subscription. It implements
/// `Transport` with `async fn`, as an implementor may.
#[derive(Default)]
struct Fake {
    state: Mutex<State>,
    /// Notified after every answer, so a test can push events after it.
    answered: Notify,
}

impl Fake {
    fn new(script: Vec<Step>) -> Self {
        let fake = Self::default();
        fake.state.lock().unwrap().script = script.into();
        fake
    }

    fn push(&self, event: Event) {
        for sink in &self.state.lock().unwrap().sinks {
            let _ = sink.push(event.clone());
        }
    }

    fn end_subscriptions(&self) {
        self.state.lock().unwrap().sinks.clear();
    }

    fn assert_script_done(&self) {
        let state = self.state.lock().unwrap();
        let left: Vec<&Request> = state.script.iter().map(|s| &s.expect).collect();
        assert!(left.is_empty(), "calls that were not made: {left:?}");
    }
}

impl Transport for Fake {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        let next = self.state.lock().unwrap().script.pop_front();
        let Some(step) = next else {
            panic!("unscripted call {request:?}");
        };
        assert_eq!(request, step.expect);
        for event in step.before {
            self.push(event);
        }
        self.answered.notify_one();
        step.reply
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        let mut state = self.state.lock().unwrap();
        if let Some(error) = state.subscribe_error.clone() {
            return Err(error);
        }
        let (sink, subscription) = subscription(SUBSCRIPTION_CAPACITY);
        state.sinks.push(sink);
        Ok(subscription)
    }
}

/// A client over `Arc<Fake>`, and the fake for the test to drive.
fn client(script: Vec<Step>) -> (Client<Arc<Fake>>, Arc<Fake>) {
    let fake = Arc::new(Fake::new(script));
    (Client::new(Arc::clone(&fake)), fake)
}

// 5. The subscription pair.

#[tokio::test]
async fn events_arrive_in_the_order_they_were_pushed() {
    let (sink, mut subscription) = subscription(8);
    for event in [removed("a"), Event::Resync, removed("b"), removed("c")] {
        sink.push(event).unwrap();
    }
    assert_eq!(subscription.next().await, Some(removed("a")));
    assert_eq!(subscription.next().await, Some(Event::Resync));
    assert_eq!(subscription.next().await, Some(removed("b")));
    assert_eq!(subscription.next().await, Some(removed("c")));
}

#[tokio::test]
async fn a_full_queue_gives_resync_next_and_drops_what_was_queued() {
    let (sink, mut subscription) = subscription(2);
    for n in 1..=5 {
        assert_eq!(sink.push(removed(&format!("p{n}"))), Ok(()));
    }
    assert_eq!(subscription.next().await, Some(Event::Resync));
    // Nothing queued before the Resync is left.
    assert!(pending_after_one_poll(subscription.next()).await);

    // Events pushed after the Resync was read arrive normally.
    sink.push(removed("p6")).unwrap();
    sink.push(removed("p7")).unwrap();
    assert_eq!(subscription.next().await, Some(removed("p6")));
    assert_eq!(subscription.next().await, Some(removed("p7")));
    drop(sink);
    assert_eq!(subscription.next().await, None);
}

#[tokio::test]
async fn a_lag_before_the_end_still_gives_resync_then_none() {
    let (sink, mut subscription) = subscription(1);
    sink.push(removed("a")).unwrap();
    sink.push(removed("b")).unwrap();
    drop(sink);
    assert_eq!(subscription.next().await, Some(Event::Resync));
    assert_eq!(subscription.next().await, None);
}

#[tokio::test]
async fn dropping_the_sink_ends_the_subscription_after_the_queued_events() {
    let (sink, mut subscription) = subscription(8);
    sink.push(removed("a")).unwrap();
    sink.push(removed("b")).unwrap();
    drop(sink);
    assert_eq!(subscription.next().await, Some(removed("a")));
    assert_eq!(subscription.next().await, Some(removed("b")));
    assert_eq!(subscription.next().await, None);
    assert_eq!(subscription.next().await, None);
}

#[tokio::test]
async fn push_after_the_subscription_is_dropped_fails() {
    let (sink, subscription) = subscription(1);
    assert!(!sink.is_closed());
    // `closed()` waits while the subscription lives.
    assert!(pending_after_one_poll(sink.closed()).await);
    drop(subscription);
    assert!(sink.is_closed());
    assert_eq!(sink.push(Event::Resync), Err(SubscriptionClosed));
    sink.closed().await;
}

#[tokio::test]
async fn a_dropped_pending_next_loses_nothing() {
    let (sink, mut subscription) = subscription(4);
    {
        let mut next = pin!(subscription.next());
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(next.as_mut().poll(cx).is_pending())).await;
        assert!(pending);
        sink.push(removed("a")).unwrap();
        // The future is dropped here without being polled again.
    }
    assert_eq!(subscription.next().await, Some(removed("a")));
}

#[tokio::test]
async fn capacity_zero_counts_as_one() {
    let (sink, mut subscription) = subscription(0);
    sink.push(removed("a")).unwrap();
    assert_eq!(subscription.next().await, Some(removed("a")));
    sink.push(removed("b")).unwrap();
    sink.push(removed("c")).unwrap();
    assert_eq!(subscription.next().await, Some(Event::Resync));
}

#[tokio::test]
async fn a_capacity_above_the_limit_does_not_panic() {
    let (sink, mut subscription) = subscription(usize::MAX);
    sink.push(removed("a")).unwrap();
    assert_eq!(subscription.next().await, Some(removed("a")));
}

// 5b. The subscription pair: wake-ups and races.

/// A waker that records whether it was woken.
#[derive(Default)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl WakeFlag {
    fn take(&self) -> bool {
        self.0.swap(false, Ordering::SeqCst)
    }
}

/// Polls `future` once with a waker that sets `flag`.
fn poll_with<F: Future>(future: Pin<&mut F>, flag: &Arc<WakeFlag>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::clone(flag));
    future.poll(&mut Context::from_waker(&waker))
}

/// Polls `future` once with a waker that does nothing.
fn poll_now<F: Future>(future: F) -> Poll<F::Output> {
    pin!(future).poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn a_parked_next_gets_resync_when_the_sink_is_dropped_after_a_lag() {
    let (sink, mut subscription) = subscription(1);
    let flag = Arc::new(WakeFlag::default());
    {
        let mut next = pin!(subscription.next());
        assert!(poll_with(next.as_mut(), &flag).is_pending());
        sink.push(removed("a")).unwrap();
        sink.push(removed("b")).unwrap(); // overflows
        drop(sink);
        assert!(flag.take());
        assert_eq!(
            poll_with(next.as_mut(), &flag),
            Poll::Ready(Some(Event::Resync))
        );
    }
    for _ in 0..3 {
        assert_eq!(poll_now(subscription.next()), Poll::Ready(None));
    }
}

#[test]
fn a_parked_next_is_woken_by_a_push() {
    let (sink, mut subscription) = subscription(4);
    let flag = Arc::new(WakeFlag::default());
    let mut next = pin!(subscription.next());
    assert!(poll_with(next.as_mut(), &flag).is_pending());
    assert!(!flag.take());
    sink.push(removed("a")).unwrap();
    assert!(flag.take());
    assert_eq!(
        poll_with(next.as_mut(), &flag),
        Poll::Ready(Some(removed("a")))
    );
}

/// The old interleaving (the push sees a full queue, the subscriber empties the queue and parks,
/// and only then the push records the lag) cannot be built any more: a push decides and records
/// the overflow under the same lock in which it looks at the queue. Nor can an overflow be the only
/// wake-up of a parked call: a call parks only on an empty queue, so the queue fills (one wake-up)
/// before it can overflow. The nearest construction is an overflow while a `next()` is parked: the
/// call is woken, and when polled it returns `Resync`, not the dropped events.
#[test]
fn a_parked_next_is_woken_by_an_overflow() {
    let (sink, mut subscription) = subscription(1);
    sink.push(removed("a")).unwrap();
    assert_eq!(
        poll_now(subscription.next()),
        Poll::Ready(Some(removed("a")))
    );

    let flag = Arc::new(WakeFlag::default());
    let mut next = pin!(subscription.next());
    assert!(poll_with(next.as_mut(), &flag).is_pending());
    sink.push(removed("b")).unwrap();
    // Not polled in between: the next push overflows the queue while the call is still parked.
    sink.push(removed("c")).unwrap();
    assert!(flag.take());
    assert_eq!(
        poll_with(next.as_mut(), &flag),
        Poll::Ready(Some(Event::Resync))
    );
}

#[test]
fn a_parked_next_is_woken_by_dropping_the_sink() {
    let (sink, mut subscription) = subscription(4);
    let flag = Arc::new(WakeFlag::default());
    {
        let mut next = pin!(subscription.next());
        assert!(poll_with(next.as_mut(), &flag).is_pending());
        drop(sink);
        assert!(flag.take());
        assert_eq!(poll_with(next.as_mut(), &flag), Poll::Ready(None));
    }
    assert_eq!(poll_now(subscription.next()), Poll::Ready(None));
}

#[test]
fn pushes_after_a_lag_are_covered_by_the_resync() {
    let (sink, mut subscription) = subscription(2);
    for n in 1..=3 {
        sink.push(removed(&format!("p{n}"))).unwrap(); // the third overflows
    }
    sink.push(removed("p4")).unwrap(); // dropped: the pending Resync covers it
    assert_eq!(
        poll_now(subscription.next()),
        Poll::Ready(Some(Event::Resync))
    );
    assert!(poll_now(subscription.next()).is_pending());
    sink.push(removed("p5")).unwrap();
    assert_eq!(
        poll_now(subscription.next()),
        Poll::Ready(Some(removed("p5")))
    );
}

#[test]
fn closed_resolves_for_waiters_registered_before_and_after_the_drop() {
    let (sink, subscription) = subscription(1);
    let first = Arc::new(WakeFlag::default());
    let second = Arc::new(WakeFlag::default());
    let mut before = pin!(sink.closed());
    let mut also_before = pin!(sink.closed());
    assert!(poll_with(before.as_mut(), &first).is_pending());
    assert!(poll_with(also_before.as_mut(), &second).is_pending());
    drop(subscription);
    assert!(first.take());
    assert!(second.take());
    assert_eq!(poll_with(before.as_mut(), &first), Poll::Ready(()));
    assert_eq!(poll_with(also_before.as_mut(), &second), Poll::Ready(()));
    assert_eq!(poll_now(sink.closed()), Poll::Ready(()));
}

/// What the consumer read in one round of the stress test.
#[derive(Debug, PartialEq)]
enum Read {
    Number(u64),
    Resync,
}

/// Checks the invariants of one round: numbers strictly increase; wherever numbers are skipped, and
/// only there, a `Resync` was read in between (two in a row are allowed: everything pushed between
/// them was dropped); and if the last number pushed is missing, a
/// `Resync` was read after the last number delivered.
fn check_round(reads: &[Read], pushed: u64, capacity: usize) {
    let mut expected_next = 0;
    let mut resync_since_number = false;
    for read in reads {
        match read {
            Read::Resync => resync_since_number = true,
            Read::Number(n) => {
                assert!(
                    *n >= expected_next,
                    "{n} after {expected_next} (capacity {capacity})"
                );
                assert_eq!(
                    *n > expected_next,
                    resync_since_number,
                    "number {n}, expected {expected_next}, Resync read: {resync_since_number} \
                     (capacity {capacity})"
                );
                expected_next = n + 1;
                resync_since_number = false;
            }
        }
    }
    assert_eq!(
        expected_next < pushed,
        resync_since_number,
        "last number read {expected_next}, pushed {pushed}, Resync read before None: \
         {resync_since_number} (capacity {capacity})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stress_one_producer_one_consumer() {
    const PUSHES: u64 = 2_000;
    const ROUNDS: usize = 60;
    let all = async {
        for capacity in [1, 2, 3, 16, 256] {
            for round in 0..ROUNDS {
                let (sink, mut subscription) = subscription(capacity);
                let producer = tokio::spawn(async move {
                    for n in 0..PUSHES {
                        sink.push(Event::PeerRemoved {
                            name: n.to_string(),
                        })
                        .unwrap();
                    }
                    // The sink is dropped here.
                });
                let mut reads = Vec::new();
                let mut count = 0usize;
                while let Some(event) = subscription.next().await {
                    reads.push(match event {
                        Event::Resync => Read::Resync,
                        Event::PeerRemoved { name } => Read::Number(name.parse().unwrap()),
                        other => panic!("unexpected event {other:?}"),
                    });
                    count += 1;
                    if (count + round).is_multiple_of(7) {
                        tokio::task::yield_now().await;
                    }
                }
                producer.await.unwrap();
                assert_eq!(subscription.next().await, None);
                check_round(&reads, PUSHES, capacity);
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(120), all)
        .await
        .expect("the stress test did not finish within its bound");
}

// 6. Client over a scripted transport.

#[tokio::test]
async fn each_method_sends_its_request_and_returns_its_answer() {
    let page = LogPage {
        messages: vec![delivered()],
        next_before: Some(42),
    };
    let (client, fake) = client(vec![
        step(
            Request::Status,
            vec![],
            Ok(Response::Status { status: a_status() }),
        ),
        step(
            Request::Peers,
            vec![],
            Ok(Response::Peers {
                peers: vec![a_peer()],
            }),
        ),
        step(
            Request::PeerAdd {
                name: "bob".into(),
                station_id: "STATION-B".into(),
            },
            vec![],
            Ok(Response::Peer { peer: a_peer() }),
        ),
        step(
            Request::PeerRemove { name: "bob".into() },
            vec![],
            Ok(Response::Done),
        ),
        step(
            send_request(),
            vec![],
            Ok(Response::Message { message: sending() }),
        ),
        step(
            Request::Message { seq: 42 },
            vec![],
            Ok(Response::Message {
                message: delivered(),
            }),
        ),
        step(
            Request::Log {
                peer: None,
                before: None,
                limit: 50,
            },
            vec![],
            Ok(Response::Log { page: page.clone() }),
        ),
        step(
            Request::Log {
                peer: Some("bob".into()),
                before: Some(42),
                limit: 10,
            },
            vec![],
            Ok(Response::Log { page: page.clone() }),
        ),
    ]);
    assert_eq!(client.status().await, Ok(a_status()));
    assert_eq!(client.peers().await, Ok(vec![a_peer()]));
    assert_eq!(client.peer_add("bob", "STATION-B").await, Ok(a_peer()));
    assert_eq!(client.peer_remove("bob").await, Ok(()));
    assert_eq!(client.send("bob", "on my way").await, Ok(sending()));
    assert_eq!(client.message(42).await, Ok(delivered()));
    assert_eq!(client.log(None, None, 50).await, Ok(page.clone()));
    assert_eq!(client.log(Some("bob"), Some(42), 10).await, Ok(page));
    fake.assert_script_done();

    let mut events = client.subscribe().await.unwrap();
    fake.push(removed("bob"));
    assert_eq!(events.next().await, Some(removed("bob")));
}

#[tokio::test]
async fn a_wrong_response_variant_is_a_bad_reply() {
    let wrong = |request: Request| step(request, vec![], Ok(Response::Done));
    let (client, fake) = client(vec![
        wrong(Request::Status),
        wrong(Request::Peers),
        wrong(Request::PeerAdd {
            name: "bob".into(),
            station_id: "STATION-B".into(),
        }),
        step(
            Request::PeerRemove { name: "bob".into() },
            vec![],
            Ok(Response::Peer { peer: a_peer() }),
        ),
        wrong(send_request()),
        wrong(Request::Message { seq: 42 }),
        wrong(Request::Log {
            peer: None,
            before: None,
            limit: 50,
        }),
    ]);
    let bad = |result: Result<(), ApiError>, detail: &str| {
        assert_eq!(
            result,
            Err(ApiError::BadReply {
                detail: detail.into()
            })
        );
    };
    let got_done = |expected: &str| format!("expected a {expected} response, got a done response");
    bad(client.status().await.map(drop), &got_done("status"));
    bad(client.peers().await.map(drop), &got_done("peers"));
    bad(
        client.peer_add("bob", "STATION-B").await.map(drop),
        &got_done("peer"),
    );
    bad(
        client.peer_remove("bob").await,
        "expected a done response, got a peer response",
    );
    bad(
        client.send("bob", "on my way").await.map(drop),
        &got_done("message"),
    );
    bad(client.message(42).await.map(drop), &got_done("message"));
    bad(client.log(None, None, 50).await.map(drop), &got_done("log"));
    fake.assert_script_done();
}

#[tokio::test]
async fn a_bad_reply_does_not_carry_the_response_contents() {
    const MARKER: &str = "MARKER-7f3a";
    let mut status = a_status();
    status.station_id = MARKER.into();
    status.instance_id = MARKER.into();
    status.towers[0].url = format!("http://{MARKER}/");
    let mut message = sending();
    message.body = MARKER.into();
    message.id = MARKER.into();
    let (client, fake) = client(vec![
        step(Request::Peers, vec![], Ok(Response::Status { status })),
        step(Request::Status, vec![], Ok(Response::Message { message })),
    ]);
    for error in [
        client.peers().await.map(drop).unwrap_err(),
        client.status().await.map(drop).unwrap_err(),
    ] {
        assert!(matches!(error, ApiError::BadReply { .. }), "{error:?}");
        assert!(!format!("{error:?}").contains(MARKER), "{error:?}");
        assert!(!error.to_string().contains(MARKER), "{error}");
    }
    fake.assert_script_done();
}

#[tokio::test]
async fn errors_from_the_transport_pass_through_unchanged() {
    let errors = [
        ApiError::Stopping,
        ApiError::Timeout { after_ms: 10_000 },
        ApiError::StationTaken {
            station_id: "STATION-B".into(),
            peer: "robert".into(),
        },
        ApiError::PeerNotFound { name: "bob".into() },
        ApiError::Invalid {
            field: "body".into(),
            reason: "must not be empty".into(),
        },
        ApiError::MessageNotFound { seq: 42 },
        ApiError::Disconnected {
            detail: "connection reset".into(),
        },
    ];
    let requests = [
        Request::Status,
        Request::Peers,
        Request::PeerAdd {
            name: "bob".into(),
            station_id: "STATION-B".into(),
        },
        Request::PeerRemove { name: "bob".into() },
        send_request(),
        Request::Message { seq: 42 },
        Request::Log {
            peer: None,
            before: None,
            limit: 50,
        },
    ];
    let script = requests
        .into_iter()
        .zip(errors.iter().cloned())
        .map(|(request, error)| step(request, vec![], Err(error)))
        .collect();
    let (client, fake) = client(script);
    let [e0, e1, e2, e3, e4, e5, e6] = errors;
    assert_eq!(client.status().await, Err(e0));
    assert_eq!(client.peers().await, Err(e1));
    assert_eq!(client.peer_add("bob", "STATION-B").await, Err(e2));
    assert_eq!(client.peer_remove("bob").await, Err(e3));
    assert_eq!(client.send("bob", "on my way").await, Err(e4));
    assert_eq!(client.message(42).await, Err(e5));
    assert_eq!(client.log(None, None, 50).await, Err(e6));
    fake.assert_script_done();
}

#[tokio::test]
async fn a_subscribe_error_passes_through_and_nothing_is_sent() {
    let not_running = ApiError::NotRunning {
        socket: "/tmp/test/station.sock".into(),
    };
    let (client, fake) = client(vec![]);
    fake.state.lock().unwrap().subscribe_error = Some(not_running.clone());
    assert_eq!(client.subscribe().await.err(), Some(not_running.clone()));
    // The empty script makes any call panic: send_and_wait must not send.
    assert_eq!(
        client.send_and_wait("bob", "on my way").await,
        Err(not_running)
    );
}

// 7. send_and_wait.

#[tokio::test]
async fn send_and_wait_sees_an_outcome_that_arrives_before_the_answer() {
    let (client, fake) = client(vec![step(
        send_request(),
        vec![event(sending()), event(delivered())],
        Ok(Response::Message { message: sending() }),
    )]);
    assert_eq!(
        client.send_and_wait("bob", "on my way").await,
        Ok(delivered())
    );
    fake.assert_script_done();
}

#[tokio::test]
async fn send_and_wait_returns_delivered() {
    let (client, fake) = client(vec![step(
        send_request(),
        vec![event(sending())],
        Ok(Response::Message { message: sending() }),
    )]);
    let (result, ()) = tokio::join!(client.send_and_wait("bob", "on my way"), async {
        fake.answered.notified().await;
        fake.push(event(delivered()));
    });
    assert_eq!(result, Ok(delivered()));
}

#[tokio::test]
async fn send_and_wait_returns_each_failure() {
    for reason in [
        FailReason::NoAnswer,
        FailReason::Refused,
        FailReason::PeerRemoved,
    ] {
        let failed = outgoing(42, 1, MessageStatus::Failed { reason });
        let (client, fake) = client(vec![step(
            send_request(),
            vec![event(sending())],
            Ok(Response::Message { message: sending() }),
        )]);
        let (result, ()) = tokio::join!(client.send_and_wait("bob", "on my way"), async {
            fake.answered.notified().await;
            fake.push(event(failed.clone()));
        });
        assert_eq!(result, Ok(failed));
    }
}

#[tokio::test]
async fn send_and_wait_returns_a_status_that_is_final_in_the_answer() {
    let (client, fake) = client(vec![step(
        send_request(),
        vec![],
        Ok(Response::Message {
            message: delivered(),
        }),
    )]);
    assert_eq!(
        client.send_and_wait("bob", "on my way").await,
        Ok(delivered())
    );
    fake.assert_script_done();
}

#[tokio::test]
async fn send_and_wait_ignores_events_for_other_messages() {
    let (client, fake) = client(vec![step(
        send_request(),
        vec![event(sending())],
        Ok(Response::Message { message: sending() }),
    )]);
    let (result, ()) = tokio::join!(client.send_and_wait("bob", "on my way"), async {
        fake.answered.notified().await;
        fake.push(event(outgoing(41, 1, MessageStatus::Delivered)));
        fake.push(event(outgoing(
            43,
            1,
            MessageStatus::Failed {
                reason: FailReason::NoAnswer,
            },
        )));
        fake.push(Event::Peer { peer: a_peer() });
        fake.push(Event::Status { status: a_status() });
        fake.push(removed("carol"));
        fake.push(event(delivered()));
    });
    assert_eq!(result, Ok(delivered()));
}

#[tokio::test]
async fn send_and_wait_rereads_after_a_resync() {
    let (client, fake) = client(vec![
        step(
            send_request(),
            vec![event(sending())],
            Ok(Response::Message { message: sending() }),
        ),
        step(
            Request::Message { seq: 42 },
            vec![],
            Ok(Response::Message {
                message: delivered(),
            }),
        ),
    ]);
    let (result, ()) = tokio::join!(client.send_and_wait("bob", "on my way"), async {
        fake.answered.notified().await;
        fake.push(Event::Resync);
    });
    assert_eq!(result, Ok(delivered()));
    fake.assert_script_done();
}

#[tokio::test]
async fn send_and_wait_keeps_waiting_when_the_reread_is_still_sending() {
    let (client, fake) = client(vec![
        step(
            send_request(),
            vec![event(sending())],
            Ok(Response::Message { message: sending() }),
        ),
        step(
            Request::Message { seq: 42 },
            vec![],
            Ok(Response::Message { message: sending() }),
        ),
    ]);
    let (result, ()) = tokio::join!(client.send_and_wait("bob", "on my way"), async {
        fake.answered.notified().await;
        fake.push(Event::Resync);
        fake.answered.notified().await;
        fake.push(event(delivered()));
    });
    assert_eq!(result, Ok(delivered()));
    fake.assert_script_done();
}

#[tokio::test]
async fn send_and_wait_fails_when_the_reread_is_another_message() {
    let mut other = outgoing(42, 0, MessageStatus::Delivered);
    other.id = "MSG-OTHER".into();
    let (client, fake) = client(vec![
        step(
            send_request(),
            vec![event(sending())],
            Ok(Response::Message { message: sending() }),
        ),
        step(
            Request::Message { seq: 42 },
            vec![],
            Ok(Response::Message { message: other }),
        ),
    ]);
    let (result, ()) = tokio::join!(client.send_and_wait("bob", "on my way"), async {
        fake.answered.notified().await;
        fake.push(Event::Resync);
    });
    match result {
        Err(ApiError::Disconnected { detail }) => {
            assert!(detail.contains("restarted"), "{detail}");
        }
        other => panic!("expected Disconnected, got {other:?}"),
    }
    fake.assert_script_done();
}

#[tokio::test]
async fn send_and_wait_passes_on_an_error_of_the_reread() {
    let (client, fake) = client(vec![
        step(
            send_request(),
            vec![event(sending())],
            Ok(Response::Message { message: sending() }),
        ),
        step(
            Request::Message { seq: 42 },
            vec![],
            Err(ApiError::MessageNotFound { seq: 42 }),
        ),
    ]);
    let (result, ()) = tokio::join!(client.send_and_wait("bob", "on my way"), async {
        fake.answered.notified().await;
        fake.push(Event::Resync);
    });
    assert_eq!(result, Err(ApiError::MessageNotFound { seq: 42 }));
}

#[tokio::test]
async fn send_and_wait_fails_when_the_subscription_ends_first() {
    let (client, fake) = client(vec![step(
        send_request(),
        vec![event(sending())],
        Ok(Response::Message { message: sending() }),
    )]);
    let (result, ()) = tokio::join!(client.send_and_wait("bob", "on my way"), async {
        fake.answered.notified().await;
        fake.end_subscriptions();
    });
    match result {
        Err(ApiError::Disconnected { detail }) => {
            assert!(detail.contains("subscription ended"), "{detail}");
        }
        other => panic!("expected Disconnected, got {other:?}"),
    }
}

#[tokio::test]
async fn send_and_wait_passes_on_a_refused_send() {
    let (client, fake) = client(vec![step(
        send_request(),
        vec![],
        Err(ApiError::PeerNotFound { name: "bob".into() }),
    )]);
    assert_eq!(
        client.send_and_wait("bob", "on my way").await,
        Err(ApiError::PeerNotFound { name: "bob".into() })
    );
    fake.assert_script_done();
}

// 8. Compile-time checks, and the transport forms the trait allows.

fn assert_send<T: Send>(_: &T) {}

/// Compiles only if `send_and_wait`'s future is `Send` for every transport.
fn send_and_wait_is_send<T: Transport>(
    client: &Client<T>,
) -> impl Future<Output = Result<Message, ApiError>> + Send + '_ {
    client.send_and_wait("bob", "on my way")
}

/// Compiles only if `next`'s future is `Send`.
fn next_is_send(
    subscription: &mut Subscription,
) -> impl Future<Output = Option<Event>> + Send + '_ {
    subscription.next()
}

#[tokio::test]
async fn futures_are_send() {
    let (client, _fake) = client(vec![]);
    let future = send_and_wait_is_send(&client);
    assert_send(&future);
    drop(future);
    let (_sink, mut subscription) = subscription(1);
    let future = next_is_send(&mut subscription);
    assert_send(&future);
    drop(future);
    assert_send(&client.status());
}

#[tokio::test]
async fn a_client_can_own_its_transport() {
    let client = Client::new(Fake::new(vec![step(
        send_request(),
        vec![event(sending()), event(delivered())],
        Ok(Response::Message { message: sending() }),
    )]));
    assert_eq!(
        client.send_and_wait("bob", "on my way").await,
        Ok(delivered())
    );
    client.transport().assert_script_done();
}

#[tokio::test]
async fn a_client_is_cheap_to_share_over_an_arc() {
    let (client, fake) = client(vec![step(
        Request::PeerRemove { name: "bob".into() },
        vec![],
        Ok(Response::Done),
    )]);
    let clone = client.clone();
    assert!(Arc::ptr_eq(client.transport(), clone.transport()));
    assert_eq!(clone.peer_remove("bob").await, Ok(()));
    fake.assert_script_done();
}

#[tokio::test]
async fn send_and_wait_returns_the_outcome_of_a_message_evicted_before_it() {
    // A station that keeps 1 message: the sent one leaves the log before its outcome, and no
    // Resync happens. The outcome's event must still come (rule 7).
    let station = chief::fake::FakeStation::new("STATION-A", "INSTANCE-1").with_log_capacity(1);
    let client = Client::new(station.clone());
    client.peer_add("bob", "STATION-B").await.unwrap();
    station.set_outcome(chief::fake::SendOutcome::Hold);
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();
    let mut sent_tx = Some(sent_tx);
    station.on_call(move |_, request| {
        if matches!(request, Request::Send { .. })
            && let Some(tx) = sent_tx.take()
        {
            let _ = tx.send(());
        }
    });
    let waiting = tokio::spawn(async move { client.send_and_wait("bob", "on my way").await });
    // The test runs on one thread: once the hook has fired, the send has been handled and the
    // waiting task is parked on its subscription.
    sent_rx.await.unwrap();
    station.receive("bob", "newer").unwrap();
    assert!(station.messages().iter().all(|m| m.seq != 1));
    let delivered = station.resolve(1, MessageStatus::Delivered).unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
        .await
        .expect("send_and_wait did not see the outcome of an evicted message")
        .unwrap();
    assert_eq!(outcome, Ok(delivered));
}
