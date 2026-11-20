#![cfg(test)]
//! `Client::follow_log` against the fake station: backlog then live with no duplicate, changes,
//! `Resync`, a peer filter, cancellation, the end of the subscription, a restart while attaching,
//! and a re-read whose calls are held while events arrive. Deterministic: messages "between the
//! subscribe and the page" are made by the fake's call hook, and a held call waits until the test
//! releases it. Nothing sleeps; every wait that could hang is bounded.

use chief::api::*;
use chief::client::{
    Client, EventSink, SUBSCRIPTION_CAPACITY, Subscription, Transport, subscription,
};
use chief::fake::{FakeStation, SendOutcome};
use chief::follow::{FOLLOW_MEMORY, LogFollower, LogUpdate};
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::sync::Notify;

fn fake_with_peers() -> FakeStation {
    let fake = FakeStation::new("STATION-A", "INSTANCE-1");
    let c = Client::new(fake.clone());
    for (name, id) in [("alice", "STATION-AL"), ("bob", "STATION-B")] {
        assert!(poll_once(c.peer_add(name, id)).unwrap().is_ok());
    }
    fake
}

/// Polls `future` once: `Some` if it is ready. Outside tokio's cooperative budget, which would
/// otherwise make a ready channel look empty after many polls in one turn of the test's task.
fn poll_once<F: Future>(future: F) -> Option<F::Output> {
    let mut future = pin!(tokio::task::unconstrained(future));
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

fn receive_n(fake: &FakeStation, peer: &str, n: usize) {
    for _ in 0..n {
        fake.receive(peer, "x").unwrap();
    }
}

/// Every update the follower can give without waiting, as `(added?, seq, rev)`.
fn ready<T: Transport>(follower: &mut LogFollower<'_, T>) -> Vec<(bool, u64, u32)> {
    let mut out = Vec::new();
    while let Some(update) = poll_once(follower.next()) {
        let update = update.unwrap().expect("the subscription ended");
        let m = update.message();
        out.push((matches!(update, LogUpdate::Added(_)), m.seq, m.rev));
    }
    out
}

fn added(seqs: impl IntoIterator<Item = u64>) -> Vec<(bool, u64, u32)> {
    seqs.into_iter().map(|s| (true, s, 0)).collect()
}

#[tokio::test]
async fn backlog_comes_first_oldest_first_then_live() {
    let fake = fake_with_peers();
    receive_n(&fake, "bob", 5);
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 3).await.unwrap();
    assert_eq!(ready(&mut f), added([3, 4, 5]));
    fake.receive("alice", "live").unwrap();
    assert_eq!(ready(&mut f), added([6]));
}

#[tokio::test]
async fn a_backlog_spans_several_pages_and_a_short_log() {
    let fake = fake_with_peers();
    let body = "x".repeat(BODY_MAX_BYTES);
    for _ in 0..40 {
        fake.receive("bob", &body).unwrap();
    }
    let c = Client::new(fake.clone());
    // 40 messages of 16 KiB need two pages under the byte budget.
    let mut f = c.follow_log(None, 50).await.unwrap();
    assert_eq!(ready(&mut f), added(1..=40));
    let logs = fake
        .requests()
        .iter()
        .filter(|r| matches!(r, Request::Log { .. }))
        .count();
    assert!(logs >= 2, "{logs}");
}

#[tokio::test]
async fn messages_between_the_subscribe_and_the_page_are_shown_once() {
    let fake = fake_with_peers();
    receive_n(&fake, "bob", 3);
    fake.on_call(|fake, request| {
        if matches!(request, Request::Log { .. }) {
            // After the subscribe and before the page: their events are queued, and the page
            // holds them too. One is an outgoing send that is delivered at once (rev 1).
            fake.receive("bob", "between").unwrap();
            fake.receive("alice", "between").unwrap();
            assert!(
                poll_once(Client::new(fake.clone()).send("bob", "sent between"))
                    .unwrap()
                    .is_ok()
            );
            fake.clear_on_call();
        }
    });
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 50).await.unwrap();
    // seq 6 is already delivered in the page: rev 1, and its stale events are skipped.
    assert_eq!(
        ready(&mut f),
        vec![
            (true, 1, 0),
            (true, 2, 0),
            (true, 3, 0),
            (true, 4, 0),
            (true, 5, 0),
            (true, 6, 1)
        ]
    );
    fake.receive("bob", "after").unwrap();
    assert_eq!(ready(&mut f), added([7]));
}

#[tokio::test]
async fn a_changed_message_yields_changed() {
    let fake = fake_with_peers();
    fake.set_outcome(SendOutcome::Hold);
    let c = Client::new(fake.clone());
    let held = c.send("bob", "held").await.unwrap();
    let mut f = c.follow_log(None, 10).await.unwrap();
    assert_eq!(ready(&mut f), added([held.seq]));
    fake.resolve(held.seq, MessageStatus::Delivered).unwrap();
    let update = f.next().await.unwrap().unwrap();
    let LogUpdate::Changed(m) = update else {
        panic!("expected Changed, got {update:?}");
    };
    assert_eq!(
        (m.seq, m.rev, m.status),
        (held.seq, 1, MessageStatus::Delivered)
    );
    // A live send that is delivered at once: Added at rev 0, then Changed at rev 1.
    fake.set_outcome(SendOutcome::Deliver);
    let sent = c.send("bob", "now").await.unwrap();
    assert_eq!(
        ready(&mut f),
        vec![(true, sent.seq, 0), (false, sent.seq, 1)]
    );
}

#[tokio::test]
async fn a_resync_yields_exactly_what_was_missed() {
    let fake = fake_with_peers();
    fake.set_outcome(SendOutcome::Hold);
    let c = Client::new(fake.clone());
    receive_n(&fake, "bob", 3);
    let held = c.send("bob", "held").await.unwrap(); // seq 4
    let mut f = c.follow_log(None, 10).await.unwrap();
    assert_eq!(ready(&mut f), added(1..=4));
    // Overflow the subscription without reading: new messages and a change to a shown one.
    fake.resolve(held.seq, MessageStatus::Delivered).unwrap();
    receive_n(&fake, "alice", SUBSCRIPTION_CAPACITY + 50);
    let newest = 4 + SUBSCRIPTION_CAPACITY as u64 + 50;
    let mut expected = vec![(false, 4, 1)];
    expected.extend(added(5..=newest));
    assert_eq!(ready(&mut f), expected);
    fake.receive("bob", "live again").unwrap();
    assert_eq!(ready(&mut f), added([newest + 1]));
}

#[tokio::test]
async fn a_resync_reads_back_no_further_than_what_is_shown() {
    let fake = fake_with_peers().with_log_capacity(3_000);
    receive_n(&fake, "bob", 2_500);
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    assert_eq!(ready(&mut f), added(2_491..=2_500));
    let before = fake.requests().len();
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 44);
    assert_eq!(ready(&mut f), added(2_501..=2_800));
    // One page (newest 1,000) reaches back past seq 2,491: no second page.
    let logs: Vec<Request> = fake.requests()[before..].to_vec();
    assert_eq!(
        logs,
        vec![
            Request::Status,
            Request::Log {
                peer: None,
                before: None,
                limit: LOG_PAGE_MAX_MESSAGES
            },
            Request::Status
        ]
    );
}

#[tokio::test]
async fn with_no_tail_only_new_messages_are_shown_also_after_a_resync() {
    let fake = fake_with_peers();
    receive_n(&fake, "bob", 5);
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    assert!(ready(&mut f).is_empty());
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1);
    assert_eq!(
        ready(&mut f),
        added(6..=5 + SUBSCRIPTION_CAPACITY as u64 + 1)
    );
}

#[tokio::test]
async fn with_no_tail_on_an_empty_log_everything_is_new() {
    let fake = fake_with_peers();
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1);
    assert_eq!(ready(&mut f), added(1..=SUBSCRIPTION_CAPACITY as u64 + 1));
}

#[tokio::test]
async fn a_peer_filter_limits_backlog_live_and_resync() {
    let fake = fake_with_peers();
    for i in 0..6 {
        fake.receive(if i % 2 == 0 { "bob" } else { "alice" }, "x")
            .unwrap();
    }
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(Some("bob"), 2).await.unwrap();
    assert_eq!(f.peer(), Some("bob"));
    assert_eq!(ready(&mut f), added([3, 5]));
    fake.receive("alice", "x").unwrap(); // 7
    fake.receive("bob", "x").unwrap(); // 8
    assert_eq!(ready(&mut f), added([8]));
    for i in 0..SUBSCRIPTION_CAPACITY + 2 {
        fake.receive(if i % 2 == 0 { "alice" } else { "bob" }, "x")
            .unwrap();
    }
    let bobs: Vec<u64> = (9..9 + SUBSCRIPTION_CAPACITY as u64 + 2)
        .filter(|s| s % 2 == 0)
        .collect();
    assert_eq!(ready(&mut f), added(bobs));
}

#[tokio::test]
async fn peer_and_status_events_are_skipped() {
    let fake = fake_with_peers();
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    fake.set_tower_state(TowerState::Connected);
    c.peer_add("carol", "STATION-C").await.unwrap();
    fake.set_link("bob", "STATION-B", Link::Relayed).unwrap();
    c.peer_remove("carol").await.unwrap();
    assert!(ready(&mut f).is_empty());
    fake.receive("bob", "x").unwrap();
    assert_eq!(ready(&mut f), added([1]));
}

#[tokio::test]
async fn a_dropped_pending_next_loses_nothing() {
    let fake = fake_with_peers();
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    assert!(poll_once(f.next()).is_none());
    fake.receive("bob", "x").unwrap();
    assert_eq!(ready(&mut f), added([1]));
}

#[tokio::test]
async fn the_end_of_the_subscription_gives_none_after_what_is_queued() {
    let fake = fake_with_peers();
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    fake.receive("bob", "x").unwrap();
    fake.stop();
    assert_eq!(f.next().await.unwrap().unwrap().message().seq, 1);
    assert_eq!(f.next().await.unwrap(), None);
    assert_eq!(f.next().await.unwrap(), None);
}

#[tokio::test]
async fn errors_of_the_attach_are_passed_through() {
    let fake = fake_with_peers();
    fake.stop();
    let c = Client::new(fake.clone());
    assert_eq!(
        c.follow_log(None, 10).await.unwrap_err(),
        ApiError::Stopping
    );
}

#[tokio::test]
async fn the_follower_forgets_the_oldest_and_never_shows_them_again() {
    let fake = fake_with_peers().with_log_capacity(FOLLOW_MEMORY + 600);
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    let total = FOLLOW_MEMORY as u64 + 100;
    for seq in 1..=total {
        fake.receive("bob", "x").unwrap();
        assert_eq!(ready(&mut f), added([seq]));
    }
    // A resync now re-reads the newest page; everything in it was shown or forgotten.
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1);
    assert_eq!(
        ready(&mut f),
        added(total + 1..=total + SUBSCRIPTION_CAPACITY as u64 + 1)
    );
}

// A transport whose log calls can be held or failed, to interrupt a re-read.

#[derive(Clone)]
struct Gated {
    fake: FakeStation,
    hold_log: Arc<AtomicBool>,
    fail_logs: Arc<AtomicUsize>,
}

impl Transport for Gated {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        if matches!(request, Request::Log { .. }) {
            if self.hold_log.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self
                .fail_logs
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(ApiError::Disconnected {
                    detail: "test".into(),
                });
            }
        }
        self.fake.call(request).await
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        self.fake.subscribe().await
    }
}

fn gated() -> (FakeStation, Gated) {
    let fake = fake_with_peers();
    let gated = Gated {
        fake: fake.clone(),
        hold_log: Arc::new(AtomicBool::new(false)),
        fail_logs: Arc::new(AtomicUsize::new(0)),
    };
    (fake, gated)
}

#[tokio::test]
async fn an_interrupted_resync_is_done_again() {
    let (fake, gated) = gated();
    let c = Client::new(gated.clone());
    receive_n(&fake, "bob", 2);
    let mut f = c.follow_log(None, 10).await.unwrap();
    assert_eq!(ready(&mut f), added([1, 2]));
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1);
    gated.hold_log.store(true, Ordering::SeqCst);
    // The re-read starts and is dropped while its log call is pending.
    assert!(poll_once(f.next()).is_none());
    assert!(poll_once(f.next()).is_none());
    gated.hold_log.store(false, Ordering::SeqCst);
    assert_eq!(ready(&mut f), added(3..=SUBSCRIPTION_CAPACITY as u64 + 3));
}

#[tokio::test]
async fn a_failed_resync_returns_the_error_and_is_tried_again() {
    let (fake, gated) = gated();
    let c = Client::new(gated.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1);
    gated.fail_logs.store(1, Ordering::SeqCst);
    assert!(matches!(f.next().await, Err(ApiError::Disconnected { .. })));
    assert_eq!(ready(&mut f), added(1..=SUBSCRIPTION_CAPACITY as u64 + 1));
}

// The end of a follower (rules 1 and 11).

#[tokio::test]
async fn a_resync_then_a_stop_ends_the_follower() {
    let fake = fake_with_peers();
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1);
    fake.stop();
    // The re-read races the subscription, which is polled first: its end wins over the error of
    // the stopped station's status call. Then the end, for good.
    assert_eq!(f.next().await, Ok(None));
    assert_eq!(f.next().await, Ok(None));
    assert_eq!(f.next().await, Ok(None));
}

#[tokio::test]
async fn a_failed_reread_returns_the_error_once_and_ends_when_the_subscription_has() {
    let fake = fake_with_peers();
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1);
    let error = ApiError::Disconnected {
        detail: "test".into(),
    };
    fake.fail_calls(Some(error.clone()));
    assert_eq!(f.next().await, Err(error));
    fake.stop();
    assert_eq!(f.next().await, Ok(None));
    assert_eq!(f.next().await, Ok(None));
}

/// Events from one fake, calls to another that a test can switch: a station that restarted while
/// the subscription has not noticed yet.
#[derive(Clone)]
struct Switch {
    events_from: FakeStation,
    calls_to: Arc<std::sync::Mutex<FakeStation>>,
}

impl Switch {
    fn switch_to(&self, fake: &FakeStation) {
        *self.calls_to.lock().unwrap() = fake.clone();
    }
}

impl Transport for Switch {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        let fake = self.calls_to.lock().unwrap().clone();
        fake.call(request).await
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        self.events_from.subscribe().await
    }
}

/// An old instance with 3 messages, and a new one with 40 that a `Switch` can move the calls to.
fn old_and_new() -> (FakeStation, FakeStation, Switch) {
    let old = fake_with_peers();
    receive_n(&old, "bob", 3);
    let new = FakeStation::new("STATION-A", "INSTANCE-2");
    let c = Client::new(new.clone());
    assert!(poll_once(c.peer_add("bob", "STATION-B")).unwrap().is_ok());
    receive_n(&new, "bob", 40);
    let switch = Switch {
        events_from: old.clone(),
        calls_to: Arc::new(std::sync::Mutex::new(old.clone())),
    };
    (old, new, switch)
}

#[tokio::test]
async fn a_reread_that_finds_a_new_instance_first_yields_nothing_and_ends() {
    let (old, new, switch) = old_and_new();
    let c = Client::new(switch.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    assert_eq!(ready(&mut f), added(1..=3));
    receive_n(&old, "bob", SUBSCRIPTION_CAPACITY + 1);
    switch.switch_to(&new);
    assert_eq!(f.next().await, Ok(None));
    assert_eq!(f.next().await, Ok(None));
    // It asked the new instance for its status and nothing else.
    assert_eq!(new.requests().last(), Some(&Request::Status));
    assert!(
        !new.requests()
            .iter()
            .any(|r| matches!(r, Request::Log { .. }))
    );
}

#[tokio::test]
async fn a_reread_that_finds_a_new_instance_after_its_pages_applies_nothing_and_ends() {
    let (old, new, switch) = old_and_new();
    let c = Client::new(switch.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    assert_eq!(ready(&mut f), added(1..=3));
    receive_n(&old, "bob", SUBSCRIPTION_CAPACITY + 1);
    // The station restarts while the re-read pages the old instance.
    let to_new = switch.clone();
    let new_one = new.clone();
    old.on_call(move |_, request| {
        if matches!(request, Request::Log { .. }) {
            to_new.switch_to(&new_one);
        }
    });
    assert_eq!(f.next().await, Ok(None));
    assert_eq!(f.next().await, Ok(None));
}

#[tokio::test]
async fn a_restart_while_attaching_is_disconnected() {
    for tail in [10, 0] {
        let fake = fake_with_peers();
        receive_n(&fake, "bob", 3);
        fake.on_call(|fake, request| {
            if matches!(request, Request::Log { .. }) {
                fake.clear_on_call();
                fake.restart("INSTANCE-2");
            }
        });
        let c = Client::new(fake.clone());
        assert!(
            matches!(
                c.follow_log(None, tail).await,
                Err(ApiError::Disconnected { detail }) if detail.contains("restarted")
            ),
            "tail {tail}"
        );
    }
}

// A re-read that is cancelled after every call goes on where it stopped.

/// Every call is pending once before it is made, so a future of `next()` that is polled twice and
/// dropped has made exactly one call.
#[derive(Clone)]
struct Stepwise(FakeStation);

struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl Transport for Stepwise {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        YieldOnce(false).await;
        self.0.call(request).await
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        self.0.subscribe().await
    }
}

/// A fake whose log of 300 messages of 16 KiB needs about ten pages to re-read.
fn big_log_after_attach() -> FakeStation {
    let fake = fake_with_peers();
    fake.set_now_ms(1_767_225_600_000);
    fake
}

fn overflow_with_big_messages(fake: &FakeStation) {
    let body = "x".repeat(BODY_MAX_BYTES);
    for _ in 0..300 {
        fake.receive("bob", &body).unwrap();
    }
}

#[tokio::test]
async fn a_reread_cancelled_after_every_call_goes_on_where_it_stopped() {
    // Uninterrupted.
    let fake = big_log_after_attach();
    let c = Client::new(Stepwise(fake.clone()));
    let mut f = c.follow_log(None, 0).await.unwrap();
    overflow_with_big_messages(&fake);
    let start = fake.requests().len();
    let mut whole = Vec::new();
    while whole.len() < 300 {
        whole.push(f.next().await.unwrap().unwrap());
    }
    let whole_calls = fake.requests()[start..].to_vec();
    let logs = whole_calls
        .iter()
        .filter(|r| matches!(r, Request::Log { .. }))
        .count();
    assert!(logs >= 5, "{logs}");
    assert_eq!(whole_calls.len(), logs + 2);

    // Cancelled after every call that completes.
    let fake = big_log_after_attach();
    let c = Client::new(Stepwise(fake.clone()));
    let mut f = c.follow_log(None, 0).await.unwrap();
    overflow_with_big_messages(&fake);
    let start = fake.requests().len();
    let mut cancelled = Vec::new();
    let mut futures = 0;
    while cancelled.len() < 300 {
        futures += 1;
        assert!(futures < 2_000, "the re-read never finishes");
        let mut next = std::pin::pin!(tokio::task::unconstrained(f.next()));
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..2 {
            if let Poll::Ready(update) = next.as_mut().poll(&mut cx) {
                cancelled.push(update.unwrap().unwrap());
                break;
            }
        }
    }
    assert_eq!(fake.requests()[start..].to_vec(), whole_calls);
    assert_eq!(cancelled, whole);
}

// The attach is bracketed by two instance checks.

/// After its subscribe, the old instance queues one more event and restarts as a new instance with
/// two messages of its own, before the follower reads any page.
#[derive(Clone)]
struct RestartAfterSubscribe(FakeStation);

impl Transport for RestartAfterSubscribe {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        self.0.call(request).await
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        let events = self.0.subscribe().await?;
        self.0.receive("bob", "old").unwrap();
        self.0.restart("INSTANCE-2");
        receive_n(&self.0, "bob", 2);
        Ok(events)
    }
}

#[tokio::test]
async fn a_subscription_of_the_old_instance_fails_the_attach() {
    for tail in [10, 0] {
        let fake = fake_with_peers();
        receive_n(&fake, "bob", 3);
        let c = Client::new(RestartAfterSubscribe(fake.clone()));
        // The subscription (with seq 4 of the old instance queued) and the pages (seq 1 and 2 of
        // the new one) come from different instances: no follower, so nothing of either is shown.
        assert!(
            matches!(
                c.follow_log(None, tail).await,
                Err(ApiError::Disconnected { detail }) if detail.contains("restarted")
            ),
            "tail {tail}"
        );
        assert_eq!(fake.status().instance_id, "INSTANCE-2");
    }
}

#[tokio::test]
async fn a_restart_before_the_first_status_attaches_to_the_new_instance() {
    let fake = fake_with_peers();
    receive_n(&fake, "bob", 3);
    fake.on_call(|fake, request| {
        if matches!(request, Request::Status) {
            fake.clear_on_call();
            fake.restart("INSTANCE-2");
            receive_n(fake, "bob", 2);
        }
    });
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    assert_eq!(ready(&mut f), added([1, 2]));
    fake.receive("bob", "live").unwrap();
    assert_eq!(ready(&mut f), added([3]));
}

#[tokio::test]
async fn a_restart_after_the_attach_ends_the_follower_after_what_is_queued() {
    let fake = fake_with_peers();
    receive_n(&fake, "bob", 3);
    let c = Client::new(fake.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    fake.restart("INSTANCE-2");
    receive_n(&fake, "bob", 5);
    for seq in 1..=3 {
        assert_eq!(f.next().await.unwrap().unwrap().message().seq, seq);
    }
    assert_eq!(f.next().await, Ok(None));
    assert_eq!(f.next().await, Ok(None));
}

// A re-read whose calls are held while the subscription goes on.

/// The calls of one kind that a [`Held`] transport received, held or not, and which of them it
/// holds.
#[derive(Default)]
struct Calls {
    received: AtomicUsize,
    /// Calls from this number on (counting from 1) wait for [`Calls::release`]; 0 holds none.
    hold_from: AtomicUsize,
    released: Notify,
}

impl Calls {
    fn received(&self) -> usize {
        self.received.load(Ordering::SeqCst)
    }

    /// Lets the next `n` calls of this kind through and holds every later one until `release`.
    fn hold_after(&self, n: usize) {
        self.hold_from
            .store(self.received() + n + 1, Ordering::SeqCst);
    }

    /// Holds the next call of this kind and every later one until `release`.
    fn hold_next(&self) {
        self.hold_after(0);
    }

    fn release(&self) {
        self.hold_from.store(0, Ordering::SeqCst);
        self.released.notify_waiters();
    }

    /// Counts a call, and waits while it is held.
    async fn arrive(&self) {
        let n = self.received.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let mut released = pin!(self.released.notified());
            released.as_mut().enable();
            let from = self.hold_from.load(Ordering::SeqCst);
            if from == 0 || n < from {
                return;
            }
            released.await;
        }
    }
}

/// Calls to a fake whose log and status calls are counted and can be held before the fake sees
/// them.
#[derive(Clone)]
struct Held {
    fake: FakeStation,
    logs: Arc<Calls>,
    statuses: Arc<Calls>,
}

fn held(fake: &FakeStation) -> Held {
    Held {
        fake: fake.clone(),
        logs: Arc::default(),
        statuses: Arc::default(),
    }
}

impl Transport for Held {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        match request {
            Request::Log { .. } => self.logs.arrive().await,
            Request::Status => self.statuses.arrive().await,
            _ => {}
        }
        self.fake.call(request).await
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        self.fake.subscribe().await
    }
}

/// Polls a pinned future once with a waker that does nothing: `Some` if it is ready.
fn poll_now<F: Future>(future: Pin<&mut F>) -> Option<F::Output> {
    match future.poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// `future`, bounded: a test that would hang fails instead.
async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("timed out")
}

#[tokio::test]
async fn a_held_reread_ends_when_the_station_stops() {
    let fake = fake_with_peers();
    let h = held(&fake);
    let c = Client::new(h.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1);
    h.logs.hold_next();
    let logs = h.logs.received();
    {
        let mut next = pin!(tokio::task::unconstrained(f.next()));
        assert!(poll_now(next.as_mut()).is_none());
        assert_eq!(h.logs.received(), logs + 1, "the page call is held");
        // The same pending `next()` notices the end, although its page call never completes.
        let stop = async {
            tokio::task::yield_now().await;
            fake.stop();
        };
        let (ended, ()) = tokio::join!(within(next.as_mut()), stop);
        assert_eq!(ended, Ok(None));
    }
    assert_eq!(f.next().await, Ok(None));
}

/// Calls through a [`Held`]; events from a subscription whose producer the test holds.
#[derive(Clone)]
struct OwnEvents {
    calls: Held,
    sink: Arc<std::sync::Mutex<Option<EventSink>>>,
}

impl Transport for OwnEvents {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        self.calls.call(request).await
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        let (sink, events) = subscription(SUBSCRIPTION_CAPACITY);
        *self.sink.lock().unwrap() = Some(sink);
        Ok(events)
    }
}

#[tokio::test]
async fn a_held_reread_ends_when_the_event_producer_is_dropped() {
    let fake = fake_with_peers();
    receive_n(&fake, "bob", 2);
    let own = OwnEvents {
        calls: held(&fake),
        sink: Arc::default(),
    };
    let c = Client::new(own.clone());
    let mut f = c.follow_log(None, 10).await.unwrap();
    assert_eq!(ready(&mut f), added([1, 2]));
    let sink = own.sink.lock().unwrap().take().unwrap();
    own.calls.logs.hold_next();
    let logs = own.calls.logs.received();
    sink.push(Event::Resync).unwrap();
    {
        let mut next = pin!(tokio::task::unconstrained(f.next()));
        assert!(poll_now(next.as_mut()).is_none());
        assert_eq!(own.calls.logs.received(), logs + 1, "the page call is held");
        let drop_producer = async {
            tokio::task::yield_now().await;
            drop(sink);
        };
        let (ended, ()) = tokio::join!(within(next.as_mut()), drop_producer);
        assert_eq!(ended, Ok(None));
    }
    assert_eq!(f.next().await, Ok(None));
}

#[tokio::test]
async fn a_second_resync_during_a_held_page_starts_the_reread_again() {
    let fake = big_log_after_attach();
    fake.set_outcome(SendOutcome::Hold);
    let h = held(&fake);
    let c = Client::new(h.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    // A gap: 300 messages of 16 KiB (about ten pages), then a send still `Sending`, seq 301.
    overflow_with_big_messages(&fake);
    assert_eq!(c.send("bob", "held").await.unwrap().seq, 301);
    let (logs, statuses) = (h.logs.received(), h.statuses.received());
    // The re-read's first page (with 301 at rev 0) passes; its second page is held.
    h.logs.hold_after(1);
    {
        let mut next = pin!(tokio::task::unconstrained(f.next()));
        assert!(poll_now(next.as_mut()).is_none());
        assert_eq!(h.logs.received(), logs + 2);
        // A second gap while the page is held: 301 is delivered (rev 1), and its event is lost
        // with 257 more.
        fake.resolve(301, MessageStatus::Delivered).unwrap();
        receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1);
        assert!(poll_now(next.as_mut()).is_none());
        // The re-read started again at once: a new instance check, and its first page is held.
        assert_eq!(h.statuses.received(), statuses + 2);
        assert_eq!(h.logs.received(), logs + 3);
        h.logs.release();
        let first = within(next.as_mut()).await.unwrap().unwrap();
        assert_eq!((first.message().seq, first.message().rev), (1, 0));
    }
    // What the log holds after the second gap, each once: 301 only at rev 1, never the stale rev 0
    // of the first page.
    let mut expected = added(2..=300);
    expected.push((true, 301, 1));
    expected.extend(added(302..=301 + SUBSCRIPTION_CAPACITY as u64 + 1));
    assert_eq!(ready(&mut f), expected);
}

#[tokio::test]
async fn live_messages_wait_behind_a_cancelled_reread() {
    let fake = fake_with_peers();
    let h = held(&fake);
    let c = Client::new(h.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1); // 1..=257, lost in a gap
    h.logs.hold_next();
    let logs = h.logs.received();
    // `next()` is cancelled while its page call is held; a live message arrives; `next()` again.
    assert!(poll_once(f.next()).is_none());
    fake.receive("bob", "live").unwrap(); // 258
    assert!(
        poll_once(f.next()).is_none(),
        "a live message overtook the re-read"
    );
    // Only the page call that was in flight was made again.
    assert_eq!(h.logs.received(), logs + 2);
    h.logs.release();
    assert_eq!(ready(&mut f), added(1..=258));
    fake.receive("bob", "after").unwrap();
    assert_eq!(ready(&mut f), added([259]));
}

#[tokio::test]
async fn a_stream_of_live_messages_does_not_make_the_held_call_again() {
    let fake = fake_with_peers();
    let h = held(&fake);
    let c = Client::new(h.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1); // 1..=257, lost in a gap
    h.logs.hold_next();
    let (logs, statuses) = (h.logs.received(), h.statuses.received());
    {
        let mut next = pin!(tokio::task::unconstrained(f.next()));
        assert!(poll_now(next.as_mut()).is_none());
        // More live messages than the subscription holds, in rounds it can hold: the same
        // `next()` takes each round while its one page call stays pending.
        for _ in 0..4 {
            receive_n(&fake, "bob", 100);
            assert!(poll_now(next.as_mut()).is_none());
        }
        assert_eq!(h.logs.received(), logs + 1);
        h.logs.release();
        let first = within(next.as_mut()).await.unwrap().unwrap();
        assert_eq!((first.message().seq, first.message().rev), (1, 0));
    }
    assert_eq!(ready(&mut f), added(2..=657));
    // One re-read with one page: the subscription never overflowed.
    assert_eq!(
        (h.logs.received(), h.statuses.received()),
        (logs + 1, statuses + 2)
    );
}

#[tokio::test]
async fn held_messages_overflowing_start_the_reread_again() {
    let fake = fake_with_peers().with_log_capacity(FOLLOW_MEMORY);
    let h = held(&fake);
    let c = Client::new(h.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1); // lost in a gap
    h.logs.hold_next();
    let (logs, statuses) = (h.logs.received(), h.statuses.received());
    let mut yielded = Vec::new();
    {
        let mut next = pin!(tokio::task::unconstrained(f.next()));
        assert!(poll_now(next.as_mut()).is_none());
        // More than FOLLOW_MEMORY live messages while the page call is held, in rounds the
        // subscription can hold.
        for _ in 0..FOLLOW_MEMORY / 200 + 1 {
            receive_n(&fake, "bob", 200);
            assert!(poll_now(next.as_mut()).is_none());
        }
        // The message that found the held ones full started the re-read again: a new instance
        // check, and a new page call, held too.
        assert_eq!(
            (h.logs.received(), h.statuses.received()),
            (logs + 2, statuses + 2)
        );
        h.logs.release();
        let first = within(next.as_mut()).await.unwrap().unwrap();
        let m = first.message();
        yielded.push((matches!(first, LogUpdate::Added(_)), m.seq, m.rev));
    }
    yielded.extend(ready(&mut f));
    // Every message the log still holds, once, oldest first.
    let log: Vec<(bool, u64, u32)> = fake.messages().iter().map(|m| (true, m.seq, 0)).collect();
    assert_eq!(log.len(), FOLLOW_MEMORY);
    assert_eq!(yielded, log);
}

#[tokio::test]
async fn a_message_held_during_a_reread_at_the_pages_rev_is_yielded_once() {
    let fake = fake_with_peers();
    fake.set_outcome(SendOutcome::Hold);
    let h = held(&fake);
    let c = Client::new(h.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    assert_eq!(c.send("bob", "held").await.unwrap().seq, 1);
    assert_eq!(ready(&mut f), added([1]));
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1); // 2..=258, lost in a gap
    h.logs.hold_next();
    assert!(poll_once(f.next()).is_none());
    // While the page call is held, 1 is delivered (rev 1) and 259 arrives; the page, read after
    // both, holds them at the same revs.
    fake.resolve(1, MessageStatus::Delivered).unwrap();
    fake.receive("bob", "live").unwrap();
    assert!(
        poll_once(f.next()).is_none(),
        "a live message overtook the re-read"
    );
    h.logs.release();
    let mut expected = vec![(false, 1, 1)];
    expected.extend(added(2..=259));
    assert_eq!(ready(&mut f), expected);
}

#[tokio::test]
async fn a_message_held_during_a_reread_at_a_higher_rev_than_the_page_comes_after_it() {
    let fake = fake_with_peers();
    fake.set_outcome(SendOutcome::Hold);
    let h = held(&fake);
    let c = Client::new(h.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    receive_n(&fake, "bob", SUBSCRIPTION_CAPACITY + 1); // 1..=257, lost in a gap
    assert_eq!(c.send("bob", "held").await.unwrap().seq, 258); // lost too
    let (logs, statuses) = (h.logs.received(), h.statuses.received());
    // The instance check and the page (258 at rev 0) pass; the final check is held.
    h.statuses.hold_after(1);
    assert!(poll_once(f.next()).is_none());
    assert_eq!(
        (h.logs.received(), h.statuses.received()),
        (logs + 1, statuses + 2)
    );
    // 258 is delivered (rev 1) while the final check is pending, and `next()` is called again.
    fake.resolve(258, MessageStatus::Delivered).unwrap();
    assert!(
        poll_once(f.next()).is_none(),
        "a live message overtook the re-read"
    );
    // Only the final check, which was in flight, was made again.
    assert_eq!(
        (h.logs.received(), h.statuses.received()),
        (logs + 1, statuses + 3)
    );
    h.statuses.release();
    let mut expected = added(1..=258);
    expected.push((false, 258, 1));
    assert_eq!(ready(&mut f), expected);
}

// An end or a gap that a call's own completion makes ready wins over its answer.

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Status,
    Log,
}

type Act = Box<dyn FnOnce(&FakeStation) + Send>;

/// One planned interleaving: on the `nth` call of `kind`, take the fake's ordinary answer, then
/// `act`, then return the answer (or, with `fail`, an error in its place).
struct Plan {
    kind: Kind,
    nth: usize,
    act: Act,
    fail: bool,
}

/// Calls to a fake, counted by kind; a planned call does something after the fake answered it and
/// before it returns.
#[derive(Clone)]
struct AfterAnswer {
    fake: FakeStation,
    statuses: Arc<AtomicUsize>,
    logs: Arc<AtomicUsize>,
    plan: Arc<std::sync::Mutex<Option<Plan>>>,
}

impl AfterAnswer {
    fn new(fake: &FakeStation) -> Self {
        Self {
            fake: fake.clone(),
            statuses: Arc::default(),
            logs: Arc::default(),
            plan: Arc::default(),
        }
    }

    /// Plans `act` for the `n`-th call of `kind` from now on (counting from 1).
    fn after(
        &self,
        kind: Kind,
        n: usize,
        fail: bool,
        act: impl FnOnce(&FakeStation) + Send + 'static,
    ) {
        let made = match kind {
            Kind::Status => self.statuses.load(Ordering::SeqCst),
            Kind::Log => self.logs.load(Ordering::SeqCst),
        };
        *self.plan.lock().unwrap() = Some(Plan {
            kind,
            nth: made + n,
            act: Box::new(act),
            fail,
        });
    }
}

impl Transport for AfterAnswer {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        let (kind, counter) = match request {
            Request::Status => (Kind::Status, &self.statuses),
            Request::Log { .. } => (Kind::Log, &self.logs),
            _ => return self.fake.call(request).await,
        };
        let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
        let plan = {
            let mut slot = self.plan.lock().unwrap();
            match slot.as_ref() {
                Some(p) if p.kind == kind && p.nth == n => slot.take(),
                _ => None,
            }
        };
        let answer = self.fake.call(request).await;
        let Some(plan) = plan else {
            return answer;
        };
        assert!(answer.is_ok(), "the fake answered normally first");
        (plan.act)(&self.fake);
        if plan.fail {
            return Err(ApiError::Disconnected {
                detail: "test".into(),
            });
        }
        answer
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        self.fake.subscribe().await
    }
}

/// A follower with nothing shown, then a gap: seq 1 is a send still `Sending`, then 257
/// messages (2..=258), all lost.
async fn a_gap_with_a_pending_send(fake: &FakeStation, c: &Client<AfterAnswer>) {
    fake.set_outcome(SendOutcome::Hold);
    assert_eq!(c.send("bob", "held").await.unwrap().seq, 1);
    receive_n(fake, "bob", SUBSCRIPTION_CAPACITY + 1);
}

/// Delivers seq 1 (rev 1) and overflows the subscription again: a second gap.
fn deliver_and_overflow(fake: &FakeStation) {
    fake.resolve(1, MessageStatus::Delivered).unwrap();
    receive_n(fake, "bob", SUBSCRIPTION_CAPACITY + 1);
}

#[tokio::test]
async fn a_stop_as_the_final_check_answers_ends_the_follower() {
    let fake = fake_with_peers();
    let a = AfterAnswer::new(&fake);
    let c = Client::new(a.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    a_gap_with_a_pending_send(&fake, &c).await;
    // The instance check, then the page, then the final check: it answers, and the station stops
    // before the answer is returned. The page must not be published.
    a.after(Kind::Status, 2, false, FakeStation::stop);
    assert_eq!(within(f.next()).await, Ok(None));
    assert_eq!(f.next().await, Ok(None));
}

#[tokio::test]
async fn a_stop_as_the_final_check_fails_ends_the_follower_without_the_error() {
    let fake = fake_with_peers();
    let a = AfterAnswer::new(&fake);
    let c = Client::new(a.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    a_gap_with_a_pending_send(&fake, &c).await;
    // The end, ready when the call fails, wins over the error.
    a.after(Kind::Status, 2, true, FakeStation::stop);
    assert_eq!(within(f.next()).await, Ok(None));
    assert_eq!(f.next().await, Ok(None));
}

#[tokio::test]
async fn a_gap_as_the_final_check_answers_starts_the_reread_again() {
    let fake = fake_with_peers();
    let a = AfterAnswer::new(&fake);
    let c = Client::new(a.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    a_gap_with_a_pending_send(&fake, &c).await;
    // The page holds seq 1 at rev 0; as the final check answers, seq 1 is delivered (rev 1) and a
    // second gap hides its event.
    let statuses = a.statuses.load(Ordering::SeqCst);
    a.after(Kind::Status, 2, false, deliver_and_overflow);
    let first = within(f.next()).await.unwrap().unwrap();
    assert_eq!(first, LogUpdate::Added(fake.messages()[0].clone()));
    assert_eq!((first.message().seq, first.message().rev), (1, 1));
    // The re-read started again: two more checks.
    assert_eq!(a.statuses.load(Ordering::SeqCst), statuses + 4);
    assert_eq!(
        ready(&mut f),
        added(2..=1 + 2 * (SUBSCRIPTION_CAPACITY as u64 + 1))
    );
}

#[tokio::test]
async fn a_stop_as_a_page_answers_ends_the_follower() {
    let fake = fake_with_peers();
    let a = AfterAnswer::new(&fake);
    let c = Client::new(a.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    a_gap_with_a_pending_send(&fake, &c).await;
    a.after(Kind::Log, 1, false, FakeStation::stop);
    // The end wins at once, before the stopped station's final check could fail.
    assert_eq!(within(f.next()).await, Ok(None));
    assert_eq!(f.next().await, Ok(None));
}

#[tokio::test]
async fn a_gap_as_a_page_answers_starts_the_reread_again() {
    let fake = fake_with_peers();
    let a = AfterAnswer::new(&fake);
    let c = Client::new(a.clone());
    let mut f = c.follow_log(None, 0).await.unwrap();
    a_gap_with_a_pending_send(&fake, &c).await;
    let statuses = a.statuses.load(Ordering::SeqCst);
    a.after(Kind::Log, 1, false, deliver_and_overflow);
    let first = within(f.next()).await.unwrap().unwrap();
    assert_eq!((first.message().seq, first.message().rev), (1, 1));
    // The stale page was dropped at once: one check before it, then the restarted re-read's two.
    assert_eq!(a.statuses.load(Ordering::SeqCst), statuses + 3);
    assert_eq!(
        ready(&mut f),
        added(2..=1 + 2 * (SUBSCRIPTION_CAPACITY as u64 + 1))
    );
}
