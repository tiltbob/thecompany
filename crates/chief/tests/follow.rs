#![cfg(test)]
//! `Client::follow_log` against the fake station: backlog then live with no duplicate, changes,
//! `Resync`, a peer filter, cancellation and the end of the subscription. Deterministic: messages
//! "between the subscribe and the page" are made by the fake's call hook. Nothing sleeps.

use chief::api::*;
use chief::client::{Client, SUBSCRIPTION_CAPACITY, Subscription, Transport};
use chief::fake::{FakeStation, SendOutcome};
use chief::follow::{FOLLOW_MEMORY, LogFollower, LogUpdate};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

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
    // At most one error first, then the end, for good.
    let first = f.next().await;
    if first.is_err() {
        assert_eq!(f.next().await, Ok(None));
    } else {
        assert_eq!(first, Ok(None));
    }
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
