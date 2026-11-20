#![cfg(test)]
//! `chief::fake::FakeStation` against the rules of `chief::api` it implements: it is the reference
//! that transport, server and frontend tests trust. Nothing sleeps or touches the network.

use chief::api::*;
use chief::client::{Client, SUBSCRIPTION_CAPACITY, Subscription};
use chief::fake::*;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

const ME: &str = "STATION-A";

fn station() -> FakeStation {
    FakeStation::new(ME, "INSTANCE-1")
}

fn client(fake: &FakeStation) -> Client<FakeStation> {
    Client::new(fake.clone())
}

/// A fake with peers `alice` (STATION-AL) and `bob` (STATION-B).
async fn with_peers() -> (FakeStation, Client<FakeStation>) {
    let fake = station();
    let c = client(&fake);
    c.peer_add("alice", "STATION-AL").await.unwrap();
    c.peer_add("bob", "STATION-B").await.unwrap();
    (fake, c)
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

/// Every event queued on `events` right now.
fn queued(events: &mut Subscription) -> Vec<Event> {
    let mut out = Vec::new();
    while let Some(Some(event)) = poll_once(events.next()) {
        out.push(event);
    }
    out
}

fn message_events(events: &[Event]) -> Vec<(u64, u32, MessageStatus)> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Message { message } => Some((message.seq, message.rev, message.status.clone())),
            _ => None,
        })
        .collect()
}

fn invalid_field(e: ApiError) -> String {
    match e {
        ApiError::Invalid { field, .. } => field,
        other => panic!("expected Invalid, got {other:?}"),
    }
}

// Status.

#[tokio::test]
async fn status_has_the_ids_and_one_tower() {
    let fake = station();
    let status = client(&fake).status().await.unwrap();
    assert_eq!(status.station_id, ME);
    assert_eq!(status.instance_id, "INSTANCE-1");
    assert_eq!(status.bootstrap_domain, None);
    assert_eq!(
        status.towers,
        vec![TowerStatus {
            url: FAKE_TOWER_URL.into(),
            state: TowerState::Connecting,
            source: None,
        }]
    );
}

#[tokio::test]
async fn a_tower_change_pushes_the_whole_status() {
    let fake = station();
    let c = client(&fake);
    let mut events = c.subscribe().await.unwrap();
    fake.set_tower_state(TowerState::Connected);
    let status = c.status().await.unwrap();
    assert_eq!(status.towers[0].state, TowerState::Connected);
    assert_eq!(queued(&mut events), vec![Event::Status { status }]);
}

#[tokio::test]
async fn standby_passes_through_the_fake() {
    let fake = station();
    let c = client(&fake);
    let mut events = c.subscribe().await.unwrap();
    fake.set_tower_state(TowerState::Standby);
    let status = c.status().await.unwrap();
    assert_eq!(status.towers[0].state, TowerState::Standby);
    assert_eq!(queued(&mut events), vec![Event::Status { status }]);
}

fn tower(url: &str, state: TowerState, source: Option<TowerSource>) -> TowerStatus {
    TowerStatus {
        url: url.to_owned(),
        state,
        source,
    }
}

/// Three towers with every kind of text rule 9 covers.
fn three_towers() -> Vec<TowerStatus> {
    vec![
        tower(
            "https://tower-c.example.invalid/",
            TowerState::Failing {
                error: "no\nroute\u{1b}[31m".into(),
            },
            Some(TowerSource::Configured),
        ),
        tower(
            "https://tower-a.example.invalid/\u{202E}x\u{2028}",
            TowerState::Connected,
            None,
        ),
        tower(
            "https://tower-b.example.invalid/",
            TowerState::Denied {
                reason: "not\r\nallowed\u{7f}".into(),
            },
            Some(TowerSource::Srv),
        ),
    ]
}

/// [`three_towers`] as the fake stores them.
fn three_towers_clean() -> Vec<TowerStatus> {
    vec![
        tower(
            "https://tower-c.example.invalid/",
            TowerState::Failing {
                error: "no route [31m".into(),
            },
            Some(TowerSource::Configured),
        ),
        tower(
            "https://tower-a.example.invalid/ x ",
            TowerState::Connected,
            None,
        ),
        tower(
            "https://tower-b.example.invalid/",
            TowerState::Denied {
                reason: "not  allowed ".into(),
            },
            Some(TowerSource::Srv),
        ),
    ]
}

#[tokio::test]
async fn set_towers_replaces_the_list_in_order_cleans_it_and_pushes_one_status() {
    let fake = station();
    let c = client(&fake);
    let mut events = c.subscribe().await.unwrap();
    fake.set_towers(three_towers());
    let status = c.status().await.unwrap();
    assert_eq!(status.towers, three_towers_clean());
    assert_eq!(fake.status(), status);
    assert_eq!(queued(&mut events), vec![Event::Status { status }]);
    // An empty list is allowed, and pushed too.
    fake.set_towers(Vec::new());
    let empty = c.status().await.unwrap();
    assert!(empty.towers.is_empty());
    assert_eq!(empty.station_id, ME);
    assert_eq!(queued(&mut events), vec![Event::Status { status: empty }]);
}

#[tokio::test]
async fn set_bootstrap_domain_sets_clears_and_pushes() {
    let fake = station();
    let c = client(&fake);
    let mut events = c.subscribe().await.unwrap();
    fake.set_bootstrap_domain(Some("team.example.invalid".into()));
    let set = c.status().await.unwrap();
    assert_eq!(
        set.bootstrap_domain.as_deref(),
        Some("team.example.invalid")
    );
    // The towers are left alone.
    assert_eq!(set.towers, station().status().towers);
    fake.set_bootstrap_domain(None);
    let cleared = c.status().await.unwrap();
    assert_eq!(cleared.bootstrap_domain, None);
    assert_eq!(
        queued(&mut events),
        vec![
            Event::Status { status: set },
            Event::Status { status: cleared }
        ]
    );
}

#[tokio::test]
async fn set_tower_state_sets_every_tower_of_the_list() {
    let fake = station();
    let c = client(&fake);
    fake.set_towers(three_towers());
    let mut events = c.subscribe().await.unwrap();
    fake.set_tower_state(TowerState::Standby);
    let status = c.status().await.unwrap();
    let expected: Vec<TowerStatus> = three_towers_clean()
        .into_iter()
        .map(|t| TowerStatus {
            state: TowerState::Standby,
            ..t
        })
        .collect();
    assert_eq!(status.towers, expected);
    assert_eq!(queued(&mut events), vec![Event::Status { status }]);
}

#[tokio::test]
async fn restart_keeps_the_towers_their_sources_and_the_domain() {
    let fake = station();
    let c = client(&fake);
    fake.set_towers(three_towers());
    fake.set_bootstrap_domain(Some("team.example.invalid".into()));
    fake.restart("INSTANCE-2");
    let status = c.status().await.unwrap();
    assert_eq!(status.instance_id, "INSTANCE-2");
    assert_eq!(
        status.bootstrap_domain.as_deref(),
        Some("team.example.invalid")
    );
    // The first tower is the fake's home tower; every other one is not observed.
    let expected: Vec<TowerStatus> = three_towers_clean()
        .into_iter()
        .enumerate()
        .map(|(i, t)| TowerStatus {
            state: if i == 0 {
                TowerState::Connecting
            } else {
                TowerState::Standby
            },
            ..t
        })
        .collect();
    assert_eq!(status.towers, expected);
}

#[tokio::test]
async fn set_bootstrap_domain_stores_the_domain_as_given() {
    let fake = station();
    fake.set_bootstrap_domain(Some("team.example.invalid\u{1b}".into()));
    assert_eq!(
        fake.status().bootstrap_domain.as_deref(),
        Some("team.example.invalid\u{1b}")
    );
}

#[tokio::test]
async fn set_towers_leaves_the_bootstrap_domain_alone() {
    let fake = station();
    fake.set_bootstrap_domain(Some("team.example.invalid".into()));
    fake.set_towers(three_towers());
    assert_eq!(
        fake.status().bootstrap_domain.as_deref(),
        Some("team.example.invalid")
    );
    fake.set_towers(Vec::new());
    assert_eq!(
        fake.status().bootstrap_domain.as_deref(),
        Some("team.example.invalid")
    );
}

// Peer book (rule 10).

#[tokio::test]
async fn peer_add_creates_appends_and_pushes() {
    let fake = station();
    let c = client(&fake);
    let mut events = c.subscribe().await.unwrap();
    let first = c.peer_add("bob", "STATION-B").await.unwrap();
    assert_eq!(first.stations.len(), 1);
    assert_eq!(first.stations[0].link, Link::Down);
    let second = c.peer_add("bob", "STATION-B2").await.unwrap();
    assert_eq!(
        second
            .stations
            .iter()
            .map(|s| s.station_id.as_str())
            .collect::<Vec<_>>(),
        vec!["STATION-B", "STATION-B2"]
    );
    assert_eq!(
        queued(&mut events),
        vec![
            Event::Peer { peer: first },
            Event::Peer {
                peer: second.clone()
            }
        ]
    );
    // Adding a station the peer has changes nothing and pushes nothing.
    assert_eq!(c.peer_add("bob", "STATION-B").await.unwrap(), second);
    assert!(queued(&mut events).is_empty());
}

#[tokio::test]
async fn peer_add_refuses_bad_names_own_id_and_taken_stations() {
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    assert_eq!(
        invalid_field(c.peer_add("-bob", "STATION-X").await.unwrap_err()),
        "name"
    );
    assert_eq!(
        invalid_field(c.peer_add("carol", ME).await.unwrap_err()),
        "station_id"
    );
    assert_eq!(
        invalid_field(c.peer_add("carol", "").await.unwrap_err()),
        "station_id"
    );
    assert_eq!(
        c.peer_add("carol", "STATION-B").await.unwrap_err(),
        ApiError::StationTaken {
            station_id: "STATION-B".into(),
            peer: "bob".into()
        }
    );
    assert!(queued(&mut events).is_empty());
    assert_eq!(c.peers().await.unwrap().len(), 2);
    drop(fake);
}

#[tokio::test]
async fn peers_are_sorted_by_name() {
    let fake = station();
    let c = client(&fake);
    for (name, id) in [
        ("zed", "S-Z"),
        ("Bob", "S-BB"),
        ("alice", "S-A"),
        ("bob", "S-B"),
    ] {
        c.peer_add(name, id).await.unwrap();
    }
    let names: Vec<String> = c
        .peers()
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect();
    assert_eq!(names, vec!["Bob", "alice", "bob", "zed"]);
}

#[tokio::test]
async fn set_link_pushes_the_whole_peer_on_a_change_only() {
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    let peer = fake.set_link("bob", "STATION-B", Link::Direct).unwrap();
    assert_eq!(peer.stations[0].link, Link::Direct);
    fake.set_link("bob", "STATION-B", Link::Direct).unwrap();
    assert_eq!(queued(&mut events), vec![Event::Peer { peer }]);
    assert!(matches!(
        fake.set_link("carol", "STATION-C", Link::Down),
        Err(ApiError::PeerNotFound { .. })
    ));
}

#[tokio::test]
async fn peer_remove_fails_pending_sends_first_and_keeps_the_messages() {
    let (fake, c) = with_peers().await;
    fake.set_outcome(SendOutcome::Hold);
    let held = c.send("bob", "one").await.unwrap();
    fake.set_outcome(SendOutcome::Deliver);
    let delivered = c.send("bob", "two").await.unwrap();
    fake.set_link("bob", "STATION-B", Link::Direct).unwrap();
    let mut events = c.subscribe().await.unwrap();
    // Its link is closed too, and pushes no `Event::Peer` (rule 10).
    c.peer_remove("bob").await.unwrap();
    let failed = Message {
        rev: 1,
        status: MessageStatus::Failed {
            reason: FailReason::PeerRemoved,
        },
        ..held.clone()
    };
    assert_eq!(
        queued(&mut events),
        vec![
            Event::Message {
                message: failed.clone()
            },
            Event::PeerRemoved { name: "bob".into() }
        ]
    );
    assert_eq!(c.message(held.seq).await.unwrap(), failed);
    assert_eq!(c.message(delivered.seq).await.unwrap().rev, 1);
    assert_eq!(
        c.peer_remove("bob").await.unwrap_err(),
        ApiError::PeerNotFound { name: "bob".into() }
    );
    assert!(c.peers().await.unwrap().iter().all(|p| p.name != "bob"));
    // A removed peer's messages stay in the log.
    assert_eq!(
        c.log(Some("bob"), None, 10).await.unwrap().messages.len(),
        2
    );
}

// Sends (rule 7).

#[tokio::test]
async fn send_validates_peer_and_body() {
    let (_fake, c) = with_peers().await;
    assert_eq!(
        c.send("carol", "hi").await.unwrap_err(),
        ApiError::PeerNotFound {
            name: "carol".into()
        }
    );
    assert_eq!(invalid_field(c.send("bob", "").await.unwrap_err()), "body");
    let too_long = "x".repeat(BODY_MAX_BYTES + 1);
    assert_eq!(
        invalid_field(c.send("bob", &too_long).await.unwrap_err()),
        "body"
    );
    let longest = "x".repeat(BODY_MAX_BYTES);
    assert_eq!(c.send("bob", &longest).await.unwrap().body, longest);
}

#[tokio::test]
async fn a_delivered_send_answers_sending_and_queues_both_events_first() {
    let (_fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    let sent = c.send("bob", "on my way").await.unwrap();
    assert_eq!(sent.status, MessageStatus::Sending);
    assert_eq!(sent.rev, 0);
    assert_eq!(sent.direction, Direction::Outgoing);
    assert_eq!(sent.peer, "bob");
    assert!(sent.at_ms > 1_700_000_000_000);
    // Both events were queued before the answer arrived (rule 4).
    assert_eq!(
        message_events(&queued(&mut events)),
        vec![
            (sent.seq, 0, MessageStatus::Sending),
            (sent.seq, 1, MessageStatus::Delivered)
        ]
    );
}

#[tokio::test]
async fn a_failing_outcome_gives_failed_with_its_reason() {
    let (fake, c) = with_peers().await;
    for reason in [FailReason::NoAnswer, FailReason::Refused] {
        fake.set_outcome(SendOutcome::Fail(reason));
        let sent = c.send("bob", "hi").await.unwrap();
        let now = c.message(sent.seq).await.unwrap();
        assert_eq!(now.rev, 1);
        assert_eq!(now.status, MessageStatus::Failed { reason });
    }
}

#[tokio::test]
async fn a_held_send_waits_for_resolve_and_a_final_status_never_changes() {
    let (fake, c) = with_peers().await;
    fake.set_outcome(SendOutcome::Hold);
    let sent = c.send("bob", "hi").await.unwrap();
    let mut events = c.subscribe().await.unwrap();
    assert_eq!(c.message(sent.seq).await.unwrap(), sent);
    let done = fake.resolve(sent.seq, MessageStatus::Delivered).unwrap();
    assert_eq!(
        (done.rev, done.status.clone()),
        (1, MessageStatus::Delivered)
    );
    assert_eq!(
        queued(&mut events),
        vec![Event::Message {
            message: done.clone()
        }]
    );
    assert_eq!(
        invalid_field(
            fake.resolve(sent.seq, MessageStatus::Delivered)
                .unwrap_err()
        ),
        "seq"
    );
    assert_eq!(
        invalid_field(fake.resolve(sent.seq, MessageStatus::Sending).unwrap_err()),
        "status"
    );
    assert_eq!(
        fake.resolve(999, MessageStatus::Delivered).unwrap_err(),
        ApiError::MessageNotFound { seq: 999 }
    );
    assert_eq!(c.message(sent.seq).await.unwrap(), done);
}

#[tokio::test]
async fn resolve_after_stop_is_refused_and_changes_nothing() {
    let (fake, c) = with_peers().await;
    fake.set_outcome(SendOutcome::Hold);
    let sent = c.send("bob", "hi").await.unwrap();
    fake.stop();
    // Rule 7 promises no outcome after a stop. The check comes first: a status that is not final
    // is refused with `Stopping` too.
    assert_eq!(
        fake.resolve(sent.seq, MessageStatus::Delivered),
        Err(ApiError::Stopping)
    );
    assert_eq!(
        fake.resolve(sent.seq, MessageStatus::Sending),
        Err(ApiError::Stopping)
    );
    // The message is unchanged.
    assert_eq!(fake.messages(), vec![sent]);
}

// Incoming messages (rules 8 and 9).

#[tokio::test]
async fn receive_logs_replaces_control_characters_and_drops_duplicates() {
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    let got = fake
        .receive_with_id(
            "bob",
            "ID-1",
            "where\u{1b}[31m are\u{7} you?\r\n\tok\u{7f}\u{85}!",
        )
        .unwrap()
        .unwrap();
    // Replaced with a space, not removed (rule 9): C0, DEL and C1, newline and tab included.
    assert_eq!(got.body, "where [31m are  you?   ok  !");
    assert_eq!(got.direction, Direction::Incoming);
    assert_eq!(got.status, MessageStatus::Received);
    assert_eq!((got.seq, got.rev), (1, 0));
    assert_eq!(fake.receive_with_id("bob", "ID-1", "again").unwrap(), None);
    // The same id from another peer is another message.
    assert!(
        fake.receive_with_id("alice", "ID-1", "hi")
            .unwrap()
            .is_some()
    );
    assert_eq!(message_events(&queued(&mut events)).len(), 2);
    assert_eq!(
        fake.receive("carol", "hi").unwrap_err(),
        ApiError::PeerNotFound {
            name: "carol".into()
        }
    );
}

#[tokio::test]
async fn seqs_start_at_one_and_grow_across_directions() {
    let (fake, c) = with_peers().await;
    let a = fake.receive("bob", "1").unwrap();
    let b = c.send("bob", "2").await.unwrap();
    let d = fake.receive("alice", "3").unwrap();
    assert_eq!((a.seq, b.seq, d.seq), (1, 2, 3));
    assert_ne!(a.id, b.id);
}

#[tokio::test]
async fn message_finds_a_seq_and_refuses_an_evicted_or_unknown_one() {
    let (fake, c) = with_peers().await;
    let fake = fake.with_log_capacity(3);
    for i in 1..=5 {
        fake.receive("bob", &i.to_string()).unwrap();
    }
    assert_eq!(
        fake.messages().iter().map(|m| m.seq).collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
    assert_eq!(c.message(4).await.unwrap().body, "4");
    for seq in [1, 2, 6] {
        assert_eq!(
            c.message(seq).await.unwrap_err(),
            ApiError::MessageNotFound { seq }
        );
    }
}

// Log paging (rule 6).

fn seqs(page: &LogPage) -> Vec<u64> {
    page.messages.iter().map(|m| m.seq).collect()
}

#[tokio::test]
async fn log_pages_newest_first_in_ascending_order_with_a_cursor() {
    let (fake, c) = with_peers().await;
    for i in 1..=7 {
        fake.receive(if i % 2 == 0 { "alice" } else { "bob" }, "x")
            .unwrap();
    }
    let page = c.log(None, None, 3).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![5, 6, 7], Some(5)));
    let page = c.log(None, Some(5), 3).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![2, 3, 4], Some(2)));
    let page = c.log(None, Some(2), 3).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![1], None));
    // Exactly reaching the oldest message also ends the cursor.
    let page = c.log(None, None, 7).await.unwrap();
    assert_eq!(page.next_before, None);
    // A peer filter.
    let page = c.log(Some("bob"), None, 2).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![5, 7], Some(5)));
    let page = c.log(Some("bob"), Some(5), 10).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![1, 3], None));
    // A name nobody has, and an empty log below the cursor.
    let page = c.log(Some("carol"), None, 10).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![], None));
    let page = c.log(None, Some(1), 10).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![], None));
}

#[tokio::test]
async fn log_limit_zero_gives_an_empty_page_with_a_cursor_that_makes_progress() {
    let (fake, c) = with_peers().await;
    let empty = c.log(None, None, 0).await.unwrap();
    assert_eq!((seqs(&empty), empty.next_before), (vec![], None));
    for _ in 0..4 {
        fake.receive("bob", "x").unwrap();
    }
    fake.receive("alice", "x").unwrap();
    let page = c.log(None, None, 0).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![], Some(6)));
    let page = c.log(Some("bob"), None, 0).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![], Some(5)));
    let page = c.log(None, Some(3), 0).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![], Some(3)));
    let page = c.log(None, Some(1), 0).await.unwrap();
    assert_eq!((seqs(&page), page.next_before), (vec![], None));
    // Asking again with the cursor and a higher limit makes progress.
    let page = c.log(None, Some(6), 2).await.unwrap();
    assert_eq!(seqs(&page), vec![4, 5]);
}

#[tokio::test]
async fn log_never_returns_more_than_the_page_maximum() {
    let (fake, c) = with_peers().await;
    let fake = fake.with_log_capacity(1_500);
    for _ in 0..1_200 {
        fake.receive("bob", "x").unwrap();
    }
    let page = c.log(None, None, u32::MAX).await.unwrap();
    assert_eq!(page.messages.len(), LOG_PAGE_MAX_MESSAGES as usize);
    assert_eq!(page.next_before, Some(201));
    assert_eq!(page.messages[0].seq, 201);
}

#[tokio::test]
async fn log_stops_before_the_byte_budget_and_holds_at_least_one() {
    let (fake, c) = with_peers().await;
    let body = "x".repeat(BODY_MAX_BYTES);
    for _ in 0..40 {
        fake.receive("bob", &body).unwrap();
    }
    let page = c.log(None, None, 100).await.unwrap();
    let size = |m: &Message| serde_json::to_vec(m).unwrap().len();
    let total: usize = page.messages.iter().map(size).sum();
    assert!(total <= LOG_PAGE_MAX_BYTES);
    assert!(page.messages.len() < 40);
    // The next older message would have pushed it over.
    let next_older = c.message(page.messages[0].seq - 1).await.unwrap();
    assert!(total + size(&next_older) > LOG_PAGE_MAX_BYTES);
    assert_eq!(page.next_before, Some(page.messages[0].seq));
    assert_eq!(page.messages.last().unwrap().seq, 40);
    // Paging on reaches every message exactly once.
    let mut all = seqs(&page);
    let mut before = page.next_before;
    while let Some(b) = before {
        let page = c.log(None, Some(b), 100).await.unwrap();
        assert!(!page.messages.is_empty());
        all.splice(0..0, seqs(&page));
        before = page.next_before;
    }
    assert_eq!(all, (1..=40).collect::<Vec<_>>());
}

// Subscriptions and stopping (rules 3, 4, 11).

#[tokio::test]
async fn subscriptions_get_events_in_order_and_dropped_ones_are_forgotten() {
    let (fake, c) = with_peers().await;
    let mut first = c.subscribe().await.unwrap();
    let second = c.subscribe().await.unwrap();
    assert_eq!(fake.subscriber_count(), 2);
    drop(second);
    assert_eq!(fake.subscriber_count(), 1);
    for i in 1..=3 {
        fake.receive("bob", &i.to_string()).unwrap();
    }
    assert_eq!(
        message_events(&queued(&mut first))
            .iter()
            .map(|e| e.0)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    drop(first);
    fake.wait_for_subscribers(0).await;
}

#[tokio::test]
async fn a_subscriber_that_does_not_read_gets_resync() {
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    for _ in 0..SUBSCRIPTION_CAPACITY + 1 {
        fake.receive("bob", "x").unwrap();
    }
    assert_eq!(events.next().await, Some(Event::Resync));
    let later = fake.receive("bob", "later").unwrap();
    assert_eq!(events.next().await, Some(Event::Message { message: later }));
}

#[tokio::test]
async fn stop_ends_subscriptions_and_refuses_calls() {
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    let got = fake.receive("bob", "x").unwrap();
    fake.stop();
    assert_eq!(events.next().await, Some(Event::Message { message: got }));
    assert_eq!(events.next().await, None);
    assert_eq!(events.next().await, None);
    assert_eq!(c.status().await.unwrap_err(), ApiError::Stopping);
    assert_eq!(c.subscribe().await.unwrap_err(), ApiError::Stopping);
    assert_eq!(fake.receive("bob", "x").unwrap_err(), ApiError::Stopping);
    assert_eq!(fake.subscriber_count(), 0);
}

#[tokio::test]
async fn wait_for_subscribers_wakes_on_subscribe_and_on_drop() {
    let fake = station();
    let c = client(&fake);
    let waiting = fake.clone();
    let waiter = tokio::spawn(async move { waiting.wait_for_subscribers(1).await });
    tokio::task::yield_now().await;
    let events = c.subscribe().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
    let waiting = fake.clone();
    let waiter = tokio::spawn(async move { waiting.wait_for_subscribers(0).await });
    tokio::task::yield_now().await;
    drop(events);
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap();
}

// Inspection.

#[tokio::test]
async fn a_hook_that_removes_or_replaces_itself_does_not_run_again() {
    let (fake, c) = with_peers().await;
    // Removing itself: one call only.
    let runs = Arc::new(Mutex::new(0));
    let count = Arc::clone(&runs);
    fake.on_call(move |fake, _| {
        *count.lock().unwrap() += 1;
        fake.clear_on_call();
    });
    c.status().await.unwrap();
    c.status().await.unwrap();
    assert_eq!(*runs.lock().unwrap(), 1);

    // Replacing itself: the new hook runs from the next call on, the old one never again.
    let first = Arc::new(Mutex::new(0));
    let second = Arc::new(Mutex::new(0));
    let (in_first, in_second) = (Arc::clone(&first), Arc::clone(&second));
    fake.on_call(move |fake, _| {
        *in_first.lock().unwrap() += 1;
        let in_second = Arc::clone(&in_second);
        fake.on_call(move |_, _| *in_second.lock().unwrap() += 1);
    });
    c.status().await.unwrap();
    c.status().await.unwrap();
    c.status().await.unwrap();
    assert_eq!((*first.lock().unwrap(), *second.lock().unwrap()), (1, 2));

    // A hook that leaves the slot alone keeps running.
    let kept = Arc::new(Mutex::new(0));
    let count = Arc::clone(&kept);
    fake.on_call(move |_, _| *count.lock().unwrap() += 1);
    c.status().await.unwrap();
    c.status().await.unwrap();
    assert_eq!(*kept.lock().unwrap(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_hook_runs_for_one_call_at_a_time() {
    let (fake, c) = with_peers().await;
    let runs = Arc::new(AtomicUsize::new(0));
    let (entered, in_hook) = std::sync::mpsc::channel::<()>();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let count = Arc::clone(&runs);
    fake.on_call(move |_, _| {
        count.fetch_add(1, Ordering::SeqCst);
        let _ = entered.send(());
        released.recv_timeout(BOUND).unwrap();
    });
    // Call A, on a worker thread, enters the hook and blocks there.
    let a = tokio::spawn({
        let c = c.clone();
        async move { c.status().await }
    });
    tokio::task::spawn_blocking(move || in_hook.recv_timeout(BOUND))
        .await
        .unwrap()
        .unwrap();
    // Call B, meanwhile, is answered without running the hook.
    assert_eq!(c.peers().await.unwrap().len(), 2);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    release.send(()).unwrap();
    assert!(
        tokio::time::timeout(BOUND, a)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    // Both calls reached the station.
    let requests = fake.requests();
    assert!(requests.contains(&Request::Status));
    assert!(requests.contains(&Request::Peers));
}

#[tokio::test]
async fn requests_are_recorded_and_the_hook_runs_first() {
    let (fake, c) = with_peers().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    fake.on_call(move |fake, request| {
        // The hook may change the station; the call then sees the change.
        if matches!(request, Request::Log { .. }) {
            fake.receive("bob", "from the hook").unwrap();
        }
        log.lock().unwrap().push(request.clone());
    });
    let page = c.log(None, None, 10).await.unwrap();
    assert_eq!(page.messages.last().unwrap().body, "from the hook");
    c.status().await.unwrap();
    fake.clear_on_call();
    c.peers().await.unwrap();
    assert_eq!(seen.lock().unwrap().len(), 2);
    let requests = fake.requests();
    assert_eq!(requests.len(), 5);
    assert_eq!(
        requests[0],
        Request::PeerAdd {
            name: "alice".into(),
            station_id: "STATION-AL".into()
        }
    );
    assert_eq!(requests[3], Request::Status);
    assert_eq!(requests[4], Request::Peers);
}

// Check order (rules 7 and 10).

#[tokio::test]
async fn peer_add_checks_name_then_id_form_then_own_id_then_taken() {
    let (_fake, c) = with_peers().await;
    // Each request fails every later check too; the first one decides.
    let err = c.peer_add("-bad", "").await.unwrap_err();
    assert_eq!(invalid_field(err.clone()), "name");
    let ApiError::Invalid { reason, .. } = c.peer_add("carol", "").await.unwrap_err() else {
        panic!("expected Invalid");
    };
    assert_eq!(reason, "is not a station id");
    let ApiError::Invalid { field, reason } = c.peer_add("carol", ME).await.unwrap_err() else {
        panic!("expected Invalid");
    };
    assert_eq!(
        (field.as_str(), reason.as_str()),
        ("station_id", "is this station's own id")
    );
    assert!(matches!(
        c.peer_add("-bad", "STATION-B").await.unwrap_err(),
        ApiError::Invalid { field, .. } if field == "name"
    ));
    assert!(matches!(
        c.peer_add("carol", "STATION-B").await.unwrap_err(),
        ApiError::StationTaken { .. }
    ));
}

#[tokio::test]
async fn send_checks_the_peer_before_the_body() {
    let (_fake, c) = with_peers().await;
    assert_eq!(
        c.send("carol", "").await.unwrap_err(),
        ApiError::PeerNotFound {
            name: "carol".into()
        }
    );
    assert_eq!(invalid_field(c.send("bob", "").await.unwrap_err()), "body");
}

// Test hooks: restart, fixed clock, failing calls, held subscribes.

#[tokio::test]
async fn restart_is_a_new_instance_that_keeps_the_peer_book() {
    let (fake, c) = with_peers().await;
    fake.set_link("bob", "STATION-B", Link::Direct).unwrap();
    fake.set_tower_state(TowerState::Connected);
    fake.receive("bob", "x").unwrap();
    let mut events = c.subscribe().await.unwrap();
    fake.stop();
    fake.restart("INSTANCE-2");
    assert_eq!(events.next().await, None);
    let status = c.status().await.unwrap();
    assert_eq!(status.instance_id, "INSTANCE-2");
    // The tower is back where a new fake starts.
    assert_eq!(
        status.towers,
        FakeStation::new(ME, "INSTANCE-X").status().towers
    );
    assert_eq!(status.towers[0].state, TowerState::Connecting);
    assert!(c.log(None, None, 10).await.unwrap().messages.is_empty());
    assert_eq!(fake.receive("bob", "y").unwrap().seq, 1);
    let peers = c.peers().await.unwrap();
    assert_eq!(peers.len(), 2);
    assert!(
        peers
            .iter()
            .all(|p| p.stations.iter().all(|s| s.link == Link::Down))
    );
}

#[tokio::test]
async fn set_now_ms_fixes_the_time_of_new_messages() {
    let (fake, c) = with_peers().await;
    let before = fake.receive("bob", "x").unwrap();
    assert!(before.at_ms > 1_700_000_000_000);
    fake.set_now_ms(1_767_225_600_000);
    assert_eq!(fake.receive("bob", "x").unwrap().at_ms, 1_767_225_600_000);
    assert_eq!(c.send("bob", "x").await.unwrap().at_ms, 1_767_225_600_000);
    // The outcome changes the message, not its time.
    let delivered = c.message(3).await.unwrap();
    assert_eq!((delivered.rev, delivered.at_ms), (1, 1_767_225_600_000));
    fake.set_now_ms(5);
    assert_eq!(fake.receive("bob", "x").unwrap().at_ms, 5);
    // Earlier messages keep their time.
    assert_eq!(c.message(1).await.unwrap().at_ms, before.at_ms);
}

#[tokio::test]
async fn fail_calls_answers_every_call_with_the_error_but_not_subscriptions() {
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    let error = ApiError::Disconnected {
        detail: "test".into(),
    };
    fake.fail_calls(Some(error.clone()));
    assert_eq!(c.status().await.unwrap_err(), error);
    assert_eq!(c.log(None, None, 10).await.unwrap_err(), error);
    assert_eq!(c.send("bob", "x").await.unwrap_err(), error);
    // Recorded, and nothing happened.
    assert_eq!(
        fake.requests().last(),
        Some(&Request::Send {
            peer: "bob".into(),
            body: "x".into()
        })
    );
    assert!(fake.messages().is_empty());
    // Subscriptions are not affected.
    let mut second = c.subscribe().await.unwrap();
    let got = fake.receive("bob", "x").unwrap();
    assert_eq!(
        events.next().await,
        Some(Event::Message {
            message: got.clone()
        })
    );
    assert_eq!(second.next().await, Some(Event::Message { message: got }));
    fake.fail_calls(None);
    assert!(c.status().await.is_ok());
}

#[tokio::test]
async fn hold_subscribes_keeps_subscribe_from_registering_until_released() {
    let fake = station();
    fake.hold_subscribes(true);
    let c = client(&fake);
    let mut subscribe = std::pin::pin!(c.subscribe());
    assert!(poll_once(subscribe.as_mut()).is_none());
    assert_eq!(fake.held_subscribes(), 1);
    assert_eq!(fake.subscriber_count(), 0);
    tokio::time::timeout(BOUND, fake.wait_for_held_subscribes(1))
        .await
        .unwrap();
    fake.hold_subscribes(false);
    let events = tokio::time::timeout(BOUND, subscribe)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fake.held_subscribes(), 0);
    assert_eq!(fake.subscriber_count(), 1);
    drop(events);
    // A held subscribe that is dropped is no longer counted.
    fake.hold_subscribes(true);
    assert!(poll_once(c.subscribe()).is_none());
    assert_eq!(fake.held_subscribes(), 0);
}

#[tokio::test]
async fn wait_for_held_subscribes_wakes_when_a_subscribe_is_held() {
    let fake = station();
    fake.hold_subscribes(true);
    let c = client(&fake);
    // The wait starts first, and waits: nothing is held yet.
    let mut waiting = pin!(fake.wait_for_held_subscribes(1));
    assert!(poll_once(waiting.as_mut()).is_none());
    let mut subscribe = pin!(c.subscribe());
    assert!(poll_once(subscribe.as_mut()).is_none());
    tokio::time::timeout(BOUND, waiting).await.unwrap();
    assert_eq!(fake.held_subscribes(), 1);
    // And again when the held subscribe goes away.
    let mut gone = pin!(fake.wait_for_held_subscribes(0));
    assert!(poll_once(gone.as_mut()).is_none());
    fake.hold_subscribes(false);
    let events = tokio::time::timeout(BOUND, subscribe)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(BOUND, gone).await.unwrap();
    drop(events);
}

// The receipt window (rule 8).

#[tokio::test]
async fn a_duplicate_inside_the_receipt_window_is_dropped() {
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    let first = fake
        .receive_with_id("bob", "ID-A", "once")
        .unwrap()
        .unwrap();
    assert_eq!(fake.receive_with_id("bob", "ID-A", "again").unwrap(), None);
    assert_eq!(fake.messages(), vec![first.clone()]);
    assert_eq!(queued(&mut events), vec![Event::Message { message: first }]);
    // The same id from another peer is another message.
    let other = fake
        .receive_with_id("alice", "ID-A", "hi")
        .unwrap()
        .unwrap();
    assert_eq!(other.seq, 2);
}

#[tokio::test]
async fn after_receipt_window_newer_messages_an_id_is_accepted_again() {
    let (fake, _c) = with_peers().await;
    // The window does not follow the log's capacity.
    let fake = fake.with_log_capacity(5);
    fake.receive_with_id("bob", "ID-A", "x").unwrap().unwrap();
    for i in 0..RECEIPT_WINDOW - 1 {
        fake.receive_with_id("alice", &format!("OTHER-{i}"), "x")
            .unwrap()
            .unwrap();
    }
    // Still inside the window, although it left the log long ago.
    assert_eq!(fake.receive_with_id("bob", "ID-A", "x").unwrap(), None);
    fake.receive_with_id("alice", "ONE-MORE", "x")
        .unwrap()
        .unwrap();
    let again = fake.receive_with_id("bob", "ID-A", "x").unwrap().unwrap();
    assert_eq!(again.seq, RECEIPT_WINDOW as u64 + 2);
}

#[tokio::test]
async fn a_duplicate_does_not_extend_the_receipt_window() {
    let (fake, _c) = with_peers().await;
    fake.receive_with_id("bob", "ID-A", "x").unwrap().unwrap();
    for i in 0..RECEIPT_WINDOW - 1 {
        fake.receive_with_id("bob", &format!("OTHER-{i}"), "x")
            .unwrap()
            .unwrap();
    }
    // A duplicate in between: dropped, and A keeps its place as the oldest.
    assert_eq!(fake.receive_with_id("bob", "ID-A", "x").unwrap(), None);
    fake.receive_with_id("bob", "NEW", "x").unwrap().unwrap();
    assert!(fake.receive_with_id("bob", "ID-A", "x").unwrap().is_some());
    // The duplicate of an id still inside the window is still dropped.
    assert_eq!(fake.receive_with_id("bob", "NEW", "x").unwrap(), None);
}

#[tokio::test]
async fn a_restart_forgets_the_receipt_window() {
    let (fake, _c) = with_peers().await;
    fake.receive_with_id("bob", "ID-A", "x").unwrap().unwrap();
    fake.restart("INSTANCE-2");
    assert!(fake.receive_with_id("bob", "ID-A", "x").unwrap().is_some());
}

#[test]
fn the_receipt_window_is_pinned() {
    assert_eq!(RECEIPT_WINDOW, 1000);
}

// The outcome of a send that has left the log (rule 7).

/// Every `Event::Message` for `seq` queued on `events` right now.
fn events_for(events: &mut Subscription, seq: u64) -> Vec<Message> {
    queued(events)
        .into_iter()
        .filter_map(|e| match e {
            Event::Message { message } if message.seq == seq => Some(message),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn an_evicted_held_send_still_gets_its_one_final_event() {
    let (fake, c) = with_peers().await;
    let fake = fake.with_log_capacity(2);
    fake.set_outcome(SendOutcome::Hold);
    let held = c.send("bob", "held").await.unwrap();
    for _ in 0..2 {
        fake.receive("bob", "newer").unwrap();
    }
    assert!(fake.messages().iter().all(|m| m.seq != held.seq));
    let mut events = c.subscribe().await.unwrap();
    let done = fake.resolve(held.seq, MessageStatus::Delivered).unwrap();
    let expected = Message {
        rev: 1,
        status: MessageStatus::Delivered,
        ..held.clone()
    };
    assert_eq!(done, expected);
    assert_eq!(events_for(&mut events, held.seq), vec![expected]);
    assert_eq!(
        c.message(held.seq).await.unwrap_err(),
        ApiError::MessageNotFound { seq: held.seq }
    );
    assert!(fake.messages().iter().all(|m| m.seq != held.seq));
    // Final now: resolving it again is refused, as for any seq that is not a held message.
    assert_eq!(
        fake.resolve(held.seq, MessageStatus::Delivered)
            .unwrap_err(),
        ApiError::MessageNotFound { seq: held.seq }
    );
}

#[tokio::test]
async fn removing_a_peer_fails_its_evicted_pending_send_with_one_event() {
    let (fake, c) = with_peers().await;
    let fake = fake.with_log_capacity(2);
    fake.set_outcome(SendOutcome::Hold);
    let held = c.send("bob", "held").await.unwrap();
    for _ in 0..2 {
        fake.receive("alice", "newer").unwrap();
    }
    assert!(fake.messages().iter().all(|m| m.seq != held.seq));
    let mut events = c.subscribe().await.unwrap();
    c.peer_remove("bob").await.unwrap();
    let failed = Message {
        rev: 1,
        status: MessageStatus::Failed {
            reason: FailReason::PeerRemoved,
        },
        ..held.clone()
    };
    assert_eq!(
        queued(&mut events),
        vec![
            Event::Message { message: failed },
            Event::PeerRemoved { name: "bob".into() }
        ]
    );
    assert_eq!(
        c.message(held.seq).await.unwrap_err(),
        ApiError::MessageNotFound { seq: held.seq }
    );
    assert!(fake.messages().iter().all(|m| m.seq != held.seq));
}

// Subscribes and stopping (finding 5 of the review of #95).

const BOUND: Duration = Duration::from_secs(5);

#[tokio::test]
async fn a_held_subscribe_fails_with_stopping_when_the_station_stops() {
    let fake = station();
    fake.hold_subscribes(true);
    let c = client(&fake);
    let mut subscribe = pin!(c.subscribe());
    assert!(poll_once(subscribe.as_mut()).is_none());
    assert_eq!(fake.held_subscribes(), 1);
    fake.stop();
    let result = tokio::time::timeout(BOUND, subscribe).await.unwrap();
    assert_eq!(result.unwrap_err(), ApiError::Stopping);
    assert_eq!(fake.held_subscribes(), 0);
    tokio::time::timeout(BOUND, fake.wait_for_held_subscribes(0))
        .await
        .unwrap();
    assert_eq!(fake.subscriber_count(), 0);
}

#[tokio::test]
async fn a_held_subscribe_fails_with_stopping_on_restart_and_never_joins_the_new_instance() {
    let (fake, c) = with_peers().await;
    fake.hold_subscribes(true);
    let mut subscribe = pin!(c.subscribe());
    assert!(poll_once(subscribe.as_mut()).is_none());
    assert_eq!(fake.held_subscribes(), 1);
    fake.receive("bob", "old instance").unwrap();
    fake.restart("INSTANCE-2");
    let result = tokio::time::timeout(BOUND, subscribe).await.unwrap();
    assert_eq!(result.unwrap_err(), ApiError::Stopping);
    assert_eq!(fake.held_subscribes(), 0);
    tokio::time::timeout(BOUND, fake.wait_for_held_subscribes(0))
        .await
        .unwrap();
    assert_eq!(fake.subscriber_count(), 0);
    // The restart kept the hold: a new subscribe is held too.
    let mut subscribe = pin!(c.subscribe());
    assert!(poll_once(subscribe.as_mut()).is_none());
    assert_eq!(fake.held_subscribes(), 1);
    // With the hold released, it works and sees the new instance only.
    fake.hold_subscribes(false);
    let mut events = tokio::time::timeout(BOUND, subscribe)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fake.subscriber_count(), 1);
    let got = fake.receive("bob", "new instance").unwrap();
    assert_eq!(got.seq, 1);
    assert_eq!(queued(&mut events), vec![Event::Message { message: got }]);
}

#[tokio::test]
async fn a_subscribe_after_stop_fails_at_once_also_while_held() {
    let fake = station();
    fake.hold_subscribes(true);
    fake.stop();
    let c = client(&fake);
    assert_eq!(
        poll_once(c.subscribe()).map(|r| r.unwrap_err()),
        Some(ApiError::Stopping)
    );
    assert_eq!(fake.held_subscribes(), 0);
}

/// Polls a `wait_for_subscribers` future once, leaves it alone, and checks that `end` still ends
/// the subscription it could have held.
async fn an_abandoned_wait_holds_no_subscription(end: impl FnOnce(&FakeStation)) {
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    let mut waiting = pin!(fake.wait_for_subscribers(5));
    assert!(poll_once(waiting.as_mut()).is_none());
    end(&fake);
    assert_eq!(
        tokio::time::timeout(BOUND, events.next()).await.unwrap(),
        None
    );
    // The waiting future is still alive here, and still not done.
    assert!(poll_once(waiting.as_mut()).is_none());
}

#[tokio::test]
async fn a_waiting_wait_for_subscribers_does_not_keep_a_subscription_open_after_stop() {
    an_abandoned_wait_holds_no_subscription(|fake| fake.stop()).await;
}

#[tokio::test]
async fn a_waiting_wait_for_subscribers_does_not_keep_a_subscription_open_after_restart() {
    an_abandoned_wait_holds_no_subscription(|fake| fake.restart("INSTANCE-2")).await;
}

#[tokio::test]
async fn wait_for_subscribers_notices_a_client_that_dropped_its_subscription() {
    let fake = station();
    let c = client(&fake);
    let first = c.subscribe().await.unwrap();
    let second = c.subscribe().await.unwrap();
    let waiting = fake.clone();
    let waiter = tokio::spawn(async move { waiting.wait_for_subscribers(1).await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    // Nothing tells the fake: the waiter has to notice by itself.
    drop(second);
    tokio::time::timeout(BOUND, waiter).await.unwrap().unwrap();
    assert_eq!(fake.subscriber_count(), 1);
    drop(first);
}

// Fixtures follow the contract (finding 7 of the review of #95).

#[tokio::test]
async fn tower_text_is_normalised_in_status_and_its_event() {
    let fake = station();
    let c = client(&fake);
    let mut events = c.subscribe().await.unwrap();
    fake.set_tower_state(TowerState::Failing {
        error: "no\nroute\u{1b}[31m\t\u{85}!".into(),
    });
    let expected = TowerState::Failing {
        error: "no route [31m  !".into(),
    };
    let status = c.status().await.unwrap();
    assert_eq!(status.towers[0].state, expected);
    assert_eq!(fake.status(), status);
    fake.set_tower_state(TowerState::Denied {
        reason: "not\r\nallowed\u{7f}".into(),
    });
    let denied = c.status().await.unwrap();
    assert_eq!(
        denied.towers[0].state,
        TowerState::Denied {
            reason: "not  allowed ".into()
        }
    );
    assert_eq!(
        queued(&mut events),
        vec![Event::Status { status }, Event::Status { status: denied }]
    );
}

/// Line breaks and reordering that are not control characters, and a C1 control character.
const WIDER: &str = "a\u{2028}b\u{2029}c\u{202E}d\u{2066}e\u{061C}f\u{200E}g\u{9B}h\u{200B}";
/// `WIDER` as rule 9 leaves it: each replaced by one space; U+200B stays.
const WIDER_CLEAN: &str = "a b c d e f g h\u{200B}";

#[tokio::test]
async fn the_wider_rule_9_set_is_replaced_in_bodies_and_tower_text() {
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    let got = fake.receive("bob", WIDER).unwrap();
    assert_eq!(got.body, WIDER_CLEAN);
    // In the log page and in the event.
    let page = c.log(None, None, 10).await.unwrap();
    assert_eq!(page.messages, vec![got.clone()]);
    assert_eq!(c.message(got.seq).await.unwrap().body, WIDER_CLEAN);
    fake.set_tower_state(TowerState::Failing {
        error: WIDER.into(),
    });
    let failing = c.status().await.unwrap();
    assert_eq!(
        failing.towers[0].state,
        TowerState::Failing {
            error: WIDER_CLEAN.into()
        }
    );
    fake.set_tower_state(TowerState::Denied {
        reason: WIDER.into(),
    });
    let denied = c.status().await.unwrap();
    assert_eq!(
        denied.towers[0].state,
        TowerState::Denied {
            reason: WIDER_CLEAN.into()
        }
    );
    assert_eq!(
        queued(&mut events),
        vec![
            Event::Message { message: got },
            Event::Status { status: failing },
            Event::Status { status: denied }
        ]
    );
}

#[tokio::test]
async fn invalid_incoming_fixtures_are_refused_and_change_nothing() {
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    let first = fake.receive("bob", "first").unwrap();
    assert_eq!(queued(&mut events).len(), 1);
    let too_long = "x".repeat(BODY_MAX_BYTES + 1);
    let id_too_long = "a".repeat(MESSAGE_ID_MAX_BYTES + 1);
    let cases: [(&str, &str, &str); 8] = [
        ("ID-X", &too_long, "body"),
        ("", "hi", "id"),
        ("ID\u{7}X", "hi", "id"),
        ("ID-X\n", "hi", "id"),
        (&id_too_long, "hi", "id"),
        ("ID X", "hi", "id"),
        ("ID.X", "hi", "id"),
        ("ID-\u{e9}", "hi", "id"),
    ];
    for (id, body, field) in cases {
        let err = fake.receive_with_id("bob", id, body).unwrap_err();
        assert_eq!(invalid_field(err), field, "id {id:?}");
    }
    assert_eq!(
        invalid_field(fake.receive("bob", &too_long).unwrap_err()),
        "body"
    );
    // Nothing changed: the log, the events, the receipt window and the next seq.
    assert_eq!(fake.messages(), vec![first]);
    assert!(queued(&mut events).is_empty());
    let accepted = fake
        .receive_with_id("bob", "ID-X", "now valid")
        .unwrap()
        .unwrap();
    assert_eq!((accepted.seq, accepted.id.as_str()), (2, "ID-X"));
    assert_eq!(queued(&mut events).len(), 1);
}

#[tokio::test]
async fn an_empty_incoming_body_is_accepted() {
    // Rule 7's lower bound is for `Send`. A peer may send an empty body, and the station logs it.
    let (fake, c) = with_peers().await;
    let mut events = c.subscribe().await.unwrap();
    let made = fake.receive("bob", "").unwrap();
    assert_eq!((made.seq, made.body.as_str()), (1, ""));
    let given = fake.receive_with_id("bob", "EMPTY-1", "").unwrap().unwrap();
    assert_eq!((given.seq, given.body.as_str()), (2, ""));
    assert_eq!(fake.messages(), vec![made.clone(), given.clone()]);
    assert_eq!(
        queued(&mut events),
        vec![
            Event::Message { message: made },
            Event::Message { message: given }
        ]
    );
    // The sending side keeps its lower bound.
    assert_eq!(invalid_field(c.send("bob", "").await.unwrap_err()), "body");
}

#[tokio::test]
async fn incoming_ids_of_the_api_form_are_accepted() {
    let (fake, _c) = with_peers().await;
    let longest = format!("{}_-x9", "Az9".repeat((MESSAGE_ID_MAX_BYTES - 4) / 3));
    assert_eq!(longest.len(), MESSAGE_ID_MAX_BYTES);
    for id in [longest.as_str(), "a", "0", "_", "-", "ID_x-9"] {
        let got = fake.receive_with_id("bob", id, "hi").unwrap().unwrap();
        assert_eq!(got.id, id);
    }
    // The ids the fake makes itself have the form too.
    let made = fake.receive("bob", "hi").unwrap();
    assert!(made.id.len() <= MESSAGE_ID_MAX_BYTES);
    assert!(
        made.id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    );
}

#[tokio::test]
async fn incoming_fixture_checks_run_in_order() {
    let (fake, _c) = with_peers().await;
    fake.receive_with_id("bob", "DUP", "x").unwrap().unwrap();
    // Each case fails every later check too; the first one decides.
    assert_eq!(
        fake.receive_with_id("carol", "", "").unwrap_err(),
        ApiError::PeerNotFound {
            name: "carol".into()
        }
    );
    assert_eq!(
        invalid_field(fake.receive_with_id("bob", "", "").unwrap_err()),
        "id"
    );
    let too_long = "x".repeat(BODY_MAX_BYTES + 1);
    assert_eq!(
        invalid_field(fake.receive_with_id("bob", "DUP", &too_long).unwrap_err()),
        "body"
    );
    assert_eq!(fake.receive_with_id("bob", "DUP", "x").unwrap(), None);
    fake.stop();
    assert_eq!(
        fake.receive_with_id("carol", "", "").unwrap_err(),
        ApiError::Stopping
    );
    assert_eq!(fake.receive("carol", "").unwrap_err(), ApiError::Stopping);
}

/// Rule 6 holds for everything the fixture API can put into the log: the largest body and the
/// longest id it accepts still fit a page.
#[tokio::test]
async fn the_largest_incoming_messages_the_fixtures_accept_fit_a_page() {
    let (fake, c) = with_peers().await;
    // One byte over is refused, so nothing larger can enter the log.
    assert_eq!(
        invalid_field(
            fake.receive("bob", &"\"".repeat(BODY_MAX_BYTES + 1))
                .unwrap_err()
        ),
        "body"
    );
    // The largest JSON per body byte: '"' and '\' are escaped to two bytes; control characters,
    // which JSON escapes to six, are replaced with spaces first (rule 9).
    let quotes = "\"".repeat(BODY_MAX_BYTES);
    let backslashes = "\\".repeat(BODY_MAX_BYTES);
    // The longest id: MESSAGE_ID_MAX_BYTES bytes, none of which JSON escapes.
    for i in 0..40 {
        let body = if i % 2 == 0 { &quotes } else { &backslashes };
        let id = format!("{i:0>width$}", width = MESSAGE_ID_MAX_BYTES);
        assert_eq!(id.len(), MESSAGE_ID_MAX_BYTES);
        fake.receive_with_id("bob", &id, body).unwrap().unwrap();
    }
    let size = |m: &Message| serde_json::to_vec(m).unwrap().len();
    let mut before = None;
    let mut seen = Vec::new();
    loop {
        let page = c.log(None, before, u32::MAX).await.unwrap();
        assert!(!page.messages.is_empty());
        let total: usize = page.messages.iter().map(size).sum();
        assert!(total <= LOG_PAGE_MAX_BYTES, "{total} bytes");
        seen.splice(0..0, seqs(&page));
        before = page.next_before;
        if before.is_none() {
            break;
        }
    }
    assert_eq!(seen, (1..=40).collect::<Vec<_>>());
}
