#![cfg(test)]
//! A transport bound to one station instance (`chief::client::BindInstance`), in-process against
//! `chief::fake::FakeStation`: a bound handle talks to its instance only, and a request it turns
//! away never reaches the station. Nothing sleeps or touches the network.

use chief::api::*;
use chief::client::{BindInstance, Client};
use chief::fake::*;
use std::time::Duration;

const ME: &str = "STATION-A";
const BOUND: Duration = Duration::from_secs(10);

/// A fake on `INSTANCE-1` with the peer `bob` (STATION-B).
async fn station() -> FakeStation {
    let fake = FakeStation::new(ME, "INSTANCE-1");
    Client::new(fake.clone())
        .peer_add("bob", "STATION-B")
        .await
        .unwrap();
    fake
}

fn is_disconnected(e: &ApiError) -> bool {
    matches!(e, ApiError::Disconnected { .. })
}

fn sends(fake: &FakeStation) -> usize {
    fake.requests()
        .iter()
        .filter(|r| matches!(r, Request::Send { .. }))
        .count()
}

#[tokio::test]
async fn a_bound_handle_works_with_its_own_instance() {
    let fake = station().await;
    let before = fake.requests().len();
    let bound = Client::new(fake.bound_to("INSTANCE-1"));
    assert_eq!(bound.status().await.unwrap().instance_id, "INSTANCE-1");
    let sent = bound.send("bob", "hello").await.unwrap();
    assert_eq!(sent.body, "hello");
    let mut events = bound.subscribe().await.unwrap();
    fake.receive("bob", "hi").unwrap();
    assert!(matches!(events.next().await, Some(Event::Message { .. })));
    assert_eq!(fake.requests().len(), before + 2);
    assert_eq!(fake.subscriber_count(), 1);
}

#[tokio::test]
async fn after_a_restart_a_bound_call_and_subscribe_never_reach_the_new_instance() {
    let fake = station().await;
    let bound = fake.bound_to("INSTANCE-1");
    // A clone of a bound handle is bound too.
    let client = Client::new(bound.clone());
    client.status().await.unwrap();
    fake.restart("INSTANCE-2");
    let recorded = fake.requests();

    let err = client.send("bob", "hello").await.unwrap_err();
    assert!(is_disconnected(&err), "{err:?}");
    let err = client.status().await.unwrap_err();
    assert!(is_disconnected(&err), "{err:?}");
    let err = client.subscribe().await.unwrap_err();
    assert!(is_disconnected(&err), "{err:?}");

    // Never reached the station: not recorded, not handled, nothing logged, no subscriber.
    assert_eq!(fake.requests(), recorded);
    assert!(fake.messages().is_empty());
    assert_eq!(fake.subscriber_count(), 0);
    // The detail names no instance.
    assert!(!err.to_string().contains("INSTANCE"), "{err}");
}

#[tokio::test]
async fn a_restart_made_by_the_hook_inside_the_call_is_caught() {
    let fake = station().await;
    let mut events = Client::new(fake.clone()).subscribe().await.unwrap();
    let client = Client::new(fake.bound_to("INSTANCE-1"));
    fake.on_call(|fake, request| {
        if matches!(request, Request::Send { .. }) {
            fake.clear_on_call();
            fake.restart("INSTANCE-2");
        }
    });
    let err = client.send("bob", "hello").await.unwrap_err();
    assert!(is_disconnected(&err), "{err:?}");
    // The new instance did not log the send, and its subscriber (none: the restart ended it) saw
    // nothing; the old subscription just ended.
    assert_eq!(sends(&fake), 0);
    assert!(fake.messages().is_empty());
    assert_eq!(events.next().await, None);
}

#[tokio::test]
async fn an_unbound_handle_of_the_same_station_keeps_working() {
    let fake = station().await;
    let bound = Client::new(fake.bound_to("INSTANCE-1"));
    fake.restart("INSTANCE-2");
    assert!(is_disconnected(&bound.status().await.unwrap_err()));
    let unbound = Client::new(fake.clone());
    assert_eq!(unbound.status().await.unwrap().instance_id, "INSTANCE-2");
    unbound.send("bob", "hello").await.unwrap();
    let _events = unbound.subscribe().await.unwrap();
    assert_eq!(sends(&fake), 1);
    assert_eq!(fake.subscriber_count(), 1);
    // Binding again, to the instance that runs now, replaces the old binding.
    let rebound = Client::new(fake.bound_to("INSTANCE-1").bound_to("INSTANCE-2"));
    assert_eq!(rebound.status().await.unwrap().instance_id, "INSTANCE-2");
}

#[tokio::test]
async fn a_held_bound_subscribe_fails_as_disconnected_after_a_restart() {
    let fake = station().await;
    fake.hold_subscribes(true);
    let bound = Client::new(fake.bound_to("INSTANCE-1"));
    let subscribe = tokio::spawn(async move { bound.subscribe().await.map(|_| ()) });
    tokio::time::timeout(BOUND, fake.wait_for_held_subscribes(1))
        .await
        .unwrap();
    fake.restart("INSTANCE-2");
    fake.hold_subscribes(false);
    let err = tokio::time::timeout(BOUND, subscribe)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(is_disconnected(&err), "{err:?}");
    assert_eq!(fake.subscriber_count(), 0);
}

#[tokio::test]
async fn client_bound_to_binds_its_transport() {
    let fake = station().await;
    let client = Client::new(fake.clone());
    let instance = client.status().await.unwrap().instance_id;
    let bound = client.bound_to(&instance);
    assert_eq!(bound.status().await.unwrap().instance_id, "INSTANCE-1");
    fake.restart("INSTANCE-2");
    assert!(is_disconnected(&bound.status().await.unwrap_err()));
    // The client it was made from is not bound.
    assert_eq!(client.status().await.unwrap().instance_id, "INSTANCE-2");
}

#[tokio::test]
async fn send_and_wait_on_a_bound_client_never_sends_to_a_restarted_station() {
    // A restart between the subscribe and the send.
    let fake = station().await;
    let bound = Client::new(fake.clone()).bound_to("INSTANCE-1");
    fake.on_call(|fake, request| {
        if matches!(request, Request::Send { .. }) {
            fake.clear_on_call();
            fake.restart("INSTANCE-2");
        }
    });
    let err = tokio::time::timeout(BOUND, bound.send_and_wait("bob", "hello"))
        .await
        .unwrap()
        .unwrap_err();
    assert!(is_disconnected(&err), "{err:?}");
    assert_eq!(sends(&fake), 0);
    assert!(fake.messages().is_empty());
    assert_eq!(fake.subscriber_count(), 0);

    // A restart before the call: the subscribe already fails, and nothing is sent.
    fake.restart("INSTANCE-3");
    let err = bound.send_and_wait("bob", "hello").await.unwrap_err();
    assert!(is_disconnected(&err), "{err:?}");
    assert_eq!(sends(&fake), 0);

    // Bound to the instance that runs, it sends and waits as before.
    let current = Client::new(fake.clone()).bound_to("INSTANCE-3");
    let done = tokio::time::timeout(BOUND, current.send_and_wait("bob", "hello"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.status, MessageStatus::Delivered);
    assert_eq!(sends(&fake), 1);
}
