use tokio::time::timeout;

use super::*;

/// Long enough that no held send reaches its deadline while a test runs.
const HELD_DEADLINE: Duration = Duration::from_secs(3_600);
const REAP_TIMEOUT: Duration = Duration::from_secs(30);

/// An actor with one peer `peer` whose only station has no endpoint behind it, and a
/// subscription that starts after that add.
async fn send_fixture(data_dir: &Path) -> (Actor, Link, Subscription) {
    let (mut actor, link) = actor_fixture(data_dir).await;
    actor
        .add_peer("peer", &node_id(&generate_node_key()).to_string())
        .unwrap_or_else(|_| panic!("add the peer"));
    let (sink, events) = subscription(SUBSCRIPTION_CAPACITY);
    actor.sinks.push(sink);
    (actor, link, events)
}

/// A refused send logged nothing, pushed nothing and started no delivery.
async fn assert_nothing_started(actor: &Actor, events: &mut Subscription) {
    assert!(actor.jobs.is_empty(), "no delivery job was spawned");
    assert!(actor.pending.is_empty(), "no send is pending");
    let log = actor
        .history
        .page(None, None, 10)
        .unwrap_or_else(|_| panic!("read message log"));
    assert!(log.messages.is_empty(), "nothing was logged");
    assert!(queued_event(events).await.is_none());
}

#[tokio::test]
async fn an_empty_body_is_refused_before_anything_starts() {
    let dir = tempfile::tempdir().expect("create actor test directory");
    let (mut actor, link, mut events) = send_fixture(&dir.path().join("station")).await;
    assert!(matches!(
        actor.send("peer", ""),
        Err(ApiError::Invalid { field, reason }) if field == "body" && reason == "must not be empty"
    ));
    assert_nothing_started(&actor, &mut events).await;
    link.shutdown().await.expect("shut down actor fixture link");
}

#[tokio::test]
async fn an_oversized_body_is_refused_before_anything_starts() {
    let dir = tempfile::tempdir().expect("create actor test directory");
    let (mut actor, link, mut events) = send_fixture(&dir.path().join("station")).await;
    let body = "x".repeat(BODY_MAX_BYTES + 1);
    assert!(matches!(
        actor.send("peer", &body),
        Err(ApiError::Invalid { field, .. }) if field == "body"
    ));
    // The limit is in bytes: this body is over it in bytes and under it in characters.
    let body = "\u{e9}".repeat(BODY_MAX_BYTES / 2 + 1);
    assert!(matches!(
        actor.send("peer", &body),
        Err(ApiError::Invalid { field, .. }) if field == "body"
    ));
    assert_nothing_started(&actor, &mut events).await;
    link.shutdown().await.expect("shut down actor fixture link");
}

#[tokio::test]
async fn a_send_to_an_unknown_peer_is_refused_before_anything_starts() {
    let dir = tempfile::tempdir().expect("create actor test directory");
    let (mut actor, link, mut events) = send_fixture(&dir.path().join("station")).await;
    assert!(matches!(
        actor.send("stranger", "hello"),
        Err(ApiError::PeerNotFound { name }) if name == "stranger"
    ));
    assert_nothing_started(&actor, &mut events).await;
    link.shutdown().await.expect("shut down actor fixture link");
}

/// Every send but one goes to `peer`, whose station is down: those deliveries end in the deadline
/// sleep and stay pending. The last one goes to `other`, and removing `other` finishes it.
#[tokio::test]
async fn the_pending_send_cap_refuses_one_more_until_a_send_finishes() {
    let dir = tempfile::tempdir().expect("create actor test directory");
    let (mut actor, link, _) = send_fixture(&dir.path().join("station")).await;
    actor.send_deadline = HELD_DEADLINE;
    actor
        .add_peer("other", &node_id(&generate_node_key()).to_string())
        .unwrap_or_else(|_| panic!("add the other peer"));
    for _ in 1..MAX_PENDING_SENDS {
        actor
            .send("peer", "held")
            .unwrap_or_else(|_| panic!("accept a send below the cap"));
    }
    actor
        .send("other", "finished by the removal")
        .unwrap_or_else(|_| panic!("accept the last send below the cap"));
    assert_eq!(actor.jobs.len(), MAX_PENDING_SENDS);
    let newest = u64::try_from(MAX_PENDING_SENDS).expect("the cap fits a sequence number");
    let (sink, mut events) = subscription(SUBSCRIPTION_CAPACITY);
    actor.sinks.push(sink);

    assert!(matches!(
        actor.send("peer", "one too many"),
        Err(ApiError::Internal { .. })
    ));
    assert_eq!(actor.jobs.len(), MAX_PENDING_SENDS);
    assert_eq!(actor.pending.len(), MAX_PENDING_SENDS);
    assert!(matches!(
        actor.history.message(newest + 1),
        Err(ApiError::MessageNotFound { .. })
    ));
    assert!(
        actor.history.message(1).is_ok(),
        "the refusal evicted nothing"
    );
    assert!(queued_event(&mut events).await.is_none());

    // Finish one send as the run loop does: the removal fails it, then its worker is reaped.
    assert!(matches!(actor.remove_peer("other"), Ok(Response::Done)));
    // The cap counts worker tasks, so a failed send whose worker is not reaped yet still counts.
    assert!(matches!(
        actor.send("peer", "still reclaiming"),
        Err(ApiError::Internal { .. })
    ));
    let reaped = timeout(REAP_TIMEOUT, actor.jobs.join_next())
        .await
        .expect("reap the finished send in time")
        .expect("a job is left to reap");
    actor.completed(reaped);
    assert_eq!(actor.jobs.len(), MAX_PENDING_SENDS - 1);

    assert!(matches!(
        actor.send("peer", "fits again"),
        Ok(Response::Message { message }) if message.seq == newest + 1
    ));
    assert_eq!(actor.jobs.len(), MAX_PENDING_SENDS);
    drop(actor);
    link.shutdown().await.expect("shut down actor fixture link");
}
