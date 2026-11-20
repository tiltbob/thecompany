use std::future::{pending, poll_fn, ready};
use std::task::Poll;

use super::*;

fn peer() -> Peer {
    Peer {
        id: crate::node_id(&generate_node_key()),
        state: Mutex::default(),
        changed: watch::Sender::new(()),
    }
}

#[tokio::test]
async fn close_ends_the_wait_before_its_budget() {
    let peer = peer();
    let mut observed = pin!(wait_for_close(
        &peer,
        ready(ConnectionError::LocallyClosed),
        Duration::from_secs(10),
    ));
    let result = poll_fn(|cx| Poll::Ready(observed.as_mut().poll(cx))).await;
    assert_eq!(result, Poll::Ready(Ok(())));
}

#[tokio::test]
async fn budget_ends_the_wait_without_a_close() {
    let peer = peer();
    let observed = wait_for_close(&peer, pending(), Duration::ZERO);
    assert_eq!(timeout(Duration::from_secs(2), observed).await, Ok(Ok(())));
}

#[tokio::test]
async fn revocation_ends_an_existing_wait() {
    let peer = peer();
    let mut observed = pin!(wait_for_close(&peer, pending(), Duration::from_secs(10)));
    let (paths, _) = broadcast::channel(1);
    poll_fn(|cx| {
        assert!(observed.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    peer.revoke(&paths);
    let result = poll_fn(|cx| Poll::Ready(observed.as_mut().poll(cx))).await;
    assert_eq!(result, Poll::Ready(Err(SendError::Closed)));
}
