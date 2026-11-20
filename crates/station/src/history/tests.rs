use super::*;
use chief::api::{BODY_MAX_BYTES, FailReason};

fn received(history: &mut History, peer: &str, body: String) -> Message {
    history
        .append(
            peer,
            "fixture".to_owned(),
            Direction::Incoming,
            body,
            MessageStatus::Received,
        )
        .expect("append fixture")
}

#[test]
fn oldest_entries_are_evicted_without_reusing_sequences() {
    let mut history = History::default();
    let mut sent = history
        .append(
            "bob",
            "fixture".to_owned(),
            Direction::Outgoing,
            "hello".to_owned(),
            MessageStatus::Sending,
        )
        .expect("append send");
    assert_eq!((sent.seq, sent.rev), (1, 0));

    for offset in 0..MESSAGE_LOG_CAPACITY {
        let message = received(&mut history, "alice", String::new());
        assert_eq!((message.seq, message.rev), (offset as u64 + 2, 0));
    }
    sent.status = MessageStatus::Failed {
        reason: FailReason::PeerRemoved,
    };
    sent.rev = 1;
    history.update(&sent);
    assert!(matches!(
        history.message(1),
        Err(ApiError::MessageNotFound { seq: 1 })
    ));

    let all = history
        .page(None, None, u32::MAX)
        .expect("read retained log");
    assert_eq!(all.messages.len(), MESSAGE_LOG_CAPACITY);
    assert_eq!(all.messages.first().map(|message| message.seq), Some(2));
    assert_eq!(all.next_before, None);
}

#[test]
fn the_exclusive_cursor_cannot_overflow() {
    let mut history = History {
        next_seq: u64::MAX - 1,
        ..History::default()
    };
    let message = received(&mut history, "bob", "fixture".to_owned());
    assert_eq!(message.seq, u64::MAX - 1);
    assert_eq!(
        history
            .page(None, None, 0)
            .expect("read empty page")
            .next_before,
        Some(u64::MAX)
    );
    assert!(
        history
            .append(
                "bob",
                "fixture".to_owned(),
                Direction::Incoming,
                String::new(),
                MessageStatus::Received,
            )
            .is_err()
    );
    assert_eq!(history.ring.len(), 1);
}

#[test]
fn paging_is_filtered_ordered_and_bounded_by_encoded_bytes() {
    let mut history = History::default();
    let body = "\"".repeat(BODY_MAX_BYTES);
    for seq in 1..=80 {
        received(
            &mut history,
            if seq % 2 == 0 { "alice" } else { "bob" },
            body.clone(),
        );
    }
    let zero = history.page(Some("bob"), None, 0).expect("read empty page");
    assert!(zero.messages.is_empty());
    assert_eq!(zero.next_before, Some(80));
    assert_eq!(
        history
            .page(Some("bob"), Some(25), 0)
            .expect("read cursor")
            .next_before,
        Some(25)
    );
    assert_eq!(
        history
            .page(Some("missing"), None, 0)
            .expect("read empty log")
            .next_before,
        None
    );

    let mut all = Vec::new();
    let mut cursor = zero.next_before;
    while let Some(before) = cursor {
        let page = history
            .page(Some("bob"), Some(before), u32::MAX)
            .expect("read page");
        let seqs: Vec<_> = page.messages.iter().map(|message| message.seq).collect();
        let bytes: usize = page
            .messages
            .iter()
            .map(|message| serde_json::to_vec(message).expect("encode fixture").len())
            .sum();
        assert!(bytes <= LOG_PAGE_MAX_BYTES);
        assert!(!seqs.is_empty());
        if let Some(next) = page.next_before {
            assert_eq!(Some(next), seqs.first().copied());
            assert!(next < before);
        }
        all.splice(0..0, seqs);
        cursor = page.next_before;
    }
    assert_eq!(all, (1..=80).step_by(2).collect::<Vec<_>>());
}
