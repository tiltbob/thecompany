//! The chat stream carries one message and one reply.
//!
//! Message bodies are UTF-8 in memory and padded standard base64 in the JSON frame.
//! The encoding keeps every permitted body within the frame cap, including text made
//! entirely of characters that JSON would escape.

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};

/// Maximum decoded chat body size in UTF-8 bytes.
pub const CHAT_BODY_MAX: usize = 16 * 1024;
/// Maximum chat ID length in ASCII bytes.
pub const CHAT_ID_MAX: usize = 64;

const CHAT_ENCODED_BODY_MAX: usize = CHAT_BODY_MAX.div_ceil(3) * 4;

/// A chat message. The station checks peer-supplied data before displaying it.
#[derive(Clone, PartialEq, Eq)]
pub struct ChatMessage {
    /// Sender-assigned ID. One to 64 ASCII letters, digits, underscores or hyphens.
    pub id: String,
    /// Sender-reported timestamp in Unix milliseconds.
    pub sent_at_ms: u64,
    /// Plain UTF-8 text, up to 16 KiB.
    pub body: String,
}

impl ChatMessage {
    /// Checks the message ID and the length of the UTF-8 body.
    pub fn validate(&self) -> Result<(), NackReason> {
        validate_id(&self.id)?;
        validate_body_len(self.body.len())
    }
}

impl fmt::Debug for ChatMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChatMessage").finish_non_exhaustive()
    }
}

impl Serialize for ChatMessage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(serde::ser::Error::custom)?;
        let mut message = serializer.serialize_struct("ChatMessage", 3)?;
        message.serialize_field("id", &self.id)?;
        message.serialize_field("sent_at_ms", &self.sent_at_ms)?;
        message.serialize_field("body", &STANDARD.encode(&self.body))?;
        message.end()
    }
}

/// Read through the bounded frame codec before converting to a validated message.
#[derive(Deserialize)]
pub(crate) struct WireChatMessage {
    id: String,
    sent_at_ms: u64,
    body: String,
}

impl TryFrom<WireChatMessage> for ChatMessage {
    type Error = NackReason;

    fn try_from(wire: WireChatMessage) -> Result<Self, Self::Error> {
        validate_id(&wire.id)?;
        if wire.body.len() > CHAT_ENCODED_BODY_MAX {
            return Err(NackReason::BodyTooLarge);
        }
        let decoded = STANDARD
            .decode(&wire.body)
            .map_err(|_| NackReason::InvalidBody)?;
        // Some overlong bodies have the same padded length as the largest valid body.
        validate_body_len(decoded.len())?;
        let body = String::from_utf8(decoded).map_err(|_| NackReason::InvalidBody)?;
        Ok(Self {
            id: wire.id,
            sent_at_ms: wire.sent_at_ms,
            body,
        })
    }
}

fn validate_id(id: &str) -> Result<(), NackReason> {
    if !(1..=CHAT_ID_MAX).contains(&id.len())
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(NackReason::InvalidId);
    }
    Ok(())
}

fn validate_body_len(len: usize) -> Result<(), NackReason> {
    if len > CHAT_BODY_MAX {
        return Err(NackReason::BodyTooLarge);
    }
    Ok(())
}

/// The receiver's reply to one chat message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "reason", rename_all = "snake_case")]
#[serde(try_from = "WireChatReply")]
pub enum ChatReply {
    Ack,
    Nack(NackReason),
}

// Read the typed reason directly, regardless of field order. An adjacently tagged enum's default
// deserializer buffers the entire reason value when it appears before the tag.
#[derive(Deserialize)]
struct WireChatReply {
    #[serde(rename = "type")]
    kind: ReplyKind,
    reason: Option<NackReason>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReplyKind {
    Ack,
    Nack,
}

impl TryFrom<WireChatReply> for ChatReply {
    type Error = &'static str;

    fn try_from(wire: WireChatReply) -> Result<Self, Self::Error> {
        match (wire.kind, wire.reason) {
            (ReplyKind::Ack, None) => Ok(Self::Ack),
            (ReplyKind::Nack, Some(reason)) => Ok(Self::Nack(reason)),
            _ => Err("chat reply type and reason do not match"),
        }
    }
}

/// A rejection reason with no peer-controlled text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum NackReason {
    #[error("unknown stream protocol")]
    UnknownProtocol,
    #[error("unsupported chat version")]
    UnsupportedVersion,
    #[error("invalid chat message ID")]
    InvalidId,
    #[error("chat body exceeds 16 KiB")]
    BodyTooLarge,
    #[error("chat body is not base64-encoded UTF-8")]
    InvalidBody,
    #[error("chat receiver unavailable")]
    Unavailable,
}

type ReceiptKey = (Box<str>, Box<str>);

/// An in-memory FIFO of first accepted chat messages across all peers.
///
/// A receipt is keyed by the local peer name and validated message ID. A retry from
/// any station of the same peer matches the same receipt. Retries leave the FIFO
/// order unchanged, and constructing a new window clears all previously seen IDs.
pub struct ReceiptWindow {
    capacity: usize,
    order: VecDeque<Arc<ReceiptKey>>,
    seen: HashSet<Arc<ReceiptKey>>,
}

impl ReceiptWindow {
    /// Starts with no receipts and allocates storage as receipts arrive.
    /// A capacity of zero disables retention.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::new(),
            seen: HashSet::new(),
        }
    }

    /// Checks for a receipt by peer and message ID, preserving FIFO order.
    pub fn contains(&self, peer: &str, id: &str) -> bool {
        let key: ReceiptKey = (peer.into(), id.into());
        self.seen.contains(&key)
    }

    /// Records an accepted message, returning `true` for a new receipt or `false` for a retry.
    ///
    /// Authorize the peer and validate the message before calling this with its ID and the
    /// bounded local peer name. In the receiving actor, record the receipt, add a new message
    /// to the log and publish its event, and decide the ACK as one serialized operation. A
    /// retry gets another ACK without adding a log entry or publishing an event.
    pub fn record(&mut self, peer: &str, id: &str) -> bool {
        if self.capacity == 0 {
            return true;
        }

        let key: ReceiptKey = (peer.into(), id.into());
        if self.seen.contains(&key) {
            return false;
        }
        if self.order.len() == self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.seen.remove(&oldest);
        }
        let key = Arc::new(key);
        self.seen.insert(Arc::clone(&key));
        self.order.push_back(key);
        true
    }
}

impl fmt::Debug for ReceiptWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiptWindow")
            .field("capacity", &self.capacity)
            .field("len", &self.order.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use numbers_base::{FrameError, WIRE_FRAME_MAX, read_frame, write_frame};
    use serde_json::json;

    fn example() -> ChatMessage {
        ChatMessage {
            id: "msg_0-A".into(),
            sent_at_ms: 42,
            body: "\0\n\u{0085}é".into(),
        }
    }

    #[test]
    fn json_uses_padded_standard_base64_and_tolerates_new_fields() {
        let original = example();
        assert_eq!(
            serde_json::to_string(&original).unwrap(),
            r#"{"id":"msg_0-A","sent_at_ms":42,"body":"AArChcOp"}"#
        );
        let wire: WireChatMessage = serde_json::from_str(
            r#"{"id":"msg_0-A","sent_at_ms":42,"body":"AArChcOp","extra":[1,2]}"#,
        )
        .unwrap();
        assert_eq!(ChatMessage::try_from(wire), Ok(original));
        let mut original = example();
        original.body = "a".into();
        assert_eq!(serde_json::to_value(original).unwrap()["body"], "YQ==");
    }

    #[tokio::test]
    async fn exact_body_and_id_limits_fit_the_wire_with_controls() {
        let original = ChatMessage {
            id: "A".repeat(CHAT_ID_MAX),
            sent_at_ms: u64::MAX,
            body: "\0".repeat(CHAT_BODY_MAX),
        };
        let mut frame = Vec::new();
        write_frame(&mut frame, &original, WIRE_FRAME_MAX)
            .await
            .unwrap();
        assert!(frame.len() - 4 <= WIRE_FRAME_MAX);
        let wire: WireChatMessage = read_frame(&mut frame.as_slice(), WIRE_FRAME_MAX, None)
            .await
            .unwrap();
        assert_eq!(ChatMessage::try_from(wire), Ok(original));
    }

    #[tokio::test]
    async fn exact_wire_cap_is_accepted_and_one_extra_byte_is_rejected() {
        let msg = r#"{"id":"m","sent_at_ms":0,"body":"","extra":""}"#;
        let frame = |len: usize| {
            let mut encoded = u32::try_from(len).unwrap().to_be_bytes().to_vec();
            encoded.extend_from_slice(&msg.as_bytes()[..msg.len() - 2]);
            encoded.resize(4 + len - 2, b'x');
            encoded.extend_from_slice(b"\"}");
            encoded
        };
        let at_cap = frame(WIRE_FRAME_MAX);
        let wire: WireChatMessage = read_frame(&mut at_cap.as_slice(), WIRE_FRAME_MAX, None)
            .await
            .unwrap();
        assert!(ChatMessage::try_from(wire).unwrap().body.is_empty());
        // The size prefix alone rejects an oversized frame without waiting for its body.
        let too_large = u32::try_from(WIRE_FRAME_MAX + 1).unwrap().to_be_bytes();
        assert!(matches!(
            read_frame::<_, WireChatMessage>(&mut too_large.as_slice(), WIRE_FRAME_MAX, None).await,
            Err(FrameError::TooLarge {
                cap: WIRE_FRAME_MAX,
                ..
            })
        ));
    }

    #[test]
    fn id_rules_match_on_both_sides() {
        for id in ["", "has space", "has.period", "é", "m\0"] {
            let original = ChatMessage {
                id: id.into(),
                ..example()
            };
            assert_eq!(original.validate(), Err(NackReason::InvalidId));
            assert!(serde_json::to_string(&original).is_err());
            let wire: WireChatMessage =
                serde_json::from_value(json!({"id": id, "sent_at_ms": 0, "body": ""})).unwrap();
            assert_eq!(ChatMessage::try_from(wire), Err(NackReason::InvalidId));
        }
        let id = "a".repeat(CHAT_ID_MAX + 1);
        let original = ChatMessage {
            id: id.clone(),
            ..example()
        };
        assert_eq!(original.validate(), Err(NackReason::InvalidId));
        let wire = WireChatMessage {
            id,
            sent_at_ms: 0,
            body: String::new(),
        };
        assert_eq!(ChatMessage::try_from(wire), Err(NackReason::InvalidId));
    }

    #[test]
    fn body_limit_counts_utf8_bytes_before_encoding_and_after_decoding() {
        for len in [CHAT_BODY_MAX + 1, CHAT_BODY_MAX + 3] {
            let body = format!("{}a", "é".repeat(len / 2));
            assert_eq!(body.len(), len);
            let wire = WireChatMessage {
                id: "m".into(),
                sent_at_ms: 0,
                body: STANDARD.encode(&body),
            };
            assert_eq!(ChatMessage::try_from(wire), Err(NackReason::BodyTooLarge));
            let original = ChatMessage { body, ..example() };
            assert_eq!(original.validate(), Err(NackReason::BodyTooLarge));
            assert!(serde_json::to_string(&original).is_err());
        }
    }

    #[test]
    fn rejects_malformed_base64_and_non_utf8_bytes() {
        for body in ["!abc", "YQ", "YQ=", "YR==", "YQ==\n", "_w==", "/w=="] {
            let wire = WireChatMessage {
                id: "m".into(),
                sent_at_ms: 0,
                body: body.into(),
            };
            assert_eq!(ChatMessage::try_from(wire), Err(NackReason::InvalidBody));
        }
    }

    #[test]
    fn replies_have_typed_reasons_independent_of_field_order() {
        assert_eq!(
            serde_json::to_string(&ChatReply::Ack).unwrap(),
            r#"{"type":"ack"}"#
        );
        let reply = ChatReply::Nack(NackReason::InvalidId);
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"type":"nack","reason":"invalid_id"}"#
        );
        assert_eq!(
            serde_json::from_str::<ChatReply>(
                r#"{"reason":"invalid_id","type":"nack","extra":true}"#
            )
            .unwrap(),
            reply
        );
        for malformed in [
            r#"{"reason":[[[]]],"type":"nack"}"#,
            r#"{"reason":{"type":"invalid_id"},"type":"nack"}"#,
            r#"{"type":"nack"}"#,
            r#"{"type":"ack","reason":"invalid_id"}"#,
        ] {
            assert!(serde_json::from_str::<ChatReply>(malformed).is_err());
        }
    }

    #[test]
    fn debug_output_excludes_message_data() {
        let original = ChatMessage {
            id: "example-id".into(),
            body: "example body".into(),
            ..example()
        };
        let debug = format!("{original:?}");
        assert!(!debug.contains(&original.id));
        assert!(!debug.contains(&original.body));
    }

    #[test]
    fn receipts_use_local_peer_name_and_message_id() {
        let mut receipts = ReceiptWindow::new(3);
        assert!(!receipts.contains("sender", "message"));
        assert!(receipts.record("sender", "message"));
        assert!(receipts.contains("sender", "message"));
        assert!(!receipts.contains("other", "message"));
        assert!(!receipts.contains("sender", "another"));
        // The caller resolves a retry from another station of this peer to the same local name.
        assert!(!receipts.record("sender", "message"));
        assert!(receipts.record("other", "message"));
        assert!(receipts.record("sender", "another"));
        assert!(!receipts.record("other", "message"));

        let debug = format!("{receipts:?}");
        for key in ["sender", "other", "message", "another"] {
            assert!(!debug.contains(key));
        }
    }

    #[test]
    fn global_fifo_evicts_first_receipts_without_refresh_on_retry() {
        let mut receipts = ReceiptWindow::new(2);
        assert!(receipts.record("sender", "first"));
        assert!(receipts.record("other", "second"));
        assert!(receipts.contains("sender", "first"));
        assert!(!receipts.record("sender", "first"));
        assert!(receipts.record("other", "third"));
        assert!(!receipts.contains("sender", "first"));
        assert!(receipts.contains("other", "second"));
        assert_eq!(receipts.order.len(), 2);
        assert_eq!(receipts.seen.len(), 2);
        assert!(!receipts.record("other", "second"));
        assert!(receipts.record("sender", "first"));
        assert!(receipts.record("other", "second"));
        assert_eq!(receipts.order.len(), 2);
        assert_eq!(receipts.seen.len(), 2);
    }

    #[test]
    fn new_windows_start_empty_and_zero_disables_retention() {
        let mut receipts = ReceiptWindow::new(1);
        assert!(receipts.record("sender", "message"));
        assert!(!receipts.record("sender", "message"));

        let mut reset = ReceiptWindow::new(1);
        assert!(reset.record("sender", "message"));
        let mut disabled = ReceiptWindow::new(0);
        assert!(disabled.record("sender", "message"));
        assert!(disabled.record("sender", "message"));
        assert!(disabled.order.is_empty());
        assert!(disabled.seen.is_empty());

        let large = ReceiptWindow::new(usize::MAX);
        assert_eq!(large.order.capacity(), 0);
        assert_eq!(large.seen.capacity(), 0);
    }
}
