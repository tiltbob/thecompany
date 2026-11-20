//! Control frames for the `numbers/0` link and its typed streams.

use std::fmt;

use serde::{Deserialize, Serialize};

pub mod chat;
pub mod towers;

use chat::NackReason;

/// Initial link hello version.
pub const LINK_VERSION: u32 = 1;
/// Initial chat stream version.
pub const CHAT_VERSION: u32 = 1;
/// Stream protocol name for chat.
pub const CHAT_PROTOCOL: &str = "chat";
/// Initial tower-announcement stream version.
pub const TOWERS_VERSION: u32 = 1;
/// Stream protocol name for tower announcements.
pub const TOWERS_PROTOCOL: &str = "towers";

/// The hello exchanged once on each authenticated link.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub v: u32,
    /// Reserved for a network id. Empty in M1.
    pub network: String,
    /// Reserved for a roster head. Empty in M1.
    pub head: String,
    /// Random generation of the running link instance.
    pub generation: [u8; 16],
    /// Fresh challenge for this handshake.
    pub challenge: [u8; 16],
    /// The client challenge, echoed by the server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub echo: Option<[u8; 16]>,
}

impl Hello {
    /// A hello for the current link version.
    pub fn local(generation: [u8; 16], challenge: [u8; 16]) -> Self {
        Self {
            v: LINK_VERSION,
            network: String::new(),
            head: String::new(),
            generation,
            challenge,
            echo: None,
        }
    }

    /// Checks the version, reserved fields, and nonzero generation and challenge.
    pub fn validate(&self) -> Result<(), HelloError> {
        if self.v != LINK_VERSION {
            return Err(HelloError::UnsupportedVersion);
        }
        if !self.network.is_empty() || !self.head.is_empty() {
            return Err(HelloError::ReservedFields);
        }
        if self.generation == [0; 16] || self.challenge == [0; 16] {
            return Err(HelloError::InvalidNonce);
        }
        Ok(())
    }
}

impl fmt::Debug for Hello {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hello")
            .field("v", &self.v)
            .finish_non_exhaustive()
    }
}

/// An incompatible link hello.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HelloError {
    #[error("unsupported link version")]
    UnsupportedVersion,
    #[error("link hello has nonempty reserved fields")]
    ReservedFields,
    #[error("link hello has a zero generation or challenge")]
    InvalidNonce,
}

/// The client echoes the server challenge to complete the link hello.
#[derive(Serialize, Deserialize)]
pub struct HelloEcho {
    pub echo: [u8; 16],
}

/// The first frame on a typed stream opened after the link hello.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamHeader {
    pub proto: String,
    pub v: u32,
}

impl StreamHeader {
    /// A header for the current chat version.
    pub fn chat() -> Self {
        Self {
            proto: CHAT_PROTOCOL.into(),
            v: CHAT_VERSION,
        }
    }

    /// A header for the current tower-announcement version.
    pub fn towers() -> Self {
        Self {
            proto: TOWERS_PROTOCOL.into(),
            v: TOWERS_VERSION,
        }
    }

    /// Checks that the stream uses the supported chat protocol and version.
    pub fn validate_chat(&self) -> Result<(), NackReason> {
        if self.proto != CHAT_PROTOCOL {
            return Err(NackReason::UnknownProtocol);
        }
        if self.v != CHAT_VERSION {
            return Err(NackReason::UnsupportedVersion);
        }
        Ok(())
    }
}

impl fmt::Debug for StreamHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamHeader")
            .field("v", &self.v)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use numbers_base::{WIRE_FRAME_MAX, read_frame, write_frame};

    #[tokio::test]
    async fn hello_and_stream_header_use_versioned_json_frames() {
        let mut frames = Vec::new();
        write_frame(&mut frames, &Hello::local([1; 16], [2; 16]), WIRE_FRAME_MAX)
            .await
            .unwrap();
        write_frame(&mut frames, &StreamHeader::chat(), WIRE_FRAME_MAX)
            .await
            .unwrap();

        let mut reader = frames.as_slice();
        let hello: Hello = read_frame(&mut reader, WIRE_FRAME_MAX, None).await.unwrap();
        let header: StreamHeader = read_frame(&mut reader, WIRE_FRAME_MAX, None).await.unwrap();
        assert_eq!(hello, Hello::local([1; 16], [2; 16]));
        assert_eq!(header, StreamHeader::chat());
        assert_eq!(hello.validate(), Ok(()));
        assert_eq!(header.validate_chat(), Ok(()));
        assert!(reader.is_empty());
    }

    #[test]
    fn rejects_incompatible_values_and_accepts_additional_fields() {
        let mut json = serde_json::to_value(Hello::local([1; 16], [2; 16])).unwrap();
        json["extra"] = true.into();
        let mut malformed = json.clone();
        malformed["generation"] = serde_json::to_value([1; 15]).unwrap();
        assert!(serde_json::from_value::<Hello>(malformed).is_err());
        let mut hello: Hello = serde_json::from_value(json).unwrap();
        hello.generation = [0; 16];
        assert_eq!(hello.validate(), Err(HelloError::InvalidNonce));
        hello.generation = [1; 16];
        hello.challenge = [0; 16];
        assert_eq!(hello.validate(), Err(HelloError::InvalidNonce));
        hello.challenge = [2; 16];
        assert_eq!(hello.validate(), Ok(()));
        hello.v = LINK_VERSION + 1;
        assert_eq!(hello.validate(), Err(HelloError::UnsupportedVersion));
        hello.v = LINK_VERSION;
        hello.network = "reserved".into();
        assert_eq!(hello.validate(), Err(HelloError::ReservedFields));
        hello.network.clear();
        hello.head = "reserved".into();
        assert_eq!(hello.validate(), Err(HelloError::ReservedFields));
        assert!(!format!("{hello:?}").contains("reserved"));

        let mut header: StreamHeader =
            serde_json::from_str(r#"{"v":1,"proto":"chat","extra":true}"#).unwrap();
        assert_eq!(header.validate_chat(), Ok(()));
        header.v = CHAT_VERSION + 1;
        assert_eq!(header.validate_chat(), Err(NackReason::UnsupportedVersion));
        header.v = CHAT_VERSION;
        header.proto = "future".into();
        assert_eq!(header.validate_chat(), Err(NackReason::UnknownProtocol));
        assert!(!format!("{header:?}").contains("future"));
    }
}
