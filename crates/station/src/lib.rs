//! `numbers-station`: the station core, a chief's node, as a library.
//!
//! It will hold `Station::start`: one actor owning the endpoint, the peer book and the message log,
//! behind an exclusive lock on the data dir, plus the local-socket server that frontends talk to.
//! It is usable in-process as a `chief` transport.
//!
//! - [`ipc`] (Unix): the local-socket server, generic over any `chief` transport.

#[cfg(unix)]
pub mod ipc;

/// How many messages the station keeps: a ring of the newest 1,000.
pub const MESSAGE_LOG_CAPACITY: usize = 1_000;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_log_capacity_is_pinned() {
        assert_eq!(MESSAGE_LOG_CAPACITY, 1_000);
    }
}
