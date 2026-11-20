//! `numbers-base`: the leaf crate every other crate may depend on, frontends included.
//!
//! It will hold the frame codec (`write_frame` / `read_frame`: a u32 length and JSON, with the size
//! cap and read deadline as parameters) and the key-file helper for daemons; later the roster
//! codec. It never depends on iroh or any network code.

/// Largest frame accepted from a peer over the network.
pub const WIRE_FRAME_MAX: usize = 64 * 1024;

/// Largest frame accepted on the station's local socket.
pub const LOCAL_FRAME_MAX: usize = 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_caps_are_pinned() {
        assert_eq!(WIRE_FRAME_MAX, 65_536);
        assert_eq!(LOCAL_FRAME_MAX, 1_048_576);
        const { assert!(WIRE_FRAME_MAX < LOCAL_FRAME_MAX) };
        // Both must be expressible in the u32 length prefix.
        assert!(u32::try_from(WIRE_FRAME_MAX).is_ok());
        assert!(u32::try_from(LOCAL_FRAME_MAX).is_ok());
    }
}
