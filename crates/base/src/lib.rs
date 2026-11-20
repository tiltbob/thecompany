//! `numbers-base`: the leaf crate every other crate may depend on, frontends included.
//!
//! It will hold the frame codec (`write_frame` / `read_frame`: a u32 length and JSON, with the size
//! cap and read deadline as parameters) and the key-file helper for daemons; later the roster
//! codec. It never depends on iroh or any network code.

use std::fmt;

/// Largest frame accepted from a peer over the network.
pub const WIRE_FRAME_MAX: usize = 64 * 1024;

/// Largest frame accepted on the station's local socket.
pub const LOCAL_FRAME_MAX: usize = 1024 * 1024;

/// A frame header announced more bytes than the cap allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameTooLarge {
    /// The length the header announced.
    pub len: u64,
    /// The cap in force.
    pub cap: usize,
}

impl fmt::Display for FrameTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "frame of {} bytes exceeds the cap of {} bytes",
            self.len, self.cap
        )
    }
}

impl std::error::Error for FrameTooLarge {}

/// Checks a frame length read from a u32 header against `cap` before any body byte is read.
pub fn check_frame_len(len: u32, cap: usize) -> Result<usize, FrameTooLarge> {
    match usize::try_from(len) {
        Ok(n) if n <= cap => Ok(n),
        _ => Err(FrameTooLarge {
            len: u64::from(len),
            cap,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_len_is_checked_against_the_cap() {
        let wire = u32::try_from(WIRE_FRAME_MAX).unwrap();
        assert_eq!(check_frame_len(0, WIRE_FRAME_MAX), Ok(0));
        assert_eq!(check_frame_len(wire, WIRE_FRAME_MAX), Ok(WIRE_FRAME_MAX));
        assert_eq!(
            check_frame_len(wire + 1, WIRE_FRAME_MAX),
            Err(FrameTooLarge {
                len: u64::from(wire) + 1,
                cap: WIRE_FRAME_MAX
            })
        );
        assert!(check_frame_len(wire + 1, LOCAL_FRAME_MAX).is_ok());
        assert!(check_frame_len(u32::MAX, LOCAL_FRAME_MAX).is_err());
    }
}
