//! `numbers-base`: the leaf crate every other crate may depend on, frontends included.
//!
//! It holds the frame codec ([`frame`]: `write_frame` / `read_frame`, a big-endian u32 length and
//! JSON, with the size cap and the read deadline as parameters) and, on Unix, the private-directory
//! and key-file helpers for daemons ([`keyfile`]); later the roster codec. It never depends on iroh
//! or any network code, and it holds no process-global state.

pub mod frame;
#[cfg(unix)]
pub mod keyfile;

pub use frame::{
    FrameError, LOCAL_FRAME_MAX, WIRE_FRAME_MAX, encode_frame, read_frame, write_frame,
};
#[cfg(unix)]
pub use keyfile::{
    KeyOrigin, PrivatePathError, StoredKey, check_private_dir, create_key_file, ensure_private_dir,
    load_or_create_key, load_or_create_key_in, read_key_file,
};
