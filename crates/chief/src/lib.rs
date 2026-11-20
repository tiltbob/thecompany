//! `chief`: the shared frontend library.
//!
//! It will hold the station API (`chief::api`: requests, responses, events, all FFI-exportable) and
//! the client that speaks it over the station's local socket. It depends on `numbers-base` only,
//! never on `numbers` or iroh: frontends hold no network state.

/// The station API version a frontend announces in its hello (`{api: 1}`).
pub const API_VERSION: u32 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_version_is_pinned() {
        assert_eq!(API_VERSION, 1);
    }
}
