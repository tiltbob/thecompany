//! `chief`: the shared frontend library.
//!
//! It will hold the station API (`chief::api`: requests, responses, events, all FFI-exportable) and
//! the client that speaks it over the station's local socket. It depends on `numbers-base` only,
//! never on `numbers` or iroh: frontends hold no network state.

/// The station API version a frontend announces in its hello (`{api: 1}`).
pub const API_VERSION: u32 = 1;

/// Whether a station answering with API version `station` can serve this frontend.
pub fn api_compatible(station: u32) -> bool {
    station == API_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_same_api_version_is_compatible() {
        assert_eq!(API_VERSION, 1);
        assert!(api_compatible(API_VERSION));
        assert!(!api_compatible(API_VERSION + 1));
        assert!(!api_compatible(0));
    }
}
