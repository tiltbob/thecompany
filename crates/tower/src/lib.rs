//! `numbers-tower`: the tower, an untrusted relay node, as an embeddable library.
//!
//! It will wrap the iroh relay server in plain-HTTP mode with a per-client rate limit (M1), then
//! TLS, QAD and an allowlist (M2), behind a `Tower` type that exposes no `iroh_relay` type. A tower
//! sees ciphertext only: this crate never contains sealing or key code.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

/// Where the dev tower listens when no `--bind` is given. Loopback only in M1.
pub const DEFAULT_BIND: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 3340));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_bind_is_loopback() {
        assert!(DEFAULT_BIND.ip().is_loopback());
        assert_eq!(DEFAULT_BIND.port(), 3340);
    }
}
