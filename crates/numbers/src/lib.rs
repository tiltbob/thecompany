//! `numbers`: the network library.
//!
//! It will hold node identity, the tower set, the iroh endpoint and its gate (the one
//! authorization point), the link between two nodes, and the protocols that run over it (chat
//! first). It takes iroh without default features; see the dependency notes in
//! `docs/ARCHITECTURE.md`.

/// The ALPN every numbers link is opened with. The trailing digit is the link protocol version.
pub const ALPN: &[u8] = b"numbers/0";

/// A node's id: the public half of its node key.
pub type NodeId = iroh::EndpointId;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alpn_is_versioned() {
        assert_eq!(ALPN, b"numbers/0");
    }

    #[test]
    fn node_id_round_trips_through_its_string_form() {
        let key = iroh::SecretKey::generate();
        let id: NodeId = key.public();
        let text = id.to_string();
        let parsed: NodeId = text.parse().unwrap();
        assert_eq!(parsed, id);
        // A key rebuilt from its bytes has the same id.
        assert_eq!(iroh::SecretKey::from_bytes(&key.to_bytes()).public(), id);
    }
}
