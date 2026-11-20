//! Node identity and key generation.

use iroh::SecretKey;

/// A node's id is the public half of its node key.
pub type NodeId = iroh::EndpointId;

/// Generates a node key for the caller to keep in its key store.
pub fn generate_node_key() -> [u8; 32] {
    SecretKey::generate().to_bytes()
}

/// Derives the public node id from its private key bytes.
pub fn node_id(key: &[u8; 32]) -> NodeId {
    SecretKey::from_bytes(key).public()
}

#[cfg(test)]
mod tests {
    use super::{NodeId, generate_node_key, node_id};

    #[test]
    fn node_id_round_trips_through_its_string_form() {
        let id = node_id(&generate_node_key());
        assert!(
            id.to_string()
                .parse::<NodeId>()
                .is_ok_and(|parsed| parsed == id)
        );
    }
}
