//! Authorization by authenticated node id and protocol.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};

use iroh::endpoint::{AfterHandshakeOutcome, Connection, EndpointHooks, Incoming, IncomingAddr};
use iroh::protocol::IncomingFilterOutcome;

use crate::NodeId;

/// Application close code for an authorization refusal.
pub const CLOSE_NOT_ALLOWED: u32 = 403;

/// The protocol a peer is trying to use.
#[derive(Clone, Copy)]
pub enum Protocol<'a> {
    /// The negotiated protocol for a connection.
    Alpn(&'a [u8]),
    /// The requested protocol for a stream on an authenticated connection.
    Stream(&'a str),
}

/// The protocols a station offers and the peers it currently authorizes.
///
/// The station supplies peer names from its local peer book. A peer cannot name itself.
pub struct Authorizer {
    alpns: &'static [&'static [u8]],
    stream_protocols: &'static [&'static str],
    peers: RwLock<HashMap<NodeId, Arc<str>>>,
}

impl Authorizer {
    /// Creates a policy with no authorized peers and the given protocol names.
    pub fn new(alpns: &'static [&'static [u8]], stream_protocols: &'static [&'static str]) -> Self {
        Self {
            alpns,
            stream_protocols,
            peers: RwLock::default(),
        }
    }

    /// Atomically replaces the station's known peers.
    ///
    /// The station is responsible for closing any existing links it revokes.
    pub fn replace_peers(&self, peers: impl IntoIterator<Item = (NodeId, String)>) {
        let peers = peers
            .into_iter()
            .map(|(id, name)| (id, Arc::from(name)))
            .collect();
        let mut current = self.peers.write().unwrap_or_else(PoisonError::into_inner);
        *current = peers;
        self.peers.clear_poison();
    }

    /// Returns the local name of a peer allowed to use the given protocol.
    ///
    /// Call this with the id from a completed TLS handshake.
    pub fn authorize(&self, peer: &NodeId, protocol: Protocol<'_>) -> Option<Arc<str>> {
        let supported = match protocol {
            Protocol::Alpn(alpn) => self.alpns.contains(&alpn),
            Protocol::Stream(name) => self.stream_protocols.contains(&name),
        };
        if !supported {
            return None;
        }
        self.peers.read().ok()?.get(peer).cloned()
    }

    fn knows(&self, peer: &NodeId) -> bool {
        self.peers
            .read()
            .is_ok_and(|peers| peers.contains_key(peer))
    }
}

impl fmt::Debug for Authorizer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Authorizer").finish_non_exhaustive()
    }
}

/// The hook and incoming filter share one policy and hold no endpoint or connection handles.
#[derive(Clone, Debug)]
pub(crate) struct Gate {
    pub(crate) authorizer: Arc<Authorizer>,
}

impl Gate {
    pub(crate) fn filter(&self, incoming: &Incoming) -> IncomingFilterOutcome {
        match incoming.remote_addr() {
            // A relay can forge this id. The hook still checks the authenticated id.
            IncomingAddr::Relay { endpoint_id, .. } if self.authorizer.knows(&endpoint_id) => {
                IncomingFilterOutcome::Accept
            }
            // A retry token proves reachability at the source address.
            IncomingAddr::Ip(_) if !incoming.remote_addr_validated() => {
                IncomingFilterOutcome::Retry
            }
            IncomingAddr::Ip(_) => IncomingFilterOutcome::Accept,
            _ => IncomingFilterOutcome::Ignore,
        }
    }
}

impl EndpointHooks for Gate {
    async fn after_handshake<'a>(&'a self, conn: &'a Connection) -> AfterHandshakeOutcome {
        // iroh has already registered the connection and may advertise address candidates.
        if self
            .authorizer
            .authorize(&conn.remote_id(), Protocol::Alpn(conn.alpn()))
            .is_some()
        {
            AfterHandshakeOutcome::Accept
        } else {
            AfterHandshakeOutcome::Reject {
                error_code: CLOSE_NOT_ALLOWED.into(),
                reason: Vec::new(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Authorizer, Protocol};
    use crate::{generate_node_key, node_id};

    const ALPN: &[u8] = b"test/0";
    const STREAM: &str = "test";

    #[test]
    fn authorizes_only_known_peers_and_configured_protocols() {
        let authorizer = Authorizer::new(&[ALPN], &[STREAM]);
        let peer = node_id(&generate_node_key());
        assert!(authorizer.authorize(&peer, Protocol::Alpn(ALPN)).is_none());

        authorizer.replace_peers([(peer, "peer".into())]);
        assert_eq!(
            authorizer.authorize(&peer, Protocol::Alpn(ALPN)).as_deref(),
            Some("peer"),
        );
        assert_eq!(
            authorizer
                .authorize(&peer, Protocol::Stream(STREAM))
                .as_deref(),
            Some("peer"),
        );
        assert!(
            authorizer
                .authorize(&peer, Protocol::Alpn(STREAM.as_bytes()))
                .is_none()
        );
        assert!(
            authorizer
                .authorize(&peer, Protocol::Stream("test/0"))
                .is_none()
        );

        authorizer.replace_peers([]);
        assert!(authorizer.authorize(&peer, Protocol::Alpn(ALPN)).is_none());
    }

    #[test]
    fn poisoned_peer_lock_denies_until_the_snapshot_is_replaced() {
        let authorizer = Authorizer::new(&[ALPN], &[STREAM]);
        let peer = node_id(&generate_node_key());
        authorizer.replace_peers([(peer, "peer".into())]);

        let result = std::panic::catch_unwind(|| {
            let _guard = authorizer.peers.write().unwrap();
            panic!("poison peer snapshot for the test");
        });
        assert!(result.is_err());
        assert!(!authorizer.knows(&peer));
        assert!(authorizer.authorize(&peer, Protocol::Alpn(ALPN)).is_none());

        authorizer.replace_peers([(peer, "updated name".into())]);
        assert!(authorizer.knows(&peer));
        assert_eq!(
            authorizer.authorize(&peer, Protocol::Alpn(ALPN)).as_deref(),
            Some("updated name"),
        );
    }
}
