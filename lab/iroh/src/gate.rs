//! Section 6: the gate (pre-handshake filter plus after_handshake hook).
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use iroh::EndpointId;
use iroh::endpoint::{
    AfterHandshakeOutcome, Connection, EndpointHooks, Incoming, IncomingAddr, Side, VarInt,
};
use iroh::protocol::{IncomingFilter, IncomingFilterOutcome};

pub const LINK_ALPN: &[u8] = b"numbers/0";
pub const OTHER_ALPN: &[u8] = b"numbers/join/0";
pub const CLOSE_NOT_ALLOWED: u32 = 403;

/// The set of allowed peer ids.
#[derive(Debug, Default)]
pub struct Authorizer(RwLock<HashSet<EndpointId>>);

impl Authorizer {
    pub fn new(ids: impl IntoIterator<Item = EndpointId>) -> Self {
        Self(RwLock::new(ids.into_iter().collect()))
    }
    pub fn contains(&self, id: &EndpointId) -> bool {
        self.0.read().map(|s| s.contains(id)).unwrap_or(false)
    }
    pub fn insert(&self, id: EndpointId) {
        if let Ok(mut s) = self.0.write() {
            s.insert(id);
        }
    }
}

/// One `after_handshake` call, recorded for tests.
#[derive(Debug, Clone)]
pub struct HookCall {
    pub side: Side,
    pub alpn: Vec<u8>,
    pub remote: EndpointId,
    pub rejected: bool,
}

#[derive(Debug, Default)]
pub struct HookLog(Mutex<Vec<HookCall>>);

impl HookLog {
    pub fn calls(&self) -> Vec<HookCall> {
        self.0.lock().map(|v| v.clone()).unwrap_or_default()
    }
    fn push(&self, call: HookCall) {
        if let Ok(mut v) = self.0.lock() {
            v.push(call);
        }
    }
}

/// ALPN-aware gate: only `numbers/0` needs the remote in the authorizer.
/// Must not hold an `Endpoint` (reference cycle).
#[derive(Debug)]
pub struct Gate {
    pub auth: Arc<Authorizer>,
    pub log: Arc<HookLog>,
    /// false = the ALPN-unaware variant the design warns against (T4c).
    pub alpn_aware: bool,
}

impl EndpointHooks for Gate {
    async fn after_handshake<'a>(&'a self, conn: &'a Connection) -> AfterHandshakeOutcome {
        let alpn: &[u8] = conn.alpn(); // alpn() -> &[u8]
        let gated = !self.alpn_aware || alpn == LINK_ALPN;
        let allowed = !gated || self.auth.contains(&conn.remote_id());
        self.log.push(HookCall {
            side: conn.side(),
            alpn: alpn.to_vec(),
            remote: conn.remote_id(),
            rejected: !allowed,
        });
        if allowed {
            AfterHandshakeOutcome::Accept
        } else {
            AfterHandshakeOutcome::Reject {
                error_code: VarInt::from_u32(CLOSE_NOT_ALLOWED),
                reason: b"not allowed".to_vec(),
            }
        }
    }
}

/// Counters for the incoming filter (tests only).
#[derive(Debug, Default)]
pub struct FilterLog {
    pub seen: AtomicUsize,
    pub ignored: AtomicUsize,
}

/// The pre-handshake filter. `enforce = false` lets everything through (T4b).
pub fn incoming_filter(
    auth: Arc<Authorizer>,
    log: Arc<FilterLog>,
    enforce: bool,
) -> IncomingFilter {
    Arc::new(move |inc: &Incoming| {
        log.seen.fetch_add(1, Ordering::SeqCst);
        match inc.remote_addr() {
            IncomingAddr::Relay { endpoint_id, .. } if enforce && !auth.contains(&endpoint_id) => {
                log.ignored.fetch_add(1, Ordering::SeqCst);
                IncomingFilterOutcome::Ignore
            }
            IncomingAddr::Ip(_) if !inc.remote_addr_validated() => IncomingFilterOutcome::Retry,
            _ => IncomingFilterOutcome::Accept,
        }
    })
}
