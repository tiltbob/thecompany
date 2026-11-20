//! M2 (section 11): the static allowlist as an `iroh_relay::server::AccessControl`. Also counts
//! every `on_connect` call (allowed or denied, with a timestamp) so tests can see how often a
//! denied endpoint retries.
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Instant;

use iroh::EndpointId;
use iroh_relay::server::{Access, AccessControl, ClientRequest, ConnectionId};

#[derive(Debug)]
pub struct Members {
    ids: RwLock<HashSet<EndpointId>>,
    /// Admit everyone but still log (a counting AllowAll).
    open: AtomicBool,
    reason: Option<String>,
    log: Mutex<Vec<(Instant, EndpointId, bool)>>,
    disconnects: AtomicUsize,
}

impl Members {
    pub fn new(ids: impl IntoIterator<Item = EndpointId>, reason: Option<&str>) -> Self {
        Self {
            ids: RwLock::new(ids.into_iter().collect()),
            open: AtomicBool::new(false),
            reason: reason.map(str::to_owned),
            log: Mutex::new(Vec::new()),
            disconnects: AtomicUsize::new(0),
        }
    }

    pub fn open() -> Self {
        let m = Self::new([], None);
        m.open.store(true, Ordering::SeqCst);
        m
    }

    pub fn insert(&self, id: EndpointId) {
        if let Ok(mut s) = self.ids.write() {
            s.insert(id);
        }
    }

    pub fn remove(&self, id: &EndpointId) {
        if let Ok(mut s) = self.ids.write() {
            s.remove(id);
        }
    }

    fn allowed(&self, id: &EndpointId) -> bool {
        self.open.load(Ordering::SeqCst) || self.ids.read().map(|s| s.contains(id)).unwrap_or(false)
    }

    /// (time, allowed) of every on_connect from `id`.
    pub fn attempts(&self, id: &EndpointId) -> Vec<(Instant, bool)> {
        self.log
            .lock()
            .map(|v| {
                v.iter()
                    .filter(|(_, i, _)| i == id)
                    .map(|(t, _, a)| (*t, *a))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn total(&self) -> (usize, usize) {
        self.log
            .lock()
            .map(|v| {
                let ok = v.iter().filter(|(_, _, a)| *a).count();
                (ok, v.len() - ok)
            })
            .unwrap_or_default()
    }

    pub fn disconnects(&self) -> usize {
        self.disconnects.load(Ordering::SeqCst)
    }
}

impl AccessControl for Members {
    async fn on_connect(&self, req: &ClientRequest) -> Access {
        let id = req.endpoint_id();
        let allowed = self.allowed(&id);
        if let Ok(mut v) = self.log.lock() {
            v.push((Instant::now(), id, allowed));
        }
        if allowed {
            Access::Allow
        } else {
            Access::Deny {
                reason: self.reason.clone(),
            }
        }
    }

    fn on_disconnect(&self, _id: EndpointId, _conn: ConnectionId) {
        self.disconnects.fetch_add(1, Ordering::SeqCst);
    }
}
