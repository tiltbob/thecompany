//! Per-transport tower announcements and the application handoff.

use iroh::endpoint::{RecvStream, SendStream};
use numbers_base::{WIRE_FRAME_MAX, read_frame, write_frame};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

use crate::proto::towers::{TowerAnnouncement, TowerNackReason, TowerReply, WireTowerAnnouncement};
use crate::proto::{StreamHeader, TOWERS_PROTOCOL, TOWERS_VERSION};
use crate::{Authorizer, CLOSE_NOT_ALLOWED, NodeId, Protocol, TowerSet};

use super::{CLOSE_LINK, EstablishedConnection, MAILBOX_CAPACITY, Peer, PeerState, Shared, lock};

type WorkingTowers = Arc<Mutex<Arc<TowerAnnouncement>>>;

/// Sets the local working-tower snapshot and receives whole announcements from peers.
pub struct TowerExchange {
    working: WorkingTowers,
    incoming: mpsc::Receiver<IncomingTowers>,
}

impl TowerExchange {
    /// Replaces the HTTPS projection captured on future admissions.
    /// Existing links do not send another announcement when this changes.
    pub fn set_working_towers(&self, set: &TowerSet) {
        let announcement = Arc::new(TowerAnnouncement::from_working_set(set));
        *lock(&self.working) = announcement;
    }

    /// Receives an announcement. Cancelling this wait leaves any queued item available.
    /// Returns `None` once the link shuts down and the mailbox drains.
    pub async fn recv(&mut self) -> Option<IncomingTowers> {
        self.incoming.recv().await
    }
}

impl fmt::Debug for TowerExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TowerExchange").finish_non_exhaustive()
    }
}

/// A validated announcement awaiting the station's decision.
pub struct IncomingTowers {
    peer: Weak<Peer>,
    connection: Weak<EstablishedConnection>,
    authorizer: Weak<Authorizer>,
    name: Arc<str>,
    announcement: TowerAnnouncement,
    reply: oneshot::Sender<TowerReply>,
}

impl IncomingTowers {
    /// Runs `commit` only while this station and its exact admitted transport remain current.
    ///
    /// The callback runs synchronously under the link's peer lock. It must check the station's
    /// current peer book and return `true` only after committing the entire snapshot. Keep it
    /// short and make no Link calls from it.
    ///
    /// Returns whether the snapshot was committed. The sender can lose the acknowledgement
    /// even when the callback commits successfully.
    pub fn accept_if_current(
        self,
        commit: impl FnOnce(NodeId, &str, TowerAnnouncement) -> bool,
    ) -> bool {
        let accepted = (|| {
            let peer = self.peer.upgrade()?;
            let connection = self.connection.upgrade()?;
            let authorizer = self.authorizer.upgrade()?;
            let state = peer.state();
            if !is_current(&state, &connection) {
                return None;
            }
            let caller = authorizer.authorize(&peer.id, Protocol::Stream(TOWERS_PROTOCOL))?;
            if caller != self.name {
                return None;
            }
            let accepted = commit(peer.id, &caller, self.announcement);
            drop(state);
            Some(accepted)
        })()
        .unwrap_or(false);
        let _ = self.reply.send(if accepted {
            TowerReply::Ack
        } else {
            unavailable()
        });
        accepted
    }
}

impl fmt::Debug for IncomingTowers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncomingTowers").finish_non_exhaustive()
    }
}

pub(super) struct TowerProtocol {
    working: WorkingTowers,
    incoming: Mutex<Option<mpsc::Sender<IncomingTowers>>>,
}

impl TowerProtocol {
    pub(super) fn new(set: &TowerSet) -> (Self, TowerExchange) {
        let working = Arc::new(Mutex::new(Arc::new(TowerAnnouncement::from_working_set(
            set,
        ))));
        let (send, incoming) = mpsc::channel(MAILBOX_CAPACITY);
        (
            Self {
                working: Arc::clone(&working),
                incoming: Mutex::new(Some(send)),
            },
            TowerExchange { working, incoming },
        )
    }

    pub(super) fn snapshot(&self) -> Arc<TowerAnnouncement> {
        Arc::clone(&lock(&self.working))
    }

    pub(super) fn close(&self) {
        lock(&self.incoming).take();
    }
}

pub(super) async fn announce(
    shared: &Shared,
    peer: &Peer,
    connection: &Arc<EstablishedConnection>,
    snapshot: Arc<TowerAnnouncement>,
) {
    let budget = shared
        .config
        .send_deadline
        .saturating_sub(connection.admitted_at.elapsed());
    if budget.is_zero() {
        return;
    }
    let can_announce = || {
        let state = peer.state();
        is_current(&state, connection)
            && shared
                .authorizer
                .authorize(&peer.id, Protocol::Stream(TOWERS_PROTOCOL))
                .is_some()
    };
    let attempt = async {
        if !can_announce() {
            return;
        }
        let Ok((mut send, mut recv)) = connection.transport.open_bi().await else {
            return;
        };
        if !can_announce() {
            return;
        }
        let wire = WireTowerAnnouncement::from(snapshot.as_ref());
        if write_frame(&mut send, &StreamHeader::towers(), WIRE_FRAME_MAX)
            .await
            .is_err()
            || write_frame(&mut send, &wire, WIRE_FRAME_MAX).await.is_err()
        {
            return;
        }
        let _ = send.finish();
        let _: Result<TowerReply, _> = read_frame(&mut recv, WIRE_FRAME_MAX, Some(budget)).await;
    };
    let _ = timeout(budget, attempt).await;
}

pub(super) async fn write_tower_reply(
    exchange: &TowerProtocol,
    shared: &Shared,
    peer: &Arc<Peer>,
    connection: &Arc<EstablishedConnection>,
    version: u32,
    send: &mut SendStream,
    recv: &mut RecvStream,
) -> Result<(), u32> {
    let protocol = Protocol::Stream(TOWERS_PROTOCOL);
    if shared.authorizer.authorize(&peer.id, protocol).is_none() {
        return Err(CLOSE_NOT_ALLOWED);
    }
    // Limit tower body reads and waits for mailbox space or the actor to one per transport.
    let permit = if version == TOWERS_VERSION {
        connection.tower_receive.try_acquire().ok()
    } else {
        None
    };
    let reply = match (version, &permit) {
        (TOWERS_VERSION, Some(_)) => {
            accept_announcement(exchange, shared, peer, connection, recv).await?
        }
        (TOWERS_VERSION, None) => unavailable(),
        _ => TowerReply::Nack {
            reason: TowerNackReason::UnsupportedVersion,
        },
    };
    let result = write_frame(send, &reply, WIRE_FRAME_MAX).await;
    drop(permit);
    result.map_err(|_| CLOSE_LINK)
}

async fn accept_announcement(
    exchange: &TowerProtocol,
    shared: &Shared,
    peer: &Arc<Peer>,
    connection: &Arc<EstablishedConnection>,
    recv: &mut RecvStream,
) -> Result<TowerReply, u32> {
    let wire: WireTowerAnnouncement =
        read_frame(recv, WIRE_FRAME_MAX, Some(shared.config.send_deadline))
            .await
            .map_err(|_| CLOSE_LINK)?;
    let Ok(announcement) = TowerAnnouncement::try_from(wire) else {
        return Ok(TowerReply::Nack {
            reason: TowerNackReason::InvalidTowers,
        });
    };

    let sender = lock(&exchange.incoming).clone();
    let Some(sender) = sender else {
        return Ok(unavailable());
    };
    // Capacity and the eventual reply share the enclosing stream deadline.
    let Ok(permit) = sender.reserve().await else {
        return Ok(unavailable());
    };
    let (reply, receive) = oneshot::channel();
    {
        let state = peer.state();
        if !is_current(&state, connection) {
            return Err(CLOSE_LINK);
        }
        let name = shared
            .authorizer
            .authorize(&peer.id, Protocol::Stream(TOWERS_PROTOCOL))
            .ok_or(CLOSE_NOT_ALLOWED)?;
        permit.send(IncomingTowers {
            peer: Arc::downgrade(peer),
            connection: Arc::downgrade(connection),
            authorizer: Arc::downgrade(&shared.authorizer),
            name,
            announcement,
            reply,
        });
    }
    Ok(receive.await.unwrap_or_else(|_| unavailable()))
}

fn is_current(state: &PeerState, connection: &Arc<EstablishedConnection>) -> bool {
    !state.revoked
        && state
            .active
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(&active.connection, connection) && active.is_open())
}

fn unavailable() -> TowerReply {
    TowerReply::Nack {
        reason: TowerNackReason::Unavailable,
    }
}
