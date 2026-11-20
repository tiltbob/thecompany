use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chief::api::{
    ApiError, BODY_MAX_BYTES, Direction, Event, FailReason, Link as PeerLink, Message,
    MessageStatus, Peer, PeerStation, RECEIPT_WINDOW, Request, Response, Status,
    TowerCandidatesPage, TowerState, check_peer_name, check_tower_url, clean, clean_tower_display,
};
use chief::client::{EventSink, SUBSCRIPTION_CAPACITY, Subscription, subscription};
use iroh::{Watcher, endpoint::RelayStatus};
use numbers::link::{IncomingChat, Link, LinkConfig, LinkHandle, LinkPath, PathUpdate};
use numbers::proto::CHAT_PROTOCOL;
use numbers::proto::chat::{ChatMessage, ChatReply, NackReason, ReceiptWindow};
use numbers::{Authorizer, NodeId, Protocol};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::{AbortHandle, JoinError, JoinSet};
use tokio::time::{Instant, sleep_until, timeout_at};

use crate::history::{History, internal};
use crate::storage::{PeerBook, SaveOutcome, Storage};
use crate::{ShutdownError, new_id};

const MAX_PENDING_SENDS: usize = 1_000;

pub(super) enum Command {
    Call {
        request: Request,
        reply: oneshot::Sender<Result<Response, ApiError>>,
    },
    Subscribe {
        reply: oneshot::Sender<Subscription>,
    },
}

struct Pending {
    message: Message,
    worker: AbortHandle,
}

pub(super) struct Actor {
    storage: Storage,
    own_id: NodeId,
    book: PeerBook,
    book_uncertain: bool,
    authorizer: Arc<Authorizer>,
    status: Status,
    link_config: LinkConfig,
    links: LinkHandle,
    paths: HashMap<NodeId, PeerLink>,
    history: History,
    receipts: ReceiptWindow,
    pending: BTreeMap<u64, Pending>,
    jobs: JoinSet<(u64, MessageStatus)>,
    sinks: Vec<EventSink>,
}

impl Actor {
    pub(super) fn new(
        storage: Storage,
        own_id: NodeId,
        book: PeerBook,
        authorizer: Arc<Authorizer>,
        status: Status,
        link_config: LinkConfig,
        links: LinkHandle,
    ) -> Self {
        Self {
            storage,
            own_id,
            book,
            book_uncertain: false,
            authorizer,
            status,
            link_config,
            links,
            paths: HashMap::new(),
            history: History::default(),
            receipts: ReceiptWindow::new(RECEIPT_WINDOW),
            pending: BTreeMap::new(),
            jobs: JoinSet::new(),
            sinks: Vec::new(),
        }
    }

    pub(super) async fn run(
        mut self,
        network: Link,
        mut incoming: mpsc::Receiver<IncomingChat>,
        mut relay: impl Watcher<Value = Vec<RelayStatus>> + Send,
        mut commands: mpsc::Receiver<Command>,
        mut stop: oneshot::Receiver<()>,
    ) -> Result<(), ShutdownError> {
        let mut paths = self.links.subscribe_paths();
        self.refresh_all_paths();
        loop {
            // Give shutdown priority while keeping command and network traffic fair.
            let running = tokio::select! {
                biased;
                _ = &mut stop => false,
                result = self.turn(&mut incoming, &mut relay, &mut paths, &mut commands) => result,
            };
            if !running {
                break;
            }
        }

        commands.close();
        drop(commands);
        self.sinks.clear();
        drop(incoming);
        drop(relay);
        drop(paths);
        self.jobs.shutdown().await;
        let result = network.shutdown().await.map_err(|_| ShutdownError);
        // Keep the data directory locked through network shutdown.
        drop(self);
        result
    }

    async fn turn(
        &mut self,
        incoming: &mut mpsc::Receiver<IncomingChat>,
        relay: &mut impl Watcher<Value = Vec<RelayStatus>>,
        paths: &mut broadcast::Receiver<PathUpdate>,
        commands: &mut mpsc::Receiver<Command>,
    ) -> bool {
        tokio::select! {
            command = commands.recv() => match command {
                Some(command) => self.command(command),
                None => return false,
            },
            chat = incoming.recv() => match chat {
                Some(chat) => self.incoming_chat(chat),
                None => return false,
            },
            status = relay.updated() => match status {
                Ok(relays) => self.relay_update(&relays),
                Err(_) => return false,
            },
            update = paths.recv() => match update {
                Ok(update) => self.path_update(update.station_id),
                Err(broadcast::error::RecvError::Lagged(_)) => self.refresh_all_paths(),
                Err(broadcast::error::RecvError::Closed) => return false,
            },
            Some(result) = self.jobs.join_next(), if !self.jobs.is_empty() => self.completed(result),
        }
        true
    }

    fn command(&mut self, command: Command) {
        match command {
            Command::Call { request, reply } => {
                let response = self.call(request);
                let _ = reply.send(response);
            }
            Command::Subscribe { reply } => {
                self.sinks.retain(|sink| !sink.is_closed());
                let (sink, events) = subscription(SUBSCRIPTION_CAPACITY);
                self.sinks.push(sink);
                if reply.send(events).is_err() {
                    self.sinks.pop();
                }
            }
        }
    }

    fn call(&mut self, request: Request) -> Result<Response, ApiError> {
        match request {
            Request::Status => Ok(Response::Status {
                status: self.status.clone(),
            }),
            // M1 pins the explicit startup tower and receives no tower announcements.
            Request::TowerCandidates { .. } => Ok(Response::TowerCandidates {
                page: TowerCandidatesPage {
                    groups: Vec::new(),
                    next_after_peer: None,
                },
            }),
            Request::TowerAdd { url } | Request::TowerRemove { url } => {
                check_tower_url(&url)?;
                Err(ApiError::TowersPinned)
            }
            Request::Peers => Ok(Response::Peers {
                peers: self
                    .book
                    .iter()
                    .map(|(name, ids)| self.peer(name, ids))
                    .collect(),
            }),
            Request::PeerAdd { name, station_id } => self.add_peer(&name, &station_id),
            Request::PeerRemove { name } => self.remove_peer(&name),
            Request::Send { peer, body } => self.send(&peer, &body),
            Request::Message { seq } => Ok(Response::Message {
                message: self.history.message(seq)?,
            }),
            Request::Log {
                peer,
                before,
                limit,
            } => Ok(Response::Log {
                page: self.history.page(peer.as_deref(), before, limit)?,
            }),
        }
    }

    fn emit(&mut self, event: Event) {
        self.sinks.retain(|sink| sink.push(event.clone()).is_ok());
    }

    fn peer(&self, name: &str, stations: &[NodeId]) -> Peer {
        Peer {
            name: name.to_owned(),
            stations: stations
                .iter()
                .map(|id| PeerStation {
                    station_id: id.to_string(),
                    link: self.paths.get(id).copied().unwrap_or(PeerLink::Down),
                })
                .collect(),
        }
    }

    fn add_peer(&mut self, name: &str, station_id: &str) -> Result<Response, ApiError> {
        check_peer_name(name)?;
        let id: NodeId = station_id
            .parse()
            .map_err(|_| invalid("station_id", "must be a valid station id"))?;
        if id == self.own_id {
            return Err(invalid("station_id", "cannot add this station as a peer"));
        }
        if let Some((owner, ids)) = self.book.iter().find(|(_, ids)| ids.contains(&id)) {
            if owner == name {
                let peer = self.peer(owner, ids);
                self.confirm_current_book()?;
                return Ok(Response::Peer { peer });
            }
            return Err(ApiError::StationTaken {
                station_id: id.to_string(),
                peer: owner.clone(),
            });
        }
        let mut book = self.book.clone();
        let ids = book.entry(name.to_owned()).or_default();
        ids.push(id);
        let peer = self.peer(name, ids);
        let saved = self.change_book(book)?;
        self.emit(Event::Peer { peer: peer.clone() });
        confirm_durable(saved)?;
        Ok(Response::Peer { peer })
    }

    fn remove_peer(&mut self, name: &str) -> Result<Response, ApiError> {
        let mut book = self.book.clone();
        let Some(removed) = book.remove(name) else {
            self.confirm_current_book()?;
            return Err(no_peer(name));
        };
        let saved = self.change_book(book)?;
        // A job that already returned was not overtaken by the removal: keep its result. Take
        // them before closing the links, so that no result caused by the closing is kept.
        while let Some(result) = self.jobs.try_join_next() {
            self.completed(result);
        }
        for id in removed {
            self.links.close_peer(id);
            self.paths.remove(&id);
        }
        let pending: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.message.peer == name)
            .map(|(&seq, _)| seq)
            .collect();
        for seq in pending {
            self.finish(seq, failed(FailReason::PeerRemoved));
        }
        self.emit(Event::PeerRemoved {
            name: name.to_owned(),
        });
        confirm_durable(saved)?;
        Ok(Response::Done)
    }

    fn confirm_current_book(&mut self) -> Result<(), ApiError> {
        if !self.book_uncertain {
            return Ok(());
        }
        let saved = self
            .storage
            .save_peers(&self.book, self.own_id)
            .map_err(|error| internal(&error.to_string()))?;
        self.book_uncertain = saved == SaveOutcome::DurabilityUncertain;
        confirm_durable(saved)
    }

    fn change_book(&mut self, book: PeerBook) -> Result<SaveOutcome, ApiError> {
        let saved = self
            .storage
            .save_peers(&book, self.own_id)
            .map_err(|error| internal(&error.to_string()))?;
        self.book_uncertain = saved == SaveOutcome::DurabilityUncertain;
        self.book = book;
        authorize_book(&self.authorizer, &self.book);
        Ok(saved)
    }

    fn send(&mut self, peer: &str, body: &str) -> Result<Response, ApiError> {
        let stations = self.book.get(peer).ok_or_else(|| no_peer(peer))?;
        if body.is_empty() {
            return Err(invalid("body", "must not be empty"));
        }
        if body.len() > BODY_MAX_BYTES {
            return Err(invalid("body", "must be at most 16 KiB"));
        }
        // Count the actual worker tasks, including aborted tasks still being reclaimed.
        if self.jobs.len() >= MAX_PENDING_SENDS {
            return Err(internal("the station has too many sends in progress"));
        }
        let deadline = Instant::now()
            .checked_add(self.link_config.send_deadline)
            .ok_or_else(|| internal("could not set the send deadline"))?;
        let id = new_id().map_err(|_| internal("could not generate a message id"))?;
        let mut stations = stations.clone();
        // A link can change while we sort. Sample once per station and preserve book order for ties.
        stations.sort_by_cached_key(|id| self.links.path(*id) == LinkPath::Down);
        let message = self.history.append(
            peer,
            id,
            Direction::Outgoing,
            body.to_owned(),
            MessageStatus::Sending,
        )?;
        let chat = ChatMessage {
            id: message.id.clone(),
            sent_at_ms: message.at_ms,
            body: message.body.clone(),
        };
        let link = self.links.clone();
        let connect_timeout = self.link_config.connect_timeout;
        let seq = message.seq;
        let worker = self.jobs.spawn(async move {
            (
                seq,
                deliver(link, stations, chat, deadline, connect_timeout).await,
            )
        });
        self.pending.insert(
            seq,
            Pending {
                message: message.clone(),
                worker,
            },
        );
        self.emit(Event::Message {
            message: message.clone(),
        });
        Ok(Response::Message { message })
    }

    fn completed(&mut self, result: Result<(u64, MessageStatus), JoinError>) {
        match result {
            Ok((seq, status)) => self.finish(seq, status),
            Err(error) => {
                // Revocation already removed any explicitly aborted job from pending.
                let seq = self
                    .pending
                    .iter()
                    .find_map(|(&seq, pending)| (pending.worker.id() == error.id()).then_some(seq));
                if let Some(seq) = seq {
                    self.finish(seq, failed(FailReason::NoAnswer));
                }
            }
        }
    }

    fn finish(&mut self, seq: u64, status: MessageStatus) {
        let Some(Pending {
            mut message,
            worker,
        }) = self.pending.remove(&seq)
        else {
            return;
        };
        worker.abort();
        message.rev = 1;
        message.status = status;
        self.history.update(&message);
        self.emit(Event::Message { message });
    }

    fn incoming_chat(&mut self, incoming: IncomingChat) {
        let peer = incoming.peer.as_ref();
        let id = &incoming.message.id;
        if !self
            .book
            .get(peer)
            .is_some_and(|stations| stations.contains(&incoming.station_id))
        {
            incoming.respond(ChatReply::Nack(NackReason::Unavailable));
            return;
        }
        if self.receipts.contains(peer, id) {
            incoming.respond(ChatReply::Ack);
            return;
        }
        let message = self.history.append(
            peer,
            id.clone(),
            Direction::Incoming,
            clean(&incoming.message.body).into_owned(),
            MessageStatus::Received,
        );
        match message {
            Ok(message) => {
                let recorded = self.receipts.record(peer, id);
                debug_assert!(
                    recorded,
                    "a new receipt must still be absent in this actor turn"
                );
                self.emit(Event::Message { message });
                incoming.respond(ChatReply::Ack);
            }
            Err(_) => incoming.respond(ChatReply::Nack(NackReason::Unavailable)),
        }
    }

    fn relay_update(&mut self, relays: &[RelayStatus]) {
        let state = tower_state(relays.first());
        let tower = &mut self.status.towers[0];
        if tower.state != state {
            tower.state = state;
            self.emit(Event::Status {
                status: self.status.clone(),
            });
        }
    }

    fn path_update(&mut self, id: NodeId) {
        let Some(peer) = self
            .authorizer
            .authorize(&id, Protocol::Stream(CHAT_PROTOCOL))
        else {
            return;
        };
        self.refresh_peer_paths(&peer);
    }

    fn refresh_all_paths(&mut self) {
        let peers: Vec<_> = self.book.keys().cloned().collect();
        for name in peers {
            self.refresh_peer_paths(&name);
        }
    }

    fn refresh_peer_paths(&mut self, name: &str) {
        let Some(stations) = self.book.get(name) else {
            return;
        };
        let mut changed = false;
        for &id in stations {
            // Queued path events may refer to a link closed by peer removal. Use the live path.
            let path = match self.links.path(id) {
                LinkPath::Down => PeerLink::Down,
                LinkPath::Relayed => PeerLink::Relayed,
                LinkPath::Direct => PeerLink::Direct,
            };
            let previous = self.paths.insert(id, path).unwrap_or(PeerLink::Down);
            changed |= path != previous;
        }
        if changed {
            self.emit(Event::Peer {
                peer: self.peer(name, stations),
            });
        }
    }
}

pub(super) fn authorize_book(authorizer: &Authorizer, book: &PeerBook) {
    authorizer.replace_peers(
        book.iter()
            .flat_map(|(name, ids)| ids.iter().map(move |&id| (id, name.clone()))),
    );
}

pub(super) fn tower_state(relay: Option<&RelayStatus>) -> TowerState {
    let Some(relay) = relay else {
        return TowerState::Standby;
    };
    if relay.is_connected() {
        TowerState::Connected
    } else if let Some(reason) = relay.auth_denied_reason() {
        TowerState::Denied {
            reason: clean_tower_display(reason),
        }
    } else if let Some(error) = relay.last_error() {
        TowerState::Failing {
            error: clean_tower_display(&error.to_string()),
        }
    } else {
        TowerState::Connecting
    }
}

async fn deliver(
    link: LinkHandle,
    stations: Vec<NodeId>,
    chat: ChatMessage,
    deadline: Instant,
    connect_timeout: Duration,
) -> MessageStatus {
    let mut stations = stations.into_iter();
    while let Some(id) = stations.next() {
        let now = Instant::now();
        let remaining = deadline.saturating_duration_since(now);
        if remaining.is_zero() {
            break;
        }
        // Leave one dial budget per later candidate when time permits. The current
        // candidate can still use one dial budget, capped by the original deadline.
        let later_count = u32::try_from(stations.len()).unwrap_or(u32::MAX);
        let reservation = connect_timeout.saturating_mul(later_count);
        let allowance = remaining
            .saturating_sub(reservation)
            .max(connect_timeout)
            .min(remaining);
        // Tokio polls the send before its timer, preserving a ready reply on a late poll.
        match timeout_at(now + allowance, link.send(id, chat.clone())).await {
            Ok(Ok(ChatReply::Ack)) => return MessageStatus::Delivered,
            Ok(Ok(ChatReply::Nack(NackReason::Unavailable))) | Ok(Err(_)) | Err(_) => {}
            Ok(Ok(ChatReply::Nack(_))) => return failed(FailReason::Refused),
        }
    }
    // With no conclusive reply, report NoAnswer at the original deadline.
    sleep_until(deadline).await;
    failed(FailReason::NoAnswer)
}

fn confirm_durable(saved: SaveOutcome) -> Result<(), ApiError> {
    match saved {
        SaveOutcome::Durable => Ok(()),
        SaveOutcome::DurabilityUncertain => Err(internal(
            "the peer book was written, but the station could not sync its data directory",
        )),
    }
}

fn no_peer(name: &str) -> ApiError {
    ApiError::PeerNotFound {
        name: name.to_owned(),
    }
}

fn invalid(field: &str, reason: &str) -> ApiError {
    ApiError::Invalid {
        field: field.to_owned(),
        reason: reason.to_owned(),
    }
}

fn failed(reason: FailReason) -> MessageStatus {
    MessageStatus::Failed { reason }
}

#[cfg(test)]
mod tests;
