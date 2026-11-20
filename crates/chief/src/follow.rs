//! Following the message log: the attach order of rule 3 in [`crate::api`], written once for every
//! frontend that shows the log live (the CLI's `chat`, later the GUI).
//!
//! [`Client::follow_log`] subscribes first, then pages the newest messages, then follows live
//! events, and drops what it has already shown (rule 2). [`LogFollower::next`] yields each message
//! once as [`LogUpdate::Added`], and again as [`LogUpdate::Changed`] only when it changed (a higher
//! `rev`). On [`Event::Resync`] it re-reads the log as far back as it shows and yields what it
//! missed.

use crate::api::{ApiError, Event, LOG_PAGE_MAX_MESSAGES, Message};
use crate::client::{Client, Subscription, Transport};
use std::collections::{BTreeMap, VecDeque};

/// How many messages a [`LogFollower`] remembers (by `seq` and `rev`). When it holds more, it
/// forgets the oldest and never shows a message that old again. Well above the station's log of
/// 1,000.
pub const FOLLOW_MEMORY: usize = 4096;

/// One change to the log as a follower shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogUpdate {
    /// A message the follower has not shown before: from the backlog, or new.
    Added(Message),
    /// A higher `rev` of a message the follower has shown: its status changed.
    Changed(Message),
}

impl LogUpdate {
    /// The message, added or changed.
    pub fn message(&self) -> &Message {
        match self {
            Self::Added(m) | Self::Changed(m) => m,
        }
    }
}

/// Follows the log of one station: the backlog, then live changes. Made by [`Client::follow_log`].
#[derive(Debug)]
pub struct LogFollower<'a, T> {
    client: &'a Client<T>,
    events: Subscription,
    peer: Option<String>,
    /// `seq -> rev` of every message yielded or queued, newest [`FOLLOW_MEMORY`].
    seen: BTreeMap<u64, u32>,
    /// Messages below this `seq` are never shown: older than the backlog, or forgotten.
    floor: u64,
    /// Updates not yet yielded, in order.
    queue: VecDeque<LogUpdate>,
    /// A [`Event::Resync`] arrived and the log has not been re-read completely since.
    needs_resync: bool,
    /// The subscription returned `None`.
    ended: bool,
}

impl<T: Transport> Client<T> {
    /// Subscribes, then reads the newest `tail` messages (of `peer`, or of everyone), then follows
    /// live changes: the attach order of rule 3 in [`crate::api`].
    ///
    /// It pages with [`Client::log`] until it holds `tail` messages or the log is exhausted (a page
    /// may be shorter than asked). With `tail` 0 it shows only messages that enter the log from
    /// now on. Errors of the subscription and of the log calls are passed through.
    pub async fn follow_log(
        &self,
        peer: Option<&str>,
        tail: u32,
    ) -> Result<LogFollower<'_, T>, ApiError> {
        let events = self.subscribe().await?;
        let (backlog, floor) = if tail == 0 {
            // A page of `limit: 0` reports where the log ends (rule 6): everything from there on
            // is new. With no matching message, everything is.
            let page = self.log(peer, None, 0).await?;
            (Vec::new(), page.next_before.unwrap_or(1))
        } else {
            let (messages, exhausted) = self.read_back(peer, tail, None).await?;
            let floor = match messages.first() {
                Some(oldest) if !exhausted => oldest.seq,
                _ => 1,
            };
            (messages, floor)
        };
        let mut follower = LogFollower {
            client: self,
            events,
            peer: peer.map(str::to_owned),
            seen: BTreeMap::new(),
            floor,
            queue: VecDeque::new(),
            needs_resync: false,
            ended: false,
        };
        for message in backlog {
            follower.offer(message);
        }
        Ok(follower)
    }

    /// Reads the log back from the newest message: until it holds `want` messages, or reaches a
    /// message at or below `down_to` (`Some`), or the log is exhausted. Returns the messages
    /// ascending by `seq`, at most `want` of them, and whether the log was exhausted.
    async fn read_back(
        &self,
        peer: Option<&str>,
        want: u32,
        down_to: Option<u64>,
    ) -> Result<(Vec<Message>, bool), ApiError> {
        let want_len = usize::try_from(want).unwrap_or(usize::MAX);
        let mut pages: Vec<Vec<Message>> = Vec::new();
        let mut held = 0usize;
        let mut before: Option<u64> = None;
        let exhausted = loop {
            let limit = u32::try_from(want_len - held)
                .unwrap_or(u32::MAX)
                .min(LOG_PAGE_MAX_MESSAGES);
            let page = self.log(peer, before, limit).await?;
            let first = page.messages.first().map(|m| m.seq);
            held += page.messages.len();
            pages.push(page.messages);
            let reached = matches!((first, down_to), (Some(f), Some(d)) if f <= d);
            match (page.next_before, first) {
                // Progress only if the cursor moves back; a station that repeats itself is done.
                (Some(next), Some(_)) if before.is_none_or(|b| next < b) => {
                    if held >= want_len || reached {
                        break false;
                    }
                    before = Some(next);
                }
                (None, _) => break true,
                _ => break false,
            }
        };
        let mut messages: Vec<Message> = pages.into_iter().flatten().collect();
        messages.sort_by_key(|m| m.seq);
        messages.dedup_by_key(|m| m.seq);
        // Only a station that ignores `limit` gives more; keep the newest, and the older ones are
        // then not reached.
        let excess = messages.len().saturating_sub(want_len);
        messages.drain(..excess);
        Ok((messages, exhausted && excess == 0))
    }
}

impl<T: Transport> LogFollower<'_, T> {
    /// The next update: first the backlog, oldest first, each as [`LogUpdate::Added`]; then live
    /// updates. `Ok(None)` once the subscription has ended (rule 11), and on every later call.
    ///
    /// Events for other peers (with a `peer` filter), peer and status events, and messages already
    /// shown at the same or a higher `rev` are skipped. After [`Event::Resync`] it re-reads the log
    /// from the newest message back to the oldest one it shows and yields, oldest first, every
    /// message that is new or changed.
    ///
    /// An error of a re-read is returned, and the re-read is tried again on the next call.
    ///
    /// Cancel-safe: updates not yet yielded stay queued in the follower, and a re-read that was
    /// interrupted is done again on the next call.
    pub async fn next(&mut self) -> Result<Option<LogUpdate>, ApiError> {
        loop {
            if let Some(update) = self.queue.pop_front() {
                return Ok(Some(update));
            }
            if self.needs_resync {
                self.resync().await?;
                continue;
            }
            if self.ended {
                return Ok(None);
            }
            // Cancel-safe; everything after it runs within the same poll.
            match self.events.next().await {
                None => self.ended = true,
                Some(Event::Resync) => self.needs_resync = true,
                Some(Event::Message { message }) => self.offer(message),
                Some(Event::Peer { .. } | Event::PeerRemoved { .. } | Event::Status { .. }) => {}
            }
        }
    }

    /// The peer this follower is limited to, if any.
    pub fn peer(&self) -> Option<&str> {
        self.peer.as_deref()
    }

    /// Re-reads the log back to the floor and queues what is new or changed. Applies nothing until
    /// the whole re-read has succeeded, so an interrupted or failed one leaves no trace.
    async fn resync(&mut self) -> Result<(), ApiError> {
        let (messages, _) = self
            .client
            .read_back(
                self.peer.as_deref(),
                // Bounded by the floor first: the station keeps at most 1,000 messages, and the
                // follower never shows more than it remembers.
                u32::try_from(FOLLOW_MEMORY).unwrap_or(u32::MAX),
                Some(self.floor),
            )
            .await?;
        self.needs_resync = false;
        for message in messages {
            self.offer(message);
        }
        Ok(())
    }

    /// Queues `message` if it is in scope and new, or a higher `rev` of one already shown.
    fn offer(&mut self, message: Message) {
        if message.seq < self.floor || self.peer.as_deref().is_some_and(|p| p != message.peer) {
            return;
        }
        let update = match self.seen.get(&message.seq) {
            Some(&rev) if message.rev <= rev => return,
            Some(_) => LogUpdate::Changed(message),
            None => LogUpdate::Added(message),
        };
        let m = update.message();
        self.seen.insert(m.seq, m.rev);
        while self.seen.len() > FOLLOW_MEMORY {
            if let Some((oldest, _)) = self.seen.pop_first() {
                self.floor = self.floor.max(oldest.saturating_add(1));
            }
        }
        self.queue.push_back(update);
    }
}
