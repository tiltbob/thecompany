//! Following the message log: the attach order of rule 3 in [`crate::api`], written once for every
//! frontend that shows the log live (the CLI's `chat`, later the GUI).
//!
//! [`Client::follow_log`] subscribes first, then pages the newest messages, then follows live
//! events, and drops what it has already shown (rule 2). [`LogFollower::next`] yields each message
//! once as [`LogUpdate::Added`], and again as [`LogUpdate::Changed`] only when it changed (a higher
//! `rev`). On [`Event::Resync`] it re-reads the log as far back as it shows and yields what it
//! missed.
//!
//! A follower belongs to one instance of the station (rule 1 in [`crate::api`]): it remembers the
//! instance it attached to, and ends when the subscription ends or a re-read finds another
//! instance (rule 11). To go on, the frontend calls [`Client::follow_log`] again.

use crate::api::{ApiError, Event, LOG_PAGE_MAX_MESSAGES, LogPage, Message};
use crate::client::{Client, Subscription, Transport};
use std::collections::{BTreeMap, VecDeque};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

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

/// Follows the log of one station instance: the backlog, then live changes. Made by
/// [`Client::follow_log`].
#[derive(Debug)]
pub struct LogFollower<'a, T> {
    client: &'a Client<T>,
    events: Subscription,
    peer: Option<String>,
    /// The station instance this follower attached to (rule 1).
    instance: String,
    /// `seq -> rev` of every message yielded or queued, newest [`FOLLOW_MEMORY`].
    seen: BTreeMap<u64, u32>,
    /// Messages below this `seq` are never shown: older than the backlog, or forgotten.
    floor: u64,
    /// Updates not yet yielded, in order.
    queue: VecDeque<LogUpdate>,
    /// A re-read after [`Event::Resync`] that has not finished, with what it has read so far.
    reread: Option<Reread>,
    /// The subscription ended, or the station is another instance now. Final.
    ended: bool,
}

/// A re-read in progress. Kept across cancelled and failed calls of [`LogFollower::next`], so it
/// goes on where it stopped.
#[derive(Debug)]
struct Reread {
    /// The instance was checked before the first page.
    checked: bool,
    pages: ReadBack,
}

/// Reading the log back from the newest message, one page at a time: until `want` messages are
/// held, a page reaches a message at or below `down_to`, or the log is exhausted.
#[derive(Debug)]
struct ReadBack {
    want: usize,
    down_to: Option<u64>,
    /// The cursor of the next page; `None` before the first one.
    before: Option<u64>,
    messages: Vec<Message>,
    done: bool,
    exhausted: bool,
}

impl ReadBack {
    fn new(want: usize, down_to: Option<u64>) -> Self {
        Self {
            want,
            down_to,
            before: None,
            messages: Vec::new(),
            done: want == 0,
            exhausted: false,
        }
    }

    /// The `limit` of the next page.
    fn limit(&self) -> u32 {
        u32::try_from(self.want.saturating_sub(self.messages.len()))
            .unwrap_or(u32::MAX)
            .min(LOG_PAGE_MAX_MESSAGES)
    }

    /// Takes the next page and decides whether another is needed.
    fn take(&mut self, page: LogPage) {
        let first = page.messages.first().map(|m| m.seq);
        self.messages.extend(page.messages);
        let reached = matches!((first, self.down_to), (Some(f), Some(d)) if f <= d);
        match (page.next_before, first) {
            // Progress only if the cursor moves back; a station that repeats itself is done.
            (Some(next), Some(_)) if self.before.is_none_or(|b| next < b) => {
                if self.messages.len() >= self.want || reached {
                    self.done = true;
                } else {
                    self.before = Some(next);
                }
            }
            (None, _) => {
                self.done = true;
                self.exhausted = true;
            }
            _ => self.done = true,
        }
    }

    /// The messages ascending by `seq`, at most `want` of them, and whether the log was exhausted.
    fn finish(self) -> (Vec<Message>, bool) {
        let mut messages = self.messages;
        messages.sort_by_key(|m| m.seq);
        messages.dedup_by_key(|m| m.seq);
        // Only a station that ignores `limit` gives more; keep the newest, and the older ones are
        // then not reached.
        let excess = messages.len().saturating_sub(self.want);
        messages.drain(..excess);
        (messages, self.exhausted && excess == 0)
    }
}

impl<T: Transport> Client<T> {
    /// Subscribes, then reads the newest `tail` messages (of `peer`, or of everyone), then follows
    /// live changes: the attach order of rule 3 in [`crate::api`].
    ///
    /// It pages with [`Client::log`] until it holds `tail` messages or the log is exhausted (a page
    /// may be shorter than asked). With `tail` 0 it shows only messages that enter the log from
    /// now on. It reads [`Client::status`] after subscribing and again after the pages; if the
    /// station restarted in between, it fails with [`ApiError::Disconnected`]. Errors of the
    /// subscription and of the calls are passed through.
    pub async fn follow_log(
        &self,
        peer: Option<&str>,
        tail: u32,
    ) -> Result<LogFollower<'_, T>, ApiError> {
        let events = self.subscribe().await?;
        let instance = self.status().await?.instance_id;
        let (backlog, floor) = if tail == 0 {
            // A page of `limit: 0` reports where the log ends (rule 6): everything from there on
            // is new. With no matching message, everything is.
            let page = self.log(peer, None, 0).await?;
            (Vec::new(), page.next_before.unwrap_or(1))
        } else {
            let mut pages = ReadBack::new(usize::try_from(tail).unwrap_or(usize::MAX), None);
            while !pages.done {
                let page = self.log(peer, pages.before, pages.limit()).await?;
                pages.take(page);
            }
            let (messages, exhausted) = pages.finish();
            let floor = match messages.first() {
                Some(oldest) if !exhausted => oldest.seq,
                _ => 1,
            };
            (messages, floor)
        };
        if self.status().await?.instance_id != instance {
            return Err(ApiError::Disconnected {
                detail: "the station restarted while attaching".to_owned(),
            });
        }
        let mut follower = LogFollower {
            client: self,
            events,
            peer: peer.map(str::to_owned),
            instance,
            seen: BTreeMap::new(),
            floor,
            queue: VecDeque::new(),
            reread: None,
            ended: false,
        };
        for message in backlog {
            follower.offer(message);
        }
        Ok(follower)
    }
}

impl<T: Transport> LogFollower<'_, T> {
    /// The next update: first the backlog, oldest first, each as [`LogUpdate::Added`]; then live
    /// updates. `Ok(None)` once the follower has ended, and on every later call.
    ///
    /// Events for other peers (with a `peer` filter), peer and status events, and messages already
    /// shown at the same or a higher `rev` are skipped. After [`Event::Resync`] it re-reads the log
    /// from the newest message back to the oldest one it shows and yields, oldest first, every
    /// message that is new or changed.
    ///
    /// The follower ends when its subscription ends (rule 11), also while a re-read is pending:
    /// before each step of a re-read it takes what the subscription has ready, and an ended
    /// subscription drops the re-read. It also ends, applying nothing, when a re-read finds another
    /// instance of the station before its first page or after its last (rule 1). An error of a
    /// re-read's call is returned once; the next call looks at the subscription again and then
    /// goes on with the re-read.
    ///
    /// Cancel-safe: updates not yet yielded stay queued in the follower, and a re-read keeps the
    /// pages it has read, so after a cancelled call it goes on from the next page. Only a call
    /// that was in flight when the future was dropped is made again.
    pub async fn next(&mut self) -> Result<Option<LogUpdate>, ApiError> {
        loop {
            if let Some(update) = self.queue.pop_front() {
                return Ok(Some(update));
            }
            if self.ended {
                return Ok(None);
            }
            if self.reread.is_some() {
                // What the subscription has ready comes first: above all its end.
                if let Poll::Ready(event) = self.ready_event() {
                    self.apply(event);
                    continue;
                }
                self.continue_reread().await?;
                continue;
            }
            // Cancel-safe; everything after it runs within the same poll.
            let event = self.events.next().await;
            self.apply(event);
        }
    }

    /// The peer this follower is limited to, if any.
    pub fn peer(&self) -> Option<&str> {
        self.peer.as_deref()
    }

    /// The next event if the subscription has one ready, without waiting. Dropping the unfinished
    /// `next()` loses nothing: it is cancel-safe.
    fn ready_event(&mut self) -> Poll<Option<Event>> {
        let mut cx = Context::from_waker(Waker::noop());
        pin!(self.events.next()).poll(&mut cx)
    }

    fn apply(&mut self, event: Option<Event>) {
        match event {
            None => self.end(),
            // A re-read in progress started before this gap, and pages it has read already may miss
            // changes made in the gap: start over.
            Some(Event::Resync) => {
                self.reread = Some(Reread {
                    checked: false,
                    pages: ReadBack::new(FOLLOW_MEMORY, Some(self.floor)),
                });
            }
            Some(Event::Message { message }) => self.offer(message),
            Some(Event::Peer { .. } | Event::PeerRemoved { .. } | Event::Status { .. }) => {}
        }
    }

    /// The follower is over: the subscription ended or the station is another instance now.
    fn end(&mut self) {
        self.ended = true;
        self.reread = None;
    }

    /// Goes on with the pending re-read: the instance check, the remaining pages, the instance
    /// check again, and then queues what is new or changed. Progress is kept after every completed
    /// call, and nothing is applied until the last check has passed.
    async fn continue_reread(&mut self) -> Result<(), ApiError> {
        let client = self.client;
        if self.reread.as_ref().is_some_and(|r| !r.checked) {
            if client.status().await?.instance_id != self.instance {
                self.end();
                return Ok(());
            }
            if let Some(reread) = self.reread.as_mut() {
                reread.checked = true;
            }
        }
        while let Some(pages) = self.reread.as_ref().map(|r| &r.pages)
            && !pages.done
        {
            let (before, limit) = (pages.before, pages.limit());
            let page = client.log(self.peer.as_deref(), before, limit).await?;
            if let Some(reread) = self.reread.as_mut() {
                reread.pages.take(page);
            }
        }
        if client.status().await?.instance_id != self.instance {
            self.end();
            return Ok(());
        }
        if let Some(reread) = self.reread.take() {
            let (messages, _) = reread.pages.finish();
            for message in messages {
                self.offer(message);
            }
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
