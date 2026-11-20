//! Following the message log: the attach order of rule 3 in [`crate::api`], written once for every
//! frontend that shows the log live (the CLI's `chat`, later the GUI).
//!
//! [`Client::follow_log`] reads the station's instance, subscribes, pages the newest messages and
//! reads the instance again; then it follows live events, and drops what it has already shown
//! (rule 2). [`LogFollower::next`] yields each message once as [`LogUpdate::Added`], and again as
//! [`LogUpdate::Changed`] only when it changed (a higher `rev`).
//!
//! On [`Event::Resync`] the follower re-reads the log as far back as it shows and yields what it
//! missed. It keeps reading the subscription while the re-read's calls are pending: an end of the
//! subscription ends the follower at once, a new `Resync` starts the re-read again, and live
//! messages wait behind the re-read, so that nothing it recovers is yielded after something newer.
//!
//! A follower belongs to one instance of the station (rule 1 in [`crate::api`]): it remembers the
//! instance it attached to, and ends when the subscription ends or a re-read finds another
//! instance (rule 11). To go on, the frontend calls [`Client::follow_log`] again.

use crate::api::{ApiError, Event, LOG_PAGE_MAX_MESSAGES, LogPage, Message};
use crate::client::{Client, Subscription, Transport};
use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;

/// How many messages a [`LogFollower`] remembers (by `seq` and `rev`). When it holds more, it
/// forgets the oldest and never shows a message that old again. Well above the station's log of
/// 1,000. Also the most live messages it holds back while a re-read is pending.
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
///
/// After an [`Event::Resync`] it re-reads the log while it keeps reading the subscription. Live
/// messages that arrive meanwhile are held back, at most [`FOLLOW_MEMORY`] of them, and yielded
/// after what the re-read recovers. See [`LogFollower::next`].
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
    /// Live messages in scope that arrived while `reread` is pending, in arrival order, at most
    /// [`FOLLOW_MEMORY`]. Offered after the re-read's messages; empty when no re-read is pending.
    held: VecDeque<Message>,
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
    /// Reads the station's instance, subscribes, reads the newest `tail` messages (of `peer`, or
    /// of everyone) and reads the instance again; then follows live changes. This is the attach
    /// order of rule 3 in [`crate::api`], with the subscribe and the pages bracketed by two
    /// [`Client::status`] calls.
    ///
    /// It pages with [`Client::log`] until it holds `tail` messages or the log is exhausted (a page
    /// may be shorter than asked). With `tail` 0 it reads one page of `limit: 0` to learn where
    /// the log ends, and shows only messages that enter the log from then on.
    ///
    /// If the two instance ids differ, it fails with [`ApiError::Disconnected`]. Two equal ids are
    /// enough to know that the subscription and every page come from that one instance: an
    /// instance id names one start of the station and never comes back, and one socket (or one
    /// in-process station) serves one instance at a time. So when the same instance answered
    /// before and after, it answered everything in between, the subscribe included. Errors of the
    /// subscription and of the calls are passed through.
    pub async fn follow_log(
        &self,
        peer: Option<&str>,
        tail: u32,
    ) -> Result<LogFollower<'_, T>, ApiError> {
        let instance = self.status().await?.instance_id;
        let events = self.subscribe().await?;
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
            held: VecDeque::new(),
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
    /// shown at the same or a higher `rev` are skipped.
    ///
    /// After [`Event::Resync`] it re-reads the log: it checks the instance, reads pages from the
    /// newest message back to the oldest one it shows, and checks the instance again. Each of
    /// these calls races the subscription, which is polled first every time:
    ///
    /// - The subscription ends: the follower ends at once, and this returns `Ok(None)` even if the
    ///   call never completes (rule 11). What the re-read read and what it held back are dropped.
    /// - Another `Resync`: the re-read starts again from the beginning. The pages it read and the
    ///   live messages it held back are dropped: pages read before this gap may miss changes made
    ///   in it, and the new re-read covers what was held back.
    /// - A live message (of the follower's peer, if it has one, and not older than what it shows):
    ///   it is held back, and the call goes on; a stream of messages neither drops the call nor
    ///   makes it again. If [`FOLLOW_MEMORY`] messages are held already, they and this one are
    ///   dropped and the re-read starts again, which covers them.
    /// - Peer and status events, and messages out of scope, are skipped, and the call goes on.
    ///
    /// When a call completes, the subscription is read once more in the same poll, before the
    /// call's answer or error is used: an end or a gap that is ready at that moment wins, also when
    /// the call's own completion made it ready (the station stopped, or overflowed the
    /// subscription, just before it answered). The answer or error is then dropped: an end returns
    /// `Ok(None)` and a gap starts the re-read again, never the error. Messages ready at that
    /// moment are held back as above.
    ///
    /// Nothing the re-read read and nothing held back is yielded before its last check has passed
    /// and the subscription had no end and no gap ready when that check completed. Then it yields,
    /// oldest first, every recovered message that is new or changed, and after them the held-back
    /// messages in arrival order, again only those new or changed. If either check finds another
    /// instance of the station (rule 1), the follower ends and applies nothing. An error of a
    /// re-read's call is returned once; the re-read keeps what it has read and held back, and the
    /// next call races the subscription again and goes on with the re-read. Updates queued before
    /// a gap was noticed are older than it, and are yielded before the re-read goes on.
    ///
    /// Cancel-safe: updates not yet yielded stay queued in the follower, a message taken from the
    /// subscription is held back or queued in the same poll, and a re-read keeps the pages it has
    /// read, so after a cancelled call it goes on from the next page. Only the call that was in
    /// flight when the future was dropped is made again.
    pub async fn next(&mut self) -> Result<Option<LogUpdate>, ApiError> {
        loop {
            if let Some(update) = self.queue.pop_front() {
                return Ok(Some(update));
            }
            if self.ended {
                return Ok(None);
            }
            if self.reread.is_some() {
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

    /// Applies an event read while no re-read is pending.
    fn apply(&mut self, event: Option<Event>) {
        match event {
            None => self.end(),
            Some(Event::Resync) => self.start_reread(),
            Some(Event::Message { message }) => self.offer(message),
            Some(Event::Peer { .. } | Event::PeerRemoved { .. } | Event::Status { .. }) => {}
        }
    }

    /// Starts a re-read from the beginning, also over one in progress: pages that one has read
    /// and messages it has held back may miss changes made in the latest gap.
    fn start_reread(&mut self) {
        self.reread = Some(Reread {
            checked: false,
            pages: ReadBack::new(FOLLOW_MEMORY, Some(self.floor)),
        });
        self.held.clear();
    }

    /// The follower is over: the subscription ended or the station is another instance now.
    fn end(&mut self) {
        self.ended = true;
        self.reread = None;
        self.held = VecDeque::new();
    }

    /// Makes the next call of the pending re-read, racing the subscription: the instance check,
    /// the next page, or the final instance check. Progress is kept after every completed call;
    /// after the final check the re-read's messages are queued, then the held-back ones.
    async fn continue_reread(&mut self) -> Result<(), ApiError> {
        let Some(reread) = self.reread.as_ref() else {
            return Ok(());
        };
        let client = self.client;
        let scope = Scope {
            peer: self.peer.as_deref(),
            floor: self.floor,
        };
        if reread.checked && !reread.pages.done {
            let (before, limit) = (reread.pages.before, reread.pages.limit());
            let call = client.log(self.peer.as_deref(), before, limit);
            match race(&mut self.events, &mut self.held, scope, call).await {
                Raced::Ended => self.end(),
                Raced::Gap => self.start_reread(),
                Raced::Done(page) => {
                    let page = page?;
                    if let Some(reread) = self.reread.as_mut() {
                        reread.pages.take(page);
                    }
                }
            }
            return Ok(());
        }
        let last = reread.checked;
        match race(&mut self.events, &mut self.held, scope, client.status()).await {
            Raced::Ended => self.end(),
            Raced::Gap => self.start_reread(),
            Raced::Done(status) => {
                if status?.instance_id != self.instance {
                    self.end();
                } else if last {
                    self.finish_reread();
                } else if let Some(reread) = self.reread.as_mut() {
                    reread.checked = true;
                }
            }
        }
        Ok(())
    }

    /// The re-read's last check passed: queues what it read, oldest first, then what was held
    /// back, in arrival order.
    fn finish_reread(&mut self) {
        if let Some(reread) = self.reread.take() {
            let (messages, _) = reread.pages.finish();
            for message in messages {
                self.offer(message);
            }
        }
        for message in std::mem::take(&mut self.held) {
            self.offer(message);
        }
    }

    /// Queues `message` if it is in scope and new, or a higher `rev` of one already shown.
    fn offer(&mut self, message: Message) {
        if !self.scope().contains(&message) {
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

    fn scope(&self) -> Scope<'_> {
        Scope {
            peer: self.peer.as_deref(),
            floor: self.floor,
        }
    }
}

/// Which messages a follower shows: those of its peer (if it has one) at or above its floor.
#[derive(Debug, Clone, Copy)]
struct Scope<'p> {
    peer: Option<&'p str>,
    floor: u64,
}

impl Scope<'_> {
    fn contains(&self, message: &Message) -> bool {
        message.seq >= self.floor && self.peer.is_none_or(|p| p == message.peer)
    }
}

/// How a call of a re-read that raced the subscription ended.
enum Raced<O> {
    /// The call completed, and the subscription had no end and no gap ready.
    Done(O),
    /// The subscription ended.
    Ended,
    /// A new gap: an [`Event::Resync`], or more live messages than may be held back.
    Gap,
}

/// What one poll of the race produced.
enum Won<O> {
    Event(Option<Event>),
    Call(O),
}

/// Why a race stops before the call's output is used.
enum Stop {
    Ended,
    Gap,
}

impl<O> From<Stop> for Raced<O> {
    fn from(stop: Stop) -> Self {
        match stop {
            Stop::Ended => Self::Ended,
            Stop::Gap => Self::Gap,
        }
    }
}

/// Runs `call` while reading `events`, polling the subscription first every time. Live messages
/// in `scope` are pushed to `held` in the poll that takes them, and the one `call` goes on. Returns
/// when the subscription ends, a `Resync` arrives, or a message arrives while `held` is full; or
/// else when the call completes.
///
/// When the call completes, its output (an answer or an error) is kept, the completed call is not
/// polled again, and in the same poll the race takes everything the subscription has ready at that
/// moment, the same way: an end or a gap found there wins and the output is dropped, also when the
/// call's own completion made it ready. Only when the subscription has nothing more ready does the
/// race return [`Raced::Done`].
///
/// Cancel-safe: a dropped race has taken from the subscription only what is in `held`.
async fn race<F: Future>(
    events: &mut Subscription,
    held: &mut VecDeque<Message>,
    scope: Scope<'_>,
    call: F,
) -> Raced<F::Output> {
    let mut call = pin!(call);
    let output = loop {
        let won = {
            // Kept across polls until it resolves, so its wake-up is never lost; dropped only when
            // it or the call is ready, or when the race is dropped (it is cancel-safe).
            let mut event = pin!(events.next());
            poll_fn(|cx| {
                if let Poll::Ready(event) = event.as_mut().poll(cx) {
                    return Poll::Ready(Won::Event(event));
                }
                call.as_mut().poll(cx).map(Won::Call)
            })
            .await
        };
        match won {
            Won::Call(output) => break output,
            Won::Event(event) => {
                if let Err(stop) = take(event, held, scope) {
                    return stop.into();
                }
            }
        }
    };
    // The call completed in this poll, and its last poll may have made an end or a gap ready
    // (the station stopped or overflowed the subscription before it answered). Take what is ready
    // now, still in this poll; the completed call is never polled again.
    loop {
        let ready = {
            let mut event = pin!(events.next());
            poll_fn(|cx| Poll::Ready(event.as_mut().poll(cx))).await
        };
        match ready {
            Poll::Pending => return Raced::Done(output),
            Poll::Ready(event) => {
                if let Err(stop) = take(event, held, scope) {
                    return stop.into();
                }
            }
        }
    }
}

/// Takes one event of the subscription during a race: an end or a `Resync` stops the race, a
/// message in `scope` is held back (a gap if `held` is full), and anything else is skipped.
fn take(event: Option<Event>, held: &mut VecDeque<Message>, scope: Scope<'_>) -> Result<(), Stop> {
    match event {
        None => Err(Stop::Ended),
        Some(Event::Resync) => Err(Stop::Gap),
        Some(Event::Message { message }) => {
            if scope.contains(&message) {
                if held.len() >= FOLLOW_MEMORY {
                    return Err(Stop::Gap);
                }
                held.push_back(message);
            }
            Ok(())
        }
        Some(Event::Peer { .. } | Event::PeerRemoved { .. } | Event::Status { .. }) => Ok(()),
    }
}
