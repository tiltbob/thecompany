//! Following the message log: the attach order of rule 3 in [`crate::api`], written once for every
//! frontend that shows the log live (the CLI's `chat`, later the GUI).
//!
//! [`Client::follow_log`] reads the station's instance, subscribes, reads where the log ends, pages
//! the newest messages and reads the instance again; then it follows live events, and drops what it
//! has already shown (rule 2). [`LogFollower::next`] yields each message once as
//! [`LogUpdate::Added`], in ascending `seq` order, and again as [`LogUpdate::Changed`] only when it
//! changed (a higher `rev`). It never shows a message below its floor: the oldest message of the
//! backlog or, with no backlog, where the log ended when it attached.
//!
//! On [`Event::Resync`] the follower re-reads the log as far back as it shows and yields what it
//! missed. It keeps reading the subscription while the re-read's calls are pending: an end of the
//! subscription ends the follower at once, a new `Resync` starts the re-read again, and live
//! messages are held back and yielded together with what the re-read recovers, in `seq` order, so
//! that no message is added after a newer one. Held-back messages are kept until the re-read
//! finishes or the follower ends.
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
/// 1,000. Also the most messages a re-read reads back, and the most live messages the follower
/// holds back while a re-read is pending: a further live message is dropped, and one more re-read
/// after this one recovers it if the log still holds it.
pub const FOLLOW_MEMORY: usize = 4096;

/// One change to the log as a follower shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogUpdate {
    /// A message the follower has not shown before: from the backlog, or new. Added messages come
    /// in ascending `seq` order: a message never shown that is older than one already added (its
    /// event was lost, and the re-read did not find it in the log) is not shown at all.
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
/// together with what the re-read recovers, in `seq` order. See [`LogFollower::next`].
#[derive(Debug)]
pub struct LogFollower<'a, T> {
    client: &'a Client<T>,
    events: Subscription,
    peer: Option<String>,
    /// The station instance this follower attached to (rule 1).
    instance: String,
    /// `seq -> rev` of every message yielded or queued, newest [`FOLLOW_MEMORY`].
    seen: BTreeMap<u64, u32>,
    /// Messages below this `seq` are never shown: older than the backlog, or than where the log
    /// ended at the attach (see [`Client::follow_log`]), or forgotten.
    floor: u64,
    /// The highest `seq` ever queued; 0 before the first. A message not in `seen` is added only
    /// above it, so that added messages come in ascending `seq` order.
    newest: u64,
    /// Updates not yet yielded, in order.
    queue: VecDeque<LogUpdate>,
    /// A re-read after [`Event::Resync`] that has not finished, with what it has read so far.
    reread: Option<Reread>,
    /// Live messages in scope that arrived while `reread` is pending, in arrival order, at most
    /// [`FOLLOW_MEMORY`]. Kept across a new `Resync` and across failed calls; offered together with
    /// the re-read's messages when it finishes; empty when no re-read is pending.
    held: VecDeque<Message>,
    /// A live message in scope was dropped because `held` was full: when the re-read finishes, one
    /// more starts.
    lagged: bool,
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
}

impl ReadBack {
    fn new(want: usize, down_to: Option<u64>) -> Self {
        Self {
            want,
            down_to,
            before: None,
            messages: Vec::new(),
            done: want == 0,
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
            _ => self.done = true,
        }
    }

    /// The messages ascending by `seq`, at most `want` of them.
    fn finish(self) -> Vec<Message> {
        let mut messages = self.messages;
        messages.sort_by_key(|m| m.seq);
        messages.dedup_by_key(|m| m.seq);
        // Only a station that ignores `limit` gives more; keep the newest, and the older ones are
        // then not reached.
        let excess = messages.len().saturating_sub(self.want);
        messages.drain(..excess);
        messages
    }
}

impl<T: Transport> Client<T> {
    /// Reads the station's instance, subscribes, reads where the log ends and the newest `tail`
    /// messages (of `peer`, or of everyone), and reads the instance again; then follows live
    /// changes. This is the attach order of rule 3 in [`crate::api`], with the subscribe and the
    /// pages bracketed by two [`Client::status`] calls.
    ///
    /// Right after the subscribe it reads one page of `limit: 0` of the whole log (no `peer`),
    /// whose cursor is where the log ends (rule 6): the newest `seq` plus one, or 1 for a log that
    /// has never held a message. With `tail` 0 it reads no other page. Otherwise it pages with
    /// [`Client::log`] until it holds `tail` messages or the log is exhausted (a page may be
    /// shorter than asked).
    ///
    /// The follower's floor is the `seq` of the oldest message of the backlog, or, when the
    /// backlog is empty (`tail` 0, or no matching message), where the log ended. It never shows a
    /// message below its floor: so a message that entered the log before the attach and is not in
    /// the backlog is not shown, also when it changes later (such as the outcome of a send that
    /// has left the log, rule 7).
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
        // Where the log ends (rule 6): `None` only for a log that has never held a message. Read
        // before the pages, so a message that enters after it is on a page or at or above it.
        let end = self.log(None, None, 0).await?.next_before.unwrap_or(1);
        let (backlog, floor) = if tail == 0 {
            (Vec::new(), end)
        } else {
            let mut pages = ReadBack::new(usize::try_from(tail).unwrap_or(usize::MAX), None);
            while !pages.done {
                let page = self.log(peer, pages.before, pages.limit()).await?;
                pages.take(page);
            }
            let messages = pages.finish();
            let floor = messages.first().map_or(end, |oldest| oldest.seq);
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
            newest: 0,
            queue: VecDeque::new(),
            reread: None,
            held: VecDeque::new(),
            lagged: false,
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
    /// Events for other peers (with a `peer` filter), peer and status events, messages below the
    /// follower's floor (see [`Client::follow_log`]), messages already shown at the same or a
    /// higher `rev`, and messages never shown that are older than one already added (see
    /// [`LogUpdate::Added`]) are skipped.
    ///
    /// After [`Event::Resync`] it re-reads the log: it checks the instance, reads pages from the
    /// newest message back to the oldest one it shows (at most [`FOLLOW_MEMORY`] messages), and
    /// checks the instance again. Each of these calls races the subscription, which is polled
    /// first every time:
    ///
    /// - The subscription ends: the follower ends at once, and this returns `Ok(None)` even if the
    ///   call never completes (rule 11). What the re-read read and what it held back are dropped.
    /// - Another `Resync`: the re-read starts again with the instance check, and the pages it has
    ///   read are dropped, since pages read before this gap may miss changes made in it. The live
    ///   messages it has held back are kept.
    /// - A live message (of the follower's peer, if it has one, and not below its floor): it is
    ///   held back, and the call goes on; a stream of messages neither drops the call nor makes it
    ///   again. If [`FOLLOW_MEMORY`] messages are held back already, this one is dropped and the
    ///   call goes on; when the re-read has finished, one more re-read starts at once, which
    ///   recovers the dropped messages that the log still holds.
    /// - Peer and status events, and messages out of scope, are skipped, and the call goes on.
    ///
    /// When a call completes, the subscription is read again in the same poll until it has
    /// nothing ready, before the call's answer or error is used: an end or a gap found there wins,
    /// also when the call's own completion made it ready (the station stopped, or overflowed the
    /// subscription, just before it answered). The answer or error is then dropped: an end
    /// returns `Ok(None)` and a gap starts the re-read again, never the error. Messages found
    /// there are held back as above.
    ///
    /// Nothing the re-read read and nothing held back is yielded before its last check has passed
    /// and the subscription had no end and no gap ready when that check completed. Then the
    /// recovered messages and the held-back ones are taken together, in ascending order of `seq`
    /// and, for one `seq`, of `rev`, and each of them that is new or changed by the rules above is
    /// yielded. If either check finds another instance of the station (rule 1), the follower ends
    /// and applies nothing. An error of a re-read's call is returned once; the re-read keeps what
    /// it has read and held back, and the next call races the subscription again and goes on with
    /// the re-read. Updates queued before a gap was noticed are older than it, and are yielded
    /// before the re-read goes on.
    ///
    /// A known limit: a change whose event is lost (in a gap, or dropped as above) is recovered
    /// only while the log holds its message. So a send that was still `Sending` when it left the
    /// log, and whose outcome's event is lost, stays `Sending` in the follower: the log no longer
    /// holds it ([`crate::api::Request::Message`] gives [`ApiError::MessageNotFound`]), and no
    /// re-read can recover it.
    ///
    /// Cancel-safe: updates not yet yielded stay queued in the follower, a message taken from the
    /// subscription is held back, queued or dropped (as above) in the same poll, and a re-read
    /// keeps the pages it has read, so after a cancelled call it goes on from the next page. Only
    /// the call that was in flight when the future was dropped is made again.
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

    /// Starts a re-read from the instance check, also over one in progress: pages that one has
    /// read may miss changes made in the latest gap. The held-back messages and `lagged` stay:
    /// they arrived before the gap, and are offered when the new re-read finishes.
    fn start_reread(&mut self) {
        self.reread = Some(Reread {
            checked: false,
            pages: ReadBack::new(FOLLOW_MEMORY, Some(self.floor)),
        });
    }

    /// The follower is over: the subscription ended or the station is another instance now.
    fn end(&mut self) {
        self.ended = true;
        self.reread = None;
        self.held = VecDeque::new();
        self.lagged = false;
    }

    /// Makes the next call of the pending re-read, racing the subscription: the instance check,
    /// the next page, or the final instance check. Progress is kept after every completed call;
    /// after the final check the re-read finishes ([`LogFollower::finish_reread`]).
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
            match race(
                &mut self.events,
                &mut self.held,
                &mut self.lagged,
                scope,
                call,
            )
            .await
            {
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
        let status = client.status();
        match race(
            &mut self.events,
            &mut self.held,
            &mut self.lagged,
            scope,
            status,
        )
        .await
        {
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

    /// The re-read's last check passed: takes what it read and what was held back together,
    /// sorts them by `seq` and then `rev` (a stable sort, so of two equal ones the page's comes
    /// first), and offers each in that order. If live messages were dropped meanwhile, starts one
    /// more re-read, which recovers those the log still holds.
    fn finish_reread(&mut self) {
        let mut messages = self
            .reread
            .take()
            .map(|reread| reread.pages.finish())
            .unwrap_or_default();
        messages.extend(std::mem::take(&mut self.held));
        messages.sort_by_key(|m| (m.seq, m.rev));
        for message in messages {
            self.offer(message);
        }
        if std::mem::take(&mut self.lagged) {
            self.start_reread();
        }
    }

    /// Queues `message` if it is in scope and new and newer than every message queued before, or
    /// a higher `rev` of one already shown.
    fn offer(&mut self, message: Message) {
        if !self.scope().contains(&message) {
            return;
        }
        let update = match self.seen.get(&message.seq) {
            Some(&rev) if message.rev <= rev => return,
            Some(_) => LogUpdate::Changed(message),
            // Never shown, and older than a message already added: its event was lost, and the
            // re-read did not find it in the log. Showing it now would add it after newer ones.
            None if message.seq <= self.newest => return,
            None => LogUpdate::Added(message),
        };
        let m = update.message();
        self.newest = self.newest.max(m.seq);
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
    /// A new gap: an [`Event::Resync`].
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
/// in `scope` are pushed to `held` in the poll that takes them, or, while `held` holds
/// [`FOLLOW_MEMORY`] messages, dropped with `lagged` set; the one `call` goes on either way.
/// Returns when the subscription ends or a `Resync` arrives; or else when the call completes.
///
/// When the call completes, its output (an answer or an error) is kept, the completed call is not
/// polled again, and in the same poll the race takes everything the subscription has ready at that
/// moment, the same way: an end or a gap found there wins and the output is dropped, also when the
/// call's own completion made it ready. Only when the subscription has nothing more ready does the
/// race return [`Raced::Done`].
///
/// Cancel-safe: a dropped race has taken from the subscription only what is in `held`, what it
/// dropped with `lagged` set, and what it skipped.
async fn race<F: Future>(
    events: &mut Subscription,
    held: &mut VecDeque<Message>,
    lagged: &mut bool,
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
                if let Err(stop) = take(event, held, lagged, scope) {
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
                if let Err(stop) = take(event, held, lagged, scope) {
                    return stop.into();
                }
            }
        }
    }
}

/// Takes one event of the subscription during a race: an end or a `Resync` stops the race, a
/// message in `scope` is held back (or, if `held` is full, dropped with `lagged` set), and
/// anything else is skipped.
fn take(
    event: Option<Event>,
    held: &mut VecDeque<Message>,
    lagged: &mut bool,
    scope: Scope<'_>,
) -> Result<(), Stop> {
    match event {
        None => Err(Stop::Ended),
        Some(Event::Resync) => Err(Stop::Gap),
        Some(Event::Message { message }) => {
            if scope.contains(&message) {
                if held.len() < FOLLOW_MEMORY {
                    held.push_back(message);
                } else {
                    *lagged = true;
                }
            }
            Ok(())
        }
        Some(Event::Peer { .. } | Event::PeerRemoved { .. } | Event::Status { .. }) => Ok(()),
    }
}
