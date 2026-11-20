use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use chief::api::{
    ApiError, Direction, LOG_PAGE_MAX_BYTES, LOG_PAGE_MAX_MESSAGES, LogPage, Message, MessageStatus,
};

use crate::MESSAGE_LOG_CAPACITY;

pub(super) struct History {
    ring: VecDeque<Message>,
    next_seq: u64,
}

impl Default for History {
    fn default() -> Self {
        Self {
            ring: VecDeque::new(),
            next_seq: 1,
        }
    }
}

impl History {
    pub(super) fn append(
        &mut self,
        peer: &str,
        id: String,
        direction: Direction,
        body: String,
        status: MessageStatus,
    ) -> Result<Message, ApiError> {
        // Keep the next sequence representable even for the exclusive cursor of a limit-0 page.
        let next = self
            .next_seq
            .checked_add(1)
            .ok_or_else(|| internal("the station ran out of message sequence numbers"))?;
        let message = Message {
            seq: self.next_seq,
            rev: 0,
            id,
            peer: peer.to_owned(),
            direction,
            body,
            at_ms: now_ms(),
            status,
        };
        self.next_seq = next;
        if self.ring.len() == MESSAGE_LOG_CAPACITY {
            self.ring.pop_front();
        }
        self.ring.push_back(message.clone());
        Ok(message)
    }

    pub(super) fn message(&self, seq: u64) -> Result<Message, ApiError> {
        self.ring
            .iter()
            .find(|message| message.seq == seq)
            .cloned()
            .ok_or(ApiError::MessageNotFound { seq })
    }

    /// Updates the log if the outgoing message is still retained. The actor separately sends
    /// the outcome event even when this finds no entry.
    pub(super) fn update(&mut self, message: &Message) {
        if let Some(entry) = self.ring.iter_mut().find(|entry| entry.seq == message.seq) {
            *entry = message.clone();
        }
    }

    pub(super) fn page(
        &self,
        peer: Option<&str>,
        before: Option<u64>,
        limit: u32,
    ) -> Result<LogPage, ApiError> {
        let mut matching = self
            .ring
            .iter()
            .rev()
            .filter(|message| {
                peer.is_none_or(|name| message.peer == name)
                    && before.is_none_or(|seq| message.seq < seq)
            })
            .peekable();
        if limit == 0 {
            return Ok(LogPage {
                messages: Vec::new(),
                next_before: matching
                    .next()
                    .map(|message| before.unwrap_or(message.seq + 1)),
            });
        }

        let limit = usize::try_from(limit.min(LOG_PAGE_MAX_MESSAGES)).unwrap_or(usize::MAX);
        let mut messages = Vec::new();
        let mut total = 0;
        while messages.len() < limit
            && let Some(message) = matching.peek()
        {
            let size = serde_json::to_vec(message)
                .map_err(|_| internal("could not encode the message log"))?
                .len();
            if size > LOG_PAGE_MAX_BYTES {
                return Err(internal("a message is too large for a log page"));
            }
            if total + size > LOG_PAGE_MAX_BYTES {
                break;
            }
            total += size;
            messages.push((*message).clone());
            matching.next();
        }
        messages.reverse();
        let next_before = if matching.peek().is_some() {
            messages.first().map(|message| message.seq)
        } else {
            None
        };
        Ok(LogPage {
            messages,
            next_before,
        })
    }
}

pub(super) fn internal(detail: &str) -> ApiError {
    ApiError::Internal {
        detail: detail.to_owned(),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests;
