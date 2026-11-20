//! The hello exchange and one-request chat streams.

use std::time::Duration;

use iroh::endpoint::{
    Connection, ConnectionError, ReadError, RecvStream, SendStream, Side, WriteError,
};
use numbers_base::{FrameError, WIRE_FRAME_MAX, read_frame, write_frame};
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::proto::chat::{ChatMessage, ChatReply, NackReason, WireChatMessage};
use crate::proto::{CHAT_PROTOCOL, Hello, StreamHeader};
use crate::{CLOSE_NOT_ALLOWED, Protocol};

use super::{CLOSE_LINK, IncomingChat, Peer, SendError, Shared, lock};

pub(super) fn close_code(connection: &Connection) -> Option<u32> {
    match connection.close_reason()? {
        ConnectionError::ApplicationClosed(close) => {
            u32::try_from(close.error_code.into_inner()).ok()
        }
        _ => None,
    }
}

pub(super) async fn exchange_hello(
    connection: &Connection,
    budget: Duration,
) -> Result<(), SendError> {
    match connection.side() {
        Side::Client => {
            let (mut send, mut recv) = connection
                .open_bi()
                .await
                .map_err(|_| SendError::BadHello)?;
            write_hello(&mut send).await?;
            read_hello(&mut recv, budget).await
        }
        Side::Server => {
            let (mut send, mut recv) = connection
                .accept_bi()
                .await
                .map_err(|_| SendError::BadHello)?;
            read_hello(&mut recv, budget).await?;
            write_hello(&mut send).await
        }
    }
}

async fn write_hello(send: &mut SendStream) -> Result<(), SendError> {
    write_frame(send, &Hello::local(), WIRE_FRAME_MAX)
        .await
        .map_err(|_| SendError::BadHello)?;
    // The peer may already have read the frame and closed its receive half.
    let _ = send.finish();
    Ok(())
}

async fn read_hello(recv: &mut RecvStream, budget: Duration) -> Result<(), SendError> {
    let hello: Hello = read_frame(recv, WIRE_FRAME_MAX, Some(budget))
        .await
        .map_err(|error| match error {
            FrameError::Timeout => SendError::Timeout,
            _ => SendError::BadHello,
        })?;
    hello.validate().map_err(|_| SendError::BadHello)
}

pub(super) async fn send_chat(
    connection: &Connection,
    message: &ChatMessage,
    budget: Duration,
) -> Result<ChatReply, SendError> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|_| SendError::Disconnected)?;
    write_frame(&mut send, &StreamHeader::chat(), WIRE_FRAME_MAX)
        .await
        .map_err(write_failure)?;
    write_frame(&mut send, message, WIRE_FRAME_MAX)
        .await
        .map_err(write_failure)?;
    // The peer can stop reading after the message. Read the reply even if that races FIN.
    let _ = send.finish();
    read_frame(&mut recv, WIRE_FRAME_MAX, Some(budget))
        .await
        .map_err(read_failure)
}

fn write_failure(error: FrameError) -> SendError {
    if let FrameError::Io(error) = error
        && let Some(WriteError::Stopped(code)) = error.get_ref().and_then(|e| e.downcast_ref())
        && code.into_inner() == u64::from(CLOSE_NOT_ALLOWED)
    {
        return SendError::NotAllowed;
    }
    SendError::Disconnected
}

fn read_failure(error: FrameError) -> SendError {
    match error {
        FrameError::Io(error) => {
            if let Some(ReadError::Reset(code)) = error.get_ref().and_then(|e| e.downcast_ref())
                && code.into_inner() == u64::from(CLOSE_NOT_ALLOWED)
            {
                return SendError::NotAllowed;
            }
            SendError::Disconnected
        }
        FrameError::Closed => SendError::Disconnected,
        FrameError::Timeout => SendError::Timeout,
        FrameError::Json(_) | FrameError::TooLarge { .. } => SendError::BadReply,
    }
}

pub(super) async fn receive_chat(
    shared: &Shared,
    peer: &Peer,
    connection: &Connection,
    mut send: SendStream,
    mut recv: RecvStream,
) {
    let budget = shared.config.send_deadline;
    let response = async {
        let reply = chat_reply(shared, peer, connection, &mut recv, budget).await?;
        write_frame(&mut send, &reply, WIRE_FRAME_MAX)
            .await
            .map_err(|_| CLOSE_LINK)?;
        let _ = send.finish();
        Ok(())
    };
    let reset = match timeout(budget, response).await {
        Ok(Ok(())) => return,
        Ok(Err(code)) => code,
        Err(_) => CLOSE_LINK,
    };
    let _ = send.reset(reset.into());
    let _ = recv.stop(reset.into());
}

async fn chat_reply(
    shared: &Shared,
    peer: &Peer,
    connection: &Connection,
    recv: &mut RecvStream,
    budget: Duration,
) -> Result<ChatReply, u32> {
    let header: StreamHeader = read_frame(recv, WIRE_FRAME_MAX, Some(budget))
        .await
        .map_err(|_| CLOSE_LINK)?;
    if let Err(reason) = header.validate_chat() {
        return Ok(ChatReply::Nack(reason));
    }
    let protocol = Protocol::Stream(CHAT_PROTOCOL);
    if shared.authorizer.authorize(&peer.id, protocol).is_none() {
        return Err(CLOSE_NOT_ALLOWED);
    }
    let wire: WireChatMessage = read_frame(recv, WIRE_FRAME_MAX, Some(budget))
        .await
        .map_err(|_| CLOSE_LINK)?;
    let message = match ChatMessage::try_from(wire) {
        Ok(message) => message,
        Err(reason) => return Ok(ChatReply::Nack(reason)),
    };

    let (reply, receive) = oneshot::channel();
    {
        // Publish only on the current link, serialized against replacement and revocation.
        let state = peer.state();
        if !state.is_current(connection) {
            return Err(CLOSE_LINK);
        }
        let caller = shared
            .authorizer
            .authorize(&peer.id, protocol)
            .ok_or(CLOSE_NOT_ALLOWED)?;
        let incoming = IncomingChat {
            station_id: peer.id,
            peer: caller,
            message,
            reply,
        };
        if lock(&shared.incoming)
            .as_ref()
            .is_none_or(|mailbox| mailbox.try_send(incoming).is_err())
        {
            return Ok(ChatReply::Nack(NackReason::Unavailable));
        }
    }
    Ok(receive
        .await
        .unwrap_or(ChatReply::Nack(NackReason::Unavailable)))
}
