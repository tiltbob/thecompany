//! The hello exchange and one-request chat streams.

use std::time::Duration;

use iroh::endpoint::{
    Connection, ConnectionError, ReadError, RecvStream, SendStream, Side, WriteError,
};
use numbers_base::{FrameError, WIRE_FRAME_MAX, read_frame, write_frame};
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::proto::chat::{ChatMessage, ChatReply, NackReason, WireChatMessage};
use crate::proto::{CHAT_PROTOCOL, Hello, HelloEcho, StreamHeader};
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
    mut local: Hello,
    budget: Duration,
) -> Result<[u8; 16], SendError> {
    let (mut send, mut recv) = match connection.side() {
        Side::Client => connection.open_bi().await,
        Side::Server => connection.accept_bi().await,
    }
    .map_err(|_| SendError::BadHello)?;

    let remote = match connection.side() {
        Side::Client => {
            write_frame(&mut send, &local, WIRE_FRAME_MAX)
                .await
                .map_err(|_| SendError::BadHello)?;
            let remote = read_hello(&mut recv, budget).await?;
            if remote.echo != Some(local.challenge) {
                return Err(SendError::BadHello);
            }
            write_frame(
                &mut send,
                &HelloEcho {
                    echo: remote.challenge,
                },
                WIRE_FRAME_MAX,
            )
            .await
            .map_err(|_| SendError::BadHello)?;
            remote
        }
        Side::Server => {
            let remote = read_hello(&mut recv, budget).await?;
            if remote.echo.is_some() {
                return Err(SendError::BadHello);
            }
            local.echo = Some(remote.challenge);
            write_frame(&mut send, &local, WIRE_FRAME_MAX)
                .await
                .map_err(|_| SendError::BadHello)?;
            let response: HelloEcho = read_frame(&mut recv, WIRE_FRAME_MAX, Some(budget))
                .await
                .map_err(hello_read_failure)?;
            if response.echo != local.challenge {
                return Err(SendError::BadHello);
            }
            remote
        }
    };
    let _ = send.finish();
    Ok(remote.generation)
}

async fn read_hello(recv: &mut RecvStream, budget: Duration) -> Result<Hello, SendError> {
    let hello: Hello = read_frame(recv, WIRE_FRAME_MAX, Some(budget))
        .await
        .map_err(hello_read_failure)?;
    hello.validate().map_err(|_| SendError::BadHello)?;
    Ok(hello)
}

fn hello_read_failure(error: FrameError) -> SendError {
    match error {
        FrameError::Timeout => SendError::Timeout,
        _ => SendError::BadHello,
    }
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
