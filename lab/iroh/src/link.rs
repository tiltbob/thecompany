//! Section 7: a minimal ProtocolHandler on `numbers/0` that echoes length-prefixed messages.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler};

use crate::Res;

pub const MAX_MSG: usize = 64 * 1024;

#[derive(Debug, Default)]
pub struct LinkStats {
    pub accepted: AtomicUsize,
    pub streams: AtomicUsize,
}

#[derive(Debug, Clone, Default)]
pub struct Link {
    pub stats: Arc<LinkStats>,
    /// Refuse every connection from `accept` (the AcceptError form).
    pub refuse: Refuse,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Refuse {
    #[default]
    No,
    /// `return Err(e!(AcceptError::NotAllowed))` only.
    ErrOnly,
    /// `conn.close(403, ..)` first, then the error.
    CloseThenErr,
}

impl ProtocolHandler for Link {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        self.stats.accepted.fetch_add(1, Ordering::SeqCst);
        let _peer = conn.remote_id(); // infallible after the handshake
        match self.refuse {
            Refuse::No => {}
            Refuse::ErrOnly => return Err(n0_error::e!(AcceptError::NotAllowed)),
            Refuse::CloseThenErr => {
                conn.close(crate::gate::CLOSE_NOT_ALLOWED.into(), b"refused in accept");
                return Err(n0_error::e!(AcceptError::NotAllowed));
            }
        }
        // When this returns the connection is dropped; loop until the peer closes it.
        while let Ok((mut send, mut recv)) = conn.accept_bi().await {
            self.stats.streams.fetch_add(1, Ordering::SeqCst);
            let msg = read_msg(&mut recv).await.map_err(AcceptError::from_boxed)?;
            write_msg(&mut send, &msg)
                .await
                .map_err(AcceptError::from_boxed)?;
        }
        Ok(())
    }
}

pub async fn write_msg(send: &mut SendStream, msg: &[u8]) -> Res {
    let len = u32::try_from(msg.len())?;
    send.write_all(&len.to_be_bytes()).await?;
    send.write_all(msg).await?;
    send.finish()?;
    Ok(())
}

pub async fn read_msg(recv: &mut RecvStream) -> Res<Vec<u8>> {
    let mut len = [0u8; 4];
    recv.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_MSG {
        return Err("message too long".into());
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Opens a bi stream, writes one message right away (streams are lazy) and reads the echo.
pub async fn request(conn: &Connection, msg: &[u8]) -> Res<Vec<u8>> {
    let (mut send, mut recv) = conn.open_bi().await?;
    write_msg(&mut send, msg).await?;
    read_msg(&mut recv).await
}
