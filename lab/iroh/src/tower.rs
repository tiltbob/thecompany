//! Section 2: the plain-HTTP dev tower.
use std::{net::SocketAddr, num::NonZeroU32};

use iroh::RelayUrl; // iroh-relay does not re-export RelayUrl; `iroh_base` is not a dependency
use iroh_relay::server::{ClientRateLimit, RelayConfig, Server, ServerConfig};

use crate::Res;

/// A running plain-HTTP relay.
#[derive(Debug)]
pub struct Tower {
    server: Server,
    url: RelayUrl,
}

impl Tower {
    /// Binds `addr` (use port 0 for a random port) and serves plain HTTP.
    pub async fn spawn(addr: SocketAddr) -> Res<Self> {
        let mut relay = RelayConfig::new(addr); // tls: None => plain HTTP
        relay.limits.client_rx = Some(ClientRateLimit::new(
            NonZeroU32::new(2 * 1024 * 1024).ok_or("zero")?,
        ));
        let mut config = ServerConfig::default();
        config.relay = Some(relay);
        let server = Server::spawn(config).await?;
        let addr = server.http_addr().ok_or("relay not enabled")?;
        let url: RelayUrl = format!("http://{addr}").parse()?;
        Ok(Self { server, url })
    }

    pub fn url(&self) -> &RelayUrl {
        &self.url
    }

    pub fn addr(&self) -> Option<SocketAddr> {
        self.server.http_addr()
    }

    pub fn https_addr(&self) -> Option<SocketAddr> {
        self.server.https_addr()
    }

    /// Graceful shutdown.
    pub async fn shutdown(self) -> Res {
        self.server.shutdown().await?;
        Ok(())
    }
}
