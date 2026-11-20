//! An embeddable, loopback-only development relay for the numbers network.
//!
//! The tower serves plain HTTP, limits traffic received from each relay client, and sees
//! only ciphertext from traffic between endpoints.

use std::error::Error;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU32;

use iroh_relay::server::{ClientRateLimit, RelayConfig, Server, ServerConfig};

/// The default dev tower bind address.
pub const DEFAULT_BIND: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 3340));

const CLIENT_RX_BYTES_PER_SECOND: NonZeroU32 =
    NonZeroU32::new(2 * 1024 * 1024).expect("client receive rate is nonzero");

/// Configuration for a plain-HTTP development tower.
///
/// Build it with [`TowerConfig::dev`] or [`Default`]. The struct is `#[non_exhaustive]` so that
/// later fields do not break callers outside this crate.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct TowerConfig {
    /// A loopback socket address. Port zero lets the operating system choose a port.
    pub bind: SocketAddr,
}

impl TowerConfig {
    /// The development tower that listens on `bind`.
    ///
    /// A dev tower speaks plain HTTP, without TLS. It accepts only a loopback bind address, and
    /// [`Tower::spawn`] refuses any other. It has no allowlist: any client that can reach the
    /// port may use the relay.
    pub fn dev(bind: SocketAddr) -> Self {
        Self { bind }
    }
}

impl Default for TowerConfig {
    /// The dev tower on [`DEFAULT_BIND`].
    fn default() -> Self {
        Self::dev(DEFAULT_BIND)
    }
}

/// An error starting or stopping a development tower.
#[derive(Debug, thiserror::Error)]
pub enum TowerError {
    #[error("the dev tower requires a loopback bind address, got {0}")]
    NonLoopbackBind(SocketAddr),
    #[error("failed to start the dev tower: {0}")]
    Start(#[source] Box<dyn Error + Send + Sync>),
    #[error("the dev tower started without an HTTP listener")]
    MissingHttpAddress,
    #[error("failed to shut down the dev tower")]
    Shutdown(#[source] Box<dyn Error + Send + Sync>),
}

/// A running development tower.
pub struct Tower {
    server: Server,
    url: String,
}

impl Tower {
    /// Starts a rate-limited HTTP relay on the configured loopback address.
    pub async fn spawn(config: TowerConfig) -> Result<Self, TowerError> {
        if !config.bind.ip().is_loopback() {
            return Err(TowerError::NonLoopbackBind(config.bind));
        }

        let mut relay = RelayConfig::new(config.bind);
        relay.limits.client_rx = Some(ClientRateLimit::new(CLIENT_RX_BYTES_PER_SECOND));
        let mut server_config = ServerConfig::default();
        server_config.relay = Some(relay);

        let server = Server::spawn(server_config)
            .await
            .map_err(|error| TowerError::Start(Box::new(error)))?;
        let addr = server.http_addr().ok_or(TowerError::MissingHttpAddress)?;
        Ok(Self {
            server,
            url: format!("http://{addr}"),
        })
    }

    /// Returns the URL of the bound listener, including the port selected by the OS.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Requests graceful shutdown and waits for the relay to stop.
    ///
    /// The tower stops accepting connections and shuts the relay server down. It does not
    /// promise that every client sees the disconnect at once: with iroh-relay 1.5, a client
    /// that connected within about the last second sometimes still reported itself connected
    /// for some seconds afterwards (8 of 180 runs), but never when shutdown came at least one
    /// second later. A test that restarts a tower should therefore check that its clients
    /// noticed.
    pub async fn shutdown(self) -> Result<(), TowerError> {
        self.server
            .shutdown()
            .await
            .map_err(|error| TowerError::Shutdown(Box::new(error)))
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    use super::{Tower, TowerConfig, TowerError};

    #[tokio::test]
    async fn serves_http_on_the_bound_port_and_closes_on_shutdown() {
        timeout(Duration::from_secs(5), async {
            let tower = Tower::spawn(TowerConfig::dev((Ipv4Addr::LOCALHOST, 0).into()))
                .await
                .unwrap();
            let authority = tower.url().strip_prefix("http://").unwrap();
            let addr: SocketAddr = authority.parse().unwrap();
            assert_eq!(addr.ip(), Ipv4Addr::LOCALHOST);
            assert_ne!(addr.port(), 0);

            let mut stream = TcpStream::connect(addr).await.unwrap();
            let request =
                format!("GET /healthz HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));

            tower.shutdown().await.unwrap();
            let error = TcpStream::connect(addr).await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ConnectionRefused);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rejects_non_loopback_addresses() {
        let bind = (Ipv4Addr::UNSPECIFIED, 0).into();
        let result = Tower::spawn(TowerConfig::dev(bind)).await;
        assert!(matches!(result, Err(TowerError::NonLoopbackBind(addr)) if addr == bind));
    }
}
