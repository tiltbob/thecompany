//! M2 (section 11): a tower as production would run it. TLS from certificate files (through
//! `reloading_resolver`), optional QAD, an allowlist and a per-client rate limit. Also serves
//! plain HTTP when `tls` is None, so the multi-tower tests can use the same type.
//!
//! The rustls crypto provider is passed explicitly everywhere; nothing here installs a process
//! default (T20 checks that none got installed).
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use iroh::RelayUrl;
use iroh_relay::server::{
    CertConfig, ClientRateLimit, DEFAULT_CERT_RELOAD_INTERVAL, DynAccessControl, Metrics,
    QuicConfig, RelayConfig, RelayService, Server, ServerConfig, TlsConfig, reloading_resolver,
};

use crate::Res;

/// Where a production tower reads its certificate chain and key (PEM).
#[derive(Debug, Clone)]
pub struct CertFiles {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug)]
pub struct TowerSpec {
    /// Plain HTTP. With TLS it still binds and serves only the captive-portal probe.
    pub http_bind: SocketAddr,
    /// HTTPS bind address and certificate files. None = plain HTTP tower.
    pub tls: Option<(SocketAddr, CertFiles)>,
    /// QAD (UDP). Needs `tls`: it inherits the relay's rustls config.
    pub quic: Option<SocketAddr>,
    /// None = AllowAll (the iroh-relay default).
    pub access: Option<Arc<dyn DynAccessControl>>,
    pub client_rx: Option<ClientRateLimit>,
}

impl TowerSpec {
    pub fn plain(http_bind: SocketAddr) -> Self {
        Self {
            http_bind,
            tls: None,
            quic: None,
            access: None,
            client_rx: None,
        }
    }

    pub fn tls(http_bind: SocketAddr, https_bind: SocketAddr, files: CertFiles) -> Self {
        Self {
            tls: Some((https_bind, files)),
            ..Self::plain(http_bind)
        }
    }
}

/// The ring provider, built locally and passed by value: no `install_default`.
pub fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// The relay's rustls server config from cert/key files, reloaded once a day.
pub async fn server_config(files: &CertFiles) -> Res<rustls::ServerConfig> {
    let provider = provider();
    let resolver = reloading_resolver(
        &provider,
        files.cert.clone(),
        files.key.clone(),
        DEFAULT_CERT_RELOAD_INTERVAL,
    )
    .await?;
    Ok(rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_cert_resolver(resolver))
}

#[derive(Debug)]
pub struct M2Tower {
    server: Server,
}

impl M2Tower {
    pub async fn spawn(spec: TowerSpec) -> Res<Self> {
        let mut relay = RelayConfig::new(spec.http_bind);
        if let Some((https_bind, files)) = &spec.tls {
            let cert = CertConfig::Manual {
                server_config: server_config(files).await?,
            };
            relay.tls = Some(TlsConfig::new(*https_bind, cert));
        }
        if let Some(access) = spec.access {
            relay.access = access;
        }
        relay.limits.client_rx = spec.client_rx;
        let mut config = ServerConfig::default();
        config.relay = Some(relay);
        config.quic = spec.quic.map(QuicConfig::new);
        let server = Server::spawn(config).await?;
        Ok(Self { server })
    }

    /// Plain-HTTP listener: the relay itself without TLS, only `/generate_204` with TLS.
    pub fn http_addr(&self) -> Option<SocketAddr> {
        self.server.http_addr()
    }
    pub fn https_addr(&self) -> Option<SocketAddr> {
        self.server.https_addr()
    }
    pub fn quic_addr(&self) -> Option<SocketAddr> {
        self.server.quic_addr()
    }

    /// The relay URL for `host` (an IP literal or a DNS name): https when TLS is on.
    pub fn url_for(&self, host: &str) -> Res<RelayUrl> {
        let s = match (self.https_addr(), self.http_addr()) {
            (Some(a), _) => format!("https://{host}:{}/", a.port()),
            (None, Some(a)) => format!("http://{host}:{}/", a.port()),
            _ => return Err("relay not enabled".into()),
        };
        Ok(s.parse()?)
    }

    /// `http://<http_addr>/` for a plain tower.
    pub fn url(&self) -> Res<RelayUrl> {
        let a = self.http_addr().ok_or("relay not enabled")?;
        self.url_for(&a.ip().to_string())
    }

    pub fn metrics(&self) -> &Metrics {
        &self.server.metrics().server
    }

    pub fn service(&self) -> Option<&RelayService> {
        self.server.relay_service()
    }

    pub async fn shutdown(self) -> Res {
        self.server.shutdown().await?;
        Ok(())
    }
}

/// `iroh::RelayConfig` for the client map; `qad` is the tower's QAD port, if any.
pub fn client_config(url: &RelayUrl, qad: Option<u16>) -> Arc<iroh::RelayConfig> {
    Arc::new(iroh::RelayConfig::new(
        url.clone(),
        qad.map(iroh_relay::RelayQuicConfig::new),
    ))
}
