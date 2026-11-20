//! The TLS tower: its configuration, its canonical URL, and the checks of its certificate files.
//!
//! The private key file is read only by iroh-relay's certificate resolver, from the path given
//! here. This module never opens, parses, logs or holds it.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iroh_base::RelayUrl;
use iroh_relay::defaults::{DEFAULT_HTTP_PORT, DEFAULT_HTTPS_PORT, DEFAULT_RELAY_QUIC_PORT};
use iroh_relay::server::{CertConfig, DEFAULT_CERT_RELOAD_INTERVAL, reloading_resolver};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

use crate::TowerError;

/// The default plain-HTTP bind address of a TLS tower: all IPv4 interfaces, port 80.
pub const DEFAULT_TLS_HTTP_BIND: SocketAddr = all_ipv4(DEFAULT_HTTP_PORT);

/// The default HTTPS bind address of a TLS tower: all IPv4 interfaces, port 443.
pub const DEFAULT_HTTPS_BIND: SocketAddr = all_ipv4(DEFAULT_HTTPS_PORT);

/// The default QUIC address discovery bind address: all IPv4 interfaces, UDP port 7842.
pub const DEFAULT_QAD_BIND: SocketAddr = all_ipv4(DEFAULT_RELAY_QUIC_PORT);

const fn all_ipv4(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port))
}

/// The TLS part of a tower's configuration: its name, its listeners and its certificate files.
///
/// Build it with [`TlsTower::new`] and pass it to [`TowerConfig::tls`](crate::TowerConfig::tls).
/// The struct is `#[non_exhaustive]`; adjust it with the builder-style setters.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TlsTower {
    /// The host name the tower is reached by: a DNS name or an IP literal.
    ///
    /// It goes into [`Tower::url`](crate::Tower::url), in lower case and without a trailing dot,
    /// and should match a name in the certificate.
    pub hostname: String,
    /// Where the relay listens for HTTPS. Port zero lets the operating system choose a port.
    pub https_bind: SocketAddr,
    /// Where QUIC address discovery listens (UDP), or `None` for no address discovery.
    ///
    /// Address discovery tells endpoints their public address, which direct paths across NATs
    /// need. It uses the relay's TLS configuration.
    pub qad_bind: Option<SocketAddr>,
    /// The certificate chain, a PEM file that starts with the tower's own certificate.
    pub cert_path: PathBuf,
    /// The private key of the certificate, a PEM file.
    ///
    /// Only iroh-relay's certificate resolver reads it, at spawn and then once a day, so a
    /// renewed certificate and key are picked up without a restart.
    pub key_path: PathBuf,
}

impl TlsTower {
    /// A TLS tower reached as `hostname`, with its certificate chain and key in PEM files.
    ///
    /// It listens on [`DEFAULT_HTTPS_BIND`] and runs address discovery on [`DEFAULT_QAD_BIND`].
    pub fn new(
        hostname: impl Into<String>,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            hostname: hostname.into(),
            https_bind: DEFAULT_HTTPS_BIND,
            qad_bind: Some(DEFAULT_QAD_BIND),
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }

    /// Sets where the relay listens for HTTPS.
    pub fn https_bind(mut self, addr: SocketAddr) -> Self {
        self.https_bind = addr;
        self
    }

    /// Sets where QUIC address discovery listens, or turns it off with `None`.
    pub fn qad_bind(mut self, addr: Option<SocketAddr>) -> Self {
        self.qad_bind = addr;
        self
    }
}

/// The canonical URL of a TLS tower reached as `hostname` on HTTPS port `port`.
///
/// The scheme is `https`, the host is in lower case without a trailing dot (an IPv6 literal is
/// bracketed), the port is left out when it is 443, and the path is `/`.
pub(crate) fn tower_url(hostname: &str, port: u16) -> Result<String, TowerError> {
    let invalid = || TowerError::InvalidHostname(hostname.to_owned());
    let name = hostname.strip_suffix('.').unwrap_or(hostname);
    let bare = name
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(name);
    let host = match bare.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => ip.to_string(),
        Ok(IpAddr::V6(ip)) => format!("[{ip}]"),
        Err(_) if name.is_empty() || name.ends_with('.') => return Err(invalid()),
        Err(_) => name.to_ascii_lowercase(),
    };
    let text = if port == DEFAULT_HTTPS_PORT {
        format!("https://{host}/")
    } else {
        format!("https://{host}:{port}/")
    };
    // The URL parser accepts more than a host name; insist that it read back what we wrote.
    let url: RelayUrl = text.parse().map_err(|_| invalid())?;
    if url.host_str() != Some(host.as_str()) || url.as_str() != text {
        return Err(invalid());
    }
    Ok(text)
}

/// Checks the certificate file: it must be readable and hold at least one PEM certificate.
///
/// The certificate chain is public. The check does not report the file's contents, because an
/// operator who swapped the two paths would otherwise see part of the key in the error.
pub(crate) fn check_certificate(path: &Path) -> Result<(), TowerError> {
    let invalid = |reason: String| TowerError::CertificateFile {
        path: path.to_owned(),
        reason,
    };
    let pem = std::fs::read(path).map_err(|error| invalid(error.to_string()))?;
    let mut found = false;
    for certificate in CertificateDer::pem_slice_iter(&pem) {
        if certificate.is_err() {
            return Err(invalid("it holds a malformed PEM section".to_owned()));
        }
        found = true;
    }
    if found {
        Ok(())
    } else {
        Err(invalid("it holds no PEM certificate".to_owned()))
    }
}

/// The relay's certificate configuration: rustls with the ring provider, passed explicitly,
/// and a resolver that loads the certificate files and reloads them once a day.
///
/// Nothing is installed process-wide.
pub(crate) async fn cert_config(tls: &TlsTower) -> Result<CertConfig, TowerError> {
    check_certificate(&tls.cert_path)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let resolver = reloading_resolver(
        &provider,
        tls.cert_path.clone(),
        tls.key_path.clone(),
        DEFAULT_CERT_RELOAD_INTERVAL,
    )
    .await
    // The certificate passed the check above, so the key is what failed to load. The reason
    // comes from the PEM reader and rustls, which name the problem but never the key's bytes.
    .map_err(|error| TowerError::KeyFile {
        path: tls.key_path.clone(),
        reason: format!("{error:#}"),
    })?;
    let server_config = server_config(provider)?.with_cert_resolver(resolver);
    Ok(CertConfig::Manual { server_config })
}

/// A rustls server configuration that still needs its certificate resolver.
type WantsCert = rustls::ConfigBuilder<rustls::ServerConfig, rustls::server::WantsServerCert>;

/// rustls with the given provider, its safe default protocol versions, and no client auth.
fn server_config(provider: Arc<CryptoProvider>) -> Result<WantsCert, TowerError> {
    rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map(|builder| builder.with_no_client_auth())
        .map_err(|error| TowerError::Start(Box::new(error)))
}

#[cfg(test)]
mod tests {
    use super::tower_url;

    #[test]
    fn leaves_out_port_443_and_keeps_any_other() {
        assert_eq!(
            tower_url("tower1.example.com", 443).unwrap(),
            "https://tower1.example.com/"
        );
        assert_eq!(
            tower_url("tower1.example.com", 8443).unwrap(),
            "https://tower1.example.com:8443/"
        );
    }

    #[test]
    fn lowers_the_case_and_strips_one_trailing_dot() {
        assert_eq!(
            tower_url("Tower1.EXAMPLE.com.", 443).unwrap(),
            "https://tower1.example.com/"
        );
        assert!(tower_url("tower1.example.com..", 443).is_err());
        assert!(tower_url(".", 443).is_err());
        assert!(tower_url("", 443).is_err());
    }

    #[test]
    fn writes_ip_literals_canonically() {
        assert_eq!(
            tower_url("127.0.0.1", 45123).unwrap(),
            "https://127.0.0.1:45123/"
        );
        assert_eq!(tower_url("::1", 443).unwrap(), "https://[::1]/");
        assert_eq!(tower_url("[0:0::1]", 8443).unwrap(), "https://[::1]:8443/");
    }

    #[test]
    fn rejects_text_that_is_not_a_host_name() {
        for name in [
            "tower/path",
            "user@tower",
            "tower:443",
            "tower one",
            "tower?x",
            "tower#x",
        ] {
            assert!(tower_url(name, 443).is_err(), "{name}");
        }
    }
}
