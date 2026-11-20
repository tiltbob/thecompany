//! The TLS tower: its configuration, its canonical URL, and its rustls server configuration.
//!
//! The certificate and key themselves are loaded, checked and reloaded in [`crate::certs`].

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use iroh_base::RelayUrl;
use iroh_relay::defaults::{DEFAULT_HTTP_PORT, DEFAULT_HTTPS_PORT, DEFAULT_RELAY_QUIC_PORT};
use iroh_relay::server::{CertConfig, DEFAULT_CERT_RELOAD_INTERVAL};

use crate::TowerError;
use crate::certs::{CurrentPair, Reloader, load_pair};

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
///
/// Its `Debug` output shows the bind addresses and the reload interval, never the host name or
/// the file paths, which often contain it.
#[derive(Clone)]
#[non_exhaustive]
pub struct TlsTower {
    /// The host name the tower is reached by: a DNS name or an IP literal.
    ///
    /// A DNS name may end in one dot, which is dropped; then it has 1 to 253 characters in
    /// labels separated by single dots, each label 1 to 63 ASCII letters, digits or hyphens,
    /// neither starting nor ending with a hyphen. An IP literal is IPv4 or IPv6, the latter
    /// with or without brackets. The name goes into [`Tower::url`](crate::Tower::url) in lower
    /// case, or written canonically, and should match a name in the certificate.
    pub hostname: String,
    /// Where the relay listens for HTTPS. Port zero lets the operating system choose a port.
    pub https_bind: SocketAddr,
    /// Where QUIC address discovery listens (UDP), or `None` for no address discovery.
    ///
    /// Address discovery tells endpoints their public address, which direct paths across NATs
    /// need. It uses the relay's TLS configuration.
    pub qad_bind: Option<SocketAddr>,
    /// The certificate chain, a PEM file that starts with the tower's own certificate.
    ///
    /// The tower reads it at spawn and at every reload. It must hold at least one certificate,
    /// and the first must parse and belong to the private key.
    pub cert_path: PathBuf,
    /// The private key of the certificate, a PEM file with exactly one unencrypted key (PKCS#8,
    /// PKCS#1 or SEC1; RSA, ECDSA P-256 or P-384, or Ed25519).
    ///
    /// The tower reads it at spawn and at every reload, in a buffer that is zeroed afterwards,
    /// and hands it to rustls, which keeps it only inside its signing key. The tower never logs
    /// it, never puts any byte of it into an error, and keeps no other live copy. Freed memory
    /// may still hold scratch copies made by the PEM decoder, which the tower cannot zero.
    pub key_path: PathBuf,
    /// How often the tower reads both files again.
    ///
    /// A pair that passes the same checks as at spawn replaces the one in use, so a renewed
    /// certificate is served without a restart. A pair that fails them is ignored, with one
    /// warning that names the file and the kind of problem, and the pair in use stays. The
    /// default is iroh-relay's `DEFAULT_CERT_RELOAD_INTERVAL`, one day. [`Tower::spawn`]
    /// refuses zero.
    ///
    /// [`Tower::spawn`]: crate::Tower::spawn
    pub reload_interval: Duration,
}

impl TlsTower {
    /// A TLS tower reached as `hostname`, with its certificate chain and key in PEM files.
    ///
    /// It listens on [`DEFAULT_HTTPS_BIND`], runs address discovery on [`DEFAULT_QAD_BIND`],
    /// and reloads its certificate once a day.
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
            reload_interval: DEFAULT_CERT_RELOAD_INTERVAL,
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

    /// Sets how often the tower reads its certificate and key files again.
    pub fn reload_interval(mut self, every: Duration) -> Self {
        self.reload_interval = every;
        self
    }
}

impl fmt::Debug for TlsTower {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsTower")
            .field("https_bind", &self.https_bind)
            .field("qad_bind", &self.qad_bind)
            .field("reload_interval", &self.reload_interval)
            .finish_non_exhaustive()
    }
}

/// The host part of the canonical URL, or `None` if `hostname` is neither a DNS host name nor
/// an IP literal.
fn canonical_host(hostname: &str) -> Option<String> {
    if let Some(inner) = hostname
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        return inner.parse::<Ipv6Addr>().ok().map(|ip| format!("[{ip}]"));
    }
    match hostname.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => return Some(ip.to_string()),
        Ok(IpAddr::V6(ip)) => return Some(format!("[{ip}]")),
        Err(_) => {}
    }
    let name = hostname.strip_suffix('.').unwrap_or(hostname);
    let valid_label = |label: &str| {
        (1..=63).contains(&label.len())
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    };
    if !(1..=253).contains(&name.len()) || !name.split('.').all(valid_label) {
        return None;
    }
    Some(name.to_ascii_lowercase())
}

/// The canonical URL of a TLS tower reached as `hostname` on HTTPS port `port`.
///
/// The scheme is `https`, the host is in lower case without a trailing dot (an IP literal is
/// written canonically, IPv6 in brackets), the port is left out when it is 443, and the path
/// is `/`.
pub(crate) fn tower_url(hostname: &str, port: u16) -> Result<String, TowerError> {
    let host = canonical_host(hostname).ok_or(TowerError::InvalidHostname)?;
    let text = if port == DEFAULT_HTTPS_PORT {
        format!("https://{host}/")
    } else {
        format!("https://{host}:{port}/")
    };
    // A second guard: the URL parser must read back exactly what we wrote. It refuses, for
    // example, a numeric name that WHATWG URL parsing would turn into an IPv4 address.
    let url: RelayUrl = text.parse().map_err(|_| TowerError::InvalidHostname)?;
    if url.host_str() != Some(host.as_str()) || url.as_str() != text {
        return Err(TowerError::InvalidHostname);
    }
    Ok(text)
}

/// The relay's certificate configuration and the task that reloads it.
///
/// rustls gets the ring provider explicitly; nothing is installed process-wide. The pair is
/// loaded and checked here, before anything is bound.
pub(crate) fn cert_config(tls: &TlsTower) -> Result<(CertConfig, Reloader), TowerError> {
    if tls.reload_interval.is_zero() {
        return Err(TowerError::InvalidReloadInterval);
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let pair = load_pair(&provider, &tls.cert_path, &tls.key_path)
        .map_err(|failure| failure.into_error())?;
    let current = Arc::new(CurrentPair::new(pair));
    let server_config = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|error| TowerError::Start(Box::new(error)))?
        .with_no_client_auth()
        .with_cert_resolver(Arc::clone(&current) as Arc<dyn rustls::server::ResolvesServerCert>);
    let reloader = Reloader::spawn(
        &current,
        provider,
        tls.cert_path.clone(),
        tls.key_path.clone(),
        tls.reload_interval,
    );
    Ok((CertConfig::Manual { server_config }, reloader))
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

    /// The DNS host-name rule, case by case.
    #[test]
    fn applies_the_dns_host_name_rule() {
        let label63 = "a".repeat(63);
        let label64 = "a".repeat(64);
        // 4 labels of 63 and 3 dots: 255 characters, over the limit of 253.
        let too_long = [label63.as_str(); 4].join(".");
        // 3 labels of 63, one of 61 and 3 dots: exactly 253 characters.
        let longest = format!("{label63}.{label63}.{label63}.{}", "b".repeat(61));
        assert_eq!(longest.len(), 253);
        for refused in [
            "tower..example.com",
            "-tower.example.com",
            "tower-.example.com",
            "tower_1.example.com",
            "tower;1.example.com",
            &format!("{label64}.example.com"),
            &too_long,
        ] {
            assert!(tower_url(refused, 443).is_err(), "accepted {refused}");
        }
        for (accepted, url) in [
            (
                format!("{label63}.example.com"),
                format!("https://{label63}.example.com/"),
            ),
            (
                "tower.123.example".to_owned(),
                "https://tower.123.example/".to_owned(),
            ),
            (
                "TOWER-1.Example.COM".to_owned(),
                "https://tower-1.example.com/".to_owned(),
            ),
            (longest.clone(), format!("https://{longest}/")),
            (format!("{longest}."), format!("https://{longest}/")),
        ] {
            assert_eq!(tower_url(&accepted, 443).unwrap(), url);
        }
    }

    /// A numeric DNS name passes the label rule, but URL parsing reads it as an IPv4 address
    /// (`1.2.3` is 1.2.0.3), so the read-back guard refuses it.
    #[test]
    fn refuses_a_numeric_name_that_a_url_would_read_as_an_address() {
        for refused in ["1.2.3", "123", "1.2", "127.1", "0x7f.1", "017.0.0.1"] {
            assert!(tower_url(refused, 443).is_err(), "accepted {refused}");
        }
        assert_eq!(tower_url("10.1.2.3", 443).unwrap(), "https://10.1.2.3/");
        assert_eq!(
            tower_url("1.example.com", 443).unwrap(),
            "https://1.example.com/"
        );
    }

    #[test]
    fn refuses_a_non_ascii_name() {
        assert!(tower_url("türm.example", 443).is_err());
    }

    /// Neither `TlsTower` nor the `TowerConfig` that holds it shows the host name.
    #[test]
    fn debug_output_leaves_out_the_host_name_and_the_paths() {
        let tls = super::TlsTower::new(
            "qzx-name.example",
            "/etc/live/qzx-cert-dir/fullchain.pem",
            "/etc/live/qzx-key-dir/privkey.pem",
        )
        .https_bind(([127, 0, 0, 1], 44443).into());
        let config = crate::TowerConfig::tls(tls.clone());
        for text in [
            format!("{tls:?}"),
            format!("{tls:#?}"),
            format!("{config:?}"),
            format!("{config:#?}"),
        ] {
            for marker in ["qzx-name", "qzx-cert-dir", "qzx-key-dir"] {
                assert!(!text.contains(marker), "{text}");
            }
            assert!(text.contains("127.0.0.1:44443"), "{text}");
        }
    }
}
