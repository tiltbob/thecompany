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

/// The longest accepted [`TlsTower::reload_interval`]: 365 days.
///
/// A longer interval is refused at spawn. The bound keeps every reload deadline far from the
/// limits of the clock, so that scheduling it can never overflow.
pub const MAX_RELOAD_INTERVAL: Duration = Duration::from_secs(365 * 24 * 60 * 60);

const fn all_ipv4(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port))
}

/// The TLS part of a tower's configuration: its name, its listeners and its certificate files.
///
/// Build it with [`TlsTower::new`] and pass it to [`TowerConfig::tls`](crate::TowerConfig::tls).
/// The struct is `#[non_exhaustive]`; adjust it with the builder-style setters.
///
/// Its `Debug` output never shows the host name or the file paths, which often contain it. It
/// shows the bind addresses and the reload interval; for a tower reached by an IP literal, a
/// bind address can be that same address.
#[derive(Clone)]
#[non_exhaustive]
pub struct TlsTower {
    /// The host name the tower is reached by: a DNS name or an IP literal.
    ///
    /// A DNS name may end in one dot, which is dropped; then it has 1 to 253 characters in
    /// labels separated by single dots, each label 1 to 63 ASCII letters, digits or hyphens,
    /// neither starting nor ending with a hyphen. An IP literal is IPv4 or IPv6, the latter
    /// with or without brackets. The name goes into [`Tower::url`](crate::Tower::url) in lower
    /// case; an IP literal is written the way the URL parser writes it (an IPv4-mapped IPv6
    /// address `::ffff:1.2.3.4` becomes `[::ffff:102:304]`), as the station's canonical tower
    /// URL writes it. The name should match a name in the certificate.
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
    /// The tower reads it at spawn and at every reload. It must be a regular file (a symbolic
    /// link to one is followed) of at most 64 KiB. It must hold at least one certificate; every
    /// certificate in it must be an X.509 version 3 certificate that webpki can parse, which
    /// excludes version 1 certificates and unknown critical extensions; and the first must
    /// belong to the private key. Chains from the public CAs are all of that kind; a private CA
    /// whose root is a version 1 certificate must leave that root out of the file. Trust, expiry
    /// and names are not checked. The file may also hold the private key: one combined file can
    /// serve as both paths.
    pub cert_path: PathBuf,
    /// The private key of the certificate, a PEM file with exactly one unencrypted key (PKCS#8,
    /// PKCS#1 or SEC1; RSA, ECDSA P-256 or P-384, or Ed25519).
    ///
    /// The tower reads it at spawn and at every reload. It must be a regular file (a symbolic
    /// link to one is followed) of at most 64 KiB, and may be the certificate file itself.
    ///
    /// What the tower does with the key: it reads the file into one buffer of fixed size, zeroed
    /// when dropped; it decodes every PEM section into a buffer zeroed when dropped; it hands
    /// the key's encoding to rustls, which zeroes it after loading. It never logs the key, never
    /// puts any byte of it into an error, and keeps no other copy of its own. What it cannot
    /// zero: the PEM decoder's scratch buffers (rustls-pki-types); the PKCS#8 wrapping rustls's
    /// ring provider builds for a SEC1 key, two vectors per curve tried (four for P-384); and
    /// ring's key pair itself, which ring frees without zeroing whenever a pair is dropped (the
    /// pair a reload replaces, a pair that fails the match, and the pair in use when the tower
    /// stops). The tower limits how long the encoded key stays in memory it controls; it does
    /// not and cannot keep key material out of freed memory.
    pub key_path: PathBuf,
    /// How often the tower reads both files again.
    ///
    /// A pair that passes the same checks as at spawn replaces the one in use, so a renewed
    /// certificate is served without a restart. A pair that fails them is ignored, with one
    /// warning that names the file and the kind of problem, and the pair in use stays. The
    /// default is iroh-relay's `DEFAULT_CERT_RELOAD_INTERVAL`, one day. [`Tower::spawn`]
    /// refuses zero and anything longer than [`MAX_RELOAD_INTERVAL`] (365 days).
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
    let url: RelayUrl = text.parse().map_err(|_| TowerError::InvalidHostname)?;
    if let Some(address) = ip_literal(&host) {
        // An IP literal is written the URL parser's way (it spells some IPv6 addresses
        // differently from std, `::ffff:1.2.3.4` as `::ffff:102:304`): accept the parsed URL
        // when its host is the same address, and use its text.
        let parsed = url.host_str().and_then(ip_literal);
        if parsed != Some(address) || url.port_or_known_default() != Some(port) {
            return Err(TowerError::InvalidHostname);
        }
        return Ok(url.as_str().to_owned());
    }
    // A DNS name: the URL parser must read back exactly what we wrote. It refuses, for
    // example, a numeric name that WHATWG URL parsing would turn into an IPv4 address.
    if url.host_str() != Some(host.as_str()) || url.as_str() != text {
        return Err(TowerError::InvalidHostname);
    }
    Ok(text)
}

/// The address of an IP literal as `canonical_host` and the URL parser write it: IPv4 bare,
/// IPv6 in brackets.
fn ip_literal(host: &str) -> Option<IpAddr> {
    match host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        Some(inner) => inner.parse::<Ipv6Addr>().ok().map(IpAddr::V6),
        None => host.parse::<Ipv4Addr>().ok().map(IpAddr::V4),
    }
}

/// The relay's certificate configuration and the task that reloads it.
///
/// rustls gets the ring provider explicitly; nothing is installed process-wide. The pair is
/// loaded and checked here, before anything is bound.
pub(crate) async fn cert_config(tls: &TlsTower) -> Result<(CertConfig, Reloader), TowerError> {
    let every = tls.reload_interval;
    let first_reload = (!every.is_zero() && every <= MAX_RELOAD_INTERVAL)
        .then(|| tokio::time::Instant::now().checked_add(every))
        .flatten()
        .ok_or(TowerError::InvalidReloadInterval)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    // On the blocking pool, so that a slow file system cannot stall the runtime's worker.
    let (loader, cert_path, key_path) = (
        Arc::clone(&provider),
        tls.cert_path.clone(),
        tls.key_path.clone(),
    );
    let pair = tokio::task::spawn_blocking(move || load_pair(&loader, &cert_path, &key_path))
        .await
        .map_err(|error| TowerError::Start(Box::new(error)))?
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
        first_reload,
        every,
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

    /// An IPv4-mapped IPv6 address comes out the way the URL parser writes it, in every
    /// spelling; the station's canonical tower URL writes it the same way.
    #[test]
    fn writes_an_ipv4_mapped_address_as_the_url_parser_does() {
        for spelling in ["::ffff:1.2.3.4", "[::ffff:1.2.3.4]", "::ffff:102:304"] {
            assert_eq!(
                tower_url(spelling, 443).unwrap(),
                "https://[::ffff:102:304]/",
                "{spelling}"
            );
        }
        assert_eq!(
            tower_url("::ffff:1.2.3.4", 8443).unwrap(),
            "https://[::ffff:102:304]:8443/"
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
