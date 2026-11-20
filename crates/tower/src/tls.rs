//! The TLS tower: its configuration, its canonical URL, and its rustls server configuration.
//!
//! A certificate from files is loaded, checked and reloaded in [`crate::certs`]. A certificate
//! from ACME is obtained and renewed by iroh-relay's ACME client (tokio-rustls-acme).

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use iroh_base::RelayUrl;
use iroh_relay::defaults::{DEFAULT_HTTP_PORT, DEFAULT_HTTPS_PORT, DEFAULT_RELAY_QUIC_PORT};
use iroh_relay::server::{AcmeConfig, CertConfig, DEFAULT_CERT_RELOAD_INTERVAL};

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

/// The TLS part of a tower's configuration: its name, its listeners and where its certificate
/// comes from.
///
/// Build it with [`TlsTower::new`] (certificate files) or [`TlsTower::acme`] (Let's Encrypt)
/// and pass it to [`TowerConfig::tls`](crate::TowerConfig::tls).
/// The struct is `#[non_exhaustive]`; adjust it with the builder-style setters.
///
/// Its `Debug` output never shows the host name, the file paths (which often contain it) or the
/// ACME contact address. It shows the bind addresses and the kind of certificate source, with
/// the reload interval or the production flag; for a tower reached by an IP literal, a bind
/// address can be that same address.
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
    /// Where the certificate comes from: PEM files, or ACME (Let's Encrypt).
    pub source: CertSource,
}

/// Where a TLS tower's certificate and private key come from.
///
/// The variants are `#[non_exhaustive]`: build them through [`TlsTower::new`] and
/// [`TlsTower::acme`]. The `Debug` output shows neither paths nor the contact address.
#[derive(Clone)]
#[non_exhaustive]
pub enum CertSource {
    /// A certificate chain and its private key in PEM files, read and reloaded by the tower.
    #[non_exhaustive]
    Files {
        /// The certificate chain, a PEM file that starts with the tower's own certificate.
        ///
        /// The tower reads it at spawn and at every reload. It must be a regular file (a symbolic
        /// link to one is followed) of at most 64 KiB. It must hold at least one certificate; every
        /// certificate in it must be an X.509 version 3 certificate that webpki can parse, which
        /// excludes version 1 certificates and unknown critical extensions; and the first must
        /// belong to the private key. Chains from the public CAs are all of that kind; a private CA
        /// whose root is a version 1 certificate must leave that root out of the file. Trust,
        /// expiry and names are not checked. The file may also hold the private key: one combined
        /// file can serve as both paths.
        cert_path: PathBuf,
        /// The private key of the certificate, a PEM file with exactly one unencrypted key (PKCS#8,
        /// PKCS#1 or SEC1; RSA, ECDSA P-256 or P-384, or Ed25519).
        ///
        /// The tower reads it at spawn and at every reload. It must be a regular file (a symbolic
        /// link to one is followed) of at most 64 KiB, and may be the certificate file itself.
        ///
        /// What the tower does with the key: it reads the file into one buffer of fixed size,
        /// zeroed when dropped; it decodes every PEM section into a buffer zeroed when dropped; it
        /// hands the key's encoding to rustls, which zeroes it after loading. It never logs the
        /// key, never puts any byte of it into an error, and keeps no other copy of its own. What
        /// it cannot zero: the PEM decoder's scratch buffers (rustls-pki-types); the PKCS#8
        /// wrapping rustls's ring provider builds for a SEC1 key, two vectors per curve tried (four
        /// for P-384); and ring's key pair itself, which ring frees without zeroing whenever a pair
        /// is dropped (the pair a reload replaces, a pair that fails the match, and the pair in use
        /// when the tower stops). The tower limits how long the encoded key stays in memory it
        /// controls; it does not and cannot keep key material out of freed memory.
        key_path: PathBuf,
        /// How often the tower reads both files again.
        ///
        /// A pair that passes the same checks as at spawn replaces the one in use, so a renewed
        /// certificate is served without a restart. A pair that fails them is ignored, with one
        /// warning that names the file and the kind of problem, and the pair in use stays. The
        /// default is iroh-relay's `DEFAULT_CERT_RELOAD_INTERVAL`, one day. [`Tower::spawn`]
        /// refuses zero and anything longer than [`MAX_RELOAD_INTERVAL`] (365 days).
        ///
        /// [`Tower::spawn`]: crate::Tower::spawn
        reload_interval: Duration,
    },
    /// A certificate from Let's Encrypt, obtained and renewed with the ACME tls-alpn-01
    /// challenge by iroh-relay's ACME client.
    ///
    /// The certificate is for the tower's host name, which must then be a DNS name of at least
    /// two labels, not an IP literal. Let's Encrypt validates it on TCP port 443 of that name,
    /// so the HTTPS listener must be reachable there from the internet. The client contacts
    /// Let's Encrypt only once the tower runs, never at configuration time.
    ///
    /// The ACME client creates the account key and the certificate's private key itself and
    /// stores them, with the certificate, in `cache_dir` (files named `cached_account_<hash>`
    /// and `cached_cert_<hash>`), written with the process's default file permissions. Those
    /// are the tower's own keys (its TLS key and its ACME account key), so the directory must
    /// be private to, owned by and writable for the tower's user. [`Tower::spawn`] checks that
    /// just before the ACME client starts: an empty path is refused; a missing directory is
    /// created (on unix with mode 0700, and so is every missing directory above it); then the
    /// directory is checked as it is, also after a create: a path that is not a directory, and
    /// on unix one that the group or others can access or that another user owns, is refused,
    /// and a file is created in it and removed again to see that the tower can write there.
    /// The ACME client does not report a cache it cannot use: it would serve its certificate
    /// from memory and register a new account and order a new certificate at every restart,
    /// and soon hit Let's Encrypt's rate limits. A systemd unit that provides the directory with
    /// `StateDirectory=` therefore needs `StateDirectoryMode=0700`.
    ///
    /// The check is a check of the moment: the ACME client opens the cache again by its path
    /// later. So the path and every directory above it must stay in place and under the
    /// control of the tower's user or root; the tower cannot protect against someone who can
    /// replace one of them after the check. And whatever is already in an existing cache (an
    /// account key, a certificate and its key) is trusted as it is.
    ///
    /// [`Tower::spawn`]: crate::Tower::spawn
    #[non_exhaustive]
    Acme {
        /// The account contact: `mailto:` followed by one address, for example
        /// `mailto:ops@example.com`. [`Tower::spawn`] refuses anything else, including an
        /// upper-case `MAILTO:` (the contact is a URL the operator writes), several addresses,
        /// white space and a `?` query.
        ///
        /// [`Tower::spawn`]: crate::Tower::spawn
        contact: String,
        /// The directory where the ACME client keeps its account key, the certificate and its
        /// private key. It must be private to the tower's user; see the variant's docs.
        cache_dir: PathBuf,
        /// `true` for Let's Encrypt's production directory, `false` (the default) for its
        /// staging directory, whose certificates no client trusts.
        production: bool,
    },
}

impl fmt::Debug for CertSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Files {
                reload_interval, ..
            } => f
                .debug_struct("Files")
                .field("reload_interval", reload_interval)
                .finish_non_exhaustive(),
            Self::Acme { production, .. } => f
                .debug_struct("Acme")
                .field("production", production)
                .finish_non_exhaustive(),
        }
    }
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
            source: CertSource::Files {
                cert_path: cert_path.into(),
                key_path: key_path.into(),
                reload_interval: DEFAULT_CERT_RELOAD_INTERVAL,
            },
        }
    }

    /// A TLS tower reached as `hostname`, with a certificate from Let's Encrypt's staging
    /// directory; see [`CertSource::Acme`] and [`TlsTower::production`].
    ///
    /// `contact` is the account's `mailto:` address and `cache_dir` the directory, private to
    /// the tower, where the ACME client keeps its keys and the certificate. It listens on
    /// [`DEFAULT_HTTPS_BIND`] and runs address discovery on [`DEFAULT_QAD_BIND`].
    pub fn acme(
        hostname: impl Into<String>,
        contact: impl Into<String>,
        cache_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            hostname: hostname.into(),
            https_bind: DEFAULT_HTTPS_BIND,
            qad_bind: Some(DEFAULT_QAD_BIND),
            source: CertSource::Acme {
                contact: contact.into(),
                cache_dir: cache_dir.into(),
                production: false,
            },
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
    ///
    /// It applies to a certificate from files; an ACME certificate is renewed by the ACME
    /// client, and this setting leaves it unchanged.
    pub fn reload_interval(mut self, every: Duration) -> Self {
        if let CertSource::Files {
            reload_interval, ..
        } = &mut self.source
        {
            *reload_interval = every;
        }
        self
    }

    /// Chooses Let's Encrypt's production directory (`true`) or its staging directory
    /// (`false`, the default) for an ACME certificate. It leaves a certificate from files
    /// unchanged.
    pub fn production(mut self, production: bool) -> Self {
        if let CertSource::Acme {
            production: current,
            ..
        } = &mut self.source
        {
            *current = production;
        }
        self
    }
}

impl fmt::Debug for TlsTower {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsTower")
            .field("https_bind", &self.https_bind)
            .field("qad_bind", &self.qad_bind)
            .field("source", &self.source)
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

/// The relay's certificate configuration and, for a certificate from files, the task that
/// reloads it.
///
/// rustls gets the ring provider explicitly; nothing is installed process-wide. Everything is
/// checked here, before anything is bound: a pair from files is loaded (on the blocking pool)
/// and checked, and an ACME
/// source needs a DNS name and a `mailto:` contact. Building the ACME configuration touches
/// neither the network nor the cache directory; iroh-relay's ACME client starts only when the
/// server runs.
pub(crate) async fn cert_config(
    tls: &TlsTower,
) -> Result<(CertConfig, Option<Reloader>), TowerError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|error| TowerError::Start(Box::new(error)))?
        .with_no_client_auth();
    match &tls.source {
        CertSource::Files {
            cert_path,
            key_path,
            reload_interval,
        } => {
            let every = *reload_interval;
            let first_reload = (!every.is_zero() && every <= MAX_RELOAD_INTERVAL)
                .then(|| tokio::time::Instant::now().checked_add(every))
                .flatten()
                .ok_or(TowerError::InvalidReloadInterval)?;
            // On the blocking pool, so that a slow file system cannot stall the runtime's worker.
            let (loader, cert, key) = (Arc::clone(&provider), cert_path.clone(), key_path.clone());
            let pair = tokio::task::spawn_blocking(move || load_pair(&loader, &cert, &key))
                .await
                .map_err(|error| TowerError::Start(Box::new(error)))?
                .map_err(|failure| failure.into_error())?;
            let current = Arc::new(CurrentPair::new(pair));
            let server_config = builder.with_cert_resolver(
                Arc::clone(&current) as Arc<dyn rustls::server::ResolvesServerCert>
            );
            let reloader = Reloader::spawn(
                &current,
                provider,
                cert_path.clone(),
                key_path.clone(),
                first_reload,
                every,
            );
            Ok((CertConfig::Manual { server_config }, Some(reloader)))
        }
        CertSource::Acme {
            contact,
            cache_dir,
            production,
        } => {
            let host = canonical_host(&tls.hostname).ok_or(TowerError::InvalidHostname)?;
            // Let's Encrypt issues certificates for DNS names under a public suffix only: at
            // least two labels, and never an IP literal.
            if !is_acme_domain(&host) {
                return Err(TowerError::AcmeNeedsDnsName);
            }
            if !is_acme_contact(contact) {
                return Err(TowerError::InvalidAcmeContact);
            }
            let acme_config = AcmeConfig::letsencrypt(*production)
                .domains(vec![host])
                .contact(vec![contact.clone()])
                .cache_path(cache_dir.clone());
            Ok((
                CertConfig::LetsEncrypt {
                    acme_config,
                    server_config_builder: builder,
                },
                None,
            ))
        }
    }
}

/// Whether `contact` is `mailto:` (lower case) followed by one address: no white space, no
/// comma, no `?`, and exactly one `@` with something on both sides.
fn is_acme_contact(contact: &str) -> bool {
    let Some(address) = contact.strip_prefix("mailto:") else {
        return false;
    };
    // Printable ASCII only: no control character, no white space, nothing outside ASCII.
    if !address
        .bytes()
        .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b',' | b'?'))
    {
        return false;
    }
    let Some((local, domain)) = address.split_once('@') else {
        return false;
    };
    !local.is_empty() && !domain.contains('@') && is_acme_domain(domain)
}

/// Whether `name` is a DNS name an ACME certificate or contact can use: a valid host name of
/// at least two labels, not an IP literal.
fn is_acme_domain(name: &str) -> bool {
    canonical_host(name).is_some_and(|host| ip_literal(&host).is_none() && host.contains('.'))
}

/// The name of the file that [`prepare_acme_cache`] creates and removes to see that the tower
/// can write to the cache directory.
const ACME_CACHE_PROBE: &str = ".numbers-tower-write-probe";

/// Makes sure the ACME cache directory exists, is private to the tower's user, belongs to that
/// user and can be written by it, before the ACME client is started (it would create the
/// directory and its key files with the process's default modes, and it does not report a
/// cache it cannot use: it serves the certificate from memory and orders a new one at every
/// restart).
///
/// An empty path is refused: the ACME client would write both keys into the working
/// directory. On unix: a missing directory is created with mode 0700, and so is every missing
/// directory above it; an existing one must not be accessible to the group or to others; a
/// path that exists and is not a directory is refused. Then, everywhere, a file named
/// [`ACME_CACHE_PROBE`] is created (`create_new`) and removed again; any failure is
/// [`TowerError::AcmeCacheUnusable`]. On unix the directory must have the same owner as that
/// file, the tower's user. The path is followed if it is a symbolic link.
pub(crate) fn prepare_acme_cache(dir: &Path) -> Result<(), TowerError> {
    let unusable = |error: std::io::Error| TowerError::AcmeCacheUnusable(error.kind());
    if dir.as_os_str().is_empty() {
        return Err(TowerError::AcmeCacheUnusable(
            std::io::ErrorKind::InvalidInput,
        ));
    }
    match std::fs::metadata(dir) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
            builder.create(dir).map_err(unusable)?;
        }
        Err(error) => return Err(unusable(error)),
    }
    // Checked again after a create too: what the create left (under the umask, or raced by
    // someone else) is not assumed to be what was asked for.
    let metadata = std::fs::metadata(dir).map_err(unusable)?;
    if !metadata.is_dir() {
        return Err(TowerError::AcmeCacheNotADirectory);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(TowerError::AcmeCacheNotPrivate);
        }
    }
    let probe_owner = probe_acme_cache(dir).map_err(unusable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.uid() != probe_owner {
            return Err(TowerError::AcmeCacheNotOwned);
        }
    }
    #[cfg(not(unix))]
    let _ = (metadata, probe_owner);
    Ok(())
}

/// Creates and removes [`ACME_CACHE_PROBE`] in `dir`, and returns the owner of the file it
/// created (on unix its uid; elsewhere 0). A probe left behind by a process that died in
/// between is removed first.
fn probe_acme_cache(dir: &Path) -> std::io::Result<u32> {
    let probe = dir.join(ACME_CACHE_PROBE);
    let create = || {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)
    };
    let file = match create() {
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            std::fs::remove_file(&probe)?;
            create()?
        }
        result => result?,
    };
    let metadata = file.metadata();
    drop(file);
    std::fs::remove_file(&probe)?;
    let metadata = metadata?;
    #[cfg(unix)]
    let owner = std::os::unix::fs::MetadataExt::uid(&metadata);
    #[cfg(not(unix))]
    let owner = {
        let _ = metadata;
        0
    };
    Ok(owner)
}

#[cfg(test)]
mod tests {
    use super::{CertSource, TlsTower, cert_config, tower_url};
    use crate::TowerError;
    use iroh_relay::server::CertConfig;

    /// An ACME source builds iroh-relay's `CertConfig::LetsEncrypt` with the right directory, the
    /// canonical domain, the contact and the cache path, without touching the cache directory
    /// (or the network: nothing here can reach it). `AcmeConfig` derives `Debug` over all its
    /// fields, and its directory constants are private, so the test reads the debug text.
    #[tokio::test]
    async fn an_acme_source_becomes_a_lets_encrypt_config_without_side_effects() {
        const PRODUCTION: &str = "https://acme-v02.api.letsencrypt.org/directory";
        const STAGING: &str = "https://acme-staging-v02.api.letsencrypt.org/directory";
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("acme-cache");
        for production in [true, false] {
            let tls = TlsTower::acme("Tower1.Example.com.", "mailto:ops@example.com", &cache)
                .production(production);
            let (cert, reloader) = cert_config(&tls).await.unwrap();
            assert!(reloader.is_none());
            let CertConfig::LetsEncrypt { acme_config, .. } = cert else {
                panic!("expected a Let's Encrypt configuration");
            };
            let debug = format!("{acme_config:?}");
            let (expected, other) = if production {
                (PRODUCTION, STAGING)
            } else {
                (STAGING, PRODUCTION)
            };
            assert!(
                debug.contains(expected) && !debug.contains(other),
                "the configuration names the wrong ACME directory"
            );
            assert!(
                debug.contains(r#"["tower1.example.com"]"#) && !debug.contains("Tower1"),
                "the domain is not the canonical host name"
            );
            assert!(
                debug.contains(r#"["mailto:ops@example.com"]"#),
                "the contact is missing"
            );
            assert!(
                debug.contains(&format!("cache_path: Some({:?})", cache)),
                "the cache path is missing"
            );
        }
        assert!(!cache.exists());
    }

    #[tokio::test]
    async fn an_acme_source_refuses_an_ip_literal_and_a_contact_without_mailto() {
        for host in [
            "127.0.0.1",
            "::1",
            "[2001:db8::1]",
            "::ffff:1.2.3.4",
            "localhost",
            "tower.",
        ] {
            let tls = TlsTower::acme(host, "mailto:ops@example.com", "/nonexistent");
            assert!(matches!(
                cert_config(&tls).await,
                Err(TowerError::AcmeNeedsDnsName)
            ));
        }
        for contact in [
            "",
            "ops@example.com",
            "mailto:",
            "https://example.com/",
            "MAILTO:ops@example.com",
            "mailto:ops",
            "mailto:@example.com",
            "mailto:ops@",
            "mailto:ops@@example.com",
            "mailto:a@b@example.com",
            "mailto:ops @example.com",
            "mailto:ops@example.com,dev@example.com",
            "mailto:ops@example.com,dev",
            // A comma or a question mark in the local part, where only the ASCII rule sees it.
            "mailto:ops,dev@example.com",
            "mailto:ops?x@example.com",
            "mailto:ops@example.com?subject=hi",
            "mailto:ops@example.com\n",
            "mailto:ops@example.com\u{1b}[31m",
            "mailto:öps@example.com",
            "mailto:ops@localhost",
            "mailto:ops@[127.0.0.1]",
            "mailto:ops@127.0.0.1",
            "mailto:<ops@example.com>",
            "mailto:ops@example.com#x",
            "mailto:ops@-example.com",
        ] {
            let tls = TlsTower::acme("tower1.example.com", contact, "/nonexistent");
            let Err(error) = cert_config(&tls).await else {
                panic!("accepted an invalid ACME contact");
            };
            assert!(matches!(error, TowerError::InvalidAcmeContact));
            // The message names `mailto:` itself; a longer contact must not appear in it.
            if contact.len() > "mailto:".len() {
                assert!(!format!("{error} {error:?}").contains(contact));
            }
        }
    }

    #[tokio::test]
    async fn an_acme_source_accepts_a_plain_mailto_contact() {
        for contact in [
            "mailto:ops@example.com",
            "mailto:first.last+tower@mail.example.com",
            "mailto:ops@Example.COM.",
        ] {
            let tls = TlsTower::acme("tower1.example.com", contact, "/nonexistent");
            assert!(cert_config(&tls).await.is_ok(), "{contact}");
        }
    }

    #[test]
    fn the_production_and_reload_setters_apply_to_their_own_source_only() {
        let acme = TlsTower::acme("tower1.example.com", "mailto:ops@example.com", "/c")
            .production(true)
            .reload_interval(std::time::Duration::from_secs(1));
        assert!(matches!(
            acme.source,
            CertSource::Acme {
                production: true,
                ..
            }
        ));
        let files = TlsTower::new("tower1.example.com", "/c.pem", "/k.pem")
            .production(true)
            .reload_interval(std::time::Duration::from_secs(1));
        assert!(matches!(
            files.source,
            CertSource::Files { reload_interval, .. } if reload_interval.as_secs() == 1
        ));
    }

    #[test]
    fn debug_output_leaves_out_the_acme_contact_and_cache() {
        let tls = TlsTower::acme(
            "qzx-name.example",
            "mailto:qzx-contact@example.com",
            "/var/lib/qzx-cache",
        );
        for text in [format!("{tls:?}"), format!("{tls:#?}")] {
            for marker in ["qzx-name", "qzx-contact", "qzx-cache"] {
                assert!(!text.contains(marker), "Debug shows {marker}");
            }
            assert!(text.contains("Acme"), "Debug does not name the source");
        }
    }

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
        // 3 labels of 63, one of 62 and 3 dots: one character over the limit.
        let one_too_long = format!("{label63}.{label63}.{label63}.{}", "b".repeat(62));
        assert_eq!(one_too_long.len(), 254);
        for refused in [
            "tower..example.com",
            "-tower.example.com",
            "tower-.example.com",
            "tower_1.example.com",
            "tower;1.example.com",
            &format!("{label64}.example.com"),
            &too_long,
            &one_too_long,
            &format!("{one_too_long}."),
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

    /// The defaults the docs of `TlsTower::new`, `TowerConfig::tls` and the bind constants
    /// promise: HTTPS on all IPv4 interfaces at 443, address discovery on UDP 7842, the plain
    /// HTTP listener on port 80, a reload once a day, and a dev tower's other defaults (no
    /// allowlist, the default client rate, iroh-relay's burst).
    #[test]
    fn a_tls_tower_starts_from_the_documented_defaults() {
        use std::net::{Ipv4Addr, SocketAddr};
        use std::time::Duration;

        let all = |port: u16| SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
        assert_eq!(super::DEFAULT_HTTPS_BIND, all(443));
        assert_eq!(super::DEFAULT_QAD_BIND, all(7842));
        assert_eq!(super::DEFAULT_TLS_HTTP_BIND, all(80));

        let tls = super::TlsTower::new("tower.example", "cert.pem", "key.pem");
        assert_eq!(tls.hostname, "tower.example");
        assert_eq!(tls.https_bind, super::DEFAULT_HTTPS_BIND);
        assert_eq!(tls.qad_bind, Some(super::DEFAULT_QAD_BIND));
        let super::CertSource::Files {
            cert_path,
            key_path,
            reload_interval,
        } = &tls.source
        else {
            panic!("TlsTower::new gives a certificate from files");
        };
        assert_eq!(cert_path, std::path::Path::new("cert.pem"));
        assert_eq!(key_path, std::path::Path::new("key.pem"));
        assert_eq!(*reload_interval, Duration::from_secs(24 * 60 * 60));

        let config = crate::TowerConfig::tls(tls);
        assert_eq!(config.bind, super::DEFAULT_TLS_HTTP_BIND);
        assert!(config.tls.is_some());
        assert_eq!(config.allow, None);
        assert_eq!(config.client_rate, crate::DEFAULT_CLIENT_RATE);
        assert_eq!(config.client_burst, None);
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
                assert!(!text.contains(marker), "Debug shows {marker}");
            }
            assert!(
                text.contains("127.0.0.1:44443"),
                "Debug does not show the bind address"
            );
        }
    }
}

#[cfg(test)]
#[cfg(unix)]
mod acme_cache_tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use super::prepare_acme_cache;
    use crate::TowerError;

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn a_missing_cache_directory_is_created_private_with_its_parents() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("state/acme");
        prepare_acme_cache(&cache).unwrap();
        assert_eq!(mode(&dir.path().join("state")), 0o700);
        assert_eq!(mode(&cache), 0o700);
    }

    #[test]
    fn an_existing_private_cache_directory_is_accepted() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        prepare_acme_cache(dir.path()).unwrap();
        assert_eq!(mode(dir.path()), 0o700);
    }

    #[test]
    fn a_cache_directory_others_can_read_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        for open in [0o755, 0o750, 0o701] {
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(open)).unwrap();
            assert!(matches!(
                prepare_acme_cache(dir.path()),
                Err(TowerError::AcmeCacheNotPrivate)
            ));
        }
    }

    /// Whether this test runs as root: the uid of a file it creates.
    fn running_as_root(dir: &Path) -> bool {
        use std::os::unix::fs::MetadataExt;
        let file = dir.join("whoami");
        std::fs::write(&file, "").unwrap();
        let root = std::fs::metadata(&file).unwrap().uid() == 0;
        std::fs::remove_file(&file).unwrap();
        root
    }

    #[test]
    fn the_write_probe_leaves_no_file_behind() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        prepare_acme_cache(dir.path()).unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        // A probe left behind by a process that died is removed too.
        std::fs::write(dir.path().join(super::ACME_CACHE_PROBE), "").unwrap();
        prepare_acme_cache(dir.path()).unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    /// A directory the tower cannot write to is refused. Root can write to a 0500 directory,
    /// so as root the probe passes; CI runs this as an unprivileged user.
    #[test]
    fn a_cache_directory_the_tower_cannot_write_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = running_as_root(dir.path());
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
        let result = prepare_acme_cache(dir.path());
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        if root {
            assert!(result.is_ok());
        } else {
            assert!(matches!(
                result,
                Err(TowerError::AcmeCacheUnusable(
                    std::io::ErrorKind::PermissionDenied
                ))
            ));
        }
    }

    /// A private, writable directory that another user owns is refused. Only root can give a
    /// directory away, so this runs as root only; as another user the probe already refuses
    /// such a directory (see the test above).
    #[test]
    fn a_cache_directory_another_user_owns_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        if !running_as_root(dir.path()) {
            return;
        }
        let cache = dir.path().join("acme");
        std::fs::create_dir(&cache).unwrap();
        std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::os::unix::fs::chown(&cache, Some(65534), Some(65534)).unwrap();
        assert!(matches!(
            prepare_acme_cache(&cache),
            Err(TowerError::AcmeCacheNotOwned)
        ));
    }

    #[test]
    fn an_empty_cache_path_is_refused() {
        assert!(matches!(
            prepare_acme_cache(Path::new("")),
            Err(TowerError::AcmeCacheUnusable(
                std::io::ErrorKind::InvalidInput
            ))
        ));
    }

    #[test]
    fn a_file_in_place_of_the_cache_directory_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("acme");
        std::fs::write(&file, "").unwrap();
        assert!(matches!(
            prepare_acme_cache(&file),
            Err(TowerError::AcmeCacheNotADirectory)
        ));
    }
}
