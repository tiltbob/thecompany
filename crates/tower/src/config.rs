//! The config file of a production tower, as `tower serve --config <PATH>` reads it.
//!
//! The file is TOML. [`load`] reads it and returns a [`TowerConfig`] ready for
//! [`Tower::spawn`](crate::Tower::spawn); [`parse`] does the same for text that is already in
//! memory.
//!
//! ```toml
//! hostname = "tower1.example.com"      # required: the DNS name (or IP literal) of the tower
//! http_bind = "0.0.0.0:80"             # optional; the plain-HTTP listener (captive-portal check)
//! https_bind = "0.0.0.0:443"           # optional; the relay over HTTPS
//! qad_bind = "0.0.0.0:7842"            # optional; QUIC address discovery (UDP)
//! qad = true                           # optional; false turns address discovery off
//! allow_file = "allow.txt"             # required unless open = true
//! open = false                         # optional; true admits every endpoint
//!
//! [tls]                                # exactly one of [tls] and [acme]
//! cert = "cert.pem"                    # the certificate chain, PEM
//! key = "key.pem"                      # its private key, PEM
//!
//! [acme]
//! contact = "mailto:ops@example.com"   # the Let's Encrypt account contact
//! cache_dir = "acme-cache"             # account key, certificate and its key; private
//! production = false                   # optional; false uses Let's Encrypt's staging directory
//!
//! [rate]                               # optional; what each client may send to the tower
//! bytes_per_second = 2097152           # required in [rate]; at least 10
//! burst_bytes = 4194304                # optional; at least 1
//! ```
//!
//! # Rules
//!
//! - Every key is checked: a key the format does not know, at the top level or in a table, is
//!   an error, so a misspelt key never silently gives another configuration.
//! - `hostname` is required. It goes into the tower's URL and, with `[acme]`, is the domain of
//!   the certificate, which must then be a DNS name. [`TlsTower::hostname`] has the rule.
//! - The binds default to [`DEFAULT_TLS_HTTP_BIND`], [`DEFAULT_HTTPS_BIND`] and
//!   [`DEFAULT_QAD_BIND`]. `qad = false` turns address discovery off; `qad_bind` must then be
//!   absent.
//! - Exactly one of `[tls]` (certificate files, read and reloaded once a day by the tower) and
//!   `[acme]` (a Let's Encrypt certificate; see [`CertSource::Acme`](crate::CertSource::Acme))
//!   must be given. With `[acme]`, the host name needs at least two labels, the contact is
//!   `mailto:` and one address, and the cache directory holds the tower's keys: when the tower
//!   starts it creates a missing one with mode 0700 and refuses one that the group or others
//!   can access (on unix). The loaded configuration says what to do; only starting the tower
//!   checks these.
//! - A production tower is not an open relay by accident: either `allow_file` names the list of
//!   endpoint ids it admits, or `open = true` admits everybody. Both together, or neither, is an
//!   error.
//! - A relative path (`allow_file`, `[tls] cert`, `[tls] key`, `[acme] cache_dir`) is resolved
//!   against the directory of the config file's path as given, not the working directory. A
//!   symbolic link to the config file is not resolved: through a link, relative paths resolve
//!   against the link's directory.
//! - `[rate]` defaults to [`DEFAULT_CLIENT_RATE`](crate::DEFAULT_CLIENT_RATE) bytes per
//!   second, with iroh-relay's default burst, a tenth of the rate.
//!
//! # The allow file
//!
//! One endpoint id per line, as iroh prints it (hex or base32). Everything from `#` to the end of
//! a line is a comment, blank lines are ignored, and the whitespace around an id is trimmed. A
//! file with no ids is valid and admits nobody. The file is read once, when the config is
//! loaded.
//!
//! # Limits and errors
//!
//! The config file may be at most [`MAX_CONFIG_FILE_SIZE`] (64 KiB) and the allow file at most
//! [`MAX_ALLOW_FILE_SIZE`] (1 MiB); both must be regular files (symbolic links are followed;
//! a directory, a device or a FIFO is refused before it is opened) and UTF-8. Every error is a
//! [`ConfigError`]: a fixed category that names the key, the line where that helps, and which
//! of the two files, but never a path, a host name, an id or any other value from a file. One
//! leading byte-order mark is ignored in both files.

use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use iroh_base::EndpointId;
use serde::Deserialize;

use crate::{
    DEFAULT_HTTPS_BIND, DEFAULT_QAD_BIND, DEFAULT_TLS_HTTP_BIND, MIN_CLIENT_RATE, TlsTower,
    TowerConfig,
};

/// The largest config file [`load`] reads: 64 KiB.
pub const MAX_CONFIG_FILE_SIZE: usize = 64 * 1024;

/// The largest allow file [`load`] and [`parse`] read: 1 MiB, about 15 000 ids.
pub const MAX_ALLOW_FILE_SIZE: usize = 1024 * 1024;

/// Which of the two files an error is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigFile {
    /// The TOML config file.
    Config,
    /// The allow file that `allow_file` names.
    Allow,
}

impl fmt::Display for ConfigFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Config => "config file",
            Self::Allow => "allow file",
        })
    }
}

/// Why a config could not be loaded.
///
/// Every variant is a fixed category. Its message names the config key, the line where that
/// helps, and which file, but never a path, a host name, an endpoint id or any other value from
/// either file. No variant has a source: the TOML parser's own message quotes the offending
/// line, so only its line number and the kind of problem are kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// The file cannot be opened or read; the kind of I/O error says why.
    Unreadable {
        /// Which file.
        file: ConfigFile,
        /// The kind of I/O error.
        kind: io::ErrorKind,
    },
    /// The path names something that is not a regular file, such as a directory, a device or a
    /// FIFO. It is refused before it is opened: opening or reading such a path can block.
    NotAFile {
        /// Which file.
        file: ConfigFile,
    },
    /// The file is larger than its limit.
    TooLarge {
        /// Which file.
        file: ConfigFile,
    },
    /// The file is not UTF-8.
    NotUtf8 {
        /// Which file.
        file: ConfigFile,
    },
    /// The config file is not valid TOML.
    Syntax {
        /// The line of the problem, when the parser says.
        line: Option<usize>,
    },
    /// The config file has a key that the format does not know.
    UnknownKey {
        /// The line of the key, when the parser says.
        line: Option<usize>,
    },
    /// A value in the config file has the wrong type or form, for example a bind address that
    /// is not one.
    InvalidValue {
        /// The line of the value, when the parser says.
        line: Option<usize>,
    },
    /// A required key is missing.
    Missing {
        /// The key, for example ``hostname`` or ``[tls] cert``.
        key: &'static str,
    },
    /// Both `[tls]` and `[acme]` are given.
    BothTlsAndAcme,
    /// Neither `[tls]` nor `[acme]` is given.
    NoCertificateSource,
    /// Both `open = true` and `allow_file` are given.
    OpenAndAllowFile,
    /// Neither `allow_file` nor `open = true` is given.
    NoAccessRule,
    /// `qad_bind` is given with `qad = false`.
    QadBindWithoutQad,
    /// `[rate] bytes_per_second` is below [`MIN_CLIENT_RATE`].
    RateTooLow,
    /// `[rate] burst_bytes` is zero.
    ZeroBurst,
    /// A line of the allow file is not a valid endpoint id.
    InvalidAllowEntry {
        /// The line, counting from 1.
        line: usize,
    },
}

/// `", line N"` when the line is known.
struct OnLine(Option<usize>);

impl fmt::Display for OnLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(line) => write!(f, ", line {line}"),
            None => Ok(()),
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable { file, kind } => write!(f, "cannot read the {file} ({kind})"),
            Self::NotAFile { file } => write!(f, "the {file} is not a regular file"),
            Self::TooLarge { file } => {
                let limit = match file {
                    ConfigFile::Config => "64 KiB",
                    ConfigFile::Allow => "1 MiB",
                };
                write!(f, "the {file} is larger than {limit}")
            }
            Self::NotUtf8 { file } => write!(f, "the {file} is not UTF-8"),
            Self::Syntax { line } => write!(f, "invalid config file{}", OnLine(*line)),
            Self::UnknownKey { line } => {
                write!(f, "unknown key in the config file{}", OnLine(*line))
            }
            Self::InvalidValue { line } => write!(
                f,
                "a value of the wrong type or form in the config file{}",
                OnLine(*line)
            ),
            Self::Missing { key } => write!(f, "`{key}` is missing in the config file"),
            Self::BothTlsAndAcme => f.write_str("`[tls]` and `[acme]` are both given"),
            Self::NoCertificateSource => f.write_str("neither `[tls]` nor `[acme]` is given"),
            Self::OpenAndAllowFile => f.write_str("`open = true` and `allow_file` are both given"),
            Self::NoAccessRule => f.write_str(
                "neither `allow_file` nor `open = true` is given; a tower admits only listed ids \
                 unless it is explicitly open",
            ),
            Self::QadBindWithoutQad => f.write_str("`qad_bind` is given with `qad = false`"),
            Self::RateTooLow => write!(
                f,
                "`[rate] bytes_per_second` must be at least {MIN_CLIENT_RATE}"
            ),
            Self::ZeroBurst => f.write_str("`[rate] burst_bytes` must be at least 1"),
            Self::InvalidAllowEntry { line } => {
                write!(f, "the allow file, line {line}: not a valid endpoint id")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    hostname: Option<String>,
    http_bind: Option<SocketAddr>,
    https_bind: Option<SocketAddr>,
    qad_bind: Option<SocketAddr>,
    qad: Option<bool>,
    allow_file: Option<PathBuf>,
    open: Option<bool>,
    tls: Option<RawTls>,
    acme: Option<RawAcme>,
    rate: Option<RawRate>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTls {
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAcme {
    contact: Option<String>,
    cache_dir: Option<PathBuf>,
    production: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRate {
    bytes_per_second: Option<u32>,
    burst_bytes: Option<u32>,
}

/// Reads the config file at `path`, and the allow file it names, into a [`TowerConfig`].
///
/// Relative paths in the file are resolved against the directory of `path` as given; a symbolic
/// link to the config file is not resolved, so through a link they resolve against the link's
/// directory. Both paths must name regular files. This function blocks on file I/O: call it
/// before the async runtime starts its work, or on its blocking pool (the `tower` binary uses
/// `tokio::task::spawn_blocking`).
pub fn load(path: &Path) -> Result<TowerConfig, ConfigError> {
    let text = read_text(path, ConfigFile::Config, MAX_CONFIG_FILE_SIZE)?;
    parse(&text, path.parent().unwrap_or(Path::new("")))
}

/// Parses config text into a [`TowerConfig`], resolving relative paths against `base_dir`.
///
/// It reads the allow file that the text names, if any; nothing else is read. Like [`load`], it
/// blocks on that read.
pub fn parse(text: &str, base_dir: &Path) -> Result<TowerConfig, ConfigError> {
    let raw: RawConfig = toml::from_str(text).map_err(|error| toml_error(&error, text))?;
    let hostname = raw
        .hostname
        .ok_or(ConfigError::Missing { key: "hostname" })?;

    let tls = match (raw.tls, raw.acme) {
        (Some(_), Some(_)) => return Err(ConfigError::BothTlsAndAcme),
        (None, None) => return Err(ConfigError::NoCertificateSource),
        (Some(tls), None) => {
            let cert = tls.cert.ok_or(ConfigError::Missing { key: "[tls] cert" })?;
            let key = tls.key.ok_or(ConfigError::Missing { key: "[tls] key" })?;
            TlsTower::new(hostname, base_dir.join(cert), base_dir.join(key))
        }
        (None, Some(acme)) => {
            let contact = acme.contact.ok_or(ConfigError::Missing {
                key: "[acme] contact",
            })?;
            let cache_dir = acme.cache_dir.ok_or(ConfigError::Missing {
                key: "[acme] cache_dir",
            })?;
            TlsTower::acme(hostname, contact, base_dir.join(cache_dir))
                .production(acme.production.unwrap_or(false))
        }
    };

    let qad_bind = match (raw.qad.unwrap_or(true), raw.qad_bind) {
        (false, Some(_)) => return Err(ConfigError::QadBindWithoutQad),
        (false, None) => None,
        (true, bind) => Some(bind.unwrap_or(DEFAULT_QAD_BIND)),
    };
    let tls = tls
        .https_bind(raw.https_bind.unwrap_or(DEFAULT_HTTPS_BIND))
        .qad_bind(qad_bind);
    let mut config = TowerConfig::tls(tls).bind(raw.http_bind.unwrap_or(DEFAULT_TLS_HTTP_BIND));

    if let Some(rate) = raw.rate {
        let bytes_per_second = rate.bytes_per_second.ok_or(ConfigError::Missing {
            key: "[rate] bytes_per_second",
        })?;
        if bytes_per_second < MIN_CLIENT_RATE {
            return Err(ConfigError::RateTooLow);
        }
        config = config.client_rate(bytes_per_second);
        if let Some(burst) = rate.burst_bytes {
            if burst == 0 {
                return Err(ConfigError::ZeroBurst);
            }
            config = config.client_burst(burst);
        }
    }

    match (raw.open.unwrap_or(false), raw.allow_file) {
        (true, Some(_)) => Err(ConfigError::OpenAndAllowFile),
        (false, None) => Err(ConfigError::NoAccessRule),
        (true, None) => Ok(config.allow_everyone()),
        (false, Some(allow_file)) => {
            let text = read_text(
                &base_dir.join(allow_file),
                ConfigFile::Allow,
                MAX_ALLOW_FILE_SIZE,
            )?;
            Ok(config.allow(parse_allow_file(&text)?))
        }
    }
}

/// Parses the text of an allow file into its endpoint ids, in file order.
///
/// Everything from `#` to the end of a line is a comment, blank lines are skipped, and the
/// whitespace around an id is trimmed. One leading byte-order mark is ignored. A line that is
/// not a valid endpoint id is an error that names the line, never its text.
pub fn parse_allow_file(text: &str) -> Result<Vec<String>, ConfigError> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut ids = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let entry = line.split('#').next().unwrap_or_default().trim();
        if entry.is_empty() {
            continue;
        }
        if entry.parse::<EndpointId>().is_err() {
            return Err(ConfigError::InvalidAllowEntry { line: index + 1 });
        }
        ids.push(entry.to_owned());
    }
    Ok(ids)
}

/// Reads a UTF-8 file of at most `limit` bytes.
///
/// The path must name a regular file (a symbolic link to one is followed): `std::fs::metadata`
/// does not block on a FIFO, while opening one, or reading a terminal or a device, can. The
/// file is read into a buffer of `limit + 1` bytes allocated up front; a file that fills it is
/// too large. The check and the open are two steps, not one: someone who can replace the file
/// between them can still make the open block.
fn read_text(path: &Path, file: ConfigFile, limit: usize) -> Result<String, ConfigError> {
    let unreadable = |error: io::Error| ConfigError::Unreadable {
        file,
        kind: error.kind(),
    };
    if !std::fs::metadata(path).map_err(unreadable)?.is_file() {
        return Err(ConfigError::NotAFile { file });
    }
    let mut source = File::open(path).map_err(unreadable)?;
    let mut bytes = vec![0; limit.saturating_add(1)];
    let mut filled = 0;
    loop {
        let Some(rest) = bytes.get_mut(filled..).filter(|rest| !rest.is_empty()) else {
            return Err(ConfigError::TooLarge { file });
        };
        match source.read(rest) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(unreadable(error)),
        }
    }
    bytes.truncate(filled);
    String::from_utf8(bytes).map_err(|_| ConfigError::NotUtf8 { file })
}

/// Turns a TOML error into a fixed category with its line. The parser's message is used only
/// to tell the kind of problem; it quotes the file, so it is never kept.
fn toml_error(error: &toml::de::Error, text: &str) -> ConfigError {
    let line = error.span().map(|span| {
        let before = text.as_bytes().get(..span.start).unwrap_or(text.as_bytes());
        before.iter().filter(|&&byte| byte == b'\n').count() + 1
    });
    let message = error.message();
    if message.starts_with("unknown field") {
        ConfigError::UnknownKey { line }
    } else if [
        "invalid type",
        "invalid value",
        "invalid length",
        "invalid socket address",
    ]
    .iter()
    .any(|prefix| message.starts_with(prefix))
    {
        ConfigError::InvalidValue { line }
    } else {
        ConfigError::Syntax { line }
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};

    use iroh_base::SecretKey;

    use super::{
        ConfigError, ConfigFile, MAX_ALLOW_FILE_SIZE, MAX_CONFIG_FILE_SIZE, load, parse,
        parse_allow_file,
    };
    use crate::{
        CertSource, DEFAULT_CLIENT_RATE, DEFAULT_HTTPS_BIND, DEFAULT_QAD_BIND,
        DEFAULT_TLS_HTTP_BIND, TowerConfig,
    };

    fn id() -> String {
        SecretKey::generate().public().to_string()
    }

    /// A directory with an allow file of two ids, which the configs below name.
    fn dir_with_allow_file() -> (tempfile::TempDir, Vec<String>) {
        let dir = tempfile::tempdir().unwrap();
        let ids = vec![id(), id()];
        std::fs::write(dir.path().join("allow.txt"), ids.join("\n")).unwrap();
        (dir, ids)
    }

    const MINIMAL: &str = "hostname = \"tower1.example.com\"\nallow_file = \"allow.txt\"\n\
                           [tls]\ncert = \"cert.pem\"\nkey = \"key.pem\"\n";

    fn files(config: &TowerConfig) -> (&Path, &Path) {
        match &config.tls.as_ref().unwrap().source {
            CertSource::Files {
                cert_path,
                key_path,
                ..
            } => (cert_path, key_path),
            CertSource::Acme { .. } => panic!("expected certificate files"),
        }
    }

    fn error(text: &str, base: &Path) -> ConfigError {
        let Err(error) = parse(text, base) else {
            panic!("the config was accepted");
        };
        error
    }

    #[test]
    fn a_minimal_config_takes_the_defaults() {
        let (dir, ids) = dir_with_allow_file();
        let config = parse(MINIMAL, dir.path()).unwrap();
        let tls = config.tls.as_ref().unwrap();
        assert_eq!(tls.hostname, "tower1.example.com");
        assert_eq!(config.bind, DEFAULT_TLS_HTTP_BIND);
        assert_eq!(tls.https_bind, DEFAULT_HTTPS_BIND);
        assert_eq!(tls.qad_bind, Some(DEFAULT_QAD_BIND));
        assert_eq!(
            files(&config),
            (&*dir.path().join("cert.pem"), &*dir.path().join("key.pem"))
        );
        assert!(config.allow == Some(ids));
        assert_eq!(config.client_rate, DEFAULT_CLIENT_RATE);
        assert_eq!(config.client_burst, None);
    }

    #[test]
    fn a_full_config_sets_every_value() {
        let (dir, ids) = dir_with_allow_file();
        let text = "hostname = \"tower1.example.com\"\n\
                    http_bind = \"127.0.0.1:8080\"\n\
                    https_bind = \"127.0.0.1:8443\"\n\
                    qad_bind = \"127.0.0.1:7843\"\n\
                    qad = true\n\
                    allow_file = \"allow.txt\"\n\
                    open = false\n\
                    [tls]\ncert = \"/etc/tower/cert.pem\"\nkey = \"keys/key.pem\"\n\
                    [rate]\nbytes_per_second = 65536\nburst_bytes = 131072\n";
        let config = parse(text, dir.path()).unwrap();
        let tls = config.tls.as_ref().unwrap();
        let addr = |text: &str| text.parse::<SocketAddr>().unwrap();
        assert_eq!(config.bind, addr("127.0.0.1:8080"));
        assert_eq!(tls.https_bind, addr("127.0.0.1:8443"));
        assert_eq!(tls.qad_bind, Some(addr("127.0.0.1:7843")));
        // An absolute path stays as it is; a relative one is resolved against the directory.
        assert_eq!(
            files(&config),
            (
                Path::new("/etc/tower/cert.pem"),
                &*dir.path().join("keys/key.pem")
            )
        );
        assert!(config.allow == Some(ids));
        assert_eq!(config.client_rate, 65536);
        assert_eq!(config.client_burst, Some(131072));
    }

    #[test]
    fn an_acme_config_gives_an_acme_source() {
        let dir = tempfile::tempdir().unwrap();
        let text = "hostname = \"tower1.example.com\"\nopen = true\nqad = false\n\
                    [acme]\ncontact = \"mailto:ops@example.com\"\ncache_dir = \"acme\"\n\
                    production = true\n";
        let config = parse(text, dir.path()).unwrap();
        let tls = config.tls.as_ref().unwrap();
        assert_eq!(tls.qad_bind, None);
        assert!(config.allow.is_none());
        match &tls.source {
            CertSource::Acme {
                contact,
                cache_dir,
                production,
            } => {
                assert_eq!(contact, "mailto:ops@example.com");
                assert_eq!(cache_dir, &dir.path().join("acme"));
                assert!(*production);
            }
            CertSource::Files { .. } => panic!("expected an ACME source"),
        }
        let staging = text.replace("production = true\n", "");
        let config = parse(&staging, dir.path()).unwrap();
        assert!(matches!(
            config.tls.unwrap().source,
            CertSource::Acme {
                production: false,
                ..
            }
        ));
    }

    #[test]
    fn refuses_what_the_format_does_not_allow() {
        let (dir, _) = dir_with_allow_file();
        let base = "hostname = \"tower1.example.com\"\n";
        let tls = "[tls]\ncert = \"c.pem\"\nkey = \"k.pem\"\n";
        let acme = "[acme]\ncontact = \"mailto:ops@example.com\"\ncache_dir = \"a\"\n";
        let allow = "allow_file = \"allow.txt\"\n";
        for (text, expected) in [
            (
                format!("{allow}{tls}"),
                ConfigError::Missing { key: "hostname" },
            ),
            (
                format!("{base}{allow}{tls}{acme}"),
                ConfigError::BothTlsAndAcme,
            ),
            (format!("{base}{allow}"), ConfigError::NoCertificateSource),
            (
                format!("{base}open = true\n{allow}{tls}"),
                ConfigError::OpenAndAllowFile,
            ),
            (format!("{base}{tls}"), ConfigError::NoAccessRule),
            (
                format!("{base}open = false\n{tls}"),
                ConfigError::NoAccessRule,
            ),
            (
                format!("{base}qad = false\nqad_bind = \"0.0.0.0:7842\"\n{allow}{tls}"),
                ConfigError::QadBindWithoutQad,
            ),
            (
                format!("{base}{allow}[tls]\nkey = \"k.pem\"\n"),
                ConfigError::Missing { key: "[tls] cert" },
            ),
            (
                format!("{base}{allow}[tls]\ncert = \"c.pem\"\n"),
                ConfigError::Missing { key: "[tls] key" },
            ),
            (
                format!("{base}{allow}[acme]\ncache_dir = \"a\"\n"),
                ConfigError::Missing {
                    key: "[acme] contact",
                },
            ),
            (
                format!("{base}{allow}[acme]\ncontact = \"mailto:o@example.com\"\n"),
                ConfigError::Missing {
                    key: "[acme] cache_dir",
                },
            ),
            (
                format!("{base}{allow}{tls}[rate]\nburst_bytes = 100\n"),
                ConfigError::Missing {
                    key: "[rate] bytes_per_second",
                },
            ),
            (
                format!("{base}{allow}{tls}[rate]\nbytes_per_second = 9\n"),
                ConfigError::RateTooLow,
            ),
            (
                format!("{base}{allow}{tls}[rate]\nbytes_per_second = 10\nburst_bytes = 0\n"),
                ConfigError::ZeroBurst,
            ),
        ] {
            assert_eq!(error(&text, dir.path()), expected, "{text}");
        }
    }

    #[test]
    fn names_the_line_of_an_unknown_key_a_wrong_value_and_bad_syntax() {
        let (dir, _) = dir_with_allow_file();
        for (text, expected) in [
            (
                format!("{MINIMAL}bogus = 1\n"),
                ConfigError::UnknownKey { line: Some(6) },
            ),
            (
                "bogus = 1\nhostname = \"tower1.example.com\"\n".to_owned(),
                ConfigError::UnknownKey { line: Some(1) },
            ),
            (
                MINIMAL.replace("key = ", "keys = "),
                ConfigError::UnknownKey { line: Some(5) },
            ),
            (
                format!("{MINIMAL}[rate]\nbytes_per_second = 100\nburst = 1\n"),
                ConfigError::UnknownKey { line: Some(8) },
            ),
            (
                "hostname = \"t.example.com\"\nopen = true\n[acme]\n\
                 contact = \"mailto:o@example.com\"\n\
                 cache_dir = \"a\"\nstaging = true\n"
                    .to_owned(),
                ConfigError::UnknownKey { line: Some(6) },
            ),
            (
                MINIMAL.replace("\"tower1.example.com\"", "5"),
                ConfigError::InvalidValue { line: Some(1) },
            ),
            (
                format!("{MINIMAL}[rate]\nbytes_per_second = -1\n"),
                ConfigError::InvalidValue { line: Some(7) },
            ),
            (
                format!("https_bind = \"not an address\"\n{MINIMAL}"),
                ConfigError::InvalidValue { line: Some(1) },
            ),
            (
                format!("{MINIMAL}open = yes\n"),
                ConfigError::Syntax { line: Some(6) },
            ),
            (
                format!("hostname = \"a.example\"\n{MINIMAL}"),
                ConfigError::Syntax { line: Some(2) },
            ),
        ] {
            assert_eq!(error(&text, dir.path()), expected, "{text}");
        }
    }

    #[test]
    fn reads_the_allow_file_with_comments_blank_lines_and_whitespace() {
        let (a, b) = (id(), id());
        let text = format!("# the team\n\n  {a}  # first\n\t\n{b}\r\n   # done\n");
        assert!(parse_allow_file(&text).unwrap() == vec![a, b]);
        assert_eq!(parse_allow_file("").unwrap(), Vec::<String>::new());
        assert_eq!(
            parse_allow_file("# nobody\n\n").unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn an_empty_allow_file_admits_nobody() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("allow.txt"), "# nobody yet\n").unwrap();
        let config = parse(MINIMAL, dir.path()).unwrap();
        assert_eq!(config.allow, Some(Vec::new()));
    }

    #[test]
    fn a_bad_allow_entry_names_its_line_and_not_its_text() {
        let dir = tempfile::tempdir().unwrap();
        let bad = &id()[..60];
        let text = format!("{}\n# comment\n\n{bad}\n", id());
        std::fs::write(dir.path().join("allow.txt"), &text).unwrap();
        let error = error(MINIMAL, dir.path());
        assert_eq!(error, ConfigError::InvalidAllowEntry { line: 4 });
        assert_eq!(
            error.to_string(),
            "the allow file, line 4: not a valid endpoint id"
        );
        assert_no_leak(&error, &[bad]);
    }

    #[test]
    fn reports_unreadable_non_file_too_large_and_non_utf8_files_by_kind() {
        let dir = tempfile::tempdir().unwrap();
        let path = |name: &str| dir.path().join(name);
        assert_eq!(
            load(&path("missing.toml")).unwrap_err(),
            ConfigError::Unreadable {
                file: ConfigFile::Config,
                kind: ErrorKind::NotFound
            }
        );
        std::fs::write(path("big.toml"), vec![b'#'; MAX_CONFIG_FILE_SIZE + 1]).unwrap();
        assert_eq!(
            load(&path("big.toml")).unwrap_err(),
            ConfigError::TooLarge {
                file: ConfigFile::Config
            }
        );
        assert_eq!(
            load(dir.path()).unwrap_err(),
            ConfigError::NotAFile {
                file: ConfigFile::Config
            }
        );
        if cfg!(unix) {
            assert_eq!(
                load(Path::new("/dev/null")).unwrap_err(),
                ConfigError::NotAFile {
                    file: ConfigFile::Config
                }
            );
        }
        std::fs::create_dir(path("allow.txt")).unwrap();
        assert_eq!(
            error(MINIMAL, dir.path()),
            ConfigError::NotAFile {
                file: ConfigFile::Allow
            }
        );
        std::fs::remove_dir(path("allow.txt")).unwrap();
        std::fs::write(path("allow.txt"), b"# caf\xe9\n").unwrap();
        assert_eq!(
            error(MINIMAL, dir.path()),
            ConfigError::NotUtf8 {
                file: ConfigFile::Allow
            }
        );
        std::fs::remove_file(path("allow.txt")).unwrap();
        std::fs::write(path("latin1.toml"), b"# caf\xe9\n").unwrap();
        assert_eq!(
            load(&path("latin1.toml")).unwrap_err(),
            ConfigError::NotUtf8 {
                file: ConfigFile::Config
            }
        );
        assert_eq!(
            error(MINIMAL, dir.path()),
            ConfigError::Unreadable {
                file: ConfigFile::Allow,
                kind: ErrorKind::NotFound
            }
        );
        std::fs::write(path("allow.txt"), vec![b'#'; MAX_ALLOW_FILE_SIZE + 1]).unwrap();
        assert_eq!(
            error(MINIMAL, dir.path()),
            ConfigError::TooLarge {
                file: ConfigFile::Allow
            }
        );
    }

    /// One leading byte-order mark is ignored in the allow file, as the TOML parser ignores one
    /// in the config file.
    #[test]
    fn a_leading_byte_order_mark_is_ignored_in_both_files() {
        let an_id = id();
        let ids = parse_allow_file(&format!("\u{feff}{an_id}\n")).unwrap();
        assert!(ids == vec![an_id.clone()]);
        let (dir, _) = dir_with_allow_file();
        std::fs::write(
            dir.path().join("allow.txt"),
            format!("\u{feff}# team\n{an_id}\n"),
        )
        .unwrap();
        let config = parse(&format!("\u{feff}{MINIMAL}"), dir.path()).unwrap();
        assert!(config.allow == Some(vec![an_id]));
    }

    /// A file of exactly its cap is read; the test of one byte more is above.
    #[test]
    fn files_of_exactly_the_cap_are_read() {
        let (dir, ids) = dir_with_allow_file();
        let mut config_text = MINIMAL.to_owned();
        config_text.push('#');
        config_text.push_str(&"x".repeat(MAX_CONFIG_FILE_SIZE - config_text.len() - 1));
        config_text.push('\n');
        assert_eq!(config_text.len(), MAX_CONFIG_FILE_SIZE);
        let config_path = dir.path().join("tower.toml");
        std::fs::write(&config_path, &config_text).unwrap();
        let mut allow_text = format!("{}\n", ids.join("\n"));
        allow_text.push('#');
        allow_text.push_str(&"x".repeat(MAX_ALLOW_FILE_SIZE - allow_text.len() - 1));
        allow_text.push('\n');
        assert_eq!(allow_text.len(), MAX_ALLOW_FILE_SIZE);
        std::fs::write(dir.path().join("allow.txt"), &allow_text).unwrap();
        let config = load(&config_path).unwrap();
        assert!(config.allow == Some(ids));
    }

    /// Through a symbolic link to the config file, relative paths resolve against the link's
    /// directory, the directory of the path as given; the link is not resolved.
    #[cfg(unix)]
    #[test]
    fn relative_paths_resolve_against_the_directory_of_the_path_as_given() {
        let (real, _) = dir_with_allow_file();
        std::fs::write(real.path().join("tower.toml"), MINIMAL).unwrap();
        let (linked, linked_ids) = dir_with_allow_file();
        let link = linked.path().join("tower.toml");
        std::os::unix::fs::symlink(real.path().join("tower.toml"), &link).unwrap();
        let config = load(&link).unwrap();
        assert_eq!(files(&config).0, linked.path().join("cert.pem"));
        assert!(config.allow == Some(linked_ids));
    }

    #[test]
    fn load_resolves_relative_paths_against_the_config_files_directory() {
        let (dir, ids) = dir_with_allow_file();
        let config_path: PathBuf = dir.path().join("tower.toml");
        std::fs::write(&config_path, MINIMAL).unwrap();
        let config = load(&config_path).unwrap();
        assert_eq!(files(&config).0, dir.path().join("cert.pem"));
        assert!(config.allow == Some(ids));
    }

    /// Fails if `error` shows any needle, as text or as decimal or hex byte lists.
    fn assert_no_leak(error: &ConfigError, needles: &[&str]) {
        let texts = [
            error.to_string(),
            format!("{error:?}"),
            format!("{error:#?}"),
        ];
        for needle in needles {
            let bytes = needle.as_bytes();
            let join =
                |format: fn(&u8) -> String| bytes.iter().map(format).collect::<Vec<_>>().join(",");
            let forms = [
                (*needle).to_owned(),
                join(|b| b.to_string()),
                join(|b| format!("{b:x}")),
                join(|b| format!("{b:#x}")),
            ];
            for text in &texts {
                let squeezed: String = text.chars().filter(|c| !c.is_whitespace()).collect();
                for form in &forms {
                    assert!(!squeezed.contains(form.as_str()), "{text}");
                }
            }
        }
    }

    /// A host name, an id and a path in a broken file appear in no error.
    #[test]
    fn errors_show_no_host_name_id_or_path_from_the_files() {
        let dir = tempfile::tempdir().unwrap();
        let (host, path, an_id) = ("qzx-host.example", "/srv/qzx-path/allow.txt", id());
        let cases = [
            format!("hostname = \"{host}\"\nbogus = \"{an_id}\"\n"),
            format!("\"{host}\" = 1\n"),
            format!("hostname = \"{host}\"\nhttps_bind = \"{path}\"\n"),
            format!("hostname = \"{host}\"\nallow_file = {an_id}\n"),
            format!(
                "hostname = \"{host}\"\nallow_file = \"{path}\"\n\
                 [tls]\ncert = \"{path}\"\nkey = \"{path}\"\n"
            ),
            format!("hostname = \"{host}\"\nopen = true\n[acme]\ncache_dir = \"{path}\"\n"),
        ];
        for text in cases {
            let error = error(&text, dir.path());
            assert_no_leak(&error, &[host, "qzx-path", &an_id]);
        }
    }
}
