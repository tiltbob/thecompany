//! The TLS tower's certificate and private key: loaded, checked, served and reloaded by the tower.
//!
//! The tower reads both PEM files itself, at spawn and then at every reload interval. The bytes
//! of the key file live in a buffer that is zeroed when it is dropped; the decoded key goes to
//! rustls (with the ring provider), which zeroes its copy of the encoding and afterwards holds
//! the key only inside its signing key. Each file is read into one buffer of fixed size,
//! allocated before the read and never grown, so reading leaves no copy behind.
//!
//! Two dependencies make copies the tower cannot zero. The PEM decoder (rustls-pki-types) keeps
//! scratch buffers of its own while it decodes, including the base64 text of the key, and frees
//! them without zeroing them. And when rustls's ring provider loads a SEC1 (`EC PRIVATE KEY`)
//! key, it wraps the key in a PKCS#8 structure first, in two ordinary vectors that are freed
//! without zeroing; for a PKCS#1 or PKCS#8 key it passes the bytes to ring without a copy of its
//! own. So this is no promise that no copy of the key ever remains in freed memory; it is a
//! promise about the memory the tower itself keeps.
//!
//! A pair is checked before it is used: the chain holds a certificate, every certificate in it
//! parses as X.509, the key is of a kind ring can sign with, and the key belongs to the first
//! certificate. There is no check of trust, expiry or names; clients do that. A reload
//! publishes a new pair only if it passes the same checks; otherwise the pair in use stays.
//!
//! No error and no log line carries any byte of either file: every failure is one of the fixed
//! categories of [`TlsFileProblem`], and upstream errors are dropped, never formatted.

use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock, Weak};
use std::time::Duration;

use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ParsedCertificate, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{Error as RustlsError, InconsistentKeys};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};
use zeroize::Zeroizing;

use crate::TowerError;

const LOG_TARGET: &str = "numbers_tower::tls";

/// The largest certificate or key file the tower reads, in bytes: 64 KiB.
///
/// A certificate chain of a few certificates and any private key fit with room to spare, and
/// the read buffer of this size plus one byte is allocated in full for every read.
const MAX_FILE_SIZE: usize = 64 * 1024;

/// Why a certificate or key file cannot be used.
///
/// Every value is a fixed category: none carries text or bytes from the file, nor an upstream
/// error. The error that holds it, [`TowerError::CertificateFile`] or [`TowerError::KeyFile`],
/// says which file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TlsFileProblem {
    /// The file cannot be opened or read; the kind of I/O error says why.
    #[error("it cannot be read ({0})")]
    Unreadable(std::io::ErrorKind),
    /// The file is larger than a certificate chain or key can reasonably be (64 KiB).
    #[error("it is larger than 64 KiB")]
    TooLarge,
    /// A PEM section in the file is malformed: a bad header, no end marker, or bad base64.
    #[error("it holds a malformed PEM section")]
    MalformedPem,
    /// The certificate file holds no `CERTIFICATE` section.
    #[error("it holds no certificate")]
    NoCertificate,
    /// A certificate in the chain is not a parsable X.509 certificate; which one is not said.
    #[error("a certificate in it cannot be parsed")]
    UnparsableCertificate,
    /// The key file holds no unencrypted private key section (PKCS#8, PKCS#1 or SEC1).
    #[error("it holds no private key")]
    NoPrivateKey,
    /// The key file holds more than one private key, so it is unclear which one to use.
    #[error("it holds more than one private key")]
    SeveralPrivateKeys,
    /// The private key is not an RSA, ECDSA (P-256, P-384) or Ed25519 key that ring can use.
    #[error("its private key is of an unsupported kind")]
    UnsupportedKey,
    /// The private key does not belong to the first certificate in the chain.
    ///
    /// It is reported against the key file.
    #[error("its private key does not match the certificate")]
    KeyMismatch,
    /// rustls cannot tell whether the private key belongs to the certificate.
    ///
    /// The ring provider can always tell, so this does not happen with it; the tower refuses
    /// such a pair rather than serve one it could not check.
    #[error("its private key cannot be checked against the certificate")]
    KeyNotCheckable,
}

/// A problem with one of the two files.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Failure {
    Certificate(TlsFileProblem),
    Key(TlsFileProblem),
}

impl Failure {
    pub(crate) fn into_error(self) -> TowerError {
        match self {
            Self::Certificate(problem) => TowerError::CertificateFile { problem },
            Self::Key(problem) => TowerError::KeyFile { problem },
        }
    }

    fn file(self) -> &'static str {
        match self {
            Self::Certificate(_) => "certificate file",
            Self::Key(_) => "private key file",
        }
    }

    fn problem(self) -> TlsFileProblem {
        match self {
            Self::Certificate(problem) | Self::Key(problem) => problem,
        }
    }
}

/// Reads a whole file into a buffer that is zeroed when dropped. Only the kind of an I/O error
/// is kept: its text could in principle name more than the kind and the OS code.
///
/// The buffer is allocated once, filled with zeros, at `MAX_FILE_SIZE + 1` bytes, and the file
/// is read into its unused tail with `Read::read` on slices of it. Nothing ever grows it, so it
/// is never reallocated and no unzeroed copy is left behind, whatever happens to the file while
/// it is read; on return it is only shortened, which does not reallocate either. A file that
/// fills the last byte is larger than the cap.
fn read(path: &Path) -> Result<Zeroizing<Vec<u8>>, TlsFileProblem> {
    let unreadable = |error: std::io::Error| TlsFileProblem::Unreadable(error.kind());
    let mut file = File::open(path).map_err(unreadable)?;
    let mut bytes = Zeroizing::new(vec![0; MAX_FILE_SIZE + 1]);
    let mut filled = 0;
    loop {
        let Some(rest) = bytes.get_mut(filled..).filter(|rest| !rest.is_empty()) else {
            return Err(TlsFileProblem::TooLarge);
        };
        match file.read(rest) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(unreadable(error)),
        }
    }
    bytes.truncate(filled);
    Ok(bytes)
}

/// The certificate chain in a PEM file, in file order.
fn certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsFileProblem> {
    let mut chain = Vec::new();
    for certificate in CertificateDer::pem_slice_iter(pem) {
        // The PEM error can quote a line of the file, so it is dropped unread.
        chain.push(certificate.map_err(|_| TlsFileProblem::MalformedPem)?);
    }
    if chain.is_empty() {
        return Err(TlsFileProblem::NoCertificate);
    }
    Ok(chain)
}

/// The one private key in a PEM file, in a buffer that is zeroed when dropped.
fn private_key(pem: &[u8]) -> Result<Zeroizing<PrivateKeyDer<'static>>, TlsFileProblem> {
    let mut found = None;
    for key in PrivateKeyDer::pem_slice_iter(pem) {
        let key = Zeroizing::new(key.map_err(|_| TlsFileProblem::MalformedPem)?);
        if found.is_some() {
            return Err(TlsFileProblem::SeveralPrivateKeys);
        }
        found = Some(key);
    }
    found.ok_or(TlsFileProblem::NoPrivateKey)
}

/// Loads and checks a certificate chain and its private key.
pub(crate) fn load_pair(
    provider: &CryptoProvider,
    cert_path: &Path,
    key_path: &Path,
) -> Result<CertifiedKey, Failure> {
    let cert_pem = read(cert_path).map_err(Failure::Certificate)?;
    let chain = certificates(&cert_pem).map_err(Failure::Certificate)?;
    // A structural X.509 parse of every certificate: webpki parses an intermediate or a CA
    // certificate the same way when it builds a path. No trust, expiry or name policy.
    if chain
        .iter()
        .any(|certificate| ParsedCertificate::try_from(certificate).is_err())
    {
        return Err(Failure::Certificate(TlsFileProblem::UnparsableCertificate));
    }

    let key_pem = read(key_path).map_err(Failure::Key)?;
    let key = private_key(&key_pem).map_err(Failure::Key)?;
    // rustls takes the copy, zeroes it once loaded, and keeps only the signing key.
    let signing_key = provider
        .key_provider
        .load_private_key(key.clone_key())
        .map_err(|_| Failure::Key(TlsFileProblem::UnsupportedKey))?;

    let pair = CertifiedKey::new(chain, signing_key);
    match pair.keys_match() {
        Ok(()) => Ok(pair),
        Err(RustlsError::InconsistentKeys(InconsistentKeys::KeyMismatch)) => {
            Err(Failure::Key(TlsFileProblem::KeyMismatch))
        }
        Err(RustlsError::InconsistentKeys(_)) => Err(Failure::Key(TlsFileProblem::KeyNotCheckable)),
        // keys_match parses the first certificate again; every certificate parsed above.
        Err(_) => Err(Failure::Certificate(TlsFileProblem::UnparsableCertificate)),
    }
}

/// The certificate resolver: hands every TLS handshake the pair in use.
pub(crate) struct CurrentPair(RwLock<Arc<CertifiedKey>>);

impl CurrentPair {
    pub(crate) fn new(pair: CertifiedKey) -> Self {
        Self(RwLock::new(Arc::new(pair)))
    }

    fn replace(&self, pair: CertifiedKey) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(pair);
    }
}

impl fmt::Debug for CurrentPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CurrentPair { .. }")
    }
}

impl ResolvesServerCert for CurrentPair {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(
            &self.0.read().unwrap_or_else(PoisonError::into_inner),
        ))
    }
}

/// The background task that reloads the pair. Dropping it stops the task.
///
/// The task holds the resolver only weakly, so it never keeps the key alive by itself, and it
/// also ends at its next tick once the resolver is gone.
pub(crate) struct Reloader {
    task: JoinHandle<()>,
    #[cfg(test)]
    pub(crate) watch: (Weak<CurrentPair>, Weak<CryptoProvider>),
}

impl Reloader {
    pub(crate) fn spawn(
        current: &Arc<CurrentPair>,
        provider: Arc<CryptoProvider>,
        cert_path: PathBuf,
        key_path: PathBuf,
        first: Instant,
        every: Duration,
    ) -> Self {
        #[cfg(test)]
        let watch = (Arc::downgrade(current), Arc::downgrade(&provider));
        let task = tokio::spawn(reload(
            Arc::downgrade(current),
            provider,
            cert_path,
            key_path,
            first,
            every,
        ));
        Self {
            task,
            #[cfg(test)]
            watch,
        }
    }
}

impl Drop for Reloader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn reload(
    current: Weak<CurrentPair>,
    provider: Arc<CryptoProvider>,
    cert_path: PathBuf,
    key_path: PathBuf,
    first: Instant,
    every: Duration,
) {
    // `first` and every later deadline come from `checked_add`, and `every` is at most
    // `MAX_RELOAD_INTERVAL`, so no deadline overflows here or in tokio's timer. tokio's
    // `Interval` is not used: it adds the period with a plain `+` after every tick.
    let mut deadline = first;
    loop {
        sleep_until(deadline).await;
        if current.strong_count() == 0 {
            return;
        }
        let (provider, cert, key) = (Arc::clone(&provider), cert_path.clone(), key_path.clone());
        let loaded = tokio::task::spawn_blocking(move || load_pair(&provider, &cert, &key)).await;
        let Some(current) = current.upgrade() else {
            return;
        };
        match loaded {
            Ok(Ok(pair)) => current.replace(pair),
            // The path is not logged: certificate paths often hold the tower's host name.
            Ok(Err(failure)) => tracing::warn!(
                target: LOG_TARGET,
                file = failure.file(),
                problem = %failure.problem(),
                "TLS certificate reload failed; the tower keeps serving the pair in use"
            ),
            Err(_) => tracing::warn!(
                target: LOG_TARGET,
                "TLS certificate reload did not finish; the tower keeps serving the pair in use"
            ),
        }
        let Some(next) = Instant::now().checked_add(every) else {
            tracing::warn!(
                target: LOG_TARGET,
                "TLS certificate reload stopped: the next reload cannot be scheduled"
            );
            return;
        };
        deadline = next;
    }
}

#[cfg(test)]
mod tests {
    use super::{TlsFileProblem, read};

    /// The cap the reader enforces.
    const CAP: usize = 64 * 1024;

    /// The reader takes a file of exactly the cap, refuses one byte more, and returns the buffer
    /// it allocated up front: its capacity is the cap plus one, whatever the file's size.
    #[test]
    fn reads_up_to_the_cap_into_a_buffer_that_never_grows() {
        let dir = tempfile::tempdir().unwrap();
        for (size, fits) in [(0, true), (10, true), (CAP, true), (CAP + 1, false)] {
            let path = dir.path().join(format!("{size}.bin"));
            std::fs::write(&path, vec![b'x'; size]).unwrap();
            match read(&path) {
                Ok(bytes) => {
                    assert!(fits, "{size} bytes were accepted");
                    assert_eq!(bytes.len(), size);
                    assert_eq!(bytes.capacity(), CAP + 1);
                }
                Err(problem) => {
                    assert!(!fits, "{size} bytes were refused: {problem}");
                    assert_eq!(problem, TlsFileProblem::TooLarge);
                }
            }
        }
    }
}
