//! The TLS tower's certificate and private key: loaded, checked, served and reloaded by the tower.
//!
//! The tower reads both PEM files itself, at spawn and then at every reload interval. The bytes
//! of the key file live in a buffer that is zeroed when it is dropped; the decoded key goes to
//! rustls (with the ring provider), which zeroes its copy of the encoding and afterwards holds
//! the key only inside its signing key. One caveat: the PEM decoder (rustls-pki-types) uses
//! scratch buffers of its own while it decodes, and frees them without zeroing them. The tower
//! cannot change that, so this is no promise that no copy of the key ever remains in freed
//! memory; it is a promise about the memory the tower keeps.
//!
//! A pair is checked before it is used: the chain holds a certificate, the first certificate
//! parses, the key is of a kind ring can sign with, and the key belongs to the certificate. A
//! reload publishes a new pair only if it passes the same checks; otherwise the pair in use
//! stays.
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
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use zeroize::Zeroizing;

use crate::TowerError;

const LOG_TARGET: &str = "numbers_tower::tls";

/// The largest certificate or key file the tower reads, in bytes.
const MAX_FILE_SIZE: u64 = 1024 * 1024;

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
    /// The file is larger than a certificate chain or key can reasonably be (1 MiB).
    #[error("it is larger than 1 MiB")]
    TooLarge,
    /// A PEM section in the file is malformed: a bad header, no end marker, or bad base64.
    #[error("it holds a malformed PEM section")]
    MalformedPem,
    /// The certificate file holds no `CERTIFICATE` section.
    #[error("it holds no certificate")]
    NoCertificate,
    /// The first certificate in the chain is not a parsable X.509 certificate.
    #[error("its certificate cannot be parsed")]
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
fn read(path: &Path) -> Result<Zeroizing<Vec<u8>>, TlsFileProblem> {
    let unreadable = |error: std::io::Error| TlsFileProblem::Unreadable(error.kind());
    let file = File::open(path).map_err(unreadable)?;
    let expected = file
        .metadata()
        .map_err(unreadable)?
        .len()
        .min(MAX_FILE_SIZE);
    // Sized up front so that the buffer is not reallocated, which would leave unzeroed copies.
    let capacity = usize::try_from(expected).unwrap_or(0).saturating_add(1);
    let mut bytes = Zeroizing::new(Vec::with_capacity(capacity));
    file.take(MAX_FILE_SIZE + 1)
        .read_to_end(&mut bytes)
        .map_err(unreadable)?;
    if bytes.len() as u64 > MAX_FILE_SIZE {
        return Err(TlsFileProblem::TooLarge);
    }
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
    let end_entity = chain
        .first()
        .ok_or(Failure::Certificate(TlsFileProblem::NoCertificate))?;
    if ParsedCertificate::try_from(end_entity).is_err() {
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
        // keys_match parses the end-entity certificate again; that passed above.
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
        every: Duration,
    ) -> Self {
        #[cfg(test)]
        let watch = (Arc::downgrade(current), Arc::downgrade(&provider));
        let task = tokio::spawn(reload(
            Arc::downgrade(current),
            provider,
            cert_path,
            key_path,
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
    every: Duration,
) {
    let mut ticks = interval_at(Instant::now() + every, every);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
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
    }
}
