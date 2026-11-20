//! The TLS tower's certificate and private key: loaded, checked, served and reloaded by the tower.
//!
//! The tower reads both PEM files itself, at spawn and then at every reload interval, on
//! tokio's blocking pool. A path must name a regular file (symbolic links are followed); a
//! directory, a device or a FIFO is refused before it is opened, because opening or reading one
//! can block. A regular file on a mount that stops answering still blocks the read: at spawn,
//! `Tower::spawn` then does not return; at reload, reloading stops until the read returns.
//!
//! A pair is checked before it is used: the chain holds a certificate, every certificate in it
//! is an X.509 version 3 certificate that webpki can parse, the key is of a kind ring can sign
//! with, and the key belongs to the first certificate. webpki's parse is strict: it refuses a
//! version 1 certificate, issuer or subject unique ids, and an unknown critical extension, for
//! every member of the chain. Chains from the public CAs are all of the accepted kind; a private
//! CA whose root is a version 1 certificate must leave that root out of the file. There is no
//! check of trust, expiry or names; clients do that. A reload publishes a new pair only if it
//! passes the same checks; otherwise the pair in use stays.
//!
//! # Key material in memory
//!
//! What the tower itself does: it reads each file into one buffer of fixed size, allocated
//! before the read, never grown and zeroed when dropped. It decodes the PEM sections one by one
//! and wraps every decoded section in a buffer that is zeroed when dropped, keeping only
//! certificates (from the certificate file) and the one private key (from the key file); so a
//! private key in the certificate file, as in a combined file, is decoded and zeroed too. The
//! key's encoding goes to rustls, which zeroes it after loading. The tower keeps no other copy
//! of its own.
//!
//! What it cannot zero, because the copies are made and freed inside its dependencies:
//!
//! - The PEM decoder (rustls-pki-types) keeps scratch buffers of its own while it decodes,
//!   including the base64 text of every section, and frees them without zeroing them.
//! - rustls's ring provider wraps a SEC1 (`EC PRIVATE KEY`) key in a PKCS#8 structure before
//!   ring loads it: two ordinary vectors per curve it tries, freed without zeroing. It tries
//!   P-256 first, so a P-384 key is wrapped twice: four such vectors. A PKCS#1 or PKCS#8 key is
//!   passed to ring without a copy of rustls's own.
//! - ring (0.17) does not zero its key pairs (`RsaKeyPair`, `EcdsaKeyPair`, `Ed25519KeyPair`
//!   have no zeroing `Drop`). Every key pair the tower drops leaves its key in freed memory: the
//!   pair a reload replaces, at every interval, also when the files did not change; a pair that
//!   fails the match with the certificate; and the pair in use when the tower stops.
//!
//! So the tower limits how long the encoded key stays in memory that it controls. It does not,
//! and cannot, keep key material out of freed memory.
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
use rustls::pki_types::pem::{PemObject, SectionKind};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ParsedCertificate, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{Error as RustlsError, InconsistentKeys};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};
use zeroize::Zeroizing;

use crate::TowerError;

use crate::TLS_LOG_TARGET as LOG_TARGET;

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
    /// The path names something that is not a regular file, such as a directory, a device or a
    /// FIFO. The tower refuses it before opening it: opening or reading such a path can block.
    #[error("it is not a regular file")]
    NotAFile,
    /// The file is larger than a certificate chain or key can reasonably be (64 KiB).
    #[error("it is larger than 64 KiB")]
    TooLarge,
    /// A PEM section in the file is malformed: a bad header, no end marker, or bad base64.
    #[error("it holds a malformed PEM section")]
    MalformedPem,
    /// The certificate file holds no `CERTIFICATE` section.
    #[error("it holds no certificate")]
    NoCertificate,
    /// A certificate in the chain is not an X.509 version 3 certificate that webpki can parse;
    /// which one is not said.
    ///
    /// webpki's parse is strict: it refuses a version 1 certificate, issuer or subject unique
    /// ids, and an unknown critical extension. A private CA whose root is a version 1
    /// certificate must leave that root out of the file.
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
///
/// Before opening, the path must name a regular file. `std::fs::metadata` follows symbolic
/// links and does not block on a FIFO; anything but a regular file is refused, since opening a
/// FIFO without a writer, or reading a terminal or a device, can block. The check and the open
/// are two steps, not one: someone who can replace the file between them can still make the
/// open block.
fn read(path: &Path) -> Result<Zeroizing<Vec<u8>>, TlsFileProblem> {
    let unreadable = |error: std::io::Error| TlsFileProblem::Unreadable(error.kind());
    if !std::fs::metadata(path).map_err(unreadable)?.is_file() {
        return Err(TlsFileProblem::NotAFile);
    }
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

/// Every PEM section of a file, in file order, each decoded into a buffer that is zeroed when
/// dropped, so that a section the caller does not keep (a private key in the certificate file,
/// for example) leaves no decoded copy. A malformed section is an error; the PEM error can quote
/// a line of the file, so it is dropped unread.
fn sections(
    pem: &[u8],
) -> impl Iterator<Item = Result<(SectionKind, Zeroizing<Vec<u8>>), TlsFileProblem>> + '_ {
    <(SectionKind, Vec<u8>)>::pem_slice_iter(pem).map(|section| {
        section
            .map(|(kind, der)| (kind, Zeroizing::new(der)))
            .map_err(|_| TlsFileProblem::MalformedPem)
    })
}

/// The certificate chain in a PEM file, in file order. Other sections are skipped.
fn certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsFileProblem> {
    let mut chain = Vec::new();
    for section in sections(pem) {
        let (kind, mut der) = section?;
        if kind == SectionKind::Certificate {
            // A certificate is public; the buffer moves out and its empty shell is zeroed.
            chain.push(CertificateDer::from(std::mem::take(&mut *der)));
        }
    }
    if chain.is_empty() {
        return Err(TlsFileProblem::NoCertificate);
    }
    Ok(chain)
}

/// The one private key in a PEM file (PKCS#1, SEC1 or PKCS#8), in a buffer that is zeroed when
/// dropped. Other sections, an encrypted PKCS#8 key among them, are skipped.
fn private_key(pem: &[u8]) -> Result<Zeroizing<PrivateKeyDer<'static>>, TlsFileProblem> {
    let mut found = None;
    for section in sections(pem) {
        let (kind, mut der) = section?;
        if !matches!(
            kind,
            SectionKind::RsaPrivateKey | SectionKind::EcPrivateKey | SectionKind::PrivateKey
        ) {
            continue;
        }
        // The buffer moves into the key, which is zeroed when dropped; `from_pem` gives a key
        // for each of these three kinds.
        let Some(key) = PrivateKeyDer::from_pem(kind, std::mem::take(&mut *der)) else {
            continue;
        };
        let key = Zeroizing::new(key);
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
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use rustls::crypto::CryptoProvider;
    use rustls::sign::CertifiedKey;
    use tokio::time::{Instant, sleep};

    use super::{CurrentPair, Reloader, TlsFileProblem, load_pair, read};

    /// The cap the reader enforces.
    const CAP: usize = 64 * 1024;

    /// The reload interval of the reload task tests: long, so that only the test moves time on.
    const EVERY: Duration = Duration::from_secs(60 * 60);

    /// A self-signed pair for 127.0.0.1 written as PEM files into `dir`, the provider, and the
    /// pair loaded from the files.
    fn pair_in(dir: &tempfile::TempDir) -> (PathBuf, PathBuf, Arc<CryptoProvider>, CertifiedKey) {
        let generated = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).unwrap();
        let (cert, key) = (dir.path().join("cert.pem"), dir.path().join("key.pem"));
        std::fs::write(&cert, generated.cert.pem()).unwrap();
        std::fs::write(&key, generated.signing_key.serialize_pem()).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let pair = load_pair(&provider, &cert, &key).unwrap();
        (cert, key, provider, pair)
    }

    /// The pair the resolver hands out now.
    fn in_use(current: &CurrentPair) -> Arc<CertifiedKey> {
        Arc::clone(&current.0.read().unwrap())
    }

    /// The reload task keeps neither the resolver nor, once the reloader is dropped, the
    /// provider alive: the resolver is gone as soon as its last holder drops it, and the
    /// provider soon after the reloader is dropped, although the next reload is an hour away.
    #[tokio::test]
    async fn the_reload_task_holds_the_resolver_weakly_and_ends_when_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key, provider, pair) = pair_in(&dir);
        let current = Arc::new(CurrentPair::new(pair));
        let reloader =
            Reloader::spawn(&current, provider, cert, key, Instant::now() + EVERY, EVERY);
        let (resolver, provider) = reloader.watch.clone();

        drop(current);
        assert_eq!(resolver.strong_count(), 0);

        drop(reloader);
        // The aborted task lets go of the provider when the runtime next runs it; no event
        // marks that, so poll under a bound that only a task that was not aborted can reach.
        let released = tokio::time::timeout(Duration::from_secs(10), async {
            let mut tick = tokio::time::interval(Duration::from_millis(10));
            while provider.strong_count() > 0 {
                tick.tick().await;
            }
        })
        .await;
        assert!(released.is_ok(), "the reload task still holds the provider");
    }

    /// Waits on real time, through the blocking pool, until the pair in use is no longer `old`,
    /// and returns it. A blocking task in flight stops paused time from moving on, so this
    /// never lets a later reload run. 5000 round trips of at least 2 ms bound it at 10 s or more.
    async fn replaced(current: &CurrentPair, old: &Arc<CertifiedKey>) -> Arc<CertifiedKey> {
        for _ in 0..5000 {
            let pair = in_use(current);
            if !Arc::ptr_eq(&pair, old) {
                return pair;
            }
            blocking_round_trip().await;
        }
        panic!("the due reload did not finish within 10 s");
    }

    /// One real round trip through the blocking pool, of at least 2 ms.
    async fn blocking_round_trip() {
        tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(2)))
            .await
            .unwrap();
    }

    /// The reload task reloads at its first deadline, not before, then once per interval: never
    /// in between, and it keeps going.
    ///
    /// Time is paused. While a blocking task is in flight on a current-thread runtime, tokio does
    /// not move paused time on by itself (`BlockingSchedule` in tokio's blocking pool inhibits
    /// auto-advance until the task is released), so a test `sleep` that ends after a reload
    /// deadline returns only once that reload has replaced the pair. Every pair read is kept
    /// alive, so `Arc::ptr_eq` is a sound "same pair" check.
    #[tokio::test(start_paused = true)]
    async fn the_reload_task_reloads_once_per_interval_from_its_first_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key, provider, pair) = pair_in(&dir);
        let current = Arc::new(CurrentPair::new(pair));
        let first = in_use(&current);
        let _reloader =
            Reloader::spawn(&current, provider, cert, key, Instant::now() + EVERY, EVERY);

        sleep(EVERY - Duration::from_secs(1)).await;
        assert!(
            Arc::ptr_eq(&in_use(&current), &first),
            "reloaded before the first deadline"
        );

        // `advance`, not `sleep`: a reload task that never waits would keep the blocking pool
        // busy, paused time would never move on, and a `sleep` would hang instead of failing.
        tokio::time::advance(Duration::from_secs(2)).await;
        let second = replaced(&current, &first).await;

        // Real time passes, paused time does not: no reload is due, so none runs.
        for _ in 0..50 {
            blocking_round_trip().await;
        }
        assert!(
            Arc::ptr_eq(&in_use(&current), &second),
            "reloaded again before the next deadline"
        );

        // The reload ran at the time `advance` moved to, so the next one is due one interval
        // later. Sleep one second past it, so that the test does not wake at the very instant
        // the reload task does.
        sleep(EVERY + Duration::from_secs(1)).await;
        let third = in_use(&current);
        assert!(
            !Arc::ptr_eq(&third, &second) && !Arc::ptr_eq(&third, &first),
            "no reload one interval later"
        );
    }

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
