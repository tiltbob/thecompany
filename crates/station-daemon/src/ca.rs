use std::fs::File;
use std::io::Read;
use std::path::Path;

use numbers::TlsRoots;
use rustix::fs::{Mode, OFlags, open};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::{PemObject, SectionKind};

pub(super) const MAX_CA_BYTES: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub(super) enum CaError {
    #[error("could not read --tower-ca; use a readable regular file")]
    Read,
    #[error("--tower-ca exceeds the 1 MiB limit")]
    TooLarge,
    #[error("invalid --tower-ca PEM; supply certificate sections")]
    Pem,
    #[error("--tower-ca contains no certificates")]
    Empty,
    #[error("--tower-ca contains an invalid DER certificate")]
    Der,
}

pub(super) fn load(path: &Path) -> Result<TlsRoots, CaError> {
    // Open without blocking on a FIFO. Check the opened handle so a symlink to a regular
    // bundle works and a path change cannot substitute another file after the type check.
    let fd = open(
        path,
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOCTTY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| CaError::Read)?;
    let file = File::from(fd);
    if !file.metadata().map_err(|_| CaError::Read)?.is_file() {
        return Err(CaError::Read);
    }
    let mut pem = Vec::new();
    file.take(MAX_CA_BYTES as u64 + 1)
        .read_to_end(&mut pem)
        .map_err(|_| CaError::Read)?;
    if pem.len() > MAX_CA_BYTES {
        return Err(CaError::TooLarge);
    }

    // The pinned parser skips unknown section labels and surrounding text. Inspect every
    // recognized section and validate each certificate as a trust anchor before startup.
    let mut store = RootCertStore::empty();
    let mut roots = Vec::new();
    for section in <(SectionKind, Vec<u8>)>::pem_slice_iter(&pem) {
        let (kind, der) = section.map_err(|_| CaError::Pem)?;
        if kind != SectionKind::Certificate {
            return Err(CaError::Pem);
        }
        store
            .add(CertificateDer::from(der.as_slice()))
            .map_err(|_| CaError::Der)?;
        roots.push(der);
    }
    if roots.is_empty() {
        return Err(CaError::Empty);
    }
    Ok(TlsRoots::WebPkiPlus(roots))
}
