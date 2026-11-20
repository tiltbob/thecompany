//! Persistent state of one station, behind its data-directory lock.
//!
//! As with `numbers_base::keyfile`, ancestors of the data directory must be writable only by
//! the station user or root. The node key and peer book are opened relative to the checked
//! directory's descriptor. The actor is the only peer-book writer. Stop it before editing the book.

#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, HashSet};
use std::ffi::OsStr;
use std::fs::{File, TryLockError};
use std::io::{self, Read, Write};
use std::path::Path;

use chief::api::check_peer_name;
use numbers::{NodeId, generate_node_key};
use numbers_base::{ensure_private_dir, load_or_create_key_in};
use rustix::fs::{AtFlags, FileType, Mode, OFlags, Stat};
use rustix::io::Errno;
use serde::{Deserialize, Serialize};

const LOCK_FILE: &str = "lock";
const KEY_FILE: &str = "node.key";
const PEER_FILE: &str = "peers.json";
const PEER_FILE_VERSION: u32 = 1;
// This also leaves room for the link records in `Response::Peers` under the 1 MiB IPC cap.
const PEER_FILE_MAX_BYTES: usize = 256 * 1024;
const STAGING_ATTEMPTS: u8 = 16;
const PRIVATE_FILE_MODE: Mode = Mode::RUSR.union(Mode::WUSR);
const OPEN_PRIVATE: OFlags = OFlags::NOFOLLOW
    .union(OFlags::NONBLOCK)
    .union(OFlags::NOCTTY)
    .union(OFlags::CLOEXEC);

/// Names are sorted for snapshots. Each peer's stations stay in insertion order.
pub(crate) type PeerBook = BTreeMap<String, Vec<NodeId>>;

/// The result of a peer-book replacement once the new file is visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SaveOutcome {
    Durable,
    /// The rename committed, but syncing the directory failed. Apply the new book in memory.
    DurabilityUncertain,
}

/// Displayable diagnostics deliberately omit user paths, IDs, and file contents.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StorageError {
    #[error("could not use the station data directory; it must be private and owned by this user")]
    DataDir,
    #[error(
        "could not open the station lock; it must be a regular file owned by this user with mode 0600"
    )]
    LockFile,
    #[error("another station is already running with this data directory")]
    AlreadyRunning,
    #[error("could not take the station data-directory lock")]
    TakeLock,
    #[error("could not load or create node.key; check its owner, permissions and validity")]
    NodeKey,
    #[error(
        "could not use peers.json; it must be a regular file owned by this user with mode 0600"
    )]
    PeerFile,
    #[error("could not read peers.json")]
    ReadPeers,
    #[error("invalid peers.json: {0}")]
    InvalidPeers(&'static str),
    #[error("peer book exceeds the 256 KiB limit")]
    PeerBookTooLarge,
    #[error("could not create a private staging file for peers.json")]
    StagePeers,
    #[error("all staging names for peers.json are taken")]
    StagingNamesTaken,
    #[error("could not write or sync the staged peer book")]
    WritePeers,
    #[error("could not replace peers.json")]
    ReplacePeers,
}

/// The station's checked data directory and the lock held until the actor stops.
pub(crate) struct Storage {
    dir: File,
    _lock: File,
    #[cfg(test)]
    fail_peer_sync: Cell<bool>,
}

impl Storage {
    /// Acquires the lock without creating or reading the node key.
    pub(crate) fn lock(data_dir: &Path) -> Result<Self, StorageError> {
        let path = if data_dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            data_dir
        };
        ensure_private_dir(path).map_err(|_| StorageError::DataDir)?;
        let dir = rustix::fs::open(
            path,
            OFlags::DIRECTORY | OFlags::RDONLY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map(File::from)
        .map_err(|_| StorageError::DataDir)?;
        let stat = rustix::fs::fstat(&dir).map_err(|_| StorageError::DataDir)?;
        if stat.st_uid != rustix::process::geteuid().as_raw()
            || Mode::from_raw_mode(stat.st_mode).intersects(Mode::RWXG | Mode::RWXO)
        {
            return Err(StorageError::DataDir);
        }

        let lock = open_lock(&dir)?;
        match lock.try_lock() {
            Ok(()) => Ok(Self {
                dir,
                _lock: lock,
                #[cfg(test)]
                fail_peer_sync: Cell::default(),
            }),
            Err(TryLockError::WouldBlock) => Err(StorageError::AlreadyRunning),
            Err(TryLockError::Error(_)) => Err(StorageError::TakeLock),
        }
    }

    pub(crate) fn load_node_key(&self) -> Result<[u8; 32], StorageError> {
        load_or_create_key_in(&self.dir, OsStr::new(KEY_FILE), generate_node_key)
            .map(|stored| stored.key)
            .map_err(|_| StorageError::NodeKey)
    }

    pub(crate) fn load_peers(&self, own_id: NodeId) -> Result<PeerBook, StorageError> {
        if !check_private_entry(&self.dir, PEER_FILE).map_err(|_| StorageError::PeerFile)? {
            return Ok(PeerBook::new());
        }
        let file = rustix::fs::openat(
            &self.dir,
            PEER_FILE,
            OFlags::RDONLY | OPEN_PRIVATE,
            Mode::empty(),
        )
        .map(File::from)
        .map_err(|_| StorageError::ReadPeers)?;
        check_private_file(&file).map_err(|_| StorageError::PeerFile)?;

        let mut bytes = Vec::new();
        file.take(PEER_FILE_MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| StorageError::ReadPeers)?;
        if bytes.len() > PEER_FILE_MAX_BYTES {
            return Err(StorageError::PeerBookTooLarge);
        }
        let disk: DiskBook = serde_json::from_slice(&bytes)
            .map_err(|_| StorageError::InvalidPeers("expected a versioned peer book"))?;
        disk.into_book(own_id)
    }

    /// Any error means the rename did not happen. Both successful outcomes commit the new book.
    pub(crate) fn save_peers(
        &self,
        book: &PeerBook,
        own_id: NodeId,
    ) -> Result<SaveOutcome, StorageError> {
        validate_book(book, own_id)?;
        let bytes =
            serde_json::to_vec(&DiskBook::from_book(book)).map_err(|_| StorageError::WritePeers)?;
        if bytes.len() > PEER_FILE_MAX_BYTES {
            return Err(StorageError::PeerBookTooLarge);
        }
        self.replace_peers(&bytes, |dir| {
            #[cfg(test)]
            if self.fail_peer_sync.replace(false) {
                return Err(io::ErrorKind::Other.into());
            }
            dir.sync_all()
        })
    }

    #[cfg(test)]
    pub(super) fn fail_next_peer_sync(&self) {
        self.fail_peer_sync.set(true);
    }

    fn replace_peers(
        &self,
        bytes: &[u8],
        sync_dir: impl FnOnce(&File) -> io::Result<()>,
    ) -> Result<SaveOutcome, StorageError> {
        let mut stage = StagedBook::create(&self.dir)?;
        stage
            .file
            .write_all(bytes)
            .and_then(|()| stage.file.sync_all())
            .map_err(|_| StorageError::WritePeers)?;
        check_private_entry(&self.dir, PEER_FILE).map_err(|_| StorageError::PeerFile)?;
        stage.commit()?;
        Ok(match sync_dir(&self.dir) {
            Ok(()) => SaveOutcome::Durable,
            Err(_) => SaveOutcome::DurabilityUncertain,
        })
    }
}

fn open_lock(dir: &File) -> Result<File, StorageError> {
    let flags = OFlags::RDWR | OPEN_PRIVATE;
    let file = match rustix::fs::openat(
        dir,
        LOCK_FILE,
        flags | OFlags::CREATE | OFlags::EXCL,
        PRIVATE_FILE_MODE,
    ) {
        Ok(fd) => {
            let file = File::from(fd);
            set_private_mode(&file).map_err(|_| StorageError::LockFile)?;
            file
        }
        Err(Errno::EXIST) => {
            check_private_entry(dir, LOCK_FILE).map_err(|_| StorageError::LockFile)?;
            rustix::fs::openat(dir, LOCK_FILE, flags, Mode::empty())
                .map(File::from)
                .map_err(|_| StorageError::LockFile)?
        }
        Err(_) => return Err(StorageError::LockFile),
    };
    check_private_file(&file).map_err(|_| StorageError::LockFile)?;
    Ok(file)
}

/// `Ok(false)` means absent. A symlink, special file, wrong owner or unsafe mode is an error.
fn check_private_entry(dir: &File, name: &str) -> io::Result<bool> {
    match rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => check_private_stat(&stat).map(|()| true),
        Err(Errno::NOENT) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn check_private_file(file: &File) -> io::Result<()> {
    check_private_stat(&rustix::fs::fstat(file)?)
}

fn is_owned_regular(stat: &Stat) -> bool {
    FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile
        && stat.st_uid == rustix::process::geteuid().as_raw()
}

fn check_private_stat(stat: &Stat) -> io::Result<()> {
    if is_owned_regular(stat) && Mode::from_raw_mode(stat.st_mode) == PRIVATE_FILE_MODE {
        Ok(())
    } else {
        Err(io::ErrorKind::PermissionDenied.into())
    }
}

fn set_private_mode(file: &File) -> io::Result<()> {
    if !is_owned_regular(&rustix::fs::fstat(file)?) {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    rustix::fs::fchmod(file, PRIVATE_FILE_MODE)?;
    check_private_file(file)
}

struct StagedBook<'a> {
    dir: &'a File,
    name: String,
    file: File,
    committed: bool,
}

impl<'a> StagedBook<'a> {
    fn create(dir: &'a File) -> Result<Self, StorageError> {
        for index in 0..STAGING_ATTEMPTS {
            let name = staging_name(index);
            match rustix::fs::openat(
                dir,
                name.as_str(),
                OFlags::WRONLY | OPEN_PRIVATE | OFlags::CREATE | OFlags::EXCL,
                PRIVATE_FILE_MODE,
            ) {
                Ok(fd) => {
                    let stage = Self {
                        dir,
                        name,
                        file: File::from(fd),
                        committed: false,
                    };
                    set_private_mode(&stage.file).map_err(|_| StorageError::StagePeers)?;
                    return Ok(stage);
                }
                Err(Errno::EXIST) => {}
                Err(_) => return Err(StorageError::StagePeers),
            }
        }
        Err(StorageError::StagingNamesTaken)
    }

    fn commit(mut self) -> Result<(), StorageError> {
        rustix::fs::renameat(self.dir, self.name.as_str(), self.dir, PEER_FILE)
            .map_err(|_| StorageError::ReplacePeers)?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for StagedBook<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = rustix::fs::unlinkat(self.dir, self.name.as_str(), AtFlags::empty());
        }
    }
}

fn staging_name(index: u8) -> String {
    format!(".peers.json.{}.{index}.tmp", std::process::id())
}

#[derive(Serialize, Deserialize)]
struct DiskBook {
    version: u32,
    peers: Vec<DiskPeer>,
}

#[derive(Serialize, Deserialize)]
struct DiskPeer {
    name: String,
    stations: Vec<String>,
}

impl DiskBook {
    fn from_book(book: &PeerBook) -> Self {
        Self {
            version: PEER_FILE_VERSION,
            peers: book
                .iter()
                .map(|(name, stations)| DiskPeer {
                    name: name.clone(),
                    stations: stations.iter().map(ToString::to_string).collect(),
                })
                .collect(),
        }
    }

    fn into_book(self, own_id: NodeId) -> Result<PeerBook, StorageError> {
        if self.version != PEER_FILE_VERSION {
            return Err(StorageError::InvalidPeers("unsupported version"));
        }
        let mut book = PeerBook::new();
        for entry in self.peers {
            let stations = entry
                .stations
                .iter()
                .map(|id| {
                    id.parse::<NodeId>()
                        .map_err(|_| StorageError::InvalidPeers("invalid station ID"))
                })
                .collect::<Result<_, _>>()?;
            if book.insert(entry.name, stations).is_some() {
                return Err(StorageError::InvalidPeers("duplicate peer name"));
            }
        }
        validate_book(&book, own_id)?;
        Ok(book)
    }
}

fn validate_book(book: &PeerBook, own_id: NodeId) -> Result<(), StorageError> {
    let mut seen = HashSet::new();
    for (name, stations) in book {
        check_peer_name(name).map_err(|_| StorageError::InvalidPeers("invalid peer name"))?;
        if stations.is_empty() {
            return Err(StorageError::InvalidPeers("peer has no stations"));
        }
        for station in stations {
            if *station == own_id {
                return Err(StorageError::InvalidPeers(
                    "station lists its own ID as a peer",
                ));
            }
            if !seen.insert(station) {
                return Err(StorageError::InvalidPeers("duplicate station ID"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
