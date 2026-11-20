//! Key files for daemons, and the private directories that hold them (and the station's socket).
//!
//! # Rules
//!
//! - A private directory is created with mode 0700. An existing one must be a directory owned by
//!   the effective user with no group or other permission bits. [`ensure_private_dir`] creates
//!   what is missing; [`check_private_dir`] only checks.
//! - A key file is published atomically: the key is written to a temporary file in the checked
//!   directory, synced, and then hard-linked to its name, which never overwrites anything. The
//!   name appears with the whole key or not at all, so a reader never sees a partial key.
//! - A key file is read only through its checked directory, and only if it is a regular file owned
//!   by the effective user with mode exactly 0600 and exactly the length of a key.
//! - After the directory check, everything goes through the directory's descriptor and the file's
//!   descriptor (`openat`, `fstat`, `linkat`, `unlinkat`), so nothing can be swapped in between a
//!   check and its use.
//!
//! # What the checks do not cover
//!
//! The checks hold for the directory as it was during the call. Every directory on the path above
//! it must be writable only by this user or by root: whoever can write an ancestor can redirect
//! later lookups of the path. The default XDG locations under the home directory satisfy this. Key
//! creation and reading are anchored to the checked descriptor; a caller that later uses the
//! directory by path (to bind a socket in it, for example) relies on that precondition.
//!
//! The helpers are content-agnostic: a key is bytes, and what the bytes mean is up to the caller.
//! No key bytes ever appear in an error, in `Debug` output or in a log.
//!
//! Unix only.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, Permissions};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use rustix::fs::{AtFlags, Mode, OFlags};
use rustix::io::Errno;

/// Mode of a key file.
const KEY_FILE_MODE: u32 = 0o600;
/// Mode of a directory this module creates.
const PRIVATE_DIR_MODE: u32 = 0o700;
/// Permission bits, including setuid, setgid and sticky.
const PERMISSION_BITS: u32 = 0o7777;
/// Group and other permission bits.
const GROUP_OTHER_BITS: u32 = 0o077;

/// Why a private directory or a key file was refused or could not be used.
///
/// Every message names the path and what to do about it. No variant holds key bytes.
#[derive(Debug)]
pub enum PrivatePathError {
    /// The file or directory belongs to another user.
    WrongOwner {
        /// The refused path.
        path: PathBuf,
        /// The uid that owns it.
        owner: u32,
        /// The effective uid of this process.
        expected: u32,
    },
    /// A key file whose mode is not exactly 0600.
    KeyFileMode {
        /// The refused key file.
        path: PathBuf,
        /// The permission bits found.
        mode: u32,
    },
    /// A directory that gives group or others some access.
    DirMode {
        /// The refused directory.
        path: PathBuf,
        /// The permission bits found.
        mode: u32,
    },
    /// Something other than a regular file is where a key file should be.
    NotRegularFile {
        /// The refused path.
        path: PathBuf,
    },
    /// Something other than a directory is where a directory should be.
    NotDirectory {
        /// The refused path.
        path: PathBuf,
    },
    /// A symbolic link is where a key file, or a directory this call created, should be.
    Symlink {
        /// The refused path.
        path: PathBuf,
    },
    /// The key file does not hold exactly as many bytes as a key.
    WrongLength {
        /// The refused key file.
        path: PathBuf,
        /// The number of bytes found.
        found: u64,
        /// The length of a key.
        expected: usize,
    },
    /// Something already exists where a new key file was to be created.
    AlreadyExists {
        /// The path that is taken.
        path: PathBuf,
    },
    /// The key file, or the directory that should hold it, does not exist.
    NotFound {
        /// The missing path: the key file, or the directory if that is what is missing.
        path: PathBuf,
    },
    /// Any other I/O failure.
    Io {
        /// The path the operation was on.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
}

impl fmt::Display for PrivatePathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongOwner {
                path,
                owner,
                expected,
            } => write!(
                f,
                "{} is owned by uid {owner}, but this process runs as uid {expected}; refusing to \
                 use it. Run as the user that owns it, or use a directory of your own",
                path.display()
            ),
            Self::KeyFileMode { path, mode } => write!(
                f,
                "key file {} has mode {mode:04o}, but a key file must have mode 0600 (owner only); \
                 refusing to use it. If nobody else can have read it, run `chmod 600` on it; \
                 otherwise treat the key as exposed and replace it",
                path.display()
            ),
            Self::DirMode { path, mode } => write!(
                f,
                "directory {} has mode {mode:04o}, but it must give no access to group or others; \
                 refusing to use it. Run `chmod 700` on it",
                path.display()
            ),
            Self::NotRegularFile { path } => write!(
                f,
                "{} is not a regular file, so it cannot be a key file; move it out of the way",
                path.display()
            ),
            Self::NotDirectory { path } => write!(
                f,
                "{} is not a directory; move it out of the way or choose another directory",
                path.display()
            ),
            Self::Symlink { path } => write!(
                f,
                "{} is a symbolic link where a key file or a newly created directory belongs; \
                 replace the link with the file or directory itself",
                path.display()
            ),
            Self::WrongLength {
                path,
                found,
                expected,
            } => write!(
                f,
                "key file {} holds {found} bytes, but a key is {expected} bytes: the file is \
                 damaged or is not a key file. Restore it from a backup; remove it only if you \
                 mean to give up the identity it held",
                path.display()
            ),
            Self::AlreadyExists { path } => write!(
                f,
                "{} already exists; a key file is never overwritten. Remove it first only if you \
                 mean to give up the identity it holds",
                path.display()
            ),
            Self::NotFound { path } => write!(f, "{} does not exist", path.display()),
            Self::Io { path, source } => write!(f, "{}: {source}", path.display()),
        }
    }
}

impl std::error::Error for PrivatePathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Whether [`load_or_create_key`] created the key or found it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOrigin {
    /// This call generated the key and published it: the first run.
    Created,
    /// The key already existed (possibly published by a concurrent starter).
    Loaded,
}

/// A key returned by [`load_or_create_key`]. Its `Debug` output never shows the key.
#[derive(Clone, PartialEq, Eq)]
pub struct StoredKey<const N: usize> {
    /// The key bytes.
    pub key: [u8; N],
    /// Whether this call created the key or loaded it.
    pub origin: KeyOrigin,
}

impl<const N: usize> fmt::Debug for StoredKey<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredKey")
            .field("key", &format_args!("<{N} bytes>"))
            .field("origin", &self.origin)
            .finish()
    }
}

/// Creates `dir` with mode 0700 if it does not exist, and checks it. An existing `dir` must be a
/// directory owned by the effective user with no group or other permission bits; anything else is
/// refused.
///
/// Missing levels are created one at a time below the first existing ancestor, each through its
/// parent's descriptor: owner checked, mode set to exactly 0700 whatever the umask, and the parent
/// synced so the new entry survives a crash. A level that this call created cannot be a symlink.
/// Existing ancestors are not checked. If `dir` existed, a symlink at `dir` is followed and the
/// directory it points to must pass the checks (a data directory moved elsewhere and linked back is
/// a legitimate setup). An empty path means the current directory.
///
/// The checks hold for the directory as it was during the call, and they assume that every
/// directory above it is writable only by this user or by root: see the [module docs](self).
pub fn ensure_private_dir(dir: &Path) -> Result<(), PrivatePathError> {
    open_private_dir(dir, Create::IfMissing).map(drop)
}

/// Checks `dir` as [`ensure_private_dir`] does, but never creates anything: a missing `dir` is
/// [`PrivatePathError::NotFound`]. For a client that wants to check a directory it expects
/// another process to have created, such as the station's socket directory.
pub fn check_private_dir(dir: &Path) -> Result<(), PrivatePathError> {
    open_private_dir(dir, Create::Never).map(drop)
}

/// Publishes `key` at `path` atomically, never overwriting: fails with
/// [`PrivatePathError::AlreadyExists`] if anything (a symlink included) is at `path`.
///
/// The parent directory goes through [`ensure_private_dir`] first, and everything after that goes
/// through its descriptor. The key is written to a temporary file in that directory, which gets
/// mode exactly 0600 and has its owner and mode checked before the key is written; the file is
/// synced, hard-linked to `path`, the temporary name is removed in every outcome, and the
/// directory is synced. So the name appears with the whole key or not at all, and a key reported
/// as created survives a crash, also on a first run that created the directory.
///
/// An error returned after the link (removing the temporary name or syncing the directory failed)
/// can leave a complete key file behind, so a retry gets `AlreadyExists`; the key can then be read.
/// A temporary file left by a crash does not block a later create. Needs a file system with hard
/// links.
pub fn create_key_file(path: &Path, key: &[u8]) -> Result<(), PrivatePathError> {
    let (dir_path, name) = split(path)?;
    let dir = open_private_dir(dir_path, Create::IfMissing)?;
    create_in(&dir, dir_path, path, name, key, write_all)
}

/// Reads an `N`-byte key file. The directory that holds it must pass [`check_private_dir`] (it is
/// never created here), and the file must be a regular file owned by the effective user with mode
/// exactly 0600 and exactly `N` bytes long.
///
/// A missing key file, or a missing directory, is [`PrivatePathError::NotFound`] naming what is
/// missing. The file is opened through the checked directory with `O_NOFOLLOW` (a symlink is
/// refused) and `O_NONBLOCK` (a FIFO cannot make the call hang), and all checks run on the open
/// descriptor.
pub fn read_key_file<const N: usize>(path: &Path) -> Result<[u8; N], PrivatePathError> {
    let (dir_path, name) = split(path)?;
    let dir = open_private_dir(dir_path, Create::Never)?;
    read_in(&dir, path, name)
}

/// The whole first-run sequence for a daemon's key: ensures the private directory, reads the key,
/// and if there is none, calls `generate` once, publishes the result and returns it as
/// [`KeyOrigin::Created`]. If another starter published a key first, its key is read and returned
/// as [`KeyOrigin::Loaded`], so concurrent starters all end up with the same key.
///
/// `generate` is called at most once, and only when no key file exists. Any other problem with an
/// existing key file (a damaged one included) is an error, never a reason to make a new key.
pub fn load_or_create_key<const N: usize>(
    path: &Path,
    generate: impl FnOnce() -> [u8; N],
) -> Result<StoredKey<N>, PrivatePathError> {
    let (dir_path, name) = split(path)?;
    let dir = open_private_dir(dir_path, Create::IfMissing)?;
    let loaded = |key| StoredKey {
        key,
        origin: KeyOrigin::Loaded,
    };
    match read_in(&dir, path, name) {
        Ok(key) => return Ok(loaded(key)),
        Err(PrivatePathError::NotFound { .. }) => {}
        Err(e) => return Err(e),
    }
    let key = generate();
    match create_in(&dir, dir_path, path, name, &key, write_all) {
        Ok(()) => Ok(StoredKey {
            key,
            origin: KeyOrigin::Created,
        }),
        Err(PrivatePathError::AlreadyExists { .. }) => read_in(&dir, path, name).map(loaded),
        Err(e) => Err(e),
    }
}

/// Whether [`open_private_dir`] may create missing directories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Create {
    IfMissing,
    Never,
}

/// Opens `dir`, creating it and its missing parents if `create` allows, and returns the checked
/// directory as an open handle (a `File` owning the directory's descriptor).
fn open_private_dir(dir: &Path, create: Create) -> Result<File, PrivatePathError> {
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    match open_dir_by_path(dir) {
        Ok(handle) => {
            check_dir(&handle, dir)?;
            return Ok(handle);
        }
        Err(e) if e == Errno::NOENT && create == Create::IfMissing => {}
        Err(e) if e == Errno::NOENT => return Err(PrivatePathError::NotFound { path: dir.into() }),
        Err(e) => return Err(dir_open_error(dir, e)),
    }

    // Walk up to the first existing ancestor, collecting the names to create.
    let mut missing = Vec::new();
    let mut base = dir;
    let mut parent = loop {
        let name = base.file_name().ok_or_else(|| {
            io_error(
                base,
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot create a directory at this path",
                ),
            )
        })?;
        missing.push(name);
        base = parent_or_dot(base);
        match open_dir_by_path(base) {
            Ok(handle) => break handle,
            Err(e) if e == Errno::NOENT => {}
            Err(e) => return Err(dir_open_error(base, e)),
        }
    };

    // Create the missing levels downward, each through its parent's descriptor.
    let mut parent_path = base.to_path_buf();
    for name in missing.into_iter().rev() {
        let path = parent_path.join(name);
        let created = match rustix::fs::mkdirat(&parent, name, Mode::RWXU) {
            Ok(()) => true,
            Err(e) if e == Errno::EXIST => false,
            Err(e) => return Err(io_error(&path, e.into())),
        };
        let fd = rustix::fs::openat(
            &parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| {
            if e == Errno::LOOP || e == Errno::MLINK {
                PrivatePathError::Symlink { path: path.clone() }
            } else if e == Errno::NOTDIR {
                PrivatePathError::NotDirectory { path: path.clone() }
            } else {
                io_error(&path, e.into())
            }
        })?;
        let handle = File::from(fd);
        if created {
            let meta = handle.metadata().map_err(|e| io_error(&path, e))?;
            let euid = effective_uid();
            // The owner first: never change the mode of something that is not ours.
            if meta.uid() != euid {
                return Err(Refusal::Owner {
                    owner: meta.uid(),
                    expected: euid,
                }
                .at(&path));
            }
            // The umask may have narrowed the mode (or a default ACL widened it).
            handle
                .set_permissions(Permissions::from_mode(PRIVATE_DIR_MODE))
                .map_err(|e| io_error(&path, e))?;
            parent.sync_all().map_err(|e| io_error(&parent_path, e))?;
        }
        // A level that appeared concurrently is checked like any existing directory.
        check_dir(&handle, &path)?;
        parent = handle;
        parent_path = path;
    }
    Ok(parent)
}

/// Opens a directory by path, following a symlink.
fn open_dir_by_path(dir: &Path) -> rustix::io::Result<File> {
    rustix::fs::open(
        dir,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
}

/// Maps an error from opening `dir` as a directory.
fn dir_open_error(dir: &Path, e: Errno) -> PrivatePathError {
    // ENOTDIR: either `dir` itself is no directory, or one of its ancestors is not.
    let is_other = e == Errno::NOTDIR && fs::metadata(dir).is_ok_and(|m| !m.is_dir());
    if is_other {
        PrivatePathError::NotDirectory { path: dir.into() }
    } else {
        io_error(dir, e.into())
    }
}

/// Checks an open directory: type, owner, no group or other bits.
fn check_dir(handle: &File, path: &Path) -> Result<(), PrivatePathError> {
    let meta = handle.metadata().map_err(|e| io_error(path, e))?;
    if !meta.is_dir() {
        return Err(PrivatePathError::NotDirectory { path: path.into() });
    }
    check_owner_and_mode(Want::PrivateDir, meta.uid(), meta.mode(), effective_uid())
        .map_err(|r| r.at(path))
}

/// Writes the key into a temporary file in the checked directory `dir` and links it to `name`.
/// `write` puts the key into the file; it is a parameter so tests can make it fail.
fn create_in(
    dir: &File,
    dir_path: &Path,
    path: &Path,
    name: &OsStr,
    key: &[u8],
    write: impl FnOnce(&mut File, &[u8]) -> io::Result<()>,
) -> Result<(), PrivatePathError> {
    let tmp = temp_name(name);
    let tmp_path = dir_path.join(&tmp);
    let open_tmp = || {
        rustix::fs::openat(
            dir,
            &tmp,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
    };
    let fd = match open_tmp() {
        Err(e) if e == Errno::EXIST => {
            // Left by a crashed run with this process id and thread: ours, and never published.
            rustix::fs::unlinkat(dir, &tmp, AtFlags::empty())
                .map_err(|e| io_error(&tmp_path, e.into()))?;
            open_tmp()
        }
        other => other,
    }
    .map_err(|e| io_error(&tmp_path, e.into()))?;
    let mut file = File::from(fd);

    let published = write_temp(&mut file, &tmp_path, key, write).and_then(|()| {
        // linkat without AT_SYMLINK_FOLLOW: EEXIST for anything at `name`, a symlink included.
        rustix::fs::linkat(dir, &tmp, dir, name, AtFlags::empty()).map_err(|e| {
            if e == Errno::EXIST {
                PrivatePathError::AlreadyExists { path: path.into() }
            } else {
                io_error(path, e.into())
            }
        })
    });
    drop(file);
    let removed = rustix::fs::unlinkat(dir, &tmp, AtFlags::empty())
        .map_err(|e| io_error(&tmp_path, e.into()));
    published?;
    removed?;
    dir.sync_all().map_err(|e| io_error(dir_path, e))
}

/// Sets the mode of the fresh temporary file, checks it, writes the key and syncs the file.
fn write_temp(
    file: &mut File,
    tmp_path: &Path,
    key: &[u8],
    write: impl FnOnce(&mut File, &[u8]) -> io::Result<()>,
) -> Result<(), PrivatePathError> {
    file.set_permissions(Permissions::from_mode(KEY_FILE_MODE))
        .map_err(|e| io_error(tmp_path, e))?;
    let meta = file.metadata().map_err(|e| io_error(tmp_path, e))?;
    // A file system that ignores modes or owners must not get the key.
    check_owner_and_mode(Want::KeyFile, meta.uid(), meta.mode(), effective_uid())
        .map_err(|r| r.at(tmp_path))?;
    write(file, key).map_err(|e| io_error(tmp_path, e))?;
    file.sync_all().map_err(|e| io_error(tmp_path, e))
}

fn write_all(file: &mut File, key: &[u8]) -> io::Result<()> {
    file.write_all(key)
}

/// Reads and checks the key file `name` in the checked directory `dir`.
fn read_in<const N: usize>(
    dir: &File,
    path: &Path,
    name: &OsStr,
) -> Result<[u8; N], PrivatePathError> {
    let fd = rustix::fs::openat(
        dir,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::NOCTTY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| {
        if e == Errno::NOENT {
            PrivatePathError::NotFound { path: path.into() }
        } else if e == Errno::LOOP || e == Errno::MLINK {
            // Linux and Apple report a symlink under O_NOFOLLOW as ELOOP, FreeBSD as EMLINK.
            PrivatePathError::Symlink { path: path.into() }
        } else {
            io_error(path, e.into())
        }
    })?;
    let file = File::from(fd);
    let meta = file.metadata().map_err(|e| io_error(path, e))?;
    if !meta.file_type().is_file() {
        return Err(PrivatePathError::NotRegularFile { path: path.into() });
    }
    check_owner_and_mode(Want::KeyFile, meta.uid(), meta.mode(), effective_uid())
        .map_err(|r| r.at(path))?;
    let wrong_length = |found: u64| PrivatePathError::WrongLength {
        path: path.into(),
        found,
        expected: N,
    };
    let want = u64::try_from(N).unwrap_or(u64::MAX);
    if meta.len() != want {
        return Err(wrong_length(meta.len()));
    }
    let mut buf = Vec::with_capacity(N.saturating_add(1));
    // One byte more than a key, so a file that grew since the fstat is noticed.
    (&file)
        .take(want.saturating_add(1))
        .read_to_end(&mut buf)
        .map_err(|e| io_error(path, e))?;
    <[u8; N]>::try_from(buf.as_slice())
        .map_err(|_| wrong_length(u64::try_from(buf.len()).unwrap_or(u64::MAX)))
}

/// The temporary name for a key file being created: hidden, and unique among concurrent creators
/// (process id, and a hash of the thread id, which is never reused within a process).
fn temp_name(name: &OsStr) -> OsString {
    let mut hasher = DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    let mut tmp = OsString::from(".");
    tmp.push(name);
    tmp.push(format!(
        ".{}.{:016x}.tmp",
        std::process::id(),
        hasher.finish()
    ));
    tmp
}

/// Splits a key-file path into its directory and its file name.
fn split(path: &Path) -> Result<(&Path, &OsStr), PrivatePathError> {
    let name = path.file_name().ok_or_else(|| {
        io_error(
            path,
            io::Error::new(io::ErrorKind::InvalidInput, "not a path to a file"),
        )
    })?;
    Ok((parent_or_dot(path), name))
}

fn parent_or_dot(path: &Path) -> &Path {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    }
}

fn io_error(path: &Path, source: io::Error) -> PrivatePathError {
    PrivatePathError::Io {
        path: path.into(),
        source,
    }
}

fn effective_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}

/// What a path is expected to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Want {
    /// A key file: mode exactly 0600.
    KeyFile,
    /// A private directory: no group or other permission bits.
    PrivateDir,
}

/// Why [`check_owner_and_mode`] refused, before a path is attached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    Owner { owner: u32, expected: u32 },
    Mode { want: Want, mode: u32 },
}

impl Refusal {
    fn at(self, path: &Path) -> PrivatePathError {
        let path = path.into();
        match self {
            Self::Owner { owner, expected } => PrivatePathError::WrongOwner {
                path,
                owner,
                expected,
            },
            Self::Mode {
                want: Want::KeyFile,
                mode,
            } => PrivatePathError::KeyFileMode { path, mode },
            Self::Mode {
                want: Want::PrivateDir,
                mode,
            } => PrivatePathError::DirMode { path, mode },
        }
    }
}

/// The owner and mode rule, as a pure function of the file's uid, its `st_mode` and the effective
/// uid. The owner is checked first; root gets no exception.
fn check_owner_and_mode(want: Want, uid: u32, st_mode: u32, euid: u32) -> Result<(), Refusal> {
    if uid != euid {
        return Err(Refusal::Owner {
            owner: uid,
            expected: euid,
        });
    }
    let mode = st_mode & PERMISSION_BITS;
    let ok = match want {
        Want::KeyFile => mode == KEY_FILE_MODE,
        Want::PrivateDir => mode & GROUP_OTHER_BITS == 0,
    };
    if ok {
        Ok(())
    } else {
        Err(Refusal::Mode { want, mode })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::os::unix::fs::symlink;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::TempDir;

    const KEY: &[u8; 32] = b"MARKER-0123456789-abcdefghij-KEY";

    /// A temporary directory and a private directory inside it (the temporary directory itself
    /// may be 0755, depending on the umask).
    fn tmp() -> (TempDir, PathBuf) {
        let t = tempfile::tempdir().unwrap();
        let dir = t.path().join("p");
        ensure_private_dir(&dir).unwrap();
        (t, dir)
    }

    fn mode_of(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().mode() & PERMISSION_BITS
    }

    fn chmod(path: &Path, mode: u32) {
        fs::set_permissions(path, Permissions::from_mode(mode)).unwrap();
    }

    /// The names in a directory, sorted.
    fn entries(dir: &Path) -> Vec<OsString> {
        let mut names: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        names.sort();
        names
    }

    fn read32(path: &Path) -> Result<[u8; 32], PrivatePathError> {
        read_key_file::<32>(path)
    }

    #[test]
    fn created_file_is_0600_in_a_0700_dir_and_reads_back() {
        let (_t, base) = tmp();
        let dir = base.join("station");
        let path = dir.join("node.key");
        create_key_file(&path, KEY).unwrap();
        // Asserted on the result, whatever the process umask is.
        assert_eq!(mode_of(&path), 0o600);
        assert_eq!(mode_of(&dir), 0o700);
        assert_eq!(fs::read(&path).unwrap(), KEY);
        assert_eq!(&read32(&path).unwrap(), KEY);
        assert_eq!(entries(&dir), ["node.key"], "no temporary file is left");
    }

    #[test]
    fn first_run_creates_every_missing_level_0700_and_leaves_ancestors_alone() {
        let t = tempfile::tempdir().unwrap();
        let ancestor = t.path().join("home");
        fs::create_dir(&ancestor).unwrap();
        chmod(&ancestor, 0o755);
        let path = ancestor.join("a/b/c/node.key");
        create_key_file(&path, KEY).unwrap();
        for level in ["a", "a/b", "a/b/c"] {
            assert_eq!(mode_of(&ancestor.join(level)), 0o700, "{level}");
        }
        assert_eq!(
            mode_of(&ancestor),
            0o755,
            "an existing ancestor is not changed"
        );
        assert_eq!(&read32(&path).unwrap(), KEY);
    }

    #[test]
    fn existing_private_dir_is_checked_not_changed() {
        let (_t, base) = tmp();
        let dir = base.join("d");
        ensure_private_dir(&dir).unwrap();
        ensure_private_dir(&dir).unwrap();
        check_private_dir(&dir).unwrap();
        chmod(&dir, 0o500);
        ensure_private_dir(&dir).unwrap();
        assert_eq!(
            mode_of(&dir),
            0o500,
            "an existing dir is checked, not changed"
        );
        chmod(&dir, 0o700);
    }

    #[test]
    fn check_private_dir_creates_nothing() {
        let (_t, base) = tmp();
        let dir = base.join("a/b");
        let err = check_private_dir(&dir).unwrap_err();
        assert!(
            matches!(&err, PrivatePathError::NotFound { path } if *path == dir),
            "{err:?}"
        );
        assert!(!base.join("a").exists());
        fs::create_dir(base.join("a")).unwrap();
        chmod(&base.join("a"), 0o755);
        let err = check_private_dir(&base.join("a")).unwrap_err();
        assert!(
            matches!(err, PrivatePathError::DirMode { mode: 0o755, .. }),
            "{err:?}"
        );
        assert_eq!(mode_of(&base.join("a")), 0o755);
    }

    #[test]
    fn key_file_with_another_mode_is_refused() {
        let (_t, base) = tmp();
        let path = base.join("node.key");
        create_key_file(&path, KEY).unwrap();
        for mode in [0o644, 0o400, 0o640, 0o604, 0o700, 0o4600] {
            chmod(&path, mode);
            let err = read32(&path).unwrap_err();
            assert!(
                matches!(&err, PrivatePathError::KeyFileMode { mode: m, .. } if *m == mode),
                "{mode:o}: {err:?}"
            );
        }
        chmod(&path, 0o600);
        assert_eq!(&read32(&path).unwrap(), KEY);
    }

    #[test]
    fn dir_with_group_or_other_bits_is_refused() {
        let (_t, base) = tmp();
        let dir = base.join("d");
        fs::create_dir(&dir).unwrap();
        for mode in [0o755, 0o750, 0o701, 0o710, 0o707] {
            chmod(&dir, mode);
            for err in [
                ensure_private_dir(&dir).unwrap_err(),
                check_private_dir(&dir).unwrap_err(),
                create_key_file(&dir.join("node.key"), KEY).unwrap_err(),
                read32(&dir.join("node.key")).unwrap_err(),
            ] {
                assert!(
                    matches!(&err, PrivatePathError::DirMode { mode: m, .. } if *m == mode),
                    "{mode:o}: {err:?}"
                );
            }
            assert!(entries(&dir).is_empty());
        }
    }

    #[test]
    fn read_refuses_an_open_directory_and_a_missing_one_distinctly() {
        let (_t, base) = tmp();
        let dir = base.join("d");
        let path = dir.join("node.key");
        create_key_file(&path, KEY).unwrap();
        chmod(&dir, 0o755);
        let err = read32(&path).unwrap_err();
        assert!(matches!(err, PrivatePathError::DirMode { .. }), "{err:?}");
        chmod(&dir, 0o700);

        let missing_dir = base.join("none");
        let err = read32(&missing_dir.join("node.key")).unwrap_err();
        assert!(
            matches!(&err, PrivatePathError::NotFound { path } if *path == missing_dir),
            "{err:?}"
        );
        assert!(!missing_dir.exists(), "a read creates no directory");

        let missing_key = dir.join("other.key");
        let err = read32(&missing_key).unwrap_err();
        assert!(
            matches!(&err, PrivatePathError::NotFound { path } if *path == missing_key),
            "{err:?}"
        );
    }

    #[test]
    fn second_create_fails_and_leaves_no_temporary_file() {
        let (_t, base) = tmp();
        let path = base.join("node.key");
        create_key_file(&path, KEY).unwrap();
        let err = create_key_file(&path, b"another key of other length").unwrap_err();
        assert!(
            matches!(err, PrivatePathError::AlreadyExists { .. }),
            "{err:?}"
        );
        assert_eq!(fs::read(&path).unwrap(), KEY);
        assert_eq!(mode_of(&path), 0o600);
        assert_eq!(entries(&base), ["node.key"]);
    }

    #[test]
    fn stale_temporary_files_do_not_block_a_create() {
        let (_t, base) = tmp();
        let name = OsStr::new("node.key");
        // One left by a crashed run of this process and thread, one by another process.
        let ours = base.join(temp_name(name));
        let foreign = base.join(".node.key.1.0000000000000000.tmp");
        for stale in [&ours, &foreign] {
            fs::write(stale, b"partial").unwrap();
        }
        let path = base.join(name);
        create_key_file(&path, KEY).unwrap();
        assert_eq!(&read32(&path).unwrap(), KEY);
        assert!(!ours.exists(), "our own leftover is replaced and removed");
        assert_eq!(
            fs::read(&foreign).unwrap(),
            b"partial",
            "another's is left alone"
        );
    }

    #[test]
    fn symlink_at_the_path_is_refused_by_create_and_read() {
        let (_t, dir) = tmp();
        let real = dir.join("real.key");
        create_key_file(&real, KEY).unwrap();
        let link = dir.join("node.key");
        symlink(&real, &link).unwrap();
        let err = read32(&link).unwrap_err();
        assert!(matches!(err, PrivatePathError::Symlink { .. }), "{err:?}");
        let err = create_key_file(&link, b"new").unwrap_err();
        assert!(
            matches!(err, PrivatePathError::AlreadyExists { .. }),
            "{err:?}"
        );
        assert_eq!(fs::read(&real).unwrap(), KEY, "the target is untouched");

        // A dangling link is not followed either: nothing is created at its target.
        let target = dir.join("target.key");
        let dangling = dir.join("dangling.key");
        symlink(&target, &dangling).unwrap();
        let err = create_key_file(&dangling, KEY).unwrap_err();
        assert!(
            matches!(err, PrivatePathError::AlreadyExists { .. }),
            "{err:?}"
        );
        assert!(!target.exists());
        let err = read32(&dangling).unwrap_err();
        assert!(matches!(err, PrivatePathError::Symlink { .. }), "{err:?}");
        let mut want = vec![
            OsString::from("dangling.key"),
            "node.key".into(),
            "real.key".into(),
        ];
        want.sort();
        assert_eq!(entries(&dir), want, "no temporary file is left");
    }

    #[test]
    fn key_goes_into_the_checked_dir_through_a_symlinked_dir() {
        let (_t, base) = tmp();
        let real = base.join("real");
        ensure_private_dir(&real).unwrap();
        let link = base.join("link");
        symlink(&real, &link).unwrap();
        let path = link.join("node.key");
        create_key_file(&path, KEY).unwrap();
        assert_eq!(entries(&real), ["node.key"]);
        assert_eq!(&read32(&path).unwrap(), KEY);
        assert_eq!(
            load_or_create_key::<32>(&path, || panic!("a key exists"))
                .unwrap()
                .origin,
            KeyOrigin::Loaded
        );
    }

    #[test]
    fn failed_write_is_cleaned_up_in_the_checked_dir() {
        let (_t, base) = tmp();
        let real = base.join("real");
        let decoy = base.join("decoy");
        ensure_private_dir(&real).unwrap();
        ensure_private_dir(&decoy).unwrap();
        let name = OsStr::new("node.key");
        // A file under the temporary name in the decoy: a cleanup by path after the swap below
        // would remove it.
        let decoy_tmp = decoy.join(temp_name(name));
        fs::write(&decoy_tmp, b"decoy").unwrap();
        let link = base.join("link");
        symlink(&real, &link).unwrap();
        let path = link.join(name);

        let dir = open_private_dir(&link, Create::IfMissing).unwrap();
        let err = create_in(&dir, &link, &path, name, KEY, |file, key| {
            file.write_all(&key[..5])?;
            // Point the path somewhere else while the create is in progress.
            fs::remove_file(&link)?;
            symlink(&decoy, &link)?;
            Err(io::Error::other("injected write failure"))
        })
        .unwrap_err();
        assert!(matches!(err, PrivatePathError::Io { .. }), "{err:?}");
        assert!(entries(&real).is_empty(), "the checked dir is cleaned up");
        assert_eq!(fs::read(&decoy_tmp).unwrap(), b"decoy");
        assert_eq!(entries(&decoy).len(), 1);
    }

    #[test]
    fn wrong_length_is_refused() {
        let (_t, base) = tmp();
        let path = base.join("node.key");
        create_key_file(&path, &KEY[..10]).unwrap();
        let err = read32(&path).unwrap_err();
        assert!(
            matches!(
                err,
                PrivatePathError::WrongLength {
                    found: 10,
                    expected: 32,
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(err.to_string().contains("damaged"));
        let err = read_key_file::<4>(&path).unwrap_err();
        assert!(matches!(
            err,
            PrivatePathError::WrongLength { found: 10, .. }
        ));
    }

    #[test]
    fn not_a_regular_file_is_refused() {
        let (_t, base) = tmp();
        let sub = base.join("node.key");
        fs::create_dir(&sub).unwrap();
        let err = read32(&sub).unwrap_err();
        assert!(
            matches!(err, PrivatePathError::NotRegularFile { .. }),
            "{err:?}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn fifo_is_refused_without_blocking() {
        let (_t, base) = tmp();
        let fifo = base.join("node.key");
        rustix::fs::mknodat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::FileType::Fifo,
            Mode::RUSR | Mode::WUSR,
            0,
        )
        .unwrap();
        let err = read32(&fifo).unwrap_err();
        assert!(
            matches!(err, PrivatePathError::NotRegularFile { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn parent_that_is_a_regular_file_fails_cleanly() {
        let (_t, base) = tmp();
        let file = base.join("plain");
        fs::write(&file, b"x").unwrap();
        let err = create_key_file(&file.join("node.key"), KEY).unwrap_err();
        assert!(
            matches!(err, PrivatePathError::NotDirectory { .. }),
            "{err:?}"
        );
        let err = ensure_private_dir(&file).unwrap_err();
        assert!(
            matches!(err, PrivatePathError::NotDirectory { .. }),
            "{err:?}"
        );
        let err = create_key_file(&file.join("sub/node.key"), KEY).unwrap_err();
        assert!(matches!(err, PrivatePathError::Io { .. }), "{err:?}");
        let err = read32(&file.join("node.key")).unwrap_err();
        assert!(
            matches!(err, PrivatePathError::NotDirectory { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn path_without_a_file_name_is_refused() {
        let err = create_key_file(Path::new("/"), KEY).unwrap_err();
        assert!(matches!(err, PrivatePathError::Io { .. }), "{err:?}");
        let err = read32(Path::new("/")).unwrap_err();
        assert!(matches!(err, PrivatePathError::Io { .. }), "{err:?}");
    }

    #[test]
    fn owner_and_mode_rule() {
        use Want::{KeyFile, PrivateDir};
        const FILE: u32 = 0o100_000;
        const DIR: u32 = 0o040_000;
        assert_eq!(
            check_owner_and_mode(KeyFile, 1000, FILE | 0o600, 1000),
            Ok(())
        );
        assert_eq!(check_owner_and_mode(KeyFile, 0, FILE | 0o600, 0), Ok(()));
        // A different owner is refused, also with a correct mode, and also for root.
        assert_eq!(
            check_owner_and_mode(KeyFile, 1001, FILE | 0o600, 1000),
            Err(Refusal::Owner {
                owner: 1001,
                expected: 1000
            })
        );
        assert_eq!(
            check_owner_and_mode(KeyFile, 1000, FILE | 0o600, 0),
            Err(Refusal::Owner {
                owner: 1000,
                expected: 0
            })
        );
        assert!(matches!(
            check_owner_and_mode(PrivateDir, 0, DIR | 0o700, 1000),
            Err(Refusal::Owner { .. })
        ));
        // The owner is checked before the mode.
        assert!(matches!(
            check_owner_and_mode(KeyFile, 2, FILE | 0o644, 1),
            Err(Refusal::Owner { .. })
        ));
        for mode in [0o400, 0o644, 0o640, 0o660, 0o700, 0o2600, 0o1600] {
            assert_eq!(
                check_owner_and_mode(KeyFile, 7, FILE | mode, 7),
                Err(Refusal::Mode {
                    want: KeyFile,
                    mode
                })
            );
        }
        for mode in [0o700, 0o500, 0o300, 0o000] {
            assert_eq!(check_owner_and_mode(PrivateDir, 7, DIR | mode, 7), Ok(()));
        }
        for mode in [0o755, 0o750, 0o710, 0o701, 0o770] {
            assert_eq!(
                check_owner_and_mode(PrivateDir, 7, DIR | mode, 7),
                Err(Refusal::Mode {
                    want: PrivateDir,
                    mode
                })
            );
        }
    }

    /// A real refusal by owner: as a normal user through files root owns, as root by handing a
    /// file and a directory to another uid.
    #[test]
    fn another_owner_is_refused_for_real() {
        let euid = effective_uid();
        if euid != 0 {
            // The owner is checked before the mode, so root's 0755 /etc is refused for its owner.
            let err = read32(Path::new("/etc/passwd")).unwrap_err();
            assert!(
                matches!(err, PrivatePathError::WrongOwner { owner: 0, expected, .. } if expected == euid),
                "{err:?}"
            );
            let err = check_private_dir(Path::new("/")).unwrap_err();
            assert!(
                matches!(err, PrivatePathError::WrongOwner { owner: 0, .. }),
                "{err:?}"
            );
        } else {
            let (_t, base) = tmp();
            let dir = base.join("d");
            let path = dir.join("node.key");
            create_key_file(&path, KEY).unwrap();
            std::os::unix::fs::chown(&path, Some(65534), None).unwrap();
            let err = read32(&path).unwrap_err();
            assert!(
                matches!(
                    err,
                    PrivatePathError::WrongOwner {
                        owner: 65534,
                        expected: 0,
                        ..
                    }
                ),
                "{err:?}"
            );
            std::os::unix::fs::chown(&dir, Some(65534), None).unwrap();
            let err = check_private_dir(&dir).unwrap_err();
            assert!(
                matches!(err, PrivatePathError::WrongOwner { owner: 65534, .. }),
                "{err:?}"
            );
        }
    }

    #[test]
    fn errors_never_show_key_bytes() {
        let marker = std::str::from_utf8(KEY).unwrap();
        let (t, dir) = tmp();
        let path = dir.join("node.key");
        create_key_file(&path, KEY).unwrap();

        let mut errors = vec![
            create_key_file(&path, KEY).unwrap_err(),
            read_key_file::<33>(&path).unwrap_err(),
            read_key_file::<3>(&path).unwrap_err(),
            read32(&dir.join("missing")).unwrap_err(),
            ensure_private_dir(&path).unwrap_err(),
            create_key_file(&path.join("sub/x"), KEY).unwrap_err(),
        ];
        let link = dir.join("link.key");
        symlink(&path, &link).unwrap();
        errors.push(read32(&link).unwrap_err());
        let sub = dir.join("sub");
        fs::create_dir(&sub).unwrap();
        errors.push(read32(&sub).unwrap_err());
        chmod(&path, 0o644);
        errors.push(read32(&path).unwrap_err());
        chmod(&path, 0o600);
        if effective_uid() == 0 {
            std::os::unix::fs::chown(&path, Some(65534), None).unwrap();
            errors.push(read32(&path).unwrap_err());
            std::os::unix::fs::chown(&path, Some(0), None).unwrap();
        } else {
            errors.push(read32(Path::new("/etc/passwd")).unwrap_err());
        }
        chmod(&dir, 0o755);
        errors.push(check_private_dir(&dir).unwrap_err());
        chmod(&dir, 0o700);

        let mut kinds = HashSet::new();
        for e in &errors {
            let text = format!("{e} | {e:?} | {e:#?}");
            assert!(!text.contains(marker), "key bytes leaked: {text}");
            assert!(
                !text.contains(&format!("{:?}", &KEY[..8])),
                "key bytes leaked: {text}"
            );
            assert!(text.contains('/'), "no path: {text}");
            kinds.insert(std::mem::discriminant(e));
        }
        // Every variant was produced at least once.
        assert_eq!(kinds.len(), 10, "{errors:#?}");
        drop(t);

        let stored = StoredKey {
            key: *KEY,
            origin: KeyOrigin::Loaded,
        };
        let text = format!("{stored:?} {stored:#?}");
        assert!(!text.contains(marker) && !text.contains("77, 65"), "{text}");
    }

    #[test]
    fn readers_never_see_a_partial_key() {
        for round in 0..100u8 {
            let (_t, base) = tmp();
            let path = base.join("d/node.key");
            let done = AtomicBool::new(false);
            let keys = [[round; 32], [round.wrapping_add(128); 32]];
            std::thread::scope(|s| {
                let reader = s.spawn(|| {
                    loop {
                        let finished = done.load(Ordering::SeqCst);
                        match read32(&path) {
                            Ok(key) => {
                                assert!(keys.contains(&key), "a mixed or partial key");
                                break;
                            }
                            Err(PrivatePathError::NotFound { .. }) if !finished => {}
                            Err(e) => panic!("round {round}: {e:?}"),
                        }
                    }
                });
                let creators: Vec<_> = keys
                    .iter()
                    .map(|key| s.spawn(|| create_key_file(&path, key)))
                    .collect();
                let results: Vec<_> = creators.into_iter().map(|c| c.join().unwrap()).collect();
                done.store(true, Ordering::SeqCst);
                reader.join().unwrap();
                let won = results.iter().filter(|r| r.is_ok()).count();
                assert_eq!(won, 1, "{results:?}");
                assert!(
                    results
                        .iter()
                        .any(|r| matches!(r, Err(PrivatePathError::AlreadyExists { .. })))
                );
            });
            assert_eq!(entries(&base.join("d")), ["node.key"]);
        }
    }

    #[test]
    fn load_or_create_creates_once_then_loads() {
        let (_t, base) = tmp();
        let path = base.join("station/node.key");
        let first = load_or_create_key(&path, || *KEY).unwrap();
        assert_eq!(first.origin, KeyOrigin::Created);
        assert_eq!(&first.key, KEY);
        let second =
            load_or_create_key::<32>(&path, || panic!("generate called for an existing key"))
                .unwrap();
        assert_eq!(second.origin, KeyOrigin::Loaded);
        assert_eq!(second.key, first.key);
        assert_eq!(entries(&base.join("station")), ["node.key"]);
    }

    #[test]
    fn load_or_create_refuses_a_damaged_key() {
        let (_t, base) = tmp();
        let path = base.join("node.key");
        create_key_file(&path, &KEY[..7]).unwrap();
        let err = load_or_create_key::<32>(&path, || panic!("a damaged key must not be replaced"))
            .unwrap_err();
        assert!(
            matches!(err, PrivatePathError::WrongLength { found: 7, .. }),
            "{err:?}"
        );
        assert_eq!(fs::read(&path).unwrap(), &KEY[..7]);
    }

    #[test]
    fn racing_starters_end_up_with_the_same_key() {
        for round in 0..50u8 {
            let (_t, base) = tmp();
            let path = base.join("d/node.key");
            let (a, b) = std::thread::scope(|s| {
                let a = s.spawn(|| load_or_create_key(&path, || [round; 32]));
                let b = s.spawn(|| load_or_create_key(&path, || [round.wrapping_add(1); 32]));
                (a.join().unwrap().unwrap(), b.join().unwrap().unwrap())
            });
            assert_eq!(a.key, b.key, "round {round}");
            let created = [a.origin, b.origin]
                .iter()
                .filter(|o| **o == KeyOrigin::Created)
                .count();
            assert_eq!(created, 1, "round {round}: {a:?} {b:?}");
        }
    }
}
