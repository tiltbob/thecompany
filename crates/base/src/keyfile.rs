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
use std::path::{Component, Path, PathBuf};

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
const DIRECTORY_DESCRIPTOR_LABEL: &str = "<directory descriptor>";

/// Why a private directory or a key file was refused or could not be used.
///
/// Errors identify the path involved. For [`load_or_create_key_in`], they identify a bare
/// filename or the directory descriptor. No variant holds key bytes.
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

/// Whether [`load_or_create_key`] or [`load_or_create_key_in`] created the key or found it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOrigin {
    /// This call generated the key and published it: the first run.
    Created,
    /// The key already existed (possibly published by a concurrent starter).
    Loaded,
}

/// A key returned by [`load_or_create_key`] or [`load_or_create_key_in`]. Its `Debug` output never
/// shows the key.
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
///
/// The temporary file gets a hidden staging name, `.<name>.<pid>.<thread hash>.<n>.tmp`; a name
/// that is taken is skipped (16 tries, then an error naming the directory), and only the entry
/// this call created is ever removed. A staging file left by a crash therefore stays behind: it is
/// a 0600 file in the private directory, it is never read as a key, it does not block a later
/// create, and it can be deleted. Needs a file system with hard links.
pub fn create_key_file(path: &Path, key: &[u8]) -> Result<(), PrivatePathError> {
    let (dir_path, name) = split(path)?;
    let dir = open_private_dir(dir_path, Create::IfMissing)?;
    create_in(&dir, Some(dir_path), path, name, key, write_all)
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
    load_or_create_in(&dir, Some(dir_path), path, name, generate)
}

/// Loads or creates a key inside an already opened directory. Use this when another operation,
/// such as locking the station's data directory, is tied to that same directory descriptor.
///
/// `dir` must be a directory owned by the effective user with no group or other permission bits.
/// Open it for reading so it can be synced. If the key is missing, the directory is synced before
/// calling `generate`, and again after publication so the new entry survives a crash. A later I/O
/// error can still leave a complete key behind, as in [`create_key_file`].
///
/// `name` must be one bare filename without separators or NUL bytes. All file operations use `dir`,
/// even if the path used to open it has since changed. Errors identify only a bare filename or the
/// directory descriptor.
///
/// Key checks, atomic publication and generator behavior are as in [`load_or_create_key`].
pub fn load_or_create_key_in<const N: usize>(
    dir: &File,
    name: &OsStr,
    generate: impl FnOnce() -> [u8; N],
) -> Result<StoredKey<N>, PrivatePathError> {
    let path = Path::new(name);
    let dir_label = Path::new(DIRECTORY_DESCRIPTOR_LABEL);
    // Compare the raw name too, since `components` normalizes trailing separators and dots.
    if !matches!(path.components().next(), Some(Component::Normal(part)) if part == name)
        || name.as_encoded_bytes().contains(&0)
    {
        return Err(io_error(
            dir_label,
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "key name must be one filename without separators or NUL bytes",
            ),
        ));
    }

    check_dir(dir, dir_label)?;
    load_or_create_in(dir, None, path, name, generate)
}

fn load_or_create_in<const N: usize>(
    dir: &File,
    dir_path: Option<&Path>,
    path: &Path,
    name: &OsStr,
    generate: impl FnOnce() -> [u8; N],
) -> Result<StoredKey<N>, PrivatePathError> {
    let loaded = |key| StoredKey {
        key,
        origin: KeyOrigin::Loaded,
    };
    match read_in(dir, path, name) {
        Ok(key) => return Ok(loaded(key)),
        Err(PrivatePathError::NotFound { .. }) => {}
        Err(e) => return Err(e),
    }
    // A caller-supplied descriptor can allow relative opens yet fail to sync, for example O_PATH.
    if dir_path.is_none() {
        dir.sync_all()
            .map_err(|error| io_error(Path::new(DIRECTORY_DESCRIPTOR_LABEL), error))?;
    }
    let key = generate();
    match create_in(dir, dir_path, path, name, &key, write_all) {
        Ok(()) => Ok(StoredKey {
            key,
            origin: KeyOrigin::Created,
        }),
        Err(PrivatePathError::AlreadyExists { .. }) => read_in(dir, path, name).map(loaded),
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
        parent = create_level(&parent, &parent_path, name, Mode::RWXU)?;
        parent_path = path;
    }
    Ok(parent)
}

/// Creates the directory `name` in `parent` (or finds it, if it appeared concurrently), makes its
/// mode exactly 0700 if this call created it, checks it and returns it open. `mkdir_mode` is the
/// mode passed to `mkdirat`: 0700 in production; tests pass narrower modes to stand in for a
/// umask, which is process-global and cannot be changed in a test.
fn create_level(
    parent: &File,
    parent_path: &Path,
    name: &OsStr,
    mkdir_mode: Mode,
) -> Result<File, PrivatePathError> {
    let path = parent_path.join(name);
    let created = match rustix::fs::mkdirat(parent, name, mkdir_mode) {
        Ok(()) => true,
        Err(e) if e == Errno::EXIST => false,
        Err(e) => return Err(io_error(&path, e.into())),
    };
    let open = || {
        rustix::fs::openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
    };
    let opened = match open() {
        Err(e) if e == Errno::ACCESS && created => {
            // A umask that strips the owner's read bit (0277, 0777) left a level we cannot open.
            // Only in this case the mode is set by name, before the owner can be checked, on an
            // entry this call created a moment earlier; the precondition that nobody else can
            // write the ancestors (see the module docs) covers the gap. Apple honours
            // AT_SYMLINK_NOFOLLOW here; Linux does not offer it for fchmodat, so it follows.
            let nofollow = rustix::fs::chmodat(parent, name, Mode::RWXU, AtFlags::SYMLINK_NOFOLLOW);
            match nofollow {
                Err(e) if e == Errno::OPNOTSUPP || e == Errno::NOTSUP || e == Errno::INVAL => {
                    rustix::fs::chmodat(parent, name, Mode::RWXU, AtFlags::empty())
                }
                other => other,
            }
            .map_err(|e| io_error(&path, e.into()))?;
            open()
        }
        other => other,
    };
    let fd = opened.map_err(|e| {
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
        parent.sync_all().map_err(|e| io_error(parent_path, e))?;
    }
    // A level that appeared concurrently is checked like any existing directory.
    check_dir(&handle, &path)?;
    Ok(handle)
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
    dir_path: Option<&Path>,
    path: &Path,
    name: &OsStr,
    key: &[u8],
    write: impl FnOnce(&mut File, &[u8]) -> io::Result<()>,
) -> Result<(), PrivatePathError> {
    // Include the directory path in diagnostics when the caller supplied one.
    let dir_label = dir_path.unwrap_or(Path::new(DIRECTORY_DESCRIPTOR_LABEL));
    let stage_label = |staged: &OsStr| match dir_path {
        Some(dir_path) => dir_path.join(staged),
        None => PathBuf::from(staged),
    };
    // An entry under a staging name proves nothing about who made it (any file name is a valid key
    // name), so a taken name is skipped, never removed. Only the name this call created is
    // unlinked below.
    let mut staged = None;
    for candidate in staging_names(name) {
        match rustix::fs::openat(
            dir,
            &candidate,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        ) {
            Ok(fd) => {
                staged = Some((candidate, fd));
                break;
            }
            Err(e) if e == Errno::EXIST => {}
            Err(e) => return Err(io_error(&stage_label(&candidate), e.into())),
        }
    }
    let Some((tmp, fd)) = staged else {
        return Err(io_error(
            dir_label,
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "all {STAGING_ATTEMPTS} staging names for {} are taken; remove leftover \
                     staging files (.{}.<pid>.<thread>.<n>.tmp) from this directory",
                    name.to_string_lossy(),
                    name.to_string_lossy(),
                ),
            ),
        ));
    };
    let tmp_path = stage_label(&tmp);
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
    dir.sync_all().map_err(|e| io_error(dir_label, e))
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

/// How many staging names [`create_in`] tries before it gives up.
const STAGING_ATTEMPTS: usize = 16;

/// The staging names for a key file being created, in the order they are tried: hidden, and
/// distinct among concurrent creators (process id, and a hash of the thread id, which is never
/// reused within a process), with a counter for names that are taken.
fn staging_names(name: &OsStr) -> impl Iterator<Item = OsString> + '_ {
    let mut hasher = DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    let (pid, thread) = (std::process::id(), hasher.finish());
    (0..STAGING_ATTEMPTS).map(move |n| {
        let mut tmp = OsString::from(".");
        tmp.push(name);
        tmp.push(format!(".{pid}.{thread:016x}.{n}.tmp"));
        tmp
    })
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
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::symlink;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;
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

    /// The staging name this thread would try first for `name`.
    fn first_staging_name(name: &str) -> OsString {
        staging_names(OsStr::new(name)).next().unwrap()
    }

    #[test]
    fn stale_staging_files_do_not_block_a_create_and_are_left_alone() {
        let (_t, base) = tmp();
        // One under the name this thread tries first, one left by another process.
        let ours = base.join(first_staging_name("node.key"));
        let foreign = base.join(".node.key.1.0000000000000000.0.tmp");
        for stale in [&ours, &foreign] {
            fs::write(stale, b"partial").unwrap();
        }
        let path = base.join("node.key");
        create_key_file(&path, KEY).unwrap();
        assert_eq!(&read32(&path).unwrap(), KEY);
        for stale in [&ours, &foreign] {
            assert_eq!(fs::read(stale).unwrap(), b"partial", "{}", stale.display());
        }
        assert_eq!(entries(&base).len(), 3, "the key and the two leftovers");
    }

    #[test]
    fn a_real_key_under_a_staging_name_survives_a_create() {
        // Any file name is a valid key name, also one that equals a staging name.
        let (_t, base) = tmp();
        let staging = base.join(first_staging_name("node.key"));
        create_key_file(&staging, KEY).unwrap();
        assert_eq!(&read32(&staging).unwrap(), KEY);

        let other = [7u8; 32];
        let path = base.join("node.key");
        create_key_file(&path, &other).unwrap();
        assert_eq!(read32(&path).unwrap(), other);
        assert_eq!(
            &read32(&staging).unwrap(),
            KEY,
            "the first key must survive"
        );
        let mut want = vec![OsString::from("node.key"), first_staging_name("node.key")];
        want.sort();
        assert_eq!(entries(&base), want, "no staging entry is left");
    }

    #[test]
    fn all_staging_names_taken_is_a_clean_error_that_removes_nothing() {
        let (_t, base) = tmp();
        let taken: Vec<_> = staging_names(OsStr::new("node.key"))
            .map(|n| base.join(n))
            .collect();
        assert_eq!(taken.len(), STAGING_ATTEMPTS);
        for t in &taken {
            fs::write(t, b"taken").unwrap();
        }
        let err = create_key_file(&base.join("node.key"), KEY).unwrap_err();
        assert!(
            matches!(&err, PrivatePathError::Io { path, source }
                if *path == base && source.kind() == io::ErrorKind::AlreadyExists),
            "{err:?}"
        );
        assert!(err.to_string().contains("staging"), "{err}");

        let dir = File::open(&base).unwrap();
        let err = load_or_create_key_in(&dir, OsStr::new("node.key"), || *KEY).unwrap_err();
        assert!(matches!(
            &err,
            PrivatePathError::Io { path, source }
                if path == Path::new("<directory descriptor>")
                    && source.kind() == io::ErrorKind::AlreadyExists
        ));
        assert!(!err.to_string().contains(base.to_string_lossy().as_ref()));
        assert!(!base.join("node.key").exists());
        for t in &taken {
            assert_eq!(fs::read(t).unwrap(), b"taken");
        }
        assert_eq!(
            entries(&base).len(),
            STAGING_ATTEMPTS,
            "nothing added or removed"
        );
    }

    #[test]
    fn a_level_created_under_a_narrow_umask_ends_up_usable_0700() {
        // The mkdir mode stands in for the umask; as a normal user 0o000 and 0o300 cannot be
        // opened for reading and take the fallback, 0o500 takes the normal path.
        let (_t, base) = tmp();
        let parent = open_private_dir(&base, Create::Never).unwrap();
        for (name, mode) in [
            ("m000", Mode::empty()),
            ("m300", Mode::WUSR | Mode::XUSR),
            ("m500", Mode::RUSR | Mode::XUSR),
        ] {
            let level = create_level(&parent, &base, OsStr::new(name), mode).unwrap();
            let dir = base.join(name);
            assert_eq!(mode_of(&dir), 0o700, "{name}");
            assert!(level.metadata().unwrap().is_dir());
            // Usable: a key can be created in it and read back.
            let path = dir.join("node.key");
            create_key_file(&path, KEY).unwrap();
            assert_eq!(&read32(&path).unwrap(), KEY, "{name}");
        }
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
        // A file under the staging name in the decoy: a cleanup by path after the swap below
        // would remove it.
        let decoy_tmp = decoy.join(first_staging_name("node.key"));
        fs::write(&decoy_tmp, b"decoy").unwrap();
        let link = base.join("link");
        symlink(&real, &link).unwrap();
        let path = link.join(name);

        let dir = open_private_dir(&link, Create::IfMissing).unwrap();
        let err = create_in(&dir, Some(&link), &path, name, KEY, |file, key| {
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
    fn directory_handle_keeps_key_operations_in_the_opened_directory() {
        let (_t, base) = tmp();
        let original = base.join("original");
        let moved = base.join("moved");
        let replacement = base.join("replacement");
        let alias = base.join("data");
        let existing_name = OsStr::new("existing.key");
        let new_name = OsStr::from_bytes(b"new-\xff.key");
        create_key_file(&original.join(existing_name), KEY).unwrap();
        create_key_file(&replacement.join(existing_name), &[0; 32]).unwrap();
        symlink(&original, &alias).unwrap();
        let dir = File::open(&alias).unwrap();

        // This fixture checks rename and retargeting of the original path. The absence of
        // descriptor-to-path reconstruction is an implementation property checked in review.
        fs::rename(&original, &moved).unwrap();
        fs::remove_file(&alias).unwrap();
        symlink(&replacement, &alias).unwrap();

        let existing = load_or_create_key_in::<32>(&dir, existing_name, || {
            panic!("the opened directory already has a key")
        })
        .unwrap();
        assert_eq!(existing.origin, KeyOrigin::Loaded);
        assert!(
            existing.key == *KEY,
            "loaded the key from the opened directory"
        );

        let created = load_or_create_key_in(&dir, new_name, || *KEY).unwrap();
        assert_eq!(created.origin, KeyOrigin::Created);
        assert!(read32(&moved.join(new_name)).unwrap() == created.key);
        assert_eq!(mode_of(&moved.join(new_name)), 0o600);
        assert_eq!(entries(&moved), [existing_name, new_name]);
        assert_eq!(entries(&replacement), [existing_name]);

        let loaded = load_or_create_key_in::<32>(&dir, new_name, || {
            panic!("the generated key already exists")
        })
        .unwrap();
        assert_eq!(loaded.origin, KeyOrigin::Loaded);
        assert!(loaded.key == created.key);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn unsyncable_directory_handle_refuses_creation_before_generation() {
        let (_t, base) = tmp();
        let dir = File::from(
            rustix::fs::open(
                &base,
                OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .unwrap(),
        );
        let name = OsStr::new("node.key");
        let error = load_or_create_key_in::<32>(&dir, name, || {
            panic!("a descriptor that cannot sync must fail before generation")
        })
        .unwrap_err();
        assert!(matches!(
            error,
            PrivatePathError::Io { path, source }
                if path == Path::new("<directory descriptor>")
                    && source.raw_os_error() == Some(Errno::BADF.raw_os_error())
        ));
        assert!(
            entries(&base).is_empty(),
            "no key or staging file was created"
        );

        create_key_file(&base.join(name), KEY).unwrap();
        let existing = load_or_create_key_in::<32>(&dir, name, || {
            panic!("loading the existing key needs no generator or sync")
        })
        .unwrap();
        assert_eq!(existing.origin, KeyOrigin::Loaded);
        assert!(existing.key == *KEY);
    }

    #[test]
    fn directory_handle_refuses_names_that_are_not_bare_filenames() {
        let (_t, base) = tmp();
        let dir = File::open(&base).unwrap();
        for name in [
            "",
            ".",
            "..",
            "/node.key",
            "../node.key",
            "sub/node.key",
            "./node.key",
            "node.key/.",
            "node.key/",
            "node.key\0ignored",
        ] {
            let error = load_or_create_key_in::<32>(&dir, OsStr::new(name), || {
                panic!("a bad filename must fail before generation")
            })
            .unwrap_err();
            assert!(matches!(
                error,
                PrivatePathError::Io { path, source }
                    if path == Path::new(DIRECTORY_DESCRIPTOR_LABEL)
                        && source.kind() == io::ErrorKind::InvalidInput
            ));
        }
        assert!(entries(&base).is_empty());
    }

    #[test]
    fn directory_handle_rechecks_type_owner_and_mode() {
        let (_t, base) = tmp();
        let dir = File::open(&base).unwrap();
        let regular = File::create(base.join("ordinary-file")).unwrap();
        let name = OsStr::new("node.key");
        let refused = |dir: &File| {
            load_or_create_key_in::<32>(dir, name, || {
                panic!("an unsafe directory must fail before generation")
            })
            .unwrap_err()
        };

        assert!(matches!(
            refused(&regular),
            PrivatePathError::NotDirectory { path } if path == Path::new(DIRECTORY_DESCRIPTOR_LABEL)
        ));
        chmod(&base, 0o750);
        assert!(matches!(
            refused(&dir),
            PrivatePathError::DirMode { mode: 0o750, path }
                if path == Path::new(DIRECTORY_DESCRIPTOR_LABEL)
        ));
        chmod(&base, 0o700);
        let wrong_owner = if effective_uid() == 0 {
            std::os::unix::fs::chown(&base, Some(65534), None).unwrap();
            let error = refused(&dir);
            std::os::unix::fs::chown(&base, Some(0), None).unwrap();
            error
        } else {
            refused(&File::open("/").unwrap())
        };
        assert!(matches!(
            wrong_owner,
            PrivatePathError::WrongOwner { path, .. } if path == Path::new(DIRECTORY_DESCRIPTOR_LABEL)
        ));
        assert_eq!(entries(&base), ["ordinary-file"]);
    }

    #[test]
    fn directory_handle_refuses_unsafe_and_damaged_key_files() {
        let (_t, base) = tmp();
        let dir = File::open(&base).unwrap();
        let name = OsStr::new("node.key");
        let path = base.join(name);
        create_key_file(&path, &KEY[..7]).unwrap();
        symlink(&path, base.join("link.key")).unwrap();
        let refused = |name| {
            load_or_create_key_in::<32>(&dir, name, || {
                panic!("an unsafe or damaged key must not be replaced")
            })
            .unwrap_err()
        };

        assert!(matches!(
            refused(OsStr::new("link.key")),
            PrivatePathError::Symlink { path } if path == Path::new("link.key")
        ));
        assert!(matches!(
            refused(name),
            PrivatePathError::WrongLength { path, found: 7, expected: 32 }
                if path == Path::new(name)
        ));
        chmod(&path, 0o640);
        assert!(matches!(
            refused(name),
            PrivatePathError::KeyFileMode { path, mode: 0o640 } if path == Path::new(name)
        ));
        assert!(fs::read(&path).unwrap() == KEY[..7]);
        assert_eq!(entries(&base), ["link.key", "node.key"]);
    }

    #[test]
    fn racing_directory_handle_calls_reuse_the_published_key() {
        let (_t, base) = tmp();
        let dir = File::open(&base).unwrap();
        let (a_ready, a_wait) = mpsc::channel();
        let (b_ready, b_wait) = mpsc::channel();
        let (a, b) = std::thread::scope(|scope| {
            let dir = &dir;
            let start = |key, ready: mpsc::Sender<()>, other: mpsc::Receiver<()>| {
                scope.spawn(move || {
                    load_or_create_key_in(dir, OsStr::new("node.key"), || {
                        // Both calls must observe the missing file before either can publish.
                        ready.send(()).unwrap();
                        other.recv_timeout(Duration::from_secs(5)).unwrap();
                        key
                    })
                    .unwrap()
                })
            };
            let a = start(*KEY, a_ready, b_wait);
            let b = start([0; 32], b_ready, a_wait);
            (a.join().unwrap(), b.join().unwrap())
        });
        assert!(a.key == b.key, "both calls return the published key");
        assert_ne!(a.origin, b.origin, "one call publishes and the other loads");
        assert_eq!(entries(&base), ["node.key"]);
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
