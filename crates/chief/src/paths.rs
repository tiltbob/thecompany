//! Where a station keeps its data and where its socket is, as pure functions of a few values.
//!
//! The station daemon and the `chief` CLI share these, so both sides agree on the socket path
//! without talking. Every function takes its inputs as arguments; [`PathEnv::read`] is the only
//! place in this crate that reads the environment, and only binaries call it.
//!
//! - The data dir defaults to `$XDG_DATA_HOME/thecompany/station` ([`default_data_dir`]).
//! - The socket is `$XDG_RUNTIME_DIR/thecompany/station.sock` for the default data dir when
//!   `XDG_RUNTIME_DIR` is set, otherwise `<data-dir>/station.sock` ([`socket_path`]). A frontend's
//!   `--station PATH` overrides it; that choice belongs to the frontend, not to this module.
//! - A path too long for a Unix socket address is refused ([`check_socket_path`]).

use crate::api::ApiError;
use std::path::{Path, PathBuf};

/// The data dir below `$XDG_DATA_HOME` (or `$HOME/.local/share`).
pub const DATA_DIR_SUBPATH: &str = "thecompany/station";

/// The directory below `$XDG_RUNTIME_DIR` that holds the socket of the default data dir.
pub const RUNTIME_SUBDIR: &str = "thecompany";

/// The socket's file name, in the runtime directory or in the data dir.
pub const SOCKET_FILE: &str = "station.sock";

/// The longest socket path, in bytes, that fits a Unix socket address with its terminating NUL:
/// `sun_path` holds 108 bytes on Linux and Android, 104 on Apple systems and the BSDs.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub const SOCKET_PATH_MAX_BYTES: usize = 107;

/// The longest socket path, in bytes, that fits a Unix socket address with its terminating NUL:
/// `sun_path` holds 108 bytes on Linux and Android, 104 on Apple systems and the BSDs.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub const SOCKET_PATH_MAX_BYTES: usize = 103;

/// The station's default data dir: `$XDG_DATA_HOME/thecompany/station`.
///
/// An unset, empty or relative `XDG_DATA_HOME` is ignored (the XDG Base Directory specification
/// says so) and `$HOME/.local/share` is used instead. `None` when that does not work either: no
/// `HOME`, or an empty or relative one.
pub fn default_data_dir(xdg_data_home: Option<&Path>, home: Option<&Path>) -> Option<PathBuf> {
    let base = match absolute(xdg_data_home) {
        Some(data_home) => data_home.to_path_buf(),
        None => absolute(home)?.join(".local/share"),
    };
    Some(base.join(DATA_DIR_SUBPATH))
}

/// The station's socket for `data_dir`.
///
/// `$XDG_RUNTIME_DIR/thecompany/station.sock` when `data_dir` is the default data dir and
/// `xdg_runtime_dir` is set and absolute; otherwise `<data_dir>/station.sock`. So a station with
/// its own `--data-dir` has its own socket, and two stations never share one.
///
/// `data_dir` is compared with `default_data_dir` by path components, without touching the file
/// system: a trailing slash makes no difference, a symlink does.
pub fn socket_path(
    data_dir: &Path,
    default_data_dir: Option<&Path>,
    xdg_runtime_dir: Option<&Path>,
) -> PathBuf {
    match absolute(xdg_runtime_dir) {
        Some(runtime) if default_data_dir == Some(data_dir) => {
            runtime.join(RUNTIME_SUBDIR).join(SOCKET_FILE)
        }
        _ => data_dir.join(SOCKET_FILE),
    }
}

/// Refuses a socket path that does not fit a Unix socket address: longer than
/// [`SOCKET_PATH_MAX_BYTES`], or empty.
///
/// Fails with [`ApiError::Invalid`] with field `socket` and a reason that names `--data-dir`.
pub fn check_socket_path(path: &Path) -> Result<(), ApiError> {
    let len = path.as_os_str().len();
    let reason = if len == 0 {
        "the socket path is empty; pass a --data-dir (or --station PATH)".to_owned()
    } else if len > SOCKET_PATH_MAX_BYTES {
        format!(
            "{} is {len} bytes long, but a Unix socket path can be at most \
             {SOCKET_PATH_MAX_BYTES} bytes here; choose a shorter --data-dir (or --station PATH)",
            path.display()
        )
    } else {
        return Ok(());
    };
    Err(ApiError::Invalid {
        field: "socket".to_owned(),
        reason,
    })
}

/// The environment variables this module's functions take, as read by [`PathEnv::read`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PathEnv {
    /// `XDG_DATA_HOME`.
    pub xdg_data_home: Option<PathBuf>,
    /// `XDG_RUNTIME_DIR`.
    pub xdg_runtime_dir: Option<PathBuf>,
    /// `HOME`.
    pub home: Option<PathBuf>,
}

impl PathEnv {
    /// Reads `XDG_DATA_HOME`, `XDG_RUNTIME_DIR` and `HOME` from the environment of this process,
    /// now. The only place in this crate that reads the environment: binaries call it once and pass
    /// the values on; libraries and tests build a `PathEnv` by hand.
    pub fn read() -> Self {
        let var = |name: &str| std::env::var_os(name).map(PathBuf::from);
        Self {
            xdg_data_home: var("XDG_DATA_HOME"),
            xdg_runtime_dir: var("XDG_RUNTIME_DIR"),
            home: var("HOME"),
        }
    }

    /// [`default_data_dir`] with these values.
    pub fn default_data_dir(&self) -> Option<PathBuf> {
        default_data_dir(self.xdg_data_home.as_deref(), self.home.as_deref())
    }

    /// [`socket_path`] for `data_dir` with these values.
    pub fn socket_path(&self, data_dir: &Path) -> PathBuf {
        socket_path(
            data_dir,
            self.default_data_dir().as_deref(),
            self.xdg_runtime_dir.as_deref(),
        )
    }
}

/// `path` if it is set and absolute (an empty path is not).
fn absolute(path: Option<&Path>) -> Option<&Path> {
    path.filter(|p| p.is_absolute())
}
