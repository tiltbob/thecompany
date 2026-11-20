//! The local-socket server: serves any [`Transport`] (the station core, or a fake in tests) to
//! frontends on a Unix socket.
//!
//! The protocol is in `chief::socket`: one connection per call or subscription, a hello first, then
//! one [`Open`] frame. Frames are capped at [`LOCAL_FRAME_MAX`] in both directions. Every frame of
//! a call, read or written, has a deadline ([`IpcConfig::frame_timeout`]): the hello, the hello
//! reply, the `Open` frame and the reply (also `Reply::Subscribed`). The event frames of an open
//! subscription never have one.
//!
//! # One server per socket
//!
//! [`IpcServer::bind`] takes an exclusive lock on `<socket path>.lock` before it touches the socket
//! path, and holds it as long as it serves. A second server for the same path fails with
//! [`IpcError::InUse`] and removes nothing, so a live socket is never taken away, also not by a
//! station with another data dir whose socket path happens to be the same.
//!
//! # Who may connect
//!
//! The socket's directory is private (mode 0700, owned by this user) and the socket has mode 0600.
//! On top of that, the server reads every client's uid with `UnixStream::peer_cred` and closes,
//! without a word, a connection from any other uid, root included.
//!
//! # A slow subscriber
//!
//! Each connection has a task of its own. A subscriber that does not read stalls only the write in
//! its own task: the transport's [`chief::client::EventSink::push`] never waits, so the station
//! goes on, and the subscriber gets [`chief::api::Event::Resync`] once its queue overflowed.
//!
//! Unix only. Emits no logs.

use chief::API_VERSION;
use chief::api::{ApiError, Request, Response};
use chief::client::{Subscription, Transport};
use chief::paths::check_socket_path;
use chief::socket::json_error_detail;
use chief::socket::wire::{Hello, HelloReply, Open, Reply};
use numbers_base::{FrameError, LOCAL_FRAME_MAX, PrivatePathError, read_frame, write_frame};
use rustix::fs::{Mode, OFlags};
use std::fmt;
use std::fs::{File, TryLockError};
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, oneshot};
use tokio::task::{JoinHandle, JoinSet};

/// The mode of the socket file: read and write for the owner only.
const SOCKET_MODE: u32 = 0o600;

/// The mode of the lock file next to the socket.
const LOCK_MODE: u32 = 0o600;

/// What is appended to the socket path to name its lock file.
pub const LOCK_SUFFIX: &str = ".lock";

/// How long the accept loop pauses after a failed `accept` (out of file descriptors, for example),
/// so that a persistent failure does not spin.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Settings of an [`IpcServer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpcConfig {
    /// How long the server waits for each frame of a call: reading the hello, writing the hello
    /// reply, reading the `Open` frame, and writing the reply (also `Reply::Subscribed` and
    /// `Reply::Err`). A client that is slower loses its connection. Never applies to the events of
    /// an open subscription: a subscriber that does not read gets `Resync` instead. Default 5 s.
    pub frame_timeout: Duration,
    /// How many connections the server serves at a time (at least 1). Further clients wait in the
    /// listen backlog until one ends. Every open subscription holds one. Default 64.
    pub max_connections: usize,
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            frame_timeout: Duration::from_secs(5),
            max_connections: 64,
        }
    }
}

/// Why [`IpcServer::bind`] failed. Each variant names the path, and its text says what to do.
#[derive(Debug)]
pub enum IpcError {
    /// The socket path does not fit a Unix socket address.
    PathTooLong {
        /// The socket path.
        path: PathBuf,
        /// Why, naming `--data-dir`.
        reason: String,
    },
    /// The socket's directory could not be created, or exists and is not private.
    Directory {
        /// The directory.
        path: PathBuf,
        /// What is wrong with it.
        source: PrivatePathError,
    },
    /// Another server holds the socket's lock: a station is already serving this socket. Nothing
    /// was touched.
    InUse {
        /// The socket path.
        path: PathBuf,
    },
    /// The socket's lock file could not be opened or locked (a symlink at its path, for example).
    Lock {
        /// The lock file's path.
        path: PathBuf,
        /// The error.
        source: io::Error,
    },
    /// Something that is not a socket is at the socket path. It was left alone.
    NotASocket {
        /// The socket path.
        path: PathBuf,
    },
    /// What is at the socket path could not be inspected or removed.
    Leftover {
        /// The socket path.
        path: PathBuf,
        /// The error.
        source: io::Error,
    },
    /// Binding the socket or setting its mode failed.
    Bind {
        /// The socket path.
        path: PathBuf,
        /// The error.
        source: io::Error,
    },
    /// The station did not answer the status call the server makes to learn its ids.
    Status {
        /// The socket path.
        path: PathBuf,
        /// The station's error.
        error: ApiError,
    },
}

impl fmt::Display for IpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PathTooLong { reason, .. } => write!(f, "cannot use the socket path: {reason}"),
            Self::Directory { path, source } => write!(
                f,
                "cannot use {} as the socket's directory: {source}",
                path.display()
            ),
            Self::InUse { path } => write!(
                f,
                "another station is already serving {}; a second station needs its own \
                 --data-dir (its socket then lives in that data dir)",
                path.display()
            ),
            Self::Lock { path, source } => write!(
                f,
                "cannot lock {}, which guards the socket next to it: {source}",
                path.display()
            ),
            Self::NotASocket { path } => write!(
                f,
                "{} exists and is not a socket; refusing to replace it. Move it away, or choose \
                 another --data-dir",
                path.display()
            ),
            Self::Leftover { path, source } => write!(
                f,
                "cannot remove the leftover socket {}: {source}",
                path.display()
            ),
            Self::Bind { path, source } => {
                write!(f, "cannot listen on {}: {source}", path.display())
            }
            Self::Status { path, error } => write!(
                f,
                "cannot serve {}: the station did not answer its status: {error}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for IpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Directory { source, .. } => Some(source),
            Self::Leftover { source, .. }
            | Self::Bind { source, .. }
            | Self::Lock { source, .. } => Some(source),
            Self::Status { error, .. } => Some(error),
            Self::PathTooLong { .. } | Self::NotASocket { .. } | Self::InUse { .. } => None,
        }
    }
}

/// A running socket server. [`IpcServer::shutdown`] stops it and removes the socket file.
///
/// Dropping it without `shutdown` also stops serving, but leaves the socket file behind; the next
/// [`IpcServer::bind`] replaces it. The accept loop is aborted, which happens on the runtime soon
/// after the drop rather than within it: the listener is closed first, then every connection is
/// aborted, then the socket lock is released. Until then a new `bind` on the same path fails with
/// [`IpcError::InUse`].
#[derive(Debug)]
pub struct IpcServer {
    path: PathBuf,
    /// The socket file's device and inode, so that shutdown removes only the file it bound.
    identity: (u64, u64),
    stop: Option<oneshot::Sender<()>>,
    /// The accept loop; it hands back the socket lock when it ends on [`IpcServer::shutdown`].
    task: Option<JoinHandle<Option<File>>>,
}

impl IpcServer {
    /// Checks or creates the socket directory, locks the socket, replaces a leftover socket, binds,
    /// sets mode 0600, and serves `transport` until [`IpcServer::shutdown`].
    ///
    /// In this order:
    /// 1. the path must fit a socket address ([`check_socket_path`]);
    /// 2. one [`Request::Status`] call on `transport` gives the station id and instance id for the
    ///    hello (if it fails, nothing is touched);
    /// 3. the parent directory goes through [`numbers_base::ensure_private_dir`] (created 0700, or
    ///    an existing one owned by this user with no group or other access);
    /// 4. the lock file `<socket path>.lock` is opened in that directory (created 0600, never
    ///    through a symlink) and locked exclusively; if another server holds it, `bind` fails with
    ///    [`IpcError::InUse`] and removes nothing. The lock is held as long as the server serves,
    ///    and the lock file is never deleted;
    /// 5. a socket left at the path is removed, and anything else there is refused and left alone;
    /// 6. the listener is bound and its file set to 0600.
    ///
    /// # Preconditions
    ///
    /// - The socket lock is what keeps a live socket from being taken away: a leftover socket is
    ///   removed only by the holder of the lock. The station still takes its data-dir lock before
    ///   it calls this (one station per data dir), but this function does not rely on it.
    /// - Every directory above the socket's directory is writable only by this user or by root
    ///   (see "What the checks do not cover" in `numbers_base::keyfile`): the directory is
    ///   checked, then used by path to lock and to bind.
    ///
    /// Must be called within a tokio runtime with I/O and time enabled; the server runs as tasks
    /// on it.
    pub async fn bind<T: Transport>(
        transport: T,
        socket: &Path,
        config: IpcConfig,
    ) -> Result<IpcServer, IpcError> {
        let path = socket.to_path_buf();
        check_socket_path(&path).map_err(|e| IpcError::PathTooLong {
            path: path.clone(),
            reason: match e {
                ApiError::Invalid { reason, .. } => reason,
                other => other.to_string(),
            },
        })?;
        let hello = match transport.call(Request::Status).await {
            Ok(Response::Status { status }) => HelloReply {
                api: API_VERSION,
                station_id: status.station_id,
                instance_id: status.instance_id,
            },
            Ok(_) => {
                return Err(IpcError::Status {
                    path,
                    error: ApiError::BadReply {
                        detail: "the station answered the status call with another kind of \
                                 response"
                            .to_owned(),
                    },
                });
            }
            Err(error) => return Err(IpcError::Status { path, error }),
        };

        let dir = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        numbers_base::ensure_private_dir(&dir).map_err(|source| IpcError::Directory {
            path: dir.clone(),
            source,
        })?;
        let lock = take_lock(&path)?;
        remove_leftover(&path)?;
        let listener = UnixListener::bind(&path).map_err(|source| IpcError::Bind {
            path: path.clone(),
            source,
        })?;
        // Until this runs the socket has the umask's mode; the private directory keeps everyone
        // else out meanwhile.
        let metadata =
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(SOCKET_MODE))
                .and_then(|()| std::fs::symlink_metadata(&path))
                .map_err(|source| IpcError::Bind {
                    path: path.clone(),
                    source,
                })?;
        let identity = (metadata.dev(), metadata.ino());

        let (stop, stopped) = oneshot::channel();
        let shared = Arc::new(Shared {
            transport,
            hello,
            config: IpcConfig {
                max_connections: config.max_connections.clamp(1, Semaphore::MAX_PERMITS),
                ..config
            },
            euid: rustix::process::geteuid().as_raw(),
        });
        let task = tokio::spawn(accept_loop(listener, lock, shared, stopped));
        Ok(IpcServer {
            path,
            identity,
            stop: Some(stop),
            task: Some(task),
        })
    }

    /// The socket's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Stops accepting, ends every connection (open subscriptions end as for a station that
    /// stopped: rule 11), removes the socket file if it is still the one this server bound, and
    /// then releases the socket lock.
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        // The accept loop ends its connections before it returns the lock; if it panicked, there
        // is nothing left to stop, and the lock went with it.
        let lock = match self.task.take() {
            Some(task) => task.await.ok().flatten(),
            None => None,
        };
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path)
            && metadata.file_type().is_socket()
            && (metadata.dev(), metadata.ino()) == self.identity
        {
            let _ = std::fs::remove_file(&self.path);
        }
        drop(lock);
    }
}

impl Drop for IpcServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// What every connection task needs.
struct Shared<T> {
    transport: T,
    hello: HelloReply,
    config: IpcConfig,
    euid: u32,
}

/// Opens `<socket>.lock` (created 0600, never through a symlink) and locks it exclusively.
fn take_lock(socket: &Path) -> Result<File, IpcError> {
    let mut name = socket.as_os_str().to_owned();
    name.push(LOCK_SUFFIX);
    let path = PathBuf::from(name);
    let failed = |source: io::Error| IpcError::Lock {
        path: path.clone(),
        source,
    };
    let fd = rustix::fs::open(
        &path,
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::from_raw_mode(LOCK_MODE),
    )
    .map_err(|e| failed(e.into()))?;
    let file = File::from(fd);
    let metadata = file.metadata().map_err(failed)?;
    if !metadata.file_type().is_file() {
        return Err(failed(io::Error::other("not a regular file")));
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err(failed(io::Error::other(format!(
            "owned by uid {}, not by this user",
            metadata.uid()
        ))));
    }
    if metadata.permissions().mode() & 0o7777 != LOCK_MODE {
        file.set_permissions(std::fs::Permissions::from_mode(LOCK_MODE))
            .map_err(failed)?;
    }
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(IpcError::InUse {
            path: socket.to_path_buf(),
        }),
        Err(TryLockError::Error(source)) => Err(failed(source)),
    }
}

/// Removes a socket left at `path`; refuses anything else there.
fn remove_leftover(path: &Path) -> Result<(), IpcError> {
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(IpcError::Leftover {
            path: path.to_path_buf(),
            source,
        }),
        Ok(metadata) if metadata.file_type().is_socket() => match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(IpcError::Leftover {
                path: path.to_path_buf(),
                source,
            }),
        },
        Ok(_) => Err(IpcError::NotASocket {
            path: path.to_path_buf(),
        }),
    }
}

/// The accept loop's resources. Fields drop in declaration order, so when the loop is aborted
/// (the [`IpcServer`] was dropped) the listener is closed before the connections are aborted, and
/// the socket lock is released last: a client that sees its connection end can no longer reach
/// this server, and a new server can lock the socket only once this one stopped listening.
struct Serving {
    listener: UnixListener,
    connections: JoinSet<()>,
    lock: File,
}

/// Accepts connections, at most `max_connections` at a time, until `stopped` fires (or the sender
/// is dropped); then closes the listener, aborts every connection task and waits for them.
/// Returns the socket lock, still held, so that [`IpcServer::shutdown`] releases it only after it
/// removed the socket file.
async fn accept_loop<T: Transport>(
    listener: UnixListener,
    lock: File,
    shared: Arc<Shared<T>>,
    mut stopped: oneshot::Receiver<()>,
) -> Option<File> {
    let slots = Arc::new(Semaphore::new(shared.config.max_connections));
    let mut serving = Serving {
        listener,
        connections: JoinSet::new(),
        lock,
    };
    loop {
        tokio::select! {
            biased;
            _ = &mut stopped => break,
            Some(_) = serving.connections.join_next() => {}
            slot = Arc::clone(&slots).acquire_owned() => {
                let Ok(slot) = slot else { break };
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    accepted = serving.listener.accept() => match accepted {
                        Ok((stream, _)) => {
                            let shared = Arc::clone(&shared);
                            serving.connections.spawn(async move {
                                serve(&shared, stream).await;
                                drop(slot);
                            });
                        }
                        Err(_) => tokio::time::sleep(ACCEPT_BACKOFF).await,
                    },
                }
            }
        }
    }
    let Serving {
        listener,
        mut connections,
        lock,
    } = serving;
    drop(listener);
    connections.shutdown().await;
    Some(lock)
}

/// Serves one connection: the uid check, the hello, then one call or one subscription. Every
/// failure ends this connection only. Every frame of a call, in either direction, must pass within
/// the frame timeout; the events of a subscription have no deadline.
async fn serve<T: Transport>(shared: &Shared<T>, mut stream: UnixStream) {
    match stream.peer_cred() {
        Ok(cred) if cred.uid() == shared.euid => {}
        _ => return,
    }
    let timeout = shared.config.frame_timeout;
    let hello: Result<Hello, FrameError> =
        read_frame(&mut stream, LOCAL_FRAME_MAX, Some(timeout)).await;
    let api = match hello {
        Ok(hello) => Some(hello.api),
        // A hello this build cannot read may come from another version: say which version this
        // is, then close.
        Err(FrameError::Json(_)) => None,
        Err(_) => return,
    };
    if write_within(&mut stream, &shared.hello, timeout)
        .await
        .is_err()
        || api != Some(API_VERSION)
    {
        return;
    }
    let open: Result<Open, FrameError> =
        read_frame(&mut stream, LOCAL_FRAME_MAX, Some(timeout)).await;
    match open {
        Ok(Open::Call { request }) => {
            let reply = match shared.transport.call(request).await {
                Ok(response) => Reply::Ok { response },
                Err(error) => Reply::Err { error },
            };
            let _ = write_reply(&mut stream, &reply, timeout).await;
        }
        Ok(Open::Subscribe) => match shared.transport.subscribe().await {
            Ok(events) => {
                if write_reply(&mut stream, &Reply::Subscribed, timeout)
                    .await
                    .is_ok()
                {
                    forward(stream, events).await;
                }
            }
            Err(error) => {
                let _ = write_reply(&mut stream, &Reply::Err { error }, timeout).await;
            }
        },
        Err(FrameError::Json(e)) => {
            let error = ApiError::BadRequest {
                detail: json_error_detail(&e),
            };
            let _ = write_reply(&mut stream, &Reply::Err { error }, timeout).await;
        }
        Err(_) => {}
    }
}

/// Writes `value` as one frame within `timeout`. After a timeout the stream is unusable; the caller
/// closes it.
async fn write_within<V: serde::Serialize>(
    stream: &mut UnixStream,
    value: &V,
    timeout: Duration,
) -> Result<(), FrameError> {
    tokio::time::timeout(timeout, write_frame(stream, value, LOCAL_FRAME_MAX))
        .await
        .map_err(|_elapsed| FrameError::Timeout)?
}

/// Writes `reply` within `timeout`. A reply too large for a frame (a response the station should
/// never make) becomes [`ApiError::Internal`], so the client learns why instead of seeing the
/// connection close.
async fn write_reply(
    stream: &mut UnixStream,
    reply: &Reply,
    timeout: Duration,
) -> Result<(), FrameError> {
    match write_within(stream, reply, timeout).await {
        Err(FrameError::TooLarge { len, cap }) => {
            let error = ApiError::Internal {
                detail: format!("the answer of {len} bytes is larger than the socket's {cap}"),
            };
            write_within(stream, &Reply::Err { error }, timeout).await
        }
        other => other,
    }
}

/// Sends every event of `events` as a frame until the subscription ends or the client goes away.
///
/// The client sends nothing after `Open::Subscribe`, so the read half is only watched for the end
/// of the stream (or an error, or a stray byte, which is a protocol violation): all three close
/// the connection. `Subscription::next` and `read` into a buffer are both cancel-safe, so they can
/// share a `select!` that is re-created on every turn; no frame is ever read here.
async fn forward(mut stream: UnixStream, mut events: Subscription) {
    let (mut reader, mut writer) = stream.split();
    let mut probe = [0u8; 1];
    loop {
        let event = tokio::select! {
            event = events.next() => event,
            _ = reader.read(&mut probe) => return,
        };
        let Some(event) = event else { return };
        if write_frame(&mut writer, &event, LOCAL_FRAME_MAX)
            .await
            .is_err()
        {
            return;
        }
    }
}
