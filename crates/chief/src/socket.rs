//! The local socket: its envelope ([`wire`]) and the client end, [`SocketTransport`].
//!
//! The station serves the station API on a Unix socket (`numbers_station::ipc`); this module is
//! the client of that server and defines the frames both ends exchange. Frames are
//! [`numbers_base::write_frame`] / [`numbers_base::read_frame`] with [`LOCAL_FRAME_MAX`] in both
//! directions.
//!
//! # One connection per call or subscription
//!
//! 1. The client connects and checks that the socket is served by its own user
//!    (`UnixStream::peer_cred`).
//! 2. The client sends [`wire::Hello`]; the server always answers [`wire::HelloReply`] with its
//!    [`API_VERSION`], its station id and its instance id, and closes the connection if the
//!    versions differ.
//! 3. The client sends [`wire::Open`]. For [`wire::Open::Call`] the server answers one
//!    [`wire::Reply`] and closes. For [`wire::Open::Subscribe`] it answers
//!    [`wire::Reply::Subscribed`] once the station has registered the subscription, then sends one
//!    [`Event`] per frame until the subscription ends, and then closes. The client sends nothing
//!    more on a subscription; it closes the connection to end it.
//!
//! # The client keeps its write half open
//!
//! After [`wire::Open`] the client sends nothing, and the server takes the end of the client's
//! stream (as well as a read error or any further byte) as the client leaving, also while the
//! station is still working on the call or on opening the subscription. It then stops waiting for
//! the station, drops the call or subscription and closes without an answer. A client therefore
//! keeps its write half open until it has read its [`wire::Reply`] (on a subscription, for as long
//! as it wants events); one that shuts its write half down after `Open` may get no answer. A call
//! whose answer never arrived may still have been carried out (rule 12 in [`crate::api`]).
//! [`SocketTransport`] never shuts down its write half: it closes the whole connection, by
//! dropping it, only when it is done with it.
//!
//! Unix only.

use crate::api::{API_VERSION, ApiError, Event, Request, Response};
use crate::client::{EventSink, SUBSCRIPTION_CAPACITY, Subscription, Transport, subscription};
use numbers_base::{FrameError, LOCAL_FRAME_MAX, encode_frame, read_frame, write_frame};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

/// The socket's own envelope around the station API: what the client and the server say before
/// and around a [`Request`], a [`Response`] or an [`Event`].
///
/// These frames are not part of the station API in [`crate::api`]: an in-process transport never
/// sees them, and a version of the API is checked by them, not carried in them. They follow the
/// same JSON conventions: enums are internally tagged with `"type"` in snake_case, unknown fields
/// are ignored. A change to them is a change of [`API_VERSION`], except [`Hello`] and
/// [`HelloReply`], which must keep their fields so that two versions can tell each other apart.
pub mod wire {
    use crate::api::{ApiError, Request, Response};
    use serde::{Deserialize, Serialize};

    /// The first frame, from the client: the client's [`crate::API_VERSION`].
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Hello {
        /// The client's API version.
        pub api: u32,
    }

    /// The server's answer to [`Hello`], always sent, also when the versions differ.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct HelloReply {
        /// The station's API version. If it differs from the client's, the server closes the
        /// connection after this frame.
        pub api: u32,
        /// The station's id ([`crate::api::Status::station_id`]).
        pub station_id: String,
        /// The station's instance id ([`crate::api::Status::instance_id`]), new on every start.
        pub instance_id: String,
    }

    /// The second frame, from the client: what this connection is for. It is the client's last
    /// frame; the client keeps its write half open until it has its answer (see "The client keeps
    /// its write half open" in [`crate::socket`]).
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum Open {
        /// One call: the server answers with one [`Reply`] and closes.
        Call {
            /// The call.
            request: Request,
        },
        /// A subscription: the server answers [`Reply::Subscribed`], then sends one
        /// [`crate::api::Event`] per frame.
        Subscribe,
    }

    /// The server's answer to [`Open`].
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum Reply {
        /// The answer to [`Open::Call`].
        Ok {
            /// The station's response.
            response: Response,
        },
        /// The answer to [`Open::Subscribe`]: the station has registered the subscription, and
        /// events follow, one per frame.
        Subscribed,
        /// The station's error, or the server's [`ApiError::BadRequest`] for a frame it could not
        /// decode.
        Err {
            /// The error, unchanged.
            error: ApiError,
        },
    }
}

use wire::{Hello, HelloReply, Open, Reply};

/// How long [`SocketTransport`] waits by default for a call: connecting, the hello and the reply
/// together.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// A [`Transport`] over the station's Unix socket.
///
/// Each call and each subscription opens its own connection. Before it says anything, the
/// transport checks that the socket is served by a process of its own effective user.
///
/// Errors of its own: [`ApiError::NotRunning`] when nothing listens on the socket or the socket
/// does not exist; [`ApiError::Disconnected`] when the connection breaks or the socket belongs to
/// another user; [`ApiError::IncompatibleApi`]; [`ApiError::Timeout`] when a call or the opening of
/// a subscription takes longer than the call timeout; [`ApiError::BadReply`] for a frame that is
/// too large or does not decode. A request too large for one frame is refused with
/// [`ApiError::BadRequest`] before the transport connects, so nothing is sent.
///
/// Needs a tokio runtime with the time driver; [`Transport::subscribe`] spawns a task on it.
#[derive(Debug, Clone)]
pub struct SocketTransport {
    path: PathBuf,
    call_timeout: Duration,
}

impl SocketTransport {
    /// A transport to the socket at `path`, with [`DEFAULT_CALL_TIMEOUT`].
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            call_timeout: DEFAULT_CALL_TIMEOUT,
        }
    }

    /// Sets how long a call may take, connecting, the hello and the reply together. It also bounds
    /// opening a subscription, but never an open subscription.
    pub fn with_call_timeout(self, timeout: Duration) -> Self {
        Self {
            call_timeout: timeout,
            ..self
        }
    }

    /// The socket's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The call timeout.
    pub fn call_timeout(&self) -> Duration {
        self.call_timeout
    }

    /// Runs `work` within the call timeout.
    async fn bounded<T>(
        &self,
        work: impl Future<Output = Result<T, ApiError>>,
    ) -> Result<T, ApiError> {
        tokio::time::timeout(self.call_timeout, work)
            .await
            .unwrap_or_else(|_elapsed| {
                Err(ApiError::Timeout {
                    after_ms: u64::try_from(self.call_timeout.as_millis()).unwrap_or(u64::MAX),
                })
            })
    }

    /// Connects, checks who serves the socket, and exchanges the hello. Then sends `open`, a whole
    /// frame made by [`encode_open`].
    async fn open(&self, open: &[u8]) -> Result<UnixStream, ApiError> {
        let mut stream = UnixStream::connect(&self.path)
            .await
            .map_err(|e| self.connect_error(&e))?;
        let served_by = stream
            .peer_cred()
            .map_err(|e| disconnected(&format!("cannot read who serves the socket: {e}")))?
            .uid();
        let me = rustix::process::geteuid().as_raw();
        if served_by != me {
            return Err(disconnected(&format!(
                "the socket {} is served by uid {served_by}, not by this user (uid {me}); \
                 refusing to talk to it",
                self.path.display()
            )));
        }
        send(&mut stream, &Hello { api: API_VERSION }).await?;
        let hello: HelloReply = receive(&mut stream).await?;
        if hello.api != API_VERSION {
            return Err(ApiError::IncompatibleApi {
                station: hello.api,
                client: API_VERSION,
            });
        }
        async {
            stream.write_all(open).await?;
            stream.flush().await
        }
        .await
        .map_err(|e| disconnected(&format!("I/O error on the socket: {e}")))?;
        Ok(stream)
    }

    fn connect_error(&self, e: &io::Error) -> ApiError {
        match e.kind() {
            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused => ApiError::NotRunning {
                socket: self.path.display().to_string(),
            },
            _ => disconnected(&format!("cannot connect to {}: {e}", self.path.display())),
        }
    }

    async fn call_inner(&self, request: Request) -> Result<Response, ApiError> {
        let open = encode_open(&Open::Call { request })?;
        let mut stream = self.open(&open).await?;
        match receive(&mut stream).await? {
            Reply::Ok { response } => Ok(response),
            Reply::Err { error } => Err(error),
            Reply::Subscribed => Err(ApiError::BadReply {
                detail: "the station answered a call as if it were a subscription".to_owned(),
            }),
        }
    }

    async fn subscribe_inner(&self) -> Result<UnixStream, ApiError> {
        let open = encode_open(&Open::Subscribe)?;
        let mut stream = self.open(&open).await?;
        match receive(&mut stream).await? {
            Reply::Subscribed => Ok(stream),
            Reply::Err { error } => Err(error),
            Reply::Ok { .. } => Err(ApiError::BadReply {
                detail: "the station answered a subscription as if it were a call".to_owned(),
            }),
        }
    }
}

impl Transport for SocketTransport {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        self.bounded(self.call_inner(request)).await
    }

    /// Opens a subscription on a connection of its own. Resolves once the station has answered
    /// that it registered the subscription (rule 3 in [`crate::api`]), within the call timeout.
    ///
    /// Spawns one task on the current tokio runtime (it panics outside one) that reads the events
    /// with no deadline and queues them. The task ends, closing the connection, when the station
    /// ends the subscription, when the connection breaks or a frame is bad (the subscription then
    /// ends: rule 11), or when the [`Subscription`] is dropped.
    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        let stream = self.bounded(self.subscribe_inner()).await?;
        let (sink, events) = subscription(SUBSCRIPTION_CAPACITY);
        tokio::spawn(forward_events(stream, sink));
        Ok(events)
    }
}

/// Reads events from `stream` into `sink` until the stream ends or fails, or the subscription is
/// dropped.
///
/// The read is cancelled only when the subscription was dropped, and then the stream is never
/// read again, so the frame codec's lack of cancel safety does not matter.
async fn forward_events(mut stream: UnixStream, sink: EventSink) {
    let mut closed = pin!(sink.closed());
    loop {
        let read = read_frame::<_, Event>(&mut stream, LOCAL_FRAME_MAX, None);
        let mut read = pin!(read);
        let frame = std::future::poll_fn(|cx| {
            if closed.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }
            read.as_mut().poll(cx).map(Some)
        })
        .await;
        match frame {
            Some(Ok(event)) => {
                if sink.push(event).is_err() {
                    return;
                }
            }
            // Dropped, ended, or broken: ending the subscription is all there is to do.
            Some(Err(_)) | None => return,
        }
    }
}

/// Describes a JSON decoding error by its category and position only.
///
/// `serde_json`'s own message can quote a value from the input (`invalid type: string "..."`),
/// and the input is a request or a reply of the station API; this text never holds any of it.
/// Both ends of the socket use it for the detail of [`ApiError::BadRequest`] and
/// [`ApiError::BadReply`].
pub fn json_error_detail(error: &serde_json::Error) -> String {
    let what = match error.classify() {
        serde_json::error::Category::Io => "could not be read",
        serde_json::error::Category::Syntax => "is not valid JSON",
        serde_json::error::Category::Data => "does not have the expected fields or types",
        serde_json::error::Category::Eof => "ends in the middle of a JSON value",
    };
    // An error inside an internally tagged enum has no position (serde reads it from a buffered
    // value and reports line 0).
    match error.line() {
        0 => format!("the frame {what}"),
        line => format!("the frame {what} (line {line}, column {})", error.column()),
    }
}

/// Serializes `open` as a whole frame, before anything is sent: a request too large for one frame
/// is refused with [`ApiError::BadRequest`] without connecting.
fn encode_open(open: &Open) -> Result<Vec<u8>, ApiError> {
    encode_frame(open, LOCAL_FRAME_MAX).map_err(|e| ApiError::BadRequest {
        detail: match &e {
            FrameError::Json(json) => json_error_detail(json),
            // The sizes only: nothing of the request.
            _ => e.to_string(),
        },
    })
}

async fn send<T: serde::Serialize>(stream: &mut UnixStream, value: &T) -> Result<(), ApiError> {
    write_frame(stream, value, LOCAL_FRAME_MAX)
        .await
        .map_err(|e| match &e {
            FrameError::Json(j) => ApiError::BadRequest {
                detail: json_error_detail(j),
            },
            FrameError::TooLarge { .. } => ApiError::BadRequest {
                detail: e.to_string(),
            },
            FrameError::Closed | FrameError::Timeout | FrameError::Io(_) => {
                disconnected(&e.to_string())
            }
        })
}

async fn receive<T: serde::de::DeserializeOwned>(stream: &mut UnixStream) -> Result<T, ApiError> {
    read_frame(stream, LOCAL_FRAME_MAX, None)
        .await
        .map_err(|e| match &e {
            FrameError::Json(j) => ApiError::BadReply {
                detail: json_error_detail(j),
            },
            FrameError::TooLarge { .. } => ApiError::BadReply {
                detail: e.to_string(),
            },
            FrameError::Closed => disconnected("the station closed the connection"),
            FrameError::Timeout | FrameError::Io(_) => disconnected(&e.to_string()),
        })
}

fn disconnected(detail: &str) -> ApiError {
    ApiError::Disconnected {
        detail: detail.to_owned(),
    }
}
