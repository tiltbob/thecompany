//! The station core, usable in-process through the same API as a local-socket client.
//!
//! One actor owns the network runtime and persistent state. Releasing its data-dir lock is the
//! last step of shutdown, after the endpoint has closed. Frontend handles submit API requests,
//! subscribe to events, and can observe when the station stops accepting requests.
//!
//! - [`ipc`] (Unix): the local-socket server, generic over any `chief` transport.

mod actor;
mod history;
mod storage;

#[cfg(unix)]
pub mod ipc;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chief::api::{ApiError, Request, Response, Status, TowerStatus};
use chief::client::{Subscription, Transport};
use iroh::Watcher;
use numbers::link::{LINK_ALPN, Link, LinkConfig};
use numbers::proto::CHAT_PROTOCOL;
use numbers::{Authorizer, Endpoint, EndpointConfig, TowerSet, TowerSetError, node_id};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use actor::{Actor, Command};
use storage::{Storage, StorageError};

pub use numbers::IpTransport;

/// How many messages the station keeps: the newest 1,000.
pub const MESSAGE_LOG_CAPACITY: usize = 1_000;

const COMMAND_CAPACITY: usize = 64;

/// Configuration for a station with one explicit M1 tower.
#[derive(Clone)]
pub struct StationConfig {
    /// Private state directory. Its ancestors must be writable only by the station user or root.
    pub data_dir: PathBuf,
    /// The tower's base URL.
    pub tower_url: String,
    /// Which IP sockets the endpoint binds. Use `RelayOnly` to disable direct paths.
    pub ip_transport: IpTransport,
    /// Budget for one dial and its application hello. A retry can use more of the send budget.
    /// When trying several stations, the sender reserves this duration for each later candidate
    /// when its remaining send budget permits.
    pub connect_timeout: Duration,
    /// Total budget for one send across all candidate stations.
    pub send_deadline: Duration,
}

impl StationConfig {
    /// Uses the default IP transport, a five-second connect timeout and a fifteen-second send
    /// deadline.
    pub fn new(data_dir: impl Into<PathBuf>, tower_url: impl Into<String>) -> Self {
        Self {
            data_dir: data_dir.into(),
            tower_url: tower_url.into(),
            ip_transport: IpTransport::Default,
            connect_timeout: Duration::from_secs(5),
            send_deadline: Duration::from_secs(15),
        }
    }
}

impl fmt::Debug for StationConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StationConfig")
            .field("ip_transport", &self.ip_transport)
            .field("connect_timeout", &self.connect_timeout)
            .field("send_deadline", &self.send_deadline)
            .finish_non_exhaustive()
    }
}

/// A failure before the station starts serving API calls.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// Another station holds the lock for this data directory.
    #[error("another station is already running with this data directory")]
    AlreadyRunning,
    /// The tower cannot be configured for this station.
    #[error("invalid station tower configuration: {0}")]
    InvalidTower(TowerSetError),
    /// A timeout is zero or cannot be represented as a deadline.
    #[error("station timeouts must be nonzero and representable as deadlines")]
    InvalidTimeouts,
    /// There is no current Tokio runtime.
    #[error("the station must be started inside a Tokio runtime")]
    RuntimeRequired,
    /// The system could not generate a station instance ID.
    #[error("could not generate a station instance ID")]
    Randomness,
    /// Local storage could not be initialized.
    #[error("{detail}")]
    Storage {
        /// A description of the operation, without paths or data contents.
        detail: String,
    },
    /// The endpoint could not bind its local sockets.
    #[error("could not bind the station endpoint")]
    Bind,
}

impl From<StorageError> for StartError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::AlreadyRunning => Self::AlreadyRunning,
            _ => Self::Storage {
                detail: error.to_string(),
            },
        }
    }
}

/// A station background task failed during shutdown.
#[derive(Debug, thiserror::Error)]
#[error("a station worker failed")]
pub struct ShutdownError;

/// Owns the lifetime of a running station. Drop requests orderly background shutdown.
pub struct Station {
    handle: StationHandle,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), ShutdownError>>,
}

impl fmt::Debug for Station {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Station").finish_non_exhaustive()
    }
}

impl Station {
    /// Takes the data-dir lock and starts the local endpoint. This never waits for a tower or a
    /// peer. Call it on a Tokio runtime with I/O and time enabled.
    pub async fn start(config: StationConfig) -> Result<Self, StartError> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| StartError::RuntimeRequired)?;
        let towers = TowerSet::from_url(&config.tower_url).map_err(StartError::InvalidTower)?;
        let tower_url = towers.entries()[0].url().to_owned();
        let now = Instant::now();
        for duration in [config.connect_timeout, config.send_deadline] {
            if duration.is_zero() || now.checked_add(duration).is_none() {
                return Err(StartError::InvalidTimeouts);
            }
        }
        let instance_id = new_id().map_err(|_| StartError::Randomness)?;
        let storage = Storage::lock(&config.data_dir)?;
        let key = storage.load_node_key()?;
        let own_id = node_id(&key);
        let book = storage.load_peers(own_id)?;
        let authorizer = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
        actor::authorize_book(&authorizer, &book);
        let endpoint_config = EndpointConfig {
            towers,
            ip_transport: config.ip_transport,
            ..EndpointConfig::default()
        };
        let endpoint = Endpoint::bind(&key, endpoint_config, Arc::clone(&authorizer))
            .await
            .map_err(|_| StartError::Bind)?;

        let mut relay = endpoint.iroh().home_relay_status();
        let status = Status {
            station_id: own_id.to_string(),
            instance_id,
            towers: vec![TowerStatus {
                url: tower_url,
                state: actor::tower_state(relay.get().first()),
                source: None,
            }],
            bootstrap_domain: None,
        };
        let link_config = LinkConfig {
            connect_timeout: config.connect_timeout,
            send_deadline: config.send_deadline,
        };
        let (network, incoming) = Link::start(endpoint, link_config);
        let actor = Actor::new(
            storage,
            own_id,
            book,
            authorizer,
            status,
            link_config,
            network.handle(),
        );
        let (commands, receive) = mpsc::channel(COMMAND_CAPACITY);
        let (stop, stopped) = oneshot::channel();
        let task = runtime.spawn(actor.run(network, incoming, relay, receive, stopped));
        Ok(Self {
            handle: StationHandle { commands },
            stop: Some(stop),
            task,
        })
    }

    /// Gets a cloneable API transport for this particular station instance.
    pub fn handle(&self) -> StationHandle {
        self.handle.clone()
    }

    /// Stops accepting calls and waits for the endpoint and its workers to close. The data-dir
    /// lock has been released when this returns.
    pub async fn shutdown(mut self) -> Result<(), ShutdownError> {
        self.request_stop();
        (&mut self.task).await.map_err(|_| ShutdownError)?
    }

    fn request_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

impl Drop for Station {
    fn drop(&mut self) {
        self.request_stop();
    }
}

/// A cloneable API transport that carries requests to the station actor.
#[derive(Clone, Debug)]
pub struct StationHandle {
    commands: mpsc::Sender<Command>,
}

impl StationHandle {
    /// Waits until the actor closes or drops its request receiver. This is cancel-safe.
    ///
    /// The receiver closes before orderly teardown finishes. Use [`Station::shutdown`] to
    /// wait for the actor to finish and the data-dir lock to be released.
    pub async fn closed(&self) {
        self.commands.closed().await;
    }
}

impl Transport for StationHandle {
    async fn call(&self, request: Request) -> Result<Response, ApiError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Call { request, reply })
            .await
            .map_err(|_| ApiError::Stopping)?;
        response.await.map_err(|_| ApiError::Stopping)?
    }

    async fn subscribe(&self) -> Result<Subscription, ApiError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Subscribe { reply })
            .await
            .map_err(|_| ApiError::Stopping)?;
        response.await.map_err(|_| ApiError::Stopping)
    }
}

fn new_id() -> Result<String, getrandom::Error> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes)?;
    Ok(format!("{:032x}", u128::from_be_bytes(bytes)))
}
