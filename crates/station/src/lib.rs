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
mod towers;

#[cfg(unix)]
pub mod ipc;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chief::api::{ApiError, Request, Response, Status};
use chief::client::{Subscription, Transport};
use numbers::link::{LINK_ALPN, Link, LinkConfig};
use numbers::proto::CHAT_PROTOCOL;
use numbers::{
    Authorizer, DnsConfig, Endpoint, EndpointConfig, Locator, TlsRoots, TowerSet, node_id,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use actor::{Actor, Command};
use storage::{Storage, StorageError};
use towers::{RuntimeTowers, TowerMode, canonical_domain};

pub use numbers::IpTransport;

/// How many messages the station keeps: the newest 1,000.
pub const MESSAGE_LOG_CAPACITY: usize = 1_000;

const COMMAND_CAPACITY: usize = 64;

/// Selects how to configure the station's towers for one run.
#[derive(Clone)]
pub enum TowerInput {
    /// Use this nonempty set for this run. The station leaves remembered towers untouched.
    Pinned(TowerSet),
    /// Use this bootstrap domain, reusing the remembered set for the same domain.
    Domain(String),
    /// Use the stored domain and set. Discover towers if its set is empty.
    Remembered,
}

/// Configuration for a station and its tower connections.
#[derive(Clone)]
pub struct StationConfig {
    /// Private state directory. Its ancestors must be writable only by the station user or root.
    pub data_dir: PathBuf,
    /// How this run obtains its tower set and bootstrap domain.
    pub tower_input: TowerInput,
    /// Root certificates for HTTPS towers.
    pub tls_roots: TlsRoots,
    /// DNS settings for tower connections and the default SRV locator.
    pub dns: DnsConfig,
    /// Optional SRV locator override. By default, discovery uses [`Self::dns`].
    pub locator: Option<Arc<dyn Locator>>,
    /// Which IP sockets the endpoint binds. Use `RelayOnly` to disable direct paths.
    pub ip_transport: IpTransport,
    /// Budget for connecting to one peer station.
    pub connect_timeout: Duration,
    /// Total budget for one send across all candidate stations.
    pub send_deadline: Duration,
}

impl StationConfig {
    /// Uses the default TLS roots, DNS and IP transport, a five-second connect timeout and a
    /// fifteen-second send deadline.
    pub fn new(data_dir: impl Into<PathBuf>, tower_input: TowerInput) -> Self {
        Self {
            data_dir: data_dir.into(),
            tower_input,
            tls_roots: TlsRoots::default(),
            dns: DnsConfig::default(),
            locator: None,
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
    /// The explicitly pinned tower set is empty.
    #[error("a pinned station requires at least one tower")]
    EmptyPinnedTowers,
    /// The supplied bootstrap domain is invalid.
    #[error("invalid station bootstrap domain")]
    InvalidBootstrapDomain,
    /// The station was asked to resume without a usable remembered domain.
    #[error("the station needs pinned towers or a bootstrap domain")]
    MissingTowerConfig,
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
    /// Takes the data-dir lock, syncs the directory, and starts the local endpoint. Discovery
    /// runs in the station actor after startup. This never waits for discovery, a tower, or a
    /// peer. Call it on a Tokio runtime with I/O and time enabled.
    pub async fn start(config: StationConfig) -> Result<Self, StartError> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| StartError::RuntimeRequired)?;
        let input = match config.tower_input {
            TowerInput::Pinned(set) if set.is_empty() => return Err(StartError::EmptyPinnedTowers),
            TowerInput::Domain(domain) => TowerInput::Domain(
                canonical_domain(&domain).map_err(|_| StartError::InvalidBootstrapDomain)?,
            ),
            input => input,
        };
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
        let mode = TowerMode::prepare(input, &storage, config.locator, config.dns)?;
        let authorizer = Arc::new(Authorizer::new(&[LINK_ALPN], &[CHAT_PROTOCOL]));
        if let Some(book) = &book {
            actor::authorize_book(&authorizer, book);
        }
        let endpoint_config = EndpointConfig {
            towers: mode.set().clone(),
            ip_transport: config.ip_transport,
            tls_roots: config.tls_roots,
            dns: config.dns,
        };
        let endpoint = Endpoint::bind(&key, endpoint_config, Arc::clone(&authorizer))
            .await
            .map_err(|_| StartError::Bind)?;

        let relay = endpoint.iroh().home_relay_status();
        let towers = RuntimeTowers {
            mode,
            control: endpoint.tower_control(),
        };
        let status = Status {
            station_id: own_id.to_string(),
            instance_id,
            towers: towers
                .statuses()
                .unwrap_or_else(|_| towers.mode.statuses(&[])),
            bootstrap_domain: towers.mode.domain().map(str::to_owned),
        };
        let (network, incoming) = Link::start(
            endpoint,
            LinkConfig {
                connect_timeout: config.connect_timeout,
                send_deadline: config.send_deadline,
            },
        );
        let actor = Actor::new(
            storage,
            own_id,
            book,
            authorizer,
            status,
            config.send_deadline,
            network.handle(),
        );
        let (commands, receive) = mpsc::channel(COMMAND_CAPACITY);
        let (stop, stopped) = oneshot::channel();
        let task = runtime.spawn(actor.run(network, incoming, relay, towers, receive, stopped));
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
