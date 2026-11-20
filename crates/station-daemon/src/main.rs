//! The `station` binary runs a chief's station in the foreground.

use std::future::Future;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use chief::paths::{PathEnv, check_socket_path};
use clap::{Parser, Subcommand, error::ErrorKind};
use numbers_station::ipc::{IpcConfig, IpcError, IpcServer};
use numbers_station::{ShutdownError, StartError, Station, StationConfig};
use tokio::signal::unix::{SignalKind, signal};
use tracing_subscriber::{Layer, filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt};

const LOG_TARGET: &str = "numbers_station_daemon::cli";
const SOCKET_PATH_GUIDANCE: &str =
    "the Unix socket path is too long; choose a shorter --data-dir or XDG_RUNTIME_DIR";

#[derive(Parser)]
#[command(
    name = "station",
    bin_name = "station",
    version,
    about = "Run a chief's station"
)]
struct Args {
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Run the station in the foreground.
    Run {
        /// Private data directory. Defaults to $XDG_DATA_HOME/thecompany/station
        /// or $HOME/.local/share/thecompany/station.
        #[arg(long, value_name = "DIR")]
        data_dir: Option<PathBuf>,
        /// Tower URL for this run.
        #[arg(long, value_name = "URL")]
        tower: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(error) => return print_argument_exit(error, io::stdout(), io::stderr()),
    };
    // Network dependency events can contain node IDs or tower URLs.
    let log = tracing_subscriber::fmt::layer()
        .with_writer(io::stderr)
        .with_filter(filter_fn(|metadata| metadata.target() == LOG_TARGET));
    if tracing_subscriber::registry().with(log).try_init().is_err() {
        eprintln!("station: could not initialize logging");
        return ExitCode::FAILURE;
    }

    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("station: {error}");
            ExitCode::FAILURE
        }
    }
}

fn print_argument_exit(
    error: clap::Error,
    mut stdout: impl Write,
    mut stderr: impl Write,
) -> ExitCode {
    // Only help and version use clap's text. The command's fixed bin_name keeps argv[0] out
    // of usage, and the error diagnostics below never render clap's argument values.
    let diagnostic = match error.kind() {
        ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => {
            let _ = write!(stdout, "{}", error.render());
            return ExitCode::SUCCESS;
        }
        ErrorKind::MissingRequiredArgument => {
            "pass --tower URL to station run; see station run --help"
        }
        _ => "invalid arguments; see station --help or station run --help",
    };
    let _ = writeln!(stderr, "station: {diagnostic}");
    ExitCode::from(2)
}

async fn run(args: Args) -> Result<(), RunError> {
    let (config, socket) = resolve(args, &PathEnv::read())?;
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| RunError::CryptoProvider)?;
    // Register before startup so signals received while binding are observed as soon as we serve.
    let stop = shutdown_signal().map_err(|_| RunError::Signals)?;
    let running = RunningStation::start(config, &socket).await?;
    tracing::info!(target: LOG_TARGET, "station listening");
    stop.await;
    running.shutdown().await.map_err(|_| RunError::Shutdown)
}

fn resolve(args: Args, env: &PathEnv) -> Result<(StationConfig, PathBuf), RunError> {
    let Action::Run { data_dir, tower } = args.action;
    let data_dir = data_dir
        .or_else(|| env.default_data_dir())
        .ok_or(RunError::DataDirectory)?;
    let socket = env.socket_path(&data_dir);
    check_socket_path(&socket).map_err(|_| RunError::SocketPath)?;
    Ok((StationConfig::new(data_dir, tower), socket))
}

fn shutdown_signal() -> io::Result<impl Future<Output = ()>> {
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => (),
            _ = terminate.recv() => (),
        }
    })
}

#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error(
        "cannot find the default data directory; pass --data-dir DIR or set XDG_DATA_HOME or HOME to an absolute path"
    )]
    DataDirectory,
    #[error("{SOCKET_PATH_GUIDANCE}")]
    SocketPath,
    #[error("could not install the rustls ring provider")]
    CryptoProvider,
    #[error("could not register SIGINT and SIGTERM handlers")]
    Signals,
    #[error("{}", station_start_diagnostic(.0))]
    Station(#[from] StartError),
    #[error("{}", socket_start_diagnostic(.0))]
    Socket(#[from] IpcError),
    #[error("a station worker failed during shutdown")]
    Shutdown,
}

// Select diagnostics by operation and error kind. The wrapped errors can contain user input.
fn station_start_diagnostic(error: &StartError) -> &'static str {
    match error {
        StartError::AlreadyRunning => "another station is already running with this data directory",
        StartError::InvalidTower(_) => {
            "invalid --tower URL; use an HTTPS root URL or a loopback HTTP root URL without credentials, query, or fragment"
        }
        StartError::InvalidTimeouts => "invalid station timeouts",
        StartError::RuntimeRequired => "the station must be started inside a Tokio runtime",
        StartError::Randomness => "could not generate a station instance ID",
        StartError::Storage { .. } => {
            "could not initialize station storage; check that --data-dir is owned by the current user with no group or other access and that the key file has mode 0600"
        }
        StartError::Bind => "could not bind the station endpoint",
    }
}

fn socket_start_diagnostic(error: &IpcError) -> &'static str {
    match error {
        IpcError::PathTooLong { .. } => SOCKET_PATH_GUIDANCE,
        IpcError::Directory { .. } => {
            "could not use the socket directory; it must be owned by the current user with no group or other access"
        }
        IpcError::InUse { .. } => {
            "another station is already serving this socket; choose a separate --data-dir, or use a separate XDG_RUNTIME_DIR with the default data directory"
        }
        IpcError::Lock { .. } => "could not open or acquire the socket lock",
        IpcError::NotASocket { .. } => {
            "the socket path is occupied by another file; move it aside, choose another --data-dir, or change XDG_RUNTIME_DIR if using the default data directory"
        }
        IpcError::Leftover { .. } => "could not inspect or remove the leftover socket",
        IpcError::Bind { .. } => "could not bind the Unix socket or set its permissions",
        IpcError::Status { .. } => {
            "could not serve the Unix socket; the station did not answer its status request"
        }
    }
}

struct RunningStation {
    station: Station,
    ipc: IpcServer,
}

impl RunningStation {
    async fn start(config: StationConfig, socket: &Path) -> Result<Self, RunError> {
        // The core takes the data-directory lock and opens its key before the socket is touched.
        let station = Station::start(config).await?;
        match IpcServer::bind(station.handle(), socket, IpcConfig::default()).await {
            Ok(ipc) => Ok(Self { station, ipc }),
            Err(error) => {
                if station.shutdown().await.is_err() {
                    tracing::error!(target: LOG_TARGET, "station cleanup after socket startup failed");
                }
                Err(error.into())
            }
        }
    }

    async fn shutdown(self) -> Result<(), ShutdownError> {
        self.ipc.shutdown().await;
        self.station.shutdown().await
    }
}

#[cfg(test)]
mod tests;
