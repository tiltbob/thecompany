//! The `tower` binary: a loopback development relay, or a production tower with `serve`.

use std::error::Error;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use numbers_tower::{DEFAULT_BIND, Tower, TowerConfig, config};
use tracing_subscriber::{Layer, filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt};

const LOG_TARGET: &str = "numbers_tower::cli";
/// The library's certificate reload warnings.
const TLS_LOG_TARGET: &str = "numbers_tower::tls";

#[derive(Debug, Parser)]
#[command(
    about = "Run a loopback-only development relay, or a production tower with `serve`",
    args_conflicts_with_subcommands = true
)]
struct Args {
    /// Loopback address to listen on.
    #[arg(long, default_value_t = DEFAULT_BIND)]
    bind: SocketAddr,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a production tower from a TOML config file.
    Serve {
        /// The config file. Relative paths in it are resolved against its directory.
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    // Dependency events and spans can include endpoint IDs and host names. Print only the
    // tower's own events.
    let log = tracing_subscriber::fmt::layer().with_filter(filter_fn(|metadata| {
        metadata.target() == LOG_TARGET || metadata.target() == TLS_LOG_TARGET
    }));
    tracing_subscriber::registry().with(log).init();

    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tower: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), Box<dyn Error + Send + Sync>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| io::Error::other("failed to install the rustls ring provider"))?;

    let shutdown_signal = shutdown_signal()?;
    let tower = match args.command {
        None => {
            let tower = Tower::spawn(TowerConfig::dev(args.bind)).await?;
            tracing::info!(target: LOG_TARGET, url = tower.url(), "dev tower listening");
            tower
        }
        Some(Command::Serve { config }) => serve(config).await?,
    };
    let signal_result = shutdown_signal.await;
    tower.shutdown().await?;
    signal_result?;
    Ok(())
}

/// Loads the config on the blocking pool, starts the tower, and logs its listeners. The log
/// line names no host name, URL, id or path (hard rule 9).
async fn serve(path: PathBuf) -> Result<Tower, Box<dyn Error + Send + Sync>> {
    let config = tokio::task::spawn_blocking(move || config::load(&path)).await??;
    let access = match &config.allow {
        Some(ids) => format!("{} ids listed", ids.len()),
        None => "open".to_owned(),
    };
    let tower = Tower::spawn(config).await?;
    let or_off =
        |addr: Option<SocketAddr>| addr.map_or_else(|| "off".to_owned(), |a| a.to_string());
    tracing::info!(
        target: LOG_TARGET,
        https = %or_off(tower.https_addr()),
        http = %tower.http_addr(),
        qad = %or_off(tower.qad_addr()),
        access = %access,
        "tower listening"
    );
    Ok(tower)
}

#[cfg(unix)]
fn shutdown_signal() -> io::Result<impl Future<Output = io::Result<()>>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => (),
            _ = terminate.recv() => (),
        }
        Ok(())
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> io::Result<impl Future<Output = io::Result<()>>> {
    Ok(tokio::signal::ctrl_c())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use clap::Parser;
    use clap::error::ErrorKind;
    use numbers_tower::DEFAULT_BIND;

    use super::{Args, Command};

    #[test]
    fn no_arguments_start_the_dev_tower_on_the_default_bind() {
        let args = Args::try_parse_from(["tower"]).unwrap();
        assert_eq!(args.bind, DEFAULT_BIND);
        assert!(args.command.is_none());
    }

    #[test]
    fn bind_sets_the_dev_towers_address() {
        let args = Args::try_parse_from(["tower", "--bind", "127.0.0.1:3341"]).unwrap();
        assert_eq!(args.bind, "127.0.0.1:3341".parse().unwrap());
        assert!(args.command.is_none());
    }

    #[test]
    fn serve_takes_a_config_file() {
        let args = Args::try_parse_from(["tower", "serve", "--config", "x"]).unwrap();
        let Some(Command::Serve { config }) = args.command else {
            panic!("expected serve");
        };
        assert_eq!(config, Path::new("x"));
    }

    #[test]
    fn bind_and_serve_cannot_be_combined() {
        let error = Args::try_parse_from([
            "tower",
            "--bind",
            "127.0.0.1:3340",
            "serve",
            "--config",
            "x",
        ])
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn serve_needs_a_config_file() {
        let error = Args::try_parse_from(["tower", "serve"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }
}
