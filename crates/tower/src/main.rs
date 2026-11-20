//! The `tower` development relay.

use std::error::Error;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::process::ExitCode;

use clap::Parser;
use numbers_tower::{DEFAULT_BIND, Tower, TowerConfig};
use tracing_subscriber::{Layer, filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt};

const LOG_TARGET: &str = "numbers_tower::cli";

#[derive(Parser)]
#[command(about = "Run a loopback-only development relay")]
struct Args {
    /// Loopback address to listen on.
    #[arg(long, default_value_t = DEFAULT_BIND)]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    // Dependency events and spans can include endpoint IDs. Print only tower events.
    let log = tracing_subscriber::fmt::layer()
        .with_filter(filter_fn(|metadata| metadata.target() == LOG_TARGET));
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
    let tower = Tower::spawn(TowerConfig::dev(args.bind)).await?;
    tracing::info!(target: LOG_TARGET, url = tower.url(), "dev tower listening");
    let signal_result = shutdown_signal.await;
    tower.shutdown().await?;
    signal_result?;
    Ok(())
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
