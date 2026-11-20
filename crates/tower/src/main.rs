//! The `tower` binary: a loopback development relay, or a production tower with `serve`.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io::{self, IsTerminal};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use numbers_tower::{CertSource, DEFAULT_BIND, TLS_LOG_TARGET, Tower, TowerConfig, config};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, filter::filter_fn};

const LOG_TARGET: &str = "numbers_tower::cli";

/// The target of iroh-relay's own events in `src/server.rs`, its ACME events among them.
const RELAY_SERVER_TARGET: &str = "iroh_relay::server";
/// The span iroh-relay runs its ACME client in (iroh-relay 1.5.0, `src/server.rs:783`).
const ACME_SPAN: &str = "acme";
/// How iroh-relay's debug message for a deployed certificate starts: `acme event: {ok:?}` with
/// tokio-rustls-acme's `EventOk::DeployedNewCert` or `DeployedCachedCert`.
const DEPLOYED_PREFIX: &str = "acme event: Deployed";

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
        /// The config file. Relative paths in it are resolved against the directory of the
        /// path as given.
        #[arg(long)]
        config: PathBuf,
    },
}

/// Whether the binary prints an event of `target`: the tower's own targets only. Events and
/// spans of dependencies can carry endpoint ids and host names (hard rule 9).
fn shown(target: &str) -> bool {
    target == LOG_TARGET || target == TLS_LOG_TARGET
}

/// What the start line says about the allowlist.
fn access_label(listed: Option<usize>) -> String {
    match listed {
        Some(count) => format!("{count} ids listed"),
        None => "open".to_owned(),
    }
}

/// What the start line says about where the certificate comes from.
fn certificate_label(config: &TowerConfig) -> &'static str {
    match config.tls.as_ref().map(|tls| &tls.source) {
        None => "none",
        Some(CertSource::Files { .. }) => "files",
        Some(CertSource::Acme {
            production: true, ..
        }) => "acme production",
        Some(CertSource::Acme { .. }) => "acme staging",
        Some(_) => "other",
    }
}

/// A step of iroh-relay's ACME client that the binary reports, with no detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcmeNote {
    /// A step failed: iroh-relay logged an error in its ACME span.
    Failed,
    /// A certificate was deployed, newly ordered or from the cache.
    Deployed,
}

/// A layer that turns iroh-relay's ACME events into [`AcmeNote`]s, without their text.
///
/// iroh-relay 1.5.0 (`src/server.rs:777-783`) logs each event of its ACME client in a span
/// named `acme`, under its own target: `error!("error: {err:?}")` for a failure and
/// `debug!("acme event: {ok:?}")` for a step. Both carry the domain, so the log filter drops
/// them. This layer looks only at the target, the span's name, the level and whether a debug
/// message starts with `acme event: Deployed`; it never keeps or prints a field. It sends the
/// note over a channel, since an event logged while the dispatcher is busy with this one would
/// be dropped. If iroh-relay's wording changes, the notes stop; nothing else breaks.
struct AcmeWatch {
    notes: UnboundedSender<AcmeNote>,
}

impl<S> Layer<S> for AcmeWatch
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let in_acme_span = ctx
            .event_span(event)
            .is_some_and(|span| span.name() == ACME_SPAN);
        if metadata.target() != RELAY_SERVER_TARGET || !in_acme_span {
            return;
        }
        let note = match *metadata.level() {
            Level::ERROR => Some(AcmeNote::Failed),
            Level::DEBUG => {
                let mut message = StartsWithDeployed::default();
                event.record(&mut message);
                message.yes().then_some(AcmeNote::Deployed)
            }
            _ => None,
        };
        if let Some(note) = note {
            // The receiver lives as long as the process; a closed channel loses only the note.
            let _ = self.notes.send(note);
        }
    }
}

/// Whether a formatted message starts with [`DEPLOYED_PREFIX`]. It compares as the message is
/// written and stops at the first byte that differs; it keeps nothing.
#[derive(Default)]
struct StartsWithDeployed {
    matched: usize,
    differs: bool,
}

impl fmt::Write for StartsWithDeployed {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let prefix = DEPLOYED_PREFIX.as_bytes();
        for &byte in text.as_bytes() {
            if self.matched == prefix.len() {
                return Err(fmt::Error);
            }
            if prefix.get(self.matched) != Some(&byte) {
                self.differs = true;
                return Err(fmt::Error);
            }
            self.matched += 1;
        }
        Ok(())
    }
}

impl StartsWithDeployed {
    fn yes(&self) -> bool {
        !self.differs && self.matched == DEPLOYED_PREFIX.len()
    }
}

impl Visit for StartsWithDeployed {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            // Stops early by design; the error only means "decided".
            let _ = fmt::write(self, format_args!("{value:?}"));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            let _ = fmt::Write::write_str(self, value);
        }
    }
}

/// Logs a note under the binary's own target, as a fixed line.
fn log_acme_note(note: AcmeNote) {
    match note {
        AcmeNote::Failed => tracing::warn!(
            target: LOG_TARGET,
            "ACME: a certificate step failed; the tower retries with a growing delay"
        ),
        AcmeNote::Deployed => tracing::info!(target: LOG_TARGET, "ACME: a certificate is in use"),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let (notes, mut received) = unbounded_channel();
    // Colour only on a terminal: journald and files get plain text. The fmt layer writes to
    // standard output.
    let log = tracing_subscriber::fmt::layer()
        .with_ansi(io::stdout().is_terminal())
        .with_filter(filter_fn(|metadata| shown(metadata.target())));
    let acme = AcmeWatch { notes }.with_filter(filter_fn(|metadata| {
        metadata.target() == RELAY_SERVER_TARGET && *metadata.level() <= Level::DEBUG
    }));
    tracing_subscriber::registry().with(log).with(acme).init();
    tokio::spawn(async move {
        while let Some(note) = received.recv().await {
            log_acme_note(note);
        }
    });

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
    let mut tower = match args.command {
        None => {
            let tower = Tower::spawn(TowerConfig::dev(args.bind)).await?;
            tracing::info!(target: LOG_TARGET, url = tower.url(), "dev tower listening");
            tower
        }
        Some(Command::Serve { config }) => serve(config).await?,
    };
    // A signal stops the tower gracefully and exits with success; a relay that stops by itself
    // is a failure, so that a service manager restarts the tower.
    let signal_result = tokio::select! {
        signal_result = shutdown_signal => signal_result,
        stopped = tower.stopped() => return Err(Box::new(stopped)),
    };
    tower.shutdown().await?;
    signal_result?;
    Ok(())
}

/// Loads the config on the blocking pool, starts the tower, and logs its listeners. The log
/// line names no host name, URL, id or path (hard rule 9).
async fn serve(path: PathBuf) -> Result<Tower, Box<dyn Error + Send + Sync>> {
    let config = tokio::task::spawn_blocking(move || config::load(&path)).await??;
    let certificate = certificate_label(&config);
    let tower = Tower::spawn(config).await?;
    let or_off =
        |addr: Option<SocketAddr>| addr.map_or_else(|| "off".to_owned(), |a| a.to_string());
    tracing::info!(
        target: LOG_TARGET,
        https = %or_off(tower.https_addr()),
        http = %tower.http_addr(),
        qad = %or_off(tower.qad_addr()),
        access = %access_label(tower.listed()),
        certificate,
        "tower listening"
    );
    if certificate == "acme staging" {
        tracing::warn!(
            target: LOG_TARGET,
            "the certificate comes from Let's Encrypt's staging directory, which stations do not \
             trust; set `production = true` in `[acme]` for a real one"
        );
    }
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

    use numbers_tower::{TlsTower, TowerConfig};
    use tracing::Level;
    use tracing_subscriber::Layer;
    use tracing_subscriber::filter::filter_fn;
    use tracing_subscriber::layer::SubscriberExt;

    use super::{
        AcmeNote, AcmeWatch, Args, Command, RELAY_SERVER_TARGET, access_label, certificate_label,
        shown,
    };

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
    fn the_log_filter_shows_the_towers_own_targets_only() {
        assert!(shown("numbers_tower::cli"));
        assert!(shown(numbers_tower::TLS_LOG_TARGET));
        assert_eq!(numbers_tower::TLS_LOG_TARGET, "numbers_tower::tls");
        for hidden in [
            "iroh_relay::server",
            "iroh",
            "iroh::socket",
            "numbers_tower",
            "numbers_tower::cli2",
            "numbers_tower::tlsx",
            "numbers",
        ] {
            assert!(!shown(hidden), "{hidden}");
        }
    }

    #[test]
    fn the_start_line_labels() {
        assert_eq!(access_label(Some(3)), "3 ids listed");
        assert_eq!(access_label(Some(0)), "0 ids listed");
        assert_eq!(access_label(None), "open");
        let files = TowerConfig::tls(TlsTower::new("tower1.example.com", "c.pem", "k.pem"));
        assert_eq!(certificate_label(&files), "files");
        let acme = |production| {
            TowerConfig::tls(
                TlsTower::acme("tower1.example.com", "mailto:ops@example.com", "/c")
                    .production(production),
            )
        };
        assert_eq!(certificate_label(&acme(false)), "acme staging");
        assert_eq!(certificate_label(&acme(true)), "acme production");
        assert_eq!(certificate_label(&TowerConfig::dev(DEFAULT_BIND)), "none");
    }

    /// Exactly the two notes come out, and nothing of the events' text.
    #[test]
    fn the_acme_layer_reports_failures_and_deployments_only() {
        #[derive(Debug)]
        enum EventOk {
            DeployedNewCert,
            DeployedCachedCert,
            CertCacheStore,
        }
        let (notes, mut received) = tokio::sync::mpsc::unbounded_channel();
        let layer = AcmeWatch { notes }.with_filter(filter_fn(|metadata| {
            metadata.target() == RELAY_SERVER_TARGET && *metadata.level() <= Level::DEBUG
        }));
        let subscriber = tracing_subscriber::registry().with(layer);
        const RELAY: &str = "iroh_relay::server";
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(target: RELAY, "acme");
            let entered = span.enter();
            tracing::error!(target: RELAY, "error: {:?}", "order for qzx-host.example failed");
            tracing::debug!(target: RELAY, "acme event: {:?}", EventOk::CertCacheStore);
            tracing::debug!(target: RELAY, "acme event: {:?}", EventOk::DeployedNewCert);
            tracing::debug!(target: RELAY, "acme event: {:?}", EventOk::DeployedCachedCert);
            tracing::info!(target: RELAY, "acme event: {:?}", EventOk::DeployedNewCert);
            // Another target in the same span.
            tracing::error!(target: "iroh_relay::server::x", "error: {:?}", "qzx-host.example");
            drop(entered);
            // The same target outside the span, and in another span.
            tracing::error!(target: RELAY, "error: {:?}", "qzx-host.example");
            tracing::debug!(target: RELAY, "acme event: {:?}", EventOk::DeployedNewCert);
            let other = tracing::info_span!(target: RELAY, "relay");
            let _other = other.enter();
            tracing::error!(target: RELAY, "error: {:?}", "qzx-host.example");
        });
        let mut seen = Vec::new();
        while let Ok(note) = received.try_recv() {
            seen.push(note);
        }
        assert_eq!(
            seen,
            vec![AcmeNote::Failed, AcmeNote::Deployed, AcmeNote::Deployed]
        );
        for note in seen {
            assert!(!format!("{note:?}").contains("qzx"));
        }
    }

    #[test]
    fn serve_needs_a_config_file() {
        let error = Args::try_parse_from(["tower", "serve"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    /// The systemd unit in `packaging/systemd/`, as shipped.
    const UNIT: &str = include_str!("../../../packaging/systemd/numbers-tower.service");

    /// The values of every `key=` line in the unit, with surrounding white space trimmed.
    /// Comment lines start with `#` or `;`.
    fn unit_values<'a>(unit: &'a str, key: &str) -> Vec<&'a str> {
        unit.lines()
            .map(str::trim)
            .filter(|line| !line.starts_with('#') && !line.starts_with(';'))
            .filter_map(|line| line.split_once('='))
            .filter(|(name, _)| name.trim() == key)
            .map(|(_, value)| value.trim())
            .collect()
    }

    /// The unit's `ExecStart` is a command line this binary accepts. The unit uses no quoting,
    /// escapes or specifiers, so splitting on white space is what systemd does with it.
    #[test]
    fn the_systemd_units_exec_start_parses() {
        let exec_start = unit_values(UNIT, "ExecStart");
        assert_eq!(exec_start.len(), 1, "exactly one ExecStart line");
        let argv: Vec<&str> = exec_start[0].split_whitespace().collect();
        assert!(
            argv.iter().all(|arg| !arg.contains(['"', '\'', '%', '\\'])),
            "no quoting, escapes or specifiers: {argv:?}"
        );
        let program = Path::new(argv[0]);
        assert!(program.is_absolute(), "{program:?}");
        assert_eq!(program.file_name().unwrap(), "tower");
        let args = Args::try_parse_from(&argv)
            .unwrap_or_else(|error| panic!("the unit's ExecStart does not parse: {error}"));
        let Some(Command::Serve { config }) = args.command else {
            panic!("expected serve");
        };
        assert_eq!(config, Path::new("/etc/numbers-tower/tower.toml"));
    }

    /// The directives the binary depends on are in the unit, once each.
    #[test]
    fn the_systemd_unit_has_what_the_binary_needs() {
        let has = |key: &str, value: &str| {
            assert_eq!(unit_values(UNIT, key), vec![value], "{key}={value}");
        };
        // The ACME cache lives under the state directory, and the tower refuses a cache that
        // the group or others can access.
        has("StateDirectory", "numbers-tower");
        has("StateDirectoryMode", "0700");
        // The binary exits with a failure when its relay stops by itself and with success on
        // SIGTERM: the first is restarted, a stop is not.
        has("Restart", "on-failure");
        // The default listeners are on ports 80 and 443, and the dynamic user is not root.
        has("AmbientCapabilities", "CAP_NET_BIND_SERVICE");
        has("CapabilityBoundingSet", "CAP_NET_BIND_SERVICE");
    }
}
