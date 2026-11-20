//! The `tower` binary: a loopback development relay, or a production tower with `serve`.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io::{self, IsTerminal};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Parser, Subcommand};
use numbers_tower::{
    CertSource, DEFAULT_BIND, TLS_LOG_TARGET, Tower, TowerConfig, TowerError, config,
};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, filter::filter_fn};

const LOG_TARGET: &str = "numbers_tower::cli";

/// The process's exit status on success, and after SIGINT or SIGTERM.
const EXIT_SUCCESS: u8 = 0;
/// A failure that may pass when the tower is started again: a listener that could not be
/// bound, a certificate file that could not be read, a relay that stopped by itself, and every
/// error not named in [`exit_status`].
const EXIT_FAILURE: u8 = 1;
/// A command-line error (clap's convention).
const EXIT_USAGE: u8 = 2;
/// A configuration error that no retry can fix: `EX_CONFIG` in `sysexits.h`, which systemd
/// prints as `CONFIG`. The systemd unit does not restart the tower after it, nor after
/// [`EXIT_USAGE`].
const EXIT_CONFIG: u8 = 78;

/// The target of iroh-relay's own events in `src/server.rs`, its ACME events among them.
const RELAY_SERVER_TARGET: &str = "iroh_relay::server";
/// The span iroh-relay runs its ACME client in (iroh-relay 1.5.0, `src/server.rs:783`).
const ACME_SPAN: &str = "acme";
/// How iroh-relay's debug message for a deployed certificate starts: `acme event: {ok:?}` with
/// tokio-rustls-acme's `EventOk::DeployedNewCert` or `DeployedCachedCert`.
const DEPLOYED: &[&str] = &["acme event: Deployed"];
/// How iroh-relay's error messages start, `error: {err:?}` with tokio-rustls-acme 0.10.0's
/// `EventError` (`src/state.rs:63-78`): an order (retried with a growing delay,
/// `src/state.rs:417-426`), and the four cache errors (`CertCacheLoad`, `CertCacheStore`,
/// `AccountCacheLoad`, `AccountCacheStore`), which are not retried.
const ERRORS: &[&str] = &["error: Order(", "error: CertCache", "error: AccountCache"];

#[derive(Debug, Parser)]
#[command(
    name = "tower",
    bin_name = "tower",
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

/// What the start line says about a listener: its port, never its address (for a tower reached
/// by an IP literal the address is the tower's identity, and logs get pasted into issues), or
/// `off`.
fn port_label(addr: Option<SocketAddr>) -> String {
    addr.map_or_else(|| "off".to_owned(), |addr| addr.port().to_string())
}

/// Where the certificate comes from, as the start line says it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Certificate {
    /// A dev tower, with no certificate.
    None,
    /// PEM files.
    Files,
    /// Let's Encrypt's staging directory, which stations do not trust.
    AcmeStaging,
    /// Let's Encrypt's production directory.
    AcmeProduction,
    /// A source this binary does not know.
    Other,
}

impl Certificate {
    fn of(config: &TowerConfig) -> Self {
        match config.tls.as_ref().map(|tls| &tls.source) {
            None => Self::None,
            Some(CertSource::Files { .. }) => Self::Files,
            Some(CertSource::Acme {
                production: true, ..
            }) => Self::AcmeProduction,
            Some(CertSource::Acme { .. }) => Self::AcmeStaging,
            Some(_) => Self::Other,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Files => "files",
            Self::AcmeStaging => "acme staging",
            Self::AcmeProduction => "acme production",
            Self::Other => "other",
        }
    }

    /// Whether the start warns that stations do not trust the certificate.
    fn warns(self) -> bool {
        self == Self::AcmeStaging
    }
}

/// A step of iroh-relay's ACME client that the binary reports, with no detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcmeNote {
    /// An order failed; the ACME client retries it with a growing delay.
    OrderFailed,
    /// The cache directory could not be read or written; the ACME client does not retry that.
    CacheFailed,
    /// Another step failed.
    StepFailed,
    /// A certificate was deployed, newly ordered or from the cache.
    Deployed,
}

/// A layer that turns iroh-relay's ACME events into [`AcmeNote`]s, without their text.
///
/// iroh-relay 1.5.0 (`src/server.rs:777-783`) logs each event of its ACME client in a span
/// named `acme`, under its own target: `error!("error: {err:?}")` for a failure and
/// `debug!("acme event: {ok:?}")` for a step. The error's text can carry the domain; the
/// step's does not (tokio-rustls-acme 0.10.0's `EventOk` variants have no fields, and the span
/// has none). The log filter ([`shown`]) drops both, as it drops every event that is not the
/// tower's own. This layer looks only at the target, the span's name, the level and how the message
/// starts ([`ERRORS`], [`DEPLOYED`]); it never keeps or prints a field. It sends the note over
/// a channel, since an event logged while the dispatcher is busy with this one would be
/// dropped. If iroh-relay's or tokio-rustls-acme's wording changes, an error becomes the
/// neutral note and a deployment is no longer reported; nothing else breaks.
struct AcmeWatch {
    notes: UnboundedSender<AcmeNote>,
}

/// The per-layer filter `main` installs in front of [`AcmeWatch`]: events of iroh-relay's
/// server module, ERROR to DEBUG. It is kept wider than [`AcmeWatch::on_event`]'s own check, so
/// the filter can only let too much through, never too little; the layer decides the rest.
fn acme_filter(metadata: &tracing::Metadata<'_>) -> bool {
    metadata.target().starts_with(RELAY_SERVER_TARGET) && *metadata.level() <= Level::DEBUG
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
            Level::ERROR => {
                let mut message = MessageStart::new(ERRORS);
                event.record(&mut message);
                Some(match message.found() {
                    Some(0) => AcmeNote::OrderFailed,
                    Some(_) => AcmeNote::CacheFailed,
                    None => AcmeNote::StepFailed,
                })
            }
            Level::DEBUG => {
                let mut message = MessageStart::new(DEPLOYED);
                event.record(&mut message);
                message.found().map(|_| AcmeNote::Deployed)
            }
            _ => None,
        };
        if let Some(note) = note {
            // The receiver lives as long as the process; a closed channel loses only the note.
            let _ = self.notes.send(note);
        }
    }
}

/// Which of a few prefixes a formatted message starts with. It compares the bytes as the
/// message is written, in as many pieces as it comes, and stops as soon as the answer is
/// known; it keeps no byte of the message, only how many bytes matched and which prefixes are
/// still possible.
struct MessageStart {
    prefixes: &'static [&'static str],
    matched: usize,
    /// Bit `i` is set while `prefixes[i]` can still match.
    possible: u32,
}

impl MessageStart {
    fn new(prefixes: &'static [&'static str]) -> Self {
        Self {
            prefixes,
            matched: 0,
            possible: (1 << prefixes.len()) - 1,
        }
    }

    /// The index of the first prefix the message starts with, if any.
    fn found(&self) -> Option<usize> {
        (0..self.prefixes.len()).find(|&index| {
            self.possible & (1 << index) != 0
                && self
                    .prefixes
                    .get(index)
                    .is_some_and(|p| p.len() <= self.matched)
        })
    }

    /// Whether more bytes could still change the answer.
    fn undecided(&self) -> bool {
        (0..self.prefixes.len()).any(|index| {
            self.possible & (1 << index) != 0
                && self
                    .prefixes
                    .get(index)
                    .is_some_and(|p| p.len() > self.matched)
        })
    }
}

impl fmt::Write for MessageStart {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        for &byte in text.as_bytes() {
            if !self.undecided() {
                return Err(fmt::Error);
            }
            for (index, prefix) in self.prefixes.iter().enumerate() {
                let bytes = prefix.as_bytes();
                if self.matched < bytes.len() && bytes.get(self.matched) != Some(&byte) {
                    self.possible &= !(1 << index);
                }
            }
            self.matched += 1;
        }
        Ok(())
    }
}

impl Visit for MessageStart {
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
        AcmeNote::OrderFailed => tracing::warn!(
            target: LOG_TARGET,
            "ACME: a certificate order failed; the tower retries with a growing delay"
        ),
        AcmeNote::CacheFailed => tracing::warn!(
            target: LOG_TARGET,
            "ACME: the cache directory could not be read or written; certificates are not kept \
             across restarts"
        ),
        AcmeNote::StepFailed => {
            tracing::warn!(target: LOG_TARGET, "ACME: a certificate step failed")
        }
        AcmeNote::Deployed => tracing::info!(target: LOG_TARGET, "ACME: a certificate is in use"),
    }
}

/// Where a command-line error goes, and with which exit status.
#[derive(Debug, PartialEq, Eq)]
struct Rendered {
    text: String,
    status: u8,
    to_stdout: bool,
}

/// Renders a clap error for printing. Help is clap's own text, on standard output, with exit
/// status 0. Every other error is a fixed text of ours on standard error with exit status 2:
/// clap's own messages repeat what was typed (a value, an unknown argument), and under a service
/// manager standard error goes to the journal. A flag's own name may be shown; a typed value
/// never.
fn render(error: &clap::Error) -> Rendered {
    let fixed = |line: String| Rendered {
        text: format!("tower: {line}\nsee `tower --help`\n"),
        status: EXIT_USAGE,
        to_stdout: false,
    };
    // The argument clap names for this error, when it is ours. For an unknown argument clap
    // puts what was typed in the same place, so it is read only for the kinds below.
    let our_argument = || match error.get(ContextKind::InvalidArg) {
        Some(ContextValue::String(arg)) => arg.clone(),
        Some(ContextValue::Strings(args)) => args.join(", "),
        _ => "an argument".to_owned(),
    };
    match error.kind() {
        ErrorKind::DisplayHelp
        | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        | ErrorKind::DisplayVersion => Rendered {
            text: error.render().to_string(),
            status: EXIT_SUCCESS,
            to_stdout: true,
        },
        ErrorKind::InvalidValue | ErrorKind::ValueValidation => {
            fixed(format!("invalid value for {}", our_argument()))
        }
        ErrorKind::MissingRequiredArgument => fixed(format!(
            "a required argument is missing: {}",
            our_argument()
        )),
        ErrorKind::ArgumentConflict => {
            fixed("arguments that cannot be used together were given".to_owned())
        }
        ErrorKind::UnknownArgument | ErrorKind::InvalidSubcommand => {
            fixed("unknown argument".to_owned())
        }
        _ => fixed("invalid command line".to_owned()),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(error) => {
            let rendered = render(&error);
            if rendered.to_stdout {
                print!("{}", rendered.text);
            } else {
                eprint!("{}", rendered.text);
            }
            return ExitCode::from(rendered.status);
        }
    };
    let (notes, mut received) = unbounded_channel();
    // Colour only on a terminal: journald and files get plain text. The fmt layer writes to
    // standard output.
    let log = tracing_subscriber::fmt::layer()
        .with_ansi(io::stdout().is_terminal())
        .with_filter(filter_fn(|metadata| shown(metadata.target())));
    let acme = AcmeWatch { notes }.with_filter(filter_fn(acme_filter));
    tracing_subscriber::registry().with(log).with(acme).init();
    tokio::spawn(async move {
        while let Some(note) = received.recv().await {
            log_acme_note(note);
        }
    });

    match run(args).await {
        Ok(()) => ExitCode::from(EXIT_SUCCESS),
        Err(error) => {
            eprintln!("tower: {error}");
            ExitCode::from(exit_status(&*error))
        }
    }
}

/// The exit status for an error that ended [`run`]: [`EXIT_CONFIG`] when the configuration
/// itself is wrong and starting again cannot help, [`EXIT_FAILURE`] otherwise.
fn exit_status(error: &(dyn Error + Send + Sync + 'static)) -> u8 {
    // Every error of the config file or the allow file: the file has to be changed. That
    // includes a file that cannot be read; systemd does not create it in the meantime.
    if error.is::<config::ConfigError>() {
        return EXIT_CONFIG;
    }
    let Some(error) = error.downcast_ref::<TowerError>() else {
        // An I/O error of the runtime or of a signal handler, a load task that did not finish.
        return EXIT_FAILURE;
    };
    // `TowerError` is `#[non_exhaustive]`, so the match needs a last arm; a variant added later
    // counts as a failure that may pass until it is placed here.
    match error {
        // Values of the configuration that the tower refuses: the same config gives the same
        // error at every start.
        TowerError::NonLoopbackBind(_)
        | TowerError::InvalidHostname
        | TowerError::InvalidReloadInterval
        | TowerError::AcmeNeedsDnsName
        | TowerError::InvalidAcmeContact
        | TowerError::InvalidAllowlistEntry { .. }
        | TowerError::InvalidClientRate(_)
        | TowerError::InvalidClientBurst => EXIT_CONFIG,
        // The ACME cache directory is not private, not the tower's own, not a directory, or
        // cannot be used: the directory or the `cache_dir` that names it has to be changed.
        TowerError::AcmeCacheNotPrivate
        | TowerError::AcmeCacheNotOwned
        | TowerError::AcmeCacheNotADirectory
        | TowerError::AcmeCacheUnusable(_) => EXIT_CONFIG,
        // A port that is taken, or certificate files in the middle of a renewal: these can
        // pass, so a service manager may start the tower again.
        TowerError::Bind { .. }
        | TowerError::CertificateFile { .. }
        | TowerError::KeyFile { .. } => EXIT_FAILURE,
        // The relay stopped by itself, or did not start or stop for another reason.
        TowerError::Stopped
        | TowerError::Start(_)
        | TowerError::MissingHttpAddress
        | TowerError::Shutdown(_) => EXIT_FAILURE,
        _ => EXIT_FAILURE,
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

/// Loads the config on the blocking pool, starts the tower, and logs its listeners' ports. The
/// log line names no host name, URL, address, id or path (hard rule 9).
async fn serve(path: PathBuf) -> Result<Tower, Box<dyn Error + Send + Sync>> {
    let config = tokio::task::spawn_blocking(move || config::load(&path)).await??;
    let certificate = Certificate::of(&config);
    let tower = Tower::spawn(config).await?;
    tracing::info!(
        target: LOG_TARGET,
        https = %port_label(tower.https_addr()),
        http = %port_label(Some(tower.http_addr())),
        qad = %port_label(tower.qad_addr()),
        access = %access_label(tower.listed()),
        certificate = certificate.label(),
        "tower listening"
    );
    if certificate.warns() {
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
    use std::fmt::Write as _;
    use std::io;
    use std::net::SocketAddr;
    use std::path::Path;

    use clap::Parser;
    use clap::error::ErrorKind;
    use numbers_tower::DEFAULT_BIND;

    use numbers_tower::{TlsTower, Tower, TowerConfig, TowerError};
    use tracing_subscriber::Layer;
    use tracing_subscriber::filter::filter_fn;
    use tracing_subscriber::layer::SubscriberExt;

    use super::{
        AcmeNote, AcmeWatch, Args, Certificate, Command, ERRORS, EXIT_CONFIG, EXIT_FAILURE,
        EXIT_USAGE, MessageStart, access_label, acme_filter, exit_status, port_label, render,
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
    fn serve_needs_a_config_file() {
        let error = Args::try_parse_from(["tower", "serve"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    /// What was typed, the program's own path included, never appears in what the binary prints
    /// for a command-line error; each error has its fixed line, on standard error, with exit
    /// status 2.
    #[test]
    fn command_line_errors_are_fixed_text_without_what_was_typed() {
        const MARKER: &str = "typed-host.example.invalid";
        let program = format!("/opt/{MARKER}/tower");
        let flag = format!("--{MARKER}");
        let address = format!("{MARKER}:443");
        let cases: [(Vec<&str>, &str); 5] = [
            (
                vec![&program, "--bind", &address],
                "tower: invalid value for --bind <BIND>",
            ),
            (vec![&program, MARKER], "tower: unknown argument"),
            (
                vec![&program, "serve", "--config", "x", &flag],
                "tower: unknown argument",
            ),
            (
                vec![&program, "serve", "--config", "x", MARKER],
                "tower: unknown argument",
            ),
            (
                vec![&program, "serve"],
                "tower: a required argument is missing: --config <CONFIG>",
            ),
        ];
        for (args, line) in cases {
            let error = Args::try_parse_from(&args).unwrap_err();
            let rendered = render(&error);
            assert_eq!(
                rendered.text,
                format!("{line}\nsee `tower --help`\n"),
                "{line}"
            );
            assert!(!rendered.text.contains(MARKER), "{line}");
            assert_eq!((rendered.status, rendered.to_stdout), (2, false), "{line}");
        }
        let error = Args::try_parse_from([
            program.as_str(),
            "--bind",
            "127.0.0.1:3340",
            "serve",
            "--config",
            "x",
        ])
        .unwrap_err();
        assert_eq!(
            render(&error).text,
            "tower: arguments that cannot be used together were given\nsee `tower --help`\n"
        );
    }

    /// Help is clap's own, for `tower` whatever the program's path, on standard output with
    /// exit status 0.
    #[test]
    fn help_names_the_program_tower() {
        const MARKER: &str = "typed-host.example.invalid";
        let program = format!("/opt/{MARKER}/not-tower");
        for args in [
            vec![program.as_str(), "--help"],
            vec![program.as_str(), "serve", "--help"],
        ] {
            let error = Args::try_parse_from(&args).unwrap_err();
            let rendered = render(&error);
            assert_eq!((rendered.status, rendered.to_stdout), (0, true));
            assert!(rendered.text.contains("Usage: tower"), "help names tower");
            assert!(
                !rendered.text.contains(MARKER),
                "help shows the typed program path"
            );
        }
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
        let addr: SocketAddr = "203.0.113.5:8443".parse().unwrap();
        assert_eq!(port_label(Some(addr)), "8443");
        assert_eq!(port_label(None), "off");
        let v6: SocketAddr = "[2001:db8::5]:443".parse().unwrap();
        assert_eq!(port_label(Some(v6)), "443");
    }

    /// The certificate label, and the staging warning decided from the source itself.
    #[test]
    fn the_certificate_label_and_the_staging_warning() {
        let files = TowerConfig::tls(TlsTower::new("tower1.example.com", "c.pem", "k.pem"));
        let acme = |production| {
            TowerConfig::tls(
                TlsTower::acme("tower1.example.com", "mailto:ops@example.com", "/c")
                    .production(production),
            )
        };
        for (config, label, warns) in [
            (files, "files", false),
            (acme(false), "acme staging", true),
            (acme(true), "acme production", false),
            (TowerConfig::dev(DEFAULT_BIND), "none", false),
        ] {
            let certificate = Certificate::of(&config);
            assert_eq!((certificate.label(), certificate.warns()), (label, warns));
        }
    }

    /// The matcher works when the message comes in pieces, and a prefix that matches only in
    /// part is no match.
    #[test]
    fn the_message_start_matches_across_pieces() {
        let found = |pieces: &[&str]| {
            let mut start = MessageStart::new(ERRORS);
            for piece in pieces {
                if start.write_str(piece).is_err() {
                    break;
                }
            }
            start.found()
        };
        assert_eq!(found(&["err", "or: Or", "der(x)"]), Some(0));
        assert_eq!(found(&["error: Cert", "Cache", "Load(..)"]), Some(1));
        assert_eq!(found(&["error: AccountCacheStore(..)"]), Some(2));
        assert_eq!(found(&["error: Ord"]), None);
        assert_eq!(found(&["error: Orders(x)"]), None);
        assert_eq!(found(&["error: CertCach"]), None);
        assert_eq!(found(&["error: CachedCertParse(..)"]), None);
        assert_eq!(found(&[""]), None);
    }

    /// Each kind of ACME event gives its note, through real tracing macros, and nothing of the
    /// events' text comes out.
    #[test]
    fn the_acme_layer_reports_each_kind_of_event() {
        #[derive(Debug)]
        enum EventOk {
            DeployedNewCert,
            DeployedCachedCert,
            CertCacheStore,
        }
        #[derive(Debug)]
        #[allow(dead_code)]
        enum EventError {
            Order(&'static str),
            CertCacheLoad(&'static str),
            CertCacheStore(&'static str),
            AccountCacheLoad(&'static str),
            AccountCacheStore(&'static str),
            CachedCertParse(&'static str),
            NewCertParse(&'static str),
            Ord(&'static str),
        }
        let (notes, mut received) = tokio::sync::mpsc::unbounded_channel();
        let layer = AcmeWatch { notes }.with_filter(filter_fn(acme_filter));
        let subscriber = tracing_subscriber::registry().with(layer);
        const RELAY: &str = "iroh_relay::server";
        const HOST: &str = "qzx-host.example";
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(target: RELAY, "acme");
            let entered = span.enter();
            tracing::error!(target: RELAY, "error: {:?}", EventError::Order(HOST));
            tracing::error!(target: RELAY, "error: {:?}", EventError::CertCacheLoad(HOST));
            tracing::error!(target: RELAY, "error: {:?}", EventError::CertCacheStore(HOST));
            tracing::error!(target: RELAY, "error: {:?}", EventError::AccountCacheLoad(HOST));
            tracing::error!(target: RELAY, "error: {:?}", EventError::AccountCacheStore(HOST));
            tracing::error!(target: RELAY, "error: {:?}", EventError::CachedCertParse(HOST));
            tracing::error!(target: RELAY, "error: {:?}", EventError::NewCertParse(HOST));
            tracing::error!(target: RELAY, "error: {:?}", EventError::Ord(HOST));
            tracing::debug!(target: RELAY, "acme event: {:?}", EventOk::CertCacheStore);
            tracing::debug!(target: RELAY, "acme event: {:?}", EventOk::DeployedNewCert);
            tracing::debug!(target: RELAY, "acme event: {:?}", EventOk::DeployedCachedCert);
            tracing::info!(target: RELAY, "acme event: {:?}", EventOk::DeployedNewCert);
            // Other targets in the same span. `acme_filter` lets the first through (it starts
            // the same way), so only the target check in `on_event` rejects it; the filter
            // rejects the second.
            let order = EventError::Order(HOST);
            tracing::error!(target: "iroh_relay::server::x", "error: {:?}", order);
            tracing::error!(target: "x::iroh_relay::server", "error: {:?}", order);
            drop(entered);
            // The same target outside the span, and in another span.
            tracing::error!(target: RELAY, "error: {:?}", EventError::Order(HOST));
            tracing::debug!(target: RELAY, "acme event: {:?}", EventOk::DeployedNewCert);
            let other = tracing::info_span!(target: RELAY, "relay");
            let _other = other.enter();
            tracing::error!(target: RELAY, "error: {:?}", EventError::Order(HOST));
        });
        let mut seen = Vec::new();
        while let Ok(note) = received.try_recv() {
            seen.push(note);
        }
        use AcmeNote::{CacheFailed, Deployed, OrderFailed, StepFailed};
        assert_eq!(
            seen,
            vec![
                OrderFailed,
                CacheFailed,
                CacheFailed,
                CacheFailed,
                CacheFailed,
                StepFailed,
                StepFailed,
                StepFailed,
                Deployed,
                Deployed,
            ]
        );
    }

    /// The systemd unit in `packaging/systemd/`, as shipped.
    const UNIT: &str = include_str!("../../../packaging/systemd/numbers-tower.service");

    /// The values of the `key=` lines in `[section]` of the unit, trimmed, read as systemd reads
    /// them for what this unit uses: blank lines and comment lines (`#`, `;`) are skipped, a
    /// `[name]` line starts a section, and a line that ends in a backslash fails the test
    /// (systemd would join it with the next line; the unit needs none).
    fn unit_values<'a>(unit: &'a str, section: &str, key: &str) -> Vec<&'a str> {
        let mut current = None;
        let mut values = Vec::new();
        for line in unit.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            assert!(
                !line.ends_with('\\'),
                "a continued line in the unit: {line}"
            );
            if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
                current = Some(name);
                continue;
            }
            if current != Some(section) {
                continue;
            }
            if let Some((name, value)) = line.split_once('=')
                && name.trim() == key
            {
                values.push(value.trim());
            }
        }
        values
    }

    /// The unit's `ExecStart` is a command line this binary accepts. The unit uses no quoting,
    /// escapes or specifiers, so splitting on white space is what systemd does with it.
    #[test]
    fn the_systemd_units_exec_start_parses() {
        let exec_start = unit_values(UNIT, "Service", "ExecStart");
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
            assert_eq!(
                unit_values(UNIT, "Service", key),
                vec![value],
                "[Service] {key}={value}"
            );
        };
        // The documented config keeps the ACME cache, with the tower's keys, in the state
        // directory; the tower requires mode 0700 when the cache is that directory itself.
        has("StateDirectory", "numbers-tower");
        has("StateDirectoryMode", "0700");
        // The binary exits with a failure when its relay stops by itself and with success on
        // SIGTERM: the first is restarted, a stop is not.
        has("Restart", "on-failure");
        // A wrong command line or configuration stays wrong: no restart after those statuses.
        has(
            "RestartPreventExitStatus",
            &format!("{EXIT_USAGE} {EXIT_CONFIG}"),
        );
        // The default listeners are on ports 80 and 443, and the dynamic user is not root.
        has("AmbientCapabilities", "CAP_NET_BIND_SERVICE");
        has("CapabilityBoundingSet", "CAP_NET_BIND_SERVICE");
    }

    /// The helper reads one section only.
    #[test]
    fn unit_values_reads_like_systemd() {
        let unit = "[Service]\nRestart=on-failure\n# Restart=no\n[X-Notes]\nRestart=always\n";
        assert_eq!(unit_values(unit, "Service", "Restart"), vec!["on-failure"]);
        assert_eq!(unit_values(unit, "X-Notes", "Restart"), vec!["always"]);
    }

    #[test]
    #[should_panic(expected = "a continued line in the unit")]
    fn unit_values_refuses_a_continued_line() {
        unit_values(
            "[Service]\nEnvironment=A=1 \\\nRestart=on-failure\n",
            "Service",
            "Restart",
        );
    }

    /// Every error of the config file or the allow file is a configuration error.
    #[test]
    fn a_config_file_error_exits_with_78() {
        let error = numbers_tower::config::parse("bogus = 1\n", Path::new("/")).unwrap_err();
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(error);
        assert_eq!(exit_status(&*boxed), EXIT_CONFIG);
    }

    fn status_of(error: TowerError) -> u8 {
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(error);
        exit_status(&*boxed)
    }

    /// A value of the configuration that the tower refuses is a configuration error.
    #[test]
    fn a_refused_config_value_exits_with_78() {
        assert_eq!(status_of(TowerError::InvalidHostname), EXIT_CONFIG);
        assert_eq!(status_of(TowerError::InvalidAcmeContact), EXIT_CONFIG);
        assert_eq!(status_of(TowerError::InvalidClientBurst), EXIT_CONFIG);
        let entry = TowerError::InvalidAllowlistEntry {
            position: 1,
            source: "not an id".into(),
        };
        assert_eq!(status_of(entry), EXIT_CONFIG);
    }

    /// An ACME cache directory that the tower refuses is a configuration error.
    #[test]
    fn a_refused_acme_cache_exits_with_78() {
        assert_eq!(status_of(TowerError::AcmeCacheNotPrivate), EXIT_CONFIG);
        assert_eq!(
            status_of(TowerError::AcmeCacheUnusable(
                io::ErrorKind::PermissionDenied
            )),
            EXIT_CONFIG
        );
    }

    /// A port that is taken and an unreadable certificate file can pass: status 1.
    #[tokio::test]
    async fn a_taken_port_or_a_missing_certificate_file_exits_with_1() {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let bind = taken.local_addr().unwrap();
        let Err(error) = Tower::spawn(TowerConfig::dev(bind)).await else {
            panic!("the port is taken");
        };
        assert!(
            matches!(error, TowerError::Bind { .. }),
            "a taken port is not reported as a bind error"
        );
        assert_eq!(status_of(error), EXIT_FAILURE);

        let dir = tempfile::tempdir().unwrap();
        let tls = TlsTower::new(
            "tower1.example.com",
            dir.path().join("missing-cert.pem"),
            dir.path().join("missing-key.pem"),
        );
        let Err(error) = Tower::spawn(TowerConfig::tls(tls)).await else {
            panic!("the certificate file is missing");
        };
        assert!(
            matches!(error, TowerError::CertificateFile { .. }),
            "a missing certificate file is not reported as a certificate file error"
        );
        assert_eq!(status_of(error), EXIT_FAILURE);
    }

    /// A relay that stopped by itself, and an error that is not the tower's, exit with 1.
    #[test]
    fn a_stopped_relay_or_another_error_exits_with_1() {
        assert_eq!(status_of(TowerError::Stopped), EXIT_FAILURE);
        let other: Box<dyn std::error::Error + Send + Sync> = Box::new(io::Error::other("x"));
        assert_eq!(exit_status(&*other), EXIT_FAILURE);
    }
}
