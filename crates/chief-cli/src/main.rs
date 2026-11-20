//! The `chief` binary: the command-line frontend to a chief's station.
//!
//! Thin on purpose: it parses the arguments, works out the socket, starts a current-thread tokio
//! runtime and hands everything else to [`chief_cli::run`]. Commands, output and exit codes are
//! documented in the library.

use chief::client::{Client, SocketTransport};
use chief::paths::PathEnv;
use chief_cli::{Cli, Exit};
use clap::Parser;
use std::process::ExitCode;
use tokio::io::BufReader;

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            // clap prints help and version to stdout, usage errors to stderr.
            let _ = error.print();
            return Exit::for_parse_error(&error).into();
        }
    };
    let mut out = std::io::stdout();
    let mut err = std::io::stderr();
    let socket = match chief_cli::socket_path(cli.station.as_deref(), &PathEnv::read()) {
        Ok(path) => path,
        Err(message) => {
            let _ = chief_cli::say(&mut err, &message);
            return Exit::NoStation.into();
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = chief_cli::say(&mut err, &format!("cannot start: {error}"));
            return Exit::Failed.into();
        }
    };
    let client = Client::new(SocketTransport::new(socket));
    let input = BufReader::new(tokio::io::stdin());
    let exit = runtime.block_on(chief_cli::run(
        &cli.command,
        &client,
        input,
        &mut out,
        &mut err,
    ));
    // Not a plain drop: tokio reads stdin with a blocking read on a thread of its own, which no
    // one can cancel, and dropping the runtime would wait for it until the user pressed Enter.
    runtime.shutdown_background();
    exit.into()
}
