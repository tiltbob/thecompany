//! `chief`: the shared frontend library.
//!
//! - [`api`]: the station API as plain data (requests, responses, events, errors) that crosses the
//!   local socket as JSON and, later, FFI. Its module docs hold the rules every station and frontend
//!   follows.
//! - [`client`]: the seam between a frontend and a station. [`client::Transport`] carries calls and
//!   subscriptions; [`client::Client`] gives them typed methods.
//! - [`follow`]: [`client::Client::follow_log`], the attach order for a live view of the log.
//! - [`paths`]: the default data dir and the socket path, as pure functions.
//! - `socket` (Unix): the local socket's envelope and `SocketTransport`, its client.
//! - `fake` (cargo feature `fake`): `FakeStation`, an in-memory station for tests.
//!
//! This crate depends on `numbers-base` only, never on `numbers` or iroh: frontends hold no network
//! state. It installs no process-global state and starts no runtime; the socket transport spawns
//! one task per subscription on the caller's runtime.

pub mod api;
pub mod client;
#[cfg(feature = "fake")]
pub mod fake;
pub mod follow;
pub mod paths;
#[cfg(unix)]
pub mod socket;

pub use api::API_VERSION;
