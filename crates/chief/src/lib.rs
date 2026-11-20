//! `chief`: the shared frontend library.
//!
//! - [`api`]: the station API as plain data (requests, responses, events, errors) that crosses the
//!   local socket as JSON and, later, FFI. Its module docs hold the rules every station and frontend
//!   follows.
//! - [`client`]: the seam between a frontend and a station. [`client::Transport`] carries calls and
//!   subscriptions; [`client::Client`] gives them typed methods.
//!
//! This crate depends on `numbers-base` only, never on `numbers` or iroh: frontends hold no network
//! state. It installs no process-global state and starts no runtime.

pub mod api;
pub mod client;

pub use api::API_VERSION;
