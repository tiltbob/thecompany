//! Section 8: relay versus direct paths.
use std::time::Duration;

use iroh::endpoint::Connection;

/// A printable snapshot of a connection's paths (no addresses that matter outside 127.0.0.1).
pub fn describe(conn: &Connection) -> String {
    conn.paths()
        .iter()
        .map(|p| {
            format!(
                "[{}{}{} {}]",
                if p.is_relay() { "relay" } else { "" },
                if p.is_ip() { "ip" } else { "" },
                if p.is_selected() { " selected" } else { "" },
                p.remote_addr()
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Waits until the selected path is the relay. Ok(false) on timeout or close.
pub async fn wait_selected_relay(conn: &Connection, timeout: Duration) -> bool {
    let mut paths = conn.paths_stream();
    let fut = paths.wait_any(|p| p.is_selected() && p.is_relay());
    matches!(tokio::time::timeout(timeout, fut).await, Ok(Some(_)))
}

/// Waits until the selected path is a direct IP path.
pub async fn wait_selected_ip(conn: &Connection, timeout: Duration) -> bool {
    let mut paths = conn.paths_stream();
    let fut = paths.wait_any(|p| p.is_selected() && p.is_ip());
    matches!(tokio::time::timeout(timeout, fut).await, Ok(Some(_)))
}
