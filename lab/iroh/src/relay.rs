//! Section 3: relay map and net-report setting.
use iroh::{NetReportConfig, RelayConfig, RelayMap, RelayMode, RelayUrl};
use iroh_relay::RelayQuicConfig; // not re-exported by iroh: needs the iroh-relay dependency

/// One entry per tower: canonical URL and QAD port. None = no QAD (every http:// tower).
pub fn relay_map(towers: &[(RelayUrl, Option<u16>)]) -> RelayMap {
    towers
        .iter()
        .map(|(url, qad)| RelayConfig::new(url.clone(), qad.map(RelayQuicConfig::new)))
        .collect()
}

pub fn relay_mode(towers: &[(RelayUrl, Option<u16>)]) -> RelayMode {
    RelayMode::Custom(relay_map(towers))
}

/// Everything on, except the captive-portal check (a GET to port 80 of the tower host).
pub fn net_report_config() -> NetReportConfig {
    let mut net_report = NetReportConfig::default();
    net_report.captive_portal_check = false;
    net_report
}
