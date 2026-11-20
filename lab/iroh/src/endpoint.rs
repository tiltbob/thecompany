//! Section 5: building the endpoint.
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use iroh::dns::DnsResolver;
use iroh::endpoint::{Builder, PortmapperConfig, QuicTransportConfig, VarInt, presets};
use iroh::{Endpoint, NetReportConfig, RelayMap, RelayMode, RelayUrl, SecretKey};

use crate::Res;
use crate::gate::{Authorizer, Gate, HookLog};
use crate::lookup::{MapLookup, TowerLookup};

/// How the relay map is built (T5/T6 compare the variants).
#[derive(Debug, Clone)]
pub enum Relays {
    /// `RelayConfig::new(url, qad)` per tower: what the design prescribes.
    Configs(Vec<(RelayUrl, Option<u16>)>),
    /// `RelayMode::custom([url])`: bare URLs, QAD assumed on UDP 7842.
    BareCustom(Vec<RelayUrl>),
    /// `RelayMap::from(url)`: bare URL, QAD assumed on UDP 7842.
    BareFrom(RelayUrl),
    /// `RelayMap::empty()`: towers come later via `insert_relay`.
    Empty,
    /// M2: a map shared with the caller (and with `MapLookup` when `lookup` is None).
    Shared(RelayMap),
}

impl Relays {
    pub fn mode(&self) -> RelayMode {
        match self {
            Relays::Configs(t) => crate::relay::relay_mode(t),
            Relays::BareCustom(urls) => RelayMode::custom(urls.iter().cloned()),
            Relays::BareFrom(url) => RelayMode::Custom(RelayMap::from(url.clone())),
            Relays::Empty => RelayMode::Custom(RelayMap::empty()),
            Relays::Shared(map) => RelayMode::Custom(map.clone()),
        }
    }
    pub fn urls(&self) -> Vec<RelayUrl> {
        match self {
            Relays::Configs(t) => t.iter().map(|(u, _)| u.clone()).collect(),
            Relays::BareCustom(urls) => urls.clone(),
            Relays::BareFrom(url) => vec![url.clone()],
            Relays::Empty => Vec::new(),
            Relays::Shared(map) => map.urls(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EpConfig {
    pub secret: [u8; 32],
    pub relays: Relays,
    /// URLs the TowerLookup answers with (defaults to the relay URLs).
    pub lookup: Option<Vec<RelayUrl>>,
    pub loopback_only: bool,
    pub relay_only: bool,
    /// Explicit UDP port for the loopback bind (0 = random).
    pub port: u16,
    pub net_report: NetReportConfig,
    pub auth: Arc<Authorizer>,
    pub hook_log: Arc<HookLog>,
    pub alpn_aware_gate: bool,
    pub no_hooks: bool,
    /// M2: roots for the tower's TLS certificate (None = iroh's default, embedded WebPKI roots).
    pub ca_tls: Option<iroh::tls::CaTlsConfig>,
    /// M2: iroh's own resolver (None = system DNS without the public fallback).
    pub dns: Option<DnsResolver>,
}

impl EpConfig {
    pub fn new(secret: [u8; 32], relays: Relays, auth: Arc<Authorizer>) -> Self {
        Self {
            secret,
            relays,
            lookup: None,
            loopback_only: true,
            relay_only: false,
            port: 0,
            net_report: crate::relay::net_report_config(),
            auth,
            hook_log: Arc::default(),
            alpn_aware_gate: true,
            no_hooks: false,
            ca_tls: None,
            dns: None,
        }
    }
}

/// Stream caps for the link: lower than noq's 100 bidi + 100 uni.
pub fn transport_config() -> QuicTransportConfig {
    QuicTransportConfig::builder()
        .max_concurrent_bidi_streams(VarInt::from_u32(16))
        .max_concurrent_uni_streams(VarInt::from_u32(0))
        .build()
}

/// System DNS only, no Cloudflare/Google/Quad9 fallback.
pub fn dns_resolver() -> DnsResolver {
    DnsResolver::builder()
        .with_system_defaults()
        .disable_fallback()
        .build()
}

pub fn builder(cfg: &EpConfig) -> Res<Builder> {
    let mut builder = Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::from_bytes(&cfg.secret))
        .relay_mode(cfg.relays.mode());
    builder = match (&cfg.relays, &cfg.lookup) {
        (Relays::Shared(map), None) => builder.address_lookup(MapLookup::new(map.clone())),
        _ => builder.address_lookup(TowerLookup::new(
            cfg.lookup.clone().unwrap_or_else(|| cfg.relays.urls()),
        )),
    };
    builder = builder
        .portmapper_config(PortmapperConfig::Disabled)
        .net_report_config(cfg.net_report.clone())
        .transport_config(transport_config())
        .dns_resolver(cfg.dns.clone().unwrap_or_else(dns_resolver));
    if let Some(ca) = &cfg.ca_tls {
        builder = builder.ca_tls_config(ca.clone());
    }
    if !cfg.no_hooks {
        builder = builder.hooks(Gate {
            auth: cfg.auth.clone(),
            log: cfg.hook_log.clone(),
            alpn_aware: cfg.alpn_aware_gate,
        });
    }
    if cfg.loopback_only {
        // bind_addr replaces only the IPv4 default; [::] stays unless the IP transports are
        // cleared first. clear_ip_transports() then bind_addr() = exactly one socket on 127.0.0.1.
        builder = builder
            .clear_ip_transports()
            .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, cfg.port)))?;
    }
    if cfg.relay_only {
        builder = builder.clear_ip_transports(); // must come after bind_addr, or it is undone
    }
    Ok(builder)
}

pub async fn bind(cfg: &EpConfig) -> Res<Endpoint> {
    Ok(builder(cfg)?.bind().await?)
}
