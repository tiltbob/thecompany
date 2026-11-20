# iroh snippets for M1 (and M2)

> These snippets were read from the published sources of **iroh 1.5.0**, **iroh-relay 1.5.0** and
> **n0-dns-resolver 0.1.1** by design reviewers, and each finding was checked by a second reviewer.
> The code fences of sections 2 to 10 compile against iroh 1.5.0 and iroh-relay 1.5.0: a generator
> copies every line of them into a lab crate that builds with `--features verbatim`. It changes
> nothing but indentation: it moves `use` lines to module level and wraps the rest in functions and
> scaffolding that supply what a fence leaves out (free variables, our own types, and in section 7
> the imports). The measured numbers come from that lab's loopback tests (127.0.0.1, also inside a
> network namespace with only loopback), which exercise equivalent code, not these exact fences.
> Section 1's feature claims and section 11 are still source reading only. The summary is the
> appendix of
> [../ARCHITECTURE.md](../ARCHITECTURE.md); this file is the code behind it.

## 1. Cargo dependencies and features
Libraries take iroh without default features (`portmapper`, `metrics`, and `fast-apple-datapath`,
which uses private Apple APIs). `tls-ring` is required: without a TLS feature `presets::Minimal` does
not exist and `bind()` fails with `InvalidCryptoProvider`. Stack MSRV is 1.91.
```toml
[workspace.dependencies]
iroh       = { version = "1.5", default-features = false, features = ["tls-ring"] }
iroh-relay = { version = "1.5", default-features = false, features = ["tls-ring"] } # review had none; `server` implies it
n0-future  = "0.3"   # BoxStream for the AddressLookup
n0-error   = "1"     # e! macro for AcceptError
n0-dns-resolver = { version = "0.1", default-features = false }   # M2 (SRV)

# crates/tower/Cargo.toml (package numbers-tower): the only crate with the relay server
iroh-relay = { workspace = true, features = ["server"] }          # implies metrics and tls-ring
rustls     = { version = "0.23", default-features = false, features = ["ring"] }   # M2 (TLS)
```

Never enable `iroh/test-utils` (it compiles the relay server, axum and simple-dns in, and its test
relay needs `CaTlsConfig::insecure_skip_verify`, which exists only under it), `fast-apple-datapath`,
`portmapper` in a library, or `iroh-relay/server` outside `tower`; `chief`/`chief-cli` take no iroh.
No defaults also means no `Endpoint::metrics()`. CI runs `cargo check -p numbers` with exactly this
set (`--workspace` unifies features). Install a rustls provider only in a binary's `main` (M2 tower).

## 2. Plain-HTTP dev tower
`ServerConfig` and `iroh_relay::server::RelayConfig` are `#[non_exhaustive]`: start from defaults and
assign fields. With `tls = None` everything (`/relay`, `/ping`, `/generate_204`, `/healthz`) is on the
HTTP address. Defaults are `AllowAll` and no rate limit, so set `client_rx` even here.
```rust
use std::{net::Ipv4Addr, num::NonZeroU32};
use iroh::RelayUrl; // iroh-relay does not re-export it, and iroh-base is no direct dependency
use iroh_relay::server::{ClientRateLimit, RelayConfig, Server, ServerConfig};
let mut relay = RelayConfig::new((Ipv4Addr::LOCALHOST, 0)); // tls: None => plain HTTP
relay.limits.client_rx =                                    // bytes/s per client (plus burst)
    Some(ClientRateLimit::new(NonZeroU32::new(2 * 1024 * 1024).expect("non-zero")));
let mut config = ServerConfig::default();
config.relay = Some(relay);
let server = Server::spawn(config).await?;
let addr = server.http_addr().expect("relay enabled");      // https_addr() is None in this mode
let url: RelayUrl = format!("http://{addr}").parse()?;      // Tower::url(); http_url() is test-utils only
server.shutdown().await?;                                   // Err: SupervisorError; see below
```

`iroh::RelayConfig` (client map entry) is a different type; `accept_conn_*` limits are unimplemented.

After `shutdown()`, a client that connected just before may not notice (possibly because the
WebSocket upgrade runs in a detached task). Shut down right after a client's `online()`, the
client's `home_relay_status()` still reported it connected 3 s later (once for over 40 s) in 8 of
180 isolated runs and in 3 of 7 runs of the whole suite; whether that connection still relayed
anything was not tested. In 180 runs each, this was not observed when shutting down at least 1 s
after the last client connected, nor with the tower on its own Tokio runtime stopped with
`Runtime::shutdown_background()`; these are observations, not guarantees. A tower-restart test
should assert that the client noticed.

## 3. Relay map and net-report setting
`RelayMode::custom(urls)`, `RelayMap::from_iter(urls)`, `RelayMap::from(url)` and
`RelayConfig::from(url)` all assume QAD on UDP 7842. A net report plans QAD probes per IP family
(`src/net_report.rs`, `if_state.have_v4` / `have_v6`): IPv4 when an up interface has a non-loopback
IPv4 address, IPv6 when one has a global or unique-local IPv6 address. Against a plain-HTTP tower a
planned probe waits out the 3 s probe timeout. The lab host had IPv4 only: there `online()` took
3.02 s instead of 0.22 s (first report). A relay-only endpoint, or any endpoint in a network
namespace with only loopback, planned no QAD and paid nothing, so the cost depends on the host.
`RelayConfig::new(url, None)` behaves the same everywhere. `TowerSet` carries a QAD setting per
tower instead:
```rust
use iroh::{RelayConfig, RelayMap, RelayMode, RelayUrl};
use iroh_relay::RelayQuicConfig; // not re-exported by iroh: needs the direct iroh-relay dependency

/// One entry per tower: canonical URL and QAD port. None = no QAD (every http:// tower);
/// a bare https:// URL defaults to 7842 (RelayQuicConfig::default()).
pub fn relay_mode(towers: &[(RelayUrl, Option<u16>)]) -> RelayMode {
    let map: RelayMap = towers
        .iter()
        .map(|(url, qad)| RelayConfig::new(url.clone(), qad.map(RelayQuicConfig::new)))
        .collect();
    RelayMode::Custom(map)
}
```

This supersedes the first review's scheme-only helper (`"http" => RelayConfig::new(url, None)`,
`_ => RelayConfig::from(url)`) and the early sketch's `RelayMode::custom([url])`: a TLS + QAD test on
random ports, or a tower with UDP 7842 blocked, needs the port per tower. Without QAD a node learns
no public address, so the M1 dev tower gives direct paths only on one host or LAN. Tests must not
touch the network, so switch off only the captive-portal check:
```rust
use iroh::NetReportConfig; // also iroh::endpoint::NetReportConfig
let mut net_report = NetReportConfig::default();
net_report.captive_portal_check = false; // no GET http://<tower-host>/generate_204 on port 80
// Passed with Builder::net_report_config(net_report) (section 5).
// NOT NetReportConfig::minimal(): it also sets https_probes = false. With no QAD the HTTPS probe is
// the only latency source, so no home tower is ever chosen and online() never resolves.
```

Measured: with the captive-portal check off, `online()` 221–229 ms; `minimal()` was not online after
30 s, with `home_relay_status()` empty throughout.

## 4. TowerLookup
Answering every `EndpointId` with the tower URLs makes `ep.connect(id, ALPN)` work with no pkarr or
DNS: it runs on each connect while no path is selected, and the QUIC Initial goes to every tower.
Keep it even though our dials could pass an `EndpointAddr`: crates that dial by bare id need one.
```rust
use std::sync::Arc;
use iroh::{EndpointId, RelayUrl, TransportAddr};
use iroh::address_lookup::{AddressLookup, EndpointData, EndpointInfo, Error, Item};
use n0_future::{StreamExt, boxed::BoxStream, stream};
#[derive(Debug, Clone)]
pub struct TowerLookup {
    relays: Arc<[RelayUrl]>, // the same canonical list the RelayMap is built from
}

impl AddressLookup for TowerLookup {
    // publish(): default no-op; the tower routes by id.
    fn resolve(&self, id: EndpointId) -> Option<BoxStream<Result<Item, Error>>> {
        if self.relays.is_empty() {
            return None;
        }
        let data = EndpointData::from_iter(self.relays.iter().cloned().map(TransportAddr::Relay));
        let item = Item::new(EndpointInfo::from_parts(id, data), "tower", None);
        Some(stream::once(Ok(item)).boxed())
    }
}
```

When the set changes at runtime (M2), hold a clone of the shared `RelayMap` instead (its state is
behind an `Arc`, so `insert_relay` / `remove_relay` show up). The lookup always answers, so a dial to
an offline peer fails by timeout, never `NoAddress`. With several towers all are tried in parallel
and the callee answers over its home relay (current behaviour, not a contract): every node needs the
same set, SRV priority/weight are ignored, and each extra tower is one dead path per connection
(budget 8). Keep two or three; canonicalise URLs once, since `RelayUrl` equality is textual.
Measured: a dial by bare id connected in 17–34 ms; with the callee on tower A, the dialer homed on
tower B and the lookup listing a dead URL, B and A, it connected in 43–51 ms.

## 5. Building the endpoint
`presets::Minimal` adds no n0 relays or lookup, only the crypto provider. Pass no ALPNs when a
`Router` is used (section 7). The key reaches the library as bytes, never as a path.
```rust
use std::net::Ipv4Addr;
use iroh::{Endpoint, RelayMap, RelayMode, SecretKey, endpoint::{PortmapperConfig, presets}};
let mut builder = Endpoint::builder(presets::Minimal)
    .secret_key(SecretKey::from_bytes(&key_bytes))  // key_bytes: [u8; 32]
    .relay_mode(relay_mode(&towers))                // or RelayMode::Custom(RelayMap::empty())
    .address_lookup(lookup.clone())                 // section 4
    .hooks(Gate { auth: auth.clone() })             // section 6
    .portmapper_config(PortmapperConfig::Disabled)  // no UPnP/PCP/NAT-PMP; exported w/o the feature
    .net_report_config(net_report)                  // section 3
    .transport_config(stream_caps);                 // below
if cfg.loopback_only {
    // bind_addr replaces only the default IPv4 socket (0.0.0.0); the default [::] socket stays.
    // Clearing the IP transports first leaves exactly one socket, on 127.0.0.1.
    builder = builder.clear_ip_transports().bind_addr((Ipv4Addr::LOCALHOST, 0))?;
}
if cfg.relay_only {
    // After bind_addr, so it removes that socket too: no UDP socket, no QAD, no direct path.
    builder = builder.clear_ip_transports(); // online() still works
}
let ep = builder.bind().await?; // Result<Endpoint, BindError>; 3–5 ms, with or without a tower
```

- **`bind_addr`** is `fn bind_addr<A>(self, addr: A) -> Result<Self, InvalidSocketAddr> where
  A: ToSocketAddr, <A as ToSocketAddr>::Err: Into<InvalidSocketAddr>`. Without
  `clear_ip_transports()` first, `[::]` stays bound (upstream test `test_bind_addr_no_clear`; the
  lab host had no IPv6, so it showed only `127.0.0.1`). `clear_ip_transports()` after `bind_addr`
  undoes it: a relay-only endpoint with no socket. The default build binds `0.0.0.0` (and `[::]`).

- **Empty map first.** `bind()` with `RelayMap::empty()` succeeds (logs "skipping net_report, empty
  RelayMap"); the endpoint comes online once `ep.insert_relay(..)` adds a tower (upstream test
  `test_endpoint_online_add_relay`, which contradicts the doc comment saying a relay is required);
  `ep.remove_relay(..)` drops one. This is how `Station::start` returns without the network.
  Measured: online 216–222 ms after `insert_relay`, also when it follows `bind()` at once (60 runs).
- Stream caps: noq defaults to 100 bidi + 100 uni streams and a 30 s idle timeout; iroh sets
  keep-alive to 5 s. The Builder hook is `transport_config`. No 0-RTT.
  ```rust
  use iroh::endpoint::{QuicTransportConfig, VarInt};
  let stream_caps = QuicTransportConfig::builder()
      .max_concurrent_bidi_streams(VarInt::from_u32(16)) // example values
      .max_concurrent_uni_streams(VarInt::from_u32(16))
      .build(); // Builder::transport_config(stream_caps)
  ```
- The default DNS resolver falls back to Cloudflare, Google and Quad9; to avoid that:
  `.dns_resolver(DnsResolver::builder().with_system_defaults().disable_fallback().build())`.

## 6. The gate
The pre-handshake filter is the only thing that limits what a stranger learns; the `after_handshake`
hook is the one authorization point for every ALPN, both directions. Both consult the `Authorizer`.
```rust
use std::sync::Arc;
use iroh::endpoint::{AfterHandshakeOutcome, Connection, EndpointHooks, Incoming, IncomingAddr};
use iroh::protocol::{IncomingFilterOutcome, Router};

pub const LINK_ALPN: &[u8] = b"numbers/0";
pub const CLOSE_NOT_ALLOWED: u32 = 403; // reviews used 1 and 403; define all close codes in `numbers`
#[derive(Debug)]
struct Gate { auth: Arc<Authorizer> } // our type; must NOT hold an Endpoint (reference cycle)
impl EndpointHooks for Gate {
    async fn after_handshake<'a>(&'a self, conn: &'a Connection) -> AfterHandshakeOutcome {
        // Gate member-only ALPNs; numbers/join/0 (M3) goes to its own invite check. Shape from upstream
        // examples/auth-hook.rs (`conn.alpn() == ALPN || ..`); conn.alpn() returns &[u8].
        if conn.alpn() != LINK_ALPN || self.auth.contains(&conn.remote_id()) {
            AfterHandshakeOutcome::Accept
        } else {
            AfterHandshakeOutcome::Reject { error_code: CLOSE_NOT_ALLOWED.into(), reason: Vec::new() }
        }
    }
}

let auth_f = auth.clone();
let router = Router::builder(ep)
    .incoming_filter(Arc::new(move |inc: &Incoming| match inc.remote_addr() {
        IncomingAddr::Relay { endpoint_id, .. } if !auth_f.contains(&endpoint_id) => {
            IncomingFilterOutcome::Ignore // no answer at all
        }
        IncomingAddr::Ip(_) if !inc.remote_addr_validated() => IncomingFilterOutcome::Retry,
        _ => IncomingFilterOutcome::Accept,
    }))
    .accept(LINK_ALPN, link)
    .spawn();
```

- **ALPN-aware (correction):** the hook runs for dialed and accepted connections alike, so an M3
  roster gate on every ALPN would refuse the invitee, and the inviter on the invitee's side.
  Measured: on our own dial the hook runs with `conn.side() == Side::Client`. A gate that ignores
  the ALPN refused our dial on `numbers/join/0` to an id not in its list: `connect()` failed after
  about 20 ms with `ConnectError::Connecting { source: ConnectingError::LocallyRejected }`
  ("Connection was rejected locally"). The ALPN-aware gate let it through. On `numbers/0` the
  ALPN-aware gate likewise refuses our own dial to an id we have not added.
- **Types.** `AfterHandshakeOutcome::Reject { error_code: VarInt, reason: Vec<u8> }`
  (`403u32.into()` works). `IncomingFilter` is
  `Arc<dyn Fn(&Incoming) -> IncomingFilterOutcome + Send + Sync + 'static>`. `IncomingFilterOutcome`
  (`Accept`, `Retry`, `Reject`, `Ignore`) and `IncomingAddr` (`Ip`, `Relay { url, endpoint_id }`,
  `Custom`) are `#[non_exhaustive]`, so the `_` arm is required.
- **Hook limits:** TLS accepts any client key, and iroh registers the connection (queues the
  station's LAN, public and port-mapped candidates, honours REACH_OUT probes) before the hook runs.
- **Filter limits:** a relayed packet's source id is what the tower says, so a hostile tower (or SRV
  hijacker) passes the filter, handshakes, learns candidates and can trigger bounded UDP probes.
- `Ignore` looks like a timeout: the CLI says "appears offline, or has not added you". Keep a
  per-protocol check in the link; peer `id`/`sent_at` are untrusted. Test: the hook rejects an
  unknown id, and a relayed attempt from one gets no response at all.
- Measured: an ignored dialer's `connect()` fails after 30.0 s with "timed out" (the idle timeout);
  the filter ran once per retransmitted Initial (15 times), and neither the hook nor the handler
  ran. Let through the filter and rejected by the hook, the dialer's `connect()` succeeds (~20 ms),
  then the connection closes with code 403 and our reason about 15 ms later; `accept` never runs.

## 7. Router and ProtocolHandler rules
The link is one `ProtocolHandler` on `numbers/0`; these lifecycle rules decide how it is written.
```rust
// trait ProtocolHandler: Send + Sync + Debug + 'static; on_accepting(&self, Accepting) and
// shutdown(&self) are defaulted. accept returns impl Future<Output = Result<(), AcceptError>> + Send.
impl ProtocolHandler for Link {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let peer = conn.remote_id(); // infallible after the handshake
        // A refusal (rule 5) is `return refuse(&conn);` from here.
        while let Ok((send, recv)) = conn.accept_bi().await { /* StreamHeader, check, dispatch */ }
        // send: iroh::endpoint::SendStream, recv: iroh::endpoint::RecvStream
        Ok(()) // returning drops and closes the connection
    }
}

/// Called from `accept` to refuse: close with our code first, then return the error.
fn refuse(conn: &Connection) -> Result<(), AcceptError> {
    conn.close(CLOSE_NOT_ALLOWED.into(), b"not allowed");
    Err(n0_error::e!(AcceptError::NotAllowed)) // or AcceptError::from_err(..) (hidden meta)
}
```

1. `spawn()` (`#[must_use]`) calls `endpoint.set_alpns`, overriding builder ALPNs: our bind takes none.
2. Dropping the `Router` aborts the accept loop; `router.shutdown().await` (`Result<(), JoinError>`)
   also closes the endpoint, so peers see a close, not a timeout (code 0, within 2–11 ms with one
   open connection). Call it on Ctrl-C and shutdown.
3. When `accept()` returns the connection closes: loop on `accept_bi` until closed.
4. Streams are lazy: the peer's `accept_bi` returns only after the opener writes, so write the
   header right after `open_bi`.
5. A refusal in `accept` comes after the handshake: the dialer's `connect()` succeeds, then sees an
   application close. Returning the error alone gives the dialer close code 0 with an empty reason,
   the same as a normal close; our code reaches the dialer only if the handler calls
   `conn.close(code, reason)` before it returns the error (`refuse` above).

## 8. Relay versus direct paths
A connection usually starts on the relay and moves to a direct path, so "relayed" belongs to the
selected path. `conn.paths()` is a snapshot (`is_relay()`, `is_ip()`, `is_selected()`,
`remote_addr()`, `rtt()`); `conn.paths_stream().wait_any(pred)` waits; `conn.path_events()` streams
`PathEvent::{Opened, Closed, Selected, Lagged}` (feeds `Peer.link`); `ep.remote_info(id)` lists a
peer's known addresses. Relay-only test (both endpoints `relay_only`); the selected path is recorded
asynchronously, so wait under a timeout and check all paths (supersedes an early one-path sketch):
```rust
let conn = ep.connect(peer_id, LINK_ALPN).await?;
let mut paths = conn.paths_stream();
let selected_relay = paths.wait_any(|p| p.is_selected() && p.is_relay());
let on_relay = tokio::time::timeout(Duration::from_secs(10), selected_relay).await?.is_some();
assert!(on_relay);
assert!(conn.paths().iter().all(|p| !p.is_ip()));
```

Measured: the assertion holds, and still holds 3 s later. With IP sockets on both sides (127.0.0.1)
the connection starts on the relay and selects the direct path 33–47 ms after `connect()` started;
the relay path stays open but unselected. With one side relay-only it never goes direct.

## 9. Liveness facts
- `online()` resolves once a home tower handshake completes. It has **no timeout** and pends forever
  with no tower, no WAN or a denying tower: always wrap it; tests await the callee's `online()` under
  a timeout before dialing. Report state from `ep.home_relay_status()` (a watcher of
  `Vec<iroh::endpoint::RelayStatus>`; `iroh::Watcher` gives `get()` and `updated()`).
- Measured: `online()` took 216–236 ms on loopback. Under heavy parallel load an endpoint twice
  missed its first net report and came online only with the next one, after 21 and 24 s.
- A first home tower comes only from a net report (every 20–26 s, or when the relay map changes), so
  a station started before its tower can take that long. One home tower at a time, lowest latency
  with hysteresis; the relay actor reconnects forever and pings every 15 s.
- Measured, tower down at start: `home_relay_status()` stays empty, with no error, until a home
  tower is chosen; once the tower was up, `online()` followed 14–24 s later. `network_change()` did
  not help (19–23 s). `remove_relay` then `insert_relay` of the same tower brought it online in
  215–220 ms (observed behaviour, not documented).
- Measured, tower stopped after the endpoint was online: in the runs where the endpoint noticed
  (see section 2 for the runs where it did not), `connected` turned false 0–51 ms after
  `Server::shutdown()` was called, with `last_error` "Failed to connect to relay server". The
  reconnect backoff (10 ms–16 s) has jitter: gaps between attempts reached about 31 s. After a 1 s
  outage the endpoint was back 0.05–2 s after the tower; after a 35 s outage, 10–28 s after it.
- Keep-alive 5 s, idle timeout 30 s: after an outage over 30 s every connection is dead and
  `conn.closed()` triggers the re-dial; keep-alive defeats the idle timeout, so time out every frame
  read. A dial to an offline peer fails only by timeout (~30 s): wrap it (`connect_timeout`).
- Measured: a dial to an offline id failed after 30.0 s with "timed out"
  (`ConnectingError::ConnectionError` with `ConnectionError::TimedOut`); a peer killed without
  closing (SIGKILL, relay or direct path) was reported closed after 30.0 s; a healthy connection
  idle for 45 s still worked.
- Two processes on one key: the relay keeps only the newest connection active (the older one only
  logs a warning; no API shows it) and re-promotes the older when the newer leaves. Measured with
  two endpoints in one process: the newer got the dial while the older still reported itself
  connected; after the newer left, the older got the next dial (34–38 ms).
- `Endpoint::close` takes up to ~3 s, and UDP sockets close only when **every** `Endpoint` clone is
  dropped (router, handlers, caches), which the start → shutdown → start test needs. Measured with
  healthy peers: `router.shutdown()`, close included, took 2–11 ms. A rebind on the same port while
  one clone was alive failed ("Failed to bind sockets"); after the last drop, the first rebind still
  failed in about 6 of 45 runs and succeeded on a retry within 11 ms.

## 10. The data-dir lock
`Station::start` takes the lock first and holds it for the node's lifetime. `chief` reaches the
running station over the socket, and the station alone writes `peers.json`. `File::try_lock` is
stable since 1.89 (flock on Unix, so a second start in the same process is refused too). iOS kills
a suspended app that holds a file lock (0xdead10cc), so the iOS station stops before suspension.
```rust
use std::fs::{File, OpenOptions, TryLockError};
use std::{io, path::Path};

/// Held for the lifetime of the node; dropping the file releases the lock.
pub struct InstanceLock(#[allow(dead_code)] File);

pub fn lock_data_dir(dir: &Path) -> io::Result<InstanceLock> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("lock"))?; // reviews used "lock" and "node.lock"; any fixed name
    match file.try_lock() {
        Ok(()) => Ok(InstanceLock(file)),
        Err(TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "another station is already running with this data dir (one station per identity)",
        )),
        Err(TryLockError::Error(e)) => Err(e),
    }
}
```

## 11. Later (M2): TLS tower, allowlist, SRV
**Not M1.** With `relay.tls` set, `Server::spawn` also binds plain HTTP serving only `/generate_204`
(clients probe port 80 whatever the URL says). ACME is tls-alpn-01 only, validated on TCP 443, and
needs `cache_path` or every restart orders a certificate. QAD inherits the relay TLS config (none:
`TlsNotConfigured`). Open 443/tcp, 80/tcp, 7842/udp. Install ring as default in the tower's `main`.
```rust
use std::sync::Arc;
use iroh_relay::server::{AcmeConfig, CertConfig, DEFAULT_CERT_RELOAD_INTERVAL, QuicConfig,
    RelayConfig, Server, ServerConfig, TlsConfig, reloading_resolver};

let provider = Arc::new(rustls::crypto::ring::default_provider());
let tls_builder = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_safe_default_protocol_versions()?
    .with_no_client_auth();
let cert = match source {
    CertSource::Files { cert, key } => CertConfig::Manual {   // renewals picked up without restart
        server_config: tls_builder.with_cert_resolver(
            reloading_resolver(&provider, cert, key, DEFAULT_CERT_RELOAD_INTERVAL).await?,
        ),
    },
    CertSource::Acme { hostname, contact, cache_dir, production } => CertConfig::LetsEncrypt {
        acme_config: AcmeConfig::letsencrypt(production)
            .domains(vec![hostname]).contact(vec![contact]).cache_path(cache_dir),
        server_config_builder: tls_builder,
    },
};
let mut relay = RelayConfig::new(http_bind);        // 0.0.0.0:80
relay.tls = Some(TlsConfig::new(https_bind, cert)); // 0.0.0.0:443: /relay, /ping, ACME
let mut config = ServerConfig::default();
config.relay = Some(relay);
config.quic = Some(QuicConfig::new(qad_bind));      // 0.0.0.0:7842 UDP
let server = Server::spawn(config).await?;
```

Build the tower URL from the hostname, not the socket address. Stations trust WebPKI roots; a private
CA needs `CaTlsConfig::with_extra_roots`, and the CI test trusts its self-signed certificate with
`CaTlsConfig::custom_roots`. `Server::spawn` cannot add HTTP routes.

**Allowlist.** `on_connect` runs once per relay connection after the client proved its key and sees
the id, headers, URI and query, not the IP. `on_disconnect` allows advisory duplicate-id bookkeeping;
`Server::relay_service()` offers `set_client_rate_limit` and `clients().disconnect(id, ..)`. No shared
bearer token: a hijacked SRV record would collect it. A denied station sees
`RelayStatus::auth_denied_reason()`, and its `online()` never resolves.
```rust
use std::{collections::HashSet, num::NonZeroU32, sync::{Arc, RwLock}};
use iroh_base::EndpointId;
use iroh_relay::server::{Access, AccessControl, ClientRateLimit, ClientRequest, RelayConfig};

#[derive(Debug, Default)]
struct Members(RwLock<HashSet<EndpointId>>);

impl AccessControl for Members {
    async fn on_connect(&self, req: &ClientRequest) -> Access {
        let allowed = self.0.read().expect("lock").contains(&req.endpoint_id());
        if allowed { Access::Allow } else { Access::Deny { reason: None } }
    }
}

let mut relay = RelayConfig::new(bind_addr);
relay.access = Arc::new(Members::default());
relay.limits.client_rx =
    Some(ClientRateLimit::new(NonZeroU32::new(2 * 1024 * 1024).expect("non-zero")));
```

**SRV.** `iroh::dns::DnsResolver` offers only A/AAAA/TXT and iroh-dns re-exports nothing from
n0-dns-resolver, hence the direct dependency. `SrvRecordData { priority, weight, port, target }`.
```rust
use iroh::RelayUrl;
use n0_dns_resolver::{DnsResolver, Error, SrvRecordData};

pub async fn lookup(domain: &str) -> Result<Vec<RelayUrl>, LocateError> {
    let name = format!("_numbers._tcp.{}.", domain.trim_end_matches('.')); // FQDN: no search list
    let recs: Vec<SrvRecordData> = match DnsResolver::system_with_fallback().lookup_srv(name).await {
        Ok(recs) => recs,                          // NODATA is Ok(vec![])
        Err(Error::NxDomain { .. }) => Vec::new(), // non_exhaustive: `{ .. }` required
        Err(err) => return Err(LocateError::Dns(err)),
    };
    let mut urls: Vec<RelayUrl> = recs
        .iter()
        .map(|r| (r.target.trim_end_matches('.'), r.port))
        .filter(|(host, _)| !host.is_empty())     // target "." arrives as "": not available
        .filter_map(|(host, port)| format!("https://{host}.:{port}").parse().ok())
        .collect();
    urls.sort();
    urls.dedup();
    if urls.is_empty() { return Err(LocateError::NoTower); }
    Ok(urls)
}
```

- Query `_numbers._tcp` (the first review used an older label). Build `https://{target}.:{port}` once
  and use it everywhere (`RelayUrl` compares textually; `url` drops port 443); leave user `--tower`
  URLs untouched; unit-test this one function.
- Priority and weight are ignored; SRV-located towers are always `https`, QAD on UDP 7842.
- `system_with_fallback()` asks Cloudflare, Google and Quad9 when system DNS fails, so a split-horizon
  zone looks like NXDOMAIN: build without the fallback or document it. On Apple it reads SCDynamicStore.
- SRV only bootstraps: a running station applies tower-set changes with `insert_relay` /
  `remove_relay`, and `TowerLookup` reads the same shared `RelayMap`.
