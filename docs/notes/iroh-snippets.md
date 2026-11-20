# iroh snippets for M1 (and M2)

> These snippets were read from the published sources of **iroh 1.5.0**, **iroh-relay 1.5.0** and
> **n0-dns-resolver 0.1.1** by design reviewers, and each finding was checked by a second reviewer.
> The code fences of sections 2 to 11 compile against iroh 1.5.0 and iroh-relay 1.5.0: a generator
> copies every line of them into a lab crate that builds with `--features verbatim`. It changes
> nothing but indentation: it moves `use` lines to module level and wraps the rest in functions and
> scaffolding that supply what a fence leaves out (free variables, our own types, and in section 7
> the imports). The measured numbers come from that lab's loopback tests (127.0.0.1, also inside a
> network namespace with only loopback), which exercise equivalent code, not these exact fences.
> Section 11 (M2) was measured the same way and its fences are generated into the same build; it
> marks what was not tested. Section 1's feature claims are still source reading only. The summary
> is the appendix of [../ARCHITECTURE.md](../ARCHITECTURE.md); this file is the code behind it.

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

## 11. M2: TLS tower, allowlist, rate limit, SRV, several towers
**Not M1.** Everything here was compiled and measured in the lab (`tests/m2.rs`, T20–T27, on
127.0.0.1, also inside a namespace with only loopback). Not tested: a relayed connection while QAD
runs (on one host, endpoints with sockets always go direct), a real network split between two
towers, the captive-portal probe, an SRV target of `"."`, ACME, and the system DNS configuration.

**TLS tower from certificate files.** The provider is passed explicitly; nothing installs a
process-wide default. Measured: `CryptoProvider::get_default()` was still `None` after every TLS
test, and nothing panicked.
```rust
use std::sync::Arc;
use iroh_relay::server::{CertConfig, DEFAULT_CERT_RELOAD_INTERVAL, QuicConfig, RelayConfig};
use iroh_relay::server::{Server, ServerConfig, TlsConfig, reloading_resolver};

let provider = Arc::new(rustls::crypto::ring::default_provider());
let server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_cert_resolver(                 // PEM files, re-read every 24 h: renewals need no restart
        reloading_resolver(&provider, cert_path, key_path, DEFAULT_CERT_RELOAD_INTERVAL).await?,
    );
let mut relay = RelayConfig::new(http_bind);  // with TLS still bound: serves /generate_204 only
// the HTTPS listener: /relay, /ping
relay.tls = Some(TlsConfig::new(https_bind, CertConfig::Manual { server_config }));
relay.access = Arc::new(Members::default());  // the allowlist, below
relay.limits.client_rx = Some(limit);         // the rate limit, below
let mut config = ServerConfig::default();
config.relay = Some(relay);
config.quic = Some(QuicConfig::new(qad_bind)); // QAD (UDP); inherits the relay's TLS config
let server = Server::spawn(config).await?;
```

- **Listeners.** HTTPS on `TlsConfig::https_bind_addr`. In TLS mode a plain-HTTP listener is always
  bound on `RelayConfig::http_bind_addr` (the required argument of `RelayConfig::new`; the library
  has no default, the iroh-relay binary uses `[::]:80`); it cannot be turned off. Measured: it
  answers `/generate_204` with 204 and `/ping`, `/relay`, `/healthz`, `/` with 404. Firewall:
  the HTTPS port, the HTTP port and the QAD port (UDP).
- **Check the certificate file yourself.** `reloading_resolver` fails at once on a missing,
  unreadable or non-PEM key file ("cert loading: reading key: ..."). A certificate file that holds
  no PEM certificate passes: the PEM reader skips what it does not know, the chain is empty and
  `Server::spawn` succeeds (found while building the tower, #70; not a lab experiment). The
  resolver also never checks that the key belongs to the certificate (source reading:
  rustls-cert-reloadable-resolver 0.7.1 does not call `CertifiedKey::keys_match`), and its error
  text comes from the PEM reader, which can quote a line of the file. The tower therefore does not
  use `reloading_resolver`: it loads and checks the pair itself and passes its own resolver to
  `with_cert_resolver` (#70). The rest of the snippet is what the tower does.
- ACME (`CertConfig::LetsEncrypt`) is tls-alpn-01 on the HTTPS port and needs `cache_path`, or
  every restart orders a certificate (source reading, not tested). `Server::spawn` cannot add
  HTTP routes. Build the tower URL from the host name, not the socket address.
- Measured: `Server::spawn` with TLS 1–2 ms; an endpoint that trusts the certificate came online
  over `https://127.0.0.1:<port>/` in 223–325 ms.

**Client side.** Stations trust the embedded WebPKI roots (`CaTlsConfig::default()`); a private CA
needs `CaTlsConfig::with_extra_roots` (not tested). Tests trust only their certificate:
```rust
use iroh::tls::CaTlsConfig;
// tower_cert: rustls::pki_types::CertificateDer<'static> (from rcgen in the tests)
let builder = builder.ca_tls_config(CaTlsConfig::custom_roots([tower_cert]));
```

- `CaTlsConfig::custom_roots([])` makes `bind()` fail: "Invalid CA root configuration <- no root
  trust anchors were provided".
- **A certificate the endpoint does not trust is silent.** The HTTPS latency probe fails TLS too,
  so no home tower is ever chosen. Measured with default roots against the self-signed tower:
  `home_relay_status()` stayed `[]` with no `last_error`, `online()` did not resolve in 12 s, and
  the tower saw 0 relay connections and 3 HTTPS connections (the probes).

**QAD.** It needs TLS: a `QuicConfig` without `relay.tls` fails `Server::spawn` with "Failed to bind
QAD listener <- TLS not configured". The relay map carries the QAD port per tower (section 3).
Without an unstable feature, the tower's counters show that QAD ran (`Counter::get` is inherent):
```rust
use iroh_relay::RelayQuicConfig;
let entry = iroh::RelayConfig::new(tower_url, Some(RelayQuicConfig::new(qad_port)));
let ran = server.metrics().server.qad_connections.get(); // also qad_incoming, qad_incoming_error
```

- Measured, bind → online: QAD on 223–317 ms (the tower counted one QAD connection per endpoint);
  QAD configured but the UDP port closed **3020–3030 ms** (the 3 s probe timeout, every first
  report); QAD off 224–271 ms. A relay-only endpoint, or one in a namespace with only loopback,
  ran no QAD and paid nothing.
- On loopback `ep.addr()` does not change (the observed address is the bound socket). With iroh's
  `unstable-net-report` (lab only) `ep.net_report()` showed `udp_v4 = true`, `global_v4` = the own
  socket and a `QadIpv4` probe next to `Https`.

**Allowlist.** `on_connect` runs once per relay connection, after the client proved its key; it sees
the id, headers, URI and query, not the IP. No shared bearer token: a hijacked SRV record would
collect it.
```rust
use std::{collections::HashSet, sync::RwLock};
use iroh::EndpointId; // not iroh_base: no direct dependency
use iroh_relay::server::{Access, AccessControl, ClientRequest};

#[derive(Debug, Default)]
pub struct Members(pub RwLock<HashSet<EndpointId>>);

impl AccessControl for Members {
    async fn on_connect(&self, req: &ClientRequest) -> Access {
        if self.0.read().is_ok_and(|ids| ids.contains(&req.endpoint_id())) {
            Access::Allow
        } else {
            // reason None reaches the client as "not authorized"
            Access::Deny { reason: Some("not a member of this network".into()) }
        }
    }
    // fn on_disconnect(&self, EndpointId, ConnectionId): defaulted, once per admitted connection
}
```

- **What a denied station sees.** `RelayStatus::auth_denied_reason()` is `Some(reason)`;
  `last_error()` says only "Failed to connect to relay server"; `online()` does not resolve while
  the denying tower is its home. It keeps retrying with the relay backoff (10 ms–16 s, jitter).
  Measured: 12–13 `on_connect` calls in 60 s, at about 0.3, 0.35, 0.4, 0.45, 0.6, 0.9, 1.3, 2, 4,
  8, 15, 34 and 51–60 s.
- **The list gates `/relay` only.** Measured: a denied station used the tower's QAD (3 QAD
  connections in 60 s) and its HTTPS `/ping` probe (that is how the tower became its home), and
  `/generate_204` answered 204. A listed station and an unlisted one could not reach each other
  through that tower in either direction (nothing within 10 s; neither handler ran).
- **A denying tower can capture a station.** With a denying and an admitting tower in its map, the
  station takes the lower-latency one as home whatever the list says, and the hysteresis keeps it.
  Measured: the denying tower became home in 2–5 of 6 stations per run (17 of 30), and they stayed
  denied for 45 s (one moved after 44.8 s). `remove_relay(denying)` brought each online through the
  other in 215–301 ms; `insert_relay` of the admitting tower while homed on the denying one did not
  help (2 of 15 online in about 220 ms, 4 after 46–50 s, 9 not within 60 s) (observed, not
  documented). What a station does on `auth_denied_reason()` is decided in #74: it marks the
  tower as denied and shows the reason. It takes the tower out of the relay map only if another
  tower of the set is not marked denied. If it is the only tower, or every tower denies, the
  station changes nothing in the map, does not nudge and does not ask DNS because of the denial.
  Putting a denied tower back and asking DNS again happen only when the set changes or at a long
  interval: the station limits its own attempts, not iroh's own retries, which go on.
- **Changing the list.** Removing an id does not end its open relay connection. The service handle
  does:
  ```rust
  if let Some(service) = server.relay_service() {
      service.clients().disconnect(id, None); // bool: was it connected; None = all its connections
      service.set_client_rate_limit(None);    // the rate limit can also change live
  }
  ```
  Measured: after `disconnect` the removed station showed not connected and `auth_denied_reason()`
  21–22 ms later (it reconnected and was denied); listed again, it was connected 20–44 ms later.

**Rate limit.** Per client, on what the tower reads from that client:
```rust
use std::num::NonZeroU32;
use iroh_relay::server::ClientRateLimit;
// bytes per second read from each client
let mut limit = ClientRateLimit::new(NonZeroU32::new(2 * 1024 * 1024).expect("non-zero"));
limit.max_burst_bytes = None; // #[non_exhaustive]: assign the field
```

- The sender sees only slowness: no error, no disconnect, no status change. Measured with 64 KiB/s
  and eight 60 000-byte echoes over a relayed connection: 13.5–13.9 s (34–35 KiB/s each way;
  the echo passes the limit twice), against 77–136 ms unlimited and 68–140 ms after
  `set_client_rate_limit(None)`. The tower counts `bytes_rx_ratelimited_total` and
  `conns_rx_ratelimited_total`; the client has only a warning in its log and the
  `relay_conns_ratelimited` counter behind iroh's `metrics` feature.
- Declared but not implemented in iroh-relay 1.5: `Limits::accept_conn_limit` and
  `accept_conn_burst` ("setting this has no effect"). Fixed in code (source reading): 30 s for the
  WebSocket upgrade, authentication and access check; 2 s server write timeout.

**SRV.** The bootstrap record is SRV: decision 13, settled in #38. `iroh::dns::DnsResolver` offers
only A/AAAA/TXT and re-exports nothing from n0-dns-resolver, hence the direct dependency. Two
resolvers are involved, and neither may fall back to public resolvers:
```rust
use std::net::SocketAddr;
use iroh::RelayUrl;
use n0_dns_resolver::{DnsProtocol, DnsResolver, Nameserver, SrvRecordData};

/// For the SRV lookup. A fresh n0-dns-resolver Builder reads nothing from the host and has an
/// empty fallback tier. Production would use `.use_system_config()` instead (not tested).
/// NOT `DnsResolver::system_with_fallback()`: Cloudflare, Google and Quad9 behind the system.
pub fn srv_resolver(ns: SocketAddr) -> DnsResolver {
    DnsResolver::builder()
        .nameserver(Nameserver::new(ns, DnsProtocol::Udp))
        .build()
}

/// iroh's own resolver, for the tower host names (relay dial, HTTPS probe, QAD).
/// NOT `iroh::dns::DnsResolver::with_nameserver(addr)`: it keeps the default fallback mode
/// (Deferred) and so adds the public resolvers behind the given one.
pub fn iroh_resolver(ns: SocketAddr) -> iroh::dns::DnsResolver {
    iroh::dns::DnsResolver::builder()
        .add_nameserver_config(iroh::dns::NameserverConfig::udp(ns.ip()).with_port(ns.port()))
        .disable_fallback()
        .build()
}

/// The canonical tower URL: no trailing dot.
pub fn tower_url(rec: &SrvRecordData) -> Option<RelayUrl> {
    let host = rec.target.trim_end_matches('.'); // targets arrive without the dot anyway
    if host.is_empty() {
        return None; // target "." means "not available" (not tested)
    }
    format!("https://{host}:{}/", rec.port).parse().ok()
}

pub async fn lookup(
    resolver: &DnsResolver,
    domain: &str,
) -> Result<Vec<RelayUrl>, n0_dns_resolver::Error> {
    let name = format!("_numbers._tcp.{}.", domain.trim_end_matches('.')); // FQDN
    let recs = match resolver.lookup_srv(name).await {
        Ok(recs) => recs,                                           // NODATA: Ok(vec![])
        Err(n0_dns_resolver::Error::NxDomain { .. }) => Vec::new(), // #[non_exhaustive]
        Err(e) => return Err(e),
    };
    let mut urls: Vec<RelayUrl> = recs.iter().filter_map(tower_url).collect();
    urls.sort();
    urls.dedup();
    Ok(urls)
}
```

- Measured against a DNS server on 127.0.0.1 (the resolver listed only that server, and the whole
  suite also passed in a namespace with only loopback): the answer is
  `SrvRecordData { priority: 10, weight: 60, port, target: "a.example.test" }` per record, target
  without the trailing dot. NXDOMAIN is `Err(Error::NxDomain { .. })` ("domain name does not exist
  (NXDOMAIN)"); an empty answer is `Ok(vec![])`. A server that does not answer gives
  `Err(Error::Timeout { .. })` ("request timed out") after 6.0 s and 6 queries. That is
  `UDP_NAMESERVER_TIMEOUT`, the budget for one UDP nameserver query: a constant, not a Builder
  setting. A lookup over several nameservers can take longer, so the caller needs its own
  deadline. Positive answers are cached for their TTL (source reading; measured: the second lookup
  sent no query, and after `clear_cache()` the next one did). Serve-stale is off unless
  `Builder::serve_stale` is set, and negative answers are not cached (`negative_max_ttl` enables
  it). The resolver logs each name and its records with `debug!`, so a binary that lets debug
  through for `n0_dns_resolver` writes the domain and the tower names to its log.
- Measured, the whole chain (SRV, tower URLs, an endpoint with `iroh_resolver` on the same server,
  TLS towers whose certificates name the targets): bind to online in 227–284 ms, with the SRV
  lookup done before the timed part; the endpoint sent only A and AAAA queries for the targets.
- **Trailing dot.** `https://a.example.test.:<port>/` also works (TLS verified against a
  certificate for `a.example.test`, online 225–270 ms), but it is a different `RelayUrl` from the
  form without the dot: two keys for one tower. Canonical form: no trailing dot, for SRV, `--tower`
  and tower sets alike; with the default `ndots = 1` a name with dots is tried as absolute first
  (source reading; system configuration not tested).
- Use `numbers::canonical_tower_url` and `TowerEntry::from_url` in production to apply the same
  validation to explicit, SRV, remembered and peer-supplied URLs.
- Priority and weight are ignored. SRV carries no QAD port, so SRV-located towers get QAD on UDP
  7842; a tower that does not serve it costs every endpoint that runs QAD 3 s at each first report
  (above). Relay-only endpoints and hosts with only loopback run no QAD and pay nothing.
- SRV only bootstraps. Tower sets passed on by peers are exchanged and shown, never used
  automatically (#73). Recovering a lost tower is the station watchdog of #74.

**Several towers.** One home tower at a time, chosen by latency (HTTPS probe, QAD if configured).
```rust
use iroh::{RelayMap, RelayMode};
let map = RelayMap::empty(); // shared state: the endpoint's insert_relay / remove_relay change it
let builder = builder.relay_mode(RelayMode::Custom(map.clone())); // a lookup can read `map` too
let previous = ep.insert_relay(tower_url.clone(), entry).await; // Some(old entry) if present
let removed = ep.remove_relay(&tower_url).await;
```

- **`home_relay_status()` lists the home tower only** (one entry; in the source it is an
  `Option<RelayStatus>`). A status for the other towers needs our own bookkeeping.
- Measured: with two towers up, bind → online median 264–423 ms; with one up and one down at start,
  420–495 ms. With both down and one up 3 s later, online 17.9–23.5 s after it was up; with
  `remove_relay` + `insert_relay` of it, 415–818 ms (observed, not documented).
- **Failover is slow.** After a successful report the next one plans no HTTPS probe:
  `ProbePlan::with_last_report` returns an empty plan when the last report had latencies, and an
  empty report keeps the old home (`handle_net_report_report`). Only the second report after the
  failure probes over HTTPS again. QAD probes have a plan of their own and run in every report,
  which is why towers with QAD moved sooner. Measured after the home tower stopped: connected to
  the other tower after
  41.0–48.7 s; with `network_change()` 1 s after the stop 41.9–49.1 s (no help); with
  `remove_relay` + `insert_relay` of the dead tower 1 s after the stop 1.22–1.44 s; with TLS towers
  and QAD (QAD probes run in every report) 22.0–27.5 s, and 44.9–46.5 s in a namespace where QAD
  does not run. Meanwhile the status shows the dead tower, not connected, `last_error` "Failed to
  connect to relay server". The station watchdog (decided in #74, to be built in #78) will call
  `nudge_towers` after the home has been missing or disconnected for some seconds. This re-inserts
  a configured tower to request a full report. The watchdog will repeat at a growing, capped
  interval while the home remains missing or disconnected.
- **Hysteresis.** A new home must measure under 2/3 of the old one's latency. After the stopped
  tower came back, home moved back to it within 50 s in 6 of 25 runs; on loopback this is noise.
- **An open relayed connection does not move.** When the tower carrying its selected path stopped,
  the connection kept that path and ended at the 30 s idle timeout in 20 of 20 runs, also when
  both sides were on the other tower 1.4 s later. A new dial after both had moved worked in
  9–31 ms; before that it failed.
- **Different home towers.** X with only A in its map and Y with only B connected in 41–225 ms when
  both lookups listed A and B (via B). A dialer that cannot reach the callee's home tower does not
  connect: towers do not forward to each other (approximated with a dead URL in place of B; a real
  network split was not tested).
- **Runtime changes** (observed, not documented). `insert_relay` returned in 0–4 ms and moved the
  home to the new tower when it measured faster (about half the runs); open connections were not
  disturbed. `remove_relay` of the home tower with another in the map: on the other in 220–244 ms.
  `remove_relay` of the **only** tower takes it out of the shared map, but the home connection
  and the status did not change: still connected, `online()` still resolves. `insert_relay` of a
  URL that is already there replaces its configuration and returns `Some(previous)`; the home
  connection and the status did not change either. A connection
  whose path runs through a removed tower kept working, and a new dial to a peer reachable only
  through that tower still connected (2–10 ms).
