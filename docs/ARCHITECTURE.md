# theCompany — architecture

The design of theCompany and its p2p network, `numbers`. The build order, the tracking issues
and the open decisions are in [ROADMAP.md](ROADMAP.md).

## Context

**theCompany** is the project; **`numbers`**
is its p2p network, giving a small self-hosted team **chat, sockets, files, locks** and a
**persistence layer** for working with coding agents.

The cast:

| Name | What it is |
|---|---|
| **chief** | A person on the team. An identity, not a machine: a chief owns stations, signs admin actions, and harness accounts and agent sessions run under their name. |
| **station** | A chief's node: a daemon on the chief's workstation. The `chief` CLI, the Linux GUI and the iOS app are frontends to a station. |
| **safehouse** | A node that runs agents in arbitrary harnesses (Claude Code and Codex first). At first a chief sets one up by hand. |
| **handler** | Later: the node that sets safehouses up, creating each in an isolated environment (an nspawn container, maybe a full QEMU VM). To be fleshed out. |
| **tower** | An untrusted node: relays traffic, lets nodes find each other, and is the final store for encrypted blocks. A DNS SRV record leads a newcomer to its first tower. Never a member. |
| **agent** | A harness process on a safehouse. Agents run nowhere else. |
| **airspace** | The persistence layer: text objects, git repos, occasional multi-gigabyte media. Every node takes part. |

Decided: **Rust**; **iroh** as transport (1.0 since June 2026, now 1.5.0: stable API and wire
protocol, dial-by-public-key QUIC, hole punching, relay fallback, official iOS support); the audience
is a **small trusted team** — chiefs are co-equal, every action is attributable to a person, one
person can be removed cleanly, everyone signs in to harnesses with their own account; work is
**tracked in GitHub issues**; in airspace, **every tower keeps every block and a write counts as
stored once two towers confirm it**; a station and a safehouse on one machine is **never suggested
and always warned against, but the two packages do not conflict**; the **SRV record only bootstraps
access — a node already on the network finds other nodes through the network first and falls back
to SRV last**; a network can **require a Nitrokey HSM to set up new nodes — and only for that, never
for day-to-day use**. We build feature by feature. The first slice is a minimal tower and station,
just enough for two chiefs to chat.

How far this has been checked: the transport, tower, login and first-slice parts were reviewed
against the published sources of iroh 1.5.0, Claude Code 2.1.328 and Codex 0.170, with a second
reviewer trying to refute each finding. The chief/station identity model and the first slice had one
further review after the rewrite, without the refutation pass. Airspace's transfer and git parts
were researched against current sources; its encryption format, naming rules and garbage collection
have **not** had that scrutiny yet and get a design pass at the start of M4. The M0 `numbers`
dependency set builds, and loopback tests exercise the M1 dev tower.

## Architecture

```
 chief ── `chief` CLI / GUI ──local socket── station ─┐                         ┌─ safehouse ── agents
 (a person)                                           ├── E2E-encrypted QUIC ───┤   (claude, codex, …)
 chief ── iOS app (station built in) ─────────────────┘  direct when possible   └─ safehouse ── agents
                         └────────── relayed through a tower when not ──────────┘
 tower: forwards ciphertext by node id; keeps encrypted blocks; never a member (SRV finds the first one)
```

| Node | Binary | Trust | Does | Never |
|---|---|---|---|---|
| tower | `tower` | untrusted with content; relied on, with checks, to keep blocks | relays ciphertext; rendezvous by node id; stores encrypted blocks | reads traffic or stored data, holds member or airspace keys, runs agents |
| safehouse | `safehouse` | member, admitted by a chief | hosts agents, terminals and harness logins; serves files, sockets, locks; keeps working copies of airspace | admin actions; starts anything on a station |
| station | `station` | member, commissioned by its chief | its chief's presence on the network: chat, management, driving safehouses; keeps working copies of airspace | spawns agents; executes anything for a peer |
| handler (later) | `handler` | member, admitted by a chief | creates and removes safehouses in isolated environments on a chief's request | runs agents itself; admin actions |

A chief can have several stations (desktop, laptop, phone). Safehouses belong to the team, not to one
chief.

### Identity and trust

- **Node key** (ed25519) — one per station, safehouse and tower. Its public half is the node's id and
  address (iroh `EndpointId`); iroh mutually authenticates and end-to-end encrypts every connection
  with it. The daemon holds it (0600 file; Keychain on iOS).
- **Chief** — a stable id (the hash of the statement that admitted them), not a key. A chief acts
  through **admin keys**: one per device they administer from, created there and never copied. On a
  workstation it is a passphrase-protected file kept by the frontend, not by the station; on the
  phone, a Keychain item behind biometrics. Admin keys sign every admin action, so each change names
  a person, and they commission that chief's stations. A station on a device without an admin key
  does everything except administer. Losing a device costs one key revocation and one station
  decommission; the chief, their accounts on safehouses and their harness logins stay (**decision 1**).
- **What a station may do** is act as its own chief on each safehouse: that chief's terminals,
  sessions, files and chat. Anything that changes what other chiefs' sessions run, or what a node
  allows others — shared harness definitions, forward-target policy, grants, the tower set, login URL
  templates — is an admin-signed statement. So a stolen station is that chief on every safehouse until
  it is decommissioned, but cannot change membership or shared configuration.
- **What the split protects.** The admin key at rest (a stolen disk or backup, a leaked station data
  dir) and against a bug that leaks station memory. It does not protect against code running as the
  chief's own OS user, which can wait for the next unlock.
- **Hardware token (Nitrokey HSM).** A network can be created with **enrollment
  requires the HSM**. Setting up a new node then needs a second signature from an *enrollment key*
  that lives only on a Nitrokey HSM: admitting a chief, commissioning a station, admitting a
  safehouse (later a handler), adding an admin key — and changing this rule. The chief's ordinary
  admin key still signs first, so every enrollment names a person. The frontend signs through
  PKCS#11; the key never leaves the token, signing needs the token and its PIN, and the roster can
  demand the token's own proof that the key was generated inside it.
  **Nothing else needs the token.** Not daily use — chat, airspace, driving safehouses, logins,
  sessions — and not routine administration either: removing a node or a chief, settings, shared
  harness definitions and issuing an invite are signed by the ordinary admin key alone. The token
  can stay in a drawer between enrollments, and a compromised node can be removed without fetching
  it.
  Two limits: node keys cannot live on the token, because iroh needs the raw key and the token has
  no Ed25519, so the HSM backs *who may enroll*, not each node's transport key; and the token signs
  with ECDSA P-256, so roster signatures carry an algorithm tag. A phone cannot enroll under this
  rule. How many tokens a team has is part of **decision 1**.

Membership is a **roster**: a hash-linked log (a DAG — each statement names its parents) of
statements signed by admin keys. The network id is the hash of genesis. Nobody shares a secret, so
removing a person needs no rotation. Authorization everywhere is remote node id → roster → "station
of chief *C*" or "safehouse" → policy for that protocol, default deny.

Rules the membership milestone (M3) implements:

- **Statements:** genesis; admit/remove chief; add/revoke admin key; commission/decommission station;
  admit/remove safehouse; invite; tower set (each tower's URL and node id); settings. A node's entry
  carries an encryption public key, which airspace needs later. Statements carry no display names,
  because towers receive the roster; names travel between members.
- **Who may sign what:** only a chief's own admin keys add that chief's admin keys and commission
  that chief's stations. Any chief may admit a chief or a safehouse and may remove anything. A
  decommissioned station id never comes back.
- **Invites:** the ticket's secret seeds a throwaway node key. The signed invite lists that join id
  and an expiry, and towers and nodes let unexpired join ids through for joining only. The joiner
  dials an inviter's station with it, so the handshake proves the secret, and sends a request signed
  by its new admin key naming its first station. The inviter's frontend shows a fingerprint over the
  whole request; the chief compares it out of band and signs one admit that embeds it. Only a signed
  admit consumes an invite; failed attempts do not.
- **Removal is strong:** statements by a removed chief that the removal did not already know about
  are void, with everything that depends on them. `chief remove` lists what that chief admitted or
  commissioned so the subtree can go in the same action. A signer picks its own parents, so "who was
  first" cannot settle two chiefs removing each other (**decision 2**).
- **Removal suspends at once and purges later.** Suspending closes that chief's links, terminals and
  forwards, stops their sessions and locks their harness profiles. Purging (vendor sign-out,
  deletion) waits until the removal is settled: acknowledged by a station of every remaining chief,
  or older than a grace period, or forced with `chief purge`. `chief network` lists nodes that have
  not acknowledged it. A chief-signed staleness limit makes a safehouse that has heard from no other
  chief for too long refuse privileged calls.
- **Sync:** every link opens by comparing roster heads and syncs before anything privileged.
- **Encoding:** signed statements are fixed binary bytes, never JSON, stored and forwarded verbatim,
  in one envelope (domain tag, id, sequence, parents, signer, algorithm) that airspace names reuse
  later. A node that meets a statement type it does not know stops and reports "upgrade required".

M1 has none of this yet: a station keeps a manual peer book (name → station ids), and "chief" is just
the name you give the person behind a station.

### Towers

A tower embeds an `iroh-relay` server. Every node keeps a connection to one tower; a node is dialed
by id through the towers, and iroh moves the connection to a direct path when it can.

- **SRV only bootstraps.** A newcomer resolves `_numbers._tcp.<domain>` (SRV) to
  `https://target:port` to reach its first tower; `--tower <url>` does the same by hand for
  development and tests. A node that is already on the network finds towers and other nodes through
  the network first, in this order: paths and addresses it already holds; what the network tells it —
  the tower set (remembered across restarts, passed on by peers, and from M3 a signed roster
  statement carried in invites) and other nodes' signed address records, which any connected node
  can pass on; then every tower in the known set; and only then the SRV record again. A changed SRV
  record therefore matters to newcomers and to a node that has lost every tower, not to a running
  network.
- **What a tower can do.** It sees who talks to whom, from which IP, when and how much, and (with the
  airspace) which node keeps or fetches which block. It can drop traffic and lose blocks. It, or whoever
  hijacks the SRV record, can forge the source id of a relayed packet far enough to learn a node's
  candidate addresses and make it send a few UDP probes. It cannot read, alter or impersonate member
  traffic or stored data, and can never become a member. DNSSEC or pinning would protect availability
  and metadata only.
- **Same set everywhere.** With no better address, dialing tries every tower in the set at once, so
  all nodes must use the same small set (two or three). SRV priority and weight are ignored.
- **Not an open relay.** A production tower admits only listed node ids and rate-limits each client:
  a static list in M2, fed from the roster in M3 (verifiable against the genesis hash, no secret).
  This is hygiene run by an untrusted party, not a security boundary.
- **Its own endpoint.** The relay server cannot host extra routes, so from M3 a tower also runs an
  iroh endpoint with its own node key. Members reach its services there, authenticated by node id:
  the roster feed first, storage from M4. Towers also use it to pull blocks from each other.
- **Direct paths across NATs need the production tower.** Address discovery (QAD, UDP 7842) requires
  TLS. M1's plain-HTTP dev tower relays, and gives direct paths only on the same host.

### Link and protocols

Two members share **one authenticated link** (ALPN `numbers/0`). It opens with a hello (version,
network id, roster head); each feature is then a typed stream whose first frame is `{proto, v}` —
`chat`, `roster`, `airspace`, `pty`, `sock`, `agent`, `login`, `file`, `lock`. One gate decides, default
deny, whether this caller may open this protocol:

| Caller → callee | Allowed |
|---|---|
| station → safehouse | everything inside the caller's chief's own account; shared configuration only as admin-signed statements |
| station → station | chat, roster sync, airspace blocks and names |
| safehouse → station | chat, roster sync, airspace blocks and names, answers on streams the station opened, file offers into quarantine |
| safehouse → safehouse | airspace blocks and names; anything else only under an admin-signed grant naming both |
| member → tower | relay; on the tower's own endpoint: roster feed, block storage, leases |
| tower → tower | pulls blocks it should hold from towers in the signed set |

`numbers/join/0` (M3) is the only surface open to non-members: invite redemption and bounded delivery
of roster proofs. Control messages are length-prefixed JSON frames (size-capped, read deadlines,
unknown fields tolerated); bulk data is raw stream bytes. One link instead of one ALPN per feature
keeps authorization in one place and hides from the tower which feature is in use.

### Stations and the chief's frontends

The station is the node; frontends hold no network state.

- **One station per identity.** A tower keeps only the newest connection per node id, so two
  processes sharing a key silently steal each other's traffic. The station takes an exclusive lock on
  its data dir, starts without waiting for the network and reports connectivity as state.
- **Local API.** A Unix socket (0700 directory, 0600 socket, peer uid checked, never TCP) carrying
  length-prefixed JSON frames. Every operation is one of three shapes: a call; a subscription (live
  events; a client that falls behind gets `Resync` and re-reads snapshots); or a byte stream (a
  header frame, then raw bytes both ways — terminals, forwards, files).
- **`chief`, the shared frontend library.** The station API as plain data that can cross FFI
  (strings, integers, bytes, own records and enums; ids, paths and URLs as strings; one error enum)
  plus a client. It runs against the socket (CLI, GUI) or against a station core in the same process
  (iOS app, tests). No process spawning, no iroh. The `chief` CLI, the GTK GUI (relm4, with VTE fed
  remote terminal bytes) and the iOS app (UniFFI → Swift) are thin layers over it.
- **The admin key stays in the frontend.** The frontend builds the statement itself from the user's
  command and shows what will be signed; it never signs bytes or a description the station supplied.
  The key file never crosses the socket.
- **A station takes input from peers, never orders.** It strips control characters from every
  peer-supplied string before a frontend sees it, never opens a URL or file on a peer's say-so,
  accepts files only as answers to its own pulls or as quarantined offers, and offers no reverse
  forwards.
- **iOS.** There are no daemons, so the app embeds the station core and is a foreground-only station
  that stops before the app is suspended. Messages for a phone wait in airspace.

### Airspace (persistence)

The requirement: a persistence layer, mostly for **text objects and git repos**, occasionally
**multi-gigabyte media**; every node type takes part, and **towers are the final store, holding
encrypted blocks**.

- **Members seal, towers keep.** A station or safehouse encrypts content before it leaves the node.
  What travels and rests everywhere is the same ciphertext block, addressed by the BLAKE3 hash of its
  bytes, so any node, a tower included, can verify a block without being able to read it. Small
  things (text objects, git data) are packed into blocks of a few MiB; large files are cut into
  blocks and can be resumed, streamed and read by range. The tower binary contains no sealing or key
  code, and CI checks that.
- **Durability (agreed).** Every tower keeps every block. A write counts as stored once two towers
  confirm it; a tower that was down catches up afterwards. The writer keeps its copy until then and
  airspace reports "not yet durable" (always, with a single tower). A member uploads once and the
  other towers pull from that tower. A confirmation is only a claim: blocks are checked by hash on
  every read, and an always-on safehouse spot-checks towers with random range requests and has missing
  blocks refilled.
- **Stations and safehouses** hold working copies — what they wrote, what they use, what they are told
  to keep — and serve blocks to each other when that is faster than a tower. They do not count
  toward the two-tower rule.
- **Keys.** Each *space* (a named collection; one team-wide space to start) has a key, wrapped to the
  encryption key of every member node. Removing a person changes the key for new writes; what they
  could already read stays readable to them. Agents never hold airspace keys: the safehouse reads and
  writes for them.
- **Names.** "The current version of this document" or "this branch" is a small signed record naming
  its predecessor, its sequence number and the roster state it was signed under. Towers hold these
  too and answer compare-and-swap, but are not trusted to order anything: members remember the newest
  record they have seen, refuse to go back, compare notes directly, and settle a fork by a fixed rule
  that keeps the loser under a conflict name (**decision 6**).
- **Garbage collection.** A tower cannot see what refers to what, so members renew leases on what is
  in use, and a tower drops a block only after its lease has lapsed and a grace period has passed.
  Towers enforce a quota per network.

What is stored:

- **Text objects** — named, versioned documents. Each version is immutable; the name moves.
- **Git repos** — a real remote: `git clone numbers://team/repo`. A push becomes one encrypted bundle
  plus a new signed manifest of the repo's branches; towers know nothing about git. Stock git does
  the pack work: on a station inside the helper that the chief's own git starts (the station daemon
  still spawns nothing), on a safehouse inside the daemon, which ties an agent's rights to its session.
  An always-on safehouse compacts bundles from time to time.
- **Large media** — airspace objects in their own right, fetched on demand. A Git LFS adapter can follow.

Transfer is planned on `iroh-blobs` (BLAKE3-verified streaming, ranges, resume, push, download from
several sources) behind our own interfaces, with a small control protocol of our own for leases,
quotas, names and tower-to-tower replication. `iroh-blobs` is still 0.x and says it is not production
quality, so it stays swappable (**decision 4**).

### Agent confinement

The rule: **agents never operate on towers or stations; they are
strictly confined to safehouses.** A handler, once it exists, runs no agents either. It is enforced
structurally:

- All PTY and harness code lives in the `safehouse` crate. No other daemon or library depends on it
  (CI: `cargo tree` per package) or may name `std::process::Command` / `tokio::process::Command`
  (CI: clippy `disallowed-types`). Two documented exceptions, both in frontends started by the
  chief: opening a sign-in URL the user asked for, and the git helper running git. The later
  `handler` crate will be the one other place allowed to start processes — containers and VMs,
  never harnesses.
- Only safehouses serve `pty`, `agent` and `login`; a station has no code that answers them.
- On a safehouse the daemon runs as its own OS account, holds the node key and never executes harness
  code in-process. Each chief maps to a dedicated OS account (or container), and every terminal,
  login and session of that chief is spawned under it. That, not an environment variable, is what
  separates one person's harness logins from another's and from the safehouse's key (**decision 3**).
- Agents never hold node keys or airspace keys. They reach chat, files, locks and airspace only through
  an MCP bridge the safehouse injects per session, attributed as (safehouse, session, owner chief) and
  granted no more than the owner has. That owner is the safehouse's assertion: a station accepts it as
  a chief's only when one of that chief's stations started the session.
- **A station and a safehouse on one machine** is never suggested and always warned against, but not
  blocked. Docs and demos always put the safehouse on another machine; the safehouse warns at
  start-up, and `chief safehouses` shows the warning, when it finds a station on its host. The two
  packages do not conflict. The rule that holds on every safehouse still applies there: agents run
  only under their own accounts — never root, the safehouse's account or an account that owns a
  station — and cannot read a station's data, reach its socket or see its processes.

### Harnesses and sessions

The safehouse is the **session host**: it owns each harness process, keeps it alive across station
disconnects, journals its events so a second station or a waking phone can attach from a cursor,
serializes prompts and holds permission requests until answered. Stations never speak a harness
protocol.

- Harness definitions are data, so new ones need no code and no SSH. One a chief pushes for
  themselves is private to them; one shared on a safehouse is an admin-signed statement. Kinds:
  - `pty` — any command in a safehouse-hosted terminal (server-side screen model, reattachable). The
    universal fallback; it ships with the safehouse, so `claude` and `codex` are usable, and can be
    logged in by hand, before the login broker exists.
  - `acp` — a generic Agent Client Protocol client, with the safehouse as the only ACP client. Covers
    Claude Code and Codex through the ACP project's adapters and about fifteen other agents natively.
  - native adapters where they are better (**decision 9**, per harness).
- A session belongs to the chief who started it and runs under that chief's harness account. Only
  the owner's stations may prompt, approve or type; other chiefs may watch and stop. Shared steering
  is offered only for accounts backed by an API key, because steering someone else's
  subscription-backed session breaks both vendors' account terms.

### Logging a safehouse's harness in from a chief

`chief login <safehouse> claude|codex` runs the harness's **own** login command on the safehouse, under
the chief's OS account there. theCompany never parses, logs or stores harness credentials and never moves
credential files between nodes; tokens are minted and kept only by the harness CLI on the safehouse.
The encrypted station–safehouse stream carries only what the vendor's flow itself passes through the chief's
browser or terminal.

Login is a structured session, so CLI, GUI and phone each render it natively; the raw terminal is an
escape hatch.

| Harness | First choice | Fallbacks |
|---|---|---|
| Claude Code | browser sign-in; the callback to `localhost` is relayed to the safehouse | paste-the-code URL; `--console` for API billing |
| Codex | device code (OpenAI's recommendation for headless machines; no ports; works from a phone) | browser sign-in with relayed callback (**decision 7**); API key piped to the CLI's stdin |

Guards, because this is the one place a safehouse asks a station to do something:

- A sign-in URL is accepted only inside a login this chief just started on that link, and only if it
  matches a station-side template for that harness (scheme, host, path, client id, redirect). The
  templates ship with the station and can be superseded by admin-signed settings; a safehouse never
  supplies them. A mismatch is shown as inert text, never opened. The frontend opens the browser,
  not the library.
- The callback relay is an HTTP-aware forwarder for one login, not a port pipe. It binds loopback on
  the port named in the captured redirect, passes only the exact requests that harness's flow needs
  (Claude: one `GET /callback` with the expected `state`; Codex: `/auth/callback`, then one
  `/success`), refuses everything else, logs nothing and closes when the login ends. One pending
  login per harness per safehouse. If a station and a safehouse do share a host, the relay is
  skipped and the browser reaches the harness's listener directly.
- Logins have a lifecycle. The safehouse caches method, account and expiry per profile and warns the
  owner's stations before a Claude login ends. It never re-runs `codex login` implicitly (that signs
  the profile out first). `chief logout` exists.

### Handlers (later)

A **handler** is the node that sets safehouses up. To start with there are none: a chief sets a
safehouse up by hand — a machine, VM or container of their choosing, the `safehouse` package, and an
invite. Longer term a handler runs on a host and, on a chief's request, creates an isolated
environment (a systemd-nspawn container, or possibly a full QEMU VM), starts a safehouse in it, gets
it admitted, and later tears it down. A handler runs no agents itself. The design leaves room for
it: a safehouse is already a self-contained node that joins by invite, and nothing ties it to the
machine it runs on. What a handler may do without a chief's signature, and whether isolation then
moves from OS accounts inside one safehouse to one safehouse per chief or per job, is **decision
14**. We flesh this out later.

## Repo layout

```
Cargo.toml  rust-toolchain.toml  clippy.toml  CLAUDE.md  .github/workflows/ci.yml  scripts/
docs/               ARCHITECTURE.md  ROADMAP.md         (this plan, kept current per milestone)
crates/
  base/             leaf crate: frame codec, key files; later the roster codec   package `numbers-base`
  numbers/          the network library: towers, endpoint + gate, link, protocols
  tower/            `tower` binary + embeddable lib (used by tests)              package `numbers-tower`
  station/          station core + local-socket server (library)                 package `numbers-station`
  station-daemon/   `station` binary                                             package `numbers-station-daemon`
  chief/            the shared frontend library: station API types + client
  chief-cli/        `chief` binary
  airspace/         persistence: a blind half towers also use, a sealed half for members   (M4)
  safehouse/        `safehouse` daemon: harnesses, terminals, login, MCP bridge  (M5)
  handler/          `handler` daemon: creates safehouses in containers or VMs    (M15)
  git-remote-numbers/   git remote helper                                        (M7)
  chief-ffi/        UniFFI → Swift, embeds the station core                      (M2 smoke check, M14)
apps/  linux/ (GTK4 / libadwaita / relm4)   ios/ (SwiftUI)                       (M13, M14)
```

Binaries are named for their role. Node packages carry a `numbers-` prefix because the crates.io
`tower` crate is already in our dependency tree (iroh → reqwest → tower), and a workspace package of
the same name would make `cargo -p tower` ambiguous. Nothing is published. `chief` and `chief-cli`
depend on `numbers-base` only, never on `numbers`, so the CLI links no network code; `station`
depends on `chief` for the API types, never the reverse.

## Appendix: verified facts the first milestones rely on

These notes draw on published sources. CI checks the M0 `numbers` dependency set with
`cargo check -p numbers --locked` and tests the M1 [dev tower](../crates/tower/src/lib.rs) over
loopback. The remaining iroh examples are source references.

- **Dependencies.** `iroh = { version = "1.5", default-features = false, features = ["tls-ring"] }`
  (MSRV 1.91); `iroh-relay` likewise, plus `server` in `tower` only; direct `n0-future 0.3` and
  `n0-error 1`; `n0-dns-resolver 0.1` for SRV in M2. Never enable `iroh/test-utils`. Keep
  `fast-apple-datapath` (private Apple APIs) and `portmapper` out of the libraries. Install the
  rustls crypto provider only in a binary's `main`.
- **Endpoint.** `Endpoint::builder(presets::Minimal)` with `.secret_key`, `.relay_mode(RelayMode::
  Custom(map))`, `.address_lookup(..)`, `.hooks(..)`, `.portmapper_config(PortmapperConfig::Disabled)`;
  `.clear_ip_transports()` gives a relay-only endpoint; tests add `.bind_addr((LOCALHOST, 0))`.
- **Relay map.** The bare-URL constructors assume QAD on UDP 7842; against a plain-HTTP tower that
  stalls every net report about 3 s. Use `iroh::RelayConfig::new(url, None)` for `http://` towers.
  Switch off only the captive-portal check (`NetReportConfig::default()` with
  `captive_portal_check = false`); `NetReportConfig::minimal()` also disables the HTTPS probes, and
  then no home tower is ever chosen.
- **Dial by id.** An `AddressLookup` returning every tower URL for any id makes
  `endpoint.connect(id, ALPN)` work with no other service. All listed relays are tried in parallel
  and the relay need not be in the dialer's own map — current behaviour, not a documented contract.
  Each extra tower costs one dead path per connection (budget 8).
- **Tower.** `iroh_relay::server::{Server, ServerConfig, RelayConfig}` are `#[non_exhaustive]`: start
  from defaults and assign fields. `tls = None` serves plain HTTP; `server.http_addr()` gives the
  bound address (`http_url()` is test-utils only). Defaults are `AllowAll` and no rate limit: set
  `relay.limits.client_rx` and, in M2, an `AccessControl` (its `on_connect` sees the authenticated
  endpoint id). `iroh::RelayConfig` (client) is a different type from
  `iroh_relay::server::RelayConfig`. In TLS mode the server also binds a plain-HTTP listener (port
  80, captive-portal responder); ACME is tls-alpn-01 on port 443 only and needs `cache_path`; QAD
  needs TLS; `Server::spawn` cannot add HTTP routes. A relay and a separate iroh endpoint can run in
  one process; the relay does not advertise that endpoint's id.
- **Router and gate.** `Router::builder(ep).incoming_filter(..).accept(ALPN, proto).spawn()`;
  `spawn` overrides builder ALPNs; dropping the `Router` aborts it, so call `router.shutdown().await`.
  When `accept()` returns the connection closes, so loop on `accept_bi`; streams are lazy, so write
  right after `open_bi`. `EndpointHooks::after_handshake` runs for every ALPN in both directions (keep
  it ALPN-aware; it must not hold an `Endpoint`) and only after iroh has registered the connection, so
  only the pre-handshake filter limits address disclosure — and for relayed attempts the claimed id
  comes from the tower.
- **Liveness.** An endpoint binds with an empty relay map and takes towers later through
  `insert_relay` / `remove_relay`. A first home tower comes only from a net report, which re-runs
  every 20–26 s or when the relay map changes, so a station started before its tower can take that
  long to come online. `Endpoint::online()` has no timeout: wrap it, and report
  `home_relay_status()`. Keep-alive is 5 s and idle timeout 30 s; a dial to an offline peer fails
  only by timeout. Two processes on one node key: the relay keeps only the newest. Sockets close only
  when every `Endpoint` clone is dropped. iOS kills an app that holds a file lock while suspended.
- **Paths.** `conn.paths()`, `conn.paths_stream().wait_any(..)`, `conn.path_events()`; a path reports
  `is_relay()`, `is_ip()`, `is_selected()`.
- **Local socket and tooling.** `tokio::net::UnixStream::peer_cred()` gives the peer uid (the std
  equivalent is unstable); `rustix` supplies `geteuid` without `unsafe`. A socket path is limited to
  108 bytes on Linux, 104 on Apple. `std::fs::File::try_lock` is stable since 1.89. UniFFI has no
  stream type and does not propagate cancellation, hence the pull-style `Subscription::next()`.
- **SRV (M2).** `n0_dns_resolver::DnsResolver::lookup_srv(name)` returns
  `SrvRecordData {priority, weight, port, target}`. Query the FQDN with a trailing dot; targets come
  back without one; NXDOMAIN is `Err(Error::NxDomain {..})`, an empty answer is `Ok(vec![])`.
  `RelayUrl` equality is textual, so canonicalise once. SRV-located towers are always `https`. The
  system resolver falls back to public resolvers unless that is disabled.
- **Claude Code login.** `claude auth login [--claudeai|--console|--sso|--email]` prints a sign-in
  URL whose redirect expects a pasted code, and hands `$BROWSER` a second URL redirecting to
  `http://localhost:<random port>/callback` (listener on 127.0.0.1; any other `/callback` request
  aborts the login). `claude auth status` prints JSON (`loggedIn`, `authMethod`, `loginExpiresAt`,
  `configDirectory`) and exits 0 or 1. There is no login command that reads an API key. `BROWSER` is
  undocumented for this, so capturing the URL from terminal output is the baseline.
- **Codex login.** `codex login` listens on 127.0.0.1:1455 (fallback 1457), answers
  `/auth/callback` with a redirect to `/success?id_token=…` and exits only once `/success` is
  fetched; `GET /cancel` is unauthenticated and a second login cancels the first; starting a login
  clears and revokes the stored one. `--device-auth` needs the ChatGPT device-code setting and lasts
  15 minutes. `--with-api-key` reads stdin and refuses a terminal. `codex app-server` offers
  `account/login/start|completed|cancel` and `account/read`.
- **Nitrokey HSM 2 (M3).** Per the vendor: RSA 1024–4096, ECC 192–521 bit (NIST P-256/384/521,
  secp256k1, brainpool) with ECDSA and ECDH, AES; no Ed25519. PKCS#11 through OpenSC. ECDSA-256
  signing takes about 80 ms. Keys can be generated on the device, and its built-in PKI can certify
  that a key was generated inside it. Encrypted key backups restore only into another Nitrokey HSM;
  an m-of-n scheme can gate access; PIN attempts are limited. Keys can also be imported, which is
  why the generated-on-device proof matters. Rust side: `cryptoki` 0.12 (PKCS#11), `p256` 0.14
  (verification on every node, towers included). iroh takes its node key as raw bytes and has no
  signer hook, so node keys stay in software.
- **iroh-blobs (M4).** 0.105.0 (2026-11-13) builds against iroh 1.x, MSRV 1.91; still 0.x, breaks API
  in most releases, and its README says it is not production quality. It gives BLAKE3 addressing,
  verified range streaming, resumable partial blobs, one-level hash sequences, a filesystem store
  (redb plus files) and download from several providers. Push exists but is off by default and
  protects nothing from garbage collection, so a lease must exist before the data arrives. A provider
  can intercept connections and requests, but request events carry neither the caller's id nor the
  size, and there is no quota. Acknowledged writes can be lost on a crash unless its store runs in
  its power-loss mode and is synced. Nothing on its wire covers leases, deletes or names. `iroh-docs`
  (no release since June) and `iroh-willow` are not usable for names.
- **Git (M7).** Git runs `git remote-<scheme>` for a URL scheme it does not know; a helper can
  advertise `connect` and pipe to `upload-pack` / `receive-pack`. Git checks fast-forward locally
  before pushing, so the helper must check again when it commits the push. gitoxide (gix 0.90) can
  read, fetch and index packs but cannot push, delta-compress or act as a helper, so the pack work
  goes to stock git, as Radicle's Rust helper does. Radicle's signed refs were replayable until March
  2026 because the signature did not bind the predecessor and the repository; ours must. Packs are
  compressed, so compaction re-uploads a repo: quotas need headroom. A Git LFS transfer agent wants
  whole files, so no range reads through LFS.
