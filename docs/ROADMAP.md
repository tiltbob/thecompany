# theCompany — roadmap

What gets built, in which order, and what is still open. The design itself is in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Roadmap

Each milestone is one small, demoable PR series with an automated check and a manual demo.

| # | Milestone | Done when | Needs |
|---|---|---|---|
| M0 | Scaffolding: workspace, toolchain pin, CI with the confinement lint, CLAUDE.md, docs, tracking issues | CI green with a real test per crate; issues exist | — |
| **M1** | **Two chiefs chat**: dev tower, station daemon, `chief` CLI over the local socket, manual peer book | CI: chat through an in-process tower, the same relay-only, a stranger refused, a second station on one data dir refused, CLI ↔ station over the socket, and a scripted walkthrough with the real binaries | M0 |
| M2 | Real-world tower, station as a service: TLS (cert files; ACME optional), QAD, SRV bootstrap, several towers, a remembered tower set (SRV again only when no known tower answers) that peers exchange and show, static allowlist + rate limit, systemd units, FFI smoke check | CI, in-process: TLS tower with a test certificate, QAD, fake SRV resolver; a station that knows its towers starts without an SRV lookup and returns to SRV only when none answers; an unlisted station id is denied. Demo: two stations on different networks, given only a domain, chat, relayed or direct | M1; decisions 13, 16, 17 (settled); **needs:** a public host and a DNS zone (demo only), and a ruling on hard rule 10 for the FFI smoke check |
| M3 | Membership: chief ids and admin keys, an enrollment key on a Nitrokey HSM with the enrollment-requires-HSM rule, signed roster with the tower set, invites, station commissions, roster sync, signed address records passed between nodes, removal, the tower's own endpoint with a roster-fed allowlist; `chief network` | a second chief joins by invite through an allowlisting tower and chats without `peer add`; with the HSM rule on, an enrollment without the token's signature is rejected by every node, while removal and daily use work without the token (CI: a software PKCS#11 token; a real Nitrokey by hand); a chief revokes one of their two devices and keeps working from the other; a removed chief is refused by every node after sync | M1 (allowlist feed: M2); decisions 1, 2; **needs:** Nitrokey HSM 2 tokens |
| M4 | Airspace I: sealed blocks on towers and members, spaces and keys, names, leases and quotas, the two-tower rule with tower-to-tower pull; text objects; `chief airspace` | an object written on one station is read on another after the first went offline; the tower's disk holds only ciphertext; "stored" needs two towers; a rolled-back name is refused; unleased blocks lapse | M3; decisions 4, 5, 6 |
| M5 | Safehouse, set up by hand by a chief: admission, `chief safehouses`, safehouse-hosted terminals (`chief shell`), `pty` harness presets, per-chief OS accounts, confinement checks | a chief runs `claude` and `codex` on a safehouse with API keys set by hand; an agent process cannot read the safehouse's key or another chief's home | M3; decision 3 |
| M6 | Harness API keys for Claude and Codex: set, status, replace and remove from a chief | both harnesses run on a safehouse on keys set from a chief with no SSH; a key never appears in a log, an error or an event; CI uses a fake harness CLI | M5; decision 8; **needs:** an Anthropic and an OpenAI API key (manual check only) |
| M7 | Airspace II: git repos — remote helper, bundles and manifests, mirrors on safehouses, compaction | `git clone`, `fetch` and `push` between a station and a safehouse with only towers always on; a non-fast-forward push is refused; two racing pushes leave one tip and one conflict ref | M4 (safehouse side: M5); decision 10 |
| M8 | Harness sessions: session host, journal, ACP kind, Claude + Codex presets, ownership policy, workspaces from airspace; `chief sessions`, `chief harness` | prompt, stream and approve from a chief; reattach from a second station; a non-owner cannot steer. CI: a fake ACP agent | M5 (M6 for keys set from a chief); decisions 9, 11 |
| M9 | Chat v2: durable history and outbox, receipts, channels, delivery to all of a chief's stations, offline delivery through airspace, chat tools for agents | history survives a restart; a message sent while the recipient was offline arrives when it returns; an agent posts to a channel | M4 (agent tools: M8) |
| M10 | Sockets: `chief forward -L` with a safehouse-side target policy | a station reaches a safehouse's localhost service; a disallowed target is refused | M5 |
| M11 | Airspace III and files: large media (resume, streaming, range reads, audits), push/pull between nodes, browsing workspaces, file tools for agents | a multi-gigabyte upload survives an interruption; another node streams a range of it; a tower that dropped a block is caught and refilled | M4 (safehouse side: M5) |
| M12 | Locks: named leases with TTL and fencing tokens, lock tools for agents | two agents contend; a chief can inspect and break, attributed | M5 (agent tools: M8) |
| M13 | Linux GUI: a third client of the station socket | a message sent from the CLI appears in the GUI | M1+ |
| M14 | iOS app: (a) foreground chat, airspace browsing and session control, (b) push | (a) works on a device; (b) a notification arrives while the phone sleeps | M9; decision 12; **needs:** Mac, iPhone, Apple Developer membership |
| M15 | Handlers: a node that creates, admits and removes safehouses in nspawn containers (maybe QEMU VMs). To be designed; can move earlier once it is | a chief asks a handler for a safehouse and gets one that is isolated from the host and from other safehouses | M5; decision 14 |

The order after M3 is open to change: M4 comes early because chat history, offline delivery, git and
agent workspaces all build on it. Network management grows one view per milestone: `chief network`
(M3: every node, its chief, who admitted it, online, relayed or direct, tower health),
`chief airspace` (M4: spaces, durability, tower usage), `chief safehouses` (M5), `chief sessions` and
`chief harness` (M8). Towers are observed from a station, never administered.

## Tracking

Work is tracked in GitHub issues on `tiltbob/thecompany`: the
[roadmap](https://github.com/tiltbob/thecompany/issues/1) and, because two agents work here, the
[coordination issue](https://github.com/tiltbob/thecompany/issues/2). Issue types are Task, Bug and
Feature; there are no `decision` / `verify` labels, so those are marked by title prefix.

- **Roadmap** — one tracking issue with every milestone as a sub-issue. Status lives there; the table
  lives in `docs/ROADMAP.md`.
- **Milestones** — one Feature issue each (M0–M15): scope, done-when, dependencies, what is needed
  from the team. Later ones start as stubs and are split into Tasks when they become active.
- **Tasks** — sub-issues for the active milestones only, one per reviewable PR. M0: workspace and
  crate skeletons; CI and guards; CLAUDE.md, docs, README. M1: dev tower; base crate (frames, key
  files); endpoint, dial-by-id and gate; link + chat protocol; station core with integration tests;
  station API + socket transport; `station` daemon + `chief` CLI; walkthrough script in CI.
- **`Decision:` and `Verify:` issues** — Tasks attached to the milestone they gate, one per item in
  the last section, each with a recommendation. Constraints accepted in review are copied into the
  milestone they bind.
- Every PR says `Closes #n`; a comment on the issue records what was done, how it was verified and
  what was deferred. Node ids, invite tickets and real hostnames stay out of issues.

How two agents share the work is in [AGENTS.md](../AGENTS.md).

## M0 + M1 in detail (what gets built first)

**Goal:** one `tower`, two `station`s, and two chiefs exchanging chat lines through the `chief` CLI.

**M0 — scaffolding**

- Workspace: `resolver = "3"`, edition 2024, toolchain pinned to 1.97 with `rust-version` equal to
  it, shared dependencies and lints (`unsafe_code = "forbid"`, `clippy::unwrap_used = "deny"`),
  `Cargo.lock` committed, `publish = false`, `doc = false` on the bins named `chief` and `tower`.
- One `clippy.toml` at the root (clippy uses the first one it finds walking up and never merges):
  it disallows the process-spawning types and sets `allow-unwrap-in-tests`. `crates/safehouse` gets
  its own in M5.
- CI: `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`;
  `cargo test --workspace`; per-package checks that `--workspace` would mask through feature
  unification — `cargo check -p numbers` with the exact iroh features the plan assumes
  (`default-features = false, features = ["tls-ring"]`), and later
  `cargo tree -p <pkg> -e normal,build` for the confinement rule.
- `CLAUDE.md`: build and test commands plus the hard rules — agent confinement; the harness-credential
  rule; towers never hold keys; one station per identity; frontends hold no network state; libraries
  install no process-global state and spawn nothing; API types must be FFI-exportable.
  `docs/ARCHITECTURE.md` and `docs/ROADMAP.md` distilled from this plan.

**M1 — crates and modules**

| Path | Contents |
|---|---|
| `crates/base/src/frame.rs` | `write_frame` / `read_frame`: u32 length + JSON, with the size cap and read deadline as parameters (64 KiB on the wire, 1 MiB on the local socket) |
| `crates/base/src/keyfile.rs` | key-file helper for daemons: create with `O_EXCL`, mode 0600 inside a 0700 dir, refuse a file with the wrong owner or mode. Load or create by path or relative to an already opened data directory. Keys reach the libraries as bytes |
| `crates/tower/src/lib.rs` | `TowerConfig`, `Tower::spawn(cfg)` wrapping `iroh_relay::server::Server::spawn` in plain-HTTP mode with a per-client rate limit; `url()`, `shutdown()`; no `iroh_relay` type in its public API |
| `crates/tower/src/main.rs` | `tower [--bind 127.0.0.1:3340]`; loopback only in M1 |
| `crates/numbers/src/identity.rs` | `NodeId` (iroh `EndpointId`) and node-key generation |
| `crates/numbers/src/towers.rs` | `TowerSet`: canonical URL list with a QAD setting per tower. M1 takes one explicit URL |
| `crates/numbers/src/endpoint.rs` | builds the iroh `Endpoint` from `presets::Minimal`: relay map with an explicit config per tower, `TowerLookup` (an `AddressLookup` mapping any node id to the tower URLs, so dialing needs only an id), port mapping off, lowered stream caps, a relay-only switch for tests |
| `crates/numbers/src/gate.rs` | the one authorization point: an `Authorizer` that maps a node id and protocol to a caller (a peer name in M1, a roster entry later) or a refusal; a pre-handshake filter that ignores relayed attempts from ids it does not know and demands address validation from direct ones; the `after_handshake` hook; close codes |
| `crates/numbers/src/link.rs` | Authenticated ALPN `numbers/0` with a version 1 hello each way and empty `network` and `head` fields. Either peer can open streams. Simultaneous dials keep the connection initiated by the lower node id. Configured deadlines cover dial, hello and chat. |
| `crates/numbers/src/proto/` | Version 1 hello and typed stream headers. One chat per stream with a typed `Ack` or `Nack`. Bodies use padded standard base64 and decode to at most 16 KiB. `ReceiptWindow` tracks accepted messages by local peer name and message id. |
| `crates/chief/src/api.rs` | the station API: `Request`, `Response`, `Event`, `ApiError`, `Status`, `Peer {name, stations: [{station_id, link}]}`, `Message {seq, rev, id, peer, direction, body, at_ms, status}`; FFI-exportable types only |
| `crates/chief/src/client.rs` | `Transport` (methods return `impl Future + Send`; static dispatch), `Client` with typed methods, a concrete `Subscription` whose `next()` is cancel-safe; re-exports `SocketTransport` |
| `crates/chief/src/socket.rs` | the local socket's envelope (`Hello`, `HelloReply`, `Open`, `Reply`; not part of the station API) and `SocketTransport`: one connection per call or subscription, the server's uid checked, a call timeout |
| `crates/chief/src/paths.rs` | the default data dir and the socket path as pure functions of `XDG_DATA_HOME`, `XDG_RUNTIME_DIR` and `HOME`; the socket-path length check |
| `crates/chief/src/follow.rs` | `Client::follow_log`: the attach order (subscribe, page the log, drop live events already seen), re-reading after `Resync`, ending with the subscription or the station instance |
| `crates/chief/src/fake.rs` | `FakeStation` (feature `fake`, tests only): an in-memory station that follows the API rules, for tests of transports, the socket server and frontends |
| `crates/station/src/lib.rs` | `Station::start(StationConfig)`: one actor owning the endpoint, the peer book (`peers.json`, written only by the actor) and a ring of the newest 1,000 messages; data-dir lock; never awaits the network at start; live events with `Resync` on lag; message outcome kept as state; replaces the characters of API rule 9 in peer text; usable in-process as a `Transport` |
| `crates/station/src/ipc.rs` | the socket server (rules below), generic over any `Transport`. The hello carries the client's API version. The reply gives the station's API version, station id and per-start instance id. The server closes the connection if the versions differ. Each call or subscription uses one connection. Both ends check the uid and cap frames at 1 MiB. Calls have frame deadlines. An open subscription has no deadline. The server holds `<socket>.lock` while serving. |
| `crates/station-daemon/src/main.rs` | `station run [--data-dir DIR] --tower URL`: foreground daemon, creates its key on first run, graceful shutdown |
| `crates/chief-cli/src/lib.rs`, `main.rs` | `chief` (clap): `status`, `id`, `peer add\|ls\|rm`, `chat`, `send`, `log`; the whole CLI is a library generic over `Transport` (tested in-process), `main.rs` only wires it to the socket; exit 0 done, 1 refused or failed, 2 usage, 3 no station |

The receiving station uses one in-memory FIFO for the newest 1,000 first receipts across all
peers. Its key is the local peer name and the message id, so retries from different stations of
the same peer share a receipt. The receiving actor records a new receipt, logs the message and
publishes its event as one operation before acknowledging it. Retries get another ACK without
adding a log entry, publishing a new event or changing FIFO order. The window starts empty after
a restart. [Decision #65](https://github.com/tiltbob/thecompany/issues/65)

**Local socket.** The data dir defaults to `$XDG_DATA_HOME/thecompany/station`. The socket path is a
function of the data dir: `$XDG_RUNTIME_DIR/thecompany/station.sock` for the default data dir when
that variable is set, otherwise `<data-dir>/station.sock`; `chief --station PATH` overrides. The
station takes the data-dir lock first; the socket server then locks `<socket>.lock`, so that a socket
another station is serving (two data dirs can name the same socket path) is never taken away, and
only then removes a leftover socket, binds and sets 0600.
The directory is created 0700, and an existing one must be owned by the same user with no group or
other access. A path too long for a socket address is refused with a message naming `--data-dir`
(by `chief`, naming `--station`).
The peer uid comes from `tokio::net::UnixStream::peer_cred()`.

**CLI surface**

```
tower                                         # dev tower on 127.0.0.1:3340, plain HTTP

station run --tower http://127.0.0.1:3340     # the chief's node; keeps running
chief id                                      # this station's id, to hand to the other chief
chief peer add bob <station-id> | peer ls | peer rm bob
chief chat bob                                # last 50 lines, then live; shows other peers too
chief send bob "on my way"                    # waits up to 120 s for the outcome once the station
                                              # accepted it: exit 0 delivered, 1 refused or failed,
                                              # 3 outcome unknown
chief log [bob]                               # print the message log and exit
chief status                                  # tower state, peers, relayed or direct
```

`chief` says plainly when no station is running. An attaching frontend subscribes first, then pages
the log, then drops live events it has already seen. A send ends `Delivered` — the peer's station
holds the message in memory, which a restart of that station loses until M9 — or `Failed` ("bob
appears offline, or has not added you": an unknown caller is ignored, which looks like a timeout).
`--tower` is required on every `station run` in M1; nothing about towers is persisted yet.
`peer rm` closes the link to that peer.

**Deliberately not in M1:** TLS, SRV bootstrap, several towers, a tower set that changes at runtime,
non-loopback plain HTTP, tower allowlist, running the station as a service (all M2); chiefs as
identities, roster, invites (M3); airspace (M4); durable history and offline delivery (M9); GUI,
FFI, safehouses.

**Reuse.** iroh's `Endpoint` / `Router` / `EndpointHooks` / `AddressLookup`, `iroh_relay::server`,
and upstream examples as templates: `examples/auth-hook.rs` and `screening-connection.rs` (the gate),
the relay binary's `build_relay_config` (M2 TLS), `iroh-ffi` packaging (xcframework), `dumbpipe`
(stream forwarding, M10), `radicle-remote-helper` (a Rust git helper that shells out to git, M7).

## M2 in detail

**Goal:** towers that run on the public internet and stations that run as services, so that two
stations on different networks, given only a domain, find their towers and chat. The demo (#46):
a TLS tower on a public host as a systemd service, the domain's SRV record naming it, two stations
started with `--domain example.com` on different networks chatting relayed and direct, and an
unlisted station denied.

**M2 — crates and modules**

| Path | What it gets in M2 | Task |
|---|---|---|
| `crates/tower/src/lib.rs` | `TowerConfig::dev(bind)`; `TowerConfig` is `#[non_exhaustive]`, so later fields do not break callers. Done | #66, Claude |
| `crates/tower/src/lib.rs` | the allowlist: endpoint ids as text, parsed at `spawn`; no list admits everybody (dev only), an empty list admits nobody; a bad entry fails `spawn` naming its position, never its text. A configurable per-client rate limit (default 2 MiB/s) and burst. `Tower::stats()`: plain integers for attempts the list denied, connections the rate limit slowed, and address-discovery connections. No `iroh_relay` type in the public API. `scripts/check-deps.sh` fails if `numbers-tower` depends on `numbers`, `numbers-station` or `chief` | #69, Claude |
| `crates/tower/src/tls.rs` | `TowerConfig::tls(..)`: certificate and key from PEM files, loaded and validated as a pair (the chain parses, the key is supported and matches the certificate) before anything is bound, and read again once a day; a failed reload keeps the pair in use. The ring provider is passed explicitly, nothing installed process-wide. No error or log line carries file content, and the key is held nowhere but inside the TLS library. QAD on its own UDP port (default 7842) or off. `Tower::url()` gives the canonical tower URL, `Tower::qad_port()` the QAD port. A TLS tower may bind any address; the dev tower stays loopback only | #70, Claude |
| `crates/tower/src/main.rs`, `config.rs` | `tower` keeps the M1 dev behaviour; `--bind` and `serve` cannot be combined. `tower serve --config /etc/numbers-tower/tower.toml` reads `hostname`, the binds (defaults 0.0.0.0:80, :443, UDP 7842), `qad = false` to turn QAD off, exactly one of `[tls] cert` and `key` or `[acme] contact`, `cache_dir` and `production` (default staging), exactly one of `allow_file` (one id per line, `#` comments) and `open = true`, and `[rate]`. Unknown keys are refused; relative paths are resolved against the directory of the config path as given (a symbolic link to the config file is not resolved); both files must be regular files (64 KiB and 1 MiB caps), and a leading byte-order mark is ignored. A host name or path that is present but empty is refused with its key. Errors name the key, the line and the file, never a path, host name, id or value. ACME maps to `CertConfig::LetsEncrypt` with a required, non-empty cache directory that must be private to, owned by and writable for the tower's user: the tower creates a missing one with mode 0700 and refuses one that others can access or another user owns (both checked before anything is created in it) or that it cannot write (a write probe that creates a file of its own name and removes only that file), checked once at start, so the path and its parents must stay under the control of the tower's user or root, and an existing cache's content is trusted (a systemd unit needs `StateDirectoryMode=0700`, #82); the host needs two labels and no IP literal, the contact one `mailto:` address; no ACME tower runs in a test. Bind failures name the listener and the reason (in use, no permission, or the OS's own text where the kind says nothing), never the address. Command-line errors are one fixed line on stderr with exit status 2 and never echo what was typed; `--help` prints clap's text to stdout. The start line logs the bound HTTPS, HTTP and QAD ports (`off` when absent), never addresses, the number of distinct listed ids or "open", and `certificate=files`, `acme staging` or `acme production` (staging also logs a warning), no host name, URL or path; ACME failures (a failed order, the cache, another step) and deployed certificates are logged as fixed lines without detail; the log filter lets the certificate reload warnings through; colour only on a terminal. A relay that stops by itself ends the process with an error, so that a service manager restarts it | #71, Claude |
| `crates/numbers/src/towers.rs` | `TowerSet`: up to 8 `TowerEntry { url, qad }`, `Qad` = `Off \| Port(u16)`, default `https` → 7842 and `http` → off. `diff`, `urls`, `len`; the serde form `[{url, qad_port}]` | #67, Codex |
| `crates/base/src/towers.rs` | `canonical_tower_url` for explicit, SRV, remembered and peer-sent URLs, shared with frontends and re-exported by `numbers` with `TowerSetError` and `MAX_TOWERS`. Plain `http` requires a literal loopback IP address | #116, Codex |
| `crates/numbers/src/endpoint.rs` | `EndpointConfig.tls_roots` (`WebPki` by default, `WebPkiPlus` with extra DER roots, `CustomOnly` for tests) and `EndpointConfig.dns` (`System` with no fallback, `Nameserver(addr)` for tests) | #67, Codex |
| `crates/numbers/src/endpoint.rs` | several towers at runtime: `set_towers(&TowerSet)` reconciles the relay map; `tower_states()` lists configured towers with the last observed outcome of the configured home; `nudge_towers()` requests a fresh net report; a weak control handle the station retains | #75, Codex |
| `crates/numbers/src/endpoint.rs` | `tls_roots` wired to `ca_tls_config`; `dns` wired to iroh's resolver with no public fallback; the relay map carries each entry's QAD port | #76, Codex |
| `crates/numbers/src/bootstrap.rs` | `srv_name(domain)` → `_numbers._tcp.<domain>.`; `towers_from_srv` (pure); the object-safe `Locator` trait, held as `Arc<dyn Locator>`; `DnsLocator` on `n0-dns-resolver`, no public fallback: with the system's resolvers it builds a fresh resolver for each lookup; with one given nameserver it keeps one resolver and clears its cache before each lookup; its own deadline (10 s by default) bounds the lookup's network waits, not the synchronous read of the system's resolver configuration; `LocateError`, whose text never carries a name | #72, Claude |
| `crates/numbers/src/testing.rs` | `FakeDns` (cargo feature `test-support`): a DNS server on 127.0.0.1 answering SRV and A from a table the test sets, able to stay silent, recording every query. The station tests reuse it | #72, Claude |
| `crates/numbers/src/proto/towers.rs` | proto `towers` v1: over an established link with a peer in the peer book, each side sends its own working set `{towers: [{url, qad_port}]}`; the receiver checks it and drops a bad list whole. The station gets an admitted-link notification and protocol dispatch beyond chat, so that it sends its set on every new link | #81, Codex |
| `crates/station/src/towers.rs` | owned by the actor: `towers.json` in the data dir (the set, each entry's source, the bootstrap domain), written atomically and only by the actor; the start order; `StationConfig` gains a tower input (`Pinned(TowerSet) \| Domain(String) \| Remembered`), a locator, `tls_roots` and `dns`; fills `source` and `bootstrap_domain` | #77, Codex |
| `crates/station/src/towers.rs` | the watchdog on the home tower, the `Denied` state, and the return to SRV (below) | #78, Codex |
| `crates/station-daemon/src/main.rs` | `station run [--data-dir DIR] [--domain DOMAIN] [--tower URL]... [--tower-ca PEM]`, and a hidden `--dns-server ADDR` for tests or none (open in #79). A first run with no `--tower`, no `--domain` and no remembered set is refused with a plain message. The rustls provider install stays in `main` | #79, Codex |
| `crates/chief/src/api.rs` | `TowerStatus.source: Option<TowerSource>` (`configured \| remembered \| srv \| peer`) and `Status.bootstrap_domain: Option<String>`. `Standby` represents a tower whose state is unobserved, including when the home tower state is unavailable. `API_VERSION` is 2. | #68, Codex |
| `crates/chief/src/api.rs` | Per decision 16 (#73), the candidates a station holds with the reporting peer's name and the explicit request that takes one into the tower set. | #109, Codex |
| `crates/chief-cli/src/lib.rs` | `chief status` prints `domain <domain>` first when the station has a bootstrap domain, then `tower <url> <state>[ (<source>)][: <detail>]` per tower: the detail is a failing tower's error or a denied tower's own reason (this project's tower says "not on this tower's list"), and the URL is always one field; `FakeStation::set_towers` and `set_bootstrap_domain` for the tests. Done. Per decision 16 (#73), the candidates and `chief tower add <url>` are #122, which waits for the API of #109 | #80, Claude |
| `packaging/systemd/` | `numbers-tower.service` (system unit: `DynamicUser`, `StateDirectory=numbers-tower` for the ACME cache, `CAP_NET_BIND_SERVICE`, sandboxing) and `station.service` (user unit, `Restart=on-failure`, `loginctl enable-linger` documented). A unit test in each binary parses its unit's `ExecStart` with clap; CI runs `systemd-analyze verify` on both. Docs: "Running as a service" | #82, Claude |
| `crates/chief-ffi` | UniFFI exports of the `chief::api` types, a small async facade (`start_station`, `request`) on an owned tokio runtime, Swift bindings generated in CI. Blocked on hard rule 10 (below) | #83, Claude |
| `scripts/walkthrough-tls.sh` | the walkthrough with the real binaries over TLS on loopback, run by CI | #84, Codex |
| `docs/notes/m2-demo.md` or a docs section | the runbook for the real-network demo | #85, Claude |

**The tower.** There are two kinds. The dev tower of M1 serves plain HTTP on loopback only. The TLS
tower takes its certificate from files or from Let's Encrypt (ACME), may bind any address, and runs
QUIC address discovery (QAD), which needs TLS. Both take the allowlist and the rate limit. The allowlist gates relaying
only: an unlisted station is refused with the reason "not on this tower's list", which it reads
from `auth_denied_reason()`, but it can still use the tower's QAD and latency probe, and it can
pick the tower as its home through the probe and then be denied. It retries on iroh's backoff, so
`denied` counts attempts, not stations. Removing an id from the list does not end an open
connection. The rate limit applies per client to what the tower reads from that client; it slows
the sender and raises no error, and iroh-relay 1.5's connection-rate limits are not implemented
and not exposed. In TLS mode iroh-relay always binds a plain-HTTP listener, which cannot be turned
off and answers only `/generate_204`. A production tower opens 443/tcp, 80/tcp and 7842/udp. The
tower's TLS key is its own transport key, and with ACME so is the ACME account key that the ACME
client creates and keeps, with the TLS key, in the tower's private cache directory: both agents read
hard rule 2 as forbidding member, admin and airspace keys on a tower, which the TLS tower does not
touch. SRV
carries no QAD port, so a tower found through SRV is assumed to serve QAD on UDP 7842; a closed QAD
port costs every station that runs QAD 3 s at each first net report.

**Finding towers.** A station takes its towers in this order: the `--tower` pins given for this
run, which also switch off SRV and are never written to `towers.json`; else the set it remembered
in `towers.json`; else one SRV lookup for its bootstrap domain. Pins and a remembered set skip the
SRV lookup, not all DNS: a tower's host name can still need an A or AAAA lookup. `--domain` is
stored on first use.
What happens when `--domain` differs from the stored one, and when `--tower` and `--domain` are
given together, is open, to be settled on #79. The record is SRV with one target per tower (decision
13, settled on #38): `_numbers._tcp.example.com. SRV 0 0 443 tower1.example.com.` gives the tower
`https://tower1.example.com/`. A target of `.` is skipped, and priority and weight are ignored.
Every tower URL is kept in one canonical form: the host in lower case with no trailing dot, the
scheme's default port left out (443 for `https`, 80 for `http`), a root path. SRV targets and
production towers are `https`; plain `http` is allowed only for a literal loopback IP address in
an explicit dev URL. NXDOMAIN and an empty answer both mean no tower.
The lookup's network waits run under its own deadline (10 s by
default), because the resolver's 6 s is the budget of one UDP query only; the deadline cannot
preempt the synchronous read of the system's resolver configuration at the start of a lookup. No
lookup is answered from an earlier one's cache: with the system's resolvers each lookup builds a
fresh resolver, and with an explicit nameserver the cache is cleared first. Neither the SRV
resolver nor iroh's resolver for the tower host names falls back to a public resolver.

**Watching the home tower** (decision 17, settled on #74). iroh alone is slow in three situations
that the lab measured: a tower that was down at start (online 14 to 24 s after it is up), a home
tower that dies while another is in the set (41 to 49 s to move), and a home tower that denies the
station while another would admit it (45 s or more, often not online within 60 s). Calling
`insert_relay` with a tower already in the map makes iroh probe the map at once. The station
watches `home_relay_status()` and leaves a connected home alone:

- **No home tower** for some seconds after start or after the set changed: it nudges iroh to
  probe the configured relay map and repeats that at a growing, capped interval while the status
  stays empty.
- **The home tower not connected** for some seconds: it nudges iroh to probe. An available
  replacement can become home.
- **The home tower denies the station:** the tower is marked `Denied` with the reason, control
  characters replaced with spaces, and shown. It is taken out of the relay map only if another
  tower of the set is not marked denied. If it is the only tower, or every tower denies, the
  station changes nothing in the map, does not nudge and does not ask DNS because of the denial.

Because of a denial, the station puts a denied tower back and asks DNS again only when the set
changes or at a long interval (about 10 minutes in #78); recovery from an outage does not wait for
that interval and follows `bootstrap_after` and the capped backoff below. The station limits its
own attempts; iroh's own retries go on
and are not the station's to stop. After each nudge a bounded recovery interval passes. When no
known tower has answered for `bootstrap_after` (about 60 s by default, a few seconds in tests) and
that interval has passed, the station asks the locator. A non-empty answer replaces the set with
the source `srv` and is remembered; an answer that arrives after a known tower has recovered is
discarded. No tower or a DNS error keeps the known set and retries with a capped backoff; a failed
lookup never wipes the set. The watchdog relies on iroh behaviour that is not documented (a change
of the relay map starts a net report), so each case has a test with real towers that fails if an
upgrade changes it; then the slower behaviour without the nudge remains, and it is correct. A
connection whose selected relay path goes down does not move to the new home tower: it ends at the
30 s idle timeout. A change of the relay map alone does not end a connection that still works
through its original tower.

**Towers named by a peer** (decision 16, settled on #73: exchange and show only). Over an
established link with a peer in the peer book, a station sends its own working set. What arrives
must be `https`, in the canonical form, with a root path, at most 8 entries and within the 64 KiB
frame cap; anything else drops the whole list. The station keeps what arrives as candidates, with
the source `peer` and the peer's name, and shows them. It never connects to a candidate, never adds
one to its relay map and never passes one on, also when every tower of its own set has failed. A
chief may take a candidate into the set with an explicit command, `chief tower add <url>`, as with
any tower URL the chief types; that is the chief's own act, so hard rule 8 is not touched. A station
whose towers are all gone therefore recovers through SRV or through the chief, not through a peer.

**What the station shows.** `home_relay_status()` lists the home tower only, so the API distinguishes
an observed state from an unobserved tower in the station's set. The home tower can be connected,
failing or `Denied { reason }`. Unobserved towers are shown as `Standby`, including when the home
status is empty, such as after a TLS failure with a certificate the station does not trust. Each
tower carries its source (`configured`, `remembered`, `srv`, `peer`). `Status` carries the bootstrap
domain, and the API carries the candidates with the peer's name. `chief status` prints all of it,
with control characters in peer and tower text replaced with spaces, and whether each peer is
relayed or direct, as in M1.
The station caps each cleaned tower error or denial reason at 1,024 UTF-8 bytes before it reports it.

**CLI surface**

```
tower serve --config /etc/numbers-tower/tower.toml    # TLS tower with allowlist and rate limit

station run --domain example.com                      # SRV on the first run; the domain is stored
station run                                           # the remembered set; SRV only as above
station run --tower https://tower1.example.com --tower https://tower2.example.com
                                                      # pins for this run; no SRV, not remembered
station run --tower https://127.0.0.1:8443 --tower-ca cert.pem   # trust the certificate in cert.pem
chief status                                          # towers: state, source, denial, candidates
chief tower add https://tower3.example.com            # take a candidate into the set
```

**Deliberately not in M2:** ACME tested at runtime (it needs port 443 on a public host; #71),
reload of the tower's config or allowlist on a signal (#69, #71), an allowlist fed from the roster
and signed tower-set statements (M3), automatic use of a peer's candidates (#73), a TXT record
(the alternative behind `Locator`, #38), DNS or SRV in the shell walkthrough (the library tests
cover them; #84), distro packages (#82), iOS builds, push and the app (M14), `chief network` (M3),
the tower's own endpoint (M3). Router port mapping stays off; whether stations use it is open
(decision 15, #42).

**Waiting for the owner:**

- The FFI smoke check (#83, #44). UniFFI's generated code uses `unsafe` (`#[unsafe(no_mangle)]` and
  unsafe blocks), which hard rule 10 forbids. Either the owner grants an exception for
  `crates/chief-ffi`, or the check moves out of M2.
- A public host and a DNS zone for the demo (#46; the runbook is #85). Nothing else waits on them.
- Whether a station may ever use a peer's candidates on its own (#73). Under hard rule 8 a station
  never opens a URL because a peer said so; automatic use would need the owner's exception and a
  destination policy. Nothing in M2 waits on it.

## Verification

- `cargo test --workspace`, in-process, bound to 127.0.0.1, port mapping and captive-portal check
  off, so no external network is touched. Timeouts are config (`connect_timeout`, `send_deadline`),
  set to a few seconds in tests:
  - chat round trip: one `Tower` + two `Station`s with temp data dirs; A sends, B receives, A sees
    `Delivered`; B replies; both sending first at the same moment also works
  - the same with IP transports disabled, asserting the selected path is the relay
  - a station not in the peer book: its send ends `Failed` at the deadline and the callee records
    neither an event nor a link; `peer rm` closes an open link
  - a peer restarts and the next send reconnects; a resent message id produces one event
  - a second `Station::start` on the same data dir is refused; start → shutdown → start works in one
    process; a station started while its tower is down returns at once and reports that
  - local socket: status, peer add, send, subscribe; a message received with no client attached is
    returned by the log; subscribe-then-page yields no duplicate; a subscriber that does not read
    gets `Resync`; dropping a pending `next()` loses nothing; a leftover socket file is replaced; a
    0755 socket directory is refused; two stations with different data dirs serve two sockets
  - frame codec (round trip, oversize rejected) and control-character replacement (API rule 9)
- `scripts/walkthrough.sh`, run by CI with the real binaries: one `tower`, two `station run` in temp
  dirs, `chief peer add` both ways, `chief send`, assert on `chief log` on the other side, stop both
  stations and assert the sockets are gone. The README carries the same steps for a person.
- M2 adds the multi-tower test and the real-world demo on a public host.
- CI green on the PR.

**M2.** The same rules hold: in-process, on 127.0.0.1, no query leaves the host, ids generated at
run time.

- `cargo test --workspace`:
  - tower: a listed endpoint comes online and an unlisted one does not within 10 s, with
    `auth_denied_reason()` set and `denied` counted; an empty list admits nobody; a low rate slows a
    relayed transfer and is counted; a bad allowlist entry fails `spawn` without echoing it (#69)
  - TLS tower with a test certificate made at run time: an endpoint that trusts it comes online and
    one that does not never does; `/healthz` over HTTPS answers a client that trusts it; QAD runs
    (`qad_connections >= 1`); the plain-HTTP listener answers `/generate_204`; missing or malformed
    certificate and key files, and a key that does not match its certificate, fail `spawn` with no
    file content in the error; a reload to a broken pair keeps the pair in use (#70); the
    `tower serve` config parses and starts in-process, and a config with both TLS and ACME is
    refused (#71)
  - tower set: canonical URLs, `http` refused off loopback, the cap of 8, the serde round trip (#67)
  - SRV against the fake DNS server: two targets give two canonical towers; NXDOMAIN, an empty
    answer and the target `.` give no tower; a silent server fails within the deadline (#72)
  - several towers: prove both endpoints have tower 1 as home, stop that tower, and explicitly
    nudge the endpoints. A fresh relay-only connection goes through tower 2 within 30 s.
    `set_towers` moves the home without rebinding when a replacement answers. The configured
    snapshot omits a removed home even if iroh remains connected. A denied home retains the last
    observed denial during retries, and other configured towers are unobserved (#75)
  - two endpoints chat through a TLS tower with QAD, and a tower named `tower1.example.com` is
    reached through the fake DNS server (#76)
  - a station given a domain gets its towers from a counting locator and chats; restarted, it comes
    online and the locator is never called; pins leave `towers.json` unchanged; a corrupt
    `towers.json` is reported and ignored (#77)
  - with its towers up, the locator is not called over 3 × `bootstrap_after`; with every known
    tower stopped, it is called once and the station chats through the new towers; a `NoTower`
    answer keeps the old set; an unlisted station shows `Denied` and its sends fail; the failover
    test proves the home tower first, then a fresh relay-only connection through the other; a sole
    denying tower gets no nudge and no DNS query (#78)
  - daemon flags, a first run without towers refused, a bad `--tower-ca` file (#79); `chief status`
    against a fake station socket covers each state and source (#80)
  - towers protocol: a peer's extra tower shows as a candidate and is never connected to, added to
    the relay map or passed on, also after the station's own towers stop; a bad list is dropped
    whole; only peers in the peer book are heard (#81)
  - each binary parses the `ExecStart` of its systemd unit (#82)
- CI also runs `systemd-analyze verify` on both units (#82), and generates the Swift bindings once
  #83 is unblocked.
- `scripts/walkthrough-tls.sh`, run by CI with the real binaries: `tower serve` with a self-signed
  certificate for 127.0.0.1 and an allow file listing two stations, two `station run` with
  `--tower https://127.0.0.1:<port>` and `--tower-ca`, `peer add`, `send`, an assertion on `log`,
  and a third, unlisted station shown as denied in `chief status` (#84). The M1 walkthrough runs
  unchanged (#71, #79).
- The demo on a public host, by hand (#46, needs the owner): a TLS tower as a systemd service, the
  SRV record in the zone, two stations on different networks given only the domain chat relayed and
  direct, and an unlisted station is denied. Only this shows TLS, QAD and SRV across real networks,
  and ACME if the host uses it.

## Decisions and open questions

Each is a `Decision:` issue with a recommendation, attached to the milestone it gates.

1. **Chief key model (M3).** Planned: a stable chief id with one admin key per device, never copied.
   Alternatives: one chief key that the chief copies between devices (a copy cannot be revoked on
   its own); or no admin keys — a chief is the set of their stations' keys (then the always-on
   daemon holds admin power). Also to settle: an admin key kept only on the phone, which is the one
   option that holds against a compromised workstation, and expiring station leases. With a
   Nitrokey HSM, how many tokens: one for the network plus a spare in a safe, brought out for each
   enrollment (enough for a small team, since enrollments are rare and every one is still signed by
   a chief's own key); or one per chief, so each can enroll alone and a lost token is revoked on its
   own.
2. **Disputes and recovery (M3).** What settles two chiefs removing each other, and what happens when
   a chief loses every admin key: an offline recovery key recorded in genesis (an HSM-held key is
   the natural one); the earliest-admitted chief wins; or countersignatures once there are three
   chiefs.
3. **Safehouse isolation (M5).** How a safehouse puts each chief's terminals and agents under their own
   OS account (root helper, systemd transient units, or containers), and what the warning for a
   machine that also runs a station checks. A same-account sandbox is not enough on current
   kernels (handlers may later move this boundary to one safehouse per chief or per job, decision
   14): Landlock cannot yet block
   connecting to a Unix socket, and same-user processes can attach to each other.
4. **Airspace engine (M4).** `iroh-blobs` 0.x behind our own interfaces (planned), or our own small
   block protocol over iroh from the start. Includes how hard a tower syncs to disk before it
   confirms.
5. **Airspace format (M4).** Planned: a fresh random key per block, with members deduplicating through
   an encrypted index, so towers never learn that two blocks are equal. Alternative: keyed convergent
   encryption, which lets towers deduplicate. Also block and pack sizes, and padding. This cannot be
   changed later without re-uploading everything.
6. **Names without a trusted sequencer (M4, M7).** The fork rule; how many towers must accept a
   compare-and-swap; whether a write with no tower reachable fails or is published later; whether an
   always-on safehouse should order writes instead.
7. **Codex browser login (M6).** Settled by the owner: dropped, with every other sign-in flow.
   Harnesses authenticate with API keys only.
8. **Harness API keys (M6).** API keys only is decided. Open: where a key is kept on the
   safehouse and in what form; whether the harness process gets the key or a proxy on the safehouse
   adds it to requests, so that an agent never holds it; one key per chief (planned) or a key a team
   shares; whether cloud-provider credentials for Claude (Bedrock, Vertex, Foundry) are in scope
   (planned: not at first); whether presets also switch off a harness's other sign-in methods where
   it has a setting for that.
9. **Harness transport, per harness (M8).** ACP adapters or native protocols. For Claude: the ACP
   adapter (an Agent SDK app), `claude -p` stream-json on the unmodified CLI, or Claude's own
   background sessions. For Codex, `codex app-server`. With API keys only, the Agent SDK terms on
   claude.ai login no longer bear on the choice.
10. **Git details (M7).** Whether a station also keeps a mirror; bundle or raw pack; when and who
    compacts; branch protection from the roster (for example agents confined to a session namespace);
    what the phone can do with repos; whether the Git LFS adapter is in scope.
11. **Vendor remote features (M8).** Claude Remote Control overlaps part of M8/M14 but is relayed by
    Anthropic. Offer it as an optional preset, or disable it by default on safehouses.
12. **The phone (M14).** Whether it embeds a station (planned) or remote-controls the chief's workstation
    station; and push — only the app's publisher can send it, so either each team signs its own app
    or the project runs a push gateway.
13. **Bootstrap record (M2).** Decided by both agents (#38): SRV with one target per tower, as
    first specified, looked up behind the `Locator` trait of `numbers::bootstrap`, so that another
    record type stays one more implementation. A multi-A record on one name does not work with more
    than one tower: iroh tells towers apart by URL and towers do not forward to each other, so two
    stations that land on different addresses of the same name cannot reach each other; and every
    tower would need a certificate for the shared name. The resolver is in the build already,
    through iroh. If SRV proves awkward on phones, a TXT record listing tower URLs keeps one name
    per tower.
14. **Handlers (M15).** What a handler is: how a chief asks for a safehouse, whether a handler may
    enroll one under a chief-signed, expiring grant or every admission needs the chief (and the HSM);
    nspawn or QEMU; one safehouse per chief or per job; what the handler host itself must never
    expose to a safehouse.
15. **Smaller ones.** GTK4 / libadwaita / relm4 / VTE assumed for the GUI (M13); whether stations use
    router port mapping (off in M1).
16. **Tower sets passed on by peers (M2).** Decided by both agents on #73: exchange and show only.
    A station sends its own working set to peers in its peer book and shows what it gets as
    candidates with the peer's name; it never connects to one, adds one to its relay map or passes
    one on, and a chief may take one into the set with an explicit command. Whether a station may
    ever use a peer's candidates on its own is an exception to hard rule 8 and left to the owner.
17. **Recovering a tower (M2).** Decided by both agents on #74: a watchdog in the station requests
    a fresh net report only after it has seen no home tower, or a home tower that is not
    connected, for some seconds, at a growing, capped interval, and never while the home tower is
    connected. A denying tower is shown and taken out of the relay map only when another tower is
    not denied; the station limits its own attempts and DNS queries, not iroh's own retries.

**Needs from the team:** a public host and DNS zone (M2 demo; nothing else waits on it); a ruling
on hard rule 10 for `crates/chief-ffi`, whose generated bindings use `unsafe` (M2 FFI smoke check,
#83); Nitrokey HSM 2
tokens, one plus a spare or one per chief (M3 manual check — CI uses a software token); two or three
tower hosts once airspace should be durable (M4 demo — CI uses in-process towers); an Anthropic API
key and an OpenAI API key for the manual checks (M5, M6, M8 — CI uses fake harness CLIs); a Mac,
an iPhone and an Apple Developer membership (M14, and an early device spike worth doing right after
M2). iOS cannot be built in this environment.

**Vendor terms.** Harnesses authenticate with API keys only: each chief uses a key the vendor
issued to them or to their organisation, under that vendor's API terms, with the unmodified
official CLI. theCompany drives or relays no subscription sign-in (hard rule 3). Revisit before
distributing beyond the team or hosting it for others.

**Interpretations of the original request (correct if wrong):**

- The original rule "agents never operate on dead-drop or chief nodes … strictly confined to handler
  nodes" now reads "never on towers or stations, strictly confined to safehouses", and is also read
  as "never on the new handlers": a handler creates safehouses but runs no agents itself.
- A chief may have several stations; the phone app counts as one.
- Tower "discovery" means finding a node's current address by its id. Who is in the network comes
  from the signed roster, never from a tower.
- SRV bootstrap lands in M2 together with the TLS tower; M1's dev tower is given by URL.
- "Finding other nodes through the network" means: addresses a node already holds, then the signed
  address records it learns from nodes it is connected to (and from M3 the signed roster's tower
  set), then every tower of its own set. A tower set that a peer sends is shown as candidates and
  not used on its own (decision 16).
- Airspace starts with one team-wide space that every member can read; restricted spaces come later.
- "Text objects" are named, versioned documents; git repos are a real remote, not snapshots.
- "Files" (push/pull between two nodes, browsing a safehouse's workspace) stays a separate, smaller
  feature built on airspace's block transfer.
- "Sockets" are stream forwards between two member nodes in the style of `ssh -L`, opened from a
  station; no reverse forwards.
- "Locks" are advisory named leases with TTL and fencing tokens, arbitrated by the node that owns the
  resource.
- Names chosen here: `chief` for the CLI, `_numbers._tcp` for the SRV label.
