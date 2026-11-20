# theCompany — roadmap

What gets built, in which order, and what is still open. The design itself is in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Roadmap

Each milestone is one small, demoable PR series with an automated check and a manual demo.

| # | Milestone | Done when | Needs |
|---|---|---|---|
| M0 | Scaffolding: workspace, toolchain pin, CI with the confinement lint, CLAUDE.md, docs, tracking issues | CI green with a real test per crate; issues exist | — |
| **M1** | **Two chiefs chat**: dev tower, station daemon, `chief` CLI over the local socket, manual peer book | CI: chat through an in-process tower, the same relay-only, a stranger refused, a second station on one data dir refused, CLI ↔ station over the socket, and a scripted walkthrough with the real binaries | M0 |
| M2 | Real-world tower, station as a service: TLS (cert files; ACME optional), QAD, SRV bootstrap, several towers, a remembered tower set that peers pass on (DNS again only when no known tower answers), static allowlist + rate limit, systemd units, FFI smoke check | CI, in-process: TLS tower with a test certificate, QAD, fake SRV resolver; a station that knows its towers starts without touching DNS and returns to SRV only when none answers; an unlisted station id is denied. Demo: two stations on different networks, given only a domain, chat, relayed or direct | M1; decision 13; **needs:** a public host and a DNS zone (demo only) |
| M3 | Membership: chief ids and admin keys, an enrollment key on a Nitrokey HSM with the enrollment-requires-HSM rule, signed roster with the tower set, invites, station commissions, roster sync, signed address records passed between nodes, removal, the tower's own endpoint with a roster-fed allowlist; `chief network` | a second chief joins by invite through an allowlisting tower and chats without `peer add`; with the HSM rule on, an enrollment without the token's signature is rejected by every node, while removal and daily use work without the token (CI: a software PKCS#11 token; a real Nitrokey by hand); a chief revokes one of their two devices and keeps working from the other; a removed chief is refused by every node after sync | M1 (allowlist feed: M2); decisions 1, 2; **needs:** Nitrokey HSM 2 tokens |
| M4 | Airspace I: sealed blocks on towers and members, spaces and keys, names, leases and quotas, the two-tower rule with tower-to-tower pull; text objects; `chief airspace` | an object written on one station is read on another after the first went offline; the tower's disk holds only ciphertext; "stored" needs two towers; a rolled-back name is refused; unleased blocks lapse | M3; decisions 4, 5, 6 |
| M5 | Safehouse, set up by hand by a chief: admission, `chief safehouses`, safehouse-hosted terminals (`chief shell`), `pty` harness presets, per-chief OS accounts, confinement checks | a chief runs `claude` and `codex` on a safehouse and logs in by hand; an agent process cannot read the safehouse's key or another chief's home | M3; decision 3 |
| M6 | Login broker for Claude and Codex: structured flows, guards, status, expiry, logout | both harnesses signed in from a chief with no SSH; CI drives the flows with a fake harness CLI | M5; decisions 7, 8; **needs:** Claude and ChatGPT accounts |
| M7 | Airspace II: git repos — remote helper, bundles and manifests, mirrors on safehouses, compaction | `git clone`, `fetch` and `push` between a station and a safehouse with only towers always on; a non-fast-forward push is refused; two racing pushes leave one tip and one conflict ref | M4 (safehouse side: M5); decision 10 |
| M8 | Harness sessions: session host, journal, ACP kind, Claude + Codex presets, ownership policy, workspaces from airspace; `chief sessions`, `chief harness` | prompt, stream and approve from a chief; reattach from a second station; a non-owner cannot steer. CI: a fake ACP agent | M5 (M6 for real accounts); decisions 9, 11 |
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
  (`default-features = false, features = ["tls-ring"]`, so far only read), and later
  `cargo tree -p <pkg> -e normal,build` for the confinement rule.
- `CLAUDE.md`: build and test commands plus the hard rules — agent confinement; the harness-credential
  rule; towers never hold keys; one station per identity; frontends hold no network state; libraries
  install no process-global state and spawn nothing; API types must be FFI-exportable.
  `docs/ARCHITECTURE.md` and `docs/ROADMAP.md` distilled from this plan.

**M1 — crates and modules**

| Path | Contents |
|---|---|
| `crates/base/src/frame.rs` | `write_frame` / `read_frame`: u32 length + JSON, with the size cap and read deadline as parameters (64 KiB on the wire, 1 MiB on the local socket) |
| `crates/base/src/keyfile.rs` | key-file helper for daemons: create with `O_EXCL`, mode 0600 inside a 0700 dir, refuse a file with the wrong owner or mode. Keys reach the libraries as bytes |
| `crates/tower/src/lib.rs` | `TowerConfig`, `Tower::spawn(cfg)` wrapping `iroh_relay::server::Server::spawn` in plain-HTTP mode with a per-client rate limit; `url()`, `shutdown()`; no `iroh_relay` type in its public API |
| `crates/tower/src/main.rs` | `tower [--bind 127.0.0.1:3340]`; loopback only in M1 |
| `crates/numbers/src/identity.rs` | `NodeId` (iroh `EndpointId`) and node-key generation |
| `crates/numbers/src/towers.rs` | `TowerSet`: canonical URL list with a QAD setting per tower. M1 takes one explicit URL |
| `crates/numbers/src/endpoint.rs` | builds the iroh `Endpoint` from `presets::Minimal`: relay map with an explicit config per tower, `TowerLookup` (an `AddressLookup` mapping any node id to the tower URLs, so dialing needs only an id), port mapping off, lowered stream caps, a relay-only switch for tests |
| `crates/numbers/src/gate.rs` | the one authorization point: an `Authorizer` that maps a node id and protocol to a caller (a peer name in M1, a roster entry later) or a refusal; a pre-handshake filter that ignores relayed attempts from ids it does not know and demands address validation from direct ones; the `after_handshake` hook; close codes |
| `crates/numbers/src/link.rs` | ALPN `numbers/0`; a hello frame each way (`v`, with `network` and `head` empty until M3); one link per peer — either side opens streams on it, and if both dialed, the one dialed by the lower node id is kept; `StreamHeader {proto, v}`; stream dispatch; timeouts from config |
| `crates/numbers/src/proto/chat.rs` | `ChatMessage {id, sent_at_ms, body}` answered by `Ack` / `Nack`; one stream per message; 16 KiB body cap; the receiver dedupes on (peer, id); the sender's id and time are untrusted |
| `crates/chief/src/api.rs` | the station API: `Request`, `Response`, `Event`, `ApiError`, `Status`, `Peer {name, stations: [{station_id, link}]}`, `Message {seq, id, peer, direction, body, at_ms, status}`; FFI-exportable types only |
| `crates/chief/src/client.rs` | `Transport` (methods return `impl Future + Send`; static dispatch), `SocketTransport`, `Client` with typed methods, a concrete `Subscription` whose `next()` is cancel-safe |
| `crates/station/src/lib.rs` | `Station::start(StationConfig)`: one actor owning the endpoint, the peer book (`peers.json`, written only by the actor) and a ring of the newest 1,000 messages; data-dir lock; never awaits the network at start; live events with `Resync` on lag; message outcome kept as state; strips control characters from peer text; usable in-process as a `Transport` |
| `crates/station/src/ipc.rs` | the socket server (rules below); hello `{api: 1}` answered with the station id and a per-start instance id; one connection per call or subscription; read deadlines on calls, never on an open subscription |
| `crates/station-daemon/src/main.rs` | `station run [--data-dir DIR] --tower URL`: foreground daemon, creates its key on first run, graceful shutdown |
| `crates/chief-cli/src/main.rs` | `chief` (clap): `status`, `id`, `peer add\|ls\|rm`, `chat`, `send`, `log` |

**Local socket.** The data dir defaults to `$XDG_DATA_HOME/thecompany/station`. The socket path is a
function of the data dir: `$XDG_RUNTIME_DIR/thecompany/station.sock` for the default data dir when
that variable is set, otherwise `<data-dir>/station.sock`; `chief --station PATH` overrides. The
station takes the data-dir lock first and only then removes a leftover socket, binds and sets 0600.
The directory is created 0700, and an existing one must be owned by the same user with no group or
other access. A path too long for a socket address is refused with a message naming `--data-dir`.
The peer uid comes from `tokio::net::UnixStream::peer_cred()`.

**CLI surface**

```
tower                                         # dev tower on 127.0.0.1:3340, plain HTTP

station run --tower http://127.0.0.1:3340     # the chief's node; keeps running
chief id                                      # this station's id, to hand to the other chief
chief peer add bob <station-id> | peer ls | peer rm bob
chief chat bob                                # last 50 lines, then live; shows other peers too
chief send bob "on my way"                    # waits for the outcome: exit 0 delivered, 1 failed
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
  - frame codec (round trip, oversize rejected) and control-character stripping
- `scripts/walkthrough.sh`, run by CI with the real binaries: one `tower`, two `station run` in temp
  dirs, `chief peer add` both ways, `chief send`, assert on `chief log` on the other side, stop both
  stations and assert the sockets are gone. The README carries the same steps for a person.
- M2 adds the multi-tower test and the real-world demo on a public host.
- CI green on the PR.

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
7. **Codex browser login (M6).** Its callback sequence passes an ID token through the relay. Keep it
   as a fallback behind device code, or drop it so that no token ever transits a station.
8. **Raw API keys and cloud-provider credentials for Claude (M6).** Unsupported at first, or a
   per-chief secret kept on the safehouse.
9. **Harness transport, per harness (M8).** ACP adapters or native protocols. The Claude ACP adapter
   is an Agent SDK app, and Anthropic's Agent SDK terms restrict offering claude.ai login in
   third-party products; compare it with `claude -p` stream-json on the unmodified CLI and with
   Claude's own background sessions. For Codex, `codex app-server`.
10. **Git details (M7).** Whether a station also keeps a mirror; bundle or raw pack; when and who
    compacts; branch protection from the roster (for example agents confined to a session namespace);
    what the phone can do with repos; whether the Git LFS adapter is in scope.
11. **Vendor remote features (M8).** Claude Remote Control overlaps part of M8/M14 but is relayed by
    Anthropic. Offer it as an optional preset, or disable it by default on safehouses.
12. **The phone (M14).** Whether it embeds a station (planned) or remote-controls the chief's workstation
    station; and push — only the app's publisher can send it, so either each team signs its own app
    or the project runs a push gateway.
13. **Bootstrap record (M2).** Planned: SRV with one target per tower, as first specified. A multi-A
    record on one name does not work with more than one tower: iroh tells towers apart by URL and
    towers do not forward to each other, so two stations that land on different addresses of the
    same name cannot reach each other; and every tower would need a certificate for the shared name.
    If SRV proves awkward (an extra resolver dependency, phones), a TXT record listing tower URLs
    keeps one name per tower and uses the resolver iroh already has.
14. **Handlers (M15).** What a handler is: how a chief asks for a safehouse, whether a handler may
    enroll one under a chief-signed, expiring grant or every admission needs the chief (and the HSM);
    nspawn or QEMU; one safehouse per chief or per job; what the handler host itself must never
    expose to a safehouse.
15. **Smaller ones.** GTK4 / libadwaita / relm4 / VTE assumed for the GUI (M13); whether stations use
    router port mapping (off in M1).

**Needs from the team:** a public host and DNS zone (M2 demo; nothing else waits on it); Nitrokey HSM 2
tokens, one plus a spare or one per chief (M3 manual check — CI uses a software token); two or three
tower hosts once airspace should be durable (M4 demo — CI uses in-process towers); Claude and
ChatGPT accounts for the manual login checks (M6, M8 — CI uses fake harness CLIs); a Mac, an iPhone
and an Apple Developer membership (M14, and an early device spike worth doing right after M2). iOS
cannot be built in this environment.

**Vendor terms.** Each person signing in to the unmodified official CLI with their own account, in a
self-hosted team tool, is inside both vendors' published terms. Hard rules: never inject refresh
tokens, never copy credential files between nodes, never hide a login method the CLI offers. Revisit
before distributing beyond the team or hosting it for others.

**Interpretations of the original request (correct if wrong):**

- The original rule "agents never operate on dead-drop or chief nodes … strictly confined to handler
  nodes" now reads "never on towers or stations, strictly confined to safehouses", and is also read
  as "never on the new handlers": a handler creates safehouses but runs no agents itself.
- A chief may have several stations; the phone app counts as one.
- Tower "discovery" means finding a node's current address by its id. Who is in the network comes
  from the signed roster, never from a tower.
- SRV bootstrap lands in M2 together with the TLS tower; M1's dev tower is given by URL.
- "Finding other nodes through the network" means: addresses a node already holds, then the tower
  set and signed address records it learns from nodes it is connected to, then every known tower.
- Airspace starts with one team-wide space that every member can read; restricted spaces come later.
- "Text objects" are named, versioned documents; git repos are a real remote, not snapshots.
- "Files" (push/pull between two nodes, browsing a safehouse's workspace) stays a separate, smaller
  feature built on airspace's block transfer.
- "Sockets" are stream forwards between two member nodes in the style of `ssh -L`, opened from a
  station; no reverse forwards.
- "Locks" are advisory named leases with TTL and fencing tokens, arbitrated by the node that owns the
  resource.
- Names chosen here: `chief` for the CLI, `_numbers._tcp` for the SRV label.
