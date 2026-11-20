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
| **chief** | A person on the team. An identity, not a machine: a chief owns stations and harness API keys, signs admin actions, and agent sessions run under their name. |
| **station** | A chief's node: a daemon on the chief's workstation. The `chief` CLI, the Linux GUI and the iOS app are frontends to a station. |
| **safehouse** | A node that runs agents in arbitrary harnesses (Claude Code and Codex first). At first a chief sets one up by hand. |
| **handler** | Later: the node that sets safehouses up, creating each in an isolated environment (an nspawn container, maybe a full QEMU VM). To be fleshed out. |
| **tower** | An untrusted node: relays traffic, lets nodes find each other, and is the final store for encrypted blocks. A DNS SRV record leads a newcomer to its first tower. Never a member. |
| **agent** | A harness process on a safehouse. Agents run nowhere else. |
| **airspace** | The persistence layer: text objects, git repos, occasional multi-gigabyte media. Every node takes part. |

Decided: **Rust**; **iroh** as transport (1.0 since June 2026, now 1.5.0: stable API and wire
protocol, dial-by-public-key QUIC, hole punching, relay fallback, official iOS support); the audience
is a **small trusted team** — chiefs are co-equal, every action is attributable to a person, one
person can be removed cleanly, every chief gives harnesses their own API key, and harnesses
authenticate with **API keys only** (no subscription sign-in is driven, relayed or supported); work is
**tracked in GitHub issues**; in airspace, **every tower keeps every block and a write counts as
stored once two towers confirm it**; a station and a safehouse on one machine is **never suggested
and always warned against, but the two packages do not conflict**; the **SRV record only bootstraps
access — a node already on the network finds other nodes through the network first and falls back
to SRV last**; a network can **require a Nitrokey HSM to set up new nodes — and only for that, never
for day-to-day use**. We build feature by feature. The first slice is a minimal tower and station,
just enough for two chiefs to chat.

How far this has been checked: the transport, tower, login and first-slice parts were reviewed
against the published sources of iroh 1.5.0, Claude Code 2.1.328 and Codex 0.170, with a second
reviewer trying to refute each finding; the API-key design that has since replaced the login part
has not had that review. The chief/station identity model and the first slice had one
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
| safehouse | `safehouse` | member, admitted by a chief | hosts agents, terminals and harness API keys; serves files, sockets, locks; keeps working copies of airspace | admin actions; starts anything on a station |
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
  decommission; the chief, their accounts on safehouses and their harness API keys stay (**decision 1**).
- **What a station may do** is act as its own chief on each safehouse: that chief's terminals,
  sessions, files and chat. Anything that changes what other chiefs' sessions run, or what a node
  allows others — shared harness definitions, forward-target policy, grants, the tower set — is an
  admin-signed statement. So a stolen station is that chief on every safehouse until
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
  **Nothing else needs the token.** Not daily use — chat, airspace, driving safehouses, API keys,
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
  forwards, stops their sessions and locks their harness profiles. Purging (API-key and file
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
by id through the towers, and iroh moves the connection to a direct path when it can. When that
tower dies, iroh moves the node to another tower only after about two net reports (41–49 s), and a
connection relayed through the dead tower does not move: it ends at the idle timeout and is dialed
again. So a station watches its home tower (below).

- **SRV only bootstraps.** A newcomer resolves `_numbers._tcp.<domain>` (SRV) to
  `https://target:port` to reach its first tower; `--tower <url>` does the same by hand for
  development and tests. A node that is already on the network finds towers and other nodes through
  the network first, in this order: paths and addresses it already holds; what the network tells it,
  which is other nodes' signed address records, passed on by any connected node, and from M3 a
  signed roster statement carried in invites; then every tower in its own set, which is the towers
  it was configured with and the ones it remembered across restarts; and only then the SRV record
  again. A changed SRV record therefore matters to newcomers and to a node that has lost every
  tower, not to a running network.
- **Towers named by a peer are candidates, not part of the set.** Stations exchange their working
  tower sets over an established link. What a peer sends is checked, kept as candidates with the
  peer's name, and shown. A station never connects to a candidate, never adds one to its relay map
  and never passes one on; a chief may take one into the set with an explicit command (decided in
  #73). Automatic use would be an exception to hard rule 8 and is the owner's to grant.
- **A station watches its home tower.** It acts only after it has seen no home tower, or a home
  tower that is not connected, for some seconds, and never while the home tower is connected. Then
  it removes and re-inserts the tower, which makes iroh probe at once, and repeats that at a
  growing, capped interval. A tower that denies the station is marked and shown with the reason. It
  is taken out of the relay map only if another tower of the set is not marked denied; if it is
  the only tower, or every tower denies, the station changes nothing in the map, does not nudge and
  does not ask DNS because of the denial. Putting a denied tower back and asking DNS again happen
  only when the set changes or at a long interval. The station limits its own attempts; it does not
  control iroh's own retries (decided in #74, built in #78).
  Station API version 2 represents an unobserved tower as `Standby`, including when there is no
  home tower observation. An explicit refusal remains `Denied`. Status can also carry the source
  of each tower and the SRV bootstrap domain.
- **What a tower can do.** It sees who talks to whom, from which IP, when and how much, and (with the
  airspace) which node keeps or fetches which block. It can drop traffic and lose blocks. It, or whoever
  hijacks the SRV record, can forge the source id of a relayed packet far enough to learn a node's
  candidate addresses and make it send a few UDP probes. It cannot read, alter or impersonate member
  traffic or stored data, and can never become a member. DNSSEC or pinning would protect availability
  and metadata only.
- **Same set everywhere.** With no better address, dialing tries every tower in the set at once.
  Towers do not forward to each other, so a dialer reaches a peer only through a tower both of them
  can reach; all nodes therefore use the same small set (two or three). SRV priority and weight are
  ignored.
  `TowerSet` caps the set at eight unique canonical URLs. Each `TowerEntry` carries its own QAD
  setting and defaults to UDP 7842 for HTTPS or QAD off for plain HTTP.
- **Not an open relay.** A production tower relays only for listed node ids and rate-limits each
  client: a static list in M2, fed from the roster in M3 (verifiable against the genesis hash, no
  secret). The list covers relaying only: an unlisted node can still use the tower's address
  discovery and latency probe. Removing an id does not end its open connection; the tower has to
  disconnect it. The rate limit slows a client down without errors. This is hygiene run by an
  untrusted party, not a security boundary.
- **Its own endpoint.** The relay server cannot host extra routes, so from M3 a tower also runs an
  iroh endpoint with its own node key. Members reach its services there, authenticated by node id:
  the roster feed first, storage from M4. Towers also use it to pull blocks from each other.
- **Direct paths across NATs need the production tower.** Address discovery (QAD, UDP 7842) requires
  TLS, and a relay-only node runs none. M1's plain-HTTP dev tower relays, and gives direct paths
  only on the same host.

### Link and protocols

Two members share **one authenticated link** (ALPN `numbers/0`). It opens with a hello (version,
network id, roster head, link generation, and a freshness challenge). Each feature then opens a
typed stream whose first frame is `{proto, v}` — `chat`, `roster`, `airspace`, `pty`, `sock`,
`agent`, `apikey`, `file`, `lock`. One gate decides, default deny, whether this caller may open
this protocol:

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

In M1, the `numbers/0` dialer opens a bidirectional stream and each end sends a hello with
`v: 1`, `network: ""` and `head: ""`. Both reserved fields must be empty strings. The initial
client hello omits `echo`, and receivers also accept `null` as absent. The `generation` stays
the same for each running link instance, while the `challenge` is fresh for each hello. Both
are nonzero arrays of 16 random bytes. The server includes the client's challenge as `echo`
in its hello. The client verifies that echo, then returns the server's challenge in an
`{"echo":[…]}` frame. The server checks the echo before admitting its connection.

A new authenticated generation can replace a connection left open by a crash. A hello for a
different generation is admitted only if the remembered generation has stayed the same since
before the fresh challenge. When concurrent hellos present different generations, the first
admission that changes the remembered generation takes precedence. A later handshake can start
from the updated value. For the same generation, crossed dials keep the connection initiated by
the lower node ID in lexicographic order of the 32 public key bytes. A fresh connection replaces
one from the same initiator. There is at most one pending incoming hello per peer. A second incoming
connection while that hello is pending is refused immediately with code `0`. A sender whose
outgoing dial loses can wait for the pending hello to establish a link and redial once if it fails.
If the fallback also loses and no open link, incoming hello or shared dial remains, the send
returns `Disconnected` even if the deadline has time left. A send that shares a failed dial
receives that dial's error under the same conditions. When the direct-only restart tests were run
on a multi-thread runtime, iroh could redial the peer's old address immediately after a stall
close. M1 relies on the tower path for recovery in this case.
[#120](https://github.com/tiltbob/thecompany/issues/120)

If the higher-ID peer closes a link initiated by the lower-ID peer and the close is lost, the
lower-ID peer can still see the link as open. While the generation stays the same, the lower-ID
peer can refuse both dials from the higher-ID peer with code `409`. The higher-ID peer can
reconnect once the lower-ID peer detects the old connection's closure or retires it. M1 relies
on QUIC and the send progress check for that detection. When looking for a connection after a
lost election, it makes at most one fallback dial.

Link application close codes are `400` for a bad hello or an acceptor's hello timeout, `403` for
authorization refusal, `409` for replacement, and `0` for ordinary closure. At its own setup
deadline, the dialer reports `Timeout` and closes any pending connection with `0`. During setup,
a remote `400` or `403` ends the dial as `BadHello` or `NotAllowed`, respectively. A remote `409`
uses the winner and fallback rules above. Other 32-bit application close codes are classified as
`Disconnected`. During the hello exchange, a close without an application code, such as a
transport-level close, or with a code above 32 bits ends the dial as `BadHello`, because every I/O
failure of the hello exchange is mapped to it. Either end of an admitted link can open a
bidirectional chat stream with the header `{"proto":"chat","v":1}`, followed by one message.
Each JSON frame has a four-byte big-endian length and a payload limit of 64 KiB.

A chat message carries `id`, `sent_at_ms` and `body`. The sender assigns an ID of 1–64 ASCII
letters, digits, underscores or hyphens and reports its own time in Unix milliseconds. The
receiver treats both as peer input. The body is padded standard base64 on the wire and decodes
to at most 16 KiB of UTF-8. The receiver replies with `{"type":"ack"}` or a
`{"type":"nack","reason":"…"}` containing one of `unknown_protocol`, `unsupported_version`,
`invalid_id`, `body_too_large`, `invalid_body` or `unavailable`. The sender reports `BadReply`
for an unknown NACK reason. The receiving actor acknowledges the message after deciding whether it
is a new message or a retry.

`connect_timeout` bounds each dial and hello together. `send_deadline` bounds the send, including
any shared dials, retries and waits for a reply. On the first attempt over an already established
connection, the sender checks for transport progress a full `connect_timeout` after queuing the
request, if this interval is positive and shorter than the remaining send budget. A
`connect_timeout` equal to or longer than `send_deadline` leaves the overall deadline in force
and skips that early check. If the connection is still current and has received no
authenticated QUIC frames since before the request was written, the sender closes it. A send
waiting for stream credit skips the progress check. After a crash leaves a connection with no
stream credit, sends can time out until QUIC's 30 s idle timeout or until the restarted peer
connects first. QUIC replenishes credits in batches. The stall was reproduced with 14 unanswered
sends on loopback. [#119](https://github.com/tiltbob/thecompany/issues/119)

If a chat stream disconnects before its connection reports a close reason, the sender waits for that
connection's close or peer revocation. The wait is bounded by `connect_timeout` and the time left
before the original send deadline. If the wait expires with the connection still open, the send
returns `Disconnected`. The overall deadline can expire first and return `Timeout`. The sender
can retry once after an ordinary connection closure and once after observing a replacement.
Each retry uses the same message ID and the original deadline. An `unavailable` reply to a retry
leaves delivery uncertain because an earlier attempt can still be accepted. The receiver handles
up to 16 concurrent incoming streams per link and applies `send_deadline` to each stream,
including waiting for the actor's reply. It refuses unauthorized chat on an established link by
resetting the stream with code `403`. A full or closed chat mailbox returns `unavailable`.

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
- **A station takes input from peers, never orders.** It replaces control characters, line
  separators and bidi controls (API rule 9) in every peer-supplied string before a frontend sees
  it, never opens a URL or file on a peer's say-so, accepts files only as answers to its own pulls
  or as quarantined offers, and offers no reverse forwards.
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
  (CI: clippy `disallowed-types`). One documented exception, in a frontend started by the chief:
  the git helper running git. The later
  `handler` crate will be the one other place allowed to start processes — containers and VMs,
  never harnesses.
- Only safehouses serve `pty`, `agent` and `apikey`; a station has no code that answers them.
- On a safehouse the daemon runs as its own OS account, holds the node key and never executes harness
  code in-process. Each chief maps to a dedicated OS account (or container), and every terminal and
  session of that chief is spawned under it. That, not an environment variable, is what separates
  one person's harness API keys from another's and from the safehouse's key (**decision 3**).
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
    universal fallback; it ships with the safehouse, so `claude` and `codex` are usable, with an API
    key set by hand, before M6 exists.
  - `acp` — a generic Agent Client Protocol client, with the safehouse as the only ACP client. Covers
    Claude Code and Codex through the ACP project's adapters and about fifteen other agents natively.
  - native adapters where they are better (**decision 9**, per harness).
- A session belongs to the chief who started it and runs on that chief's API key. Only the owner's
  stations may prompt, approve or type; other chiefs may watch and stop. Shared steering can be
  offered: every session runs on an API key, so the vendors' terms for subscription accounts do not
  stand in the way.

### Giving a safehouse's harness an API key

Harnesses authenticate with **API keys only**. theCompany drives no sign-in: no browser flow, no
device code, no pasted code and no callback relay, and a station never opens a URL for a harness.
A chief creates the key in the vendor's console.

`chief apikey set <safehouse> claude|codex` reads the key without echo, or from stdin, never from
the command line. The key travels over the encrypted station–safehouse stream to the safehouse,
which keeps it for that chief and that harness. Every terminal and session that chief starts there
gets the key the way the harness takes one:

| Harness | How it takes an API key |
|---|---|
| Claude Code | no login command reads a key; it takes one from the `ANTHROPIC_API_KEY` environment variable (an interactive session asks once whether to use it, `-p` uses it without asking) or from the output of the command the `apiKeyHelper` setting names |
| Codex | `codex login --with-api-key` reads it from stdin, refusing a terminal, and keeps it in Codex's own store: `auth.json` under `CODEX_HOME`, or the OS credential store |

- **Write-only from outside.** `chief apikey status` says for each harness whether a key is set,
  when it was set and whether the vendor last rejected it. It never returns the key or a part of
  it: the safehouse answers from its own record and never passes on what a harness CLI prints
  (`codex login status` prints part of the key). `chief apikey rm` removes it; replacing a key is
  `set` again.
- **Never in the open.** A key is never logged and never part of an error or an event. It never
  reaches a tower, another chief or airspace, and the station does not keep it after sending it.
- **Per chief.** On the safehouse the key belongs to the chief's OS account (**decision 3**), so
  another chief's sessions cannot read it.
- **The agent can read it.** A harness needs its key to work, so an agent in a session can read the
  key that session runs on. A spending limit set at the vendor is the control for that. Hiding the
  key from the agent behind a proxy on the safehouse that adds it to requests is part of
  **decision 8**.
- **No lifecycle to manage.** A key has no session to renew. One the vendor rejects shows in the
  status and is reported to the owner's stations.
- theCompany never reads, copies or moves a harness's own credential files. A subscription sign-in
  typed by hand into a terminal on a safehouse is outside what theCompany supports.

Still open (**decision 8**): where the key is kept on the safehouse and in what form; whether the
harness process gets the key or a proxy adds it; whether a team may share one key; whether
cloud-provider credentials count; whether presets also switch off a harness's other sign-in methods
where it has a setting for that.

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
  safehouse/        `safehouse` daemon: harnesses, terminals, API keys, MCP bridge (M5)
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

These notes draw on published sources. CI checks the isolated `numbers` dependency set with
`cargo check -p numbers --locked` and tests the M1 [dev tower](../crates/tower/src/lib.rs) and
[endpoint and gate](../crates/numbers/src/endpoint.rs) over loopback. The iroh snippets were also
compiled in a harness, and equivalent loopback lab tests supplied the measured numbers below
(see [notes/iroh-snippets.md](notes/iroh-snippets.md)).

- **Dependencies.** `iroh = { version = "1.5", default-features = false, features = ["tls-ring"] }`
  (MSRV 1.91); `iroh-relay` likewise, plus `server` in `tower` only; direct `n0-future 0.3` and
  `n0-error 1`; `n0-dns-resolver 0.1` for SRV in M2. Never enable `iroh/test-utils`. Keep
  `fast-apple-datapath` (private Apple APIs) and `portmapper` out of the libraries. Install the
  rustls crypto provider only in a binary's `main`.
- **Endpoint.** `Endpoint::builder(presets::Minimal)` with `.secret_key`, `.relay_mode(RelayMode::
  Custom(map))`, `.address_lookup(..)`, `.hooks(..)`,
  `.portmapper_config(PortmapperConfig::Disabled)`, `.net_report_config(..)` and
  `.transport_config(..)` (stream caps); `.clear_ip_transports()` gives a relay-only endpoint with
  no socket. `bind_addr` returns a `Result` and replaces only the default IPv4 socket, keeping
  `[::]` (per upstream's `test_bind_addr_no_clear`; the lab host had no IPv6), so tests use
  `.clear_ip_transports().bind_addr((LOCALHOST, 0))?` for exactly one socket on 127.0.0.1. `bind()`
  takes 3–5 ms, with or without a reachable tower.
  `EndpointConfig` defaults to WebPKI roots and system DNS with public fallback disabled. Until the
  TLS and DNS settings are wired, binding with custom roots or an explicit nameserver returns an error.
- **Relay map.** The bare-URL constructors assume QAD on UDP 7842. An endpoint with IP sockets
  plans QAD probes per IP family for which the host has a usable address (IPv4: non-loopback; IPv6:
  global or unique-local), and against a plain-HTTP tower each waits out a 3 s timeout: on the
  IPv4-only lab host `online()` took 3.0 s instead of 0.22 s. Relay-only endpoints and
  loopback-only namespaces paid nothing, so the cost depends on the host. Use
  `iroh::RelayConfig::new(url, None)` for `http://` towers (`iroh_relay::RelayQuicConfig` for a QAD
  port): it behaves the same everywhere. Switch off only the captive-portal check
  (`NetReportConfig::default()` with `captive_portal_check = false`); `NetReportConfig::minimal()`
  also disables the HTTPS probes, and then no home tower is ever chosen (not online after 30 s).
  Against a TLS tower, a configured QAD port that is closed costs the same 3.0 s at every first
  report. The map in `RelayMode::Custom(map)` is shared: `insert_relay` / `remove_relay` on the
  endpoint change it, so a lookup can read the same map.
- **Dial by id.** An `AddressLookup` returning every tower URL for any id makes
  `endpoint.connect(id, ALPN)` work with no other service. All listed relays are tried in parallel
  and the relay need not be in the dialer's own map — current behaviour, not a documented contract.
  Each extra tower costs one dead path per connection (budget 8). Measured: `connect()` 17–34 ms;
  with callee and dialer homed on different towers and a dead URL in the list, 43–51 ms.
- **Tower.** `iroh_relay::server::{Server, ServerConfig, RelayConfig}` are `#[non_exhaustive]`: start
  from defaults and assign fields. `tls = None` serves plain HTTP; `server.http_addr()` gives the
  bound address (`http_url()` is test-utils only). Defaults are `AllowAll` and no rate limit: set
  `relay.limits.client_rx` and, in M2, an `AccessControl` (its `on_connect` sees the authenticated
  endpoint id). `iroh::RelayConfig` (client) is a different type from
  `iroh_relay::server::RelayConfig`. In TLS mode the server also binds a plain-HTTP listener on
  `RelayConfig::http_bind_addr` that cannot be turned off and answers only `/generate_204`; ACME is
  tls-alpn-01 on the HTTPS port only and needs `cache_path` (source reading); QAD needs TLS;
  `Server::spawn` cannot add HTTP routes. The allowlist (`AccessControl`, `Access::Deny { reason }`)
  gates `/relay` only: a denied endpoint still used QAD and the `/ping` probe. Removing an id from
  the list does not disconnect it; `relay_service().clients().disconnect(id, None)` does (21–22 ms).
  The rate limit (`ClientRateLimit`, live through `set_client_rate_limit`) only slows the sender
  (64 KiB/s gave 34–35 KiB/s each way for an echo, no error, no status change) and is counted in
  `bytes_rx_ratelimited_total`; `Limits::accept_conn_limit` and `accept_conn_burst` are declared
  but not implemented. A relay and a separate iroh endpoint can run in one process; the relay does
  not advertise that endpoint's id. iroh-relay does not re-export `RelayUrl`; it comes from `iroh`.
  After `Server::shutdown()` right after a client came online,
  the client still reported itself connected in 8 of 180 runs (whether it could still relay was not
  tested); it was not observed in 180 runs each when shutting down at least 1 s after the last
  connect or stopping the tower's own runtime (observations, not guarantees).
- **Router and gate.** `Router::builder(ep).incoming_filter(..).accept(ALPN, proto).spawn()`;
  `spawn` overrides builder ALPNs; dropping the `Router` aborts it, so call `router.shutdown().await`.
  When `accept()` returns the connection closes, so loop on `accept_bi`; streams are lazy, so write
  right after `open_bi`. `EndpointHooks::after_handshake` runs for every ALPN in both directions (keep
  it ALPN-aware; it must not hold an `Endpoint`) and only after iroh has registered the connection, so
  only the pre-handshake filter limits address disclosure — and for relayed attempts the claimed id
  comes from the tower. On our own dials the hook runs as `Side::Client`, so a gate that ignores the
  ALPN refuses them (`ConnectingError::LocallyRejected`). An error returned from `accept` reaches
  the dialer as close code 0 with no reason; to send our code, call `conn.close(code, reason)`
  first.
- **Liveness.** An endpoint binds with an empty relay map and takes towers later through
  `insert_relay` / `remove_relay`; it came online about 220 ms after the insert. A first home tower
  comes only from a net report, which re-runs every 20–26 s or when the relay map changes. With its
  tower down at start, an endpoint came online 14–24 s after the tower did; meanwhile
  `home_relay_status()` was empty with no error, and `network_change()` did not help. Removing and
  re-inserting the tower brought it online in about 220 ms (observed, not documented); with a
  second tower down as well, 18–24 s waiting and 0.4–0.8 s with the remove and insert. Under heavy
  load an endpoint occasionally missed its first report and came online only after 21–24 s.
  `home_relay_status()` lists the home tower only. After the home tower stopped, an endpoint moved
  to the second tower after 41–49 s, because the report after a successful one plans no HTTPS probe
  (`ProbePlan::with_last_report`); `network_change()` did not help, QAD (TLS towers) made it
  22–28 s, and removing and re-inserting the dead tower made it 0.2–0.4 s (observed, not
  documented). Home moves back to a recovered tower only if it is clearly faster (hysteresis; 6 of
  25 runs within 50 s). A connection relayed through the stopped tower kept that path and ended at
  the idle timeout in 20 of 20 runs; a new dial worked once both ends had moved. `remove_relay` of
  the home tower moved the endpoint to another in 220–244 ms; `remove_relay` of the only tower
  took it out of the map but left the home connection and the status as they were. A tower that
  denies an endpoint can still become its home when another tower is
  in the map (17 of 30) and keeps it; `remove_relay` of the denying tower freed it in about 0.2 s.
  `Endpoint::online()` has no timeout: wrap it, and report `home_relay_status()`. When the endpoint
  noticed a stopped tower (see Tower), it reported not connected 0–51 ms after `shutdown()` was
  called; the reconnect backoff (10 ms–16 s) has jitter, and gaps between attempts reached about
  31 s. Keep-alive is 5 s and idle timeout 30 s; a dial to an
  offline peer, or one whose filter ignores us, fails only by that timeout (30.0 s), and a peer
  killed without closing is noticed after 30.0 s. Two processes on one node key: the relay keeps
  only the newest, and the older one notices nothing (measured with two endpoints in one process).
  Sockets close only when every `Endpoint` clone is dropped; a rebind of the same port right after
  the last drop failed in about 6 of 45 runs and succeeded on a retry within 11 ms. With healthy
  peers, `router.shutdown()` including the close took 2–11 ms. iOS kills an app that holds a file
  lock while suspended.
- **Paths.** `conn.paths()`, `conn.paths_stream().wait_any(..)`, `conn.path_events()`; a path reports
  `is_relay()`, `is_ip()`, `is_selected()`. On one host a connection starts on the relay and selects
  the direct path 33–47 ms after `connect()` starts.
- **Local socket and tooling.** `tokio::net::UnixStream::peer_cred()` gives the peer uid (the std
  equivalent is unstable); `rustix` supplies `geteuid` without `unsafe`. A socket address holds
  108 bytes on Linux and 104 on Apple, including the terminating NUL, so a socket path can have 107
  or 103 bytes. `std::fs::File::try_lock` is stable since 1.89. UniFFI has no
  stream type and does not propagate cancellation, hence the pull-style `Subscription::next()`.
- **Bootstrap (M2).** The record is SRV (decision 13, settled in #38); it was measured against a
  DNS server on 127.0.0.1. `n0_dns_resolver::DnsResolver::lookup_srv(name)` returns
  `SrvRecordData {priority, weight, port, target}`; targets come back without a trailing dot;
  NXDOMAIN is `Err(Error::NxDomain {..})`, an empty answer is `Ok(vec![])`, and a silent server
  `Err(Error::Timeout {..})` after 6.0 s. That is the resolver's budget for one UDP nameserver
  query, a constant; a lookup over several nameservers can take longer, so the caller sets its own
  deadline. Positive answers are cached for their TTL (source; measured: a second lookup sent no
  query, and after `clear_cache()` the next one did), negative ones are not. The resolver logs each
  name and its records at debug level. A fresh `n0_dns_resolver` builder has no public
  fallback; `iroh::dns::DnsResolver::with_nameserver` keeps one, so iroh's resolver for the tower
  host names is built with `.disable_fallback()`. `https://host.:port/` works but is a different
  `RelayUrl` from `https://host:port/`; the canonical form has no trailing dot. An endpoint
  bound with the URLs from the SRV answer came online through a TLS tower named by the record in
  227–284 ms (bind to online; the lookup was done before the timed part).
  Use `canonical_tower_url` for explicit, SRV, remembered and peer-supplied tower URLs. It lowercases
  DNS names, removes the trailing dot and default port, and keeps the root slash. Plain HTTP
  requires a literal loopback address.
- **TLS and QAD (M2).** A tower reads its certificate and key from PEM files itself (regular files
  only; a directory, device or FIFO could block) and checks the pair before it serves it: the
  chain holds a certificate, every one is an X.509 version 3 certificate that webpki parses (no
  version 1 root), the key file holds exactly one key that ring can use, and the key belongs to
  the certificate. It hands the pair to
  rustls through its own certificate resolver, in
  `rustls::ServerConfig::builder_with_provider(..)` and `CertConfig::Manual`; the provider is passed
  explicitly and no process-wide default was installed or needed. It reads both files again once a
  day and replaces the pair only when the new one passes the same checks. iroh-relay's own
  `reloading_resolver(&provider, cert, key, DEFAULT_CERT_RELOAD_INTERVAL)` is not used: it fails at
  once on a missing, unreadable or non-PEM key file, but accepts a certificate file that holds no
  PEM certificate (an empty chain; `Server::spawn` succeeds), never checks that the key belongs to
  the certificate (source reading), and its errors can quote the file (found while building the
  tower, #70). A test endpoint trusts the
  certificate with `iroh::tls::CaTlsConfig::custom_roots` (an empty list fails `bind()`) and came
  online in 223–325 ms. An endpoint that does not trust it never chooses a home tower: empty
  status, no error, `online()` pending. QAD ran when the relay map carried the tower's QAD port
  (the tower's `qad_connections` counter shows it); relay-only endpoints and hosts without a
  routed interface run none.
- **Claude Code and API keys.** There is no login command that reads an API key. The CLI takes one
  from `ANTHROPIC_API_KEY` (an interactive session asks once whether to use it and remembers the
  answer; `-p` always uses it) or from the output of the shell command the `apiKeyHelper` setting
  names, cached for five minutes by default. Other credentials can take precedence over both, among
  them cloud-provider settings, `ANTHROPIC_AUTH_TOKEN` and a signed-in apps gateway; a stored
  subscription sign-in does not. The full order is on the vendor's authentication page.
  `forceLoginMethod` cannot restrict the CLI to API keys: it takes `claudeai`, `console` or
  `gateway`, and where managed settings set it, an environment key or a helper is refused at
  startup. Bare mode (`--bare` or `CLAUDE_CODE_SIMPLE=1`) reads no OAuth or keychain credentials
  and, for the Anthropic API, uses `ANTHROPIC_API_KEY` or an `apiKeyHelper` given with `--settings`;
  cloud providers still work, and it loads fewer features. `claude auth status` reports `authMethod`
  as `api_key` or `api_key_helper`. (Read in the published Claude Code documentation on 2026-11-20,
  not checked against a CLI version.)
- **Codex and API keys.** `codex login --with-api-key` reads the key from stdin, refuses a terminal
  and does not check it ("Authentication is checked when you send a request"). It stores the key
  through `cli_auth_credentials_store`: `file` (`auth.json` under `CODEX_HOME`, default
  `~/.codex`), `keyring` or `auto`; `ephemeral` keeps it in that process's memory only, so it cannot
  give a key to a later `codex` process. `codex login status` exits 0 or 1 and, for a key, prints
  its first 8 and last 5 characters to stderr. `codex logout` clears the stored credentials.
  `CODEX_API_KEY` overrides the stored credentials in `codex exec`, in `codex review` (which runs
  through exec) and in some other commands; an interactive session does not authenticate with it,
  though its startup login-method check reads it. `forced_login_method = "api"` allows the API
  credential family, Amazon Bedrock API keys and AWS access keys included, not OpenAI API keys
  alone; if the active credentials do not match, Codex signs the user out and exits. (Read in the
  Codex source on 2026-11-20: release 0.171.0, commit ccf9be8, and `main` after it.)
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
