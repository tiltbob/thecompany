# theCompany

A p2p network, called `numbers`, for a small self-hosted team working with coding agents: chat,
sockets, files, locks and a persistence layer.

| Name | What it is |
|---|---|
| chief | a person on the team: an identity, not a machine |
| station | a chief's node: a daemon on the chief's workstation; the `chief` CLI, GUI and phone app are frontends to it |
| safehouse | a node that runs agents in harnesses (Claude Code and Codex first) |
| handler | later: the node that sets safehouses up in isolated environments |
| tower | an untrusted node: relay, rendezvous, and the final store for encrypted blocks |
| airspace | the persistence layer: text objects, git repos, occasional large media |

## Walkthrough

Build the dev tower, station daemon and chief CLI from the repository root.

```bash
cargo build --locked -p numbers-tower -p numbers-station-daemon -p chief-cli
```

Start the dev tower in one terminal and leave it running. Wait for `dev tower listening`.

```bash
target/debug/tower --bind 127.0.0.1:3340
```

In a second Bash terminal at the repository root, start two stations with separate private data
directories and local sockets.

```bash
demo=$(mktemp -d /tmp/thecompany-demo.XXXXXXXX)
tower_url=http://127.0.0.1:3340
alice_socket="$demo/alice/station.sock"
bob_socket="$demo/bob/station.sock"

target/debug/station run --data-dir "$demo/alice" --tower "$tower_url" &
alice_pid=$!
target/debug/station run --data-dir "$demo/bob" --tower "$tower_url" &
bob_pid=$!
```

Run `status` again if needed until both stations report the tower as `connected`.

```bash
target/debug/chief --station "$alice_socket" status
target/debug/chief --station "$bob_socket" status
```

Capture each station ID in a shell variable and add each chief to the other's peer book.

```bash
alice_id=$(target/debug/chief --station "$alice_socket" id)
bob_id=$(target/debug/chief --station "$bob_socket" id)
target/debug/chief --station "$alice_socket" peer add bob "$bob_id" >/dev/null
target/debug/chief --station "$bob_socket" peer add alice "$alice_id" >/dev/null
```

Send a message as Alice, then look for `<- alice hello from Alice` in Bob's log.

```bash
target/debug/chief --station "$alice_socket" send bob 'hello from Alice'
target/debug/chief --station "$bob_socket" log alice
```

Stop both stations, confirm their sockets are gone, and remove the demo data. Press Ctrl+C in the
tower terminal to stop it.

```bash
kill -TERM "$alice_pid" "$bob_pid"
wait "$alice_pid"
wait "$bob_pid"
test ! -e "$alice_socket" && test ! -e "$bob_socket" && rm -rf -- "$demo"
unset alice_id bob_id
```

CI runs [the same walkthrough](scripts/walkthrough.sh) with a tower port chosen by the OS and an
isolated loopback interface. On Linux with `unshare`, `ip`, GNU `timeout` and unprivileged user
namespaces enabled, run it after building the binaries.

```bash
unshare --user --map-root-user --net -- bash -ec '
  ip link set lo up
  exec bash scripts/walkthrough.sh
'
```

Set `WALKTHROUGH_BIN_DIR` to the directory containing `tower`, `station` and `chief` if you built
them outside `target/debug`.

The [TLS walkthrough](scripts/walkthrough-tls.sh) uses OpenSSL to generate a temporary CA and
loopback server certificate. It verifies that two admitted stations can chat and that the tower
denies a third station. Run it with the same namespace command, using `scripts/walkthrough-tls.sh`
in place of `scripts/walkthrough.sh`.

## Where things are

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — the design
- [docs/ROADMAP.md](docs/ROADMAP.md) — milestones, what is being built first, open decisions
- [AGENTS.md](AGENTS.md) — build commands, hard rules, and how the coding agents work here
- [packaging/systemd/README.md](packaging/systemd/README.md) — running the tower as a systemd service
- [M2 public tower demo](docs/notes/m2-demo.md) — configuring a public tower and connecting two networks
- [Roadmap issue](https://github.com/tiltbob/thecompany/issues/1) — status of every milestone
