# M2 public tower demo

Use a Linux tower host with systemd, a public DNS zone you control, and two workstations on
different networks. Use `example.com` and `tower1.example.com` as placeholders for your bootstrap
domain and tower hostname. Run the workstation commands in Bash, from the repository checkout.

## 1. Prepare the host and DNS

Allow inbound TCP 80, TCP 443, and UDP 7842 through the host firewall and any provider firewall.
Free all three ports before starting the service. In particular the tower binds port 80 even
with a certificate supplied from files. It uses 80 for captive-portal checks, 443 for the HTTPS
relay and Let's Encrypt TLS-ALPN validation, and UDP 7842 for QUIC address discovery (QAD).

Publish an A record for the tower and an SRV record in the bootstrap zone. Replace the example
address with the host's public address. Add an AAAA record only when the tower is reachable on
its advertised IPv6 address on all three ports.

```dns
tower1.example.com.       IN A   192.0.2.10
_numbers._tcp.example.com. IN SRV 0 0 443 tower1.example.com.
```

From each workstation, confirm that its DNS resolver returns the SRV target and that target's
public address. The station uses `https://tower1.example.com/` from this record and assumes QAD
on UDP 7842. Each tower has its own SRV target. The station ignores SRV priority and weight.

```bash
dig +short SRV _numbers._tcp.example.com.
dig +short A tower1.example.com.
```

## 2. Start the two stations and collect their IDs

Run this on Alice's and Bob's workstations. Keep each shell open for the remaining steps. The
fresh private directory gives each station a stable identity for this demo and its own local
socket. The daemon makes IPC available while tower discovery and connection continue.

```bash
cargo build --release --locked -p numbers-station-daemon -p chief-cli
umask 077
demo=$(mktemp -d /tmp/thecompany-m2-demo.XXXXXXXX)
socket="$demo/station.sock"
target/release/station run --data-dir "$demo" --domain example.com >"$demo/station.log" 2>&1 &
station_pid=$!
```

Once the socket appears, capture the ID in a private file. Share it with the tower operator and
the other chief over a private channel. Save the other chief's ID as one line in `$demo/peer-id`
on each workstation. Keep IDs, raw status output, and keys out of demo notes and shared logs.

```bash
target/release/chief --station "$socket" id >"$demo/station-id" 2>"$demo/chief.err"
```

For a tower certificate issued by a private CA, distribute that CA's PEM certificate to both
workstations and add `--tower-ca /path/to/ca.pem` to `station run`. The server key stays on the
tower host. Use `--domain` for discovery in either case. A `--tower` pin conflicts with `--domain`
and bypasses SRV in a pinned run.

The station persists the bootstrap domain and tower set in its data directory. With a nonempty
remembered set, a restart tries that set before returning to SRV if the known towers fail. A
restart may still resolve the towers' A or AAAA records. A spelling change that only affects
case or the final dot retains the set. A different domain replaces the entire remembered set,
including manually adopted towers, and durably writes an empty set before discovery. A reload
with an empty set also syncs that state before the lookup. Keep the same data directory when
restarting the demo station to reuse its key, and use a fresh directory to demonstrate first
discovery again.

## 3. Install and start the tower

On the tower host, build the binary and install the system unit from the checkout.

```bash
cargo build --release --locked -p numbers-tower
sudo install -m 0755 target/release/tower /usr/local/bin/tower
sudo install -d -m 0755 /etc/numbers-tower
sudo install -m 0644 packaging/systemd/numbers-tower.service /etc/systemd/system/numbers-tower.service
sudoedit /etc/numbers-tower/tower.toml /etc/numbers-tower/allow.txt
```

Put only Alice's and Bob's IDs in `allow.txt`, one per line. The tower resolves `allow_file`
relative to `tower.toml` and reads it at startup. Keep it root-owned and readable by the service.
Restart the tower after a change to the list. Use the following common header in `tower.toml` and
choose one certificate section below. The listeners use the default ports from step 1.

```toml
hostname = "tower1.example.com"
allow_file = "allow.txt"
```

### Let's Encrypt

Add this section, using the real tower hostname in the common header and a contact address
you control. Start with `production = false` if you want to verify issuance in staging first.
Staging certificates are untrusted by the stations. After a staging certificate is in use, set
`production = true` and restart before testing the stations.

```toml
[acme]
contact = "mailto:ops@example.com"
cache_dir = "/var/lib/numbers-tower/acme"
production = true
```

The system unit runs `tower serve --config /etc/numbers-tower/tower.toml` with `DynamicUser=yes`.
The service sees `StateDirectory=numbers-tower` as writable and owned by its allocated user with
mode 0700. On the host, the directory can appear under a mapped owner, depending on the systemd
version. Let the service create its ACME cache under the state directory. If it refuses an
existing cache, check access and ownership as seen by the service. An empty directory created
by hand can be removed so the service recreates it. Preserve populated caches and their keys
while correcting access.

### Certificate files

Use a PEM certificate chain for the SRV target hostname and its matching PEM key. Install the
credential drop-in and edit its two `LoadCredential=` source paths to point to your files. Keep
the source private key readable only by root. systemd reads the files when the service starts
and supplies private read-only copies to the service.

```bash
sudo install -d -m 0755 /etc/systemd/system/numbers-tower.service.d
sudo install -m 0644 packaging/systemd/numbers-tower.service.d/tls-files.conf.example \
    /etc/systemd/system/numbers-tower.service.d/tls-files.conf
sudoedit /etc/systemd/system/numbers-tower.service.d/tls-files.conf
```

Add this section, using it as the only certificate section in `tower.toml`.

```toml
[tls]
cert = "/run/credentials/numbers-tower.service/tls-cert.pem"
key = "/run/credentials/numbers-tower.service/tls-key.pem"
```

After renewal, run `sudo systemctl restart numbers-tower` to refresh the credential copies.
The tower's daily reload reads the service's copies, which stay fixed for the life of the service.

### Start the service

```bash
sudo chmod 0644 /etc/numbers-tower/tower.toml /etc/numbers-tower/allow.txt
sudo systemctl daemon-reload
sudo systemctl enable --now numbers-tower
sudo journalctl -u numbers-tower -o cat -f
```

Check the tower's startup line for `https=443 http=80 qad=7842`, two listed IDs, and either
`certificate="files"` or `certificate="acme production"`. For ACME, wait for `ACME: a certificate
is in use`. The [service guide](../../packaging/systemd/README.md) explains the journal messages
and certificate drop-in.

## 4. Chat and inspect the path

On each workstation, rerun this status check until the tower is connected. `chief status`
includes the bootstrap domain, the tower state and source, and a path for each peer station.
The filter shows the state, source, and path while leaving IDs and URLs in the private file.
On first discovery the tower source is `srv`. After restarting with a saved set it is `remembered`.

```bash
target/release/chief --station "$socket" status >"$demo/status" 2>"$demo/chief.err" &&
    awk '$1 == "tower" {print $1, $3, $4} $1 == "peer" {print $1, $2, $4}' "$demo/status"
```

On Alice's workstation add Bob, then on Bob's workstation do the same with `alice` as the peer
name. Each `peer-id` file contains the other chief's ID.

```bash
IFS= read -r peer_id <"$demo/peer-id"
target/release/chief --station "$socket" peer add bob "$peer_id" >/dev/null 2>"$demo/chief.err"
unset peer_id
```

Send from Alice and look for the incoming line in Bob's log.

```bash
# Alice
target/release/chief --station "$socket" send bob 'hello from Alice' 2>"$demo/chief.err"

# Bob
target/release/chief --station "$socket" log alice >"$demo/messages" 2>"$demo/chief.err" &&
    grep -Eq '^[0-9]+ [^ ]+ <- alice hello from Alice$' "$demo/messages"
```

Rerun the filtered status command while the stations are active. `relayed` means the peer link
uses the tower. `direct` means the stations found a direct path. QAD and working UDP help a link
switch to `direct` when the networks' NATs and firewalls permit it. A successful chat can stay
`relayed`. Record the path actually observed in each test.

To check denial, start a third station from a fresh private data directory on either workstation,
using the same `--domain` and CA setting. Keep its ID out of `allow.txt`. Its private `chief status`
output should show `denied (srv): not on this tower's list` for the relay. The ACL applies to
relaying. Address discovery can still answer the unlisted station.

When done, stop each station with SIGTERM and check that its socket has disappeared.
Run this in each shell that started a station. If the tower was provisioned only for this demo,
use `sudo systemctl disable --now numbers-tower` on the host and keep the certificate and account
state.

```bash
kill -TERM "$station_pid"
wait "$station_pid" && test ! -e "$socket"
```

The [TLS loopback walkthrough](../../scripts/walkthrough-tls.sh) exercises private CA trust,
allowlisted chat, denial, and socket cleanup. The public host, public DNS, and two-network path
checks are tracked under [the real-network demo](https://github.com/tiltbob/thecompany/issues/46).
