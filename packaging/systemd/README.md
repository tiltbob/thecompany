# Running the tower as a service

`numbers-tower.service` runs the production tower (`tower serve`) as a systemd system service.
The commands below run on the tower host, from a checkout of this repository. `example.com`
names stand for yours.

## What goes where

| What | Where |
|---|---|
| the binary | `/usr/local/bin/tower` |
| the config | `/etc/numbers-tower/tower.toml` |
| the allow file | `/etc/numbers-tower/allow.txt` |
| the unit | `/etc/systemd/system/numbers-tower.service` |
| the ACME cache | `/var/lib/numbers-tower/acme`, created by the tower |

The config and the allow file hold nothing secret (endpoint ids are public keys). They are
owned by root and readable by everyone, so the service's user can read them although it owns
nothing in `/etc`. The unit does not use `ConfigurationDirectory=`: the files must be in place
before the first start anyway, and the tower takes the config path from `ExecStart`.

The service runs as a user that systemd allocates when it starts (`DynamicUser=yes`). It sees
the file system read-only, except its state directory and a private `/tmp`, and no home
directories. `StateDirectory=numbers-tower` makes `/var/lib/numbers-tower` a symbolic link to
`/var/lib/private/numbers-tower`, which only root can reach from outside the service. Inside
the service that directory belongs to the service's user and has mode 0700
(`StateDirectoryMode=0700`). Seen from outside, its owner depends on the systemd version: the
dynamic user's number, or `nobody` where systemd maps the directory into the service. systemd
changes the owner of what is inside only when the directory itself had another owner (measured
under systemd 255), and it never changes the modes inside.

The ACME cache holds the tower's TLS key and its ACME account key. The tower creates it with
mode 0700, and refuses to start with a cache that the group or others can access, that another
user owns, or that it cannot write. Under this unit, with the config below, the tower creates
the cache itself, so those checks pass. They matter when someone else made the directory: a
cache directory created by hand as root inside the state directory is refused. If it is empty,
remove it, and the tower creates its own. If it holds a cache, keep it: it contains the tower's
TLS key and its ACME account key. Repair its owner and its mode instead; the tower's error line
says which of the two it refused. A `cache_dir` outside the state directory does not work
under this unit: the service sees the rest of the file system read-only, and its private `/tmp`
is emptied when it stops.

## Install

Build the binary:

```bash
cargo build --release -p numbers-tower --locked
```

Install it:

```bash
sudo install -m 0755 target/release/tower /usr/local/bin/tower
```

Create the config directory:

```bash
sudo install -d -m 0755 /etc/numbers-tower
```

Write `/etc/numbers-tower/tower.toml`. A complete config for a Let's Encrypt certificate:

```toml
hostname = "tower1.example.com"
allow_file = "allow.txt"

[acme]
contact = "mailto:ops@example.com"
cache_dir = "/var/lib/numbers-tower/acme"
production = false
```

`allow_file` is relative, so it is read from `/etc/numbers-tower/`. The listeners take their
defaults: HTTPS on `0.0.0.0:443`, HTTP on `0.0.0.0:80`, address discovery on UDP
`0.0.0.0:7842`. `production = false` (also the default) takes the certificate from Let's
Encrypt's staging directory, which stations do not trust. Start with staging; once the tower
gets a certificate, set `production = true` and restart it. Staging has much higher rate
limits, so mistakes there do not use up production's.

Write `/etc/numbers-tower/allow.txt`: one endpoint id per line, `#` starts a comment. Then
make both files readable:

```bash
sudo chmod 0644 /etc/numbers-tower/tower.toml /etc/numbers-tower/allow.txt
```

Install the unit:

```bash
sudo install -m 0644 packaging/systemd/numbers-tower.service /etc/systemd/system/numbers-tower.service
```

```bash
sudo systemctl daemon-reload
```

## Start

```bash
sudo systemctl enable --now numbers-tower
```

```bash
journalctl -u numbers-tower -o cat -f
```

`-o cat` shows the tower's own lines only: journald's usual prefix holds this machine's host
name. The tower's log lines start with its own timestamp (UTC), left out below; its `tower: …`
error lines have none. The start line gives the ports of the listeners (`off` for one that is
off), the number of listed ids (or `open`) and where the certificate comes from:

```text
INFO numbers_tower::cli: tower listening https=443 http=80 qad=7842 access=3 ids listed certificate="acme staging"
```

With staging it is followed by:

```text
WARN numbers_tower::cli: the certificate comes from Let's Encrypt's staging directory, which stations do not trust; set `production = true` in `[acme]` for a real one
```

The ACME client's progress comes as four fixed lines, without detail:

```text
INFO numbers_tower::cli: ACME: a certificate is in use
```

The tower serves a certificate; nothing to do.

```text
WARN numbers_tower::cli: ACME: a certificate order failed; the tower retries with a growing delay
```

Let's Encrypt did not issue the certificate. Check that the host name resolves to this host
and that port 443 is reachable from the internet; the tower keeps trying.

```text
WARN numbers_tower::cli: ACME: the cache directory could not be read or written; certificates are not kept across restarts
```

The state directory is not usable. With this unit and the config above that should not
happen: check whether the unit or `cache_dir` was changed. Until it is fixed, every restart
orders a new certificate, and Let's Encrypt's rate limits soon stop that.

```text
WARN numbers_tower::cli: ACME: a certificate step failed
```

Another step failed. If it repeats and no certificate comes into use, check the host name, the
contact address and port 443.

## When the tower stops

`systemctl stop` ends the tower with success, and it stays stopped. An error ends it with a line
that starts with `tower: `, and with an exit status that decides what systemd does next.

Errors that can pass exit with status 1, and `Restart=on-failure` starts the tower again after 5
seconds: a port that is taken or not allowed (`tower: cannot bind the HTTPS listener (…)`), the
relay stopping by itself (`tower: the tower's relay stopped by itself`), and certificate files
that cannot be read, which can be a renewal in progress.

A configuration error exits with status 78: the config file or the allow file, a value in them
that the tower refuses, or an ACME cache directory that it refuses. A wrong command line exits
with status 2.
`RestartPreventExitStatus=2 78` keeps systemd from restarting it: the service ends as failed.
`systemctl enable --now` has already returned by then, so check:

```bash
systemctl status numbers-tower
```

It shows `Active: failed (Result: exit-code)` and `(code=exited, status=78)`; the journal has
the `tower: …` line. A config error quotes nothing from the files: a syntax error, an unknown
key or a value of the wrong form gives the line number in the config file, a missing key
names the key, and a file that cannot be read says which file and why. A command-line error is
one fixed line and a pointer to `tower --help`, and never shows what was typed. Fix the config
or the unit, then:

```bash
sudo systemctl restart numbers-tower
```

The tower logs no host name, address, endpoint id or path, on purpose: logs get copied into
issues and chats. To see which host or file a problem is about, look at the config.

## Ports

Open these to the internet:

| Port | What |
|---|---|
| 443 TCP | the relay over HTTPS; Let's Encrypt validates the host name here (tls-alpn-01) |
| 80 TCP | plain HTTP, for captive-portal checks |
| 7842 UDP | QUIC address discovery |

The service binds 80 and 443 with `CAP_NET_BIND_SERVICE`, its only capability.

## Certificate files instead of ACME

The service's user cannot read a key that only root can read. systemd can: with
`LoadCredential=` it reads the files when the service starts and gives the service read-only
copies in `/run/credentials/numbers-tower.service/`, readable by the service's user only.
[`numbers-tower.service.d/tls-files.conf.example`](numbers-tower.service.d/tls-files.conf.example)
does this for certbot's paths; change the paths in it to yours.

Install the drop-in:

```bash
sudo install -d -m 0755 /etc/systemd/system/numbers-tower.service.d
```

```bash
sudo install -m 0644 packaging/systemd/numbers-tower.service.d/tls-files.conf.example /etc/systemd/system/numbers-tower.service.d/tls-files.conf
```

```bash
sudo systemctl daemon-reload
```

In `tower.toml`, replace the `[acme]` table with:

```toml
[tls]
cert = "/run/credentials/numbers-tower.service/tls-cert.pem"
key = "/run/credentials/numbers-tower.service/tls-key.pem"
```

Then restart the tower:

```bash
sudo systemctl restart numbers-tower
```

The start line then says `certificate="files"`.

**Renewal needs a restart.** The copies are made when the service starts and do not change
while it runs. The tower reloads its certificate files once a day, but under this drop-in it
reads the same copies again, so a renewed certificate is used only after
`sudo systemctl restart numbers-tower`. With certbot, a deploy hook can run that command.

## Running the station as a service

`station.service` is a systemd user unit. Run these commands on the workstation as the chief who
owns the station. Build and install the binaries and unit from the repository checkout.

```bash
cargo build --release --locked -p numbers-station-daemon -p chief-cli
install -Dm 0755 target/release/station "$HOME/.local/bin/station"
install -Dm 0755 target/release/chief "$HOME/.local/bin/chief"
install -Dm 0644 packaging/systemd/station.service \
    "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/station.service"
systemctl --user daemon-reload
```

The shipped unit runs `station run` and uses the station's saved tower configuration. For a new
station, start it once with your bootstrap domain and then press Ctrl+C after it has discovered
and connected to a tower. Inspect `chief status` privately from a second terminal. Replace
`example.com` with the team's bootstrap domain.

```bash
"$HOME/.local/bin/station" run --domain example.com
```

For explicit tower pins, a private CA, or other startup flags, use
`systemctl --user edit station.service`. Clear `ExecStart` before replacing it. For example,
this override uses a private CA with domain discovery. Adjust the domain and CA path.

```ini
[Service]
ExecStart=
ExecStart=%h/.local/bin/station run --domain example.com --tower-ca %h/.config/thecompany/ca.pem
```

For pins, use `--tower URL` in the replacement command and omit `--domain`. Pins apply to the
current run, so a one-off foreground run with pins leaves the saved configuration as it was.
`--tower-ca` also applies to the current run. After editing the startup command, restart a running
service with `systemctl --user restart station.service`.

Both launches use `$XDG_DATA_HOME/thecompany/station`, or `$HOME/.local/share/thecompany/station`
when `XDG_DATA_HOME` is absent, empty, or relative. With the default data directory and an
absolute `XDG_RUNTIME_DIR`, the socket is `$XDG_RUNTIME_DIR/thecompany/station.sock`. Otherwise it
is `station.sock` inside the data directory. Keep the same XDG settings in the user manager and
your terminal if you customize them. If you set `--data-dir` to a nondefault directory, pass
`--station PATH` to `chief` to select its socket.

The daemon locks the data directory before opening the node key. A foreground invocation using
the service's data directory therefore fails while the service owns the lock, and the service
cannot take the same data directory while a foreground instance is running. Stop the foreground
instance before starting the unit. Keep the data directory on a local filesystem so the daemon
can rely on its file lock. Use the same data directory to preserve the station identity.

```bash
systemctl --user enable --now station.service
systemctl --user status station.service
journalctl --user -u station.service -o cat
```

`enable` sets the station to start with the user manager. To start it at boot and keep it running
after logout, set `loginctl enable-linger "$USER"`. Whether you can enable lingering for your
account depends on the workstation's login policy.

The service restarts after a failure, with a five-second delay. Command-line errors exit with
status 2 and prevent automatic restart. Other startup failures, including a missing tower
configuration or a data directory lock held by another instance, exit with status 1. Check the
journal and correct the cause before starting the service again if it reaches systemd's start
limit. `systemctl --user stop station.service` sends SIGTERM. The station removes the socket
before shutting down the core and releasing its data-directory lock.
