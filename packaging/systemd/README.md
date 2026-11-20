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
cache directory created by hand as root inside the state directory is refused; remove it, and
the tower creates its own. A `cache_dir` outside the state directory does not work
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
