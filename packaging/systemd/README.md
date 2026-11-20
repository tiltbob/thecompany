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

The service runs as a user that systemd allocates when it starts (`DynamicUser=yes`), with
the whole file system read-only except its state directory. `StateDirectory=numbers-tower`
makes `/var/lib/numbers-tower` a symbolic link to `/var/lib/private/numbers-tower`; systemd
gives that directory to the service's user and sets its mode to 0700 at every start
(`StateDirectoryMode=0700`). `/var/lib/private` is accessible to root only. The ACME cache
holds the tower's TLS key and its ACME account key: the tower creates it with mode 0700 and
refuses to start with a cache that the group or others can access.

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
limits, so mistakes there cost nothing.

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
journalctl -u numbers-tower -f
```

Each line starts with a timestamp, left out here. The start line gives the bound addresses,
the number of listed ids and where the certificate comes from:

```text
INFO numbers_tower::cli: tower listening https=0.0.0.0:443 http=0.0.0.0:80 qad=0.0.0.0:7842 access=3 ids listed certificate="acme staging"
```

With staging it is followed by:

```text
WARN numbers_tower::cli: the certificate comes from Let's Encrypt's staging directory, which stations do not trust; set `production = true` in `[acme]` for a real one
```

The ACME client reports two things, without detail:

```text
INFO numbers_tower::cli: ACME: a certificate is in use
WARN numbers_tower::cli: ACME: a certificate step failed; the tower retries with a growing delay
```

If the relay stops by itself, the tower logs `tower: the tower's relay stopped by itself` and
exits with a failure; `Restart=on-failure` starts it again after 5 seconds. `systemctl stop`
ends it with success, and it stays stopped. A config error also ends the tower with one
`tower: …` line, which names the key and the file but no value from them; systemd then tries
again every 5 seconds until the config is fixed.

The tower logs no host name, endpoint id or path, on purpose: logs get copied into issues and
chats. To see which host or file a problem is about, look at the config.

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
