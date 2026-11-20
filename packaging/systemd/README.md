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
the file system read-only, except its state directory, a private `/tmp` and the host's
`/dev/shm`, and no home directories; the rest of its sandbox is under
[The sandbox](#the-sandbox).
`StateDirectory=numbers-tower` makes `/var/lib/numbers-tower` a symbolic link to
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
and that port 443 is reachable from the internet; the tower keeps trying. If both are fine, see
[When the sandbox refuses something](#when-the-sandbox-refuses-something).

```text
WARN numbers_tower::cli: ACME: the cache directory could not be read or written; certificates are not kept across restarts
```

The state directory is not usable. With this unit and the config above that should not
happen: check whether the unit or `cache_dir` was changed. If neither was, see
[When the sandbox refuses something](#when-the-sandbox-refuses-something). Until it is fixed,
every restart orders a new certificate, and Let's Encrypt's rate limits soon stop that.

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

## The sandbox

Besides the dynamic user, the read-only file system and its one capability, the unit takes away
from the service what a relay does not need. In practice the tower:

- cannot change kernel settings under `/proc/sys` and `/sys`, load kernel modules, read the
  kernel log, change control groups, set the clock or change the host name;
- cannot create or enter namespaces through `unshare(2)`, `clone(2)` or `setns(2)`, use realtime
  scheduling or change its execution domain (`personality(2)`); from systemd 251 on,
  `clone3(2)` is refused with `ENOSYS` and libc falls back to `clone(2)`, while systemd 250
  does not cover `clone3(2)`. systemd.exec(5) says the namespace rule is enforced on x86,
  x86-64, mips, ppc64 and s390 only, so not on arm64;
- sees a private `/dev` with only `null`, `zero`, `full`, `random`, `urandom` and the TTY
  devices, read-only except `/dev/pts` and the host's `/dev/shm` (and `/dev/mqueue` and
  `/dev/hugepages` where the host has them), which stay writable;
- cannot make memory writable and executable at once;
- can make only the system calls of systemd's `@system-service` set, and only through the
  architecture systemd was built for: a call outside the set fails with "permission denied"
  (`EPERM`), except that systemd 255 answers a call newer than itself with `ENOSYS`, and a call
  through another architecture's entry point (i386 on x86-64) ends the thread that made it
  (`SIGSYS`);
- sees no process of another user in `/proc`, and none of its files about the kernel
  (`/proc/sys`, `/proc/net`, `/proc/meminfo` and the like);
- creates files that only its own user can read (`UMask=0077`); a file that exists already
  keeps its mode, so an ACME cache written under an earlier unit keeps its 0644 files, also
  after a renewal, which rewrites the certificate file in place.

Each setting in the unit has a comment that says what it does. The unit needs systemd 250 or
later: `ProcSubset=pid` together with `ProtectKernelTunables=yes` or `ProtectKernelLogs=yes`
works from 250 on (systemd's NEWS), and `scripts/check-units.sh` needs 250 too.

To see how much the sandbox leaves open:

```bash
systemd-analyze security numbers-tower
```

It prints one line per setting and ends with the overall level. Under systemd 255 this unit
gives `Overall exposure level for numbers-tower.service: 1.6 OK`, with and without the
certificate drop-in; the unit before the sandbox gave `5.4 MEDIUM`. Most lines still marked
`✗` are about the network and `CAP_NET_BIND_SERVICE`, which a relay needs. `DeviceAllow=` is
marked for the read access to real-time clocks that `ProtectClock=yes` adds. The others are
settings the unit does not take, such as `PrivateUsers=` and leaving `@privileged` and
`@resources` out of the filter.

### When the sandbox refuses something

In the tests (systemd 255) systemd logged nothing when the sandbox refused a call or a path:
only the tower's own error line showed it, and that line looks like any other permission
problem. Whether the kernel's audit log records a refused call was not seen there. With a test
drop-in that took `bind` out of the filter (`SystemCallFilter=~bind`), the journal had:

```text
tower: cannot bind the plain-HTTP listener (permission denied)
numbers-tower.service: Main process exited, code=exited, status=1/FAILURE
numbers-tower.service: Failed with result 'exit-code'.
numbers-tower.service: Scheduled restart job, restart counter is at 1.
```

`systemctl status numbers-tower` showed `Active: activating (auto-restart) (Result: exit-code)`
and `(code=exited, status=1/FAILURE)`, and the tower was started again every 5 seconds. A
write to a read-only path is refused the same way: with `cache_dir` outside the state directory
the tower logged `tower: cannot use the ACME cache directory (read-only filesystem or storage
medium)` and stopped with status 78. A path that the sandbox hides (in `/proc` or `/dev`) is
"not found" for the service. A socket of another address family than IPv4, IPv6 and local
(`RestrictAddressFamilies=`) fails with "Address family not supported by protocol"
(`EAFNOSUPPORT`). glibc's name lookup tries a netlink socket for a name with several addresses,
and for every name when the caller asks only for the address families the host has (as `getent`
does), and carries on without it, so seeing this in a trace is expected.

A filter that kills instead of answering `EPERM` (the default when `SystemCallErrorNumber=` is
not set) shows differently: there is no line from the tower, systemd logs
`Main process exited, code=killed, status=31/SYS`, and `systemctl status` shows
`(code=killed, signal=SYS)`. This unit answers `EPERM`, so expect the first form. A call through
another architecture's entry point is the exception: `SystemCallArchitectures=native` ends the
thread that made it, whatever `SystemCallErrorNumber=` says. In a test with two threads, a call
from the second thread ended only that thread; the process went on and systemd logged nothing.

On the ACME client's path the cause is not shown. A refusal while the client resolves the Let's
Encrypt name, connects to it or writes the cache shows only as a repeating `ACME: a certificate
order failed; the tower retries with a growing delay`, `ACME: a certificate step failed` or
`ACME: the cache directory could not be read or written; certificates are not kept across
restarts`, and the service stays active. The same lines come from DNS, firewall or Let's
Encrypt trouble, so check those first (see [Start](#start)). The tests did not place a real
order under the sandbox. To check name resolution under the unit's own sandbox lines (not those
of its drop-ins), turn them into properties:

```bash
mapfile -t p < <(sed -n -E 's/^((DynamicUser|NoNewPrivileges|Protect[A-Za-z]+|Restrict[A-Za-z]+|PrivateDevices|LockPersonality|MemoryDenyWriteExecute|SystemCall[A-Za-z]+|ProcSubset|UMask)=.*)/--property=\1/p' /etc/systemd/system/numbers-tower.service)
```

and look the name up in a transient service with them:

```bash
sudo systemd-run --wait --pipe --quiet "${p[@]}" getent ahosts acme-staging-v02.api.letsencrypt.org
```

It should list the same addresses as `getent ahosts acme-staging-v02.api.letsencrypt.org` run
directly (with `production = true` the name is `acme-v02.api.letsencrypt.org`). On a host with
no IPv6 address other than `::1`, the sandboxed lookup also lists IPv6 addresses that the direct
one leaves out: `getent` asks for the address families the host has, which glibc finds out
through a netlink socket; the sandbox refuses that socket, and glibc then assumes both families.
Compare the IPv4 addresses on such a host. If they differ, or if the order keeps failing while
DNS and port 443 are fine, take settings out one at a time as described below, starting with
`SystemCallFilter=`, `MemoryDenyWriteExecute=` and `RestrictAddressFamilies=`.

### Taking one setting out on one host

If a refusal looks like the sandbox's, take the one setting out with a drop-in, restart, and see
whether the error goes away. An empty value resets a list such as `SystemCallFilter=`; a yes/no
setting takes `no`, and one with values takes its default (`ProcSubset=all`,
`ProtectProc=default`). Three settings cannot be taken out this way while `DynamicUser=yes` is
set: systemd keeps `NoNewPrivileges=yes` and `ProtectSystem=strict` whatever a drop-in says,
and turns `ProtectHome=no` into `ProtectHome=read-only`.
`systemctl show -p NoNewPrivileges -p ProtectSystem -p ProtectHome numbers-tower` shows the
values in effect. A refusal that stays with such a drop-in does not rule those settings out.

For the system-call filter:

```bash
sudo install -d -m 0755 /etc/systemd/system/numbers-tower.service.d
```

```bash
printf '[Service]\nSystemCallFilter=\n' | sudo tee /etc/systemd/system/numbers-tower.service.d/no-system-call-filter.conf
```

```bash
sudo systemctl daemon-reload
```

```bash
sudo systemctl restart numbers-tower
```

Under systemd 255, `systemctl status numbers-tower` then lists `no-system-call-filter.conf`
under `Drop-In:`, `systemctl show -p SystemCallFilter numbers-tower` prints
`SystemCallFilter=~` (no call is filtered), and `systemd-analyze security` gives `3.0 OK`. The
other settings, the certificate drop-in and `SystemCallArchitectures=native` stay in force.
Remove the file, run `sudo systemctl daemon-reload` and restart the tower to put the filter
back. If the filter was in the way, say which line of the journal showed it in an issue, so the
unit can be fixed for everyone.
