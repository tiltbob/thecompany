#!/usr/bin/env bash
# Runs `systemd-analyze verify` on the tower's systemd unit in packaging/systemd/, once alone (as
# shipped) and once with the example drop-in for certificate files. ExecStart points at a stub:
# verify checks only that the program exists and is executable, and a test in
# crates/tower/src/main.rs checks the arguments. Before those two runs, a preflight verifies a
# minimal unit of the script's own, to tell whether verify works in this environment at all.
# Outcomes:
# - pass: verify printed nothing and exited 0, in both runs;
# - fail: any message, a warning as much as an error, or a non-zero exit status, in either run;
# - systemd-analyze missing, `systemd-analyze --version` failing, a version older than 250 (no
#   --recursive-errors) or one that cannot be read, or a preflight that printed anything or exited
#   non-zero (verify cannot run here, see the comment above preflight()): fail if the environment
#   variable CI is set, otherwise print a `skipped:` line and succeed.
# Works from any directory; builds nothing.
set -euo pipefail
cd "$(dirname "$0")/.."

unit=packaging/systemd/numbers-tower.service
dropin=packaging/systemd/numbers-tower.service.d/tls-files.conf.example
installed=/usr/local/bin/tower
min_version=250

# skip_or_fail <reason>: fails under CI, skips otherwise.
skip_or_fail() {
    if [[ -n ${CI:-} ]]; then
        echo "error: $1" >&2
        exit 1
    fi
    echo "skipped: $1; the units were not verified"
    exit 0
}

if ! command -v systemd-analyze >/dev/null 2>&1; then
    skip_or_fail "systemd-analyze is not installed"
fi
if ! version_output=$(systemd-analyze --version); then
    skip_or_fail "systemd-analyze --version failed"
fi
version=$(awk 'NR == 1 { print $2 }' <<<"$version_output")
if ! [[ $version =~ ^[0-9]+$ ]] || ((version < min_version)); then
    skip_or_fail "systemd-analyze is version ${version:-unknown}; the check needs $min_version or later"
fi

count=$(grep -c "^ExecStart=$installed " "$unit" || true)
[[ $count == 1 ]] || {
    echo "error: $unit needs exactly one line starting with ExecStart=$installed" >&2
    exit 1
}

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

# The stub's path goes into the unit in double quotes, so white space and `&` need nothing;
# `%` (specifiers) is written `%%`. systemd 255 refuses an executable name with a backslash or a
# quote ("Executable name contains special characters"), so a scratch path with one of those (or
# a control character) is refused here, naming TMPDIR, instead of being blamed on the unit.
stub=$scratch/bin/tower
if [[ $stub == *[\\\"\']* || $stub == *[[:cntrl:]]* ]]; then
    echo "error: the scratch directory's path has a backslash, a quote or a control character;" \
        "set TMPDIR to a plainer directory: $scratch" >&2
    exit 1
fi
mkdir "$scratch/bin"
printf '#!/bin/sh\nexit 0\n' >"$stub"
chmod +x "$stub"
quoted=${stub//%/%%}

# How systemd-analyze reports (src/analyze/analyze-verify-util.c, read at 255): an error, such
# as an ExecStart program that is not executable, gives a non-zero exit status. A syntax
# warning, such as an unknown key or a value it cannot parse ("..., ignoring."), is only
# printed, unless --recursive-errors is given. With --recursive-errors=no a syntax warning in the
# unit or its drop-ins gives a non-zero exit status too, and the units it depends on
# (network-online.target, multi-user.target) are not loaded, so warnings about the host's own
# unit files cannot appear. verify needs no running systemd: it starts a manager in test mode.
# With 255 (as root, no systemd running) and with 259 on a GitHub runner that works as it is.
# It does not work everywhere: in an unprivileged container with systemd 257 and no
# /run/systemd, verify failed for every unit, a minimal one included, with "Failed to create
# directory '/run/systemd/': Permission denied" (seen in the review of this script; 258 is
# reported to do the same). The preflight below is for environments like that one.
#
# verify prints nothing for a clean unit, so any output also fails the check. No message has
# been seen from the host here (systemd 255 with no systemd running); if one shows up in CI,
# filter that exact message below, with a comment saying where it comes from.
#
# preflight: verify a minimal unit of our own, with the same options and the same stub, before
# ours. systemd 255 prints nothing for it and exits 0. Any output or a non-zero status means that
# verify does not work in this environment, whatever the reason (the /run/systemd case above is
# one), so it is a skip, or a failure under CI, and not blamed on our files. The result decides,
# not the text of a message. The path checks above come first, so a path systemd refuses is not
# taken for a broken environment.
preflight() {
    local status=0 output unit_file=$scratch/preflight/numbers-preflight.service
    mkdir "$scratch/preflight"
    printf '[Service]\nType=oneshot\nExecStart="%s"\n' "$quoted" >"$unit_file"
    output=$(systemd-analyze verify --recursive-errors=no "$unit_file" 2>&1) || status=$?
    if [[ $status != 0 || -n $output ]]; then
        local reason="systemd-analyze verify does not work in this environment: on a minimal"
        reason+=" unit of this script's own it exited with status $status and printed:"
        reason+=" ${output:-nothing}"
        skip_or_fail "$reason"
    fi
}

preflight

# The copy keeps the unit's file name (verify derives the unit's name from it) and its line
# numbers: one line is replaced by one line, without sed.
mkdir "$scratch/units"
copy=$scratch/units/$(basename "$unit")
while IFS= read -r line || [[ -n $line ]]; do
    if [[ $line == "ExecStart=$installed "* ]]; then
        line="ExecStart=\"$quoted\" ${line#"ExecStart=$installed "}"
    fi
    printf '%s\n' "$line"
done <"$unit" >"$copy"

# verify <run> <file>: runs after a clean preflight, so every message is about our files. <run>
# names the run, <file> the file in packaging/systemd/ it checks.
verify() {
    local status=0 output
    output=$(systemd-analyze verify --recursive-errors=no "$copy" 2>&1) || status=$?
    if [[ -n $output ]]; then
        printf '%s\n' "$output" >&2
    fi
    if [[ $status != 0 || -n $output ]]; then
        echo "error: systemd-analyze verify of $1 failed on $2 (exit status $status; the" \
            "scratch copies keep that file's line numbers)" >&2
        exit 1
    fi
    echo "ok: $1 ($2)"
}

verify "the unit alone" "$unit"

# verify loads drop-ins from <unit>.d/*.conf next to the unit file.
mkdir "$copy.d"
cp "$dropin" "$copy.d/tls-files.conf"
verify "the unit with the drop-in" "$dropin"

echo "ok: systemd-analyze version $version"
